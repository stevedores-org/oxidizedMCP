//! Aggregates tools/list and routes tools/call to skill HTTP backends.

use crate::mcp_types::{JsonRpcRequest, JsonRpcResponse, ToolCallResult, ToolDescriptor};
use crate::registry::{RegistryLoader, RegistrySource, SkillEntry, SkillManifest};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, warn};

pub const TOOL_NAMESPACE_SEP: &str = "::";

/// Default TTL between `tools/list` refreshes. Matches the
/// Issue #3 Epic 3 Feature 3.1 acceptance criterion (60 seconds).
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Error)]
pub enum MeshError {
    #[error("registry error: {0}")]
    Registry(#[from] crate::registry::RegistryError),
    #[error("skill '{0}' not found")]
    SkillNotFound(String),
    #[error("tool '{tool}' not found on skill '{skill}'")]
    ToolNotFound { tool: String, skill: String },
    #[error("invalid namespaced tool '{0}' (expected skill::tool)")]
    InvalidToolName(String),
    #[error("HTTP error calling skill '{0}': {1}")]
    Http(String, #[source] reqwest::Error),
    #[error("skill '{0}' returned error: {1}")]
    SkillError(String, String),
}

pub struct SkillMesh {
    loader: RegistryLoader,
    registry_source: RegistrySource,
    client: reqwest::Client,
    manifest: Option<SkillManifest>,
    tool_index: HashMap<String, (String, String)>,
    aggregated_tools: Vec<ToolDescriptor>,
    refresh_interval: Duration,
    last_refreshed: Option<Instant>,
}

impl SkillMesh {
    pub fn new(registry_source: RegistrySource) -> Self {
        Self::with_refresh_interval(registry_source, DEFAULT_REFRESH_INTERVAL)
    }

    /// Construct with a custom TTL between automatic `tools/list` refreshes.
    /// A zero duration disables auto-refresh — callers must invoke `refresh()` explicitly.
    pub fn with_refresh_interval(registry_source: RegistrySource, refresh_interval: Duration) -> Self {
        Self {
            loader: RegistryLoader::new(),
            registry_source,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("reqwest client"),
            manifest: None,
            tool_index: HashMap::new(),
            aggregated_tools: Vec::new(),
            refresh_interval,
            last_refreshed: None,
        }
    }

    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }

    pub async fn refresh(&mut self) -> Result<(), MeshError> {
        let manifest = self.loader.load(&self.registry_source).await?;
        self.rebuild_index(&manifest).await?;
        self.manifest = Some(manifest);
        self.last_refreshed = Some(Instant::now());
        Ok(())
    }

    /// Refresh from the registry only if the cache has expired (or hasn't been
    /// populated yet). Returns `true` when a refresh was performed.
    ///
    /// Skill backends are not contacted while the cache is fresh — this is the
    /// path the stdio server takes on every `tools/list` so cold-IDE-restarts
    /// don't stampede the cluster's skill pods.
    pub async fn refresh_if_stale(&mut self) -> Result<bool, MeshError> {
        let is_stale = match self.last_refreshed {
            None => true,
            Some(_) if self.refresh_interval.is_zero() => false,
            Some(last) => last.elapsed() >= self.refresh_interval,
        };
        if is_stale {
            self.refresh().await?;
        }
        Ok(is_stale)
    }

    pub fn manifest(&self) -> Option<&SkillManifest> {
        self.manifest.as_ref()
    }

    pub fn list_tools(&self) -> &[ToolDescriptor] {
        &self.aggregated_tools
    }

    pub async fn call_tool(&self, namespaced_name: &str, arguments: Value) -> Result<ToolCallResult, MeshError> {
        let (skill_name, tool_name) = parse_namespaced_tool(namespaced_name)?;
        let skill = self.find_skill(&skill_name)?;

        let request = JsonRpcRequest {
            jsonrpc: crate::mcp_types::JSONRPC_VERSION.to_string(),
            id: Some(json!(1)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": tool_name,
                "arguments": arguments
            })),
        };

        let response = self
            .post_json_rpc(&skill.endpoint, &request)
            .await
            .map_err(|e| MeshError::Http(skill_name.clone(), e))?;

        if let Some(err) = response.error {
            return Err(MeshError::SkillError(
                skill_name.clone(),
                format!("{} ({})", err.message, err.code),
            ));
        }

        let result = response
            .result
            .ok_or_else(|| MeshError::SkillError(skill_name.clone(), "empty result".into()))?;

        serde_json::from_value(result)
            .map_err(|e| MeshError::SkillError(skill_name, e.to_string()))
    }

    async fn rebuild_index(&mut self, manifest: &SkillManifest) -> Result<(), MeshError> {
        let mut aggregated = Vec::new();
        let mut index = HashMap::new();

        for skill in manifest.active_skills() {
            match self.fetch_skill_tools(skill).await {
                Ok(tools) => {
                    for tool in tools {
                        let namespaced = format!("{}{}{}", skill.name, TOOL_NAMESPACE_SEP, tool.name);
                        index.insert(
                            namespaced.clone(),
                            (skill.name.clone(), tool.name.clone()),
                        );
                        aggregated.push(ToolDescriptor {
                            name: namespaced,
                            description: format!("{} — {}", skill.description, tool.description),
                            input_schema: tool.input_schema,
                        });
                    }
                }
                Err(e) => {
                    warn!(skill = %skill.name, error = %e, "skipping skill during discovery");
                }
            }
        }

        aggregated.sort_by(|a, b| a.name.cmp(&b.name));
        self.aggregated_tools = aggregated;
        self.tool_index = index;
        debug!(tool_count = self.aggregated_tools.len(), "skill mesh indexed");
        Ok(())
    }

    async fn fetch_skill_tools(&self, skill: &SkillEntry) -> Result<Vec<ToolDescriptor>, MeshError> {
        let request = JsonRpcRequest {
            jsonrpc: crate::mcp_types::JSONRPC_VERSION.to_string(),
            id: Some(json!(1)),
            method: "tools/list".to_string(),
            params: None,
        };

        let response = self
            .post_json_rpc(&skill.endpoint, &request)
            .await
            .map_err(|e| MeshError::Http(skill.name.clone(), e))?;

        if let Some(err) = response.error {
            return Err(MeshError::SkillError(
                skill.name.clone(),
                format!("{} ({})", err.message, err.code),
            ));
        }

        let result = response
            .result
            .ok_or_else(|| MeshError::SkillError(skill.name.clone(), "empty tools/list".into()))?;

        let tools: Vec<ToolDescriptor> = if let Some(arr) = result.get("tools").and_then(|v| v.as_array()) {
            arr.iter()
                .filter_map(|v| serde_json::from_value(v.clone()).ok())
                .collect()
        } else {
            serde_json::from_value(result).map_err(|e| {
                MeshError::SkillError(skill.name.clone(), format!("invalid tools/list: {e}"))
            })?
        };

        Ok(tools)
    }

    async fn post_json_rpc(
        &self,
        endpoint: &str,
        request: &JsonRpcRequest,
    ) -> Result<JsonRpcResponse, reqwest::Error> {
        self.client
            .post(endpoint)
            .json(request)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    fn find_skill(&self, name: &str) -> Result<&SkillEntry, MeshError> {
        let manifest = self
            .manifest
            .as_ref()
            .ok_or_else(|| MeshError::SkillNotFound(name.to_string()))?;
        manifest
            .active_skills()
            .find(|s| s.name == name)
            .ok_or_else(|| MeshError::SkillNotFound(name.to_string()))
    }
}

pub fn parse_namespaced_tool(name: &str) -> Result<(String, String), MeshError> {
    let (skill, tool) = name
        .split_once(TOOL_NAMESPACE_SEP)
        .ok_or_else(|| MeshError::InvalidToolName(name.to_string()))?;
    if skill.is_empty() || tool.is_empty() {
        return Err(MeshError::InvalidToolName(name.to_string()));
    }
    Ok((skill.to_string(), tool.to_string()))
}

pub fn namespaced_tool(skill: &str, tool: &str) -> String {
    format!("{skill}{TOOL_NAMESPACE_SEP}{tool}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_types::{JsonRpcResponse, ToolsListResult};
    use axum::{routing::post, Json, Router};
    use serde_json::json;

    async fn mock_skill_server() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/mcp",
            post(|Json(req): Json<JsonRpcRequest>| async move {
                if req.method == "tools/list" {
                    let result = ToolsListResult {
                        tools: vec![ToolDescriptor {
                            name: "echo".to_string(),
                            description: "Echo input".to_string(),
                            input_schema: json!({
                                "type": "object",
                                "properties": { "message": { "type": "string" } }
                            }),
                        }],
                    };
                    Json(JsonRpcResponse::ok(
                        req.id,
                        serde_json::to_value(result).unwrap(),
                    ))
                } else {
                    Json(JsonRpcResponse::err(req.id, -32601, "unknown method"))
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}/mcp"), handle)
    }

    #[tokio::test]
    async fn discovers_and_lists_namespaced_tools() {
        let (endpoint, _handle) = mock_skill_server().await;
        let yaml = format!(
            r#"
version: 1
environment: test
skills:
  - name: echo-skill
    description: Mock echo
    endpoint: {endpoint}
"#
        );
        let dir = std::env::temp_dir().join(format!("oxidized-mcp-test-{}", uuid_simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.yaml");
        std::fs::write(&path, yaml).unwrap();

        let mut mesh = SkillMesh::new(RegistrySource::File(path));
        mesh.refresh().await.unwrap();
        let tools = mesh.list_tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "echo-skill::echo");
    }

    #[tokio::test]
    async fn refresh_if_stale_populates_on_first_call() {
        let (endpoint, _handle) = mock_skill_server().await;
        let path = write_registry(&endpoint);

        let mut mesh = SkillMesh::with_refresh_interval(
            RegistrySource::File(path),
            Duration::from_secs(60),
        );

        let refreshed = mesh.refresh_if_stale().await.unwrap();
        assert!(refreshed, "first call must refresh from cold cache");
        assert_eq!(mesh.list_tools().len(), 1);
    }

    #[tokio::test]
    async fn refresh_if_stale_is_noop_within_ttl() {
        let (endpoint, _handle) = mock_skill_server().await;
        let path = write_registry(&endpoint);

        let mut mesh = SkillMesh::with_refresh_interval(
            RegistrySource::File(path),
            Duration::from_secs(60),
        );

        mesh.refresh().await.unwrap();
        let refreshed = mesh.refresh_if_stale().await.unwrap();
        assert!(!refreshed, "second call within TTL must not refresh");
    }

    #[tokio::test]
    async fn refresh_if_stale_refetches_after_ttl_elapses() {
        let (endpoint, _handle) = mock_skill_server().await;
        let path = write_registry(&endpoint);

        // Sub-tick TTL so the test stays fast; behaviour is identical at 60s.
        let mut mesh = SkillMesh::with_refresh_interval(
            RegistrySource::File(path),
            Duration::from_millis(50),
        );

        mesh.refresh().await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let refreshed = mesh.refresh_if_stale().await.unwrap();
        assert!(refreshed, "must refresh once TTL has elapsed");
    }

    #[tokio::test]
    async fn refresh_if_stale_with_zero_ttl_only_refreshes_once() {
        let (endpoint, _handle) = mock_skill_server().await;
        let path = write_registry(&endpoint);

        let mut mesh = SkillMesh::with_refresh_interval(
            RegistrySource::File(path),
            Duration::ZERO,
        );

        let first = mesh.refresh_if_stale().await.unwrap();
        let second = mesh.refresh_if_stale().await.unwrap();
        assert!(first, "cold cache must refresh even with TTL=0");
        assert!(!second, "TTL=0 disables auto-refresh after initial populate");
    }

    fn write_registry(endpoint: &str) -> std::path::PathBuf {
        let yaml = format!(
            r#"
version: 1
environment: test
skills:
  - name: echo-skill
    description: Mock echo
    endpoint: {endpoint}
"#
        );
        let dir = std::env::temp_dir().join(format!("oxidized-mcp-test-{}", uuid_simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.yaml");
        std::fs::write(&path, yaml).unwrap();
        path
    }

    fn uuid_simple() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}
