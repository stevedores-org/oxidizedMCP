//! Aggregates tools/list and routes tools/call to skill HTTP backends.

use crate::mcp_types::{JsonRpcRequest, JsonRpcResponse, ToolCallResult, ToolDescriptor};
use crate::registry::{RegistryLoader, RegistrySource, SkillEntry, SkillManifest};
use arc_swap::ArcSwap;
use serde_json::{json, Value};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, warn};

pub const TOOL_NAMESPACE_SEP: &str = "::";

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

/// Immutable view of the mesh at one moment. `refresh` builds a new snapshot
/// and atomically swaps it into place so concurrent readers always see a
/// consistent `(manifest, aggregated_tools)` pair.
#[derive(Default)]
struct MeshSnapshot {
    manifest: Option<SkillManifest>,
    aggregated_tools: Vec<ToolDescriptor>,
}

pub struct SkillMesh {
    loader: RegistryLoader,
    registry_source: RegistrySource,
    client: reqwest::Client,
    snapshot: ArcSwap<MeshSnapshot>,
}

impl SkillMesh {
    pub fn new(registry_source: RegistrySource) -> Self {
        Self {
            loader: RegistryLoader::new(),
            registry_source,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("reqwest client"),
            snapshot: ArcSwap::from_pointee(MeshSnapshot::default()),
        }
    }

    /// Re-fetch the registry, re-discover tools, and atomically swap in the
    /// new snapshot. `&self` so the call is safe to make from a background
    /// refresh task while readers are calling `list_tools`/`call_tool`.
    pub async fn refresh(&self) -> Result<(), MeshError> {
        let manifest = self.loader.load(&self.registry_source).await?;
        let new_snapshot = self.build_snapshot(manifest).await;
        self.snapshot.store(Arc::new(new_snapshot));
        Ok(())
    }

    pub fn manifest(&self) -> Option<SkillManifest> {
        self.snapshot.load().manifest.clone()
    }

    /// Returns a clone of the currently-aggregated tool list. Cloning is cheap
    /// at the scales we expect (tens to low hundreds of tools); the alternative
    /// of exposing the underlying Arc would force callers to manage lifetimes
    /// against the next swap.
    pub fn list_tools(&self) -> Vec<ToolDescriptor> {
        self.snapshot.load().aggregated_tools.clone()
    }

    pub async fn call_tool(
        &self,
        namespaced_name: &str,
        arguments: Value,
    ) -> Result<ToolCallResult, MeshError> {
        let (skill_name, tool_name) = parse_namespaced_tool(namespaced_name)?;
        let endpoint = self.resolve_skill_endpoint(&skill_name)?;

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
            .post_json_rpc(&endpoint, &request)
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

        serde_json::from_value(result).map_err(|e| MeshError::SkillError(skill_name, e.to_string()))
    }

    async fn build_snapshot(&self, manifest: SkillManifest) -> MeshSnapshot {
        let mut aggregated = Vec::new();

        for skill in manifest.active_skills() {
            match self.fetch_skill_tools(skill).await {
                Ok(tools) => {
                    for tool in tools {
                        let namespaced =
                            format!("{}{}{}", skill.name, TOOL_NAMESPACE_SEP, tool.name);
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
        debug!(tool_count = aggregated.len(), "skill mesh snapshot built");
        MeshSnapshot {
            manifest: Some(manifest),
            aggregated_tools: aggregated,
        }
    }

    async fn fetch_skill_tools(
        &self,
        skill: &SkillEntry,
    ) -> Result<Vec<ToolDescriptor>, MeshError> {
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

        let tools: Vec<ToolDescriptor> =
            if let Some(arr) = result.get("tools").and_then(|v| v.as_array()) {
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

    fn resolve_skill_endpoint(&self, name: &str) -> Result<String, MeshError> {
        let snapshot = self.snapshot.load();
        let manifest = snapshot
            .manifest
            .as_ref()
            .ok_or_else(|| MeshError::SkillNotFound(name.to_string()))?;
        let endpoint = manifest
            .active_skills()
            .find(|s| s.name == name)
            .map(|s| s.endpoint.clone())
            .ok_or_else(|| MeshError::SkillNotFound(name.to_string()))?;
        Ok(endpoint)
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    /// Skill server that swaps its advertised tool list when its request
    /// counter crosses a threshold — used to verify that a second refresh
    /// picks up newly added tools.
    async fn mock_skill_server_with_versions(
        flip_after_n_requests: usize,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let counter = Arc::new(AtomicUsize::new(0));
        let app_counter = counter.clone();
        let app = Router::new().route(
            "/mcp",
            post(move |Json(req): Json<JsonRpcRequest>| {
                let c = app_counter.clone();
                async move {
                    if req.method == "tools/list" {
                        let seen = c.fetch_add(1, Ordering::SeqCst);
                        let tools = if seen < flip_after_n_requests {
                            vec![ToolDescriptor {
                                name: "v1".to_string(),
                                description: "first generation".to_string(),
                                input_schema: json!({}),
                            }]
                        } else {
                            vec![
                                ToolDescriptor {
                                    name: "v1".to_string(),
                                    description: "first generation".to_string(),
                                    input_schema: json!({}),
                                },
                                ToolDescriptor {
                                    name: "v2".to_string(),
                                    description: "added later".to_string(),
                                    input_schema: json!({}),
                                },
                            ]
                        };
                        Json(JsonRpcResponse::ok(
                            req.id,
                            serde_json::to_value(ToolsListResult { tools }).unwrap(),
                        ))
                    } else {
                        Json(JsonRpcResponse::err(req.id, -32601, "unknown method"))
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}/mcp"), counter, handle)
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

        let mesh = SkillMesh::new(RegistrySource::File(path));
        mesh.refresh().await.unwrap();
        let tools = mesh.list_tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "echo-skill::echo");
    }

    /// A second refresh picks up tools that appeared on a skill between calls,
    /// and the new snapshot is visible to read paths without restarting.
    #[tokio::test]
    async fn refresh_picks_up_new_tools_atomically() {
        let (endpoint, _counter, _handle) = mock_skill_server_with_versions(1).await;
        let yaml = format!(
            r#"
version: 1
environment: test
skills:
  - name: ver
    description: Versioned skill
    endpoint: {endpoint}
"#
        );
        let dir = std::env::temp_dir().join(format!("oxidized-mcp-rtest-{}", uuid_simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.yaml");
        std::fs::write(&path, yaml).unwrap();

        let mesh = Arc::new(SkillMesh::new(RegistrySource::File(path)));
        mesh.refresh().await.unwrap();
        let first = mesh.list_tools();
        assert_eq!(first.len(), 1, "first refresh sees one tool");
        assert_eq!(first[0].name, "ver::v1");

        // Refresh from a clone of the same Arc — proves &self is enough and
        // that a background task could legitimately drive refreshes.
        let bg = mesh.clone();
        bg.refresh().await.unwrap();

        let second = mesh.list_tools();
        assert_eq!(second.len(), 2, "second refresh picks up v2");
        let names: Vec<_> = second.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"ver::v1"));
        assert!(names.contains(&"ver::v2"));
    }

    /// If a refresh fails (e.g. registry source unreachable), the previously
    /// loaded snapshot must remain in place — readers continue working.
    #[tokio::test]
    async fn refresh_failure_preserves_previous_snapshot() {
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
        let dir = std::env::temp_dir().join(format!("oxidized-mcp-stale-{}", uuid_simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.yaml");
        std::fs::write(&path, &yaml).unwrap();

        let mesh = SkillMesh::new(RegistrySource::File(path.clone()));
        mesh.refresh().await.unwrap();
        assert_eq!(mesh.list_tools().len(), 1);

        // Remove the registry file — next refresh must error but leave the
        // prior snapshot intact for readers.
        std::fs::remove_file(&path).unwrap();
        assert!(mesh.refresh().await.is_err());
        let after = mesh.list_tools();
        assert_eq!(after.len(), 1, "stale snapshot survives a failed refresh");
        assert_eq!(after[0].name, "echo-skill::echo");
    }

    fn uuid_simple() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}
