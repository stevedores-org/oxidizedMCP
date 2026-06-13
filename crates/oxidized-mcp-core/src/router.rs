//! Aggregates tools/list and routes tools/call to skill HTTP backends.

use crate::auth::{AuthMode, Authenticator};
use crate::mcp_types::{JsonRpcRequest, JsonRpcResponse, ToolCallResult, ToolDescriptor};
use crate::registry::{RegistryLoader, RegistrySource, SkillEntry, SkillManifest};
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
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
    #[error("auth error calling skill '{0}': {1}")]
    Auth(String, #[source] crate::auth::AuthError),
    #[error("skill '{skill}' is unreachable (last error: {last_error})")]
    SkillUnreachable { skill: String, last_error: String },
}

/// Health status of a single skill as observed at the last `refresh`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SkillStatus {
    Healthy,
    Down,
}

/// Per-skill health observed at the last refresh. `tools_count` is the count
/// from the most recent successful `tools/list`; it stays at the last good
/// value while the skill is Down so operators can see what the skill used to
/// expose. `last_error` is populated whenever the latest probe failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillHealth {
    pub status: SkillStatus,
    pub tools_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Immutable view of the mesh at one moment. `refresh` builds a new snapshot
/// and atomically swaps it into place so concurrent readers always see a
/// consistent `(manifest, aggregated_tools, health)` triple.
#[derive(Default)]
struct MeshSnapshot {
    manifest: Option<SkillManifest>,
    aggregated_tools: Vec<ToolDescriptor>,
    health: BTreeMap<String, SkillHealth>,
}

pub struct SkillMesh {
    loader: RegistryLoader,
    registry_source: RegistrySource,
    client: reqwest::Client,
    snapshot: ArcSwap<MeshSnapshot>,
    authenticator: Authenticator,
}

impl SkillMesh {
    pub fn new(registry_source: RegistrySource) -> Self {
        Self::with_auth(registry_source, Authenticator::new(AuthMode::None))
    }

    /// Build a mesh that authenticates outbound `tools/list` + `tools/call`
    /// HTTP requests via the supplied [`Authenticator`]. Use this in
    /// production where skill endpoints sit behind a Gateway that requires
    /// Bearer tokens.
    pub fn with_auth(registry_source: RegistrySource, authenticator: Authenticator) -> Self {
        Self {
            loader: RegistryLoader::new(),
            registry_source,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("reqwest client"),
            snapshot: ArcSwap::from_pointee(MeshSnapshot::default()),
            authenticator,
        }
    }

    pub fn authenticator(&self) -> &Authenticator {
        &self.authenticator
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

    /// Per-skill health from the most recent refresh, keyed by skill name.
    /// BTreeMap so ops output is sorted deterministically.
    pub fn health(&self) -> BTreeMap<String, SkillHealth> {
        self.snapshot.load().health.clone()
    }

    pub async fn call_tool(
        &self,
        namespaced_name: &str,
        arguments: Value,
    ) -> Result<ToolCallResult, MeshError> {
        let (skill_name, tool_name) = parse_namespaced_tool(namespaced_name)?;
        // Fast-fail on skills we already know are down — the HTTP attempt
        // would burn the per-call timeout before returning the same error.
        if let Some(reason) = self.skill_unreachable_reason(&skill_name) {
            return Err(MeshError::SkillUnreachable {
                skill: skill_name,
                last_error: reason,
            });
        }
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

        let response = self.post_json_rpc(&skill_name, &endpoint, &request).await?;

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
        let mut health = BTreeMap::new();

        for skill in manifest.active_skills() {
            match self.fetch_skill_tools(skill).await {
                Ok(tools) => {
                    let tools_count = tools.len();
                    for tool in tools {
                        let namespaced =
                            format!("{}{}{}", skill.name, TOOL_NAMESPACE_SEP, tool.name);
                        aggregated.push(ToolDescriptor {
                            name: namespaced,
                            description: format!("{} — {}", skill.description, tool.description),
                            input_schema: tool.input_schema,
                        });
                    }
                    health.insert(
                        skill.name.clone(),
                        SkillHealth {
                            status: SkillStatus::Healthy,
                            tools_count,
                            last_error: None,
                        },
                    );
                }
                Err(e) => {
                    let err_str = e.to_string();
                    warn!(skill = %skill.name, error = %err_str, "skill probe failed; marking Down");
                    health.insert(
                        skill.name.clone(),
                        SkillHealth {
                            status: SkillStatus::Down,
                            tools_count: 0,
                            last_error: Some(err_str),
                        },
                    );
                }
            }
        }

        aggregated.sort_by(|a, b| a.name.cmp(&b.name));
        debug!(
            tool_count = aggregated.len(),
            skill_count = health.len(),
            "skill mesh snapshot built"
        );
        MeshSnapshot {
            manifest: Some(manifest),
            aggregated_tools: aggregated,
            health,
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
            .post_json_rpc(&skill.name, &skill.endpoint, &request)
            .await?;

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
        skill_name: &str,
        endpoint: &str,
        request: &JsonRpcRequest,
    ) -> Result<JsonRpcResponse, MeshError> {
        let mut req = self.client.post(endpoint).json(request);

        // Attach Bearer when the mesh is configured for authenticated outbound
        // calls. Auth failures are NOT silently swallowed — a misconfigured
        // gcloud session should surface as a clear MeshError, not as an
        // anonymous request that the cluster Gateway rejects with 401/403.
        if let Some(token) = self
            .authenticator
            .bearer_token()
            .await
            .map_err(|e| MeshError::Auth(skill_name.to_string(), e))?
        {
            req = req.bearer_auth(token);
        } else {
            // Fall back to Azure AD token if configured
            let use_azure = std::env::var("OXIDIZED_MCP_USE_AZURE_AD")
                .map(|v| v == "true")
                .unwrap_or(false)
                || std::env::var("OXIDIZED_MCP_ENV")
                    .map(|v| v == "staging" || v == "production")
                    .unwrap_or(false);

            if use_azure && endpoint.starts_with("https://") {
                match self.loader.fetch_azure_token().await {
                    Ok(token) => {
                        req = req.header("Authorization", format!("Bearer {token}"));
                    }
                    Err(e) => {
                        return Err(MeshError::Registry(
                            crate::registry::RegistryError::AzureAuth(e),
                        ));
                    }
                }
            }
        }

        req.send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| MeshError::Http(skill_name.to_string(), e))?
            .json()
            .await
            .map_err(|e| MeshError::Http(skill_name.to_string(), e))
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

    /// Returns Some(last_error) when the skill is known to be Down.
    /// Returns None when the skill is Healthy or has no recorded health yet
    /// (e.g. before the first refresh) — the caller proceeds with the HTTP
    /// attempt in that case, so we never reject calls based on stale absence.
    fn skill_unreachable_reason(&self, name: &str) -> Option<String> {
        let snapshot = self.snapshot.load();
        let health = snapshot.health.get(name)?;
        if health.status == SkillStatus::Down {
            Some(
                health
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string()),
            )
        } else {
            None
        }
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
    use axum::{response::IntoResponse, routing::post, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    /// Skill server that can be toggled between healthy (returns tools) and
    /// down (returns 500). Used to verify health tracking across refreshes
    /// and that call_tool fast-fails on Down skills.
    async fn mock_toggleable_skill_server() -> (String, Arc<AtomicBool>, tokio::task::JoinHandle<()>)
    {
        let down = Arc::new(AtomicBool::new(false));
        let app_down = down.clone();
        let app = Router::new().route(
            "/mcp",
            post(move |Json(req): Json<JsonRpcRequest>| {
                let d = app_down.clone();
                async move {
                    if d.load(Ordering::SeqCst) {
                        return (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            Json(JsonRpcResponse::err(req.id, -32000, "skill down")),
                        )
                            .into_response();
                    }
                    match req.method.as_str() {
                        "tools/list" => Json(JsonRpcResponse::ok(
                            req.id,
                            serde_json::to_value(ToolsListResult {
                                tools: vec![ToolDescriptor {
                                    name: "ping".to_string(),
                                    description: "Ping".to_string(),
                                    input_schema: json!({}),
                                }],
                            })
                            .unwrap(),
                        ))
                        .into_response(),
                        "tools/call" => Json(JsonRpcResponse::ok(
                            req.id,
                            serde_json::to_value(ToolCallResult::text("pong")).unwrap(),
                        ))
                        .into_response(),
                        _ => Json(JsonRpcResponse::err(req.id, -32601, "unknown")).into_response(),
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}/mcp"), down, handle)
    }

    fn write_registry(yaml: &str, tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("oxidized-mcp-{tag}-{}", uuid_simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.yaml");
        std::fs::write(&path, yaml).unwrap();
        path
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
        let path = write_registry(&yaml, "discover");

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
        let path = write_registry(&yaml, "rtest");

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
        let path = write_registry(&yaml, "stale");

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

    /// A down skill is recorded as such in the health map after refresh, and
    /// once the skill recovers a subsequent refresh marks it Healthy again.
    #[tokio::test]
    async fn health_tracks_skill_status_across_refreshes() {
        let (endpoint, down, _handle) = mock_toggleable_skill_server().await;
        let yaml = format!(
            r#"
version: 1
environment: test
skills:
  - name: flaky
    description: Toggleable skill
    endpoint: {endpoint}
"#
        );
        let path = write_registry(&yaml, "health");

        let mesh = SkillMesh::new(RegistrySource::File(path));
        mesh.refresh().await.unwrap();
        let h = mesh.health();
        assert_eq!(h["flaky"].status, SkillStatus::Healthy);
        assert_eq!(h["flaky"].tools_count, 1);
        assert!(h["flaky"].last_error.is_none());

        // Take the skill down and refresh again.
        down.store(true, Ordering::SeqCst);
        mesh.refresh().await.unwrap();
        let h = mesh.health();
        assert_eq!(h["flaky"].status, SkillStatus::Down);
        assert!(h["flaky"].last_error.is_some());

        // Recover the skill and refresh — status returns to Healthy.
        down.store(false, Ordering::SeqCst);
        mesh.refresh().await.unwrap();
        let h = mesh.health();
        assert_eq!(h["flaky"].status, SkillStatus::Healthy);
        assert_eq!(h["flaky"].tools_count, 1);
    }

    /// call_tool refuses to issue a network request when the skill is known
    /// Down — the error must be SkillUnreachable, not Http, proving the fast
    /// path triggered.
    #[tokio::test]
    async fn call_tool_fast_fails_on_down_skill() {
        let (endpoint, down, _handle) = mock_toggleable_skill_server().await;
        let yaml = format!(
            r#"
version: 1
environment: test
skills:
  - name: flaky
    description: Toggleable skill
    endpoint: {endpoint}
"#
        );
        let path = write_registry(&yaml, "fastfail");

        let mesh = SkillMesh::new(RegistrySource::File(path));
        mesh.refresh().await.unwrap();

        // Sanity: healthy → call_tool succeeds.
        let ok = mesh.call_tool("flaky::ping", json!({})).await;
        assert!(ok.is_ok(), "healthy skill call should succeed, got {ok:?}");

        // Take down, refresh so the snapshot reflects the failure, then call.
        down.store(true, Ordering::SeqCst);
        mesh.refresh().await.unwrap();
        let err = mesh
            .call_tool("flaky::ping", json!({}))
            .await
            .expect_err("Down skill must fail");
        match err {
            MeshError::SkillUnreachable { skill, last_error } => {
                assert_eq!(skill, "flaky");
                assert!(
                    !last_error.is_empty(),
                    "fast-fail must include the recorded probe error"
                );
            }
            other => panic!("expected SkillUnreachable, got {other:?}"),
        }
    }

    fn uuid_simple() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    /// Skill server that captures the inbound `Authorization` header on every
    /// request so the test can verify what oxidizedMCP actually sent.
    async fn mock_skill_server_capturing_auth() -> (
        String,
        Arc<std::sync::Mutex<Vec<Option<String>>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let captured: Arc<std::sync::Mutex<Vec<Option<String>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_app = captured.clone();
        let app = Router::new().route(
            "/mcp",
            post(
                move |headers: axum::http::HeaderMap, Json(req): Json<JsonRpcRequest>| {
                    let captured = captured_app.clone();
                    async move {
                        let auth_value = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.to_string());
                        captured.lock().unwrap().push(auth_value);
                        Json(JsonRpcResponse::ok(
                            req.id,
                            serde_json::to_value(ToolsListResult {
                                tools: vec![ToolDescriptor {
                                    name: "echo".to_string(),
                                    description: "Echo".to_string(),
                                    input_schema: json!({}),
                                }],
                            })
                            .unwrap(),
                        ))
                    }
                },
            ),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}/mcp"), captured, handle)
    }

    #[tokio::test]
    async fn auth_mode_none_sends_no_authorization_header() {
        let (endpoint, captured, _handle) = mock_skill_server_capturing_auth().await;
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
        let dir = std::env::temp_dir().join(format!("oxidized-mcp-auth-none-{}", uuid_simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.yaml");
        std::fs::write(&path, yaml).unwrap();

        let mesh = SkillMesh::new(RegistrySource::File(path));
        mesh.refresh().await.unwrap();

        let seen = captured.lock().unwrap().clone();
        assert!(
            !seen.is_empty(),
            "skill server should have received at least one request"
        );
        assert!(
            seen.iter().all(|h| h.is_none()),
            "AuthMode::None must not send an Authorization header, got: {seen:?}"
        );
    }

    /// Inject a fake gcloud script via `Authenticator::with_gcloud_binary` so
    /// the wiring from AuthMode::GcloudIdentity → outbound header is verified
    /// end-to-end without mutating PATH (which would race with parallel tests).
    #[tokio::test]
    async fn auth_mode_gcloud_identity_sends_bearer_header() {
        let (endpoint, captured, _handle) = mock_skill_server_capturing_auth().await;
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
        let dir = std::env::temp_dir().join(format!("oxidized-mcp-auth-gcloud-{}", uuid_simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let registry_path = dir.join("registry.yaml");
        std::fs::write(&registry_path, yaml).unwrap();

        // Fake gcloud script that prints a deterministic token. We point the
        // Authenticator at it directly via with_gcloud_binary — no PATH mutation.
        let gcloud = dir.join("fake-gcloud");
        std::fs::write(&gcloud, "#!/bin/sh\nprintf 'test-id-token-xyz\\n'\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&gcloud, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mesh = SkillMesh::with_auth(
            RegistrySource::File(registry_path),
            Authenticator::with_gcloud_binary(
                AuthMode::GcloudIdentity { audience: None },
                gcloud.to_string_lossy().into_owned(),
            ),
        );
        mesh.refresh()
            .await
            .expect("refresh should succeed with mock gcloud");

        let seen = captured.lock().unwrap().clone();
        assert!(!seen.is_empty(), "expected at least one outbound request");
        assert!(
            seen.iter()
                .all(|h| h.as_deref() == Some("Bearer test-id-token-xyz")),
            "every outbound request must carry the Bearer token, got: {seen:?}"
        );
    }
}
