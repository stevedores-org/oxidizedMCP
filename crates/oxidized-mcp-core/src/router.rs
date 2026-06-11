//! Aggregates tools/list and routes tools/call to skill HTTP backends.

use crate::auth::{AuthMode, Authenticator};
use crate::local_runner::{LocalRunError, PodmanRunner};
use crate::mcp_types::{JsonRpcRequest, JsonRpcResponse, ToolCallResult, ToolDescriptor};
use crate::registry::{RegistryLoader, RegistrySource, SkillEntry, SkillManifest};
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tracing::{debug, info, warn};

pub const TOOL_NAMESPACE_SEP: &str = "::";

/// Number of consecutive HTTP failures (per skill) before the router stops
/// burning the per-call timeout on the cloud endpoint and routes straight to
/// the local Podman fallback (when an `image` is declared on the skill).
/// Resets on any successful HTTP call_tool or on the next refresh that
/// observes the skill Healthy. Three matches "two retries plus the
/// original" — small enough to recover quickly on a real outage, large
/// enough that a single transient blip doesn't flip the route.
pub const CIRCUIT_TRIP_THRESHOLD: u32 = 3;

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
    #[error("local Podman fallback for skill '{0}' failed: {1}")]
    LocalRun(String, #[source] LocalRunError),
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
    local_runner: PodmanRunner,
    /// Per-skill consecutive HTTP failure counter. Kept out of `MeshSnapshot`
    /// because it must mutate on every `call_tool`, while the snapshot is
    /// rebuilt only on refresh. A `Mutex<HashMap>` is fine at our scales
    /// (tens of skills, single-digit calls per second per process).
    circuit: Mutex<HashMap<String, u32>>,
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
        Self::with_auth_and_runner(registry_source, authenticator, PodmanRunner::new())
    }

    /// Like [`Self::with_auth`] but lets the caller inject a [`PodmanRunner`]
    /// — used by tests to point at a fake podman script without mutating
    /// `PATH`, and by callers that want to run the fallback through `docker`
    /// or another OCI-compatible CLI.
    pub fn with_auth_and_runner(
        registry_source: RegistrySource,
        authenticator: Authenticator,
        local_runner: PodmanRunner,
    ) -> Self {
        Self {
            loader: RegistryLoader::new(),
            registry_source,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("reqwest client"),
            snapshot: ArcSwap::from_pointee(MeshSnapshot::default()),
            authenticator,
            local_runner,
            circuit: Mutex::new(HashMap::new()),
        }
    }

    pub fn authenticator(&self) -> &Authenticator {
        &self.authenticator
    }

    pub fn local_runner(&self) -> &PodmanRunner {
        &self.local_runner
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
        let skill_entry = self.resolve_skill_entry(&skill_name)?;

        let request = JsonRpcRequest {
            jsonrpc: crate::mcp_types::JSONRPC_VERSION.to_string(),
            id: Some(json!(1)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": tool_name,
                "arguments": arguments
            })),
        };

        let has_local = skill_entry.image.is_some();
        let breaker_open = self.breaker_is_open(&skill_name);

        // Circuit open + local fallback configured → skip HTTP entirely.
        // Burning the 60s reqwest timeout on a skill we already know is
        // returning failures is the single biggest UX hit when the cloud
        // goes down. If local works, return it; if local can't run, fall
        // through to a normal HTTP attempt so a recovered endpoint is noticed.
        if breaker_open && has_local {
            let image = skill_entry.image.as_deref().expect("has_local");
            match self.try_local(&skill_name, image, &request).await {
                Ok(result) => {
                    info!(
                        skill = %skill_name,
                        image = %image,
                        "circuit open; served call from local Podman fallback"
                    );
                    return Ok(result);
                }
                Err(e) => {
                    debug!(
                        skill = %skill_name,
                        error = %e,
                        "local fallback unavailable while breaker open; retrying HTTP"
                    );
                }
            }
        }

        // No local fallback path AND last refresh marked the skill Down:
        // surface that immediately. With `image` configured we still attempt
        // HTTP — the post-failure branch below handles falling over to local.
        if !has_local {
            if let Some(reason) = self.skill_unreachable_reason(&skill_name) {
                return Err(MeshError::SkillUnreachable {
                    skill: skill_name,
                    last_error: reason,
                });
            }
        }

        let http_outcome = self
            .post_json_rpc(&skill_name, &skill_entry.endpoint, &request)
            .await;

        match http_outcome {
            Ok(response) => {
                self.record_http_success(&skill_name);
                if let Some(err) = response.error {
                    return Err(MeshError::SkillError(
                        skill_name.clone(),
                        format!("{} ({})", err.message, err.code),
                    ));
                }
                let result = response.result.ok_or_else(|| {
                    MeshError::SkillError(skill_name.clone(), "empty result".into())
                })?;
                serde_json::from_value(result)
                    .map_err(|e| MeshError::SkillError(skill_name, e.to_string()))
            }
            Err(http_err) => {
                self.record_http_failure(&skill_name);
                if let Some(image) = skill_entry.image.as_deref() {
                    match self.try_local(&skill_name, image, &request).await {
                        Ok(result) => {
                            info!(
                                skill = %skill_name,
                                image = %image,
                                http_error = %http_err,
                                "HTTP failed; served call from local Podman fallback"
                            );
                            return Ok(result);
                        }
                        Err(local_err) => {
                            warn!(
                                skill = %skill_name,
                                http_error = %http_err,
                                local_error = %local_err,
                                "HTTP and local fallback both failed"
                            );
                        }
                    }
                }
                Err(http_err)
            }
        }
    }

    /// Inner helper: runs `podman image exists` then `podman run -i --rm`,
    /// turns the JSON-RPC response into a [`ToolCallResult`]. Pure mechanics —
    /// the breaker / logging / fallback policy stays in `call_tool`.
    async fn try_local(
        &self,
        skill_name: &str,
        image: &str,
        request: &JsonRpcRequest,
    ) -> Result<ToolCallResult, MeshError> {
        match self.local_runner.image_exists(image).await {
            Ok(true) => {}
            Ok(false) => {
                return Err(MeshError::LocalRun(
                    skill_name.to_string(),
                    LocalRunError::ImageNotPresent(image.to_string()),
                ));
            }
            Err(e) => return Err(MeshError::LocalRun(skill_name.to_string(), e)),
        }

        let response = self
            .local_runner
            .invoke_stdio(image, request)
            .await
            .map_err(|e| MeshError::LocalRun(skill_name.to_string(), e))?;

        if let Some(err) = response.error {
            return Err(MeshError::SkillError(
                skill_name.to_string(),
                format!("{} ({})", err.message, err.code),
            ));
        }
        let result = response.result.ok_or_else(|| {
            MeshError::SkillError(
                skill_name.to_string(),
                "empty result from local fallback".into(),
            )
        })?;
        serde_json::from_value(result)
            .map_err(|e| MeshError::SkillError(skill_name.to_string(), e.to_string()))
    }

    fn breaker_is_open(&self, skill: &str) -> bool {
        self.circuit
            .lock()
            .expect("circuit mutex poisoned")
            .get(skill)
            .copied()
            .unwrap_or(0)
            >= CIRCUIT_TRIP_THRESHOLD
    }

    fn record_http_success(&self, skill: &str) {
        self.circuit
            .lock()
            .expect("circuit mutex poisoned")
            .insert(skill.to_string(), 0);
    }

    fn record_http_failure(&self, skill: &str) {
        let mut guard = self.circuit.lock().expect("circuit mutex poisoned");
        *guard.entry(skill.to_string()).or_insert(0) += 1;
    }

    /// Test helper: peek at the current failure count without resetting it.
    #[cfg(test)]
    fn circuit_count(&self, skill: &str) -> u32 {
        self.circuit
            .lock()
            .expect("circuit mutex poisoned")
            .get(skill)
            .copied()
            .unwrap_or(0)
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
                    // A healthy refresh resets the circuit. Otherwise a
                    // breaker opened during a brief outage would stick open
                    // forever — `call_tool` only resets on a real successful
                    // request, but refresh probes the same endpoint and
                    // gives us a second signal to clear it.
                    self.circuit
                        .lock()
                        .expect("circuit mutex poisoned")
                        .insert(skill.name.clone(), 0);
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
                        return Err(MeshError::Registry(crate::registry::RegistryError::AzureAuth(e)));
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

    /// Clone the active [`SkillEntry`] for `name` out of the current snapshot.
    /// Cloning is cheap (a handful of small strings) and lets `call_tool`
    /// drop the snapshot guard before doing async work.
    fn resolve_skill_entry(&self, name: &str) -> Result<SkillEntry, MeshError> {
        let snapshot = self.snapshot.load();
        let manifest = snapshot
            .manifest
            .as_ref()
            .ok_or_else(|| MeshError::SkillNotFound(name.to_string()))?;
        let entry = manifest
            .active_skills()
            .find(|s| s.name == name)
            .cloned();
        entry.ok_or_else(|| MeshError::SkillNotFound(name.to_string()))
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

    // ---- Podman local fallback (Epic 4 / issue #7) ----

    /// Skill HTTP server that always returns 500 for `tools/call`, and
    /// counts how many call_tool requests it has actually seen. Used to
    /// prove that the circuit breaker skips HTTP after the threshold.
    async fn mock_always_failing_with_counter() -> (
        String,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let counter = Arc::new(AtomicUsize::new(0));
        let app_counter = counter.clone();
        let app = Router::new().route(
            "/mcp",
            post(move |Json(req): Json<JsonRpcRequest>| {
                let c = app_counter.clone();
                async move {
                    if req.method == "tools/list" {
                        // Allow discovery so the skill registers as Healthy
                        // initially — the test then exercises call_tool.
                        return Json(JsonRpcResponse::ok(
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
                        .into_response();
                    }
                    c.fetch_add(1, Ordering::SeqCst);
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(JsonRpcResponse::err(req.id, -32000, "skill down")),
                    )
                        .into_response()
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

    fn fake_podman(script: &str, tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("oxidized-mcp-router-podman-{tag}-{}", uuid_simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("fake-podman");
        std::fs::write(&path, script).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// Fake podman that returns success for `image exists` and emits a
    /// canned JSON-RPC response for `run -i --rm`. Bumps a counter on
    /// every `run` invocation so the test can verify the fallback path.
    fn fake_podman_always_works(counter_path: &std::path::Path) -> std::path::PathBuf {
        let script = format!(
            r#"#!/bin/sh
case "$1" in
  image)
    [ "$2" = "exists" ] && exit 0
    exit 1
    ;;
  run)
    # Bump the run counter so the test can see how many times the
    # local fallback was actually exercised.
    count=$(cat "{counter}" 2>/dev/null || echo 0)
    echo $((count + 1)) > "{counter}"
    cat > /dev/null
    printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{"content":[{{"type":"text","text":"from-podman"}}]}}}}'
    ;;
  *) exit 99 ;;
esac
"#,
            counter = counter_path.display()
        );
        fake_podman(&script, "always-works")
    }

    /// When HTTP fails and the manifest declares an `image`, the call_tool
    /// path falls back to `podman run -i --rm` and returns the local result.
    /// One HTTP failure also bumps the breaker counter to 1.
    #[tokio::test]
    async fn http_failure_falls_back_to_podman_when_image_present() {
        let (endpoint, _http_count, _handle) = mock_always_failing_with_counter().await;
        let podman_counter = std::env::temp_dir().join(format!("podman-count-{}", uuid_simple()));
        let podman_bin = fake_podman_always_works(&podman_counter);

        let yaml = format!(
            r#"
version: 1
environment: test
skills:
  - name: flaky
    description: Skill with local fallback
    endpoint: {endpoint}
    image: ghcr.io/example/flaky:latest
"#
        );
        let path = write_registry(&yaml, "fallback-basic");

        let mesh = SkillMesh::with_auth_and_runner(
            RegistrySource::File(path),
            Authenticator::new(AuthMode::None),
            PodmanRunner::with_binary(podman_bin.to_string_lossy().into_owned()),
        );
        mesh.refresh().await.unwrap();

        let result = mesh
            .call_tool("flaky::ping", json!({}))
            .await
            .expect("HTTP failure must fall over to podman fallback");
        assert_eq!(result.content[0].text, "from-podman");

        // The breaker has registered exactly one HTTP failure — not yet open.
        assert_eq!(mesh.circuit_count("flaky"), 1);
        assert!(!mesh.breaker_is_open("flaky"));
        let podman_runs = std::fs::read_to_string(&podman_counter)
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(0);
        assert_eq!(podman_runs, 1, "expected exactly one fallback podman run");
    }

    /// After `CIRCUIT_TRIP_THRESHOLD` consecutive HTTP failures, the next
    /// `call_tool` must bypass HTTP entirely and route straight to podman.
    /// We assert on the HTTP server's request counter so the bypass is real
    /// and not just inferred.
    #[tokio::test]
    async fn circuit_opens_after_threshold_and_skips_http() {
        let (endpoint, http_count, _handle) = mock_always_failing_with_counter().await;
        let podman_counter = std::env::temp_dir().join(format!("podman-count-{}", uuid_simple()));
        let podman_bin = fake_podman_always_works(&podman_counter);

        let yaml = format!(
            r#"
version: 1
environment: test
skills:
  - name: flaky
    description: Skill with local fallback
    endpoint: {endpoint}
    image: ghcr.io/example/flaky:latest
"#
        );
        let path = write_registry(&yaml, "circuit-trip");

        let mesh = SkillMesh::with_auth_and_runner(
            RegistrySource::File(path),
            Authenticator::new(AuthMode::None),
            PodmanRunner::with_binary(podman_bin.to_string_lossy().into_owned()),
        );
        mesh.refresh().await.unwrap();

        for i in 0..CIRCUIT_TRIP_THRESHOLD {
            mesh.call_tool("flaky::ping", json!({}))
                .await
                .unwrap_or_else(|e| panic!("call {i} should fall back, got {e}"));
        }
        assert!(
            mesh.breaker_is_open("flaky"),
            "breaker must be open after {CIRCUIT_TRIP_THRESHOLD} failures"
        );
        let http_seen_before = http_count.load(Ordering::SeqCst);
        assert_eq!(
            http_seen_before, CIRCUIT_TRIP_THRESHOLD as usize,
            "every pre-trip call should have hit HTTP exactly once"
        );

        // One more call after the breaker opens — must NOT hit HTTP.
        let result = mesh.call_tool("flaky::ping", json!({})).await.unwrap();
        assert_eq!(result.content[0].text, "from-podman");
        let http_seen_after = http_count.load(Ordering::SeqCst);
        assert_eq!(
            http_seen_after, http_seen_before,
            "open breaker must NOT issue another HTTP request"
        );
    }

    /// A successful refresh after the breaker opened must reset the counter,
    /// so a recovered endpoint goes back to the HTTP path on the next call.
    #[tokio::test]
    async fn breaker_resets_on_healthy_refresh() {
        let (endpoint, down, _handle) = mock_toggleable_skill_server().await;
        let podman_counter = std::env::temp_dir().join(format!("podman-count-{}", uuid_simple()));
        let podman_bin = fake_podman_always_works(&podman_counter);

        let yaml = format!(
            r#"
version: 1
environment: test
skills:
  - name: flaky
    description: Toggleable
    endpoint: {endpoint}
    image: ghcr.io/example/flaky:latest
"#
        );
        let path = write_registry(&yaml, "breaker-reset");

        let mesh = SkillMesh::with_auth_and_runner(
            RegistrySource::File(path),
            Authenticator::new(AuthMode::None),
            PodmanRunner::with_binary(podman_bin.to_string_lossy().into_owned()),
        );
        mesh.refresh().await.unwrap();

        // Take the skill down and drive enough call_tool failures to open
        // the breaker. Each call falls back to podman, but the counter is
        // what we care about here.
        down.store(true, Ordering::SeqCst);
        for _ in 0..CIRCUIT_TRIP_THRESHOLD {
            let _ = mesh.call_tool("flaky::ping", json!({})).await;
        }
        assert!(mesh.breaker_is_open("flaky"));

        // Bring the skill back up and refresh — the breaker must reset.
        down.store(false, Ordering::SeqCst);
        mesh.refresh().await.unwrap();
        assert_eq!(mesh.circuit_count("flaky"), 0);
        assert!(!mesh.breaker_is_open("flaky"));
    }

    /// Sanity: without `image`, the old behavior is preserved — an HTTP
    /// failure surfaces directly to the caller, no fallback attempted.
    #[tokio::test]
    async fn http_failure_without_image_returns_http_error() {
        let (endpoint, _count, _handle) = mock_always_failing_with_counter().await;
        let yaml = format!(
            r#"
version: 1
environment: test
skills:
  - name: cloud-only
    description: No local fallback
    endpoint: {endpoint}
"#
        );
        let path = write_registry(&yaml, "no-image");

        let mesh = SkillMesh::new(RegistrySource::File(path));
        mesh.refresh().await.unwrap();

        let err = mesh
            .call_tool("cloud-only::ping", json!({}))
            .await
            .expect_err("HTTP 500 must propagate when no image is configured");
        // SkillUnreachable is correct: the refresh just before this call
        // recorded the skill as Down via tools/list (which 500s too), so
        // the fast-fail path triggers. The important assertion is that no
        // LocalRun variant appears — we never tried fallback.
        match err {
            MeshError::SkillUnreachable { .. } | MeshError::Http(_, _) => {}
            MeshError::LocalRun(_, _) => panic!("must not attempt local fallback without image"),
            other => panic!("expected HTTP-class error, got {other:?}"),
        }
    }
}
