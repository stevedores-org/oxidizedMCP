//! MCP JSON-RPC server over stdio (IDE transport).

use crate::health::HealthState;
use anyhow::Result;
use oxidized_mcp_core::{
    JsonRpcRequest, JsonRpcResponse, SkillMesh, ToolCallParams, ToolsListResult,
    MCP_PROTOCOL_VERSION,
};
use serde_json::json;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

const SERVER_NAME: &str = "oxidized-mcp";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Run the MCP stdio loop against an already-refreshed mesh. The caller is
/// responsible for the initial `mesh.refresh()`, for spawning any background
/// refresh task, and for flipping `health.mark_ready()` before invoking this
/// function — this function stays focused on the JSON-RPC transport.
///
/// Each request is dispatched on its own task so in-flight `tools/call`
/// round-trips don't serialize the loop. Responses are tagged by id, so
/// out-of-order writes are valid JSON-RPC.
///
/// `health` is unused inside the loop today; the parameter exists so the caller
/// can let this function drop the handle on exit, simplifying shutdown ordering.
pub async fn run_stdio_server(mesh: Arc<SkillMesh>, _health: Option<HealthState>) -> Result<()> {
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));

    while let Some(line) = stdin.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = JsonRpcResponse::err(None, -32700, format!("parse error: {e}"));
                write_response(&stdout, &resp).await?;
                continue;
            }
        };

        // Notifications have no id — do not write a response.
        if request.id.is_none() {
            debug!(method = %request.method, "notification received");
            continue;
        }

        let mesh = mesh.clone();
        let stdout = stdout.clone();
        tokio::spawn(async move {
            if let Some(resp) = handle_request(&mesh, request).await {
                if let Err(e) = write_response(&stdout, &resp).await {
                    error!(error = %e, "failed to write response");
                }
            }
        });
    }

    Ok(())
}

async fn handle_request(mesh: &SkillMesh, request: JsonRpcRequest) -> Option<JsonRpcResponse> {
    let id = request.id.clone();

    match request.method.as_str() {
        "initialize" => Some(JsonRpcResponse::ok(
            id,
            json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": SERVER_NAME,
                    "version": SERVER_VERSION,
                }
            }),
        )),
        "tools/list" => {
            let tools = mesh.list_tools();
            Some(JsonRpcResponse::ok(
                id,
                serde_json::to_value(ToolsListResult { tools })
                    .unwrap_or_else(|e| json!({ "error": e.to_string() })),
            ))
        }
        "tools/call" => {
            let params: ToolCallParams = match request.params {
                Some(p) => match serde_json::from_value(p) {
                    Ok(v) => v,
                    Err(e) => {
                        return Some(JsonRpcResponse::err(
                            id,
                            -32602,
                            format!("invalid tools/call params: {e}"),
                        ));
                    }
                },
                None => {
                    return Some(JsonRpcResponse::err(
                        id,
                        -32602,
                        "missing tools/call params",
                    ));
                }
            };

            match mesh.call_tool(&params.name, params.arguments).await {
                Ok(result) => Some(JsonRpcResponse::ok(
                    id,
                    serde_json::to_value(result)
                        .unwrap_or_else(|e| json!({ "error": e.to_string() })),
                )),
                Err(e) => {
                    warn!(tool = %params.name, error = %e, "tool call failed");
                    Some(JsonRpcResponse::ok(
                        id,
                        serde_json::to_value(oxidized_mcp_core::ToolCallResult::error(
                            e.to_string(),
                        ))
                        .unwrap_or_else(|_| json!({ "isError": true })),
                    ))
                }
            }
        }
        "ping" => Some(JsonRpcResponse::ok(id, json!({}))),
        other => Some(JsonRpcResponse::err(
            id,
            -32601,
            format!("method not found: {other}"),
        )),
    }
}

async fn write_response(
    stdout: &Mutex<tokio::io::Stdout>,
    response: &JsonRpcResponse,
) -> Result<()> {
    let mut payload = serde_json::to_vec(response)?;
    payload.push(b'\n');
    let mut guard = stdout.lock().await;
    guard.write_all(&payload).await?;
    guard.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidized_mcp_core::RegistrySource;

    #[tokio::test]
    async fn initialize_returns_server_info() {
        let mesh = SkillMesh::new(RegistrySource::File(std::path::PathBuf::from(
            "/nonexistent",
        )));
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(json!(1)),
            method: "initialize".into(),
            params: None,
        };
        let resp = handle_request(&mesh, req).await.unwrap();
        let result = resp.result.unwrap();
        assert_eq!(
            result.get("serverInfo").and_then(|v| v.get("name")),
            Some(&json!(SERVER_NAME))
        );
    }
}
