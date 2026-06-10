//! MCP JSON-RPC server over stdio (IDE transport).

use anyhow::Result;
use oxidized_mcp_core::{
    JsonRpcRequest, JsonRpcResponse, SkillMesh, ToolCallParams, ToolsListResult,
    MCP_PROTOCOL_VERSION,
};
use serde_json::json;
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use tracing::{debug, error, warn};

const SERVER_NAME: &str = "oxidized-mcp";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Run the MCP stdio loop against an already-refreshed mesh. The caller is
/// responsible for the initial `mesh.refresh()` and for spawning any background
/// refresh task — this function stays focused on the JSON-RPC transport.
pub async fn run_stdio_server(mesh: Arc<SkillMesh>) -> Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                error!(error = %e, "stdin read failed");
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = JsonRpcResponse::err(None, -32700, format!("parse error: {e}"));
                write_response(&mut stdout, &resp)?;
                continue;
            }
        };

        // Notifications have no id — do not write a response
        if request.id.is_none() && request.method.starts_with("notifications/") {
            debug!(method = %request.method, "notification received");
            continue;
        }

        let response = handle_request(&mesh, request).await;
        if let Some(resp) = response {
            write_response(&mut stdout, &resp)?;
        }
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
        "notifications/initialized" => None,
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

fn write_response(stdout: &mut impl Write, response: &JsonRpcResponse) -> Result<()> {
    let payload = serde_json::to_string(response)?;
    writeln!(stdout, "{payload}")?;
    stdout.flush()?;
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
