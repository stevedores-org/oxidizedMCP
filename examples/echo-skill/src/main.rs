//! Minimal containerizable skill — MCP JSON-RPC over HTTP at `/mcp`.

use axum::{routing::post, Json, Router};
use oxidized_mcp_core::{
    JsonRpcRequest, JsonRpcResponse, ToolCallParams, ToolCallResult, ToolDescriptor,
    ToolsListResult,
};
use serde_json::json;
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let app = Router::new().route("/mcp", post(handle_mcp));
    // Bind 0.0.0.0 by default so the OCI image (dockworker.toml) is reachable
    // from oxidized-mcp running outside the container. Override with
    // ECHO_SKILL_BIND=127.0.0.1:9100 for tightened laptop dev when you don't
    // want the listener on every interface. Bad values fall back to the
    // default with a warning so a typo never silently leaves you on 0.0.0.0
    // when you thought you'd locked it down.
    let default_addr = SocketAddr::from(([0, 0, 0, 0], 9100));
    let addr = match std::env::var("ECHO_SKILL_BIND") {
        Ok(raw) => match raw.parse::<SocketAddr>() {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(
                    "ECHO_SKILL_BIND={raw:?} could not be parsed as host:port ({e}); falling back to {default_addr}"
                );
                default_addr
            }
        },
        Err(_) => default_addr,
    };
    tracing::info!("echo-skill listening on http://{addr}/mcp");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn handle_mcp(Json(req): Json<JsonRpcRequest>) -> Json<JsonRpcResponse> {
    match req.method.as_str() {
        "tools/list" => Json(JsonRpcResponse::ok(
            req.id,
            serde_json::to_value(ToolsListResult {
                tools: vec![ToolDescriptor {
                    name: "echo".to_string(),
                    description: "Echo a message back to the agent".to_string(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {
                            "message": { "type": "string", "description": "Text to echo" }
                        },
                        "required": ["message"]
                    }),
                }],
            })
            .unwrap(),
        )),
        "tools/call" => {
            let params: ToolCallParams =
                match req.params.and_then(|p| serde_json::from_value(p).ok()) {
                    Some(p) => p,
                    None => {
                        return Json(JsonRpcResponse::err(
                            req.id,
                            -32602,
                            "invalid tools/call params",
                        ));
                    }
                };

            if params.name != "echo" {
                return Json(JsonRpcResponse::err(
                    req.id,
                    -32601,
                    format!("unknown tool: {}", params.name),
                ));
            }

            let message = params
                .arguments
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("(empty)");

            Json(JsonRpcResponse::ok(
                req.id,
                serde_json::to_value(ToolCallResult::text(format!("echo: {message}"))).unwrap(),
            ))
        }
        other => Json(JsonRpcResponse::err(
            req.id,
            -32601,
            format!("method not found: {other}"),
        )),
    }
}
