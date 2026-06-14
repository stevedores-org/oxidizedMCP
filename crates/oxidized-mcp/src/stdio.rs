//! MCP JSON-RPC server over stdio (IDE transport).

use crate::health::HealthState;
use anyhow::Result;
use oxidized_mcp_core::{
    JsonRpcRequest, JsonRpcResponse, SkillMesh, StreamEvent, ToolCallParams, ToolCallResult,
    ToolsListResult, MCP_PROTOCOL_VERSION,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Stdout};
use tokio::sync::{Mutex, Notify};
use tracing::{debug, error, warn};

const SERVER_NAME: &str = "oxidized-mcp";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shared registry of in-flight streaming `tools/call` requests, keyed by the
/// stable string form of the IDE's JSON-RPC request id. When a
/// `$/cancelRequest` (or MCP's `notifications/cancelled`) arrives, the
/// dispatcher looks up the matching cancel handle and fires it — the SSE pump
/// then drops the upstream response and the dispatch task exits.
type InFlight = Arc<std::sync::Mutex<HashMap<String, Arc<Notify>>>>;

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
    let in_flight: InFlight = Arc::new(std::sync::Mutex::new(HashMap::new()));

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

        // Notifications have no id. The cancel protocol lives on this branch:
        // `$/cancelRequest` (issue #24 wording) and `notifications/cancelled`
        // (MCP spec name) both fire the matching in-flight cancel handle.
        // Other notifications are dropped — there's no response to write.
        if request.id.is_none() {
            if is_cancel_method(&request.method) {
                if let Some(target_id) = cancel_target_id(&request.params) {
                    let key = id_key(&target_id);
                    let handle = in_flight
                        .lock()
                        .expect("in-flight mutex poisoned")
                        .get(&key)
                        .cloned();
                    if let Some(h) = handle {
                        debug!(target_id = %target_id, "cancellation requested; firing");
                        h.notify_waiters();
                    } else {
                        debug!(target_id = %target_id, "cancellation for unknown id; ignoring");
                    }
                }
            } else {
                debug!(method = %request.method, "notification received");
            }
            continue;
        }

        let mesh = mesh.clone();
        let stdout = stdout.clone();
        let in_flight = in_flight.clone();
        tokio::spawn(async move {
            dispatch(&mesh, &stdout, &in_flight, request).await;
        });
    }

    Ok(())
}

/// Branch point between the streaming `tools/call` path and the existing
/// single-response handler. Pulled into its own function so the streaming
/// branch can write multiple stdout frames (notifications + final response)
/// for a single inbound request, which the `handle_request -> Option<resp>`
/// shape would otherwise rule out.
async fn dispatch(
    mesh: &SkillMesh,
    stdout: &Arc<Mutex<Stdout>>,
    in_flight: &InFlight,
    request: JsonRpcRequest,
) {
    if request.method == "tools/call" {
        let id = request.id.clone();
        match parse_tool_call_params(request.params.clone()) {
            Ok(params) => {
                if mesh.is_skill_streaming(&params.name) {
                    stream_tool_call(mesh, stdout, in_flight, id, params).await;
                    return;
                }
            }
            Err(err_fn) => {
                let resp = err_fn(id);
                let _ = write_response(stdout, &resp).await;
                return;
            }
        }
    }

    if let Some(resp) = handle_request(mesh, request).await {
        if let Err(e) = write_response(stdout, &resp).await {
            error!(error = %e, "failed to write response");
        }
    }
}

/// Drive a streaming `tools/call` from open → notifications → final response.
/// Registers a cancel handle keyed by request id for the duration so an
/// inbound cancellation can drop the upstream connection mid-stream.
async fn stream_tool_call(
    mesh: &SkillMesh,
    stdout: &Arc<Mutex<Stdout>>,
    in_flight: &InFlight,
    id: Option<Value>,
    params: ToolCallParams,
) {
    let mut stream = match mesh
        .open_streaming_call(&params.name, params.arguments)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            warn!(tool = %params.name, error = %e, "streaming tool call failed to open");
            let resp = tool_error_response(id, &e.to_string());
            let _ = write_response(stdout, &resp).await;
            return;
        }
    };

    let key = id.as_ref().map(id_key).unwrap_or_default();
    in_flight
        .lock()
        .expect("in-flight mutex poisoned")
        .insert(key.clone(), stream.cancel_handle());

    let final_response = loop {
        match stream.recv().await {
            Some(Ok(StreamEvent::Notification(notif))) => {
                if let Err(e) = write_notification(stdout, &notif).await {
                    warn!(error = %e, "failed to write notification; dropping stream");
                    stream.cancel();
                    break tool_error_response(id.clone(), "stdout write failed");
                }
            }
            Some(Ok(StreamEvent::Final(mut resp))) => {
                // Rewrite the upstream id with the IDE's request id so the
                // client can correlate the response with its `tools/call`.
                resp.id = id.clone();
                break resp;
            }
            Some(Err(e)) => {
                warn!(tool = %params.name, error = %e, "streaming tool call errored mid-flight");
                break tool_error_response(id.clone(), &e.to_string());
            }
            None => {
                // Channel closed without a Final — pump already surfaced any
                // error event; an empty close is the cancellation signature.
                break JsonRpcResponse::err(
                    id.clone(),
                    -32000,
                    "tools/call cancelled or upstream stream closed",
                );
            }
        }
    };

    in_flight
        .lock()
        .expect("in-flight mutex poisoned")
        .remove(&key);
    if let Err(e) = write_response(stdout, &final_response).await {
        error!(error = %e, "failed to write final streaming response");
    }
}

/// Boxed builder for the JSON-RPC error response, returned by params parsing
/// so the caller can stamp it with whatever id was on the inbound request.
type ParamErrFn = Box<dyn FnOnce(Option<Value>) -> JsonRpcResponse + Send>;

fn parse_tool_call_params(params: Option<Value>) -> Result<ToolCallParams, ParamErrFn> {
    match params {
        Some(p) => serde_json::from_value::<ToolCallParams>(p).map_err(|e| {
            let msg = format!("invalid tools/call params: {e}");
            Box::new(move |id| JsonRpcResponse::err(id, -32602, msg.clone())) as ParamErrFn
        }),
        None => Err(
            Box::new(|id| JsonRpcResponse::err(id, -32602, "missing tools/call params"))
                as ParamErrFn,
        ),
    }
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
            let tools = mesh.list_tools_cached().await;
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
                    Some(tool_error_response(id, &e.to_string()))
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

fn tool_error_response(id: Option<Value>, message: &str) -> JsonRpcResponse {
    JsonRpcResponse::ok(
        id,
        serde_json::to_value(ToolCallResult::error(message.to_string()))
            .unwrap_or_else(|_| json!({ "isError": true })),
    )
}

async fn write_response(stdout: &Mutex<Stdout>, response: &JsonRpcResponse) -> Result<()> {
    let mut payload = serde_json::to_vec(response)?;
    payload.push(b'\n');
    let mut guard = stdout.lock().await;
    guard.write_all(&payload).await?;
    guard.flush().await?;
    Ok(())
}

/// Write an SSE-relayed notification. The `id` field is elided so strict
/// clients that treat `id: null` as "this is a response, not a notification"
/// (per JSON-RPC 2.0 §4.2) classify it correctly.
async fn write_notification(stdout: &Mutex<Stdout>, notif: &JsonRpcRequest) -> Result<()> {
    let mut obj = serde_json::Map::new();
    obj.insert("jsonrpc".to_string(), Value::String(notif.jsonrpc.clone()));
    obj.insert("method".to_string(), Value::String(notif.method.clone()));
    if let Some(params) = &notif.params {
        obj.insert("params".to_string(), params.clone());
    }
    let mut payload = serde_json::to_vec(&Value::Object(obj))?;
    payload.push(b'\n');
    let mut guard = stdout.lock().await;
    guard.write_all(&payload).await?;
    guard.flush().await?;
    Ok(())
}

fn is_cancel_method(method: &str) -> bool {
    method == "$/cancelRequest" || method == "notifications/cancelled"
}

fn cancel_target_id(params: &Option<Value>) -> Option<Value> {
    let params = params.as_ref()?;
    params
        .get("requestId")
        .or_else(|| params.get("id"))
        .cloned()
}

/// Stable string form of a JSON-RPC id so the in-flight map can key by it
/// regardless of whether the id is a number or a string.
fn id_key(v: &Value) -> String {
    v.to_string()
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

    #[test]
    fn cancel_method_aliases_recognised() {
        assert!(is_cancel_method("$/cancelRequest"));
        assert!(is_cancel_method("notifications/cancelled"));
        assert!(!is_cancel_method("ping"));
    }

    #[test]
    fn cancel_target_id_accepts_both_field_names() {
        // MCP spec wording uses `requestId`; LSP-style uses `id`.
        let p1 = Some(json!({ "requestId": 7 }));
        let p2 = Some(json!({ "id": "abc" }));
        let p3 = Some(json!({ "unrelated": true }));
        assert_eq!(cancel_target_id(&p1), Some(json!(7)));
        assert_eq!(cancel_target_id(&p2), Some(json!("abc")));
        assert_eq!(cancel_target_id(&p3), None);
    }
}
