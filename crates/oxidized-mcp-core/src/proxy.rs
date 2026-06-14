//! Pure JSON-RPC forwarding to skill HTTP backends.
//!
//! `router.rs` owns the routing decisions — circuit breaker, Podman fallback,
//! skill resolution, health tracking. This module owns the wire layer that
//! actually moves bytes between the proxy and a skill. Keeping the two
//! separated means the SSE streaming path (`open_streaming_call`) lives next
//! to the single-shot path (`post_json_rpc`) without having to know anything
//! about resilience or auth resolution.

use crate::mcp_types::{JsonRpcRequest, JsonRpcResponse};
use crate::router::MeshError;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};
use tracing::debug;

/// One event decoded from a skill's SSE stream.
///
/// Notifications carry whatever JSON-RPC notification body the skill emits and
/// are forwarded to the IDE verbatim. The `Final` event ends the stream and
/// carries the JSON-RPC response that becomes the IDE's `tools/call` result.
/// The proxy rewrites the response's `id` to match the IDE request before
/// emitting it, so the skill is free to use any id it likes internally.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Notification(JsonRpcRequest),
    Final(JsonRpcResponse),
}

/// Handle to an in-flight upstream SSE call. `recv` yields the next decoded
/// event; dropping the stream or calling `cancel()` closes the upstream HTTP
/// connection so the skill stops doing work.
#[derive(Debug)]
pub struct SseStream {
    rx: mpsc::Receiver<Result<StreamEvent, MeshError>>,
    cancel: Arc<Notify>,
}

impl SseStream {
    pub async fn recv(&mut self) -> Option<Result<StreamEvent, MeshError>> {
        self.rx.recv().await
    }

    /// Signal the pump task to drop the underlying response and exit.
    /// Safe to call more than once.
    pub fn cancel(&self) {
        self.cancel.notify_waiters();
    }

    /// A clonable handle that fires the same cancellation signal as
    /// [`Self::cancel`]. Stdio gives this to its in-flight registry so an
    /// inbound `$/cancelRequest` for the same JSON-RPC id can stop the
    /// stream even when the dispatch task is parked on `recv`.
    pub fn cancel_handle(&self) -> Arc<Notify> {
        self.cancel.clone()
    }
}

impl Drop for SseStream {
    fn drop(&mut self) {
        // Closing the receiver implicitly makes the pump task's `tx.send`
        // fail, but it would still wait on the next chunk before noticing.
        // Notifying cancellation wakes it immediately so an upstream that
        // emits one chunk every minute doesn't keep us hanging on after
        // the caller has lost interest.
        self.cancel.notify_waiters();
    }
}

/// Single-shot JSON-RPC POST. The caller has already resolved any outbound
/// auth header and decided which endpoint to hit. Errors are tagged with
/// `skill_name` so the call site doesn't have to wrap them.
pub async fn post_json_rpc(
    client: &reqwest::Client,
    endpoint: &str,
    skill_name: &str,
    auth_header: Option<String>,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, MeshError> {
    let mut req = client.post(endpoint).json(request);
    if let Some(value) = auth_header {
        req = req.header("Authorization", value);
    }
    req.send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| MeshError::Http(skill_name.to_string(), e))?
        .json()
        .await
        .map_err(|e| MeshError::Http(skill_name.to_string(), e))
}

/// Open a Server-Sent Events stream against a skill's `tools/call` endpoint.
///
/// The initial POST is awaited so connection errors surface synchronously —
/// the IDE then gets one clean JSON-RPC error in lieu of a partially-started
/// stream. On success a background task pumps chunks off the response body,
/// parses SSE frames, and pushes decoded `StreamEvent`s into the returned
/// channel. The task exits cleanly on: end-of-stream, decode error, network
/// error, dropped receiver, or `cancel()` from the caller.
pub async fn open_streaming_call(
    client: &reqwest::Client,
    endpoint: &str,
    skill_name: &str,
    auth_header: Option<String>,
    request: &JsonRpcRequest,
) -> Result<SseStream, MeshError> {
    let mut req = client
        .post(endpoint)
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .json(request);
    if let Some(value) = auth_header {
        req = req.header("Authorization", value);
    }

    let response = req
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| MeshError::Http(skill_name.to_string(), e))?;

    let (tx, rx) = mpsc::channel(64);
    let cancel = Arc::new(Notify::new());
    let cancel_for_task = cancel.clone();
    let skill = skill_name.to_string();

    tokio::spawn(async move {
        pump_sse(response, tx, cancel_for_task, skill).await;
    });

    Ok(SseStream { rx, cancel })
}

/// Drives the upstream `Response`, feeds bytes into the SSE parser, and pushes
/// decoded events into `tx`. Returns when the response ends, the receiver is
/// dropped, the caller cancels, or any decode/network error fires.
async fn pump_sse(
    mut response: reqwest::Response,
    tx: mpsc::Sender<Result<StreamEvent, MeshError>>,
    cancel: Arc<Notify>,
    skill: String,
) {
    let mut parser = SseParser::new();
    loop {
        let chunk_result = tokio::select! {
            biased;
            _ = cancel.notified() => {
                debug!(skill = %skill, "SSE stream cancelled by caller");
                return;
            }
            chunk = response.chunk() => chunk,
        };

        match chunk_result {
            Ok(Some(bytes)) => {
                parser.feed(&bytes);
                while let Some(data) = parser.next_event() {
                    let decoded = decode_sse_payload(&skill, &data);
                    let is_final = matches!(&decoded, Ok(StreamEvent::Final(_)));
                    if tx.send(decoded).await.is_err() {
                        // Receiver dropped — caller lost interest.
                        return;
                    }
                    if is_final {
                        // A clean terminal event closes the stream;
                        // anything the skill emits after it is noise.
                        return;
                    }
                }
            }
            Ok(None) => {
                // Upstream closed without a Final event. Surface that as an
                // error so the IDE doesn't sit on a perpetual spinner.
                let _ = tx
                    .send(Err(MeshError::SkillError(
                        skill.clone(),
                        "SSE stream ended without a final response".into(),
                    )))
                    .await;
                return;
            }
            Err(e) => {
                let _ = tx.send(Err(MeshError::Http(skill.clone(), e))).await;
                return;
            }
        }
    }
}

fn decode_sse_payload(skill: &str, data: &str) -> Result<StreamEvent, MeshError> {
    let value: Value = serde_json::from_str(data).map_err(|e| {
        MeshError::SkillError(skill.to_string(), format!("SSE data is not JSON: {e}"))
    })?;

    // Notifications carry a `method` and no `id`; responses carry an `id`
    // plus either `result` or `error`. We use the absence/presence of `id`
    // (treating `null` as absent, per JSON-RPC 2.0) as the discriminator —
    // a request-shaped body without an id is structurally a notification.
    let has_id = value.get("id").map(|v| !v.is_null()).unwrap_or(false);

    if !has_id && value.get("method").is_some() {
        let req: JsonRpcRequest = serde_json::from_value(value).map_err(|e| {
            MeshError::SkillError(skill.to_string(), format!("invalid notification: {e}"))
        })?;
        Ok(StreamEvent::Notification(req))
    } else if has_id && (value.get("result").is_some() || value.get("error").is_some()) {
        let resp: JsonRpcResponse = serde_json::from_value(value).map_err(|e| {
            MeshError::SkillError(skill.to_string(), format!("invalid response: {e}"))
        })?;
        Ok(StreamEvent::Final(resp))
    } else {
        Err(MeshError::SkillError(
            skill.to_string(),
            "SSE data must be a JSON-RPC notification or response".into(),
        ))
    }
}

/// Minimal SSE frame parser sufficient for our use case: extract the
/// concatenated `data:` field of each event, ignore comments and other SSE
/// fields. Heartbeat events (data-less frames) are silently consumed so the
/// caller never sees empty strings.
///
/// The parser normalizes CRLF to LF on feed so the rest of the code only has
/// to scan for `\n\n`. SSE payloads are UTF-8 text by spec; a stray CR inside
/// a `data:` field is non-conforming and we don't attempt to preserve it.
struct SseParser {
    buffer: Vec<u8>,
}

impl SseParser {
    fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    fn feed(&mut self, chunk: &[u8]) {
        for &b in chunk {
            if b == b'\r' {
                continue;
            }
            self.buffer.push(b);
        }
    }

    fn next_event(&mut self) -> Option<String> {
        loop {
            let term_pos = self.buffer.windows(2).position(|w| w == b"\n\n")?;
            let event_bytes: Vec<u8> = self.buffer.drain(..term_pos + 2).collect();
            let event_text = match std::str::from_utf8(&event_bytes[..term_pos]) {
                Ok(s) => s,
                Err(_) => continue, // skip invalid-UTF-8 frame entirely
            };

            let mut data_lines: Vec<&str> = Vec::new();
            for line in event_text.split('\n') {
                if line.is_empty() || line.starts_with(':') {
                    continue;
                }
                if let Some(rest) = line.strip_prefix("data:") {
                    data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
                }
                // Other fields (event:, id:, retry:) are accepted but ignored —
                // our protocol carries everything inside the `data` payload.
            }

            if data_lines.is_empty() {
                // Comment-only or non-data event; loop to find the next one.
                continue;
            }
            return Some(data_lines.join("\n"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sse_parser_yields_single_event() {
        let mut p = SseParser::new();
        p.feed(b"data: hello\n\n");
        assert_eq!(p.next_event().as_deref(), Some("hello"));
        assert!(p.next_event().is_none());
    }

    #[test]
    fn sse_parser_yields_multiple_events_split_across_chunks() {
        let mut p = SseParser::new();
        p.feed(b"data: one\n\ndata: tw");
        assert_eq!(p.next_event().as_deref(), Some("one"));
        assert!(p.next_event().is_none());
        p.feed(b"o\n\n");
        assert_eq!(p.next_event().as_deref(), Some("two"));
    }

    #[test]
    fn sse_parser_concatenates_multiline_data() {
        let mut p = SseParser::new();
        p.feed(b"data: line1\ndata: line2\n\n");
        assert_eq!(p.next_event().as_deref(), Some("line1\nline2"));
    }

    #[test]
    fn sse_parser_handles_crlf_line_endings() {
        let mut p = SseParser::new();
        p.feed(b"data: payload\r\n\r\n");
        assert_eq!(p.next_event().as_deref(), Some("payload"));
    }

    #[test]
    fn sse_parser_skips_comments_and_heartbeats() {
        let mut p = SseParser::new();
        p.feed(b": heartbeat\n\ndata: real\n\n");
        // Heartbeat frame is consumed silently; first surfaced event is "real".
        assert_eq!(p.next_event().as_deref(), Some("real"));
    }

    #[test]
    fn decode_notification_without_id() {
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": { "percent": 42 }
        })
        .to_string();
        let event = decode_sse_payload("skill", &payload).unwrap();
        match event {
            StreamEvent::Notification(req) => {
                assert_eq!(req.method, "notifications/progress");
                assert!(req.id.is_none());
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[test]
    fn decode_final_with_result() {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 7,
            "result": { "content": [{ "type": "text", "text": "done" }] }
        })
        .to_string();
        let event = decode_sse_payload("skill", &payload).unwrap();
        match event {
            StreamEvent::Final(resp) => {
                assert_eq!(resp.id, Some(json!(7)));
                assert!(resp.result.is_some());
            }
            other => panic!("expected Final, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_unknown_shape() {
        let payload = "{\"foo\":\"bar\"}";
        let err = decode_sse_payload("skill", payload).unwrap_err();
        match err {
            MeshError::SkillError(skill, msg) => {
                assert_eq!(skill, "skill");
                assert!(msg.contains("must be"));
            }
            other => panic!("expected SkillError, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_invalid_json() {
        let err = decode_sse_payload("skill", "not json").unwrap_err();
        match err {
            MeshError::SkillError(_, msg) => assert!(msg.contains("not JSON")),
            other => panic!("expected SkillError, got {other:?}"),
        }
    }
}
