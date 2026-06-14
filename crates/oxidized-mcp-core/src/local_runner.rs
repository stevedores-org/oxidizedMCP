//! Local Podman runner for skill fallback when the cloud endpoint is unreachable.
//!
//! A skill manifest may declare an `image` ref alongside its HTTP endpoint.
//! When the mesh can't reach the cloud endpoint, it asks the runner whether
//! the OCI image is present locally; if so, the runner spawns
//! `podman run -i --rm <image>` and exchanges a single JSON-RPC request /
//! response pair over stdio (the same transport `oxidized-mcp` itself speaks).

use crate::mcp_types::{JsonRpcRequest, JsonRpcResponse};
use std::process::Stdio;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

#[derive(Debug, Error)]
pub enum LocalRunError {
    #[error("failed to spawn '{binary}': {source}")]
    Spawn {
        binary: String,
        #[source]
        source: std::io::Error,
    },
    #[error("podman stdin write failed: {0}")]
    StdinWrite(#[source] std::io::Error),
    #[error("podman stdout read failed: {0}")]
    StdoutRead(#[source] std::io::Error),
    #[error("podman closed stdout before returning a JSON-RPC line")]
    EmptyResponse,
    #[error("invalid JSON-RPC response from podman: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("image '{0}' is not present in the local Podman store")]
    ImageNotPresent(String),
}

/// Wraps the `podman` CLI to run skill OCI images as a single-shot stdio
/// MCP server. `binary` is configurable so tests can inject a fake script
/// without mutating `PATH` (which races with other tests).
#[derive(Debug, Clone)]
pub struct PodmanRunner {
    binary: String,
}

impl Default for PodmanRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl PodmanRunner {
    pub fn new() -> Self {
        Self {
            binary: "podman".to_string(),
        }
    }

    /// Inject a fake or alternative binary path. Used by tests, and by
    /// callers that want to point at `docker` or a wrapper.
    pub fn with_binary(binary: impl Into<String>) -> Self {
        Self {
            binary: binary.into(),
        }
    }

    pub fn binary(&self) -> &str {
        &self.binary
    }

    /// Returns Ok(true) when `<binary> image exists <image>` exits 0,
    /// Ok(false) when the binary ran but reported the image is missing,
    /// and Err when the binary itself can't be spawned (e.g. podman not
    /// installed). Callers use the Err variant to short-circuit fallback
    /// instead of treating "podman missing" as "image missing".
    pub async fn image_exists(&self, image: &str) -> Result<bool, LocalRunError> {
        let status = Command::new(&self.binary)
            .args(["image", "exists", image])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map_err(|e| LocalRunError::Spawn {
                binary: self.binary.clone(),
                source: e,
            })?;
        Ok(status.success())
    }

    /// Spawn the image, write a single JSON-RPC request line, close stdin,
    /// read a single JSON-RPC response line. The container is expected to
    /// behave like a normal MCP stdio server: one line in, one line out,
    /// then exit on EOF.
    pub async fn invoke_stdio(
        &self,
        image: &str,
        request: &JsonRpcRequest,
    ) -> Result<JsonRpcResponse, LocalRunError> {
        let mut child = Command::new(&self.binary)
            .args(["run", "-i", "--rm", image])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| LocalRunError::Spawn {
                binary: self.binary.clone(),
                source: e,
            })?;

        // Take stdin in its own scope so it drops (closes) before we wait on
        // stdout — otherwise a container that exits on EOF would hang.
        {
            let mut stdin = child.stdin.take().expect("stdin piped");
            let mut payload = serde_json::to_vec(request)?;
            payload.push(b'\n');
            stdin
                .write_all(&payload)
                .await
                .map_err(LocalRunError::StdinWrite)?;
            stdin.shutdown().await.map_err(LocalRunError::StdinWrite)?;
        }

        let stdout = child.stdout.take().expect("stdout piped");
        let mut lines = BufReader::new(stdout).lines();
        let first = lines
            .next_line()
            .await
            .map_err(LocalRunError::StdoutRead)?
            .ok_or(LocalRunError::EmptyResponse)?;

        // Reap the child so we don't leave a zombie; ignoring the status is
        // intentional — the JSON response we already read is the source of
        // truth for whether the call succeeded.
        let _ = child.wait().await;

        let response: JsonRpcResponse = serde_json::from_str(&first)?;
        Ok(response)
    }
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use crate::test_helpers::{fake_executable, test_env};
    use serde_json::json;

    #[tokio::test]
    async fn image_exists_returns_true_when_fake_binary_exits_zero() {
        let _guard = test_env::lock();
        let script = r#"#!/bin/sh
case "$1 $2" in
  "image exists") exit 0 ;;
  *) exit 99 ;;
esac
"#;
        let bin = fake_executable::write(script, "exists-true");
        let runner = PodmanRunner::with_binary(bin.to_string_lossy().into_owned());
        assert!(runner
            .image_exists("ghcr.io/example/skill:latest")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn image_exists_returns_false_when_fake_binary_exits_nonzero() {
        let _guard = test_env::lock();
        let script = r#"#!/bin/sh
exit 1
"#;
        let bin = fake_executable::write(script, "exists-false");
        let runner = PodmanRunner::with_binary(bin.to_string_lossy().into_owned());
        assert!(!runner.image_exists("missing:tag").await.unwrap());
    }

    #[tokio::test]
    async fn image_exists_errors_when_binary_missing() {
        let runner = PodmanRunner::with_binary("/definitely/not/a/real/path/to/podman".to_string());
        let err = runner
            .image_exists("anything")
            .await
            .expect_err("missing binary must surface as Spawn error");
        matches!(err, LocalRunError::Spawn { .. });
    }

    #[tokio::test]
    async fn invoke_stdio_round_trips_one_line() {
        let _guard = test_env::lock();
        // Fake "podman run" that echoes a canned JSON-RPC response. We don't
        // care that it ignores stdin — a real skill image would parse it; we
        // only need to verify the parent reads one line and parses it back.
        let script = r#"#!/bin/sh
# Drain stdin so the parent's write doesn't SIGPIPE.
cat > /dev/null
echo '{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"hello"}]}}'
"#;
        let bin = fake_executable::write(script, "invoke-ok");
        let runner = PodmanRunner::with_binary(bin.to_string_lossy().into_owned());

        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "tools/call".to_string(),
            params: Some(json!({"name": "echo", "arguments": {}})),
        };
        let resp = runner.invoke_stdio("img:latest", &req).await.unwrap();
        assert!(resp.error.is_none());
        let result = resp.result.expect("result present");
        let text = result["content"][0]["text"].as_str().unwrap();
        assert_eq!(text, "hello");
    }

    #[tokio::test]
    async fn invoke_stdio_errors_when_container_emits_invalid_json() {
        let _guard = test_env::lock();
        let script = r#"#!/bin/sh
cat > /dev/null
echo 'not a json line'
"#;
        let bin = fake_executable::write(script, "invoke-bad");
        let runner = PodmanRunner::with_binary(bin.to_string_lossy().into_owned());
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "tools/call".to_string(),
            params: None,
        };
        let err = runner
            .invoke_stdio("img:latest", &req)
            .await
            .expect_err("non-JSON output must error");
        matches!(err, LocalRunError::InvalidJson(_));
    }
}
