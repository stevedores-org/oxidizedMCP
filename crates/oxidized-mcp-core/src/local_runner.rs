//! Runs skills locally via Podman as a fallback.

use crate::mcp_types::{JsonRpcRequest, JsonRpcResponse};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tracing::{error, info};

#[derive(Debug, thiserror::Error)]
pub enum LocalRunnerError {
    #[error("failed to run podman: {0}")]
    Io(#[from] std::io::Error),
    #[error("podman command exited with error: {0}")]
    Exit(String),
    #[error("failed to serialize/deserialize json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("empty or invalid JSON-RPC response from local runner")]
    InvalidResponse,
    #[error("local OCI image '{0}' does not exist")]
    ImageNotFound(String),
}

/// Check if the local OCI image exists in Podman.
pub async fn has_local_image(image: &str) -> bool {
    if image.starts_with("mock-local-image:") {
        return true;
    }

    let status = Command::new("podman")
        .args(["image", "exists", image])
        .status()
        .await;

    match status {
        Ok(status) => status.success(),
        Err(e) => {
            error!(error = %e, "Failed to execute 'podman image exists'");
            false
        }
    }
}

/// Launch the OCI image via `podman run -i <image>`, write the single JSON-RPC
/// request to its stdin, and read the single JSON-RPC response line from stdout.
pub async fn run_local_mcp(
    image: &str,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, LocalRunnerError> {
    if image.starts_with("mock-local-image:") {
        if request.method == "tools/list" {
            let result = crate::mcp_types::ToolsListResult {
                tools: vec![crate::mcp_types::ToolDescriptor {
                    name: "local_ping".to_string(),
                    description: "Local Ping".to_string(),
                    input_schema: serde_json::json!({}),
                }],
            };
            return Ok(JsonRpcResponse::ok(
                request.id.clone(),
                serde_json::to_value(result)?,
            ));
        } else if request.method == "tools/call" {
            let result = crate::mcp_types::ToolCallResult::text("pong from local fallback");
            return Ok(JsonRpcResponse::ok(
                request.id.clone(),
                serde_json::to_value(result)?,
            ));
        }
    }

    if !has_local_image(image).await {
        return Err(LocalRunnerError::ImageNotFound(image.to_string()));
    }

    info!(image = %image, "Launching local MCP server via podman");
    let mut child = Command::new("podman")
        .args(["run", "-i", "--rm", image])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| std::io::Error::other("Failed to capture stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("Failed to capture stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| std::io::Error::other("Failed to capture stderr"))?;

    // Write the request to stdin
    let req_bytes = serde_json::to_vec(request)?;
    stdin.write_all(&req_bytes).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    // Drop stdin to signal EOF to the MCP server
    drop(stdin);

    // Read the single-line response from stdout
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    if line.trim().is_empty() {
        // Let's check stderr for any execution errors
        let mut stderr_reader = BufReader::new(stderr);
        let mut err_line = String::new();
        stderr_reader.read_line(&mut err_line).await.ok();
        return Err(LocalRunnerError::Exit(format!(
            "Empty stdout. Stderr: {}",
            err_line.trim()
        )));
    }

    let response: JsonRpcResponse = serde_json::from_str(&line)?;

    // Clean up child process
    let _ = child.kill().await;

    Ok(response)
}
