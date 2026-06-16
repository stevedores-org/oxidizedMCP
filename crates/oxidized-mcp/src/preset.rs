use anyhow::{Context, Result};
use oxidized_mcp_core::{
    JsonRpcRequest, JsonRpcResponse, ToolCallParams, ToolCallResult, ToolDescriptor,
    ToolsListResult, MCP_PROTOCOL_VERSION,
};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Stdout};
use tokio::sync::Mutex;
use tracing::{error, info};

const SERVER_NAME: &str = "lornu-mcp";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn run_preset_server(preset: &str) -> Result<()> {
    if preset != "dev" {
        return Err(anyhow::anyhow!("Unsupported preset: {}", preset));
    }

    info!("preset {} stdio server started", preset);

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

        if request.id.is_none() {
            // Drop notifications
            continue;
        }

        let stdout_clone = stdout.clone();
        tokio::spawn(async move {
            if let Some(resp) = handle_request(request).await {
                if let Err(e) = write_response(&stdout_clone, &resp).await {
                    error!(error = %e, "failed to write response");
                }
            }
        });
    }

    Ok(())
}

async fn handle_request(request: JsonRpcRequest) -> Option<JsonRpcResponse> {
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
            let tools = get_tools_list();
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

            let res = match params.name.as_str() {
                "intent.get" => handle_intent_get()
                    .map(|v| ToolCallResult::text(serde_json::to_string_pretty(&v).unwrap())),
                "intent.pin" => handle_intent_pin(params.arguments)
                    .map(|v| ToolCallResult::text(serde_json::to_string_pretty(&v).unwrap())),
                "intent.status" => handle_intent_status()
                    .map(|v| ToolCallResult::text(serde_json::to_string_pretty(&v).unwrap())),
                "github.issues" => handle_github_issues(params.arguments)
                    .map(|v| ToolCallResult::text(serde_json::to_string_pretty(&v).unwrap())),
                "github.PRs" => handle_github_prs(params.arguments)
                    .map(|v| ToolCallResult::text(serde_json::to_string_pretty(&v).unwrap())),
                "github.reviews" => handle_github_reviews(params.arguments)
                    .map(|v| ToolCallResult::text(serde_json::to_string_pretty(&v).unwrap())),
                "validate" => handle_validate(params.arguments)
                    .map(|v| ToolCallResult::text(serde_json::to_string_pretty(&v).unwrap())),
                other => Err(anyhow::anyhow!("tool not found: {other}")),
            };

            match res {
                Ok(result) => Some(JsonRpcResponse::ok(
                    id,
                    serde_json::to_value(result)
                        .unwrap_or_else(|e| json!({ "error": e.to_string() })),
                )),
                Err(e) => Some(JsonRpcResponse::ok(
                    id,
                    serde_json::to_value(ToolCallResult::error(e.to_string()))
                        .unwrap_or_else(|_| json!({ "isError": true })),
                )),
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

async fn write_response(stdout: &Mutex<Stdout>, response: &JsonRpcResponse) -> Result<()> {
    let mut payload = serde_json::to_vec(response)?;
    payload.push(b'\n');
    let mut guard = stdout.lock().await;
    guard.write_all(&payload).await?;
    guard.flush().await?;
    Ok(())
}

fn get_tools_list() -> Vec<ToolDescriptor> {
    vec![
        ToolDescriptor {
            name: "intent.get".to_string(),
            description: "Retrieve the active objective/intent manifest (from intent/current.yaml) in the repository".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDescriptor {
            name: "intent.pin".to_string(),
            description: "Pin/write a new active intent to intent/current.yaml in the repository".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "objective_id": {
                        "type": "string",
                        "description": "Stable objective ID matching pattern ^OBJ-[0-9]{4}-[A-Z0-9-]+$"
                    },
                    "epic": {
                        "type": "string",
                        "description": "Short human-readable epic name"
                    },
                    "constraints": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Non-negotiable rules for agents"
                    },
                    "acceptance": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Verifiable done criteria"
                    },
                    "active_agents": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Authorized personas (e.g. builder, reviewer, librarian, auditor)"
                    },
                    "github_issue": {
                        "type": "integer",
                        "description": "Tracking GitHub issue number"
                    },
                    "github_branch": {
                        "type": "string",
                        "description": "Working GitHub branch"
                    },
                    "aivcs_branch": {
                        "type": "string",
                        "description": "aivcs branch context"
                    }
                },
                "required": ["objective_id", "epic"]
            }),
        },
        ToolDescriptor {
            name: "intent.status".to_string(),
            description: "Check the status of the current intent and detect branch drift".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDescriptor {
            name: "github.issues".to_string(),
            description: "Interact with GitHub issues using the gh CLI".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "view", "create"],
                        "description": "GitHub issue action to perform"
                    },
                    "number": {
                        "type": "integer",
                        "description": "Issue number (required for view)"
                    },
                    "title": {
                        "type": "string",
                        "description": "Issue title (required for create)"
                    },
                    "body": {
                        "type": "string",
                        "description": "Issue body description (optional for create)"
                    }
                },
                "required": ["action"]
            }),
        },
        ToolDescriptor {
            name: "github.PRs".to_string(),
            description: "Interact with GitHub Pull Requests using the gh CLI".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "view", "create"],
                        "description": "GitHub PR action to perform"
                    },
                    "number": {
                        "type": "integer",
                        "description": "PR number (required for view)"
                    },
                    "title": {
                        "type": "string",
                        "description": "PR title (required for create)"
                    },
                    "body": {
                        "type": "string",
                        "description": "PR body description (optional for create)"
                    },
                    "base": {
                        "type": "string",
                        "description": "PR base branch (optional for create)"
                    },
                    "head": {
                        "type": "string",
                        "description": "PR head branch (optional for create)"
                    }
                },
                "required": ["action"]
            }),
        },
        ToolDescriptor {
            name: "github.reviews".to_string(),
            description: "Interact with GitHub Pull Request reviews using the gh CLI".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "create"],
                        "description": "Reviews action to perform"
                    },
                    "number": {
                        "type": "integer",
                        "description": "Pull Request number (required)"
                    },
                    "event": {
                        "type": "string",
                        "enum": ["APPROVE", "REQUEST_CHANGES", "COMMENT"],
                        "description": "Review decision/event (required for create)"
                    },
                    "body": {
                        "type": "string",
                        "description": "Review body comment (optional)"
                    }
                },
                "required": ["action", "number"]
            }),
        },
        ToolDescriptor {
            name: "validate".to_string(),
            description: "Run the local-ci validator command for checking project correctness and drift".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "args": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional arguments to pass to local-ci"
                    }
                }
            }),
        }
    ]
}

fn find_intent_file() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        let path = dir.join("intent/current.yaml");
        if path.is_file() {
            return Some(path);
        }
        if dir.join(".git").exists() {
            return Some(path);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

fn handle_intent_get() -> Result<Value> {
    let path =
        find_intent_file().context("Could not determine repository root or locate intent file")?;
    handle_intent_get_from_path(&path)
}

fn handle_intent_get_from_path(path: &Path) -> Result<Value> {
    if !path.exists() {
        return Ok(json!({
            "status": "uninitialized",
            "message": format!("No active intent pinned. File does not exist at {}", path.display())
        }));
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read intent file at {}", path.display()))?;
    let val: serde_yaml::Value =
        serde_yaml::from_str(&content).context("Failed to parse intent/current.yaml as YAML")?;
    let json_val = serde_json::to_value(val).context("Failed to convert YAML to JSON")?;
    Ok(json_val)
}

fn validate_objective_id(id: &str) -> bool {
    if !id.starts_with("OBJ-") {
        return false;
    }
    let parts: Vec<&str> = id.split('-').collect();
    if parts.len() < 3 {
        return false;
    }
    let year = parts[1];
    if year.len() != 4 || !year.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }
    let slug = parts[2..].join("-");
    if slug.is_empty() {
        return false;
    }
    slug.chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '-')
}

fn handle_intent_pin(args: Value) -> Result<Value> {
    let path = find_intent_file().context("Could not determine repository root")?;
    handle_intent_pin_to_path(args, &path)
}

fn handle_intent_pin_to_path(args: Value, path: &Path) -> Result<Value> {
    let objective_id = args
        .get("objective_id")
        .and_then(|v| v.as_str())
        .context("Missing required parameter: objective_id")?;
    let epic = args
        .get("epic")
        .and_then(|v| v.as_str())
        .context("Missing required parameter: epic")?;

    if !validate_objective_id(objective_id) {
        return Err(anyhow::anyhow!(
            "Invalid objective_id format. Must match OBJ-YYYY-SLUG (e.g. OBJ-2026-SDF-001)"
        ));
    }

    let mut map = serde_json::Map::new();
    map.insert("objective_id".to_string(), json!(objective_id));
    map.insert("epic".to_string(), json!(epic));

    if let Some(constraints) = args.get("constraints") {
        map.insert("constraints".to_string(), constraints.clone());
    }
    if let Some(acceptance) = args.get("acceptance") {
        map.insert("acceptance".to_string(), acceptance.clone());
    }
    if let Some(active_agents) = args.get("active_agents") {
        map.insert("active_agents".to_string(), active_agents.clone());
    }

    let mut github_map = serde_json::Map::new();
    if let Some(issue) = args.get("github_issue") {
        github_map.insert("issue".to_string(), issue.clone());
    }
    if let Some(branch) = args.get("github_branch") {
        github_map.insert("branch".to_string(), branch.clone());
    }
    if !github_map.is_empty() {
        map.insert("github".to_string(), Value::Object(github_map));
    }

    let mut aivcs_map = serde_json::Map::new();
    if let Some(branch) = args.get("aivcs_branch") {
        aivcs_map.insert("branch".to_string(), branch.clone());
    }
    if !aivcs_map.is_empty() {
        map.insert("aivcs".to_string(), Value::Object(aivcs_map));
    }

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    map.insert("updated_at".to_string(), json!(now));

    let pinned_val = Value::Object(map);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory {}", parent.display()))?;
    }

    let yaml_str = serde_yaml::to_string(&pinned_val)
        .context("Failed to serialize intent manifest to YAML")?;
    std::fs::write(path, yaml_str)
        .with_context(|| format!("Failed to write intent file to {}", path.display()))?;

    Ok(pinned_val)
}

fn get_current_git_branch(repo_dir: &Path) -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(repo_dir)
        .output()
        .context("Failed to execute git command")?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "git rev-parse HEAD failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(branch)
}

fn handle_intent_status() -> Result<Value> {
    let path =
        find_intent_file().context("Could not determine repository root or locate intent file")?;
    if !path.exists() {
        return Ok(json!({
            "status": "uninitialized",
            "drift": false,
            "message": "No active intent pinned."
        }));
    }

    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read intent file at {}", path.display()))?;
    let val: Value =
        serde_yaml::from_str(&content).context("Failed to parse intent/current.yaml")?;

    let objective_id = val
        .get("objective_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let epic = val.get("epic").and_then(|v| v.as_str()).unwrap_or("");
    let pinned_branch = val
        .get("github")
        .and_then(|g| g.get("branch"))
        .and_then(|b| b.as_str());

    let repo_dir = path.parent().and_then(|p| p.parent()).unwrap_or(&path);
    let current_branch = get_current_git_branch(repo_dir).unwrap_or_else(|_| "unknown".to_string());

    let mut drift = false;
    let mut message = "Intent is active.".to_string();

    if let Some(pb) = pinned_branch {
        if current_branch != "unknown" && current_branch != pb {
            drift = true;
            message = format!(
                "Drift detected: current branch '{}' does not match pinned branch '{}'.",
                current_branch, pb
            );
        }
    }

    Ok(json!({
        "objective_id": objective_id,
        "epic": epic,
        "pinned_branch": pinned_branch,
        "current_branch": current_branch,
        "drift": drift,
        "status": "active",
        "message": message
    }))
}

fn run_gh_command(args: &[&str]) -> Result<String> {
    let mut cmd = Command::new("gh");
    cmd.args(args);

    if let Ok(tok) = std::env::var("GITHUB_TOKEN") {
        cmd.env("GITHUB_TOKEN", tok);
    } else if let Ok(tok) = std::env::var("GH_TOKEN") {
        cmd.env("GITHUB_TOKEN", tok);
    } else if let Ok(tok) = std::fs::read_to_string("/home/aivcs2/.secrets/stevedores_gh_token") {
        cmd.env("GITHUB_TOKEN", tok.trim());
    } else if let Ok(tok) = std::fs::read_to_string("/home/aivcs2/.secrets/gh_token") {
        cmd.env("GITHUB_TOKEN", tok.trim());
    }

    let output = cmd.output().context("Failed to execute gh command")?;
    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "gh command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn handle_github_issues(args: Value) -> Result<Value> {
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .context("Missing action")?;
    match action {
        "list" => {
            let res = run_gh_command(&["issue", "list", "--limit", "10"])?;
            Ok(json!({ "output": res }))
        }
        "view" => {
            let number = args
                .get("number")
                .and_then(|v| v.as_i64())
                .context("Missing number for view action")?;
            let num_str = number.to_string();
            let repo_slug = run_gh_command(&[
                "repo",
                "view",
                "--json",
                "nameWithOwner",
                "-q",
                ".nameWithOwner",
            ])
            .unwrap_or_default();

            let res = if !repo_slug.is_empty() {
                run_gh_command(&[
                    "api",
                    &format!("repos/{}/issues/{}", repo_slug.trim(), num_str),
                ])
                .unwrap_or_else(|_| {
                    run_gh_command(&["issue", "view", &num_str]).unwrap_or_default()
                })
            } else {
                run_gh_command(&["issue", "view", &num_str])?
            };
            Ok(json!({ "output": res }))
        }
        "create" => {
            let title = args
                .get("title")
                .and_then(|v| v.as_str())
                .context("Missing title for create action")?;
            let body = args.get("body").and_then(|v| v.as_str()).unwrap_or("");
            let res = run_gh_command(&["issue", "create", "--title", title, "--body", body])?;
            Ok(json!({ "output": res }))
        }
        other => Err(anyhow::anyhow!("Unsupported action: {other}")),
    }
}

fn handle_github_prs(args: Value) -> Result<Value> {
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .context("Missing action")?;
    match action {
        "list" => {
            let res = run_gh_command(&["pr", "list", "--limit", "10"])?;
            Ok(json!({ "output": res }))
        }
        "view" => {
            let number = args
                .get("number")
                .and_then(|v| v.as_i64())
                .context("Missing number for view action")?;
            let num_str = number.to_string();
            let repo_slug = run_gh_command(&[
                "repo",
                "view",
                "--json",
                "nameWithOwner",
                "-q",
                ".nameWithOwner",
            ])
            .unwrap_or_default();

            let res = if !repo_slug.is_empty() {
                run_gh_command(&[
                    "api",
                    &format!("repos/{}/pulls/{}", repo_slug.trim(), num_str),
                ])
                .unwrap_or_else(|_| run_gh_command(&["pr", "view", &num_str]).unwrap_or_default())
            } else {
                run_gh_command(&["pr", "view", &num_str])?
            };
            Ok(json!({ "output": res }))
        }
        "create" => {
            let title = args
                .get("title")
                .and_then(|v| v.as_str())
                .context("Missing title for create action")?;
            let body = args.get("body").and_then(|v| v.as_str()).unwrap_or("");
            let mut pr_args = vec!["pr", "create", "--title", title, "--body", body];

            let base = args.get("base").and_then(|v| v.as_str());
            let head = args.get("head").and_then(|v| v.as_str());
            if let Some(b) = base {
                pr_args.push("--base");
                pr_args.push(b);
            }
            if let Some(h) = head {
                pr_args.push("--head");
                pr_args.push(h);
            }

            let res = run_gh_command(&pr_args)?;
            Ok(json!({ "output": res }))
        }
        other => Err(anyhow::anyhow!("Unsupported action: {other}")),
    }
}

fn handle_github_reviews(args: Value) -> Result<Value> {
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .context("Missing action")?;
    let number = args
        .get("number")
        .and_then(|v| v.as_i64())
        .context("Missing number")?;
    let num_str = number.to_string();

    match action {
        "list" => {
            let repo_slug = run_gh_command(&[
                "repo",
                "view",
                "--json",
                "nameWithOwner",
                "-q",
                ".nameWithOwner",
            ])
            .unwrap_or_default();
            if repo_slug.is_empty() {
                return Err(anyhow::anyhow!(
                    "Could not resolve repository name for reviews list"
                ));
            }
            let res = run_gh_command(&[
                "api",
                &format!("repos/{}/pulls/{}/reviews", repo_slug.trim(), num_str),
            ])?;
            Ok(json!({ "output": res }))
        }
        "create" => {
            let event = args
                .get("event")
                .and_then(|v| v.as_str())
                .context("Missing event for create action (APPROVE, REQUEST_CHANGES, COMMENT)")?;
            let body = args.get("body").and_then(|v| v.as_str()).unwrap_or("");

            let event_flag = match event {
                "APPROVE" => "--approve",
                "REQUEST_CHANGES" => "--request-changes",
                "COMMENT" => "--comment",
                other => {
                    return Err(anyhow::anyhow!(
                        "Invalid event: {other}. Must be APPROVE, REQUEST_CHANGES, or COMMENT"
                    ))
                }
            };

            let mut review_args = vec!["pr", "review", &num_str, event_flag];
            if !body.is_empty() {
                review_args.push("-b");
                review_args.push(body);
            }
            let res = run_gh_command(&review_args)?;
            Ok(json!({ "output": res }))
        }
        other => Err(anyhow::anyhow!("Unsupported action: {other}")),
    }
}

fn handle_validate(args: Value) -> Result<Value> {
    let mut cmd = Command::new("local-ci");
    if let Some(args_arr) = args.get("args").and_then(|v| v.as_array()) {
        for arg in args_arr {
            if let Some(s) = arg.as_str() {
                cmd.arg(s);
            }
        }
    }

    let output = cmd.output().context("Failed to execute local-ci")?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let status = output.status.code().unwrap_or(-1);

    Ok(json!({
        "status": status,
        "stdout": stdout,
        "stderr": stderr,
        "success": output.status.success()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_objective_id() {
        assert!(validate_objective_id("OBJ-2026-SDF-001"));
        assert!(validate_objective_id("OBJ-2026-SDF-ABC-123"));
        assert!(!validate_objective_id("OBJ-26-SDF"));
        assert!(!validate_objective_id("OBJ-2026-"));
        assert!(!validate_objective_id("OBJ-2026-sdf")); // Must be uppercase or digit/hyphen
        assert!(!validate_objective_id("NOT-2026-SDF"));
    }

    #[test]
    fn test_intent_pin_and_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("intent/current.yaml");

        let args = json!({
            "objective_id": "OBJ-2026-TEST-PIN",
            "epic": "Test Epic",
            "constraints": ["test rule"],
            "acceptance": ["test works"],
            "active_agents": ["builder"],
            "github_issue": 123,
            "github_branch": "feat/test-branch"
        });

        // Pin
        let pinned = handle_intent_pin_to_path(args, &path).unwrap();
        assert_eq!(
            pinned.get("objective_id").unwrap().as_str().unwrap(),
            "OBJ-2026-TEST-PIN"
        );
        assert_eq!(pinned.get("epic").unwrap().as_str().unwrap(), "Test Epic");
        assert!(path.is_file());

        // Get
        let read = handle_intent_get_from_path(&path).unwrap();
        assert_eq!(
            read.get("objective_id").unwrap().as_str().unwrap(),
            "OBJ-2026-TEST-PIN"
        );
        assert_eq!(read.get("epic").unwrap().as_str().unwrap(), "Test Epic");
    }
}
