//! oxidizedMCP — Sovereign Skill Mesh proxy for agents, MCP, and skills.

mod health;
mod stdio;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use oxidized_mcp_core::{AuthMode, Authenticator, RegistrySource, SkillMesh, SkillStatus};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "oxidized-mcp",
    about = "Sovereign Skill Mesh — one MCP entrypoint for all Lornu skills",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the MCP stdio server (configure once in mcp.json)
    Start {
        /// Deployment environment label (staging, production, local)
        #[arg(long, env = "OXIDIZED_MCP_ENV", default_value = "local")]
        env: String,

        /// Path to skills registry YAML/JSON manifest
        #[arg(long, env = "OXIDIZED_MCP_REGISTRY")]
        registry: Option<PathBuf>,

        /// Re-fetch the registry every N seconds. 0 disables periodic refresh
        /// (one-shot at startup only). Failed refreshes are logged and the
        /// previous snapshot remains in place.
        #[arg(long, env = "OXIDIZED_MCP_REFRESH_INTERVAL_SECS", default_value_t = 60)]
        refresh_interval_secs: u64,

        /// Optional port for /healthz and /readyz HTTP probes (cluster deploy).
        /// Omit for IDE / laptop use — the stdio transport doesn't need it.
        #[arg(long, env = "OXIDIZED_MCP_HEALTH_PORT")]
        health_port: Option<u16>,

        /// Bind the health endpoints on all interfaces (0.0.0.0). Default is
        /// loopback so /healthz on a laptop isn't exposed on the LAN. Set this
        /// in cluster deployments so kubelet can reach the probe via the pod
        /// IP. Env var must be the literal string "true" (clap bool semantics).
        #[arg(long, env = "OXIDIZED_MCP_HEALTH_BIND_ALL")]
        health_bind_all: bool,
    },

    /// Refresh skill discovery from registry and print tool count
    Discover {
        #[arg(long, env = "OXIDIZED_MCP_REGISTRY")]
        registry: Option<PathBuf>,
    },

    /// List aggregated tool names (skill::tool)
    ListTools {
        #[arg(long, env = "OXIDIZED_MCP_REGISTRY")]
        registry: Option<PathBuf>,
    },

    /// Inspect per-skill health from the last refresh
    Health {
        #[arg(long, env = "OXIDIZED_MCP_REGISTRY")]
        registry: Option<PathBuf>,

        /// Emit JSON instead of the human-readable table
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("oxidized_mcp=info".parse()?))
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            env,
            registry,
            refresh_interval_secs,
            health_port,
            health_bind_all,
        } => {
            let source = resolve_registry(registry.as_deref())?;
            let bind = if health_bind_all {
                health::HealthBind::All
            } else {
                health::HealthBind::Loopback
            };
            let auth_mode = AuthMode::from_env();
            info!(
                environment = %env,
                refresh_secs = refresh_interval_secs,
                ?source,
                ?health_port,
                ?bind,
                ?auth_mode,
                "starting oxidizedMCP stdio server"
            );

            let mesh = Arc::new(SkillMesh::with_auth(source, Authenticator::new(auth_mode)));
            mesh.refresh()
                .await
                .context("initial registry refresh failed")?;

            let refresh_handle = if refresh_interval_secs > 0 {
                Some(spawn_refresh_loop(
                    mesh.clone(),
                    Duration::from_secs(refresh_interval_secs),
                ))
            } else {
                None
            };

            let health_state = health::HealthState::new();
            let health_handle = health_port.map(|port| {
                let state_for_server = health_state.clone();
                let bind_for_server = bind.clone();
                tokio::spawn(async move {
                    if let Err(e) = health::serve(port, bind_for_server, state_for_server).await {
                        error!(error = %e, "health server crashed");
                    }
                })
            });

            // /readyz flips to 200 immediately — the initial refresh above
            // already succeeded (we'd have errored out otherwise).
            health_state.mark_ready();

            let result = stdio::run_stdio_server(mesh.clone(), Some(health_state.clone()))
                .await
                .context("stdio MCP server failed");

            // Stdio loop ended (clean shutdown from EOF, or an error). Either
            // way, stop the background refresh + health server so the process
            // can exit.
            if let Some(handle) = refresh_handle {
                handle.abort();
            }
            health_state.shutdown();
            if let Some(handle) = health_handle {
                let _ = handle.await;
            }

            result?;
        }
        Commands::Discover { registry } => {
            let source = resolve_registry(registry.as_deref())?;
            let mesh = SkillMesh::new(source);
            mesh.refresh().await?;
            let tool_count = mesh.list_tools().len();
            let (skill_count, env) = mesh
                .manifest()
                .map(|m| {
                    (
                        m.skills.iter().filter(|s| s.enabled).count(),
                        m.environment.clone(),
                    )
                })
                .unwrap_or((0, String::new()));
            println!("discovered {tool_count} tools from {skill_count} skills (env: {env})");
        }
        Commands::ListTools { registry } => {
            let source = resolve_registry(registry.as_deref())?;
            let mesh = SkillMesh::new(source);
            mesh.refresh().await?;
            for tool in mesh.list_tools() {
                println!("{} — {}", tool.name, tool.description);
            }
        }
        Commands::Health { registry, json } => {
            let source = resolve_registry(registry.as_deref())?;
            let mesh = SkillMesh::new(source);
            // Refresh ignores transient registry errors so `health` can still
            // emit the prior snapshot (empty on a cold start) — operators want
            // to see SOMETHING when triaging an outage.
            if let Err(e) = mesh.refresh().await {
                warn!(error = %e, "refresh failed before health probe; showing prior snapshot");
            }
            let health = mesh.health();
            if json {
                println!("{}", serde_json::to_string_pretty(&health)?);
            } else {
                print_health_table(&health);
            }
            // Non-zero exit if any skill is Down — useful for CI gates.
            if health.values().any(|h| h.status == SkillStatus::Down) {
                std::process::exit(2);
            }
        }
    }

    Ok(())
}

fn print_health_table(health: &std::collections::BTreeMap<String, oxidized_mcp_core::SkillHealth>) {
    if health.is_empty() {
        println!("no skills in registry");
        return;
    }
    let name_width = health.keys().map(|n| n.len()).max().unwrap_or(4).max(6);
    println!(
        "{:<name_width$}  {:<8}  {:>6}  LAST ERROR",
        "SKILL",
        "STATUS",
        "TOOLS",
        name_width = name_width,
    );
    for (name, h) in health {
        let status = match h.status {
            SkillStatus::Healthy => "healthy",
            SkillStatus::Down => "DOWN",
        };
        let last_error = h.last_error.as_deref().unwrap_or("");
        println!(
            "{:<name_width$}  {:<8}  {:>6}  {}",
            name,
            status,
            h.tools_count,
            last_error,
            name_width = name_width,
        );
    }
}

/// Spawn a background task that re-fetches the registry on a fixed interval.
/// The task survives transient errors (logs and continues), so a flaky network
/// or a briefly-unavailable registry endpoint never kills the proxy.
fn spawn_refresh_loop(mesh: Arc<SkillMesh>, interval: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // First tick fires immediately; skip it so we don't refresh twice
        // back-to-back with the initial refresh the caller already did.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match mesh.refresh().await {
                Ok(()) => {
                    info!(tools = mesh.list_tools().len(), "registry refreshed");
                }
                Err(e) => {
                    warn!(error = %e, "registry refresh failed; keeping previous snapshot");
                }
            }
        }
    })
}

fn resolve_registry(explicit: Option<&Path>) -> Result<RegistrySource> {
    let env_url = std::env::var("OXIDIZED_MCP_REGISTRY_URL").ok();
    let defaults = [
        PathBuf::from("registry/skills.yaml"),
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("oxidized-mcp/registry.yaml"),
    ];

    RegistrySource::resolve(
        explicit,
        env_url,
        defaults.iter().find(|p| p.exists()).map(|p| p.as_path()),
    )
    .context(
        "no skill registry found — set --registry, OXIDIZED_MCP_REGISTRY, or OXIDIZED_MCP_REGISTRY_URL",
    )
}
