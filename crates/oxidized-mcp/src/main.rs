//! oxidizedMCP — Sovereign Skill Mesh proxy for agents, MCP, and skills.

mod stdio;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use oxidized_mcp_core::{RegistrySource, SkillMesh};
use std::path::{Path, PathBuf};
use tracing::info;
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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("oxidized_mcp=info".parse()?))
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start { env, registry } => {
            let source = resolve_registry(registry.as_deref())?;
            info!(environment = %env, ?source, "starting oxidizedMCP stdio server");
            let mut mesh = SkillMesh::new(source);
            stdio::run_stdio_server(&mut mesh)
                .await
                .context("stdio MCP server failed")?;
        }
        Commands::Discover { registry } => {
            let source = resolve_registry(registry.as_deref())?;
            let mut mesh = SkillMesh::new(source);
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
            let mut mesh = SkillMesh::new(source);
            mesh.refresh().await?;
            for tool in mesh.list_tools() {
                println!("{} — {}", tool.name, tool.description);
            }
        }
    }

    Ok(())
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
