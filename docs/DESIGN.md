# Technical Design: Sovereign Skill Mesh (`oxidizedMCP`)

Status: **MVP in progress** — see [Issue #1](https://github.com/stevedores-org/oxidizedMCP/issues/1)

## Problem

MCP sprawl: every skill needs its own `mcp.json` entry, env vars, and local paths. Laptops drift from the sovereign control plane.

## Solution

A **small Rust binary on the laptop** that does not execute skills — it **discovers and proxies** to containerized skill services governed by GitOps.

## Components

### `oxidized-mcp` (this repo)

| Crate | Role |
|-------|------|
| `oxidized-mcp-core` | Registry loading, skill discovery, HTTP JSON-RPC routing |
| `oxidized-mcp` | CLI + stdio MCP server for IDEs |

### Skill containers (separate repos / images)

- HTTP endpoint at `/mcp` implementing `tools/list` and `tools/call`
- Packaged by dockworker.ai (no Dockerfiles)
- Deployed via Flux to AKS; secrets via ESO

### Skill registry (future: `lornu-ai/skills-registry`)

- K8s ConfigMap or hub API listing `name → endpoint`
- Consumed by `oxidized-mcp` via `OXIDIZED_MCP_REGISTRY_URL`

## Protocol

1. IDE sends MCP JSON-RPC on stdio to `oxidized-mcp`
2. Proxy loads registry, calls each skill's `tools/list`, merges as `skill::tool`
3. On `tools/call`, proxy forwards JSON-RPC to the skill's HTTP `/mcp` endpoint

## Security (planned)

- Local: registry file or localhost-only skills
- Staging/prod: OIDC workload identity from laptop → AKS hub registry
- No API keys on laptops; ESO in cluster
