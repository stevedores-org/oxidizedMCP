# Technical Design: Sovereign Skill Mesh (`oxidizedMCP`)

Status: **MVP shipped; revised planning active** — see [Issue #1](https://github.com/stevedores-org/oxidizedMCP/issues/1) (founding principles) and [Issue #3](https://github.com/stevedores-org/oxidizedMCP/issues/3) (revised TDD).

> **Authoritative revised blueprint**: [docs/TDD_REVISED.md](./TDD_REVISED.md) (gap analysis + epic mapping)

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

- Hub API or GitOps-published manifest listing `name → endpoint`
- Consumed by `oxidized-mcp` via `OXIDIZED_MCP_REGISTRY_URL` (manifest JSON) or local `OXIDIZED_MCP_REGISTRY` YAML
- **Rust client done** (`develop`): periodic refresh, lazy 60s `tools/list` cache, atomic snapshot swap — see [Issue #6](https://github.com/stevedores-org/oxidizedMCP/issues/6)

## Protocol

1. IDE sends MCP JSON-RPC on stdio to `oxidized-mcp`
2. Proxy loads registry, calls each skill's `tools/list`, merges as `skill::tool`
3. On `tools/call`, proxy forwards JSON-RPC to the skill's HTTP `/mcp` endpoint

## Security (in progress — Epic 2)

- **Local**: registry file or localhost-only skills
- **Staging/prod**: `azure_identity` / `az login` Bearer tokens → AKS ingress JWT validation
- **MVP today**: `az account get-access-token`, env bearer tokens, workload-identity federated exchange (`registry.rs`)
- **Target**: `DefaultAzureCredential` with token refresh loop; no static API keys on laptops; ESO in cluster

## Resilience (planned — Epic 4)

- HTTP timeout → Podman image check → `podman run -i` stdio passthrough
- Circuit breaker on repeated AKS failures

## Streaming (planned — Epic 1)

- Long-running `tools/call` responses via SSE from cloud skills back to IDE stdio
