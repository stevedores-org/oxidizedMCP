# oxidizedMCP

**Sovereign Skill Mesh** — one MCP entrypoint for all agent skills.

Developers configure their IDE **once**. `oxidizedMCP` discovers containerized skills from a central registry and routes `tools/list` + `tools/call` over HTTP to skill backends running locally (Podman) or in the Azure AKS hub.

> Design rationale: [Issue #1 — Founding principles](https://github.com/stevedores-org/oxidizedMCP/issues/1)

## Quick start

```bash
# Build the proxy
cargo build --release -p oxidized-mcp

# Terminal 1 — example skill container
cargo run -p echo-skill

# Terminal 2 — discover tools
cargo run -p oxidized-mcp -- discover

# Terminal 3 — MCP stdio server (what Cursor/Windsurf launch)
cargo run -p oxidized-mcp -- start --env local
```

## IDE configuration (one entry, forever)

```json
{
  "mcpServers": {
    "oxidized-mesh": {
      "command": "oxidized-mcp",
      "args": ["start", "--env", "local"]
    }
  }
}
```

Point `OXIDIZED_MCP_REGISTRY` at `registry/skills.yaml` or set `OXIDIZED_MCP_REGISTRY_URL` to your GitOps-published manifest.

## Architecture

```
┌─────────────┐   stdio MCP    ┌──────────────┐   HTTP JSON-RPC   ┌─────────────┐
│ Cursor /    │ ─────────────► │ oxidized-mcp │ ────────────────► │ echo-skill  │
│ Windsurf    │  tools/list    │   (proxy)    │   tools/call      │ (container) │
└─────────────┘  tools/call    └──────┬───────┘                   └─────────────┘
                                      │
                                      ▼
                              registry/skills.yaml
                              (future: K8s ConfigMap / hub API)
```

### Tool namespacing

Tools are aggregated as `skill::tool` (e.g. `echo::echo`) so agents never collide across skills.

## Registry manifest

```yaml
version: 1
environment: staging
skills:
  - name: github
    description: GitHub API operations
    endpoint: https://skills.staging.example.com/github/mcp
    enabled: true
```

## Environment variables

| Variable | Purpose |
|----------|---------|
| `OXIDIZED_MCP_REGISTRY` | Path to local manifest YAML |
| `OXIDIZED_MCP_REGISTRY_URL` | Remote manifest URL (JSON) |
| `OXIDIZED_MCP_ENV` | Environment label (`local`, `staging`, `production`) |
| `OXIDIZED_MCP_REFRESH_INTERVAL_SECS` | Re-fetch the registry every N seconds (default `60`; `0` disables) |

## Roadmap

**MVP (shipped)**: stdio proxy, HTTP routing, registry aggregation — see [Issue #1](https://github.com/stevedores-org/oxidizedMCP/issues/1).

- [x] **Epic 1.1** — Rust proxy, dynamic discovery, JSON-RPC routing
- [x] **Epic 1.1** — Azure AD OIDC for AKS hub registry
- [x] **Epic 1.1** — Periodic registry refresh with atomic snapshot swap
- [ ] **Epic 1.1** — Per-skill auth (forward IDE bearer or Workload Identity)
- [ ] **Epic 1.1** — Skill health probes + degraded-skill eviction
- [ ] **Epic 2** — OCI skill packaging via dockworker.ai
- [ ] **Epic 3** — Flux/Crossplane skill registry in AKS

**Revised blueprint** ([Issue #3](https://github.com/stevedores-org/oxidizedMCP/issues/3), [docs/TDD_REVISED.md](./docs/TDD_REVISED.md)):

| Epic | Issue | Status |
|------|-------|--------|
| Protocol Translator (SSE) | [#4](https://github.com/stevedores-org/oxidizedMCP/issues/4) | Partial |
| Zero-Trust Auth (`azure_identity`) | [#5](https://github.com/stevedores-org/oxidizedMCP/issues/5) | Partial |
| Dynamic Registry + 60s cache | [#6](https://github.com/stevedores-org/oxidizedMCP/issues/6) | Partial |
| Podman local fallback | [#7](https://github.com/stevedores-org/oxidizedMCP/issues/7) | Not started |

## License

MIT
