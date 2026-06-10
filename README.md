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

| Variable | Purpose | Default |
|----------|---------|---------|
| `OXIDIZED_MCP_REGISTRY` | Path to local manifest YAML | — |
| `OXIDIZED_MCP_REGISTRY_URL` | Remote manifest URL (JSON) | — |
| `OXIDIZED_MCP_ENV` | Environment label (`local`, `staging`, `production`) | `local` |
| `OXIDIZED_MCP_TTL_SECS` | TTL between automatic `tools/list` refreshes. `0` disables auto-refresh. | `60` |

`tools/list` from the IDE is served from cache while fresh; on expiry, the next
request triggers a refresh against the registry + skill backends. The IDE
sees new skills the next time it lists tools after a deploy — without
restarting Cursor/Windsurf.

## Roadmap (from Issue #1)

- [x] **Epic 1.1** — Rust proxy, dynamic discovery, JSON-RPC routing
- [x] **Epic 3.1** — `tools/list` aggregator with TTL cache (60s default)
- [ ] **Epic 1.x** — SSE / streaming for long-running skills
- [ ] **Epic 2** — Zero-secret auth broker (Workload Identity Federation / GCP)
- [ ] **Epic 2** — OCI skill packaging via dockworker.ai
- [ ] **Epic 3** — Flux/Crossplane skill registry in the GKE hub
- [ ] **Epic 4** — Podman fallback for offline mode

## License

MIT
