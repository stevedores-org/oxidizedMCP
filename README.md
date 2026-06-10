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

## Cluster deployment

For long-lived deployments (K8s, systemd), opt in to HTTP health endpoints:

```bash
# Cluster pod — kubelet needs the pod IP, so bind all interfaces:
oxidized-mcp start --env production --health-port 8080 --health-bind-all
# GET /healthz → 200 while the process is alive
# GET /readyz  → 200 once the first skill-mesh refresh succeeds (else 503)
```

For local dev (kind/k3d, systemd on your laptop), omit `--health-bind-all` — the default is loopback so /healthz isn't exposed on the LAN:

```bash
oxidized-mcp start --health-port 8080
# /healthz only reachable on 127.0.0.1:8080
```

The proxy is OCI-packaged via `dockworker.toml` (no Dockerfile). The `--health-port` flag is opt-in — IDE/local use leaves it off and the process only speaks stdio.

> **Env-var gotcha** — `OXIDIZED_MCP_HEALTH_BIND_ALL` is a clap boolean: it accepts the literal strings `"true"` and `"false"`, not `"1"`/`"0"` or `"yes"`/`"no"`. K8s manifests must use `"true"`.

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
| `OXIDIZED_MCP_HEALTH_PORT` | TCP port for `/healthz` and `/readyz` (unset = no HTTP probes) |
| `OXIDIZED_MCP_HEALTH_BIND_ALL` | `"true"` to bind health endpoints on `0.0.0.0` (cluster pods); default loopback |

## Roadmap (from Issue #1)

- [x] **Epic 1.1** — Rust proxy, dynamic discovery, JSON-RPC routing
- [x] **Epic 1.1** — Azure AD OIDC for AKS hub registry
- [x] **Epic 1.1** — Periodic registry refresh with atomic snapshot swap
- [x] **Epic 1.1** — `/healthz` + `/readyz` HTTP probes + dockworker.toml manifests
- [ ] **Epic 1.1** — Per-skill auth (forward IDE bearer or Workload Identity)
- [ ] **Epic 1.1** — Skill health probes + degraded-skill eviction
- [ ] **Epic 2** — OCI skill packaging via dockworker.ai
- [ ] **Epic 3** — Flux/Crossplane skill registry in AKS

## License

MIT
