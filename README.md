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

### Lornu AI agent skills

`oxidizedMCP` can route repository-focused agent skills through the
`lornu.ai-mcp` hub. Start `lornu-mcp-hub-rs` in HTTP mode and point each skill
entry at its JSON-RPC endpoint:

```yaml
skills:
  - name: infra-code
    description: Lornu infra-code agent skill for Crossplane, Flux, ESO, GitOps review, and workspace-safe automation
    endpoint: http://127.0.0.1:8080/mcp
    enabled: true

  - name: gke-fleets
    description: Lornu GKE fleet agent skill for fleet membership, Gateway API, workload identity, and cluster attach workflows
    endpoint: http://127.0.0.1:8080/mcp
    enabled: true
```

Use repo-local manifests in `lornu-ai/infra-code` and
`lornu-ai/lornu-ai-gke-fleets` for the canonical agent skill scopes.

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
| `OXIDIZED_MCP_AUTH_MODE` | Outbound-call auth: `none` (default), or `gcloud-identity` for GKE Gateway / IAP backends |
| `OXIDIZED_MCP_AUTH_AUDIENCE` | Optional `--audiences` flag for `gcloud auth print-identity-token` |

### Authenticating to GKE Gateway / IAP backends

When skill endpoints live behind a GKE Gateway listener that validates Google
identity tokens, set:

```bash
export OXIDIZED_MCP_AUTH_MODE=gcloud-identity
export OXIDIZED_MCP_AUTH_AUDIENCE=https://api.lornu.ai     # whatever the Gateway expects
```

oxidizedMCP shells out to `gcloud auth print-identity-token` and attaches the
result as `Authorization: Bearer …` on every outbound `tools/list` and
`tools/call`. Tokens are cached in-process for ~55 minutes (gcloud ID-token
lifetime is 60). **No static secrets touch disk** — the developer's existing
`gcloud auth login` session is the only credential surface.

If `gcloud` is missing or the session has expired, requests fail with a clear
error pointing the user at `gcloud auth login`.

## Roadmap

**MVP (shipped)**: stdio proxy, HTTP routing, registry aggregation — see [Issue #1](https://github.com/stevedores-org/oxidizedMCP/issues/1).

- [x] **Epic 1.1** — Rust proxy, dynamic discovery, JSON-RPC routing
- [x] **Epic 1.1** — Azure AD OIDC for AKS hub registry
- [x] **Epic 1.1** — Periodic registry refresh with atomic snapshot swap
- [x] **Epic 1.1** — `/healthz` + `/readyz` HTTP probes + dockworker.toml manifests
- [x] **Epic 1.1** — Skill health probes + degraded-skill eviction
- [x] **Epic 2** — Outbound auth via GCP identity tokens (gcloud-identity mode)
- [ ] **Epic 1.1** — Per-skill auth (forward IDE bearer or Workload Identity)
- [ ] **Epic 2** — Workload Identity Federation for cluster-side `oxidized-mcp` (not just developer laptops)
- [ ] **Epic 2** — OCI skill packaging via dockworker.ai
- [ ] **Epic 3** — Flux/Crossplane skill registry in the GKE hub
- [ ] **Epic 4** — Podman fallback for offline mode

**Revised blueprint** ([Issue #3](https://github.com/stevedores-org/oxidizedMCP/issues/3), [docs/TDD_REVISED.md](./docs/TDD_REVISED.md)):

| Epic | Issue | Status |
|------|-------|--------|
| Protocol Translator (SSE)         | [#4](https://github.com/stevedores-org/oxidizedMCP/issues/4) | Not started (sync HTTP routing done; no SSE plumbing yet) |
| Zero-Trust Auth (`azure_identity`) | [#5](https://github.com/stevedores-org/oxidizedMCP/issues/5) | Partial (federated client-credentials exchange shipped; `DefaultAzureCredential` refresh loop still pending) |
| Dynamic Registry + 60s cache       | [#6](https://github.com/stevedores-org/oxidizedMCP/issues/6) | Done (`--refresh-interval-secs`, default 60; needs K8s ConfigMap source next) |
| Health probes + dockworker.toml    | (n/a)                                                         | Done (PR #11) |
| Podman local fallback              | [#7](https://github.com/stevedores-org/oxidizedMCP/issues/7) | Not started |

## License

MIT
