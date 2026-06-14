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

Point `OXIDIZED_MCP_REGISTRY` at `registry/skills.yaml` or set `OXIDIZED_MCP_REGISTRY_URL` to your GitOps-published manifest (or, when deployed, the hub skill-registry API).

### Dynamic discovery & `tools/list` cache (Epic 3 / Issue #6)

The stdio server's `tools/list` handler calls `SkillMesh::list_tools_cached()`:

- **Fresh snapshot** — returns the in-memory aggregated tool list with no skill re-probes.
- **Stale snapshot** — triggers a registry refresh (same path as the background loop), then returns the updated list.
- **TTL** — defaults to 60 seconds and matches `OXIDIZED_MCP_REFRESH_INTERVAL_SECS` when set to a positive value. Set the interval to `0` to disable both periodic background refresh and time-based lazy refresh (startup refresh only).
- **Outages** — failed refreshes log a warning and serve the last good snapshot so the IDE stays responsive.
- **Concurrency** — all refresh paths share a lock so concurrent IDE `tools/list` calls and the background ticker cannot stampede skill backends.

Background `--refresh-interval-secs` (default 60) still runs for health probes and proactive discovery; lazy refresh covers IDE-driven `tools/list` when the snapshot ages out between ticks.

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
    # Optional — local Podman fallback when the hub endpoint is unreachable:
    # image: ghcr.io/example/github-skill:latest
```

## Environment variables

| Variable | Purpose |
|----------|---------|
| `OXIDIZED_MCP_REGISTRY` | Path to local manifest YAML |
| `OXIDIZED_MCP_REGISTRY_URL` | Remote manifest URL (JSON) |
| `OXIDIZED_MCP_ENV` | Environment label (`local`, `staging`, `production`) |
| `OXIDIZED_MCP_REFRESH_INTERVAL_SECS` | Re-fetch the registry every N seconds (default `60`; `0` disables). Also sets the lazy `tools/list` cache TTL in `start` mode. |
| `OXIDIZED_MCP_HEALTH_PORT` | TCP port for `/healthz` and `/readyz` (unset = no HTTP probes) |
| `OXIDIZED_MCP_HEALTH_BIND_ALL` | `"true"` to bind health endpoints on `0.0.0.0` (cluster pods); default loopback |
| `OXIDIZED_MCP_AUTH_MODE` | Outbound-call auth: `none` (default), or `gcloud-identity` for GKE Gateway / IAP backends |
| `OXIDIZED_MCP_AUTH_AUDIENCE` | Optional `--audiences` flag for `gcloud auth print-identity-token` |
| `OXIDIZED_MCP_USE_AZURE_AD` | Force Entra ID auth on HTTPS hub requests (`true`) |
| `OXIDIZED_MCP_AZURE_RESOURCE` / `OXIDIZED_MCP_AZURE_SCOPE` | Token scope (default `https://management.azure.com/.default`) |
| `OXIDIZED_MCP_BEARER_TOKEN` | Static bearer override (CI/tests; skips `az login`) |

### Authenticating to GKE Gateway / IAP backends

When skill endpoints live behind a GKE Gateway listener that validates Google
identity tokens, set:

```bash
export OXIDIZED_MCP_AUTH_MODE=gcloud-identity
export OXIDIZED_MCP_AUTH_AUDIENCE=https://api.lornu.ai     # whatever the Gateway expects
```

oxidizedMCP shells out to `gcloud auth print-identity-token` and attaches the
result as `Authorization: Bearer …` on every outbound `tools/list` and
`tools/call`. Tokens are cached in-process for ~55 minutes.

### Azure authentication (Epic 2 / Issue #5)

When `OXIDIZED_MCP_ENV=staging|production` or `OXIDIZED_MCP_USE_AZURE_AD=true`, the proxy attaches `Authorization: Bearer <token>` on all **HTTPS** registry and skill requests via `azure_identity`:

- **Local dev**: `DeveloperToolsCredential` (`az login` / `azd auth login`)
- **Kubernetes**: `WorkloadIdentityCredential` when `AZURE_FEDERATED_TOKEN_FILE` is set
- **Token refresh**: SDK-managed cache + proactive 5-minute background refresh loop
- **On failure**: MCP errors include `Run az login to refresh your Azure CLI session`

### Local Podman fallback (Epic 4 / Issue #7)

When an AKS hub skill endpoint times out or returns a transport error,
`oxidizedMCP` can fall back to a cached OCI image on your laptop:

1. Declare an optional `image` ref on the skill entry in the registry manifest.
2. On HTTP failure, the router runs `podman image exists <image>`.
3. If the image is present locally, it invokes `podman run -i --rm <image>` and
   exchanges one JSON-RPC request/response line over stdio (same transport as
   the proxy itself).
4. After **3 consecutive HTTP failures** on that skill, a per-skill circuit
   breaker opens and subsequent calls skip HTTP entirely — avoiding repeated
   60s timeouts during an outage. A successful HTTP call or a Healthy refresh
   probe resets the breaker.

```yaml
skills:
  - name: infra-code
    description: Crossplane / Flux / ESO GitOps skill
    endpoint: https://skills.staging.example.com/infra-code/mcp
    image: ghcr.io/lornu-ai/infra-code-skill:latest
    enabled: true
```

Pre-pull images while online: `podman pull ghcr.io/lornu-ai/infra-code-skill:latest`.
Skills without `image` keep the previous HTTP-only behavior.

Implementation: `crates/oxidized-mcp-core/src/local_runner.rs` +
`SkillMesh::call_tool` in `router.rs`. See `registry/skills.example.yaml`.

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
- [x] **Epic 4** — Podman fallback for offline mode

**Revised blueprint** ([Issue #3](https://github.com/stevedores-org/oxidizedMCP/issues/3), [docs/TDD_REVISED.md](./docs/TDD_REVISED.md)):

| Epic | Issue | Status |
|------|-------|--------|
| Protocol Translator (SSE)         | [#4](https://github.com/stevedores-org/oxidizedMCP/issues/4) | **Rust done** (PR #30) — SSE streaming + client cancellation implemented |
| Zero-Trust Auth (`azure_identity`) | [#5](https://github.com/stevedores-org/oxidizedMCP/issues/5) | **Rust done** — ingress JWT validation remains infra |
| Dynamic Registry + 60s cache       | [#6](https://github.com/stevedores-org/oxidizedMCP/issues/6) | **Rust done** (PR #32) — hub `skill-registry` service + GitOps publish remain |
| Health probes + dockworker.toml    | (n/a)                                                         | Done (PR #11) |
| Podman local fallback              | [#7](https://github.com/stevedores-org/oxidizedMCP/issues/7) | Done (Epic 4) |

## License

MIT
