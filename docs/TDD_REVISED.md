# Technical Design Document (TDD): `oxidizedMCP` — Sovereign Skill Mesh Proxy (Revised)

**Status**: Active Planning (reopened via [Issue #3](https://github.com/stevedores-org/oxidizedMCP/issues/3))
**Supersedes**: High-level hybrid design in [Issue #1](https://github.com/stevedores-org/oxidizedMCP/issues/1)
**Core Stack**: Rust (`tokio`, `tracing`, `azure_identity`), MCP JSON-RPC 2.0, Azure AKS Hub

## 1. Executive Summary: Why Planning Reopened

The initial plan treated `oxidizedMCP` as a simple pass-through. IDEs (Cursor, Windsurf) communicate with MCP servers via **stdio**. A remote Kubernetes cluster cannot natively receive stdio.

Therefore `oxidizedMCP` must act as:

1. **Protocol translator** — stdio ↔ HTTP/SSE (or WebSockets)
2. **Zero-trust auth broker** — ephemeral Entra ID tokens, no static secrets on laptops
3. **Resilience layer** — Podman fallback when the AKS hub is unreachable

## 2. Deep Architecture

### 2.1 Protocol Translation Layer

| Hop | Transport | Payload |
|-----|-----------|---------|
| IDE → Proxy | stdio | JSON-RPC 2.0 |
| Proxy → Cloud | HTTP POST / SSE | JSON-RPC forwarded to skill `/mcp` |
| Cloud → Skill | Ingress + JWT validation | Routed to skill pod |

### 2.2 Sovereign Authentication (Zero-Secret Local Auth)

- Use `DefaultAzureCredential` from the `azure_identity` crate (hooks into `az login`)
- Attach short-lived JWT as `Authorization: Bearer <token>` on all AKS hub requests
- On expiry: return MCP error instructing `az login`

### 2.3 Local Podman Fallback (Offline Mode)

If AKS hub times out:

1. Check local Podman for cached OCI image for the skill
2. If present: `podman run -i <image>` and pipe stdio directly
3. Circuit breaker prevents hammering unreachable cloud endpoints

## 3. MVP vs Revised TDD (Gap Analysis)

| Capability | MVP (`main`) | Revised TDD | Gap |
|------------|--------------|-------------|-----|
| stdio MCP server | `crates/oxidized-mcp/src/stdio.rs` | Epic 1 | **Partial** — no SSE streaming |
| stdio → HTTP routing | `oxidized-mcp-core` router | Epic 1 | **Done** for sync HTTP |
| `tools/list` aggregation | `SkillMesh` + namespacing | Epic 3 | **Done** locally |
| Registry URL fetch | `registry.rs` | Epic 3 | **Partial** — no 60s cache TTL |
| Azure auth | `az account get-access-token` + env tokens | Epic 2 | **Partial** — needs `azure_identity` + refresh loop |
| Podman fallback | — | Epic 4 | **Not started** |
| Hub skill-registry service | — | Epic 3 | **Not started** (`lornu-ai/skills-registry`) |
| Ingress JWT validation | — | Epic 2 (infra) | **Not started** |

## 4. Target Module Layout

| Module | Crate path | Responsibility |
|--------|------------|----------------|
| Proxy | `crates/oxidized-mcp-core/src/proxy.rs` | stdio ↔ HTTP/SSE translation |
| Auth | `crates/oxidized-mcp-core/src/auth.rs` | `azure_identity` token broker |
| Local runner | `crates/oxidized-mcp-core/src/local_runner.rs` | Podman exec fallback |
| Registry service | `lornu-ai/skills-registry/` | FastAPI hub discovery API |

## 5. Agile Backlog (Epics)

| Epic | Title | GitHub Issue |
|------|-------|--------------|
| 1 | Protocol Translator Engine (stdio ↔ HTTP/SSE) | [#4](https://github.com/stevedores-org/oxidizedMCP/issues/4) |
| 2 | Zero-Trust Developer Authentication | [#5](https://github.com/stevedores-org/oxidizedMCP/issues/5) |
| 3 | Dynamic Skill Discovery & Registry | [#6](https://github.com/stevedores-org/oxidizedMCP/issues/6) |
| 4 | Local Fallback & Podman Integration | [#7](https://github.com/stevedores-org/oxidizedMCP/issues/7) |

Full user stories and acceptance criteria: [Issue #3](https://github.com/stevedores-org/oxidizedMCP/issues/3).

## 6. Relationship to `lornu.ai-mcp`

| Repo | Role |
|------|------|
| **stevedores-org/oxidizedMCP** (OSS) | Generic skill mesh proxy, registry format, protocol translation |
| **lornu-ai/lornu.ai-mcp** (private) | Secrets masking, Lornu-native hub skills, private endpoints |

`lornu-mesh` composes `oxidized-mcp-core` (git dep) with the in-process Lornu hub. OSS proxy evolution does not block private-layer work.
