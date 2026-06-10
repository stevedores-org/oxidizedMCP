# oxidizedMCP — Agent Guide

## What this is

Single MCP proxy for all Lornu skills. IDEs talk to `oxidized-mcp` over stdio; the proxy discovers skills from a registry and routes tool calls to HTTP skill containers.

## Layout

| Path | Purpose |
|------|---------|
| `crates/oxidized-mcp-core` | Registry, discovery, routing |
| `crates/oxidized-mcp` | CLI + stdio MCP server |
| `examples/echo-skill` | Local dev skill template |
| `registry/skills.yaml` | Default local manifest |

## Conventions

- Tool names are `skill::tool` (see `TOOL_NAMESPACE_SEP`)
- Skills expose MCP JSON-RPC at `{endpoint}` with `tools/list` and `tools/call`
- Use `cargo test --workspace` before PRs

## Epic tracking

Issue #1 defines Epics 1–3. MVP = Epic 1.1 (proxy + discovery).
