# Unified MCP Setup: `lornu-mcp` Dev Preset

This guide details how to configure the unified `lornu-mcp` CLI stdio server under the **1MCP pattern** for different AI developer tools.

By running the unified preset, your IDE agents gain a single, consistent entry point to access local intents, query repository metadata via GitHub CLI (`gh`), and execute validation stages locally via `local-ci`.

---

## 1. Prerequisites & Build

Before configuring your AI tools, ensure you have the `lornu-mcp` binary compiled and available on your system.

### Build from Source
From the root of the `oxidizedMCP` repository:
```bash
cargo build --release
```
The compiled binary will be located at:
```
target/release/lornu-mcp
```

### Install to Path (Optional)
To make the binary globally accessible under the command `lornu-mcp`, copy it to your local binaries path or link it:
```bash
cp target/release/lornu-mcp ~/.cargo/bin/
# Or to system path:
sudo cp target/release/lornu-mcp /usr/local/bin/
```

Verify that it compiles and runs correctly:
```bash
lornu-mcp --help
```

---

## 2. Capability Preset: `dev`

The command `lornu-mcp serve --preset dev` (or simply `lornu-mcp serve` as `--preset dev` is the default) exposes a unified set of MCP tools:

| Tool Category | Tool Name | Description |
|---|---|---|
| **Intent Management** | `intent.get` | Reads repo-local `intent/current.yaml` (autodetects project root). |
| | `intent.pin` | Writes/Updates active objective to `intent/current.yaml`. |
| | `intent.status` | Detects current git branch drift against pinned branch. |
| **GitHub Access** | `github.issues` | Lists, views, or creates GitHub Issues using `gh` CLI. |
| | `github.PRs` | Lists, views, or creates GitHub Pull Requests using `gh` CLI. |
| | `github.reviews` | Lists or creates reviews on pull requests using `gh` CLI. |
| **Project Validation** | `validate` | Invokes the `local-ci` validator for verifying the project state. |

---

## 3. Client Configuration

Configure your client to point to the absolute path of the built `lornu-mcp` binary. 

### 3.1 Claude Code
Claude Code reads custom MCP servers from the global user config file at `~/.claude/mcp.json`.

Create or edit `~/.claude/mcp.json`:
```json
{
  "mcpServers": {
    "lornu-preset-dev": {
      "command": "/path/to/target/release/lornu-mcp",
      "args": ["serve", "--preset", "dev"]
    }
  }
}
```
> [!NOTE]
> Make sure to replace `/path/to/target/release/lornu-mcp` with the absolute path of your compiled binary (e.g. `/home/username/.cargo/bin/lornu-mcp`).

---

### 3.2 Cursor
Cursor supports MCP servers configured either globally or per workspace.

#### Global Configuration
1. Open Cursor Settings (**Ctrl+,** or **Cmd+,**).
2. Go to **Features** -> **MCP**.
3. Click **+ Add New MCP Server**.
4. Set the following fields:
   - **Name**: `lornu-preset-dev`
   - **Type**: `command`
   - **Command**: `/path/to/target/release/lornu-mcp serve --preset dev`
5. Click **Save**.

#### Workspace Configuration
Alternatively, you can commit the configuration directly to your project workspace. Create or edit `<workspace_dir>/.cursor/mcp.json`:
```json
{
  "mcpServers": {
    "lornu-preset-dev": {
      "command": "/path/to/target/release/lornu-mcp",
      "args": ["serve", "--preset", "dev"]
    }
  }
}
```

---

### 3.3 Antigravity CLI
Antigravity CLI loads custom MCP configurations from the local application configuration folder at `~/.gemini/antigravity-cli/mcp_config.json`.

Create or edit `~/.gemini/antigravity-cli/mcp_config.json`:
```json
{
  "mcpServers": {
    "lornu-preset-dev": {
      "command": "/path/to/target/release/lornu-mcp",
      "args": ["serve", "--preset", "dev"]
    }
  }
}
```

---

## 4. Verification

After configuring the client and restarting it (or refreshing the MCP server list in the IDE panel), verify the following:

1. **Active Connection**: The `lornu-preset-dev` status should show as **Connected** or **Green**.
2. **Tools Discovered**: Check the tools dropdown or list in your IDE. You should see `intent.get`, `intent.pin`, `intent.status`, `github.issues`, `github.PRs`, `github.reviews`, and `validate`.
3. **Run Intent Get**: Try calling the tool to check if it walks up the directory and reads your objective manifest:
   ```json
   // Example JSON-RPC tools/call request from client
   {
     "method": "tools/call",
     "params": {
       "name": "intent.get",
       "arguments": {}
     }
   }
   ```
