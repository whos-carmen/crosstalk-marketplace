# crosstalk MCP — build

The SQS-backed crosstalk MCP server, published as **`crosstalk-mcp`** on npm.

## Build

```bash
cd mcp
npm install
npm run build   # outputs dist/server.js + dist/reconnect-proxy.js
```

## Install from npm

```bash
npm install -g crosstalk-mcp
# In your MCP host config: { "command": "crosstalk-mcp" }
```

## Runtime prereqs

- Node.js >=20
- `~/.crosstalk/config.env` with Cognito credentials (set up by onboarding)

## Entry points

| Binary | What it does |
|--------|-------------|
| `crosstalk-mcp` | **MCP server + reconnect proxy** (recommended). Spawns the real server as a supervised child with crash recovery. |
| `crosstalk-watch` | **Email watcher CLI**. Monitors `~/.crosstalk/inbox.jsonl` and sends email via SMTP (nodemailer). |

### crosstalk-mcp (reconnect proxy)

- **Supervised child**: the real `server.js` runs as a child process
- **Crash recovery**: if the child dies, pending requests receive a `-32000` JSON-RPC error, and the child is respawned with exponential backoff (1s → 30s cap)
- **Request buffering**: tool calls arriving during the respawn gap are queued and drained once the new child is ready

To run without the proxy (debugging):

```json
{ "command": "node", "args": ["path/to/mcp/dist/server.js"] }
```

### crosstalk-watch (email watcher)

```
crosstalk-watch           → start background daemon
crosstalk-watch --stop    → stop daemon
crosstalk-watch --status  → check if running
crosstalk-watch --init    → copy SMTP config template
crosstalk-watch --test    → check inbox once, send email, exit
```

**fs.watch resilience:**
- Inode-change re-arm: detects atomic-rename file replacements by comparing inode numbers
- Debounce: coalesces rapid change events within a 100ms window
- Fallback polling: 30s interval stats the file as a safety net for silent watcher death (common on macOS)
- Error recovery: watcher errors trigger re-creation

## Tools (MCP)

| Tool | Description |
|------|-------------|
| `send_message` | Send a screened message to a peer via SQS |
| `check_inbox` | Read unread messages from the local inbox store |
| `reply` | Convenience wrapper over send_message |
| `crosstalk_identity` | Show Ed25519 public key + fingerprint |

## Files

| Source | Purpose |
|--------|---------|
| `src/server.js` | Main MCP server — 4 tools, store-only auto-poller |
| `src/reconnect-proxy.js` | Supervisor wrapper — crash recovery, request buffering, JSON-RPC error handling |
| `src/watch.js` | Inbox watcher daemon — fs.watch resilience, email via nodemailer |
| `bin/crosstalk-watch.js` | CLI for the inbox watcher |
| `src/cognito-creds.js` | Cognito User Pool -> Identity Pool -> STS |
| `src/sqs.js` | SQS send/receive/delete |
| `src/inbox-store.js` | Durable local inbox with dedup + compaction |
| `src/identity.js` | Ed25519 keypair generation, persistence, signing |
| `src/canonical-envelope.js` | Byte-identical canonical envelope serialization |
