#!/usr/bin/env node
// crosstalk MCP reconnect proxy — supervisor wrapper for the MCP server.
//
// The MCP host spawns THIS script instead of the raw server. This proxy:
//   1. Spawns the real server (./server.js from the same dist directory) as a child
//   2. Proxies stdin/stdout for JSON-RPC communication
//   3. Tracks in-flight requests and replies with JSON-RPC errors on crash
//   4. Respaws the child on crash with exponential backoff (1s → 30s cap)
//   5. Buffers incoming requests during the respawn gap and drains them
//      once the new child is ready
//
// Usage (from MCP host config):
//   { "command": "node", "args": ["path/to/dist/reconnect-proxy.js"] }
//
// To bypass the proxy (debug / single-process mode):
//   { "command": "node", "args": ["path/to/dist/server.js"] }

import { spawn } from "node:child_process";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createInterface } from "node:readline";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SERVER_PATH = join(__dirname, "server.js");

// ── Constants ────────────────────────────────────────────────────────────────
const INITIAL_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 30_000;
const BACKOFF_FACTOR = 2;
const MAX_CONSECUTIVE_CRASHES = 10;

// ── State ────────────────────────────────────────────────────────────────────
let child = null;
let childExited = false;
let backoffMs = INITIAL_BACKOFF_MS;
let crashCount = 0;
let pendingRequests = new Map();
let requestBuffer = [];
let draining = false;
let healthy = false;

// ── JSON-RPC helpers ─────────────────────────────────────────────────────────

function isJsonRpcRequest(line) {
  try {
    const msg = JSON.parse(line);
    return msg && typeof msg === "object" && msg.id !== undefined && msg.id !== null && msg.method;
  } catch {
    return false;
  }
}

function isJsonRpcResponse(line) {
  try {
    const msg = JSON.parse(line);
    return msg && typeof msg === "object" && msg.id !== undefined && msg.id !== null && (msg.result !== undefined || msg.error !== undefined);
  } catch {
    return false;
  }
}

function parseId(line) {
  try {
    const msg = JSON.parse(line);
    return msg.id;
  } catch {
    return null;
  }
}

function buildErrorResponse(id, code, message) {
  return JSON.stringify({
    jsonrpc: "2.0",
    id,
    error: { code, message },
  }) + "\n";
}

// Notifications (no id) are silently dropped on crash — they don't need a response.
function isNotification(line) {
  try {
    const msg = JSON.parse(line);
    return msg && typeof msg === "object" && msg.id === undefined && msg.method;
  } catch {
    return false;
  }
}

// ── Child process management ─────────────────────────────────────────────────

function startChild() {
  if (child) {
    try { child.kill(); } catch { /* ignore */ }
    child = null;
  }

  childExited = false;
  healthy = false;
  draining = false;

  child = spawn(process.execPath, [SERVER_PATH], {
    stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env },
    cwd: dirname(SERVER_PATH),
  });

  child.on("spawn", () => {
    crashCount = 0;
    backoffMs = INITIAL_BACKOFF_MS;
    process.stderr.write(`crosstalk-proxy: child spawned (pid ${child.pid})\n`);
  });

  // Forward child's stdout → our stdout (responses from server to host)
  // Intercept to track in-flight request resolution
  const rl = createInterface({ input: child.stdout, crlfDelay: Infinity });
  rl.on("line", (line) => {
    if (isJsonRpcResponse(line)) {
      const id = parseId(line);
      if (id !== null) pendingRequests.delete(id);
    }
    // Forward to host regardless
    process.stdout.write(line + "\n");
  });

  // Forward child's stderr → our stderr (logs from server)
  child.stderr.on("data", (data) => {
    process.stderr.write(data);
  });

  child.on("exit", (code, signal) => {
    childExited = true;
    healthy = false;
    const reason = signal
      ? `signal ${signal}`
      : `exit code ${code}`;
    process.stderr.write(`crosstalk-proxy: child exited (${reason})\n`);

    // Fail all pending requests with JSON-RPC error
    const pendingSnapshot = new Map(pendingRequests);
    pendingRequests.clear();

    for (const [id, method] of pendingSnapshot) {
      process.stdout.write(buildErrorResponse(
        id,
        -32000,
        `Server error: child process crashed (${reason}). The request "${method}" was not completed. The server is being respawned.`,
      ));
    }

    // Decide whether to respawn
    crashCount++;
    if (crashCount > MAX_CONSECUTIVE_CRASHES) {
      process.stderr.write(
        `crosstalk-proxy: ${MAX_CONSECUTIVE_CRASHES} consecutive crashes — giving up.\n`,
      );
      process.exit(1);
    }

    // Exponential backoff
    const waitMs = Math.min(backoffMs, MAX_BACKOFF_MS);
    process.stderr.write(
      `crosstalk-proxy: respawning in ${Math.round(waitMs / 1000)}s (attempt #${crashCount})...\n`,
    );
    backoffMs *= BACKOFF_FACTOR;

    setTimeout(() => {
      if (!childExited) return; // already restarted
      startChild();
    }, waitMs);
  });

  child.on("error", (err) => {
    process.stderr.write(`crosstalk-proxy: child error (${err.message})\n`);
    // 'exit' will fire after 'error', so respawn logic is there
  });

  // Mark healthy once the child is ready
  child.stdout.on("readable", () => {
    if (childExited) return;
    if (!healthy) {
      healthy = true;
      process.stderr.write("crosstalk-proxy: child is ready\n");
      drainBuffer();
    }
  });

  // Heuristic: if no data arrives within 100ms, assume child is ready
  const readyTimer = setTimeout(() => {
    if (childExited) return;
    if (!healthy) {
      healthy = true;
      process.stderr.write("crosstalk-proxy: child assumed ready\n");
      drainBuffer();
    }
  }, 100);
  if (readyTimer.unref) readyTimer.unref();
}

function drainBuffer() {
  if (draining) return;
  draining = true;
  while (requestBuffer.length > 0 && healthy && !childExited) {
    const line = requestBuffer.shift();
    forwardRequest(line);
  }
  draining = false;
}

// ── Request forwarding ───────────────────────────────────────────────────────

function forwardRequest(line) {
  if (!child || childExited || !child.stdin.writable) return false;

  const id = parseId(line);
  if (id !== null && isJsonRpcRequest(line)) {
    let method = "(unknown)";
    try { method = JSON.parse(line).method; } catch { /* ignore */ }
    pendingRequests.set(id, method);
  }

  child.stdin.write(line + "\n");
  return true;
}

// ── Stdin handler (host → proxy → child) ─────────────────────────────────────

const stdinRl = createInterface({ input: process.stdin, crlfDelay: Infinity });

stdinRl.on("line", (line) => {
  if (!line.trim()) return;

  if (!child || childExited) {
    // Child is down — buffer or drop
    if (isJsonRpcRequest(line)) {
      requestBuffer.push(line);
    } else if (isNotification(line)) {
      process.stderr.write("crosstalk-proxy: dropped notification (child is down)\n");
    }
    return;
  }

  if (!healthy) {
    requestBuffer.push(line);
    return;
  }

  forwardRequest(line);
});

// ── Signal handling ──────────────────────────────────────────────────────────

function shutdown(signal) {
  process.stderr.write(`crosstalk-proxy: received ${signal}, shutting down child\n`);
  if (child && !childExited) {
    child.kill(signal === "SIGTERM" ? "SIGTERM" : "SIGINT");
  }
  setTimeout(() => process.exit(0), 2000);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

process.stdin.on("end", () => {
  process.stderr.write("crosstalk-proxy: stdin closed (host disconnected), shutting down\n");
  if (child && !childExited) child.kill("SIGTERM");
  setTimeout(() => process.exit(0), 1000);
});

process.on("uncaughtException", (err) => {
  process.stderr.write(`crosstalk-proxy: uncaught exception (${err.message})\n`);
});

process.on("unhandledRejection", (reason) => {
  process.stderr.write(`crosstalk-proxy: unhandled rejection (${String(reason).slice(0, 200)})\n`);
});

// ── Start ────────────────────────────────────────────────────────────────────
process.stderr.write(
  `crosstalk-proxy: starting (pid ${process.pid}, child: ${SERVER_PATH})\n`,
);
startChild();

setInterval(() => {
  if (!child && childExited && crashCount > MAX_CONSECUTIVE_CRASHES) {
    process.exit(1);
  }
}, 5000).unref();
