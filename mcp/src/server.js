#!/usr/bin/env node
// crosstalk MCP for Reasonix — screened, SQS-backed messaging.
// No auto-poller (inbox polling is handled externally via crosstalk-watcher or the Claude plugin).
//
// Config: read from ~/.crosstalk/config.env (written by /crosstalk-join) — the CROSSTALK_SQS_COGNITO_*
// bootstrap + CROSSTALK_SQS_INBOX_URL. Creds resolve via the vendored cognito-creds resolver
// (User Pool auth -> Identity Pool -> scoped, auto-refreshing STS).

import { readFileSync, existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { resolveCognitoCreds, cognitoConfigFromEnv } from "./cognito-creds.js";
import { sendMessage, receiveMessages, deleteMessage } from "./sqs.js";
import { loadOrCreateIdentity, signCanonical } from "./identity.js";
import { takeUnread, unreadCount, appendIfNew } from "./inbox-store.js";

// ── CROSSTALK_DEBUG stderr logging ───────────────────────────────────────────
const DEBUG = !!process.env.CROSSTALK_DEBUG;
function debug(...args) {
  if (DEBUG) process.stderr.write(`crosstalk: ${args.join(" ")}\n`);
}

const CONFIG_PATH = process.env.CROSSTALK_CONFIG || join(homedir(), ".crosstalk", "config.env");

// Parse a KEY=VALUE env file (tolerates `export `, quotes, comments) into an object.
function loadConfig(path) {
  const out = {};
  if (!existsSync(path)) return out;
  for (let line of readFileSync(path, "utf8").split("\n")) {
    line = line.trim();
    if (!line || line.startsWith("#")) continue;
    line = line.replace(/^export\s+/, "");
    const eq = line.indexOf("=");
    if (eq < 0) continue;
    const k = line.slice(0, eq).trim();
    let v = line.slice(eq + 1).trim().replace(/\s+#.*$/, "");
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
    out[k] = v;
  }
  return out;
}

// Derive {region, account, peer} from the own-inbox URL:
//   https://sqs.<region>.amazonaws.com/<account>/crosstalk-inbox-<peer>.fifo
function parseInboxUrl(url) {
  const m = /^https:\/\/sqs\.([^.]+)\.amazonaws\.com\/(\d{12})\/crosstalk-inbox-(.+)\.fifo$/.exec(url || "");
  if (!m) return null;
  return { region: m[1], account: m[2], peer: m[3] };
}

const screenQueueUrl = (region, account, to) =>
  `https://sqs.${region}.amazonaws.com/${account}/crosstalk-screen-${to}.fifo`;

const cfg = loadConfig(CONFIG_PATH);
const cognito = cognitoConfigFromEnv(cfg);
const inbox = parseInboxUrl(cfg.CROSSTALK_SQS_INBOX_URL);
const ready = !!(cognito && inbox);

// Ed25519 signing identity (first-run mints + persists ~/.crosstalk/identity.pem, 0600).
// FAIL-SOFT: if it can't load/create, we send UNSIGNED rather than block sends.
const IDENTITY_PATH = process.env.CROSSTALK_IDENTITY || join(homedir(), ".crosstalk", "identity.pem");
let identity = null;
if (ready) {
  try { identity = loadOrCreateIdentity(IDENTITY_PATH); }
  catch (e) { process.stderr.write(`crosstalk: signing identity unavailable (${e?.message || e}); sending unsigned\n`); }
}

// Build the outbound envelope body, SIGNED when an identity is present.
// The canonical fields {msg_id, from, to, subject, content, ts} are signed via the vendored
// canonicalEnvelope — byte-identical to the receiver's. Fail-soft to unsigned on any signing error.
function buildBody({ from, to, subject, content }) {
  const msg_id = randomBytes(8).toString("hex");
  const ts = new Date().toISOString();
  const base = { from, to, subject: subject || "", content, msg_id, ts };
  if (identity) {
    try {
      const sig = signCanonical(identity.privateKey, { msg_id, from, to, subject: subject || "", content, ts });
      return JSON.stringify({ ...base, sig, advertised_pubkey: identity.pubkeyB64, from_node: from });
    } catch (e) {
      process.stderr.write(`crosstalk: sign failed (${e?.message || e}); sending unsigned\n`);
    }
  }
  return JSON.stringify(base);
}

async function creds() {
  return resolveCognitoCreds({
    region: cognito.region,
    userPoolId: cognito.userPoolId,
    clientId: cognito.clientId,
    identityPoolId: cognito.identityPoolId,
    username: cognito.username,
    refreshToken: cognito.refreshToken,
    password: cognito.password,
  });
}

function notReadyResult() {
  return {
    isError: true,
    content: [{
      type: "text",
      text: `crosstalk is not configured yet. Run /crosstalk-join to sign in, get approved, and install your credentials to ${CONFIG_PATH}.`,
    }],
  };
}

const INBOX_STORE = process.env.CROSSTALK_INBOX_STORE || join(homedir(), ".crosstalk", "inbox.jsonl");

// ── Sent-message tracking (in-memory) ─────────────────────────────────────────
// Tracks recently sent messages and their inferred screening status.
// Does not persist across restarts — useful for same-session diagnosis.
const sentMessages = new Map(); // msg_id → { to, subject, ts, status, details, sqsMsgId? }

// After sending to the screen queue, attempt a brief poll on the queue to
// detect whether the screen has already consumed the message. If the message
// is still visible, screening is pending. If it's gone, it was either accepted
// or silently rejected — we can't distinguish server-side without async bounce
// support from the screen function.
async function checkScreenVerdict(queueUrl, creds, expectedBody, waitSeconds = 1) {
  try {
    const msgs = await receiveMessages({ region: inbox.region, queueUrl, max: 10, waitSeconds, creds });
    for (const m of msgs) {
      if (m.Body === expectedBody) {
        return { status: "pending", detail: "still in the screen queue (not yet processed)" };
      }
    }
    return { status: "consumed", detail: "consumed by the content screen (accepted or rejected)" };
  } catch (e) {
    debug("screen verdict poll failed:", e?.message);
    return { status: "unknown", detail: `poll failed: ${e?.message}` };
  }
}

const server = new McpServer(
  { name: "crosstalk", version: "0.1.0" },
  { capabilities: { tools: {} } },
);

server.tool(
  "send_message",
  "Send a crosstalk message to a peer. Routes through the content screen (academic/discussion only; operational content is blocked by design). You can only reach peers you've been granted access to.",
  { to: z.string().describe("recipient peer name"), subject: z.string().optional(), content: z.string().describe("message body") },
  async ({ to, subject, content }) => {
    if (!ready) return notReadyResult();
    const body = buildBody({ from: inbox.peer, to, subject: subject || "", content });
    const msg_id = JSON.parse(body).msg_id;
    try {
      const c = await creds();
      const queueUrl = screenQueueUrl(inbox.region, inbox.account, to);
      debug(`send_message: to=${to} queueUrl=${queueUrl}`);
      const r = await sendMessage({ region: inbox.region, queueUrl, body, creds: c });
      const sqsMsgId = r.MessageId || "?";
      debug(`send_message: sent msg_id=${msg_id} sqsMsgId=${sqsMsgId}`);

      // Track in memory
      sentMessages.set(msg_id, { to, subject: subject || "", ts: new Date().toISOString(), status: "sent", detail: "queued to content screen" });

      // Best-effort screen verdict check (1s poll)
      const verdict = await checkScreenVerdict(queueUrl, c, body);
      sentMessages.set(msg_id, { ...sentMessages.get(msg_id), status: verdict.status, detail: verdict.detail });

      const lines = [
        `Sent to ${to} via the content screen (MessageId ${sqsMsgId}).`,
        `Message tracking id: ${msg_id}.`,
        `Screen verdict: ${verdict.detail}.`,
        `You will NOT be notified if the screen rejects your message — use check_message_status to check delivery outcome.`,
      ];
      return { content: [{ type: "text", text: lines.join("\n") }] };
    } catch (e) {
      const msg = String(e?.message || e);
      debug(`send_message: failed msg_id=${msg_id} error=${msg.slice(0, 200)}`);
      sentMessages.set(msg_id, { to, subject: subject || "", ts: new Date().toISOString(), status: "failed", detail: msg.slice(0, 200) });
      const hint = /AccessDenied|not authorized/i.test(msg) ? ` — you don't have a grant to message "${to}" (ask the admin to grant it).` : "";
      return { isError: true, content: [{ type: "text", text: `send failed: ${msg.split("\n").slice(-2).join(" ").slice(0, 300)}${hint}` }] };
    }
  },
);

server.tool(
  "check_message_status",
  "Check the delivery/screening status of a previously sent message by its tracking id (returned by send_message). Lets you confirm whether the message was consumed by the screen or is still pending.",
  { msg_id: z.string().describe("message tracking id from send_message response") },
  async ({ msg_id }) => {
    if (!ready) return notReadyResult();
    const record = sentMessages.get(msg_id);
    if (!record) {
      return { content: [{ type: "text", text: `No record found for message id "${msg_id}". Tracking is in-memory only — ids from before this session are not available.` }] };
    }
    let statusLine = `Message ${msg_id} → ${record.to}`;
    if (record.subject) statusLine += ` (${record.subject})`;
    statusLine += `\nSent at: ${record.ts}`;
    statusLine += `\nStatus: ${record.status}`;
    statusLine += `\nDetail: ${record.detail}`;
    if (record.status === "consumed") {
      statusLine += `\n\nNote: "consumed" means the screen function processed the message. It was either forwarded to the recipient's inbox (accepted) or silently deleted (rejected). There is no server-side way to distinguish which — ask the recipient to check their inbox.`;
    }
    return { content: [{ type: "text", text: statusLine }] };
  },
);

server.tool(
  "check_inbox",
  "Fetch and acknowledge new crosstalk messages from your own inbox. Reads from the local store (populated by the external auto-poller). Messages are marked as read automatically.",
  { limit: z.number().int().min(1).max(10).optional() },
  async ({ limit }) => {
    if (!ready) return notReadyResult();
    try {
      // First try the local store (populated by the auto-poller / crosstalk-watcher via MCP).
      const stored = takeUnread(INBOX_STORE, limit || 10);
      if (stored.length) {
        const out = stored.map((p) => `from ${p.from || "?"}${p.subject ? ` [${p.subject}]` : ""}: ${p.content || ""}`);
        return { content: [{ type: "text", text: out.join("\n\n") }] };
      }
      // Fall back to direct SQS receive when the store is empty (no auto-poller running).
      const c = await creds();
      const msgs = await receiveMessages({ region: inbox.region, queueUrl: cfg.CROSSTALK_SQS_INBOX_URL, max: limit || 5, creds: c });
      if (!msgs.length) return { content: [{ type: "text", text: "No new messages." }] };
      const out = [];
      for (const m of msgs) {
        let parsed; try { parsed = JSON.parse(m.Body); } catch { parsed = { content: m.Body }; }
        out.push(`from ${parsed.from || "?"}${parsed.subject ? ` [${parsed.subject}]` : ""}: ${parsed.content || ""}`);
        try { await deleteMessage({ region: inbox.region, queueUrl: cfg.CROSSTALK_SQS_INBOX_URL, receiptHandle: m.ReceiptHandle, creds: c }); } catch { /* best-effort ack */ }
      }
      return { content: [{ type: "text", text: out.join("\n\n") }] };
    } catch (e) {
      return { isError: true, content: [{ type: "text", text: `check_inbox failed: ${String(e?.message || e).split("\n").slice(-2).join(" ").slice(0, 300)}` }] };
    }
  },
);

server.tool(
  "reply",
  "Reply to a peer (convenience over send_message).",
  { to: z.string(), content: z.string(), subject: z.string().optional() },
  async ({ to, content, subject }) => {
    if (!ready) return notReadyResult();
    const body = buildBody({ from: inbox.peer, to, subject: subject || "re:", content });
    try {
      const c = await creds();
      const r = await sendMessage({ region: inbox.region, queueUrl: screenQueueUrl(inbox.region, inbox.account, to), body, creds: c });
      return { content: [{ type: "text", text: `Reply sent to ${to} (MessageId ${r.MessageId || "?"}) — queued to content screen. Use check_message_status with the tracking id to check delivery.` }] };
    } catch (e) {
      return { isError: true, content: [{ type: "text", text: `reply failed: ${String(e?.message || e).split("\n").slice(-2).join(" ").slice(0, 300)}` }] };
    }
  },
);

server.tool(
  "crosstalk_identity",
  "Show this peer's signing identity — public key + fingerprint — so the network admin can pin it out-of-band (enables cryptographic origin-verification of your messages). The private key never leaves this machine.",
  {},
  async () => {
    if (!ready) return notReadyResult();
    if (!identity) return { isError: true, content: [{ type: "text", text: "No signing identity is available (it could not be created); messages are sent unsigned." }] };
    return { content: [{ type: "text", text:
      `peer:        ${inbox.peer}\n` +
      `public key:  ${identity.pubkeyB64}\n` +
      `fingerprint: ${identity.fingerprint}\n\n` +
      `Send the fingerprint to the network admin out-of-band so they can pin your key.` }] };
  },
);

const transport = new StdioServerTransport();
await server.connect(transport);

// ── Background inbox poller: SQS → local store (no Claude channel push) ──────────
// The watcher (systemd crosstalk-notify.path) detects local store changes and sends
// email. The write to disk before SQS ack ensures store-before-ack durability.
async function pollOnce() {
  const c = await creds();
  debug("pollOnce: polling inbox");
  const msgs = await receiveMessages({ region: inbox.region, queueUrl: cfg.CROSSTALK_SQS_INBOX_URL, max: 10, waitSeconds: 20, creds: c });
  debug(`pollOnce: received ${msgs.length} message(s)`);
  for (const m of msgs) {
    let parsed; try { parsed = JSON.parse(m.Body); } catch { parsed = { content: m.Body }; }
    debug(`pollOnce: storing msg_id=${parsed.msg_id || "?"}`);
    appendIfNew(INBOX_STORE, parsed);  // durable before ack — never lost
    try { await deleteMessage({ region: inbox.region, queueUrl: cfg.CROSSTALK_SQS_INBOX_URL, receiptHandle: m.ReceiptHandle, creds: c }); }
    catch (e) { process.stderr.write(`crosstalk: ack failed (${e?.message || e})\n`); }
  }
}

async function pollLoop() {
  for (;;) {
    try { await pollOnce(); }
    catch (e) { process.stderr.write(`crosstalk: poll error (${e?.message || e})\n`); await new Promise((r) => setTimeout(r, 5000)); }
  }
}

// Log unread count at startup
if (ready) {
  const n = unreadCount(INBOX_STORE);
  if (n > 0) {
    process.stderr.write(`crosstalk: ${n} unread message(s) in the local inbox — call check_inbox to read them.\n`);
  }
  pollLoop(); // fire-and-forget; the MCP stays responsive to tool calls
}
