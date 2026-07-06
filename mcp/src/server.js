#!/usr/bin/env node
// crosstalk MCP — screened, SQS-backed messaging with built-in auto-poller.
// The auto-poller (pollLoop) writes new SQS messages to ~/.crosstalk/inbox.jsonl.
// External watchers (crosstalk-watch, Claude plugin) read from that store.
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

// ── Peer name validation ──────────────────────────────────────────────────────
const PEER_NAME_RE = /^[a-z][a-z0-9-]{1,31}$/;
function validatePeerName(name) {
  return typeof name === "string" && PEER_NAME_RE.test(name);
}

// Build the outbound envelope body, SIGNED when an identity is present.
// The canonical fields {msg_id, from, to, subject, content, ts} are signed via the vendored
// canonicalEnvelope — byte-identical to the receiver's. Fail-soft to unsigned on any signing error.
// Returns { body (JSON string), msg_id } so callers don't need to re-parse.
function buildBody({ from, to, subject, content }) {
  if (!validatePeerName(from)) throw new Error(`invalid sender peer name: "${from}"`);
  if (!validatePeerName(to)) throw new Error(`invalid recipient peer name: "${to}"`);
  const msg_id = randomBytes(8).toString("hex");
  const ts = new Date().toISOString();
  const base = { from, to, subject: subject || "", content, msg_id, ts };
  if (identity) {
    try {
      const sig = signCanonical(identity.privateKey, { msg_id, from, to, subject: subject || "", content, ts });
      return { body: JSON.stringify({ ...base, sig, advertised_pubkey: identity.pubkeyB64, from_node: from }), msg_id };
    } catch (e) {
      process.stderr.write(`crosstalk: sign failed (${e?.message || e}); sending unsigned\n`);
    }
  }
  return { body: JSON.stringify(base), msg_id };
}

async function creds() {
  return resolveCognitoCreds({
    region: cognito.region,
    userPoolId: cognito.userPoolId,
    clientId: cognito.clientId,
    identityPoolId: cognito.identityPoolId,
    username: cognito.username,
    refreshToken: cognito.refreshToken,
    password: cognito.password, // TODO: after first successful creds() call, clear cognito.password from memory to reduce exposure window
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
// Tracks recently sent messages and their ids for check_message_status queries.
// Does not persist across restarts — useful for same-session diagnosis.
// Capped at 500 entries; oldest are evicted on overflow.
const sentMessages = new Map(); // msg_id → { to, subject, ts, status, detail }
const MAX_SENT_TRACKED = 500;
function trackMessage(msg_id, data) {
  if (sentMessages.size >= MAX_SENT_TRACKED) {
    // Evict oldest entry
    const firstKey = sentMessages.keys().next().value;
    if (firstKey) sentMessages.delete(firstKey);
  }
  sentMessages.set(msg_id, data);
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
    const { body, msg_id } = buildBody({ from: inbox.peer, to, subject: subject || "", content });
    try {
      const c = await creds();
      const queueUrl = screenQueueUrl(inbox.region, inbox.account, to);
      debug(`send_message: to=${to} queueUrl=${queueUrl}`);
      const r = await sendMessage({ region: inbox.region, queueUrl, body, creds: c });
      const sqsMsgId = r.MessageId || "?";
      debug(`send_message: sent msg_id=${msg_id} sqsMsgId=${sqsMsgId}`);

      // Track in memory (screen verdict is not available server-side without
      // consuming from the FIFO queue, which would block the screen Lambda)
      trackMessage(msg_id, { to, subject: subject || "", ts: new Date().toISOString(), status: "sent", detail: "queued to content screen (verdict unknown — screen function processes asynchronously)" });

      const lines = [
        `Sent to ${to} via the content screen (MessageId ${sqsMsgId}).`,
        `Message tracking id: ${msg_id}.`,
        `You will NOT be notified if the screen rejects your message — use check_message_status to poll delivery outcome.`,
      ];
      return { content: [{ type: "text", text: lines.join("\n") }] };
    } catch (e) {
      const msg = String(e?.message || e);
      debug(`send_message: failed msg_id=${msg_id} error=${msg.slice(0, 200)}`);
      trackMessage(msg_id, { to, subject: subject || "", ts: new Date().toISOString(), status: "failed", detail: msg.slice(0, 200) });
      const hint = /AccessDenied|not authorized/i.test(msg) ? ` — you may not have a grant to message this peer, or the peer may not exist. Ask the admin to check.` : "";
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
    if (record.status === "sent") {
      statusLine += `\n\nNote: The screen function processes messages asynchronously. This status only confirms the message was queued — it does not confirm delivery. Ask the recipient to check their inbox.`;
    }
    return { content: [{ type: "text", text: statusLine }] };
  },
);

server.tool(
  "check_inbox",
  "Fetch and acknowledge new crosstalk messages from your own inbox. Reads from the local store (populated by the built-in auto-poller). Messages are marked as read automatically.",
  { limit: z.number().int().min(1).max(10).optional() },
  async ({ limit }) => {
    if (!ready) return notReadyResult();
    const max = limit || 10;
    try {
      // First try the local store (populated by the built-in auto-poller).
      const stored = takeUnread(INBOX_STORE, max);
      // If the store returned fewer results than requested, SQS may have newer
      // messages that haven't been polled yet — fall through to a direct receive.
      if (stored.length >= max) {
        const out = stored.map((p) => `from ${p.from || "?"}${p.subject ? ` [${p.subject}]` : ""}: ${p.content || ""}`);
        return { content: [{ type: "text", text: out.join("\n\n") }] };
      }
      // Fall back to direct SQS receive (store empty or partial).
      const c = await creds();
      const msgs = await receiveMessages({ region: inbox.region, queueUrl: cfg.CROSSTALK_SQS_INBOX_URL, max: Math.max(0, max - stored.length), creds: c });
      if (!msgs.length) {
        if (stored.length) {
          const out = stored.map((p) => `from ${p.from || "?"}${p.subject ? ` [${p.subject}]` : ""}: ${p.content || ""}`);
          return { content: [{ type: "text", text: out.join("\n\n") }] };
        }
        return { content: [{ type: "text", text: "No new messages." }] };
      }
      const out = [];
      // Include partial local store results first (older), then SQS results (newer)
      for (const p of stored) {
        out.push(`from ${p.from || "?"}${p.subject ? ` [${p.subject}]` : ""}: ${p.content || ""}`);
      }
      // Cap SQS results to respect the user's limit
      const sqsLimit = Math.max(0, max - stored.length);
      const cappedMsgs = msgs.slice(0, sqsLimit);
      for (const m of cappedMsgs) {
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
    const { body, msg_id } = buildBody({ from: inbox.peer, to, subject: subject || "re:", content });
    try {
      const c = await creds();
      const r = await sendMessage({ region: inbox.region, queueUrl: screenQueueUrl(inbox.region, inbox.account, to), body, creds: c });
      const sqsMsgId = r.MessageId || "?";
      trackMessage(msg_id, { to, subject: subject || "re:", ts: new Date().toISOString(), status: "sent", detail: "queued to content screen" });
      return { content: [{ type: "text", text: `Reply sent to ${to} (MessageId ${sqsMsgId}). Message tracking id: ${msg_id}. Use check_message_status to check delivery outcome.` }] };
    } catch (e) {
      const msg = String(e?.message || e);
      const hint = /AccessDenied|not authorized/i.test(msg) ? ` — you may not have a grant to message this peer, or the peer may not exist. Ask the admin to check.` : "";
      return { isError: true, content: [{ type: "text", text: `reply failed: ${msg.split("\n").slice(-2).join(" ").slice(0, 300)}${hint}` }] };
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

// Exponential backoff state for pollLoop error recovery
let pollBackoff = 1; // seconds, doubles on each error, caps at 60, resets to 1 on success

async function pollLoop() {
  for (;;) {
    try { await pollOnce(); pollBackoff = 1; }
    catch (e) {
      const waitMs = Math.min(pollBackoff * 1000, 60000);
      process.stderr.write(`crosstalk: poll error (${e?.message || e}); retrying in ${waitMs / 1000}s\n`);
      await new Promise((r) => setTimeout(r, waitMs));
      pollBackoff = Math.min(pollBackoff * 2, 60);
    }
  }
}

// Log unread count at startup
if (ready) {
  const n = unreadCount(INBOX_STORE);
  if (n > 0) {
    process.stderr.write(`crosstalk: ${n} unread message(s) in the local inbox — call check_inbox to read them.\n`);
  }
  pollLoop().catch(e => process.stderr.write(`crosstalk: poll loop crashed (${e?.message || e})\n`));
}
