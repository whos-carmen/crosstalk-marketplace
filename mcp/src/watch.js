#!/usr/bin/env node
// crosstalk inbox watcher — email notification when new messages arrive.
// Monitors ~/.crosstalk/inbox.jsonl for changes and sends email via SMTP.
// Designed as a cross-platform replacement for the original bash + systemd notify.sh.
//
// Usage (via bin/crosstalk-watch.js):
//   crosstalk-watch                    # start background daemon
//   crosstalk-watch --stop             # stop background daemon
//   crosstalk-watch --status           # check if daemon is running
//   crosstalk-watch --init             # copy smtp.conf.template

import { readFileSync, existsSync, watch, mkdirSync, writeFileSync, unlinkSync, statSync, renameSync } from "node:fs";
import { writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { homedir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createTransport } from "nodemailer";

// ── Paths ────────────────────────────────────────────────────────────────────

const __dirname = dirname(fileURLToPath(import.meta.url));
const PACKAGE_ROOT = join(__dirname, "..");

const CROSSTALK_DIR = join(homedir(), ".crosstalk");
const WATCHER_DIR = join(CROSSTALK_DIR, "watcher");
const CONFIG_PATH = join(WATCHER_DIR, "smtp.conf");
const CONFIG_TEMPLATE = join(PACKAGE_ROOT, "smtp.conf.template");
const INBOX_PATH = process.env.CROSSTALK_INBOX_STORE || join(CROSSTALK_DIR, "inbox.jsonl");
const STATE_PATH = join(WATCHER_DIR, "state.json");
const LAST_RUN_PATH = join(WATCHER_DIR, ".last_run");
const PID_PATH = join(WATCHER_DIR, "watcher.pid");

// ── Config parsing ───────────────────────────────────────────────────────────

function loadConfig(path) {
  const out = {};
  if (!existsSync(path)) return out;
  for (const line of readFileSync(path, "utf8").split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eq = trimmed.indexOf("=");
    if (eq < 0) continue;
    let k = trimmed.slice(0, eq).trim();
    let v = trimmed.slice(eq + 1).trim().replace(/\s+#.*$/, "");
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
    out[k] = v;
  }
  return out;
}

// ── Daemon management ────────────────────────────────────────────────────────

const DAEMON_ARGS = ["--daemon-child"]; // internal flag — never type this by hand

export function daemonize() {
  mkdirSync(WATCHER_DIR, { recursive: true, mode: 0o700 });

  // Check if already running
  if (existsSync(PID_PATH)) {
    try {
      const existingPid = parseInt(readFileSync(PID_PATH, "utf8").trim(), 10);
      if (existingPid && isAlive(existingPid)) {
        console.error(`crosstalk-watch: daemon already running (pid ${existingPid})`);
        console.error("  Use:  crosstalk-watch --stop");
        process.exit(1);
      }
    } catch { /* stale pid file */ }
  }

  const child = spawn(process.execPath, [process.argv[1], ...DAEMON_ARGS], {
    cwd: process.cwd(),
    stdio: "ignore",
    detached: true,
  });

  child.unref();

  // Write PID file
  try {
    writeFileSync(PID_PATH, String(child.pid) + "\n", { mode: 0o600 });
  } catch (e) {
    console.error(`crosstalk-watch: failed to write PID file: ${e.message}`);
  }

  console.error(`crosstalk-watch: daemon started (pid ${child.pid})`);
  console.error(`  Watching: ${INBOX_PATH}`);
  console.error(`  PID file: ${PID_PATH}`);
  console.error("  To stop:  crosstalk-watch --stop");
  process.exit(0);
}

export function daemonStop() {
  if (!existsSync(PID_PATH)) {
    console.error("crosstalk-watch: no daemon PID file found at", PID_PATH);
    process.exit(1);
  }

  try {
    const pid = parseInt(readFileSync(PID_PATH, "utf8").trim(), 10);
    if (!pid) throw new Error("empty PID file");

    try {
      process.kill(pid, "SIGTERM");
      console.error(`crosstalk-watch: daemon (pid ${pid}) stopped`);
    } catch (e) {
      if (e.code === "ESRCH") {
        console.error(`crosstalk-watch: daemon (pid ${pid}) was not running`);
      } else {
        throw e;
      }
    }

    unlinkSync(PID_PATH);
  } catch (e) {
    if (e.code === "ENOENT") return; // already gone
    console.error(`crosstalk-watch: failed to stop daemon: ${e.message}`);
    process.exit(1);
  }
}

export function daemonStatus() {
  if (!existsSync(PID_PATH)) {
    console.error("crosstalk-watch: daemon is not running");
    process.exit(1);
  }
  try {
    const pid = parseInt(readFileSync(PID_PATH, "utf8").trim(), 10);
    if (pid && isAlive(pid)) {
      console.error(`crosstalk-watch: daemon is running (pid ${pid})`);
    } else {
      console.error(`crosstalk-watch: daemon is not running (stale pid ${pid})`);
      unlinkSync(PID_PATH);
      process.exit(1);
    }
  } catch (e) {
    console.error(`crosstalk-watch: daemon is not running (${e.message})`);
    process.exit(1);
  }
}

function isAlive(pid) {
  try {
    return process.kill(pid, 0); // no-op signal check
  } catch {
    return false;
  }
}

// ── State management ─────────────────────────────────────────────────────────

function loadState() {
  if (!existsSync(STATE_PATH)) return { seen: {}, lastRun: 0 };
  try {
    const raw = readFileSync(STATE_PATH, "utf8");
    return JSON.parse(raw);
  } catch {
    return { seen: {}, lastRun: 0 };
  }
}

async function saveState(state) {
  try {
    // Prune unbounded state.seen growth — keep the 200 most recent entries
    const keys = Object.keys(state.seen);
    if (keys.length > 1000) {
      // state.seen values are timestamps (ms); sort oldest-first and drop the extras
      const sorted = keys.sort((a, b) => (state.seen[a] || 0) - (state.seen[b] || 0));
      const pruned = {};
      for (const k of sorted.slice(-200)) pruned[k] = state.seen[k];
      state.seen = pruned;
    }

    mkdirSync(WATCHER_DIR, { recursive: true, mode: 0o700 });
    // Atomic write: write to temp file, then rename
    const tmpPath = STATE_PATH + ".tmp." + process.pid;
    await writeFile(tmpPath, JSON.stringify(state, null, 2) + "\n", { mode: 0o600 });
    renameSync(tmpPath, STATE_PATH);
  } catch (e) {
    console.error(`crosstalk-watch: failed to save state: ${e.message}`);
  }
}

// ── Email sending ─────────────────────────────────────────────────────────────

function buildEmailContent(newMessages, unreadCount) {
  const lines = [];
  lines.push(`You have ${newMessages.length} new crosstalk message(s) (${unreadCount} total unread).`);
  lines.push("");
  lines.push("─".repeat(58));

  for (const msg of newMessages) {
    const from = msg.from || "?";
    const subject = msg.subject || "";
    const content = (msg.content || "").slice(0, 300);
    const ts = msg.ts || "";
    lines.push(`From:  ${from}`);
    if (subject) lines.push(`Subj:  ${subject}`);
    lines.push(`When:  ${ts}`);
    lines.push(content);
    lines.push("");
    lines.push("─".repeat(58));
  }

  lines.push("");
  lines.push("Ask your AI to call check_inbox to read and reply.");
  return lines.join("\n");
}

async function sendEmail(config, newMessages, unreadCount) {
  const host = config.SMTP_HOST;
  const port = parseInt(config.SMTP_PORT || "587", 10);
  const user = config.SMTP_USER;
  const pass = config.SMTP_PASS;
  const from = config.SMTP_FROM;
  const to = config.NOTIFY_TO;

  if (!host || !user || !pass || !from || !to) {
    console.error("crosstalk-watch: smtp.conf is incomplete — missing required fields");
    return false;
  }

  if (to === "you@example.com") {
    console.error("crosstalk-watch: NOTIFY_TO is still the placeholder — edit smtp.conf first");
    return false;
  }

  const subject = `\u{1F4EC} Crosstalk: ${newMessages.length} new message(s) — ${unreadCount} total unread`;
  const body = buildEmailContent(newMessages, unreadCount);

  const transporter = createTransport({
    host,
    port,
    secure: port === 465,
    auth: { user, pass },
    connectionTimeout: 10000,
    socketTimeout: 15000,
  });

  try {
    const info = await transporter.sendMail({
      from: `Crosstalk Watcher <${from}>`,
      to,
      subject,
      text: body,
    });
    console.error(`crosstalk-watch: sent ${newMessages.length} notification(s) to ${to} (id=${info.messageId})`);
    return true;
  } catch (e) {
    console.error(`crosstalk-watch: email failed: ${e.message}`);
    return false;
  }
}

// ── Main check logic ─────────────────────────────────────────────────────────

async function checkAndNotify() {
  if (checkInProgress) return;
  checkInProgress = true;
  try {
    await _checkAndNotify();
  } finally {
    checkInProgress = false;
  }
}

async function _checkAndNotify() {
  if (!existsSync(INBOX_PATH)) return;

  const config = loadConfig(CONFIG_PATH);

  // Throttle
  const now = Math.floor(Date.now() / 1000);
  let lastRun = 0;
  if (existsSync(LAST_RUN_PATH)) {
    try {
      lastRun = parseInt(readFileSync(LAST_RUN_PATH, "utf8").trim(), 10) || 0;
    } catch { /* ignore */ }
  }
  const minInterval = parseInt(config.MIN_INTERVAL || "60", 10);
  if (now - lastRun < minInterval) return; // too soon

  // Load state
  const state = loadState();

  // Read inbox
  const raw = readFileSync(INBOX_PATH, "utf8").trim();
  if (!raw) return;
  const lines = raw.split("\n").filter(Boolean);

  // Find new unread messages
  const newMessages = [];
  for (const line of lines) {
    let msg;
    try {
      msg = JSON.parse(line);
    } catch {
      continue;
    }
    const msgId = msg.msg_id || msg.message_id || msg.id;
    if (!msgId) continue;
    if (msg._read) continue; // skip already-read
    if (!state.seen[msgId]) {
      newMessages.push(msg);
      state.seen[msgId] = Date.now();
    }
  }

  if (newMessages.length === 0) return;

  // Count total unread
  let unreadCount = 0;
  for (const line of lines) {
    try {
      const m = JSON.parse(line);
      if (!m._read) unreadCount++;
    } catch { /* skip */ }
  }

  const sent = await sendEmail(config, newMessages, unreadCount);

  // Always persist seen state — prevents re-notification on next trigger
  // even if email delivery fails. Only update lastRun on success to allow
  // retry on the next file-change event.
  await saveState(state);
  if (sent) {
    await writeFile(LAST_RUN_PATH, String(now), "utf8").catch(() => {});
  }
}

// ── File watcher (resilient) ────────────────────────────────────────────────
//
// == fs.watch resilience ==
// - Inode-change re-arm: on each change, stat the file; if inode changed
//   (atomic rename / write-tmp-then-rename), close the old watcher and
//   create a new one. This prevents silent drop on macOS (kqueue tracks
//   by inode, not path).
// - Debounce: coalesces rapid change events within 100ms, collapsing
//   rename+change into one notification.
// - Fallback polling: a 30s interval stats the file and compares mtime;
//   catches events fs.watch silently dropped (common on macOS under
//   rename-heavy traffic).
// - Error recovery: on watcher 'error', close and re-create.

let currentWatcher = null;
let debounceTimer = null;
let fallbackTimer = null;
let checkInProgress = false;

// File stat tracking for inode re-arm and fallback polling
let lastKnownIno = null;
let lastKnownMtime = 0;
let lastKnownSize = 0;

const FALLBACK_POLL_MS = 30_000;
const DEBOUNCE_MS = 100;

function refreshFileStat() {
  try {
    const stat = statSync(INBOX_PATH);
    lastKnownSize = stat.size;
    lastKnownMtime = stat.mtimeMs;
    lastKnownIno = stat.ino || null;
  } catch {
    lastKnownSize = 0;
    lastKnownMtime = 0;
    lastKnownIno = null;
  }
}

function rearmIfInodeChanged() {
  try {
    const stat = statSync(INBOX_PATH);
    if (stat.ino && stat.ino !== lastKnownIno) {
      // Inode changed — file was replaced; re-arm the watcher
      lastKnownIno = stat.ino;
      setupWatcher();
    }
  } catch { /* file may not exist momentarily; polling will catch it */ }
}

function setupWatcher() {
  // Close previous watcher
  if (currentWatcher) {
    try { currentWatcher.close(); } catch { /* ignore */ }
    currentWatcher = null;
  }

  try {
    currentWatcher = watch(INBOX_PATH, (eventType) => {
      // Re-arm if inode changed (atomic rename pattern)
      rearmIfInodeChanged();
      // Debounce coalesced events
      debouncedNotify();
    });

    currentWatcher.on("error", (err) => {
      console.error(`crosstalk-watch: fs.watch error (${err.message}), re-creating`);
      setupWatcher();
    });
  } catch (err) {
    console.error(`crosstalk-watch: fs.watch setup failed (${err.message}), relying on fallback polling`);
  }
}

function debouncedNotify() {
  if (debounceTimer) clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    debounceTimer = null;
    checkAndNotify().catch((e) => {
      console.error(`crosstalk-watch: notify failed: ${e.message}`);
    });
  }, DEBOUNCE_MS);
}

function setupFallbackPoll() {
  if (fallbackTimer) clearInterval(fallbackTimer);
  fallbackTimer = setInterval(() => {
    try {
      const stat = statSync(INBOX_PATH);
      // Save inode before refreshFileStat overwrites lastKnownIno
      if (stat.size !== lastKnownSize || stat.mtimeMs !== lastKnownMtime) {
        // fs.watch missed something — process it
        // Check inode BEFORE refreshFileStat overwrites lastKnownIno
        if (stat.ino && stat.ino !== lastKnownIno) {
          lastKnownIno = stat.ino;
          setupWatcher();
        }
        refreshFileStat();
        checkAndNotify().catch((e) => {
          console.error(`crosstalk-watch: poll notify failed: ${e.message}`);
        });
      }
    } catch { /* file may not exist; skip this tick */ }
  }, FALLBACK_POLL_MS);
  if (fallbackTimer.unref) fallbackTimer.unref();
}

export function testOnce() {
  console.error("crosstalk-watch: checking inbox once");
  mkdirSync(WATCHER_DIR, { recursive: true, mode: 0o700 });
  checkAndNotify()
    .then(() => process.exit(0))
    .catch((e) => {
      console.error(`crosstalk-watch: check failed: ${e.message}`);
      process.exit(1);
    });
}

export function startWatcher() {
  console.error("crosstalk-watch: watching", INBOX_PATH);

  // Ensure watcher dir exists
  mkdirSync(WATCHER_DIR, { recursive: true, mode: 0o700 });

  // Ensure inbox file exists before statting or watching it
  if (!existsSync(INBOX_PATH)) {
    writeFileSync(INBOX_PATH, "", { mode: 0o600 });
  }

  // Get baseline file stat
  refreshFileStat();

  // Run once immediately (fire-and-forget)
  checkAndNotify().catch((e) => {
    console.error(`crosstalk-watch: initial check failed: ${e.message}`);
  });

  // Set up resilient fs.watch
  setupWatcher();

  // Always run fallback polling (catches silent watcher death)
  setupFallbackPoll();

  process.on("SIGINT", () => { process.exit(0); });
  process.on("SIGTERM", () => { process.exit(0); });
}
