#!/usr/bin/env node
// crosstalk-watch CLI — email notification for new crosstalk inbox messages.
//
// Usage:
//   crosstalk-watch                     # run in foreground (Ctrl+C to stop)
//   crosstalk-watch --install           # install as a systemd user service
//   crosstalk-watch --init              # copy smtp.conf.template and print setup guide

import { readFileSync, existsSync, copyFileSync, mkdirSync } from "node:fs";
import { execSync } from "node:child_process";
import { homedir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { startWatcher } from "../src/watch.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PACKAGE_ROOT = join(__dirname, "..");
const CROSSTALK_DIR = join(homedir(), ".crosstalk");
const WATCHER_DIR = join(CROSSTALK_DIR, "watcher");
const CONFIG_PATH = join(WATCHER_DIR, "smtp.conf");
const CONFIG_TEMPLATE = join(PACKAGE_ROOT, "smtp.conf.template");

// ── Config parser (shared with watch.js, inline for CLI) ──────────────────────

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

// ── Commands ──────────────────────────────────────────────────────────────────

function cmdInit() {
  mkdirSync(WATCHER_DIR, { recursive: true, mode: 0o700 });

  if (existsSync(CONFIG_PATH)) {
    console.error("crosstalk-watch: smtp.conf already exists at", CONFIG_PATH);
  } else {
    copyFileSync(CONFIG_TEMPLATE, CONFIG_PATH);
    console.error("crosstalk-watch: created", CONFIG_PATH);
  }

  console.error("");
  console.error("To configure, edit the file and fill in your SMTP settings.");
  console.error("Then run:");
  console.error("  crosstalk-watch                    # run in foreground");
  console.error("  crosstalk-watch --install          # install as systemd service");
  console.error("");
}

function cmdInstall() {
  if (!existsSync(CONFIG_PATH)) {
    console.error("crosstalk-watch: no smtp.conf found. Run --init first.");
    process.exit(1);
  }

  // Validate config has real values
  const cfg = loadConfig(CONFIG_PATH);
  if (!cfg.NOTIFY_TO || cfg.NOTIFY_TO === "you@example.com") {
    console.error("crosstalk-watch: NOTIFY_TO is still the placeholder. Edit", CONFIG_PATH, "first.");
    process.exit(1);
  }

  const SYSTEMD_DIR = join(homedir(), ".config", "systemd", "user");
  const SERVICE_SRC = join(PACKAGE_ROOT, "..", "..", "watcher", "crosstalk-notify.service");
  const PATH_SRC = join(PACKAGE_ROOT, "..", "..", "watcher", "crosstalk-notify.path");
  const SERVICE_DST = join(SYSTEMD_DIR, "crosstalk-notify.service");
  const PATH_DST = join(SYSTEMD_DIR, "crosstalk-notify.path");

  if (!existsSync(SERVICE_SRC)) {
    console.error("crosstalk-watch: systemd unit files not found alongside this package.");
    console.error("  Expected at:", SERVICE_SRC);
    console.error("  The --install flag requires the full crosstalk-marketplace checkout.");
    console.error("  For standalone usage, run: crosstalk-watch  (foreground)");
    process.exit(1);
  }

  mkdirSync(SYSTEMD_DIR, { recursive: true });
  copyFileSync(SERVICE_SRC, SERVICE_DST);
  copyFileSync(PATH_SRC, PATH_DST);

  console.error("crosstalk-watch: installed systemd units to", SYSTEMD_DIR);

  try {
    execSync("systemctl --user daemon-reload", { stdio: "inherit" });
    execSync("systemctl --user enable --now crosstalk-notify.path", { stdio: "inherit" });
    console.error("crosstalk-watch: enabled and started crosstalk-notify.path");
    console.error("");
    console.error("Commands:");
    console.error("  systemctl --user status crosstalk-notify.path   # check watcher");
    console.error("  systemctl --user stop crosstalk-notify.path      # pause");
    console.error("  journalctl --user -u crosstalk-notify.service -f # follow logs");
  } catch (e) {
    console.error("crosstalk-watch: systemd setup failed:", e.message);
    console.error("  Try running manually: crosstalk-watch (foreground)");
  }
}

function showHelp() {
  console.error("Usage: crosstalk-watch [--init | --install]");
  console.error("");
  console.error("  (no args)    Run the inbox watcher in the foreground");
  console.error("  --init       Copy smtp.conf.template to ~/.crosstalk/watcher/");
  console.error("  --install    Install systemd user service for the watcher");
  console.error("");
}

// ── Main ─────────────────────────────────────────────────────────────────────

const arg = process.argv[2];

if (arg === "--init" || arg === "init") {
  cmdInit();
} else if (arg === "--install" || arg === "install" || arg === "-i") {
  cmdInstall();
} else if (arg === "--help" || arg === "-h" || arg === "help") {
  showHelp();
} else {
  startWatcher();
}
