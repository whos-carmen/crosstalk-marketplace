#!/usr/bin/env node
// crosstalk-watch CLI — email notification for new crosstalk inbox messages.
//
// Usage:
//   crosstalk-watch                        # fork to background (daemon mode)
//   crosstalk-watch --stop                 # stop the background daemon
//   crosstalk-watch --status               # check if daemon is running
//   crosstalk-watch --init                 # copy smtp.conf.template and print guide
//   crosstalk-watch --test                 # check inbox once + send email (for testing)

import { readFileSync, existsSync, copyFileSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  startWatcher,
  testOnce,
  daemonize,
  daemonStop,
  daemonStatus,
} from "../src/watch.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PACKAGE_ROOT = join(__dirname, "..");
const CROSSTALK_DIR = join(homedir(), ".crosstalk");
const WATCHER_DIR = join(CROSSTALK_DIR, "watcher");
const CONFIG_PATH = join(WATCHER_DIR, "smtp.conf");
const CONFIG_TEMPLATE = join(PACKAGE_ROOT, "smtp.conf.template");

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
  console.error("1. Edit the file and fill in your SMTP settings.");
  console.error("2. Run:  crosstalk-watch --test   # test email delivery");
  console.error("3. Run:  crosstalk-watch           # start daemon");
  console.error("");
}

function showHelp() {
  console.error("Usage: crosstalk-watch [command]");
  console.error("");
  console.error("  (no args)     Start the watcher as a background daemon");
  console.error("  --stop         Stop the background daemon");
  console.error("  --status       Check if the daemon is running");
  console.error("  --init         Copy smtp.conf.template to ~/.crosstalk/watcher/");
  console.error("  --test         Check inbox once and send email (test config)");
  console.error("  --help         Show this message");
  console.error("");
}

// ── Main ─────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

// This flag is set by the parent process when daemonizing — the child
// picks it up and just runs the watcher without any CLI dispatch.
if (args[0] === "--daemon-child") {
  startWatcher();
  // Falls through to nothing — startWatcher owns the event loop
} else if (args[0] === "--stop" || args[0] === "stop") {
  daemonStop();
} else if (args[0] === "--status" || args[0] === "status") {
  daemonStatus();
} else if (args[0] === "--init" || args[0] === "init") {
  cmdInit();
} else if (args[0] === "--test" || args[0] === "test") {
  testOnce();
} else if (args[0] === "--help" || args[0] === "-h" || args[0] === "help") {
  showHelp();
} else {
  // Default: daemon mode
  daemonize();
}
