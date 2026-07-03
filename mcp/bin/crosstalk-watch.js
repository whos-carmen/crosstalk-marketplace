#!/usr/bin/env node
// crosstalk-watch CLI — email notification for new crosstalk inbox messages.
//
// Usage:
//   crosstalk-watch                        # run in foreground (Ctrl+C to stop)
//   crosstalk-watch --daemon               # fork to background (runs until stopped)
//   crosstalk-watch --daemon --stop        # stop the background daemon
//   crosstalk-watch --daemon --status      # check if daemon is running
//   crosstalk-watch --init                 # copy smtp.conf.template and print guide

import { readFileSync, existsSync, copyFileSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  startWatcher,
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
  console.error("2. Run:  crosstalk-watch              # test in foreground");
  console.error("3. Then: crosstalk-watch --daemon     # fork to background");
  console.error("");
}

function showHelp() {
  console.error("Usage: crosstalk-watch [command]");
  console.error("");
  console.error("  (no args)     Run the inbox watcher in the foreground");
  console.error("  --daemon       Fork to background (daemon mode)");
  console.error("  --daemon --stop   Stop the background daemon");
  console.error("  --daemon --status Check if daemon is running");
  console.error("  --init         Copy smtp.conf.template to ~/.crosstalk/watcher/");
  console.error("  --help         Show this message");
  console.error("");
}

// ── Main ─────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

// This flag is set by the parent process when daemonizing — the child
// picks it up and just runs the watcher without any CLI dispatch.
if (args[0] === "--daemon-child") {
  startWatcher();
  // Don't call process.exit() here — startWatcher sets up the event loop
  // (fs.watch, keep-alive interval) and the process must stay alive.
}

if (args[0] === "--daemon" || args[0] === "daemon") {
  const sub = args[1];
  if (sub === "--stop" || sub === "stop") {
    daemonStop();
  } else if (sub === "--status" || sub === "status") {
    daemonStatus();
  } else {
    daemonize();
  }
} else if (args[0] === "--init" || args[0] === "init") {
  cmdInit();
} else if (args[0] === "--help" || args[0] === "-h" || args[0] === "help") {
  showHelp();
} else {
  startWatcher();
}
