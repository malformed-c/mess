#!/usr/bin/env bun
// End-to-end test for the mess channel: spawn the channel server as Claude Code
// would (stdio MCP), complete the initialize handshake as identity "testing-mess",
// then send that agent a mess message and assert the server pushes it back as a
// `notifications/claude/channel` event. Uses the real mess daemon; cleans up after.
import { spawn } from "node:child_process";

const AGENT = "testing-mess";
const BODY = "hello via channel " + Math.floor(performance.now());

// A clean session env: no host session-id vars (those would resolve to the
// tester's own identity and outrank MESS_AGENT), just MESS_AGENT=testing-mess.
const env: Record<string, string> = { ...process.env } as Record<string, string>;
for (const k of ["CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "MESS_SESSION_ID"]) delete env[k];
env.MESS_AGENT = AGENT;

const srv = spawn("bun", [import.meta.dir + "/mess-channel.ts"], {
  stdio: ["pipe", "pipe", "inherit"],
  env,
});

const send = (msg: object) => srv.stdin.write(JSON.stringify(msg) + "\n");

let buf = "";
let got = false;
const deadline = setTimeout(() => finish(false, "timed out waiting for channel event"), 8000);

srv.stdout.setEncoding("utf8");
srv.stdout.on("data", (chunk: string) => {
  buf += chunk;
  for (let nl; (nl = buf.indexOf("\n")) >= 0; ) {
    const line = buf.slice(0, nl).trim();
    buf = buf.slice(nl + 1);
    if (!line) continue;
    let msg: any;
    try {
      msg = JSON.parse(line);
    } catch {
      continue;
    }
    if (msg.id === 1 && msg.result) {
      // initialize acknowledged -> complete handshake, then trigger a peer message
      send({ jsonrpc: "2.0", method: "notifications/initialized" });
      setTimeout(fire, 300);
    }
    if (msg.method === "notifications/claude/channel") {
      const p = msg.params ?? {};
      const okBody = p.content === BODY;
      const okFrom = p.meta?.from === "tester";
      const okKind = p.meta?.kind === "direct";
      console.log("← channel event:", JSON.stringify(p));
      finish(okBody && okFrom && okKind, okBody && okFrom && okKind ? "" : "event fields mismatch");
    }
  }
});

function fire() {
  // A peer sends the agent a direct message via the real daemon.
  const p = spawn("mess", ["send", "--as", "tester", AGENT, BODY], { stdio: "inherit" });
  p.on("exit", (code) => {
    if (code !== 0) finish(false, "mess send failed");
  });
}

function finish(pass: boolean, why: string) {
  if (got) return;
  got = true;
  clearTimeout(deadline);
  try {
    srv.kill();
  } catch {}
  spawn("mess", ["rm", AGENT], { stdio: "ignore" });
  spawn("mess", ["rm", "tester"], { stdio: "ignore" });
  console.log(pass ? "PASS: channel delivered the peer message" : "FAIL: " + why);
  setTimeout(() => process.exit(pass ? 0 : 1), 200);
}

// Kick off the MCP handshake.
send({
  jsonrpc: "2.0",
  id: 1,
  method: "initialize",
  params: {
    protocolVersion: "2024-11-05",
    capabilities: {},
    clientInfo: { name: "e2e", version: "0" },
  },
});
