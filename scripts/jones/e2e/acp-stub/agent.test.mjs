import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import * as NodeTimersPromises from "node:timers/promises";
import { parseOptions } from "./agent.mjs";

const agent = NodeURL.fileURLToPath(new URL("./agent.mjs", import.meta.url));
async function peer(t, timeoutMs = 5000) {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "je06-unit-")),
  );
  const state = NodePath.join(root, "owned");
  const sentinel = NodePath.join(root, "unrelated");
  const frames = [];
  let child, sentinelProcess, exit, sentinelExit;
  let output = "",
    stderr = "",
    closed = false,
    setupComplete = false;
  t.after(async () => {
    if (child && !closed) child.kill("SIGTERM");
    const deadline = setTimeout(() => {
      if (child && !closed) child.kill("SIGKILL");
    }, 1000);
    try {
      if (exit) await exit;
      if (setupComplete) {
        NodeAssert.equal(await NodeFSP.readFile(sentinel, "utf8"), "unchanged");
        NodeAssert.equal(stderr, "");
        NodeAssert.equal(
          sentinelProcess.exitCode,
          null,
          "Unrelated owned sentinel process survived peer cleanup",
        );
        process.kill(sentinelProcess.pid, 0);
      }
    } finally {
      clearTimeout(deadline);
      if (sentinelProcess) sentinelProcess.kill("SIGTERM");
      if (sentinelExit) await sentinelExit;
      await NodeFSP.rm(root, { recursive: true, force: true });
      await NodeAssert.rejects(NodeFSP.access(root), { code: "ENOENT" });
    }
  });
  await NodeFSP.mkdir(state, { mode: 0o700 });
  await NodeFSP.writeFile(sentinel, "unchanged");
  sentinelProcess = NodeChildProcess.spawn(
    process.execPath,
    ["-e", "setInterval(() => {}, 1000)"],
    { env: { HOME: root, TMPDIR: root }, stdio: "ignore" },
  );
  sentinelExit = new Promise((resolve, reject) => {
    sentinelProcess.once("close", resolve);
    sentinelProcess.once("error", reject);
  });
  child = NodeChildProcess.spawn(
    process.execPath,
    [agent, "--state-dir", state, "--run-id", "unit", "--timeout-ms", String(timeoutMs)],
    {
      env: { PATH: NodePath.dirname(process.execPath), HOME: root, TMPDIR: root },
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  exit = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => {
      closed = true;
      resolve({ code, signal });
    });
  });
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (data) => {
    output += data;
    let pos;
    while ((pos = output.indexOf("\n")) !== -1) {
      frames.push(JSON.parse(output.slice(0, pos)));
      output = output.slice(pos + 1);
    }
  });
  child.stderr.on("data", (data) => {
    stderr += data;
  });
  setupComplete = true;
  async function until(predicate) {
    const end = Date.now() + 2000;
    while (Date.now() < end) {
      const match = frames.find(predicate);
      if (match) return match;
      if (closed) throw new Error("Peer closed before expected frame");
      await NodeTimersPromises.setTimeout(5);
    }
    throw new Error("Peer response timeout");
  }
  let next = 0;
  function send(method, params, id) {
    child.stdin.write(
      `${JSON.stringify({ jsonrpc: "2.0", ...(id === undefined ? {} : { id }), method, params })}\n`,
    );
  }
  async function rpc(method, params = {}, id = ++next) {
    send(method, params, id);
    return (await until((frame) => frame.id === id)).result;
  }
  async function ledger() {
    const files = (await NodeFSP.readdir(state)).filter((name) => name.startsWith("ledger-"));
    return (
      await Promise.all(files.map((name) => NodeFSP.readFile(NodePath.join(state, name), "utf8")))
    ).flatMap((text) => text.trim().split("\n").filter(Boolean).map(JSON.parse));
  }
  await rpc("initialize", { protocolVersion: 2 }, "initialize");
  const { sessionId } = await rpc("session/new", { cwd: root, mcpServers: [] });
  return { child, exit, frames, rpc, send, until, ledger, state, sessionId };
}

NodeTest.test(
  "v2 acceptance stays held, file release completes once, resume replays without delivery",
  async (t) => {
    const p = await peer(t);
    const processStat = await NodeFSP.readFile(`/proc/${p.child.pid}/stat`, "utf8").catch(
      (error) => {
        if (error.code === "ENOENT") return null;
        throw error;
      },
    );
    const start = (await p.ledger()).find((entry) => entry.event === "start");
    NodeAssert.equal(start.pid, p.child.pid);
    NodeAssert.equal(
      start.startTicks,
      processStat === null
        ? null
        : processStat.slice(processStat.lastIndexOf(")") + 2).split(" ")[19],
    );
    NodeAssert.deepEqual(
      await p.rpc("session/prompt", {
        sessionId: p.sessionId,
        prompt: [{ type: "text", text: "JE06-unit-M1" }],
      }),
      {},
    );
    await p.until((f) => f.params?.update?.state === "running");
    NodeAssert.equal(
      p.frames.some((f) => f.params?.update?.state === "idle"),
      false,
    );
    NodeAssert.equal((await p.ledger()).filter((e) => e.event === "delivery").length, 1);
    await NodeFSP.writeFile(NodePath.join(p.state, "release"), "unit", { flag: "wx" });
    await p.until((f) => f.params?.update?.stopReason === "end_turn");
    const before = p.frames.find((f) => f.params?.update?.sessionUpdate === "agent_message_chunk");
    await p.rpc("session/resume", { sessionId: p.sessionId, replayFrom: { type: "start" } });
    const after = p.frames.filter((f) => f.params?.update?.sessionUpdate === "agent_message_chunk");
    NodeAssert.equal(after.length, 2);
    NodeAssert.equal(after[1].params.update.messageId, before.params.update.messageId);
    NodeAssert.equal((await p.ledger()).filter((e) => e.event === "delivery").length, 1);
    NodeAssert.equal((await p.rpc("session/list")).sessions.length, 1);
    await p.rpc("session/close", { sessionId: p.sessionId });
    p.child.stdin.end();
    NodeAssert.equal((await p.exit).code, 0);
  },
);

NodeTest.test("duplicate prompt deliveries are counted, never hidden by the stub", async (t) => {
  const p = await peer(t);
  const prompt = {
    sessionId: p.sessionId,
    prompt: [{ type: "text", text: "JE06-unit-M2-edited" }],
  };
  await p.rpc("session/prompt", prompt);
  await p.rpc("session/prompt", prompt);
  const entries = (await p.ledger()).filter((e) => e.event === "delivery");
  NodeAssert.deepEqual(
    entries.map((e) => e.ordinal),
    [1, 2],
  );
});

NodeTest.test(
  "cancel notification remains responsive while M1 is held and completes only once",
  async (t) => {
    const p = await peer(t);
    await p.rpc("session/prompt", {
      sessionId: p.sessionId,
      prompt: [{ type: "text", text: "JE06-unit-M1" }],
    });
    p.send("session/cancel", { sessionId: p.sessionId });
    await p.until((f) => f.params?.update?.stopReason === "cancelled");
    p.send("session/cancel", { sessionId: p.sessionId });
    await p.rpc("session/list");
    NodeAssert.equal((await p.ledger()).filter((e) => e.event === "complete").length, 1);
    NodeAssert.equal(
      p.frames.some((f) => f.params?.update?.sessionUpdate === "agent_message_chunk"),
      false,
    );
  },
);

for (const kind of ["eof", "malformed", "timeout", "signal"]) {
  NodeTest.test(`owned peer ${kind} closes without touching a sibling sentinel`, async (t) => {
    const p = await peer(t, kind === "timeout" ? 500 : 5000);
    await p.rpc("session/prompt", {
      sessionId: p.sessionId,
      prompt: [{ type: "text", text: "JE06-unit-M1" }],
    });
    if (kind === "eof") p.child.stdin.end();
    if (kind === "malformed") p.child.stdin.write("{broken}\n");
    if (kind === "signal") p.child.kill("SIGTERM");
    NodeAssert.equal((await p.exit).code, ["malformed", "timeout"].includes(kind) ? 1 : 0);
    NodeAssert.equal((await p.ledger()).filter((e) => e.event === "close").length, 1);
  });
}
NodeTest.test("scope parsing refuses unknown arguments and path traversal run IDs", () => {
  NodeAssert.throws(() => parseOptions(["--extra", "yes"]), /arguments/);
  NodeAssert.throws(() => parseOptions(["--state-dir", "/tmp", "--run-id", "../escape"]), /scope/);
});
