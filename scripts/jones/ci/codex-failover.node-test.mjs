import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";
import * as NodeTest from "node:test";

const peerPath = NodeURL.fileURLToPath(new URL("../e2e/codex-failover/peer.mjs", import.meta.url));
async function fixture(run) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "synthetic-codex-failover-"));
  const children = [];
  const start = (label, ...args) => {
    const child = NodeChildProcess.spawn(
      process.execPath,
      [peerPath, "--state-dir", root, "--account", label, ...args, "app-server"],
      { stdio: ["pipe", "pipe", "pipe"] },
    );
    children.push(child);
    const frames = [];
    let wake;
    let stderr = "";
    let nextId = 0;
    child.stderr.setEncoding("utf8").on("data", (text) => {
      stderr += text;
    });
    const closed = NodeEvents.once(child, "close");
    NodeReadline.createInterface({ input: child.stdout }).on("line", (line) => {
      frames.push(JSON.parse(line));
      wake?.();
    });
    const wait = async (predicate) => {
      const timeout = AbortSignal.timeout(3000);
      while (!frames.some(predicate)) {
        await new Promise((resolve, reject) => {
          const abort = () => {
            wake = undefined;
            reject(new Error(`Synthetic frame deadline: ${stderr}`));
          };
          wake = () => {
            timeout.removeEventListener("abort", abort);
            wake = undefined;
            resolve();
          };
          timeout.addEventListener("abort", abort, { once: true });
          if (timeout.aborted) abort();
        });
      }
      return frames.find(predicate);
    };
    const request = async (method, params = {}) => {
      const id = ++nextId;
      child.stdin.write(JSON.stringify({ id, method, params }) + "\n");
      const frame = await wait((entry) => entry.id === id);
      NodeAssert.equal(frame.error, undefined, JSON.stringify(frame.error));
      return frame.result;
    };
    return {
      request,
      wait,
      frames,
      async stop() {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
        await closed;
        NodeAssert.equal(stderr, "");
      },
    };
  };
  try {
    await run({ root, start });
  } finally {
    for (const child of children) {
      if (child.exitCode === null && child.signalCode === null) {
        const closed = NodeEvents.once(child, "close");
        child.kill("SIGKILL");
        await closed;
      }
    }
    await NodeFSP.rm(root, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.stat(root), { code: "ENOENT" });
  }
}
const startTurn = (peer, threadId, marker = "SYNTHETIC_FIRST") =>
  peer.request("turn/start", {
    threadId,
    input: [{ type: "text", text: marker }],
    model: "synthetic-codex-model",
    effort: "medium",
    serviceTier: "priority",
    config: { mcp_servers: { secret: "NEVER_PERSIST_THIS" } },
  });
const terminal = (peer, turnId) =>
  peer.wait((entry) => entry.method === "turn/completed" && entry.params.turn.id === turnId);

NodeTest.test(
  "A/B acknowledge then fail; restarted C resumes the same native history and accepts a fresh message",
  async () => {
    await fixture(async ({ root, start }) => {
      let threadId;
      const cwd = NodePath.join(root, "execution-workspace");
      for (const label of ["A", "B", "C"]) {
        const peer = start(label);
        await peer.request("initialize");
        NodeAssert.match(
          (await peer.request("account/read")).account.email,
          new RegExp(`synthetic-${label.toLowerCase()}@`),
        );
        NodeAssert.equal((await peer.request("model/list")).data[0].model, "synthetic-codex-model");
        const result = await peer.request(
          threadId ? "thread/resume" : "thread/start",
          threadId ? { threadId } : { cwd },
        );
        threadId ??= result.thread.id;
        NodeAssert.equal(result.thread.id, threadId);
        NodeAssert.equal(result.thread.turns.length, label.charCodeAt(0) - 65);
        const { turn } = await startTurn(
          peer,
          threadId,
          label === "A" ? "SYNTHETIC_FIRST" : "Continue where you left off.",
        );
        NodeAssert.equal(turn.status, "inProgress");
        const done = await terminal(peer, turn.id);
        const ackIndex = peer.frames.findIndex((frame) => frame.result?.turn?.id === turn.id);
        NodeAssert.ok(ackIndex < peer.frames.indexOf(done));
        NodeAssert.equal(done.params.turn.status, label === "C" ? "completed" : "failed");
        if (label !== "C") {
          NodeAssert.deepEqual(
            done.params.turn.error.codexErrorInfo,
            label === "A"
              ? "usageLimitExceeded"
              : { responseStreamDisconnected: { httpStatusCode: 503 } },
          );
          NodeAssert.equal(
            peer.frames.find((frame) => frame.method === "error").params.willRetry,
            false,
          );
        } else {
          const fresh = await startTurn(peer, threadId, "SYNTHETIC_FRESH_ON_C");
          await terminal(peer, fresh.turn.id);
          const history = (await peer.request("thread/read", { threadId, includeTurns: true }))
            .thread.turns;
          NodeAssert.equal(history.length, 4);
          NodeAssert.equal(history[3].items[1].text, "SYNTHETIC_C_COMPLETED");
        }
        await peer.stop();
      }
      const names = await NodeFSP.readdir(root);
      const contents = await Promise.all(
        names.map((name) => NodeFSP.readFile(NodePath.join(root, name), "utf8")),
      );
      NodeAssert.ok(
        contents.every(
          (text) => !text.includes("NEVER_PERSIST_THIS") && !text.includes("mcp_servers"),
        ),
      );
      const traces = contents
        .filter((_, i) => names[i].endsWith(".ndjson"))
        .flatMap((text) => text.trim().split("\n").map(JSON.parse));
      for (const instance of ["B", "C"]) {
        NodeAssert.ok(
          traces.some(
            (entry) =>
              entry.instance === instance &&
              entry.method === "turn/start" &&
              entry.inputMarker === "SYNTHETIC_CONTINUATION",
          ),
        );
      }
      NodeAssert.ok(
        traces.some(
          (entry) =>
            entry.instance === "C" &&
            entry.nativeThreadId === threadId &&
            entry.inputMarker === "SYNTHETIC_FRESH_ON_C" &&
            entry.serviceTier === "priority",
        ),
      );
    });
  },
);

NodeTest.test("all-unavailable gives a terminal failure across all five accounts", async () => {
  await fixture(async ({ start }) => {
    for (const label of ["A", "B", "C", "D", "E"]) {
      const peer = start(label, "--mode", "all-unavailable");
      const { thread } = await peer.request("thread/start");
      const { turn } = await startTurn(peer, thread.id);
      NodeAssert.equal((await terminal(peer, turn.id)).params.turn.status, "failed");
      await peer.stop();
    }
  });
});

NodeTest.test(
  "a held turn can be interrupted, then a fresh held turn released without a late completion",
  async () => {
    await fixture(async ({ root, start }) => {
      const peer = start("C", "--hold");
      const { thread } = await peer.request("thread/start");
      const { turn } = await startTurn(peer, thread.id);
      NodeAssert.equal(
        peer.frames.some((frame) => frame.method === "turn/completed"),
        false,
      );
      const held = await peer.request("thread/read", { threadId: thread.id, includeTurns: true });
      NodeAssert.deepEqual(held.thread.status, { type: "active", activeFlags: [] });
      await peer.request("turn/interrupt", { threadId: thread.id, turnId: turn.id });
      NodeAssert.equal((await terminal(peer, turn.id)).params.turn.status, "interrupted");
      const next = await startTurn(peer, thread.id, "SYNTHETIC_AFTER_CANCEL");
      await NodeFSP.writeFile(
        NodePath.join(root, `release-${next.turn.id}`),
        "SYNTHETIC_RELEASE\n",
        {
          flag: "wx",
        },
      );
      NodeAssert.equal((await terminal(peer, next.turn.id)).params.turn.status, "completed");
      NodeAssert.equal(
        peer.frames.filter(
          (frame) => frame.method === "turn/completed" && frame.params.turn.id === turn.id,
        ).length,
        1,
      );
      await peer.stop();
    });
  },
);
