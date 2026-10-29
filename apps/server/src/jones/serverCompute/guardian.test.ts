// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off -- Synthetic Node process, pipe and clock fixtures exercise the actual ownership boundary.
import * as NodeChildProcess from "node:child_process";
import type * as NodeStream from "node:stream";
import * as NodeEvents from "node:events";
import * as NodeVM from "node:vm";
import { afterEach, expect, it } from "vite-plus/test";
import { guardianSource } from "./guardian.ts";
import { controls, writeControl, type Bootstrap } from "./protocol.ts";
const owned: NodeChildProcess.ChildProcess[] = [];
afterEach(async () => {
  for (const child of owned.splice(0))
    if (child.exitCode === null && child.signalCode === null) {
      const exited = NodeEvents.once(child, "exit");
      child.kill("SIGKILL");
      await exited;
    }
});
const boot: Bootstrap = {
  version: 1,
  build: "a".repeat(64),
  authorityId: "fixture",
  slotId: 0,
  slotGeneration: 1,
  nonce: "fixture-nonce",
};
const blocked = `const { Worker } = require('node:worker_threads');
const boot = JSON.parse(process.argv[1]);
new Worker(process.argv[2], { eval: true, workerData: boot });
Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);`;
const start = () => {
  const child = NodeChildProcess.spawn(
    process.execPath,
    ["-e", blocked, JSON.stringify(boot), guardianSource],
    {
      stdio: ["ignore", "ignore", "pipe", "pipe"],
      env: {},
    },
  );
  owned.push(child);
  const pipe = child.stdio[3] as NodeStream.Duplex;
  return { child, pipe, messages: controls(pipe)[Symbol.asyncIterator]() };
};

it("observes lifetime EOF independently while the child main thread is synchronously blocked", async () => {
  const { child, pipe, messages } = start();
  expect((await messages.next()).value).toMatchObject({ type: "ready", pid: child.pid });
  const exited = NodeEvents.once(child, "exit");
  pipe.destroy();
  expect((await exited)[1]).toBe("SIGKILL");
});

it("arms a hard deadline on the independent guardian and self-kills only its own blocked process", async () => {
  const { child, pipe, messages } = start();
  expect((await messages.next()).value?.type).toBe("ready");
  await writeControl(pipe, { ...boot, type: "go" });
  expect((await messages.next()).value?.type).toBe("go");
  await writeControl(pipe, { ...boot, type: "loaded" });
  expect((await messages.next()).value?.type).toBe("loaded");
  const exited = NodeEvents.once(child, "exit");
  // The blocked OS child uses its own real clock; a parent fake clock cannot drive this hard deadline.
  // oxlint-disable-next-line t3code/no-wall-clock-in-tests -- This is an absolute cross-process deadline, not an elapsed-time assertion.
  await writeControl(pipe, { ...boot, type: "arm", requestId: 1, deadline: Date.now() + 100 });
  expect((await messages.next()).value?.type).toBe("armed");
  expect((await exited)[1]).toBe("SIGKILL");
});

it("fails closed on a malformed lifetime command without accepting a PID/signal target", async () => {
  const { child, pipe, messages } = start();
  await messages.next();
  const exited = NodeEvents.once(child, "exit");
  await writeControl(pipe, { ...boot, type: "terminate", pid: process.pid, signal: "SIGTERM" });
  expect((await exited)[1]).toBe("SIGKILL");
});

it("survives no missing guardian FD route: native work cannot outlive failed guardian initialization", async () => {
  // The fixture main mirrors the production error handler, while remaining blocked in synchronous work.
  // Failed initialization self-kills in the independent thread before main-loop callbacks are needed.
  const child = NodeChildProcess.spawn(
    process.execPath,
    ["-e", blocked, JSON.stringify(boot), guardianSource],
    {
      stdio: ["ignore", "ignore", "ignore"],
      env: {},
    },
  );
  owned.push(child);
  expect((await NodeEvents.once(child, "exit"))[1]).toBe("SIGKILL");
});

/** Only the OS pipe, process and clock are synthetic; execute the actual embedded guardian state machine. */
function clockedGuardian() {
  const kills: Array<{ pid: number; signal: string }> = [],
    messages: Array<Record<string, unknown>> = [];
  const outbound: Array<Record<string, unknown>> = [];
  class Pipe extends NodeEvents.EventEmitter {
    write(line: string, done: (error?: Error) => void) {
      messages.push(JSON.parse(line));
      done();
      return true;
    }
  }
  class ParentPort extends NodeEvents.EventEmitter {
    postMessage(value: Record<string, unknown>): void {
      outbound.push(value);
    }
  }
  const pipe = new Pipe(),
    parentPort = new ParentPort();
  const timeouts = new Map<number, { at: number; run: () => void }>(),
    intervals: Array<() => void> = [];
  let now = 0,
    rss = 0,
    id = 0;
  NodeVM.runInNewContext(guardianSource, {
    require: (name: string) => {
      if (name === "node:worker_threads") return { workerData: boot, parentPort };
      if (name === "node:net")
        return {
          Socket: function () {
            return pipe;
          },
        };
      throw new Error("Unexpected guardian dependency");
    },
    Buffer,
    performance: { now: () => now },
    Date: { now: () => now },
    process: {
      pid: 1234,
      on: () => {},
      memoryUsage: { rss: () => rss },
      kill: (pid: number, signal: string) => {
        kills.push({ pid, signal });
      },
    },
    setTimeout: (run: () => void, ms: number) => {
      const token = ++id;
      timeouts.set(token, { at: now + ms, run });
      return token;
    },
    clearTimeout: (token: number) => {
      timeouts.delete(token);
    },
    setInterval: (run: () => void) => {
      intervals.push(run);
    },
  });
  const command = (value: Record<string, unknown>) =>
    pipe.emit("data", Buffer.from(`${JSON.stringify({ ...boot, ...value })}\n`));
  return {
    kills,
    messages,
    outbound,
    pipe,
    command,
    setRss: (value: number) => {
      rss = value;
    },
    advance: (ms: number) => {
      now += ms;
      for (const [token, timeout] of timeouts)
        if (now >= timeout.at) {
          timeouts.delete(token);
          timeout.run();
        }
      for (const run of intervals) run();
    },
  };
}

it("has no inferred background-tail deadline while foreground deadlines remain independent", () => {
  const f = clockedGuardian();
  f.command({ type: "go" });
  f.command({ type: "loaded" });
  expect(f.outbound).toEqual([{ type: "go" }]);
  f.advance(60_001);
  expect(f.kills).toEqual([]);
  f.command({ type: "arm", requestId: 1, deadline: 60_101 });
  expect(f.messages.at(-1)).toMatchObject({ type: "armed", requestId: 1 });
  f.advance(101);
  expect(f.kills).toEqual([{ pid: 1234, signal: "SIGKILL" }]);
});

it.each(["scan", "scanDone"])(
  "rejects removed %s control instead of retaining a private scan permit",
  (type) => {
    const f = clockedGuardian();
    f.command({ type: "go" });
    f.command({ type: "loaded" });
    f.command({ type, keyId: "key", ...(type === "scan" ? { deadline: 60_000 } : {}) });
    expect(f.kills).toEqual([{ pid: 1234, signal: "SIGKILL" }]);
  },
);

it("retains startup, FD3 EOF and consecutive RSS containment independently of native readiness", () => {
  const startup = clockedGuardian();
  startup.advance(10_001);
  expect(startup.kills).toEqual([{ pid: 1234, signal: "SIGKILL" }]);
  const eof = clockedGuardian();
  eof.command({ type: "go" });
  eof.command({ type: "loaded" });
  eof.pipe.emit("end");
  expect(eof.kills).toEqual([{ pid: 1234, signal: "SIGKILL" }]);
  const rss = clockedGuardian();
  rss.command({ type: "go" });
  rss.command({ type: "loaded" });
  rss.setRss(513 * 1024 * 1024);
  rss.advance(1000);
  expect(rss.kills).toEqual([]);
  rss.advance(1000);
  expect(rss.messages).toContainEqual({ ...boot, type: "overBudget" });
  expect(rss.kills).toEqual([{ pid: 1234, signal: "SIGKILL" }]);
});

it("reports exact RSS bytes on FD3 independently of foreground work and preserves the consecutive-sample policy", () => {
  const f = clockedGuardian();
  f.command({ type: "go" });
  f.command({ type: "loaded" });
  f.command({ type: "arm", requestId: 1, deadline: 30_000 });
  f.command({ type: "credit", requestId: 1 });
  f.command({ type: "cancel", requestId: 1 });
  expect(f.outbound).toEqual([
    { type: "go" },
    { type: "credit", requestId: 1 },
    { type: "cancel", requestId: 1 },
  ]);
  f.setRss(123_456_789);
  f.advance(1000);
  expect(f.messages.at(-1)).toEqual({ ...boot, type: "resources", rss: 123_456_789 });
  f.setRss(513 * 1024 * 1024);
  f.advance(1000);
  f.setRss(512 * 1024 * 1024);
  f.advance(1000);
  f.setRss(513 * 1024 * 1024);
  f.advance(1000);
  expect(f.kills).toEqual([]);
  // Resource observation neither clears nor extends the independently armed foreground deadline.
  f.advance(26_001);
  expect(f.kills).toContainEqual({ pid: 1234, signal: "SIGKILL" });
});
