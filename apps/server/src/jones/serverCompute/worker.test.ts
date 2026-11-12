// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Real response encoding and credits use bounded synthetic pipes, without a native finder.
import * as NodeStream from "node:stream";
import * as NodeOS from "node:os";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeProcess from "node:process";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import { expect, it } from "vite-plus/test";
import {
  decodeReply,
  frames,
  LIMITS,
  type Bootstrap,
  type Request,
  type Operation,
  type Reply,
} from "./protocol.ts";
import {
  WorkerAllocations,
  WorkerReplies,
  decodeNativeBlockQualification,
  configureComputeSelfPriority,
} from "./worker.ts";
import { minimalWorkerEnvironment } from "./workerCommand.ts";

const boot: Bootstrap = {
  version: 1,
  build: "a".repeat(64),
  authorityId: "authority",
  slotId: 0,
  slotGeneration: 1,
  nonce: "nonce",
};
const request: Request = {
  ...boot,
  requestId: 1,
  operation: "searchContents",
  input: { query: "needle", limit: 3, caseSensitive: false, wholeWord: false, useRegex: false },
  token: {
    authorityId: boot.authorityId,
    slotId: 0,
    slotGeneration: 1,
    keyId: "key",
    incarnation: 1,
  },
};

it.each([
  "open",
  "list",
  "search",
  "searchContents",
  "refresh",
  "refreshContent",
  "close",
] as const)(
  "brackets %s execution separately from chunking and preserves failure/cancellation settlement",
  async (operation: Operation) => {
    for (const mode of ["ok", "failed", "cancelled"] as const) {
      const pipe = new NodeStream.PassThrough(),
        decoded: Reply[] = [];
      const controller = new AbortController();
      let now = 1_000_000,
        release!: () => void,
        settled = false;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const replies = new WorkerReplies(
        { ...request, operation, clock: { parentSentMs: 10 } },
        pipe,
        () => now,
      );
      const read = (async () => {
        for await (const frame of frames(pipe, LIMITS.batchBytes)) {
          decoded.push(decodeReply(frame, boot));
          replies.acknowledge();
        }
      })();
      const cause = { _tag: "WorkspaceSearchIndexScanTimedOut" };
      try {
        now += 3;
        const work = replies
          .execute(async () => {
            await gate;
            if (mode !== "ok") throw cause;
          }, controller.signal)
          .finally(() => {
            settled = true;
          });
        void work.catch(() => {});
        if (mode === "cancelled") controller.abort();
        await Promise.resolve();
        expect(settled).toBe(false);
        expect(decoded).toEqual([]);
        now += 20;
        release();
        let failure: unknown;
        try {
          await work;
        } catch (error) {
          failure = error;
        }
        now += 5;
        if (failure) await replies.failure(failure, controller.signal);
        else {
          const output = ["list", "search"].includes(operation)
            ? { entries: [{ path: "a", kind: "file" as const }], truncated: false }
            : operation === "searchContents"
              ? { matches: [], truncated: false }
              : undefined;
          await replies.result(
            output,
            operation === "close" ? undefined : { scanning: false, warmupComplete: true },
            controller.signal,
          );
        }
        await replies.settle();
        pipe.end();
        await read;
        const terminal = decoded.at(-1)!;
        expect(terminal.timing).toEqual({
          parentSentMs: 10,
          receivedMs: 1_000_000,
          executionStartedMs: 1_000_003,
          executionCompletedMs: 1_000_023,
          executionOutcome: mode,
          replyStartedMs: 1_000_028,
          handoffMs: 1_000_028,
        });
        expect(terminal.sequence).toBe(decoded.length - 1);
        if (mode !== "ok")
          expect(terminal.body).toMatchObject({
            type: "failure",
            scanTimedOut: true,
            reason:
              mode === "cancelled"
                ? "cancelled"
                : operation === "open"
                  ? "finder-create"
                  : operation === "refresh" || operation === "refreshContent"
                    ? "finder-refresh"
                    : "finder-search",
          });
      } finally {
        release();
        pipe.destroy();
        await read.catch(() => {});
      }
    }
  },
);

it("rejects an unwritable frame before waiting on exhausted credits and leaves no sequence hole", async () => {
  const pipe = new NodeStream.PassThrough(),
    decoded: unknown[] = [];
  const read = (async () => {
    for await (const frame of frames(pipe, LIMITS.batchBytes))
      decoded.push(decodeReply(frame, boot));
  })();
  try {
    const replies = new WorkerReplies(request, pipe);
    const item = { path: "a.ts", lineNumber: 1, lineContent: "needle", matchRanges: [] };
    await replies.send({ type: "chunk", items: [item] });
    await replies.send({ type: "chunk", items: [item] });
    // All two credits are outstanding; validation must reject without waiting for an impossible third ack.
    const failure = await replies
      .send({ type: "chunk", items: [{ ...item, lineContent: "x".repeat(LIMITS.batchBytes) }] })
      .then(
        () => {
          throw new Error("oversize frame was accepted");
        },
        (cause: unknown) => cause,
      );
    expect(failure).toMatchObject({ reason: "oversize" });
    replies.acknowledge();
    await replies.failure(failure, new AbortController().signal);
    replies.acknowledge();
    replies.acknowledge();
    await replies.settle();
    expect(() => replies.acknowledge()).toThrow();
    pipe.end();
    await read;
    expect(decoded).toMatchObject([
      { sequence: 0, body: { type: "chunk" } },
      { sequence: 1, body: { type: "chunk" } },
      { sequence: 2, body: { type: "failure", reason: "oversize" } },
    ]);
  } finally {
    pipe.destroy();
    await read.catch(() => {});
  }
});

it("keeps four generation allocation tombstones despite failed creates, close ACKs and lost resident mappings", () => {
  const allocations = new WorkerAllocations();
  const token = (id: number) => ({ ...request.token, keyId: `key-${id}`, incarnation: id + 1 });
  const key = (id: number) => ({ cwd: `/synthetic/${id}`, variant: "paths" as const });
  // The production worker reserves before Scope.make/create and never calls a refund on failure or close.
  allocations.reserve(token(0), key(0));
  expect(() => allocations.reserve({ ...token(0), incarnation: 9 }, key(0))).toThrow();
  expect(() => allocations.reserve(token(1), key(0))).toThrow();
  allocations.reserve(token(1), key(1));
  allocations.reserve(token(2), key(2));
  allocations.reserve(token(3), key(3));
  expect(() => allocations.reserve(token(4), key(4))).toThrow();
  // Only a new child generation owns a fresh allocation ledger.
  const replacement = new WorkerAllocations();
  expect(() => replacement.reserve({ ...token(0), slotGeneration: 2 }, key(0))).not.toThrow();
});

it("closes the qualification argument to a single path-shaped native log option", () => {
  const value = {
    targetWorkspace: "/synthetic/case/blocked-workspace",
    logFileHint: "/synthetic/case/barrier/native-block.log",
  };
  const argument = (input: unknown) => "--qualification-native-block=" + JSON.stringify(input);
  expect(decodeNativeBlockQualification(argument(value))).toEqual(value);
  expect(Object.isFrozen(decodeNativeBlockQualification(argument(value)))).toBe(true);
  for (const invalid of [
    null,
    [],
    {},
    { ...value, pid: 123 },
    { ...value, signal: "SIGKILL" },
    { ...value, logFileHint: "/elsewhere/native-block.log" },
    { ...value, targetWorkspace: "relative/blocked-workspace" },
    { ...value, targetWorkspace: "/synthetic/case/../blocked-workspace" },
    { ...value, logFileHint: value.logFileHint + "\0" },
    { ...value, logFileHint: "x".repeat(5000) },
  ])
    expect(() => decodeNativeBlockQualification(argument(invalid))).toThrow();
  expect(() => decodeNativeBlockQualification(JSON.stringify(value))).toThrow();
  expect(() => decodeNativeBlockQualification("--qualification-native-block={")).toThrow();
});

it.each([-20, 0, 10, 15, 19])("compute self-priority never raises initial nice %s", (initial) => {
  const writes: number[] = [];
  let actual = initial;
  const selected = configureComputeSelfPriority({
    read: () => actual,
    write: (value) => {
      writes.push(value);
      actual = value;
    },
  });
  expect(selected).toBe(Math.max(initial, NodeOS.constants.priority.PRIORITY_BELOW_NORMAL));
  expect(writes).toEqual(
    initial < NodeOS.constants.priority.PRIORITY_BELOW_NORMAL
      ? [NodeOS.constants.priority.PRIORITY_BELOW_NORMAL]
      : [],
  );
});
it.each([NaN, Infinity, -21, 20, 1.5])(
  "compute self-priority refuses invalid read %s before writing",
  (invalid) => {
    const writes: number[] = [];
    expect(() =>
      configureComputeSelfPriority({
        read: () => invalid,
        write: (value) => {
          writes.push(value);
        },
      }),
    ).toThrow(expect.objectContaining({ reason: "unavailable" }));
    expect(writes).toEqual([]);
  },
);
it.each(["first-read", "write", "readback", "wrong-readback"])(
  "compute self-priority fails closed on %s",
  (failure) => {
    const writes: number[] = [];
    let reads = 0;
    expect(() =>
      configureComputeSelfPriority({
        read: () => {
          reads++;
          if (failure === "first-read" || (failure === "readback" && reads === 2))
            throw new Error(failure);
          return reads === 1 ? 0 : failure === "wrong-readback" ? 0 : 10;
        },
        write: (value) => {
          writes.push(value);
          if (failure === "write") throw new Error(failure);
        },
      }),
    ).toThrow(expect.objectContaining({ reason: "unavailable" }));
    expect(writes).toEqual(failure === "first-read" ? [] : [10]);
  },
);

it.each(["args", "bootstrap", "build", "heap", "priority"])(
  "real private entry refuses %s before forbidden priority/guardian/native effects",
  async (mode) => {
    const directory = await NodeFSP.mkdtemp(
      NodePath.join(NodeOS.tmpdir(), "jones-compute-priority-guard-"),
    );
    const filename = NodePath.join(directory, "guard.mjs");
    const worker = NodeURL.pathToFileURL(NodePath.join(import.meta.dirname, "worker.ts")).href;
    const command = NodeURL.pathToFileURL(
      NodePath.join(import.meta.dirname, "workerCommand.ts"),
    ).href;
    const entry = NodePath.resolve(import.meta.dirname, "../../bin.ts");
    const source = [
      'import { createRequire, syncBuiltinESMExports, registerHooks } from "node:module";',
      'const require=createRequire(import.meta.url);const os=require("node:os"),threads=require("node:worker_threads");',
      'const trace=[];os.getPriority=pid=>{if(pid!==0)throw Error("Nonself priority target");trace.push("read");return 0;};os.setPriority=(pid,nice)=>{if(pid!==0 || nice!==10)throw Error("Unexpected priority target/value");trace.push("write");};',
      'threads.Worker=class {constructor(){trace.push("guardian");throw Error("Forbidden guardian entry");}};syncBuiltinESMExports();',
      'registerHooks({resolve(specifier,context,next){if(specifier.endsWith("nativeSearchRuntime.ts")){trace.push("native");throw Error("Forbidden native entry");}return next(specifier,context);}});',
      `const {runServerComputeWorkerCommand}=await import(${JSON.stringify(worker)});`,
      "const before=trace.slice();",
      `const {currentBuildIdentity}=await import(${JSON.stringify(command)});`,
      `process.argv[1]=${JSON.stringify(entry)};`,
      `const mode=${JSON.stringify(mode)};const build=await currentBuildIdentity();`,
      'const boot={version:1,build:mode==="build"?"b".repeat(64):build,authorityId:"synthetic",slotId:0,slotGeneration:1,nonce:"synthetic"};',
      'const args=mode==="args"?["1","2","3"]:[JSON.stringify(mode==="bootstrap"?{}:boot)];',
      'let reason;try{await runServerComputeWorkerCommand(args);throw Error("Unexpected success");}catch(error){reason=error.reason??error.message;}',
      'process.stdout.write(JSON.stringify({before,trace,reason})+"\\n");',
    ].join("\n");
    try {
      await NodeFSP.writeFile(filename, source);
      const execute = NodeUtil.promisify(NodeChildProcess.execFile);
      const result = await execute(
        NodeProcess.execPath,
        [...(mode === "heap" ? [] : ["--max-old-space-size=256"]), filename],
        {
          env: {
            ...minimalWorkerEnvironment(NodeProcess.env, false, false),
            HOME: directory,
            NODE_OPTIONS: "",
          },
          maxBuffer: 65536,
          timeout: 10000,
        },
      );
      const receipt = JSON.parse(result.stdout);
      expect(receipt.before).toEqual([]);
      expect(receipt.reason).toBe(
        mode === "heap" || mode === "priority" ? "unavailable" : "protocol",
      );
      expect(receipt.trace).toEqual(mode === "priority" ? ["read", "write", "read"] : []);
    } finally {
      await NodeFSP.rm(directory, { recursive: true });
    }
  },
);
