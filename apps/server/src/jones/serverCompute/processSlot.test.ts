// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off -- Synthetic Node process, pipe and clock fixtures exercise the actual ownership boundary.
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import * as NodeChildProcess from "node:child_process";
import * as NodeStream from "node:stream";
import {
  diagnosticTrace,
  type DiagnosticEvent,
} from "../serverParallelism/diagnosticObservation.ts";
import { guardianSource } from "./guardian.ts";
import { ProcessSlot, withComputeChildOperations, type ComputeChild } from "./processSlot.ts";
import type {
  ComputeObservation,
  ComputeRequestTiming,
} from "../serverParallelism/observations.ts";
import { frames, writeFrame, LIMITS, type Bootstrap, type Request } from "./protocol.ts";
const bootstrap: Bootstrap = {
  version: 1,
  build: "a".repeat(64),
  authorityId: "authority",
  slotId: 0,
  slotGeneration: 1,
  nonce: "nonce",
};

function telemetrySource(
  resource: unknown = 123_456_789,
  corruptTiming: false | "order" | "echo" = false,
) {
  const guardian = guardianSource.replace(
    "send({ type: 'ready', pid: process.pid });",
    `send({ type: 'ready', pid: process.pid }); send({ type: 'resources', rss: ${JSON.stringify(resource)} });`,
  );
  return `const { Worker } = require('node:worker_threads');
const { getHeapStatistics } = require('node:v8');
const boot = JSON.parse(process.argv[1]);
const guardian = new Worker(${JSON.stringify(guardian)}, { eval: true, workerData: boot });
guardian.on('error', () => process.kill(process.pid, 'SIGKILL'));
(async () => {
  await new Promise(resolve => guardian.once('message', resolve));
  const { WorkerReplies } = await import(${JSON.stringify(new URL("./worker.ts", import.meta.url).href)});
  const { frames, decodeRequest, LIMITS, writeFrame } = await import(${JSON.stringify(new URL("./protocol.ts", import.meta.url).href)});
  let active;
  guardian.on('message', msg => {
    if (!active || active.id !== msg.requestId) throw Error('unexpected control');
    if (msg.type === 'credit') active.replies.acknowledge();
    else if (msg.type === 'cancel') active.controller.abort(); else throw Error('control');
  });
  guardian.postMessage({ type: 'nativeReady', heapLimit: getHeapStatistics().heap_size_limit, availableParallelism: 2 });
  for await (const raw of frames(process.stdin, LIMITS.requestBytes)) {
    const request = decodeRequest(raw, boot);
    // Deliberately unrelated clock origin; do not change process/global clocks.
    const replies = new WorkerReplies(request, process.stdout, () => performance.now() + 1_000_000);
    const controller = new AbortController(); active = { id: request.requestId, replies, controller };
    try {
      await replies.execute(async () => {
        if (request.input?.query === 'cancelled') controller.abort();
        if (request.input?.query === 'failed' || controller.signal.aborted) throw { _tag: 'WorkspaceSearchIndexScanTimedOut' };
      }, controller.signal);
      if (${corruptTiming !== false}) {
        const { input, clock, ...envelope } = request;
        await writeFrame(process.stdout, { ...envelope, sequence: 0, timing: { parentSentMs: clock.parentSentMs + ${corruptTiming === "echo" ? 1 : 0}, receivedMs: 1,
          executionStartedMs: 2, executionCompletedMs: ${corruptTiming === "order" ? 1 : 3}, executionOutcome: 'ok', replyStartedMs: 3, handoffMs: 4 },
          body: { type: 'done', truncated: false, status: { scanning: false, warmupComplete: true } } }, LIMITS.batchBytes);
      } else await replies.result({ matches: [{ path: 'a', lineNumber: 1, lineContent: 'needle', matchRanges: [] }], truncated: false },
        { scanning: false, warmupComplete: true }, controller.signal);
    } catch (cause) { await replies.failure(cause, controller.signal); }
    await replies.settle(); active = undefined;
  }
})().catch(() => process.kill(process.pid, 'SIGKILL'));`;
}

it("carries different-epoch timing through real replies/credits/guardian and isolates throwing observers", async () => {
  const observations: ComputeObservation[] = [],
    timings: ComputeRequestTiming[] = [],
    faults: unknown[] = [];
  const child = await ProcessSlot.start(
    {
      file: process.execPath,
      args: ["--max-old-space-size=256", "-e", telemetrySource()],
      env: {},
      build: bootstrap.build,
    },
    bootstrap,
    async () => {},
    (cause) => faults.push(cause),
    (event) => {
      observations.push(event);
      throw new Error("resource sink");
    },
  );
  try {
    for (const [index, query] of ["ok", "failed", "cancelled", "ok"].entries()) {
      const request: Request = {
        ...bootstrap,
        requestId: index + 1,
        operation: "searchContents",
        input: { query, limit: 1, caseSensitive: false, wholeWord: false, useRegex: false },
        token: {
          authorityId: bootstrap.authorityId,
          slotId: 0,
          slotGeneration: 1,
          keyId: "key",
          incarnation: 1,
        },
      };
      const result = child.request(request, undefined, (event) => {
        timings.push(event);
        throw new Error("timing sink");
      });
      if (query === "ok")
        expect((await result).items).toEqual([
          { path: "a", lineNumber: 1, lineContent: "needle", matchRanges: [] },
        ]);
      else
        await expect(result).rejects.toMatchObject({
          reason: query === "failed" ? "finder-search" : "cancelled",
          scanTimedOut: true,
        });
      const timing = timings.at(-1)!;
      expect(timing.worker?.executionOutcome).toBe(query === "ok" ? "ok" : query);
      expect(timing.worker!.executionStartedMs - timing.worker!.parentSentMs).toBeGreaterThan(
        900_000,
      );
      expect(timing.delivery!.offsetUpperMs).toBeLessThan(-900_000);
      expect(timing.delivery!.durationMs - timing.delivery!.uncertaintyMs).toBe(0);
      expect(timing.parentContinuationMs).toBeGreaterThanOrEqual(0);
      expect(faults).toEqual([]);
    }
    expect(observations).toContainEqual({
      phase: "resources",
      operation: "pool",
      slotId: 0,
      generation: 1,
      outcome: "ok",
      rssBytes: 123_456_789,
    });
  } finally {
    await child.retire();
  }
});

it.each([-1, 1.5, "123", { rss: 123 }, Number.MAX_SAFE_INTEGER + 1])(
  "rejects malformed guardian resource value %j before observing it",
  async (rss) => {
    const observations: ComputeObservation[] = [];
    await expect(
      ProcessSlot.start(
        {
          file: process.execPath,
          args: ["--max-old-space-size=256", "-e", telemetrySource(rss)],
          env: {},
          build: bootstrap.build,
        },
        bootstrap,
        async () => {},
        () => {},
        (event) => observations.push(event),
      ),
    ).rejects.toMatchObject({ reason: "unavailable" });
    expect(observations).toEqual([]);
  },
);

it.each(["order", "echo"] as const)(
  "rejects malformed terminal timing (%s) before credit/reuse even when data is valid",
  async (kind) => {
    const child = await ProcessSlot.start(
      {
        file: process.execPath,
        args: ["--max-old-space-size=256", "-e", telemetrySource(123, kind)],
        env: {},
        build: bootstrap.build,
      },
      bootstrap,
      async () => {},
      () => {},
    );
    try {
      await expect(
        child.request({
          ...bootstrap,
          requestId: 1,
          operation: "list",
          input: null,
          token: {
            authorityId: bootstrap.authorityId,
            slotId: 0,
            slotGeneration: 1,
            keyId: "key",
            incarnation: 1,
          },
        }),
      ).rejects.toMatchObject({ reason: "protocol" });
    } finally {
      await child.retire();
    }
  },
);

it("uses actual worker framing and parent credits at the aggregate boundary, fails oversize explicitly, and reuses the child", async () => {
  // Only native result production is synthetic. Actual WorkerReplies, ProcessSlot, frames and FD3 guardian execute.
  // This qualifies the transport regression, not native execution or resource retirement.
  const workerUrl = new URL("./worker.ts", import.meta.url).href;
  const protocolUrl = new URL("./protocol.ts", import.meta.url).href;
  const source = `const { Worker } = require('node:worker_threads');
const { getHeapStatistics } = require('node:v8');
const boot = JSON.parse(process.argv[1]);
const guardian = new Worker(${JSON.stringify(guardianSource)}, { eval: true, workerData: boot });
guardian.on('error', () => process.kill(process.pid, 'SIGKILL'));
guardian.on('exit', () => process.kill(process.pid, 'SIGKILL'));
(async () => {
  await new Promise(resolve => guardian.once('message', msg => { if (msg.type !== 'go') throw Error('go'); resolve(); }));
  const { WorkerReplies } = await import(${JSON.stringify(workerUrl)});
  const { frames, decodeRequest, LIMITS } = await import(${JSON.stringify(protocolUrl)});
  let active;
  guardian.on('message', msg => {
    if (!active || active.id !== msg.requestId) throw Error('unexpected control');
    if (msg.type === 'credit') active.replies.acknowledge();
    else if (msg.type === 'cancel') active.controller.abort();
    else throw Error('control');
  });
  guardian.postMessage({ type: 'nativeReady', heapLimit: getHeapStatistics().heap_size_limit, availableParallelism: 2 });
  const status = { scanning: false, warmupComplete: true };
  for await (const raw of frames(process.stdin, LIMITS.requestBytes)) {
    const request = decodeRequest(raw, boot), replies = new WorkerReplies(request, process.stdout);
    const controller = new AbortController(); active = { id: request.requestId, replies, controller };
    const item = { path: 'a.ts', lineNumber: 1, lineContent: 'needle', matchRanges: [] };
    await replies.execute(async () => {}, controller.signal);
    try {
      if (request.input.query === 'exact' || request.input.query === 'beyond-boundary') {
        const { input, clock, ...envelope } = request;
        let remaining = LIMITS.resultBytes - LIMITS.terminalBytes, sequence = 0;
        while (remaining > 0) {
          let bytes = Math.min(240 * 1024, remaining);
          if (remaining > bytes && remaining - bytes < 2048) bytes -= 2048;
          const body = { type: 'chunk', items: [{ ...item, lineContent: '' }] };
          const base = Buffer.byteLength(JSON.stringify({ ...envelope, sequence, body }));
          body.items[0].lineContent = 'x'.repeat(bytes - base);
          await replies.send(body); remaining -= bytes; sequence++;
        }
        if (request.input.query === 'beyond-boundary') await replies.send({ type: 'chunk', items: [item] });
        await replies.result({ matches: [], truncated: false }, status, controller.signal);
      } else {
        const matches = request.input.query === 'aggregate' ? Array.from({ length: 180 }, () => ({ ...item, lineContent: 'x'.repeat(100 * 1024) })) :
          request.input.query === 'single' ? [{ ...item, lineContent: 'x'.repeat(LIMITS.batchBytes) }] : [item];
        await replies.result({ matches, truncated: false }, status, controller.signal);
      }
    } catch (cause) { await replies.failure(cause, controller.signal); }
    // The next request cannot execute until every actual written frame has been acknowledged.
    await replies.settle(); active = undefined;
  }
})().catch(() => process.kill(process.pid, 'SIGKILL'));`;
  const faults: unknown[] = [];
  const child = await ProcessSlot.start(
    {
      file: process.execPath,
      args: ["--max-old-space-size=256", "-e", source],
      env: {},
      build: bootstrap.build,
    },
    bootstrap,
    async () => {},
    (cause) => {
      faults.push(cause);
    },
  );
  let requestId = 0;
  const request = (query: string): Request => ({
    ...bootstrap,
    requestId: ++requestId,
    operation: "searchContents",
    input: { query, limit: 200, caseSensitive: false, wholeWord: false, useRegex: false },
    token: {
      authorityId: bootstrap.authorityId,
      slotId: 0,
      slotGeneration: 1,
      keyId: "key",
      incarnation: 1,
    },
  });
  try {
    let exactTiming: ComputeRequestTiming | undefined;
    const exact = await child.request(request("exact"), undefined, (event) => {
      exactTiming = event;
    });
    expect(exact.final).toEqual({
      type: "done",
      truncated: false,
      status: { scanning: false, warmupComplete: true },
    });
    expect(exact.items.length).toBeGreaterThan(2);
    expect(exactTiming?.worker?.executionOutcome).toBe("ok");
    for (const query of ["beyond-boundary", "aggregate", "single"]) {
      // Rejection delivers no partially assembled SlotResult, even when many chunks preceded failure.
      await expect(child.request(request(query))).rejects.toMatchObject({
        reason: "oversize",
        scanTimedOut: false,
      });
      const later = await child.request(request("fresh"));
      expect(later.items).toEqual([
        { path: "a.ts", lineNumber: 1, lineContent: "needle", matchRanges: [] },
      ]);
      expect(later.final.truncated).toBe(false);
      expect(faults).toEqual([]);
    }
  } finally {
    await child.retire();
  }
});

it("rejects stale incarnation replies and proves captured process retirement before returning capacity", async () => {
  // External native operation is represented by a fixed faulty reply. The real driver/FD3 guardian/framing execute.
  const source = `const { Worker } = require('node:worker_threads');
const { getHeapStatistics } = require('node:v8');
const boot = JSON.parse(process.argv[1]);
const guardian = new Worker(${JSON.stringify(guardianSource)}, { eval: true, workerData: boot });
guardian.on('error', () => process.kill(process.pid, 'SIGKILL'));
guardian.on('message', msg => {
  if (msg.type === 'go') guardian.postMessage({ type: 'nativeReady', heapLimit: getHeapStatistics().heap_size_limit, availableParallelism: 2 });
});
let buffer = Buffer.alloc(0);
process.stdin.on('data', chunk => {
  buffer = Buffer.concat([buffer, chunk]);
  if (buffer.length < 4 || buffer.length < 4 + buffer.readUInt32BE()) return;
  const request = JSON.parse(buffer.subarray(4).toString());
  const { input, clock, ...envelope } = request;
  const payload = Buffer.from(JSON.stringify({ ...envelope, token: { ...request.token, incarnation: request.token.incarnation + 1 }, sequence: 0,
    body: { type: 'done', status: { scanning: false, warmupComplete: true }, truncated: false } }));
  const header = Buffer.alloc(4); header.writeUInt32BE(payload.length); process.stdout.write(Buffer.concat([header,payload]));
});`;
  let registered = false;
  const child = await ProcessSlot.start(
    {
      file: process.execPath,
      args: ["--max-old-space-size=256", "-e", source],
      env: {},
      build: bootstrap.build,
    },
    bootstrap,
    async () => {
      registered = true;
    },
    () => {},
  );
  try {
    expect(registered).toBe(true);
    const request: Request = {
      ...bootstrap,
      requestId: 1,
      operation: "list",
      input: null,
      token: {
        authorityId: bootstrap.authorityId,
        slotId: 0,
        slotGeneration: 1,
        keyId: "key",
        incarnation: 1,
      },
    };
    await expect(child.request(request)).rejects.toMatchObject({ reason: "protocol" });
  } finally {
    await child.retire();
  }
});

it("keeps a child alive after close ACK and returns retirement only after the captured process exits", async () => {
  const workerUrl = new URL("./worker.ts", import.meta.url).href;
  const protocolUrl = new URL("./protocol.ts", import.meta.url).href;
  const source = `const { Worker } = require('node:worker_threads');
const { getHeapStatistics } = require('node:v8');
const boot = JSON.parse(process.argv[1]);
const guardian = new Worker(${JSON.stringify(guardianSource)}, { eval: true, workerData: boot });
guardian.on('error', () => process.kill(process.pid, 'SIGKILL'));
(async () => {
  await new Promise(resolve => guardian.once('message', resolve));
  const { WorkerReplies } = await import(${JSON.stringify(workerUrl)});
  const { frames, decodeRequest, LIMITS } = await import(${JSON.stringify(protocolUrl)});
  let active;
  guardian.on('message', msg => {
    if (!active || active.id !== msg.requestId || msg.type !== 'credit') throw Error('control');
    active.replies.acknowledge();
  });
  guardian.postMessage({ type: 'nativeReady', heapLimit: getHeapStatistics().heap_size_limit, availableParallelism: 2 });
  for await (const raw of frames(process.stdin, LIMITS.requestBytes)) {
    const request = decodeRequest(raw, boot), replies = new WorkerReplies(request, process.stdout);
    active = { id: request.requestId, replies };
    // Synthetic destroy returns immediately. It has no authority to end this process's native lifetime.
    await replies.result(undefined, undefined, new AbortController().signal);
    await replies.settle(); active = undefined;
  }
})().catch(() => process.kill(process.pid, 'SIGKILL'));`;
  const faults: unknown[] = [];
  const child = await ProcessSlot.start(
    {
      file: process.execPath,
      args: ["--max-old-space-size=256", "-e", source],
      env: {},
      build: bootstrap.build,
    },
    bootstrap,
    async () => {},
    (cause) => {
      faults.push(cause);
    },
  );
  try {
    const request: Request = {
      ...bootstrap,
      requestId: 1,
      operation: "close",
      input: null,
      token: {
        authorityId: bootstrap.authorityId,
        slotId: 0,
        slotGeneration: 1,
        keyId: "key",
        incarnation: 1,
      },
    };
    expect((await child.request(request)).final).toEqual({ type: "done" });
    expect(() => process.kill(child.pid, 0)).not.toThrow();
    expect(faults).toEqual([]);
  } finally {
    await child.retire();
  }
  expect(() => process.kill(child.pid, 0)).toThrow();
  expect(faults).toHaveLength(1);
});

// A closed raw FD3 peer isolates the parent's validation, including frames rejected by the real guardian.
function readinessSource(patch: Record<string, unknown> = {}, duplicate = false) {
  const { omit, ...fields } = patch;
  return `const { Socket } = require('node:net');
const boot = JSON.parse(process.argv[1]);
const pipe = new Socket({ fd: 3, readable: true, writable: true });
const send = value => pipe.write(JSON.stringify({ ...boot, ...value }) + '\\n');
const ready = { type: 'nativeReady', heapLimit: 300 * 1024 * 1024, availableParallelism: 5, ...${JSON.stringify(fields)} };
${typeof omit === "string" ? `delete ready[${JSON.stringify(omit)}];` : ""}
pipe.on('error', () => process.exit(1));
let buffer = '';
pipe.on('data', bytes => {
  buffer += bytes.toString();
  let end;
  while ((end = buffer.indexOf('\\n')) >= 0) {
    const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
    const msg = JSON.parse(line);
    if (msg.type === 'go') { send({ type: 'go' }); send(ready); ${duplicate ? "send({ ...ready, availableParallelism: 99 });" : ""} }
    else if (msg.type === 'loaded') send({ type: 'loaded' });
    else if (msg.type === 'terminate') process.exit(0);
  }
});
send({ type: 'ready', pid: process.pid });`;
}

it("binds frozen metadata to captured bootstrap, preserves it through wrappers and exit, and separates generations", async () => {
  const records: ComputeChild[] = [];
  for (const generation of [1, 2]) {
    const boot = { ...bootstrap, slotGeneration: generation, nonce: `metadata-${generation}` };
    const child = await ProcessSlot.start(
      {
        file: process.execPath,
        args: ["--max-old-space-size=256", "-e", readinessSource()],
        env: {},
        build: boot.build,
      },
      boot,
      async () => {},
      () => {},
      undefined,
      (starting) => {
        expect(starting.runtimeMetadata).toBeUndefined();
        records.push(
          withComputeChildOperations(starting, {
            request: (...args) => starting.request(...args),
            retire: () => starting.retire(),
          }),
        );
      },
    );
    let requests = 0,
      retirements = 0;
    const wrapper = withComputeChildOperations(child, {
      request: async () => {
        requests++;
        return { items: [], final: { type: "done" } };
      },
      retire: async () => {
        retirements++;
        await child.retire();
      },
    });
    const metadata = child.runtimeMetadata;
    try {
      expect(metadata).toEqual({
        heapLimitBytes: 300 * 1024 * 1024,
        availableParallelism: 5,
        parallelismSource: "node:os.availableParallelism",
      });
      expect(Object.isFrozen(metadata)).toBe(true);
      expect(records.at(-1)?.runtimeMetadata).toBe(metadata);
      expect(Reflect.set(metadata!, "availableParallelism", 999)).toBe(false);
      expect(wrapper.runtimeMetadata).toBe(metadata);
      expect(wrapper.bootstrap).toBe(child.bootstrap);
      expect(wrapper.pid).toBe(child.pid);
      boot.nonce = "mutated-caller";
      expect(child.bootstrap.nonce).toBe(`metadata-${generation}`);
      expect(Object.isFrozen(child.bootstrap)).toBe(true);
      await wrapper.request({
        ...child.bootstrap,
        requestId: 1,
        operation: "list",
        input: null,
        token: {
          authorityId: boot.authorityId,
          slotId: 0,
          slotGeneration: generation,
          keyId: "key",
          incarnation: 1,
        },
      });
      expect(requests).toBe(1);
    } finally {
      await wrapper.retire();
    }
    expect(retirements).toBe(1);
    expect(child.runtimeMetadata).toBe(metadata);
    expect(wrapper.runtimeMetadata).toBe(metadata);
    expect(() => process.kill(child.pid, 0)).toThrow(); // Metadata remains available after proven exit.
  }
  expect(records[0]?.runtimeMetadata).not.toBe(records[1]?.runtimeMetadata);
  expect(records.map((record) => record.bootstrap.slotGeneration)).toEqual([1, 2]);
});

it.each([
  { availableParallelism: 0 },
  { availableParallelism: -1 },
  { availableParallelism: 1.5 },
  { availableParallelism: "5" },
  { availableParallelism: null },
  { availableParallelism: Number.MAX_SAFE_INTEGER + 1 },
  { heapLimit: 256 * 1024 * 1024 - 1 },
  { heapLimit: 400 * 1024 * 1024 + 1 },
  { heapLimit: "314572800" },
  { heapLimit: null },
  { extra: 1 },
  { omit: "availableParallelism" },
  { omit: "heapLimit" },
  { nonce: "wrong-nonce" },
  { slotGeneration: 2 },
  { build: "b".repeat(64) },
])("rejects malformed or misbound nativeReady before retaining metadata: %j", async (patch) => {
  const capture: { child?: ComputeChild } = {};
  await expect(
    ProcessSlot.start(
      {
        file: process.execPath,
        args: ["--max-old-space-size=256", "-e", readinessSource(patch)],
        env: {},
        build: bootstrap.build,
      },
      bootstrap,
      async () => {},
      () => {},
      undefined,
      (child) => {
        capture.child = child;
      },
    ),
  ).rejects.toMatchObject({ reason: "unavailable" });
  expect(capture.child).toBeDefined();
  expect(capture.child?.runtimeMetadata).toBeUndefined();
  expect(() => process.kill(capture.child!.pid, 0)).toThrow();
});

it("rejects duplicate nativeReady without replacing the first immutable record", async () => {
  const capture: { child?: ComputeChild } = {};
  let notify!: () => void;
  const failed = new Promise<void>((resolve) => {
    notify = resolve;
  });
  let child: ProcessSlot | undefined;
  try {
    try {
      child = await ProcessSlot.start(
        {
          file: process.execPath,
          args: ["--max-old-space-size=256", "-e", readinessSource({}, true)],
          env: {},
          build: bootstrap.build,
        },
        bootstrap,
        async () => {},
        notify,
        undefined,
        (starting) => {
          capture.child = starting;
        },
      );
    } catch (cause) {
      expect(cause).toMatchObject({ reason: "unavailable" });
    }
    await failed;
    expect(capture.child?.runtimeMetadata?.availableParallelism).toBe(5);
    expect(Object.isFrozen(capture.child?.runtimeMetadata)).toBe(true);
  } finally {
    await (child ?? capture.child)?.retire();
  }
});

vi.mock("node:child_process", async (original) => {
  const actual = await original<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});
function diagnosticPeer(
  patch: Record<string, unknown> = {},
  boot: Bootstrap = bootstrap,
  pid = 321,
) {
  const child = new NodeChildProcess.ChildProcess();
  const input = new NodeStream.PassThrough(),
    output = new NodeStream.PassThrough(),
    stderr = new NodeStream.PassThrough();
  let terminated!: () => void;
  const terminateRequested = new Promise<void>((resolve) => {
    terminated = resolve;
  });
  const control = new NodeStream.Duplex({
    read() {},
    write(chunk, _encoding, callback) {
      const message = JSON.parse(String(chunk)) as { type: string; requestId?: number };
      const send = (body: Record<string, unknown>) =>
        control.push(JSON.stringify({ ...boot, ...body }) + "\n");
      if (message.type === "go") {
        send({ type: "go" });
        send({
          type: "nativeReady",
          heapLimit: 300 * 1024 * 1024,
          availableParallelism: 2,
          ...patch,
        });
      } else if (message.type === "loaded") send({ type: "loaded" });
      else if (message.type === "arm" || message.type === "disarm")
        send({ type: message.type === "arm" ? "armed" : "disarmed", requestId: message.requestId });
      else if (message.type === "terminate") terminated();
      callback();
    },
  });
  Object.defineProperty(child, "pid", { value: pid });
  Object.assign(child, {
    stdin: input,
    stdout: output,
    stderr,
    stdio: [input, output, stderr, control],
  });
  vi.spyOn(child, "kill").mockImplementation(() => {
    queueMicrotask(() => child.emit("exit", null, "SIGKILL"));
    return true;
  });
  vi.mocked(NodeChildProcess.spawn).mockReturnValueOnce(child);
  queueMicrotask(() => control.push(JSON.stringify({ ...boot, type: "ready", pid }) + "\n"));
  const requests = (async () => {
    for await (const value of frames(input, LIMITS.requestBytes)) {
      const request = value as Request & { clock: { parentSentMs: number } };
      await writeFrame(
        output,
        {
          ...boot,
          requestId: request.requestId,
          operation: request.operation,
          token: request.token,
          sequence: 0,
          timing: {
            parentSentMs: request.clock.parentSentMs,
            receivedMs: 1000 + pid - 321,
            executionStartedMs: 1000 + pid - 321,
            executionCompletedMs: 1000 + pid - 321,
            executionOutcome: "ok",
            replyStartedMs: 1000 + pid - 321,
            handoffMs: 1000 + pid - 321,
          },
          body: { type: "done" },
        },
        LIMITS.batchBytes,
      );
    }
  })().catch(() => {});
  return { child, terminateRequested, requests };
}
describe("synchronous diagnostic ownership isolation", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date", "performance"] });
    vi.setSystemTime(0);
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });
  it.each(["throw", "full"] as const)(
    "keeps request sequencing and distinct exit/close/retire milestones with a %s sink",
    async (mode) => {
      const peer = diagnosticPeer();
      const trace = diagnosticTrace(mode === "full" ? 1 : 100),
        events: DiagnosticEvent[] = [],
        faults: unknown[] = [];
      const child = await ProcessSlot.start(
        { file: "synthetic", args: [], env: {}, build: bootstrap.build },
        bootstrap,
        async () => {},
        (cause) => faults.push(cause),
        undefined,
        undefined,
        (event) => {
          trace.append(event);
          events.push(event);
          if (mode === "throw") throw new Error("sink");
        },
      );
      try {
        const request: Request = {
          ...bootstrap,
          requestId: 7,
          operation: "close",
          input: null,
          token: {
            authorityId: bootstrap.authorityId,
            slotId: 0,
            slotGeneration: 1,
            keyId: "key",
            incarnation: 1,
          },
        };
        expect((await child.request(request)).final).toEqual({ type: "done" });
        expect(faults).toEqual([]);
        expect(
          events
            .filter(
              (row) =>
                row.kind === "lifecycle" &&
                ["parent-send", "terminal-receive", "disarm", "settle"].includes(row.event),
            )
            .map((row) => [row.kind === "lifecycle" ? row.event : "", row.requestId, row.token]),
        ).toEqual(
          ["parent-send", "terminal-receive", "disarm", "settle"].map((event) => [
            event,
            7,
            request.token,
          ]),
        );
        expect(events).toContainEqual(
          expect.objectContaining({
            event: "terminal-receive",
            timing: expect.objectContaining({
              worker: expect.objectContaining({ receivedMs: 1000 }),
              delivery: expect.objectContaining({ offsetLowerMs: -1001, offsetUpperMs: -999 }),
            }),
          }),
        );
        const retirement = child.retire();
        await peer.terminateRequested;
        expect(faults).toEqual([]);
        expect(
          events.some((row) => row.kind === "lifecycle" && row.event === "captured-exit"),
        ).toBe(false);
        peer.child.emit("exit", 0, null);
        await retirement;
        expect(faults).toHaveLength(1);
        expect(
          events.filter((row) => row.kind === "lifecycle" && row.event === "captured-exit"),
        ).toHaveLength(1);
        expect(
          events.some((row) => row.kind === "lifecycle" && row.event === "captured-close"),
        ).toBe(false);
        peer.child.emit("close", 0, null);
        expect(
          events.filter((row) => row.kind === "lifecycle" && row.event === "captured-close"),
        ).toHaveLength(1);
        expect(
          events.every(
            (row) => row.pid === 321 && row.bootstrap?.nonce === bootstrap.nonce && row.atMs === 0,
          ),
        ).toBe(true);
        expect(trace.report().overflow).toBe(mode === "full");
        expect(vi.getMockedSystemTime()?.getTime()).toBe(0);
      } finally {
        peer.child.emit("exit", 0, null);
        await child.retire().catch(() => {});
        await peer.requests;
      }
    },
  );

  it("still rejects mismatched readiness and captures real exit when the sink throws", async () => {
    const peer = diagnosticPeer({ nonce: "mismatch" });
    const events: DiagnosticEvent[] = [];
    await expect(
      ProcessSlot.start(
        { file: "synthetic", args: [], env: {}, build: bootstrap.build },
        bootstrap,
        async () => {},
        () => {},
        undefined,
        undefined,
        (event) => {
          events.push(event);
          throw new Error("sink");
        },
      ),
    ).rejects.toMatchObject({ reason: "unavailable" });
    expect(events).toContainEqual(
      expect.objectContaining({ event: "captured-exit", signal: "SIGKILL" }),
    );
    expect(events.some((row) => row.kind === "lifecycle" && row.event === "loaded")).toBe(false);
    await peer.requests;
  });
  it.each(["parallel", "serialized"] as const)(
    "retains raw clocks and request/token identity across children in %s traffic",
    async (mode) => {
      const boots = [bootstrap, { ...bootstrap, slotId: 1, nonce: "second" }];
      const peers = boots.map((boot, index) => diagnosticPeer({}, boot, 321 + index));
      const events: DiagnosticEvent[] = [];
      const children = await Promise.all(
        boots.map((boot) =>
          ProcessSlot.start(
            { file: "synthetic", args: [], env: {}, build: boot.build },
            boot,
            async () => {},
            () => {},
            undefined,
            undefined,
            (event) => {
              events.push(event);
            },
          ),
        ),
      );
      try {
        const call = (index: number) => {
          const boot = boots[index]!;
          return children[index]!.request({
            ...boot,
            requestId: 9,
            operation: "close",
            input: null,
            token: {
              authorityId: boot.authorityId,
              slotId: boot.slotId,
              slotGeneration: boot.slotGeneration,
              keyId: `key-${index}`,
              incarnation: 1,
            },
          });
        };
        if (mode === "parallel") await Promise.all([call(0), call(1)]);
        else {
          await call(0);
          await call(1);
        }
        const terminals = events.filter(
          (row) => row.kind === "lifecycle" && row.event === "terminal-receive",
        );
        expect(terminals).toHaveLength(2);
        expect(terminals.map((row) => [row.pid, row.requestId, row.token?.keyId]).sort()).toEqual([
          [321, 9, "key-0"],
          [322, 9, "key-1"],
        ]);
        expect(terminals).toContainEqual(
          expect.objectContaining({
            pid: 322,
            timing: expect.objectContaining({
              worker: expect.objectContaining({ receivedMs: 1001 }),
            }),
          }),
        );
        const cancelled = new AbortController();
        cancelled.abort();
        await expect(
          children[0]!.request(
            {
              ...bootstrap,
              requestId: 10,
              operation: "close",
              input: null,
              token: {
                authorityId: bootstrap.authorityId,
                slotId: 0,
                slotGeneration: 1,
                keyId: "key-0",
                incarnation: 1,
              },
            },
            cancelled.signal,
          ),
        ).rejects.toMatchObject({ reason: "cancelled" });
        expect(events.some((row) => row.requestId === 10)).toBe(false);
      } finally {
        await Promise.all(
          children.map(async (child, index) => {
            peers[index]!.child.emit("exit", 0, null);
            await child.retire().catch(() => {});
            peers[index]!.child.emit("close", 0, null);
            await peers[index]!.requests;
          }),
        );
      }
    },
  );
});
