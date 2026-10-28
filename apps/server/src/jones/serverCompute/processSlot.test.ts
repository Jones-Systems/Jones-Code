// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off -- Synthetic Node process, pipe and clock fixtures exercise the actual ownership boundary.
import { expect, it } from "vite-plus/test";
import { guardianSource } from "./guardian.ts";
import { ProcessSlot } from "./processSlot.ts";
import type {
  ComputeObservation,
  ComputeRequestTiming,
} from "../serverParallelism/observations.ts";
import { type Bootstrap, type Request } from "./protocol.ts";
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
