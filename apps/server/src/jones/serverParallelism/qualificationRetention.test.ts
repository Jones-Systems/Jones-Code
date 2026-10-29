// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Production harness/supervisor with controlled external child, filesystem, clock and journal boundaries; no native process or fixture files.
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import type { ComputeChild, SlotResult } from "../serverCompute/processSlot.ts";
import type { Bootstrap, Request } from "../serverCompute/protocol.ts";
import type { ComputeObservation } from "./observations.ts";
import type { NativePoolObservation } from "./resourceSamples.ts";
import { processResourceSample } from "./resourceSamples.ts";
import { qualificationRetentionComplete, qualificationStream, qualificationWorkload } from "./qualificationRetention.ts";

const external = vi.hoisted(() => ({
  start: undefined as unknown as (boot: Bootstrap, register: (pid: number) => Promise<void>, onExit: (cause: unknown) => void, observe: (event: ComputeObservation) => void, captured: (child: ComputeChild) => void) => Promise<ComputeChild>,
  retired: undefined as unknown as (slot: number, generation: number, nonce: string, pid: number) => Promise<void>,
  pools: undefined as unknown as (pid: number) => Promise<NativePoolObservation>,
  wakeSampler: undefined as (() => void) | undefined,
  waits: 0,
  writes: 0,
  mkdirs: 0,
  readerWindowIncomplete: false,
}));
vi.mock("node:fs/promises", () => ({
  mkdir: async () => { external.mkdirs++; },
  writeFile: async () => { external.writes++; },
  rename: async () => {},
  rm: async () => {},
}));
vi.mock("node:perf_hooks", () => ({
  monitorEventLoopDelay: () => ({ enable() {}, disable() {}, percentile: () => 0, max: 0 }),
}));
vi.mock("node:timers/promises", () => ({
  setTimeout: async (ms: number, _value: unknown, options: { signal: AbortSignal }) => {
    if (options.signal.aborted) throw new Error("aborted");
    if (ms === 3_000) {
      // The final stable wait puts maintenance/guardian/shutdown near the actual
      // production deadline. Resource cadence is an external controlled boundary.
      external.waits++;
      await vi.advanceTimersByTimeAsync(external.waits === 1 ? ms : 599_750 - performance.now());
      return;
    }
    if (ms !== 10) throw new Error("Unexpected harness wait");
    await new Promise<void>((resolve) => {
      const done = () => {
        options.signal.removeEventListener("abort", done);
        if (external.wakeSampler === done) external.wakeSampler = undefined;
        resolve();
      };
      external.wakeSampler = done;
      options.signal.addEventListener("abort", done, { once: true });
    });
  },
}));
vi.mock("../serverCompute/journal.ts", () => ({
  GenerationJournal: { acquire: async () => ({
    claim: { authorityId: "qualification" }, reserve: async () => {}, registered: async () => {},
    release: async () => {}, retired: (...args: Parameters<typeof external.retired>) => external.retired(...args),
  }) },
}));
vi.mock("../serverCompute/processSlot.ts", async (original) => ({
  ...await original<typeof import("../serverCompute/processSlot.ts")>(),
  ProcessSlot: { start: (...args: Parameters<typeof external.start>) => external.start(...args) },
}));
vi.mock("./resourceSamples.ts", async (original) => ({
  ...await original<typeof import("./resourceSamples.ts")>(),
  sampleMacPerformanceCores: async () => ({ source: "sysctl:hw.perflevel0.physicalcpu", status: "observed", value: 4, raw: "4" }),
  sampleNativePools: (pid: number) => external.pools(pid),
}));
vi.mock("./combinedQualification.ts", async (original) => {
  const actual = await original<typeof import("./combinedQualification.ts")>();
  return { ...actual, combinedReaderWriterWindow: async (
    _file: string, _count: 2 | 3, acquisitions: number, compute: () => Promise<void>, observations: () => readonly ComputeObservation[],
  ) => {
    await compute();
    return {
      readerQueue: actual.percentiles(Array.from({ length: 2 * acquisitions }, () => 1)),
      writer: actual.percentiles([1]), control: actual.percentiles([1]),
      computeObservations: observations(), observationLimitReached: external.readerWindowIncomplete,
    };
  } };
});
import { runNativeQualification } from "./nativeQualification.ts";

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "performance"] });
  external.waits = 0;
  external.writes = 0;
  external.mkdirs = 0;
  external.readerWindowIncomplete = false;
});
afterEach(() => {
  external.wakeSampler?.();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

function fixture(holdReplacement = false) {
  let enterPool!: () => void, releasePool!: () => void;
  const poolEntered = new Promise<void>((resolve) => { enterPool = resolve; });
  const poolRelease = new Promise<void>((resolve) => { releasePool = resolve; });
  const children: (ComputeChild & { exited: boolean; requests: Request[]; entries: Map<string, string>; timer: ReturnType<typeof setInterval> })[] = [];
  const journalOrder: string[] = [];
  external.retired = async (_slot, _generation, _nonce, pid) => {
    const child = children.find((child) => child.pid === pid)!;
    expect(child.exited).toBe(true);
    journalOrder.push(`exit:${pid}`);
    // Exercise the sampler during retirement, while old generation accounting
    // remains charged. This is a fake-clock boundary drain, not a real sleep.
    external.wakeSampler?.();
    await vi.advanceTimersByTimeAsync(external.waits === 2 ? 200 : 0);
    journalOrder.push(`journal:${pid}`);
  };
  external.start = async (bootstrap, register, onExit, observe, captured) => {
    const pid = 10_000 + children.length;
    const entries = new Map<string, string>();
    const requests: Request[] = [];
    const child = {
      pid, bootstrap, entries, requests, exited: false,
      runtimeMetadata: { heapLimitBytes: 96 * 1024 * 1024, availableParallelism: 4, parallelismSource: "node:os.availableParallelism" as const },
      timer: setInterval(() => observe({ phase: "resources", operation: "pool", slotId: bootstrap.slotId, generation: bootstrap.slotGeneration, outcome: "ok", rssBytes: 1024 }), 1_000),
      request: async (request: Request, _signal?: AbortSignal, timing?: Parameters<ComputeChild["request"]>[2]): Promise<SlotResult> => {
        expect(child.exited).toBe(false);
        requests.push(request);
        if (request.operation === "open") {
          const cwd = (request.input as { cwd: string }).cwd;
          entries.set(request.token.keyId, cwd);
          expect(entries.size).toBeLessThanOrEqual(4);
        }
        const cwd = entries.get(request.token.keyId)!;
        const key = /workspace-([0-9]+)$/.exec(cwd)![1]!;
        const items = request.operation === "search" ? [{ path: "file-0.txt", kind: "file" }] :
          request.operation === "searchContents" && (request.input as { query: string }).query === "needle" ?
            [{ path: "file-0.txt", lineNumber: 1, lineContent: `needle-${key} alpha beta`, matchRanges: [{ start: 0, end: 6 }] }] : [];
        const at = performance.now();
        timing?.({ worker: { parentSentMs: at, receivedMs: at, executionStartedMs: at, executionCompletedMs: at, executionOutcome: "ok", replyStartedMs: at, handoffMs: at }, delivery: { lowerMs: 0, upperMs: 0, durationMs: 0, uncertaintyMs: 0, offsetLowerMs: 0, offsetUpperMs: 0 } });
        return { items, final: { type: "done", truncated: false, status: { scanning: false, warmupComplete: true } } };
      },
      retire: async () => {
        external.wakeSampler?.();
        await vi.advanceTimersByTimeAsync(0);
        child.exited = true;
        onExit(new Error("captured intentional exit"));
        clearInterval(child.timer);
      },
    };
    children.push(child);
    captured(child);
    await register(pid);
    return child;
  };
  external.pools = async (pid) => {
    const child = children.find((child) => child.pid === pid)!;
    expect(child.exited).toBe(false);
    if (holdReplacement && child.bootstrap.slotGeneration > 1) {
      enterPool();
      await poolRelease;
      expect(child.exited).toBe(false);
    }
    expect(child.requests.some((request) => request.operation === "searchContents" && (request.input as { query?: string }).query === "needle")).toBe(true);
    return { pid, method: process.platform === "linux" ? "linux-proc-task-comm" : "macos-sample", status: "complete", threads: [
      { tid: "1", name: "fff-bg-0" }, { tid: "2", name: "fff-bg-1" },
      ...Array.from({ length: 4 }, (_, i) => ({ tid: String(3 + i), name: `fff-search-${i}` })),
    ], raw: "controlled external OS boundary" };
  };
  return { children, journalOrder, poolEntered, releasePool };
}

// These literals come from the workload's alternating path/content keys, not
// from retention formulas: path=1 operation, content=positive+absent, refresh=1.
it.each([
  { count: 2 as const, windows: 3, acquisitions: 500, files: 512, steady: 4_500, control: 1_500, churn: 204, readerLimit: false },
  { count: 3 as const, windows: 3, acquisitions: 500, files: 512, steady: 6_750, control: 2_250, churn: 300, readerLimit: false },
  { count: 2 as const, windows: 5, acquisitions: 2_000, files: 2_048, steady: 30_000, control: 6_000, churn: 204, readerLimit: true },
  { count: 3 as const, windows: 5, acquisitions: 2_000, files: 2_048, steady: 45_000, control: 9_000, churn: 300, readerLimit: false },
])("retains the production workload through every churn generation and shutdown ($count/$windows/$acquisitions)", async (case_) => {
  const holdReplacement = case_.count === 2 && case_.windows === 3;
  const { children, journalOrder, poolEntered, releasePool } = fixture(holdReplacement);
  external.readerWindowIncomplete = case_.readerLimit;
  const running = runNativeQualification({
    count: case_.count, directory: "/synthetic/qualification", command: { file: "/synthetic/node", args: [], env: {}, build: "a".repeat(64) },
    windows: case_.windows, acquisitionsPerWindow: case_.acquisitions, filesPerKey: case_.files,
    sample: async (pids) => processResourceSample(pids, pids.map((pid) => ({ pid, rssBytes: 1024, cpuMs: 0, threads: 1 })), []),
  });
  if (holdReplacement) {
    try {
      await poolEntered;
      await vi.advanceTimersByTimeAsync(0);
      expect(children).toHaveLength(case_.count + 1);
      expect(children.flatMap((child) => child.requests).filter((request) => request.operation === "open")).toHaveLength(4 * case_.count + 1);
      expect(children.at(-1)!.exited).toBe(false);
    } finally { releasePool(); }
  }
  const report = await running;
  const basis = report.generationEvidence.filter((record) => record.basisGrepAttempted).length;
  const data = report.executions.filter((event) => event.phase.startsWith("steady-"));
  expect(data).toHaveLength(case_.steady);
  expect(report.executions.filter((event) => event.phase === "serialized-negative-control")).toHaveLength(case_.control);
  expect(report.executions).toHaveLength(case_.steady + case_.control + case_.churn + basis);
  expect(report.windows).toHaveLength(case_.windows);
  expect(report.windows.every((window) => window.readerQueue.count === 2 * case_.acquisitions && window.observationLimitReached === case_.readerLimit)).toBe(true);
  expect(report.churnWindows.map((window) => window.workingSet)).toEqual([4 * case_.count + 1, 8 * case_.count]);
  expect(report.churnWindows.every((window) => Object.keys(window.failures).length === 0)).toBe(true);
  // Files are exercised by the real fixture driver; external writes alone are mocked.
  expect(external.writes).toBe(8 * case_.count * (case_.files + 1) + 6 * (12 * case_.count + 1));
  expect(report.steadyRotations).toBe(0);
  expect(report.generationEvidence.length).toBeGreaterThan(case_.count);
  expect(report.generationEvidence.every((record) => record.grep && record.poolReads.length > 0 && record.poolReads.every((read) => read.observation.status === "complete"))).toBe(true);
  expect(report.generationEvidence.every((record) => record.poolReads.every((read) => read.pid === record.pid && read.bootstrap.nonce === record.bootstrap.nonce && read.grep.requestId === record.grep!.requestId))).toBe(true);
  expect(report.resources.some((row) => row.phase === "shutdown")).toBe(true);
  expect(report.observations.some((event) => event.phase === "resources")).toBe(true);
  expect(report.observations.some((event) => "operation" in event && event.operation === "status")).toBe(true);
  const final = report.observations.filter((event) => "counts" in event).at(-1)!;
  expect("counts" in final && final.counts.chargedAllocations).toBe(0);
  expect(children.every((child) => child.exited)).toBe(true);
  expect(journalOrder).toHaveLength(2 * children.length);
  expect(report.shutdownProved).toBe(true);
  expect(report.predicates.observationComplete).toBe(!case_.readerLimit);
  for (const stream of [report.retention.observations, report.retention.executions, report.retention.resources]) {
    expect(stream.dropped).toBe(0);
    expect(stream.attempted).toBe(stream.retained);
    expect(stream.overflow).toBe(false);
  }
  if (case_.count === 3 && case_.windows === 3)
    expect(report.retention.observations.retained).toBeGreaterThan(100_000);
}, 120_000);

it("latches independent stream overflow and preserves order after later phases", () => {
  const streams = [qualificationStream<string>(2), qualificationStream<string>(1), qualificationStream<string>(3)];
  for (const stream of streams) {
    const capacity = stream.report().capacity;
    for (let i = 0; i <= capacity; i++) stream.append(`phase-${i}`);
    stream.append("shutdown");
    expect(stream.rows).toEqual(Array.from({ length: capacity }, (_, i) => `phase-${i}`));
    expect(stream.report()).toEqual({ capacity, attempted: capacity + 2, retained: capacity, dropped: 2, overflow: true });
  }
  const workload = qualificationWorkload({ count: 3 });
  for (let overflowing = 0; overflowing < 3; overflowing++) {
    const independent = Array.from({ length: 3 }, () => qualificationStream<string>(1));
    independent[overflowing]!.append("steady");
    independent[overflowing]!.append("churn");
    const [observations, executions, resources] = independent.map((stream) => stream.report());
    expect(qualificationRetentionComplete({ workload, elapsedMs: 1, observations: observations!, executions: executions!, resources: resources! })).toBe(false);
  }
  const complete = { overflow: false };
  expect(qualificationRetentionComplete({ workload, elapsedMs: workload.retentionMs + 1, observations: complete, executions: complete, resources: complete })).toBe(false);
});

it.each([
  { windows: 2 }, { windows: 3.5 }, { windows: NaN },
  { acquisitionsPerWindow: 499 }, { acquisitionsPerWindow: 2_001 },
  { filesPerKey: 127 }, { filesPerKey: 2_049 }, { filesPerKey: Infinity },
])("rejects invalid workload before fixture writes ($windows/$acquisitionsPerWindow/$filesPerKey)", async (invalid) => {
  await expect(runNativeQualification({ count: 2, directory: "/synthetic/invalid", command: { file: "/synthetic/node", args: [], env: {}, build: "a".repeat(64) }, ...invalid })).rejects.toThrow("bounded representative envelope");
  expect(external.writes).toBe(0);
  expect(external.mkdirs).toBe(0);
});

it("reserves cadence boundary rows and finite shutdown without declaring journal latency bounded", () => {
  const plan = qualificationWorkload({ count: 3 });
  const stream = qualificationStream<number>(plan.capacities.resources);
  // Independent cadence replay covers the inclusive initial/final boundaries,
  // plus admitted in-flight and checkpoint reads. This supplements the driver test.
  for (let ms = 0; ms <= 617_300; ms += 10) stream.append(ms);
  // N=3 independent replay: 141 possible opens + 48 stable-boundary reads,
  // each with an in-flight boundary, and two final rows. Do not read the
  // expected count back from the capacity's own terms.
  for (let checkpoint = 0; checkpoint < 380; checkpoint++) stream.append(617_300);
  expect(stream.report().overflow).toBe(false);
  expect(stream.report().retained).toBeGreaterThan(60_000);
});
