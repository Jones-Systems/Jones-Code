// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Production harness/supervisor with controlled external child, filesystem, clock and journal boundaries; no native process or fixture files.
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import type { ComputeChild, SlotResult } from "../serverCompute/processSlot.ts";
import type { Bootstrap, Request } from "../serverCompute/protocol.ts";
import type { ComputeObservation } from "./observations.ts";
import type { NativePoolObservation } from "./resourceSamples.ts";
import { processResourceSample } from "./resourceSamples.ts";
import {
  qualificationRetentionComplete,
  qualificationStream,
  qualificationWorkload,
} from "./qualificationRetention.ts";

type StartArgs = Parameters<typeof import("../serverCompute/processSlot.ts").ProcessSlot.start>;

const external = vi.hoisted(() => ({
  start: undefined as unknown as (...args: StartArgs) => Promise<ComputeChild>,
  retired: undefined as unknown as (
    slot: number,
    generation: number,
    nonce: string,
    pid: number,
  ) => Promise<void>,
  pools: undefined as unknown as (pid: number) => Promise<NativePoolObservation>,
  wait: undefined as unknown as (ms: number, signal: AbortSignal) => Promise<void>,
  writes: 0,
  mkdirs: 0,
  readerWindowIncomplete: false,
  reserves: 0,
  registrations: 0,
}));
vi.mock("node:fs/promises", () => ({
  mkdir: async () => {
    external.mkdirs++;
  },
  writeFile: async () => {
    external.writes++;
  },
  rename: async () => {},
  rm: async () => {},
}));
vi.mock("node:perf_hooks", () => ({
  monitorEventLoopDelay: () => ({ enable() {}, disable() {}, percentile: () => 0, max: 0 }),
}));
vi.mock("node:timers/promises", () => ({
  setTimeout: (ms: number, _value: unknown, options: { signal: AbortSignal }) =>
    external.wait(ms, options.signal),
}));
vi.mock("../serverCompute/journal.ts", () => ({
  GenerationJournal: {
    acquire: async () => ({
      claim: { authorityId: "qualification" },
      reserve: async () => {
        external.reserves++;
      },
      registered: async () => {
        external.registrations++;
      },
      release: async () => {},
      retired: (...args: Parameters<typeof external.retired>) => external.retired(...args),
    }),
  },
}));
vi.mock("../serverCompute/processSlot.ts", async (original) => ({
  ...(await original<typeof import("../serverCompute/processSlot.ts")>()),
  ProcessSlot: { start: (...args: StartArgs) => external.start(...args) },
}));
vi.mock("./resourceSamples.ts", async (original) => ({
  ...(await original<typeof import("./resourceSamples.ts")>()),
  sampleMacPerformanceCores: async () => ({
    source: "sysctl:hw.perflevel0.physicalcpu",
    status: "observed",
    value: 4,
    raw: "4",
  }),
  sampleNativePools: (pid: number) => external.pools(pid),
}));
vi.mock("./combinedQualification.ts", async (original) => {
  const actual = await original<typeof import("./combinedQualification.ts")>();
  return {
    ...actual,
    combinedReaderWriterWindow: async (
      _file: string,
      _count: 2 | 3,
      acquisitions: number,
      compute: () => Promise<void>,
      observations: () => readonly ComputeObservation[],
    ) => {
      await compute();
      return {
        readerQueue: actual.percentiles(Array.from({ length: 2 * acquisitions }, () => 1)),
        writer: actual.percentiles([1]),
        control: actual.percentiles([1]),
        computeObservations: observations(),
        observationLimitReached: external.readerWindowIncomplete,
      };
    },
  };
});
import { runNativeQualification } from "./nativeQualification.ts";

let cleanupFixture: (() => Promise<void>) | undefined;

beforeEach(() => {
  if (cleanupFixture) throw new Error("Previous qualification fixture was not joined");
  vi.useFakeTimers({
    toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "performance"],
  });
  external.writes = 0;
  external.mkdirs = 0;
  external.readerWindowIncomplete = false;
  external.reserves = 0;
  external.registrations = 0;
});
afterEach(async () => {
  try {
    await cleanupFixture?.();
  } finally {
    vi.useRealTimers();
    vi.restoreAllMocks();
  }
});

type Outcome<T> = { kind: "fulfilled"; value: T } | { kind: "rejected"; cause: unknown };
type DriverOutcome = Outcome<Awaited<ReturnType<typeof runNativeQualification>>>;
function observed<T>(running: Promise<T>): Promise<Outcome<T>> {
  return running.then(
    (value): Outcome<T> => ({ kind: "fulfilled", value }),
    (cause: unknown): Outcome<T> => ({ kind: "rejected", cause }),
  );
}
function deferred() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}
type GenerationMilestone = {
  kind: "retire" | "journal" | "replacement-pool";
  pid: number;
  bootstrap: Bootstrap;
};
type Milestone =
  | { kind: "stable-wait"; ordinal: number }
  | {
      kind: "startup";
      stage: "entered" | "captured" | "registered";
      pid: number;
      bootstrap: Bootstrap;
      command: StartArgs[0];
    }
  | GenerationMilestone
  | { kind: "sampler-wait"; ordinal: number; signal: AbortSignal }
  | { kind: "collection"; ordinal: number; pids: readonly number[] }
  | { kind: "sampler-abort"; ordinal: number }
  | { kind: "complete"; outcome: DriverOutcome };
type Edge = { milestone: Milestone; release: () => void };
type TraceMilestone =
  | Exclude<Milestone, { kind: "complete" | "sampler-wait" }>
  | { kind: "sampler-wait"; ordinal: number }
  | { kind: "complete"; outcome: DriverOutcome["kind"] };

// Only this controller advances time. Boundary mocks enqueue gates; sampler
// receipts prove a whole iteration drained before exit or durable retirement.
function milestoneController() {
  const queue: Edge[] = [];
  const gates = new Set<() => void>();
  const trace: { at: number; state: "entered" | "released"; milestone: TraceMilestone }[] = [];
  const samplerReceipts: {
    kind: "retire" | "journal";
    pid: number;
    from: number;
    to: number;
    collections: number[];
  }[] = [];
  let changed = deferred();
  let closed = false;
  let advancing = false;
  let stableWaits = 0;
  let samplerWaits = 0;
  let collections = 0;
  let collected = 0;
  let lastCollection: Extract<Milestone, { kind: "collection" }> | undefined;
  let sampler: Edge | undefined;
  let samplerAborted = false;
  const remember = (milestone: Milestone, state: "entered" | "released" = "entered") => {
    const receipt: TraceMilestone =
      milestone.kind === "complete"
        ? { kind: "complete", outcome: milestone.outcome.kind }
        : milestone.kind === "sampler-wait"
          ? { kind: "sampler-wait", ordinal: milestone.ordinal }
          : milestone;
    trace.push({ at: performance.now(), state, milestone: receipt });
    if (trace.length > 64) trace.shift();
  };
  const enqueue = (milestone: Milestone, release: () => void) => {
    remember(milestone);
    queue.push({ milestone, release });
    changed.release();
    changed = deferred();
  };
  const edge = async (milestone: Milestone, signal?: AbortSignal) => {
    if (closed) return;
    const gate = deferred();
    const release = () => {
      signal?.removeEventListener("abort", abort);
      if (gates.delete(release)) remember(milestone, "released");
      gate.release();
    };
    const abort = () => {
      if (milestone.kind === "sampler-wait") {
        samplerAborted = true;
        enqueue({ kind: "sampler-abort", ordinal: milestone.ordinal }, () => {});
      }
      release();
    };
    gates.add(release);
    enqueue(milestone, release);
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    await gate.promise;
  };
  const next = async (driver: Promise<DriverOutcome>): Promise<Edge> => {
    while (queue.length === 0) {
      const terminal = closed
        ? await driver
        : await Promise.race([changed.promise.then(() => undefined), driver]);
      if (terminal) {
        const milestone: Milestone = { kind: "complete", outcome: terminal };
        remember(milestone);
        return { milestone, release: () => {} };
      }
    }
    return queue.shift()!;
  };
  const terminalBefore = (outcome: DriverOutcome, required: string): never => {
    if (outcome.kind === "rejected") throw outcome.cause;
    throw new Error(`Driver completed before ${required}`);
  };
  const advance = async (ms: number) => {
    if (advancing || !Number.isFinite(ms) || ms < 0)
      throw new Error(`Invalid fixture advance: ${ms}`);
    advancing = true;
    try {
      await vi.advanceTimersByTimeAsync(ms);
    } finally {
      advancing = false;
    }
  };
  const acceptSampler = (item: Edge) => {
    if (item.milestone.kind === "collection") {
      collected = item.milestone.ordinal;
      lastCollection = item.milestone;
      item.release();
      return true;
    }
    if (item.milestone.kind === "sampler-abort") {
      samplerAborted = true;
      sampler = undefined;
      item.release();
      return true;
    }
    if (item.milestone.kind === "sampler-wait") {
      if (sampler && !samplerAborted) throw new Error("Overlapping sampler waits");
      sampler = item;
      return true;
    }
    return false;
  };
  const awaitSampler = async (driver: Promise<DriverOutcome>) => {
    while (!sampler) {
      const item = await next(driver);
      if (closed) return;
      if (item.milestone.kind === "complete")
        terminalBefore(item.milestone.outcome, "sampler wait");
      if (!acceptSampler(item))
        throw new Error(`Expected sampler wait, saw ${item.milestone.kind}`);
      if (samplerAborted) throw new Error("Sampler aborted before retirement receipt");
    }
    return sampler;
  };
  const sampleBoundary = async (driver: Promise<DriverOutcome>, boundary: GenerationMilestone) => {
    const waiting = await awaitSampler(driver);
    if (!waiting || closed) return;
    const first = waiting.milestone;
    if (first.kind !== "sampler-wait") throw new Error("Missing sampler identity");
    const before = collected;
    sampler = undefined;
    waiting.release();
    const following = await awaitSampler(driver);
    if (!following || closed) return;
    const last = following.milestone;
    if (last.kind !== "sampler-wait") throw new Error("Missing next sampler identity");
    expect(last.ordinal).toBe(first.ordinal + 1);
    expect(collected).toBeGreaterThan(before);
    expect(lastCollection?.pids.includes(boundary.pid)).toBe(boundary.kind === "retire");
    if (boundary.kind === "replacement-pool") throw new Error("Unexpected sampler boundary");
    samplerReceipts.push({
      kind: boundary.kind,
      pid: boundary.pid,
      from: first.ordinal,
      to: last.ordinal,
      collections: Array.from({ length: collected - before }, (_, i) => before + i + 1),
    });
  };
  return {
    edge,
    trace,
    samplerReceipts,
    get stableWaits() {
      return stableWaits;
    },
    get samplerAborted() {
      return samplerAborted;
    },
    get pendingGates() {
      return gates.size;
    },
    get pendingMilestones() {
      return queue.map((item) => item.milestone.kind);
    },
    wait: async (ms: number, signal: AbortSignal) => {
      if (signal.aborted) throw new Error("aborted");
      if (ms === 3_000) {
        await edge({ kind: "stable-wait", ordinal: ++stableWaits }, signal);
      } else if (ms === 10) {
        await edge({ kind: "sampler-wait", ordinal: ++samplerWaits, signal }, signal);
      } else {
        throw new Error(`Unexpected harness wait: ${ms}`);
      }
      if (ms === 3_000 && signal.aborted) throw new Error("aborted");
    },
    sample: async (pids: readonly number[]) => {
      await edge({ kind: "collection", ordinal: ++collections, pids: [...pids] });
      return processResourceSample(
        pids,
        pids.map((pid) => ({ pid, rssBytes: 1024, cpuMs: 0, threads: 1 })),
        [],
      );
    },
    run: async (driver: Promise<DriverOutcome>, held: (milestone: GenerationMilestone) => void) => {
      let releasedStableWaits = 0;
      while (true) {
        const item = await next(driver);
        const milestone = item.milestone;
        if (milestone.kind === "complete") {
          if (milestone.outcome.kind === "rejected") throw milestone.outcome.cause;
          if (!closed) expect(releasedStableWaits).toBe(2);
          return;
        }
        if (closed || milestone.kind === "startup") {
          item.release();
          continue;
        }
        if (acceptSampler(item)) continue;
        if (milestone.kind === "stable-wait") {
          expect(milestone.ordinal).toBe(releasedStableWaits + 1);
          if (milestone.ordinal > 2) throw new Error("Unexpected stable-wait ordinal");
          await advance(milestone.ordinal === 1 ? 3_000 : 599_750 - performance.now());
          expect(performance.now()).toBe(milestone.ordinal === 1 ? 3_000 : 599_750);
          releasedStableWaits++;
        } else if (milestone.kind === "replacement-pool") {
          const at = performance.now();
          await advance(0);
          expect(performance.now()).toBe(at);
          held(milestone);
        } else if (milestone.kind === "retire" || milestone.kind === "journal") {
          await sampleBoundary(driver, milestone);
          if (!closed)
            await advance(milestone.kind === "journal" && releasedStableWaits === 2 ? 200 : 0);
        } else {
          throw new Error(`Unexpected fixture milestone: ${milestone.kind}`);
        }
        item.release();
      }
    },
    release: () => {
      closed = true;
      for (const release of gates) release();
      changed.release();
    },
  };
}

function fixture(holdReplacement = false, startupFailure?: Error) {
  const clock = milestoneController();
  let firstFailure: { cause: unknown } | undefined;
  let heldPools = 0;
  let heldRequests = 0;
  let starts = 0;
  let captures = 0;
  const boundary = async <T>(work: () => T | Promise<T>): Promise<T> => {
    try {
      return await work();
    } catch (cause) {
      firstFailure ??= { cause };
      throw cause;
    }
  };
  external.wait = (ms, signal) => boundary(() => clock.wait(ms, signal));
  const children: (ComputeChild & {
    exited: boolean;
    requests: Request[];
    entries: Map<string, string>;
    timer: ReturnType<typeof setInterval>;
  })[] = [];
  const journalOrder: string[] = [];
  external.retired = (_slot, _generation, _nonce, pid) =>
    boundary(async () => {
      const child = children.find((child) => child.pid === pid)!;
      expect(child.exited).toBe(true);
      expect([_slot, _generation, _nonce]).toEqual([
        child.bootstrap.slotId,
        child.bootstrap.slotGeneration,
        child.bootstrap.nonce,
      ]);
      journalOrder.push(`exit:${pid}`);
      await clock.edge({ kind: "journal", pid, bootstrap: child.bootstrap });
      journalOrder.push(`journal:${pid}`);
    });
  external.start = async (
    ...[_command, bootstrap, register, onExit, observe, captured]: StartArgs
  ): Promise<ComputeChild> =>
    boundary(async () => {
      if (!observe || !captured)
        throw new Error("Qualification must supply observation and capture callbacks");
      expect(_command).toEqual({
        file: "/synthetic/node",
        args: [],
        env: {},
        build: bootstrap.build,
      });
      expect(bootstrap.authorityId).toBe("qualification");
      const pid = 10_000 + children.length;
      starts++;
      await clock.edge({ kind: "startup", stage: "entered", pid, bootstrap, command: _command });
      if (startupFailure) throw startupFailure;
      const entries = new Map<string, string>();
      const requests: Request[] = [];
      const child = {
        pid,
        bootstrap,
        entries,
        requests,
        exited: false,
        runtimeMetadata: {
          heapLimitBytes: 96 * 1024 * 1024,
          availableParallelism: 4,
          parallelismSource: "node:os.availableParallelism" as const,
        },
        timer: setInterval(
          () =>
            observe({
              phase: "resources",
              operation: "pool",
              slotId: bootstrap.slotId,
              generation: bootstrap.slotGeneration,
              outcome: "ok",
              rssBytes: 1024,
            }),
          1_000,
        ),
        request: async (
          request: Request,
          _signal?: AbortSignal,
          timing?: Parameters<ComputeChild["request"]>[2],
        ): Promise<SlotResult> =>
          boundary(async () => {
            expect(child.exited).toBe(false);
            requests.push(request);
            if (request.operation === "open") {
              const cwd = (request.input as { cwd: string }).cwd;
              entries.set(request.token.keyId, cwd);
              expect(entries.size).toBeLessThanOrEqual(4);
            }
            const cwd = entries.get(request.token.keyId)!;
            const key = /workspace-([0-9]+)$/.exec(cwd)![1]!;
            const items =
              request.operation === "search"
                ? [{ path: "file-0.txt", kind: "file" }]
                : request.operation === "searchContents" &&
                    (request.input as { query: string }).query === "needle"
                  ? [
                      {
                        path: "file-0.txt",
                        lineNumber: 1,
                        lineContent: `needle-${key} alpha beta`,
                        matchRanges: [{ start: 0, end: 6 }],
                      },
                    ]
                  : [];
            const at = performance.now();
            timing?.({
              worker: {
                parentSentMs: at,
                receivedMs: at,
                executionStartedMs: at,
                executionCompletedMs: at,
                executionOutcome: "ok",
                replyStartedMs: at,
                handoffMs: at,
              },
              delivery: {
                lowerMs: 0,
                upperMs: 0,
                durationMs: 0,
                uncertaintyMs: 0,
                offsetLowerMs: 0,
                offsetUpperMs: 0,
              },
            });
            return {
              items,
              final: {
                type: "done",
                truncated: false,
                status: { scanning: false, warmupComplete: true },
              },
            };
          }),
        retire: () =>
          boundary(async () => {
            await clock.edge({ kind: "retire", pid, bootstrap });
            child.exited = true;
            onExit(new Error("captured intentional exit"));
            clearInterval(child.timer);
          }),
      };
      children.push(child);
      captured(child);
      captures++;
      await clock.edge({ kind: "startup", stage: "captured", pid, bootstrap, command: _command });
      await register(pid);
      await clock.edge({ kind: "startup", stage: "registered", pid, bootstrap, command: _command });
      return child;
    });
  external.pools = (pid) =>
    boundary(async () => {
      const child = children.find((child) => child.pid === pid)!;
      expect(child.exited).toBe(false);
      if (holdReplacement && heldPools === 0 && child.bootstrap.slotGeneration > 1) {
        heldPools++;
        heldRequests = children.reduce((total, child) => total + child.requests.length, 0);
        await clock.edge({ kind: "replacement-pool", pid, bootstrap: child.bootstrap });
        expect(child.exited).toBe(false);
      }
      expect(
        child.requests.some(
          (request) =>
            request.operation === "searchContents" &&
            (request.input as { query?: string }).query === "needle",
        ),
      ).toBe(true);
      return {
        pid,
        method: process.platform === "linux" ? "linux-proc-task-comm" : "macos-sample",
        status: "complete",
        threads: [
          { tid: "1", name: "fff-bg-0" },
          { tid: "2", name: "fff-bg-1" },
          ...Array.from({ length: 4 }, (_, i) => ({ tid: String(3 + i), name: `fff-search-${i}` })),
        ],
        raw: "controlled external OS boundary",
      };
    });
  return {
    children,
    journalOrder,
    clock,
    get heldPools() {
      return heldPools;
    },
    get heldRequests() {
      return heldRequests;
    },
    get starts() {
      return starts;
    },
    get captures() {
      return captures;
    },
    finish: async (
      driver: Promise<DriverOutcome>,
      controlling: Promise<Outcome<void>>,
      signal: AbortController,
      failure?: { cause: unknown },
    ) => {
      clock.release();
      signal.abort();
      const [result, controlled] = await Promise.all([driver, controlling]);
      for (const child of children) clearInterval(child.timer);
      cleanupFixture = undefined;
      const failures = new Set<unknown>();
      if (firstFailure) failures.add(firstFailure.cause);
      if (failure) failures.add(failure.cause);
      if (result.kind === "rejected") failures.add(result.cause);
      if (controlled.kind === "rejected") failures.add(controlled.cause);
      if (clock.pendingGates !== 0) failures.add(new Error("Unreleased fixture gates"));
      if (failures.size) {
        const requests: Partial<Record<Request["operation"], number>> = {};
        for (const child of children)
          for (const request of child.requests)
            requests[request.operation] = (requests[request.operation] ?? 0) + 1;
        throw new AggregateError(
          [...failures],
          `Qualification fixture failed; ${JSON.stringify({
            starts,
            captures,
            reserves: external.reserves,
            registrations: external.registrations,
            requests,
            heldPools,
            milestones: clock.trace,
          })}`,
        );
      }
    },
  };
}

// These literals come from the workload's alternating path/content keys, not
// from retention formulas: path=1 operation, content=positive+absent, refresh=1.
it.each([
  {
    count: 2 as const,
    windows: 3,
    acquisitions: 500,
    files: 512,
    steady: 4_500,
    control: 1_500,
    churn: 204,
    readerLimit: false,
  },
  {
    count: 3 as const,
    windows: 3,
    acquisitions: 500,
    files: 512,
    steady: 6_750,
    control: 2_250,
    churn: 300,
    readerLimit: false,
  },
  {
    count: 2 as const,
    windows: 5,
    acquisitions: 2_000,
    files: 2_048,
    steady: 30_000,
    control: 6_000,
    churn: 204,
    readerLimit: true,
  },
  {
    count: 3 as const,
    windows: 5,
    acquisitions: 2_000,
    files: 2_048,
    steady: 45_000,
    control: 9_000,
    churn: 300,
    readerLimit: false,
  },
])(
  "retains the production workload through every churn generation and shutdown ($count/$windows/$acquisitions)",
  async (case_) => {
    const holdReplacement = case_.count === 2 && case_.windows === 3;
    const owned = fixture(holdReplacement);
    const { children, journalOrder, clock } = owned;
    const signal = new AbortController();
    external.readerWindowIncomplete = case_.readerLimit;
    const driver = observed(
      runNativeQualification({
        count: case_.count,
        directory: "/synthetic/qualification",
        command: { file: "/synthetic/node", args: [], env: {}, build: "a".repeat(64) },
        windows: case_.windows,
        acquisitionsPerWindow: case_.acquisitions,
        filesPerKey: case_.files,
        signal: signal.signal,
        sample: clock.sample,
      }),
    );
    const controlling = observed(
      clock.run(driver, (milestone) => {
        expect(children).toHaveLength(case_.count + 1);
        expect(
          children
            .flatMap((child) => child.requests)
            .filter((request) => request.operation === "open"),
        ).toHaveLength(4 * case_.count + 1);
        expect(children.at(-1)!.pid).toBe(milestone.pid);
        expect(children.at(-1)!.bootstrap).toEqual(milestone.bootstrap);
        expect(children.at(-1)!.exited).toBe(false);
        expect(children.reduce((total, child) => total + child.requests.length, 0)).toBe(
          owned.heldRequests,
        );
        expect(clock.pendingMilestones).not.toContain("retire");
        expect(clock.pendingMilestones).not.toContain("journal");
        expect(clock.pendingMilestones).not.toContain("startup");
      }),
    );
    cleanupFixture = () => owned.finish(driver, controlling, signal);
    let failure: { cause: unknown } | undefined;
    try {
      const controlled = await controlling;
      if (controlled.kind === "rejected") throw controlled.cause;
      const result = await driver;
      if (result.kind === "rejected") throw result.cause;
      const report = result.value;
      expect(owned.heldPools).toBe(holdReplacement ? 1 : 0);
      expect(owned.starts).toBe(children.length);
      expect(owned.captures).toBe(children.length);
      expect(external.reserves).toBe(children.length);
      expect(external.registrations).toBe(children.length);
      expect(clock.samplerAborted).toBe(true);
      expect(clock.samplerReceipts).toHaveLength(2 * children.length);
      for (const child of children) {
        expect(
          clock.samplerReceipts
            .filter((receipt) => receipt.pid === child.pid)
            .map((receipt) => receipt.kind),
        ).toEqual(["retire", "journal"]);
      }
      const basis = report.generationEvidence.filter((record) => record.basisGrepAttempted).length;
      const data = report.executions.filter((event) => event.phase.startsWith("steady-"));
      expect(data).toHaveLength(case_.steady);
      expect(
        report.executions.filter((event) => event.phase === "serialized-negative-control"),
      ).toHaveLength(case_.control);
      expect(report.executions).toHaveLength(case_.steady + case_.control + case_.churn + basis);
      expect(report.windows).toHaveLength(case_.windows);
      expect(
        report.windows.every(
          (window) =>
            window.readerQueue.count === 2 * case_.acquisitions &&
            window.observationLimitReached === case_.readerLimit,
        ),
      ).toBe(true);
      expect(report.churnWindows.map((window) => window.workingSet)).toEqual([
        4 * case_.count + 1,
        8 * case_.count,
      ]);
      expect(report.churnWindows.every((window) => Object.keys(window.failures).length === 0)).toBe(
        true,
      );
      // Files are exercised by the real fixture driver; external writes alone are mocked.
      expect(external.writes).toBe(
        8 * case_.count * (case_.files + 1) + 6 * (12 * case_.count + 1),
      );
      expect(report.steadyRotations).toBe(0);
      expect(report.generationEvidence.length).toBeGreaterThan(case_.count);
      expect(
        report.generationEvidence.every(
          (record) =>
            record.grep &&
            record.poolReads.length > 0 &&
            record.poolReads.every((read) => read.observation.status === "complete"),
        ),
      ).toBe(true);
      expect(
        report.generationEvidence.every((record) =>
          record.poolReads.every(
            (read) =>
              read.pid === record.pid &&
              read.bootstrap.nonce === record.bootstrap.nonce &&
              read.grep.requestId === record.grep!.requestId,
          ),
        ),
      ).toBe(true);
      expect(report.resources.some((row) => row.phase === "shutdown")).toBe(true);
      expect(report.observations.some((event) => event.phase === "resources")).toBe(true);
      expect(
        report.observations.some((event) => "operation" in event && event.operation === "status"),
      ).toBe(true);
      const final = report.observations.filter((event) => "counts" in event).at(-1)!;
      expect("counts" in final && final.counts.chargedAllocations).toBe(0);
      expect(children.every((child) => child.exited)).toBe(true);
      expect(journalOrder).toHaveLength(2 * children.length);
      for (const child of children) {
        const exit = journalOrder.indexOf(`exit:${child.pid}`);
        expect(exit).toBeGreaterThanOrEqual(0);
        expect(journalOrder[exit + 1]).toBe(`journal:${child.pid}`);
      }
      expect(report.shutdownProved).toBe(true);
      expect(report.predicates.observationComplete).toBe(!case_.readerLimit);
      for (const stream of [
        report.retention.observations,
        report.retention.executions,
        report.retention.resources,
      ]) {
        expect(stream.dropped).toBe(0);
        expect(stream.attempted).toBe(stream.retained);
        expect(stream.overflow).toBe(false);
      }
      if (case_.count === 3 && case_.windows === 3)
        expect(report.retention.observations.retained).toBeGreaterThan(100_000);
    } catch (cause) {
      failure = { cause };
    } finally {
      await owned.finish(driver, controlling, signal, failure);
    }
  },
  120_000,
);

it("observes startup rejection before the replacement-pool milestone without advancing time", async () => {
  const startupFailure = new Error("controlled startup rejection");
  const owned = fixture(true, startupFailure);
  const signal = new AbortController();
  const driver = observed(
    runNativeQualification({
      count: 2,
      directory: "/synthetic/startup-rejection",
      command: { file: "/synthetic/node", args: [], env: {}, build: "a".repeat(64) },
      signal: signal.signal,
      sample: owned.clock.sample,
    }),
  );
  const controlling = observed(
    owned.clock.run(driver, () => {
      throw new Error("Startup rejection must precede replacement-pool entry");
    }),
  );
  cleanupFixture = () => owned.finish(driver, controlling, signal);
  let failure: { cause: unknown } | undefined;
  try {
    const controlled = await controlling;
    const terminal = await driver;
    expect(controlled.kind).toBe("rejected");
    expect(terminal.kind).toBe("rejected");
    expect(owned.heldPools).toBe(0);
    expect(owned.children).toHaveLength(0);
    expect(owned.starts).toBe(1);
    expect(owned.captures).toBe(0);
    expect(external.reserves).toBe(1);
    expect(external.registrations).toBe(0);
    expect(owned.clock.stableWaits).toBe(0);
    expect(performance.now()).toBe(0);
  } catch (cause) {
    failure = { cause };
  } finally {
    let combined: unknown;
    try {
      await owned.finish(driver, controlling, signal, failure);
    } catch (cause) {
      combined = cause;
    }
    if (!(combined instanceof AggregateError))
      throw new Error("Expected joined startup and shutdown failures");
    expect(combined.errors).toContain(startupFailure);
    const terminal = await driver;
    if (terminal.kind !== "rejected") throw new Error("Missing terminal startup failure");
    expect(combined.errors).toContain(terminal.cause);
    expect(terminal.cause).not.toBe(startupFailure);
    expect(owned.clock.pendingGates).toBe(0);
    expect(owned.clock.samplerAborted).toBe(true);
    if (failure) throw combined;
  }
});

it("latches independent stream overflow and preserves order after later phases", () => {
  const streams = [
    qualificationStream<string>(2),
    qualificationStream<string>(1),
    qualificationStream<string>(3),
  ];
  for (const stream of streams) {
    const capacity = stream.report().capacity;
    for (let i = 0; i <= capacity; i++) stream.append(`phase-${i}`);
    stream.append("shutdown");
    expect(stream.rows).toEqual(Array.from({ length: capacity }, (_, i) => `phase-${i}`));
    expect(stream.report()).toEqual({
      capacity,
      attempted: capacity + 2,
      retained: capacity,
      dropped: 2,
      overflow: true,
    });
  }
  const workload = qualificationWorkload({ count: 3 });
  for (let overflowing = 0; overflowing < 3; overflowing++) {
    const independent = Array.from({ length: 3 }, () => qualificationStream<string>(1));
    independent[overflowing]!.append("steady");
    independent[overflowing]!.append("churn");
    const [observations, executions, resources] = independent.map((stream) => stream.report());
    expect(
      qualificationRetentionComplete({
        workload,
        elapsedMs: 1,
        observations: observations!,
        executions: executions!,
        resources: resources!,
      }),
    ).toBe(false);
  }
  const complete = { overflow: false };
  expect(
    qualificationRetentionComplete({
      workload,
      elapsedMs: workload.retentionMs + 1,
      observations: complete,
      executions: complete,
      resources: complete,
    }),
  ).toBe(false);
});

it.each([
  { windows: 2 },
  { windows: 3.5 },
  { windows: NaN },
  { acquisitionsPerWindow: 499 },
  { acquisitionsPerWindow: 2_001 },
  { filesPerKey: 127 },
  { filesPerKey: 2_049 },
  { filesPerKey: Infinity },
])(
  "rejects invalid workload before fixture writes ($windows/$acquisitionsPerWindow/$filesPerKey)",
  async (invalid) => {
    await expect(
      runNativeQualification({
        count: 2,
        directory: "/synthetic/invalid",
        command: { file: "/synthetic/node", args: [], env: {}, build: "a".repeat(64) },
        ...invalid,
      }),
    ).rejects.toThrow("bounded representative envelope");
    expect(external.writes).toBe(0);
    expect(external.mkdirs).toBe(0);
  },
);

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
