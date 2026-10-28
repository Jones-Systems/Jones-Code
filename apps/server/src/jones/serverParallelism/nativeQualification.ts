// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Qualified synthetic harness owns every workspace, journal and captured child.
import { mkdir, writeFile, rename, rm } from "node:fs/promises";
import { join } from "node:path";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { setTimeout as pause } from "node:timers/promises";
import { ComputeSupervisor } from "../serverCompute/supervisor.ts";
import { GenerationJournal } from "../serverCompute/journal.ts";
import { isKey } from "../serverCompute/protocol.ts";
import { ProcessSlot } from "../serverCompute/processSlot.ts";
import type { ComputeChild } from "../serverCompute/processSlot.ts";
import type { WorkerCommand } from "../serverCompute/workerCommand.ts";
import type { LogicalFinderHandle } from "./computeContract.ts";
import type { ComputeObservation } from "./observations.ts";
import { sampleProcesses, processResourceSample, resourcePredicates, type ProcessResourceSample } from "./resourceSamples.ts";
import { combinedReaderWriterWindow, percentiles, summarizeComputeObservations } from "./combinedQualification.ts";

export interface NativeQualificationOptions {
  readonly count: 2 | 3;
  readonly directory: string;
  readonly command: WorkerCommand;
  readonly filesPerKey?: number;
  readonly acquisitionsPerWindow?: number;
  readonly windows?: number;
  readonly baselineWriterP99Ms?: number;
  readonly baselineWriterP95Ms?: number;
  readonly baselineLoopP99Ms?: number;
  readonly baselineChurn?: readonly { readonly workingSet: number; readonly durationMs: number; readonly latency: { readonly p99: number | null } }[];
  readonly sample?: typeof sampleProcesses;
  readonly signal?: AbortSignal;
  readonly onShutdown?: () => void;
  readonly onProductiveOverlap?: (pids: readonly number[]) => void;
}

/** Runs real pinned finders in N productive children; no timer/fake CPU workers.
 * Failure to observe productive overlap is an explicit failed predicate.
 */
export async function runNativeQualification(options: NativeQualificationOptions) {
  const { count, directory, command } = options;
  const windows = options.windows ?? 3, acquisitions = options.acquisitionsPerWindow ?? 500;
  const files = options.filesPerKey ?? 512;
  if (![windows, acquisitions, files].every(Number.isSafeInteger) || ![2, 3].includes(count) || windows < 3 || windows > 5 || acquisitions < 500 || acquisitions > 2_000 || files < 128 || files > 2_048)
    throw new Error("Qualification workload outside bounded representative envelope");
  await mkdir(directory, { recursive: false, mode: 0o700 });
  const observations: ComputeObservation[] = [];
  const observe = (event: ComputeObservation) => {
    if (observations.length < 100_000) observations.push(event); else observationLimitReached = true;
  };
  const captured = new Map<number, ComputeChild>();
  const active = new Map<number, number>();
  const keySlots = new Map<string, number>();
  const keyIdentity = (cwd: string, variant: string) => JSON.stringify([cwd, variant]);
  const resources: { at: number; finishedAt: number; phase: string; sample: ProcessResourceSample; active: number[] }[] = [];
  const executions: { pid: number; phase: string; start: number; end: number }[] = [];
  let stopped = false, samplerFailure = false, observationLimitReached = false;
  let productiveOverlapSamples = 0, rotations = 0, coldReopens = 0;
  let phase = "fixture";
  const parentCpuStart = process.cpuUsage(), mainThreadCpuStart = process.threadCpuUsage();
  const requestMs: number[] = [];
  const journal = await GenerationJournal.acquire(join(directory, "journal"), count);
  const supervisor = new ComputeSupervisor({ count, build: command.build, journal,
    observe,
    start: async (boot, register, onExit) => {
      const child = await ProcessSlot.start(command, boot, register, onExit, observe);
      if (boot.slotGeneration > 1) rotations++;
      captured.set(child.pid, child);
      return { pid: child.pid, bootstrap: child.bootstrap,
        request: async (request, signal, observeTiming) => {
          if (request.operation === "open") {
            coldReopens++;
            if (isKey(request.input)) keySlots.set(keyIdentity(request.input.cwd, request.input.variant), request.token.slotId);
          }
          const data = !["open", "close", "status"].includes(request.operation);
          if (data) active.set(child.pid, (active.get(child.pid) ?? 0) + 1);
          const start = performance.now();
          try { return await child.request(request, signal, (timing) => {
            observeTiming?.(timing);
            if (data && timing.worker && timing.delivery && timing.worker.executionOutcome === "ok") {
              if (executions.length < 100_000) executions.push({ pid: child.pid, phase,
                start: timing.worker.executionStartedMs + timing.delivery.offsetUpperMs,
                end: timing.worker.executionCompletedMs + timing.delivery.offsetLowerMs });
              else observationLimitReached = true;
            }
          }); }
          finally {
            if (data) {
              active.set(child.pid, (active.get(child.pid) ?? 1) - 1);
              requestMs.push(performance.now() - start);
            }
          }
        },
        retire: async () => { await child.retire(); captured.delete(child.pid); active.delete(child.pid); },
      };
    } });
  const keys: LogicalFinderHandle[] = [];
  const monitor = monitorEventLoopDelay({ resolution: 10 });
  monitor.enable();
  const controller = new AbortController();
  const abort = () => controller.abort();
  options.signal?.addEventListener("abort", abort, { once: true });
  if (options.signal?.aborted) controller.abort();
  const deadline = setTimeout(abort, 10 * 60_000);
  const sampler = (async () => {
    let previous: typeof resources[number] | undefined;
    while (!stopped) {
      const before = new Set([...active].filter(([, jobs]) => jobs > 0).map(([pid]) => pid));
      const requested = [process.pid, ...captured.keys()], at = performance.now(), sampledPhase = phase;
      let sample: ProcessResourceSample;
      try { sample = await (options.sample ?? sampleProcesses)(requested); }
      catch { sample = processResourceSample(requested, [], ["sampler-threw"]); }
      if (sample.status !== "complete" || sample.requestedPids.length !== requested.length ||
          requested.some((pid) => !sample.requestedPids.includes(pid))) samplerFailure = true;
      const stillActive = [...before].filter((pid) => (active.get(pid) ?? 0) > 0);
      if (resources.length < 60_000) resources.push({ at, finishedAt: performance.now(), phase: sampledPhase, sample, active: stillActive });
      else observationLimitReached = true;
      // A live receipt is only a parent-loss trigger. Final productive acceptance
      // additionally requires worker execution intervals and positive CPU deltas.
      if (sample.status === "complete" && previous?.sample.status === "complete" && stillActive.length === count && stillActive.every((pid) => {
        const old = previous!.sample.processes.find((row) => row.pid === pid), now = sample.processes.find((row) => row.pid === pid);
        return old && now && now.cpuMs > old.cpuMs;
      })) options.onProductiveOverlap?.(stillActive);
      previous = resources.at(-1);
      // Measurement cadence only. Progress assertions use jobs/CPU and captured exit, never this delay.
      await pause(10, undefined, { signal: controller.signal }).catch(() => {});
      if (controller.signal.aborted) return;
    }
  })();
  let shutdownProved = false;
  try {
    // All 8N keys are synthetic and bounded. The first 4N stay resident together.
    for (let key = 0; key < 8 * count; key++) {
      const cwd = join(directory, `workspace-${key}`);
      await mkdir(cwd);
      await writeFile(join(cwd, ".gitignore"), "ignored/\n");
      const content = `needle-${key} alpha beta\n`.repeat(256);
      for (let file = 0; file < files; file++) await writeFile(join(cwd, `file-${file}.txt`), content);
      keys.push(supervisor.register({ cwd, variant: key % 2 === 0 ? "paths" : "content" }));
    }
    const request = async (handle: LogicalFinderHandle) => {
      if (controller.signal.aborted) throw new Error("Qualification deadline");
      if (handle.key.variant === "content") {
        const result = await supervisor.run(handle, "searchContents", { query: "needle", limit: 32,
          caseSensitive: true, wholeWord: false, useRegex: false }, controller.signal);
        if (result.matches.length === 0) throw new Error("Native content search did no useful work");
        // Full absent-match traversal prevents a first-file hit from being the
        // only productive sample; this uses the existing native grep algorithm.
        const absent = await supervisor.run(handle, "searchContents", { query: "qualification-absent-marker", limit: 32,
          caseSensitive: true, wholeWord: false, useRegex: false }, controller.signal);
        if (absent.matches.length !== 0) throw new Error("Synthetic absent marker unexpectedly matched");
      } else {
        const result = await supervisor.run(handle, "search", { query: "file", limit: 32 }, controller.signal);
        if (result.entries.length === 0) throw new Error("Native path search did no useful work");
      }
    };
    phase = "cold-all-resident";
    for (const handle of keys.slice(0, 4 * count)) await supervisor.ensure(handle, controller.signal);
    const warmCounts = observations.filter((event) => "counts" in event).at(-1);
    if (!warmCounts || !("counts" in warmCounts) || warmCounts.counts.physicalEntries !== 4 * count)
      throw new Error("All-resident native cohort was not constructed");
    const cohorts = Array.from({ length: count }, (_, slot) => keys.slice(0, 4 * count).filter((handle) =>
      keySlots.get(keyIdentity(handle.key.cwd, handle.key.variant)) === slot));
    if (cohorts.some((cohort) => cohort.length !== 4)) throw new Error("Incomplete native process cohort mapping");
    phase = "warm-idle";
    await pause(3_000, undefined, { signal: controller.signal });
    const windowsResult = [];
    const steadyRotationStart = rotations;
    for (let window = 0; window < windows; window++) {
      phase = `steady-${window}`;
      const loop = monitorEventLoopDelay({ resolution: 10 }); loop.enable();
      const observationStart = observations.length;
      try {
        const result = await combinedReaderWriterWindow(join(directory, `combined-${window}.sqlite`), count, acquisitions,
          async () => {
            // Continuous useful work on every child; all resident keys participate.
            await Promise.all(Array.from({ length: count }, async (_, lane) => {
              for (let round = 0; round < acquisitions; round++) await request(cohorts[lane]![round % 4]!);
            }));
          }, () => observations.slice(observationStart));
        windowsResult.push({ ...result, loop: { p99: loop.percentile(99) / 1e6, max: loop.max / 1e6 } });
      } finally { loop.disable(); }
    }
    const steadyRotations = rotations - steadyRotationStart;
    phase = "serialized-negative-control";
    const serializedExecutionStart = executions.length;
    for (let round = 0; round < acquisitions; round++) for (const cohort of cohorts) await request(cohort[round % 4]!);
    const serializedExecutions = executions.slice(serializedExecutionStart);
    const churnWindows = [];
    for (const workingSet of [4 * count + 1, 8 * count]) {
      phase = `churn-${workingSet}`;
      const started = performance.now(), beforeRotations = rotations, beforeReopens = coldReopens;
      const latency: number[] = [], failures: Record<string, number> = {};
      for (let round = 0; round < 3; round++) {
        for (const handle of keys.slice(0, workingSet)) {
          const begin = performance.now();
          try {
            await request(handle);
            // Native filesystem watcher churn and ignored-file changes; actual refresh follows.
            const changed = join(handle.key.cwd, "churn.txt"), moved = join(handle.key.cwd, "renamed.txt");
            await writeFile(changed, "needle churn\n".repeat(128)); await rename(changed, moved);
            await mkdir(join(handle.key.cwd, "ignored"), { recursive: true });
            await writeFile(join(handle.key.cwd, "ignored", "hidden.txt"), "needle ignored");
            await supervisor.run(handle, "refresh", undefined, controller.signal);
            await rm(moved);
          } catch (cause) {
            const reason = typeof cause === "object" && cause !== null && "reason" in cause ? String(cause.reason) : "unknown";
            failures[reason] = (failures[reason] ?? 0) + 1;
          }
          latency.push(performance.now() - begin);
        }
        // Hot sibling traffic through surviving cached facades after rotation.
        for (const hot of keys.slice(0, count)) await request(hot);
      }
      churnWindows.push({ workingSet, durationMs: performance.now() - started, latency: percentiles(latency), failures,
        rotations: rotations - beforeRotations, demandReopens: coldReopens - beforeReopens });
    }
    phase = "churn-settling";
    await pause(3_000, undefined, { signal: controller.signal });
    phase = "shutdown";
    await supervisor.shutdown(); shutdownProved = true; options.onShutdown?.();
    stopped = true; controller.abort(); await sampler; monitor.disable();
    const resourceResult = resourcePredicates(resources.map((row) => row.sample), process.pid, count);
    const { childRssWithinBudget } = resourceResult;
    // A whole OS sampling interval must be contained by a conservative worker
    // execution interval for every child. Clock uncertainty narrows, never widens, proof.
    for (let i = 1; i < resources.length; i++) {
      const before = resources[i - 1]!, now = resources[i]!;
      if (!now.phase.startsWith("steady-") || before.phase !== now.phase ||
          before.sample.status !== "complete" || now.sample.status !== "complete") continue;
      const children = now.sample.processes.filter((row) => row.pid !== process.pid);
      if (children.length === count && children.every((row) => {
        const old = before.sample.processes.find((item) => item.pid === row.pid);
        return old && row.cpuMs > old.cpuMs && executions.some((interval) => interval.pid === row.pid &&
          interval.phase === now.phase && interval.start <= before.at && interval.end >= now.finishedAt);
      })) productiveOverlapSamples++;
    }
    const overlaps = (events: typeof executions) => events.some((left) => events.some((right) =>
      right.pid !== left.pid && Math.max(left.start, right.start) < Math.min(left.end, right.end)));
    const serializedNegativeControl = serializedExecutions.length >= count * acquisitions && !overlaps(serializedExecutions);
    const parentCpu = process.cpuUsage(parentCpuStart), mainThreadCpu = process.threadCpuUsage(mainThreadCpuStart);
    const queues = windowsResult.map((window) => window.readerQueue);
    const normalTargets = windowsResult.length >= 3 && windowsResult.every((window) => window.readerQueue.count >= 500 && (window.readerQueue.p95 ?? Infinity) <= 10 &&
      (window.readerQueue.p99 ?? Infinity) <= 50 && window.loop.p99 <= 50 && window.loop.max <= 100);
    const writerTarget = options.baselineWriterP99Ms === undefined ? "unknown" :
      windowsResult.every((window) => (window.writer.p99 ?? Infinity) <= Math.max(50, 1.2 * options.baselineWriterP99Ms!));
    const writerP95Target = options.baselineWriterP95Ms === undefined ? "unknown" :
      windowsResult.every((window) => (window.writer.p95 ?? Infinity) <= 1.1 * options.baselineWriterP95Ms!);
    const loopComparison = options.baselineLoopP99Ms === undefined ? "unknown" :
      windowsResult.every((window) => window.loop.p99 <= options.baselineLoopP99Ms!);
    const lifecycleSamples = observations.flatMap((event) => "counts" in event ? [event.counts] : []);
    const lifecycleBounds = lifecycleSamples.length === 0 ? "unknown" : lifecycleSamples.every((counts) =>
      counts.chargedAllocations <= 4 * count && counts.physicalEntries <= 4 * count && counts.retiringGenerations <= count);
    const resourceCoverage = { samples: resources.length,
      missingPidSamples: resources.filter((row) => row.sample.missingPids.length > 0).length,
      threadCountsAvailable: resourceResult.resourcesComplete && resources.some((row) => row.sample.processes.length > 1) &&
        resources.every((row) => row.sample.processes.every((item) => item.threads !== undefined)),
      watchCountsAvailable: resourceResult.resourcesComplete && resources.some((row) => row.sample.processes.length > 1) &&
        resources.every((row) => row.sample.processes.every((item) => item.inotifyWatches !== undefined)),
      nativeTailInternalQueues: "unknown", childAvailableParallelism: "unknown" };
    const churnThroughput = options.baselineChurn?.length === 2 ? churnWindows.every((window) => {
      const baseline = options.baselineChurn!.find((item) => item.workingSet === window.workingSet);
      return baseline && Number.isFinite(baseline.durationMs) && baseline.durationMs > 0 &&
        window.durationMs <= 1.1 * baseline.durationMs && (window.latency.p99 ?? Infinity) <= 1.1 * (baseline.latency.p99 ?? 0);
    }) : "unknown";
    return { schema: "jones.server-parallelism.native-qualification/v1", build: command.build, count,
      windows: windowsResult, churnWindows, queues, request: percentiles(requestMs), resources,
      productiveOverlapSamples, executions, resourceCoverage, serializedNegativeControl, steadyRotations, rotations, shutdownProved, parentCpu, mainThreadCpu,
      resourceMeaning: "parentCpu includes reader/checkpoint threads; mainThreadCpu is the serving harness thread; child CPU/RSS are separate samples",
      childRssWithinBudget,
      predicates: { productiveOverlap: productiveOverlapSamples > 0, serializedNegativeControl, normalTargets, writerTarget, writerP95Target, loopComparison,
        shutdownProved, lifecycleBounds,
        guardianResourcesObserved: observations.some((event) => event.phase === "resources") ? true : "unknown", noSteadyChurn: steadyRotations === 0,
        churnFailuresAbsent: churnWindows.length === 2 && churnWindows.every((window) => Object.keys(window.failures).length === 0),
        resourcesComplete: resourceResult.resourcesComplete && !samplerFailure && !observationLimitReached,
        observationComplete: !observationLimitReached && windowsResult.every((window) => !window.observationLimitReached),
        // Resource ceilings and throughput regression need exact-host interpretation against baseline.
        aggregateResourceBudget: "unknown", childRssWithinBudget, churnThroughput },
      observations, computePhases: summarizeComputeObservations(observations),
      guardianRss: observations.filter((event) => event.phase === "resources"),
      guardianRssAvailability: observations.some((event) => event.phase === "resources") ? "observed" : "unavailable",
      timingMeaning: "execute is child-local; reply is chunk preparation/delivery before terminal encoding; reply_delivery is an interval estimate including terminal encoding, with per-exchange clock uncertainty; null is unavailable",
      missingCapabilities: ["full native-tail phase/pending queues",
        "child availableParallelism readback through ProcessSlot",
        ...(process.platform === "darwin" ? ["kernel watcher counts (OS-specific)"] : [])] };
  } finally {
    clearTimeout(deadline); stopped = true; controller.abort(); await sampler; monitor.disable();
    options.signal?.removeEventListener("abort", abort);
    if (!shutdownProved) { await supervisor.shutdown(); options.onShutdown?.(); }
    // Fixture removal is the invoking harness owner's duty after reading the report.
    // Uncertain retirement must preserve the journal and fixture for reconciliation.
  }
}
