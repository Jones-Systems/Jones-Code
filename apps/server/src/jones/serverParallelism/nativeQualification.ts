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
import { sampleProcesses, type ProcessResources } from "./resourceSamples.ts";
import { combinedReaderWriterWindow, percentiles, summarizeComputeObservations } from "./combinedQualification.ts";

export interface NativeQualificationOptions {
  readonly count: 2 | 3;
  readonly directory: string;
  readonly command: WorkerCommand;
  readonly filesPerKey?: number;
  readonly acquisitionsPerWindow?: number;
  readonly windows?: number;
  readonly baselineWriterP99Ms?: number;
  readonly onProductiveOverlap?: (pids: readonly number[]) => void;
}

/** Runs real pinned finders in N productive children; no timer/fake CPU workers.
 * Failure to observe productive overlap is an explicit failed predicate.
 */
export async function runNativeQualification(options: NativeQualificationOptions) {
  const { count, directory, command } = options;
  const windows = options.windows ?? 3, acquisitions = options.acquisitionsPerWindow ?? 500;
  const files = options.filesPerKey ?? 512;
  if (windows < 3 || windows > 5 || acquisitions < 500 || acquisitions > 2_000 || files < 128 || files > 2_048)
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
  const resources: { at: number; phase: string; processes: ProcessResources[]; active: number[] }[] = [];
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
          try { return await child.request(request, signal, observeTiming); }
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
  const deadline = setTimeout(() => controller.abort(), 10 * 60_000);
  const sampler = (async () => {
    let previous: ProcessResources[] = [];
    while (!stopped) {
      const before = new Set([...active].filter(([, jobs]) => jobs > 0).map(([pid]) => pid));
      try {
        const processes = await sampleProcesses([process.pid, ...captured.keys()]);
        const stillActive = [...before].filter((pid) => (active.get(pid) ?? 0) > 0);
        if (stillActive.length === count && stillActive.every((pid) => {
          const old = previous.find((sample) => sample.pid === pid), now = processes.find((sample) => sample.pid === pid);
          return old && now && now.cpuMs > old.cpuMs;
        })) {
          productiveOverlapSamples++;
          options.onProductiveOverlap?.(stillActive);
        }
        if (resources.length < 6_000) resources.push({ at: performance.now(), phase, processes, active: stillActive });
        else observationLimitReached = true;
        previous = processes;
      } catch { samplerFailure = true; }
      // Measurement cadence only. Progress assertions use jobs/CPU and captured exit, never this delay.
      await pause(100, undefined, { signal: controller.signal }).catch(() => {});
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
    await supervisor.shutdown(); shutdownProved = true;
    const childRssWithinBudget = resources.every((sample) => {
      const children = sample.processes.filter((item) => item.pid !== process.pid);
      return children.every((item) => item.rssBytes <= 512 * 1024 * 1024) &&
        children.reduce((total, item) => total + item.rssBytes, 0) <= count * 512 * 1024 * 1024;
    });
    const parentCpu = process.cpuUsage(parentCpuStart), mainThreadCpu = process.threadCpuUsage(mainThreadCpuStart);
    const queues = windowsResult.map((window) => window.readerQueue);
    const normalTargets = windowsResult.every((window) => (window.readerQueue.p95 ?? Infinity) <= 10 &&
      (window.readerQueue.p99 ?? Infinity) <= 50 && window.loop.p99 <= 50 && window.loop.max <= 100);
    const writerTarget = options.baselineWriterP99Ms === undefined ? "unknown" :
      windowsResult.every((window) => (window.writer.p99 ?? Infinity) <= Math.max(50, 1.2 * options.baselineWriterP99Ms!));
    return { schema: "jones.server-parallelism.native-qualification/v1", build: command.build, count,
      windows: windowsResult, churnWindows, queues, request: percentiles(requestMs), resources,
      productiveOverlapSamples, rotations, shutdownProved, parentCpu, mainThreadCpu,
      resourceMeaning: "parentCpu includes reader/checkpoint threads; mainThreadCpu is the serving harness thread; child CPU/RSS are separate samples",
      childRssWithinBudget,
      predicates: { productiveOverlap: productiveOverlapSamples > 0, normalTargets, writerTarget,
        resourcesComplete: !samplerFailure && !observationLimitReached,
        // Resource ceilings and throughput regression need exact-host interpretation against baseline.
        aggregateResourceBudget: "requires-baseline-and-host-budget", childRssWithinBudget, churnThroughput: "requires-baseline" },
      observations, computePhases: summarizeComputeObservations(observations),
      guardianRss: observations.filter((event) => event.phase === "resources"),
      guardianRssAvailability: observations.some((event) => event.phase === "resources") ? "observed" : "unavailable",
      timingMeaning: "execute is child-local; reply is chunk preparation/delivery before terminal encoding; reply_delivery is an interval estimate including terminal encoding, with per-exchange clock uncertainty; null is unavailable",
      missingCapabilities: ["full native-tail phase/pending queues",
        ...(process.platform === "darwin" ? ["per-process native thread and kernel watcher counts"] : [])] };
  } finally {
    clearTimeout(deadline); stopped = true; controller.abort(); await sampler; monitor.disable();
    if (!shutdownProved) await supervisor.shutdown();
    // Fixture removal is the invoking harness owner's duty after reading the report.
    // Uncertain retirement must preserve the journal and fixture for reconciliation.
  }
}
