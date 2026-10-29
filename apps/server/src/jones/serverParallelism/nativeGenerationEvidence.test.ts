// @effect-diagnostics globalTimers:off -- Real supervisor allocation/retirement with controlled external child, journal and OS collectors; no native processes or sleeps.
import { afterEach, expect, it, vi } from "vite-plus/test";
import { ComputeSupervisor } from "../serverCompute/supervisor.ts";
import type { ComputeChild, SlotResult } from "../serverCompute/processSlot.ts";
import type { Request, Bootstrap } from "../serverCompute/protocol.ts";
import type { LogicalFinderHandle } from "./computeContract.ts";
import type { ComputeObservation } from "./observations.ts";
import type { NativePoolObservation } from "./resourceSamples.ts";
import {
  NativeGenerationEvidence,
  generationIdentity,
  type EvidenceGeneration,
} from "./nativeGenerationEvidence.ts";

function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const cleanup: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0)) await close();
  vi.useRealTimers();
});
const namedPools = (pid: number): NativePoolObservation => ({
  pid,
  status: "complete",
  method: "linux-proc-task-comm",
  raw: "controlled collector",
  threads: [
    { tid: "1", name: "fff-bg-0" },
    { tid: "2", name: "fff-bg-1" },
    { tid: "3", name: "fff-search-0" },
  ],
});

function fixture(count: 2 | 3 = 2) {
  vi.useFakeTimers({
    toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "performance"],
  });
  const captured = new Map<number, ComputeChild>();
  const records: EvidenceGeneration[] = [];
  const events: ComputeObservation[] = [];
  const requested: Request[] = [];
  const entered = gate(),
    release = gate();
  const exitEntered = gate(),
    exitRelease = gate(),
    journalEntered = gate(),
    journalRelease = gate();
  let holdRead = false,
    holdExit = false,
    holdJournal = false,
    empty = false,
    wrongMatch = false,
    fail = false;
  const reads: number[] = [];
  const resources: string[] = [];
  const observerController = new AbortController();
  const evidence = new NativeGenerationEvidence({
    captured,
    now: () => performance.now(),
    method: "linux-proc-task-comm",
    signal: observerController.signal,
    pools: async (pid) => {
      reads.push(pid);
      if (holdRead) {
        entered.resolve();
        await release.promise;
      }
      if (fail) throw new Error("collector failed");
      return namedPools(pid);
    },
    resources: async () => {
      resources.push("full-resource-row");
    },
  });
  const pool = new ComputeSupervisor({
    count,
    build: "a".repeat(64),
    observe: (event) => events.push(event),
    journal: {
      claim: { authorityId: "authority" },
      reserve: async () => {},
      registered: async () => {},
      release: async () => {},
      retired: async (_slot, _generation, _nonce, pid) => {
        const record = records.find((record) => record.pid === pid)!;
        expect(record.exitCapturedAt).toBeDefined();
        if (holdJournal) {
          journalEntered.resolve();
          await journalRelease.promise;
        }
        record.journalRetiredAt = performance.now();
        captured.delete(pid);
      },
    },
    start: async (bootstrap, register) => {
      const pid = 100 + records.length;
      const record: EvidenceGeneration = {
        bootstrap,
        pid,
        startedAt: performance.now(),
        nativePools: [],
        poolReads: [],
        faulted: false,
        basisGrepAttempted: false,
      };
      const entries = new Map<string, string>();
      const child: ComputeChild = {
        pid,
        bootstrap,
        request: async (request): Promise<SlotResult> => {
          requested.push(request);
          if (request.operation === "open")
            entries.set(request.token.keyId, (request.input as { cwd: string }).cwd);
          const cwd = entries.get(request.token.keyId)!;
          const key = cwd.split("/").at(-1)!;
          const items =
            request.operation === "searchContents" && !empty
              ? [
                  {
                    path: "file-0.txt",
                    lineContent: wrongMatch
                      ? "unrelated workspace marker"
                      : `needle-${key} alpha beta`,
                    lineNumber: 1,
                    matchRanges: [{ start: 0, end: 6 }],
                  },
                ]
              : request.operation === "search"
                ? [{ path: "file-0.txt", kind: "file" }]
                : [];
          const result: SlotResult = {
            items,
            final: {
              type: "done",
              truncated: false,
              status: { scanning: false, warmupComplete: true },
            },
          };
          evidence.completed(record, request, result, `needle-${key} alpha beta`);
          return result;
        },
        retire: async () => {
          record.retirementStartedAt = performance.now();
          if (holdExit) {
            exitEntered.resolve();
            await exitRelease.promise;
          }
          record.exitCapturedAt = performance.now();
        },
      };
      captured.set(pid, child);
      records.push(record);
      await register(pid);
      return child;
    },
  });
  const handles = Array.from({ length: 8 * count }, (_, key) =>
    pool.register({ cwd: `/synthetic/${key}`, variant: key % 2 === 0 ? "paths" : "content" }),
  );
  const qualify = (handle: LogicalFinderHandle, phase = "churn") =>
    evidence.qualify(handle, phase, async () => {
      const result = await pool.run(handle, "searchContents", {
        query: "needle",
        limit: 32,
        caseSensitive: true,
        wholeWord: false,
        useRegex: false,
      });
      if (result.matches.length === 0) throw new Error("empty positive grep");
      evidence.confirmPositive(handle);
    });
  cleanup.push(async () => {
    release.resolve();
    exitRelease.resolve();
    journalRelease.resolve();
    await evidence.stopAndDrain();
    await pool.shutdown();
  });
  return {
    pool,
    evidence,
    records,
    captured,
    handles,
    requested,
    events,
    reads,
    resources,
    entered,
    release,
    exitEntered,
    exitRelease,
    journalEntered,
    journalRelease,
    qualify,
    abortObserver: () => observerController.abort(),
    holdRead: () => {
      holdRead = true;
    },
    holdExit: () => {
      holdExit = true;
    },
    holdJournal: () => {
      holdJournal = true;
    },
    empty: () => {
      empty = true;
    },
    wrongMatch: () => {
      wrongMatch = true;
    },
    fail: (value: boolean) => {
      fail = value;
    },
  };
}

it.each([2, 3] as const)(
  "awaits short generation checkpoints before the next rotation with N=%s",
  async (count) => {
    const f = fixture(count);
    for (const handle of f.handles.slice(0, 4 * count)) await f.pool.ensure(handle);
    expect(f.records).toHaveLength(count);
    const counts = f.events.filter((event) => "counts" in event).at(-1)!;
    expect("counts" in counts && counts.counts.physicalEntries).toBe(4 * count);
    for (const handle of f.handles.slice(0, count)) await f.qualify(handle, "warm-idle");
    f.holdRead();
    const next = (async () => {
      await f.pool.run(f.handles[4 * count]!, "search", { query: "file", limit: 32 });
      await f.qualify(f.handles[4 * count]!);
      await f.pool.ensure(f.handles[4 * count + 1]!);
    })();
    await f.entered.promise;
    const last = f.records.at(-1)!;
    expect(last.bootstrap.slotGeneration).toBeGreaterThan(1);
    expect(last.grep).toBeDefined();
    expect(last.poolReads).toHaveLength(0);
    const opens = f.requested.filter((request) => request.operation === "open").length;
    expect(opens).toBe(4 * count + 1);
    expect(performance.now()).toBe(0); // Shorter than one background sampler tick.
    f.release.resolve();
    await next;
    expect(last.poolReads).toHaveLength(1);
    expect(f.resources.length).toBeGreaterThan(count);
    expect(f.requested.filter((request) => request.operation === "open")).toHaveLength(
      4 * count + 2,
    );
    expect(last.poolReads[0]!.grep.token).toEqual(last.grep!.token);
  },
);

it("drains the final basis read before shutdown, then retains charges through exit and journal", async () => {
  const f = fixture();
  for (const handle of f.handles.slice(0, 8)) await f.pool.ensure(handle);
  await f.qualify(f.handles[0]!, "warm-idle");
  const record = f.records[0]!;
  f.holdRead();
  f.holdExit();
  f.holdJournal();
  const reading = f.evidence.checkpoint(record, "shutdown");
  await f.entered.promise;
  const finish = (async () => {
    await f.evidence.stopAndDrain();
    await f.pool.shutdown();
  })();
  expect(record.exitCapturedAt).toBeUndefined();
  expect(record.poolReads).toHaveLength(1);
  f.release.resolve();
  await reading;
  await f.exitEntered.promise;
  const charged = () => {
    const event = f.events.filter((event) => "counts" in event).at(-1)!;
    return "counts" in event ? event.counts.chargedAllocations : -1;
  };
  expect(charged()).toBe(8);
  f.exitRelease.resolve();
  await f.journalEntered.promise;
  expect(charged()).toBe(8);
  expect(f.captured.has(record.pid!)).toBe(true);
  f.journalRelease.resolve();
  await finish;
  expect(charged()).toBe(0);
  expect(f.captured.size).toBe(0);
  expect(record.poolReads.at(-1)!.phase).toBe("shutdown");
});

it("does not certify an empty positive grep or retry it on a later phase", async () => {
  const f = fixture();
  await f.pool.ensure(f.handles[0]!);
  f.empty();
  await expect(f.qualify(f.handles[0]!)).rejects.toThrow("empty positive grep");
  await expect(f.qualify(f.handles[0]!, "settling")).rejects.toThrow("completed positive grep");
  expect(f.requested.filter((request) => request.operation === "searchContents")).toHaveLength(1);
  expect(f.records[0]!.grep).toBeUndefined();
  expect(f.records[0]!.basisGrepFailure).toBe("request-failed");
  expect(f.reads).toHaveLength(0);
});

it("retains failed pool attempts after a later successful phase", async () => {
  const f = fixture();
  await f.pool.ensure(f.handles[0]!);
  f.fail(true);
  await f.qualify(f.handles[0]!);
  f.fail(false);
  await f.qualify(f.handles[0]!, "churn-settling");
  expect(f.records[0]!.nativePools.map((read) => read.status)).toEqual(["unavailable", "complete"]);
  expect(f.records[0]!.poolReads.map((read) => read.phase)).toEqual(["churn", "churn-settling"]);
});

it("rejects a nonempty positive result from the wrong synthetic workspace", async () => {
  const f = fixture();
  await f.pool.ensure(f.handles[0]!);
  f.wrongMatch();
  await expect(f.qualify(f.handles[0]!)).rejects.toThrow("empty, stale or unbound");
  expect(f.records[0]!.grep).toBeUndefined();
  expect(f.reads).toHaveLength(0);
});

it("preserves cancelled basis work and closes admission without another grep", async () => {
  const f = fixture();
  await f.pool.ensure(f.handles[0]!);
  const controller = new AbortController();
  controller.abort();
  await expect(
    f.evidence.qualify(f.handles[0]!, "churn", async () => {
      await f.pool.run(
        f.handles[0]!,
        "searchContents",
        { query: "needle", limit: 32, caseSensitive: true, wholeWord: false, useRegex: false },
        controller.signal,
      );
      f.evidence.confirmPositive(f.handles[0]!);
    }),
  ).rejects.toBeDefined();
  expect(f.records[0]!.grep).toBeUndefined();
  expect(f.records[0]!.basisGrepFailure).toBe("request-failed");
  expect(f.reads).toHaveLength(0);
  await f.evidence.stopAndDrain();
  await expect(f.qualify(f.handles[0]!, "shutdown")).rejects.toThrow("admission stopped");
});

it("allows emergency captured-child containment when an admitted observer stops answering", async () => {
  const f = fixture();
  await f.pool.ensure(f.handles[0]!);
  await f.qualify(f.handles[0]!, "warm-idle");
  const record = f.records[0]!;
  f.holdRead();
  const reading = f.evidence.checkpoint(record, "churn");
  const rejected = expect(reading).rejects.toThrow("Observation aborted");
  await f.entered.promise;
  f.abortObserver();
  await rejected;
  await f.evidence.stopAndDrain();
  await f.pool.shutdown();
  expect(record.poolReads.at(-1)!.observation.error).toBe("native-pool-observation-aborted");
  expect(record.exitCapturedAt).toBeDefined();
  expect(record.journalRetiredAt).toBeDefined();
  expect(f.captured.size).toBe(0);
  // A late OS result cannot overwrite the failed receipt after containment.
  f.release.resolve();
  expect(record.nativePools.map((row) => row.status)).toEqual(["complete", "unavailable"]);
});

it.each([
  "fault",
  "pid-reuse",
  "nonce-change",
  "generation-change",
  "authority-change",
  "build-change",
] as const)("invalidates a read across %s without transferring grep/basis", async (change) => {
  const f = fixture();
  await f.pool.ensure(f.handles[0]!);
  await f.qualify(f.handles[0]!, "warm-idle");
  const record = f.records[0]!,
    child = f.captured.get(record.pid!)!;
  f.holdRead();
  const read = f.evidence.checkpoint(record, "churn");
  await f.entered.promise;
  const changed: Bootstrap = {
    ...child.bootstrap,
    ...(change === "pid-reuse" || change === "nonce-change" ? { nonce: "replacement" } : {}),
    ...(change === "generation-change"
      ? { slotGeneration: child.bootstrap.slotGeneration + 1 }
      : {}),
    ...(change === "authority-change" ? { authorityId: "other" } : {}),
    ...(change === "build-change" ? { build: "b".repeat(64) } : {}),
  };
  if (change === "fault") record.faulted = true;
  else f.captured.set(child.pid, { ...child, bootstrap: changed });
  f.release.resolve();
  await read;
  expect(record.poolReads.at(-1)!.observation.status).toBe("unavailable");
  expect(record.poolReads.at(-1)!.observation.error).toBe("generation-changed-during-observation");
  expect(generationIdentity(child.pid, record.grep!.bootstrap)).toBe(
    generationIdentity(child.pid, child.bootstrap),
  );
  // Restore only this controlled external child so normal fixture teardown can complete.
  f.captured.set(child.pid, child);
});

it("does not confirm a child reply until the supervisor caller accepts its result", async () => {
  const f = fixture();
  await f.pool.ensure(f.handles[0]!);
  await f.pool.run(f.handles[0]!, "searchContents", {
    query: "needle",
    limit: 32,
    caseSensitive: true,
    wholeWord: false,
    useRegex: false,
  });
  expect(f.records[0]!.grep).toBeUndefined();
  f.records[0]!.faulted = true;
  expect(() => f.evidence.confirmPositive(f.handles[0]!)).toThrow("empty, stale or unbound");
});
