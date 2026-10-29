// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Controlled child/native and journal boundaries exercise the real supervisor, scheduler, protection and child allocation ledger.
import { afterEach, expect, it, vi } from "vite-plus/test";
import type { LogicalFinderHandle, WorkspaceKey } from "../serverParallelism/computeContract.ts";
import type { ComputeObservation } from "../serverParallelism/observations.ts";
import { computeFailure } from "./failure.ts";
import type { ComputeChild, SlotResult } from "./processSlot.ts";
import { LIMITS, type Bootstrap, type Request, type ScanStatus } from "./protocol.ts";
import { ComputeSupervisor, type JournalPort } from "./supervisor.ts";
import { WorkerAllocations } from "./worker.ts";

function deferred() {
  let resolve!: () => void, reject!: (cause: unknown) => void;
  const promise = new Promise<void>((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  // Held external boundaries can reject before the consumer reaches them.
  void promise.catch(() => {});
  return { promise, resolve, reject };
}
const owned: Array<{ pool: ComputeSupervisor; cleanup: () => void }> = [];
afterEach(async () => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const { pool, cleanup } of owned.splice(0)) {
    cleanup();
    await pool.shutdown().catch(() => {});
  }
});
const fakeClock = () =>
  vi.useFakeTimers({
    toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "performance"],
  });
type Gate = ReturnType<typeof deferred>;
type TestChild = ComputeChild & {
  requests: Request[];
  signals: Map<number, AbortSignal | undefined>;
  entries: Map<string, Request["token"]>;
  tails: Set<string>;
  exit: Gate;
  journal: Gate;
  retirementStarted: Gate;
  journalStarted: Gate;
  retireCalls: number;
  exited: boolean;
  crash: () => void;
  lateExit: () => void;
};
function fixture(count: 2 | 3 = 2) {
  const children: TestChild[] = [],
    events: ComputeObservation[] = [];
  const observers = new Set<(event: ComputeObservation) => void>();
  const gates = new Map<string, { entered: Gate; release: Gate }>();
  const creates = new Map<string, { entered: Gate; release: Gate }>();
  let status: ScanStatus = { scanning: false, warmupComplete: true },
    released = false;
  const journal: JournalPort = {
    claim: { authorityId: "authority" },
    reserve: async () => {},
    registered: async () => {},
    retired: async (id, generation) => {
      const child = children.find(
        (c) => c.bootstrap.slotId === id && c.bootstrap.slotGeneration === generation,
      )!;
      expect(child.exited).toBe(true);
      child.journalStarted.resolve();
      await child.journal.promise;
    },
    release: async () => {
      released = true;
    },
  };
  const pool = new ComputeSupervisor({
    count,
    build: "a".repeat(64),
    journal,
    observe: (event) => {
      events.push(event);
      for (const observe of observers) observe(event);
    },
    start: async (bootstrap: Bootstrap, register, onExit) => {
      const pid = 100 + children.length;
      await register(pid);
      const death = deferred(),
        allocations = new WorkerAllocations();
      const child: TestChild = {
        pid,
        bootstrap,
        requests: [],
        signals: new Map(),
        entries: new Map(),
        tails: new Set(),
        exit: deferred(),
        journal: deferred(),
        retirementStarted: deferred(),
        journalStarted: deferred(),
        retireCalls: 0,
        exited: false,
        crash: () => {
          death.reject(computeFailure("worker-exit"));
          onExit(computeFailure("worker-exit"));
        },
        lateExit: () => onExit(computeFailure("worker-exit")),
        retire: async () => {
          child.retireCalls++;
          child.retirementStarted.resolve();
          await child.exit.promise;
          child.exited = true;
          child.entries.clear();
          child.tails.clear();
        },
        request: async (request, signal): Promise<SlotResult> => {
          if (child.exited) throw computeFailure("worker-exit");
          child.requests.push(request);
          child.signals.set(request.requestId, signal);
          if (request.operation === "open") {
            allocations.reserve(request.token, request.input as WorkspaceKey);
            child.entries.set(request.token.keyId, request.token);
            child.tails.add(request.token.keyId);
          } else expect(child.entries.get(request.token.keyId)).toEqual(request.token);
          const gate =
            gates.get(`${pid}:${request.operation}:${request.token.keyId}`) ??
            (request.operation === "open"
              ? creates.get((request.input as WorkspaceKey).cwd)
              : undefined);
          if (request.operation === "open") creates.delete((request.input as WorkspaceKey).cwd);
          gate?.entered.resolve();
          // A noncooperative external native request can hold its lease after caller cancellation.
          await Promise.race([gate?.release.promise ?? Promise.resolve(), death.promise]);
          if (request.operation === "close") child.entries.delete(request.token.keyId);
          return {
            items:
              request.operation === "list"
                ? [{ path: `slot-${bootstrap.slotId}`, kind: "file" }]
                : [],
            final: {
              type: "done",
              ...(request.operation === "close" ? {} : { status }),
              ...(["list", "search", "searchContents"].includes(request.operation)
                ? { truncated: false }
                : {}),
            },
          };
        },
      };
      // Unheld proof boundaries complete normally, but each test can replace them before retirement starts.
      child.exit.resolve();
      child.journal.resolve();
      children.push(child);
      return child;
    },
  });
  owned.push({
    pool,
    cleanup: () => {
      for (const gate of [...gates.values(), ...creates.values()]) gate.release.resolve();
      for (const child of children) {
        child.exit.resolve();
        child.journal.resolve();
      }
    },
  });
  const register = (id: number) => pool.register({ cwd: `/synthetic/${id}`, variant: "paths" });
  const acquire = async (id: number) => {
    const handle = register(id);
    await pool.ensure(handle);
    return handle;
  };
  const fill = async () => {
    const handles = [];
    for (let id = 0; id < count * 4; id++) handles.push(await acquire(id));
    return handles;
  };
  const token = (handle: LogicalFinderHandle) =>
    children
      .flatMap((child) => child.requests)
      .findLast((r) => r.operation === "open" && (r.input as WorkspaceKey).cwd === handle.key.cwd)!
      .token;
  const owner = (handle: LogicalFinderHandle) => {
    const t = token(handle);
    return children.find(
      (child) =>
        child.bootstrap.slotId === t.slotId && child.bootstrap.slotGeneration === t.slotGeneration,
    )!;
  };
  const hold = (handle: LogicalFinderHandle, operation: Request["operation"]) => {
    const child = owner(handle),
      key = `${child.pid}:${operation}:${token(handle).keyId}`;
    const gate = { entered: deferred(), release: deferred() };
    gates.set(key, gate);
    return gate;
  };
  const counts = () => {
    const event = events.findLast((event) => "counts" in event);
    if (!event || !("counts" in event)) throw new Error("Missing lifecycle observation");
    return event.counts;
  };
  const waitForCounts = (predicate: (value: ReturnType<typeof counts>) => boolean) => {
    if (predicate(counts())) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const observe = (event: ComputeObservation) => {
        if (!("counts" in event) || !predicate(event.counts)) return;
        observers.delete(observe);
        resolve();
      };
      observers.add(observe);
    });
  };
  const pending = (handle: LogicalFinderHandle, signal?: AbortSignal) => {
    const promise = pool.ensure(handle, signal);
    void promise.catch(() => {});
    return promise;
  };
  return {
    pool,
    children,
    register,
    acquire,
    fill,
    token,
    owner,
    hold,
    counts,
    waitForCounts,
    pending,
    events,
    holdCreate: (id: number) => {
      const gate = { entered: deferred(), release: deferred() };
      creates.set(`/synthetic/${id}`, gate);
      return gate;
    },
    setStatus: (value: ScanStatus) => {
      status = value;
    },
    released: () => released,
  };
}

it.each([2, 3] as const)(
  "keeps %s full generations charged past readiness and a 60-second hidden native tail",
  async (count) => {
    fakeClock();
    const f = fixture(count),
      handles = await f.fill();
    expect(f.children).toHaveLength(count);
    expect(f.children.every((c) => c.tails.size === 4)).toBe(true);
    const gate = f.hold(handles[0]!, "refresh"),
      refresh = f.pool.run(handles[0]!, "refresh", undefined);
    await gate.entered.promise;
    expect(f.counts()).toMatchObject({
      activeForegroundOpenRefresh: 1,
      chargedAllocations: 4 * count,
    });
    gate.release.resolve();
    await refresh;
    await vi.advanceTimersByTimeAsync(60_001);
    expect(f.counts()).toMatchObject({
      activeForegroundOpenRefresh: 0,
      chargedAllocations: 4 * count,
      observedNativeScanning: 0,
      observedNativeWarmup: 0,
    });
    expect(f.children).toHaveLength(count);
    expect(f.children.every((c) => c.retireCalls === 0 && c.tails.size === 4)).toBe(true);
    expect(
      f.children.every((c) => c.requests.filter((r) => r.operation === "open").length === 4),
    ).toBe(true);
  },
);

it("holds all charges through captured exit AND successful journal retirement before overflow opens", async () => {
  const f = fixture();
  await f.fill();
  const old = f.children[0]!;
  old.exit = deferred();
  old.journal = deferred();
  const waiting = f.pending(f.register(9));
  await old.retirementStarted.promise;
  expect(f.counts()).toMatchObject({
    physicalEntries: 8,
    chargedAllocations: 8,
    retiringGenerations: 1,
  });
  expect(f.children).toHaveLength(2);
  old.exit.resolve();
  await old.journalStarted.promise;
  expect(old.exited).toBe(true);
  expect(f.children).toHaveLength(2);
  expect(f.counts().chargedAllocations).toBe(8);
  old.journal.resolve();
  await waiting;
  expect(f.children).toHaveLength(3);
  expect(f.children[2]!.bootstrap).toMatchObject({ slotId: 0, slotGeneration: 2 });
  expect(f.counts().chargedAllocations).toBe(5);
});

it.each(["exit", "journal"] as const)(
  "fails closed with retained charges on unknown %s during planned rotation",
  async (boundary) => {
    const f = fixture();
    await f.fill();
    const old = f.children[0]!;
    old[boundary] = deferred();
    const waiting = f.pending(f.register(9)),
      rejected = expect(waiting).rejects.toBeDefined();
    await old.retirementStarted.promise;
    if (boundary === "journal") await old.journalStarted.promise;
    old[boundary].reject(computeFailure("retirement-unknown"));
    await rejected;
    expect(f.counts()).toMatchObject({
      chargedAllocations: 8,
      physicalEntries: 8,
      retiringGenerations: 1,
    });
    expect(f.children).toHaveLength(2);
    expect(() => f.register(10)).toThrow();
    await expect(f.pool.shutdown()).rejects.toMatchObject({ reason: "retirement-unknown" });
    expect(f.released()).toBe(false);
  },
);

it("drains all pre-cut sibling work and refresh groups while post-cut arrivals wait unbound and another child progresses", async () => {
  fakeClock();
  const f = fixture(),
    handles = await f.fill();
  const old = f.owner(handles[0]!),
    other = f.owner(handles[1]!);
  const original = f.token(handles[4]!);
  old.exit = deferred();
  const listGate = f.hold(handles[0]!, "list"),
    otherGate = f.hold(handles[1]!, "list"),
    refreshGate = f.hold(handles[4]!, "refresh");
  const running = f.pool.run(handles[0]!, "list", undefined);
  await listGate.entered.promise;
  await vi.advanceTimersByTimeAsync(1);
  const otherRunning = f.pool.run(handles[1]!, "list", undefined);
  await otherGate.entered.promise;
  const queued = f.pool.run(handles[2]!, "list", undefined);
  const refreshes = [
    f.pool.run(handles[4]!, "refresh", undefined),
    f.pool.run(handles[4]!, "refresh", undefined),
  ];
  const overflow = f.register(9),
    waiting = f.pending(overflow);
  // Both generations are busy; the LRU tie deterministically chooses slot zero before these admissions.
  const laterList = f.pool.run(handles[0]!, "list", undefined),
    laterRefresh = f.pool.run(handles[4]!, "refresh", undefined);
  otherGate.release.resolve();
  await otherRunning;
  await f.pool.run(handles[3]!, "list", undefined);
  expect(other.requests.filter((r) => r.operation === "list")).toHaveLength(2);
  expect(old.retireCalls).toBe(0);
  listGate.release.resolve();
  await running;
  await queued;
  await refreshGate.entered.promise;
  expect(old.requests.filter((r) => r.operation === "refresh")).toHaveLength(1);
  expect(old.requests.filter((r) => r.operation === "refresh")[0]!.token).toEqual(original);
  refreshGate.release.resolve();
  await Promise.all(refreshes);
  await old.retirementStarted.promise;
  expect(old.requests.filter((r) => r.operation === "list")).toHaveLength(2);
  expect(f.counts().chargedAllocations).toBe(8);
  // Future work never pins the drain or consumes old tokens. Reserve the new generation for oldest pressure.
  old.exit.resolve();
  await Promise.all([waiting, laterList, laterRefresh]);
  const replacement = f.children[2]!;
  expect(
    (replacement.requests.find((r) => r.operation === "open")!.input as WorkspaceKey).cwd,
  ).toBe(overflow.key.cwd);
  expect(replacement.requests.filter((r) => r.operation === "refresh")).toHaveLength(1);
  expect(f.token(handles[4]!).incarnation).toBeGreaterThan(original.incarnation);
  expect(old.requests.filter((r) => r.operation === "refresh")).toHaveLength(1);
  expect(f.children.filter((c) => c.retireCalls > 0)).toHaveLength(1);
});

it("cancels vanished pressure before teardown and binds future requests to the surviving cohort", async () => {
  fakeClock();
  const f = fixture(),
    handles = await f.fill();
  const oldToken = f.token(handles[0]!),
    old = f.owner(handles[0]!);
  const a = f.hold(handles[0]!, "list"),
    b = f.hold(handles[1]!, "list");
  const running = f.pool.run(handles[0]!, "list", undefined);
  await a.entered.promise;
  await vi.advanceTimersByTimeAsync(1);
  const other = f.pool.run(handles[1]!, "list", undefined);
  await b.entered.promise;
  const controller = new AbortController(),
    overflow = f.pending(f.register(9), controller.signal);
  const cancelled = expect(overflow).rejects.toMatchObject({ reason: "cancelled" });
  const future = f.pool.run(handles[0]!, "refresh", undefined);
  controller.abort();
  await cancelled;
  a.release.resolve();
  await Promise.all([running, future]);
  b.release.resolve();
  await other;
  expect(old.retireCalls).toBe(0);
  expect(f.children).toHaveLength(2);
  expect(old.requests.findLast((r) => r.operation === "refresh")!.token).toEqual(oldToken);
});

it("keeps original post-cut deadlines and a cancelled running lease until native return", async () => {
  fakeClock();
  const f = fixture(),
    handles = await f.fill(),
    old = f.owner(handles[0]!);
  const a = f.hold(handles[0]!, "list"),
    b = f.hold(handles[1]!, "list");
  const controller = new AbortController();
  const running = f.pool.run(handles[0]!, "list", undefined, controller.signal);
  const cancelled = expect(running).rejects.toMatchObject({ reason: "cancelled" });
  await a.entered.promise;
  await vi.advanceTimersByTimeAsync(1);
  const other = f.pool.run(handles[1]!, "list", undefined);
  await b.entered.promise;
  const before = expect(f.pool.run(handles[2]!, "list", undefined)).rejects.toMatchObject({
    reason: "queue-timeout",
  });
  const trigger = expect(f.pending(f.register(9))).rejects.toMatchObject({
    reason: "queue-timeout",
  });
  await vi.advanceTimersByTimeAsync(1000);
  const after = expect(f.pool.run(handles[0]!, "refresh", undefined)).rejects.toMatchObject({
    reason: "queue-timeout",
  });
  await vi.advanceTimersByTimeAsync(4000);
  const survivor = f.pending(f.register(10));
  controller.abort();
  await cancelled;
  await vi.advanceTimersByTimeAsync(6001);
  await Promise.all([before, trigger, after]);
  expect(old.retireCalls).toBe(0);
  expect(old.requests.some((r) => r.operation === "refresh")).toBe(false);
  // Caller settlement and queued expiry do not release the still-running native lease.
  a.release.resolve();
  await old.retirementStarted.promise;
  await survivor;
  b.release.resolve();
  await other;
  expect(old.requests.filter((r) => r.operation === "list")).toHaveLength(1);
});

it("fails fault-bound jobs once during drain without replay while genuinely unbound demand survives", async () => {
  fakeClock();
  const f = fixture(),
    handles = await f.fill(),
    old = f.owner(handles[0]!);
  const a = f.hold(handles[0]!, "list"),
    b = f.hold(handles[1]!, "list");
  const running = f.pool.run(handles[0]!, "list", undefined);
  await a.entered.promise;
  await vi.advanceTimersByTimeAsync(1);
  const other = f.pool.run(handles[1]!, "list", undefined);
  await b.entered.promise;
  const queued = f.pool.run(handles[2]!, "list", undefined),
    refresh = f.pool.run(handles[4]!, "refresh", undefined);
  const faults = Promise.allSettled([running, queued, refresh]);
  old.exit = deferred();
  const overflow = f.pending(f.register(9)),
    future = f.pool.run(handles[0]!, "refresh", undefined);
  old.crash();
  await old.retirementStarted.promise;
  expect(
    (await faults).every(
      (result) =>
        result.status === "rejected" &&
        (result.reason as { reason: string }).reason === "worker-exit",
    ),
  ).toBe(true);
  expect(old.requests.filter((r) => ["list", "refresh"].includes(r.operation))).toHaveLength(1);
  expect(f.counts().chargedAllocations).toBe(8);
  old.exit.resolve();
  await old.journalStarted.promise;
  await f.waitForCounts((value) => value.chargedAllocations === 4);
  const beforeDrain = performance.now();
  await vi.advanceTimersByTimeAsync(0);
  expect(performance.now()).toBe(beforeDrain);
  expect(f.children).toHaveLength(2); // Fault cooldown, not a free allocation or an eager replay.
  await vi.advanceTimersByTimeAsync(1000);
  await Promise.all([overflow, future]);
  b.release.resolve();
  await other;
  expect(f.children).toHaveLength(3);
  expect(old.requests.filter((r) => r.operation === "list")).toHaveLength(1);
  // Delayed callbacks from the captured old generation cannot invalidate its replacement.
  old.lateExit();
  await f.pool.run(handles[0]!, "list", undefined);
  expect(f.children[2]!.retireCalls).toBe(0);
});

it("reopens every evicted facade only on fresh demand and planned rotations add no cooldown", async () => {
  fakeClock();
  const now = vi.spyOn(performance, "now");
  const f = fixture(),
    handles = await f.fill();
  const siblings = [handles[0]!, handles[2]!, handles[4]!, handles[6]!],
    oldTokens = siblings.map(f.token);
  await f.acquire(9);
  expect(f.children).toHaveLength(3);
  expect(f.children.flatMap((c) => c.requests).filter((r) => r.operation === "open")).toHaveLength(
    9,
  );
  for (let i = 0; i < siblings.length; i++) {
    await f.pool.run(siblings[i]!, "list", undefined);
    const token = f.token(siblings[i]!);
    expect(token).not.toEqual(oldTokens[i]);
    expect(token.incarnation).toBeGreaterThan(oldTokens[i]!.incarnation);
    expect(
      f.children.flatMap((c) => c.requests).filter((r) => r.operation === "open"),
    ).toHaveLength(10 + i);
  }
  // The clock has not advanced: repeated planned rotation could not succeed if failure backoff were added.
  expect(now.mock.results.at(-1)?.value).toBe(0);
});

it("uses newest sibling access for whole-cohort idle TTL and never restarts without demand", async () => {
  fakeClock();
  const f = fixture(),
    handles = await f.fill();
  await vi.advanceTimersByTimeAsync(LIMITS.idleMs - 250);
  await Promise.all([
    f.pool.run(handles[0]!, "list", undefined),
    f.pool.run(handles[1]!, "list", undefined),
  ]);
  await vi.advanceTimersByTimeAsync(500);
  expect(f.children.every((c) => c.retireCalls === 0)).toBe(true);
  await vi.advanceTimersByTimeAsync(LIMITS.idleMs);
  expect(f.children.every((c) => c.retireCalls === 1)).toBe(true);
  expect(f.counts()).toMatchObject({
    logicalHandles: 8,
    physicalEntries: 0,
    chargedAllocations: 0,
  });
  await vi.advanceTimersByTimeAsync(LIMITS.idleMs);
  expect(f.children).toHaveLength(2);
  await f.pool.run(handles[0]!, "list", undefined);
  expect(f.children).toHaveLength(3);
  expect(f.children[2]!.bootstrap.slotGeneration).toBe(2);
});

it("keeps shutdown charges after successful close ACKs until exit and journal proof", async () => {
  const f = fixture();
  await f.fill();
  const first = f.children[0]!;
  first.exit = deferred();
  first.journal = deferred();
  const stopping = f.pool.shutdown();
  await first.retirementStarted.promise;
  expect(f.children.every((c) => c.entries.size === 0 && c.tails.size === 4)).toBe(true);
  expect(f.children.flatMap((c) => c.requests).filter((r) => r.operation === "close")).toHaveLength(
    8,
  );
  expect(f.counts()).toMatchObject({ physicalEntries: 8, chargedAllocations: 8 });
  expect(f.released()).toBe(false);
  first.exit.resolve();
  await first.journalStarted.promise;
  expect(f.counts().chargedAllocations).toBe(8);
  first.journal.resolve();
  await stopping;
  expect(f.counts()).toMatchObject({ chargedAllocations: 0, physicalEntries: 0 });
  expect(f.released()).toBe(true);
});

it("retires a possibly allocating failed open without refunding its charge or replaying bound work", async () => {
  const f = fixture(),
    create = f.holdCreate(1),
    handle = f.register(1);
  const opening = f.pending(handle),
    failure = expect(opening).rejects.toMatchObject({ reason: "finder-create" });
  await create.entered.promise;
  const old = f.owner(handle);
  old.exit = deferred();
  old.journal = deferred();
  const bound = expect(f.pool.run(handle, "list", undefined)).rejects.toMatchObject({
    reason: "finder-create",
  });
  create.release.reject(computeFailure("finder-create"));
  await Promise.all([failure, bound]);
  await old.retirementStarted.promise;
  expect(f.counts()).toMatchObject({ physicalEntries: 1, chargedAllocations: 1 });
  const future = f.pool.run(handle, "list", undefined);
  void future.catch(() => {});
  expect(f.children).toHaveLength(1);
  old.exit.resolve();
  await old.journalStarted.promise;
  expect(f.counts().chargedAllocations).toBe(1);
  old.journal.resolve();
  await future;
  // A different proved-empty slot may serve a fresh invocation; no failed operation is replayed.
  expect(f.children).toHaveLength(2);
  expect(old.requests.filter((r) => r.operation === "open")).toHaveLength(1);
  expect(old.requests.some((r) => r.operation === "list")).toBe(false);
  expect(f.token(handle).incarnation).toBe(2);
});

it("reserves freed capacity for the oldest surviving absent key ahead of post-cut hot facades", async () => {
  fakeClock();
  const f = fixture(),
    handles = await f.fill(),
    old = f.owner(handles[0]!);
  old.exit = deferred();
  const controller = new AbortController(),
    first = f.pending(f.register(9), controller.signal);
  const cancelled = expect(first).rejects.toMatchObject({ reason: "cancelled" });
  await old.retirementStarted.promise;
  const hot = f.pool.run(handles[0]!, "list", undefined);
  const oldest = f.register(10),
    next = f.pending(oldest),
    later = f.pending(f.register(11));
  controller.abort();
  await cancelled;
  expect(f.children[1]!.retireCalls).toBe(0);
  old.exit.resolve();
  await Promise.all([hot, next, later]);
  expect(
    (f.children[2]!.requests.find((r) => r.operation === "open")!.input as WorkspaceKey).cwd,
  ).toBe(oldest.key.cwd);
});

it("finishes already-started retirement without a speculative replacement after all pressure cancels", async () => {
  fakeClock();
  const f = fixture();
  await f.fill();
  const old = f.children[0]!;
  old.exit = deferred();
  const controller = new AbortController(),
    pressure = f.pending(f.register(9), controller.signal);
  const cancelled = expect(pressure).rejects.toMatchObject({ reason: "cancelled" });
  await old.retirementStarted.promise;
  controller.abort();
  await cancelled;
  expect(f.counts().chargedAllocations).toBe(8);
  old.exit.resolve();
  await old.journalStarted.promise;
  await f.waitForCounts((value) => value.chargedAllocations === 4);
  const beforeDrain = performance.now();
  await vi.advanceTimersByTimeAsync(0);
  expect(performance.now()).toBe(beforeDrain);
  expect(f.children).toHaveLength(2);
  expect(f.counts().chargedAllocations).toBe(4);
});

it("keeps a draining coalesced refresh lease when only one caller cancels", async () => {
  fakeClock();
  const f = fixture(),
    handles = await f.fill(),
    old = f.owner(handles[0]!);
  const a = f.hold(handles[0]!, "list"),
    b = f.hold(handles[1]!, "list"),
    refresh = f.hold(handles[4]!, "refresh");
  const running = f.pool.run(handles[0]!, "list", undefined);
  await a.entered.promise;
  await vi.advanceTimersByTimeAsync(1);
  const other = f.pool.run(handles[1]!, "list", undefined);
  await b.entered.promise;
  const controller = new AbortController(),
    first = f.pool.run(handles[4]!, "refresh", undefined, controller.signal);
  const cancelled = expect(first).rejects.toMatchObject({ reason: "cancelled" });
  const survivor = f.pool.run(handles[4]!, "refresh", undefined),
    pressure = f.pending(f.register(9));
  a.release.resolve();
  await running;
  await refresh.entered.promise;
  controller.abort();
  await cancelled;
  const request = old.requests.findLast((r) => r.operation === "refresh")!;
  expect(old.signals.get(request.requestId)?.aborted).toBe(false);
  expect(old.retireCalls).toBe(0);
  refresh.release.resolve();
  await survivor;
  await pressure;
  b.release.resolve();
  await other;
  expect(old.requests.filter((r) => r.operation === "refresh")).toHaveLength(1);
});

it("releases the lifecycle gate after implicit open while the first productive request still owns its child", async () => {
  const f = fixture(),
    create = f.holdCreate(1),
    handle = f.register(1);
  const running = f.pool.run(handle, "list", undefined);
  await create.entered.promise;
  const list = f.hold(handle, "list"),
    other = f.pending(f.register(2));
  create.release.resolve();
  await list.entered.promise;
  await other;
  expect(f.children).toHaveLength(2);
  expect(f.owner(handle).retireCalls).toBe(0);
  list.release.resolve();
  await running;
});

it("charges post-cut future requests to the original per-key queue cap and unlinks each cancellation once", async () => {
  fakeClock();
  const f = fixture(),
    handles = await f.fill(),
    old = f.owner(handles[0]!);
  const a = f.hold(handles[0]!, "list"),
    b = f.hold(handles[1]!, "list");
  const running = f.pool.run(handles[0]!, "list", undefined);
  await a.entered.promise;
  await vi.advanceTimersByTimeAsync(1);
  const other = f.pool.run(handles[1]!, "list", undefined);
  await b.entered.promise;
  const pressure = f.pending(f.register(9));
  const controllers = Array.from({ length: 8 }, () => new AbortController());
  const queued = controllers.map((controller) =>
    f.pool.run(handles[0]!, "list", undefined, controller.signal),
  );
  const cancelled = Promise.allSettled(queued);
  await expect(f.pool.run(handles[0]!, "list", undefined)).rejects.toMatchObject({
    reason: "queue-full",
  });
  controllers.forEach((controller) => {
    controller.abort();
    controller.abort();
  });
  expect(
    (await cancelled).every(
      (result) =>
        result.status === "rejected" &&
        (result.reason as { reason: string }).reason === "cancelled",
    ),
  ).toBe(true);
  expect(old.retireCalls).toBe(0);
  a.release.resolve();
  await running;
  await pressure;
  b.release.resolve();
  await other;
  expect(old.requests.filter((r) => r.operation === "list")).toHaveLength(1);
});
