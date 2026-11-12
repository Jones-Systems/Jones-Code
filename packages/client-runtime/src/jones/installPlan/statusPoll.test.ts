import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import {
  STATUS_POLL_CADENCE as C,
  createStatusPoller,
  deviceStatusActive,
  statusPollDelayMs,
  type StatusPollerOptions,
} from "./statusPoll.ts";

const middle = () => 0.5;

describe("cadence table", () => {
  it.each([
    { active: false, attention: true, failures: 0, expected: C.idleVisibleMs },
    { active: true, attention: true, failures: 0, expected: C.activeMs },
    { active: false, attention: false, failures: 0, expected: C.idleHiddenMs },
    { active: true, attention: false, failures: 0, expected: C.activeHiddenMs },
    { active: false, attention: true, failures: 1, expected: C.failureBackoffMinMs },
    { active: true, attention: true, failures: 2, expected: 2 * C.failureBackoffMinMs },
    { active: false, attention: false, failures: 30, expected: C.failureBackoffMaxMs },
  ])("active=$active attention=$attention failures=$failures", ({ expected, ...context }) => {
    expect(statusPollDelayMs(context, middle)).toBe(expected);
  });

  it("matches the owner's cadence", () => {
    expect(C).toMatchObject({
      idleVisibleMs: 600_000,
      activeMs: 60_000,
      idleHiddenMs: 1_800_000,
      activeHiddenMs: 120_000,
      regainedAfterMs: 120_000,
      failureBackoffMinMs: 60_000,
      failureBackoffMaxMs: 600_000,
      timeoutMs: 10_000,
      maxDevices: 16,
      maxConcurrent: 4,
    });
  });

  it("spreads every delay by at most ±10%", () => {
    const context = { active: false, attention: true, failures: 0 };
    expect(statusPollDelayMs(context, () => 0)).toBe(540_000);
    expect(statusPollDelayMs(context, () => 0.999_999)).toBe(660_000);
  });
});

describe("activity", () => {
  const state = (patch: Partial<JonesUpdateState>): JonesUpdateState => ({
    source: "jones-actions",
    channel: "jones-main",
    phase: "available",
    capability: { check: true, download: true, install: true, operations: 1 },
    ...patch,
  });
  const receipt = (phase: JonesOperationReceipt["phase"]) =>
    ({ phase, operationId: "op" }) as unknown as JonesOperationReceipt;
  it("treats host work, a restart and outstanding ledger work as active", () => {
    const fresh = (s: JonesUpdateState) => ({ state: s, freshness: "fresh" as const });
    expect(deviceStatusActive(fresh(state({})), undefined)).toBe(false);
    for (const phase of ["preparing", "downloading", "verifying", "installing"] as const)
      expect(deviceStatusActive(fresh(state({ phase })), undefined)).toBe(true);
    expect(deviceStatusActive(fresh(state({ phase: "checking" })), undefined)).toBe(false);
    expect(deviceStatusActive(fresh(state({ operation: receipt("handed-off") })), undefined)).toBe(
      true,
    );
    expect(deviceStatusActive(fresh(state({ operation: receipt("completed") })), undefined)).toBe(
      false,
    );
    const op = (r: JonesOperationReceipt | undefined) => ({
      operations: [
        {
          operationId: "op",
          step: "download" as const,
          lease: { claimant: "c", reservedAt: "", leaseUntil: "" },
          request: { kind: "operation-start" as const, input: {} as never },
          ...(r === undefined ? {} : { receipt: r }),
        },
      ],
    });
    expect(deviceStatusActive(undefined, op(undefined))).toBe(true);
    expect(deviceStatusActive(undefined, op(receipt("running")))).toBe(true);
    expect(deviceStatusActive(undefined, op(receipt("interrupted")))).toBe(false);
  });
});

describe("scheduler", () => {
  let now = 0;
  beforeEach(() => {
    vi.useFakeTimers();
    now = 0;
  });
  afterEach(() => {
    vi.useRealTimers();
  });
  const advance = async (ms: number) => {
    now += ms;
    await vi.advanceTimersByTimeAsync(ms);
  };
  function poller(patch: Partial<StatusPollerOptions> & { ids?: string[] } = {}) {
    const ticks: string[] = [];
    const health = new Map<string, { failures: number; lastSuccessAt?: number }>();
    const instance = createStatusPoller({
      devices: () => patch.ids ?? ["a"],
      isActive: () => false,
      attention: () => true,
      tick: async (id) => {
        ticks.push(id);
      },
      onHealth: (id, value) => health.set(id, value),
      now: () => now,
      random: middle,
      ...patch,
    });
    instance.sync();
    return { instance, ticks, health };
  }

  it("refreshes an idle visible device every 10 minutes, never sooner", async () => {
    const { instance, ticks } = poller();
    await advance(C.idleVisibleMs - 1);
    expect(ticks).toEqual([]);
    await advance(1);
    expect(ticks).toEqual(["a"]);
    await advance(C.idleVisibleMs);
    expect(ticks).toEqual(["a", "a"]);
    instance.close();
  });

  it("speeds up when a device becomes active and slows down when the window is hidden", async () => {
    let active = false;
    let attention = true;
    const { instance, ticks } = poller({ isActive: () => active, attention: () => attention });
    await advance(30_000);
    active = true;
    instance.reschedule();
    await advance(C.activeMs - 30_000);
    expect(ticks).toHaveLength(1);
    attention = false;
    instance.reschedule();
    await advance(C.activeMs);
    expect(ticks).toHaveLength(1);
    await advance(C.activeHiddenMs - C.activeMs);
    expect(ticks).toHaveLength(2);
    active = false;
    instance.reschedule();
    await advance(C.idleHiddenMs - 1);
    expect(ticks).toHaveLength(2);
    await advance(1);
    expect(ticks).toHaveLength(3);
    instance.close();
  });

  it("refreshes on regained focus only after two minutes without success", async () => {
    const { instance, ticks } = poller();
    await advance(C.idleVisibleMs);
    expect(ticks).toHaveLength(1);
    await advance(C.regainedAfterMs);
    instance.regained();
    await advance(0);
    expect(ticks).toHaveLength(1);
    await advance(1);
    instance.regained();
    await advance(0);
    expect(ticks).toHaveLength(2);
    instance.close();
  });

  it("keeps one refresh in flight per device and at most four overall", async () => {
    const pending: Array<() => void> = [];
    const ids = Array.from({ length: 20 }, (_, i) => `d${i}`);
    let started = 0;
    const { instance } = poller({
      ids,
      tick: () =>
        new Promise<void>((resolve) => {
          started++;
          pending.push(resolve);
        }),
    });
    await advance(C.idleVisibleMs);
    expect(started).toBe(C.maxConcurrent);
    instance.regained();
    instance.regained();
    await advance(0);
    expect(started).toBe(C.maxConcurrent);
    for (let round = 0; round < 4; round++) {
      for (const resolve of pending.splice(0)) resolve();
      await advance(0);
    }
    expect(started).toBe(C.maxDevices);
    instance.close();
  });

  it("times out a hung refresh, backs off 60s → 10 min and resets on success", async () => {
    let mode: "hang" | "fail" | "ok" = "hang";
    let aborted = 0;
    const { instance, ticks, health } = poller({
      tick: (id, signal) => {
        ticks.push(id);
        if (mode === "ok") return Promise.resolve();
        if (mode === "fail") return Promise.reject(new Error("unreachable"));
        signal.addEventListener("abort", () => aborted++);
        return new Promise<void>(() => {});
      },
    });
    await advance(C.idleVisibleMs);
    await advance(C.timeoutMs);
    expect(aborted).toBe(1);
    expect(health.get("a")).toEqual({ failures: 1 });
    mode = "fail";
    const delays = [60_000, 120_000, 240_000, 480_000, 600_000, 600_000];
    for (const [index, delay] of delays.entries()) {
      await advance(delay - 1);
      expect(ticks).toHaveLength(index + 1);
      await advance(1);
      expect(ticks).toHaveLength(index + 2);
    }
    mode = "ok";
    await advance(C.failureBackoffMaxMs);
    expect(health.get("a")).toEqual({ failures: 0, lastSuccessAt: now });
    const before = ticks.length;
    await advance(C.idleVisibleMs);
    expect(ticks).toHaveLength(before + 1);
    instance.close();
  });

  it("stops polling removed devices and everything after close", async () => {
    let ids = ["a", "b"];
    const ticks: string[] = [];
    const instance = createStatusPoller({
      devices: () => ids,
      isActive: () => false,
      attention: () => true,
      tick: async (id) => {
        ticks.push(id);
      },
      now: () => now,
      random: middle,
    });
    instance.sync();
    ids = ["a"];
    instance.sync();
    await advance(C.idleVisibleMs);
    expect(ticks).toEqual(["a"]);
    instance.close();
    await advance(C.idleHiddenMs * 2);
    expect(ticks).toEqual(["a"]);
  });
});
