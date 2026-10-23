import * as NodeOS from "node:os";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  assertResourceAdmission,
  cpuSample,
  memoryAdmitted,
  swapRate,
  vmSample,
  waitForResourceAdmission,
} from "./ios-admission.ts";
import type { AdmissionEvent, AdmissionWaitOptions } from "./ios-admission.ts";

describe("macOS resource admission", () => {
  const base = () => ({
    sourceSha: "a".repeat(40),
    hostname: NodeOS.hostname(),
    serialSlot: "owner-slot",
    measuredAt: 100,
    cpuUsed: 30,
    availableMemoryBytes: 1024 ** 3,
    memoryPressure: "normal",
    swapBytesPerSecond: 0,
    freeDiskBytes: 30 * 1024 ** 3,
  });
  const env = {
    GITHUB_ACTIONS: "true",
    RUNNER_ENVIRONMENT: "github-hosted",
    RUNNER_OS: "macOS",
    ImageOS: "macos26-arm64",
    GITHUB_RUN_ID: "123",
    GITHUB_RUN_ATTEMPT: "1",
  };
  const hosted = () => ({
    ...base(),
    hostClass: "github-hosted-ephemeral",
    runId: "123",
    runAttempt: "1",
    serialSlot: "gh-123-1",
    freeDiskBytes: 1024,
  });
  it("keeps laptop disk protection and binds the hosted exception to the actual job", () => {
    expect(() => assertResourceAdmission(base(), base().sourceSha, 101, {})).not.toThrow();
    expect(() =>
      assertResourceAdmission(
        { ...base(), freeDiskBytes: 29 * 1024 ** 3 },
        base().sourceSha,
        101,
        {},
      ),
    ).toThrow("30 GiB");
    expect(() => assertResourceAdmission(hosted(), base().sourceSha, 101, env)).not.toThrow();
    for (const changed of [
      { RUNNER_ENVIRONMENT: "self-hosted" },
      { GITHUB_RUN_ID: "124" },
      { ImageOS: "macos15" },
    ])
      expect(() =>
        assertResourceAdmission(hosted(), base().sourceSha, 101, { ...env, ...changed }),
      ).toThrow();
    expect(() =>
      assertResourceAdmission({ ...hosted(), serialSlot: "other" }, base().sourceSha, 101, env),
    ).toThrow("slot");
    expect(() =>
      assertResourceAdmission({ ...base(), cpuUsed: 85 }, base().sourceSha, 101, {}),
    ).toThrow("CPU");
    expect(() => assertResourceAdmission(base(), base().sourceSha, 60_100, {})).toThrow("fresh");
  });
  it("holds critical pressure only above 10 MiB/s and rejects unknown samples", () => {
    expect(memoryAdmitted("warning", 100 * 1024 ** 2)).toBe(true);
    expect(memoryAdmitted("normal", 100 * 1024 ** 2)).toBe(true);
    expect(memoryAdmitted("critical", 10 * 1024 ** 2)).toBe(true);
    expect(memoryAdmitted("critical", 10 * 1024 ** 2 + 1)).toBe(false);
    expect(() => memoryAdmitted("unknown", 0)).toThrow("unavailable");
    expect(() => memoryAdmitted("normal", Number.NaN)).toThrow("unavailable");
  });
  it("uses the second CPU sample and actual elapsed swap interval", () => {
    expect(
      cpuSample(
        "CPU usage: 80.0% user, 10.0% sys, 10.0% idle\nCPU usage: 20.0% user, 5.0% sys, 75.0% idle",
      ),
    ).toBe(25);
    expect(() => cpuSample("CPU usage: 20% user, 5% sys, 75% idle")).toThrow("Two");
    const sample = vmSample(
      "Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 10.\nPages inactive: 20.\nPages speculative: 3.\nSwapins: 2.\nSwapouts: 4.\n",
    );
    expect(sample.availableMemoryBytes).toBe(33 * 16384);
    expect(swapRate(sample, { ...sample, swap: 10 }, 2)).toBe(2 * 16384);
    expect(() => swapRate(sample, { ...sample, swap: 1 }, 2)).toThrow("invalid");
    expect(() => swapRate(sample, sample, 0)).toThrow("invalid");
  });
});

describe("bounded CPU readiness", () => {
  const harness = (cpus = [30]) => {
    let time = 100,
      calls = 0,
      heavyStarts = 0;
    const controller = new AbortController();
    const events: AdmissionEvent[] = [];
    const waits: number[] = [];
    const sample = () => ({
      sourceSha: "a".repeat(40),
      hostname: NodeOS.hostname(),
      serialSlot: "owner-slot",
      measuredAt: time,
      cpuUsed: cpus[Math.min(calls++, cpus.length - 1)],
      availableMemoryBytes: 1024 ** 3,
      memoryPressure: "normal",
      swapBytesPerSecond: 0,
      freeDiskBytes: 30 * 1024 ** 3,
    });
    const options: AdmissionWaitOptions = {
      sourceSha: "a".repeat(40),
      serialSlot: "owner-slot",
      signal: controller.signal,
      sample: async () => sample(),
      now: () => time,
      monotonicNow: () => time,
      env: {},
      record: async (event) => {
        events.push(event);
      },
      wait: async (ms) => {
        waits.push(ms);
        time += ms;
      },
    };
    return {
      options,
      events,
      waits,
      controller,
      sample,
      advance: (ms: number) => {
        time += ms;
      },
      counts: () => ({ calls, heavyStarts }),
      run: async () => {
        const admitted = await waitForResourceAdmission(options);
        heavyStarts++;
        return admitted;
      },
    };
  };

  it("starts immediately on a fresh passing sample without waiting", async () => {
    const h = harness();
    expect((await h.run()).cpuUsed).toBe(30);
    expect(h.waits).toEqual([]);
    expect(h.counts()).toEqual({ calls: 1, heavyStarts: 1 });
    expect(h.events.map((event) => event.kind)).toEqual(["started", "sample", "ready"]);
    expect(h.events[0]).toMatchObject({ at: 100, deadlineAt: 120_100, elapsedMs: 0 });
  });

  it("retains every held sample and starts once after a fresh passing sample", async () => {
    const h = harness([85, 100, 84.99]);
    expect((await h.run()).cpuUsed).toBe(84.99);
    expect(h.waits).toEqual([20_000, 20_000]);
    expect(h.counts()).toEqual({ calls: 3, heavyStarts: 1 });
    expect(
      h.events.filter((event) => event.kind === "sample").map((event) => event.sample),
    ).toEqual([
      expect.objectContaining({ cpuUsed: 85, measuredAt: 100 }),
      expect.objectContaining({ cpuUsed: 100, measuredAt: 20_100 }),
      expect.objectContaining({ cpuUsed: 84.99, measuredAt: 40_100 }),
    ]);
    expect(h.events.map((event) => event.kind)).toEqual([
      "started",
      "sample",
      "held",
      "sample",
      "held",
      "sample",
      "ready",
    ]);
  });

  it("stops permanent busy at 120 seconds with no heavy work or deadline sample", async () => {
    const h = harness([85]);
    await expect(h.run()).rejects.toThrow("deadline");
    expect(h.waits).toEqual(Array(6).fill(20_000));
    expect(h.counts()).toEqual({ calls: 6, heavyStarts: 0 });
    expect(h.events.at(-1)).toMatchObject({
      kind: "aborted",
      at: 120_100,
      deadlineAt: 120_100,
      elapsedMs: 120_000,
      reason: "CPU admission readiness deadline exceeded",
    });
  });

  it.each([
    { cpuUsed: Number.NaN },
    { cpuUsed: -1 },
    { cpuUsed: 101 },
    { cpuUsed: undefined },
    { measuredAt: -60_000 },
    { measuredAt: 101 },
    { sourceSha: "b".repeat(40) },
    { hostname: "different-host" },
    { serialSlot: "other-slot" },
    { hostClass: "unknown" },
    { availableMemoryBytes: 0 },
    { memoryPressure: "unknown" },
    { memoryPressure: "critical", swapBytesPerSecond: 10 * 1024 ** 2 + 1 },
    { swapBytesPerSecond: Number.NaN },
    { freeDiskBytes: 1 },
  ])("fails closed without retry for invalid or non-CPU-held evidence: %j", async (patch) => {
    const h = harness([85]);
    h.options.sample = async () => ({ ...h.sample(), ...patch });
    await expect(h.run()).rejects.toThrow();
    expect(h.waits).toEqual([]);
    expect(h.counts()).toEqual({ calls: 1, heavyStarts: 0 });
    expect(h.events.map((event) => event.kind)).toEqual(["started", "sample", "aborted"]);
  });

  it("does not retry missing evidence or collector errors, even CPU assertion text", async () => {
    for (const collect of [
      async () => undefined,
      async () => {
        throw new Error("CPU admission held");
      },
    ]) {
      const h = harness();
      h.options.sample = collect;
      await expect(h.run()).rejects.toThrow();
      expect(h.waits).toEqual([]);
      expect(h.counts().heavyStarts).toBe(0);
      expect(h.events.at(-1)?.kind).toBe("aborted");
    }
  });

  it("does not sample after cancellation and cancels during a held wait", async () => {
    const before = harness();
    before.controller.abort(new Error("owner cancelled"));
    await expect(before.run()).rejects.toThrow("owner cancelled");
    expect(before.counts()).toEqual({ calls: 0, heavyStarts: 0 });
    const held = harness([85]);
    held.options.wait = async () => {
      held.advance(5_000);
      held.controller.abort(new Error("owner cancelled"));
    };
    await expect(held.run()).rejects.toThrow("owner cancelled");
    expect(held.counts()).toEqual({ calls: 1, heavyStarts: 0 });
    expect(held.events.at(-1)).toMatchObject({
      kind: "aborted",
      at: 5_100,
      reason: "owner cancelled",
    });
  });

  it("rejects samples returned after cancellation or the deadline", async () => {
    for (const cancel of [true, false]) {
      const h = harness();
      h.options.sample = async () => {
        if (cancel) h.controller.abort(new Error("owner cancelled"));
        else h.advance(120_000);
        return h.sample();
      };
      await expect(h.run()).rejects.toThrow(cancel ? "owner cancelled" : "deadline");
      expect(h.counts()).toEqual({ calls: 1, heavyStarts: 0 });
      expect(h.events.map((event) => event.kind)).toEqual(["started", "sample", "aborted"]);
    }
  });

  it("caps the last wait by time spent collecting samples", async () => {
    const h = harness([85]);
    h.options.sample = async () => {
      h.advance(35_000);
      return h.sample();
    };
    await expect(h.run()).rejects.toThrow("deadline");
    expect(h.waits).toEqual([20_000, 20_000]);
    expect(h.counts().heavyStarts).toBe(0);
    const last = harness([85]);
    last.options.sample = async () => {
      last.advance(110_000);
      return last.sample();
    };
    await expect(last.run()).rejects.toThrow("deadline");
    expect(last.waits).toEqual([10_000]);
    expect(last.counts()).toEqual({ calls: 1, heavyStarts: 0 });
  });

  it("rechecks freshness and cancellation after evidence writes", async () => {
    for (const cancel of [true, false]) {
      const h = harness();
      h.options.record = async (event) => {
        h.events.push(event);
        if (event.kind === "ready") {
          if (cancel) h.controller.abort(new Error("owner cancelled"));
          else h.advance(60_000);
        }
      };
      await expect(h.run()).rejects.toThrow(cancel ? "owner cancelled" : "fresh");
      expect(h.counts().heavyStarts).toBe(0);
      expect(h.events.at(-1)?.kind).toBe("aborted");
    }
  });

  it("fails closed when evidence cannot be retained", async () => {
    const h = harness();
    h.options.record = async () => {
      throw new Error("evidence unavailable");
    };
    await expect(h.run()).rejects.toThrow("Admission and evidence recording failed");
    expect(h.counts()).toEqual({ calls: 0, heavyStarts: 0 });
  });

  it("actively aborts an in-flight collector at the deadline", async () => {
    vi.useFakeTimers();
    try {
      const h = harness();
      h.options.now = Date.now;
      h.options.monotonicNow = Date.now;
      let sampled = false;
      h.options.sample = (signal) =>
        new Promise((_, reject) => {
          sampled = true;
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      const result = expect(h.run()).rejects.toThrow("deadline");
      await vi.advanceTimersByTimeAsync(120_000);
      await result;
      expect(sampled).toBe(true);
      expect(h.counts().heavyStarts).toBe(0);
      expect(h.events.at(-1)).toMatchObject({ kind: "aborted", elapsedMs: 120_000 });
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});
