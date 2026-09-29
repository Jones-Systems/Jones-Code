import type { HostStatus, HostStatusSnapshot } from "@t3tools/contracts";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { fetchHostStatus, hostStatusMetrics, observeHostStatus } from "./hostStatus";

const gib = 1024 ** 3;
const available = {
  id: "vps",
  status: "available",
  load1: 4,
  logicalCpuCount: 4,
  availableMemoryBytes: 4 * gib,
  totalMemoryBytes: 16 * gib,
  sampledAt: "2026-09-29T12:00:00.000Z",
} satisfies Extract<HostStatus, { status: "available" }>;
const snapshot: HostStatusSnapshot = { hosts: [available] };

class Visibility extends EventTarget {
  visibilityState: DocumentVisibilityState = "visible";
  set(value: DocumentVisibilityState) {
    this.visibilityState = value;
    this.dispatchEvent(new Event("visibilitychange"));
  }
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("host status metrics", () => {
  it.each([
    [1, 25, "healthy"],
    [3.2, 25, "warning"],
    [4, 25, "critical"],
    [1, 7, "warning"],
    [1, 4, "critical"],
    [3.2, 4, "critical"],
    [4, 7, "critical"],
    [3.2, 7, "warning"],
    [4, 4, "critical"],
  ] as const)("combines load %s and RAM %s percent into %s", (load1, percent, health) => {
    expect(
      hostStatusMetrics({
        ...available,
        load1,
        availableMemoryBytes: percent,
        totalMemoryBytes: 100,
      }),
    ).toHaveProperty("health", health);
  });
  it("keeps combined health unavailable for missing or stale hosts", () => {
    expect(hostStatusMetrics(undefined)).toHaveProperty("health", "unavailable");
    expect(
      hostStatusMetrics({ id: "home", status: "unavailable", reason: "stale" }),
    ).toHaveProperty("health", "unavailable");
  });
  it("omits trailing zeros for whole metrics and keeps one decimal for fractional values", () => {
    expect(
      hostStatusMetrics({
        ...available,
        load1: 17,
        logicalCpuCount: 16,
        availableMemoryBytes: 44 * gib,
        totalMemoryBytes: 64 * gib,
      }),
    ).toMatchObject({ load: "17", ram: "44" });
    expect(
      hostStatusMetrics({ ...available, load1: 17.26, availableMemoryBytes: 4.26 * gib }),
    ).toMatchObject({ load: "17.3", ram: "4.3" });
  });
  it("keeps raw load above CPU count and classifies per-core thresholds", () => {
    expect(hostStatusMetrics({ ...available, load1: 7 }).load).toBe("7");
    expect(hostStatusMetrics({ ...available, load1: 3.19 }).loadHealth).toBe("healthy");
    expect(hostStatusMetrics({ ...available, load1: 3.2 }).loadHealth).toBe("warning");
    expect(hostStatusMetrics(available).loadHealth).toBe("critical");
    expect(hostStatusMetrics(available).detail).toContain("4 logical CPUs");
  });
  it("uses GiB and the strict available-RAM percentage boundaries", () => {
    expect(hostStatusMetrics(available).ram).toBe("4");
    for (const [fraction, health] of [
      [0.1, "healthy"],
      [0.099, "warning"],
      [0.05, "warning"],
      [0.049, "critical"],
    ] as const) {
      expect(
        hostStatusMetrics({
          ...available,
          availableMemoryBytes: 10000 * fraction,
          totalMemoryBytes: 10000,
        }).ramHealth,
      ).toBe(health);
    }
  });
  it("keeps missing and unavailable hosts gray with a reason", () => {
    expect(hostStatusMetrics(undefined).loadHealth).toBe("unavailable");
    expect(hostStatusMetrics({ id: "home", status: "unavailable", reason: "stale" })).toMatchObject(
      { ram: "—", ramHealth: "unavailable", detail: "Host status sample is stale" },
    );
  });
});

describe("visible host-status polling", () => {
  it("polls every ten seconds, pauses hidden pages, and releases listeners/timer on cleanup", async () => {
    vi.useFakeTimers();
    const visibility = new Visibility();
    const request = vi.fn().mockResolvedValue(snapshot);
    const receive = vi.fn();
    const stop = observeHostStatus(visibility, receive, request);
    await vi.advanceTimersByTimeAsync(0);
    expect(receive).toHaveBeenLastCalledWith(snapshot);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(request).toHaveBeenCalledTimes(2);
    visibility.set("hidden");
    await vi.advanceTimersByTimeAsync(20_000);
    expect(request).toHaveBeenCalledTimes(2);
    visibility.set("visible");
    await vi.advanceTimersByTimeAsync(0);
    expect(request).toHaveBeenCalledTimes(3);
    stop();
    expect(request.mock.calls[0]?.[0].aborted).toBe(true);
    receive.mockClear();
    visibility.set("visible");
    await vi.advanceTimersByTimeAsync(20_000);
    expect(request).toHaveBeenCalledTimes(3);
    expect(receive).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
  it("does not overlap requests or deliver a result after disposal", async () => {
    vi.useFakeTimers();
    const visibility = new Visibility();
    const receive = vi.fn();
    let resolve!: (value: HostStatusSnapshot) => void;
    const request = vi.fn(
      () =>
        new Promise<HostStatusSnapshot>((done) => {
          resolve = done;
        }),
    );
    const stop = observeHostStatus(visibility, receive, request);
    await vi.advanceTimersByTimeAsync(30_000);
    visibility.set("visible");
    expect(request).toHaveBeenCalledTimes(1);
    receive.mockClear();
    stop();
    resolve(snapshot);
    await vi.advanceTimersByTimeAsync(0);
    expect(receive).not.toHaveBeenCalled();
  });
  it("clears old healthy values when a request fails and recovers on the next poll", async () => {
    vi.useFakeTimers();
    const receive = vi.fn();
    const request = vi
      .fn()
      .mockResolvedValueOnce(snapshot)
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValue(snapshot);
    const stop = observeHostStatus(new Visibility(), receive, request);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(receive).toHaveBeenLastCalledWith(null);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(receive).toHaveBeenLastCalledWith(snapshot);
    stop();
  });
});

describe("typed gateway request", () => {
  it("requests only the primary gateway endpoint with browser credentials", async () => {
    vi.stubGlobal("window", {
      location: { origin: "https://app.example", href: "https://app.example/" },
    });
    const fetch = vi.fn().mockResolvedValue(Response.json(snapshot));
    vi.stubGlobal("fetch", fetch);
    expect(await fetchHostStatus(new AbortController().signal)).toEqual(snapshot);
    const request = new Request(fetch.mock.calls[0]?.[0], fetch.mock.calls[0]?.[1]);
    expect(request.url).toBe("https://app.example/api/host-status");
    expect(request.credentials).toBe("include");
  });
  it("rejects malformed responses rather than displaying healthy values", async () => {
    vi.stubGlobal("window", {
      location: { origin: "https://app.example", href: "https://app.example/" },
    });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(Response.json({ hosts: [{ ...available, load1: -1 }] })),
    );
    await expect(fetchHostStatus(new AbortController().signal)).rejects.toThrow();
  });
});
