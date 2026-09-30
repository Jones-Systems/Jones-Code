import type { HostStatus, HostStatusSnapshot } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { fetchHostStatus, hostStatusMetrics, observeHostStatus } from "./hostStatus";

const gib = 1024 ** 3;
const available = {
  id: "vps",
  status: "available",
  cpuUsagePercent: 23,
  logicalCpuCount: 4,
  occupiedMemoryBytes: 12 * gib,
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

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date(available.sampledAt));
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("host status metrics", () => {
  it("shows CPU percentage and occupied/total RAM in GiB with an explicit definition", () => {
    const metrics = hostStatusMetrics(available);
    expect(metrics).toMatchObject({ cpu: "23%", ram: "12/16 GiB", health: "healthy" });
    expect(metrics.detail).toContain("CPU 23%");
    expect(metrics.detail).toContain("Occupied RAM 12/16 GiB");
    expect(metrics.detail).toContain("includes reclaimable cache");
    expect(metrics.detail).toContain("not memory pressure");
    expect(hostStatusMetrics({ ...available, logicalCpuCount: 128 }).cpu).toBe("23%");
  });
  it.each([[0, "healthy"], [79.9, "healthy"], [80, "warning"], [95, "critical"], [100, "critical"]] as const)(
    "colors CPU %s independently of occupied memory", (cpuUsagePercent, health) => {
      expect(hostStatusMetrics({ ...available, cpuUsagePercent, occupiedMemoryBytes: 16 * gib }).health).toBe(health);
    },
  );
  it("formats fractional GiB and zero usage without confusing zero with missing", () => {
    expect(hostStatusMetrics({ ...available, cpuUsagePercent: 0, occupiedMemoryBytes: 0 })).toMatchObject({ cpu: "0%", ram: "0/16 GiB" });
    expect(hostStatusMetrics({ ...available, cpuUsagePercent: 23.26, occupiedMemoryBytes: 12.26 * gib })).toMatchObject({ cpu: "23.3%", ram: "12.3/16 GiB" });
    expect(hostStatusMetrics(undefined)).toMatchObject({ cpu: "—", ram: "—", health: "unavailable" });
  });
  it("hides values at the thirty-second source freshness boundary", () => {
    vi.advanceTimersByTime(29_999);
    expect(hostStatusMetrics(available).cpu).toBe("23%");
    vi.advanceTimersByTime(1);
    expect(hostStatusMetrics(available)).toMatchObject({ cpu: "—", ram: "—", health: "unavailable", detail: "Host status sample is stale" });
    expect(hostStatusMetrics({ id: "home", status: "unavailable", reason: "stale" }).health).toBe("unavailable");
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

describe("source timestamp expiry", () => {
  it("expires a presented sample while the next refresh remains in flight", async () => {
    const receive = vi.fn();
    let resolve!: (value: HostStatusSnapshot) => void;
    const request = vi.fn().mockResolvedValueOnce(snapshot).mockImplementation(() => new Promise<HostStatusSnapshot>((done) => { resolve = done; }));
    const stop = observeHostStatus(new Visibility(), receive, request);
    await vi.advanceTimersByTimeAsync(29_999);
    expect(receive).toHaveBeenLastCalledWith(snapshot);
    await vi.advanceTimersByTimeAsync(1);
    expect(receive).toHaveBeenLastCalledWith({ hosts: [{ id: "vps", status: "unavailable", reason: "stale" }] });
    expect(request).toHaveBeenCalledTimes(2);
    resolve(snapshot);
    await vi.advanceTimersByTimeAsync(0);
    expect(receive).toHaveBeenLastCalledWith({ hosts: [{ id: "vps", status: "unavailable", reason: "stale" }] });
    stop();
    expect(vi.getTimerCount()).toBe(0);
  });
  it("expires each host from its own source time and rejects already stale samples", async () => {
    const receive = vi.fn();
    const older = { ...available, sampledAt: new Date(Date.now() - 25_000).toISOString() };
    const mini = { ...available, id: "mini" as const };
    const stop = observeHostStatus(new Visibility(), receive, vi.fn().mockResolvedValue({ hosts: [older, mini] }));
    await vi.advanceTimersByTimeAsync(5_000);
    expect(receive).toHaveBeenLastCalledWith({ hosts: [{ id: "vps", status: "unavailable", reason: "stale" }, mini] });
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
      vi.fn().mockResolvedValue(Response.json({ hosts: [{ ...available, cpuUsagePercent: -1 }] })),
    );
    await expect(fetchHostStatus(new AbortController().signal)).rejects.toThrow();
  });
});
