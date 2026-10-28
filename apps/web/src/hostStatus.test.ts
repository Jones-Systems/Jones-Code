import { EnvironmentId, type HostStatus, type HostStatusSnapshot } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import {
  type HostStatusCache,
  fetchHostStatus,
  hostStatusMetrics,
  observeHostStatus,
} from "./hostStatus";
import type * as EffectTypes from "effect/Effect";
import type {
  EnvironmentRegistry as RegistryTypes,
  EnvironmentSupervisor as SupervisorTypes,
} from "@t3tools/client-runtime/connection";
import {
  BearerConnectionTarget,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "@t3tools/client-runtime/connection";

const harness = vi.hoisted(() => ({
  prepared: new Map<string, PreparedConnection>(),
  selected: [] as string[],
}));

vi.mock("./connection/runtime", async () => {
  const { EnvironmentRegistry, EnvironmentSupervisor } =
    await import("@t3tools/client-runtime/connection");
  const Effect = await import("effect/Effect");
  const Layer = await import("effect/Layer");
  const Option = await import("effect/Option");
  const SubscriptionRef = await import("effect/SubscriptionRef");
  const { Atom } = await import("effect/reactivity");
  const { layerRemoteHttpClient } = await import("@t3tools/client-runtime/rpc");
  const registry = {
    run: <A, E, R>(environmentId: EnvironmentId, effect: EffectTypes.Effect<A, E, R>) =>
      Effect.gen(function* () {
        harness.selected.push(environmentId);
        const prepared = yield* SubscriptionRef.make(
          Option.fromNullishOr(harness.prepared.get(environmentId)),
        );
        return yield* effect.pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, {
            prepared,
          } as unknown as SupervisorTypes.EnvironmentSupervisor["Service"]),
        );
      }),
  } as unknown as RegistryTypes.EnvironmentRegistry["Service"];
  return {
    connectionAtomRuntime: Atom.runtime(
      Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, registry).pipe(
        Layer.provideMerge(layerRemoteHttpClient((...args) => globalThis.fetch(...args))),
      ),
    ),
  };
});

const selectedEnvironment = EnvironmentId.make("remote-selected");
function prepare(base = "https://app.example", token: string | null = null) {
  harness.prepared.set(selectedEnvironment, {
    environmentId: selectedEnvironment,
    label: "Synthetic selected server",
    httpBaseUrl: base,
    socketUrl: `${base.replace(/^http/, "ws")}/ws`,
    httpAuthorization: token ? { _tag: "Bearer", token } : null,
    target: token
      ? new BearerConnectionTarget({
          environmentId: selectedEnvironment,
          label: "Synthetic",
          connectionId: "fixture",
        })
      : new PrimaryConnectionTarget({
          environmentId: selectedEnvironment,
          label: "Synthetic",
          httpBaseUrl: base,
          wsBaseUrl: `${base.replace(/^http/, "ws")}/ws`,
        }),
  });
}

const gib = 1024 ** 3;
const available = {
  id: "vps",
  status: "available",
  cpuUsagePercent: 23,
  logicalCpuCount: 4,
  occupiedMemoryBytes: 12 * gib,
  totalMemoryBytes: 16 * gib,
  availableMemoryBytes: 4 * gib,
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
  harness.prepared.clear();
  harness.selected.length = 0;
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("host status metrics", () => {
  it("shows whole-number total CPU percentage and available RAM in GiB", () => {
    const metrics = hostStatusMetrics(available);
    expect(metrics).toMatchObject({
      cpu: "23%",
      ram: "4",
      cpuHealth: "healthy",
      ramHealth: "warning",
      health: "warning",
    });
    expect(metrics.detail).toContain("CPU 23% of total cores");
    expect(metrics.detail).toContain("RAM 4 GiB available of 16 GiB");
    expect(hostStatusMetrics({ ...available, logicalCpuCount: 128 }).cpu).toBe("23%");
  });
  it.each([
    [0, "healthy"],
    [49.9, "healthy"],
    [50, "warning"],
    [75, "warning"],
    [75.1, "elevated"],
    [90, "elevated"],
    [90.1, "critical"],
    [100, "critical"],
  ] as const)("colors each metric from unrounded utilization %s", (percent, health) => {
    expect(
      hostStatusMetrics({ ...available, cpuUsagePercent: percent, availableMemoryBytes: 16 * gib }),
    ).toMatchObject({ cpuHealth: health, ramHealth: "healthy", health });
    expect(
      hostStatusMetrics({
        ...available,
        cpuUsagePercent: 0,
        totalMemoryBytes: 1000,
        availableMemoryBytes: Math.round(1000 * (1 - percent / 100)),
      }),
    ).toMatchObject({ cpuHealth: "healthy", ramHealth: health, health });
  });
  it("rounds displayed values without confusing zero or unavailable RAM", () => {
    expect(
      hostStatusMetrics({ ...available, cpuUsagePercent: 0, availableMemoryBytes: 0 }),
    ).toMatchObject({ cpu: "0%", ram: "0", ramHealth: "critical", health: "critical" });
    expect(
      hostStatusMetrics({
        ...available,
        cpuUsagePercent: 23.76,
        availableMemoryBytes: 12.76 * gib,
      }),
    ).toMatchObject({ cpu: "24%", ram: "13" });
    const { availableMemoryBytes: _memory, ...olderSample } = available;
    expect(hostStatusMetrics(olderSample)).toMatchObject({
      cpu: "23%",
      ram: "—",
      ramHealth: "unavailable",
    });
    expect(hostStatusMetrics(undefined)).toMatchObject({
      cpu: "—",
      ram: "—",
      cpuHealth: "unavailable",
      ramHealth: "unavailable",
      health: "unavailable",
    });
  });
  it.each(["mini", "home"] as const)(
    "shows combined swap instead of low RAM alarms on %s",
    (id) => {
      const mac = { ...available, id, availableMemoryBytes: 0, swapIoBytesPerSecond: 0 };
      expect(hostStatusMetrics(mac)).toMatchObject({
        ram: "0.0 mb/s",
        ramHealth: "healthy",
        health: "healthy",
      });
      expect(hostStatusMetrics(mac).tooltipLines).toContain("RAM 0 GiB available of 16 GiB");
      expect(hostStatusMetrics(mac).tooltipLines).toContain("Swap I/O 0.0 MiB/s");
      expect(hostStatusMetrics({ ...mac, swapIoBytesPerSecond: 10 * 1024 ** 2 })).toMatchObject({
        ramHealth: "healthy",
      });
      expect(hostStatusMetrics({ ...mac, swapIoBytesPerSecond: 10.01 * 1024 ** 2 })).toMatchObject({
        ram: "10.0 mb/s",
        ramHealth: "warning",
        health: "warning",
      });
      expect(hostStatusMetrics({ ...mac, cpuUsagePercent: 95 })).toMatchObject({
        cpuHealth: "critical",
        ramHealth: "healthy",
        health: "critical",
      });
      const { swapIoBytesPerSecond: _swap, ...olderMac } = mac;
      expect(hostStatusMetrics(olderMac)).toMatchObject({
        ram: "—",
        ramHealth: "unavailable",
        health: "healthy",
      });
      expect(hostStatusMetrics(olderMac).detail).toContain("Swap I/O unavailable");
    },
  );
  it.each(["vps", "test", "mini", "home"] as const)(
    "provides one concise line per metric on %s, with swap only on Macs",
    (id) => {
      const metrics = hostStatusMetrics({
        ...available,
        id,
        cpuUsagePercent: 18,
        availableMemoryBytes: 46 * gib,
        totalMemoryBytes: 64 * gib,
        swapIoBytesPerSecond: 0.3 * 1024 ** 2,
        availableStorageBytes: 220 * gib,
        totalStorageBytes: 500 * gib,
        storageMountPoint: "/",
      });
      expect(metrics.tooltipLines).toEqual([
        "CPU 18% of total cores",
        "RAM 46 GiB available of 64 GiB",
        ...(id === "mini" || id === "home" ? ["Swap I/O 0.3 MiB/s"] : []),
        "Storage 220 GiB free of 500 GiB",
      ]);
      expect(metrics.detail).toBe(metrics.tooltipLines.join(". "));
    },
  );
  it("identifies a non-root filesystem and preserves zero and fractional capacities", () => {
    expect(
      hostStatusMetrics({
        ...available,
        availableStorageBytes: 0,
        totalStorageBytes: 500.5 * gib,
        storageMountPoint: "/srv/data",
      }).tooltipLines,
    ).toContain("Storage (/srv/data) 0 GiB free of 500.5 GiB");
  });
  it("keeps CPU available with missing optional readings from older servers", () => {
    const { availableMemoryBytes: _memory, ...olderSample } = available;
    expect(hostStatusMetrics(olderSample).tooltipLines).toEqual([
      "CPU 23% of total cores",
      "RAM unavailable",
      "Storage unavailable",
    ]);
    expect(hostStatusMetrics({ ...olderSample, id: "home" }).tooltipLines).toEqual([
      "CPU 23% of total cores",
      "RAM unavailable",
      "Swap I/O unavailable",
      "Storage unavailable",
    ]);
    expect(hostStatusMetrics({ ...available, availableStorageBytes: 0 }).tooltipLines).toContain(
      "Storage unavailable",
    );
    expect(
      hostStatusMetrics({ ...available, totalStorageBytes: 500 * gib }).tooltipLines,
    ).toContain("Storage unavailable");
  });
  it("hides values at the thirty-second source freshness boundary", () => {
    vi.advanceTimersByTime(29_999);
    expect(hostStatusMetrics(available).cpu).toBe("23%");
    vi.advanceTimersByTime(1);
    expect(hostStatusMetrics(available)).toMatchObject({
      cpu: "—",
      ram: "—",
      health: "unavailable",
      detail: "Host status sample is stale",
      tooltipLines: ["Host status sample is stale"],
    });
    expect(hostStatusMetrics({ id: "home", status: "unavailable", reason: "stale" }).health).toBe(
      "unavailable",
    );
  });
  it.each([
    [5_000, "23%"],
    [5_001, "—"],
  ] as const)("applies the gateway clock-skew boundary at %s ms ahead", (offset, cpu) => {
    const sampledAt = new Date(vi.getMockedSystemTime()!.getTime() + offset).toISOString();
    expect(hostStatusMetrics({ ...available, sampledAt }).cpu).toBe(cpu);
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
  it("retains all fresh hosts after refresh failures, expires them at source age, and recovers", async () => {
    const hosts = (["vps", "test", "mini", "home"] as const).map((id) => ({ ...available, id }));
    const fullSnapshot: HostStatusSnapshot = { hosts };
    const cache: HostStatusCache = { snapshot: null, settledAt: -Infinity };
    const receive = vi.fn();
    let resolve!: (value: HostStatusSnapshot) => void;
    const request = vi
      .fn()
      .mockResolvedValueOnce(fullSnapshot)
      .mockRejectedValueOnce(new Error("offline"))
      .mockRejectedValueOnce(new Error("still offline"))
      .mockImplementation(
        () =>
          new Promise<HostStatusSnapshot>((done) => {
            resolve = done;
          }),
      );
    const stop = observeHostStatus(new Visibility(), receive, request, cache);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(receive).toHaveBeenLastCalledWith(fullSnapshot);
    expect(cache.snapshot).toEqual(fullSnapshot);
    expect(request).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(receive).toHaveBeenLastCalledWith(fullSnapshot);
    expect(request).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(9_999);
    expect(receive).toHaveBeenLastCalledWith(fullSnapshot);
    await vi.advanceTimersByTimeAsync(1);
    const expired = {
      hosts: hosts.map(({ id }) => ({ id, status: "unavailable", reason: "stale" })),
    };
    expect(receive).toHaveBeenLastCalledWith(expired);
    expect(cache.snapshot).toEqual(expired);
    expect(request).toHaveBeenCalledTimes(4);
    const recovered = {
      hosts: hosts.map((host) => ({
        ...host,
        sampledAt: new Date(vi.getMockedSystemTime()!.getTime()).toISOString(),
      })),
    };
    resolve(recovered);
    await vi.advanceTimersByTimeAsync(0);
    expect(receive).toHaveBeenLastCalledWith(recovered);
    stop();
    expect(vi.getTimerCount()).toBe(0);
  });
  it("replaces a fresh host with an explicit unavailable response while preserving other hosts", async () => {
    const mini = { ...available, id: "mini" as const };
    const unavailableSnapshot: HostStatusSnapshot = {
      hosts: [{ id: "vps", status: "unavailable", reason: "upstream_unavailable" }, mini],
    };
    const receive = vi.fn();
    const request = vi
      .fn()
      .mockResolvedValueOnce({ hosts: [available, mini] })
      .mockResolvedValue(unavailableSnapshot);
    const stop = observeHostStatus(new Visibility(), receive, request);
    await vi.advanceTimersByTimeAsync(0);
    expect(receive).toHaveBeenLastCalledWith({ hosts: [available, mini] });
    await vi.advanceTimersByTimeAsync(10_000);
    expect(receive).toHaveBeenLastCalledWith(unavailableSnapshot);
    stop();
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("source timestamp expiry", () => {
  it("expires a presented sample while the next refresh remains in flight", async () => {
    const receive = vi.fn();
    let resolve!: (value: HostStatusSnapshot) => void;
    const request = vi
      .fn()
      .mockResolvedValueOnce(snapshot)
      .mockImplementation(
        () =>
          new Promise<HostStatusSnapshot>((done) => {
            resolve = done;
          }),
      );
    const stop = observeHostStatus(new Visibility(), receive, request);
    await vi.advanceTimersByTimeAsync(29_999);
    expect(receive).toHaveBeenLastCalledWith(snapshot);
    await vi.advanceTimersByTimeAsync(1);
    expect(receive).toHaveBeenLastCalledWith({
      hosts: [{ id: "vps", status: "unavailable", reason: "stale" }],
    });
    expect(request).toHaveBeenCalledTimes(2);
    resolve(snapshot);
    await vi.advanceTimersByTimeAsync(0);
    expect(receive).toHaveBeenLastCalledWith({
      hosts: [{ id: "vps", status: "unavailable", reason: "stale" }],
    });
    stop();
    expect(vi.getTimerCount()).toBe(0);
  });
  it("expires each host from its own source time and rejects already stale samples", async () => {
    const receive = vi.fn();
    const older = {
      ...available,
      sampledAt: new Date(vi.getMockedSystemTime()!.getTime() - 25_000).toISOString(),
    };
    const mini = { ...available, id: "mini" as const };
    const stop = observeHostStatus(
      new Visibility(),
      receive,
      vi.fn().mockResolvedValue({ hosts: [older, mini] }),
    );
    await vi.advanceTimersByTimeAsync(5_000);
    expect(receive).toHaveBeenLastCalledWith({
      hosts: [{ id: "vps", status: "unavailable", reason: "stale" }, mini],
    });
    stop();
  });
});

describe("typed selected-environment gateway request", () => {
  beforeEach(() => {
    vi.useRealTimers();
    prepare();
  });
  it("requests the selected browser gateway endpoint with browser credentials", async () => {
    vi.stubGlobal("window", {
      location: { origin: "https://app.example", href: "https://app.example/" },
    });
    const fetch = vi.fn().mockResolvedValue(Response.json(snapshot));
    vi.stubGlobal("fetch", fetch);
    expect(await fetchHostStatus(selectedEnvironment, new AbortController().signal)).toEqual(
      snapshot,
    );
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
    await expect(
      fetchHostStatus(selectedEnvironment, new AbortController().signal),
    ).rejects.toThrow();
  });
  it("uses the desktop-managed serving environment and bearer without browser cookies", async () => {
    prepare("http://127.0.0.1:3773", "synthetic-desktop-token");
    const bearer = vi.fn().mockResolvedValue("synthetic-desktop-token");
    vi.stubGlobal("window", {
      location: { origin: "t3code://app", href: "t3code://app/" },
      desktopBridge: {
        getLocalEnvironmentBootstraps: () => [
          {
            id: "primary",
            label: "Fixture server",
            httpBaseUrl: "http://127.0.0.1:3773",
            wsBaseUrl: "ws://127.0.0.1:3773",
          },
        ],
        getLocalEnvironmentBearerToken: bearer,
      },
    });
    const fetcher = vi.fn().mockResolvedValue(Response.json(snapshot));
    vi.stubGlobal("fetch", fetcher);
    expect(await fetchHostStatus(selectedEnvironment, new AbortController().signal)).toEqual(
      snapshot,
    );
    const request = new Request(fetcher.mock.calls[0]?.[0], fetcher.mock.calls[0]?.[1]);
    expect(request.url).toBe("http://127.0.0.1:3773/api/host-status");
    expect(request.credentials).toBe("same-origin");
    expect(request.headers.get("authorization")).toBe("Bearer synthetic-desktop-token");
    expect(bearer).not.toHaveBeenCalled();
  });
  it("routes a selected remote server even when the desktop local environment is disabled", async () => {
    prepare("https://selected-vps.example", "synthetic-remote-token");
    vi.stubGlobal("window", {
      location: { origin: "t3code://app", href: "t3code://app/" },
      desktopBridge: { getLocalEnvironmentEnabled: () => false },
    });
    const fetcher = vi.fn().mockResolvedValue(Response.json(snapshot));
    vi.stubGlobal("fetch", fetcher);
    expect(await fetchHostStatus(selectedEnvironment, new AbortController().signal)).toEqual(
      snapshot,
    );
    expect(harness.selected).toContain(selectedEnvironment);
    const request = new Request(fetcher.mock.calls[0]?.[0], fetcher.mock.calls[0]?.[1]);
    expect(request.url).toBe("https://selected-vps.example/api/host-status");
    expect(request.headers.get("authorization")).toBe("Bearer synthetic-remote-token");
    expect(request.credentials).toBe("same-origin");
  });
  it("does not substitute the renderer machine when the local desktop environment is disabled", async () => {
    harness.prepared.clear();
    const fetcher = vi.fn();
    vi.stubGlobal("fetch", fetcher);
    vi.stubGlobal("window", {
      location: { origin: "t3code://app", href: "t3code://app/" },
      desktopBridge: { getLocalEnvironmentEnabled: () => false },
    });
    await expect(
      fetchHostStatus(selectedEnvironment, new AbortController().signal),
    ).rejects.toThrow();
    expect(fetcher).not.toHaveBeenCalled();
  });
  it("aborts the selected environment HTTP request when its observer is disposed", async () => {
    prepare("https://selected-vps.example", "synthetic-remote-token");
    let requestSignal: AbortSignal | null | undefined;
    let finish!: (response: Response) => void;
    const fetcher = vi.fn<typeof fetch>((_input, init) => {
      requestSignal = init?.signal;
      return new Promise<Response>((resolve) => {
        finish = resolve;
      });
    });
    vi.stubGlobal("fetch", fetcher);
    const abort = new AbortController();
    const result = fetchHostStatus(selectedEnvironment, abort.signal);
    const rejected = expect(result).rejects.toThrow();
    await vi.waitFor(() => expect(fetcher).toHaveBeenCalledOnce());
    abort.abort();
    await rejected;
    await vi.waitFor(() => expect(requestSignal?.aborted).toBe(true));
    finish(Response.json(snapshot));
  });
  it.each(
    [401, 403, 404, 500].flatMap((status) => [
      { status, token: null },
      { status, token: "synthetic-remote-token" },
    ]),
  )(
    "rejects HTTP $status with token $token as a failed gateway request",
    async ({ status, token }) => {
      prepare("https://remote.example", token);
      vi.stubGlobal("window", {
        location: { origin: "https://remote.example", href: "https://remote.example/" },
      });
      const fetcher = vi
        .fn()
        .mockResolvedValue(Response.json({ code: "synthetic-error" }, { status }));
      vi.stubGlobal("fetch", fetcher);
      await expect(
        fetchHostStatus(selectedEnvironment, new AbortController().signal),
      ).rejects.toThrow();
      const request = new Request(fetcher.mock.calls[0]?.[0], fetcher.mock.calls[0]?.[1]);
      expect(request.url).toBe("https://remote.example/api/host-status");
      expect(request.credentials).toBe(token ? "same-origin" : "include");
    },
  );
});

describe("retained host-status cache", () => {
  it("seeds fresh values synchronously, preserves the due time, and expires during a pending refresh", async () => {
    const cache: HostStatusCache = { snapshot, settledAt: vi.getMockedSystemTime()!.getTime() };
    const receive = vi.fn();
    const request = vi.fn(() => new Promise<HostStatusSnapshot>(() => {}));
    await vi.advanceTimersByTimeAsync(1_000);
    const stop = observeHostStatus(new Visibility(), receive, request, cache);
    expect(receive).toHaveBeenLastCalledWith(snapshot);
    expect(request).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(9_000);
    expect(request).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(20_000);
    expect(cache.snapshot?.hosts[0]).toEqual({ id: "vps", status: "unavailable", reason: "stale" });
    stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("ignores a canceled late result without changing the retained sample or cadence", async () => {
    const cache: HostStatusCache = {
      snapshot,
      settledAt: vi.getMockedSystemTime()!.getTime() - 10_000,
    };
    const original = { ...cache };
    let resolve!: (value: HostStatusSnapshot) => void;
    const stop = observeHostStatus(
      new Visibility(),
      vi.fn(),
      () =>
        new Promise((done) => {
          resolve = done;
        }),
      cache,
    );
    stop();
    resolve({ hosts: [] });
    await vi.advanceTimersByTimeAsync(0);
    expect(cache).toEqual(original);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("retains an in-flight result completed while hidden and waits until its next due time", async () => {
    const cache: HostStatusCache = { snapshot: null, settledAt: -Infinity };
    const visibility = new Visibility();
    let resolve!: (value: HostStatusSnapshot) => void;
    const request = vi.fn(
      () =>
        new Promise<HostStatusSnapshot>((done) => {
          resolve = done;
        }),
    );
    const stop = observeHostStatus(visibility, vi.fn(), request, cache);
    visibility.set("hidden");
    await vi.advanceTimersByTimeAsync(1_000);
    resolve(snapshot);
    await vi.advanceTimersByTimeAsync(0);
    expect(cache).toEqual({ snapshot, settledAt: vi.getMockedSystemTime()!.getTime() });
    visibility.set("visible");
    await vi.advanceTimersByTimeAsync(9_999);
    expect(request).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(request).toHaveBeenCalledTimes(2);
    stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("accepts retained samples five seconds ahead and expires from their original timestamp", async () => {
    const futureSnapshot: HostStatusSnapshot = {
      hosts: [
        {
          ...available,
          sampledAt: new Date(vi.getMockedSystemTime()!.getTime() + 5_000).toISOString(),
        },
      ],
    };
    const cache: HostStatusCache = {
      snapshot: futureSnapshot,
      settledAt: vi.getMockedSystemTime()!.getTime(),
    };
    const receive = vi.fn();
    const request = vi.fn();
    const visibility = new Visibility();
    visibility.set("hidden");
    const stop = observeHostStatus(visibility, receive, request, cache);
    expect(receive).toHaveBeenLastCalledWith(futureSnapshot);
    await vi.advanceTimersByTimeAsync(34_999);
    expect(receive).toHaveBeenLastCalledWith(futureSnapshot);
    await vi.advanceTimersByTimeAsync(1);
    expect(receive).toHaveBeenLastCalledWith({
      hosts: [{ id: "vps", status: "unavailable", reason: "stale" }],
    });
    expect(request).not.toHaveBeenCalled();
    stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["invalid", "2026-09-29T12:00:05.001Z", "2026-09-29T11:59:30.000Z"])(
    "rejects a retained invalid, excessively future, or expired source timestamp: %s",
    (sampledAt) => {
      const cache: HostStatusCache = {
        snapshot: { hosts: [{ ...available, sampledAt }] },
        settledAt: vi.getMockedSystemTime()!.getTime(),
      };
      const receive = vi.fn();
      const stop = observeHostStatus(new Visibility(), receive, vi.fn(), cache);
      expect(receive).toHaveBeenLastCalledWith({
        hosts: [{ id: "vps", status: "unavailable", reason: "stale" }],
      });
      stop();
    },
  );
});
