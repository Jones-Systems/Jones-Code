import type { HostStatus, HostStatusSnapshot } from "@t3tools/contracts";
export { fetchHostStatus } from "./jones/hostStatus/state";

export const HOST_STATUS_NAMES = {
  vps: "VPS",
  test: "Test",
  mini: "Mini",
  home: "Laptop",
} as const;
export const HOST_STATUS_IDS = ["vps", "test", "mini", "home"] as const;

const HOST_STATUS_MAX_AGE_MS = 30_000;

function utilizationHealth(percent: number) {
  return percent > 90
    ? "critical"
    : percent > 75
      ? "elevated"
      : percent >= 50
        ? "warning"
        : "healthy";
}

function freshHostStatus(host: HostStatus, now: number): HostStatus {
  if (host.status === "unavailable") return host;
  const age = now - Date.parse(host.sampledAt);
  return !Number.isFinite(age) || age < 0 || age >= HOST_STATUS_MAX_AGE_MS
    ? { id: host.id, status: "unavailable", reason: "stale" }
    : host;
}

export function hostStatusMetrics(sample: HostStatus | undefined) {
  const host = sample && freshHostStatus(sample, Date.now());
  if (!host || host.status === "unavailable") {
    const reason = !host
      ? "Waiting for host status"
      : {
          not_configured: "Not configured",
          upstream_unavailable: "Host status source unavailable",
          invalid_response: "Invalid host status response",
          stale: "Host status sample is stale",
        }[host.reason];
    return {
      cpu: "—",
      ram: "—",
      cpuHealth: "unavailable",
      ramHealth: "unavailable",
      health: "unavailable",
      detail: reason,
    } as const;
  }
  const cpuHealth = utilizationHealth(host.cpuUsagePercent);
  const cpu = `${Math.round(host.cpuUsagePercent)}%`;
  const availableMemoryBytes = host.availableMemoryBytes;
  if (host.id === "mini" || host.id === "home") {
    const swap = host.swapIoBytesPerSecond;
    const swapMiB = swap === undefined ? undefined : swap / 1024 ** 2;
    const ramHealth = swapMiB === undefined ? "unavailable" : swapMiB > 10 ? "warning" : "healthy";
    const ram = swapMiB === undefined ? "— mb/s" : `${swapMiB.toFixed(1)} mb/s`;
    const health = cpuHealth === "healthy" && ramHealth === "warning" ? "warning" : cpuHealth;
    const availableRam =
      availableMemoryBytes === undefined
        ? "unavailable"
        : `${(availableMemoryBytes / 1024 ** 3).toFixed(1)} GiB (collector estimate, including reclaimable memory)`;
    return {
      health,
      cpuHealth,
      ramHealth,
      cpu,
      ram,
      detail: `CPU ${cpu} of total cores. Swap I/O ${swapMiB === undefined ? "unavailable" : `${swapMiB.toFixed(1)} MiB/s (combined reads and writes, 10-second average)`}. Available RAM ${availableRam}. Swap activity above 10 MiB/s is amber; this is not native macOS memory pressure or an agent-admission decision.`,
    } as const;
  }
  const ramHealth =
    availableMemoryBytes === undefined
      ? "unavailable"
      : utilizationHealth(100 * (1 - availableMemoryBytes / host.totalMemoryBytes));
  const ram =
    availableMemoryBytes === undefined ? "—" : String(Math.round(availableMemoryBytes / 1024 ** 3));
  const health =
    availableMemoryBytes === undefined
      ? cpuHealth
      : utilizationHealth(
          Math.max(host.cpuUsagePercent, 100 * (1 - availableMemoryBytes / host.totalMemoryBytes)),
        );
  return {
    health,
    cpuHealth,
    ramHealth,
    cpu,
    ram,
    detail: `CPU ${cpu} of total cores. Available RAM ${ram === "—" ? "unavailable" : `${ram} GiB (collector estimate, including reclaimable memory)`}. Colors reflect CPU and RAM utilization.`,
  } as const;
}

export interface HostStatusCache {
  snapshot: HostStatusSnapshot | null;
  settledAt: number;
}

export function observeHostStatus(
  visibility: Pick<Document, "visibilityState" | "addEventListener" | "removeEventListener">,
  receive: (snapshot: HostStatusSnapshot | null) => void,
  request: (signal: AbortSignal) => Promise<HostStatusSnapshot>,
  cache: HostStatusCache = { snapshot: null, settledAt: -Infinity },
) {
  let disposed = false;
  let pending = false;
  const abort = new AbortController();
  let pollTimer: ReturnType<typeof setTimeout> | undefined;
  let expiryTimer: ReturnType<typeof setTimeout> | undefined;
  const clearExpiry = () => {
    clearTimeout(expiryTimer);
    expiryTimer = undefined;
  };
  const publish = (snapshot: HostStatusSnapshot | null) => {
    clearExpiry();
    if (snapshot === null) {
      cache.snapshot = null;
      receive(null);
      return;
    }
    const now = Date.now();
    const hosts = snapshot.hosts.map((host) => freshHostStatus(host, now));
    cache.snapshot = { ...snapshot, hosts };
    receive(cache.snapshot);
    const deadlines = hosts.flatMap((host) =>
      host.status === "available" ? [Date.parse(host.sampledAt) + HOST_STATUS_MAX_AGE_MS] : [],
    );
    if (deadlines.length > 0) {
      expiryTimer = setTimeout(
        () => {
          if (!disposed) publish({ ...snapshot, hosts });
        },
        Math.min(...deadlines) - now,
      );
    }
  };
  const schedule = () => {
    clearTimeout(pollTimer);
    if (disposed || pending || visibility.visibilityState !== "visible") return;
    const delay = cache.settledAt + 10_000 - Date.now();
    if (delay <= 0) void refresh();
    else pollTimer = setTimeout(() => void refresh(), delay);
  };
  const refresh = async () => {
    if (disposed || pending || visibility.visibilityState !== "visible") return;
    pending = true;
    try {
      const snapshot = await request(abort.signal);
      if (!disposed) {
        cache.settledAt = Date.now();
        publish(snapshot);
      }
    } catch {
      if (!disposed) {
        cache.settledAt = Date.now();
        publish(null);
      }
    } finally {
      pending = false;
      schedule();
    }
  };
  const onVisibility = () => {
    publish(cache.snapshot);
    schedule();
  };
  visibility.addEventListener("visibilitychange", onVisibility);
  publish(cache.snapshot);
  schedule();
  return () => {
    disposed = true;
    clearTimeout(pollTimer);
    clearExpiry();
    visibility.removeEventListener("visibilitychange", onVisibility);
    abort.abort();
  };
}
