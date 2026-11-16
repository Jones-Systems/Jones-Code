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
const HOST_STATUS_MAX_FUTURE_SKEW_MS = 5_000;
// oxlint-disable-next-line t3code/no-em-dash-copy -- Owner-approved missing-reading symbol, not prose punctuation.
const UNAVAILABLE_READING = "—";

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
  return !Number.isFinite(age) ||
    age < -HOST_STATUS_MAX_FUTURE_SKEW_MS ||
    age >= HOST_STATUS_MAX_AGE_MS
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
    const diagnosis =
      host?.reason === "stale" && host.diagnostic === "collector_disconnected"
        ? [
            "Host status collector is disconnected",
            "Check the collector connection to the Netdata parent",
          ]
        : host?.reason === "stale" && host.diagnostic === "sample_expired"
          ? ["Host status sample has expired", "Wait for a recent collector sample"]
          : host?.reason === "upstream_unavailable" && host.diagnostic === "upstream_failure"
            ? [reason, "Check the host status source connection"]
            : undefined;
    const tooltipLines = diagnosis
      ? [...diagnosis, "Retries every 10 seconds while visible"]
      : [reason];
    return {
      cpu: UNAVAILABLE_READING,
      ram: UNAVAILABLE_READING,
      cpuHealth: "unavailable",
      ramHealth: "unavailable",
      health: "unavailable",
      detail: tooltipLines.join(". "),
      tooltipLines,
    } as const;
  }
  const cpuHealth = utilizationHealth(host.cpuUsagePercent);
  const cpu = `${Math.round(host.cpuUsagePercent)}%`;
  const availableMemoryBytes = host.availableMemoryBytes;
  const formatGiB = (bytes: number) => `${Number((bytes / 1024 ** 3).toFixed(1))} GiB`;
  const storageLabel =
    host.storageMountPoint &&
    host.storageMountPoint !== "/" &&
    host.storageMountPoint !== "/System/Volumes/Data"
      ? `Storage (${host.storageMountPoint})`
      : "Storage";
  const tooltipLines = [
    `CPU ${cpu} of total cores`,
    `RAM ${availableMemoryBytes === undefined ? "unavailable" : `${formatGiB(availableMemoryBytes)} available of ${formatGiB(host.totalMemoryBytes)}`}`,
    ...(host.id === "mini" || host.id === "home"
      ? [
          `Swap I/O ${host.swapIoBytesPerSecond === undefined ? "unavailable" : `${(host.swapIoBytesPerSecond / 1024 ** 2).toFixed(1)} MiB/s`}`,
        ]
      : []),
    `${storageLabel} ${host.availableStorageBytes === undefined || host.totalStorageBytes === undefined ? "unavailable" : `${formatGiB(host.availableStorageBytes)} free of ${formatGiB(host.totalStorageBytes)}`}`,
  ];
  if (host.id === "mini" || host.id === "home") {
    const swap = host.swapIoBytesPerSecond;
    const swapMiB = swap === undefined ? undefined : swap / 1024 ** 2;
    const ramHealth = swapMiB === undefined ? "unavailable" : swapMiB > 10 ? "warning" : "healthy";
    const ram = swapMiB === undefined ? UNAVAILABLE_READING : `${swapMiB.toFixed(1)} mb/s`;
    const health = cpuHealth === "healthy" && ramHealth === "warning" ? "warning" : cpuHealth;
    return {
      health,
      cpuHealth,
      ramHealth,
      cpu,
      ram,
      detail: tooltipLines.join(". "),
      tooltipLines,
    } as const;
  }
  const ramHealth =
    availableMemoryBytes === undefined
      ? "unavailable"
      : utilizationHealth(100 * (1 - availableMemoryBytes / host.totalMemoryBytes));
  const ram =
    availableMemoryBytes === undefined
      ? UNAVAILABLE_READING
      : String(Math.round(availableMemoryBytes / 1024 ** 3));
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
    detail: tooltipLines.join(". "),
    tooltipLines,
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
        publish(cache.snapshot);
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
