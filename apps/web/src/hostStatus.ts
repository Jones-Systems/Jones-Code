import type { HostStatus, HostStatusSnapshot } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { PrimaryEnvironmentHttpClient, layer } from "./environments/primary/httpClient";
import { primaryEnvironmentHttpLayer } from "./environments/primary/httpLayer";

export const HOST_STATUS_NAMES = { vps: "VPS", test: "Test", mini: "Mini", home: "Home" } as const;
export const HOST_STATUS_IDS = ["vps", "test", "mini", "home"] as const;

const hostStatusRequest = PrimaryEnvironmentHttpClient.pipe(
  Effect.flatMap((client) => client.hostStatus.snapshot({ headers: {} })),
  Effect.timeout("8 seconds"),
);

export function fetchHostStatus(signal: AbortSignal): Promise<HostStatusSnapshot> {
  // Retain RequestInit alongside the client so browser cookie credentials reach the request.
  return Effect.runPromise(
    hostStatusRequest.pipe(
      Effect.provide(layer.pipe(Layer.provideMerge(primaryEnvironmentHttpLayer))),
    ),
    { signal },
  );
}

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

export function observeHostStatus(
  visibility: Pick<Document, "visibilityState" | "addEventListener" | "removeEventListener">,
  receive: (snapshot: HostStatusSnapshot | null) => void,
  request = fetchHostStatus,
) {
  let disposed = false;
  let pending = false;
  const abort = new AbortController();
  let expiryTimer: ReturnType<typeof setTimeout> | undefined;
  const clearExpiry = () => {
    clearTimeout(expiryTimer);
    expiryTimer = undefined;
  };
  const publish = (snapshot: HostStatusSnapshot) => {
    clearExpiry();
    const now = Date.now();
    const hosts = snapshot.hosts.map((host) => freshHostStatus(host, now));
    receive({ ...snapshot, hosts });
    const deadlines = hosts.flatMap((host) =>
      host.status === "available" ? [Date.parse(host.sampledAt) + HOST_STATUS_MAX_AGE_MS] : [],
    );
    if (deadlines.length > 0) {
      expiryTimer = setTimeout(
        () => {
          if (!disposed && visibility.visibilityState === "visible")
            publish({ ...snapshot, hosts });
        },
        Math.min(...deadlines) - now,
      );
    }
  };
  const refresh = async () => {
    if (disposed || pending || visibility.visibilityState !== "visible") return;
    pending = true;
    try {
      const snapshot = await request(abort.signal);
      if (!disposed && visibility.visibilityState === "visible") publish(snapshot);
    } catch {
      if (!disposed) {
        clearExpiry();
        receive(null);
      }
    } finally {
      pending = false;
    }
  };
  const onVisibility = () => {
    clearExpiry();
    receive(null);
    void refresh();
  };
  const timer = setInterval(() => void refresh(), 10_000);
  visibility.addEventListener("visibilitychange", onVisibility);
  void refresh();
  return () => {
    disposed = true;
    clearInterval(timer);
    clearExpiry();
    visibility.removeEventListener("visibilitychange", onVisibility);
    abort.abort();
  };
}
