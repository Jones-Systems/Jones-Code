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

export function hostStatusMetrics(host: HostStatus | undefined) {
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
      load: "—",
      ram: "—",
      loadHealth: "unavailable",
      ramHealth: "unavailable",
      detail: reason,
    } as const;
  }
  const loadPerCore = host.load1 / host.logicalCpuCount;
  const availableFraction = host.availableMemoryBytes / host.totalMemoryBytes;
  return {
    load: host.load1.toFixed(1),
    ram: (host.availableMemoryBytes / 1024 ** 3).toFixed(1),
    loadHealth: loadPerCore >= 1 ? "critical" : loadPerCore >= 0.8 ? "warning" : "healthy",
    ramHealth:
      availableFraction < 0.05 ? "critical" : availableFraction < 0.1 ? "warning" : "healthy",
    detail: `1-minute load ${host.load1}; ${host.logicalCpuCount} logical CPUs. Available RAM ${(host.availableMemoryBytes / 1024 ** 3).toFixed(1)} GiB (${(availableFraction * 100).toFixed(1)}%).`,
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
  const refresh = async () => {
    if (disposed || pending || visibility.visibilityState !== "visible") return;
    pending = true;
    try {
      const snapshot = await request(abort.signal);
      if (!disposed && visibility.visibilityState === "visible") receive(snapshot);
    } catch {
      if (!disposed) receive(null);
    } finally {
      pending = false;
    }
  };
  const onVisibility = () => {
    receive(null);
    void refresh();
  };
  const timer = setInterval(() => void refresh(), 10_000);
  visibility.addEventListener("visibilitychange", onVisibility);
  void refresh();
  return () => {
    disposed = true;
    clearInterval(timer);
    visibility.removeEventListener("visibilitychange", onVisibility);
    abort.abort();
  };
}
