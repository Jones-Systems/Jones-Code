import type { EnvironmentId, HostStatusSnapshot } from "@t3tools/contracts";

export async function discoverGlobalHostStatusSource(
  connections: {
    readonly environmentIds: ReadonlyArray<EnvironmentId>;
    readonly connectedEnvironmentIds: ReadonlyArray<EnvironmentId>;
    readonly primaryEnvironmentId: EnvironmentId | null;
  },
  signal: AbortSignal,
  request: (environmentId: EnvironmentId, signal: AbortSignal) => Promise<HostStatusSnapshot>,
): Promise<EnvironmentId | null> {
  const connected = new Set(connections.connectedEnvironmentIds);
  const candidates = connections.environmentIds
    .filter((id) => connected.has(id))
    .sort((left, right) => {
      if (left === connections.primaryEnvironmentId) return -1;
      if (right === connections.primaryEnvironmentId) return 1;
      return left < right ? -1 : left > right ? 1 : 0;
    });
  for (const environmentId of candidates) {
    signal.throwIfAborted();
    try {
      const snapshot = await request(environmentId, signal);
      signal.throwIfAborted();
      if (
        snapshot.hosts.some(
          (host) =>
            host.id === "vps" &&
            (host.status === "available" || host.reason !== "not_configured"),
        )
      ) {
        return environmentId;
      }
    } catch {
      signal.throwIfAborted();
    }
  }
  return null;
}
