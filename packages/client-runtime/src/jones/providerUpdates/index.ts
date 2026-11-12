import type { ProviderInstanceId, ServerProvider } from "@t3tools/contracts";

export type ProviderUpdateCandidate = ServerProvider & {
  readonly versionAdvisory: NonNullable<ServerProvider["versionAdvisory"]> & {
    readonly status: "behind_latest";
    readonly latestVersion: string;
  };
};

/** Instance IDs are scoped to one environment; select freshness before eligibility. */
function freshestProviderSnapshots<T extends ServerProvider>(providers: ReadonlyArray<T>): T[] {
  const latest = new Map<ProviderInstanceId, T>();
  for (const provider of providers) {
    const current = latest.get(provider.instanceId);
    if (!current || provider.checkedAt.localeCompare(current.checkedAt) >= 0) {
      latest.set(provider.instanceId, provider);
    }
  }
  return [...latest.values()];
}

export function isProviderUpdateCandidate(
  provider: ServerProvider,
): provider is ProviderUpdateCandidate {
  return (
    provider.enabled &&
    provider.compatibilityAdvisory?.latestVersionStatus !== "broken" &&
    provider.compatibilityAdvisory?.latestVersionStatus !== "unsupported" &&
    provider.versionAdvisory?.status === "behind_latest" &&
    provider.versionAdvisory.latestVersion !== null
  );
}

export function isProviderUpdateActive(provider: Pick<ServerProvider, "updateState">): boolean {
  return provider.updateState?.status === "queued" || provider.updateState?.status === "running";
}

export function collectProviderUpdateCandidates(
  providers: ReadonlyArray<ServerProvider>,
): ProviderUpdateCandidate[] {
  return freshestProviderSnapshots(providers).filter(isProviderUpdateCandidate);
}

export function hasOneClickUpdateProviderCandidate(
  candidate: ProviderUpdateCandidate,
  providers: ReadonlyArray<ServerProvider>,
): boolean {
  const current = freshestProviderSnapshots(providers).find(
    (provider) => provider.instanceId === candidate.instanceId,
  );
  return (
    current !== undefined &&
    current.driver === candidate.driver &&
    isProviderUpdateCandidate(current) &&
    current.versionAdvisory.canUpdate === true &&
    current.versionAdvisory.updateCommand !== null
  );
}

export function canOneClickUpdateProviderCandidate(
  candidate: ProviderUpdateCandidate,
  providers: ReadonlyArray<ServerProvider>,
): boolean {
  const current = freshestProviderSnapshots(providers).find(
    (provider) => provider.instanceId === candidate.instanceId,
  );
  return (
    current !== undefined &&
    !isProviderUpdateActive(current) &&
    hasOneClickUpdateProviderCandidate(candidate, [current])
  );
}

export function providerUpdateNotificationKey(
  providers: ReadonlyArray<ProviderUpdateCandidate>,
): string | null {
  const parts = freshestProviderSnapshots(providers)
    .map((provider) =>
      [
        provider.driver,
        provider.instanceId,
        provider.versionAdvisory.latestVersion,
        canOneClickUpdateProviderCandidate(provider, [provider]) ? "one-click" : "settings",
      ].join(":"),
    )
    .sort();
  return parts.length > 0 ? parts.join("|") : null;
}
