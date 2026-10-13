export const SIDEBAR_ENVIRONMENT_FILTERS = ["VPS", "Test", "Mini", "Home"] as const;
export type SidebarEnvironmentFilter = (typeof SIDEBAR_ENVIRONMENT_FILTERS)[number];
export type SidebarEnvironmentFilters = Readonly<Record<SidebarEnvironmentFilter, boolean>>;

export const DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS: SidebarEnvironmentFilters = {
  VPS: true,
  Test: true,
  Mini: true,
  Home: true,
};

export function toggleSidebarEnvironmentFilter(
  filters: SidebarEnvironmentFilters,
  label: SidebarEnvironmentFilter,
): SidebarEnvironmentFilters {
  return { ...filters, [label]: !filters[label] };
}

export function excludedSidebarEnvironmentIds(
  identities: ReadonlyArray<{ readonly environmentId: string; readonly label?: string | null }>,
  filters: SidebarEnvironmentFilters,
): ReadonlySet<string> {
  const excluded = new Set<string>();
  for (const identity of identities) {
    const label = SIDEBAR_ENVIRONMENT_FILTERS.find(
      (candidate) => candidate.toLowerCase() === identity.label?.trim().toLowerCase(),
    );
    if (label !== undefined && !filters[label]) excluded.add(identity.environmentId);
  }
  return excluded;
}

export function sidebarEnvironmentExclusionKey(excludedIds: ReadonlySet<string>): string {
  return JSON.stringify([...excludedIds].sort());
}

export function filterSidebarEnvironmentThreads<T extends { readonly environmentId: string }>(
  threads: readonly T[],
  excludedIds: ReadonlySet<string>,
): readonly T[] {
  return excludedIds.size === 0
    ? threads
    : threads.filter((thread) => !excludedIds.has(thread.environmentId));
}
