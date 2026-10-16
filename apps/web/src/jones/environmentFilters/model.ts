import type { EnvironmentMachineKind } from "@t3tools/contracts";

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
  machines?: ReadonlyMap<string, EnvironmentMachineKind>,
): ReadonlySet<string> {
  const excluded = new Set<string>();
  for (const identity of identities) {
    const normalized = identity.label?.trim().toLowerCase();
    let label = SIDEBAR_ENVIRONMENT_FILTERS.find(
      (candidate) => candidate.toLowerCase() === normalized,
    );
    const machine = machines?.get(identity.environmentId);
    if (label === undefined) {
      if (normalized === "laptop") label = "Home";
      else if (normalized === "mac mini" || normalized === "mac-mini") label = "Mini";
      else if (machine === "laptop") label = "Home";
      else if (machine === "mac-mini") label = "Mini";
    }
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
