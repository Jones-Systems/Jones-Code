import type { SidebarProjectSortOrder } from "@t3tools/contracts/settings";
import { sortSidebarV2ProjectGroups } from "../../components/Sidebar.logic";
import { deriveLogicalProjectKey, getProjectOrderKey } from "../../logicalProject";
import type { SidebarProjectSnapshot } from "../../sidebarProjectGrouping";
import type { Project, SidebarThreadSummary } from "../../types";
import { legacyProjectCwdPreferenceKey } from "../../uiStateStore";

export function projectPreferenceIds(project: Project): string[] {
  return [
    ...new Set([
      getProjectOrderKey(project),
      legacyProjectCwdPreferenceKey(project.workspaceRoot),
      deriveLogicalProjectKey(project, { groupingMode: "repository" }),
      deriveLogicalProjectKey(project, { groupingMode: "repository_path" }),
    ]),
  ];
}

export function groupPreferenceIds(group: SidebarProjectSnapshot): string[] {
  return [...new Set([group.projectKey, ...group.memberProjects.flatMap(projectPreferenceIds)])];
}

export function orderProjectGroups(input: {
  groups: readonly SidebarProjectSnapshot[];
  threads: readonly SidebarThreadSummary[];
  sortOrder: SidebarProjectSortOrder;
  projectOrder: readonly string[];
  delegatedUnderParent: ReadonlySet<string>;
}): SidebarProjectSnapshot[] {
  const baseline = sortSidebarV2ProjectGroups(
    input.groups,
    input.threads,
    input.sortOrder === "manual" ? "updated_at" : input.sortOrder,
    input.delegatedUnderParent,
  );
  if (input.sortOrder !== "manual" || input.projectOrder.length === 0) return baseline;
  const ranks = new Map(input.projectOrder.map((key, index) => [key, index]));
  // Exact keys distinguish root/subdirectory groups that share a repository alias.
  const rank = (group: SidebarProjectSnapshot) =>
    ranks.get(group.projectKey) ??
    Math.min(...groupPreferenceIds(group).map((key) => ranks.get(key) ?? Infinity));
  const ordered = baseline.filter((group) => !group.isScratch).sort((a, b) => rank(a) - rank(b));
  let index = 0;
  return baseline.map((group) => (group.isScratch ? group : ordered[index++]!));
}

// Keep slots for absent environments: a partial snapshot must not erase their saved order.
export function mergeVisibleOrder(
  stored: readonly string[],
  visibleKeys: readonly string[],
  preferenceIds: (key: string) => readonly string[] = (key) => [key],
): string[] {
  const groupByAlias = new Map<string, string>();
  for (const key of visibleKeys) {
    for (const alias of preferenceIds(key)) {
      if (!groupByAlias.has(alias)) groupByAlias.set(alias, key);
    }
  }
  for (const key of visibleKeys) groupByAlias.set(key, key);
  const seen = new Set<string>();
  const result: string[] = [];
  let index = 0;
  for (const key of stored) {
    const group = groupByAlias.get(key);
    if (group === undefined) {
      result.push(key);
    } else if (!seen.has(group)) {
      seen.add(group);
      result.push(visibleKeys[index++]!);
    }
  }
  result.push(...visibleKeys.slice(index));
  return [...new Set(result)];
}
