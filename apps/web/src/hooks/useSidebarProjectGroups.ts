import { orderProjectGroups } from "../jones/presentationOrder/projectOrder";
import { selectProjectGroupingSettings } from "../logicalProject";
import { buildSidebarProjectSnapshots } from "../sidebarProjectGrouping";
import { useEnvironmentIdentities, usePrimaryEnvironmentId } from "../state/environments";
import { useDelegatedUnderParent, useProjects, useThreadShells } from "../state/entities";
import { useUiStateStore } from "../uiStateStore";
import { useClientSettings } from "./useSettings";

export function useSidebarProjectGroups() {
  const projects = useProjects();
  const threads = useThreadShells();
  const delegatedUnderParent = useDelegatedUnderParent();
  const projectOrder = useUiStateStore((store) => store.projectOrder);
  const sortOrder = useClientSettings((s) => s.sidebarProjectSortOrder);
  const groupingSettings = useClientSettings(selectProjectGroupingSettings);
  const environments = useEnvironmentIdentities();
  const primaryEnvironmentId = usePrimaryEnvironmentId();
  const labels = new Map(
    environments.map((environment) => [environment.environmentId, environment.label] as const),
  );
  const unsortedGroups = buildSidebarProjectSnapshots({
    projects,
    groupScratchProjects: true,
    settings: groupingSettings,
    primaryEnvironmentId,
    resolveEnvironmentLabel: (environmentId) => labels.get(environmentId) ?? null,
  });
  return orderProjectGroups({
    groups: unsortedGroups,
    threads,
    sortOrder,
    projectOrder,
    delegatedUnderParent,
  });
}
