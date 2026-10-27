import {
  type ClientSettings,
  type ProjectId,
  resolveEnvironmentMachineKind,
} from "@t3tools/contracts";

import { isDesktopLocalConnectionTarget } from "../connection/desktopLocal";
import { useProjects } from "../state/entities";
import { useEnvironments } from "../state/environments";

// Each helper reads its own source hook so the compiler caches the derived value on that
// source. Taking the source as an argument instead leaves the helper hook-free, and the
// compiler then treats it as a plain function and caches nothing.
export function useEnvironmentLookups() {
  const { environments } = useEnvironments();
  const environmentLabelById = new Map(
    environments.map((environment) => [environment.environmentId, environment.label] as const),
  );
  const projectEnvironmentLocationById = new Map(
    environments.map((environment) => {
      const isPrimary = environment.entry.target._tag === "PrimaryConnectionTarget";
      const isLocal = isPrimary || isDesktopLocalConnectionTarget(environment.entry.target);
      return [
        environment.environmentId,
        {
          kind: isLocal ? "local" : "remote",
          label: isPrimary ? "Local" : isLocal ? `${environment.label} (Local)` : environment.label,
          machine: resolveEnvironmentMachineKind(environment.serverConfig),
        },
      ] as const;
    }),
  );
  return { environmentLabelById, projectEnvironmentLocationById };
}

export function useProjectLookups() {
  const projects = useProjects();
  const projectCwdById = new Map<ProjectId, string>(
    projects.map((project) => [project.id, project.workspaceRoot]),
  );
  const projectByKey = new Map(
    projects.map((project) => [`${project.environmentId}:${project.id}`, project]),
  );
  const projectTitleById = new Map<ProjectId, string>(
    projects.map((project) => [project.id, project.title]),
  );
  return { projectCwdById, projectByKey, projectTitleById };
}

export const selectCommandPaletteClientSettings = (settings: ClientSettings) => ({
  sidebarProjectGroupingMode: settings.sidebarProjectGroupingMode,
  sidebarProjectGroupingOverrides: settings.sidebarProjectGroupingOverrides,
  sidebarProjectSortOrder: settings.sidebarProjectSortOrder,
  sidebarThreadSortOrder: settings.sidebarThreadSortOrder,
});
