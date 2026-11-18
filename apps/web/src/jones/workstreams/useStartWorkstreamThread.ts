import { scopeProjectRef } from "@t3tools/client-runtime/environment";
import type {
  PersonalWorkstreamsState,
  PersonalWorkstreamView,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { AuthOrchestrationOperateScope, type EnvironmentId } from "@t3tools/contracts";
import { useRouter } from "@tanstack/react-router";
import { stackedThreadToast, toastManager } from "../../components/ui/toast";
import type { NewThreadHandler } from "../../hooks/useHandleNewThread";
import { useScratchProject } from "../../hooks/useScratchProject";
import type { SidebarProjectSnapshot } from "../../sidebarProjectGrouping";
import { usePrimaryEnvironmentId } from "../../state/environments";
import { readEnvironmentScope, useEnvironmentsWithScope } from "../../state/session";

export type WorkstreamThreadAction = { run: () => void } | { disabledReason: string };

export function useStartWorkstreamThread(
  projects: readonly SidebarProjectSnapshot[],
  state: PersonalWorkstreamsState,
  currentEnvironmentId: EnvironmentId | null,
  handleNewThread: NewThreadHandler,
): (workstream: PersonalWorkstreamView) => WorkstreamThreadAction {
  const router = useRouter();
  const primaryEnvironmentId = usePrimaryEnvironmentId();
  const { scratchWorkspaceRootFor, openScratchProject } = useScratchProject();
  const operable = useEnvironmentsWithScope(state.environments, AuthOrchestrationOperateScope);
  const ready = new Set(
    state.environments
      .filter(
        ({ status, confirmed, environmentId }) =>
          status === "ready" && confirmed && operable.has(environmentId),
      )
      .map(({ environmentId }) => environmentId),
  );
  const scratchTargets = [...ready].filter((id) => scratchWorkspaceRootFor(id) !== null);
  const scratchEnvironmentId =
    [primaryEnvironmentId, currentEnvironmentId].find(
      (id) => id !== null && scratchTargets.includes(id),
    ) ?? (scratchTargets.length === 1 ? scratchTargets[0] : undefined);

  return (workstream) => {
    const projectKey = workstream.nameIcon?.projectKey;
    const projectGroup = projects.find((project) => project.projectKey === projectKey);
    const project =
      projectGroup &&
      [projectGroup, ...projectGroup.memberProjects].find(
        (member) => member.archivedAt == null && ready.has(member.environmentId),
      );
    const environmentId = projectKey ? project?.environmentId : scratchEnvironmentId;
    if (environmentId === undefined) {
      return {
        disabledReason: projectKey
          ? "Open the linked project on a writable environment."
          : "A writable environment with No project support is required.",
      };
    }
    return {
      run: () => {
        if (!readEnvironmentScope(environmentId, AuthOrchestrationOperateScope)) return;
        const href = router.state.location.href;
        void (async () => {
          const target = project ?? (await openScratchProject(environmentId));
          if (target && router.state.location.href === href) {
            await handleNewThread(scopeProjectRef(target.environmentId, target.id), {
              workstreamId: workstream.id,
            });
          }
        })().catch((error: unknown) => {
          toastManager.add(
            stackedThreadToast({
              type: "error",
              title: "Could not start a Workstream thread",
              description: error instanceof Error ? error.message : "An error occurred.",
            }),
          );
        });
      },
    };
  };
}
