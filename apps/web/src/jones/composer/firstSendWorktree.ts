import type { StartThreadTurnInput } from "@t3tools/client-runtime/operations";
import type { ProjectId, ServerConfig } from "@t3tools/contracts";
import type { DraftThreadEnvMode } from "../../composerDraftStore";

export function usesVerifiedDefault(
  config: ServerConfig | null | undefined,
  projectId: ProjectId | null | undefined,
): boolean {
  if (!config || !projectId) return false;
  const capabilities = config.environment?.capabilities;
  return (
    capabilities?.repoFreshness === true &&
    (config.settings.projectSettingsOverrides[projectId]?.repoFreshnessCadenceSeconds ?? 0) > 0
  );
}

export function resolveFirstSendWorktreePreparation(input: {
  isFirstMessage: boolean;
  sendEnvMode: DraftThreadEnvMode;
  worktreePath: string | null;
  projectCwd: string;
  projectIsGitRepo: boolean;
  tracksGitRepositories: boolean;
  repositories?: ReadonlyArray<{ readonly path: string }> | undefined;
  baseBranch: string | null;
  startFromOrigin: boolean;
  verifiedDefault?: boolean;
}): NonNullable<StartThreadTurnInput["bootstrap"]>["prepareWorktree"] {
  if (!input.isFirstMessage || input.sendEnvMode !== "worktree" || input.worktreePath !== null) {
    return undefined;
  }
  const preparation = {
    projectCwd: input.projectCwd,
    ...(input.repositories === undefined ? {} : { repositories: input.repositories }),
  };
  if (
    input.verifiedDefault &&
    input.projectIsGitRepo &&
    !input.tracksGitRepositories &&
    (input.repositories?.length ?? 0) === 0 &&
    input.baseBranch === null
  ) {
    return {
      ...preparation,
      baseFreshness: input.startFromOrigin ? "fetch-now" : "verified",
    };
  }
  return {
    ...preparation,
    ...(input.baseBranch !== null ? { baseBranch: input.baseBranch } : {}),
    ...(input.startFromOrigin ? { startFromOrigin: true } : {}),
  };
}
