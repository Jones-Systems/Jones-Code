import type { StartThreadTurnInput } from "@t3tools/client-runtime/operations";
import type { ProjectId, ServerConfig } from "@t3tools/contracts";
import type { DraftThreadEnvMode } from "../../composerDraftStore";

export function usesVerifiedDefault(
  config: ServerConfig | null | undefined,
  projectId: ProjectId | null | undefined,
): boolean {
  if (!config || !projectId) return false;
  const capabilities = config.environment.capabilities;
  return (
    "repoFreshness" in capabilities &&
    capabilities.repoFreshness === true &&
    (config.settings.projectSettingsOverrides[projectId]?.repoFreshnessCadenceSeconds ?? 0) > 0
  );
}

export function resolveFirstSendWorktreePreparation(input: {
  isFirstMessage: boolean;
  sendEnvMode: DraftThreadEnvMode;
  worktreePath: string | null;
  projectCwd: string;
  baseBranch: string | null;
  startFromOrigin: boolean;
  verifiedDefault?: boolean;
}): NonNullable<StartThreadTurnInput["bootstrap"]>["prepareWorktree"] {
  if (!input.isFirstMessage || input.sendEnvMode !== "worktree" || input.worktreePath !== null) {
    return undefined;
  }
  if (input.verifiedDefault && input.baseBranch === null) {
    return {
      projectCwd: input.projectCwd,
      baseFreshness: input.startFromOrigin ? "fetch-now" : "verified",
    };
  }
  return {
    projectCwd: input.projectCwd,
    ...(input.baseBranch !== null ? { baseBranch: input.baseBranch } : {}),
    ...(input.startFromOrigin ? { startFromOrigin: true } : {}),
  };
}
