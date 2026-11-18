import type { StartThreadTurnInput } from "@t3tools/client-runtime/operations";
import type { DraftThreadEnvMode } from "../../composerDraftStore";
export function resolveFirstSendWorktreePreparation(input: {
  isFirstMessage: boolean;
  sendEnvMode: DraftThreadEnvMode;
  worktreePath: string | null;
  projectCwd: string;
  baseBranch: string | null;
  startFromOrigin: boolean;
}): NonNullable<StartThreadTurnInput["bootstrap"]>["prepareWorktree"] {
  if (!input.isFirstMessage || input.sendEnvMode !== "worktree" || input.worktreePath !== null) {
    return undefined;
  }
  return {
    projectCwd: input.projectCwd,
    ...(input.baseBranch !== null ? { baseBranch: input.baseBranch } : {}),
    ...(input.startFromOrigin ? { startFromOrigin: true } : {}),
  };
}
