import { vcsActionLabelText } from "@t3tools/client-runtime/state/vcs";
import { useMessages } from "gt-react-native";
import { useMemo } from "react";

import { dedupeRemoteBranchesWithLocalMatches } from "@t3tools/shared/git";

import { useBranches } from "./queries";
import { useVcsActionState } from "./use-vcs-action-state";
import type { GitRepositoryTarget } from "./thread-git-target";
import { useThreadSelection } from "./use-thread-selection";
import { useSelectedThreadWorktree } from "./use-selected-thread-worktree";

/** Git state for the selected thread's own folder, or for `repository` when given. */
export function useSelectedThreadGitState(repository?: GitRepositoryTarget) {
  const messages = useMessages();
  const { selectedThread } = useThreadSelection();
  const threadWorktree = useSelectedThreadWorktree();
  const selectedThreadCwd = repository?.cwd ?? threadWorktree.selectedThreadCwd;

  const selectedThreadGitTarget = {
    environmentId: selectedThread?.environmentId ?? null,
    cwd: selectedThreadCwd,
  };
  const gitActionState = useVcsActionState(selectedThreadGitTarget);

  const selectedThreadBranchTarget = {
    environmentId: selectedThread?.environmentId ?? null,
    cwd: selectedThreadCwd,
    query: null,
  };
  const selectedThreadBranchState = useBranches(selectedThreadBranchTarget);
  const selectedThreadBranches = useMemo(
    () =>
      dedupeRemoteBranchesWithLocalMatches(selectedThreadBranchState.data?.refs ?? []).filter(
        (branch) => !branch.isRemote,
      ),
    [selectedThreadBranchState.data?.refs],
  );

  return {
    gitOperationLabel: gitActionState.currentLabel,
    selectedThreadBranches,
    selectedThreadBranchesLoading: selectedThreadBranchState.isPending,
  };
}
