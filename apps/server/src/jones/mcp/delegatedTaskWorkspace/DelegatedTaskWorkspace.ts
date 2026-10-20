// @effect-diagnostics nodeBuiltinImport:off - Canonicalizes worktree paths like t3_thread_launch's existing-worktree check without adding a FileSystem requirement to OrchestratorMcpService's layer.
import * as NodeFSP from "node:fs/promises";
import {
  OrchestratorMcpFailure,
  type OrchestratorMcpDelegateTaskInput,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type * as ThreadLaunchService from "../../../orchestration-v2/ThreadLaunchService.ts";
import type * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";

type ExplicitWorkspace = NonNullable<OrchestratorMcpDelegateTaskInput["workspaceStrategy"]>;

export interface DelegatedTaskWorkspaceInput {
  readonly request: Pick<OrchestratorMcpDelegateTaskInput, "isolation" | "workspaceStrategy">;
  /** The command receipt already exists, so dispatch replays the accepted delegation. */
  readonly isRetryOfAcceptedDelegation: boolean;
  /** The child's project (see resolveDelegatedTaskProject); loaded only when a strategy needs validating. */
  readonly project: Effect.Effect<
    { readonly id: ProjectId; readonly workspaceRoot: string },
    OrchestratorMcpFailure
  >;
  readonly git: Pick<GitVcsDriver.GitVcsDriver["Service"], "listWorktreePaths">;
  readonly launches: Pick<ThreadLaunchService.ThreadLaunchService["Service"], "checkWorktreeBase">;
}

const invalid = (message: string) =>
  new OrchestratorMcpFailure({ code: "invalid_request", message });

const realPath = (path: string) =>
  Effect.tryPromise(() => NodeFSP.realpath(path)).pipe(Effect.orElseSucceed(() => path));

export interface DelegatedTaskProjectInput {
  readonly request: Pick<OrchestratorMcpDelegateTaskInput, "projectId" | "workspaceStrategy">;
  readonly parentProjectId: ProjectId;
  /** The command receipt already exists, so dispatch replays the accepted delegation. */
  readonly isRetryOfAcceptedDelegation: boolean;
  /** Fails with invalid_request when the project does not exist. */
  readonly loadProject: (
    projectId: ProjectId,
  ) => Effect.Effect<{ readonly deletedAt: string | null }, OrchestratorMcpFailure>;
}

export interface DelegatedTaskProject {
  /** Project whose settings, worktrees and setup apply to the child. */
  readonly projectId: ProjectId;
  /** Set only for another project, so same-project commands are unchanged. */
  readonly commandProjectId: ProjectId | undefined;
}

/**
 * Selects the project a delegated child runs in. Another project is explicit
 * cross-project delegation: the child is still this thread's subagent, but it
 * cannot share or copy this thread's checkout, so it needs a workspaceStrategy
 * naming a worktree of that project.
 */
export const resolveDelegatedTaskProject = Effect.fn("jones.delegatedTaskWorkspace.resolveProject")(
  function* (
    input: DelegatedTaskProjectInput,
  ): Effect.fn.Return<DelegatedTaskProject, OrchestratorMcpFailure> {
    const requested = input.request.projectId;
    if (requested === undefined || requested === input.parentProjectId)
      return { projectId: input.parentProjectId, commandProjectId: undefined };
    if (input.request.workspaceStrategy === undefined)
      return yield* invalid(
        `Project ${requested} is not this thread's project, so pass workspaceStrategy with a new or existing worktree of that project; a child cannot share or copy this thread's checkout there. No task was created.`,
      );
    // Replay returns the accepted delegation, which may name a project deleted since.
    if (input.isRetryOfAcceptedDelegation)
      return { projectId: requested, commandProjectId: requested };
    const project = yield* input.loadProject(requested);
    if (project.deletedAt !== null)
      return yield* invalid(`Project ${requested} was deleted. No task was created.`);
    return { projectId: requested, commandProjectId: requested };
  },
);

/**
 * Resolves the explicit `workspaceStrategy` of a `delegate_task` call. The
 * child is still created through `delegated_task.request`, so it keeps the
 * subagent record, lineage, completion wake, task_status and task_cancel; the
 * strategy only defers its start until the checkout is prepared. Returns
 * undefined when no explicit strategy applies, leaving legacy `isolation` to
 * its existing resolution.
 */
export const resolveExplicitDelegatedTaskWorkspace = Effect.fn(
  "jones.delegatedTaskWorkspace.resolveExplicit",
)(function* (
  input: DelegatedTaskWorkspaceInput,
): Effect.fn.Return<ExplicitWorkspace | undefined, OrchestratorMcpFailure> {
  const strategy = input.request.workspaceStrategy;
  if (strategy === undefined) return undefined;
  if (input.request.isolation !== undefined)
    return yield* invalid(
      'Pass either isolation or workspaceStrategy, not both. isolation:"worktree" starts from the parent commit; workspaceStrategy names a base, branch, or existing worktree.',
    );
  // Replay returns the accepted delegation. Re-validating could fail it after
  // its worktree was created or its base moved, so the request is not re-read.
  if (input.isRetryOfAcceptedDelegation) return undefined;
  const project = yield* input.project;
  if (strategy.type === "existing_worktree") {
    // Same admission as t3_thread_launch: without it a child could be pointed
    // at any directory on the machine.
    const worktrees = yield* input.git.listWorktreePaths(project.workspaceRoot).pipe(
      Effect.flatMap((paths) => Effect.forEach(paths, realPath)),
      Effect.orElseSucceed((): ReadonlyArray<string> => []),
    );
    if (!worktrees.includes(yield* realPath(strategy.worktreePath)))
      return yield* invalid(
        `worktreePath must be one of the git worktrees of project ${project.id}. t3_worktree_list shows them. No task was created.`,
      );
    return strategy;
  }
  // The child is accepted before its worktree is provisioned, so a base that
  // cannot resolve is refused while the caller can still correct it.
  yield* input.launches
    .checkWorktreeBase({ projectId: project.id, workspaceStrategy: strategy })
    .pipe(
      Effect.mapError((error) =>
        invalid(
          `${error.message} No task was created. Pass a branch, tag, or commit that exists there${
            strategy.startFromOrigin === true
              ? "; the project has no origin remote to fetch it from."
              : ", or set startFromOrigin:true if it exists only on origin."
          }`,
        ),
      ),
    );
  return strategy;
});
