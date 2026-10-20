import * as Schema from "effect/Schema";

import { ProjectId, TrimmedNonEmptyString } from "../baseSchemas.ts";

/**
 * Checkout a delegated child is bound to before it starts. The child remains an
 * app-owned subagent of the caller (its own provider session, recorded in the
 * caller's lineage); this only selects where it runs. These are the
 * `t3_thread_launch` worktree members. `root` is omitted because a delegated
 * child without a strategy shares the caller's checkout, and the project's main
 * checkout is already one of the project's git worktrees.
 */
export const DelegatedTaskWorkspaceStrategy = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("worktree"),
    baseRef: Schema.optional(TrimmedNonEmptyString),
    branch: Schema.optional(TrimmedNonEmptyString),
    startFromOrigin: Schema.optional(Schema.Boolean),
  }),
  Schema.Struct({
    type: Schema.Literal("existing_worktree"),
    worktreePath: TrimmedNonEmptyString,
    branch: Schema.optional(TrimmedNonEmptyString),
  }),
]).annotate({
  description:
    'Bind the child to its own checkout before it starts; not with isolation. {type:"worktree",baseRef:"parent-branch",branch:"new-branch",startFromOrigin:false} creates a new worktree from baseRef (omitted baseRef uses the project\'s default branch, not the parent commit; startFromOrigin:true fetches it from origin first). {type:"existing_worktree",worktreePath:"/absolute/path"} binds one of the child project\'s git worktrees (see t3_worktree_list). Uncommitted edits are not copied.',
});
export type DelegatedTaskWorkspaceStrategy = typeof DelegatedTaskWorkspaceStrategy.Type;

/** Spread into the `delegate_task` input; optional so existing callers are unaffected. */
export const DelegatedTaskWorkspaceFields = {
  workspaceStrategy: Schema.optional(DelegatedTaskWorkspaceStrategy),
  projectId: Schema.optional(
    ProjectId.annotate({
      description:
        "Run the child in another registered project (see t3_project_list); omitted means this thread's project. Another project requires workspaceStrategy for one of that project's worktrees. The child is still this thread's subagent with this thread's mode limits.",
    }),
  ),
};
