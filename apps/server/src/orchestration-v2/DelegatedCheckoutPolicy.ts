import {
  type CommandId,
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { sha256, canonicalJson } from "./CanonicalJson.ts";
import { nativeWorktreePath } from "../vcs/worktreePath.ts";

export class DelegatedCheckoutPlanError extends Schema.TaggedError<DelegatedCheckoutPlanError>()(
  "DelegatedCheckoutPlanError",
  { message: Schema.String },
) {}

export const DelegatedCheckoutPlanV1 = Schema.Struct({
  version: Schema.Literal(1),
  parentThreadId: ThreadId,
  parentCheckoutPath: Schema.NonEmptyString,
  parentCommit: Schema.String.check(
    Schema.makeFilter((value) => /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(value)),
  ),
  canonicalProjectRoot: Schema.NonEmptyString,
  projectWorkspaceRoot: Schema.NonEmptyString,
  childThreadId: ThreadId,
  branch: Schema.NonEmptyString,
  worktreePath: Schema.NonEmptyString,
  workspaceStrategy: OrchestrationV2ThreadLaunchWorkspaceStrategy,
}).check(
  Schema.makeFilter(
    (plan) =>
      plan.childThreadId !== plan.parentThreadId &&
      plan.worktreePath !== plan.parentCheckoutPath &&
      plan.worktreePath !== plan.canonicalProjectRoot &&
      plan.workspaceStrategy.type === "worktree" &&
      plan.workspaceStrategy.baseRef === plan.parentCommit &&
      plan.workspaceStrategy.branch === plan.branch &&
      plan.workspaceStrategy.startFromOrigin === false,
  ),
);
export type DelegatedCheckoutPlanV1 = typeof DelegatedCheckoutPlanV1.Type;

/** The caller pins the parent's local HEAD before accepting the delegated command. */
export function planDelegatedCheckout(input: {
  readonly commandId: CommandId;
  readonly parentThreadId: ThreadId;
  readonly parentCheckoutPath: string;
  readonly parentCommit: string;
  readonly childThreadId: ThreadId;
  readonly canonicalProjectRoot: string;
  readonly projectWorkspaceRoot?: string;
  readonly canonicalWorktreesDir: string;
}): DelegatedCheckoutPlanV1 {
  if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(input.parentCommit))
    throw new DelegatedCheckoutPlanError({
      message: "Delegation requires a resolved immutable local parent HEAD.",
    });
  if (input.childThreadId === input.parentThreadId)
    throw new DelegatedCheckoutPlanError({
      message: "A delegated child requires a distinct application thread.",
    });
  const branch = `t3code/delegated-${sha256(
    canonicalJson({
      commandId: input.commandId,
      childThreadId: input.childThreadId,
    }),
  )}`;
  const worktreePath = nativeWorktreePath({
    worktreesDir: input.canonicalWorktreesDir,
    cwd: input.canonicalProjectRoot,
    branch,
  });
  if (worktreePath === input.parentCheckoutPath || worktreePath === input.canonicalProjectRoot)
    throw new DelegatedCheckoutPlanError({
      message: "The delegated checkout must be physically distinct from the parent checkout.",
    });
  return {
    version: 1,
    canonicalProjectRoot: input.canonicalProjectRoot,
    projectWorkspaceRoot: input.projectWorkspaceRoot ?? input.canonicalProjectRoot,
    parentThreadId: input.parentThreadId,
    parentCheckoutPath: input.parentCheckoutPath,
    parentCommit: input.parentCommit,
    childThreadId: input.childThreadId,
    branch,
    worktreePath,
    workspaceStrategy: {
      type: "worktree",
      baseRef: input.parentCommit,
      branch,
      startFromOrigin: false,
    },
  };
}

export function planStandaloneBirthPlacement(input: {
  readonly kind: "fork" | "mcp_create";
  readonly birthCommandId: CommandId;
  readonly targetThreadId: ThreadId;
  readonly canonicalWorktreesDir: string;
  readonly projectWorkspaceRoot: string;
}): { readonly branch: string; readonly worktreePath: string } {
  const branch = `t3code/${input.kind === "fork" ? "fork" : "thread"}-${sha256(
    canonicalJson({ commandId: input.birthCommandId, threadId: input.targetThreadId }),
  )}`;
  return {
    branch,
    worktreePath: nativeWorktreePath({
      worktreesDir: input.canonicalWorktreesDir,
      cwd: input.projectWorkspaceRoot,
      branch,
    }),
  };
}

export function planStandaloneCheckout(input: {
  readonly kind: "fork" | "mcp_create";
  readonly birthCommandId: CommandId;
  readonly parentThreadId: ThreadId;
  readonly parentCheckoutPath: string;
  readonly parentCommit: string;
  readonly childThreadId: ThreadId;
  readonly canonicalProjectRoot: string;
  readonly projectWorkspaceRoot: string;
  readonly canonicalWorktreesDir: string;
  readonly branch: string;
  readonly worktreePath: string;
}): DelegatedCheckoutPlanV1 {
  const placement = planStandaloneBirthPlacement({ ...input, targetThreadId: input.childThreadId });
  if (placement.branch !== input.branch || placement.worktreePath !== input.worktreePath)
    throw new DelegatedCheckoutPlanError({ message: "The standalone birth placement changed." });
  const plan: DelegatedCheckoutPlanV1 = {
    version: 1,
    parentThreadId: input.parentThreadId,
    parentCheckoutPath: input.parentCheckoutPath,
    parentCommit: input.parentCommit,
    canonicalProjectRoot: input.canonicalProjectRoot,
    projectWorkspaceRoot: input.projectWorkspaceRoot,
    childThreadId: input.childThreadId,
    ...placement,
    workspaceStrategy: {
      type: "worktree",
      baseRef: input.parentCommit,
      branch: placement.branch,
      startFromOrigin: false,
    },
  };
  if (!Schema.is(DelegatedCheckoutPlanV1)(plan))
    throw new DelegatedCheckoutPlanError({
      message: "Standalone checkout requires a distinct thread and immutable committed base.",
    });
  return plan;
}
