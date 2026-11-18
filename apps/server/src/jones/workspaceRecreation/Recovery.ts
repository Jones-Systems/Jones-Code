import type {
  OrchestrationMessageContext,
  OrchestrationV2UserInputQuestion,
} from "@t3tools/contracts";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import { git, sha256 } from "../workspaceRetirement/Filesystem.ts";
import { RecreationHold, releaseMissingBranchRegistration } from "./Native.ts";

export function needsOwnerChoice(cause: unknown): boolean {
  return (
    cause instanceof RecreationHold &&
    (cause.reason === "source_unavailable" ||
      cause.reason === "branch_changed" ||
      cause.reason === "branch_in_use")
  );
}

export const questionId = "checkout-recovery";
type Choice = NonNullable<OrchestrationMessageContext["checkoutRecovery"]>["choice"];

const options = {
  conversation: {
    value: "conversation",
    label: "Continue without a checkout",
    description: "Read project source and attach a worktree later if needed.",
  },
  branch: {
    value: "branch",
    label: "Restore saved branch",
    description: "Fetch the latest version of the branch connected to this thread.",
  },
  default: {
    value: "default",
    label: "New worktree from latest main",
    description: "Create a task worktree from the current remote default branch.",
  },
} as const;

export function question(input: {
  readonly branch: string | null;
  readonly failure?: string;
  readonly failedChoice?: Choice;
  readonly conversationAvailable: boolean;
}): OrchestrationV2UserInputQuestion {
  return {
    id: questionId,
    header: "Checkout unavailable",
    question: input.failure ?? "This thread's worktree is missing. How would you like to continue?",
    options: [
      ...(input.conversationAvailable && input.failedChoice !== "conversation"
        ? [options.conversation]
        : []),
      ...(input.branch !== null && input.failedChoice !== "branch"
        ? [{ ...options.branch, description: `Fetch and resume ${input.branch}.` }]
        : []),
      ...(input.failedChoice === "default" && input.conversationAvailable ? [] : [options.default]),
    ],
    allowCustomAnswer: false,
    required: true,
  };
}

class RecoveryFailure extends Data.TaggedError("CheckoutRecoveryFailure")<{
  readonly message: string;
  readonly cause?: unknown;
}> {
  constructor(message: string, options?: ErrorOptions) {
    super({ message, ...(options?.cause === undefined ? {} : { cause: options.cause }) });
  }
}

export const failureDetail = (error: unknown): string =>
  error instanceof RecoveryFailure
    ? error.message
    : "Jones Code could not prepare the selected workspace. The agent did not start.";

const runGit = (cwd: string, args: ReadonlyArray<string>, detail: string) =>
  Effect.tryPromise({
    try: () => git(cwd, args).then((result) => result.toString().trim()),
    catch: (cause) => new RecoveryFailure(detail, { cause }),
  });

const branchRef = (branch: string) => `refs/heads/${branch}`;

const branchHead = (cwd: string, ref: string) =>
  runGit(
    cwd,
    ["rev-parse", "--verify", "--end-of-options", `${ref}^{commit}`],
    "Branch commit unavailable.",
  );

export const make = Effect.gen(function* () {
  const workflow = yield* GitWorkflow.GitWorkflowService;
  const path = yield* Path.Path;
  const remoteFor = Effect.fn("CheckoutRecovery.remoteFor")(function* (
    cwd: string,
    branch: string,
    local: boolean,
  ) {
    if (local) {
      const remote = yield* workflow.resolveWorktreeBaseRemote({ cwd, refName: branch });
      if (remote !== null) return remote;
    }
    const configured = yield* runGit(cwd, ["remote"], "Git remotes could not be read.");
    if (configured === "") return null;
    return { remoteName: yield* workflow.resolvePrimaryRemoteName(cwd), branchName: branch };
  });
  const availableBranch = Effect.fn("CheckoutRecovery.availableBranch")(function* (
    cwd: string,
    branch: string | null,
  ) {
    if (branch === null || branch.startsWith("-")) return null;
    const valid = yield* Effect.result(
      runGit(cwd, ["check-ref-format", branchRef(branch)], "Invalid saved branch."),
    );
    if (valid._tag === "Failure") return null;
    if (yield* workflow.hasCommit({ cwd, refName: branchRef(branch) })) return branch;
    const remote = yield* remoteFor(cwd, branch, false);
    if (remote === null) return null;
    return (yield* workflow.fetchWorktreeBaseCommit({ cwd, refName: branch, remote })) !== null
      ? branch
      : null;
  });
  const attach = Effect.fn("CheckoutRecovery.attach")(function* <E, R>(input: {
    readonly cwd: string;
    readonly worktreePath: string;
    readonly runId: string;
    readonly branch: string | null;
    readonly previousHead?: string;
    readonly choice: "branch" | "default";
    readonly hold: (target: string) => Effect.Effect<void, E, R>;
  }) {
    const { cwd } = input;
    const suffix = sha256(input.runId).slice(0, 16);
    const target = path.join(path.dirname(input.worktreePath), `recovered-${suffix}`);
    const branch = input.choice === "branch" ? input.branch : `t3/recovered-${suffix}`;
    if (branch === null)
      return yield* new RecoveryFailure("There is no saved branch for this thread.");
    const ref = branchRef(branch);
    yield* runGit(cwd, ["check-ref-format", ref], "The saved branch name is invalid.");
    const local = input.choice === "branch" && (yield* workflow.hasCommit({ cwd, refName: ref }));
    const localHead = local ? yield* branchHead(cwd, ref) : null;
    const remote = yield* (
      input.choice === "branch"
        ? remoteFor(cwd, branch, local)
        : Effect.gen(function* () {
            const remoteName = yield* workflow.resolvePrimaryRemoteName(cwd);
            const branchName = yield* workflow.resolveDefaultBranchName(cwd, remoteName);
            return { remoteName, branchName: branchName ?? "main" };
          })
    ).pipe(
      Effect.mapError(
        (cause) =>
          new RecoveryFailure("The connected Git remote or branch could not be resolved.", {
            cause,
          }),
      ),
    );
    const base =
      remote === null
        ? null
        : yield* workflow
            .fetchWorktreeBaseCommit({ cwd, refName: remote.branchName, remote })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new RecoveryFailure("Fetching the branch from its Git remote failed.", { cause }),
              ),
            );
    const remoteHead = base?.commitSha;
    const head = remoteHead ?? localHead;
    if (head === null)
      return yield* new RecoveryFailure("The branch is no longer available on its Git remote.");
    const isAncestor = (older: string, newer: string) =>
      runGit(
        cwd,
        ["merge-base", "--is-ancestor", older, newer],
        "The branch histories differ.",
      ).pipe(
        Effect.as(true),
        Effect.orElseSucceed(() => false),
      );
    let advance = false;
    if (localHead !== null && remoteHead !== undefined && localHead !== remoteHead) {
      advance = yield* isAncestor(localHead, remoteHead);
      if (!advance && !(yield* isAncestor(remoteHead, localHead))) {
        return yield* new RecoveryFailure(
          "The saved branch and its remote have diverged. The local commits were kept.",
        );
      }
    }
    const requestedHead = advance || localHead === null ? head : localHead;
    if (
      input.choice === "branch" &&
      input.previousHead !== undefined &&
      input.previousHead !== requestedHead &&
      !(yield* isAncestor(input.previousHead, requestedHead))
    ) {
      return yield* new RecoveryFailure(
        "The current branch no longer contains the saved checkout's commit.",
      );
    }
    yield* input.hold(target);
    if (local) {
      yield* Effect.tryPromise({
        try: () =>
          releaseMissingBranchRegistration({
            cwd,
            path: input.worktreePath,
            branch,
            owner: input.runId,
          }),
        catch: (cause) =>
          new RecoveryFailure("The old Git worktree registration could not be released safely.", {
            cause,
          }),
      });
    }
    yield* workflow
      .createWorktree({
        cwd,
        path: target,
        refName: local ? branch : head,
        ...(local
          ? {}
          : {
              newRefName: branch,
              ...(input.choice === "branch" && base !== null
                ? { baseRefName: `refs/remotes/${base.remoteRefName}` }
                : {}),
            }),
      })
      .pipe(
        Effect.mapError(
          (cause) =>
            new RecoveryFailure(
              "Git could not create the recovery worktree. The branch may already be checked out elsewhere.",
              { cause },
            ),
        ),
      );
    const observed = yield* branchHead(target, "HEAD");
    if (observed !== (localHead ?? head)) {
      return yield* new RecoveryFailure("The branch changed while its worktree was being created.");
    }
    if (advance) {
      yield* runGit(
        target,
        ["merge", "--ff-only", "--", requestedHead],
        "The restored branch could not be fast-forwarded.",
      );
    }
    const readback = yield* branchHead(target, "HEAD");
    const symbolic = yield* runGit(
      target,
      ["symbolic-ref", "HEAD"],
      "The new worktree is detached.",
    );
    if (symbolic !== ref || readback !== requestedHead) {
      return yield* new RecoveryFailure(
        "The new worktree's branch and commit could not be verified.",
      );
    }
    return { path: target, branch };
  });
  return { availableBranch, attach };
});
