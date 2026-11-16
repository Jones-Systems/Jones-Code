import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as Semaphore from "effect/Semaphore";

/** Totals for Git subprocesses run under `provideGitProcessTotals`, including cache lookups it starts. */
export interface GitProcessTotals {
  commands: number;
  executionMs: number;
  sharedPermitWaitMsMax: number;
  backgroundGateWaitMsMax: number;
}

const CurrentGitProcessTotals = Context.Reference<GitProcessTotals | undefined>(
  "jones/git/GitProcessTotals",
  { defaultValue: () => undefined },
);

export const provideGitProcessTotals =
  (totals: GitProcessTotals) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.provideService(effect, CurrentGitProcessTotals, totals);

// These phases annotate the caller's Git span without introducing another span.
export const withGitPermitTiming = Effect.fnUntraced(function* <A, E, R>(
  run: Effect.Effect<A, E, R>,
  permits: Semaphore.Semaphore,
  attribute: "git.background_gate_wait_ms" | "git.shared_permit_wait_ms",
) {
  const queuedAt = yield* Clock.currentTimeMillis;
  let admitted = false;
  const recordWait = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const waitMs = Math.max(0, now - queuedAt);
    yield* Effect.annotateCurrentSpan(attribute, waitMs);
    const totals = yield* CurrentGitProcessTotals;
    if (totals === undefined) return;
    if (attribute === "git.background_gate_wait_ms") {
      totals.backgroundGateWaitMsMax = Math.max(totals.backgroundGateWaitMsMax, waitMs);
    } else {
      totals.sharedPermitWaitMsMax = Math.max(totals.sharedPermitWaitMsMax, waitMs);
    }
  });
  return yield* permits
    .withPermits(1)(
      Effect.gen(function* () {
        admitted = true;
        yield* recordWait;
        return yield* run;
      }),
    )
    .pipe(Effect.ensuring(Effect.suspend(() => (admitted ? Effect.void : recordWait))));
});

export const withGitExecutionTiming = Effect.fnUntraced(function* <A, E, R>(
  run: Effect.Effect<A, E, R>,
) {
  const startedAt = yield* Clock.currentTimeMillis;
  return yield* run.pipe(
    Effect.ensuring(
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const executionMs = Math.max(0, now - startedAt);
        yield* Effect.annotateCurrentSpan("git.execution_ms", executionMs);
        const totals = yield* CurrentGitProcessTotals;
        if (totals === undefined) return;
        totals.commands += 1;
        totals.executionMs += executionMs;
      }),
    ),
  );
});

export const recordWorktreeProcessTime = Effect.fnUntraced(function* (
  operation: string,
  phase: "spawn" | "exit",
) {
  if (operation !== "GitVcsDriver.createWorktree") return;
  yield* Effect.annotateCurrentSpan(
    `git.worktree_add.${phase}_at_ms`,
    yield* Clock.currentTimeMillis,
  );
});
