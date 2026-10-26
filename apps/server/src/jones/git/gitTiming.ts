import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import type * as Semaphore from "effect/Semaphore";

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
    yield* Effect.annotateCurrentSpan(attribute, Math.max(0, now - queuedAt));
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
        yield* Effect.annotateCurrentSpan("git.execution_ms", Math.max(0, now - startedAt));
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
