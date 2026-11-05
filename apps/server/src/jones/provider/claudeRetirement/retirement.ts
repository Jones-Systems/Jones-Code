import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { ProviderControlOutcomeUnknown } from "@t3tools/provider-core/server/jones/providerControlOutcome";

export interface ClaudeQueryRetirement {
  readonly start: Effect.Effect<void>;
  readonly join: Effect.Effect<void, ProviderControlOutcomeUnknown>;
}

// One record belongs to one captured query. Raw SDK observers outlive a released caller.
export function makeClaudeQueryRetirement(input: {
  readonly deadline: number;
  readonly cancel?: Effect.Effect<void, unknown>;
  readonly close: Effect.Effect<void, unknown>;
  readonly termination: Effect.Effect<void, unknown>;
  readonly drain: Effect.Effect<void>;
  readonly retire: Effect.Effect<void, unknown>;
  readonly failure: (cause?: unknown) => ProviderControlOutcomeUnknown;
  readonly fence: () => void;
}): ClaudeQueryRetirement {
  const completion = Deferred.makeUnsafe<void, ProviderControlOutcomeUnknown>();
  let started = false;
  let closeStarted = false;
  let failure: ProviderControlOutcomeUnknown | undefined;
  const fail = (cause?: unknown) =>
    Effect.suspend(() => {
      if (Deferred.isDoneUnsafe(completion)) return Effect.void;
      input.fence();
      failure ??= input.failure(cause);
      Deferred.doneUnsafe(completion, Effect.fail(failure));
      return Effect.void;
    });
  const startClose = Effect.suspend(() => {
    if (closeStarted) return Effect.void;
    closeStarted = true;
    return input.close.pipe(
      Effect.andThen(input.termination),
      Effect.andThen(input.drain),
      Effect.andThen(input.retire),
      Effect.exit,
      Effect.flatMap((exit) =>
        Exit.isSuccess(exit)
          ? Deferred.succeed(completion, undefined).pipe(Effect.asVoid)
          : fail(Cause.squash(exit.cause)),
      ),
      Effect.forkDetach({ startImmediately: true }),
      Effect.asVoid,
    );
  });
  return {
    start: Effect.uninterruptible(
      Effect.suspend(() => {
        if (started) return Effect.void;
        started = true;
        return Effect.gen(function* () {
          if (input.cancel !== undefined) {
            const cancelled = Deferred.makeUnsafe<void>();
            yield* input.cancel.pipe(
              Effect.exit,
              Effect.andThen(Deferred.succeed(cancelled, undefined)),
              Effect.forkDetach({ startImmediately: true }),
            );
            // Join the observer, never the possibly uninterruptible native call.
            const cancelBudget = Math.max(
              0,
              Math.min(2_000, input.deadline - (yield* Clock.currentTimeMillis)),
            );
            yield* Deferred.await(cancelled).pipe(Effect.timeoutOption(cancelBudget));
          }
          yield* startClose;
        }).pipe(
          Effect.interruptible,
          Effect.ensuring(startClose),
          Effect.forkDetach({ startImmediately: true }),
          Effect.asVoid,
        );
      }),
    ),
    join: Effect.gen(function* () {
      const remaining = Math.max(0, input.deadline - (yield* Clock.currentTimeMillis));
      yield* Deferred.await(completion).pipe(
        Effect.timeoutOrElse({
          duration: remaining,
          orElse: () =>
            fail().pipe(
              Effect.andThen(startClose),
              Effect.andThen(Deferred.await(completion)),
            ),
        }),
      );
    }).pipe(Effect.onInterrupt(() => fail().pipe(Effect.andThen(startClose)))),
  };
}
