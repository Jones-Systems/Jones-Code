import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";

import type {
  NativeAdmissionUse,
  UpdateAdmissionFence,
  UpdateAdmissionRefused,
} from "./UpdateAdmissionFence.ts";

export const workflowAdmission = (fence: Option.Option<UpdateAdmissionFence["Service"]>) => {
  // Only a refusal to acquire may be retried. An admitted effect can already have acted.
  const attempt = <A, E, R>(
    work: (use: NativeAdmissionUse | undefined) => Effect.Effect<A, E, R>,
  ): Effect.Effect<Result.Result<A, UpdateAdmissionRefused>, E, R> => {
    if (Option.isNone(fence)) {
      return Effect.suspend(() => Effect.map(work(undefined), Result.succeed));
    }
    return Effect.acquireUseRelease(
      Scope.make(),
      (scope) => {
        const acquire = fence.value.nativeUse.pipe(
          Scope.provide(scope),
          Effect.tap((use) => use.checkCurrent),
        );
        return Effect.matchEffect(acquire, {
          onFailure: (error) => Effect.succeed(Result.fail(error)),
          onSuccess: (use) => Effect.map(work(use), Result.succeed),
        });
      },
      Scope.close,
    );
  };

  const run = <A, E, R>(work: Effect.Effect<A, E, R>) =>
    attempt(() => work).pipe(Effect.flatMap(Effect.fromResult));

  const whenOpen = Effect.fnUntraced(function* <A, E, R>(
    work: (use: NativeAdmissionUse | undefined) => Effect.Effect<A, E, R>,
  ): Effect.fn.Return<A, E, R> {
    for (;;) {
      const admitted = yield* attempt(work);
      if (Result.isSuccess(admitted)) return admitted.success;
      if (Option.isSome(fence)) {
        // Provider admission is open during an automatic final check. Native work is still
        // fenced then, so leave time between attempts and acquire a fresh use every time.
        yield* fence.value.awaitAdmission.pipe(
          Effect.andThen(Effect.sleep("250 millis")),
          Effect.interruptible,
        );
      }
    }
  });

  const hold = <A, E, R>(work: Effect.Effect<A, E, R>) => whenOpen(() => work);

  // A remote observation can poll for minutes. Retire it when the fence rises so it cannot
  // poll again or hold handoff while waiting for the remote result.
  const observe = Effect.fnUntraced(function* <A, E, R>(
    read: Effect.Effect<A, E, R>,
  ): Effect.fn.Return<A, E, R> {
    for (;;) {
      const result = yield* whenOpen((use) =>
        use === undefined
          ? Effect.asSome(read)
          : Effect.raceFirst(Effect.asSome(read), Effect.as(use.invalidated, Option.none<A>())),
      );
      if (Option.isSome(result)) return result.value;
    }
  });

  return { run, hold, observe };
};
