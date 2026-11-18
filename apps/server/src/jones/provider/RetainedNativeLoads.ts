import type { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Semaphore from "effect/Semaphore";
import type * as Scope from "effect/Scope";

export function makeRetainedNativeLoads<A, E>(scope: Scope.Scope, conflict: () => E) {
  const originals = new Map<
    ThreadId,
    {
      readonly key: string;
      readonly fiber: Fiber.Fiber<A, E>;
    }
  >();
  const admissions = new Map<ThreadId, Semaphore.Semaphore>();
  return (
    threadId: ThreadId,
    key: string,
    load: Effect.Effect<A, E>,
    recover?: (cause: E) => Effect.Effect<A, E>,
    releaseUnapplied?: (cause: E) => boolean,
  ) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        let admission = admissions.get(threadId);
        if (admission === undefined) {
          admission = Semaphore.makeUnsafe(1);
          admissions.set(threadId, admission);
        }
        // Serialize only installation of this thread's original fiber. The
        // retained native work and other threads never wait under this permit.
        const { captured, created } = yield* Effect.gen(function* () {
          let original = originals.get(threadId);
          const created = original === undefined;
          if (original !== undefined && original.key !== key) return yield* Effect.fail(conflict());
          if (original === undefined) {
            const fiber = yield* restore(load).pipe(
              Effect.tapError((cause) =>
                admission.withPermits(1)(
                  Effect.sync(() => {
                    if (originals.get(threadId) === original && releaseUnapplied?.(cause)) {
                      originals.delete(threadId);
                    }
                  }),
                ),
              ),
              Effect.forkIn(scope),
            );
            original = { key, fiber };
            originals.set(threadId, original);
          }
          return { captured: original, created };
        }).pipe(admission.withPermits(1));
        const value = yield* restore(
          Fiber.join(captured.fiber).pipe(
            Effect.catch((cause) =>
              created || recover === undefined ? Effect.fail(cause) : recover(cause),
            ),
          ),
        );
        return {
          value,
          complete: Effect.sync(() => {
            if (originals.get(threadId) === captured) originals.delete(threadId);
          }),
        };
      }),
    );
}
