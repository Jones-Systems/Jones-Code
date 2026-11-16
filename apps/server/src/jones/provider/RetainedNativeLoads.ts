import type { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import type * as Scope from "effect/Scope";

export function makeRetainedNativeLoads<A, E>(
  scope: Scope.Scope,
  conflict: () => E,
) {
  const originals = new Map<ThreadId, {
    readonly key: string;
    readonly fiber: Fiber.Fiber<A, E>;
  }>();
  return (threadId: ThreadId, key: string, load: Effect.Effect<A, E>) =>
    Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
      let original = originals.get(threadId);
      if (original !== undefined && original.key !== key) return yield* Effect.fail(conflict());
      if (original === undefined) {
        const fiber = yield* restore(load).pipe(Effect.forkIn(scope));
        original = { key, fiber };
        originals.set(threadId, original);
      }
      const captured = original;
      const value = yield* restore(Fiber.join(captured.fiber));
      return {
        value,
        complete: Effect.sync(() => {
          if (originals.get(threadId) === captured) originals.delete(threadId);
        }),
      };
    }));
}
