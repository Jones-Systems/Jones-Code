import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import type { RetirementError } from "./Receipts.ts";

// Registration is synchronous and runs before interruption can enter the open's
// error handler. The handler therefore always owns the acquired claim.
export const withRegisteredOpen = <H, A, E, R, E2, R2, R3>(
  acquire: Effect.Effect<H, E, R>,
  registeredOpen: (handle: H) => Effect.Effect<A, E2, R2>,
  openFailed: (handle: H) => Effect.Effect<void, never, R3>,
) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const handle = yield* acquire;
      return yield* Effect.suspend(() => restore(registeredOpen(handle))).pipe(
        Effect.onError(() => openFailed(handle)),
      );
    }),
  );

// Native close is owned by the manager. Persistence retries never close again.
export const make = Effect.gen(function* () {
  const scope = yield* Scope.Scope;
  const handles = new Map<
    object,
    { release: Effect.Effect<void, RetirementError>; drained: boolean; running: boolean }
  >();
  const kick = Effect.fn("WorkspaceDuty.retryRelease")(function* (handle: object) {
    const state = handles.get(handle);
    if (!state || !state.drained || state.running) return;
    state.running = true;
    yield* Effect.gen(function* () {
      let delay = 250;
      while (handles.has(handle)) {
        const result = yield* state.release.pipe(Effect.exit);
        if (result._tag === "Success") {
          handles.delete(handle);
          return;
        }
        yield* Effect.logWarning("Workspace duty release persistence unavailable; retrying");
        yield* Effect.sleep(delay);
        delay = Math.min(delay * 2, 30_000);
      }
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          state.running = false;
        }),
      ),
      Effect.interruptible,
      Effect.forkIn(scope, { startImmediately: true }),
    );
  });
  return {
    register: (handle: object, release: Effect.Effect<void, RetirementError>) => {
      handles.set(handle, { release, drained: false, running: false });
    },
    drained: (owned: readonly object[]) =>
      Effect.forEach(
        owned,
        (handle) =>
          Effect.gen(function* () {
            const state = handles.get(handle);
            if (state) state.drained = true;
            yield* kick(handle);
          }),
        { discard: true },
      ),
    retry: Effect.suspend(() => Effect.forEach([...handles.keys()], kick, { discard: true })),
  };
});
