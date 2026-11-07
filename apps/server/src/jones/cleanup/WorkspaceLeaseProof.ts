import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

interface HeldLease {
  readonly key: string;
  readonly fiberId: number;
  active: boolean;
}
class WorkspaceLeaseContext extends Context.Service<
  WorkspaceLeaseContext,
  {
    readonly leases: readonly HeldLease[];
  }
>()("t3/jones/cleanup/WorkspaceLeaseProof/WorkspaceLeaseContext") {}

/** Observation only: never acquires a permit or converts inherited context into
 * authority. The exact original acquisition must still be active on this fiber. */
export const hasWorkspaceLeaseProof = Effect.fn("WorkspaceLeaseProof.hasLease")(function* (
  key: string,
) {
  const fiberId = yield* Effect.fiberId;
  const context = yield* Effect.serviceOption(WorkspaceLeaseContext);
  return (
    Option.isSome(context) &&
    context.value.leases.some(
      (lease) => lease.active && lease.key === key && lease.fiberId === fiberId,
    )
  );
});

/** Reentrancy belongs to this exact fiber and live acquisition, never a child
 * fiber inheriting context, a serialized token or a completed outer scope. */
export const withWorkspaceLeaseProof = Effect.fn("WorkspaceLeaseProof.withLease")(function* <
  A,
  E,
  R,
>(
  key: string,
  acquire: <B, E2, R2>(effect: Effect.Effect<B, E2, R2>) => Effect.Effect<B, E2, R2>,
  effect: Effect.Effect<A, E, R>,
): Effect.fn.Return<A, E, R> {
  const fiberId = yield* Effect.fiberId;
  const inherited = yield* Effect.serviceOption(WorkspaceLeaseContext);
  const leases = Option.isSome(inherited) ? inherited.value.leases : [];
  if (leases.some((lease) => lease.active && lease.key === key && lease.fiberId === fiberId))
    return yield* effect;
  return yield* acquire(
    Effect.suspend(() => {
      const held: HeldLease = { key, fiberId, active: true };
      return effect.pipe(
        Effect.provideService(WorkspaceLeaseContext, { leases: [...leases, held] }),
        Effect.ensuring(
          Effect.sync(() => {
            held.active = false;
          }),
        ),
      );
    }),
  );
});
