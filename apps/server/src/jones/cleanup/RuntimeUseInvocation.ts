import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import {
  assertCheckedDatabaseOwner,
  type CheckedDatabaseOwnerToken,
} from "./CheckedRuntimeOwner.ts";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";
import { validateRuntimeUseClaim } from "./RuntimeUseClaim.ts";
import {
  digest,
  RuntimeUseError,
  runtimeUseError,
  type RuntimeUseBinding,
  type RuntimeUseProducer,
} from "./RuntimeUseProtocol.ts";

const fail = (reason: string) => Effect.fail(new RuntimeUseError({ reason }));
const checkOwner = (owner: CheckedDatabaseOwnerToken) =>
  Effect.try({
    try: () => assertCheckedDatabaseOwner(owner),
    catch: runtimeUseError,
  });

/** One live admission owner, after a known committed reservation. No constructor
 * from persisted IDs exists. Recovery can read pending, but cannot replay it. */
export const prepareRuntimeUseInvocation = Effect.fn("RuntimeUseInvocation.prepare")(
  function* (input: {
    readonly binding: RuntimeUseBinding;
    readonly owner: CheckedDatabaseOwnerToken;
    readonly revalidateWorkspace: Effect.Effect<void, RuntimeUseError>;
    // The actual caller's persisted thread/session/target and deletion checks.
    // Called inside each owning transaction, never for filesystem observations.
    readonly validate: Effect.Effect<void, RuntimeUseError>;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const store = yield* makeRuntimeUseStore;
    const validate = input.validate.pipe(
      Effect.andThen(
        input.binding.original.claim === null
          ? Effect.void
          : validateRuntimeUseClaim(input.binding.original.claim).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
      ),
    );
    yield* checkOwner(input.owner);
    yield* input.revalidateWorkspace;
    const reservation = yield* sql
      .withTransaction(validate.pipe(Effect.andThen(store.reserve(input.binding))))
      .pipe(Effect.mapError(runtimeUseError));
    if (!reservation.created) return yield* fail("runtime_use_prior_outcome_unknown_no_replay");
    const binding = reservation.record.binding;
    let phase: "fresh" | "checking" | "entered" | "revoked" = "fresh";
    const revokeBeforeEntry = Effect.uninterruptible(
      Effect.suspend(() => {
        if (phase === "entered" || phase === "revoked") return Effect.succeed(false);
        // Synchronous revocation wins against the last synchronous entry check.
        // A checking invocation rechecks this value after its SQL/owner checks.
        phase = "revoked";
        return store
          .recordRetirement({
            schema: "jones.workspace-runtime-retirement/v1",
            useId: binding.useId,
            bindingSha256: digest(binding),
            outcome: "not_invoked",
            observationCount: 0,
          })
          .pipe(Effect.as(true));
      }),
    );
    const invoke = <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E | RuntimeUseError, R> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (phase !== "fresh") return yield* fail("runtime_use_invocation_already_claimed");
          phase = "checking";
          return yield* restore(
            Effect.gen(function* () {
              yield* checkOwner(input.owner);
              yield* input.revalidateWorkspace;
              yield* sql.withTransaction(validate).pipe(Effect.mapError(runtimeUseError));
              yield* checkOwner(input.owner);
              yield* input.revalidateWorkspace;
              return yield* Effect.suspend((): Effect.Effect<A, E | RuntimeUseError, R> => {
                if (phase !== "checking") return fail("runtime_use_invocation_revoked");
                // Once entered, any failure/interruption leaves pending uncertainty.
                // No completion/ACK/finalizer is interpreted as physical retirement.
                phase = "entered";
                return effect;
              });
            }),
          ).pipe(Effect.onExit(() => revokeBeforeEntry.pipe(Effect.asVoid)));
        }),
      );
    const observeProducer = Effect.fn("RuntimeUseInvocation.observeProducer")(function* (
      producer: Omit<RuntimeUseProducer, "schema" | "useId" | "bindingSha256">,
    ) {
      if (phase !== "entered") return yield* fail("runtime_use_producer_outside_invocation");
      return yield* store.observe({
        ...producer,
        schema: "jones.workspace-runtime-producer/v1",
        useId: binding.useId,
        bindingSha256: digest(binding),
      });
    });
    const validateProducer = Effect.suspend(() =>
      phase === "entered" ? validate : fail("runtime_use_producer_outside_invocation"),
    );
    const revalidateProducer = Effect.gen(function* () {
      yield* checkOwner(input.owner);
      yield* input.revalidateWorkspace;
      yield* sql.withTransaction(validateProducer).pipe(Effect.mapError(runtimeUseError));
      yield* checkOwner(input.owner);
      yield* input.revalidateWorkspace;
    });
    return {
      binding,
      invoke,
      revokeBeforeEntry,
      observeProducer,
      revalidateProducer,
      producerValidation: { validate: validateProducer, revalidate: revalidateProducer },
    };
  },
);
