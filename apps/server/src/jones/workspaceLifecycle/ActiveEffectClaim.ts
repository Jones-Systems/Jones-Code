import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import type { EffectOutboxV2, OrchestrationEffectV2 } from "../../orchestration-v2/EffectOutbox.ts";
import { sameOperationRequest } from "./NativeOperation.ts";

export class EffectClaimLost extends Schema.TaggedError<EffectClaimLost>()("EffectClaimLost", {
  effectId: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

export function findEffectClaimLost(error: unknown): EffectClaimLost | undefined {
  const visited = new Set<unknown>();
  let current = error;
  while (typeof current === "object" && current !== null && !visited.has(current)) {
    visited.add(current);
    if (Schema.is(EffectClaimLost)(current)) return current;
    if (Cause.isCause(current)) {
      const failure = Cause.findErrorOption(current);
      current = Option.isSome(failure) ? failure.value : undefined;
    } else current = "cause" in current ? current.cause : undefined;
  }
  return undefined;
}

export class CurrentEffectClaim extends Context.Service<
  CurrentEffectClaim,
  {
    readonly nativeCustody: boolean;
    readonly check: Effect.Effect<void, EffectClaimLost>;
    readonly lost: Effect.Effect<never, EffectClaimLost>;
    readonly hasLost: Effect.Effect<boolean>;
    readonly invalidate: (cause?: unknown) => Effect.Effect<void>;
    readonly use: <A, E, R>(
      sql: (claim: OrchestrationEffectV2) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | EffectClaimLost, R>;
    readonly settle: <A, E, R>(
      sql: (claim: OrchestrationEffectV2) => Effect.Effect<A, E, R>,
      allowLost?: boolean,
    ) => Effect.Effect<A, E | EffectClaimLost, R>;
  }
>()("t3/jones/workspaceLifecycle/ActiveEffectClaim/CurrentEffectClaim") {}

// Put this inside an executor's existing failure handler, so custody loss takes
// its normal correlated run-failure path instead of only interrupting a caller.
export const guardClaim = Effect.fn("ActiveEffectClaim.guardClaim")(function* <A, E, R>(
  effect: Effect.Effect<A, E, R>,
) {
  const claim = yield* Effect.serviceOption(CurrentEffectClaim);
  if (Option.isNone(claim) || !claim.value.nativeCustody) return yield* effect;
  yield* claim.value.check;
  return yield* Effect.raceFirst(effect, claim.value.lost);
});

// The claiming worker is the only production issuer. A scope owns the renewal
// fiber; neither a persisted envelope nor a downstream session creates a handle.
export const withActiveEffectClaim = Effect.fn("ActiveEffectClaim.withActiveEffectClaim")(
  function* <A, E, R>(
    outbox: EffectOutboxV2["Service"],
    captured: OrchestrationEffectV2,
    leaseDurationMs: number,
    renew: boolean,
    run: (claim: CurrentEffectClaim["Service"]) => Effect.Effect<A, E, R>,
  ) {
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const permit = yield* Semaphore.make(1);
        const loss = yield* Deferred.make<EffectClaimLost>();
        let active = true;
        let expiry = captured.leaseExpiresAt;
        const snapshot = () => ({ ...captured, leaseExpiresAt: expiry });
        const invalidate = Effect.fn("ActiveEffectClaim.invalidate")(function* (cause?: unknown) {
          active = false;
          yield* Deferred.succeed(
            loss,
            new EffectClaimLost({
              effectId: captured.id,
              ...(cause === undefined ? {} : { cause }),
            }),
          );
        });
        const checkWithPermitHeld = Effect.gen(function* () {
          if (!active) return yield* new EffectClaimLost({ effectId: captured.id });
          const current = yield* outbox
            .get(captured.id)
            .pipe(
              Effect.mapError((cause) => new EffectClaimLost({ effectId: captured.id, cause })),
            );
          if (
            !active ||
            Option.isNone(current) ||
            current.value.status !== "running" ||
            current.value.threadId !== captured.threadId ||
            current.value.commandId !== captured.commandId ||
            current.value.leaseOwner !== captured.leaseOwner ||
            current.value.attemptCount !== captured.attemptCount ||
            current.value.leaseExpiresAt !== expiry ||
            expiry === null ||
            (captured.payloadIdentity !== undefined &&
              current.value.payloadIdentity !== captured.payloadIdentity) ||
            !sameOperationRequest(current.value.request, captured.request) ||
            !sameOperationRequest(
              current.value.nativeCreationExecutionReference,
              captured.nativeCreationExecutionReference,
            ) ||
            DateTime.toEpochMillis(DateTime.makeUnsafe(expiry)) <=
              DateTime.toEpochMillis(yield* DateTime.now)
          )
            return yield* new EffectClaimLost({ effectId: captured.id });
        }).pipe(Effect.tapError((cause) => invalidate(cause)));
        const claim: CurrentEffectClaim["Service"] = {
          nativeCustody: renew,
          check: permit.withPermits(1)(checkWithPermitHeld),
          lost: Deferred.await(loss).pipe(Effect.flatMap(Effect.fail)),
          hasLost: Effect.sync(() => !active),
          invalidate,
          use: (sql) =>
            permit.withPermits(1)(
              Effect.uninterruptible(
                checkWithPermitHeld.pipe(Effect.andThen(Effect.suspend(() => sql(snapshot())))),
              ),
            ),
          settle: (sql, allowLost = false) =>
            permit.withPermits(1)(
              Effect.uninterruptible(
                Effect.gen(function* () {
                  if (!active && !allowLost)
                    return yield* new EffectClaimLost({ effectId: captured.id });
                  return yield* sql(snapshot());
                }),
              ),
            ),
        };
        let renewal: Fiber.Fiber<void> | undefined;
        if (renew) {
          renewal = yield* Effect.gen(function* () {
            while (true) {
              yield* Effect.sleep(Math.max(1, leaseDurationMs / 3));
              yield* permit.withPermits(1)(
                Effect.uninterruptible(
                  Effect.gen(function* () {
                    if (!active) return yield* new EffectClaimLost({ effectId: captured.id });
                    // An error may have followed commit. Lose custody conservatively;
                    // never guess a new expected value or issue a replacement renewal.
                    expiry = yield* outbox.renew({ claim: snapshot(), leaseDurationMs });
                    if (!active) return yield* new EffectClaimLost({ effectId: captured.id });
                  }),
                ),
              );
            }
          }).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause) ? Effect.void : invalidate(cause),
            ),
            Effect.forkChild,
          );
        }
        return yield* run(claim).pipe(
          Effect.provideService(CurrentEffectClaim, claim),
          Effect.ensuring(
            Effect.gen(function* () {
              if (renewal !== undefined) yield* Fiber.interrupt(renewal);
              active = false;
            }),
          ),
        );
      }),
    );
  },
);
