import {
  GoalContinuityError,
  type ContinuityDispatchPermit,
  type ContinuityDeliveryEvidence,
  type ContinuityPreparedTicket,
  type RecoveryProjection,
} from "@t3tools/contracts";
import type {
  GoalContinuityContext,
  RecoveryBoundary,
} from "@t3tools/provider-core/server/jones/GoalContinuityContext";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import type * as Scope from "effect/Scope";
import { continuityDigest } from "./model.ts";

type CompactionPoint = Extract<RecoveryBoundary["point"], { type: "compaction" }>;

/** One captured conversation/run/producer. This is recovery bookkeeping, never
 * a goal runner. The service owns durable matching, CAS and single-use permits.
 */
export function makeCodexRecovery(input: {
  readonly context: GoalContinuityContext;
  readonly boundary: RecoveryBoundary;
  readonly scope: Scope.Scope;
  readonly revalidate: Effect.Effect<void, GoalContinuityError>;
  readonly inject: (
    text: string,
    permit: ContinuityDispatchPermit,
  ) => Effect.Effect<
    | { readonly outcome: "acknowledged" }
    | {
        readonly outcome: "not_sent";
        readonly proof: Extract<ContinuityDeliveryEvidence, { outcome: "not_sent" }>["proof"];
      },
    unknown
  >;
  readonly stop: (nativeTurnId: string | undefined) => Effect.Effect<void>;
}) {
  const started = new Set<string>();
  const completed = new Set<string>();
  const consumedPermits = new Set<string>();
  const keyFor = (point: CompactionPoint) => JSON.stringify([point.nativeTurnId, point.itemId]);
  const boundaries = new Map<
    string,
    {
      readonly boundary: RecoveryBoundary;
      readonly ticket:
        | Parameters<GoalContinuityContext["beginDelivery"]>[0]["pendingTicket"]
        | null;
    }
  >();
  let lastCompleted: string | undefined;
  let continuationHeld = false;

  const revalidate = input.boundary.producerOrigin.revalidateCurrent.pipe(
    Effect.mapError(() => new GoalContinuityError({ reason: "binding_mismatch" })),
    Effect.andThen(input.revalidate),
  );
  const stop = (nativeTurnId?: string) =>
    Effect.suspend(() => {
      // A late acknowledgement never releases this held native run. A later
      // explicit admission gets a new captured run context and the durable gate.
      continuationHeld = true;
      return input.stop(nativeTurnId);
    });
  const fail = () => new GoalContinuityError({ reason: "recovery_required" });

  const continuation = Effect.fn("CodexRecovery.checkContinuation")(function* () {
    if (continuationHeld) return false;
    yield* revalidate;
    const check = yield* input.context.checkContinuation(input.boundary);
    yield* revalidate;
    if (check.requirement.outcome === "hold") return false;
    if (check.requirement.outcome === "absent") return check.currentOwner === null;
    const expected = input.boundary.witness;
    const matches = (owner: typeof check.currentOwner) =>
      owner !== null &&
      owner.environmentId === expected.environmentId &&
      owner.threadId === expected.threadId &&
      owner.providerInstanceId === expected.providerInstanceId &&
      owner.providerThreadId === expected.providerThreadId &&
      owner.driver === expected.driver &&
      owner.nativeThreadId === expected.nativeThreadId;
    return matches(check.currentOwner) && matches(check.requirement.owner);
  });

  const fresh = Effect.fn("CodexRecovery.prepareRecovery")(function* (boundary: RecoveryBoundary) {
    yield* revalidate;
    const projection = yield* input.context.prepareRecovery(boundary);
    yield* revalidate;
    if (projection.outcome === "hold" || projection.outcome === "unsupported")
      return yield* new GoalContinuityError({ reason: projection.reason });
    if (
      projection.outcome === "context" &&
      new TextEncoder().encode(projection.text).byteLength > 4096
    )
      return yield* new GoalContinuityError({ reason: "projection_too_large" });
    return projection;
  });

  const settleUnknown = (
    permit: ContinuityDispatchPermit,
    cause: "timeout" | "cancelled" | "lost_response",
  ) =>
    input.context.settleDelivery({ permit, evidence: { outcome: "unknown", cause } }).pipe(
      Effect.catch(() => Effect.logWarning("Jones continuity delivery settlement is unavailable.")),
      Effect.asVoid,
    );

  const recover = Effect.fn("CodexRecovery.recover")(
    function* (key: string) {
      const pending = boundaries.get(key);
      if (!pending) return yield* fail();
      // Claim once before the first asynchronous read. A failure leaves the durable
      // obligation held; it never turns a duplicate notification into a retry.
      boundaries.delete(key);
      let projection: Extract<RecoveryProjection, { outcome: "context" }> | undefined;
      let ticket: ContinuityPreparedTicket | undefined;
      // Only an explicitly refused preparation may reread. No dispatch or unknown
      // commit is retried, and generation churn is bounded to two fresh reads.
      for (let ordinal = 0; ordinal < 2; ordinal++) {
        const current = yield* fresh(pending.boundary);
        if (current.outcome === "absent" && pending.ticket === null) return;
        if (current.outcome !== "context" || pending.ticket === null) return yield* fail();
        const prepared = yield* input.context.beginDelivery({
          pendingTicket: pending.ticket,
          projection: current,
        });
        if (prepared.outcome === "prepared") {
          projection = current;
          ticket = prepared.ticket;
          break;
        }
        if (prepared.outcome !== "refused" || prepared.reason !== "stale") return yield* fail();
      }
      if (ticket === undefined || projection === undefined) return yield* fail();
      const preparedTicket = ticket;
      const capsule = projection.text;
      yield* revalidate;
      yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const dispatch = yield* input.context.markDispatching(preparedTicket);
          if (dispatch.outcome !== "dispatching") return yield* fail();
          const permit = dispatch.permit;
          // No permit is reconstructed, shared with a successor or consumed twice.
          if (consumedPermits.has(permit.permitId)) return yield* fail();
          consumedPermits.add(permit.permitId);
          // Retain the original waiter after timeout so a late acknowledgement can
          // settle only this permit. Neither settlement nor acknowledgement resumes.
          const acknowledgement = yield* restore(
            Effect.gen(function* () {
              const current = yield* revalidate.pipe(Effect.exit);
              if (current._tag === "Failure")
                return {
                  outcome: "not_sent" as const,
                  proof: {
                    type: "dispatch_precluded" as const,
                    revocationRef: permit.dispatchAttemptId,
                  },
                };
              return yield* input.inject(capsule, permit);
            }),
          ).pipe(
            Effect.flatMap((sent) =>
              input.context.settleDelivery({
                permit,
                evidence:
                  sent.outcome === "not_sent"
                    ? sent
                    : {
                        outcome: "acknowledged",
                        rpcRequestId: permit.rpcRequestId,
                        acknowledgementRef: permit.rpcRequestId,
                        witness: pending.boundary.witness,
                        producer: pending.boundary.producer,
                      },
              }),
            ),
            Effect.flatMap((settled) =>
              settled.outcome === "settled" &&
              (settled.receipt.phase === "acknowledged" || settled.receipt.phase === "not_sent")
                ? Effect.succeed(settled.receipt.phase === "acknowledged")
                : Effect.fail(fail()),
            ),
            Effect.onError(() => settleUnknown(permit, "lost_response")),
            Effect.onInterrupt(() => settleUnknown(permit, "cancelled")),
            Effect.forkIn(input.scope, { startImmediately: true }),
          );
          const acknowledged = yield* restore(
            Fiber.join(acknowledgement).pipe(
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () =>
                  settleUnknown(permit, "timeout").pipe(Effect.andThen(Effect.fail(fail()))),
              }),
            ),
          ).pipe(Effect.onInterrupt(() => settleUnknown(permit, "cancelled")));
          if (!acknowledged) return yield* fail();
        }),
      );
      if (!(yield* continuation())) return yield* fail();
    },
    (effect, key) =>
      effect.pipe(
        Effect.onError(() => stop(JSON.parse(key)[0] as string)),
        Effect.onInterrupt(() => stop(JSON.parse(key)[0] as string)),
      ),
  );

  return {
    boundary: input.boundary,
    get held() {
      return continuationHeld;
    },
    hold: stop,
    checkContinuation: continuation().pipe(Effect.catch(() => Effect.succeed(false))),
    turnStartContext: Effect.gen(function* () {
      if (!(yield* continuation())) return yield* fail();
      return yield* fresh(input.boundary);
    }).pipe(
      Effect.onError(() =>
        stop().pipe(Effect.forkIn(input.scope, { startImmediately: true }), Effect.asVoid),
      ),
    ),
    /** Only short durable work is awaited by the notification reader. */
    transition: Effect.fn("CodexRecovery.transition")(
      function* (point: CompactionPoint, type: "started" | "completed", automatic: boolean) {
        const key = keyFor(point);
        const boundary: RecoveryBoundary = { ...input.boundary, point };
        yield* revalidate;
        // The durable service is authoritative; the local sets additionally stop
        // reordered/duplicate notifications from launching another executor.
        const result = yield* input.context.recordRecoveryTransition(boundary, {
          eventId: continuityDigest([input.boundary.producer.producerId, point, type]),
          type,
        });
        if (result.outcome === "absent") {
          if (type === "completed" && !automatic) {
            boundaries.set(key, { boundary, ticket: null });
            lastCompleted = key;
          }
          return;
        }
        if (result.outcome !== "recorded") return yield* fail();
        if (type === "started") {
          if (!completed.has(key)) started.add(key);
          return;
        }
        if (completed.has(key)) return;
        if (!started.has(key) || result.pendingTicket === null) return yield* fail();
        completed.add(key);
        started.delete(key);
        boundaries.set(key, { boundary, ticket: result.pendingTicket });
        lastCompleted = key;
        if (automatic)
          yield* recover(key).pipe(
            Effect.catch(() =>
              Effect.logWarning("Jones continuity recovery held the native goal."),
            ),
            Effect.forkIn(input.scope, { startImmediately: true }),
          );
      },
      (effect, point) =>
        effect.pipe(
          Effect.onError(() =>
            stop(point.nativeTurnId).pipe(
              Effect.forkIn(input.scope, { startImmediately: true }),
              Effect.asVoid,
            ),
          ),
        ),
    ),
    recoverCompleted: Effect.suspend(() =>
      lastCompleted === undefined ? Effect.fail(fail()) : recover(lastCompleted),
    ),
  };
}
