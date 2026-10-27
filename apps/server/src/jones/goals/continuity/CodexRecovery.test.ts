import { assert, it } from "@effect/vitest";
import {
  GoalContinuityError,
  type ContinuityDeliveryEvidence,
  type ContinuityDeliveryReceipt,
  type ContinuityDispatchPermit,
  type ContinuityOwner,
  type ContinuityPreparedTicket,
  type ContinuityWitness,
  type DeliveryGate,
  type RecoveryProjection,
} from "@t3tools/contracts";
import type {
  GoalContinuityContext,
  RecoveryBoundary,
} from "@t3tools/provider-core/server/jones/GoalContinuityContext";
import { makeProviderEventProducer } from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import { makeCodexRecovery } from "./CodexRecovery.ts";

const owner = {
  environmentId: "environment", threadId: "thread", providerInstanceId: "codex",
  providerThreadId: "provider-thread", driver: "codex", nativeThreadId: "native-thread",
} as ContinuityOwner;
const witness = {
  ...owner, providerSessionId: "session", runtimeGeneration: "generation", runId: "run", attemptId: "attempt",
} as ContinuityWitness;
const point = { type: "compaction", nativeTurnId: "native-turn", itemId: "compact-item" } as const;
const digest = "a".repeat(64);
const capability = {
  strongIdentity: true, turnStart: "fresh_context", compaction: "fresh_context_injection",
  strictPreResumptionBarrier: "unsupported",
} as const;
const clear: DeliveryGate = { outcome: "clear", fenceRevision: "0" };

// The callback port is the external boundary here. This fixture records calls
// and supplies typed durable outcomes; it does not replace the recovery executor.
const fixture = Effect.fnUntraced(function* (waitForAck = false) {
  const scope = yield* Scope.Scope;
  const sent = yield* Deferred.make<void>();
  const reply = yield* Deferred.make<void, GoalContinuityError>();
  const stopped = yield* Deferred.make<void>();
  const unknown = yield* Deferred.make<void>();
  const acknowledged = yield* Deferred.make<void>();
  const calls: string[] = [];
  const transitions: Array<{ boundary: RecoveryBoundary; type: "started" | "completed" }> = [];
  const settlements: Array<{ permit: ContinuityDispatchPermit; evidence: ContinuityDeliveryEvidence }> = [];
  const injected: string[] = [];
  const state = {
    current: true, absent: false, stalePreparations: 0, uncertainDispatch: false,
    replaceAtDispatch: false, rejectPreclusion: false, protocolNoEffect: false,
    nativeStops: 0, gate: clear as DeliveryGate,
    projection: { outcome: "context", goalId: "goal", revision: "1", generation: "1",
      snapshotDigest: digest, text: "Fresh untrusted checkpoint" } as RecoveryProjection,
  };
  const producerOrigin = makeProviderEventProducer({
    driver: witness.driver, instanceId: witness.providerInstanceId,
    providerSessionId: witness.providerSessionId, runtimeGeneration: witness.runtimeGeneration,
  }, Effect.sync(() => state.current)).origin;
  const boundary: RecoveryBoundary = {
    witness, producerOrigin, point: { type: "turn_start" },
    producer: {
      producerId: "producer-1", driver: witness.driver,
      providerInstanceId: witness.providerInstanceId, providerSessionId: witness.providerSessionId,
      runtimeGeneration: witness.runtimeGeneration,
    },
  };
  let receipt: ContinuityDeliveryReceipt | undefined;
  const context: GoalContinuityContext = {
    capability,
    recordRecoveryTransition: (observed, transition) => Effect.sync(() => {
      calls.push(`record:${transition.type}`);
      assert.strictEqual(observed.producerOrigin, producerOrigin);
      transitions.push({ boundary: observed, type: transition.type });
      if (state.absent) return { outcome: "absent", gate: clear } as const;
      const exact = receipt !== undefined && JSON.stringify(receipt.identity.point) === JSON.stringify(observed.point);
      if (transition.type === "completed" && !exact)
        return { outcome: "refused", reason: "invalid_transition" } as const;
      if (receipt === undefined) {
        receipt = {
          identity: { deliveryId: "delivery-1", originalOwner: owner, goalId: "goal", revision: "1",
            generation: "1", point: observed.point, witness, producer: boundary.producer },
          deliveryRevision: "1", fenceRevision: "1", phase: "pending", preparation: null,
          events: [{ eventId: transition.eventId, deliveryRevision: "1", fenceRevision: "1",
            phase: "pending", transition, permit: null, evidence: null }], dispositions: [],
        };
      }
      state.gate = { outcome: "hold", fenceRevision: "1", reason: "delivery_pending", deliveryIds: ["delivery-1"] };
      return { outcome: "recorded", receipt,
        pendingTicket: { type: "pending_ticket", identity: receipt.identity,
          deliveryRevision: receipt.deliveryRevision, fenceRevision: receipt.fenceRevision }, gate: state.gate } as const;
    }),
    prepareRecovery: (observed) => Effect.sync(() => {
      calls.push("fresh");
      assert.strictEqual(observed.producerOrigin, producerOrigin);
      return state.absent ? { outcome: "absent" } as const : state.projection;
    }),
    beginDelivery: ({ pendingTicket, projection }) => Effect.sync(() => {
      calls.push("prepare");
      if (state.stalePreparations-- > 0) return { outcome: "refused", reason: "stale" } as const;
      const ticket: ContinuityPreparedTicket = {
        ...pendingTicket, type: "prepared_ticket",
        preparation: { target: { owner, goalId: projection.goalId, revision: projection.revision,
          generation: projection.generation, snapshotDigest: projection.snapshotDigest }, capsuleDigest: digest },
      };
      return { outcome: "prepared", ticket, gate: state.gate } as const;
    }),
    markDispatching: (ticket) => Effect.suspend(() => {
      calls.push("dispatching");
      if (state.uncertainDispatch) return Effect.fail(new GoalContinuityError({ reason: "unavailable" }));
      if (state.replaceAtDispatch) state.current = false;
      const permit: ContinuityDispatchPermit = {
        ...ticket, type: "dispatch_permit", permitId: "permit-1", dispatchAttemptId: "attempt-1", rpcRequestId: "request-1",
      };
      return Effect.succeed({ outcome: "dispatching", permit, gate: state.gate } as const);
    }),
    settleDelivery: (settlement) => Effect.gen(function* () {
      calls.push(`settle:${settlement.evidence.outcome}`);
      settlements.push(settlement);
      assert.strictEqual(settlement.permit.identity.witness, witness);
      assert.equal(settlement.permit.identity.producer.producerId, "producer-1");
      // Refusal must remain safe even if the original live proof is unavailable.
      if (state.rejectPreclusion && settlement.evidence.outcome === "not_sent" && settlement.evidence.proof.type === "dispatch_precluded")
        return { outcome: "refused", reason: "no_effect_proof_required" } as const;
      const phase = settlement.evidence.outcome;
      const original = receipt!;
      receipt = { ...original, phase, preparation: settlement.permit.preparation,
        events: [...original.events, { eventId: `settlement-${settlements.length}`,
          deliveryRevision: original.deliveryRevision, fenceRevision: original.fenceRevision,
          phase, transition: null, permit: settlement.permit, evidence: settlement.evidence }] };
      state.gate = phase === "acknowledged" ? clear : {
        outcome: "hold", fenceRevision: "1", reason: phase === "unknown" ? "delivery_unknown" : "recovery_required",
        deliveryIds: ["delivery-1"],
      };
      if (phase === "unknown") yield* Deferred.succeed(unknown, undefined);
      if (phase === "acknowledged") yield* Deferred.succeed(acknowledged, undefined);
      return { outcome: "settled", receipt, gate: state.gate } as const;
    }),
    checkContinuation: () => Effect.sync(() => {
      calls.push("continuation");
      return { currentOwner: state.absent ? null : owner, requirement: state.gate.outcome === "hold"
        ? { outcome: "hold", reason: state.gate.reason, gate: state.gate, current: null }
        : state.absent ? { outcome: "absent", gate: state.gate }
        : { outcome: "required", owner, target: { goalId: "goal", revision: "1", generation: "1", snapshotDigest: digest }, gate: state.gate } };
    }),
  };
  const recovery = makeCodexRecovery({
    context, boundary, scope,
    revalidate: Effect.suspend(() => state.current ? Effect.void
      : Effect.fail(new GoalContinuityError({ reason: "binding_mismatch" }))),
    inject: (text, permit) => Effect.gen(function* () {
      calls.push("inject");
      injected.push(text);
      yield* Deferred.succeed(sent, undefined);
      if (state.protocolNoEffect) return { outcome: "not_sent", proof: {
        type: "protocol_no_effect", rpcRequestId: permit.rpcRequestId, responseRef: "method-not-found",
      } } as const;
      if (waitForAck) yield* Deferred.await(reply);
      return { outcome: "acknowledged" } as const;
    }),
    stop: () => Effect.sync(() => {
      calls.push("stop");
      if (state.current) state.nativeStops++;
    }).pipe(Effect.andThen(Deferred.succeed(stopped, undefined)), Effect.asVoid),
  });
  return { state, recovery, context, calls, transitions, settlements, injected, sent, reply, stopped, unknown, acknowledged };
});

it.effect("fresh turn context never reuses the earlier checkpoint and rejects oversized UTF-8", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const first = yield* f.recovery.turnStartContext;
    f.state.projection = { outcome: "context", goalId: "goal", revision: "1", generation: "2",
      snapshotDigest: "b".repeat(64), text: "The newer persisted checkpoint" };
    const next = yield* f.recovery.turnStartContext;
    assert.notDeepEqual(first, next);
    assert.equal(next.outcome === "context" && next.generation, "2");
    f.state.projection = { outcome: "context", goalId: "goal", revision: "1", generation: "2",
      snapshotDigest: digest, text: "é".repeat(2049) };
    assert.equal((yield* Effect.exit(f.recovery.turnStartContext))._tag, "Failure");
    assert.deepEqual(f.injected, []);
  }).pipe(Effect.scoped),
);

it.effect("pending and unknown gates survive a missing current record and reject native continuation", () =>
  Effect.gen(function* () {
    for (const reason of ["delivery_pending", "delivery_unknown", "recovery_required"] as const) {
      const f = yield* fixture();
      f.state.absent = true;
      f.state.gate = { outcome: "hold", fenceRevision: "3", reason, deliveryIds: ["original-delivery"] };
      assert.isFalse(yield* f.recovery.checkContinuation);
      assert.equal((yield* Effect.exit(f.recovery.turnStartContext))._tag, "Failure");
      yield* Deferred.await(f.stopped);
      assert.isFalse(f.calls.includes("fresh"));
      assert.deepEqual(f.injected, []);
      f.state.gate = clear;
      assert.isFalse(yield* f.recovery.checkContinuation);
    }
  }).pipe(Effect.scoped),
);

it.effect("the notification reader returns before injection acknowledgement and sends only once", () =>
  Effect.gen(function* () {
    const f = yield* fixture(true);
    yield* f.recovery.transition(point, "started", true);
    // If the reader awaits injection this call cannot return to deliver reply.
    yield* f.recovery.transition(point, "completed", true);
    yield* Deferred.await(f.sent);
    assert.deepEqual(f.calls.slice(0, 6), ["record:started", "record:completed", "fresh", "prepare", "dispatching", "inject"]);
    assert.isFalse(yield* f.recovery.checkContinuation);
    yield* f.recovery.transition(point, "completed", true);
    assert.equal(f.injected.length, 1);
    yield* Deferred.succeed(f.reply, undefined);
    yield* Deferred.await(f.acknowledged);
    assert.equal(f.settlements[0]?.evidence.outcome, "acknowledged");
    assert.deepEqual(f.injected, ["Fresh untrusted checkpoint"]);
    assert.isFalse(f.calls.includes("turn/start"));
  }).pipe(Effect.scoped),
);

it.effect("unpaired, foreign-item and retired-producer completions cannot launch recovery", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* f.recovery.transition(point, "started", false);
    assert.equal((yield* Effect.exit(f.recovery.transition({ ...point, itemId: "foreign" }, "completed", true)))._tag, "Failure");
    assert.equal((yield* Effect.exit(f.recovery.transition({ ...point, nativeTurnId: "foreign" }, "completed", true)))._tag, "Failure");
    f.state.current = false;
    const before = f.transitions.length;
    assert.equal((yield* Effect.exit(f.recovery.transition(point, "completed", true)))._tag, "Failure");
    assert.equal(f.transitions.length, before);
    assert.deepEqual(f.injected, []);
  }).pipe(Effect.scoped),
);

it.effect("an uncertain dispatch commit sends nothing and duplicate completion cannot retry it", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    f.state.uncertainDispatch = true;
    yield* f.recovery.transition(point, "started", false);
    yield* f.recovery.transition(point, "completed", false);
    assert.equal((yield* Effect.exit(f.recovery.recoverCompleted))._tag, "Failure");
    yield* f.recovery.transition(point, "completed", true);
    assert.equal(f.calls.filter((call) => call === "dispatching").length, 1);
    assert.deepEqual(f.injected, []);
    assert.isFalse(yield* f.recovery.checkContinuation);
    assert.equal(f.state.nativeStops, 1);
  }).pipe(Effect.scoped),
);

it.effect("generation churn is bounded before preparation and never retries a dispatched permit", () =>
  Effect.gen(function* () {
    for (const stale of [1, 2]) {
      const f = yield* fixture();
      f.state.stalePreparations = stale;
      yield* f.recovery.transition(point, "started", false);
      yield* f.recovery.transition(point, "completed", false);
      const result = yield* Effect.exit(f.recovery.recoverCompleted);
      assert.equal(f.calls.filter((call) => call === "fresh").length, 2);
      assert.equal(f.injected.length, stale === 1 ? 1 : 0);
      assert.equal(result._tag, stale === 1 ? "Success" : "Failure");
    }
  }).pipe(Effect.scoped),
);

it.effect("replacement after permit commit sends nothing; rejected preclusion remains unknown without stopping the replacement", () =>
  Effect.gen(function* () {
    for (const rejected of [false, true]) {
      const f = yield* fixture();
      f.state.replaceAtDispatch = true;
      f.state.rejectPreclusion = rejected;
      yield* f.recovery.transition(point, "started", false);
      yield* f.recovery.transition(point, "completed", false);
      assert.equal((yield* Effect.exit(f.recovery.recoverCompleted))._tag, "Failure");
      assert.deepEqual(f.injected, []);
      assert.deepEqual(f.settlements.map((entry) => entry.evidence.outcome),
        rejected ? ["not_sent", "unknown"] : ["not_sent"]);
      assert.isFalse(yield* f.recovery.checkContinuation);
      assert.equal(f.state.nativeStops, 0);
    }
  }).pipe(Effect.scoped),
);

it.effect("a protocol no-effect response settles not_sent and retains recovery_required without retry", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    f.state.protocolNoEffect = true;
    yield* f.recovery.transition(point, "started", false);
    yield* f.recovery.transition(point, "completed", false);
    assert.equal((yield* Effect.exit(f.recovery.recoverCompleted))._tag, "Failure");
    assert.deepEqual(f.settlements.map((entry) => entry.evidence.outcome), ["not_sent"]);
    assert.equal(f.injected.length, 1);
    assert.equal(f.state.gate.outcome === "hold" && f.state.gate.reason, "recovery_required");
    assert.isFalse(yield* f.recovery.checkContinuation);
  }).pipe(Effect.scoped),
);

it.effect("timeout fences native continuation; a retired producer's late ack settles only its original permit", () =>
  Effect.gen(function* () {
    const f = yield* fixture(true);
    yield* f.recovery.transition(point, "started", true);
    yield* f.recovery.transition(point, "completed", true);
    yield* Deferred.await(f.sent);
    yield* TestClock.adjust("10 seconds");
    yield* Deferred.await(f.unknown);
    yield* Deferred.await(f.stopped);
    assert.isFalse(yield* f.recovery.checkContinuation);
    const original = f.settlements[0]!.permit;
    f.state.current = false;
    yield* Deferred.succeed(f.reply, undefined);
    yield* Deferred.await(f.acknowledged);
    assert.strictEqual(f.settlements.at(-1)!.permit, original);
    assert.deepEqual(f.settlements.map((entry) => entry.evidence.outcome), ["unknown", "acknowledged"]);
    assert.equal(f.injected.length, 1);
    assert.equal(f.state.nativeStops, 1);
    assert.isFalse(yield* f.recovery.checkContinuation);
  }).pipe(Effect.scoped),
);

it.effect("caller cancellation leaves the original waiter alive without retry or automatic resume", () =>
  Effect.gen(function* () {
    const f = yield* fixture(true);
    yield* f.recovery.transition(point, "started", false);
    yield* f.recovery.transition(point, "completed", false);
    const waiting = yield* f.recovery.recoverCompleted.pipe(Effect.forkChild);
    yield* Deferred.await(f.sent);
    yield* Fiber.interrupt(waiting);
    assert.isFalse(yield* f.recovery.checkContinuation);
    yield* Deferred.succeed(f.reply, undefined);
    yield* Deferred.await(f.acknowledged);
    assert.deepEqual(f.settlements.map((entry) => entry.evidence.outcome), ["unknown", "acknowledged"]);
    assert.equal(f.injected.length, 1);
    assert.equal(f.state.nativeStops, 1);
    assert.isFalse(yield* f.recovery.checkContinuation);
  }).pipe(Effect.scoped),
);

it.effect("handoff restoration uses its matched completion while an absent record preserves ordinary behavior", () =>
  Effect.gen(function* () {
    for (const absent of [false, true]) {
      const f = yield* fixture();
      f.state.absent = absent;
      yield* f.recovery.transition(point, "started", false);
      yield* f.recovery.transition(point, "completed", false);
      assert.deepEqual(f.injected, []);
      yield* f.recovery.recoverCompleted;
      assert.equal(f.injected.length, absent ? 0 : 1);
      assert.isTrue(yield* f.recovery.checkContinuation);
    }
  }).pipe(Effect.scoped),
);
