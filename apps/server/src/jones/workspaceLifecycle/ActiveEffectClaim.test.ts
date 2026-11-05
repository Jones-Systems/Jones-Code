import { assert, it } from "@effect/vitest";
import { CommandId, ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import { CurrentEffectClaim, findEffectClaimLost, guardClaim, withActiveEffectClaim } from "./ActiveEffectClaim.ts";

const layer = Outbox.layer.pipe(Layer.provideMerge(Sqlite.layerMemory));
const fixture = Effect.gen(function* () {
  const outbox = yield* Outbox.EffectOutboxV2;
  yield* outbox.enqueue([{
    id: "effect:active-claim", commandId: CommandId.make("command:active-claim"),
    threadId: ThreadId.make("thread:active-claim"), request: { type: "terminal.cleanup" },
  }]);
  const captured = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 30_000 }));
  return { outbox, captured };
});

it.effect("short execution writes no renewal and a scoped handle cannot be reused after settlement", () =>
  Effect.gen(function* () {
    const { outbox, captured } = yield* fixture;
    let renewals = 0;
    const observed = { ...outbox, renew: (input: Parameters<typeof outbox.renew>[0]) => {
      renewals++;
      return outbox.renew(input);
    } };
    const handle = yield* withActiveEffectClaim(observed, captured, 30_000, true, (claim) => Effect.gen(function* () {
      yield* TestClock.adjust("9 seconds");
      assert.strictEqual(Option.getOrThrow(yield* outbox.get(captured.id)).leaseExpiresAt, captured.leaseExpiresAt);
      assert.isTrue(yield* claim.settle((current) => outbox.succeed({ effectId: current.id, workerId: "worker", claim: current })));
      return claim;
    }));
    yield* TestClock.adjust("1 minute");
    assert.strictEqual(renewals, 0);
    assert.strictEqual((yield* handle.use(() => Effect.void).pipe(Effect.flip))._tag, "EffectClaimLost");
  }).pipe(Effect.provide(layer)),
);

it.effect("unmanaged work keeps its existing completion path without a custody timer or expiry gate", () =>
  Effect.gen(function* () {
    const { outbox, captured } = yield* fixture;
    let renewals = 0;
    const observed = { ...outbox, renew: (input: Parameters<typeof outbox.renew>[0]) => {
      renewals++;
      return outbox.renew(input);
    } };
    yield* withActiveEffectClaim(observed, captured, 30_000, false, (claim) => Effect.gen(function* () {
      yield* TestClock.adjust("40 seconds");
      assert.strictEqual(yield* guardClaim(Effect.succeed("completed")), "completed");
      assert.isTrue(yield* claim.settle((current) => outbox.succeed({ effectId: current.id, workerId: "worker", claim: current })));
    }));
    assert.strictEqual(renewals, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("unknown renewal outcome fails the guarded wait once and fences every later boundary", () =>
  Effect.gen(function* () {
    const { outbox, captured } = yield* fixture;
    const entered = yield* Deferred.make<CurrentEffectClaim["Service"]>();
    let renewals = 0;
    let usedAfterLoss = false;
    const observed = { ...outbox, renew: () => {
      renewals++;
      return Effect.fail(new Outbox.EffectOutboxError({ operation: "renew", effectId: captured.id, cause: "Synthetic lost SQL reply" }));
    } };
    const fiber = yield* withActiveEffectClaim(observed, captured, 30_000, true, (claim) =>
      Deferred.succeed(entered, claim).pipe(Effect.andThen(guardClaim(Effect.never))),
    ).pipe(Effect.forkChild);
    const handle = yield* Deferred.await(entered);
    yield* TestClock.adjust("10 seconds");
    const result = yield* Fiber.await(fiber);
    assert.strictEqual(result._tag, "Failure");
    if (result._tag === "Failure") assert.isDefined(findEffectClaimLost(result.cause));
    yield* handle.use(() => Effect.sync(() => { usedAfterLoss = true; })).pipe(Effect.flip);
    yield* TestClock.adjust("1 minute");
    assert.strictEqual(renewals, 1);
    assert.isFalse(usedAfterLoss);
  }).pipe(Effect.provide(layer)),
);

it.effect("issue and renewal SQL share one claim permit while independent provider waits release it", () =>
  Effect.gen(function* () {
    const { outbox, captured } = yield* fixture;
    const issued = yield* Deferred.make<void>();
    const finishIssue = yield* Deferred.make<void>();
    const renewed = yield* Deferred.make<void>();
    const observed = { ...outbox, renew: (input: Parameters<typeof outbox.renew>[0]) =>
      outbox.renew(input).pipe(Effect.tap(() => Deferred.succeed(renewed, undefined))),
    };
    yield* withActiveEffectClaim(observed, captured, 30_000, true, (claim) => Effect.gen(function* () {
      const issue = yield* claim.use(() => Deferred.succeed(issued, undefined).pipe(
        Effect.andThen(Deferred.await(finishIssue)),
      )).pipe(Effect.forkChild);
      yield* Deferred.await(issued);
      yield* TestClock.adjust("10 seconds");
      assert.strictEqual(Option.getOrThrow(yield* outbox.get(captured.id)).leaseExpiresAt, captured.leaseExpiresAt);
      yield* Deferred.succeed(finishIssue, undefined);
      yield* Fiber.join(issue);
      yield* Deferred.await(renewed);
      const live = Option.getOrThrow(yield* outbox.get(captured.id));
      assert.notStrictEqual(live.leaseExpiresAt, captured.leaseExpiresAt);
      yield* claim.use((current) => Effect.sync(() => assert.strictEqual(current.leaseExpiresAt, live.leaseExpiresAt)));
      assert.isTrue(yield* claim.settle((current) => outbox.succeed({ effectId: current.id, workerId: "worker", claim: current })));
    }).pipe(Effect.ensuring(Deferred.succeed(finishIssue, undefined))));
  }).pipe(Effect.provide(layer)),
);

it.effect("cancellation invalidates admission before the next renewal or terminal success", () =>
  Effect.gen(function* () {
    const { outbox, captured } = yield* fixture;
    yield* withActiveEffectClaim(outbox, captured, 30_000, true, (claim) => Effect.gen(function* () {
      const cancelled = yield* outbox.cancelUnsettled({ threadId: captured.threadId, effectTypes: [captured.request.type], reason: "Synthetic cancellation" });
      yield* claim.invalidate();
      yield* outbox.signalCancellations(cancelled);
      yield* TestClock.adjust("40 seconds");
      assert.strictEqual((yield* claim.use(() => Effect.void).pipe(Effect.flip))._tag, "EffectClaimLost");
      assert.strictEqual((yield* claim.settle((current) => outbox.succeed({ effectId: current.id, workerId: "worker", claim: current })).pipe(Effect.flip))._tag, "EffectClaimLost");
      const row = Option.getOrThrow(yield* outbox.get(captured.id));
      assert.strictEqual(row.status, "cancelled");
      assert.isNull(row.leaseExpiresAt);
    }));
  }).pipe(Effect.provide(layer)),
);
