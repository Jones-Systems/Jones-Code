import { assert, it } from "@effect/vitest";
import { CommandId, ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import {
  CurrentEffectClaim,
  findEffectClaimLost,
  guardClaim,
  withActiveEffectClaim,
} from "./ActiveEffectClaim.ts";

const layer = Outbox.layer.pipe(Layer.provideMerge(Sqlite.layerMemory));
const fixture = Effect.gen(function* () {
  const outbox = yield* Outbox.EffectOutboxV2;
  yield* outbox.enqueue([
    {
      id: "effect:active-claim",
      commandId: CommandId.make("command:active-claim"),
      threadId: ThreadId.make("thread:active-claim"),
      request: { type: "terminal.cleanup" },
    },
  ]);
  const captured = Option.getOrThrow(
    yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 30_000 }),
  );
  return { outbox, captured };
});

const heldReadFixture = Effect.gen(function* () {
  const { outbox, captured } = yield* fixture;
  const readCaptured = yield* Deferred.make<Option.Option<Outbox.OrchestrationEffectV2>>();
  const releaseRead = yield* Deferred.make<void>();
  let holdNextRead = true;
  const observed = {
    ...outbox,
    get: (effectId: string) =>
      Effect.gen(function* () {
        const current = yield* outbox.get(effectId);
        if (holdNextRead) {
          holdNextRead = false;
          yield* Deferred.succeed(readCaptured, current);
          yield* Deferred.await(releaseRead);
        }
        return current;
      }),
  };
  return { outbox, captured, observed, readCaptured, releaseRead };
});

it.effect(
  "short execution writes no renewal and a scoped handle cannot be reused after settlement",
  () =>
    Effect.gen(function* () {
      const { outbox, captured } = yield* fixture;
      let renewals = 0;
      const observed = {
        ...outbox,
        renew: (input: Parameters<typeof outbox.renew>[0]) => {
          renewals++;
          return outbox.renew(input);
        },
      };
      const handle = yield* withActiveEffectClaim(observed, captured, 30_000, true, (claim) =>
        Effect.gen(function* () {
          yield* TestClock.adjust("9 seconds");
          assert.strictEqual(
            Option.getOrThrow(yield* outbox.get(captured.id)).leaseExpiresAt,
            captured.leaseExpiresAt,
          );
          assert.isTrue(
            yield* claim.settle((current) =>
              outbox.succeed({ effectId: current.id, workerId: "worker", claim: current }),
            ),
          );
          return claim;
        }),
      );
      yield* TestClock.adjust("1 minute");
      assert.strictEqual(renewals, 0);
      assert.strictEqual(
        (yield* handle.use(() => Effect.void).pipe(Effect.flip))._tag,
        "EffectClaimLost",
      );
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "unmanaged work keeps its existing completion path without a custody timer or expiry gate",
  () =>
    Effect.gen(function* () {
      const { outbox, captured } = yield* fixture;
      let renewals = 0;
      const observed = {
        ...outbox,
        renew: (input: Parameters<typeof outbox.renew>[0]) => {
          renewals++;
          return outbox.renew(input);
        },
      };
      yield* withActiveEffectClaim(observed, captured, 30_000, false, (claim) =>
        Effect.gen(function* () {
          yield* TestClock.adjust("40 seconds");
          assert.strictEqual(yield* guardClaim(Effect.succeed("completed")), "completed");
          assert.isTrue(
            yield* claim.settle((current) =>
              outbox.succeed({ effectId: current.id, workerId: "worker", claim: current }),
            ),
          );
        }),
      );
      assert.strictEqual(renewals, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "unknown renewal outcome fails the guarded wait once and fences every later boundary",
  () =>
    Effect.gen(function* () {
      const { outbox, captured } = yield* fixture;
      const entered = yield* Deferred.make<CurrentEffectClaim["Service"]>();
      let renewals = 0;
      let usedAfterLoss = false;
      const observed = {
        ...outbox,
        renew: () => {
          renewals++;
          return Effect.fail(
            new Outbox.EffectOutboxError({
              operation: "renew",
              effectId: captured.id,
              cause: "Synthetic lost SQL reply",
            }),
          );
        },
      };
      const fiber = yield* withActiveEffectClaim(observed, captured, 30_000, true, (claim) =>
        Deferred.succeed(entered, claim).pipe(Effect.andThen(guardClaim(Effect.never))),
      ).pipe(Effect.forkChild);
      const handle = yield* Deferred.await(entered);
      yield* TestClock.adjust("10 seconds");
      const result = yield* Fiber.await(fiber);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.isDefined(findEffectClaimLost(result.cause));
      yield* handle
        .use(() =>
          Effect.sync(() => {
            usedAfterLoss = true;
          }),
        )
        .pipe(Effect.flip);
      yield* TestClock.adjust("1 minute");
      assert.strictEqual(renewals, 1);
      assert.isFalse(usedAfterLoss);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "issue and renewal SQL share one claim permit while independent provider waits release it",
  () =>
    Effect.gen(function* () {
      const { outbox, captured } = yield* fixture;
      const issued = yield* Deferred.make<void>();
      const finishIssue = yield* Deferred.make<void>();
      const childChecked = yield* Deferred.make<void>();
      const renewed = yield* Deferred.make<void>();
      const observed = {
        ...outbox,
        renew: (input: Parameters<typeof outbox.renew>[0]) =>
          outbox.renew(input).pipe(Effect.tap(() => Deferred.succeed(renewed, undefined))),
      };
      yield* withActiveEffectClaim(observed, captured, 30_000, true, (claim) =>
        Effect.gen(function* () {
          const issue = yield* claim
            .use(() =>
              Effect.gen(function* () {
                yield* claim.check;
                const child = yield* claim.check.pipe(
                  Effect.andThen(Deferred.succeed(childChecked, undefined)),
                  Effect.forkChild,
                );
                yield* Deferred.succeed(issued, undefined);
                yield* Deferred.await(finishIssue);
                return child;
              }),
            )
            .pipe(
              Effect.flatMap((child) => Fiber.join(child)),
              Effect.forkChild,
            );
          yield* Deferred.await(issued);
          yield* TestClock.adjust("10 seconds");
          assert.isFalse(yield* Deferred.isDone(childChecked));
          assert.strictEqual(
            Option.getOrThrow(yield* outbox.get(captured.id)).leaseExpiresAt,
            captured.leaseExpiresAt,
          );
          yield* Deferred.succeed(finishIssue, undefined);
          yield* Fiber.join(issue);
          yield* Deferred.await(renewed);
          const live = Option.getOrThrow(yield* outbox.get(captured.id));
          assert.notStrictEqual(live.leaseExpiresAt, captured.leaseExpiresAt);
          yield* claim.use((current) =>
            Effect.sync(() => assert.strictEqual(current.leaseExpiresAt, live.leaseExpiresAt)),
          );
          assert.isTrue(
            yield* claim.settle((current) =>
              outbox.succeed({ effectId: current.id, workerId: "worker", claim: current }),
            ),
          );
        }).pipe(Effect.ensuring(Deferred.succeed(finishIssue, undefined))),
      );
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "a public check inside claim SQL still rejects changed payload bytes and latches loss",
  () =>
    Effect.gen(function* () {
      const { outbox, captured } = yield* fixture;
      const sql = yield* SqlClient.SqlClient;
      yield* withActiveEffectClaim(outbox, captured, 30_000, true, (claim) =>
        Effect.gen(function* () {
          const changed = yield* claim.use(() =>
            sql.withTransaction(
              Effect.gen(function* () {
                yield* claim.check;
                yield* sql`UPDATE orchestration_v2_effect_outbox SET payload_json='{"type": "terminal.cleanup"}'
                WHERE effect_id=${captured.id}`;
                const current = Option.getOrThrow(yield* outbox.get(captured.id));
                assert.deepEqual(current.request, captured.request);
                assert.notStrictEqual(current.payloadIdentity, captured.payloadIdentity);
                const failure = yield* claim.check.pipe(Effect.flip);
                assert.strictEqual(failure._tag, "EffectClaimLost");
                assert.strictEqual(failure.effectId, captured.id);
                return current;
              }),
            ),
          );
          assert.isTrue(yield* claim.hasLost);
          assert.strictEqual((yield* claim.lost.pipe(Effect.flip)).effectId, captured.id);
          let writes = 0;
          assert.strictEqual(
            (yield* claim
              .use(() =>
                Effect.sync(() => {
                  writes++;
                }),
              )
              .pipe(Effect.flip))._tag,
            "EffectClaimLost",
          );
          assert.strictEqual(writes, 0);
          assert.strictEqual(
            (yield* claim
              .settle((current) =>
                outbox.succeed({ effectId: current.id, workerId: "worker", claim: current }),
              )
              .pipe(Effect.flip))._tag,
            "EffectClaimLost",
          );
          assert.deepEqual(Option.getOrThrow(yield* outbox.get(captured.id)), changed);
        }),
      );
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "a public check holding an old SQL snapshot retains custody across renewal and completes once",
  () =>
    Effect.gen(function* () {
      const { outbox, captured, observed, readCaptured, releaseRead } = yield* heldReadFixture;
      const started = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      const renewed = yield* Deferred.make<void>();
      let renewals = 0;
      let dispatches = 0;
      let completions = 0;
      const renewing = {
        ...observed,
        renew: (input: Parameters<typeof outbox.renew>[0]) => {
          renewals++;
          return outbox.renew(input).pipe(Effect.tap(() => Deferred.succeed(renewed, undefined)));
        },
      };
      yield* withActiveEffectClaim(renewing, captured, 30_000, true, (claim) =>
        Effect.gen(function* () {
          const operation = yield* guardClaim(
            Effect.gen(function* () {
              dispatches++;
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(finish);
              completions++;
              return "completed";
            }),
          ).pipe(Effect.forkChild);
          assert.deepEqual(Option.getOrThrow(yield* Deferred.await(readCaptured)), captured);
          yield* TestClock.adjust("10 seconds");
          assert.strictEqual(renewals, 0);
          assert.strictEqual(dispatches, 0);
          assert.strictEqual(
            Option.getOrThrow(yield* outbox.get(captured.id)).leaseExpiresAt,
            captured.leaseExpiresAt,
          );
          yield* Deferred.succeed(releaseRead, undefined);
          yield* Deferred.await(started);
          yield* Deferred.await(renewed);
          const live = Option.getOrThrow(yield* outbox.get(captured.id));
          assert.notStrictEqual(live.leaseExpiresAt, captured.leaseExpiresAt);
          yield* claim.use((current) =>
            Effect.sync(() => assert.strictEqual(current.leaseExpiresAt, live.leaseExpiresAt)),
          );
          assert.isFalse(yield* claim.hasLost);
          assert.strictEqual(completions, 0);
          yield* Deferred.succeed(finish, undefined);
          assert.strictEqual(yield* Fiber.join(operation), "completed");
          assert.strictEqual(renewals, 1);
          assert.strictEqual(dispatches, 1);
          assert.strictEqual(completions, 1);
          assert.isFalse(yield* claim.hasLost);
          assert.isTrue(
            yield* claim.settle((current) =>
              outbox.succeed({ effectId: current.id, workerId: "worker", claim: current }),
            ),
          );
        }).pipe(
          Effect.ensuring(
            Deferred.succeed(releaseRead, undefined).pipe(
              Effect.andThen(Deferred.succeed(finish, undefined)),
            ),
          ),
        ),
      );
      const settled = Option.getOrThrow(yield* outbox.get(captured.id));
      assert.strictEqual(settled.status, "succeeded");
      assert.strictEqual(settled.attemptCount, captured.attemptCount);
      assert.strictEqual(settled.commandId, captured.commandId);
      assert.strictEqual(settled.payloadIdentity, captured.payloadIdentity);
      assert.isNull(settled.leaseExpiresAt);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["worker", "attempt", "payload", "expiry"] as const)(
  "a held public check still rejects external %s changes when renewal is due",
  (change) =>
    Effect.gen(function* () {
      const { outbox, captured, observed, readCaptured, releaseRead } = yield* heldReadFixture;
      const sql = yield* SqlClient.SqlClient;
      if (change === "worker")
        yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_owner='successor'
          WHERE effect_id=${captured.id}`;
      if (change === "attempt")
        yield* sql`UPDATE orchestration_v2_effect_outbox SET attempt_count=${captured.attemptCount + 1}
          WHERE effect_id=${captured.id}`;
      if (change === "payload")
        yield* sql`UPDATE orchestration_v2_effect_outbox SET payload_json='{"type": "terminal.cleanup"}'
          WHERE effect_id=${captured.id}`;
      if (change === "expiry")
        yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at='2099-01-01T00:00:00.000Z'
          WHERE effect_id=${captured.id}`;
      const changed = yield* outbox.get(captured.id);
      if (change === "payload") {
        const current = Option.getOrThrow(changed);
        assert.deepEqual(current.request, captured.request);
        assert.notStrictEqual(current.payloadIdentity, captured.payloadIdentity);
      }
      let renewals = 0;
      let dispatches = 0;
      const renewing = {
        ...observed,
        renew: (input: Parameters<typeof outbox.renew>[0]) => {
          renewals++;
          return outbox.renew(input);
        },
      };
      yield* withActiveEffectClaim(renewing, captured, 30_000, true, (claim) =>
        Effect.gen(function* () {
          const operation = yield* guardClaim(
            Effect.sync(() => {
              dispatches++;
            }),
          ).pipe(Effect.forkChild);
          assert.deepEqual(yield* Deferred.await(readCaptured), changed);
          yield* TestClock.adjust("10 seconds");
          assert.strictEqual(renewals, 0);
          yield* Deferred.succeed(releaseRead, undefined);
          const result = yield* Fiber.await(operation);
          assert.strictEqual(result._tag, "Failure");
          if (result._tag === "Failure") assert.isDefined(findEffectClaimLost(result.cause));
          assert.isTrue(yield* claim.hasLost);
          assert.strictEqual((yield* claim.lost.pipe(Effect.flip)).effectId, captured.id);
          assert.strictEqual(
            (yield* claim
              .use(() =>
                Effect.sync(() => {
                  dispatches++;
                }),
              )
              .pipe(Effect.flip))._tag,
            "EffectClaimLost",
          );
          assert.strictEqual(
            (yield* claim
              .settle((current) =>
                outbox.succeed({ effectId: current.id, workerId: "worker", claim: current }),
              )
              .pipe(Effect.flip))._tag,
            "EffectClaimLost",
          );
          yield* TestClock.adjust("1 minute");
          assert.strictEqual(renewals, 0);
          assert.strictEqual(dispatches, 0);
          assert.deepEqual(yield* outbox.get(captured.id), changed);
        }).pipe(Effect.ensuring(Deferred.succeed(releaseRead, undefined))),
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("cancellation invalidates admission before the next renewal or terminal success", () =>
  Effect.gen(function* () {
    const { outbox, captured } = yield* fixture;
    yield* withActiveEffectClaim(outbox, captured, 30_000, true, (claim) =>
      Effect.gen(function* () {
        const cancelled = yield* outbox.cancelUnsettled({
          threadId: captured.threadId,
          effectTypes: [captured.request.type],
          reason: "Synthetic cancellation",
        });
        yield* claim.invalidate();
        yield* outbox.signalCancellations(cancelled);
        yield* TestClock.adjust("40 seconds");
        assert.strictEqual(
          (yield* claim.use(() => Effect.void).pipe(Effect.flip))._tag,
          "EffectClaimLost",
        );
        assert.strictEqual(
          (yield* claim
            .settle((current) =>
              outbox.succeed({ effectId: current.id, workerId: "worker", claim: current }),
            )
            .pipe(Effect.flip))._tag,
          "EffectClaimLost",
        );
        const row = Option.getOrThrow(yield* outbox.get(captured.id));
        assert.strictEqual(row.status, "cancelled");
        assert.isNull(row.leaseExpiresAt);
      }),
    );
  }).pipe(Effect.provide(layer)),
);
