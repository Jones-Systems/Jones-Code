import { assert, it } from "@effect/vitest";
import { CommandId, RunId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Outbox from "./EffectOutbox.ts";

const layer = Outbox.layer.pipe(Layer.provideMerge(Sqlite.layerMemory));
const fixture = Effect.gen(function* () {
  const outbox = yield* Outbox.EffectOutboxV2;
  const sql = yield* SqlClient.SqlClient;
  yield* outbox.enqueue([{
    id: "effect:renewal", commandId: CommandId.make("command:renewal"),
    threadId: ThreadId.make("thread:renewal"), request: { type: "terminal.cleanup" },
  }]);
  const claimed = yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 30_000 });
  assert.isTrue(Option.isSome(claimed));
  if (Option.isNone(claimed)) return yield* Effect.die("Missing synthetic claim");
  return { sql, outbox, claim: claimed.value };
});

it.effect("renewal CAS advances the same live claim and rejects a stale old-expiry callback", () =>
  Effect.gen(function* () {
    const { outbox, claim } = yield* fixture;
    yield* TestClock.adjust("10 seconds");
    const next = yield* outbox.renew({ claim, leaseDurationMs: 30_000 });
    assert.strictEqual(DateTime.toEpochMillis(DateTime.makeUnsafe(next)) -
      DateTime.toEpochMillis(DateTime.makeUnsafe(claim.leaseExpiresAt!)), 10_000);
    const current = yield* outbox.get(claim.id);
    assert.isTrue(Option.isSome(current));
    if (Option.isNone(current)) return;
    assert.strictEqual(current.value.attemptCount, claim.attemptCount);
    assert.strictEqual(current.value.payloadIdentity, claim.payloadIdentity);
    assert.strictEqual(current.value.leaseExpiresAt, next);
    const stale = yield* outbox.renew({ claim, leaseDurationMs: 30_000 }).pipe(Effect.flip);
    assert.strictEqual(stale._tag, "EffectClaimRenewalRefused");
    assert.deepEqual(yield* outbox.get(claim.id), current);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["worker", "attempt", "command", "thread", "payload", "expiry", "expired", "cancelled", "succeeded", "failed"] as const)(
  "renewal refuses changed %s without modifying the row",
  (change) => Effect.gen(function* () {
    const { sql, outbox, claim } = yield* fixture;
    yield* TestClock.adjust("10 seconds");
    if (change === "worker") yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_owner='successor'`;
    if (change === "attempt") yield* sql`UPDATE orchestration_v2_effect_outbox SET attempt_count=2`;
    if (change === "command") yield* sql`UPDATE orchestration_v2_effect_outbox SET command_id='successor'`;
    if (change === "thread") yield* sql`UPDATE orchestration_v2_effect_outbox SET thread_id='successor'`;
    if (change === "payload") yield* sql`UPDATE orchestration_v2_effect_outbox SET payload_json='{"type":"preview.cleanup"}'`;
    if (change === "expiry") yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at='2099-01-01T00:00:00.000Z'`;
    if (change === "expired") yield* TestClock.adjust("20 seconds");
    if (change === "cancelled" || change === "succeeded" || change === "failed")
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status=${change}`;
    const before = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
    assert.strictEqual((yield* outbox.renew({ claim, leaseDurationMs: 30_000 }).pipe(Effect.flip))._tag,
      "EffectClaimRenewalRefused");
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, before);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["succeed", "retry", "fail"] as const)(
  "guarded %s cannot settle a newer attempt with the same worker ID",
  (settle) => Effect.gen(function* () {
    const { sql, outbox, claim } = yield* fixture;
    yield* sql`UPDATE orchestration_v2_effect_outbox SET attempt_count=2`;
    const before = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
    const input = { effectId: claim.id, workerId: "worker", claim, error: "Synthetic", delayMs: 0 };
    assert.isFalse(yield* outbox[settle](input));
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, before);
  }).pipe(Effect.provide(layer)),
);

it.effect("renewal after terminal settlement cannot resurrect a completed claim", () =>
  Effect.gen(function* () {
    const { outbox, claim } = yield* fixture;
    assert.isTrue(yield* outbox.succeed({ effectId: claim.id, workerId: "worker", claim }));
    yield* TestClock.adjust("10 seconds");
    assert.strictEqual((yield* outbox.renew({ claim, leaseDurationMs: 30_000 }).pipe(Effect.flip))._tag,
      "EffectClaimRenewalRefused");
    assert.isTrue(Option.exists(yield* outbox.get(claim.id), (row) => row.status === "succeeded"));
  }).pipe(Effect.provide(layer)),
);

it.effect("a native envelope cannot renew only its outbox row or hide its native reference", () =>
  Effect.gen(function* () {
    const outbox = yield* Outbox.EffectOutboxV2;
    const sql = yield* SqlClient.SqlClient;
    const id = "effect:native-renewal";
    const commandId = CommandId.make("command:native-renewal");
    yield* outbox.enqueue([{
      id, commandId, threadId: ThreadId.make("thread:native-renewal"),
      request: { type: "provider-turn.start", runId: RunId.make("run:native-renewal") },
      nativeCreationExecutionReference: {
        version: 2, stage: "native_command", claimId: "claim:native-renewal",
        effectId: id, stageCommandId: commandId,
      },
    }]);
    const claim = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 30_000 }));
    yield* TestClock.adjust("10 seconds");
    const { nativeCreationExecutionReference: reference, ...withoutReference } = claim;
    assert.isDefined(reference);
    for (const candidate of [claim, withoutReference]) {
      assert.strictEqual((yield* outbox.renew({ claim: candidate, leaseDurationMs: 30_000 }).pipe(Effect.flip))._tag,
        "EffectClaimRenewalRefused");
      assert.deepEqual(yield* outbox.get(id), Option.some(claim));
    }
    assert.deepEqual(yield* sql`SELECT effect_id FROM jones_native_creation_execution_starts`, []);
  }).pipe(Effect.provide(layer)),
);
