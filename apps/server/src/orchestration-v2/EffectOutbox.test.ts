import { assert, it } from "@effect/vitest";
import { CommandId, RunId, ThreadId } from "@t3tools/contracts";
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
  yield* outbox.enqueue([
    {
      id: "effect:renewal",
      commandId: CommandId.make("command:renewal"),
      threadId: ThreadId.make("thread:renewal"),
      request: { type: "terminal.cleanup" },
    },
  ]);
  const claimed = yield* outbox.claimNext({ workerId: "worker" });
  assert.isTrue(Option.isSome(claimed));
  if (Option.isNone(claimed)) return yield* Effect.die("Missing synthetic claim");
  return { sql, outbox, claim: claimed.value };
});

it.effect(
  "elapsed time preserves the original running generation and guarded completion settles once",
  () =>
    Effect.gen(function* () {
      const { outbox, claim } = yield* fixture;
      yield* TestClock.adjust("60 seconds");
      assert.deepEqual(yield* outbox.get(claim.id), Option.some(claim));
      assert.isTrue(Option.isNone(yield* outbox.claimNext({ workerId: "successor" })));
      assert.isTrue(yield* outbox.succeed({ effectId: claim.id, workerId: "worker", claim }));
      assert.isFalse(yield* outbox.succeed({ effectId: claim.id, workerId: "worker", claim }));
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "worker",
  "attempt",
  "command",
  "thread",
  "payload",
  "generation",
  "cancelled",
  "succeeded",
  "failed",
] as const)("captured settlement refuses changed %s without modifying the row", (change) =>
  Effect.gen(function* () {
    const { sql, outbox, claim } = yield* fixture;
    yield* TestClock.adjust("10 seconds");
    if (change === "worker")
      yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_owner='successor'`;
    if (change === "attempt") yield* sql`UPDATE orchestration_v2_effect_outbox SET attempt_count=2`;
    if (change === "command")
      yield* sql`UPDATE orchestration_v2_effect_outbox SET command_id='successor'`;
    if (change === "thread")
      yield* sql`UPDATE orchestration_v2_effect_outbox SET thread_id='successor'`;
    if (change === "payload")
      yield* sql`UPDATE orchestration_v2_effect_outbox SET payload_json='{"type":"preview.cleanup"}'`;
    if (change === "generation")
      yield* sql`UPDATE orchestration_v2_effect_outbox SET updated_at='2099-01-01T00:00:00.000Z'`;
    if (change === "cancelled" || change === "succeeded" || change === "failed")
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status=${change}`;
    const before = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
    for (const method of ["succeed", "retry", "fail"] as const)
      assert.isFalse(
        yield* outbox[method]({
          effectId: claim.id,
          workerId: "worker",
          claim,
          error: "Synthetic",
          delayMs: 0,
        }),
      );
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, before);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["succeed", "retry", "fail"] as const)(
  "guarded %s cannot settle a newer attempt with the same worker ID",
  (settle) =>
    Effect.gen(function* () {
      const { sql, outbox, claim } = yield* fixture;
      yield* sql`UPDATE orchestration_v2_effect_outbox SET attempt_count=2`;
      const before = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
      const input = {
        effectId: claim.id,
        workerId: "worker",
        claim,
        error: "Synthetic",
        delayMs: 0,
      };
      assert.isFalse(yield* outbox[settle](input));
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, before);
    }).pipe(Effect.provide(layer)),
);

it.effect("captured settlement cannot resurrect a completed claim", () =>
  Effect.gen(function* () {
    const { outbox, claim } = yield* fixture;
    assert.isTrue(yield* outbox.succeed({ effectId: claim.id, workerId: "worker", claim }));
    const before = yield* outbox.get(claim.id);
    yield* TestClock.adjust("10 seconds");
    for (const method of ["succeed", "retry", "fail"] as const)
      assert.isFalse(
        yield* outbox[method]({
          effectId: claim.id,
          workerId: "worker",
          claim,
          error: "Synthetic",
          delayMs: 0,
        }),
      );
    assert.deepEqual(yield* outbox.get(claim.id), before);
    assert.isTrue(Option.exists(before, (row) => row.status === "succeeded"));
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "a native envelope cannot complete only its outbox row or hide its native reference",
  () =>
    Effect.gen(function* () {
      const outbox = yield* Outbox.EffectOutboxV2;
      const sql = yield* SqlClient.SqlClient;
      const id = "effect:native-renewal";
      const commandId = CommandId.make("command:native-renewal");
      yield* outbox.enqueue([
        {
          id,
          commandId,
          threadId: ThreadId.make("thread:native-renewal"),
          request: { type: "provider-turn.start", runId: RunId.make("run:native-renewal") },
          nativeCreationExecutionReference: {
            version: 2,
            stage: "native_command",
            claimId: "claim:native-renewal",
            effectId: id,
            stageCommandId: commandId,
          },
        },
      ]);
      const claim = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker" }));
      yield* TestClock.adjust("10 seconds");
      const { nativeCreationExecutionReference: reference, ...withoutReference } = claim;
      assert.isDefined(reference);
      for (const candidate of [claim, withoutReference]) {
        assert.isFalse(
          yield* outbox.succeed({ effectId: id, workerId: "worker", claim: candidate }),
        );
        assert.deepEqual(yield* outbox.get(id), Option.some(claim));
      }
      assert.deepEqual(
        yield* sql`SELECT effect_id FROM jones_native_creation_execution_starts`,
        [],
      );
    }).pipe(Effect.provide(layer)),
);
