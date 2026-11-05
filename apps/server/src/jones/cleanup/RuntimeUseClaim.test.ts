import { assert, describe, it } from "@effect/vitest";
import { CommandId, RunId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import {
  currentRuntimeUseClaim,
  validateRuntimeUseClaim,
  withRuntimeUseClaim,
} from "./RuntimeUseClaim.ts";
import type { RuntimeUseOutboxClaim } from "./RuntimeUseProtocol.ts";

const layer = Outbox.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const claimed = Effect.gen(function* () {
  yield* runMigrations();
  const outbox = yield* Outbox.EffectOutboxV2;
  yield* outbox.enqueue([
    {
      id: "effect-original",
      commandId: CommandId.make("command-original"),
      threadId: ThreadId.make("thread-original"),
      request: { type: "provider-turn.start", runId: RunId.make("run-original") },
    },
  ]);
  const effect = yield* outbox.claimNext({ workerId: "worker-original" });
  assert.isTrue(Option.isSome(effect));
  if (Option.isNone(effect)) return yield* Effect.die("Expected actual claim");
  return effect.value;
});
const capture = (effect: Outbox.OrchestrationEffectV2) =>
  withRuntimeUseClaim(
    effect,
    "worker-original",
    Effect.gen(function* () {
      const current = yield* currentRuntimeUseClaim;
      if (Option.isNone(current)) return yield* Effect.die("Expected captured worker context");
      return current.value;
    }),
  );
const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });

describe("expected runtime-use outbox claim", () => {
  it.effect("captures the actual caller once and validates that exact running generation", () =>
    Effect.gen(function* () {
      const effect = yield* claimed;
      const expected = yield* capture(effect);
      assert.strictEqual(expected.effectId, "effect-original");
      assert.strictEqual(expected.workerId, "worker-original");
      assert.strictEqual(expected.attemptCount, 1);
      assert.strictEqual(expected.claimedAt, effect.updatedAt);
      assert.strictEqual(expected.runId, "run-original");
      assert.strictEqual(expected.attemptId, null);
      yield* validateRuntimeUseClaim(expected);
      assert.isTrue(Option.isNone(yield* currentRuntimeUseClaim));
    }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "never substitutes whichever new worker, attempt, command or request currently owns the row",
    () =>
      Effect.gen(function* () {
        const expected = yield* capture(yield* claimed);
        const sql = yield* SqlClient.SqlClient;
        const mutations = [
          sql`UPDATE orchestration_v2_effect_outbox SET lease_owner='other-worker'`,
          sql`UPDATE orchestration_v2_effect_outbox SET attempt_count=attempt_count+1`,
          sql`UPDATE orchestration_v2_effect_outbox SET updated_at='2040-01-01T00:00:00.000Z'`,
          sql`UPDATE orchestration_v2_effect_outbox SET command_id='other-command'`,
          sql`UPDATE orchestration_v2_effect_outbox SET thread_id='other-thread'`,
          sql`UPDATE orchestration_v2_effect_outbox SET payload_json='{"type":"provider-turn.start","runId":"other-run"}'`,
          sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded'`,
        ];
        for (const mutation of mutations) {
          // Roll back each deliberate drift, then re-prove the untouched original.
          yield* refused(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* mutation;
                yield* refused(validateRuntimeUseClaim(expected));
                return yield* Effect.fail("rollback synthetic drift");
              }),
            ),
          );
          yield* validateRuntimeUseClaim(expected);
        }
      }).pipe(Effect.provide(layer)),
  );
  it.effect("null run is genuinely runless and cannot wildcard an actual run or attempt", () =>
    Effect.gen(function* () {
      const expected = yield* capture(yield* claimed);
      yield* refused(validateRuntimeUseClaim({ ...expected, runId: null }));
      yield* refused(validateRuntimeUseClaim({ ...expected, attemptId: "unrelated-attempt" }));
      const outbox = yield* Outbox.EffectOutboxV2;
      yield* outbox.succeed({ effectId: expected.effectId, workerId: expected.workerId });
      yield* outbox.enqueue([
        {
          id: "runless",
          commandId: CommandId.make("runless-command"),
          threadId: ThreadId.make("runless-thread"),
          request: { type: "terminal.cleanup" },
        },
      ]);
      const next = yield* outbox.claimNext({ workerId: "worker-original" });
      if (Option.isNone(next)) return yield* Effect.die("Expected runless claim");
      const runless = yield* capture(next.value);
      assert.strictEqual(runless.runId, null);
      yield* validateRuntimeUseClaim(runless);
    }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "captured native references are immutable and cannot be fabricated into an ordinary claim",
    () =>
      Effect.gen(function* () {
        const expected = yield* capture(yield* claimed);
        const changed: RuntimeUseOutboxClaim = {
          ...expected,
          nativeReference: {
            version: 2,
            claimId: "foreign-claim",
            stageCommandId: CommandId.make("foreign-command"),
            effectId: expected.effectId,
            stage: "native_command",
          },
        };
        yield* refused(validateRuntimeUseClaim(changed));
        yield* validateRuntimeUseClaim(expected);
      }).pipe(Effect.provide(layer)),
  );
});
