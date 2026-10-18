import { assert, it } from "@effect/vitest";
import { CommandId, ProviderSessionId, RunId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import { NativeCreationExecutionReferenceV2 } from "./NativeCreationExecutionTypes.ts";

const threadId = ThreadId.make("thread:completion");
const commandId = CommandId.make("command:completion");
// Upstream decodes the whole payload as a request. Keep this independent of
// the Jones-aware decoder: it is what an ordinary T3 Code worker would run.
const upstreamDecode = Schema.decodeUnknownExit(
  Schema.fromJsonString(Outbox.OrchestrationEffectRequestV2),
);

const upstreamOnly = Outbox.layer.pipe(
  Layer.provideMerge(
    Layer.effectDiscard(runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] })),
  ),
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);

const reference = (effectId: string) =>
  Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)({
    version: 2,
    claimId: "claim:completion",
    stageCommandId: commandId,
    effectId,
    stage: "native_command",
  });

it.effect("ordinary succeed is one guarded UPDATE that reads no payload and no Jones table", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const outbox = yield* Outbox.EffectOutboxV2;
    assert.deepEqual(yield* sql`SELECT name FROM sqlite_master WHERE name LIKE 'jones_%'`, []);
    yield* outbox.enqueue([
      {
        id: "effect:ordinary:start",
        commandId,
        threadId,
        request: { type: "provider-turn.start", runId: RunId.make("run:ordinary") },
      },
      {
        id: "effect:ordinary:continue",
        commandId,
        threadId,
        request: {
          type: "provider-runtime.continue",
          sourceRunId: RunId.make("run:ordinary"),
          preparedForRestart: true,
        },
      },
    ]);
    // Lease rows as a worker claim would; claiming needs projection state this test omits.
    yield* sql`UPDATE orchestration_v2_effect_outbox SET status='running', attempt_count=1,
        lease_owner='worker', lease_expires_at='2099-01-01T00:00:00.000Z'`;
    for (const effectId of ["effect:ordinary:start", "effect:ordinary:continue"]) {
      const queries: Array<string> = [];
      const succeeded = yield* outbox.succeed({ effectId, workerId: "worker" }).pipe(
        Effect.provideService(Statement.CurrentTransformer, (statement) =>
          Effect.sync(() => {
            queries.push(statement.compile()[0]);
            return statement;
          }),
        ),
      );
      assert.isTrue(succeeded);
      assert.strictEqual(queries.length, 1);
      assert.match(queries[0]!.trim(), /^UPDATE orchestration_v2_effect_outbox/);
      assert.notInclude(queries[0]!, "jones_");
    }
    assert.deepEqual(
      yield* sql`SELECT effect_id, status FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
      [
        { effect_id: "effect:ordinary:continue", status: "succeeded" },
        { effect_id: "effect:ordinary:start", status: "succeeded" },
      ],
    );
  }).pipe(Effect.provide(upstreamOnly)),
);

it.effect(
  "upstream decoding rejects native envelopes for start and non-start effects, and recovery cancels a pending native start",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const outbox = yield* Outbox.EffectOutboxV2;
      yield* outbox.enqueue([
        {
          id: "effect:native:start",
          commandId,
          threadId,
          nativeCreationExecutionReference: yield* reference("effect:native:start"),
          request: { type: "provider-turn.start", runId: RunId.make("run:native") },
        },
        {
          id: "effect:native:detach",
          commandId,
          threadId,
          nativeCreationExecutionReference: yield* reference("effect:native:detach"),
          request: {
            type: "provider-session.detach",
            providerSessionId: ProviderSessionId.make("session:native"),
          },
        },
        {
          id: "effect:plain:start",
          commandId,
          threadId,
          request: { type: "provider-turn.start", runId: RunId.make("run:plain") },
        },
      ]);
      const payloads = yield* sql<{
        readonly effect_id: string;
        readonly payload_json: string;
      }>`SELECT effect_id, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
      assert.deepEqual(
        payloads.map((row) => [row.effect_id, Exit.isSuccess(upstreamDecode(row.payload_json))]),
        [
          ["effect:native:detach", false],
          ["effect:native:start", false],
          ["effect:plain:start", true],
        ],
      );

      assert.deepEqual(yield* outbox.reconcileAfterProcessLoss, { requeued: 0, cancelled: 2 });
      assert.deepEqual(
        yield* sql`SELECT effect_id, status FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        [
          { effect_id: "effect:native:detach", status: "pending" },
          { effect_id: "effect:native:start", status: "cancelled" },
          { effect_id: "effect:plain:start", status: "cancelled" },
        ],
      );
      assert.isFalse(
        yield* outbox.succeed({ effectId: "effect:native:start", workerId: "worker" }),
      );
    }).pipe(Effect.provide(upstreamOnly)),
);
