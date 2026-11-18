import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NodePath from "@effect/platform-node/NodePath";
import { CommandId, ThreadId, OrchestrationV2DomainEventJson } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../../persistence/Migrations.ts";
import { OrchestrationEffectRequestV2 } from "../../orchestration-v2/EffectOutbox.ts";
import {
  canonical,
  digest,
  preparationOriginId,
  type PreparationOrigin,
} from "./CheckoutProtocol.ts";
import { createLocalBridgeSchema } from "../cleanup/sharedAdmission/LocalBridge.ts";
import { decodedCommand, planDigests } from "./CheckoutPreparationSchema.ts";
import { OrdinaryCheckout } from "./OrdinaryCheckout.ts";
import * as Repository from "./OrdinaryCheckoutSqlite.ts";

const decodeEvent = Schema.decodeUnknownSync(OrchestrationV2DomainEventJson);
const decodeRequest = Schema.decodeUnknownSync(OrchestrationEffectRequestV2);
const now = "2026-11-16T00:00:00.000Z";
const database = Repository.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
  Layer.provideMerge(NodePath.layer),
);
// Retained-history fixtures exercise the real repository, not acceptance issuance.
// Positive EventSink/physical/runtime graph qualification belongs to its owners.
const fixture = Effect.gen(function* () {
  yield* runMigrations();
  yield* createLocalBridgeSchema;
  const sql = yield* SqlClient.SqlClient;
  const checkout = yield* OrdinaryCheckout;
  const thread = {
    id: "thread",
    projectId: "project",
    createdAt: now,
    updatedAt: now,
    deletedAt: null,
    branch: null,
    worktreePath: null,
  };
  const run = {
    id: "run",
    threadId: "thread",
    ordinal: 1,
    providerInstanceId: "codex",
    modelSelection: { instanceId: "codex", model: "synthetic" },
    providerThreadId: "provider",
    userMessageId: "message",
    rootNodeId: "node",
    activeAttemptId: "attempt",
    status: "preparing",
    requestedAt: now,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
    workspacePreparation: { type: "worktree", branch: "work" },
  };
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at)
    VALUES('project','Synthetic','/synthetic','[]',${now},${now})`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json)
    VALUES('thread','project','Synthetic','codex','full-access','default',${now},${now},${canonical(thread)})`;
  yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
    VALUES(1,'birth','thread','thread',1,'thread.created',${now},NULL,'client',${canonical(thread)},'{}',2)`;
  yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error)
    VALUES('original','thread','thread','message.dispatch',${now},2,'accepted',NULL)`;
  yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,provider_thread_id,status,requested_at,payload_json)
    VALUES('run','thread',1,'codex','provider','preparing',${now},${canonical(run)})`;
  yield* sql`INSERT INTO orchestration_v2_projection_run_attempts(attempt_id,thread_id,run_id,attempt_ordinal,root_node_id,provider,provider_thread_id,status,payload_json)
    VALUES('attempt','thread','run',1,'node','codex','provider','preparing','{}')`;
  const identity = {
    commandId: "original",
    subjectThreadId: "thread",
    birth: { eventId: "birth", sequence: 1 },
    runId: "run",
    attemptId: "attempt",
  };
  const origin: PreparationOrigin = {
    version: 1,
    ...identity,
    originId: preparationOriginId(identity),
    commandType: "message.dispatch",
    commandDigest: digest({ retained: "command" }),
    receiptThreadId: "thread",
    receiptSequence: 2,
    nodeId: "node",
    messageId: "message",
    providerThreadId: "provider",
    basisEventId: "pruned-basis",
    basisSequence: 2,
    authorityPlanDigest: digest([]),
    acceptedPlanDigest: digest([]),
    strategyJson: canonical(run.workspacePreparation),
    strategyDigest: digest(run.workspacePreparation),
    before: null,
    initial: {
      projectId: "project",
      threadId: "thread",
      projectRoot: "/synthetic",
      worktreePath: null,
      threadBranch: null,
      projectRevision: now,
      threadRevision: now,
    },
    classification: { kind: "prepared", parentThreadId: null, requestId: null },
    journalOperationKey: "shared-origin",
    journalBindingDigest: digest("binding"),
    recordedAt: now,
  };
  const certificate = {
    journalId: "journal",
    operationKey: origin.journalOperationKey,
    bindingSha256: origin.journalBindingDigest,
    receiptId: origin.commandId,
    authorityRevision: "origin",
  };
  yield* sql`INSERT INTO jones_common_admission_bridge(operation_key,certificate_json,certificate_sha256)
    VALUES(${certificate.operationKey},${canonical(certificate)},${digest(certificate)})`;
  yield* sql`INSERT INTO jones_ordinary_checkout_preparing_origins ${sql.insert({
    origin_id: origin.originId,
    command_id: origin.commandId,
    subject_thread_id: "thread",
    birth_event_id: "birth",
    run_id: "run",
    attempt_id: "attempt",
    record_json: canonical(origin),
  })}`;
  return { sql, checkout, origin };
});

it.effect(
  "retained preparation survives basis pruning but refuses receipt, attempt and birth drift",
  () =>
    Effect.gen(function* () {
      const { sql, checkout, origin } = yield* fixture;
      assert.deepStrictEqual(yield* checkout.readPreparationForRun("thread", "run"), origin);
      yield* checkout.validatePreparationOrigin(origin, "preparing");
      for (const mutation of [
        sql`UPDATE orchestration_command_receipts SET result_sequence=3 WHERE command_id='original'`,
        sql`UPDATE orchestration_v2_projection_runs SET payload_json=json_set(payload_json,'$.activeAttemptId','replacement') WHERE run_id='run'`,
        sql`DELETE FROM orchestration_events WHERE event_id='birth'`,
      ]) {
        const result = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* mutation;
              yield* checkout.validatePreparationOrigin(origin, "preparing");
            }),
          )
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(result));
        // A failed validation transaction leaves the original retained authority intact.
        yield* checkout.validatePreparationOrigin(origin, "preparing");
      }
    }).pipe(Effect.provide(database)),
);

it.effect("binding chain rejects forks and late failures roll back all certificate rows", () =>
  Effect.gen(function* () {
    const { sql, origin } = yield* fixture;
    const insert = (
      revision: number,
      command: string,
    ) => sql`INSERT INTO jones_ordinary_checkout_preparing_bindings
    (origin_id,binding_revision,command_id,record_json) VALUES(${origin.originId},${revision},${command},'{}')`;
    yield* insert(1, "metadata");
    const fork = yield* insert(1, "fork").pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(fork));
    const repeatedCommand = yield* insert(2, "metadata").pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(repeatedCommand));
    const late = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* insert(2, "rename");
          yield* sql`INSERT INTO jones_ordinary_checkout_preparing_releases VALUES(${origin.originId},'admission','release','effect','{}')`;
          return yield* Effect.fail("synthetic late failure");
        }),
      )
      .pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(late));
    assert.strictEqual(
      (yield* sql`SELECT * FROM jones_ordinary_checkout_preparing_bindings`).length,
      1,
    );
    assert.strictEqual(
      (yield* sql`SELECT * FROM jones_ordinary_checkout_preparing_releases`).length,
      0,
    );
  }).pipe(Effect.provide(database)),
);

it.effect(
  "plan equality masks only assigned turn-item ordinal, retaining batch membership and availability",
  () =>
    Effect.gen(function* () {
      const event = decodeEvent({
        id: "event",
        threadId: "thread",
        occurredAt: now,
        type: "turn-item.updated",
        payload: {
          id: "item",
          threadId: "thread",
          runId: "run",
          nodeId: "node",
          providerThreadId: "provider",
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 0,
          status: "completed",
          title: null,
          startedAt: null,
          completedAt: now,
          updatedAt: now,
          type: "reasoning",
          text: "retained content",
          streaming: false,
        },
      });
      const request = decodeRequest({
        type: "provider-turn.steer-batch",
        providerSessionId: "session",
        providerThreadId: "provider",
        providerTurnId: "turn",
        messageId: "one",
        messageIds: ["one", "two"],
      });
      const effect = {
        id: "effect",
        commandId: CommandId.make("command"),
        threadId: ThreadId.make("thread"),
        request,
      };
      const before = { events: [event], effects: [effect] };
      assert.strictEqual(event.type, "turn-item.updated");
      if (event.type !== "turn-item.updated") return;
      const assigned = { ...event, payload: { ...event.payload, ordinal: 4 } };
      assert.strictEqual(
        planDigests(before).authority,
        planDigests({ ...before, events: [assigned] }).authority,
      );
      assert.notStrictEqual(
        planDigests(before).actual,
        planDigests({ ...before, events: [assigned] }).actual,
      );
      assert.notStrictEqual(
        planDigests(before).authority,
        planDigests({ ...before, effects: [{ ...effect, availableAt: DateTime.makeUnsafe(now) }] })
          .authority,
      );
      if (request.type !== "provider-turn.steer-batch") return;
      assert.notStrictEqual(
        planDigests(before).authority,
        planDigests({
          ...before,
          effects: [
            { ...effect, request: { ...request, messageIds: [...request.messageIds].reverse() } },
          ],
        }).authority,
      );
      const original = yield* decodedCommand({
        type: "prepared-run.release",
        commandId: "release",
        threadId: "thread",
        runId: "run",
      });
      const changed = yield* decodedCommand({
        type: "prepared-run.release",
        commandId: "release",
        threadId: "thread",
        runId: "other",
      });
      assert.notStrictEqual(original.digest, changed.digest);
    }),
);
