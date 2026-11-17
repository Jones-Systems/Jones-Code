import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthSessionId,
  CommandId,
  EnvironmentAuthenticatedPrincipal,
  MessageId,
  ProviderDriverKind,
  type ImportedHistoryStart,
} from "@t3tools/contracts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../../config.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import {
  delivery,
  principal,
  providerInstanceId,
  runId,
  seedExecutionIdentity,
  setup,
  setupWithoutQueuedDelivery,
  threadId,
} from "./ImportedHistoryFixture.testkit.ts";

const config = ServerConfig.layerTest(process.cwd(), {
  prefix: "jones-imported-management-",
}).pipe(Layer.provide(NodeServices.layer), Layer.orDie);
const adapter: ProviderAdapterV2Shape = {
  instanceId: providerInstanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("Imported management tests must not open a provider"),
};
const runtime = ProviderReplayHarness.layerWithRegistryAndServices(
  { name: "jones-imported-management", runtimePolicyOverride: { cwd: "/fixture" } },
  ProviderAdapterRegistry.layerFromAdapters([adapter]),
  {
    databaseLayer: SqlitePersistence.layerMemory,
    serverConfigLayer: config,
    runEffectWorker: false,
  },
);
const receiving = ThreadManagement.layer.pipe(
  Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory))),
  Layer.provideMerge(Layer.mergeAll(config, SqlitePersistence.layerMemory, runtime)),
);
const seed = setup.pipe(Effect.andThen(seedExecutionIdentity));
const reviewedCommand = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const review = yield* orchestrator.reviewImportedHistory({ threadId, delivery });
  assert.equal(review.status, "available");
  assert.isNotNull(review.reviewedBasis);
  return {
    type: "thread.imported-history.start",
    commandId: CommandId.make("command:management:reviewed"),
    threadId,
    delivery,
    reviewedBasis: review.reviewedBasis!,
  } satisfies ImportedHistoryStart;
});
const snapshot = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* EventSink.EventSinkV2;
  return {
    sequence: yield* sink.latestSequence(),
    thread: yield* sql`SELECT * FROM orchestration_v2_projection_threads ORDER BY thread_id`,
    runs: yield* sql`SELECT * FROM orchestration_v2_projection_runs ORDER BY run_id`,
    attempts:
      yield* sql`SELECT * FROM orchestration_v2_projection_run_attempts ORDER BY attempt_id`,
    nodes: yield* sql`SELECT * FROM orchestration_v2_projection_nodes ORDER BY node_id`,
    messages: yield* sql`SELECT * FROM orchestration_v2_projection_messages ORDER BY message_id`,
    providers:
      yield* sql`SELECT * FROM orchestration_v2_projection_provider_threads ORDER BY provider_thread_id`,
    handoffs:
      yield* sql`SELECT * FROM orchestration_v2_projection_context_handoffs ORDER BY context_handoff_id`,
    scopes:
      yield* sql`SELECT * FROM orchestration_v2_projection_checkpoint_scopes ORDER BY scope_id`,
    items: yield* sql`SELECT * FROM orchestration_v2_projection_turn_items ORDER BY turn_item_id`,
    choices: yield* sql`SELECT * FROM jones_imported_history_choices ORDER BY command_id`,
    outcomes: yield* sql`SELECT * FROM jones_imported_history_outcomes ORDER BY command_id`,
    receipts: yield* sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
    reservations:
      yield* sql`SELECT * FROM jones_imported_history_start_reservations ORDER BY effect_id`,
    executionStarts:
      yield* sql`SELECT * FROM jones_imported_history_execution_starts ORDER BY effect_id`,
    effects: yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
  };
});
const provideReceiving = Effect.fn("ImportedHistoryManagement.test.provideReceiving")(function <
  A,
  E,
  R,
>(effect: Effect.Effect<A, E, R>) {
  return effect.pipe(
    Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
    Effect.provide(receiving.pipe(Layer.provideMerge(NodeServices.layer))),
  );
});

it.effect(
  "management planner and sink roll back together when durable outcome insertion fails",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sql = yield* SqlClient.SqlClient;
      const command = yield* reviewedCommand;
      const before = yield* snapshot;
      yield* sql`CREATE TRIGGER refuse_imported_outcome BEFORE INSERT ON jones_imported_history_outcomes
      BEGIN SELECT RAISE(ABORT, 'synthetic outcome refusal'); END`;
      const failure = yield* orchestrator.startWithImportedHistory(command).pipe(Effect.flip);
      assert.equal(failure._tag, "OrchestratorProjectionError");
      assert.deepEqual(yield* snapshot, before);
      yield* sql`DROP TRIGGER refuse_imported_outcome`;
      const [accepted, concurrentReplay] = yield* Effect.all(
        [
          orchestrator.startWithImportedHistory(command),
          orchestrator.startWithImportedHistory(command),
        ],
        { concurrency: 2 },
      );
      assert.deepEqual(concurrentReplay, accepted);
      assert.equal(accepted.status, "accepted");
      assert.equal(accepted.runId, runId);
    }).pipe(provideReceiving),
);

it.effect(
  "exact replay and observation preserve acceptance; changed command and actor cannot reuse it",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sql = yield* SqlClient.SqlClient;
      const command = yield* reviewedCommand;
      const accepted = yield* orchestrator.startWithImportedHistory(command);
      assert.equal(accepted.status, "accepted");
      const before = yield* snapshot;
      assert.deepEqual(yield* orchestrator.startWithImportedHistory(command), accepted);
      assert.deepEqual(yield* orchestrator.observeImportedHistoryStart(command), accepted);
      const conflict = yield* orchestrator
        .startWithImportedHistory({
          ...command,
          reviewedBasis: "0".repeat(64),
        })
        .pipe(Effect.flip);
      assert.equal(conflict._tag, "OrchestratorProjectionError");
      const other = { ...principal, sessionId: AuthSessionId.make("session:management:other") };
      yield* sql`INSERT INTO auth_sessions(session_id,subject,scopes,method,issued_at,expires_at)
      SELECT ${other.sessionId},subject,scopes,method,issued_at,expires_at FROM auth_sessions
      WHERE session_id=${principal.sessionId}`;
      const actorConflict = yield* orchestrator
        .observeImportedHistoryStart(command)
        .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, other), Effect.flip);
      assert.equal(actorConflict._tag, "OrchestratorProjectionError");
      assert.deepEqual(yield* snapshot, before);
      const missing = yield* orchestrator.observeImportedHistoryStart({
        threadId,
        commandId: CommandId.make("command:management:missing"),
      });
      assert.isNull(missing);
    }).pipe(provideReceiving),
);

it.effect("stale reviewed basis records one durable rejection without starting held work", () =>
  Effect.gen(function* () {
    yield* seed;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const reviewed = yield* reviewedCommand;
    const command = { ...reviewed, reviewedBasis: "0".repeat(64) };
    const sequence = yield* sink.latestSequence();
    const rejected = yield* orchestrator.startWithImportedHistory(command);
    assert.equal(rejected.status, "rejected");
    assert.equal(rejected.reason, "review_changed");
    assert.deepEqual(yield* orchestrator.startWithImportedHistory(command), rejected);
    assert.equal(yield* sink.latestSequence(), sequence);
    const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(threadId, [
      "runs",
    ]);
    assert.equal(records.runs.find((run) => run.id === runId)?.status, "queued");
    assert.isTrue(records.runs.find((run) => run.id === runId)?.queueHeld);
    assert.lengthOf(
      yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id=${command.commandId} AND status='rejected'`,
      1,
    );
    assert.lengthOf(
      yield* sql`SELECT * FROM jones_imported_history_outcomes WHERE command_id=${command.commandId}`,
      1,
    );
    assert.isEmpty(
      yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE command_id=${command.commandId}`,
    );
    assert.isEmpty(
      yield* sql`SELECT * FROM jones_imported_history_start_reservations WHERE command_id=${command.commandId}`,
    );
  }).pipe(provideReceiving),
);

it.effect(
  "observing an execution claim requires reconciliation and never starts a second effect",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const command = yield* reviewedCommand;
      const accepted = yield* orchestrator.startWithImportedHistory(command);
      assert.equal(accepted.status, "accepted");
      assert.isDefined(sink.assertImportedHistoryStartAllowed);
      const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(threadId, [
        "runs",
      ]);
      const started = records.runs.find((run) => run.id === accepted.runId);
      assert.isDefined(started);
      assert.isNotNull(started!.providerThreadId);
      yield* sink.assertImportedHistoryStartAllowed!({
        threadId,
        runId: started!.id,
        providerThreadId: started!.providerThreadId!,
      });
      const before = yield* snapshot;
      const observed = yield* orchestrator.observeImportedHistoryStart(command);
      assert.equal(observed?.status, "unknown");
      assert.equal(observed?.reason, "execution_requires_reconciliation");
      assert.deepEqual(yield* orchestrator.startWithImportedHistory(command), observed);
      assert.deepEqual(yield* snapshot, before);
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE auth_sessions SET revoked_at='2026-10-07T01:00:00Z' WHERE session_id=${principal.sessionId}`;
      const revoked = yield* orchestrator.observeImportedHistoryStart(command).pipe(Effect.flip);
      assert.equal(revoked._tag, "OrchestratorProjectionError");
      assert.deepEqual(yield* snapshot, before);
    }).pipe(provideReceiving),
);

it.effect("reviewed message uses the current decider and binds its real fresh-context start", () =>
  Effect.gen(function* () {
    yield* setupWithoutQueuedDelivery;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const sql = yield* SqlClient.SqlClient;
    const commandId = CommandId.make("command:management:message");
    const messageId = MessageId.make("message:management:message");
    const messageDelivery = {
      type: "message",
      command: {
        type: "message.dispatch",
        commandId,
        threadId,
        messageId,
        text: "Use the reviewed imported transcript",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
        dispatchMode: { type: "start_immediately" },
      },
    } satisfies ImportedHistoryStart["delivery"];
    const review = yield* threads.reviewImportedHistory({ threadId, delivery: messageDelivery });
    assert.equal(review.status, "available");
    assert.isNotNull(review.reviewedBasis);
    const command: ImportedHistoryStart = {
      type: "thread.imported-history.start",
      commandId,
      threadId,
      delivery: messageDelivery,
      reviewedBasis: review.reviewedBasis!,
    };
    const before = yield* snapshot;
    const identityConflict = yield* threads
      .startWithImportedHistory({
        ...command,
        delivery: {
          ...messageDelivery,
          command: {
            ...messageDelivery.command,
            commandId: CommandId.make("command:management:different-message"),
          },
        },
      })
      .pipe(Effect.flip);
    assert.equal(identityConflict._tag, "OrchestratorProjectionError");
    assert.deepEqual(yield* snapshot, before);
    const accepted = yield* threads.startWithImportedHistory(command);
    assert.equal(accepted.status, "accepted");
    const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(threadId, [
      "runs",
      "contextHandoffs",
      "providerThreads",
    ]);
    const started = records.runs.find((run) => run.id === accepted.runId);
    assert.equal(started?.userMessageId, messageId);
    assert.equal(started?.status, "starting");
    assert.isNotNull(started?.contextHandoffId);
    assert.include(
      records.contextHandoffs.find((handoff) => handoff.id === started?.contextHandoffId)
        ?.summaryText ?? "",
      "Imported transcript",
    );
    assert.isNull(
      records.providerThreads.find((provider) => provider.id === started?.providerThreadId)
        ?.nativeThreadRef,
    );
    assert.lengthOf(
      yield* sql`SELECT * FROM jones_imported_history_start_reservations WHERE command_id=${commandId}`,
      1,
    );
    assert.deepEqual(yield* threads.startWithImportedHistory(command), accepted);
    assert.deepEqual(yield* threads.observeImportedHistoryStart(command), accepted);
    assert.isEmpty(yield* sql`SELECT * FROM jones_imported_history_execution_starts`);
  }).pipe(provideReceiving),
);
