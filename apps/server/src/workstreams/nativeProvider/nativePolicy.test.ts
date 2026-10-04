import { assert, it } from "@effect/vitest";
import {
  CommandId, EventId, MessageId, NodeId, ProjectId, ProviderDriverKind,
  ProviderInstanceId, ProviderThreadId, RunAttemptId, RunId, ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import type { ProviderAdapterV2Shape } from "../../orchestration-v2/ProviderAdapter.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter: ProviderAdapterV2Shape = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("Settlement policy must not open a provider process"),
};
const database = SqlitePersistenceMemory;
const testLayer = Layer.mergeAll(
  database,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "native-settlement-policy" }, ProviderAdapterRegistry.makeLayer([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ),
);

it.effect("V2 policy denial retains pins, snooze and history and produces only a rejected receipt", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("native-policy-thread");
    const now = yield* DateTime.now;
    yield* orchestrator.dispatch({
      type: "thread.create", commandId: CommandId.make("native-policy-create"), threadId,
      projectId: ProjectId.make("native-policy-project"), title: "Native policy", modelSelection,
      runtimeMode: "full-access", interactionMode: "default", branch: null, worktreePath: null,
      createdBy: "user", creationSource: "web",
    });
    const thread = yield* projections.getThread(threadId);
    yield* projections.apply({
      id: EventId.make("native-policy-metadata"), type: "thread.metadata-updated", threadId, occurredAt: now,
      payload: { ...thread, pinnedAt: now, pinOrderKey: "synthetic-order", snoozedAt: now,
        snoozedUntil: DateTime.add(now, { days: 1 }) },
    });
    yield* projections.apply({
      id: EventId.make("native-policy-run"), type: "run.created", threadId, occurredAt: now,
      payload: {
        id: RunId.make("native-policy-run"), threadId, ordinal: 1, providerInstanceId: instanceId,
        modelSelection, providerThreadId: ProviderThreadId.make("native-policy-provider-thread"),
        userMessageId: MessageId.make("native-policy-message"), rootNodeId: NodeId.make("native-policy-node"),
        activeAttemptId: RunAttemptId.make("native-policy-attempt"), status: "running",
        requestedAt: now, startedAt: now, completedAt: null, checkpointId: null, contextHandoffId: null,
      },
    });
    const before = yield* projections.getThreadProjection(threadId);
    const eventsBefore = yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`;
    const commandId = CommandId.make("native-policy-settle");
    const outcome = yield* orchestrator.dispatch({ type: "thread.settle", commandId, threadId }).pipe(Effect.result);
    assert.equal(outcome._tag, "Failure");
    if (outcome._tag === "Failure") assert.equal(outcome.failure._tag, "OrchestratorDispatchError");
    assert.deepEqual(yield* projections.getThreadProjection(threadId), before);
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`, eventsBefore);
    assert.deepEqual(yield* sql`SELECT status FROM orchestration_command_receipts WHERE command_id = ${commandId}`,
      [{ status: "rejected" }]);
  }).pipe(Effect.provide(testLayer)),
);
