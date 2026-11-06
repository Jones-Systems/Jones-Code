import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderSessionId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2RunAttempt,
} from "@t3tools/contracts";
import { unobservedRuntimeIdentity } from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const database = SqlitePersistence.layerMemory;
const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(database),
);
const testLayer = Layer.mergeAll(stores, EventSink.layer.pipe(Layer.provide(stores)));

it.effect(
  "atomically publishes attributed settlement and checkpoint effects only for the current run attempt",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const startedAt = DateTime.makeUnsafe("2026-09-01T12:00:00Z");
      const terminalAt = DateTime.makeUnsafe("2026-09-01T12:00:05Z");
      const threadId = ThreadId.make("thread:atomic-settlement");
      const runId = RunId.make("run:atomic-settlement");
      const attemptId = RunAttemptId.make("attempt:atomic-settlement");
      const nodeId = NodeId.make("node:atomic-settlement");
      const providerInstanceId = ProviderInstanceId.make("codex");
      const providerThreadId = ProviderThreadId.make("provider-thread:atomic-settlement");
      const modelSelection = { instanceId: providerInstanceId, model: "test-model" };
      const attempt: OrchestrationV2RunAttempt = {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId,
        providerThreadId,
        providerTurnId: null,
        providerSettlement: null,
        reason: "initial",
        status: "running",
        startedAt,
        completedAt: null,
      };
      const run = {
        id: runId,
        threadId,
        ordinal: 1,
        providerInstanceId,
        modelSelection,
        providerThreadId,
        userMessageId: MessageId.make("message:atomic-settlement"),
        rootNodeId: nodeId,
        activeAttemptId: attemptId,
        status: "running" as const,
        requestedAt: startedAt,
        startedAt,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      yield* sink.write({
        events: [
          {
            id: EventId.make("event:atomic:thread"),
            type: "thread.created",
            threadId,
            occurredAt: startedAt,
            payload: {
              createdBy: "user",
              creationSource: "web",
              id: threadId,
              projectId: ProjectId.make("project:atomic"),
              title: "Synthetic settlement",
              providerInstanceId,
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              activeProviderThreadId: null,
              lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
              forkedFrom: null,
              createdAt: startedAt,
              updatedAt: startedAt,
              archivedAt: null,
              settledOverride: null,
              settledAt: null,
              lastVisitedAt: null,
              deletedAt: null,
            },
          },
          {
            id: EventId.make("event:atomic:run"),
            type: "run.created",
            threadId,
            runId,
            occurredAt: startedAt,
            payload: run,
          },
          {
            id: EventId.make("event:atomic:attempt"),
            type: "run-attempt.created",
            threadId,
            runId,
            occurredAt: startedAt,
            payload: attempt,
          },
        ],
      });
      const settlement = {
        runAttemptId: attemptId,
        providerTurnId: ProviderTurnId.make("provider-turn:atomic"),
        status: "completed" as const,
        completedAt: terminalAt,
      };
      const events: ReadonlyArray<OrchestrationV2DomainEvent> = [
        {
          id: EventId.make("event:atomic:terminal"),
          type: "run-attempt.updated",
          threadId,
          runId,
          occurredAt: terminalAt,
          payload: {
            ...attempt,
            status: "completed",
            completedAt: terminalAt,
            providerTurnId: settlement.providerTurnId,
            providerSettlement: settlement,
          },
        },
        {
          id: EventId.make("event:atomic:waiting"),
          type: "run.updated",
          threadId,
          runId,
          occurredAt: terminalAt,
          payload: { ...run, status: "waiting" },
        },
      ];
      const effects = [
        {
          id: "effect:atomic:checkpoint",
          commandId: CommandId.make("command:atomic:checkpoint"),
          threadId,
          request: {
            type: "checkpoint.capture" as const,
            runId,
            scopeId: CheckpointScopeId.make("scope:atomic"),
          },
        },
      ];
      const historical = { ...attempt };
      delete (historical as { providerSettlement?: unknown }).providerSettlement;
      for (const version of [historical, attempt]) {
        yield* sink.write({
          events: [
            {
              id: EventId.make(
                `event:atomic:compat:${Object.hasOwn(version, "providerSettlement")}`,
              ),
              type: "run-attempt.updated",
              threadId,
              runId,
              occurredAt: startedAt,
              payload: version,
            },
          ],
        });
        const shell = (yield* projections.getShellSnapshot()).threads[0]!;
        const targeted = yield* projections.getThreadShell(threadId);
        const detail = yield* projections.getThreadSnapshot(threadId);
        assert.equal(
          Object.hasOwn(shell, "latestRunProviderSettlement"),
          Object.hasOwn(version, "providerSettlement"),
        );
        assert.deepEqual(targeted?.latestRunProviderSettlement, version.providerSettlement);
        assert.deepEqual(
          detail.projection.attempts[0]?.providerSettlement,
          version.providerSettlement,
        );
      }
      const before = yield* sink.latestSequence();
      for (const guard of [
        {
          threadId: ThreadId.make("wrong-thread"),
          activeAttemptId: attemptId,
          expectedStatus: "running" as const,
        },
        {
          threadId,
          activeAttemptId: RunAttemptId.make("superseded-attempt"),
          expectedStatus: "running" as const,
        },
        { threadId, activeAttemptId: attemptId, expectedStatus: "completed" as const },
      ]) {
        assert.isFalse(
          (yield* sink.writeIfRunCurrent({ ...guard, runId, events, effects })).committed,
        );
        assert.equal(yield* sink.latestSequence(), before);
        assert.equal((yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`).length, 0);
        assert.isNull(
          (yield* projections.getThreadRecords(threadId, ["runs", "attempts"])).attempts[0]
            ?.providerSettlement,
        );
      }
      const committed = yield* sink.writeIfRunCurrent({
        threadId,
        runId,
        activeAttemptId: attemptId,
        expectedStatus: "running",
        events,
        effects,
      });
      assert.isTrue(committed.committed);
      assert.equal(committed.storedEvents.length, 2);
      assert.equal((yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`).length, 1);
      const detail = yield* projections.getThreadRecords(threadId, ["runs", "attempts"]);
      assert.deepEqual(detail.attempts[0]?.providerSettlement, settlement);
      assert.equal(detail.runs[0]?.status, "waiting");
      assert.isNull(detail.runs[0]?.completedAt);
      const shell = yield* projections.getShellSnapshot();
      assert.deepEqual(shell.threads[0]?.latestRunProviderSettlement, settlement);
      assert.deepEqual(
        (yield* projections.getThreadShell(threadId))?.latestRunProviderSettlement,
        settlement,
      );
      assert.deepEqual(
        (yield* projections.getThreadSnapshot(threadId)).projection.attempts[0]?.providerSettlement,
        settlement,
      );
      const after = yield* sink.latestSequence();
      assert.isFalse(
        (yield* sink.writeIfRunCurrent({
          threadId,
          runId,
          activeAttemptId: attemptId,
          expectedStatus: "running",
          events,
          effects,
        })).committed,
      );
      assert.equal(yield* sink.latestSequence(), after);
    }).pipe(Effect.provide(testLayer)),
);


it.effect.each(["exact", "changed-session", "changed-native", "installed-generation"] as const)(
  "stopped-runtime replacement atomically revalidates the old identity: %s", (mode) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make(`stop-identity:${mode}`);
    const providerThreadId = ProviderThreadId.make(`stop-provider:${mode}`);
    const instanceId = ProviderInstanceId.make("codex");
    const driver = ProviderDriverKind.make("codex");
    const runId = RunId.make(`stop-run:${mode}`);
    const attemptId = RunAttemptId.make(`stop-attempt:${mode}`);
    const provider: OrchestrationV2ProviderThread = {
      id: providerThreadId, appThreadId: threadId, driver, providerInstanceId: instanceId,
      providerSessionId: ProviderSessionId.make("stopped-session"), ownerNodeId: null,
      nativeThreadRef: { driver, nativeId: "native-original", strength: "strong" },
      nativeConversationHeadRef: null, status: "idle", firstRunOrdinal: null, lastRunOrdinal: null,
      handoffIds: [], forkedFrom: null, createdAt: now, updatedAt: now,
    };
    const run: OrchestrationV2Run = {
      id: runId, threadId, ordinal: 1, providerInstanceId: instanceId,
      modelSelection: { instanceId, model: "synthetic" }, providerThreadId,
      userMessageId: MessageId.make("stop-message"), rootNodeId: NodeId.make("stop-node"),
      activeAttemptId: attemptId, status: "starting", requestedAt: now, startedAt: null,
      completedAt: null, checkpointId: null, contextHandoffId: null,
    };
    yield* sink.write({ events: [
      { id: EventId.make(`${mode}:birth`), type: "thread.created", threadId, occurredAt: now,
        payload: {
          createdBy: "user", creationSource: "web",
          id: threadId, projectId: ProjectId.make("stop-project"), title: "Stopped replacement",
          providerInstanceId: instanceId, modelSelection: run.modelSelection,
          runtimeMode: "full-access", interactionMode: "default", branch: null, worktreePath: null,
          activeProviderThreadId: providerThreadId,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null, createdAt: now, updatedAt: now, archivedAt: null, settledOverride: null,
          settledAt: null, lastVisitedAt: null, deletedAt: null,
        } },
      { id: EventId.make(`${mode}:provider`), type: "provider-thread.updated", threadId, occurredAt: now, payload: provider },
      { id: EventId.make(`${mode}:run`), type: "run.updated", threadId, runId, occurredAt: now, payload: run },
    ] });
    const changed = {
      ...provider,
      ...(mode === "changed-session" ? { providerSessionId: ProviderSessionId.make("foreign-session") } : {}),
      ...(mode === "changed-native" ? { nativeThreadRef: { ...provider.nativeThreadRef!, nativeId: "foreign-native" } } : {}),
      ...(mode === "installed-generation" ? { runtimeIdentity: {
        runtimeGeneration: "newly-installed", requested: { providerInstanceId: instanceId, providerDriver: driver, model: "synthetic", serviceTier: null },
        observed: unobservedRuntimeIdentity(),
      } } : {}),
    };
    yield* sink.write({ events: [{ id: EventId.make(`${mode}:concurrent`), type: "provider-thread.updated", threadId, occurredAt: now, payload: changed }] });
    const before = yield* sink.latestSequence({ threadId });
    const result = yield* sink.writeIfRunCurrent({
      threadId, runId, activeAttemptId: attemptId, expectedStatus: "starting",
      runtimeIdentityBoundary: {
        expectedGeneration: null, expectedActiveProviderThreadId: providerThreadId,
        expectedProviderThreadIdentity: {
          id: provider.id, appThreadId: provider.appThreadId, driver: provider.driver,
          providerInstanceId: provider.providerInstanceId, providerSessionId: provider.providerSessionId,
          nativeThreadRef: provider.nativeThreadRef,
        },
      },
      events: [
        { id: EventId.make(`${mode}:replacement`), type: "provider-thread.updated", threadId, occurredAt: now,
          payload: { ...provider, providerSessionId: ProviderSessionId.make("fresh-session") } },
        { id: EventId.make(`${mode}:running`), type: "run.updated", threadId, runId, occurredAt: now, payload: { ...run, status: "running", startedAt: now } },
      ],
    });
    assert.equal(result.committed, mode === "exact");
    if (mode !== "exact") {
      assert.equal(yield* sink.latestSequence({ threadId }), before);
      assert.equal((yield* projections.getThreadRecords(threadId, ["runs"])).runs[0]!.status, "starting");
    }
  }).pipe(Effect.provide(testLayer)),
);
