import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  EventId,
  NodeId,
  PlanId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  type OrchestrationV2ServerCommand,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import {
  SqlitePersistenceMemory,
  makeSqlitePersistenceLive,
} from "../persistence/Layers/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ThreadMetadataMcpService from "../mcp/ThreadMetadataMcpService.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for metadata controls"),
} as ProviderAdapterV2Shape;
const database = SqlitePersistenceMemory;
const makeTestLayer = (projectId: ProjectId) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const workspaceRoot = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-control-reads-workspace-",
      });
      return Layer.mergeAll(
        database,
        ProjectionStore.layer.pipe(Layer.provide(database)),
        makeOrchestratorV2ReplayLayerWithRegistry(
          { name: "control-reads" },
          ProviderAdapterRegistry.makeLayer([adapter]),
          {
            databaseLayer: database,
            runEffectWorker: false,
            checkoutFixture: {
              projects: [{ projectId, title: "Control reads", workspaceRoot }],
              resolvePath: () => undefined,
            },
          },
        ),
      );
    }),
  ).pipe(Layer.provide(NodeServices.layer));

it.effect(
  "dispatches metadata, queue resume and request controls without hydrating unrelated history",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread:control-dispatch");
      const now = yield* DateTime.now;
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create-control"),
        threadId,
        projectId: ProjectId.make("project:control-dispatch"),
        title: "Before",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      for (const enabled of [false, true]) {
        yield* orchestrator.dispatch({
          type: "thread.auto-settle.set",
          commandId: CommandId.make(`auto-settle-${enabled}`),
          threadId,
          enabled,
        });
        const updated = yield* projections.getThreadProjection(threadId);
        assert.equal(updated.thread.autoSettleDisabledAt == null, enabled);
        const shell = yield* projections.getThreadShell(threadId);
        assert.ok(shell);
        assert.equal(shell.autoSettleDisabledAt == null, enabled);
      }
      yield* sql`INSERT INTO orchestration_v2_projection_messages
      (message_id, thread_id, run_id, node_id, role, streaming, created_at, updated_at, payload_json)
      VALUES ('obsolete', ${threadId}, NULL, NULL, 'assistant', 0, ${DateTime.formatIso(now)}, ${DateTime.formatIso(now)}, '{"obsolete":true}')`;
      assert.equal((yield* Effect.exit(projections.getThreadProjection(threadId)))._tag, "Failure");
      yield* orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make("resume-empty-queue"),
        threadId,
      });
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("rename-control"),
        threadId,
        title: "After",
      });
      yield* orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("mode-control"),
        threadId,
        runtimeMode: "approval-required",
      });
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("model-control"),
        threadId,
        modelSelection: { ...modelSelection, model: "gpt-6" },
      });
      const sessionId = ProviderSessionId.make("session:control-dispatch");
      yield* projections.apply({
        id: EventId.make("attach-control"),
        type: "provider-session.attached",
        threadId,
        occurredAt: now,
        payload: {
          id: sessionId,
          driver: adapter.driver,
          providerInstanceId: instanceId,
          status: "ready",
          cwd: "/repo",
          model: "gpt-6",
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
      });
      for (const mode of ["live", "message"] as const) {
        const requestId = RuntimeRequestId.make(`request:${mode}`);
        yield* projections.apply({
          id: EventId.make(`request:${mode}`),
          type: "runtime-request.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: requestId,
            nodeId: NodeId.make(`node:${mode}`),
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            responseCapability:
              mode === "live"
                ? { type: "live", providerSessionId: sessionId }
                : { type: "message" },
            createdAt: now,
            resolvedAt: null,
          },
        });
        const nodeId = NodeId.make(`node:${mode}`);
        yield* projections.apply({
          id: EventId.make(`node:${mode}`),
          type: "node.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: nodeId,
            threadId,
            runId: null,
            parentNodeId: null,
            rootNodeId: nodeId,
            kind: "user_input_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: requestId,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          },
        });
        yield* projections.apply({
          id: EventId.make(`item:${mode}`),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: TurnItemId.make(`item:${mode}`),
            threadId,
            runId: null,
            nodeId,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: mode === "live" ? 1 : 2,
            status: "waiting",
            title: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
            type: "user_input_request",
            requestId,
            questions: [],
          },
        });
        yield* orchestrator.dispatch(
          mode === "live"
            ? {
                type: "runtime-request.respond",
                commandId: CommandId.make(`respond:${mode}`),
                threadId,
                requestId,
                decision: "accept",
              }
            : {
                type: "thread.user-input.dismiss",
                commandId: CommandId.make(`respond:${mode}`),
                threadId,
                requestId,
              },
        );
        assert.equal(
          (yield* projections.getRuntimeRequest(threadId, requestId))?.status,
          "resolved",
        );
        const response = yield* projections.getRuntimeResponseContext(threadId, requestId);
        assert.equal(response.node?.status, mode === "live" ? "completed" : "cancelled");
        assert.equal(response.item?.status, mode === "live" ? "completed" : "cancelled");
      }
      const fs = yield* FileSystem.FileSystem;
      const newWorkspaceRoot = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-control-reads-new-workspace-",
      });
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("workspace-control"),
        threadId,
        worktreePath: newWorkspaceRoot,
      });
      const thread = yield* projections.getThread(threadId);
      assert.equal(thread.title, "After");
      assert.equal(thread.modelSelection.model, "gpt-6");
      assert.equal(thread.runtimeMode, "approval-required");
      assert.deepEqual(
        (yield* projections.getThreadProviderContext(threadId)).providerSessions,
        [],
      );
      yield* sql`INSERT INTO orchestration_v2_projection_turn_items
        (turn_item_id, thread_id, run_id, node_id, provider_thread_id, provider_turn_id,
          type, status, ordinal, updated_at, payload_json)
        VALUES ('obsolete-output', ${threadId}, NULL, NULL, NULL, NULL,
          'command_execution', 'completed', 900, ${DateTime.formatIso(now)}, '{"obsolete":true}')`;
      yield* sql`INSERT INTO orchestration_v2_projection_plans
        (plan_id, thread_id, run_id, node_id, kind, status, payload_json)
        VALUES ('obsolete-plan', ${threadId}, NULL, 'old-node', 'proposed', 'completed', '{"obsolete":true}')`;
      yield* sql`INSERT INTO orchestration_v2_projection_context_handoffs
        (context_handoff_id, thread_id, target_run_id, to_provider_thread_id, strategy, status, updated_at, payload_json)
        VALUES ('obsolete-handoff', ${threadId}, 'old-run', 'old-provider-thread', 'full_thread_summary', 'ready', ${DateTime.formatIso(now)}, '{"obsolete":true}')`;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("dispatch-with-old-history"),
        threadId,
        messageId: MessageId.make("fresh-input"),
        text: "Continue",
        attachments: [],
        dispatchMode: { type: "defer_start" },
        createdBy: "user",
        creationSource: "web",
      });
      const fresh = yield* projections.getThreadRecords(threadId, ["turnItems"], {
        turnItemTypes: ["user_message"],
      });
      assert.isAbove(fresh.turnItems.at(-1)!.ordinal, 900);
      yield* orchestrator.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("archive-with-old-history"),
        threadId,
      });
      yield* orchestrator.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("delete-with-old-history"),
        threadId,
      });
      assert.isNotNull((yield* projections.getThread(threadId)).deletedAt);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          makeTestLayer(ProjectId.make("project:control-dispatch")),
          NodeServices.layer,
        ),
      ),
      Effect.scoped,
    ),
);

it.effect("implements a proposed plan that the command projection leaves out", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:implement-plan");
    const planId = PlanId.make("plan:implement-plan");
    const now = yield* DateTime.now;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-implement-plan"),
      threadId,
      projectId: ProjectId.make("project:implement-plan"),
      title: "Plan",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "plan",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* projections.apply({
      id: EventId.make("plan:implement-plan"),
      type: "plan.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: planId,
        threadId,
        runId: null,
        nodeId: NodeId.make("node:implement-plan"),
        kind: "proposed_plan",
        status: "active",
        markdown: "# Plan\n\n1. Do the thing.",
      },
    });

    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("implement-plan"),
      threadId,
      messageId: MessageId.make("implement-plan-input"),
      text: "Implement the plan.",
      attachments: [],
      sourcePlanRef: { threadId, planId },
      dispatchMode: { type: "defer_start" },
      createdBy: "user",
      creationSource: "web",
    });

    assert.equal((yield* projections.getPlan(threadId, planId))?.status, "completed");
  }).pipe(Effect.provide(makeTestLayer(ProjectId.make("project:implement-plan")))),
);

// Stop's settle follow-up runs after the provider interrupt returns, possibly
// long after the Stop (retries) or again (an effect replayed after a crash).
// A later run's background work is not that Stop's to end.
it.effect("settles only the stopped run's background work, once", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:settle-binding");
    const providerThreadId = ProviderThreadId.make("provider-thread:settle-binding");
    const now = yield* DateTime.now;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-settle-binding"),
      threadId,
      projectId: ProjectId.make("project:settle-binding"),
      title: "Settle binding",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* projections.apply({
      id: EventId.make("settle-binding:provider-thread"),
      type: "provider-thread.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: providerThreadId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        providerSessionId: null,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 2,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    });
    const commandItem = (ordinal: number) => TurnItemId.make(`turn-item:settle-binding:${ordinal}`);
    for (const ordinal of [1, 2]) {
      const runId = RunId.make(`run:settle-binding:${ordinal}`);
      const attemptId = RunAttemptId.make(`attempt:settle-binding:${ordinal}`);
      const nodeId = NodeId.make(`node:settle-binding:${ordinal}`);
      const providerTurnId = ProviderTurnId.make(`provider-turn:settle-binding:${ordinal}`);
      yield* projections.apply({
        id: EventId.make(`settle-binding:run:${ordinal}`),
        type: "run.created",
        threadId,
        occurredAt: now,
        payload: {
          id: runId,
          threadId,
          ordinal,
          providerInstanceId: instanceId,
          modelSelection,
          providerThreadId,
          userMessageId: MessageId.make(`message:settle-binding:${ordinal}`),
          rootNodeId: nodeId,
          activeAttemptId: attemptId,
          status: "completed",
          requestedAt: now,
          startedAt: now,
          completedAt: now,
          checkpointId: null,
          contextHandoffId: null,
        },
      });
      yield* projections.apply({
        id: EventId.make(`settle-binding:attempt:${ordinal}`),
        type: "run-attempt.created",
        threadId,
        occurredAt: now,
        payload: {
          id: attemptId,
          runId,
          attemptOrdinal: 1,
          rootNodeId: nodeId,
          providerInstanceId: instanceId,
          providerThreadId,
          providerTurnId,
          reason: "initial",
          status: "completed",
          startedAt: now,
          completedAt: now,
        },
      });
      yield* projections.apply({
        id: EventId.make(`settle-binding:turn:${ordinal}`),
        type: "provider-turn.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: providerTurnId,
          providerThreadId,
          nodeId,
          runAttemptId: attemptId,
          nativeTurnRef: null,
          ordinal,
          status: "completed",
          startedAt: now,
          completedAt: now,
        },
      });
      yield* projections.apply({
        id: EventId.make(`settle-binding:item:${ordinal}`),
        type: "turn-item.updated",
        threadId,
        runId,
        occurredAt: now,
        payload: {
          id: commandItem(ordinal),
          threadId,
          runId,
          nodeId,
          providerThreadId,
          providerTurnId,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: ordinal * 10,
          status: "running",
          title: `Background command ${ordinal}`,
          startedAt: now,
          completedAt: null,
          updatedAt: now,
          type: "command_execution",
          input: `sleep ${ordinal}`,
        },
      });
    }
    const itemStatuses = Effect.map(projections.getThreadProjection(threadId), (projection) =>
      projection.turnItems
        .flatMap((item) => (item.type === "command_execution" ? [`${item.id}:${item.status}`] : []))
        .toSorted(),
    );
    // The settle that followed a Stop of run 1's turn, dispatched only after
    // run 2 had settled with work of its own.
    const settle = {
      type: "thread.background-work.settle",
      commandId: CommandId.make("stop-run-1:background-work-settled"),
      threadId,
      providerThreadId,
      providerTurnId: ProviderTurnId.make("provider-turn:settle-binding:1"),
    } as const;
    yield* orchestrator.dispatch(settle);
    assert.deepEqual(yield* itemStatuses, [
      `${commandItem(1)}:interrupted`,
      `${commandItem(2)}:running`,
    ]);

    // A settle that found nothing to end replays as a no-op, even after work
    // it would match appears: its receipt is recorded with no events.
    const emptySettle = {
      ...settle,
      commandId: CommandId.make("stop-run-1-again:background-work-settled"),
    };
    const first = yield* orchestrator.dispatch(emptySettle);
    assert.lengthOf(first.storedEvents, 0);
    yield* projections.apply({
      id: EventId.make("settle-binding:item:late"),
      type: "turn-item.updated",
      threadId,
      runId: RunId.make("run:settle-binding:1"),
      occurredAt: now,
      payload: {
        id: commandItem(3),
        threadId,
        runId: RunId.make("run:settle-binding:1"),
        nodeId: NodeId.make("node:settle-binding:1"),
        providerThreadId,
        providerTurnId: ProviderTurnId.make("provider-turn:settle-binding:1"),
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 30,
        status: "running",
        title: "Late background command",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "command_execution",
        input: "sleep 3",
      },
    });
    const replayed = yield* orchestrator.dispatch(emptySettle);
    assert.lengthOf(replayed.storedEvents, 0);
    assert.deepEqual(yield* itemStatuses, [
      `${commandItem(1)}:interrupted`,
      `${commandItem(2)}:running`,
      `${commandItem(3)}:running`,
    ]);
  }).pipe(Effect.provide(makeTestLayer(ProjectId.make("project:settle-binding")))),
);

it.effect("persists message blocking and rejects foreign content before any effects", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:blocked-target");
    const senderThreadId = ThreadId.make("thread:blocked-sender");
    for (const id of [threadId, senderThreadId]) {
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`create:${id}`),
        threadId: id,
        projectId: ProjectId.make("project:block-messages"),
        title: "Message controls",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
    }
    assert.isFalse((yield* projections.getThread(threadId)).threadMessagesBlocked ?? false);
    assert.isFalse((yield* projections.getThreadShell(threadId))?.threadMessagesBlocked);
    const acceptedCommand = {
      type: "message.dispatch",
      commandId: CommandId.make("foreign:before-block"),
      threadId,
      senderThreadId,
      messageId: MessageId.make("message:foreign-before-block"),
      text: "Already accepted work",
      attachments: [],
      createdBy: "agent",
      creationSource: "mcp",
      dispatchMode: { type: "start_immediately" },
    } as const;
    const accepted = yield* orchestrator.dispatch(acceptedCommand);
    // Seed a retained intent on the active run to test cancellation at admission,
    // independently of the MCP attachment checks covered by L23.
    const currentThread = yield* projections.getThread(threadId);
    const settlementIntent = {
      commandId: CommandId.make("settle-intent:block-target"),
      runId: (yield* projections.getThreadProjection(threadId)).runs[0]!.id,
      mcpCredentialId: "credential:block-target",
      providerSessionId: ProviderSessionId.make("session:block-target"),
      providerInstanceId: instanceId,
    };
    yield* (yield* EventSink.EventSinkV2).write({
      events: [
        {
          type: "thread.metadata-updated",
          id: EventId.make("fixture:block-settlement-intent"),
          threadId,
          occurredAt: yield* DateTime.now,
          payload: { ...currentThread, selfSettlement: settlementIntent },
        },
      ],
    });
    const blocked = yield* orchestrator.dispatch({
      type: "thread.metadata.update",
      commandId: CommandId.make("block:target"),
      threadId,
      threadMessagesBlocked: true,
    });
    assert.isTrue((yield* projections.getThread(threadId)).threadMessagesBlocked);
    assert.isTrue((yield* projections.getThreadShell(threadId))?.threadMessagesBlocked);
    assert.isTrue(
      (yield* orchestrator.getShellSnapshot()).threads.find((t) => t.id === threadId)
        ?.threadMessagesBlocked,
    );
    const rows = yield* sql<{
      blocked: number;
    }>`SELECT json_extract(payload_json, '$.threadMessagesBlocked') AS blocked
      FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}`;
    assert.equal(rows[0]?.blocked, 1);
    assert.deepEqual((yield* projections.getThread(threadId)).selfSettlement, settlementIntent);
    const captureState = Effect.gen(function* () {
      return {
        target: yield* projections.getThreadProjection(threadId),
        sender: yield* projections.getThreadProjection(senderThreadId),
        events: yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
        receipts: yield* sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
        outbox: yield* sql`SELECT * FROM orchestration_v2_effect_outbox`,
      };
    });
    const beforeReplay = yield* captureState;
    const replayed = yield* orchestrator.dispatch(acceptedCommand);
    assert.equal(replayed.sequence, accepted.sequence);
    assert.deepEqual(replayed.storedEvents, accepted.storedEvents);
    assert.deepEqual(yield* captureState, beforeReplay);
    const assertBlocked = (command: OrchestrationV2ServerCommand) =>
      Effect.gen(function* () {
        const beforeDenial = yield* captureState;
        const error = yield* orchestrator.dispatch(command).pipe(Effect.flip);
        assert.equal(error._tag, "OrchestratorThreadMessagesBlockedError");
        assert.include(error.message, "blocking messages from other threads");
        assert.deepEqual(yield* captureState, beforeDenial);
        const receipts =
          yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${command.commandId}`;
        assert.lengthOf(receipts, 0);
      });
    const before = yield* projections.getThreadProjection(threadId);
    const effectsBefore = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
    const targetRunId = RunId.make("run:blocked-active");
    const modes = [
      { type: "start_immediately" },
      { type: "defer_start" },
      { type: "queue_after_active" },
      { type: "steer_active", targetRunId },
      { type: "restart_active", targetRunId },
    ] as const;
    for (const dispatchMode of modes) {
      yield* assertBlocked({
        type: "message.dispatch",
        commandId: CommandId.make(`send:blocked:${dispatchMode.type}`),
        threadId,
        senderThreadId,
        messageId: MessageId.make(`message:blocked:${dispatchMode.type}`),
        text: "Foreign message",
        attachments: [],
        createdBy: "agent",
        creationSource: "mcp",
        dispatchMode,
      });
    }
    for (const command of [
      { type: "queued-run.edit", runId: targetRunId, text: "Foreign edit" },
      {
        type: "runtime-request.respond",
        requestId: RuntimeRequestId.make("request:blocked"),
        answers: { q: ["Foreign answer"] },
      },
    ] as const) {
      yield* assertBlocked({
        ...command,
        commandId: CommandId.make(`blocked:${command.type}`),
        threadId,
        senderThreadId,
      });
    }
    const merge = {
      type: "thread.merge_back",
      sourceThreadId: senderThreadId,
      targetThreadId: threadId,
      sourcePoint: { type: "run", runId: targetRunId },
      creationSource: "mcp",
    } as const;
    yield* assertBlocked({
      ...merge,
      commandId: CommandId.make("blocked:merge"),
      createdBy: "agent",
    });
    const ownerMerge = yield* orchestrator
      .dispatch({ ...merge, commandId: CommandId.make("owner:merge"), createdBy: "user" })
      .pipe(Effect.flip);
    // The normal lineage validation still runs for owner transfers.
    assert.equal(ownerMerge._tag, "OrchestratorDispatchError");
    assert.deepEqual(yield* projections.getThreadProjection(threadId), before);
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, effectsBefore);
    const ownerCommand = {
      type: "message.dispatch",
      commandId: CommandId.make("owner:blocked-message"),
      threadId,
      messageId: MessageId.make("message:owner-blocked"),
      text: "Owner input",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "defer_start" },
    } as const;
    yield* orchestrator.dispatch(ownerCommand);
    assert.isNull((yield* projections.getThread(threadId)).selfSettlement);
    yield* orchestrator.dispatch({
      ...ownerCommand,
      commandId: CommandId.make("self:blocked-message"),
      messageId: MessageId.make("message:self-blocked"),
      createdBy: "agent",
      creationSource: "mcp",
      senderThreadId: threadId,
    });
    yield* orchestrator.dispatch({
      type: "thread.metadata.update",
      commandId: CommandId.make("allow:target"),
      threadId,
      threadMessagesBlocked: false,
    });
    assert.isFalse((yield* projections.getThreadShell(threadId))?.threadMessagesBlocked);
    yield* orchestrator.dispatch({
      ...ownerCommand,
      commandId: CommandId.make("foreign:allowed-message"),
      messageId: MessageId.make("message:foreign-allowed"),
      createdBy: "agent",
      creationSource: "mcp",
      senderThreadId,
    });
    const after = yield* projections.getThreadProjection(threadId);
    assert.lengthOf(after.messages, 4);
    assert.lengthOf(after.runs, 4);
    assert.isTrue(
      blocked.storedEvents.some((stored) => stored.event.type === "thread.metadata-updated"),
    );
  }).pipe(Effect.provide(makeTestLayer(ProjectId.make("project:block-messages")))),
);

it.effect("keeps peer blocking and accepted receipts across a scoped SQLite close and reopen", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-block-reopen-" });
    const dbPath = path.join(directory, "state.sqlite");
    const workspaceRoot = path.join(directory, "workspace");
    yield* fs.makeDirectory(workspaceRoot);
    const threadId = ThreadId.make("thread:peer-block-reopen");
    const senderThreadId = ThreadId.make("thread:peer-block-reopen-sender");
    const scope: McpInvocationScope = {
      environmentId: EnvironmentId.make("environment:peer-block-reopen"),
      threadId,
      providerSessionId: "session:peer-block-reopen",
      providerInstanceId: instanceId,
      capabilities: new Set(["orchestration"]),
      issuedAt: 1,
    };
    const makeFileRuntime = () => {
      const database = makeSqlitePersistenceLive(dbPath).pipe(Layer.provide(NodeServices.layer));
      const core = Layer.mergeAll(
        database,
        ProjectionStore.layer.pipe(Layer.provide(database)),
        makeOrchestratorV2ReplayLayerWithRegistry(
          { name: "peer-block-reopen" },
          ProviderAdapterRegistry.makeLayer([adapter]),
          {
            databaseLayer: database,
            runEffectWorker: false,
            checkoutFixture: {
              projects: [
                {
                  projectId: ProjectId.make("project:peer-block-reopen"),
                  title: "Restart controls",
                  workspaceRoot,
                },
              ],
              resolvePath: () => undefined,
            },
          },
        ),
      );
      const threads = ThreadManagement.layer.pipe(Layer.provideMerge(core));
      return Layer.mergeAll(
        threads,
        ThreadMetadataMcpService.layer.pipe(
          Layer.provide(threads),
          Layer.provide(NodeCrypto.layer),
        ),
      );
    };
    const acceptedCommand = {
      type: "message.dispatch" as const,
      commandId: CommandId.make("command:peer-block-reopen-accepted"),
      threadId,
      senderThreadId,
      messageId: MessageId.make("message:peer-block-reopen-accepted"),
      text: "Accepted before blocking",
      attachments: [],
      createdBy: "agent" as const,
      creationSource: "mcp" as const,
      dispatchMode: { type: "defer_start" as const },
    };
    const accepted = yield* Effect.scoped(
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        for (const id of [threadId, senderThreadId])
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`create:${id}`),
            threadId: id,
            projectId: ProjectId.make("project:peer-block-reopen"),
            title: "Restart controls",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdBy: "user",
            creationSource: "web",
          });
        const accepted = yield* orchestrator.dispatch(acceptedCommand);
        yield* (yield* ThreadMetadataMcpService.ThreadMetadataMcpService).update(scope, {
          action: "block_thread_messages",
          clientRequestId: "block-before-close",
        });
        return accepted;
      }).pipe(Effect.provide(makeFileRuntime())),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const sql = yield* SqlClient.SqlClient;
        assert.isTrue((yield* projections.getThread(threadId)).threadMessagesBlocked);
        assert.isTrue((yield* projections.getThreadShell(threadId))?.threadMessagesBlocked);
        assert.isTrue(
          (yield* orchestrator.getShellSnapshot()).threads.find((row) => row.id === threadId)
            ?.threadMessagesBlocked,
        );
        const receiptBefore =
          yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${acceptedCommand.commandId}`;
        assert.lengthOf(receiptBefore, 1);
        const freshId = CommandId.make("command:peer-block-reopen-denied");
        const denied = yield* orchestrator
          .dispatch({
            ...acceptedCommand,
            commandId: freshId,
            messageId: MessageId.make("message:peer-block-reopen-denied"),
          })
          .pipe(Effect.flip);
        assert.equal(denied._tag, "OrchestratorThreadMessagesBlockedError");
        assert.lengthOf(
          yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${freshId}`,
          0,
        );
        const replayed = yield* orchestrator.dispatch(acceptedCommand);
        assert.equal(replayed.sequence, accepted.sequence);
        assert.deepEqual(replayed.storedEvents, accepted.storedEvents);
        assert.deepEqual(
          yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${acceptedCommand.commandId}`,
          receiptBefore,
        );
        const allowed = yield* (yield* ThreadMetadataMcpService.ThreadMetadataMcpService).update(
          scope,
          {
            action: "allow_thread_messages",
            clientRequestId: "self-allow-after-reopen",
          },
        );
        assert.isFalse(allowed.threadMessagesBlocked);
        assert.isFalse((yield* projections.getThreadShell(threadId))?.threadMessagesBlocked);
      }).pipe(Effect.provide(makeFileRuntime())),
    );
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect(
  "continues already accepted queued peer work after the recipient blocks new messages",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread:blocked-accepted-queue");
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create:blocked-accepted-queue"),
        threadId,
        projectId: ProjectId.make("project:blocked-accepted-queue"),
        title: "Accepted queue",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const message = {
        type: "message.dispatch" as const,
        threadId,
        text: "Accepted work",
        attachments: [],
        senderThreadId: ThreadId.make("thread:accepted-peer"),
        createdBy: "agent" as const,
        creationSource: "mcp" as const,
        dispatchMode: { type: "start_immediately" as const },
      };
      yield* orchestrator.dispatch({
        ...message,
        commandId: CommandId.make("accepted:active"),
        messageId: MessageId.make("message:accepted-active"),
      });
      const queuedCommand = {
        ...message,
        commandId: CommandId.make("accepted:queued"),
        messageId: MessageId.make("message:accepted-queued"),
        dispatchMode: { type: "queue_after_active" as const },
      };
      const accepted = yield* orchestrator.dispatch(queuedCommand);
      const before = yield* orchestrator.getThreadProjection(threadId);
      const queued = before.runs.find((run) => run.status === "queued")!;
      assert.isDefined(queued);
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("block:accepted-queue"),
        threadId,
        threadMessagesBlocked: true,
      });
      yield* orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("interrupt:accepted-active"),
        threadId,
        runId: before.runs[0]!.id,
        holdQueue: true,
      });
      const held = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(held.runs.find((run) => run.id === queued.id)?.status, "queued");
      assert.isTrue(held.runs.find((run) => run.id === queued.id)?.queueHeld);
      const effectsBefore = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
      const resume = {
        type: "queue.resume" as const,
        commandId: CommandId.make("resume:blocked-accepted-queue"),
        threadId,
      };
      yield* orchestrator.dispatch(resume);
      const progressed = yield* orchestrator.getThreadProjection(threadId);
      assert.isTrue(progressed.thread.threadMessagesBlocked);
      assert.equal(progressed.runs.find((run) => run.id === queued.id)?.status, "starting");
      assert.equal(
        progressed.messages.find((row) => row.id === queued.userMessageId)?.text,
        "Accepted work",
      );
      const effectsAfter = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
      assert.isAbove(effectsAfter.length, effectsBefore.length);
      const replayed = yield* orchestrator.dispatch(queuedCommand);
      assert.equal(replayed.sequence, accepted.sequence);
      assert.deepEqual(replayed.storedEvents, accepted.storedEvents);
      yield* orchestrator.dispatch(resume);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, effectsAfter);
      assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 2);
    }).pipe(Effect.provide(makeTestLayer(ProjectId.make("project:blocked-accepted-queue")))),
);
