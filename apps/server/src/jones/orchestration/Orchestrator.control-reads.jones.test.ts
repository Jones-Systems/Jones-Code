import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  type OrchestrationV2ServerCommand,
  RunId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadMetadataMcpService from "../../mcp/ThreadMetadataMcpService.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import type { ProviderAdapterV2Shape } from "../../orchestration-v2/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for metadata controls"),
} as ProviderAdapterV2Shape;
const database = SqlitePersistence.layerMemory;
const testLayer = Layer.mergeAll(
  database,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "control-reads" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ),
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
  }).pipe(Effect.provide(testLayer)),
);

it.effect("keeps peer blocking and accepted receipts across a scoped SQLite close and reopen", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-block-reopen-" });
    const dbPath = path.join(directory, "state.sqlite");
    const threadId = ThreadId.make("thread:peer-block-reopen");
    const senderThreadId = ThreadId.make("thread:peer-block-reopen-sender");
    const scope: McpInvocationScope = {
      environmentId: EnvironmentId.make("environment:peer-block-reopen"),
      thread: {
        threadId,
        providerSessionId: "session:peer-block-reopen",
        providerInstanceId: instanceId,
      },
      client: undefined,
      requestNamespace: "peer-block-reopen",
      capabilities: new Set(["orchestration"]),
      issuedAt: 1,
    };
    const makeFileRuntime = () => {
      const database = SqlitePersistence.layerFromPath(dbPath).pipe(
        Layer.provide(NodeServices.layer),
      );
      const core = Layer.mergeAll(
        database,
        ProjectionStore.layer.pipe(Layer.provide(database)),
        ProviderReplayHarness.layerWithRegistry(
          { name: "peer-block-reopen" },
          ProviderAdapterRegistry.layerFromAdapters([adapter]),
          { databaseLayer: database, runEffectWorker: false },
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
    }).pipe(Effect.provide(testLayer)),
);
