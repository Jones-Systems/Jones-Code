import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import { CommandId, EnvironmentId, MessageId, NodeId, ProjectId, ProviderDriverKind, ProviderInstanceId,
  RunId, RunAttemptId, ProviderThreadId, ProviderTurnId, ProviderSessionId, OrchestrationV2ProviderThread,
  RuntimeRequestId, ThreadId, type OrchestrationV2Subagent, type RemoteTaskObservation } from "@t3tools/contracts";
import type * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Fixture from "../jones/threadResultDependencies/testkit.ts";
import { DependencyCommit } from "../jones/threadResultDependencies/DependencyCommit.ts";
import * as Planned from "../jones/updates/PlannedUpdateContinuityService.ts";
import * as RemoteReceipts from "../jones/orchestration/RemoteSendReceiptStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Registry from "./ProviderAdapterRegistry.ts";
import * as Replay from "./testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as Sessions from "./ProviderSessionManager.ts";

const instanceId = ProviderInstanceId.make("codex");
const adapter = { instanceId, driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Core recovery tests never execute a provider"),
} as Adapter.ProviderAdapterV2["Service"];
const runtime = Replay.layerWithRegistry({ name: "core-command-recovery" }, Registry.layerFromAdapters([adapter]),
  { databaseLayer: Sqlite.layerMemory, runEffectWorker: false, runContinuationWorker: false }).pipe(
    Layer.provideMerge(Fixture.layer));

it.effect("failover recovery retains unresolved evidence without a registered native provider and never creates a replacement run", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const providerTurnId = ProviderTurnId.make("unresolved-native-failure");
    yield* f.sql`INSERT INTO jones_codex_failover_evidence (provider_turn_id, thread_id, run_id, attempt_id,
      provider_thread_id, provider_instance_id, disposition, occurred_at) VALUES (${providerTurnId}, ${f.receiver.id},
      ${f.receiverRun.id}, ${RunAttemptId.make("unknown-native-attempt")}, ${ProviderThreadId.make("unknown-native-thread")},
      ${instanceId}, 'unavailable', ${DateTime.formatIso(Fixture.time)})`;
    const sequence = yield* f.sink.latestSequence();
    yield* orchestrator.recoverCodexFailover;
    yield* orchestrator.recoverCodexFailover;
    assert.equal(yield* f.sink.latestSequence(), sequence);
    assert.equal((yield* f.sql`SELECT provider_turn_id FROM jones_codex_failover_evidence WHERE provider_turn_id = ${providerTurnId}`).length, 1);
    assert.equal((yield* f.sql`SELECT provider_turn_id FROM jones_codex_failover_decisions WHERE provider_turn_id = ${providerTurnId}`).length, 0);
  }).pipe(Effect.provide(runtime)),
);

it.effect("revalidates native attachment in the self-settlement transaction and commits project items with its original intent", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sessions = yield* Sessions.ProviderSessionManagerV2;
    const nativeSessionId = ProviderSessionId.make("actual-native-session");
    const providerThreadId = ProviderThreadId.make("self-native-thread");
    const attemptId = RunAttemptId.make("self-attempt");
    const provider = Schema.decodeUnknownSync(OrchestrationV2ProviderThread)({ id: providerThreadId, driver: "codex",
      providerInstanceId: instanceId, providerSessionId: nativeSessionId, appThreadId: f.receiver.id, ownerNodeId: null,
      nativeThreadRef: { driver: "codex", nativeId: "original-native-thread", strength: "strong" }, nativeConversationHeadRef: null,
      status: "active", firstRunOrdinal: 1, lastRunOrdinal: 1, handoffIds: [], forkedFrom: null,
      createdAt: Fixture.time, updatedAt: Fixture.time });
    yield* f.sink.write({ events: [
      f.event({ type: "provider-thread.updated", threadId: f.receiver.id, payload: provider }),
      f.event({ type: "thread.metadata-updated", threadId: f.receiver.id, payload: { ...f.receiver, activeProviderThreadId: providerThreadId } }),
      f.event({ type: "run.updated", threadId: f.receiver.id, runId: f.receiverRun.id,
        payload: { ...f.receiverRun, providerThreadId, activeAttemptId: attemptId } }),
      f.event({ type: "run-attempt.created", threadId: f.receiver.id, runId: f.receiverRun.id,
        payload: { id: attemptId, runId: f.receiverRun.id, attemptOrdinal: 1, rootNodeId: NodeId.make("self-node"),
          providerInstanceId: instanceId, providerThreadId, providerTurnId: null, reason: "initial", status: "running",
          startedAt: Fixture.time, completedAt: null } }),
    ] });
    let calls = 0;
    let revokeInTransaction = true;
    using _attachment = vi.spyOn(sessions, "isMcpCallerAttached").mockImplementation((input) => Effect.sync(() => {
      assert.equal(input.providerSessionId, nativeSessionId);
      assert.equal(input.mcpCredentialId, "distinct-registry-credential");
      assert.equal(input.providerInstanceId, instanceId);
      return ++calls !== 2 || !revokeInTransaction;
    }));
    const request = { threadId: f.receiver.id, commandId: CommandId.make("self-acceptance"),
      providerInstanceId: instanceId, mcpCredentialId: "distinct-registry-credential", projectRemaining: ["Owner follow-up"] };
    assert.isTrue(Exit.isFailure(yield* orchestrator.requestSelfSettlement(request).pipe(Effect.exit)));
    assert.isUndefined((yield* f.projection.getThread(f.receiver.id)).selfSettlement);
    assert.equal((yield* f.sql`SELECT command_id FROM jones_self_settlement_project_items`).length, 0);
    revokeInTransaction = false;
    const intent = yield* orchestrator.requestSelfSettlement(request);
    assert.equal(intent.providerSessionId, nativeSessionId);
    assert.deepEqual(yield* orchestrator.requestSelfSettlement(request), intent);
    assert.equal((yield* f.sql`SELECT command_id FROM jones_self_settlement_project_items WHERE command_id = ${request.commandId}`).length, 1);
    yield* orchestrator.dispatch({ type: "thread.stop", threadId: f.receiver.id, commandId: CommandId.make("stop-self-intent") });
    assert.isNull((yield* f.projection.getThread(f.receiver.id)).selfSettlement);
  }).pipe(Effect.provide(runtime)),
);

it.effect("accepts initial thread creation, first message and dependency together, with no partial thread on refusal", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("atomic-created-target");
    const commandId = CommandId.make("atomic-create-and-send");
    const messageId = MessageId.make("atomic-initial-message");
    const request = { ...f.requestInput("atomic-create"), commandId, targetThreadId: threadId, messageId };
    const command = { type: "thread.create" as const, commandId, threadId,
      projectId: ProjectId.make("result-test"), title: "Custom owner title", modelSelection: f.receiver.modelSelection,
      runtimeMode: "full-access" as const, interactionMode: "default" as const,
      branch: null, worktreePath: null, createdBy: "user" as const, creationSource: "web" as const,
      initialMessage: { messageId, text: "Create with a durable initial request", attachments: [] } };
    const before = yield* f.sink.latestSequence();
    const refused = yield* orchestrator.dispatch(command).pipe(Effect.provideService(DependencyCommit, {
      ...request, localDependency: { ...request.localDependency!, waiterGeneration: 99 },
    }), Effect.exit);
    assert.isTrue(Exit.isFailure(refused));
    assert.equal(yield* f.sink.latestSequence(), before);
    assert.isNull(yield* f.projection.getThreadShell(threadId));
    const accepted = yield* orchestrator.dispatch(command).pipe(Effect.provideService(DependencyCommit, request));
    assert.isTrue(accepted.storedEvents.some(({ event }) => event.type === "thread.created"));
    assert.isTrue(accepted.storedEvents.some(({ event }) => event.type === "message.updated" && event.payload.id === messageId));
    assert.equal((yield* f.store.get(request.localDependency!.dependencyId))?.state, "awaiting");
    assert.equal((yield* f.projection.getThread(threadId)).title, "Custom owner title");
    const replay = yield* orchestrator.dispatch(command).pipe(Effect.provideService(DependencyCommit, request));
    assert.equal(replay.replayed, true);
    assert.deepEqual(replay.storedEvents.map(({ sequence }) => sequence), accepted.storedEvents.map(({ sequence }) => sequence));
  }).pipe(Effect.provide(runtime)),
);

it.effect("serializes duplicate remote sends and guards workspace preparation against target replacement and recursive dispatch", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const capture: RemoteReceipts.SendCapture = { originNamespace: "authenticated-origin", clientRequestId: "remote-core",
      requestDigest: "c".repeat(64), threadId: f.receiver.id, messageId: RemoteReceipts.messageIdFor("authenticated-origin", "remote-core"),
      mode: "queue", expectedThreadCreatedAt: DateTime.formatIso(f.receiver.createdAt) };
    const commandId = RemoteReceipts.commandIdFor(capture.originNamespace, capture.clientRequestId);
    const send = () => orchestrator.remoteSendReceipts.withSendCommit(capture, orchestrator.dispatch({
      type: "message.dispatch", commandId, threadId: capture.threadId, messageId: capture.messageId,
      text: "Original captured send", attachments: [], dispatchMode: { type: "queue_after_active" },
      createdBy: "agent", creationSource: "mcp",
    }));
    const sent = yield* Effect.all([send(), send()], { concurrency: 2 });
    assert.deepEqual(sent[0], sent[1]);
    assert.equal(sent[0]!.delivery, "queued");
    assert.equal((yield* f.projection.getThreadRecords(f.receiver.id, ["messages"],
      { messageIds: [capture.messageId] })).messages.length, 1);
    const changed = { ...capture, clientRequestId: "replacement-fence",
      messageId: RemoteReceipts.messageIdFor(capture.originNamespace, "replacement-fence"), expectedThreadCreatedAt: "different-birth" };
    let preparations = 0;
    const guarded = yield* orchestrator.remoteSendReceipts.withSendCommit(changed,
      orchestrator.remoteSendReceipts.guardPreparation(RemoteReceipts.commandIdFor(changed.originNamespace, changed.clientRequestId),
        Effect.sync(() => { preparations++; })),
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(guarded));
    assert.equal(preparations, 0);
    const recursive = yield* orchestrator.remoteSendReceipts.withSendCommit({ ...capture, clientRequestId: "recursive-preparation",
      messageId: RemoteReceipts.messageIdFor(capture.originNamespace, "recursive-preparation") },
      orchestrator.remoteSendReceipts.guardPreparation(RemoteReceipts.commandIdFor(capture.originNamespace, "recursive-preparation"),
        orchestrator.dispatch({ type: "queue.resume", threadId: f.receiver.id, commandId: CommandId.make("recursive-queue") })),
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(recursive));
  }).pipe(Effect.provide(runtime)),
);

it.effect.each([false, true])("planned queue recovery completes custody only after durable resume (refused: %s)", (refused) => {
  let finished = 0;
  const continuity = Layer.mock(Planned.PlannedUpdateContinuity)({
    queueThreadIds: Effect.succeed([ThreadId.make("receiver")]),
    queueCommand: () => refused ? Effect.fail(new Planned.PlannedContinuityError({ cause: "held proof" }))
      : Effect.succeed(CommandId.make("resume-planned-core")),
    finishQueue: () => Effect.sync(() => { finished++; }),
  });
  return Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const queued = { ...f.run("planned-queued", f.receiver.id, "queued", 2), startedAt: null, queueHeld: true };
    yield* f.sink.write({ events: [f.event({ type: "run.created", threadId: f.receiver.id, runId: queued.id, payload: queued })] });
    yield* orchestrator.recoverPlannedUpdateQueues;
    assert.equal(finished, refused ? 0 : 1);
    const current = (yield* f.projection.getThreadRecords(f.receiver.id, ["runs"])).runs.find((run) => run.id === queued.id);
    assert.equal(current?.queueHeld, refused);
  }).pipe(Effect.provide(runtime.pipe(Layer.provide(continuity))));
});

it.effect("restores remote question observations through the actual under-parent-lock dispatch and replays once", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const taskId = NodeId.make("core-remote-task");
    const observation: RemoteTaskObservation = { schemaVersion: "t3.remote-task-observation/v1",
      environmentId: EnvironmentId.make("remote-core"), threadId: ThreadId.make("remote-child"), originalRunId: RunId.make("remote-original"),
      blockingQuestions: { resumeToken: "core-question", requests: [
        { requestId: RuntimeRequestId.make("remote-question"), runId: RunId.make("remote-original") },
      ] } };
    const task: OrchestrationV2Subagent = { id: taskId, threadId: f.receiver.id, runId: f.receiverRun.id,
      parentNodeId: NodeId.make("core-parent-node"), origin: "app_owned", createdBy: "agent", driver: ProviderDriverKind.make("remote"),
      providerInstanceId: instanceId, providerThreadId: null, childThreadId: null,
      remoteChild: { environmentId: observation.environmentId, threadId: observation.threadId,
        runId: observation.originalRunId, label: "Remote" }, nativeTaskRef: null, prompt: "Question fixture",
      title: null, model: null, status: "running", result: null, startedAt: Fixture.time, completedAt: null, updatedAt: Fixture.time };
    yield* f.sink.write({ events: [f.event({ type: "subagent.updated", threadId: f.receiver.id, payload: task })] });
    yield* f.sql`INSERT INTO jones_remote_task_observations (parent_thread_id, task_id, observation_json, signal_pending)
      VALUES (${f.receiver.id}, ${taskId}, ${JSON.stringify(observation)}, 1)`;
    const recover = orchestrator.recoverRemoteTaskQuestions;
    const read = orchestrator.readRemoteTaskObservation;
    if (recover === undefined || read === undefined) return yield* Effect.die("Required core observation ports missing");
    yield* recover;
    const first = yield* f.projection.getThreadRecords(f.receiver.id, ["messages", "runs"]);
    assert.equal(first.messages.filter((message) => message.id.startsWith("message:remote-question:")).length, 1);
    assert.deepEqual(yield* read(f.receiver.id, taskId), observation);
    yield* recover;
    const second = yield* f.projection.getThreadRecords(f.receiver.id, ["messages", "runs"]);
    assert.equal(second.runs.length, first.runs.length);
    assert.equal((yield* f.sql<{ signal_pending: number }>`SELECT signal_pending FROM jones_remote_task_observations
      WHERE parent_thread_id = ${f.receiver.id} AND task_id = ${taskId}`)[0]?.signal_pending, 0);
  }).pipe(Effect.provide(runtime)),
);
