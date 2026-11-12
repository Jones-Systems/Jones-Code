import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as SqlClient from "effect/sql/SqlClient";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Executor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Legacy from "../../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Worktree from "../../mcp/WorktreeMcpService.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import { ClaudeProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as Projects from "../../project/ProjectService.ts";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as Setup from "../../project/ProjectSetupScriptRunner.ts";
import * as VcsStatus from "../../vcs/VcsStatusBroadcaster.ts";
import * as Conversation from "../conversationWorkspace/ConversationPolicy.ts";

const threadId = ThreadId.make("thread:active-workspace-handoff");
const projectId = ProjectId.make("project:active-workspace-handoff");
const driver = ProviderDriverKind.make("claudeAgent");
const selection = { instanceId: ProviderInstanceId.make("claudeAgent"), model: "synthetic" };
const requestId = "handoff-regression";
const handoffInput = {
  branch: "work/handoff-regression",
  startFromOrigin: false,
  runSetupScript: false,
  continuationPrompt: "continue",
  clientRequestId: requestId,
};
const request = JSON.stringify({
  callerThreadId: threadId,
  branch: handoffInput.branch,
  baseRef: null,
  startFromOrigin: false,
  runSetupScript: false,
  continuationPrompt: "continue",
  continuationAttachments: [],
});

it.live.each(["handoff", "unexpected failure"] as const)(
  "active workspace transition: %s",
  (scenario) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const detached = yield* Deferred.make<void>();
      const failures =
        yield* Queue.unbounded<
          Queue.Queue<Adapter.ProviderAdapterV2Event, Adapter.ProviderAdapterEventStreamError>
        >();
      let starts = 0;
      const unused = () => Effect.die("Unexpected fake-provider operation");
      const adapter = Adapter.ProviderAdapterV2.of({
        instanceId: selection.instanceId,
        driver,
        getCapabilities: () => Effect.succeed(ClaudeProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: (input) =>
          Effect.gen(function* () {
            const events = yield* Queue.unbounded<
              Adapter.ProviderAdapterV2Event,
              Adapter.ProviderAdapterEventStreamError
            >();
            yield* Queue.offer(failures, events);
            const now = yield* DateTime.now;
            const generation =
              input.runtimeLifecycle === undefined
                ? "synthetic"
                : yield* input.runtimeLifecycle.reserve(input.threadId);
            if (input.runtimeLifecycle !== undefined)
              yield* input.runtimeLifecycle.admit({
                runtimeGeneration: generation,
                threadId: input.threadId,
              });
            const bind = (providerThread: OrchestrationV2ProviderThread) => {
              const identity = {
                runtimeGeneration: generation,
                requested: Adapter.requestedRuntimeIdentity(selection, driver),
                observed: Adapter.unobservedRuntimeIdentity(),
              };
              return input.runtimeLifecycle === undefined
                ? Effect.succeed({ ...providerThread, runtimeIdentity: identity })
                : input.runtimeLifecycle.bind({ providerThread, ...identity });
            };
            let active:
              | Parameters<Adapter.ProviderAdapterV2SessionRuntime["startTurn"]>[0]
              | undefined;
            const terminal = (
              turn: NonNullable<typeof active>,
              status: "completed" | "interrupted",
            ) =>
              Queue.offer(events, {
                type: "turn.terminal",
                driver,
                providerThreadId: turn.providerThread.id,
                providerTurnId: ProviderTurnId.make(`turn:${turn.runId}`),
                runOrdinal: turn.runOrdinal,
                status,
                failure: null,
                threadDisposition: "reusable",
              }).pipe(Effect.asVoid);
            return {
              instanceId: selection.instanceId,
              driver,
              providerSessionId: input.providerSessionId,
              providerSession: {
                id: input.providerSessionId,
                driver,
                providerInstanceId: selection.instanceId,
                status: "ready",
                cwd: input.runtimePolicy.cwd ?? process.cwd(),
                model: selection.model,
                capabilities: ClaudeProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
              events: Stream.fromQueue(events),
              ensureThread: (value) =>
                bind({
                  id:
                    value.existingProviderThread?.id ??
                    ProviderThreadId.make("provider-thread:active-handoff"),
                  driver,
                  providerInstanceId: selection.instanceId,
                  providerSessionId: input.providerSessionId,
                  appThreadId: value.threadId,
                  ownerNodeId: null,
                  nativeThreadRef: { driver, nativeId: "synthetic-thread", strength: "strong" },
                  nativeConversationHeadRef: null,
                  status: "idle",
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  contextUsage: null,
                  forkedFrom: null,
                  createdAt: now,
                  updatedAt: now,
                }),
              resumeThread: ({ providerThread }) =>
                bind({ ...providerThread, providerSessionId: input.providerSessionId }),
              startTurn: (turn) =>
                Effect.gen(function* () {
                  starts += 1;
                  active = turn;
                  const time = yield* DateTime.now;
                  yield* Queue.offer(events, {
                    type: "provider_turn.updated",
                    driver,
                    providerTurn: {
                      id: ProviderTurnId.make(`turn:${turn.runId}`),
                      providerThreadId: turn.providerThread.id,
                      nodeId: turn.rootNodeId,
                      runAttemptId: turn.attemptId,
                      nativeTurnRef: null,
                      ordinal: turn.runOrdinal,
                      status: "running",
                      startedAt: time,
                      completedAt: null,
                    },
                  });
                  if (starts === 1) yield* Deferred.succeed(started, undefined);
                  else yield* terminal(turn, "completed");
                }),
              interruptTurn: () =>
                active === undefined ? Effect.void : terminal(active, "interrupted"),
              steerTurn: unused,
              compactThread: unused,
              respondToRuntimeRequest: unused,
              readThreadSnapshot: unused,
              rollbackThread: unused,
              forkThread: unused,
            } satisfies Adapter.ProviderAdapterV2SessionRuntime;
          }),
      });
      const config = Layer.effect(
        ServerConfig.ServerConfig,
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const value = yield* Harness.makeReplayServerConfig("active-workspace-handoff").pipe(
            Effect.orDie,
          );
          yield* Effect.addFinalizer(() =>
            fs.remove(value.baseDir, { recursive: true }).pipe(Effect.orDie),
          );
          return value;
        }),
      ).pipe(Layer.provide(NodeServices.layer));
      const layer = Harness.layerWithRegistryAndServices(
        { name: "active-workspace-handoff" },
        Registry.layerFromAdapters([adapter]),
        {
          serverConfigLayer: config,
          onSessionDetached: () => Deferred.succeed(detached, undefined).pipe(Effect.asVoid),
        },
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const settings = yield* ServerSettings.ServerSettingsService;
        const sessions = yield* Sessions.ProviderSessionManagerV2;
        const executor = yield* Executor.ThreadCommandExecutor;
        const sql = yield* SqlClient.SqlClient;
        const threads = yield* ThreadManagement.ThreadManagementService.pipe(
          Effect.provide(
            ThreadManagement.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(Orchestrator.OrchestratorV2, orchestrator),
                  Layer.succeed(Sessions.ProviderSessionManagerV2, sessions),
                  Layer.succeed(Executor.ThreadCommandExecutor, executor),
                  Layer.succeed(ServerSettings.ServerSettingsService, settings),
                  Layer.mock(Legacy.LegacyV1ThreadImporter)({}),
                  Receipts.layer.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, sql))),
                ),
              ),
            ),
          ),
        );
        const waitFor = (predicate: (value: OrchestrationV2ThreadProjection) => boolean) =>
          Effect.gen(function* () {
            const afterSequence = yield* sink.latestSequence();
            const current = yield* orchestrator.getThreadProjection(threadId);
            if (predicate(current)) return current;
            return yield* sink.stream({ threadId, afterSequence }).pipe(
              Stream.mapEffect(() => orchestrator.getThreadProjection(threadId)),
              Stream.filter(predicate),
              Stream.runHead,
              Effect.map(Option.getOrThrow),
            );
          });
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId,
          title: "Active handoff",
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("start"),
          threadId,
          messageId: MessageId.make("initial"),
          text: "initial",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* Deferred.await(started);
        const running = yield* waitFor(
          (p) =>
            p.runs[0]?.status === "running" && p.providerTurns.some((t) => t.status === "running"),
        );
        const initialRun = running.runs[0]!;
        if (scenario === "unexpected failure") {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("continue"),
            threadId,
            messageId: MessageId.make("continuation"),
            text: "continue",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "mcp",
          });
          const events = yield* Queue.take(failures);
          yield* Queue.fail(
            events,
            new Adapter.ProviderAdapterEventStreamError({
              driver,
              providerSessionId: running.providerThreads[0]!.providerSessionId!,
              cause: "Synthetic unexpected stream failure",
            }),
          );
          const held = yield* waitFor((p) => p.runs.some((r) => r.queueHeld));
          assert.equal(held.runs.find((r) => r.id === initialRun.id)?.status, "failed");
          assert.isTrue(held.runs.find((r) => r.userMessageId === "continuation")?.queueHeld);
          assert.equal(starts, 1);
          return;
        }
        const cfg = yield* ServerConfig.ServerConfig.pipe(Effect.provide(config));
        let record: Conversation.HandoffRecord = {
          revision: 1,
          members: [threadId],
          threadId,
          requestId,
          request,
          commit: "synthetic-commit",
          baseRef: "main",
          startedFromOrigin: false,
          phase: "checkout_created",
          path: cfg.baseDir,
          setup: null,
          result: null,
        };
        const worktree = yield* Worktree.make.pipe(
          Effect.provide(
            Layer.mergeAll(
              Layer.succeed(ThreadManagement.ThreadManagementService, threads),
              Layer.succeed(ServerSettings.ServerSettingsService, settings),
              Layer.mock(Projects.ProjectService)({
                getById: () =>
                  Effect.succeedSome({
                    id: projectId,
                    title: "Synthetic",
                    workspaceRoot: cfg.baseDir,
                    repositoryIdentity: null,
                    faviconPath: null,
                    defaultModelSelection: selection,
                    defaultThreadEnvMode: null,
                    scripts: [],
                    createdAt: "2026-11-12T00:00:00Z",
                    updatedAt: "2026-11-12T00:00:00Z",
                    deletedAt: null,
                  }),
              }),
              Layer.mock(Conversation.ConversationPolicy)({
                ownsPath: (path) => path === cfg.baseDir,
                registerTurnAttachment: () => Effect.void,
                lookup: () => Effect.succeed(null),
                readHandoff: () => Effect.succeed(record),
                updateHandoff: (_old, patch) =>
                  Effect.sync(
                    () => (record = { ...record, ...patch, revision: record.revision + 1 }),
                  ),
                markAttached: () => Effect.void,
                dispose: () => Effect.succeed(null),
              }),
              Layer.mock(GitWorkflow.GitWorkflowService)({}),
              Layer.mock(Setup.ProjectSetupScriptRunner)({}),
              Layer.mock(VcsStatus.VcsStatusBroadcaster)({}),
              NodeServices.layer,
            ),
          ),
        );
        const result = yield* worktree.handoff(
          {
            environmentId: EnvironmentId.make("synthetic"),
            issuedAt: 0,
            requestNamespace: "handoff-test",
            capabilities: new Set(["worktree"]),
            client: undefined,
            thread: {
              threadId,
              providerInstanceId: selection.instanceId,
              providerSessionId: running.providerThreads[0]!.providerSessionId!,
            },
          },
          handoffInput,
        );
        assert.equal(result.continuation.status, "scheduled");
        assert.equal(record.phase, "done");
        yield* Deferred.await(detached);
        const ended = yield* waitFor((p) =>
          p.runs.some(
            (r) =>
              r.id === initialRun.id && ["failed", "interrupted", "completed"].includes(r.status),
          ),
        );
        assert.equal(ended.runs.find((r) => r.id === initialRun.id)?.status, "interrupted");
        const complete = yield* waitFor(
          (p) =>
            p.runs.length === 2 &&
            p.runs.every((r) => ["failed", "interrupted", "completed"].includes(r.status)),
        );
        assert.equal(starts, 2);
        assert.isFalse(complete.runs.some((r) => r.queueHeld));
        assert.equal(complete.runs[1]?.status, "completed");
        assert.isFalse(
          complete.turnItems.some((item) => item.type === "error" && item.status === "failed"),
        );
      }).pipe(Effect.provide(layer));
    }).pipe(Effect.scoped),
);
