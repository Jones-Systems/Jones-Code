import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Exit from "effect/Exit";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Failure from "@t3tools/provider-core/server/failure";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
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
  RunAttemptId,
  ThreadId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as SqlClient from "effect/sql/SqlClient";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Projection from "../../orchestration-v2/ProjectionStore.ts";
import * as Executor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Legacy from "../../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
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

vi.mock("@t3tools/provider-core/server/failure", { spy: true });
vi.mock("effect/Layer", { spy: true });

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

it.live.each([
  "handoff",
  "handoff without terminal",
  "handoff unresponsive",
  "settlement identity guards",
  "unexpected failure",
  "failure before planning",
  "failure after planning",
  "failure after lease",
] as const)("active workspace transition: %s", (scenario) =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const detached = yield* Deferred.make<void>();
    const failureReached = yield* Deferred.make<void>();
    const runtimeClosed = yield* Deferred.make<void>();
    const allowFailureFinalization = yield* Deferred.make<void>();
    const handoffControlStarted = yield* Deferred.make<void>();
    const allowHandoffControl = yield* Deferred.make<void>();
    const detachStarted = yield* Deferred.make<void>();
    const leaseAcquired = yield* Deferred.make<void>();
    const streamFailureObserved = yield* Deferred.make<void>();
    let injectedFailure: Adapter.ProviderAdapterEventStreamError | undefined;
    const failures =
      yield* Queue.unbounded<
        Queue.Queue<Adapter.ProviderAdapterV2Event, Adapter.ProviderAdapterEventStreamError>
      >();
    const unresponsiveFinished = yield* Deferred.make<void>();
    let starts = 0;
    const unused = () => Effect.die("Unexpected fake-provider operation");
    const adapter = Adapter.ProviderAdapterV2.of({
      instanceId: selection.instanceId,
      driver,
      getCapabilities: Effect.fn("ActiveWorkspaceHandoff.getCapabilities")(() =>
        Effect.succeed(ClaudeProviderCapabilitiesV2),
      ),
      planSelectionTransition: Effect.fn("ActiveWorkspaceHandoff.planSelectionTransition")(() =>
        Effect.succeed({ type: "apply_on_next_turn" } as const),
      ),
      openSession: Effect.fn("ActiveWorkspaceHandoff.openSession")(function* (input) {
        yield* Effect.addFinalizer(() =>
          Deferred.succeed(runtimeClosed, undefined).pipe(Effect.asVoid),
        );
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
        let active: Parameters<Adapter.ProviderAdapterV2SessionRuntime["startTurn"]>[0] | undefined;
        const terminal = (turn: NonNullable<typeof active>, status: "completed" | "interrupted") =>
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
          startTurn: Effect.fn("ActiveWorkspaceHandoff.startTurn")(function* (turn) {
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
          interruptTurn: (input) =>
            scenario === "handoff unresponsive"
              ? new Adapter.ProviderAdapterInterruptTimeoutError({
                  driver,
                  providerThreadId: input.providerThread.id,
                  providerTurnId: input.providerTurnId,
                  timeoutMs: 1,
                })
              : scenario === "failure after lease" || scenario === "handoff without terminal"
                ? Effect.void
                : scenario.startsWith("failure ")
                  ? Deferred.succeed(failureReached, undefined).pipe(
                      Effect.andThen(Deferred.await(allowFailureFinalization)),
                    )
                  : active === undefined
                    ? Effect.void
                    : terminal(active, "interrupted"),
          steerTurn: unused,
          compactThread: unused,
          respondToRuntimeRequest: unused,
          readThreadSnapshot: unused,
          rollbackThread: unused,
          forkThread: unused,
        } satisfies Adapter.ProviderAdapterV2SessionRuntime;
      }),
    });
    const fixtureConfig = Layer.effect(
      ServerConfig.ServerConfig,
      Harness.makeReplayServerConfig("active-workspace-handoff-fixture").pipe(Effect.orDie),
    ).pipe(Layer.provide(NodeServices.layer));
    const actualConfigs: ServerConfig.ServerConfig["Service"][] = [];
    const constructLayer = (yield* Effect.promise(() =>
      vi.importActual<typeof Layer>("effect/Layer"),
    )).effect;
    const observeConfig = <I, S, E, R>(service: Context.Key<I, S>, build: Effect.Effect<S, E, R>) =>
      constructLayer(
        service,
        build.pipe(
          Effect.tap((value) =>
            Effect.sync(() => {
              if (service.key === ServerConfig.ServerConfig.key)
                actualConfigs.push(value as ServerConfig.ServerConfig["Service"]);
            }),
          ),
        ),
      );
    const configConstruction = vi
      .spyOn(Layer, "effect")
      .mockImplementation(observeConfig as typeof Layer.effect);
    const layer = Harness.layerWithRegistryAndServices(
      { name: "active-workspace-handoff" },
      Registry.layerFromAdapters([adapter]),
      {
        beforeEffectExecution: Effect.fn("ActiveWorkspaceHandoff.beforeEffectExecution")(
          function* (effect) {
            if (
              effect.request.type === "provider-turn.interrupt" &&
              scenario === "failure after planning"
            ) {
              yield* Deferred.succeed(handoffControlStarted, undefined);
              yield* Deferred.await(allowHandoffControl);
            }
            if (effect.request.type === "provider-session.detach")
              yield* Deferred.succeed(detachStarted, undefined);
          },
        ),
        onSessionDetached: () => Deferred.succeed(detached, undefined).pipe(Effect.asVoid),
      },
    );
    configConstruction.mockRestore();
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const settings = yield* ServerSettings.ServerSettingsService;
      const sessions = yield* Sessions.ProviderSessionManagerV2;
      const executor = yield* Executor.ThreadCommandExecutor;
      if (scenario === "failure after lease") {
        const get = sessions.get;
        const getSpy = vi
          .spyOn(sessions, "get")
          .mockImplementation((id, options) =>
            get(id, options).pipe(
              Effect.tap(() =>
                options?.controlScope === undefined
                  ? Effect.void
                  : Deferred.succeed(leaseAcquired, undefined).pipe(
                      Effect.andThen(Deferred.await(allowHandoffControl)),
                    ),
              ),
            ),
          );
        const projectFailure = (yield* Effect.promise(() =>
          vi.importActual<typeof Failure>("@t3tools/provider-core/server/failure"),
        )).makeProviderFailure;
        const failureSpy = vi.spyOn(Failure, "makeProviderFailure").mockImplementation((input) => {
          const result = projectFailure(input);
          // The manager projects this failure after recording ownership and
          // before release can wait on the captured handoff lease.
          if (
            injectedFailure !== undefined &&
            Cause.isCause(input.cause) &&
            Cause.squash(input.cause) === injectedFailure
          )
            Deferred.doneUnsafe(streamFailureObserved, Effect.void);
          return result;
        });
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            getSpy.mockRestore();
            failureSpy.mockRestore();
          }),
        );
      }
      const sql = yield* SqlClient.SqlClient;
      if (scenario === "handoff unresponsive") {
        const settle = vi.spyOn(sessions, "settleHandoff");
        const executor = yield* Worker.OrchestrationEffectExecutorV2;
        const execute = executor.execute;
        const execution = vi.spyOn(executor, "execute").mockImplementation((effect, options) =>
          execute(effect, options).pipe(
            Effect.onExit((exit) =>
              effect.request.type !== "provider-turn.interrupt"
                ? Effect.void
                : Effect.gen(function* () {
                    assert.isTrue(Exit.isFailure(exit));
                    assert.equal(settle.mock.calls.length, 0);
                    const projection = yield* orchestrator
                      .getThreadProjection(threadId)
                      .pipe(Effect.orDie);
                    assert.equal(projection.runs[0]?.status, "running");
                    assert.equal(starts, 1);
                    yield* Deferred.succeed(unresponsiveFinished, undefined);
                  }),
            ),
          ),
        );
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            execution.mockRestore();
            settle.mockRestore();
          }),
        );
      }
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
      const waitFor = Effect.fn("ActiveWorkspaceHandoff.waitFor")(function* (
        predicate: (value: OrchestrationV2ThreadProjection) => boolean,
      ) {
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
      if (scenario === "settlement identity guards") {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queue-identity-control"),
          threadId,
          messageId: MessageId.make("identity-control"),
          text: "queued identity control",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const projection = yield* Projection.ProjectionStoreV2;
        const context = yield* projection.getRunningTurnContext(threadId);
        assert.equal(context.run?.id, initialRun.id);
        assert.equal(context.run?.activeAttemptId, context.providerTurn?.runAttemptId);
        assert.equal(context.providerTurn?.providerThreadId, context.providerThread?.id);
        const runtime = Option.getOrThrow(
          yield* sessions.get(context.providerThread!.providerSessionId!, { touchActivity: false }),
        );
        let settlements = 0;
        const input = {
          runtime,
          threadId,
          providerThreadId: context.providerThread!.id,
          providerTurnId: context.providerTurn!.id,
          settle: Effect.sync(() => {
            settlements += 1;
          }),
        };
        const settleHandoff = sessions.settleHandoff!;
        yield* settleHandoff({ ...input, runtime: { ...runtime } });
        yield* settleHandoff({
          ...input,
          providerThreadId: ProviderThreadId.make("different-provider-thread"),
        });
        yield* settleHandoff({
          ...input,
          providerTurnId: ProviderTurnId.make("different-provider-turn"),
        });
        assert.equal(settlements, 0);
        const getContext = projection.getRunningTurnContext;
        const mismatch = vi.spyOn(projection, "getRunningTurnContext").mockImplementation((id) =>
          getContext(id).pipe(
            Effect.map((value) => ({
              ...value,
              run:
                value.run === undefined
                  ? undefined
                  : { ...value.run, activeAttemptId: RunAttemptId.make("different-attempt") },
            })),
          ),
        );
        yield* Effect.addFinalizer(() => Effect.sync(() => mismatch.mockRestore()));
        yield* settleHandoff(input);
        assert.equal(settlements, 0);
        mismatch.mockRestore();
        yield* settleHandoff(input);
        assert.equal(settlements, 1);
        const unchanged = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(unchanged.runs.find((run) => run.id === initialRun.id)?.status, "running");
        assert.equal(
          unchanged.runs.find((run) => run.userMessageId === "identity-control")?.status,
          "queued",
        );
        assert.equal(starts, 1);
        return;
      }
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
      const failStream = Effect.fn("ActiveWorkspaceHandoff.failStream")(function* () {
        const events = yield* Queue.take(failures);
        yield* Queue.fail(
          events,
          new Adapter.ProviderAdapterEventStreamError({
            driver,
            providerSessionId: running.providerThreads[0]!.providerSessionId!,
            cause: "Synthetic overlapping stream failure",
          }),
        );
        yield* Deferred.await(failureReached);
        assert.isFalse(yield* sessions.isLive(running.providerThreads[0]!.providerSessionId!));
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).runs[0]?.status,
          "running",
        );
      });
      if (scenario === "failure before planning") yield* failStream();
      assert.equal(actualConfigs.length, 1);
      assert.equal(actualConfigs[0]!.cwd, process.cwd());
      assert.isTrue(actualConfigs[0]!.baseDir.startsWith(process.env.TMPDIR!));
      const cfg = Context.get(yield* Layer.build(fixtureConfig), ServerConfig.ServerConfig);
      assert.notEqual(cfg.baseDir, actualConfigs[0]!.baseDir);
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
            NodeCrypto.layer,
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
      if (scenario === "handoff unresponsive") {
        yield* Deferred.await(unresponsiveFinished);
        return;
      }
      if (scenario === "failure after lease") {
        yield* Deferred.await(leaseAcquired);
        const events = yield* Queue.take(failures);
        injectedFailure = new Adapter.ProviderAdapterEventStreamError({
          driver,
          providerSessionId: running.providerThreads[0]!.providerSessionId!,
          cause: "Synthetic post-lease stream failure",
        });
        yield* Queue.fail(events, injectedFailure);
        yield* Deferred.await(streamFailureObserved);
        assert.isTrue(yield* sessions.isLive(running.providerThreads[0]!.providerSessionId!));
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).runs[0]?.status,
          "running",
        );
        yield* Deferred.succeed(allowHandoffControl, undefined);
      }
      if (scenario === "failure after planning") {
        yield* Deferred.await(handoffControlStarted);
        yield* failStream();
        yield* Deferred.succeed(allowHandoffControl, undefined);
      }
      if (scenario.startsWith("failure ")) {
        if (scenario === "failure after planning") yield* Deferred.await(detachStarted);
        yield* Deferred.succeed(allowFailureFinalization, undefined);
        if (scenario === "failure after planning") yield* Deferred.await(detached);
        else if (scenario !== "failure after lease") yield* Deferred.await(runtimeClosed);
        const finalized = yield* waitFor((p) =>
          p.runs.some(
            (r) =>
              r.id === initialRun.id && ["failed", "interrupted", "completed"].includes(r.status),
          ),
        );
        assert.equal(finalized.runs.find((r) => r.id === initialRun.id)?.status, "failed");
        const held = yield* waitFor((p) => p.runs.some((r) => r.queueHeld));
        const continuation = held.runs.find((r) => r.id !== initialRun.id)!;
        assert.equal(continuation.status, "queued");
        assert.isTrue(continuation.queueHeld);
        const holds = yield* sql<{
          reason: string;
        }>`SELECT reason FROM jones_queue_holds WHERE run_id = ${continuation.id}`;
        assert.equal(holds[0]?.reason, "provider_failure");
        assert.equal(starts, 1);
        return;
      }
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
