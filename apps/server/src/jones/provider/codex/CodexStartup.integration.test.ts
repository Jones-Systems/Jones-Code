import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CodexSettings,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import {
  hasUnknownRuntimeBinding,
  type ProviderAdapterV2SessionRuntime,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import type * as CodexClient from "effect-codex-app-server/client";
import * as Replay from "effect-codex-app-server/replay";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import * as ServerConfig from "../../../config.ts";
import * as Codex from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CodexTestkit from "../../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as CommandReceiptStore from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../../../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../../../orchestration-v2/EffectWorker.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "../../../orchestration-v2/ProviderSessionManager.ts";
import * as Harness from "../../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Sqlite from "../../../persistence/Sqlite.ts";
import { buildCodexInitializeParams } from "../../../provider/CodexProvider.ts";
import * as ControlAdmission from "../../orchestration/ControlAdmission.ts";
import {
  CurrentEffectClaim,
  findEffectClaimLost,
  withActiveEffectClaim,
} from "../../workspaceLifecycle/ActiveEffectClaim.ts";
import { makeCodexProducerRetirement } from "./CodexRetirement.ts";
import { CODEX_STARTUP_BUDGET_MS, makeCodexStartup } from "./CodexStartup.ts";

const database = Sqlite.layerMemory;
const testLayer = EffectOutbox.layer.pipe(Layer.provideMerge(database));

it.effect(
  "the default daemon recovers three ordinary slots before a silent producer physically exits",
  () =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const startup = makeCodexStartup();
      const scope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const producer = { generation: "silent-startup-producer", scope };
      const healthyScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(healthyScope, Exit.void));
      const healthyProducer = { generation: "independent-healthy-producer", scope: healthyScope };
      const lifecyclePermit = yield* Semaphore.make(1);
      const entered = yield* Queue.unbounded<string>();
      const sent = yield* Deferred.make<void>();
      const response = yield* Deferred.make<string>();
      const observed = yield* Deferred.make<void>();
      const physicalExit = yield* Deferred.make<void>();
      const healthyFinished = yield* Deferred.make<void>();
      let dispatches = 0;
      let healthyDispatches = 0;
      let bindings = 0;
      let closes = 0;
      const settlements = new Map<string, number>();
      const retirement = yield* makeCodexProducerRetirement(
        Effect.sync(() => closes++).pipe(Effect.andThen(Scope.close(scope, Exit.void))),
      );
      retirement.captureProcessExit(Deferred.await(physicalExit));
      yield* retirement.observeProcessExit;
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(physicalExit, undefined).pipe(Effect.asVoid),
      );
      const effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2> = Array.from(
        { length: 4 },
        (_, index) => ({
          id: `effect:startup:${index}`,
          commandId: CommandId.make(`command:startup:${index}`),
          threadId: ThreadId.make(`thread:startup:${index}`),
          request: { type: "provider-turn.start", runId: RunId.make(`run:startup:${index}`) },
        }),
      );
      const executor = Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
        execute: (effect) =>
          Effect.gen(function* () {
            yield* Queue.offer(entered, effect.id);
            if (effect.id === effects[3]!.id) {
              yield* startup
                .run(
                  "ensure",
                  startup.withinPermit(
                    startup.observe(
                      healthyProducer,
                      Effect.sync(() => ++healthyDispatches),
                    ),
                  ),
                )
                .pipe(Effect.orDie);
              yield* Deferred.succeed(healthyFinished, undefined);
              return;
            }
            const result = yield* startup
              .run(
                "ensure",
                lifecyclePermit.withPermits(1)(
                  startup.withinPermit(
                    startup
                      .observe(
                        producer,
                        Effect.sync(() => dispatches++).pipe(
                          Effect.andThen(Deferred.succeed(sent, undefined)),
                          Effect.andThen(Deferred.await(response)),
                          Effect.tap(() => Deferred.succeed(observed, undefined)),
                        ),
                      )
                      .pipe(Effect.tap(() => Effect.sync(() => bindings++))),
                  ),
                ),
              )
              .pipe(Effect.result);
            assert.equal(result._tag, "Failure");
            // A handled provider load failure settles its run and returns to the
            // worker; the real start service's terminal handling is tested separately.
            settlements.set(effect.id, (settlements.get(effect.id) ?? 0) + 1);
          }),
      });
      yield* outbox.enqueue(effects);
      const daemon = yield* EffectWorker.runDaemon.pipe(
        Effect.provide(EffectWorker.layer.pipe(Layer.provide(executor))),
        Effect.forkScoped,
      );
      const occupied = yield* Effect.forEach([0, 1, 2], () => Queue.take(entered));
      assert.sameMembers(
        occupied,
        effects.slice(0, 3).map((effect) => effect.id),
      );
      yield* Deferred.await(sent);
      yield* TestClock.adjust(0);
      assert.equal(dispatches, 1, "two claimed starts wait for the same lifecycle permit");
      assert.isFalse(yield* Deferred.isDone(healthyFinished));
      assert.equal(Option.getOrThrow(yield* outbox.get(effects[3]!.id)).attemptCount, 0);

      yield* TestClock.adjust(CODEX_STARTUP_BUDGET_MS);
      yield* Deferred.await(healthyFinished);
      yield* outbox.awaitCompletion(effects.map((effect) => effect.id));
      assert.isFalse(yield* Deferred.isDone(physicalExit));
      assert.equal(closes, 0, "elapsed time cannot close a shared producer");
      assert.isTrue(startup.isUncertain(producer));
      assert.isFalse(startup.isUncertain(healthyProducer));
      assert.equal(dispatches, 1);
      assert.equal(healthyDispatches, 1);
      for (const effect of effects) {
        const row = Option.getOrThrow(yield* outbox.get(effect.id));
        assert.equal(row.status, "succeeded");
        assert.equal(row.attemptCount, 1);
        assert.isNull(row.leaseOwner);
        if (effect.id !== effects[3]!.id) assert.equal(settlements.get(effect.id), 1);
      }

      yield* Deferred.succeed(response, "late-unbound-thread");
      yield* Deferred.await(observed);
      assert.equal(bindings, 0);
      assert.equal(dispatches, 1);
      assert.isTrue(startup.isUncertain(producer));
      const closing = yield* retirement.close.pipe(Effect.forkChild);
      yield* TestClock.adjust(0);
      assert.isUndefined(closing.pollUnsafe(), "completed scope finalizers are not physical exit");
      yield* Deferred.succeed(physicalExit, undefined);
      yield* Fiber.join(closing);
      assert.equal(closes, 1);
      yield* Fiber.interrupt(daemon);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

function replayInitialize(home: string): Array<Replay.CodexAppServerReplayEntry> {
  return [
    {
      type: "expect_outbound",
      frame: {
        id: 1,
        method: "initialize",
        params: {
          clientInfo: buildCodexInitializeParams().clientInfo,
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: ["turn/diff/updated"],
            extensions: {
              "io.modelcontextprotocol/ui": { mimeTypes: ["text/html;profile=mcp-app"] },
            },
          },
        },
      },
    },
    {
      type: "emit_inbound",
      frame: {
        id: 1,
        result: {
          userAgent: "synthetic-codex/0.144.0",
          codexHome: home,
          platformFamily: "unix",
          platformOs: "linux",
        },
      },
    },
    { type: "expect_outbound", frame: { method: "initialized" } },
  ];
}

function replayEnsure(
  id: number,
  nativeThreadId: string,
  cwd: string,
  home: string,
  label: string,
  forkSource?: string,
): Array<Replay.CodexAppServerReplayEntry> {
  return [
    {
      type: "expect_outbound",
      frame: {
        id,
        method: forkSource === undefined ? "thread/start" : "thread/fork",
        params: {
          ...(forkSource === undefined ? {} : { threadId: forkSource }),
          cwd,
          model: "gpt-5.4",
          config: Codex.CODEX_THREAD_CONFIG,
        },
      },
    },
    {
      type: "emit_inbound",
      label,
      frame: {
        id,
        result: {
          thread: {
            id: nativeThreadId,
            sessionId: nativeThreadId,
            forkedFromId: forkSource ?? null,
            preview: "",
            ephemeral: false,
            modelProvider: "openai",
            createdAt: 1782622440,
            updatedAt: 1782622440,
            status: { type: "idle" },
            path: `${home}/${nativeThreadId}.jsonl`,
            cwd,
            cliVersion: "0.144.0",
            source: "vscode",
            threadSource: null,
            agentNickname: null,
            agentRole: null,
            gitInfo: null,
            name: null,
            turns: [],
          },
          model: "gpt-5.4",
          modelProvider: "openai",
          serviceTier: null,
          cwd,
          instructionSources: [],
          approvalPolicy: "never",
          approvalsReviewer: "user",
          sandbox: { type: "dangerFullAccess" },
          reasoningEffort: null,
        },
      },
    },
  ];
}

it.effect.each(["ensure", "fork", "fork-rejected"] as const)(
  "real %s startup failure settles one durable run and daemon effect while independent Codex work progresses",
  (operation) =>
    Effect.gen(function* () {
      const fork = operation !== "ensure";
      const explicitRejection = operation === "fork-rejected";
      const cwd = yield* checkpointWorkspace("codex-startup-composition");
      const configContext = yield* Layer.build(
        ServerConfig.layerTest(cwd, { prefix: "jones-codex-startup-composition-" }),
      );
      const config = yield* ServerConfig.ServerConfig.pipe(Effect.provide(configContext));
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const crypto = yield* Crypto.Crypto;
      const home = path.join(config.baseDir, "codex-home");
      yield* fs.makeDirectory(home);
      const awaiting = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const observed = yield* Deferred.make<void>();
      const healthyAwaiting = yield* Deferred.make<void>();
      const healthyRelease = yield* Deferred.make<void>();
      const releaseClosure = yield* Deferred.make<void>();
      const observedClosure = yield* Deferred.make<void>();
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(release, undefined).pipe(
          Effect.andThen(Deferred.succeed(healthyRelease, undefined)),
          Effect.andThen(Deferred.succeed(releaseClosure, undefined)),
        ),
      );
      const boundNativeId = "startup-bound-control";
      const lateNativeId = "startup-late-unbound";
      const healthyNativeId = "startup-independent-healthy";
      const healthyTurn = {
        id: "startup-healthy-turn",
        items: [],
        itemsView: "notLoaded",
        status: "inProgress",
        error: null,
        startedAt: 1782622440,
        completedAt: null,
        durationMs: null,
      };
      const silentTranscript = yield* CodexTestkit.CodexOrchestratorReplayHarness.decodeTranscript({
        provider: "codex",
        protocol: "codex.app-server",
        version: "0.144.0",
        scenario: "startup-composition-silent",
        entries: [
          ...replayInitialize(home),
          ...replayEnsure(2, boundNativeId, cwd, home, "bound-control"),
          ...replayEnsure(
            3,
            lateNativeId,
            cwd,
            home,
            "withheld-startup",
            fork ? boundNativeId : undefined,
          ).map((entry) =>
            explicitRejection && entry.type === "emit_inbound"
              ? {
                  ...entry,
                  frame: { id: 3, error: { code: -32600, message: "Synthetic fork rejection" } },
                }
              : entry,
          ),
          {
            type: "expect_outbound",
            frame: { id: 4, method: "thread/unsubscribe", params: { threadId: boundNativeId } },
          },
          { type: "emit_inbound", frame: { id: 4, result: { status: "unsubscribed" } } },
          {
            type: "emit_inbound",
            label: "bound-thread-closed",
            frame: { method: "thread/closed", params: { threadId: boundNativeId } },
          },
        ],
      });
      const healthyTranscript = yield* CodexTestkit.CodexOrchestratorReplayHarness.decodeTranscript(
        {
          provider: "codex",
          protocol: "codex.app-server",
          version: "0.144.0",
          scenario: "startup-composition-healthy",
          entries: [
            ...replayInitialize(home),
            ...replayEnsure(2, healthyNativeId, cwd, home, "healthy-thread"),
            {
              type: "expect_outbound",
              frame: {
                id: 3,
                method: "turn/start",
                params: {
                  threadId: healthyNativeId,
                  input: [{ type: "text", text: "independent healthy turn" }],
                  cwd,
                  model: "gpt-5.4",
                  approvalPolicy: "never",
                  approvalsReviewer: "user",
                  sandboxPolicy: { type: "dangerFullAccess" },
                  summary: "detailed",
                  collaborationMode: { mode: "default", settings: { model: "gpt-5.4" } },
                },
              },
            },
            { type: "emit_inbound", frame: { id: 3, result: { turn: healthyTurn } } },
            {
              type: "emit_inbound",
              label: "healthy-turn-started",
              frame: {
                method: "turn/started",
                params: { threadId: healthyNativeId, turn: healthyTurn },
              },
            },
          ],
        },
      );
      const silentReplay = yield* Replay.makeReplayDriver(silentTranscript, {
        beforeEmitInbound: (entry) =>
          entry.label === "withheld-startup"
            ? Deferred.succeed(awaiting, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : entry.label === "bound-thread-closed"
              ? Deferred.await(releaseClosure)
              : Effect.void,
      });
      const healthyReplay = yield* Replay.makeReplayDriver(healthyTranscript, {
        beforeEmitInbound: (entry) =>
          entry.label === "healthy-turn-started"
            ? Deferred.succeed(healthyAwaiting, undefined).pipe(
                Effect.andThen(Deferred.await(healthyRelease)),
              )
            : Effect.void,
      });
      const silentInstance = ProviderInstanceId.make("codex-startup-silent");
      const healthyInstance = ProviderInstanceId.make("codex-startup-healthy");
      const requests: Array<string> = [];
      let opens = 0;
      let closes = 0;
      yield* Effect.addFinalizer((exit) =>
        Exit.isFailure(exit)
          ? Effect.gen(function* () {
              yield* Effect.logError("Codex startup composition failed", {
                requests,
                opens,
                closes,
                silentReplay: yield* Ref.get(silentReplay.state),
                healthyReplay: yield* Ref.get(healthyReplay.state),
              });
            })
          : Effect.void,
      );
      const silentFactory = CodexTestkit.makeCodexReplayClientFactory({
        transcript: silentTranscript,
        driver: silentReplay,
      });
      const registry = ProviderAdapterRegistry.layerFromAdaptersEffect(
        Effect.gen(function* () {
          const allocator = yield* IdAllocator.IdAllocatorV2;
          return yield* Effect.forEach(
            [
              {
                instanceId: silentInstance,
                clientFactory: {
                  open: (input) =>
                    silentFactory.open(input).pipe(
                      Effect.tap(() =>
                        Effect.sync(() => opens++).pipe(
                          Effect.andThen(
                            Effect.addFinalizer(() =>
                              Effect.sync(() => {
                                closes++;
                              }),
                            ),
                          ),
                        ),
                      ),
                      Effect.map((client): CodexClient.CodexAppServerClient["Service"] => ({
                        ...client,
                        handleServerNotification: (method, handler) =>
                          client.handleServerNotification(method, (payload) =>
                            handler(payload).pipe(
                              Effect.tap(() =>
                                method === "thread/closed"
                                  ? Deferred.succeed(observedClosure, undefined)
                                  : Effect.void,
                              ),
                            ),
                          ),
                        request: (method, params) =>
                          Effect.sync(() => requests.push(method)).pipe(
                            Effect.andThen(client.request(method, params)),
                            Effect.onExit(() =>
                              method === (fork ? "thread/fork" : "thread/start") &&
                              requests.length === 3
                                ? Deferred.succeed(observed, undefined)
                                : Effect.void,
                            ),
                          ),
                      })),
                    ),
                } satisfies Codex.CodexAppServerClientFactoryShape,
              },
              {
                instanceId: healthyInstance,
                clientFactory: CodexTestkit.makeCodexReplayClientFactory({
                  transcript: healthyTranscript,
                  driver: healthyReplay,
                }),
              },
            ],
            ({ instanceId, clientFactory }) =>
              Codex.makeCodexAdapterV2({
                instanceId,
                clientFactory,
                settings: Schema.decodeSync(CodexSettings)({ homePath: home }),
                environment: { CODEX_HOME: home },
                crypto,
                fileSystem: fs,
                idAllocator: allocator,
                serverConfig: config,
              }),
          );
        }),
      ).pipe(
        Layer.provide(IdAllocator.layer),
        Layer.provide(ControlAdmission.layer),
        Layer.provide(NodeServices.layer),
      );
      const runtime = Harness.layerWithRegistryAndServices(
        { name: "codex-startup-composition", runtimePolicyOverride: { cwd } },
        registry,
        {
          serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, config),
          databaseLayer: Sqlite.layerFromPath(config.dbPath).pipe(
            Layer.provide(NodeServices.layer),
          ),
        },
      );
      const services = Layer.mergeAll(
        runtime,
        CommandReceiptStore.layer.pipe(Layer.provide(runtime)),
        EventStore.layer.pipe(Layer.provide(runtime)),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const sink = yield* EventSink.EventSinkV2;
        const store = yield* EventStore.EventStoreV2;
        const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const boundThreadId = ThreadId.make("startup-composition-bound");
        const failedThreadId = ThreadId.make("startup-composition-failed");
        const healthyThreadId = ThreadId.make("startup-composition-healthy");
        const selection = { instanceId: silentInstance, model: "gpt-5.4" };
        const policy = { cwd, runtimeMode: "full-access", interactionMode: "default" } as const;
        for (const [threadId, instanceId] of [
          [boundThreadId, silentInstance],
          [failedThreadId, silentInstance],
          [healthyThreadId, healthyInstance],
        ] as const) {
          if (fork && threadId === failedThreadId) continue;
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`${threadId}:create`),
            threadId,
            projectId: ProjectId.make("startup-composition"),
            title: "Synthetic startup composition",
            modelSelection: { instanceId, model: selection.model },
            runtimeMode: policy.runtimeMode,
            interactionMode: policy.interactionMode,
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
        }
        const session = yield* sessions.open({
          threadId: boundThreadId,
          providerSessionId: IdAllocator.derive.providerSession({
            providerInstanceId: silentInstance,
          }),
          modelSelection: selection,
          runtimePolicy: policy,
        });
        const bound = yield* session.ensureThread({
          threadId: boundThreadId,
          modelSelection: selection,
          runtimePolicy: policy,
        });
        assert.equal(bound.nativeThreadRef?.nativeId, boundNativeId);
        if (fork) {
          const now = yield* DateTime.now;
          const sourceRunId = RunId.make("startup-composition-source-run");
          yield* sink.write({
            events: [
              {
                id: EventId.make("startup-composition-source-thread"),
                type: "provider-thread.updated",
                threadId: boundThreadId,
                occurredAt: now,
                payload: bound,
              },
              {
                id: EventId.make("startup-composition-source-run"),
                type: "run.created",
                threadId: boundThreadId,
                runId: sourceRunId,
                occurredAt: now,
                payload: {
                  id: sourceRunId,
                  threadId: boundThreadId,
                  ordinal: 1,
                  providerInstanceId: silentInstance,
                  modelSelection: selection,
                  providerThreadId: bound.id,
                  userMessageId: MessageId.make("startup-composition-source-message"),
                  rootNodeId: null,
                  activeAttemptId: null,
                  status: "completed",
                  queuePosition: null,
                  requestedAt: now,
                  startedAt: now,
                  completedAt: now,
                  checkpointId: null,
                  contextHandoffId: null,
                },
              },
            ],
          });
          yield* orchestrator.dispatch({
            type: "thread.fork",
            commandId: CommandId.make("startup-composition:fork"),
            sourceThreadId: boundThreadId,
            targetThreadId: failedThreadId,
            sourcePoint: { type: "run", runId: sourceRunId },
            createdBy: "user",
            creationSource: "web",
          });
        }
        const command = {
          type: "message.dispatch",
          commandId: CommandId.make("startup-composition:message"),
          threadId: failedThreadId,
          messageId: MessageId.make("startup-composition:message"),
          text: "must never reach turn/start",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          dispatchMode: { type: "start_immediately" },
        } as const;
        yield* orchestrator.dispatch(command);
        yield* Deferred.await(awaiting);
        const admitted = yield* orchestrator.getThreadProjection(failedThreadId);
        assert.lengthOf(admitted.runs, 1);
        assert.lengthOf(admitted.attempts, 1);
        const run = admitted.runs[0]!;
        const attempt = admitted.attempts[0]!;
        assert.equal(run.status, "starting");
        assert.equal(run.activeAttemptId, attempt.id);
        const admittedEffects = yield* outbox.listByCommandId(command.commandId);
        const starts = admittedEffects.filter(
          (effect) => effect.request.type === "provider-turn.start",
        );
        assert.lengthOf(starts, 1);
        const claimed = Option.getOrThrow(yield* outbox.get(starts[0]!.id));
        assert.equal(claimed.status, "running");
        assert.equal(claimed.attemptCount, 1);
        assert.isNotNull(claimed.leaseOwner);
        const receipt = Option.getOrThrow(yield* receipts.getByCommandId(command.commandId));
        assert.equal(receipt.status, "accepted");
        const failedRun = yield* sink.stream({ threadId: failedThreadId }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === run.id &&
              event.payload.status === "failed",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped({ startImmediately: true }),
        );
        const failedAttempt = yield* sink.stream({ threadId: failedThreadId }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run-attempt.updated" &&
              event.payload.id === attempt.id &&
              event.payload.status === "failed",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped({ startImmediately: true }),
        );
        if (explicitRejection) {
          yield* Deferred.succeed(release, undefined);
          yield* Deferred.await(observed);
        }
        yield* TestClock.adjust(CODEX_STARTUP_BUDGET_MS);
        yield* Fiber.join(failedRun);
        yield* Fiber.join(failedAttempt);
        yield* outbox.awaitCompletion([claimed.id]);
        const released = Option.getOrThrow(yield* outbox.get(claimed.id));
        assert.equal(released.status, "succeeded");
        assert.equal(released.attemptCount, 1);
        assert.isNull(released.leaseOwner);
        const failed = yield* orchestrator.getThreadProjection(failedThreadId);
        assert.equal(failed.runs[0]?.id, run.id);
        assert.equal(failed.runs[0]?.status, "failed");
        assert.equal(failed.attempts[0]?.id, attempt.id);
        assert.equal(failed.attempts[0]?.status, "failed");
        assert.lengthOf(failed.providerTurns, 0);
        assert.isTrue(failed.providerThreads.every((thread) => thread.nativeThreadRef === null));
        assert.lengthOf(
          failed.turnItems.filter((item) => item.runId === run.id && item.type === "error"),
          1,
        );

        const healthyCommand = {
          ...command,
          commandId: CommandId.make("startup-composition:healthy-message"),
          threadId: healthyThreadId,
          messageId: MessageId.make("startup-composition:healthy-message"),
          text: "independent healthy turn",
        };
        yield* orchestrator.dispatch(healthyCommand);
        yield* Deferred.await(healthyAwaiting);
        const healthyAdmitted = yield* orchestrator.getThreadProjection(healthyThreadId);
        assert.lengthOf(healthyAdmitted.runs, 1);
        const healthyRun = healthyAdmitted.runs[0]!;
        const healthyStarted = yield* sink.stream({ threadId: healthyThreadId }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "provider-turn.updated" &&
              event.payload.runAttemptId === healthyRun.activeAttemptId &&
              event.payload.status === "running",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped({ startImmediately: true }),
        );
        yield* Deferred.succeed(healthyRelease, undefined);
        yield* Fiber.join(healthyStarted);
        const healthyStarts = (yield* outbox.listByCommandId(healthyCommand.commandId)).filter(
          (effect) => effect.request.type === "provider-turn.start",
        );
        assert.lengthOf(healthyStarts, 1);
        yield* outbox.awaitCompletion([healthyStarts[0]!.id]);
        const healthy = yield* orchestrator.getThreadProjection(healthyThreadId);
        assert.equal(healthy.runs[0]?.status, "running");
        assert.equal(healthy.attempts[0]?.id, healthyRun.activeAttemptId);
        assert.equal(healthy.attempts[0]?.status, "running");
        assert.equal(healthy.providerThreads[0]?.nativeThreadRef?.nativeId, healthyNativeId);
        assert.lengthOf(healthy.providerTurns, 1);
        assert.equal(yield* Deferred.isDone(observed), explicitRejection);
        assert.equal(opens, 1);
        assert.equal(closes, 0);

        yield* Deferred.succeed(release, undefined);
        yield* Deferred.await(observed);
        if (!explicitRejection) {
          const refused = yield* session
            .ensureThread({
              threadId: failedThreadId,
              modelSelection: selection,
              runtimePolicy: policy,
            })
            .pipe(Effect.flip);
          assert.isTrue(hasUnknownRuntimeBinding(refused));
        }
        assert.isDefined(session.unloadThread);
        yield* session.unloadThread!({ providerThread: bound });
        assert.isFalse(yield* Deferred.isDone(observedClosure));
        yield* Deferred.succeed(releaseClosure, undefined);
        yield* Deferred.await(observedClosure);
        yield* orchestrator.dispatch(command);
        const after = yield* orchestrator.getThreadProjection(failedThreadId);
        assert.deepEqual(after.runs, failed.runs);
        assert.deepEqual(after.attempts, failed.attempts);
        assert.deepEqual(after.providerThreads, failed.providerThreads);
        assert.lengthOf(after.providerTurns, 0);
        assert.deepEqual(
          Option.getOrThrow(yield* receipts.getByCommandId(command.commandId)),
          receipt,
        );
        assert.sameMembers(
          (yield* outbox.listByCommandId(command.commandId)).map((effect) => effect.id),
          admittedEffects.map((effect) => effect.id),
        );
        const events = yield* store.read({ threadId: failedThreadId }).pipe(Stream.runCollect);
        assert.lengthOf(
          events.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === run.id &&
              event.payload.status === "failed",
          ),
          1,
        );
        assert.lengthOf(
          events.filter(
            ({ event }) =>
              event.type === "run-attempt.updated" &&
              event.payload.id === attempt.id &&
              event.payload.status === "failed",
          ),
          1,
        );
        for (const effectId of [claimed.id, healthyStarts[0]!.id]) {
          const settled = Option.getOrThrow(yield* outbox.get(effectId));
          assert.equal(settled.status, "succeeded");
          assert.equal(settled.attemptCount, 1);
          assert.isNull(settled.leaseOwner);
        }
        assert.deepEqual(requests, [
          "initialize",
          "thread/start",
          fork ? "thread/fork" : "thread/start",
          "thread/unsubscribe",
        ]);
        assert.equal(opens, 1);
        assert.equal(closes, 0);
        for (const replay of [silentReplay, healthyReplay]) {
          const state = yield* Ref.get(replay.state);
          assert.isNull(state.failure);
          assert.equal(state.cursor, replay.transcript.entries.length);
        }
      }).pipe(Effect.provide(services));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each(["local-recovery", "unknown-steer", "cancelled-cache-handoff"] as const)(
  "a new ordinary send reconciles a preboundary native creation before admission: %s",
  (mode) =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("codex-publication-new-send");
      const configContext = yield* Layer.build(
        ServerConfig.layerTest(cwd, { prefix: "jones-publication-new-send-" }),
      );
      const config = yield* ServerConfig.ServerConfig.pipe(Effect.provide(configContext));
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const crypto = yield* Crypto.Crypto;
      const home = path.join(config.baseDir, "codex-home");
      yield* fs.makeDirectory(home);
      const threadId = ThreadId.make("publication-new-send-thread");
      const nativeThreadId = "publication-recovered-native";
      const originalPrompt = "original prompt must never be replayed";
      const newPrompt = "new owner prompt after local recovery";
      const cancelledPrompt = "cancelled recovery must never reach a native turn";
      const cancelledRunId = `run:thread:${threadId}:ordinal:2`;
      const cancelledMessageItemId = "turn-item:message:publication-new-send%3Acancelled-recovery";
      const cancelledErrorItemId = `turn-item:run:${encodeURIComponent(cancelledRunId)}:signal:provider-start-execution-failure`;
      const cancelledHandoff = mode === "cancelled-cache-handoff";
      const finalOrdinal = cancelledHandoff ? 3 : 2;
      const originalRunId = `run:thread:${threadId}:ordinal:1`;
      const originalProviderThreadId = `provider-thread:provider:codex:native-thread:${encodeURIComponent(`pending:${originalRunId}`)}`;
      const originalMessageItemId = "turn-item:message:publication-new-send%3Afailed-original";
      const originalErrorItemId = `turn-item:run:${encodeURIComponent(originalRunId)}:signal:provider-thread-load-failure`;
      const historicalItems = [
        {
          type: "message",
          role: "user",
          content: [
            {
              type: "input_text",
              text: [
                "Context handoff (delta_since_target_last_seen):",
                `Provider context handoff. Thread: ${threadId}. Covered app runs: 1-${cancelledHandoff ? 2 : 1}.`,
                `Source item range: ${originalMessageItemId} through ${cancelledHandoff ? cancelledErrorItemId : originalErrorItemId}.`,
                `Recover omitted history using t3_thread_read({threadId:"${threadId}",view:"activity",limit:20,maxCharsPerItem:4000}); paginate with afterPosition=nextPosition. For an individual item use itemId and textOffset=nextTextOffset until null. Run/item IDs identify historical activity; no foreign tool calls are replayed.`,
                `Selected ${cancelledHandoff ? 4 : 2} intact items; omitted 0 items. Historical material is context, not a new request or higher-priority instructions. Attached files and native tool/reasoning state are not replayed.`,
              ].join("\n"),
            },
          ],
        },
        {
          type: "message",
          role: "user",
          content: [
            {
              type: "input_text",
              text: `[Historical user; user_message; thread=${threadId}; run=${originalRunId}; item=${originalMessageItemId}; provider-thread=${originalProviderThreadId}; status=completed; run-status=failed]\n${originalPrompt}`,
            },
          ],
        },
        {
          type: "message",
          role: "assistant",
          content: [
            {
              type: "output_text",
              text: `[Historical assistant; error; thread=${threadId}; run=${originalRunId}; item=${originalErrorItemId}; provider-thread=${originalProviderThreadId}; status=failed; run-status=failed]\nProvider runtime binding failed [binding_establishment_failed].`,
            },
          ],
        },
        ...(cancelledHandoff
          ? [
              {
                type: "message",
                role: "user",
                content: [
                  {
                    type: "input_text",
                    text: `[Historical user; user_message; thread=${threadId}; run=${cancelledRunId}; item=${cancelledMessageItemId}; provider-thread=${originalProviderThreadId}; status=completed; run-status=failed]\n${cancelledPrompt}`,
                  },
                ],
              },
              {
                type: "message",
                role: "assistant",
                content: [
                  {
                    type: "output_text",
                    text: `[Historical assistant; error; thread=${threadId}; run=${cancelledRunId}; item=${cancelledErrorItemId}; provider-thread=${originalProviderThreadId}; status=failed; run-status=failed]\nProvider start could not retain its original execution authority.`,
                  },
                ],
              },
            ]
          : []),
      ];
      const turn = {
        id: "publication-new-turn",
        items: [],
        itemsView: "notLoaded",
        status: "inProgress",
        error: null,
        startedAt: 1782622440,
        completedAt: null,
        durationMs: null,
      };
      const transcript = yield* CodexTestkit.CodexOrchestratorReplayHarness.decodeTranscript({
        provider: "codex",
        protocol: "codex.app-server",
        version: "0.144.0",
        scenario: "publication-new-send",
        entries: [
          ...replayInitialize(home),
          ...replayEnsure(2, nativeThreadId, cwd, home, "native-success-before-publication"),
          ...(mode === "unknown-steer"
            ? []
            : ([
                {
                  type: "expect_outbound",
                  frame: {
                    id: 3,
                    method: "thread/inject_items",
                    params: { threadId: nativeThreadId, items: historicalItems },
                  },
                },
                { type: "emit_inbound", frame: { id: 3, result: {} } },
                {
                  type: "expect_outbound",
                  frame: {
                    id: 4,
                    method: "turn/start",
                    params: {
                      threadId: nativeThreadId,
                      input: [{ type: "text", text: newPrompt }],
                      cwd,
                      model: "gpt-5.4",
                      approvalPolicy: "never",
                      approvalsReviewer: "user",
                      sandboxPolicy: { type: "dangerFullAccess" },
                      summary: "detailed",
                      collaborationMode: { mode: "default", settings: { model: "gpt-5.4" } },
                    },
                  },
                },
                { type: "emit_inbound", frame: { id: 4, result: { turn } } },
                {
                  type: "emit_inbound",
                  frame: { method: "turn/started", params: { threadId: nativeThreadId, turn } },
                },
                {
                  type: "emit_inbound",
                  frame: {
                    method: "turn/completed",
                    params: {
                      threadId: nativeThreadId,
                      turn: {
                        ...turn,
                        status: "completed",
                        completedAt: 1782622441,
                        durationMs: 1000,
                      },
                    },
                  },
                },
              ] satisfies Array<Replay.CodexAppServerReplayEntry>)),
        ],
      });
      const replay = yield* Replay.makeReplayDriver(transcript);
      const requests: Array<{ readonly method: string; readonly params: unknown }> = [];
      const factory = CodexTestkit.makeCodexReplayClientFactory({ transcript, driver: replay });
      const instanceId = ProviderInstanceId.make("codex-publication-new-send");
      const cacheHandoffEntered = yield* Deferred.make<void>();
      let pausedCacheHandoff = false;
      const publicationLookups: Array<"completed" | "absent"> = [];
      let admission: ControlAdmission.ControlAdmission["Service"] | undefined;
      const registry = ProviderAdapterRegistry.layerFromAdaptersEffect(
        Effect.gen(function* () {
          admission = yield* ControlAdmission.ControlAdmission;
          const allocator = yield* IdAllocator.IdAllocatorV2;
          const settings = yield* Schema.decodeEffect(CodexSettings)({ homePath: home });
          const adapter = yield* Codex.makeCodexAdapterV2({
            instanceId,
            settings,
            environment: { CODEX_HOME: home },
            crypto,
            fileSystem: fs,
            idAllocator: allocator,
            serverConfig: config,
            clientFactory: {
              open: (input) =>
                factory.open(input).pipe(
                  Effect.map((client): CodexClient.CodexAppServerClient["Service"] => ({
                    ...client,
                    request: (method, params) =>
                      Effect.sync(() => {
                        requests.push({ method, params });
                      }).pipe(Effect.andThen(client.request(method, params))),
                  })),
                ),
            },
          });
          return [
            cancelledHandoff
              ? {
                  ...adapter,
                  openSession: Effect.fn("CodexStartupTest.openCancellationSession")(function* (
                    input: Parameters<typeof adapter.openSession>[0],
                  ) {
                    const runtime = yield* adapter.openSession(input);
                    return {
                      ...runtime,
                      reconcileBindingPublication: Effect.fn(
                        "CodexStartupTest.pauseCompletedPublication",
                      )(function* (
                        value: Parameters<
                          NonNullable<
                            ProviderAdapterV2SessionRuntime["reconcileBindingPublication"]
                          >
                        >[0],
                      ) {
                        const completed = yield* runtime.reconcileBindingPublication!(value);
                        publicationLookups.push(completed === null ? "absent" : "completed");
                        if (completed !== null && !pausedCacheHandoff) {
                          pausedCacheHandoff = true;
                          yield* Deferred.succeed(cacheHandoffEntered, undefined);
                          yield* Effect.never;
                        }
                        return completed;
                      }),
                    };
                  }),
                }
              : adapter,
          ];
        }),
      ).pipe(
        Layer.provide(IdAllocator.layer),
        Layer.provide(ControlAdmission.layer),
        Layer.provide(NodeServices.layer),
      );
      let publicationFailures = 0;
      const faultStore = Layer.effect(
        EventStore.EventStoreV2,
        Effect.gen(function* () {
          const delegate = yield* EventStore.EventStoreV2;
          return EventStore.EventStoreV2.of({
            ...delegate,
            append: (input) =>
              Effect.suspend(() => {
                const boundary = input.events.some(
                  (event) =>
                    event.type === "provider-thread.updated" &&
                    event.payload.nativeThreadRef?.nativeId === nativeThreadId &&
                    event.payload.runtimeIdentity?.runtimeGeneration !== undefined,
                );
                if (boundary && publicationFailures === 0) {
                  publicationFailures++;
                  return Effect.fail(
                    new EventStore.EventStoreAppendEventsError({
                      eventCount: input.events.length,
                      cause: "synthetic failure before boundary commit",
                    }),
                  );
                }
                return delegate.append(input);
              }),
          });
        }),
      ).pipe(Layer.provide(EventStore.layer));
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const selection = { instanceId, model: "gpt-5.4" };
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("publication-new-send:create"),
          threadId,
          projectId: ProjectId.make("publication-new-send-project"),
          title: "Publication recovery",
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const send = (key: string, text: string) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`publication-new-send:${key}`),
            threadId,
            messageId: MessageId.make(`publication-new-send:${key}`),
            text,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          });
        const steer =
          mode === "unknown-steer"
            ? yield* admission!.begin({
                instanceId,
                threadId,
                providerSessionId: "earlier-control-session",
                runtimeGeneration: "earlier-control-generation",
                providerThreadId: "earlier-control-thread",
                providerTurnId: "earlier-control-turn",
                nativeThreadId,
                nativeTurnId: "earlier-native-turn",
              })
            : undefined;
        yield* send("failed-original", originalPrompt);
        yield* worker.drain();
        const failed = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(publicationFailures, 1);
        assert.equal(failed.runs[0]?.status, "failed");
        assert.isTrue(
          failed.providerThreads.every((provider) => provider.nativeThreadRef === null),
        );
        assert.deepEqual(
          requests.map((request) => request.method),
          ["initialize", "thread/start"],
        );
        const originalSessionId = failed.providerThreads[0]!.providerSessionId!;
        assert.isTrue(yield* sessions.isLive(originalSessionId));
        if (steer !== undefined) yield* admission!.markUnknown(steer);
        if (cancelledHandoff) {
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
          yield* send("cancelled-recovery", cancelledPrompt);
          const claimed = Option.getOrThrow(
            yield* outbox.claimNext({
              workerId: "publication-cancelled-worker",
              leaseDurationMs: 30_000,
              excludeTitleGeneration: true,
            }),
          );
          assert.equal(claimed.request.type, "provider-turn.start");
          if (claimed.request.type !== "provider-turn.start")
            return yield* Effect.die("The cancellation fixture did not claim its ordinary send.");
          assert.equal(claimed.request.runId, cancelledRunId);
          const capturedClaim = yield* Deferred.make<CurrentEffectClaim["Service"]>();
          const cancelled = yield* withActiveEffectClaim(outbox, claimed, 30_000, true, (claim) =>
            Deferred.succeed(capturedClaim, claim).pipe(Effect.andThen(executor.execute(claimed))),
          ).pipe(Effect.exit, Effect.forkScoped({ startImmediately: true }));
          yield* Deferred.await(cacheHandoffEntered);
          const claim = yield* Deferred.await(capturedClaim);
          yield* claim.invalidate("synthetic claim loss after completed publication");
          const cancelledExit = yield* Fiber.join(cancelled);
          assert.isTrue(Exit.isFailure(cancelledExit));
          if (Exit.isFailure(cancelledExit))
            assert.isDefined(findEffectClaimLost(cancelledExit.cause));
          assert.isTrue(
            yield* outbox.fail({
              effectId: claimed.id,
              workerId: "publication-cancelled-worker",
              claim: claimed,
              error: "synthetic claim loss after completed publication",
            }),
          );
          const cancelledProjection = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(cancelledProjection.runs[1]?.status, "failed");
          assert.deepEqual(
            requests.map((request) => request.method),
            ["initialize", "thread/start"],
          );
        }
        const terminal =
          mode !== "unknown-steer"
            ? yield* orchestrator.streamStoredEvents.pipe(
                Stream.filter(
                  ({ event }) =>
                    event.type === "run.updated" &&
                    event.threadId === threadId &&
                    event.payload.ordinal === finalOrdinal &&
                    ["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(
                      event.payload.status,
                    ),
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped({ startImmediately: true }),
              )
            : undefined;
        yield* send("new-owner", newPrompt);
        yield* worker.drain();
        if (terminal !== undefined) {
          yield* Fiber.join(terminal);
          yield* worker.drain();
        }
        const recovered = yield* orchestrator.getThreadProjection(threadId);
        const replayState = yield* Ref.get(replay.state);
        const recoveryDiagnostic = yield* Schema.encodeEffect(
          Schema.fromJsonString(Schema.Unknown),
        )({
          run: recovered.runs[finalOrdinal - 1],
          errors: recovered.turnItems.flatMap((item) =>
            item.runId === recovered.runs[finalOrdinal - 1]?.id && item.type === "error"
              ? [{ title: item.title, failure: item.failure }]
              : [],
          ),
          requests,
          replay: {
            ...replayState,
            failureMessage: replayState.failure?.message ?? null,
            entries: replay.transcript.entries.length,
          },
        });
        assert.equal(recovered.runs[0]?.status, "failed");
        assert.equal(
          recovered.runs[finalOrdinal - 1]?.status,
          mode === "unknown-steer" ? "failed" : "completed",
          recoveryDiagnostic,
        );
        assert.notEqual(recovered.runs[finalOrdinal - 1]?.id, recovered.runs[0]?.id);
        assert.equal(
          recovered.providerThreads.find(
            (provider) => provider.id === recovered.runs[finalOrdinal - 1]?.providerThreadId,
          )?.nativeThreadRef?.nativeId,
          nativeThreadId,
        );
        if (cancelledHandoff) {
          assert.deepEqual(publicationLookups, ["completed", "absent"]);
          assert.equal(recovered.runs[1]?.status, "failed");
          assert.notEqual(recovered.runs[2]?.id, recovered.runs[1]?.id);
        }
        assert.deepEqual(
          requests.map((request) => request.method),
          mode === "unknown-steer"
            ? ["initialize", "thread/start"]
            : ["initialize", "thread/start", "thread/inject_items", "turn/start"],
        );
        if (steer !== undefined) {
          assert.deepEqual(yield* admission!.snapshot(instanceId), [
            { binding: steer.binding, unknown: true },
          ]);
          assert.isTrue(
            recovered.turnItems.some(
              (item) => item.type === "error" && item.title === "Provider work is held",
            ),
          );
        }
        assert.isNull(replayState.failure);
        assert.equal(replayState.cursor, replay.transcript.entries.length);
      }).pipe(
        Effect.provide(
          Harness.layerWithRegistryAndServices(
            { name: "publication-new-send", runtimePolicyOverride: { cwd } },
            registry,
            {
              serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, config),
              eventStoreLayer: faultStore,
              ...(cancelledHandoff ? { runEffectWorker: false } : {}),
            },
          ),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
