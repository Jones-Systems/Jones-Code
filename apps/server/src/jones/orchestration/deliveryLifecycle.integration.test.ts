import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CodexSettings,
  CommandId,
  MessageId,
  PlanId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
  type RunId,
} from "@t3tools/contracts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import type * as CodexClient from "effect-codex-app-server/client";
import * as Replay from "effect-codex-app-server/replay";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ServerConfig from "../../config.ts";
import * as Git from "../../git/GitWorkflowService.ts";
import * as Codex from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CodexTestkit from "../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Launch from "../../orchestration-v2/ThreadLaunchService.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Intake from "../../orchestration-v2/ThreadMessageIntake.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Database from "../../persistence/Sqlite.ts";
import * as Folders from "../../project/ManagedProjectFolders.ts";
import * as Clones from "../../project/ProjectCloneTracker.ts";
import * as Projects from "../../project/ProjectService.ts";
import * as Setup from "../../project/ProjectSetupScriptRunner.ts";
import * as SetupTracker from "../../project/WorktreeSetupTracker.ts";
import { buildCodexInitializeParams } from "../../provider/CodexProvider.ts";
import * as DocumentText from "../../provider/DocumentText.ts";
import * as Providers from "../../provider/ProviderRegistry.ts";
import * as Settings from "../../serverSettings.ts";
import * as Terminals from "../../terminal/Manager.ts";
import * as Text from "../../textGeneration/TextGeneration.ts";
import * as ControlAdmission from "./ControlAdmission.ts";
import * as Metadata from "./threadMetadataReads.ts";

const projectId = ProjectId.make("project:delivery-lifecycle");
const instanceA = ProviderInstanceId.make("codex-delivery-a");
const instanceB = ProviderInstanceId.make("codex-delivery-b");
const selection: ModelSelection = {
  instanceId: instanceA,
  model: "gpt-5.4",
  options: [{ id: "reasoningEffort", value: "low" }],
};
const switched: ModelSelection = {
  instanceId: instanceB,
  model: "gpt-6",
  options: [{ id: "reasoningEffort", value: "high" }],
};
const nativeId = "native-delivery-lifecycle";

function initialize(home: string): Array<Replay.CodexAppServerReplayEntry> {
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
          userAgent: "Synthetic Codex",
          codexHome: home,
          platformFamily: "unix",
          platformOs: "macos",
        },
      },
    },
    { type: "expect_outbound", frame: { method: "initialized" } },
  ];
}

function nativeThread(cwd: string, home: string, model: string) {
  return {
    thread: {
      id: nativeId,
      sessionId: nativeId,
      forkedFromId: null,
      preview: "",
      ephemeral: false,
      modelProvider: "openai",
      createdAt: 1782622440,
      updatedAt: 1782622440,
      status: { type: "idle" },
      path: `${home}/${nativeId}.jsonl`,
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
    model,
    modelProvider: "openai",
    serviceTier: null,
    cwd,
    instructionSources: [],
    approvalPolicy: "never",
    approvalsReviewer: "user",
    sandbox: { type: "dangerFullAccess" },
    reasoningEffort: null,
  };
}

function turn(id: string, status: "inProgress" | "completed" | "interrupted") {
  return {
    id,
    items: [],
    itemsView: "notLoaded",
    status,
    error: null,
    startedAt: 1782622440,
    completedAt: status === "inProgress" ? null : 1782622450,
    durationMs: null,
  };
}

function startTurn(
  id: number,
  ordinal: number,
  cwd: string,
  target: ModelSelection,
  text: string,
): Array<Replay.CodexAppServerReplayEntry> {
  const effort = target.options?.find((option) => option.id === "reasoningEffort")?.value;
  const started = turn(`delivery-turn-${ordinal}`, "inProgress");
  return [
    {
      type: "expect_outbound",
      frame: {
        id,
        method: "turn/start",
        params: {
          threadId: nativeId,
          input: [{ type: "text", text }],
          cwd,
          model: target.model,
          effort,
          approvalPolicy: "never",
          approvalsReviewer: "user",
          sandboxPolicy: { type: "dangerFullAccess" },
          summary: "detailed",
          collaborationMode: {
            mode: "default",
            settings: { model: target.model, reasoning_effort: effort },
          },
        },
      },
    },
    { type: "emit_inbound", frame: { id, result: { turn: started } } },
    {
      type: "emit_inbound",
      frame: { method: "turn/started", params: { threadId: nativeId, turn: started } },
    },
  ];
}

const terminal = (
  ordinal: number,
  status: "completed" | "interrupted",
  label?: string,
): Replay.CodexAppServerReplayEntry => ({
  type: "emit_inbound",
  ...(label === undefined ? {} : { label }),
  frame: {
    method: "turn/completed",
    params: { threadId: nativeId, turn: turn(`delivery-turn-${ordinal}`, status) },
  },
});

const fixture = (runEffectWorker: boolean) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace("jones-delivery-lifecycle");
    const configContext = yield* Layer.build(
      ServerConfig.layerTest(cwd, { prefix: "jones-delivery-lifecycle-" }),
    );
    const config = yield* ServerConfig.ServerConfig.pipe(Effect.provide(configContext));
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;
    const home = path.join(config.baseDir, "synthetic-codex-home");
    yield* fs.makeDirectory(home);
    const firstComplete = yield* Deferred.make<void>();
    const secondComplete = yield* Deferred.make<void>();
    yield* Effect.addFinalizer(() =>
      Deferred.succeed(firstComplete, undefined).pipe(
        Effect.andThen(Deferred.succeed(secondComplete, undefined)),
        Effect.asVoid,
      ),
    );
    const requests: Array<{ instanceId: ProviderInstanceId; method: string; params: unknown }> = [];
    const transcriptA = yield* CodexTestkit.CodexOrchestratorReplayHarness.decodeTranscript({
      provider: "codex",
      protocol: "codex.app-server",
      version: "0.144.0",
      scenario: "delivery-a",
      entries: [
        ...initialize(home),
        {
          type: "expect_outbound",
          frame: {
            id: 2,
            method: "thread/start",
            params: { cwd, model: selection.model, config: Codex.CODEX_THREAD_CONFIG },
          },
        },
        {
          type: "emit_inbound",
          frame: { id: 2, result: nativeThread(cwd, home, selection.model) },
        },
        ...startTurn(3, 1, cwd, selection, "First"),
        terminal(1, "completed", "complete-first"),
        ...startTurn(4, 2, cwd, selection, "Second"),
        terminal(2, "completed", "complete-second"),
        {
          type: "expect_outbound",
          frame: { id: 5, method: "thread/unsubscribe", params: { threadId: nativeId } },
        },
        { type: "emit_inbound", frame: { id: 5, result: { status: "unsubscribed" } } },
        {
          type: "emit_inbound",
          frame: { method: "thread/closed", params: { threadId: nativeId } },
        },
      ],
    });
    const transcriptB = yield* CodexTestkit.CodexOrchestratorReplayHarness.decodeTranscript({
      provider: "codex",
      protocol: "codex.app-server",
      version: "0.144.0",
      scenario: "delivery-b",
      entries: [
        ...initialize(home),
        {
          type: "expect_outbound",
          frame: {
            id: 2,
            method: "thread/resume",
            params: {
              threadId: nativeId,
              excludeTurns: true,
              cwd,
              model: switched.model,
              config: Codex.CODEX_THREAD_CONFIG,
            },
          },
        },
        { type: "emit_inbound", frame: { id: 2, result: nativeThread(cwd, home, switched.model) } },
        ...startTurn(3, 3, cwd, switched, "Third"),
        {
          type: "expect_outbound",
          frame: {
            id: 4,
            method: "turn/interrupt",
            params: { threadId: nativeId, turnId: "delivery-turn-3" },
          },
        },
        { type: "emit_inbound", frame: { id: 4, result: {} } },
        terminal(3, "interrupted"),
      ],
    });
    const replayA = yield* Replay.makeReplayDriver(transcriptA, {
      beforeEmitInbound: (entry) =>
        entry.label === "complete-first"
          ? Deferred.await(firstComplete)
          : entry.label === "complete-second"
            ? Deferred.await(secondComplete)
            : Effect.void,
    });
    const replayB = yield* Replay.makeReplayDriver(transcriptB);
    yield* Effect.addFinalizer((exit) =>
      Exit.isFailure(exit)
        ? Effect.gen(function* () {
            yield* Effect.logError("Delivery lifecycle failed", {
              requests,
              replayA: yield* Ref.get(replayA.state),
              replayB: yield* Ref.get(replayB.state),
            });
          })
        : Effect.void,
    );
    const nativeRegistry = Registry.layerFromAdaptersEffect(
      Effect.gen(function* () {
        const allocator = yield* IdAllocator.IdAllocatorV2;
        return yield* Effect.forEach(
          [
            { instanceId: instanceA, transcript: transcriptA, driver: replayA },
            { instanceId: instanceB, transcript: transcriptB, driver: replayB },
          ],
          ({ instanceId, transcript, driver }) => {
            const factory = CodexTestkit.makeCodexReplayClientFactory({ transcript, driver });
            return Codex.makeCodexAdapterV2({
              instanceId,
              settings: Schema.decodeSync(CodexSettings)({ homePath: home }),
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
                        Effect.sync(() => requests.push({ instanceId, method, params })).pipe(
                          Effect.andThen(client.request(method, params)),
                        ),
                      raw: {
                        ...client.raw,
                        request: (method, params) =>
                          // A synthetic native sibling requires real thread-closure evidence rather than producer retirement.
                          method === "thread/loaded/list"
                            ? Effect.succeed({ data: [nativeId, "synthetic-sibling"] })
                            : client.raw.request(method, params),
                      },
                    })),
                  ),
              },
            });
          },
        );
      }),
    ).pipe(
      Layer.provide(IdAllocator.layer),
      Layer.provide(ControlAdmission.layer),
      Layer.provide(NodeServices.layer),
    );
    // The synthetic account catalog proves both adapters use this fixture's same native history.
    const registry = Layer.effect(
      Registry.ProviderAdapterRegistryV2,
      Effect.gen(function* () {
        const original = yield* Registry.ProviderAdapterRegistryV2;
        return Registry.ProviderAdapterRegistryV2.of({
          ...original,
          getMetadata: (instanceId) =>
            original.get(instanceId).pipe(
              Effect.flatMap((adapter) => adapter.getCapabilities()),
              Effect.map((capabilities) => ({
                driver: Codex.CODEX_DRIVER_KIND,
                continuationKey: `codex:home:${home}`,
                enabled: true,
                capabilities,
              })),
              Effect.mapError(
                (cause) => new Registry.ProviderAdapterRegistryMetadataError({ instanceId, cause }),
              ),
            ),
        });
      }),
    ).pipe(Layer.provide(nativeRegistry));
    const database = Database.layerMemory;
    const runtime = Harness.layerWithRegistryAndServices(
      { name: "delivery-lifecycle", runtimePolicyOverride: { cwd } },
      registry,
      {
        databaseLayer: database,
        runEffectWorker,
        serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, config),
      },
    );
    const failRead = yield* Ref.make<CommandId | undefined>(undefined);
    const failProjection = yield* Ref.make<ThreadId | undefined>(undefined);
    const projectAvailable = yield* Ref.make(true);
    const projectReads = yield* Ref.make(0);
    const settingsReads = yield* Ref.make(0);
    const providerReads = yield* Ref.make(0);
    const baseReceipts = Receipts.layer.pipe(Layer.provide(database));
    const receipts = Layer.effect(
      Receipts.CommandReceiptStoreV2,
      Effect.map(Receipts.CommandReceiptStoreV2, (store) =>
        Receipts.CommandReceiptStoreV2.of({
          ...store,
          getByCommandId: (commandId) =>
            Ref.get(failRead).pipe(
              Effect.flatMap((failed) =>
                failed === commandId
                  ? Effect.fail(new Receipts.CommandReceiptStoreReadError({ commandId }))
                  : store.getByCommandId(commandId),
              ),
            ),
        }),
      ),
    ).pipe(Layer.provide(baseReceipts));
    const baseSettings = Settings.layerTest({
      providerInstances: {
        [instanceA]: { driver: "codex", enabled: true },
        [instanceB]: { driver: "codex", enabled: true },
      },
    });
    const settings = Layer.effect(
      Settings.ServerSettingsService,
      Effect.map(Settings.ServerSettingsService, (service) =>
        Settings.ServerSettingsService.of({
          ...service,
          getSettings: Ref.update(settingsReads, (count) => count + 1).pipe(
            Effect.andThen(service.getSettings),
          ),
        }),
      ),
    ).pipe(Layer.provide(baseSettings));
    const baseThreads = Threads.layer.pipe(
      Layer.provide(Layer.mergeAll(runtime, receipts, settings)),
    );
    const threads = Layer.effect(
      Threads.ThreadManagementService,
      Effect.map(Threads.ThreadManagementService, (service) =>
        Threads.ThreadManagementService.of({
          ...service,
          getThreadProjection: (threadId) =>
            Ref.get(failProjection).pipe(
              Effect.flatMap((failed) =>
                failed === threadId
                  ? Ref.set(failProjection, undefined).pipe(
                      Effect.andThen(
                        Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId })),
                      ),
                    )
                  : service.getThreadProjection(threadId),
              ),
            ),
        }),
      ),
    ).pipe(Layer.provide(baseThreads));
    const project = {
      id: projectId,
      title: "Synthetic delivery",
      workspaceRoot: cwd,
      repositoryIdentity: null,
      faviconPath: null,
      defaultModelSelection: selection,
      defaultThreadEnvMode: null,
      scripts: [],
      createdAt: "2026-11-02T00:00:00.000Z",
      updatedAt: "2026-11-02T00:00:00.000Z",
      deletedAt: null,
    };
    const external = Layer.mergeAll(
      NodeServices.layer,
      Layer.succeed(ServerConfig.ServerConfig, config),
      settings,
      SetupTracker.layer,
      Layer.mock(Clones.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
      Layer.mock(Terminals.TerminalManager)({ close: () => Effect.void }),
      Layer.mock(Projects.ProjectService)({
        getById: () =>
          Ref.update(projectReads, (count) => count + 1).pipe(
            Effect.andThen(Ref.get(projectAvailable)),
            Effect.map((available) => (available ? Option.some(project) : Option.none())),
          ),
      }),
      Layer.mock(Folders.ManagedProjectFolders)({
        namedProjectsRoot: cwd,
        isScratchProject: () => Effect.succeed(false),
        folderForThread: () => Effect.succeedNone,
      }),
      Layer.mock(Git.GitWorkflowService)({ isRepository: () => Effect.succeed(true) }),
      Layer.mock(Setup.ProjectSetupScriptRunner)({
        runForThread: () => Effect.succeed({ status: "no-script" as const }),
      }),
      Layer.mock(Text.TextGeneration)({}),
      Layer.mock(DocumentText.DocumentText)({
        ensure: () => Effect.die("This fixture sends no uploads"),
      }),
      Layer.mock(Providers.ProviderRegistry)({
        getProviders: Ref.update(providerReads, (count) => count + 1).pipe(Effect.as([])),
      }),
    );
    const launches = Launch.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          runtime,
          external,
          receipts,
          threads,
          IdAllocator.layer,
          Metadata.layer.pipe(Layer.provide(database)),
        ),
      ),
    );
    const layer = Layer.mergeAll(
      runtime,
      external,
      receipts,
      threads,
      launches,
      EventStore.layer.pipe(Layer.provide(database)),
    );
    const launch = (key: string): Launch.ThreadLaunchInput => ({
      commandId: CommandId.make(`launch:${key}`),
      threadId: ThreadId.make(`thread:${key}`),
      projectId,
      title: "Synthetic delivery",
      modelSelection: selection,
      runtimeMode: "full-access",
      interactionMode: "default",
      workspaceStrategy: { type: "root" },
      runSetupScript: false,
      initialMessage: { messageId: MessageId.make(`${key}:first`), text: "First", attachments: [] },
      createdBy: "user",
      creationSource: "web",
    });
    return {
      layer,
      launch,
      cwd,
      requests,
      firstComplete,
      secondComplete,
      failRead,
      failProjection,
      projectAvailable,
      projectReads,
      settingsReads,
      providerReads,
    };
  });

const waitRun = (threadId: ThreadId, runId: RunId, status: string) =>
  Effect.gen(function* () {
    const threads = yield* Threads.ThreadManagementService;
    yield* threads.streamStoredEventsFrom({ threadId }).pipe(
      Stream.filter(
        ({ event }) =>
          event.type === "run.updated" &&
          event.payload.id === runId &&
          event.payload.status === status,
      ),
      Stream.take(1),
      Stream.runDrain,
    );
  });

const finishStart = (threadId: ThreadId, runId: RunId) =>
  Effect.gen(function* () {
    const outbox = yield* Outbox.EffectOutboxV2;
    const threads = yield* Threads.ThreadManagementService;
    const starts = (yield* outbox.listByThreadId(threadId)).filter(
      (effect) => effect.request.type === "provider-turn.start" && effect.request.runId === runId,
    );
    assert.lengthOf(starts, 1);
    yield* outbox.awaitCompletion(starts.map((effect) => effect.id));
    assert.equal(Option.getOrThrow(yield* outbox.get(starts[0]!.id)).status, "succeeded");
    const run = (yield* threads.getThreadProjection(threadId)).runs.find(
      (candidate) => candidate.id === runId,
    )!;
    assert.equal(run.status, "running");
    assert.isNotNull(run.activeAttemptId);
    assert.isNotNull(run.providerThreadId);
    return yield* threads
      .streamStoredEventsFrom({ threadId, eventType: "provider-turn.updated" })
      .pipe(
        Stream.filterMap(({ event }) =>
          event.type === "provider-turn.updated" &&
          event.payload.runAttemptId === run.activeAttemptId &&
          event.payload.providerThreadId === run.providerThreadId &&
          event.payload.status === "running"
            ? Option.some(event.payload)
            : Option.none(),
        ),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      );
  });

it.effect(
  "public launch, inherited send, compatible account/options switch, lost reply, same-key retry and Stop preserve one execution per message",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture(true);
        yield* Effect.gen(function* () {
          const launches = yield* Launch.ThreadLaunchService;
          const threads = yield* Threads.ThreadManagementService;
          const settings = yield* Settings.ServerSettingsService;
          const outbox = yield* Outbox.EffectOutboxV2;
          const receipts = yield* Receipts.CommandReceiptStoreV2;
          const input = f.launch("spine");
          const threadId = input.threadId!;
          const lostReply = new Error("Synthetic lost public launch response");
          const lost = yield* Intake.launchThread(input).pipe(
            Effect.andThen(Effect.fail(lostReply)),
            Effect.flip,
          );
          assert.strictEqual(lost, lostReply);
          const first = (yield* threads.getThreadProjection(threadId)).runs[0]!;
          yield* waitRun(threadId, first.id, "running");
          yield* finishStart(threadId, first.id);
          const launchEvidence = yield* launches.getDeliveryEvidence(input);
          assert.deepEqual(launchEvidence.creation, { status: "accepted", threadId });
          assert.deepEqual(launchEvidence.initialMessage, {
            status: "accepted",
            commandId: initialMessageCommandId(input.commandId),
            messageId: input.initialMessage!.messageId,
          });
          const firstEffects = (yield* outbox.listByThreadId(threadId)).map((effect) => effect.id);
          yield* Ref.set(f.projectAvailable, false);
          yield* settings.updateSettings({
            providerInstances: {
              [instanceA]: { driver: "codex", enabled: false },
              [instanceB]: { driver: "codex", enabled: true },
            },
          });
          const readsBeforeRetry = [
            yield* Ref.get(f.projectReads),
            yield* Ref.get(f.settingsReads),
            yield* Ref.get(f.providerReads),
          ];
          const resumed = yield* Intake.launchThread(input);
          assert.isTrue(resumed.resumed);
          assert.equal(resumed.threadId, threadId);
          assert.deepEqual(
            [
              yield* Ref.get(f.projectReads),
              yield* Ref.get(f.settingsReads),
              yield* Ref.get(f.providerReads),
            ],
            readsBeforeRetry,
          );
          assert.sameMembers(
            (yield* outbox.listByThreadId(threadId)).map((effect) => effect.id),
            firstEffects,
          );
          yield* Ref.set(f.projectAvailable, true);
          yield* settings.updateSettings({
            providerInstances: {
              [instanceA]: { driver: "codex", enabled: true },
              [instanceB]: { driver: "codex", enabled: true },
            },
          });
          yield* Deferred.succeed(f.firstComplete, undefined);
          yield* waitRun(threadId, first.id, "completed");
          const second = {
            type: "message.dispatch" as const,
            commandId: CommandId.make("spine:second"),
            threadId,
            messageId: MessageId.make("spine:second"),
            text: "Second",
            attachments: [],
            dispatchMode: { type: "start_immediately" as const },
            createdBy: "user" as const,
            creationSource: "web" as const,
          };
          assert.isFalse("modelSelection" in second);
          yield* Intake.dispatchCommand(second);
          const secondRun = (yield* threads.getThreadProjection(threadId)).runs.find(
            (run) => run.userMessageId === second.messageId,
          )!;
          yield* waitRun(threadId, secondRun.id, "running");
          yield* finishStart(threadId, secondRun.id);
          yield* Deferred.succeed(f.secondComplete, undefined);
          yield* waitRun(threadId, secondRun.id, "completed");
          yield* Intake.dispatchCommand({
            type: "provider.switch",
            commandId: CommandId.make("spine:switch"),
            threadId,
            modelSelection: switched,
          });
          assert.deepEqual(
            (yield* threads.getThreadProjection(threadId)).thread.modelSelection,
            switched,
          );
          const third = {
            ...second,
            commandId: CommandId.make("spine:third"),
            messageId: MessageId.make("spine:third"),
            text: "Third",
            modelSelection: switched,
          };
          const lostSend = new Error("Synthetic lost public send response");
          assert.strictEqual(
            yield* Intake.dispatchCommand(third).pipe(
              Effect.andThen(Effect.fail(lostSend)),
              Effect.flip,
            ),
            lostSend,
          );
          const thirdRun = (yield* threads.getThreadProjection(threadId)).runs.find(
            (run) => run.userMessageId === third.messageId,
          )!;
          yield* waitRun(threadId, thirdRun.id, "running");
          const thirdTurn = yield* finishStart(threadId, thirdRun.id);
          assert.equal(thirdTurn.nativeTurnRef?.nativeId, "delivery-turn-3");
          const acceptedReceipt = Option.getOrThrow(
            yield* receipts.getByCommandId(third.commandId),
          );
          const thirdEffects = (yield* outbox.listByCommandId(third.commandId)).map(
            (effect) => effect.id,
          );
          assert.equal((yield* threads.getDispatchDeliveryEvidence(third)).status, "accepted");
          yield* settings.updateSettings({
            providerInstances: {
              [instanceA]: { driver: "codex", enabled: false },
              [instanceB]: { driver: "codex", enabled: false },
            },
          });
          assert.isTrue((yield* Intake.dispatchCommand(third)).replayed);
          assert.deepEqual(
            Option.getOrThrow(yield* receipts.getByCommandId(third.commandId)),
            acceptedReceipt,
          );
          assert.sameMembers(
            (yield* outbox.listByCommandId(third.commandId)).map((effect) => effect.id),
            thirdEffects,
          );
          const stop = {
            type: "thread.stop" as const,
            commandId: CommandId.make("spine:stop"),
            threadId,
          };
          yield* Intake.dispatchCommand(stop);
          assert.isTrue((yield* Intake.dispatchCommand(stop)).replayed);
          yield* waitRun(threadId, thirdRun.id, "interrupted");
          const stopEffects = yield* outbox.listByCommandId(stop.commandId);
          const interrupts = stopEffects.filter(
            (effect) => effect.request.type === "provider-turn.interrupt",
          );
          assert.lengthOf(interrupts, 1);
          assert.include(interrupts[0]!.request, {
            providerThreadId: thirdTurn.providerThreadId,
            providerTurnId: thirdTurn.id,
          });
          yield* outbox.awaitCompletion(interrupts.map((effect) => effect.id));
          const completedInterrupt = Option.getOrThrow(yield* outbox.get(interrupts[0]!.id));
          assert.equal(completedInterrupt.status, "succeeded");
          assert.equal(completedInterrupt.attemptCount, 1);
          const final = yield* threads.getThreadProjection(threadId);
          assert.lengthOf((yield* threads.getShellSnapshot()).threads, 1);
          assert.lengthOf(
            final.messages.filter((message) => message.role === "user"),
            3,
          );
          assert.lengthOf(final.runs, 3);
          assert.lengthOf(final.attempts, 3);
          assert.deepEqual(
            final.runs.map((run) => run.modelSelection),
            [selection, selection, switched],
          );
          // Strict replay asserts the actual outbound prompt, model and options for each tuple.
          assert.deepEqual(
            f.requests
              .filter((request) => request.method === "turn/start")
              .map((request) => request.instanceId),
            [instanceA, instanceA, instanceB],
          );
          assert.lengthOf(
            f.requests.filter((request) => request.method === "thread/start"),
            1,
          );
          assert.lengthOf(
            f.requests.filter(
              (request) => request.method === "thread/resume" && request.instanceId === instanceB,
            ),
            1,
          );
          assert.lengthOf(
            f.requests.filter((request) => request.method === "turn/interrupt"),
            1,
          );
          assert.deepEqual(
            f.requests.find((request) => request.method === "turn/interrupt"),
            {
              instanceId: instanceB,
              method: "turn/interrupt",
              params: { threadId: nativeId, turnId: "delivery-turn-3" },
            },
          );
        }).pipe(
          Effect.ensuring(
            Deferred.succeed(f.firstComplete, undefined).pipe(
              Effect.andThen(Deferred.succeed(f.secondComplete, undefined)),
            ),
          ),
          Effect.provide(f.layer),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "a committed launch and failed/read-lost first message retain separate exact delivery facts",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture(false);
        yield* Effect.gen(function* () {
          const launches = yield* Launch.ThreadLaunchService;
          const threads = yield* Threads.ThreadManagementService;
          const outbox = yield* Outbox.EffectOutboxV2;
          const store = yield* EventStore.EventStoreV2;
          const settings = yield* Settings.ServerSettingsService;
          const accepted = f.launch("projection-failed");
          yield* Ref.set(f.failProjection, accepted.threadId);
          const original = yield* Intake.launchThread(accepted).pipe(Effect.flip);
          assert.equal(original._tag, "ThreadLaunchError");
          const evidence = yield* launches.getDeliveryEvidence(accepted);
          assert.deepEqual(evidence.creation, { status: "accepted", threadId: accepted.threadId });
          assert.equal(evidence.initialMessage.status, "accepted");
          yield* Ref.set(f.failRead, initialMessageCommandId(accepted.commandId));
          const unreadable = yield* launches.getDeliveryEvidence(accepted);
          assert.deepEqual(unreadable.creation, evidence.creation);
          assert.equal(unreadable.initialMessage.status, "unknown");
          const before = yield* outbox.listByThreadId(accepted.threadId!);
          assert.equal(
            (yield* Intake.launchThread(accepted).pipe(Effect.flip))._tag,
            "ThreadLaunchError",
          );
          assert.deepEqual(yield* outbox.listByThreadId(accepted.threadId!), before);
          yield* Ref.set(f.failRead, undefined);
          assert.equal(
            (yield* launches.getDeliveryEvidence(accepted)).initialMessage.status,
            "accepted",
          );
          const wrongMessage = {
            ...accepted,
            initialMessage: {
              ...accepted.initialMessage!,
              messageId: MessageId.make("another-message"),
            },
          };
          assert.equal(
            (yield* launches.getDeliveryEvidence(wrongMessage)).initialMessage.status,
            "unknown",
          );
          assert.equal(
            (yield* launches.getDeliveryEvidence({
              ...accepted,
              projectId: ProjectId.make("other-project"),
            })).creation.status,
            "unknown",
          );
          assert.equal(
            (yield* launches.getDeliveryEvidence({
              ...accepted,
              threadId: ThreadId.make("other-thread"),
            })).creation.status,
            "unknown",
          );
          for (const mismatched of [
            wrongMessage,
            { ...accepted, projectId: ProjectId.make("other-project") },
            { ...accepted, threadId: ThreadId.make("other-thread") },
          ]) {
            assert.equal(
              (yield* Intake.launchThread(mismatched).pipe(Effect.flip))._tag,
              "ThreadLaunchError",
            );
          }
          assert.lengthOf((yield* threads.getShellSnapshot()).threads, 1);
          assert.sameMembers(
            (yield* outbox.listByThreadId(accepted.threadId!)).map((effect) => effect.id),
            before.map((effect) => effect.id),
          );
          const replayed = yield* Intake.launchThread({
            ...accepted,
            workspaceStrategy: {
              type: "worktree",
              branch: "must-not-create",
              baseRef: "missing-base",
            },
          });
          assert.isTrue(replayed.resumed);
          const originalRun = replayed.projection.runs[0]!;
          assert.deepEqual(originalRun.workspacePreparation, { type: "root" });
          yield* waitRun(accepted.threadId!, originalRun.id, "starting");
          assert.lengthOf((yield* threads.getThreadProjection(accepted.threadId!)).runs, 1);
          const absent = f.launch("not-admitted");
          assert.equal(
            (yield* launches.getDeliveryEvidence(absent)).initialMessage.status,
            "unknown",
          );
          const shellOnly = f.launch("shell-only");
          yield* Intake.launchThread({ ...shellOnly, initialMessage: undefined });
          const missingMessage = yield* launches.getDeliveryEvidence(shellOnly);
          assert.deepEqual(missingMessage.creation, {
            status: "accepted",
            threadId: shellOnly.threadId,
          });
          assert.equal(missingMessage.initialMessage.status, "unknown");
          const reuse = {
            ...shellOnly,
            commandId: CommandId.make("shell-only:reuse"),
            reuseExistingThread: true,
            initialMessage: undefined,
          };
          yield* Intake.launchThread(reuse);
          assert.deepEqual(
            (yield* launches.getDeliveryEvidence(reuse)).creation,
            missingMessage.creation,
          );
          yield* settings.updateSettings({
            providerInstances: {
              [instanceA]: { driver: "codex", enabled: false },
              [instanceB]: { driver: "codex", enabled: true },
            },
          });
          assert.equal(
            (yield* Intake.launchThread(shellOnly).pipe(Effect.flip))._tag,
            "ThreadManagementProviderInstanceDisabledError",
          );
          assert.lengthOf((yield* threads.getThreadProjection(shellOnly.threadId!)).runs, 0);
          assert.equal(
            (yield* launches.getDeliveryEvidence(shellOnly)).initialMessage.status,
            "unknown",
          );
          yield* settings.updateSettings({
            providerInstances: {
              [instanceA]: { driver: "codex", enabled: true },
              [instanceB]: { driver: "codex", enabled: true },
            },
          });
          yield* Intake.launchThread(shellOnly);
          assert.equal(
            (yield* launches.getDeliveryEvidence(shellOnly)).initialMessage.status,
            "accepted",
          );
          const rejected = f.launch("rejected-first");
          const invalidPlan = {
            ...rejected,
            initialMessage: {
              ...rejected.initialMessage!,
              sourcePlanRef: { threadId: rejected.threadId!, planId: PlanId.make("missing-plan") },
            },
          };
          assert.equal(
            (yield* Intake.launchThread(invalidPlan).pipe(Effect.flip))._tag,
            "ThreadLaunchError",
          );
          const rejectedEvidence = yield* launches.getDeliveryEvidence(invalidPlan);
          assert.deepEqual(rejectedEvidence.creation, {
            status: "accepted",
            threadId: rejected.threadId,
          });
          assert.equal(rejectedEvidence.initialMessage.status, "rejected");
          // Editing after terminal rejection deliberately uses a new message key in the proven thread.
          const recovery = {
            type: "message.dispatch" as const,
            commandId: CommandId.make("rejected-first:corrected"),
            threadId: rejected.threadId!,
            messageId: MessageId.make("rejected-first:corrected"),
            text: "Corrected first prompt",
            attachments: [],
            dispatchMode: { type: "start_immediately" as const },
            createdBy: "user" as const,
            creationSource: "web" as const,
          };
          const effectsBeforeRetry = yield* outbox.listByThreadId(rejected.threadId!);
          assert.equal(
            (yield* Intake.launchThread(invalidPlan).pipe(Effect.flip))._tag,
            "ThreadLaunchError",
          );
          assert.deepEqual(yield* outbox.listByThreadId(rejected.threadId!), effectsBeforeRetry);
          yield* Intake.dispatchCommand(recovery);
          const recovered = yield* threads.getThreadProjection(rejected.threadId!);
          assert.lengthOf(
            recovered.messages.filter((message) => message.role === "user"),
            1,
          );
          assert.lengthOf(recovered.runs, 1);
          assert.equal(recovered.runs[0]?.userMessageId, recovery.messageId);
          const claims = yield* store
            .readByCommandId({ commandId: rejected.commandId })
            .pipe(Stream.runCollect);
          assert.lengthOf(
            claims.filter(({ event }) => event.type === "thread.created"),
            1,
          );
          assert.lengthOf(
            f.requests,
            0,
            "the evidence-only cases never execute an external provider",
          );
        }).pipe(
          Effect.ensuring(
            Deferred.succeed(f.firstComplete, undefined).pipe(
              Effect.andThen(Deferred.succeed(f.secondComplete, undefined)),
            ),
          ),
          Effect.provide(f.layer),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
