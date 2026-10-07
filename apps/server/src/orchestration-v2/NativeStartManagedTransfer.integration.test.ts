import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CodexSettings,
  AuthSessionId,
  EnvironmentId,
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";
import packageJson from "../../package.json" with { type: "json" };
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as NativeAuthority from "../nativeCreation/NativeCreationAuthority.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as LegacyRepositorySqlite from "../nativeCreation/NativeCreationRepositorySqlite.ts";
import {
  NativeCreationRepository,
  type NativeCreationClaimInput,
} from "../nativeCreation/NativeCreationRepository.ts";
import {
  NativeCreationExecutionRepository,
  layer as executionLayer,
} from "../nativeCreation/NativeCreationExecutionRepository.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "../nativeCreation/NativeCreationPreparation.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as ProviderTurnStartService from "./ProviderTurnStartService.ts";
import * as ContextHandoffService from "./ContextHandoffService.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as ProviderInstanceRegistry from "../provider/Services/ProviderInstanceRegistry.ts";
import * as ProviderAuthService from "../provider/Services/ProviderAuthService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import * as ThreadTitleRegenerationService from "./ThreadTitleRegenerationService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ResourceCleanupService from "./ResourceCleanupService.ts";
import * as Ordinary from "./OrdinaryCheckoutOwnership.ts";
import {
  type NativeStartEnteredParticipantCaptureV1,
  makeOrdinaryCheckoutStore,
} from "./OrdinaryCheckoutStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as Manager from "./ProviderSessionManager.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  ProviderAdapterOpenSessionError,
  ProviderAdapterTurnStartError,
  type ProviderAdapterV2TurnInput,
  type ProviderNativeStartProducerCaptureV1,
  type ProviderNativeStartAcknowledgmentV1,
} from "./ProviderAdapter.ts";
import * as Adapter from "./Adapters/CodexAdapterV2.ts";
import { makeReplayServerConfig } from "./Adapters/CodexAdapterV2.testkit.ts";
const modelSelection = { instanceId: Adapter.CODEX_DEFAULT_INSTANCE_ID, model: "gpt-5.4" };
const runtimePolicy = {
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
  cwd: "/workspace",
};
const nativeId = "native-original-start";
const nativeTurnId = "native-original-turn";
const prompt = "Exercise the actual original start";
const nativeTurn = {
  id: nativeTurnId,
  items: [],
  itemsView: "notLoaded",
  status: "inProgress",
  error: null,
  startedAt: null,
  completedAt: null,
  durationMs: null,
};
const transcript: CodexReplay.CodexAppServerReplayTranscript = {
  provider: "codex",
  protocol: "codex.app-server",
  version: "0.144.0",
  scenario: "native-original-confirmation",
  entries: [
    {
      type: "expect_outbound",
      frame: {
        id: 1,
        method: "initialize",
        params: {
          clientInfo: { name: "T3 Code", title: "T3 Code", version: packageJson.version },
          capabilities: { experimentalApi: true, optOutNotificationMethods: ["turn/diff/updated"] },
        },
      },
    },
    {
      type: "emit_inbound",
      frame: {
        id: 1,
        result: {
          userAgent: "synthetic Codex",
          codexHome: "/synthetic/codex",
          platformFamily: "unix",
          platformOs: "linux",
        },
      },
    },
    { type: "expect_outbound", frame: { method: "initialized" } },
    {
      type: "expect_outbound",
      frame: { id: 2, method: "thread/start", params: { config: Adapter.CODEX_THREAD_CONFIG } },
    },
    {
      type: "emit_inbound",
      frame: {
        id: 2,
        result: {
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
            path: "/synthetic/original.jsonl",
            cwd: "/workspace",
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
          cwd: "/workspace",
          instructionSources: [],
          approvalPolicy: "on-request",
          approvalsReviewer: "user",
          sandbox: { type: "workspaceWrite", writableRoots: [], networkAccess: false },
          reasoningEffort: "medium",
        },
      },
    },
    {
      type: "expect_outbound",
      frame: {
        id: 3,
        method: "turn/start",
        params: {
          threadId: nativeId,
          input: [{ type: "text", text: prompt }],
          cwd: "/workspace",
          model: "gpt-5.4",
          approvalPolicy: "never",
          approvalsReviewer: "user",
          sandboxPolicy: { type: "dangerFullAccess" },
          summary: "detailed",
        },
      },
    },
    { type: "emit_inbound", frame: { id: 3, result: { turn: nativeTurn } } },
  ],
};

const timestamp = "2026-10-02T12:34:56Z";
const decodeFixtureBinding = Schema.decodeUnknownSync(NativePreparationBinding);
const decodeFixtureHistory = Schema.decodeUnknownSync(NativeCreationHistoricalBinding);
const decodeFixtureCommand = Schema.decodeUnknownSync(OrchestrationV2Command);
const releaseCommand = (preparation: {
  readonly command: { readonly commandId: string; readonly threadId: string };
}) =>
  decodeFixtureCommand({
    type: "prepared-run.release",
    commandId: CommandId.make(preparation.command.commandId),
    threadId: ThreadId.make(preparation.command.threadId),
    runId: RunId.make("fixture-prepared-run"),
  });
const nativeFixture = Effect.fnUntraced(function* (
  operationId = "fixture-operation",
  text = "Synthetic prompt",
  path = "/fixture/worktree",
  projectCwd = "/fixture/project",
) {
  const binding = decodeFixtureBinding({
    backend_instance: "fixture-backend",
    environment_id: "fixture-environment",
    project_id: "fixture-project",
    project_cwd: projectCwd,
    account_ref: "fixture-account",
    runtime_mode: "full-access" as const,
    interaction_mode: "default" as const,
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "gpt-5.4" },
  });
  const command = nativePreparationCommand(
    operationId,
    binding,
    text,
    "Synthetic thread",
    timestamp,
  );
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(
      nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1",
        operation_id: operationId,
        binding,
        command,
        preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
        prompt_digest: nativeCreationSha256(text),
        command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
      }),
    ),
  );
  const historical = decodeFixtureHistory({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "qualified-fixture-account",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  const input: NativeCreationClaimInput = {
    preparation,
    resources: {
      projectCwd: binding.project_cwd,
      branch: historical.requestedBranch,
      worktreePath: path,
    },
    claimId: `claim-${operationId}`,
    claimedBootId: "fixture-boot",
    claimedAt: timestamp,
    actorSessionId: "fixture-session",
    grantId: "fixture-grant",
    grantRevision: 1,
  };
  return { input, historical, preparation, authorize: Effect.succeed(historical) };
});

const database = SqlitePersistenceMemory;
const stores = Layer.mergeAll(
  database,
  EventStore.layer.pipe(Layer.provide(database)),
  CommandReceiptStore.layer.pipe(Layer.provide(database)),
  ProjectionStore.layer.pipe(Layer.provide(database)),
  EffectOutbox.layer.pipe(Layer.provide(database)),
  LegacyRepositorySqlite.layer.pipe(Layer.provide(database)),
  executionLayer.pipe(Layer.provide(database)),
  IdAllocator.layer,
);
const persistence = EventSink.layer.pipe(Layer.provideMerge(stores));
const fixtureBase = Effect.fnUntraced(function* (
  options: {
    readonly starting?: boolean;
    readonly nativeAuthority?: boolean;
    readonly enrolled?: boolean;
  } = {},
) {
  yield* TestClock.setTime(Date.parse(timestamp));
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const projection = yield* ProjectionStore.ProjectionStoreV2;
  const repo = yield* NativeCreationExecutionRepository;
  const legacy = yield* NativeCreationRepository;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const fs = yield* FileSystem.FileSystem;
  const config = yield* Effect.acquireRelease(
    makeReplayServerConfig("joined-native-transfer"),
    (c) => fs.remove(c.baseDir, { recursive: true }).pipe(Effect.orDie),
  );
  const workspace = `${config.baseDir}/workspace`;
  yield* fs.makeDirectory(workspace);
  const runtimePolicy = {
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    cwd: workspace,
  };
  const native = yield* nativeFixture(
    "joined-actual-original",
    prompt,
    workspace,
    options.nativeAuthority === true ? workspace : undefined,
  );
  yield* legacy.claim(native.input, native.authorize);
  const command = releaseCommand(native.preparation);
  if (command.type !== "prepared-run.release") return yield* Effect.die("Original release missing");
  yield* repo.reserveCommandIdentities(native.input.claimId, [command.commandId]);
  yield* repo.recordNormalizedCommand(native.input.claimId, command);
  const digest = Option.getOrThrow(yield* repo.getReservedCommand(command.commandId)).commandDigest;
  const now = yield* DateTime.now;
  const threadId = command.threadId;
  const projectId = ProjectId.make("fixture-project");
  yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES (${projectId},'Joined fixture',${workspace},'[]',${timestamp},${timestamp})`;
  const appThread: OrchestrationV2AppThread = {
    id: threadId,
    projectId,
    title: "Joined actual start",
    providerInstanceId: modelSelection.instanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: options.nativeAuthority === true ? native.input.resources.branch : null,
    worktreePath: options.nativeAuthority === true ? workspace : null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make("joined-birth"),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: appThread,
      },
    ],
  });
  const actualTranscript: typeof transcript = {
    ...transcript,
    entries: transcript.entries.map((entry, index) =>
      index === 5
        ? {
            type: "expect_outbound" as const,
            frame: {
              id: 3,
              method: "turn/start",
              params: {
                threadId: nativeId,
                input: [{ type: "text", text: prompt }],
                cwd: workspace,
                model: "gpt-5.4",
                approvalPolicy: "never",
                approvalsReviewer: "user",
                sandboxPolicy: { type: "dangerFullAccess" },
                summary: "detailed",
              },
            },
          }
        : entry,
    ),
  };
  const driver = yield* CodexReplay.makeReplayDriver(actualTranscript);
  const responseReceived = yield* Deferred.make<void>();
  const releaseResponse = yield* Deferred.make<void>();
  let requests = 0;
  const clientFactory: Adapter.CodexAppServerClientFactoryShape = {
    open: (openInput) =>
      Effect.gen(function* () {
        const context = yield* Layer.build(CodexReplay.layerReplayWithDriver(driver));
        const client = yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
          Effect.provide(context),
        );
        const request: typeof client.request = (method, params) =>
          Effect.sync(() => {
            if (method === "turn/start") requests++;
          }).pipe(
            Effect.andThen(client.request(method, params)),
            Effect.tap(() =>
              method === "turn/start" ? Deferred.succeed(responseReceived, undefined) : Effect.void,
            ),
            Effect.tap(() =>
              method === "turn/start" ? Deferred.await(releaseResponse) : Effect.void,
            ),
          );
        return { ...client, request };
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterOpenSessionError({
              driver: Adapter.CODEX_DRIVER_KIND,
              providerSessionId: openInput.providerSessionId,
              cause,
            }),
        ),
      ),
  };
  const adapter = Adapter.makeCodexAdapterV2({
    instanceId: modelSelection.instanceId,
    settings: yield* Schema.decodeUnknownEffect(CodexSettings)({}),
    environment: {},
    fileSystem: fs,
    idAllocator,
    serverConfig: config,
    clientFactory,
  });
  const actualStores = Layer.mergeAll(
    Layer.succeed(EventSink.EventSinkV2, sink),
    Layer.succeed(ProjectionStore.ProjectionStoreV2, projection),
    Layer.succeed(IdAllocator.IdAllocatorV2, idAllocator),
    ThreadCommandExecutor.layer,
  );
  const ingestor = ProviderEventIngestor.layer.pipe(Layer.provide(actualStores));
  const mcp = Layer.effect(
    McpSessionRegistry.McpSessionRegistry,
    McpSessionRegistry.__testing.make(),
  ).pipe(
    Layer.provide(
      Layer.succeed(
        HttpServer.HttpServer,
        HttpServer.HttpServer.of({
          address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
          serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
        }),
      ),
    ),
    Layer.provide(
      Layer.succeed(ServerEnvironment.ServerEnvironment, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("joined-synthetic-environment")),
        getDescriptor: Effect.die("unused descriptor"),
      }),
    ),
    Layer.provide(NodeServices.layer),
  );
  const managerContext = yield* Layer.build(
    Manager.layerWithOptions({ idleTimeoutMs: 60_000, configureMcp: false }).pipe(
      Layer.provide(
        Layer.mergeAll(
          actualStores,
          ingestor,
          mcp,
          ProviderAdapterRegistry.makeSingleLayer(adapter),
        ),
      ),
    ),
  );
  const manager = yield* Effect.service(Manager.ProviderSessionManagerV2).pipe(
    Effect.provide(managerContext),
  );
  const executionContext = yield* Layer.build(
    RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          actualStores,
          ingestor,
          ServerSettings.layerTest(),
          Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
        ),
      ),
    ),
  );
  const runExecution = yield* Effect.service(RunExecutionService.RunExecutionServiceV2).pipe(
    Effect.provide(executionContext),
  );
  const providerSessionId = ProviderSessionId.make("session:joined-original");
  const runtime = yield* manager.open({
    threadId,
    providerSessionId,
    modelSelection,
    runtimePolicy,
  });
  const providerThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
  const owner = { ...appThread, activeProviderThreadId: providerThread.id };
  const attemptId = RunAttemptId.make("attempt:joined-original");
  const nodeId = NodeId.make("node:joined-original");
  const scopeId = CheckpointScopeId.make("scope:joined-original");
  const history = yield* repo.readHistoryByClaim(native.input.claimId);
  const messageId = MessageId.make(history.intent.messageId);
  const scope = {
    threadId,
    runId: command.runId,
    nodeId,
    providerInstanceId: modelSelection.instanceId,
    occurredAt: now,
  };
  const events: OrchestrationV2DomainEvent[] = [
    {
      ...scope,
      id: EventId.make("joined-thread-current"),
      type: "thread.metadata-updated",
      payload: owner,
    },
    {
      ...scope,
      id: EventId.make("joined-provider-current"),
      type: "provider-thread.updated",
      payload: providerThread,
    },
    {
      ...scope,
      id: EventId.make("joined-run"),
      type: "run.created",
      payload: {
        id: command.runId,
        threadId,
        ordinal: 1,
        providerInstanceId: modelSelection.instanceId,
        modelSelection,
        providerThreadId: providerThread.id,
        userMessageId: messageId,
        rootNodeId: nodeId,
        activeAttemptId: attemptId,
        status: options.starting === true ? "starting" : "running",
        requestedAt: now,
        startedAt: options.starting === true ? null : now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    },
    {
      ...scope,
      id: EventId.make("joined-attempt"),
      type: "run-attempt.created",
      payload: {
        id: attemptId,
        runId: command.runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId: modelSelection.instanceId,
        providerThreadId: providerThread.id,
        providerTurnId: null,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    },
    {
      ...scope,
      id: EventId.make("joined-node"),
      type: "node.updated",
      payload: {
        id: nodeId,
        threadId,
        runId: command.runId,
        parentNodeId: null,
        rootNodeId: nodeId,
        kind: "root_turn",
        status: "running",
        countsForRun: true,
        providerThreadId: providerThread.id,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: scopeId,
        startedAt: now,
        completedAt: null,
      },
    },
    {
      ...scope,
      id: EventId.make("joined-scope"),
      type: "checkpoint-scope.created",
      payload: {
        id: scopeId,
        threadId,
        runId: command.runId,
        nodeId,
        parentScopeId: null,
        providerThreadId: providerThread.id,
        kind: "root_run",
        ordinalWithinParent: 0,
        advancesAppRunCount: true,
        cwd: workspace,
        createdAt: now,
      },
    },
    {
      ...scope,
      id: EventId.make("joined-message"),
      type: "message.updated",
      payload: {
        id: messageId,
        threadId,
        runId: command.runId,
        nodeId,
        role: "user",
        text: prompt,
        attachments: [],
        streaming: false,
        createdBy: "user",
        creationSource: "web",
        createdAt: now,
        updatedAt: now,
      },
    },
  ];
  const effectId = `effect:${command.commandId}:provider-turn.start:${command.runId}`;
  const reference = {
    version: 2 as const,
    claimId: native.input.claimId,
    stageCommandId: command.commandId,
    effectId,
    stage: "native_command" as const,
  };
  const store = yield* makeOrdinaryCheckoutStore();
  const captured = yield* store.capture({
    command,
    threadId,
    projectId,
    branch: appThread.branch,
    canonicalProjectRoot: workspace,
    canonicalCheckoutPath: workspace,
    source: { projectWorkspaceRoot: workspace, worktreePath: appThread.worktreePath },
    leaseId: "lease:joined-original",
  });
  const accepted = yield* sink.commitCommand({
    commandId: command.commandId,
    threadId,
    commandType: command.type,
    acceptedAt: now,
    events,
    ordinaryCheckout: captured,
    effects: [
      {
        id: effectId,
        commandId: command.commandId,
        threadId,
        request: { type: "provider-turn.start", runId: command.runId },
        nativeCreationExecutionReference: reference,
      },
    ],
  });
  assert.isTrue(accepted.committed);
  yield* sql`INSERT INTO orchestration_v2_native_command_identities (command_id,kind,version,command_type,aggregate_kind,aggregate_id,normalized_command_digest,binding_digest) VALUES (${command.commandId},'native_creation_stage',2,${command.type},'thread',${threadId},${digest},${native.preparation.bindingDigest})`;
  const input: ProviderAdapterV2TurnInput = {
    appThread: owner,
    threadId,
    runId: command.runId,
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId,
    rootNodeId: nodeId,
    providerThread,
    message: { messageId, text: prompt, attachments: [], createdBy: "user", creationSource: "web" },
    modelSelection,
    runtimePolicy,
  };
  const authSessionContext = yield* Layer.build(AuthSessions.layer);
  const authSessions = yield* Effect.service(AuthSessions.AuthSessionRepository).pipe(
    Effect.provide(authSessionContext),
  );
  const actorSessionId = AuthSessionId.make(native.input.actorSessionId);
  if (options.nativeAuthority === true) {
    yield* authSessions.create({
      sessionId: actorSessionId,
      subject: "Synthetic actual native actor",
      scopes: ["orchestration:operate"],
      method: "bearer-access-token",
      client: {
        label: null,
        ipAddress: null,
        userAgent: null,
        deviceType: "bot",
        os: null,
        browser: null,
      },
      issuedAt: now,
      expiresAt: DateTime.makeUnsafe("2099-01-01T00:00:00Z"),
    });
    if (options.enrolled !== false)
      yield* sql`INSERT INTO native_creation_automation_enrollments (session_id,enrolled_at) VALUES (${actorSessionId},${timestamp})`;
  }
  let currentGrant: NativeAuthority.NativeCreationGrant = {
    grantId: native.input.grantId,
    revision: native.input.grantRevision,
    actorSessionId,
    issuerId: "synthetic-current-issuer",
    expiresAt: DateTime.makeUnsafe("2099-01-01T00:00:00Z"),
    revoked: false,
    operationId: native.preparation.operationId,
    preparationId: native.preparation.preparationId,
    preparationSha256: native.preparation.preparationSha256,
    bindingDigest: native.preparation.bindingDigest,
    binding: native.historical,
    resources: native.input.resources,
    allowedStages: ["claim", "native_command"],
    recoveryScopes: [],
  };
  const authorityContext = yield* Layer.build(
    NativeAuthority.NativeCreationAuthorityLive.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(AuthSessions.AuthSessionRepository, authSessions),
          Layer.succeed(NativeCreationRepository, legacy),
          Layer.succeed(NativeCreationExecutionRepository, repo),
          Layer.succeed(NativeAuthority.NativeCreationGrantResolver, {
            resolveCurrent: () =>
              Effect.sync(() => ({
                enrolledSessionId: actorSessionId,
                trustedIssuerId: "synthetic-current-issuer",
                grant: currentGrant,
              })),
          }),
          Layer.succeed(NativeAuthority.NativeCreationBindingResolver, {
            resolveCurrent: () => Effect.succeed(native.historical),
          }),
        ),
      ),
    ),
  );
  const authority = yield* Effect.service(NativeAuthority.NativeCreationAuthority).pipe(
    Effect.provide(authorityContext),
  );
  const unavailableContext = yield* Layer.build(
    NativeAuthority.NativeCreationAuthorityUnavailable.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(AuthSessions.AuthSessionRepository, authSessions),
          Layer.succeed(NativeCreationRepository, legacy),
          Layer.succeed(NativeCreationExecutionRepository, repo),
        ),
      ),
    ),
  );
  const unavailableAuthority = yield* Effect.service(NativeAuthority.NativeCreationAuthority).pipe(
    Effect.provide(unavailableContext),
  );
  const projectContext = yield* Layer.build(ProjectStore.layer);
  const projectStore = yield* Effect.service(ProjectStore.ProjectStoreV2).pipe(
    Effect.provide(projectContext),
  );
  return {
    sink,
    sql,
    repo,
    authority,
    unavailableAuthority,
    authSessions,
    actorSessionId,
    projectStore,
    nativeResources: native.input.resources,
    changeGrant: (changes: Partial<NativeAuthority.NativeCreationGrant>) => {
      currentGrant = { ...currentGrant, ...changes };
    },
    outbox,
    store,
    input,
    runtime,
    manager,
    runExecution,
    projection,
    reference,
    startNativeEffect: repo.startEffectV2(reference, timestamp, native.authorize),
    scopeId,
    responseReceived,
    releaseResponse,
    driver,
    requests: () => requests,
    effectId,
    nativeOperation: {
      operationId: effectId,
      operation: "start_turn" as const,
      instanceId: modelSelection.instanceId,
      threadId,
      providerSessionId,
      providerThreadId: providerThread.id,
      attemptId,
    },
  };
});
const fixture = Effect.fnUntraced(function* (options: { readonly nativeAuthority?: boolean } = {}) {
  const f = yield* fixtureBase(options);
  const effect = Option.getOrThrow(
    yield* f.outbox.claimNext({ workerId: "worker:joined-original", leaseDurationMs: 60_000 }),
  );
  const linked = yield* f.store.readEffectLink(effect);
  if (linked === null || effect.leaseExpiresAt === null)
    return yield* Effect.die("Actual owned linked claim missing");
  const originalUse = (yield* f.store.beginUse({
    operationId: Ordinary.ordinaryCheckoutOutboxOperationIdV1(effect.id, effect.attemptCount),
    admission: linked.link.admission,
    source: {
      kind: "outbox",
      link: linked.link,
      workerId: "worker:joined-original",
      expectedAttempt: effect.attemptCount,
      leaseExpiresAt: DateTime.makeUnsafe(effect.leaseExpiresAt),
    },
    targetSource: {
      projectWorkspaceRoot: f.input.runtimePolicy.cwd!,
      worktreePath: f.input.appThread.worktreePath,
    },
  })).record.subject.use;
  const start = yield* f.store.bindOutboxExecution(originalUse);
  const nativeExecution =
    options.nativeAuthority === true
      ? {
          context: yield* f.authority.issueExecution!({ reference: f.reference, timestamp }),
          resources: f.nativeResources,
        }
      : undefined;
  if (options.nativeAuthority !== true) yield* f.startNativeEffect;
  return { ...f, effect, start, nativeExecution };
});
const startedFixture = Effect.fnUntraced(function* () {
  const f = yield* fixture();
  let capture: ProviderNativeStartProducerCaptureV1 | undefined;
  let ack: ProviderNativeStartAcknowledgmentV1 | undefined;
  const acknowledgments: ProviderNativeStartAcknowledgmentV1[] = [];
  let entered: NativeStartEnteredParticipantCaptureV1 | undefined;
  const running = yield* f.runtime
    .startTurn({
      ...f.input,
      nativeOperation: f.nativeOperation,
      nativeStartConfirmation: {
        beforeDispatch: (source) =>
          Effect.gen(function* () {
            capture = source;
            const attachment = Manager.readIssuedProviderNativeStartAttachment(source);
            assert.isNotNull(attachment);
            entered = yield* f.store.captureNativeStartEnteredParticipant({
              startExecution: f.start,
              producerCapture: source,
            });
            return {
              evidenceRevision: entered.evidenceRevision,
              revalidate: f.store.revalidateNativeEnteredBeforeAcknowledgment(entered).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterTurnStartError({
                      driver: Adapter.CODEX_DRIVER_KIND,
                      threadId: f.input.threadId,
                      providerThreadId: f.input.providerThread.id,
                      runId: f.input.runId,
                      cause,
                    }),
                ),
              ),
            };
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterTurnStartError({
                  driver: Adapter.CODEX_DRIVER_KIND,
                  threadId: f.input.threadId,
                  providerThreadId: f.input.providerThread.id,
                  runId: f.input.runId,
                  cause,
                }),
            ),
          ),
        acknowledged: (packet) =>
          Effect.gen(function* () {
            ack = packet;
            acknowledgments.push(packet);
            assert.isNotNull(Adapter.readIssuedCodexNativeStartAcknowledgment(packet));
            if (entered === undefined)
              return yield* Effect.die("Original entered participant not returned");
            yield* f.store.recordNativeStartAcknowledgment({
              enteredCapture: entered,
              acknowledgment: packet,
            });
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterTurnStartError({
                  driver: Adapter.CODEX_DRIVER_KIND,
                  threadId: f.input.threadId,
                  providerThreadId: f.input.providerThread.id,
                  runId: f.input.runId,
                  cause,
                }),
            ),
          ),
      },
    })
    .pipe(Effect.forkScoped);
  yield* Deferred.await(f.responseReceived).pipe(Effect.raceFirst(Fiber.join(running)));
  assert.isDefined(capture);
  assert.isUndefined(ack);
  assert.strictEqual(f.requests(), 1);
  assert.strictEqual(Option.getOrThrow(yield* f.outbox.get(f.effect.id)).status, "running");
  yield* Deferred.succeed(f.releaseResponse, undefined);
  yield* Fiber.join(running);
  assert.isDefined(ack);
  const attachment = Manager.readIssuedProviderNativeStartAttachment(capture);
  assert.isNotNull(attachment);
  const proof = yield* f.repo.readNativeEffectConfirmation(f.effect.id);
  assert.isNotNull(proof);
  assert.strictEqual(Option.getOrThrow(yield* f.outbox.get(f.effect.id)).status, "succeeded");
  const admission = yield* f.store.resolveAdmission(f.start.originalUse.admission);
  assert.isNotNull(admission.run);
  const managed = {
    kind: "captured_managed_run" as const,
    captureId: attachment!.captureId,
    run: admission.run!,
    checkpointScopeId: f.scopeId,
    driver: Adapter.CODEX_DRIVER_KIND,
    binding: {
      threadId: f.input.threadId,
      providerThreadId: f.input.providerThread.id,
      providerSessionId: f.runtime.providerSessionId,
      instanceId: f.runtime.instanceId,
    },
    runtimeGeneration: capture!.binding.runtimeGeneration,
    nativeThreadId: capture!.binding.nativeThreadId,
    evidenceRevision: attachment!.evidenceRevision,
  };
  const returned = Manager.readIssuedProviderNativeStartReturned(capture);
  assert.isNotNull(returned);
  const observation = {
    kind: "dispatch_returned" as const,
    startExecution: f.start,
    managedExecutor: managed,
    observedAt: returned!.observedAt,
  };
  const actualAck = acknowledgments[0];
  if (actualAck === undefined) return yield* Effect.die("Actual native ACK missing");
  const activationInput = {
    enteredCapture: entered!,
    acknowledgment: actualAck,
    managedExecutor: managed,
    actualStartObservation: observation,
    revalidateCaptured: attachment!.revalidateCaptured,
  };
  return {
    f,
    capture: capture!,
    ack: actualAck,
    entered: entered!,
    attachment: attachment!,
    managed,
    observation,
    activationInput,
  };
});
it.effect(
  "actual producer ACK completes the original SQL claim before one managed activation",
  () =>
    Effect.gen(function* () {
      const x = yield* startedFixture();
      const { f } = x;
      const before =
        yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id=${f.effect.id}`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.store.validateExecution(f.start, true))));
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            f.store.activateManagedRun({
              startExecution: f.start,
              managedExecutor: x.managed,
              actualStartObservation: x.observation,
              revalidateCaptured: x.attachment.revalidateCaptured,
            }),
          ),
        ),
      );
      const owner = yield* f.sink.readProviderRuntimeEvidence!(f.input.threadId);
      assert.isNotNull(owner);
      assert.notStrictEqual(
        nativeCreationCanonicalJson(owner!.binding),
        nativeCreationCanonicalJson(x.capture.binding),
      );
      const activated = yield* f.store.activateNativeManagedRun(x.activationInput);
      assert.strictEqual(activated.executor.kind, "captured_managed_run");
      assert.deepEqual(yield* f.store.activateNativeManagedRun(x.activationInput), activated);
      const history = yield* f.store.readExecutionHistory(f.start.originalUse);
      assert.deepEqual(
        history.facts.map((fact) => fact.eventKind),
        ["bind", "activate", "retire"],
      );
      assert.strictEqual(
        history.participants.find((item) => item.ref.associationId === f.start.associationId)
          ?.state,
        "retired",
      );
      assert.deepEqual(
        yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id=${f.effect.id}`,
        before,
      );
      assert.strictEqual(f.requests(), 1);
      assert.deepEqual(yield* Ref.get(f.driver.state), {
        cursor: transcript.entries.length,
        failure: null,
      });
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);
it.effect(
  "copied capabilities and every complete native binding mismatch deny without consuming the original",
  () =>
    Effect.gen(function* () {
      const x = yield* startedFixture();
      const { f } = x;
      const inputs = [
        { ...x.activationInput, enteredCapture: { ...x.entered } },
        { ...x.activationInput, acknowledgment: { ...x.ack } },
        ...Object.keys(x.managed.binding).map((key) => ({
          ...x.activationInput,
          managedExecutor: {
            ...x.managed,
            binding: { ...x.managed.binding, [key]: `different-${key}` },
          },
        })),
        ...[
          { runtimeGeneration: "different-generation" },
          { runtimeGeneration: ` ${x.managed.runtimeGeneration} ` },
          { runtimeGeneration: undefined },
          { runtimeGeneration: null },
          { nativeThreadId: "different-native" },
          { nativeThreadId: ` ${x.managed.nativeThreadId} ` },
          { nativeThreadId: undefined },
          { nativeThreadId: null },
          { evidenceRevision: x.attachment.evidenceRevision + 1 },
          { driver: "claude-code" },
          { captureId: "copied-managed-owner" },
        ].map((delta) => ({ ...x.activationInput, managedExecutor: { ...x.managed, ...delta } })),
      ];
      for (const input of inputs)
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              f.store.activateNativeManagedRun({
                ...input,
                actualStartObservation: {
                  ...x.observation,
                  managedExecutor: input.managedExecutor,
                },
              } as typeof x.activationInput),
            ),
          ),
        );
      assert.deepEqual(
        (yield* f.store.readExecutionHistory(f.start.originalUse)).facts.map(
          (fact) => fact.eventKind,
        ),
        ["bind"],
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            f.store.captureNativeStartEnteredParticipant({
              startExecution: f.start,
              producerCapture: x.capture,
            }),
          ),
        ),
      );
      yield* f.store.activateNativeManagedRun(x.activationInput);
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);
it.effect(
  "a real transaction failure rolls back both transfer facts and retries only the same retained owner",
  () =>
    Effect.gen(function* () {
      const x = yield* startedFixture();
      const { f } = x;
      const before =
        yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id=${f.effect.id}`;
      yield* f.sql`CREATE TRIGGER owned_native_transfer_fault BEFORE INSERT ON orchestration_v2_ordinary_checkout_execution_associations WHEN NEW.event_kind='retire' BEGIN SELECT RAISE(ABORT,'owned synthetic transfer rollback'); END`;
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(f.store.activateNativeManagedRun(x.activationInput))),
      );
      assert.deepEqual(
        (yield* f.store.readExecutionHistory(f.start.originalUse)).facts.map(
          (fact) => fact.eventKind,
        ),
        ["bind"],
      );
      assert.deepEqual(
        yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id=${f.effect.id}`,
        before,
      );
      yield* f.sql`DROP TRIGGER owned_native_transfer_fault`;
      yield* f.store.activateNativeManagedRun(x.activationInput);
      assert.deepEqual(
        (yield* f.store.readExecutionHistory(f.start.originalUse)).facts.map(
          (fact) => fact.eventKind,
        ),
        ["bind", "activate", "retire"],
      );
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);
it.effect(
  "concurrent exact duplicates consume one pair and a reopened lifetime owner reads history without reconstructing capability",
  () =>
    Effect.gen(function* () {
      const x = yield* startedFixture();
      const { f } = x;
      const refs = yield* Effect.all(
        [
          f.store.activateNativeManagedRun(x.activationInput),
          f.store.activateNativeManagedRun(x.activationInput),
        ],
        { concurrency: 2 },
      );
      assert.deepEqual(refs[0], refs[1]);
      const reopened = yield* makeOrdinaryCheckoutStore();
      const history = yield* reopened.readExecutionHistory(f.start.originalUse);
      assert.deepEqual(
        history.facts.map((fact) => fact.eventKind),
        ["bind", "activate", "retire"],
      );
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(reopened.activateNativeManagedRun(x.activationInput))),
      );
      assert.deepEqual(yield* reopened.readExecutionHistory(f.start.originalUse), history);
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);
it.effect(
  "a closed actual managed owner preserves the completed ACK and refuses transfer or re-entry",
  () =>
    Effect.gen(function* () {
      const x = yield* startedFixture();
      const { f } = x;
      const proof = yield* f.repo.readNativeEffectConfirmation(f.effect.id);
      yield* f.manager.close(f.runtime.providerSessionId);
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(f.store.activateNativeManagedRun(x.activationInput))),
      );
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.store.validateExecution(f.start, true))));
      assert.deepEqual(yield* f.repo.readNativeEffectConfirmation(f.effect.id), proof);
      assert.deepEqual(
        (yield* f.store.readExecutionHistory(f.start.originalUse)).facts.map(
          (fact) => fact.eventKind,
        ),
        ["bind"],
      );
      yield* f.store.retainExecutionUnknown(
        f.start,
        "Actual original native managed owner closed before transfer",
      );
      assert.strictEqual(
        (yield* f.store.readUse(f.start.originalUse.operationId))?.state,
        "unknown",
      );
      assert.strictEqual(Option.getOrThrow(yield* f.outbox.get(f.effect.id)).status, "succeeded");
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);
it.effect(
  "an observed current-owner rotation permanently closes the original managed capture",
  () =>
    Effect.gen(function* () {
      const x = yield* startedFixture();
      const { f } = x;
      const owner = yield* f.sink.readProviderRuntimeEvidence!(f.input.threadId);
      assert.isNotNull(owner);
      const rotated = yield* f.sink.registerProviderRuntime!({
        expectedBinding: owner!.binding,
        expectedRegisteredBinding: owner!.binding,
        expectedEvidenceRevision: owner!.evidenceRevision,
        actualBinding: { ...x.capture.binding, runtimeGeneration: "replacement-managed-owner" },
        expectedRunId: f.input.runId,
        expectedRunAttemptId: f.input.attemptId,
      });
      assert.isTrue(rotated.committed);
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(f.store.activateNativeManagedRun(x.activationInput))),
      );
      const current = yield* f.sink.readProviderRuntimeEvidence!(f.input.threadId);
      assert.isNotNull(current);
      const restored = yield* f.sink.registerProviderRuntime!({
        expectedBinding: owner!.binding,
        expectedRegisteredBinding: current!.binding,
        expectedEvidenceRevision: current!.evidenceRevision,
        actualBinding: x.capture.binding,
        expectedRunId: f.input.runId,
        expectedRunAttemptId: f.input.attemptId,
      });
      assert.isTrue(restored.committed);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(x.attachment.revalidateCaptured)));
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(f.store.activateNativeManagedRun(x.activationInput))),
      );
      assert.deepEqual(
        (yield* f.store.readExecutionHistory(f.start.originalUse)).facts.map(
          (fact) => fact.eventKind,
        ),
        ["bind"],
      );
      assert.strictEqual(Option.getOrThrow(yield* f.outbox.get(f.effect.id)).status, "succeeded");
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);
it.effect(
  "expired original entered deadline retains ACK custody without revival or a new native RPC",
  () =>
    Effect.gen(function* () {
      const x = yield* startedFixture();
      const { f } = x;
      yield* TestClock.adjust("61 seconds");
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(f.store.activateNativeManagedRun(x.activationInput))),
      );
      assert.deepEqual(
        (yield* f.store.readExecutionHistory(f.start.originalUse)).facts.map(
          (fact) => fact.eventKind,
        ),
        ["bind"],
      );
      assert.strictEqual(Option.getOrThrow(yield* f.outbox.get(f.effect.id)).status, "succeeded");
      assert.strictEqual(f.requests(), 1);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);

it.effect(
  "the actual RunExecution owner commits authentic ACK and transfer after complete native start return",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture({ nativeAuthority: true });
      const current = yield* f.projection.getThreadRecords(f.input.threadId, [
        "runs",
        "nodes",
        "attempts",
        "checkpointScopes",
      ]);
      const run = current.runs.find((candidate) => candidate.id === f.input.runId)!;
      const rootNode = current.nodes.find((candidate) => candidate.id === f.input.rootNodeId)!;
      const attempt = current.attempts.find((candidate) => candidate.id === f.input.attemptId)!;
      const checkpointScope = current.checkpointScopes.find(
        (candidate) => candidate.id === f.scopeId,
      )!;
      const completions: Array<{
        confirmation: import("../nativeCreation/NativeCreationExecutionRepository.ts").NativeEffectConfirmationV1;
        execution: Ordinary.OrdinaryCheckoutExecutionRefV1;
      }> = [];
      const running = yield* f.runExecution
        .startRootRun({
          commandId: f.effect.commandId,
          appThread: f.input.appThread,
          providerSessionId: f.runtime.providerSessionId,
          session: f.runtime,
          run,
          rootNode,
          checkpointScope,
          providerThread: f.input.providerThread,
          attempt,
          attemptId: f.input.attemptId,
          providerTurnOrdinal: f.input.providerTurnOrdinal,
          message: f.input.message,
          modelSelection: f.input.modelSelection,
          runtimePolicy: f.input.runtimePolicy,
          nativeStart: {
            operation: f.nativeOperation,
            startExecution: f.start,
            ...(f.nativeExecution === undefined ? {} : { execution: f.nativeExecution }),
            onTransferred: (completion) => {
              completions.push(completion);
            },
          },
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(f.responseReceived).pipe(Effect.raceFirst(Fiber.join(running)));
      assert.strictEqual(f.requests(), 1);
      assert.strictEqual(completions.length, 0);
      assert.strictEqual(Option.getOrThrow(yield* f.outbox.get(f.effect.id)).status, "running");
      assert.deepEqual(
        (yield* f.store.readExecutionHistory(f.start.originalUse)).facts.map(
          (fact) => fact.eventKind,
        ),
        ["bind"],
      );
      yield* Deferred.succeed(f.releaseResponse, undefined);
      yield* Fiber.join(running);
      assert.strictEqual(completions.length, 1);
      assert.strictEqual(completions[0]!.confirmation.effectId, f.effect.id);
      assert.strictEqual(completions[0]!.execution.executor.kind, "captured_managed_run");
      assert.strictEqual(Option.getOrThrow(yield* f.outbox.get(f.effect.id)).status, "succeeded");
      assert.deepEqual(
        (yield* f.store.readExecutionHistory(f.start.originalUse)).facts.map(
          (fact) => fact.eventKind,
        ),
        ["bind", "activate", "retire"],
      );
      assert.strictEqual(f.requests(), 1);
      yield* f.manager.close(f.runtime.providerSessionId);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);

it.effect(
  "required native worker refuses a void completion and holds the real original claim without replay",
  () =>
    Effect.gen(function* () {
      const f = yield* fixtureBase();
      let executions = 0;
      const context = yield* Layer.build(
        EffectWorker.layerWithOptions({
          workerId: "worker:joined-original",
          leaseDurationMs: 60_000,
        }).pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(EffectOutbox.EffectOutboxV2, f.outbox),
              Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
                execute: () =>
                  Effect.sync(() => {
                    executions++;
                  }),
              }),
            ),
          ),
        ),
      );
      const worker = yield* Effect.service(EffectWorker.OrchestrationEffectWorkerV2).pipe(
        Effect.provide(context),
      );
      const failed = yield* Effect.exit(worker.runOnce);
      assert.isTrue(Exit.isFailure(failed));
      assert.strictEqual(executions, 1);
      const actual = Option.getOrThrow(yield* f.outbox.get(f.effectId));
      assert.strictEqual(actual.status, "running");
      assert.strictEqual(actual.attemptCount, 1);
      assert.strictEqual(actual.leaseOwner, "worker:joined-original");
      assert.strictEqual(actual.completedAt, null);
      const holds = yield* f.outbox.listHeldByThreadId(f.input.threadId);
      assert.strictEqual(holds.length, 1);
      assert.strictEqual(holds[0]!.effectId, actual.id);
      assert.strictEqual(holds[0]!.expectedAttempt, actual.attemptCount);
      assert.strictEqual(holds[0]!.operationId, actual.id);
      assert.strictEqual(holds[0]!.evidence.outcome, "unknown");
      assert.isNull(yield* f.repo.readNativeEffectConfirmation(actual.id));
      assert.isFalse(yield* worker.runOnce);
      assert.strictEqual(executions, 1);
      assert.strictEqual(f.requests(), 0);
      yield* f.manager.close(f.runtime.providerSessionId);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);

const actualWorkerForFixture = Effect.fnUntraced(function* (
  f: Effect.Success<ReturnType<typeof fixtureBase>>,
  mode?: "lost" | "copied" | "no_issuer" | "unavailable",
) {
  const domains = Layer.mergeAll(
    Layer.succeed(EventSink.EventSinkV2, f.sink),
    Layer.succeed(ProjectionStore.ProjectionStoreV2, f.projection),
    Layer.succeed(IdAllocator.IdAllocatorV2, yield* IdAllocator.IdAllocatorV2),
    Layer.succeed(Manager.ProviderSessionManagerV2, f.manager),
    Layer.succeed(RunExecutionService.RunExecutionServiceV2, f.runExecution),
    ...(mode === "no_issuer"
      ? []
      : [
          Layer.succeed(
            NativeAuthority.NativeCreationAuthority,
            mode === "unavailable" ? f.unavailableAuthority : f.authority,
          ),
        ]),
    ServerSettings.layerTest(),
    RuntimePolicy.layerWithOverride({ cwd: f.input.runtimePolicy.cwd! }).pipe(
      Layer.provide(RuntimePolicy.layer),
    ),
    Layer.mock(ContextHandoffService.ContextHandoffServiceV2)({}),
    Layer.mock(GitWorkflowService.GitWorkflowService)({}),
    Layer.mock(ProjectService.ProjectService)({
      getById: (id) =>
        f.projectStore
          .get(id)
          .pipe(Effect.map(Option.map((row) => ({ ...row, id: row.projectId }))), Effect.orDie),
    }),
    Layer.mock(ProviderAuthService.ProviderAuthService)({}),
    Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
      getInstance: () => Effect.succeed(undefined),
    }),
    Layer.mock(RunFinalizationService.RunFinalizationService)({}),
    Layer.mock(CheckpointRollbackService.CheckpointRollbackServiceV2)({}),
    Layer.mock(ProviderTurnControlService.ProviderTurnControlServiceV2)({}),
    Layer.mock(RuntimeRequestService.RuntimeRequestServiceV2)({}),
    Layer.mock(ThreadTitleRegenerationService.ThreadTitleRegenerationService)({}),
    Layer.mock(ThreadManagementService.ThreadManagementService)({}),
    Layer.mock(ResourceCleanupService.ResourceCleanupService)({}),
  );
  const startLayer = ProviderTurnStartService.layer.pipe(Layer.provide(domains));
  const executorContext = yield* Layer.build(
    EffectWorker.executorLayer.pipe(Layer.provide(Layer.merge(domains, startLayer))),
  );
  const actualExecutor = yield* Effect.service(EffectWorker.OrchestrationEffectExecutorV2).pipe(
    Effect.provide(executorContext),
  );
  const workerContext = yield* Layer.build(
    EffectWorker.layerWithOptions({
      workerId: "worker:joined-original",
      leaseDurationMs: 60_000,
    }).pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(EventSink.EventSinkV2, f.sink),
          Layer.succeed(EffectOutbox.EffectOutboxV2, f.outbox),
          Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
            execute: (effect, options) =>
              actualExecutor.execute(effect, options).pipe(
                Effect.flatMap((result) =>
                  mode === "lost"
                    ? Effect.fail(
                        new EffectWorker.OrchestrationEffectExecutionError({
                          effectId: effect.id,
                          effectType: effect.request.type,
                          cause: "Lost original private executor return after committed transfer",
                        }),
                      )
                    : mode === "copied" && result !== undefined
                      ? Effect.succeed({ ...result })
                      : Effect.succeed(result),
                ),
                Effect.mapError(
                  (cause) =>
                    new EffectWorker.OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              ),
          }),
        ),
      ),
    ),
  );
  const worker = yield* Effect.service(EffectWorker.OrchestrationEffectWorkerV2).pipe(
    Effect.provide(workerContext),
  );
  return worker;
});

it.effect(
  "the actual worker and turn-start service consume the authentic ACK transfer without generic resettlement",
  () =>
    Effect.gen(function* () {
      const f = yield* fixtureBase({ starting: true, nativeAuthority: true });
      const worker = yield* actualWorkerForFixture(f);
      const running = yield* worker.runOnce.pipe(Effect.forkScoped);
      yield* Deferred.await(f.responseReceived).pipe(Effect.raceFirst(Fiber.join(running)));
      const claimed = Option.getOrThrow(yield* f.outbox.get(f.effectId));
      assert.strictEqual(claimed.status, "running");
      assert.strictEqual(claimed.attemptCount, 1);
      assert.deepEqual(
        (yield* f.repo.readHistoryByClaim(f.reference.claimId)).effectsV2.map((fact) => [
          fact.phase,
          fact.effectId,
        ]),
        [["started", f.effectId]],
      );
      assert.strictEqual(f.requests(), 1);
      assert.isNull(yield* f.repo.readNativeEffectConfirmation(f.effectId));
      yield* Deferred.succeed(f.releaseResponse, undefined);
      assert.isTrue(yield* Fiber.join(running));
      const completed = Option.getOrThrow(yield* f.outbox.get(f.effectId));
      assert.strictEqual(completed.status, "succeeded");
      assert.strictEqual(completed.attemptCount, claimed.attemptCount);
      assert.isNull(completed.leaseOwner);
      assert.isNull(completed.leaseExpiresAt);
      const proof = yield* f.repo.readNativeEffectConfirmation(f.effectId);
      assert.isNotNull(proof);
      assert.strictEqual(proof!.confirmedAt, completed.completedAt);
      const admission = yield* f.store.readAdmissionForRun({
        threadId: f.input.threadId,
        runId: f.input.runId,
      });
      assert.isNotNull(admission);
      const histories = yield* f.sql<{
        association_json: string;
      }>`SELECT association_json FROM orchestration_v2_ordinary_checkout_execution_associations WHERE event_kind='activate'`;
      assert.strictEqual(histories.length, 1);
      const managed = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(Ordinary.OrdinaryCheckoutExecutionRefV1),
      )(histories[0]!.association_json);
      const history = yield* f.store.readExecutionHistory(managed.originalUse);
      assert.deepEqual(
        history.facts.map((fact) => fact.eventKind),
        ["bind", "activate", "retire"],
      );
      assert.strictEqual((yield* f.outbox.listHeldByThreadId(f.input.threadId)).length, 0);
      assert.isFalse(yield* worker.runOnce);
      assert.strictEqual(f.requests(), 1);
      yield* f.manager.close(f.runtime.providerSessionId);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);

it.effect.each(["lost", "copied"] as const)(
  "a %s private worker result retains authentic transferred custody without terminal claim revival",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* fixtureBase({ starting: true, nativeAuthority: true });
      const worker = yield* actualWorkerForFixture(f, mode);
      const running = yield* worker.runOnce.pipe(Effect.exit, Effect.forkScoped);
      yield* Deferred.await(f.responseReceived).pipe(Effect.raceFirst(Fiber.join(running)));
      yield* Deferred.succeed(f.releaseResponse, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(running)));
      const completed = Option.getOrThrow(yield* f.outbox.get(f.effectId));
      assert.strictEqual(completed.status, "succeeded");
      assert.strictEqual(completed.attemptCount, 1);
      assert.isNull(completed.leaseOwner);
      assert.isNull(completed.leaseExpiresAt);
      const proof = yield* f.repo.readNativeEffectConfirmation(f.effectId);
      assert.isNotNull(proof);
      assert.strictEqual(proof!.confirmedAt, completed.completedAt);
      const associations = yield* f.sql<{
        association_json: string;
      }>`SELECT association_json FROM orchestration_v2_ordinary_checkout_execution_associations WHERE event_kind='activate'`;
      assert.strictEqual(associations.length, 1);
      const managed = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(Ordinary.OrdinaryCheckoutExecutionRefV1),
      )(associations[0]!.association_json);
      const history = yield* f.store.readExecutionHistory(managed.originalUse);
      assert.deepEqual(
        history.facts.map((fact) => fact.eventKind),
        ["bind", "activate", "retire", "unknown"],
      );
      assert.strictEqual(
        history.participants.find((p) => p.ref.associationId === managed.associationId)!.state,
        "unknown",
      );
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.store.revalidateExecution(managed))));
      assert.strictEqual((yield* f.outbox.listHeldByThreadId(f.input.threadId)).length, 0);
      assert.isFalse(yield* worker.runOnce);
      assert.strictEqual(f.requests(), 1);
      yield* f.manager.close(f.runtime.providerSessionId);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);

it.effect.each([
  "revoked_session",
  "expired_session",
  "scopes",
  "enrollment",
  "revoked_grant",
  "revision",
  "resources",
  "failed_start_commit",
] as const)("the actual native issuer refuses %s before original provider dispatch", (mode) =>
  Effect.gen(function* () {
    const f = yield* fixtureBase({
      starting: true,
      nativeAuthority: true,
      enrolled: mode !== "enrollment",
    });
    if (mode === "revoked_session")
      yield* f.authSessions.revoke({ sessionId: f.actorSessionId, revokedAt: yield* DateTime.now });
    if (mode === "expired_session")
      yield* f.sql`UPDATE auth_sessions SET expires_at='2020-01-01T00:00:00Z' WHERE session_id=${f.actorSessionId}`;
    if (mode === "scopes")
      yield* f.sql`UPDATE auth_sessions SET scopes='[]' WHERE session_id=${f.actorSessionId}`;
    if (mode === "revoked_grant") f.changeGrant({ revoked: true });
    if (mode === "revision") f.changeGrant({ revision: 2 });
    if (mode === "resources")
      f.changeGrant({
        resources: { projectCwd: "/unrelated", branch: "unrelated", worktreePath: "/unrelated" },
      });
    if (mode === "failed_start_commit")
      yield* f.sql.unsafe(
        `CREATE TRIGGER owned_fail_native_start BEFORE INSERT ON native_creation_effect_facts WHEN json_extract(NEW.fact_json, '$.phase')='started' BEGIN SELECT RAISE(ABORT,'owned original start commit failure'); END`,
      );
    const worker = yield* actualWorkerForFixture(f);
    assert.isTrue(Exit.isFailure(yield* worker.runOnce.pipe(Effect.exit)));
    assert.strictEqual(f.requests(), 0);
    assert.isNull(yield* f.repo.readNativeEffectConfirmation(f.effectId));
    assert.deepEqual((yield* f.repo.readHistoryByClaim(f.reference.claimId)).effectsV2, []);
    assert.deepEqual(
      yield* f.sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations WHERE event_kind='activate'`,
      [],
    );
    assert.strictEqual((yield* f.outbox.listHeldByThreadId(f.input.threadId)).length, 1);
    assert.isFalse(yield* worker.runOnce);
    yield* f.manager.close(f.runtime.providerSessionId);
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);

it.effect(
  "current native grant loss during the original RPC cannot publish ACK authority or retry",
  () =>
    Effect.gen(function* () {
      const f = yield* fixtureBase({ starting: true, nativeAuthority: true });
      const worker = yield* actualWorkerForFixture(f);
      const running = yield* worker.runOnce.pipe(Effect.exit, Effect.forkScoped);
      yield* Deferred.await(f.responseReceived).pipe(Effect.raceFirst(Fiber.join(running)));
      assert.strictEqual(
        (yield* f.repo.readHistoryByClaim(f.reference.claimId)).effectsV2.length,
        1,
      );
      f.changeGrant({ revoked: true });
      yield* Deferred.succeed(f.releaseResponse, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(running)));
      assert.isNull(yield* f.repo.readNativeEffectConfirmation(f.effectId));
      assert.strictEqual(f.requests(), 1);
      assert.strictEqual((yield* f.outbox.listHeldByThreadId(f.input.threadId)).length, 1);
      assert.isFalse(yield* worker.runOnce);
      assert.strictEqual(f.requests(), 1);
      yield* f.manager.close(f.runtime.providerSessionId);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);

it.effect.each(["no_issuer", "unavailable"] as const)(
  "actual native worker denies %s authority without any native execution start or RPC",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* fixtureBase({ starting: true, nativeAuthority: true });
      const worker = yield* actualWorkerForFixture(f, mode);
      assert.isTrue(Exit.isFailure(yield* worker.runOnce.pipe(Effect.exit)));
      assert.strictEqual(f.requests(), 0);
      assert.deepEqual((yield* f.repo.readHistoryByClaim(f.reference.claimId)).effectsV2, []);
      assert.isNull(yield* f.repo.readNativeEffectConfirmation(f.effectId));
      assert.strictEqual((yield* f.outbox.listHeldByThreadId(f.input.threadId)).length, 1);
      assert.isFalse(yield* worker.runOnce);
      assert.strictEqual(f.requests(), 0);
      yield* f.manager.close(f.runtime.providerSessionId);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);
