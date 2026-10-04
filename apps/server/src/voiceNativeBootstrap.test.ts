import { Buffer } from "node:buffer";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import { assert, it } from "@effect/vitest";
import {
  AuthSessionId,
  CommandId,
  EventId,
  GitCommandError,
  NativeBootstrapDispatchResultV2Json,
  NativeBootstrapSubmission,
  NativeCreationHistoricalBinding,
  ORCHESTRATION_PROTOCOL_QUERY_PARAM,
  ORCHESTRATION_PROTOCOL_VERSION,
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationDispatchCommandError,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadTurnStartCommand,
  WsOrchestrationV2DispatchNativeBootstrapRpc,
  WsRpcGroup,
} from "@t3tools/contracts";
import {
  Cause,
  Context,
  Data,
  DateTime,
  Deferred,
  Effect,
  FileSystem,
  Layer,
  Option,
  Path,
  Queue,
  Schema,
  Stream,
} from "effect";
import * as TestClock from "effect/testing/TestClock";
import { HttpRouter, HttpServer, HttpServerRequest } from "effect/unstable/http";
import { RpcGroup, RpcSerialization, RpcServer } from "effect/unstable/rpc";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as ServerConfig from "./config.ts";
import * as GitWorkflow from "./git/GitWorkflowService.ts";
import * as AuthSessions from "./persistence/AuthSessions.ts";
import { layer as nativeRepositoryLayer } from "./persistence/Layers/NativeCreationRepository.ts";
import { SqlitePersistenceMemory } from "./persistence/Layers/Sqlite.ts";
import { NativeCreationRepository } from "./persistence/Services/NativeCreationRepository.ts";
import * as ManagedProjectFolders from "./project/ManagedProjectFolders.ts";
import * as ProjectCloneTracker from "./project/ProjectCloneTracker.ts";
import * as ProjectService from "./project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "./project/ProjectSetupScriptRunner.ts";
import * as WorktreeSetupTracker from "./project/WorktreeSetupTracker.ts";
import { makeProviderRegistryLayer } from "./provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "./serverSettings.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import * as TextGeneration from "./textGeneration/TextGeneration.ts";
import { nativeWorktreePath } from "./vcs/worktreePath.ts";
import {
  dispatchNativeBootstrapRpc,
  hasCompatibleOrchestrationProtocol,
  nativeBootstrapRpcSerialization,
} from "./ws.ts";
import { CodexProviderCapabilitiesV2 } from "./orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "./orchestration-v2/EventSink.ts";
import * as IdAllocator from "./orchestration-v2/IdAllocator.ts";
import {
  NativeCreationAuthority,
  NativeCreationAuthorityError,
  NativeCreationAuthorityLive,
  NativeCreationBindingResolver,
  NativeCreationGrantResolver,
  type NativeCreationGrant,
} from "./orchestration-v2/NativeCreationAuthority.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./orchestration-v2/NativeCreationPreparation.ts";
import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import type { ProviderAdapterV2Shape } from "./orchestration-v2/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "./orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "./orchestration-v2/ThreadManagementService.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./orchestration-v2/testkit/ProviderReplayHarness.ts";

const fixtureUrl = new URL(
  "./voiceNativeBootstrap.fixtures/python-native-request.json.txt",
  import.meta.url,
);
const bearer = "synthetic-test-bearer";
const prompt = "Original synthetic V2 voice prompt\nwith café, 雪 and 🧭.";
const title = "Native voice — café 雪";
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-6.1-sol",
  options: [{ id: "reasoningEffort", value: "high" }],
};
const legacyRequestSchema = Schema.Struct({
  _tag: Schema.Literal("Request"),
  id: Schema.String,
  tag: Schema.Literal(ORCHESTRATION_V2_WS_METHODS.dispatchCommand),
  payload: ThreadTurnStartCommand,
  headers: Schema.Array(Schema.Tuple([Schema.String, Schema.String])),
});
const nativeRequestSchema = Schema.Struct({
  _tag: Schema.Literal("Request"),
  id: Schema.String,
  tag: Schema.Literal(ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap),
  payload: NativeBootstrapSubmission,
  headers: Schema.Array(Schema.Tuple([Schema.String, Schema.String])),
});
const successSchema = Schema.Struct({
  _tag: Schema.Literal("Exit"),
  requestId: Schema.String,
  exit: Schema.Struct({
    _tag: Schema.Literal("Success"),
    value: NativeBootstrapDispatchResultV2Json,
  }),
});
const failureSchema = Schema.Struct({
  _tag: Schema.Literal("Exit"),
  requestId: Schema.String,
  exit: Schema.Struct({
    _tag: Schema.Literal("Failure"),
    cause: Schema.Array(Schema.Union([
      Schema.Struct({ _tag: Schema.Literal("Fail"), error: Schema.Unknown }),
      Schema.Struct({ _tag: Schema.Literal("Die"), defect: Schema.Unknown }),
      Schema.Struct({ _tag: Schema.Literal("Interrupt"), fiberId: Schema.optional(Schema.Number) }),
    ])),
  }),
});
const defectSchema = Schema.Struct({
  _tag: Schema.Literal("Defect"),
  defect: nativeBootstrapRpcSerialization.codecFor(Schema.Defect()),
});
const ordinaryRpc = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchCommand);
if (ordinaryRpc === undefined || ordinaryRpc._tag !== ORCHESTRATION_V2_WS_METHODS.dispatchCommand) {
  throw new Error("The production V2 ordinary dispatch RPC is unavailable");
}
const voiceRpcGroup = RpcGroup.make(WsOrchestrationV2DispatchNativeBootstrapRpc, ordinaryRpc);

const loadLegacyRequest = Effect.fnUntraced(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const bytes = yield* fs.readFileString(yield* path.fromFileUrl(fixtureUrl));
  const [request] = nativeBootstrapRpcSerialization.makeUnsafe().decode(bytes);
  return { bytes, request: yield* Schema.decodeUnknownEffect(legacyRequestSchema)(request) };
});

interface HarnessOptions {
  readonly name: string;
  readonly runSetupScript?: boolean;
  readonly unknownWorktreeOutcome?: boolean;
  readonly setupExitCode?: number;
  readonly qualifyRecovery?: boolean;
  readonly capabilityAvailable?: boolean;
  readonly setupEntered?: Deferred.Deferred<void>;
  readonly setupCompletion?: Deferred.Deferred<void>;
  readonly cancelEntered?: Deferred.Deferred<void>;
  readonly cancelBlocked?: Deferred.Deferred<void>;
  readonly failureAfterRelease?: Error;
}

const buildNativeWsHarness = Effect.fnUntraced(function* (options: HarnessOptions) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const scratch = yield* fs.makeTempDirectoryScoped({
    directory: process.cwd(),
    prefix: ".t3-voice-native-bootstrap-",
  });
  const projectCwd = path.join(scratch, "project");
  const worktreesDir = path.join(scratch, "worktrees");
  yield* fs.makeDirectory(projectCwd, { recursive: true });
  const projectId = ProjectId.make(`voice-project:${options.name}`);
  const project = {
    id: projectId,
    title: "Synthetic native Voice project",
    workspaceRoot: projectCwd,
    repositoryIdentity: null,
    faviconPath: null,
    defaultModelSelection: modelSelection,
    defaultThreadEnvMode: null,
    scripts: [],
    createdAt: "2026-10-02T12:00:00.000Z",
    updatedAt: "2026-10-02T12:00:00.000Z",
    deletedAt: null,
  };
  const qualified = new Map<string, {
    readonly grant: NativeCreationGrant;
    readonly binding: typeof NativeCreationHistoricalBinding.Type;
  }>();
  const qualify = (grant: NativeCreationGrant, binding: typeof NativeCreationHistoricalBinding.Type) => {
    qualified.set(grant.actorSessionId, { grant, binding });
  };
  const unavailable = () => new NativeCreationAuthorityError({
    code: "unsupported_authority",
    message: "This test has no explicitly qualified synthetic native authority",
  });
  const database = SqlitePersistenceMemory;
  const repositoryLayer = nativeRepositoryLayer.pipe(Layer.provide(database));
  const sessionsLayer = Layer.effect(AuthSessions.AuthSessionRepository, AuthSessions.make).pipe(
    Layer.provide(database),
  );
  const liveAuthority = NativeCreationAuthorityLive.pipe(
    Layer.provide(Layer.mergeAll(
      sessionsLayer,
      repositoryLayer,
      Layer.succeed(NativeCreationGrantResolver, {
        resolveCurrent: ({ actorSessionId }) => {
          const current = qualified.get(actorSessionId);
          return current === undefined ? Effect.fail(unavailable()) : Effect.succeed({
            enrolledSessionId: current.grant.actorSessionId,
            trustedIssuerId: current.grant.issuerId,
            grant: current.grant,
          });
        },
      }),
      Layer.succeed(NativeCreationBindingResolver, {
        resolveCurrent: (preparation) => {
          const current = [...qualified.values()].find((entry) =>
            entry.grant.preparationId === preparation.preparationId,
          );
          return current === undefined ? Effect.fail(unavailable()) : Effect.succeed(current.binding);
        },
      }),
    )),
    Layer.orDie,
  );
  let enableRecovery = Effect.void;
  const authorityLayer = Layer.effect(NativeCreationAuthority,
    NativeCreationAuthority.pipe(Effect.map((authority) => ({
      ...authority,
      authorize: (input: Parameters<typeof authority.authorize>[0]) =>
        input.stage === "bootstrap_detachment" && options.cancelEntered && options.cancelBlocked
          ? Effect.suspend(() => enableRecovery).pipe(
              Effect.andThen(Deferred.succeed(options.cancelEntered, undefined)),
              Effect.andThen(Deferred.await(options.cancelBlocked)),
              Effect.andThen(authority.authorize(input)),
            )
          : authority.authorize(input),
    }))),
  ).pipe(Layer.provide(liveAuthority));
  const adapter = {
    instanceId: modelSelection.instanceId,
    driver: ProviderDriverKind.make("codex"),
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    openSession: () => Effect.die("Live provider execution is disabled in Voice conformance tests"),
  } as ProviderAdapterV2Shape;
  const orchestrator = makeOrchestratorV2ReplayLayerWithRegistry(
    { name: `voice-native:${options.name}` },
    ProviderAdapterRegistry.makeLayer([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ).pipe(Layer.provide(Layer.mergeAll(database, repositoryLayer, authorityLayer)));
  const threadsLayer = ThreadManagement.layer.pipe(Layer.provide(orchestrator));
  const receiptsLayer = CommandReceiptStore.layer.pipe(Layer.provide(database));
  const worktreeInputs: Array<Parameters<GitWorkflow.GitWorkflowService["Service"]["createWorktree"]>[0]> = [];
  const setupInputs: Array<Parameters<ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"]>[0]> = [];
  const externalOrder: string[] = [];
  const originalFailures: unknown[] = [];
  const trackerLayer = Layer.effect(WorktreeSetupTracker.WorktreeSetupTracker,
    WorktreeSetupTracker.make.pipe(Effect.map((tracker) => ({
      ...tracker,
      finish: (threadId, phase, error) => phase === "done" && options.failureAfterRelease
        ? Effect.die(options.failureAfterRelease)
        : tracker.finish(threadId, phase, error),
    } satisfies WorktreeSetupTracker.WorktreeSetupTracker["Service"]))),
  );
  const externalServices = Layer.mergeAll(
    trackerLayer,
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({
      close: () => Effect.sync(() => { externalOrder.push("setup-detached"); }),
    }),
    Layer.succeed(ProjectService.ProjectService, {
      create: () => Effect.die("unused"),
      bootstrap: () => Effect.die("unused"),
      update: () => Effect.die("unused"),
      delete: () => Effect.die("unused"),
      getById: (id) => Effect.succeed(id === projectId ? Option.some(project) : Option.none()),
      getByWorkspaceRoot: () => Effect.succeed(Option.some(project)),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree: (input) => Effect.gen(function* () {
        worktreeInputs.push(input);
        externalOrder.push("worktree");
        if (input.path == null || input.newRefName === undefined) {
          return yield* Effect.die("Native checkout must bind its exact path and branch");
        }
        assert.isTrue(input.path.startsWith(`${worktreesDir}${path.sep}`));
        if (options.unknownWorktreeOutcome && options.qualifyRecovery) {
          yield* Effect.suspend(() => enableRecovery);
        }
        yield* fs.makeDirectory(input.path, { recursive: true });
        if (options.unknownWorktreeOutcome) return yield* new GitCommandError({
          operation: "GitVcsDriver.createWorktree",
          command: "git",
          cwd: input.cwd,
          detail: "Synthetic response lost after checkout allocation",
          exitCode: null,
        });
        return { worktree: { path: input.path, refName: input.newRefName } };
      }),
      renameBranch: () => Effect.die("Canonical native branch must not be renamed"),
      fetchRemote: () => Effect.die("This native binding does not fetch origin"),
      remoteExists: () => Effect.succeed(true),
      remoteBranchExists: () => Effect.succeed(true),
      removeWorktree: () => Effect.die("Logical recovery must not silently remove external checkout"),
      resolveRemoteTrackingCommit: () => Effect.die("This native binding uses its exact local base"),
    }),
    Layer.succeed(ProjectSetupScriptRunner.ProjectSetupScriptRunner, {
      runForThread: (input) => Effect.gen(function* () {
        setupInputs.push(input);
        externalOrder.push("setup-started");
        if (options.setupEntered) yield* Deferred.succeed(options.setupEntered, undefined);
        return {
          status: "started" as const,
          scriptId: "synthetic-setup",
          scriptName: "Synthetic setup",
          scriptCommand: "synthetic-setup-command",
          terminalId: "synthetic-setup-terminal",
          cwd: input.worktreePath,
          async: false,
          completion: Effect.gen(function* () {
            if (options.setupCompletion) yield* Deferred.await(options.setupCompletion);
            if (options.qualifyRecovery) yield* Effect.suspend(() => enableRecovery);
            externalOrder.push("setup-completed");
            return { exitCode: options.setupExitCode ?? 0, durationMs: 1 };
          }),
        };
      }),
    }),
    Layer.mock(TextGeneration.TextGeneration)({
      generateThreadTitle: () => Effect.die("Native canonical title must remain exact"),
      generateBranchName: () => Effect.die("Native canonical branch must remain exact"),
    }),
    ServerSettings.layerTest(),
    makeProviderRegistryLayer(),
    Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
      namedProjectsRoot: path.join(scratch, "projects"),
      folderForThread: () => Effect.succeed(Option.none()),
    }),
  );
  const launchLayer = ThreadLaunch.layer.pipe(Layer.provide(Layer.mergeAll(
    externalServices, threadsLayer, receiptsLayer, IdAllocator.layer,
  )));
  const context = Context.merge(
    yield* Effect.context<FileSystem.FileSystem | Path.Path>(),
    yield* Layer.build(Layer.mergeAll(
      launchLayer,
      threadsLayer,
      orchestrator,
      database,
      repositoryLayer,
      authorityLayer,
      sessionsLayer,
      ProjectStore.layer.pipe(Layer.provide(database)),
      ServerConfig.layerTest(projectCwd, scratch),
      externalServices,
    )),
  );
  const fixture = yield* Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const repository = yield* NativeCreationRepository;
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const sessions = yield* AuthSessions.AuthSessionRepository;
    const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const now = yield* DateTime.now;
    const timestamp = DateTime.formatIso(now);
    const operationId = `voice-native:${options.name}`;
    const binding = Schema.decodeUnknownSync(NativePreparationBinding)({
      backend_instance: "synthetic-backend",
      environment_id: "synthetic-environment",
      project_id: projectId,
      project_cwd: projectCwd,
      account_ref: "synthetic-account",
      runtime_mode: "full-access",
      interaction_mode: "default",
      base_branch: "main",
      start_from_origin: false,
      run_setup_script: options.runSetupScript ?? true,
      provider_model_selection: modelSelection,
    });
    const command = nativePreparationCommand(operationId, binding, prompt, title, timestamp);
    yield* Effect.addFinalizer(() => Effect.gen(function* () {
      if (options.cancelBlocked) yield* Deferred.succeed(options.cancelBlocked, undefined);
      if (options.setupCompletion) yield* Deferred.succeed(options.setupCompletion, undefined);
      yield* tracker.cancel(command.threadId);
      // Finish retains a snapshot for 30 seconds; advance the fixture clock to release its own fiber.
      yield* TestClock.adjust("30 seconds");
      assert.isNull(yield* tracker.get(command.threadId));
    }));
    yield* projects.apply({
      sequence: 0,
      eventId: EventId.make(`voice-project-created:${options.name}`),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: timestamp,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId, title: project.title, workspaceRoot: projectCwd,
        defaultModelSelection: modelSelection, scripts: [], createdAt: timestamp, updatedAt: timestamp,
      },
    });
    const session = {
      sessionId: AuthSessionId.make(`voice-native-session:${options.name}`),
      subject: `voice-native:${options.name}`,
      method: "bearer-access-token" as const,
      scopes: ["orchestration:read", "orchestration:operate"] as const,
    };
    yield* sessions.create({
      ...session,
      issuedAt: now,
      expiresAt: DateTime.add(now, { days: 1 }),
      client: { label: null, ipAddress: null, userAgent: null, deviceType: "bot", os: null, browser: null },
    });
    yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES (${session.sessionId}, ${timestamp})`;
    const canonicalText = nativeCreationCanonicalJson({
      schema: "voice.t3-bootstrap-preparation/v1",
      operation_id: operationId,
      preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
      binding,
      command,
      binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
      prompt_digest: nativeCreationSha256(command.message.text),
      command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
    });
    const preparation = yield* validateNativeCreationPreparation(new TextEncoder().encode(canonicalText));
    const historical = Schema.decodeUnknownSync(NativeCreationHistoricalBinding)({
      backendInstance: binding.backend_instance,
      environmentId: binding.environment_id,
      projectId,
      projectCwd,
      accountRef: binding.account_ref,
      accountBindingId: "synthetic-qualified-account",
      accountBindingRevision: 1,
      providerModelSelection: binding.provider_model_selection,
      runtimeMode: binding.runtime_mode,
      interactionMode: binding.interaction_mode,
      baseBranch: binding.base_branch,
      startFromOrigin: false,
      runSetupScript: binding.run_setup_script,
      requestedBranch: command.bootstrap.prepareWorktree.branch,
    });
    const resources = {
      projectCwd,
      branch: historical.requestedBranch,
      worktreePath: nativeWorktreePath({ worktreesDir, cwd: projectCwd, branch: historical.requestedBranch }),
    };
    const guard = {
      schema: "t3.native-creation-guard/v1" as const,
      grantId: `synthetic-voice-grant:${options.name}`,
      grantRevision: 1,
    };
    const grant: NativeCreationGrant = {
      grantId: guard.grantId,
      revision: 1,
      actorSessionId: session.sessionId,
      issuerId: "synthetic-issuer",
      expiresAt: DateTime.add(now, { days: 1 }),
      revoked: false,
      operationId: preparation.operationId,
      preparationId: preparation.preparationId,
      preparationSha256: preparation.preparationSha256,
      bindingDigest: preparation.bindingDigest,
      binding: historical,
      resources,
      allowedStages: ["claim", "normalization", "tracker_registration", "bootstrap_detachment", "worktree_ownership", "fetch", "worktree", "setup", "setup_detachment", "setup_completion_detachment", "native_command"],
      recoveryScopes: [],
    };
    qualify(grant, historical);
    enableRecovery = Effect.gen(function* () {
      const facts = yield* sink.readNativeCommandFacts({
        threadId: command.threadId,
        commandId: CommandId.make(`${command.commandId}:native:v2:create`),
      });
      assert.equal(facts.creationProvenance, "native_created");
      assert.isNotNull(facts.incarnation);
      qualify({
        ...grant,
        allowedStages: [...grant.allowedStages, "cleanup"],
        recoveryScopes: [{
          scopeId: `${preparation.operationId}:cleanup:thread`,
          resource: { kind: "thread", threadId: command.threadId, incarnation: facts.incarnation! },
        }],
      }, historical);
    }).pipe(Effect.orDie);
    const request = Schema.decodeUnknownSync(nativeRequestSchema)({
      _tag: "Request",
      id: `voice-native-request:${options.name}`,
      tag: ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap,
      payload: {
        schema: "t3.native-bootstrap-submission/v1",
        preparationBase64: Buffer.from(canonicalText).toString("base64"),
        creationGuard: guard,
      },
      headers: [],
    });
    const nativeContext = yield* Effect.context<NativeCreationAuthority | NativeCreationRepository | ServerConfig.ServerConfig | FileSystem.FileSystem>();
    let ordinaryHandlerCalls = 0;
    const startup: Pick<ServerRuntimeStartup.ServerRuntimeStartup["Service"], "enqueueCommand"> = {
      enqueueCommand: (effect) => effect,
    };
    const handlers = voiceRpcGroup.toLayer({
      [ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap]: (submission) => dispatchNativeBootstrapRpc(
        submission,
        session,
        { dispatchNativeBootstrap: (submission, server) => launches.dispatchNativeBootstrap(submission, server).pipe(
          Effect.tapCause((cause) => Effect.sync(() => { originalFailures.push(Cause.squash(cause)); })),
        ) },
        startup,
        nativeContext,
        Effect.succeed(options.capabilityAvailable ?? true),
      ),
      [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: () => Effect.sync(() => {
        ordinaryHandlerCalls += 1;
      }).pipe(Effect.andThen(Effect.die("The unchanged V1 payload must fail the actual V2 RPC codec before its handler"))),
    });
    // This transport selects production RPC schemas and the exported native handler; full route composition has separate coverage.
    const routes = HttpRouter.add("GET", "/ws", Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      assert.equal(request.headers.authorization, `Bearer ${bearer}`);
      const requestUrl = HttpServerRequest.toURL(request);
      assert.isTrue(Option.isSome(requestUrl) && hasCompatibleOrchestrationProtocol(requestUrl.value));
      const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
      yield* RpcServer.make(voiceRpcGroup, { disableTracing: true }).pipe(
        Effect.provideService(RpcServer.Protocol, protocol),
        Effect.provide(handlers),
        Effect.forkScoped,
      );
      return yield* httpEffect;
    }));
    const serverContext = yield* Layer.build(HttpRouter.serve(routes, {
      disableLogger: true,
      disableListenLog: true,
    }).pipe(
      Layer.provideMerge(NodeHttpServer.layerTest),
      Layer.provide(Layer.succeed(RpcSerialization.RpcSerialization, nativeBootstrapRpcSerialization)),
    ));
    const server = Context.get(serverContext, HttpServer.HttpServer);
    if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected a real TCP test server");
    return {
      url: `ws://127.0.0.1:${server.address.port}/ws?${ORCHESTRATION_PROTOCOL_QUERY_PARAM}=${ORCHESTRATION_PROTOCOL_VERSION}`,
      command, preparation, resources, repository, sink, sql, tracker, request,
      ordinaryHandlerCalls: () => ordinaryHandlerCalls,
    };
  }).pipe(Effect.provide(context));
  return { ...fixture, worktreeInputs, setupInputs, externalOrder, originalFailures };
});

class NativeWebSocketError extends Data.TaggedError("NativeWebSocketError")<{
  readonly cause: unknown;
}> {}

const openSocket = Effect.fnUntraced(function* (url: string) {
  const messages = yield* Effect.acquireRelease(
    Queue.unbounded<Effect.Effect<string, NativeWebSocketError>>(),
    Queue.shutdown,
  );
  const socket = yield* Effect.acquireRelease(
    Effect.sync(() => new NodeSocket.NodeWS.WebSocket(url, {
      headers: { authorization: `Bearer ${bearer}` },
    })),
    (socket) => Effect.callback<void>((resume) => {
      if (socket.readyState === NodeSocket.NodeWS.WebSocket.CLOSED) return resume(Effect.void);
      socket.once("close", () => resume(Effect.void));
      socket.terminate();
    }),
  );
  socket.on("message", (bytes) => { Queue.offerUnsafe(messages, Effect.succeed(bytes.toString())); });
  socket.on("error", (cause) => { Queue.offerUnsafe(messages, Effect.fail(new NativeWebSocketError({ cause }))); });
  socket.on("close", (code, reason) => {
    Queue.offerUnsafe(messages, Effect.fail(new NativeWebSocketError({ cause: { code, reason: reason.toString() } })));
  });
  yield* Effect.callback<void, NativeWebSocketError>((resume) => {
    socket.once("open", () => resume(Effect.void));
    socket.once("error", (cause) => resume(Effect.fail(new NativeWebSocketError({ cause }))));
  });
  return { socket, messages };
});

const writeRequest = Effect.fnUntraced(function* (
  connection: Effect.Success<ReturnType<typeof openSocket>>,
  request: unknown,
) {
  const serializer = yield* RpcSerialization.RpcSerialization;
  const bytes = serializer.makeUnsafe().encode(request);
  assert.isDefined(bytes);
  connection.socket.send(bytes!);
  return bytes!;
});

const sendRequest = Effect.fnUntraced(function* (
  connection: Effect.Success<ReturnType<typeof openSocket>>,
  request: unknown,
) {
  const requestBytes = yield* writeRequest(connection, request);
  const responseBytes = yield* Queue.take(connection.messages).pipe(Effect.flatten);
  const serializer = yield* RpcSerialization.RpcSerialization;
  const [response] = serializer.makeUnsafe().decode(responseBytes);
  return { response, responseBytes, requestBytes };
});

const assertAccepted = (response: unknown, requestId: string) => {
  const success = Schema.decodeUnknownSync(successSchema)(response);
  assert.equal(success.requestId, requestId);
  assert.equal(success.exit.value.commandAcceptance, "accepted");
  return success.exit.value;
};

const assertRejected = (response: unknown, requestId: string, code: string) => {
  const failure = Schema.decodeUnknownSync(failureSchema)(response);
  assert.equal(failure.requestId, requestId);
  const fail = failure.exit.cause.find((reason) => reason._tag === "Fail");
  assert.isDefined(fail);
  if (fail?._tag !== "Fail") throw new Error("Expected a typed native bootstrap rejection");
  const rejected = Schema.decodeUnknownSync(OrchestrationDispatchCommandError)(fail.error);
  assert.equal(rejected.creationRejectionCode, code);
  return rejected;
};

it.layer(Layer.mergeAll(
  NodeServices.layer,
  Layer.succeed(RpcSerialization.RpcSerialization, nativeBootstrapRpcSerialization),
))("native Voice V2 source conformance", (it) => {
  it.effect("keeps the unchanged Python request as legacy codec evidence and rejects it at the real V2 WS codec", () =>
    Effect.gen(function* () {
      const legacy = yield* loadLegacyRequest();
      assert.equal(nativeCreationSha256(legacy.bytes), "ad348db025fb50c0c17e02ee79c77e5789cf338c3601b3671ce50d9bf273c369");
      assert.deepEqual(legacy.request.headers, []);
      assert.equal(legacy.request.payload.type, "thread.turn.start");
      assert.equal(legacy.request.payload.message.text, "Original synthetic voice prompt\nwith a second line and café.");
      assert.equal(legacy.request.payload.bootstrap?.runSetupScript, false);
      const harness = yield* buildNativeWsHarness({ name: "legacy" });
      const connection = yield* openSocket(harness.url);
      const { response } = yield* sendRequest(connection, legacy.request);
      const failure = Schema.decodeUnknownSync(failureSchema)(response);
      assert.equal(failure.requestId, legacy.request.id);
      assert.isTrue(failure.exit.cause.some((reason) => reason._tag === "Die"));
      assert.equal(harness.ordinaryHandlerCalls(), 0);
      assert.lengthOf(harness.worktreeInputs, 0);
      const rows = yield* harness.sql<{ count: number }>`SELECT COUNT(*) AS count FROM native_creation_intents`;
      assert.equal(rows[0]?.count, 0);
    }).pipe(Effect.scoped),
  );

  it.effect.each([false, true])("accepts exact synthetic native V2 bytes with runSetupScript=%s and ordered stage evidence", (runSetupScript) =>
    Effect.gen(function* () {
      const harness = yield* buildNativeWsHarness({ name: `positive-${runSetupScript}`, runSetupScript });
      const connection = yield* openSocket(harness.url);
      const wire = yield* sendRequest(connection, harness.request);
      const accepted = assertAccepted(wire.response, harness.request.id);
      assert.equal(accepted.version, 2);
      assert.equal(accepted.commandId, harness.command.commandId);
      assert.equal(accepted.threadId, harness.command.threadId);
      assert.equal(accepted.messageId, harness.command.message.messageId);
      const creation = accepted.creation;
      assert.isNotNull(creation);
      if (creation === null) return yield* Effect.die("Native accepted stages must have bounded attributable creation evidence");
      assert.deepEqual(creation.stageCommands.map((stage) => [stage.commandId, stage.commandType]), [
        [`${harness.command.commandId}:native:v2:create`, "thread.create"],
        [`${harness.command.commandId}:native:v2:message`, "message.dispatch"],
        [harness.command.commandId, "prepared-run.release"],
      ]);
      let previousSequence = 0;
      for (const stage of creation.stageCommands) {
        assert.equal(stage.receipt?.status, "accepted");
        assert.equal(stage.event?.sequence, stage.receipt?.resultSequence);
        assert.isAbove(stage.event?.sequence ?? 0, previousSequence);
        previousSequence = stage.event!.sequence;
        const facts = yield* harness.sink.readNativeCommandFacts({ threadId: accepted.threadId, commandId: stage.commandId });
        assert.equal(facts.identity?.normalizedCommandDigest, stage.commandDigest);
        assert.equal(facts.receipt?.resultSequence, stage.event?.sequence);
        assert.isTrue(facts.eventMetadata.some((event) => event.commandId === stage.commandId &&
          event.applicationEventVersion === 2 && event.eventId === stage.event?.eventId && event.sequence === stage.event.sequence));
      }
      assert.deepEqual(creation.incarnation, creation.stageCommands[0]!.event);
      assert.equal(creation.finalReceipt?.resultSequence, previousSequence);
      assert.equal(creation.commandDigest, harness.preparation.commandDigest);
      assert.notEqual(creation.normalizedCommandDigest, harness.preparation.commandDigest);
      assert.equal(creation.outcome, "in_progress");
      assert.isFalse(creation.overflow);
      assert.lengthOf(creation.effectsV2, 0);
      assert.equal(creation.effectsV1.some((fact) => fact.kind === "setup" && fact.phase === "completed" && fact.result === "succeeded"), runSetupScript);
      const finalFacts = yield* harness.sink.readNativeCommandFacts({ threadId: accepted.threadId, commandId: accepted.commandId });
      assert.equal(finalFacts.creationProvenance, "native_created");
      assert.deepEqual(finalFacts.incarnation, creation.incarnation);
      assert.equal(finalFacts.projection?.thread.title, title);
      assert.deepEqual(finalFacts.projection?.thread.modelSelection, modelSelection);
      assert.equal(finalFacts.projection?.thread.worktreePath, harness.resources.worktreePath);
      assert.equal(finalFacts.projection?.messages.find((message) => message.id === accepted.messageId)?.text, prompt);
      assert.deepEqual(harness.externalOrder, runSetupScript
        ? ["worktree", "setup-started", "setup-completed", "setup-detached"]
        : ["worktree"]);
      assert.lengthOf(harness.worktreeInputs, 1);
      assert.equal(harness.worktreeInputs[0]!.cwd, harness.resources.projectCwd);
      assert.equal(harness.worktreeInputs[0]!.path, harness.resources.worktreePath);
      assert.equal(harness.worktreeInputs[0]!.newRefName, harness.resources.branch);
      assert.lengthOf(harness.setupInputs, runSetupScript ? 1 : 0);
      if (runSetupScript) assert.equal(harness.setupInputs[0]!.worktreePath, harness.resources.worktreePath);
      const history = yield* harness.repository.readHistoryByClaim(creation.claimId);
      assert.equal(history.intent.claimedBootId, ServerRuntimeStartup.nativeCreationBootId);
      const outbox = yield* harness.sql<{ command_id: string; effect_type: string; payload_json: string }>`SELECT command_id, effect_type, payload_json FROM orchestration_v2_effect_outbox`;
      assert.lengthOf(outbox, 1);
      assert.equal(outbox[0]!.command_id, accepted.commandId);
      assert.equal(outbox[0]!.effect_type, "provider-turn.start");
      const payload = JSON.parse(outbox[0]!.payload_json);
      assert.equal(payload.nativeCreationExecutionReference.stageCommandId, accepted.commandId);
      assert.equal(payload.nativeCreationExecutionReference.claimId, creation.claimId);
      assert.isFalse(wire.responseBytes.includes("canonicalPreparation"));
      assert.isFalse(wire.responseBytes.includes("canonicalCommand"));
      const replay = yield* sendRequest(connection, harness.request);
      assert.deepEqual(assertAccepted(replay.response, harness.request.id), accepted);
      assert.lengthOf(harness.worktreeInputs, 1);
      assert.lengthOf(harness.setupInputs, runSetupScript ? 1 : 0);
    }).pipe(Effect.scoped),
  );

  it.effect("rejects unavailable native capability before claim or allocation", () =>
    Effect.gen(function* () {
      const harness = yield* buildNativeWsHarness({ name: "unavailable", capabilityAvailable: false });
      const connection = yield* openSocket(harness.url);
      assertRejected((yield* sendRequest(connection, harness.request)).response, harness.request.id, "unsupported_authority");
      assert.lengthOf(harness.worktreeInputs, 0);
      assert.lengthOf(harness.setupInputs, 0);
      const claims = yield* harness.sql<{ count: number }>`SELECT COUNT(*) AS count FROM native_creation_intents`;
      assert.equal(claims[0]?.count, 0);
    }).pipe(Effect.scoped),
  );

  it.effect("holds unknown checkout outcomes across WS retries without another allocation, release or cleanup", () =>
    Effect.gen(function* () {
      const harness = yield* buildNativeWsHarness({ name: "unknown", unknownWorktreeOutcome: true, qualifyRecovery: true });
      const connection = yield* openSocket(harness.url);
      assertRejected((yield* sendRequest(connection, harness.request)).response, harness.request.id, "unresolved_claim");
      assertRejected((yield* sendRequest(connection, harness.request)).response, harness.request.id, "unresolved_claim");
      assert.lengthOf(harness.worktreeInputs, 1);
      assert.lengthOf(harness.setupInputs, 0);
      const history = yield* harness.repository.readHistoryByClaim(`native:v2:${harness.command.commandId}`);
      assert.isTrue(history.effects.some((fact) => fact.kind === "worktree" && fact.phase === "completed" && fact.result === "unknown"));
      assert.isFalse(history.effects.some((fact) => fact.kind === "cleanup"));
      const facts = yield* harness.sink.readNativeCommandFacts({ threadId: harness.command.threadId, commandId: harness.command.commandId });
      assert.isNull(facts.receipt);
      assert.isNull(facts.projection?.thread.deletedAt);
      const effects = yield* harness.sql<{ count: number }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox`;
      assert.equal(effects[0]?.count, 0);
      assert.isNull(yield* harness.repository.readThreadRecoveryCommand(`${harness.command.commandId}:bootstrap-thread-delete`));
    }).pipe(Effect.scoped),
  );

  it.effect.each([false, true])("preserves the known setup failure and allows logical recovery only with current qualified cleanup=%s", (qualifyRecovery) =>
    Effect.gen(function* () {
      const harness = yield* buildNativeWsHarness({ name: `known-${qualifyRecovery}`, setupExitCode: 1, qualifyRecovery });
      const connection = yield* openSocket(harness.url);
      assertRejected((yield* sendRequest(connection, harness.request)).response, harness.request.id, "unresolved_claim");
      const original = harness.originalFailures[0];
      assert.isTrue(original instanceof NativeCreationAuthorityError);
      if (original instanceof NativeCreationAuthorityError) assert.equal(original.message, "Native setup did not complete successfully.");
      const deleteId = CommandId.make(`${harness.command.commandId}:bootstrap-thread-delete`);
      const facts = yield* harness.sink.readNativeCommandFacts({ threadId: harness.command.threadId, commandId: deleteId });
      const companion = yield* harness.repository.readThreadRecoveryCommand(deleteId);
      if (qualifyRecovery) {
        assert.isNotNull(companion);
        assert.equal(facts.receipt?.status, "accepted");
        const deleted = facts.events.filter((stored) => stored.event.type === "thread.deleted");
        assert.lengthOf(deleted, 1);
        assert.equal(deleted[0]!.event.threadId, harness.command.threadId);
        const history = yield* harness.repository.readHistoryByClaim(`native:v2:${harness.command.commandId}`);
        const completed = history.effects.find((fact) => fact.effectId === companion?.commandStartEffectId && fact.phase === "completed");
        assert.isDefined(completed);
        if (completed?.kind === "native_command" && completed.phase === "completed") assert.equal(completed.sequence, facts.receipt?.resultSequence);
        assert.isFalse(history.effects.some((fact) => fact.kind === "cleanup" && fact.phase === "completed"));
      } else {
        assert.isNull(companion);
        assert.isNull(facts.receipt);
        assert.isNull(facts.projection?.thread.deletedAt);
      }
      assertRejected((yield* sendRequest(connection, harness.request)).response, harness.request.id, "unresolved_claim");
      const after = yield* harness.sink.readNativeCommandFacts({ threadId: harness.command.threadId, commandId: deleteId });
      assert.deepEqual(after.receipt, facts.receipt);
      assert.deepEqual(after.events, facts.events);
      assert.lengthOf(harness.worktreeInputs, 1);
      assert.lengthOf(harness.setupInputs, 1);
    }).pipe(Effect.scoped),
  );

  it.effect("never deletes an accepted final release after a later tracker failure", () =>
    Effect.gen(function* () {
      const failure = new Error("Synthetic tracker failure after accepted native release");
      const harness = yield* buildNativeWsHarness({ name: "accepted-release", qualifyRecovery: true, failureAfterRelease: failure });
      const connection = yield* openSocket(harness.url);
      const failed = yield* sendRequest(connection, harness.request);
      const defect = Schema.decodeUnknownSync(defectSchema)(failed.response).defect;
      assert.instanceOf(defect, Error);
      if (defect instanceof Error) assert.equal(defect.message, failure.message);
      assert.equal(harness.originalFailures[0], failure);
      const facts = yield* harness.sink.readNativeCommandFacts({ threadId: harness.command.threadId, commandId: harness.command.commandId });
      assert.equal(facts.receipt?.status, "accepted");
      assert.isNull(facts.projection?.thread.deletedAt);
      assert.isNull(yield* harness.repository.readThreadRecoveryCommand(`${harness.command.commandId}:bootstrap-thread-delete`));
      const replay = yield* sendRequest(connection, harness.request);
      assertAccepted(replay.response, harness.request.id);
      assert.lengthOf(harness.worktreeInputs, 1);
      assert.lengthOf(harness.setupInputs, 1);
    }).pipe(Effect.scoped),
  );

  it.effect("finishes server-owned native preparation once after its WS waiter disconnects", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const complete = yield* Deferred.make<void>();
      yield* Effect.addFinalizer(() => Deferred.succeed(complete, undefined));
      const harness = yield* buildNativeWsHarness({ name: "disconnect", setupEntered: entered, setupCompletion: complete });
      const connection = yield* openSocket(harness.url);
      yield* writeRequest(connection, harness.request);
      yield* Deferred.await(entered);
      yield* Effect.callback<void>((resume) => {
        connection.socket.once("close", () => resume(Effect.void));
        connection.socket.terminate();
      });
      assert.equal((yield* harness.tracker.get(harness.command.threadId))?.phase, "running");
      const retryConnection = yield* openSocket(harness.url);
      assertRejected((yield* sendRequest(retryConnection, harness.request)).response, harness.request.id, "unresolved_claim");
      yield* Deferred.succeed(complete, undefined);
      yield* harness.tracker.stream(harness.command.threadId).pipe(
        Stream.filter((snapshot) => snapshot?.phase === "done"),
        Stream.runHead,
      );
      assertAccepted((yield* sendRequest(retryConnection, harness.request)).response, harness.request.id);
      assert.lengthOf(harness.worktreeInputs, 1);
      assert.lengthOf(harness.setupInputs, 1);
      assert.isNull(yield* harness.repository.readThreadRecoveryCommand(`${harness.command.commandId}:bootstrap-thread-delete`));
    }).pipe(Effect.scoped),
  );

  it.effect("explicit native cancellation before external work owns exact qualified recovery", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const blocked = yield* Deferred.make<void>();
      yield* Effect.addFinalizer(() => Deferred.succeed(blocked, undefined));
      const harness = yield* buildNativeWsHarness({ name: "cancel", cancelEntered: entered, cancelBlocked: blocked });
      const connection = yield* openSocket(harness.url);
      yield* writeRequest(connection, harness.request);
      yield* Deferred.await(entered);
      assert.isTrue(yield* harness.tracker.cancel(harness.command.threadId));
      assert.equal((yield* harness.tracker.get(harness.command.threadId))?.phase, "cancelled");
      assert.lengthOf(harness.worktreeInputs, 0);
      assert.lengthOf(harness.setupInputs, 0);
      const deleteId = CommandId.make(`${harness.command.commandId}:bootstrap-thread-delete`);
      const facts = yield* harness.sink.readNativeCommandFacts({ threadId: harness.command.threadId, commandId: deleteId });
      assert.equal(facts.receipt?.status, "accepted");
      assert.lengthOf(facts.events.filter((stored) => stored.event.type === "thread.deleted" && stored.event.threadId === harness.command.threadId), 1);
      assert.isNotNull(yield* harness.repository.readThreadRecoveryCommand(deleteId));
    }).pipe(Effect.scoped),
  );
});
