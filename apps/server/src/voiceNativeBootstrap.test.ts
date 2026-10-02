import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import { assert, it } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  AuthSessionId,
  ClientOrchestrationCommand,
  DEFAULT_SERVER_SETTINGS,
  OrchestrationCommand,
  OrchestrationRpcSchemas,
  ORCHESTRATION_WS_METHODS,
  ThreadTurnStartCommand,
  WorktreeSetupSnapshot,
} from "@t3tools/contracts";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as RelayClient from "@t3tools/shared/relayClient";
import { SqlitePersistenceMemory } from "./persistence/Layers/Sqlite.ts";
import * as NativeCreationRepositoryLayer from "./persistence/Layers/NativeCreationRepository.ts";
import { NativeCreationAuthorityUnavailable } from "./orchestration/NativeCreationAuthority.ts";
import * as AuthSessions from "./persistence/AuthSessions.ts";
import {
  ConfigProvider,
  Data,
  Deferred,
  Effect,
  FileSystem,
  Layer,
  Path,
  Queue,
  Schema,
  Stream,
} from "effect";
import { HttpClient, HttpRouter, HttpServer } from "effect/unstable/http";
import { RpcSerialization } from "effect/unstable/rpc";
import * as TestClock from "effect/testing/TestClock";
import { websocketRpcRouteLayer } from "./ws.ts";
import * as ServerConfig from "./config.ts";
import { EnvironmentAuth } from "./auth/EnvironmentAuth.ts";
import { SessionStore } from "./auth/SessionStore.ts";
import { PairingGrantStore } from "./auth/PairingGrantStore.ts";
import { AnalyticsService } from "./telemetry/AnalyticsService.ts";
import { OrchestrationEngineService } from "./orchestration/Services/OrchestrationEngine.ts";
import { OrchestrationCommandInvariantError } from "./orchestration/Errors.ts";
import { ThreadDeletionReactor } from "./orchestration/Services/ThreadDeletionReactor.ts";
import { ProjectionSnapshotQuery } from "./orchestration/Services/ProjectionSnapshotQuery.ts";
import { CheckpointDiffQuery } from "./checkpointing/CheckpointDiffQuery.ts";
import { Keybindings } from "./keybindings.ts";
import { EnvironmentThemeService } from "./environmentTheme.ts";
import { UsageLimitSources } from "./usage/UsageLimitSources.ts";
import { ExternalLauncher } from "./process/externalLauncher.ts";
import { RemoteOpenTargets } from "./environment/RemoteOpenTargets.ts";
import { GitWorkflowService } from "./git/GitWorkflowService.ts";
import { ReviewService } from "./review/ReviewService.ts";
import { VcsProvisioningService } from "./vcs/VcsProvisioningService.ts";
import { VcsStatusBroadcaster } from "./vcs/VcsStatusBroadcaster.ts";
import { VcsProcess } from "./vcs/VcsProcess.ts";
import { TerminalManager } from "./terminal/Manager.ts";
import { PreviewManager } from "./preview/Manager.ts";
import { PreviewAutomationBroker } from "./mcp/PreviewAutomationBroker.ts";
import { DeviceService } from "./device/DeviceService.ts";
import { PortDiscovery } from "./preview/PortScanner.ts";
import { ProviderRegistry } from "./provider/Services/ProviderRegistry.ts";
import { ModelManifest } from "./provider/ModelManifest.ts";
import { ProviderService } from "./provider/Services/ProviderService.ts";
import { ProviderSessionDirectory } from "./provider/Services/ProviderSessionDirectory.ts";
import { ProviderAuthService } from "./provider/Services/ProviderAuthService.ts";
import { ProviderInstanceRegistry } from "./provider/Services/ProviderInstanceRegistry.ts";
import { CodexInstallation } from "./provider/CodexInstallation.ts";
import { AntigravityInstallation } from "./provider/AntigravityInstallation.ts";
import { ServerSelfUpdate } from "./cloud/selfUpdate.ts";
import { ServerLifecycleEvents } from "./serverLifecycleEvents.ts";
import { ServerRuntimeStartup } from "./serverRuntimeStartup.ts";
import { ServerSettingsService } from "./serverSettings.ts";
import { WorkspaceEntries } from "./workspace/WorkspaceEntries.ts";
import { WorkspaceFileSystem } from "./workspace/WorkspaceFileSystem.ts";
import { WorkspacePaths } from "./workspace/WorkspacePaths.ts";
import {
  ProjectSetupScriptRunner,
  ProjectSetupScriptOperationError,
} from "./project/ProjectSetupScriptRunner.ts";
import { ProjectCloneTracker } from "./project/ProjectCloneTracker.ts";
import { RepositoryIdentityResolver } from "./project/RepositoryIdentityResolver.ts";
import { ProjectFaviconResolver } from "./project/ProjectFaviconResolver.ts";
import { ServerSecretStore } from "./auth/ServerSecretStore.ts";
import * as WorktreeSetupTracker from "./project/WorktreeSetupTracker.ts";
import { ServerEnvironment } from "./environment/ServerEnvironment.ts";
import { BackgroundPolicy } from "./background/BackgroundPolicy.ts";
import { SourceControlRepositoryService } from "./sourceControl/SourceControlRepositoryService.ts";
import { PullRequestService } from "./pullRequest/PullRequestService.ts";
import { PullRequestSyncReactor } from "./orchestration/PullRequestSyncReactor.ts";
import { ProcessDiagnostics } from "./diagnostics/ProcessDiagnostics.ts";
import { TraceDiagnostics } from "./diagnostics/TraceDiagnostics.ts";
import { HostResources } from "./resourceTelemetry/HostResources.ts";
import { ProcessResourceMonitor } from "./diagnostics/ProcessResourceMonitor.ts";
import { ResourceTelemetry } from "./resourceTelemetry/ResourceTelemetry.ts";
import { UsageService } from "./usage/UsageService.ts";

const commandSchema = ClientOrchestrationCommand;
const nativeRequestSchema = Schema.Struct({
  _tag: Schema.Literal("Request"),
  id: Schema.String,
  tag: Schema.Literal(ORCHESTRATION_WS_METHODS.dispatchCommand),
  payload: commandSchema,
  headers: Schema.Array(Schema.Tuple([Schema.String, Schema.String])),
});
const nativeSuccessSchema = Schema.Struct({
  _tag: Schema.Literal("Exit"),
  requestId: Schema.String,
  exit: Schema.Struct({
    _tag: Schema.Literal("Success"),
    value: OrchestrationRpcSchemas.dispatchCommand.output,
  }),
});
const nativeFailureSchema = Schema.Struct({
  _tag: Schema.Literal("Exit"),
  requestId: Schema.String,
  exit: Schema.Struct({ _tag: Schema.Literal("Failure"), cause: Schema.Unknown }),
});
const decodeNativeRequest = Schema.decodeUnknownEffect(nativeRequestSchema);
const decodeTurnStart = Schema.decodeUnknownEffect(ThreadTurnStartCommand);
const decodeNativeSuccess = Schema.decodeUnknownSync(nativeSuccessSchema);
const decodeNativeFailure = Schema.decodeUnknownEffect(nativeFailureSchema);
const isWorktreeSetupSnapshot = Schema.is(WorktreeSetupSnapshot);
const fixtureUrl = new URL(
  "./voiceNativeBootstrap.fixtures/python-native-request.json.txt",
  import.meta.url,
);
const nativeWorktreePath = "/voice-fixture/native-worktree";
const bearer = "synthetic-test-bearer";

const loadRequest = Effect.fnUntraced(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const fixtureBytes = yield* fs.readFileString(yield* path.fromFileUrl(fixtureUrl));
  const serializer = yield* RpcSerialization.RpcSerialization;
  const [request] = serializer.makeUnsafe().decode(fixtureBytes);
  const decoded = yield* decodeNativeRequest(request);
  const payload = yield* decodeTurnStart(decoded.payload);
  return { ...decoded, payload };
});

const requestWithSetup = (
  request: Effect.Success<ReturnType<typeof loadRequest>>,
  runSetupScript: boolean,
) => ({
  ...request,
  payload: { ...request.payload, bootstrap: { ...request.payload.bootstrap, runSetupScript } },
});

type HarnessOptions = {
  repository?: boolean;
  baseCommit?: boolean;
  failAt?: "message" | "worktree" | "setup-launch" | "setup-completion";
  worktreeGate?: Deferred.Deferred<void>;
  worktreeEntered?: Deferred.Deferred<void>;
};

// This seam proves the existing trusted WS bootstrap path. It does not remove
// or qualify the engine's dispatch guard, queue activation, or live providers.
const buildNativeWsHarness = Effect.fnUntraced(function* (options: HarnessOptions = {}) {
  const commands: Array<OrchestrationCommand> = [];
  const effects: string[] = [];
  const ownershipInputs: Array<
    Parameters<OrchestrationEngineService["Service"]["acquireWorktreeOwnership"]>[0]
  > = [];
  const worktreeInputs: Array<Parameters<GitWorkflowService["Service"]["createWorktree"]>[0]> = [];
  const setupInputs: Array<Parameters<ProjectSetupScriptRunner["Service"]["runForThread"]>[0]> = [];
  const completed = yield* Deferred.make<void>();
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "t3-voice-native-bootstrap-" });
  const config = ServerConfig.layerTest(scratch, scratch);
  const tracker = yield* WorktreeSetupTracker.make;
  const fixture = yield* loadRequest();
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      if (options.worktreeGate) yield* Deferred.succeed(options.worktreeGate, undefined);
      yield* tracker.cancel(fixture.payload.threadId);
      // The real tracker owns a 30-second retained snapshot. Advance only this
      // test's clock so its detached retention fiber removes its own state.
      yield* TestClock.adjust("30 seconds");
      assert.isNull(yield* tracker.get(fixture.payload.threadId));
    }),
  );
  const seams = Layer.mergeAll(
    Layer.mock(EnvironmentAuth)({
      authenticateWebSocketUpgrade: (request) =>
        Effect.sync(() => {
          assert.equal(request.headers.authorization, `Bearer ${bearer}`);
          effects.push("bearer-upgrade");
          return {
            sessionId: AuthSessionId.make("voice-test-session"),
            subject: "voice-test",
            method: "bearer-access-token" as const,
            scopes: [AuthOrchestrationOperateScope],
          };
        }),
    }),
    Layer.mock(SessionStore)({
      cookieName: "voice-test-cookie",
      legacyCookieName: undefined,
      recordClientConnection: () => Effect.void,
      markConnected: () => Effect.void,
      markDisconnected: () => Effect.void,
    }),
    Layer.mock(AnalyticsService)({ record: () => Effect.void }),
    Layer.mock(OrchestrationEngineService)({
      dispatch: (command) =>
        Effect.gen(function* () {
          commands.push(command);
          effects.push(command.type);
          if (options.failAt === "message" && command.type === "thread.message.user.append") {
            return yield* new OrchestrationCommandInvariantError({
              commandType: command.type,
              detail: "injected message persistence failure",
            });
          }
          const result = { sequence: commands.length };
          if (
            command.type === "thread.activity.append" &&
            command.activity.kind === "worktree-setup" &&
            isWorktreeSetupSnapshot(command.activity.payload) &&
            command.activity.payload.phase !== "running"
          ) {
            yield* Deferred.succeed(completed, undefined);
          }
          return result;
        }),
      acquireWorktreeOwnership: (threadId) =>
        Effect.sync(() => {
          effects.push("ownership");
          ownershipInputs.push(threadId);
          return {
            resourcePath: nativeWorktreePath,
            leaseId: "voice-test-lease",
            ownerThreadId: threadId,
            ownerIncarnation: "voice-test-create-event",
            branch: fixture.payload.bootstrap?.prepareWorktree?.branch ?? null,
            acquiredAtMs: 0,
            renewedAtMs: 0,
            expiresAtMs: 300_000,
          };
        }),
      readEvents: () => Stream.empty,
    }),
    Layer.mock(ThreadDeletionReactor)({ drainThrough: () => Effect.void }),
    Layer.mock(GitWorkflowService)({
      isRepository: () => Effect.succeed(options.repository ?? true),
      hasCommit: () => Effect.succeed(options.baseCommit ?? true),
      createWorktree: (input) =>
        Effect.gen(function* () {
          effects.push("native-worktree");
          worktreeInputs.push(input);
          if (options.worktreeEntered) yield* Deferred.succeed(options.worktreeEntered, undefined);
          if (options.worktreeGate) yield* Deferred.await(options.worktreeGate);
          if (options.failAt === "worktree")
            return yield* Effect.die(new Error("injected worktree failure"));
          return {
            worktree: { path: nativeWorktreePath, refName: input.newRefName ?? "voice/bootstrap" },
          };
        }),
    }),
    Layer.mock(VcsStatusBroadcaster)({ refreshStatus: () => Effect.die("unused status result") }),
    Layer.mock(ProjectSetupScriptRunner)({
      runForThread: (input) =>
        Effect.gen(function* () {
          effects.push("setup");
          setupInputs.push(input);
          if (options.failAt === "setup-launch") {
            return yield* new ProjectSetupScriptOperationError({
              threadId: input.threadId,
              worktreePath: input.worktreePath,
              operation: "openTerminal",
              cause: { message: "injected setup launch failure" },
            });
          }
          return {
            status: "started" as const,
            scriptId: "setup",
            scriptName: "Setup",
            scriptCommand: "synthetic setup",
            terminalId: "voice-test-setup",
            cwd: nativeWorktreePath,
            async: false,
            completion:
              options.failAt === "setup-completion"
                ? Effect.die(new Error("injected setup completion failure"))
                : Effect.succeed({ exitCode: 0, durationMs: 1 }),
          };
        }),
    }),
    Layer.succeed(WorktreeSetupTracker.WorktreeSetupTracker, tracker),
    Layer.mock(ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(ServerRuntimeStartup)({
      enqueueCommand: (effect) => effect,
      markRunningProviderSessionsForContinuation: Effect.succeed([]),
      markOptedInProviderSessionsForContinuation: Effect.succeed([]),
      clearProviderSessionContinuationMarkers: () => Effect.void,
    }),
    Layer.mock(ServerSettingsService)({ getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS) }),
    Layer.mock(ProjectionSnapshotQuery)({ getProjectShellById: () => Effect.succeedNone }),
    Layer.mock(BackgroundPolicy)({ removeRpcClient: () => Effect.void }),
    Layer.mock(VcsProcess)({ run: () => Effect.die("Unexpected native process invocation") }),
    Layer.mock(CheckpointDiffQuery)({}),
    Layer.mock(Keybindings)({}),
    Layer.mock(EnvironmentThemeService)({}),
    Layer.mock(UsageLimitSources)({}),
    Layer.mock(ExternalLauncher)({}),
    Layer.mock(RemoteOpenTargets)({}),
    Layer.mock(ReviewService)({}),
    Layer.mock(VcsProvisioningService)({}),
    Layer.mock(TerminalManager)({}),
    Layer.mock(PreviewManager)({}),
    Layer.mock(PreviewAutomationBroker)({}),
    Layer.mock(DeviceService)({}),
    Layer.mock(PortDiscovery)({}),
    Layer.mock(ProviderRegistry)({}),
    Layer.mock(ModelManifest)({}),
    Layer.mock(ProviderService)({}),
    Layer.mock(ProviderSessionDirectory)({}),
    Layer.mock(ProviderAuthService)({}),
    Layer.mock(ProviderInstanceRegistry)({}),
    Layer.mock(CodexInstallation)({
      managedDirectory: path.join(scratch, "unused-codex-runtime"),
    }),
    Layer.mock(AntigravityInstallation)({
      managedDirectory: path.join(scratch, "unused-antigravity-runtime"),
    }),
    Layer.mock(ServerSelfUpdate)({}),
    Layer.mock(ServerLifecycleEvents)({}),
    Layer.mock(WorkspaceEntries)({}),
    Layer.mock(WorkspaceFileSystem)({}),
    Layer.mock(WorkspacePaths)({}),
    Layer.mock(RepositoryIdentityResolver)({}),
    Layer.mock(ProjectFaviconResolver)({}),
    Layer.mock(ServerSecretStore)({}),
    Layer.mock(ServerEnvironment)({}),
    Layer.mock(SourceControlRepositoryService)({}),
    Layer.mock(PullRequestService)({}),
    Layer.mock(PullRequestSyncReactor)({}),
    Layer.mock(PairingGrantStore)({}),
    Layer.mock(ProcessDiagnostics)({}),
    Layer.mock(TraceDiagnostics)({}),
    Layer.mock(HostResources)({}),
    Layer.mock(ProcessResourceMonitor)({}),
    Layer.mock(ResourceTelemetry)({}),
    Layer.mock(UsageService)({}),
    Layer.mock(RelayClient.RelayClient)({}),
    Layer.succeed(HostProcessEnvironment, {}),
    Layer.succeed(HostProcessPlatform, "linux"),
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("Unexpected HTTP request")),
    ),
  );
  yield* Layer.build(
    HttpRouter.serve(websocketRpcRouteLayer, {
      disableListenLog: true,
      disableLogger: true,
    }).pipe(
      Layer.provide(seams),
      Layer.provide(config),
      Layer.provide(
        NativeCreationAuthorityUnavailable.pipe(
          Layer.provide(AuthSessions.layer),
          Layer.provideMerge(NativeCreationRepositoryLayer.layer),
        ),
      ),
      Layer.provide(SqlitePersistenceMemory),
      Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
      Layer.provide(NodeServices.layer),
    ),
  );
  const server = yield* HttpServer.HttpServer;
  assert.notEqual(server.address._tag, "UnixPathAddress");
  if (server.address._tag === "UnixPathAddress")
    return yield* Effect.die("Expected TCP test server");
  return {
    url: `ws://127.0.0.1:${server.address.port}/ws`,
    commands,
    effects,
    worktreeInputs,
    setupInputs,
    ownershipInputs,
    completed,
  };
});

class NativeWebSocketError extends Data.TaggedError("NativeWebSocketError")<{
  readonly cause: unknown;
}> {}

const openSocket = Effect.fnUntraced(function* (url: string) {
  const messages = yield* Queue.unbounded<Effect.Effect<string, NativeWebSocketError>>();
  const socket = yield* Effect.acquireRelease(
    Effect.sync(
      () =>
        new NodeSocket.NodeWS.WebSocket(url, {
          headers: { authorization: `Bearer ${bearer}` },
        }),
    ),
    (socket) => Effect.sync(() => socket.terminate()),
  );
  socket.on("message", (bytes) => {
    Queue.offerUnsafe(messages, Effect.succeed(bytes.toString()));
  });
  socket.on("error", (error) => {
    Queue.offerUnsafe(messages, Effect.fail(new NativeWebSocketError({ cause: error })));
  });
  socket.on("close", (code, reason) => {
    Queue.offerUnsafe(
      messages,
      Effect.fail(new NativeWebSocketError({ cause: { code, reason: reason.toString() } })),
    );
  });
  yield* Effect.callback<void, NativeWebSocketError>((resume) => {
    socket.once("open", () => resume(Effect.void));
    socket.once("error", (error) =>
      resume(Effect.fail(new NativeWebSocketError({ cause: error }))),
    );
  });
  return { socket, messages };
});

const sendRequest = Effect.fnUntraced(function* (
  connection: Effect.Success<ReturnType<typeof openSocket>>,
  request: typeof nativeRequestSchema.Type,
) {
  const serializer = yield* RpcSerialization.RpcSerialization;
  const bytes = serializer.makeUnsafe().encode(request);
  assert.isDefined(bytes);
  connection.socket.send(bytes!);
  const responseBytes = yield* Queue.take(connection.messages).pipe(Effect.flatten);
  const [response] = serializer.makeUnsafe().decode(responseBytes);
  return { response, responseBytes, requestBytes: bytes! };
});

const assertFinalSequence = (
  response: unknown,
  request: typeof nativeRequestSchema.Type,
  commands: ReadonlyArray<OrchestrationCommand>,
) => {
  const success = decodeNativeSuccess(response);
  assert.equal(success.requestId, request.id);
  const finalIndex = commands.findIndex((command) => command.type === "thread.turn.start");
  assert.isAtLeast(finalIndex, 0);
  assert.equal(success.exit.value.sequence, finalIndex + 1);
  const final = commands[finalIndex];
  assert.equal(final?.type, "thread.turn.start");
  if (final?.type === "thread.turn.start") {
    assert.equal(final.bootstrap, undefined);
    assert.equal(final.commandId, request.payload.commandId);
    assert.equal(
      final.message.messageId,
      request.payload.type === "thread.turn.start" ? request.payload.message.messageId : undefined,
    );
  }
};

it.layer(Layer.mergeAll(NodeServices.layer, RpcSerialization.layerJson))(
  "native voice bootstrap source conformance",
  (it) => {
    it.effect(
      "decodes the Python-shaped Request with the real JSON serializer and native command schema",
      () =>
        Effect.gen(function* () {
          const request = yield* loadRequest();
          assert.equal(request._tag, "Request");
          assert.equal(typeof request.id, "string");
          assert.deepEqual(request.headers, []);
          const turn = yield* decodeTurnStart(request.payload);
          assert.isTrue(turn.bootstrap?.prepareWorktree?.requireWorktree);
          assert.isNull(turn.bootstrap?.createThread?.worktreePath);
          assert.equal(turn.bootstrap?.runSetupScript, false);
          const nativeSelection = turn.bootstrap?.createThread?.modelSelection;
          assert.isDefined(nativeSelection);
          assert.notEqual(nativeSelection?.instanceId, "voice-backend");
          if (turn.modelSelection) assert.deepEqual(turn.modelSelection, nativeSelection);
        }),
    );

    it.effect.each([false, true])(
      "uses actual WS bootstrap with runSetupScript=%s and native final sequence bytes",
      (runSetupScript) =>
        Effect.gen(function* () {
          const request = requestWithSetup(yield* loadRequest(), runSetupScript);
          const harness = yield* buildNativeWsHarness();
          const connection = yield* openSocket(harness.url);
          const result = yield* sendRequest(connection, request);
          assertFinalSequence(result.response, request, harness.commands);
          const evidenceDir = process.env.T3_VOICE_BOOTSTRAP_EVIDENCE_DIR;
          if (evidenceDir) {
            const fs = yield* FileSystem.FileSystem;
            const path = yield* Path.Path;
            assert.isBelow(Buffer.byteLength(result.responseBytes), 64 * 1024);
            yield* fs.writeFileString(
              path.join(evidenceDir, `native-success-response-${runSetupScript}.json`),
              result.responseBytes,
            );
            assert.isBelow(Buffer.byteLength(result.requestBytes), 64 * 1024);
            yield* fs.writeFile(
              path.join(evidenceDir, `native-success-request-${runSetupScript}.json`),
              typeof result.requestBytes === "string"
                ? new TextEncoder().encode(result.requestBytes)
                : result.requestBytes,
            );
          }
          const serializer = yield* RpcSerialization.RpcSerialization;
          assert.deepEqual(
            serializer.makeUnsafe().decode(new TextEncoder().encode(result.responseBytes)),
            [result.response],
          );
          assert.deepEqual(
            harness.effects.filter((effect) =>
              [
                "thread.create",
                "thread.message.user.append",
                "native-worktree",
                "thread.meta.update",
                "ownership",
                "setup",
                "thread.turn.start",
              ].includes(effect),
            ),
            [
              "thread.create",
              "thread.message.user.append",
              "native-worktree",
              "thread.meta.update",
              ...(runSetupScript ? ["ownership", "setup"] : []),
              "thread.turn.start",
            ],
          );
          const created = harness.commands.find((command) => command.type === "thread.create");
          assert.equal(created?.worktreePath, null);
          assert.deepEqual(
            created?.modelSelection,
            request.payload.bootstrap.createThread?.modelSelection,
          );
          const message = harness.commands.find(
            (command) => command.type === "thread.message.user.append",
          );
          assert.deepEqual(message?.message, {
            messageId: request.payload.message.messageId,
            text: request.payload.message.text,
            attachments: [],
          });
          assert.equal(harness.worktreeInputs[0]?.path, null);
          assert.equal(
            harness.worktreeInputs[0]?.cwd,
            request.payload.bootstrap.prepareWorktree?.projectCwd,
          );
          assert.equal(
            harness.worktreeInputs[0]?.baseRefName,
            request.payload.bootstrap.prepareWorktree?.baseBranch,
          );
          assert.equal(
            harness.worktreeInputs[0]?.newRefName,
            request.payload.bootstrap.prepareWorktree?.branch,
          );
          const metadata = harness.commands.find(
            (command) => command.type === "thread.meta.update",
          );
          assert.equal(metadata?.worktreePath, nativeWorktreePath);
          assert.equal(metadata?.branch, request.payload.bootstrap.prepareWorktree?.branch);
          assert.equal(harness.setupInputs.length, runSetupScript ? 1 : 0);
          assert.deepEqual(
            harness.ownershipInputs,
            runSetupScript ? [request.payload.threadId] : [],
          );
          if (runSetupScript) {
            assert.equal(harness.setupInputs[0]?.threadId, request.payload.threadId);
            assert.equal(
              harness.setupInputs[0]?.projectId,
              request.payload.bootstrap.createThread?.projectId,
            );
            assert.equal(
              harness.setupInputs[0]?.projectCwd,
              request.payload.bootstrap.prepareWorktree?.projectCwd,
            );
            assert.equal(harness.setupInputs[0]?.worktreePath, nativeWorktreePath);
            assert.isDefined(harness.setupInputs[0]?.observeCompletion);
          }
        }).pipe(Effect.scoped, Effect.provide(NodeHttpServer.layerTest)),
    );

    it.effect.each([{ repository: false }, { baseCommit: false }])(
      "rejects required worktree preconditions before creating any thread: %j",
      (options) =>
        Effect.gen(function* () {
          const request = yield* loadRequest();
          const harness = yield* buildNativeWsHarness(options);
          const connection = yield* openSocket(harness.url);
          const { response } = yield* sendRequest(connection, request);
          const failure = yield* decodeNativeFailure(response);
          assert.equal(failure.requestId, request.id);
          assert.isFalse(harness.commands.some((command) => command.type === "thread.create"));
          assert.isFalse(harness.commands.some((command) => command.type === "thread.turn.start"));
          assert.lengthOf(harness.worktreeInputs, 0);
        }).pipe(Effect.scoped, Effect.provide(NodeHttpServer.layerTest)),
    );

    it.effect.each(["message", "worktree", "setup-completion"] as const)(
      "returns a native failure after partial effects at %s without another bootstrap attempt",
      (failAt) =>
        Effect.gen(function* () {
          const request = requestWithSetup(yield* loadRequest(), failAt === "setup-completion");
          const harness = yield* buildNativeWsHarness({ failAt });
          const connection = yield* openSocket(harness.url);
          const { response } = yield* sendRequest(connection, request);
          const failure = yield* decodeNativeFailure(response);
          assert.equal(failure.requestId, request.id);
          assert.equal(
            harness.commands.filter((command) => command.type === "thread.create").length,
            1,
          );
          assert.isFalse(harness.commands.some((command) => command.type === "thread.turn.start"));
          assert.equal(
            harness.commands.filter((command) => command.type === "thread.delete").length,
            1,
          );
          assert.isAtMost(harness.worktreeInputs.length, 1);
          assert.isAtMost(harness.setupInputs.length, 1);
        }).pipe(Effect.scoped, Effect.provide(NodeHttpServer.layerTest)),
    );

    it.effect(
      "records a setup launch failure and still returns the native accepted final turn sequence",
      () =>
        Effect.gen(function* () {
          const request = requestWithSetup(yield* loadRequest(), true);
          const harness = yield* buildNativeWsHarness({ failAt: "setup-launch" });
          const connection = yield* openSocket(harness.url);
          const { response } = yield* sendRequest(connection, request);
          assertFinalSequence(response, request, harness.commands);
          assert.isTrue(
            harness.commands.some(
              (command) =>
                command.type === "thread.activity.append" &&
                command.activity.kind === "setup-script.failed",
            ),
          );
          assert.isFalse(harness.commands.some((command) => command.type === "thread.delete"));
          assert.lengthOf(harness.setupInputs, 1);
        }).pipe(Effect.scoped, Effect.provide(NodeHttpServer.layerTest)),
    );

    it.effect("continues server-owned bootstrap after the requesting socket disconnects", () =>
      Effect.gen(function* () {
        const request = yield* loadRequest();
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const harness = yield* buildNativeWsHarness({
          worktreeEntered: entered,
          worktreeGate: release,
        });
        const connection = yield* openSocket(harness.url);
        const serializer = yield* RpcSerialization.RpcSerialization;
        connection.socket.send(serializer.makeUnsafe().encode(request)!);
        yield* Deferred.await(entered);
        assert.isTrue(harness.commands.some((command) => command.type === "thread.create"));
        assert.isFalse(harness.commands.some((command) => command.type === "thread.turn.start"));
        yield* Effect.callback<void>((resume) => {
          connection.socket.once("close", () => resume(Effect.void));
          connection.socket.terminate();
        });
        yield* Deferred.succeed(release, undefined);
        yield* Deferred.await(harness.completed);
        assert.equal(
          harness.commands.filter((command) => command.type === "thread.turn.start").length,
          1,
        );
        assert.isFalse(harness.commands.some((command) => command.type === "thread.delete"));
        assert.lengthOf(harness.worktreeInputs, 1);
      }).pipe(Effect.scoped, Effect.provide(NodeHttpServer.layerTest)),
    );
  },
);
