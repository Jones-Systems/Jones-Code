import * as NativeCreationRepositoryLayer from "./persistence/Layers/NativeCreationRepository.ts";
import { NativeCreationRepository } from "./persistence/Services/NativeCreationRepository.ts";
import {
  NativeCreationAuthority,
  NativeCreationAuthorityLive,
  NativeCreationAuthorityError,
  NativeCreationGrantResolver,
  NativeCreationBindingResolver,
  NativeCreationGrantResolverUnavailable,
  NativeCreationBindingResolverUnavailable,
  getNativeCreationExecutionReference,
  authorizeNativeCreationExecution,
} from "./orchestration-v2/NativeCreationAuthority.ts";
import * as AuthSessions from "./persistence/AuthSessions.ts";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import {
  NativePreparationBinding,
  nativePreparationCommand,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativeCreationV2CommandDigest,
} from "./orchestration-v2/NativeCreationPreparation.ts";
import { NativeCreationHistoricalBinding } from "@t3tools/contracts";
import { nativeWorktreePath } from "./vcs/worktreePath.ts";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "node:crypto";
import * as Context from "effect/Context";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";

import {
  type DeviceServiceState,
  AuthAccessTokenType,
  AuthStandardClientScopes,
  AuthEnvironmentBootstrapTokenType,
  AuthTokenExchangeGrantType,
  CommandId,
  NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES,
  NativeBootstrapSubmission,
  DEFAULT_SERVER_SETTINGS,
  type DpopFailureReason,
  EnvironmentId,
  EventId,
  GitCommandError,
  KeybindingRule,
  MessageId,
  ExternalLauncherCommandNotFoundError,
  OrchestrationV2ShellSnapshot,
  OrchestrationV2ThreadDetailSnapshot,
  TerminalNotRunningError,
  ORCHESTRATION_V2_WS_METHODS,
  ORCHESTRATION_PROTOCOL_VERSION,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  ORCHESTRATION_PROTOCOL_QUERY_PARAM,
  OrchestrationV2Command,
  type ThreadTurnDispatchGuardV2,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2ThreadStreamItem,
  type OrchestrationV2ShellStreamItem,
  type OrchestrationV2TurnItem,
  type OrchestrationV2DomainEvent,
  TurnItemId,
  ProviderSessionId,
  ProviderThreadId,
  type PreviewEvent,
  ProjectId,
  ProjectMutation,
  type ProviderAuthState,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderQueueRefreshResult,
  type ProviderQueueInventory,
  ServerProvider,
  type ProviderInstallState,
  ProviderSetupError,
  SourceControlRepositoryError,
  ResolvedKeybindingRule,
  type ServerLifecycleStreamEvent,
  ThreadId,
  TurnId,
  UsageLimitSourceId,
  USAGE_CONTRACT_VERSION,
  type UsagePricing,
  WS_METHODS,
  WsRpcGroup,
  EditorId,
  WorktreeSetupSnapshot,
  type WorktreeSetupStageId,
} from "@t3tools/contracts";
import {
  computeDpopAccessTokenHash,
  computeDpopJwkThumbprint,
  type DpopPublicJwk,
} from "@t3tools/shared/dpop";
import { RELAY_HEALTH_REQUEST_TYP, RELAY_MINT_REQUEST_TYP } from "@t3tools/shared/relayJwt";
import * as RelayClient from "@t3tools/shared/relayClient";
import { assert, it } from "@effect/vitest";
import { assertFailure, assertInclude, assertTrue } from "@effect/vitest/utils";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Tracer from "effect/Tracer";
import { ChildProcessSpawner } from "effect/unstable/process";
import {
  FetchHttpClient,
  HttpBody,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
  HttpRouter,
  HttpServer,
} from "effect/unstable/http";
import { OtlpSerialization, OtlpTracer } from "effect/unstable/observability";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import * as NetAddress from "effect/unstable/net/NetAddress";
import * as Socket from "effect/unstable/socket/Socket";
import { vi } from "vite-plus/test";

const TEST_EPOCH = DateTime.makeUnsafe("1970-01-01T00:00:00.000Z");
const SUCCESSFUL_GIT_EXECUTION = {
  exitCode: ChildProcessSpawner.ExitCode(0),
  stdout: "",
  stderr: "",
  stdoutTruncated: false,
  stderrTruncated: false,
};
// HTTP bodies use the JSON codec, which carries V2 DateTime.Utc fields as ISO strings.
const decodeTransferThreadSnapshot = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.toCodecJson(OrchestrationV2ThreadDetailSnapshot)),
);
const decodeTransferShellSnapshot = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.toCodecJson(OrchestrationV2ShellSnapshot)),
);
const encodeTestJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeTestJsonEffect = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

import * as BackgroundPolicy from "./background/BackgroundPolicy.ts";
import * as ServerConfig from "./config.ts";
import * as DeviceService from "./device/DeviceService.ts";
import { HTTP_ROUTER_CONFIG, makeRoutesLayer } from "./server.ts";
import { resolveAvailableEditorsForConfig, resolveFileManagerRevealKindForConfig } from "./ws.ts";
import * as CheckpointDiffQuery from "./checkpointing/CheckpointDiffQuery.ts";
import * as GitManager from "./git/GitManager.ts";
import * as EnvironmentTheme from "./environmentTheme.ts";
import * as UsageLimitSources from "./usage/UsageLimitSources.ts";
import * as Keybindings from "./keybindings.ts";
import * as ExternalLauncher from "./process/externalLauncher.ts";
import * as RemoteOpenTargets from "./environment/RemoteOpenTargets.ts";
import * as Orchestrator from "./orchestration-v2/Orchestrator.ts";
import * as EventSink from "./orchestration-v2/EventSink.ts";
import * as EffectOutbox from "./orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "./orchestration-v2/EffectWorker.ts";
import * as ResourceCleanup from "./orchestration-v2/ResourceCleanupService.ts";
import { makeThreadLiveEventCoalescer } from "./orchestration-v2/ThreadLiveEventCoalescer.ts";
import * as ThreadManagement from "./orchestration-v2/ThreadManagementService.ts";
import * as ThreadLaunch from "./orchestration-v2/ThreadLaunchService.ts";
import * as CommandReceiptStore from "./orchestration-v2/CommandReceiptStore.ts";
import * as EventStore from "./orchestration-v2/EventStore.ts";
import * as TurnItemPositionStore from "./orchestration-v2/TurnItemPositionStore.ts";
import * as ThreadCommandExecutor from "./orchestration-v2/ThreadCommandExecutor.ts";
import * as IdAllocator from "./orchestration-v2/IdAllocator.ts";
import * as ProjectionStore from "./orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import * as ProjectService from "./project/ProjectService.ts";
import * as ProjectEnrichmentService from "./project/ProjectEnrichmentService.ts";
import * as ThreadSearch from "./orchestration-v2/ThreadSearch.ts";
import * as CheckpointStore from "./checkpointing/CheckpointStore.ts";
import * as McpSessionRegistry from "./mcp/McpSessionRegistry.ts";
import * as TextGeneration from "./textGeneration/TextGeneration.ts";
import * as ProviderSessionManager from "./orchestration-v2/ProviderSessionManager.ts";
import * as ProviderSessionGoalService from "./orchestration-v2/ProviderSessionGoalService.ts";
import * as AcpRegistrySupport from "./provider/acp/AcpRegistrySupport.ts";
import * as AcpRegistryRuntimeCoordinator from "./provider/acp/AcpRegistryRuntimeCoordinator.ts";
import * as ProviderMaintenanceRunner from "./provider/providerMaintenanceRunner.ts";
import * as PullRequestSyncReactor from "./orchestration-v2/PullRequestSyncReactor.ts";
import { CodexProviderCapabilitiesV2 } from "./orchestration-v2/Adapters/CodexAdapterV2.ts";
import { ClaudeProviderCapabilitiesV2 } from "./orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import {
  OrchestrationV2ProductionLayerLive,
  OrchestrationV2EventSinkLayerLive,
} from "./orchestration-v2/runtimeLayer.ts";
import { SqlitePersistenceMemory } from "./persistence/Layers/Sqlite.ts";
import { SessionStore, type SessionCredentialInternalError } from "./auth/SessionStore.ts";
import { OrchestrationEventStore } from "./persistence/Services/OrchestrationEventStore.ts";
import { PersistenceSqlError } from "./persistence/Errors.ts";
import * as ProviderRegistry from "./provider/Services/ProviderRegistry.ts";
import * as ModelManifest from "./provider/ModelManifest.ts";
import {
  ProviderAdapterProtocolError,
  type ProviderAdapterV2SessionRuntime,
} from "./orchestration-v2/ProviderAdapter.ts";
import { ProviderAuthService } from "./provider/Services/ProviderAuthService.ts";
import { ProviderInstanceRegistry } from "./provider/Services/ProviderInstanceRegistry.ts";
import {
  AntigravityInstallation,
  AntigravityInstallationError,
} from "./provider/AntigravityInstallation.ts";
import { CodexInstallation } from "./provider/CodexInstallation.ts";
import type { ProviderInstance } from "./provider/ProviderDriver.ts";
import {
  makeManualOnlyProviderMaintenanceCapabilities,
  ProviderVersionCache,
} from "./provider/providerMaintenance.ts";
import * as ServerLifecycleEvents from "./serverLifecycleEvents.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";
import * as ServerActivation from "./serverActivation.ts";
import * as ServiceLauncherClient from "./cloud/serviceLauncherClient.ts";
import * as ServerSettings from "./serverSettings.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import * as ProjectCloneTracker from "./project/ProjectCloneTracker.ts";
import * as WorktreeSetupTracker from "./project/WorktreeSetupTracker.ts";
import * as PreviewManager from "./preview/Manager.ts";
import * as PortScanner from "./preview/PortScanner.ts";
import * as BrowserTraceCollector from "./observability/BrowserTraceCollector.ts";
import * as NativeAppIconResolver from "./assets/NativeAppIconResolver.ts";
import * as ProjectFaviconResolver from "./project/ProjectFaviconResolver.ts";
import * as T3ProjectFileLoader from "./project/T3ProjectFileLoader.ts";
import * as ProjectSetupScriptRunner from "./project/ProjectSetupScriptRunner.ts";
import * as RepositoryIdentityResolver from "./project/RepositoryIdentityResolver.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import * as WorkspaceEntries from "./workspace/WorkspaceEntries.ts";
import * as WorkspaceFileSystem from "./workspace/WorkspaceFileSystem.ts";
import * as WorkspacePaths from "./workspace/WorkspacePaths.ts";
import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";
import * as VcsDriver from "./vcs/VcsDriver.ts";
import * as VcsStatusBroadcaster from "./vcs/VcsStatusBroadcaster.ts";
import * as VcsDriverRegistry from "./vcs/VcsDriverRegistry.ts";
import * as VcsProvisioningService from "./vcs/VcsProvisioningService.ts";
import * as GitHubCli from "./sourceControl/GitHubCli.ts";
import * as AzureDevOpsCli from "./sourceControl/AzureDevOpsCli.ts";
import * as BitbucketApi from "./sourceControl/BitbucketApi.ts";
import * as ForgejoCli from "./sourceControl/ForgejoCli.ts";
import * as GitLabCli from "./sourceControl/GitLabCli.ts";
import * as SourceControlProviderRegistry from "./sourceControl/SourceControlProviderRegistry.ts";
import * as VcsProcess from "./vcs/VcsProcess.ts";
import * as GitWorkflowService from "./git/GitWorkflowService.ts";
import * as ReviewService from "./review/ReviewService.ts";
import * as SourceControlRepositoryService from "./sourceControl/SourceControlRepositoryService.ts";
import { REPLAY_MARKER_MAX_AGE } from "./auth/replayMarkers.ts";
import * as ServerSecretStore from "./auth/ServerSecretStore.ts";
import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import * as PairingGrantStore from "./auth/PairingGrantStore.ts";
import * as CloudManagedEndpointRuntime from "./cloud/ManagedEndpointRuntime.ts";
import * as AgentAwarenessRelay from "./relay/AgentAwarenessRelay.ts";
import * as CloudCliTokenManager from "./cloud/CliTokenManager.ts";
import * as ProcessDiagnostics from "./diagnostics/ProcessDiagnostics.ts";
import * as HostResources from "./resourceTelemetry/HostResources.ts";
import * as ProcessResourceMonitor from "./diagnostics/ProcessResourceMonitor.ts";
import * as TraceDiagnostics from "./diagnostics/TraceDiagnostics.ts";
import * as DesktopTelemetryReceiver from "./resourceTelemetry/DesktopTelemetryReceiver.ts";
import * as NativeTelemetryClient from "./resourceTelemetry/NativeTelemetryClient.ts";
import * as ProcessAttribution from "./resourceTelemetry/ProcessAttribution.ts";
import * as ResourceAttribution from "./resourceTelemetry/ResourceAttribution.ts";
import * as ResourceTelemetry from "./resourceTelemetry/ResourceTelemetry.ts";
import * as UsageService from "./usage/UsageService.ts";
import * as TokenAccountingService from "./tokenAccounting/TokenAccountingService.ts";
import * as AnalyticsService from "./telemetry/AnalyticsService.ts";
import * as Data from "effect/Data";

import {
  measureHttpGet,
  openMeasuredWsClient,
} from "../integration/NetworkTransferMeasurement.integration.ts";
import { makeSqlStatementCounter } from "../integration/SqlStatementCounter.integration.ts";
import {
  diagnosticOutput,
  TRANSFER_HISTORY_TURN_COUNT,
  TRANSFER_HISTORY_TOOLS_PER_TURN,
  TRANSFER_HISTORY_MCP_RESULT_BYTES,
  TRANSFER_MEASURED_TOOLS,
  TRANSFER_MEASURED_MCP_RESULT_BYTES,
} from "../integration/fixtures/transferBudget.ts";
import {
  formatTransferBudgetReport,
  formatTransferBudgetResult,
  type TransferBudgetRun,
  transferBudgetViolations,
} from "../integration/TransferBudgetReport.integration.ts";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import { DEFAULT_SIGNAL_EXPORT, otlpSerializationLayer } from "@t3tools/shared/observability";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";

const defaultProjectId = ProjectId.make("project-default");
const defaultThreadId = ThreadId.make("thread-default");
const defaultDesktopBootstrapToken = "test-desktop-bootstrap-token";
const defaultModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
} as const;

const providerSetupInstanceId = ProviderInstanceId.make("antigravity-custom-profile");
const providerSetupDriver = ProviderDriverKind.make("antigravity");
const providerSetupInstallState: ProviderInstallState = {
  driver: providerSetupDriver,
  operationId: "install-operation",
  phase: "downloading",
  downloadedBytes: 128,
  totalBytes: 256,
  version: "test-release",
  installedVersion: null,
  canRemove: false,
  message: null,
};
const providerSetupAuthState: ProviderAuthState = {
  instanceId: providerSetupInstanceId,
  phase: "idle",
  flowId: null,
  authorizationUrl: null,
  expiresAt: null,
  message: null,
};
const providerSetupInstance: ProviderInstance = {
  instanceId: providerSetupInstanceId,
  driverKind: providerSetupDriver,
  enabled: false,
  displayName: "Google account",
  continuationIdentity: {
    driverKind: providerSetupDriver,
    continuationKey: providerSetupInstanceId,
  },
  get orchestrationAdapter(): never {
    throw new Error("Provider setup must not start a chat session.");
  },
  get snapshot(): never {
    throw new Error("Installation routing must not probe the provider.");
  },
  get textGeneration(): never {
    throw new Error("Provider setup must not generate text.");
  },
};

const makeRouterProviderInstance = (
  driver = ProviderDriverKind.make("codex"),
): ProviderInstance => {
  const instanceId = ProviderInstanceId.make(driver);
  return {
    instanceId: instanceId,
    driverKind: driver,
    enabled: true,
    displayName: "Synthetic router provider",
    continuationIdentity: {
      driverKind: driver,
      continuationKey: "router-test-synthetic",
    },
    orchestrationAdapter: {
      instanceId: instanceId,
      driver: driver,
      getCapabilities: () =>
        Effect.succeed(
          driver === "claudeAgent" ? ClaudeProviderCapabilitiesV2 : CodexProviderCapabilitiesV2,
        ),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
      openSession: () => Effect.die("Router fixtures must not launch a live provider."),
    },
    snapshot: {
      getSnapshot: Effect.succeed({
        instanceId,
        driver,
        enabled: true,
        installed: true,
        version: null,
        status: "ready",
        auth: { status: "authenticated" },
        checkedAt: "2026-01-01T00:00:00.000Z",
        supportedRuntimeModes: ["full-access", "approval-required"],
        models: [],
        slashCommands: [],
        skills: [],
      }),
      refresh: Effect.die("Router fixtures must not probe a live provider."),
      streamChanges: Stream.empty,
      applyUsageLimits: () => Effect.void,
      resolveMaintenance: () =>
        Effect.succeed(
          makeManualOnlyProviderMaintenanceCapabilities({ provider: driver, packageName: null }),
        ),
    },
    get textGeneration(): never {
      throw new Error("Router fixtures must not generate provider text.");
    },
  };
};
const routerProviderInstance = makeRouterProviderInstance();

const testEnvironmentDescriptor = {
  environmentId: EnvironmentId.make("environment-test"),
  label: "Test environment",
  platform: {
    os: "darwin" as const,
    arch: "arm64" as const,
  },
  serverVersion: "0.0.0-test",
  orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
  capabilities: {
    repositoryIdentity: true,
  },
};
const nativeRouterEnvironmentDescriptor: Effect.Success<
  ServerEnvironment.ServerEnvironment["Service"]["getDescriptor"]
> = {
  ...testEnvironmentDescriptor,
  capabilities: {
    ...testEnvironmentDescriptor.capabilities,
    nativeBootstrapCreation: {
      submissionSchema: "t3.native-bootstrap-submission/v1",
      preparationSchema: "voice.t3-bootstrap-preparation/v1",
      observationSchema: "t3.native-creation-observation/v2",
      guardRequired: true,
    },
  },
};
const makeGuardedQueueTransportCommand = (suffix: string) => ({
  type: "thread.turn.start",
  commandId: CommandId.make(`cmd-queue-${suffix}`),
  threadId: defaultThreadId,
  message: {
    messageId: MessageId.make(`msg-queue-${suffix}`),
    role: "user",
    text: "Synthetic queue request",
    attachments: [],
  },
  modelSelection: defaultModelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  dispatchGuard: {
    observedSnapshotSequence: 0,
    expectedModelSelection: defaultModelSelection,
    expectedSessionStatus: null,
    expectedActiveTurnId: null,
    expectedLatestTurnId: null,
    requireIdle: true,
  },
  createdAt: "2026-01-01T00:00:00.000Z",
});

type RouterV2Services =
  | Orchestrator.OrchestratorV2
  | ThreadManagement.ThreadManagementService
  | ThreadLaunch.ThreadLaunchService
  | ThreadCommandExecutor.ThreadCommandExecutor
  | ProjectService.ProjectService
  | ProjectStore.ProjectStoreV2
  | ProjectionStore.ProjectionStoreV2
  | EventSink.EventSinkV2
  | EventStore.EventStoreV2
  | OrchestrationEventStore
  | CommandReceiptStore.CommandReceiptStoreV2
  | TurnItemPositionStore.TurnItemPositionStoreV2
  | IdAllocator.IdAllocatorV2
  | EffectOutbox.EffectOutboxV2
  | EffectWorker.OrchestrationEffectWorkerV2
  | ProviderSessionManager.ProviderSessionManagerV2
  | ProviderAuthService
  | AuthSessions.AuthSessionRepository
  | NativeCreationRepository
  | NativeCreationAuthority
  | ServerConfig.ServerConfig
  | SqlClient.SqlClient;

type AwaitedObservation = Effect.Success<
  ReturnType<Orchestrator.OrchestratorV2["Service"]["observeCommand"]>
>;

const routerMessageDispatch = (
  suffix: string,
): Extract<OrchestrationV2Command, { type: "message.dispatch" }> => ({
  type: "message.dispatch",
  commandId: CommandId.make("router:message:" + suffix),
  threadId: defaultThreadId,
  messageId: MessageId.make("router:message:" + suffix),
  text: "Synthetic queue request",
  attachments: [],
  modelSelection: defaultModelSelection,
  dispatchMode: { type: "start_immediately" },
  createdBy: "user",
  creationSource: "web",
});

const routerThreadCreate = (
  threadId = defaultThreadId,
  projectId = defaultProjectId,
): Extract<OrchestrationV2Command, { type: "thread.create" }> => ({
  type: "thread.create",
  commandId: CommandId.make("router:create:" + threadId),
  threadId,
  projectId,
  title: "Default Thread",
  modelSelection: defaultModelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  createdBy: "user",
  creationSource: "web",
});

const seedRouterProject = Effect.fnUntraced(function* (
  context: Context.Context<ProjectService.ProjectService>,
  projectId = defaultProjectId,
  workspaceRoot?: string,
) {
  const projects = Context.get(context, ProjectService.ProjectService);
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root =
    workspaceRoot ?? (yield* fs.makeTempDirectoryScoped({ prefix: "t3-router-project-" }));
  yield* fs.makeDirectory(root, { recursive: true });
  return yield* projects.create({
    commandId: CommandId.make("router:create:" + projectId),
    projectId,
    title: "Default Project",
    workspaceRoot: path.resolve(root),
    defaultModelSelection,
    scripts: [],
  });
});

const seedRouterThread = Effect.fnUntraced(function* (
  context: Context.Context<
    ProjectService.ProjectService | ThreadManagement.ThreadManagementService
  >,
  threadId = defaultThreadId,
  projectId = defaultProjectId,
) {
  yield* seedRouterProject(context, projectId);
  const threads = Context.get(context, ThreadManagement.ThreadManagementService);
  return yield* threads.dispatch(routerThreadCreate(threadId, projectId));
});

const routerLaunchInput = (suffix: string) => ({
  commandId: CommandId.make("router:launch:" + suffix),
  threadId: ThreadId.make("router:launch:" + suffix),
  projectId: defaultProjectId,
  title: "Synthetic launch",
  modelSelection: defaultModelSelection,
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
  workspaceStrategy: { type: "root" as const },
  initialMessage: { text: "Synthetic first send", attachments: [] },
});

const routerTurnItem = (
  ordinal: number,
  changes: Partial<Extract<OrchestrationV2TurnItem, { type: "command_execution" }>> = {},
): Extract<OrchestrationV2TurnItem, { type: "command_execution" }> => ({
  type: "command_execution",
  id: TurnItemId.make("router:tool:" + ordinal),
  threadId: defaultThreadId,
  runId: null,
  nodeId: null,
  providerThreadId: null,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal,
  status: "completed",
  title: "Synthetic tool",
  startedAt: TEST_EPOCH,
  completedAt: TEST_EPOCH,
  updatedAt: TEST_EPOCH,
  input: "synthetic command",
  output: "ok",
  ...changes,
});

const routerItemEvent = (
  item: OrchestrationV2TurnItem,
  suffix: string = item.id,
): OrchestrationV2DomainEvent => ({
  id: EventId.make("router:event:" + suffix),
  threadId: item.threadId,
  type: "turn-item.updated",
  occurredAt: item.updatedAt,
  payload: item,
});

const writeRouterItems = (
  context: Context.Context<EventSink.EventSinkV2>,
  items: ReadonlyArray<OrchestrationV2TurnItem>,
  batch = "initial",
) =>
  Context.get(context, EventSink.EventSinkV2).write({
    events: items.map((item, index) => routerItemEvent(item, batch + ":" + index + ":" + item.id)),
  });

const awaitRouterRunStatus = (
  context: Context.Context<RouterV2Services>,
  threadId: ThreadId,
  status: "failed" | "queued" | "cancelled" | "starting",
) =>
  Context.get(context, ThreadManagement.ThreadManagementService)
    .streamStoredEventsFrom({ threadId, afterSequence: 0 })
    .pipe(
      Stream.filter(
        (stored) => stored.event.type === "run.updated" && stored.event.payload.status === status,
      ),
      Stream.runHead,
    );

// Plays Git for one router worktree launch: the checkout happens only after the
// caller's mutation gate passes, at exactly the requested path and branch, and
// local status reads that branch back only at the created checkout.
const makeRouterWorktreeGit = () => {
  let checkout: { readonly path: string; readonly refName: string } | null = null;
  const createWorktree: GitVcsDriver.GitVcsDriver["Service"]["createWorktree"] = (input, options) =>
    Effect.gen(function* () {
      const { path, newRefName } = input;
      assertTrue(path !== null && newRefName !== undefined);
      yield* options?.revalidateMutation ?? Effect.void;
      checkout = { path, refName: newRefName };
      return { worktree: { path, refName: newRefName } };
    });
  const gitManager: Partial<GitManager.GitManager["Service"]> = {
    invalidateLocalStatus: () => Effect.void,
    localStatus: (input) =>
      Effect.succeed({
        isRepo: true,
        hasPrimaryRemote: false,
        isDefaultRef: false,
        refName: checkout !== null && input.cwd === checkout.path ? checkout.refName : null,
        hasWorkingTreeChanges: false,
        workingTree: { files: [], insertions: 0, deletions: 0 },
      }),
  };
  return { createWorktree, gitManager, checkoutPath: () => checkout?.path ?? null };
};

const seedRouterProvider = Effect.fnUntraced(function* (
  context: Context.Context<RouterV2Services>,
  status: "ready" | "stopped" = "ready",
  threadId = defaultThreadId,
) {
  const projection = yield* Context.get(context, Orchestrator.OrchestratorV2).getThreadProjection(
    threadId,
  );
  const sessionId = ProviderSessionId.make("router:synthetic-session");
  const providerThreadId = ProviderThreadId.make("router:synthetic-provider-thread");
  yield* Context.get(context, EventSink.EventSinkV2).write({
    events: [
      {
        id: EventId.make("router:session-attached"),
        threadId,
        driver: ProviderDriverKind.make("codex"),
        providerInstanceId: ProviderInstanceId.make("codex"),
        type: "provider-session.attached",
        occurredAt: TEST_EPOCH,
        payload: {
          id: sessionId,
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          status,
          cwd: projection.thread.worktreePath ?? "/synthetic/router-project",
          model: defaultModelSelection.model,
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: TEST_EPOCH,
          updatedAt: TEST_EPOCH,
          lastError: null,
        },
      },
      {
        id: EventId.make("router:provider-thread"),
        threadId,
        type: "provider-thread.updated",
        occurredAt: TEST_EPOCH,
        payload: {
          id: providerThreadId,
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerSessionId: sessionId,
          appThreadId: threadId,
          ownerNodeId: null,
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
          status: "idle",
          firstRunOrdinal: null,
          lastRunOrdinal: null,
          handoffIds: [],
          forkedFrom: null,
          createdAt: TEST_EPOCH,
          updatedAt: TEST_EPOCH,
        },
      },
    ],
  });
  return sessionId;
});

const makeRouterFeedbackRuntime = (
  sessionId: ProviderSessionId,
  uploadFeedback: NonNullable<ProviderAdapterV2SessionRuntime["uploadFeedback"]>,
): ProviderAdapterV2SessionRuntime => {
  const driver = ProviderDriverKind.make("codex");
  const instanceId = ProviderInstanceId.make("codex");
  const unused = () => Effect.die("Feedback routing must not invoke another provider operation");
  return {
    instanceId,
    driver,
    providerSessionId: sessionId,
    providerSession: {
      id: sessionId,
      driver,
      providerInstanceId: instanceId,
      status: "ready",
      cwd: "/synthetic/router-project",
      model: defaultModelSelection.model,
      capabilities: CodexProviderCapabilitiesV2,
      createdAt: TEST_EPOCH,
      updatedAt: TEST_EPOCH,
      lastError: null,
    },
    events: Stream.empty,
    uploadFeedback,
    ensureThread: unused,
    resumeThread: unused,
    startTurn: unused,
    steerTurn: unused,
    interruptTurn: unused,
    respondToRuntimeRequest: unused,
    readThreadSnapshot: unused,
    rollbackThread: unused,
    forkThread: unused,
  };
};

const browserOtlpTracingLayer = Layer.mergeAll(
  FetchHttpClient.layer,
  OtlpSerialization.layerJson,
  Layer.succeed(HttpClient.TracerDisabledWhen, () => true),
);

const routerUsagePricing: UsagePricing = {
  status: "unavailable",
  source: "synthetic-router-rates",
  fetchedAt: null,
  knownModels: 0,
};
const routerUsageLayer = Layer.succeed(UsageService.UsageService, {
  readSummary: (input) =>
    Effect.succeed({
      contractVersion: USAGE_CONTRACT_VERSION,
      readAt: "1970-01-01T00:00:00.000Z",
      timeZone: input.timeZone,
      sinceDay: input.sinceDay,
      untilDay: input.untilDay,
      buckets: [],
      sources: [],
      pricing: routerUsagePricing,
      scanDurationMs: 0,
    }),
  refreshRates: Effect.succeed(routerUsagePricing),
});

const makeAuthTestLayer = () =>
  EnvironmentAuth.layer.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(
      Layer.mock(ServerEnvironment.ServerEnvironmentIdentity)({
        getEnvironmentId: Effect.succeed(testEnvironmentDescriptor.environmentId),
      }),
    ),
  );

const makeBrowserOtlpPayload = (spanName: string) =>
  Effect.gen(function* () {
    const collector = yield* Effect.acquireRelease(
      Effect.promise(async () => {
        const NodeHttp = await import("node:http");

        return await new Promise<{
          readonly close: () => Promise<void>;
          readonly firstRequest: Promise<{
            readonly body: string;
            readonly contentType: string | null;
          }>;
          readonly url: string;
        }>((resolve, reject) => {
          let resolveFirstRequest:
            | ((request: { readonly body: string; readonly contentType: string | null }) => void)
            | undefined;
          const firstRequest = new Promise<{
            readonly body: string;
            readonly contentType: string | null;
          }>((resolveRequest) => {
            resolveFirstRequest = resolveRequest;
          });

          const server = NodeHttp.createServer((request, response) => {
            const chunks: Buffer[] = [];
            request.on("data", (chunk) => {
              chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            });
            request.on("end", () => {
              resolveFirstRequest?.({
                body: Buffer.concat(chunks).toString("utf8"),
                contentType: request.headers["content-type"] ?? null,
              });
              resolveFirstRequest = undefined;
              response.statusCode = 204;
              response.end();
            });
          });

          server.on("error", reject);
          server.listen(0, "127.0.0.1", () => {
            const address = server.address();
            if (!address || typeof address === "string") {
              reject(new Error("Expected TCP collector address"));
              return;
            }

            resolve({
              url: `http://127.0.0.1:${address.port}/v1/traces`,
              firstRequest,
              close: () =>
                new Promise<void>((resolveClose, rejectClose) => {
                  server.close((error) => {
                    if (error) {
                      rejectClose(error);
                      return;
                    }
                    resolveClose();
                  });
                }),
            });
          });
        });
      }),
      ({ close }) => Effect.promise(close),
    );

    // The exporter's batch fiber is forked while the layer builds and ticks on
    // a wall-clock interval, so the whole tracer runs on the live clock.
    yield* Layer.build(
      OtlpTracer.layer({
        url: collector.url,
        exportInterval: "10 millis",
        resource: {
          serviceName: "t3code-web",
          attributes: {
            "service.runtime": "t3-web",
            "service.mode": "browser",
            "service.version": "test",
          },
        },
      }).pipe(Layer.provide(browserOtlpTracingLayer)),
    ).pipe(
      Effect.flatMap((tracing) =>
        Effect.void.pipe(Effect.withSpan(spanName), Effect.provideContext(tracing)),
      ),
      TestClock.withLive,
    );

    const request = yield* Effect.raceFirst(
      Effect.promise(() => collector.firstRequest).pipe(Effect.orDie),
      Effect.sleep(Duration.seconds(1)).pipe(
        Effect.andThen(Effect.die(new Error("Timed out waiting for OTLP trace export"))),
      ),
    );
    // @effect-diagnostics-next-line preferSchemaOverJson:off
    return JSON.parse(request.body) as OtlpTracer.TraceData;
  });

const buildAppUnderTest = <SetupError = never>(options?: {
  onPairingChangesSubscribed?: Effect.Effect<void>;
  config?: Partial<ServerConfig.ServerConfig["Service"]>;
  layers?: {
    keybindings?: Partial<Keybindings.Keybindings["Service"]>;
    environmentTheme?: Partial<EnvironmentTheme.EnvironmentThemeService["Service"]>;
    providerRegistry?: Partial<ProviderRegistry.ProviderRegistry["Service"]>;
    modelManifest?: Partial<ModelManifest.ModelManifest["Service"]>;
    usageLimitSources?: Partial<UsageLimitSources.UsageLimitSources["Service"]>;
    tokenAccounting?: TokenAccountingService.TokenAccountingService["Service"];
    providerAuth?: Partial<ProviderAuthService["Service"]>;
    providerInstanceRegistry?: Partial<ProviderInstanceRegistry["Service"]>;
    antigravityInstallation?: Partial<AntigravityInstallation["Service"]>;
    codexInstallation?: Partial<CodexInstallation["Service"]>;
    serverSettings?: Partial<ServerSettings.ServerSettingsService["Service"]>;
    externalLauncher?: Partial<ExternalLauncher.ExternalLauncher["Service"]>;
    vcsDriver?: Partial<VcsDriver.VcsDriver["Service"]>;
    vcsDriverRegistry?: Partial<VcsDriverRegistry.VcsDriverRegistry["Service"]>;
    gitVcsDriver?: Partial<GitVcsDriver.GitVcsDriver["Service"]>;
    gitManager?: Partial<GitManager.GitManager["Service"]>;
    sourceControlRepositoryService?: Partial<
      SourceControlRepositoryService.SourceControlRepositoryService["Service"]
    >;
    reviewService?: Partial<ReviewService.ReviewService["Service"]>;
    vcsStatusBroadcaster?: Partial<VcsStatusBroadcaster.VcsStatusBroadcaster["Service"]>;
    projectSetupScriptRunner?: Partial<
      ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]
    >;
    terminalManager?: Partial<TerminalManager.TerminalManager["Service"]>;
    nativeCreationGrantResolver?: NativeCreationGrantResolver["Service"];
    nativeCreationBindingResolver?: NativeCreationBindingResolver["Service"];
    onWorkspaceNormalization?: () => void;
    onNativeCreationServices?: (
      repository: NativeCreationRepository["Service"],
      sql: SqlClient.SqlClient,
    ) => Effect.Effect<void>;
    onAuthSessionStore?: (
      sessions: SessionStore["Service"],
    ) => Effect.Effect<void, SessionCredentialInternalError>;
    orchestrator?: Partial<Orchestrator.OrchestratorV2["Service"]>;
    threadManagement?: Partial<ThreadManagement.ThreadManagementService["Service"]>;
    threadLaunch?: Partial<ThreadLaunch.ThreadLaunchService["Service"]>;
    projectService?: Partial<ProjectService.ProjectService["Service"]>;
    providerSessionManager?: Partial<ProviderSessionManager.ProviderSessionManagerV2["Service"]>;
    providerGoalService?: Partial<ProviderSessionGoalService.ProviderSessionGoalService["Service"]>;
    wrapThreadManagement?: (
      service: ThreadManagement.ThreadManagementService["Service"],
    ) => ThreadManagement.ThreadManagementService["Service"];
    wrapProjectService?: (
      service: ProjectService.ProjectService["Service"],
    ) => ProjectService.ProjectService["Service"];
    wrapProjectEnrichment?: (
      service: ProjectEnrichmentService.ProjectEnrichmentService["Service"],
    ) => ProjectEnrichmentService.ProjectEnrichmentService["Service"];
    wrapApplicationEvents?: (
      service: OrchestrationEventStore["Service"],
    ) => OrchestrationEventStore["Service"];
    onV2Services?: (
      context: Context.Context<RouterV2Services>,
    ) => Effect.Effect<
      void,
      SetupError,
      FileSystem.FileSystem | Path.Path | import("effect/Scope").Scope
    >;
    analyticsService?: Partial<AnalyticsService.AnalyticsService["Service"]>;
    projectionStore?: Partial<ProjectionStore.ProjectionStoreV2["Service"]>;
    checkpointDiffQuery?: Partial<CheckpointDiffQuery.CheckpointDiffQuery["Service"]>;
    browserTraceCollector?: Partial<BrowserTraceCollector.BrowserTraceCollector["Service"]>;
    serverLifecycleEvents?: Partial<ServerLifecycleEvents.ServerLifecycleEvents["Service"]>;
    serverRuntimeStartup?: Partial<ServerRuntimeStartup.ServerRuntimeStartup["Service"]>;
    serverEnvironment?: Partial<ServerEnvironment.ServerEnvironment["Service"]>;
    repositoryIdentityResolver?: Partial<
      RepositoryIdentityResolver.RepositoryIdentityResolver["Service"]
    >;
    cloudManagedEndpointRuntime?: Partial<
      CloudManagedEndpointRuntime.CloudManagedEndpointRuntime["Service"]
    >;
    relayClient?: Partial<RelayClient.RelayClient["Service"]>;
    agentAwarenessRelay?: Partial<AgentAwarenessRelay.AgentAwarenessRelay["Service"]>;
    cloudCliTokenManager?: Partial<CloudCliTokenManager.CloudCliTokenManager["Service"]>;
    httpClient?: HttpClient.HttpClient;
    nativeTelemetryClient?: Partial<NativeTelemetryClient.NativeTelemetryClient["Service"]>;
    desktopTelemetryReceiver?: Partial<
      DesktopTelemetryReceiver.DesktopTelemetryReceiver["Service"]
    >;
  };
}) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const tempBaseDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-router-test-" });
    const baseDir = options?.config?.baseDir ?? tempBaseDir;
    const devUrl = options?.config?.devUrl;
    const derivedPaths = yield* ServerConfig.deriveServerPaths(baseDir, devUrl);
    const config: ServerConfig.ServerConfig["Service"] = {
      logLevel: "Info",
      traceMinLevel: "Info",
      traceTimingEnabled: true,
      traceBatchWindowMs: 200,
      traceMaxBytes: 10 * 1024 * 1024,
      traceMaxFiles: 10,
      otlpTracesUrl: undefined,
      otlpMetricsUrl: undefined,
      otlpLogsUrl: undefined,
      otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
      otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
      otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
      otelEnvironment: OtelEnvironment.none,
      mode: "desktop",
      port: 0,
      host: "127.0.0.1",
      cwd: process.cwd(),
      baseDir,
      ...derivedPaths,
      staticDir: undefined,
      devUrl,
      devAllowedOrigins: [],
      noBrowser: true,
      startupPresentation: "browser",
      desktopBootstrapToken: defaultDesktopBootstrapToken,
      autoBootstrapProjectFromCwd: false,
      logWebSocketEvents: false,
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
      ...options?.config,
    };
    const layerConfig = ServerConfig.layer(config);
    const defaultVcsDriver: VcsDriver.VcsDriver["Service"] = {
      capabilities: {
        kind: "git",
        supportsWorktrees: true,
        supportsBookmarks: false,
        supportsAtomicSnapshot: false,
        supportsPushDefaultRemote: true,
        ignoreClassifier: "native",
      },
      execute: () =>
        Effect.succeed({
          exitCode: ChildProcessSpawner.ExitCode(0),
          stdout: "",
          stderr: "",
          stdoutTruncated: false,
          stderrTruncated: false,
        }),
      detectRepository: () => Effect.succeed(null),
      isInsideWorkTree: () => Effect.succeed(false),
      listWorkspaceFiles: () =>
        Effect.succeed({
          paths: [],
          truncated: false,
          freshness: {
            source: "live-local",
            observedAt: TEST_EPOCH,
            expiresAt: Option.none(),
          },
        }),
      listRemotes: () =>
        Effect.succeed({
          remotes: [],
          freshness: {
            source: "live-local",
            observedAt: TEST_EPOCH,
            expiresAt: Option.none(),
          },
        }),
      filterIgnoredPaths: (_cwd, relativePaths) => Effect.succeed(relativePaths),
      initRepository: () => Effect.void,
      ...options?.layers?.vcsDriver,
    };
    const vcsDriverRegistryLayer = Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
      get: () => Effect.succeed(defaultVcsDriver),
      detect: (input) =>
        defaultVcsDriver.detectRepository(input.cwd).pipe(
          Effect.filterOrElse(
            (repository) => repository !== null,
            () =>
              defaultVcsDriver.isInsideWorkTree(input.cwd).pipe(
                Effect.map((isInsideWorkTree) =>
                  isInsideWorkTree
                    ? {
                        kind: "git" as const,
                        rootPath: input.cwd,
                        metadataPath: null,
                        freshness: {
                          source: "live-local" as const,
                          observedAt: TEST_EPOCH,
                          expiresAt: Option.none(),
                        },
                      }
                    : null,
                ),
              ),
          ),
          Effect.map((repository) =>
            repository
              ? ({
                  kind: repository.kind,
                  repository,
                  driver: defaultVcsDriver,
                } satisfies VcsDriverRegistry.VcsDriverHandle)
              : null,
          ),
        ),
      resolve: (input) =>
        Effect.succeed({
          kind:
            input.requestedKind === "auto" || !input.requestedKind ? "git" : input.requestedKind,
          repository: {
            kind:
              input.requestedKind === "auto" || !input.requestedKind ? "git" : input.requestedKind,
            rootPath: input.cwd,
            metadataPath: null,
            freshness: {
              source: "live-local",
              observedAt: TEST_EPOCH,
              expiresAt: Option.none(),
            },
          },
          driver: defaultVcsDriver,
        }),
      ...options?.layers?.vcsDriverRegistry,
    });
    const gitVcsDriverLayer = Layer.mock(GitVcsDriver.GitVcsDriver)({
      ...options?.layers?.gitVcsDriver,
    });
    const gitManagerLayer = Layer.mock(GitManager.GitManager)({
      ...options?.layers?.gitManager,
    });
    const serverSettingsLayer = Layer.mock(ServerSettings.ServerSettingsService)({
      start: Effect.void,
      ready: Effect.void,
      getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
      updateSettings: () => Effect.succeed(DEFAULT_SERVER_SETTINGS),
      streamChanges: Stream.empty,
      ...options?.layers?.serverSettings,
    });
    const sourceControlProviderRegistryLayer = SourceControlProviderRegistry.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          AzureDevOpsCli.layer,
          BitbucketApi.layer,
          GitHubCli.layer,
          GitLabCli.layer,
          ForgejoCli.layer,
        ),
      ),
      Layer.provide(gitVcsDriverLayer),
      Layer.provide(vcsDriverRegistryLayer),
      Layer.provide(serverSettingsLayer),
    );
    const workspacePathsLayer =
      options?.layers?.onWorkspaceNormalization === undefined
        ? WorkspacePaths.layer
        : Layer.effect(
            WorkspacePaths.WorkspacePaths,
            WorkspacePaths.make.pipe(
              Effect.map((paths) => {
                const normalizeWorkspaceRoot: WorkspacePaths.WorkspacePaths["Service"]["normalizeWorkspaceRoot"] =
                  (workspaceRoot, normalizationOptions) =>
                    Effect.sync(() => options.layers?.onWorkspaceNormalization?.()).pipe(
                      Effect.andThen(
                        paths.normalizeWorkspaceRoot(workspaceRoot, normalizationOptions),
                      ),
                    );
                return { ...paths, normalizeWorkspaceRoot };
              }),
            ),
          );
    const workspaceEntriesLayer = WorkspaceEntries.layer.pipe(
      Layer.provide(workspacePathsLayer),
      Layer.provideMerge(vcsDriverRegistryLayer),
    );
    const workspaceAndProjectServicesLayer = Layer.mergeAll(
      workspacePathsLayer,
      workspaceEntriesLayer,
      WorkspaceFileSystem.layer.pipe(
        Layer.provide(workspacePathsLayer),
        Layer.provide(workspaceEntriesLayer),
      ),
      ProjectFaviconResolver.layer.pipe(
        Layer.provide(workspacePathsLayer),
        Layer.provide(T3ProjectFileLoader.layer),
      ),
      NativeAppIconResolver.layer,
    );
    const gitWorkflowLayer = GitWorkflowService.layer.pipe(
      Layer.provideMerge(vcsDriverRegistryLayer),
      Layer.provideMerge(gitVcsDriverLayer),
      Layer.provideMerge(gitManagerLayer),
    );
    const vcsProvisioningLayer = VcsProvisioningService.layer.pipe(
      Layer.provide(vcsDriverRegistryLayer),
    );
    const reviewLayer = options?.layers?.reviewService
      ? Layer.mock(ReviewService.ReviewService)({
          ...options.layers.reviewService,
        })
      : ReviewService.layer.pipe(
          Layer.provideMerge(gitVcsDriverLayer),
          Layer.provide(vcsDriverRegistryLayer),
        );
    const vcsStatusBroadcasterLayer = options?.layers?.vcsStatusBroadcaster
      ? Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({
          ...options.layers.vcsStatusBroadcaster,
        })
      : VcsStatusBroadcaster.layer.pipe(Layer.provide(gitWorkflowLayer));
    const resourceTelemetryLayer = ResourceTelemetry.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          NativeTelemetryClient.layerTest(options?.layers?.nativeTelemetryClient),
          DesktopTelemetryReceiver.layerTest(options?.layers?.desktopTelemetryReceiver),
          ProcessAttribution.layer,
          ResourceAttribution.layer,
        ),
      ),
    );
    const serviceLauncherClientLayer = ServiceLauncherClient.layer.pipe(
      Layer.provide(Layer.succeed(HostProcessEnvironment, {})),
    );

    // This is the production V2 service graph over one task-owned in-memory
    // database. Its workers remain parked; a transport test must explicitly
    // drain a synthetic effect and cannot accidentally start a provider CLI.
    const v2RuntimeLayer = OrchestrationV2ProductionLayerLive.pipe(
      Layer.provideMerge(OrchestrationV2EventSinkLayerLive),
      Layer.flatMap((context) => {
        const orchestrator = Context.get(context, Orchestrator.OrchestratorV2);
        const threads = Context.get(context, ThreadManagement.ThreadManagementService);
        const projects = Context.get(context, ProjectService.ProjectService);
        const projections = Context.get(context, ProjectionStore.ProjectionStoreV2);
        const providerSessions = Context.get(
          context,
          ProviderSessionManager.ProviderSessionManagerV2,
        );
        const applicationEvents = Context.get(context, OrchestrationEventStore);
        const providerGoals = Context.get(
          context,
          ProviderSessionGoalService.ProviderSessionGoalService,
        );
        const providerAuth = Context.get(context, ProviderAuthService);
        let decorated = Context.add(context, Orchestrator.OrchestratorV2, {
          ...orchestrator,
          ...options?.layers?.orchestrator,
        });
        decorated = Context.add(decorated, ThreadManagement.ThreadManagementService, {
          ...(options?.layers?.wrapThreadManagement?.(threads) ?? threads),
          ...options?.layers?.threadManagement,
        });
        decorated = Context.add(decorated, ProjectService.ProjectService, {
          ...(options?.layers?.wrapProjectService?.(projects) ?? projects),
          ...options?.layers?.projectService,
        });
        decorated = Context.add(decorated, ProjectionStore.ProjectionStoreV2, {
          ...projections,
          ...options?.layers?.projectionStore,
        });
        decorated = Context.add(decorated, ProviderSessionManager.ProviderSessionManagerV2, {
          ...providerSessions,
          ...options?.layers?.providerSessionManager,
        });
        decorated = Context.add(
          decorated,
          OrchestrationEventStore,
          options?.layers?.wrapApplicationEvents?.(applicationEvents) ?? applicationEvents,
        );
        decorated = Context.add(decorated, ProviderSessionGoalService.ProviderSessionGoalService, {
          ...providerGoals,
          ...options?.layers?.providerGoalService,
        });
        // The V2 graph exports ProviderAuthService, which shadows the outer mock for routes.
        decorated = Context.add(decorated, ProviderAuthService, {
          ...providerAuth,
          ...options?.layers?.providerAuth,
        });
        return Layer.succeedContext(decorated);
      }),
      Layer.provideMerge(
        ResourceCleanup.live.pipe(
          Layer.provide(OrchestrationV2EventSinkLayerLive),
          Layer.provide(ThreadCommandExecutor.layer),
        ),
      ),
      Layer.provide(Layer.succeed(ServerActivation.ServerActivation, Effect.never)),
      Layer.provideMerge(
        ProjectEnrichmentService.layer.pipe(
          // HTTP handlers consume exported services after route construction.
          Layer.flatMap((context) => {
            const service = Context.get(context, ProjectEnrichmentService.ProjectEnrichmentService);
            return Layer.succeed(
              ProjectEnrichmentService.ProjectEnrichmentService,
              options?.layers?.wrapProjectEnrichment?.(service) ?? service,
            );
          }),
        ),
      ),
      Layer.provide(CheckpointStore.layer.pipe(Layer.provide(vcsDriverRegistryLayer))),
      Layer.provideMerge(ThreadSearch.layer),
      Layer.provideMerge(ProjectStore.layer),
    );

    const routerRuntimeLayer = Layer.fresh(ThreadLaunch.layer).pipe(
      Layer.provideMerge(v2RuntimeLayer),
      Layer.provideMerge(IdAllocator.layer),
      Layer.provide(
        CommandReceiptStore.layerFromApplicationReceipts.pipe(Layer.provide(v2RuntimeLayer)),
      ),
      Layer.flatMap((context) =>
        Layer.succeedContext(
          Context.add(context, ThreadLaunch.ThreadLaunchService, {
            ...Context.get(context, ThreadLaunch.ThreadLaunchService),
            ...options?.layers?.threadLaunch,
          }),
        ),
      ),
    );

    const servedRoutesLayer = HttpRouter.serve(makeRoutesLayer, {
      disableListenLog: true,
      disableLogger: true,
      routerConfig: HTTP_ROUTER_CONFIG,
    })
      .pipe(
        // Provide after serve so route construction and deferred HTTP handlers
        // share the same runtime services and in-memory authentication database.
        Layer.provideMerge(routerRuntimeLayer),
        Layer.provideMerge(McpSessionRegistry.layer),
        Layer.provide(Layer.mergeAll(serviceLauncherClientLayer, SqlitePersistenceMemory)),
      )
      .pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(Keybindings.Keybindings)({
              loadConfigState: Effect.succeed({
                keybindings: [],
                issues: [],
              }),
              streamChanges: Stream.empty,
              ...options?.layers?.keybindings,
            }),
            Layer.mock(EnvironmentTheme.EnvironmentThemeService)({
              current: Effect.succeed([]),
              streamChanges: Stream.empty,
              ...options?.layers?.environmentTheme,
            }),
            Layer.mock(UsageLimitSources.UsageLimitSources)({
              current: Effect.succeed([]),
              streamChanges: Stream.make([]),
              refresh: Effect.void,
              ...options?.layers?.usageLimitSources,
            }),
          ),
        ),
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(ModelManifest.ModelManifest)({
              forceRefresh: Effect.succeed(ModelManifest.BUNDLED_MODEL_MANIFEST),
              ...options?.layers?.modelManifest,
            }),
            Layer.mock(ProviderRegistry.ProviderRegistry)({
              getProviders: Effect.succeed([]),
              refresh: () => Effect.succeed([]),
              refreshInstance: () => Effect.succeed([]),
              getProviderMaintenanceCapabilitiesForInstance: (_instanceId, provider) =>
                Effect.succeed(
                  makeManualOnlyProviderMaintenanceCapabilities({ provider, packageName: null }),
                ),
              setProviderMaintenanceActionState: () => Effect.succeed([]),
              streamChanges: Stream.empty,
              ...options?.layers?.providerRegistry,
            }),
            Layer.mock(ProviderAuthService)({
              ...options?.layers?.providerAuth,
            }),
            Layer.mock(ProviderInstanceRegistry)({
              getInstance: (id) =>
                Effect.succeed(
                  id === defaultModelSelection.instanceId ? routerProviderInstance : undefined,
                ),
              listInstances: Effect.succeed([routerProviderInstance]),
              ...options?.layers?.providerInstanceRegistry,
            }),
            Layer.mock(CodexInstallation)({
              managedDirectory: "unused-test-codex-runtime",
              ...options?.layers?.codexInstallation,
            }),
            Layer.mock(AntigravityInstallation)({
              managedDirectory: "unused-test-antigravity-runtime",
              ...options?.layers?.antigravityInstallation,
            }),
            Layer.mock(AcpRegistrySupport.AcpRegistryCatalog)({}),
            Layer.mock(AcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator)({}),
            Layer.mock(ProviderMaintenanceRunner.ProviderMaintenanceRunner)({}),
            Layer.mock(DeviceService.DeviceService)({
              state: Effect.succeed(EMPTY_DEVICE_STATE),
              currentReadiness: () => Effect.succeed(null),
              sessionsForThread: () => Effect.succeed([]),
            }),
          ),
        ),
        Layer.provide(serverSettingsLayer),
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(ExternalLauncher.ExternalLauncher)({
              resolveAvailableEditors: () => Effect.succeed([]),
              resolveFileManagerRevealKind: () => Effect.undefined,
              ...options?.layers?.externalLauncher,
            }),
            Layer.mock(RemoteOpenTargets.RemoteOpenTargets)({
              resolveTargets: () => Effect.succeed([]),
            }),
          ),
        ),
        Layer.provide(
          Layer.mock(ProcessDiagnostics.ProcessDiagnostics)({
            read: Effect.succeed({
              serverPid: process.pid,
              readAt: TEST_EPOCH,
              processCount: 0,
              totalRssBytes: 0,
              totalCpuPercent: 0,
              processes: [],
              error: Option.none(),
            }),
            signal: (input) =>
              Effect.succeed({
                pid: input.pid,
                signal: input.signal,
                signaled: true,
                message: Option.none(),
              }),
          }),
        ),
        Layer.provide([
          HostResources.layer,
          Layer.mock(ProcessResourceMonitor.ProcessResourceMonitor)({
            readHistory: (input) =>
              Effect.succeed({
                readAt: TEST_EPOCH,
                windowMs: input.windowMs,
                bucketMs: input.bucketMs,
                sampleIntervalMs: 5_000,
                retainedSampleCount: 0,
                totalCpuSecondsApprox: 0,
                buckets: [],
                topProcesses: [],
                error: Option.none(),
              }),
          }),
        ]),
        Layer.provide(
          Layer.mock(TraceDiagnostics.TraceDiagnostics)({
            read: () =>
              Effect.succeed({
                traceFilePath: "",
                scannedFilePaths: [],
                readAt: TEST_EPOCH,
                recordCount: 0,
                parseErrorCount: 0,
                firstSpanAt: Option.none(),
                lastSpanAt: Option.none(),
                failureCount: 0,
                interruptionCount: 0,
                slowSpanThresholdMs: 1_000,
                slowSpanCount: 0,
                logLevelCounts: {},
                topSpansByCount: [],
                slowestSpans: [],
                commonFailures: [],
                latestFailures: [],
                latestWarningAndErrorLogs: [],
                partialFailure: Option.none(),
                error: Option.none(),
              }),
          }),
        ),
        Layer.provide(gitManagerLayer),
        Layer.provide(gitVcsDriverLayer),
        Layer.provide(gitWorkflowLayer),
        Layer.provide(reviewLayer),
        Layer.provide(vcsProvisioningLayer),
        Layer.provide(
          Layer.mock(SourceControlRepositoryService.SourceControlRepositoryService)({
            ...options?.layers?.sourceControlRepositoryService,
          }),
        ),
        Layer.provideMerge(vcsStatusBroadcasterLayer),
        Layer.provide(
          Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({
            runForThread: () => Effect.succeed({ status: "no-script" as const }),
            ...options?.layers?.projectSetupScriptRunner,
          }),
        ),
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(TerminalManager.TerminalManager)({
              ...options?.layers?.terminalManager,
            }),
            WorktreeSetupTracker.layer,
            ProjectCloneTracker.layer.pipe(
              Layer.provide(
                Layer.mock(SourceControlRepositoryService.SourceControlRepositoryService)({
                  ...options?.layers?.sourceControlRepositoryService,
                }),
              ),
            ),
          ),
        ),
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(PreviewManager.PreviewManager)({
              open: () => Effect.die("PreviewManager not stubbed in this test"),
              navigate: () => Effect.die("PreviewManager not stubbed in this test"),
              resize: () => Effect.die("PreviewManager not stubbed in this test"),
              reportStatus: () => Effect.void,
              refresh: () => Effect.void,
              close: () => Effect.void,
              list: () => Effect.succeed({ sessions: [], serverEpoch: "test-server", revision: 0 }),
              events: Stream.empty,
              subscribeEvents: Effect.flatMap(PubSub.unbounded<PreviewEvent>(), (pubsub) =>
                PubSub.subscribe(pubsub),
              ),
            }),
            Layer.mock(PortScanner.PortDiscovery)({
              scan: () => Effect.succeed([]),
              subscribe: () => Effect.void,
              retain: Effect.void,
              registerTerminalProcesses: () => Effect.void,
              unregisterTerminal: () => Effect.void,
            }),
          ),
        ),
        Layer.provide(
          Layer.mock(PullRequestSyncReactor.PullRequestSyncReactor)({
            start: () => Effect.void,
            drain: Effect.void,
            requestSync: () => Effect.void,
          }),
        ),
        Layer.provide(
          Layer.mock(TextGeneration.TextGeneration)({
            generateBranchName: () => Effect.succeed({ branch: "router-test-branch" }),
            generateThreadTitle: () => Effect.succeed({ title: "Router test thread" }),
          }),
        ),
        Layer.provide(
          Layer.mock(CheckpointDiffQuery.CheckpointDiffQuery)({
            getTurnDiff: () =>
              Effect.succeed({
                threadId: defaultThreadId,
                fromTurnCount: 0,
                toTurnCount: 0,
                diff: "",
              }),
            getFullThreadDiff: () =>
              Effect.succeed({
                threadId: defaultThreadId,
                fromTurnCount: 0,
                toTurnCount: 0,
                diff: "",
              }),
            ...options?.layers?.checkpointDiffQuery,
          }),
        ),
      );

    const appLayer = servedRoutesLayer
      .pipe(
        Layer.provide(resourceTelemetryLayer),
        Layer.provide(routerUsageLayer),
        Layer.provide(
          options?.layers?.tokenAccounting
            ? Layer.succeed(
                TokenAccountingService.TokenAccountingService,
                options.layers.tokenAccounting,
              )
            : TokenAccountingService.layer,
        ),
        Layer.provide(
          Layer.mock(AnalyticsService.AnalyticsService)({
            record: () => Effect.void,
            flush: Effect.void,
            ...options?.layers?.analyticsService,
          }),
        ),
        Layer.provide(
          Layer.mock(BrowserTraceCollector.BrowserTraceCollector)({
            record: () => Effect.void,
            ...options?.layers?.browserTraceCollector,
          }),
        ),
        Layer.provide(otlpSerializationLayer(config.otlpTracesExport.protocol)),
        Layer.provide(
          Layer.mock(ServerLifecycleEvents.ServerLifecycleEvents)({
            publish: (event) => Effect.succeed({ ...(event as any), sequence: 1 }),
            snapshot: Effect.succeed({ sequence: 0, events: [] }),
            stream: Stream.empty,
            ...options?.layers?.serverLifecycleEvents,
          }),
        ),
        Layer.provide(
          Layer.mock(ServerRuntimeStartup.ServerRuntimeStartup)({
            awaitCommandReady: Effect.void,
            markHttpListening: Effect.void,
            markRunningProviderSessionsForContinuation: Effect.succeed([]),
            markOptedInProviderSessionsForContinuation: Effect.succeed([]),
            clearProviderSessionContinuationMarkers: () => Effect.void,
            enqueueCommand: (effect) => effect,
            ...options?.layers?.serverRuntimeStartup,
          }),
        ),
        Layer.provide(
          Layer.mock(BackgroundPolicy.BackgroundPolicy)({
            reportClientActivity: () => Effect.void,
            removeRpcClient: () => Effect.void,
            reportHostPowerState: () => Effect.void,
            snapshot: Effect.succeed({
              hostPower: {
                source: "unknown",
                idle: "unknown",
                idleSeconds: null,
                locked: "unknown",
                suspended: false,
                onBattery: "unknown",
                lowPowerMode: "unknown",
                thermalState: "unknown",
                stale: true,
                updatedAt: TEST_EPOCH,
              },
              leases: [],
              activeForegroundLeaseCount: 0,
              activeScopeKeys: [],
              shouldRunOpportunisticWork: false,
              updatedAt: TEST_EPOCH,
            }),
            streamChanges: Stream.empty,
            subscribe: Effect.succeed({
              latest: {
                hostPower: {
                  source: "unknown",
                  idle: "unknown",
                  idleSeconds: null,
                  locked: "unknown",
                  suspended: false,
                  onBattery: "unknown",
                  lowPowerMode: "unknown",
                  thermalState: "unknown",
                  stale: true,
                  updatedAt: TEST_EPOCH,
                },
                leases: [],
                activeForegroundLeaseCount: 0,
                activeScopeKeys: [],
                shouldRunOpportunisticWork: false,
                updatedAt: TEST_EPOCH,
              },
              changes: Stream.empty,
            }),
            hasDemand: () => Effect.succeed(false),
            shouldRunScopeWork: () => Effect.succeed(false),
            shouldRunOpportunisticWork: Effect.succeed(false),
          }),
        ),
        Layer.provide(
          Layer.mock(ServerEnvironment.ServerEnvironment)({
            getEnvironmentId: Effect.succeed(testEnvironmentDescriptor.environmentId),
            getDescriptor: Effect.succeed(testEnvironmentDescriptor),
            ...options?.layers?.serverEnvironment,
          }),
        ),
        Layer.provide(
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
            resolve: () => Effect.succeed(null),
            ...options?.layers?.repositoryIdentityResolver,
          }),
        ),
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(
              CloudManagedEndpointRuntime.CloudManagedEndpointRuntime,
              CloudManagedEndpointRuntime.CloudManagedEndpointRuntime.of({
                applyConfig: () => Effect.succeed({ status: "disabled" }),
                recoveryRequests: Stream.empty,
                requestRecovery: () => Effect.void,
                withLinkStateLock: (effect) => effect,
                ...options?.layers?.cloudManagedEndpointRuntime,
              }),
            ),
            Layer.mock(AgentAwarenessRelay.AgentAwarenessRelay)({
              requestCatchUp: () => Effect.void,
              ...options?.layers?.agentAwarenessRelay,
            }),
          ),
        ),
        Layer.provide(
          Layer.succeed(
            RelayClient.RelayClient,
            RelayClient.RelayClient.of({
              resolve: Effect.succeed({
                status: "missing",
                version: RelayClient.CLOUDFLARED_VERSION,
              }),
              install: Effect.die("unused relay-client install"),
              installWithProgress: () => Effect.die("unused relay-client install"),
              ...options?.layers?.relayClient,
            }),
          ),
        ),
        Layer.provide(
          Layer.mock(CloudCliTokenManager.CloudCliTokenManager)({
            get: Effect.die(new Error("Unexpected T3 Connect CLI authorization request.")),
            getExisting: Effect.succeedNone,
            hasCredential: Effect.succeed(false),
            clear: Effect.void,
            ...options?.layers?.cloudCliTokenManager,
          }),
        ),
        Layer.updateService(PairingGrantStore.PairingGrantStore, (grants) => {
          const subscribed = options?.onPairingChangesSubscribed;
          if (!subscribed) return grants;
          return {
            ...grants,
            streamChanges: Stream.unwrap(
              Effect.gen(function* () {
                const changes =
                  yield* Queue.unbounded<PairingGrantStore.BootstrapCredentialChange>();
                yield* grants.streamChanges.pipe(
                  Stream.runForEach((change) => Queue.offer(changes, change)),
                  Effect.forkScoped({ startImmediately: true }),
                );
                yield* subscribed;
                return Stream.fromQueue(changes);
              }),
            ),
          };
        }),
      )
      .pipe(
        Layer.provideMerge(
          NativeCreationAuthorityLive.pipe(
            Layer.provide(
              options?.layers?.nativeCreationGrantResolver === undefined
                ? NativeCreationGrantResolverUnavailable
                : Layer.succeed(
                    NativeCreationGrantResolver,
                    options.layers.nativeCreationGrantResolver,
                  ),
            ),
            Layer.provide(
              options?.layers?.nativeCreationBindingResolver === undefined
                ? NativeCreationBindingResolverUnavailable
                : Layer.succeed(
                    NativeCreationBindingResolver,
                    options.layers.nativeCreationBindingResolver,
                  ),
            ),
            Layer.provideMerge(AuthSessions.layer),
            Layer.provideMerge(
              Layer.effect(
                NativeCreationRepository,
                Effect.gen(function* () {
                  const repository = yield* NativeCreationRepositoryLayer.make;
                  const sql = yield* SqlClient.SqlClient;
                  if (options?.layers?.onNativeCreationServices !== undefined)
                    yield* options.layers.onNativeCreationServices(repository, sql);
                  return repository;
                }),
              ),
            ),
          ),
        ),
        Layer.provideMerge(makeAuthTestLayer()),
        Layer.provideMerge(ServerSecretStore.layer),
        Layer.provide(workspaceAndProjectServicesLayer),
        Layer.provideMerge(sourceControlProviderRegistryLayer),
        Layer.provideMerge(
          options?.layers?.httpClient === undefined
            ? FetchHttpClient.layer
            : Layer.succeed(HttpClient.HttpClient, options.layers.httpClient),
        ),
        Layer.provide(GitHubCli.layer.pipe(Layer.provideMerge(VcsProcess.layer))),
        Layer.provideMerge(layerConfig),
      );

    const appContext = yield* Layer.build(appLayer);
    if (options?.layers?.onV2Services !== undefined) {
      yield* options.layers.onV2Services(appContext);
    }
    if (options?.layers?.onAuthSessionStore !== undefined) {
      yield* options.layers.onAuthSessionStore(Context.get(appContext, SessionStore));
    }
    return config;
  });

const parseSessionCookieFromWsUrl = (
  wsUrl: string,
): { readonly cookie: string | null; readonly url: string } => {
  const next = new URL(wsUrl);
  const cookie = next.hash.startsWith("#cookie=")
    ? decodeURIComponent(next.hash.slice("#cookie=".length))
    : null;
  next.hash = "";
  return {
    cookie,
    url: next.toString(),
  };
};

const wsRpcProtocolLayer = (wsUrl: string, onMessage?: (message: string) => void) => {
  const { cookie, url } = parseSessionCookieFromWsUrl(wsUrl);
  const webSocketConstructorLayer = Layer.succeed(
    Socket.WebSocketConstructor,
    (socketUrl, protocols) => {
      // Socket.makeWebSocket only ever passes its `protocols` option here.
      const socket = new NodeSocket.NodeWS.WebSocket(
        socketUrl,
        protocols as string | string[] | undefined,
        cookie ? { headers: { cookie } } : undefined,
      );
      if (onMessage) socket.on("message", (data) => onMessage(data.toString()));
      return socket as unknown as globalThis.WebSocket;
    },
  );

  return RpcClient.layerProtocolSocket().pipe(
    Layer.provide(Socket.layerWebSocket(url).pipe(Layer.provide(webSocketConstructorLayer))),
    Layer.provide(RpcSerialization.layerJson),
  );
};

const makeWsRpcClient = RpcClient.make(WsRpcGroup);
type WsRpcClient =
  typeof makeWsRpcClient extends Effect.Effect<infer Client, any, any> ? Client : never;

const withWsRpcClient = <A, E, R>(
  wsUrl: string,
  f: (client: WsRpcClient) => Effect.Effect<A, E, R>,
  onMessage?: (message: string) => void,
) => makeWsRpcClient.pipe(Effect.flatMap(f), Effect.provide(wsRpcProtocolLayer(wsUrl, onMessage)));

// Holds the first ACK after `passedAcks` earlier ACKs have been sent unchanged.
const withFirstWsAckHeld = (
  wsUrl: string,
  held: Deferred.Deferred<void>,
  release: Deferred.Deferred<void>,
  passedAcks = 0,
) => {
  let holdNextAck = true;
  let remainingPassedAcks = passedAcks;
  return Layer.effect(RpcClient.Protocol)(
    Effect.map(RpcClient.Protocol, (protocol) =>
      RpcClient.Protocol.of({
        ...protocol,
        send: (clientId, request, transferables) => {
          const send = protocol.send(clientId, request, transferables);
          if (request._tag !== "Ack" || !holdNextAck) {
            return send;
          }
          if (remainingPassedAcks > 0) {
            remainingPassedAcks -= 1;
            return send;
          }
          holdNextAck = false;
          return Deferred.succeed(held, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(send),
          );
        },
      }),
    ),
  ).pipe(Layer.provide(wsRpcProtocolLayer(wsUrl)));
};

const appendSessionCookieToWsUrl = (url: string, sessionCookieHeader: string) => {
  const isAbsoluteUrl = /^[a-zA-Z][a-zA-Z\d+.-]*:/.test(url);
  const next = new URL(url, "http://localhost");
  next.hash = `cookie=${encodeURIComponent(sessionCookieHeader)}`;
  return isAbsoluteUrl ? next.toString() : `${next.pathname}${next.search}${next.hash}`;
};

const getHttpServerUrl = (pathname = "") =>
  Effect.gen(function* () {
    const server = yield* HttpServer.HttpServer;
    const address = server.address as NetAddress.InetAddress;
    return `http://127.0.0.1:${address.port}${pathname}`;
  });

const bootstrapBrowserSession = (
  credential = defaultDesktopBootstrapToken,
  options?: {
    readonly headers?: Record<string, string>;
  },
) =>
  Effect.gen(function* () {
    const bootstrapUrl = yield* getHttpServerUrl("/api/auth/browser-session");
    const response = yield* fetchEffect(bootstrapUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...options?.headers,
      },
      body: jsonRequestBody({
        credential,
      }),
    });
    const body = yield* responseJsonEffect<{
      readonly authenticated: boolean;
      readonly sessionMethod: string;
      readonly expiresAt: string;
    }>(response);
    return {
      response,
      body,
      cookie: response.headers["set-cookie"],
    };
  });

const exchangeAccessToken = (
  credential = defaultDesktopBootstrapToken,
  options?: {
    readonly headers?: Record<string, string>;
    readonly scope?: string;
    readonly clientMetadata?: {
      readonly label?: string;
      readonly deviceType?: string;
      readonly os?: string;
    };
  },
) =>
  Effect.gen(function* () {
    const tokenUrl = yield* getHttpServerUrl("/oauth/token");
    const response = yield* fetchEffect(tokenUrl, {
      method: "POST",
      headers: {
        "content-type": "application/x-www-form-urlencoded",
        ...options?.headers,
      },
      body: new URLSearchParams({
        grant_type: AuthTokenExchangeGrantType,
        subject_token: credential,
        subject_token_type: AuthEnvironmentBootstrapTokenType,
        requested_token_type: AuthAccessTokenType,
        scope:
          options?.scope ??
          "orchestration:read orchestration:operate terminal:operate review:write relay:read access:read access:write relay:write",
        ...(options?.clientMetadata?.label ? { client_label: options.clientMetadata.label } : {}),
        ...(options?.clientMetadata?.deviceType
          ? { client_device_type: options.clientMetadata.deviceType }
          : {}),
        ...(options?.clientMetadata?.os ? { client_os: options.clientMetadata.os } : {}),
      }).toString(),
    });
    const body = yield* responseJsonEffect<{
      readonly access_token?: string;
      readonly issued_token_type?: string;
      readonly token_type?: string;
      readonly expires_in?: number;
      readonly scope?: string;
      readonly _tag?: string;
      readonly code?: string;
      readonly reason?: string;
      readonly dpopFailureReason?: DpopFailureReason;
      readonly traceId?: string;
    }>(response);
    return {
      response,
      body,
    };
  });

const makeDpopProof = (input: {
  readonly method: string;
  readonly url: string;
  readonly iat: number;
  readonly accessToken?: string;
  readonly jti?: string;
  readonly privateKey?: NodeCrypto.KeyObject;
  readonly publicJwk?: DpopPublicJwk;
}) => {
  const keyPair =
    input.privateKey && input.publicJwk
      ? { privateKey: input.privateKey, publicJwk: input.publicJwk }
      : (() => {
          const { privateKey, publicKey } = NodeCrypto.generateKeyPairSync("ec", {
            namedCurve: "P-256",
          });
          return { privateKey, publicJwk: publicKey.export({ format: "jwk" }) as DpopPublicJwk };
        })();
  const header = Buffer.from(
    JSON.stringify({
      typ: "dpop+jwt",
      alg: "ES256",
      jwk: keyPair.publicJwk,
    }),
  ).toString("base64url");
  const payload = Buffer.from(
    JSON.stringify({
      htm: input.method,
      htu: input.url,
      jti: input.jti ?? "proof-1",
      iat: input.iat,
      ...(input.accessToken ? { ath: computeDpopAccessTokenHash(input.accessToken) } : {}),
    }),
  ).toString("base64url");
  const signature = NodeCrypto.sign("sha256", Buffer.from(`${header}.${payload}`), {
    key: keyPair.privateKey,
    dsaEncoding: "ieee-p1363",
  }).toString("base64url");
  return {
    proof: `${header}.${payload}.${signature}`,
    thumbprint: computeDpopJwkThumbprint(keyPair.publicJwk),
    privateKey: keyPair.privateKey,
    publicJwk: keyPair.publicJwk,
  };
};

const makeCloudMintCredentialRequest = (input: {
  readonly privateKey: string;
  readonly environmentId: EnvironmentId;
  readonly clientProofKeyThumbprint: string;
  readonly issuer?: string;
  readonly audience?: string;
  readonly subject?: string;
  readonly jti?: string;
  readonly nonce: string;
  readonly issuedAt: string;
  readonly expiresAt: string;
  readonly scope?: ReadonlyArray<"environment:connect">;
}) => {
  const payload = {
    iss: input.issuer ?? "https://relay.example.test",
    aud: input.audience ?? `t3-env:${input.environmentId}`,
    sub: input.subject ?? "user_123",
    jti: input.jti ?? "cloud-mint-jti-1",
    environmentId: input.environmentId,
    clientProofKeyThumbprint: input.clientProofKeyThumbprint,
    cnf: {
      jkt: input.clientProofKeyThumbprint,
    },
    nonce: input.nonce,
    iat: Math.floor(DateTime.makeUnsafe(input.issuedAt).epochMilliseconds / 1_000),
    exp: Math.floor(DateTime.makeUnsafe(input.expiresAt).epochMilliseconds / 1_000),
    scope: input.scope ?? ["environment:connect"],
  } as const;
  const header = Buffer.from(
    JSON.stringify({ alg: "EdDSA", typ: RELAY_MINT_REQUEST_TYP }),
  ).toString("base64url");
  const encodedPayload = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signingInput = `${header}.${encodedPayload}`;
  return {
    proof: `${signingInput}.${NodeCrypto.sign(null, Buffer.from(signingInput), input.privateKey).toString("base64url")}`,
  };
};

const makeCloudEnvironmentHealthRequest = (input: {
  readonly privateKey: string;
  readonly environmentId: EnvironmentId;
  readonly issuer?: string;
  readonly audience?: string;
  readonly subject?: string;
  readonly jti?: string;
  readonly nonce: string;
  readonly issuedAt: string;
  readonly expiresAt: string;
  readonly scope?: ReadonlyArray<"environment:status">;
}) => {
  const payload = {
    iss: input.issuer ?? "https://relay.example.test",
    aud: input.audience ?? `t3-env:${input.environmentId}`,
    sub: input.subject ?? "user_123",
    jti: input.jti ?? "cloud-health-jti-1",
    environmentId: input.environmentId,
    nonce: input.nonce,
    iat: Math.floor(DateTime.makeUnsafe(input.issuedAt).epochMilliseconds / 1_000),
    exp: Math.floor(DateTime.makeUnsafe(input.expiresAt).epochMilliseconds / 1_000),
    scope: input.scope ?? ["environment:status"],
  } as const;
  const header = Buffer.from(
    JSON.stringify({ alg: "EdDSA", typ: RELAY_HEALTH_REQUEST_TYP }),
  ).toString("base64url");
  const encodedPayload = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signingInput = `${header}.${encodedPayload}`;
  return {
    proof: `${signingInput}.${NodeCrypto.sign(null, Buffer.from(signingInput), input.privateKey).toString("base64url")}`,
  };
};

const decodeCompactJwtPayload = <A>(token: string): A => {
  const encodedPayload = token.split(".")[1];
  if (!encodedPayload) {
    throw new Error("JWT does not contain a payload.");
  }
  return JSON.parse(Buffer.from(encodedPayload, "base64url").toString("utf8")) as A;
};

class AuthenticationGetterError extends Data.TaggedError("AuthenticationGetterError")<{
  readonly message: string;
}> {}

class TestHttpRequestError extends Data.TaggedError("TestHttpRequestError")<{
  readonly cause: unknown;
}> {}

const testRequestUrl = (input: Parameters<typeof fetch>[0]): string => {
  const value = input.toString();
  if (!/^https?:\/\//i.test(value)) {
    return value;
  }
  const url = new URL(value);
  return `${url.pathname}${url.search}`;
};

const fetchEffect = (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
  const request = HttpClientRequest.make((init?.method ?? "GET") as "GET" | "POST")(
    testRequestUrl(input),
    {
      headers: init?.headers as Record<string, string> | undefined,
    },
  ).pipe(
    typeof init?.body === "string"
      ? HttpClientRequest.bodyText(
          init.body,
          (init.headers as Record<string, string> | undefined)?.["content-type"] ??
            "application/json",
        )
      : (request) => request,
  );
  const effect = HttpClient.execute(request);
  return (
    init?.redirect === "manual"
      ? effect.pipe(Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }))
      : effect
  ).pipe(Effect.mapError((cause) => new TestHttpRequestError({ cause })));
};

const jsonRequestBody = (value: unknown): string => {
  return JSON.stringify(value);
};

const responseJsonEffect = <A>(response: HttpClientResponse.HttpClientResponse) =>
  response.json.pipe(
    Effect.map((json) => json as A),
    Effect.mapError((cause) => new TestHttpRequestError({ cause })),
  );

const responseOk = (response: HttpClientResponse.HttpClientResponse) =>
  response.status >= 200 && response.status < 300;

const getAuthenticatedSessionCookieHeader = (credential = defaultDesktopBootstrapToken) =>
  Effect.gen(function* () {
    const { response, cookie } = yield* bootstrapBrowserSession(credential);
    if (!responseOk(response)) {
      return yield* new AuthenticationGetterError({
        message: `Expected bootstrap session response to succeed, got ${response.status}`,
      });
    }

    if (!cookie) {
      return yield* new AuthenticationGetterError({
        message: "Expected bootstrap session response to set a cookie.",
      });
    }

    return cookie.split(";")[0] ?? cookie;
  });

const getAuthenticatedBearerSessionToken = (credential = defaultDesktopBootstrapToken) =>
  Effect.gen(function* () {
    const { response, body } = yield* exchangeAccessToken(credential);
    if (!responseOk(response)) {
      return yield* new AuthenticationGetterError({
        message: `Expected bearer bootstrap response to succeed, got ${response.status}`,
      });
    }

    if (!body.access_token) {
      return yield* new AuthenticationGetterError({
        message: "Expected token exchange response to include an access token.",
      });
    }

    return body.access_token;
  });

const extractSessionTokenFromSetCookie = (cookieHeader: string): string => {
  const [nameValue] = cookieHeader.split(";", 1);
  const token = nameValue?.split("=", 2)[1];
  if (!token) {
    throw new Error("Expected session cookie header to contain a token value.");
  }
  return token;
};

const splitHeaderTokens = (value: string | null | undefined) =>
  (value ?? "")
    .split(",")
    .map((token) => token.trim())
    .filter((token) => token.length > 0)
    .toSorted();

const assertBrowserApiCorsResponseHeaders = (
  headers: Readonly<Record<string, string | undefined>>,
  options?: {
    readonly origin?: string;
    readonly credentials?: boolean;
  },
) => {
  assert.equal(headers["access-control-allow-origin"], options?.origin ?? "*");
  assert.equal(
    headers["access-control-allow-credentials"],
    options?.credentials ? "true" : undefined,
  );
};

const assertBrowserApiCorsPreflightHeaders = (
  headers: Readonly<Record<string, string | undefined>>,
  options?: {
    readonly origin?: string;
    readonly credentials?: boolean;
  },
) => {
  assertBrowserApiCorsResponseHeaders(headers, options);
  assert.deepEqual(splitHeaderTokens(headers["access-control-allow-methods"] ?? null), [
    "GET",
    "OPTIONS",
    "POST",
  ]);
  assert.deepEqual(splitHeaderTokens(headers["access-control-allow-headers"]), [
    "authorization",
    "b3",
    "content-type",
    "dpop",
    "traceparent",
    ORCHESTRATION_PROTOCOL_HEADER,
  ]);
};
const crossOriginClientOrigin = "http://remote-client.test:3773";

const getWsServerUrl = (
  pathname = "",
  options?: { authenticated?: boolean; credential?: string },
) =>
  Effect.gen(function* () {
    const server = yield* HttpServer.HttpServer;
    const address = server.address as NetAddress.InetAddress;
    const next = new URL(`ws://127.0.0.1:${address.port}${pathname}`);
    next.searchParams.set(
      ORCHESTRATION_PROTOCOL_QUERY_PARAM,
      String(ORCHESTRATION_PROTOCOL_VERSION),
    );
    const baseUrl = next.toString();
    if (options?.authenticated === false) {
      return baseUrl;
    }
    return appendSessionCookieToWsUrl(
      baseUrl,
      yield* getAuthenticatedSessionCookieHeader(options?.credential),
    );
  });

// Mirrors NodeHttpServer.layerTest, which does not expose server options,
// with the production `websocket: { perMessageDeflate: true }` setting.
const NodeHttpServerTestWithWsDeflate = HttpServer.layerTestClient.pipe(
  Layer.provide(
    Layer.fresh(FetchHttpClient.layer).pipe(
      Layer.provide(Layer.succeed(FetchHttpClient.RequestInit)({ keepalive: false })),
    ),
  ),
  Layer.provideMerge(
    Layer.unwrap(
      Effect.map(
        Effect.promise(() => import("node:http")),
        (NodeHttp) =>
          NodeHttpServer.layer(NodeHttp.createServer, {
            port: 0,
            websocket: { perMessageDeflate: true },
          }),
      ),
    ),
  ),
);

const EMPTY_DEVICE_STATE: DeviceServiceState = {
  hosts: [],
  hostStatus: "disabled",
  hostStatuses: {},
  devices: [],
  sessions: [],
  onboardingCompleted: false,
  agentAccessEnabled: false,
  hubBasePath: DeviceService.DEVICE_HUB_ROUTE_PREFIX,
  revision: 0,
};

it.layer(NodeServices.layer)("server router seam", (it) => {
  it.effect("parks HTTP ingress until command readiness", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const staticDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-router-gate-" });
      yield* fileSystem.writeFileString(path.join(staticDir, "index.html"), "ready");
      const entered = yield* Deferred.make<void>();
      const ready = yield* Deferred.make<void>();
      const completed = yield* Deferred.make<void>();

      yield* buildAppUnderTest({
        config: { staticDir },
        layers: {
          serverRuntimeStartup: {
            awaitCommandReady: Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(ready)),
            ),
          },
        },
      });
      const request = yield* HttpClient.get("/").pipe(
        Effect.tap(() => Deferred.succeed(completed, undefined)),
        Effect.forkChild,
      );
      yield* Deferred.await(entered);
      assert.isFalse(yield* Deferred.isDone(completed));

      yield* Deferred.succeed(ready, undefined);
      assert.equal((yield* Fiber.join(request)).status, 200);
      assert.isTrue(yield* Deferred.isDone(completed));
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves static index content for GET / when staticDir is configured", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const staticDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-router-static-" });
      const indexPath = path.join(staticDir, "index.html");
      yield* fileSystem.writeFileString(indexPath, "<html>router-static-ok</html>");

      yield* buildAppUnderTest({ config: { staticDir } });

      const response = yield* HttpClient.get("/");
      assert.equal(response.status, 200);
      assert.include(yield* response.text, "router-static-ok");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("revalidates static files without sending unchanged bodies", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const staticDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-static-cache-" });
      const assetPath = path.join(staticDir, "app.js");
      yield* fileSystem.writeFileString(assetPath, 'export const build = "first";');
      yield* buildAppUnderTest({ config: { staticDir } });

      const initial = yield* HttpClient.get("/app.js");
      assert.equal(initial.status, 200);
      assert.equal(initial.headers["cache-control"], "no-cache");
      assert.include(yield* initial.text, "first");
      const etag = initial.headers.etag;
      assert.isDefined(etag);
      assert.isDefined(initial.headers["last-modified"]);

      for (const headers of [
        { "if-none-match": etag! },
        { "if-none-match": `"older", ${etag!.replace(/^W\//, "")}` },
        { "if-none-match": "*" },
        { "if-modified-since": initial.headers["last-modified"]! },
      ]) {
        const response = yield* HttpClient.get("/app.js", { headers });
        assert.equal(response.status, 304);
        assert.equal(response.headers.etag, etag);
        assert.equal(response.headers["cache-control"], "no-cache");
        assert.equal(yield* response.text, "");
      }

      const mismatched = yield* HttpClient.get("/app.js", {
        headers: {
          "if-none-match": '"another-build"',
          "if-modified-since": initial.headers["last-modified"]!,
        },
      });
      assert.equal(mismatched.status, 200);
      assert.include(yield* mismatched.text, "first");

      yield* fileSystem.writeFileString(assetPath, 'export const build = "the next build";');
      const changed = yield* HttpClient.get("/app.js", { headers: { "if-none-match": etag! } });
      assert.equal(changed.status, 200);
      assert.notEqual(changed.headers.etag, etag);
      assert.include(yield* changed.text, "next build");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves changed HTML with the same size and timestamp", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const staticDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-static-html-" });
      const indexPath = path.join(staticDir, "index.html");
      const modifiedAt = DateTime.toDateUtc(DateTime.makeUnsafe("1985-10-26T08:15:00.000Z"));
      yield* fileSystem.writeFileString(indexPath, "<html>old build</html>");
      yield* fileSystem.utimes(indexPath, modifiedAt, modifiedAt);
      yield* buildAppUnderTest({ config: { staticDir } });

      const initial = yield* HttpClient.get("/");
      assert.equal(yield* initial.text, "<html>old build</html>");
      const previousEtag = initial.headers.etag ?? '"previous-html"';
      const nextHtml = "<html>new build</html>";
      yield* fileSystem.writeFileString(indexPath, nextHtml);
      yield* fileSystem.utimes(indexPath, modifiedAt, modifiedAt);

      for (const [resource, headers] of [
        ["/", { "if-none-match": previousEtag }],
        ["/threads/example", { "if-modified-since": modifiedAt.toUTCString() }],
        ["/", { "if-none-match": "*" }],
      ] as const) {
        const response = yield* HttpClient.get(resource, { headers });
        assert.equal(response.status, 200);
        assert.equal(yield* response.text, nextHtml);
        assert.equal(response.headers["cache-control"], "no-cache");
        assert.isUndefined(response.headers.etag);
        assert.isUndefined(response.headers["last-modified"]);
      }

      const head = yield* HttpClient.head("/", {
        headers: { "if-none-match": previousEtag, "accept-encoding": "identity" },
      });
      assert.equal(head.status, 200);
      assert.equal(head.headers["content-length"], String(Buffer.byteLength(nextHtml)));
      assert.equal(yield* head.text, "");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("caches hashed static assets without freezing mutable files or SPA fallbacks", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const staticDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-static-hashes-" });
      yield* fileSystem.makeDirectory(path.join(staticDir, "assets"));
      yield* fileSystem.makeDirectory(path.join(staticDir, ".vite"));
      yield* fileSystem.writeFileString(
        path.join(staticDir, ".vite", "manifest.json"),
        `{
          "index.html": { "file": "assets/index-AbCd0123.js", "isEntry": true },
          "large.js": { "file": "assets/large-aBcD9876.js" }
        }`,
      );
      yield* fileSystem.writeFileString(path.join(staticDir, "index.html"), "<html>app</html>");
      yield* fileSystem.writeFileString(
        path.join(staticDir, "assets", "index-AbCd0123.js"),
        "export const app = true;",
      );
      yield* fileSystem.writeFileString(path.join(staticDir, "assets", "config.json"), "{}");
      const largeAsset = "export const value = 123;\n".repeat(8192);
      yield* fileSystem.writeFileString(
        path.join(staticDir, "assets", "large-aBcD9876.js"),
        largeAsset,
      );
      yield* buildAppUnderTest({ config: { staticDir } });

      const asset = yield* HttpClient.get("/assets/index-AbCd0123.js");
      assert.equal(asset.status, 200);
      assert.equal(asset.headers["cache-control"], "public, max-age=31536000, immutable");
      assert.equal(yield* asset.text, "export const app = true;");

      const head = yield* HttpClient.head("/assets/index-AbCd0123.js", {
        headers: { "accept-encoding": "identity" },
      });
      assert.equal(head.status, 200);
      assert.equal(head.headers.etag, asset.headers.etag);
      assert.equal(head.headers["content-length"], String("export const app = true;".length));
      assert.equal(yield* head.text, "");

      const compressed = yield* HttpClient.get("/assets/large-aBcD9876.js", {
        headers: { "accept-encoding": "gzip" },
      });
      assert.equal(compressed.headers["content-encoding"], "gzip");
      assert.equal(compressed.headers.vary, "Accept-Encoding");
      assert.equal(yield* compressed.text, largeAsset);
      const compressedHead = yield* HttpClient.head("/assets/large-aBcD9876.js", {
        headers: { "accept-encoding": "gzip" },
      });
      assert.equal(compressedHead.status, 200);
      assert.equal(compressedHead.headers["content-encoding"], "gzip");
      assert.equal(compressedHead.headers.vary, "Accept-Encoding");
      assert.equal(compressedHead.headers.etag, compressed.headers.etag);
      assert.equal(compressedHead.headers["content-length"], compressed.headers["content-length"]);
      assert.equal(yield* compressedHead.text, "");
      const unchanged = yield* HttpClient.get("/assets/large-aBcD9876.js", {
        headers: { "accept-encoding": "identity", "if-none-match": compressed.headers.etag! },
      });
      assert.equal(unchanged.status, 304);
      assert.equal(unchanged.headers.vary, "Accept-Encoding");
      assert.equal(yield* unchanged.text, "");

      for (const resource of [
        "/assets/config.json",
        "/threads/example",
        "/assets/old-ZyXw9876.js",
      ]) {
        const response = yield* HttpClient.get(resource);
        assert.equal(response.status, 200);
        assert.equal(response.headers["cache-control"], "no-cache");
        assert.equal(
          yield* response.text,
          resource.endsWith("config.json") ? "{}" : "<html>app</html>",
        );
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  for (const manifest of [
    { label: "missing", contents: null },
    { label: "nonmatching", contents: '{"other.js":{"file":"assets/other-AbCd0123.js"}}' },
    { label: "malformed", contents: "{not-json" },
  ]) {
    it.effect(`revalidates hash-like static filenames with a ${manifest.label} manifest`, () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const staticDir = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-static-mutable-",
        });
        yield* fileSystem.makeDirectory(path.join(staticDir, "assets"));
        if (manifest.contents !== null) {
          yield* fileSystem.makeDirectory(path.join(staticDir, ".vite"));
          yield* fileSystem.writeFileString(
            path.join(staticDir, ".vite", "manifest.json"),
            manifest.contents,
          );
        }
        const filePath = path.join(staticDir, "assets", "config-20260904.js");
        yield* fileSystem.writeFileString(filePath, "first config");
        yield* buildAppUnderTest({ config: { staticDir } });

        const initial = yield* HttpClient.get("/assets/config-20260904.js");
        assert.equal(initial.headers["cache-control"], "no-cache");
        assert.equal(yield* initial.text, "first config");

        yield* fileSystem.writeFileString(filePath, "replacement config");
        const changed = yield* HttpClient.get("/assets/config-20260904.js", {
          headers: { "if-none-match": initial.headers.etag! },
        });
        assert.equal(changed.status, 200);
        assert.equal(changed.headers["cache-control"], "no-cache");
        assert.notEqual(changed.headers.etag, initial.headers.etag);
        assert.equal(yield* changed.text, "replacement config");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
    );
  }

  it.effect("binds static metadata and bytes to one file across atomic replacement", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const staticDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-static-replace-" });
      const beforeOpenPath = path.join(staticDir, "before-open.txt");
      const afterOpenPath = path.join(staticDir, "after-open.txt");
      const afterOpenSnapshotPath = path.join(staticDir, "after-open-snapshot.txt");
      const windowsHost = HostProcessPlatform.defaultValue() === "win32";
      const original = "original bytes";
      const replacement = "replacement bytes with a different size";
      for (const filePath of [beforeOpenPath, afterOpenPath]) {
        yield* fileSystem.writeFileString(filePath, original);
        yield* fileSystem.writeFileString(`${filePath}.next`, replacement);
      }
      if (windowsHost) {
        // Windows cannot replace an open destination, so model the race with its original handle.
        yield* fileSystem.writeFileString(afterOpenSnapshotPath, original);
      }
      const replaced = new Set<string>();
      const replaceOnce = Effect.fnUntraced(function* (filePath: string) {
        if (replaced.has(filePath)) return;
        replaced.add(filePath);
        yield* fileSystem.rename(`${filePath}.next`, filePath);
      });
      const replacingFileSystem = FileSystem.FileSystem.of({
        ...fileSystem,
        stat: (filePath) =>
          fileSystem
            .stat(filePath)
            .pipe(
              Effect.tap(() => (filePath === beforeOpenPath ? replaceOnce(filePath) : Effect.void)),
            ),
        open: (filePath, options) =>
          fileSystem
            .open(
              filePath === afterOpenPath && windowsHost ? afterOpenSnapshotPath : filePath,
              options,
            )
            .pipe(
              Effect.tap(() => (filePath === afterOpenPath ? replaceOnce(filePath) : Effect.void)),
            ),
      });
      yield* buildAppUnderTest({ config: { staticDir } }).pipe(
        Effect.provideService(FileSystem.FileSystem, replacingFileSystem),
      );

      for (const [name, expected] of [
        ["before-open.txt", replacement],
        ["after-open.txt", original],
      ] as const) {
        const response = yield* HttpClient.get(`/${name}`, {
          headers: { "accept-encoding": "identity" },
        });
        assert.equal(response.status, 200);
        assert.equal(response.headers["content-length"], String(expected.length));
        assert.isTrue(response.headers.etag?.startsWith(`W/"${expected.length.toString(16)}-`));
        assert.equal(yield* response.text, expected);
        assert.isTrue(replaced.has(path.join(staticDir, name)));
        assert.equal(yield* fileSystem.readFileString(path.join(staticDir, name)), replacement);
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("closes static file handles after GET, HEAD, 304, and request cancellation", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const staticDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-static-close-" });
      const filePath = path.join(staticDir, "app.txt");
      const body = "file content\n".repeat(1024);
      yield* fileSystem.writeFileString(filePath, body);
      const closed = yield* Queue.unbounded<FileSystem.File>();
      const blocked = yield* Deferred.make<void>();
      const active = new Set<FileSystem.File>();
      let blockAfterOpen = false;
      let bodyReads = 0;
      const trackedFileSystem = FileSystem.FileSystem.of({
        ...fileSystem,
        open: (candidate, options) =>
          Effect.gen(function* () {
            if (candidate !== filePath) return yield* fileSystem.open(candidate, options);
            let opened: FileSystem.File | undefined;
            // Registered first, so this signal runs after the real descriptor-close finalizer.
            yield* Effect.addFinalizer(() =>
              Effect.gen(function* () {
                if (opened === undefined) return;
                active.delete(opened);
                yield* Queue.offer(closed, opened);
              }),
            );
            const file = yield* fileSystem.open(candidate, options);
            opened = file;
            active.add(file);
            if (blockAfterOpen) {
              yield* Deferred.succeed(blocked, undefined);
              return yield* Effect.never;
            }
            return new Proxy(file, {
              get(target, key) {
                if (key === "readAlloc") {
                  return (size: number) => {
                    bodyReads += 1;
                    return target.readAlloc(size);
                  };
                }
                return Reflect.get(target, key, target);
              },
            });
          }),
      });
      yield* buildAppUnderTest({ config: { staticDir } }).pipe(
        Effect.provideService(FileSystem.FileSystem, trackedFileSystem),
      );

      const get = yield* HttpClient.get("/app.txt");
      assert.equal(yield* get.text, body);
      yield* Queue.take(closed);
      assert.equal(active.size, 0);
      assert.isAbove(bodyReads, 0);
      const readsAfterGet = bodyReads;

      const head = yield* HttpClient.head("/app.txt", { headers: { "accept-encoding": "gzip" } });
      assert.equal(head.status, 200);
      assert.equal(head.headers["content-encoding"], "gzip");
      assert.equal(yield* head.text, "");
      yield* Queue.take(closed);
      assert.equal(active.size, 0);
      assert.equal(bodyReads, readsAfterGet);

      const unchanged = yield* HttpClient.get("/app.txt", {
        headers: { "if-none-match": get.headers.etag! },
      });
      assert.equal(unchanged.status, 304);
      yield* Queue.take(closed);
      assert.equal(active.size, 0);
      assert.equal(bodyReads, readsAfterGet);

      blockAfterOpen = true;
      const cancelled = yield* HttpClient.get("/app.txt").pipe(Effect.forkChild);
      yield* Deferred.await(blocked);
      assert.equal(active.size, 1);
      yield* Fiber.interrupt(cancelled);
      yield* Queue.take(closed);
      assert.equal(active.size, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("redirects to dev URL when configured", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: { devUrl: new URL("http://127.0.0.1:5173") },
      });

      const url = yield* getHttpServerUrl("/foo/bar?token=test-token");
      const response = yield* fetchEffect(url, { redirect: "manual" });

      assert.equal(response.status, 302);
      assert.equal(response.headers.location, "http://127.0.0.1:5173/foo/bar?token=test-token");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves the public environment descriptor without requiring auth", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const url = yield* getHttpServerUrl("/.well-known/t3/environment");
      const response = yield* fetchEffect(url);
      const body = yield* responseJsonEffect<typeof testEnvironmentDescriptor>(response);

      assert.equal(response.status, 200);
      assert.deepEqual(body, testEnvironmentDescriptor);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "mounts native Workstreams routes closed by default and keeps dedicated tokens out of general APIs",
    () =>
      Effect.gen(function* () {
        let bearer = "";
        let sql: SqlClient.SqlClient | undefined;
        yield* buildAppUnderTest({
          layers: {
            onAuthSessionStore: (sessions) =>
              sessions
                .issue({
                  subject: "workstreams-native:synthetic-unenrolled",
                  method: "bearer-access-token",
                  scopes: [
                    "workstreams:native:context",
                    "workstreams:native:settlement",
                    "workstreams:native:reconciliation",
                  ],
                })
                .pipe(
                  Effect.map((session) => {
                    bearer = session.token;
                  }),
                ),
            onV2Services: (context) =>
              Effect.sync(() => {
                sql = Context.get(context, SqlClient.SqlClient);
              }),
          },
        });
        const cookie = yield* getAuthenticatedSessionCookieHeader();
        for (const [method, path] of [
          ["GET", "/api/workstreams/native/v1/context"],
          ["POST", "/api/workstreams/native/v1/attestations"],
          ["POST", "/api/workstreams/native/v1/settlements"],
          ["POST", "/api/workstreams/native/v1/settlements/lookup"],
        ] as const) {
          const url = yield* getHttpServerUrl(path);
          const browser = yield* fetchEffect(url, { method, headers: { cookie } });
          assert.equal(browser.status, 403);
          const native = yield* fetchEffect(url, {
            method,
            headers: { authorization: `Bearer ${bearer}` },
          });
          assert.equal(native.status, 403);
          assert.deepEqual(yield* responseJsonEffect(native), {
            protocol: "workstreams-t3-provider/1.0.0",
            state: "rejected",
            reason: "forbidden",
          });
        }
        for (const path of [
          "/api/orchestration/shell",
          `/api/orchestration/threads/${defaultThreadId}`,
          "/api/workstreams",
          "/api/workstreams/registration-context",
        ]) {
          const result = yield* fetchEffect(yield* getHttpServerUrl(path), {
            headers: {
              authorization: `Bearer ${bearer}`,
              [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
            },
          });
          assert.equal(result.status, 403);
        }
        const dispatch = yield* fetchEffect(
          yield* getHttpServerUrl("/api/orchestration/dispatch"),
          {
            method: "POST",
            headers: { authorization: `Bearer ${bearer}`, "content-type": "application/json" },
            // The endpoint decodes ThreadTurnStartCommand before its scope check.
            body: jsonRequestBody(makeGuardedQueueTransportCommand("native-denied")),
          },
        );
        assert.equal(dispatch.status, 403);
        const committed = yield* sql!<{
          count: number;
        }>`SELECT count(*) AS count FROM orchestration_events`;
        assert.equal(committed[0]?.count, 0);
        const registration = yield* fetchEffect(
          yield* getHttpServerUrl("/api/workstreams/registration-context"),
          {
            headers: { cookie },
          },
        );
        assert.equal(registration.status, 500);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "mounts queue compatibility routes with closed responses and scoped provider access",
    () =>
      Effect.gen(function* () {
        const instanceId = defaultModelSelection.instanceId;
        const providers: ReadonlyArray<ServerProvider> = [
          {
            instanceId,
            driver: ProviderDriverKind.make("codex"),
            displayName: "Synthetic Codex",
            enabled: true,
            installed: true,
            version: null,
            status: "ready",
            auth: { status: "authenticated" },
            checkedAt: "2026-01-01T00:00:00.000Z",
            models: [],
            slashCommands: [],
            skills: [],
          },
        ];
        const refreshes: ProviderInstanceId[] = [];
        const goalReads: unknown[] = [];
        const observationReads: unknown[] = [];
        const command = routerMessageDispatch("routes");
        let underlyingObserve: Orchestrator.OrchestratorV2["Service"]["observeCommand"];
        let observation: AwaitedObservation | undefined;
        const goal = {
          schema: "t3.provider-goal-state/v1" as const,
          threadId: defaultThreadId,
          providerInstanceId: instanceId,
          nativeThreadId: "synthetic-native-thread",
          observedAtMs: 1000,
          state: "active" as const,
          reasonCode: "goal_present" as const,
        };
        yield* buildAppUnderTest({
          layers: {
            providerRegistry: {
              getProviders: Effect.succeed(providers),
              refreshInstance: (requested) =>
                Effect.sync(() => {
                  refreshes.push(requested);
                  return providers;
                }),
            },
            providerGoalService: {
              get: (input) =>
                Effect.sync(() => {
                  goalReads.push(input);
                  return { ...goal, objective: "private synthetic objective" };
                }),
            },
            onV2Services: (context) =>
              Effect.gen(function* () {
                yield* seedRouterThread(context);
                underlyingObserve = Context.get(
                  context,
                  Orchestrator.OrchestratorV2,
                ).observeCommand;
                observation = yield* underlyingObserve({
                  threadId: command.threadId,
                  commandId: command.commandId,
                  messageId: command.messageId,
                });
              }),
            threadManagement: {
              observeCommand: (input) =>
                Effect.sync(() => observationReads.push(input)).pipe(
                  Effect.andThen(underlyingObserve(input)),
                ),
            },
          },
        });
        const operateToken = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
          scope: "orchestration:operate",
        });
        assert.equal(operateToken.response.status, 200);
        const queuePath = `/api/provider-queue/instances/${instanceId}`;
        const goalPath = `/api/orchestration/threads/${defaultThreadId}/provider-goal-state?expectedInstanceId=${instanceId}`;
        const observationPath = `/api/orchestration/v2/threads/${defaultThreadId}/commands/${command.commandId}?messageId=${command.messageId}`;
        for (const path of [
          "/api/provider-queue/inventory",
          `${queuePath}/usage`,
          goalPath,
          observationPath,
        ]) {
          const response = yield* fetchEffect(yield* getHttpServerUrl(path), {
            headers: { authorization: `Bearer ${operateToken.body.access_token}` },
          });
          assert.equal(response.status, 403);
        }
        const readToken = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
          scope: "orchestration:read",
        });
        assert.equal(readToken.response.status, 200);
        const readHeaders = { authorization: `Bearer ${readToken.body.access_token}` };
        const forbiddenRefresh = yield* fetchEffect(
          yield* getHttpServerUrl(`${queuePath}/refresh`),
          {
            method: "POST",
            headers: readHeaders,
          },
        );
        assert.equal(forbiddenRefresh.status, 403);
        assert.deepEqual(refreshes, []);
        assert.deepEqual(goalReads, []);
        assert.deepEqual(observationReads, []);

        const inventoryResponse = yield* fetchEffect(
          yield* getHttpServerUrl("/api/provider-queue/inventory"),
          { headers: readHeaders },
        );
        assert.equal(inventoryResponse.status, 200);
        const inventory = yield* responseJsonEffect<ProviderQueueInventory>(inventoryResponse);
        const instances: ProviderQueueInventory["instances"] = [
          {
            instanceId,
            displayName: "Synthetic Codex",
            driver: "codex",
            enabled: true,
            capabilityRefs: [],
          },
        ];
        assert.deepEqual(inventory, {
          schemaVersion: "t3.provider-queue-inventory/v1",
          inventoryRevision: NodeCrypto.createHash("sha256")
            .update(encodeTestJson(instances))
            .digest("hex"),
          observedAt: inventory.observedAt,
          evidence: "configured-provider-registry",
          instances,
        });
        assertTrue(Number.isFinite(Date.parse(inventory.observedAt)));
        const usageResponse = yield* fetchEffect(yield* getHttpServerUrl(`${queuePath}/usage`), {
          headers: readHeaders,
        });
        assert.equal(usageResponse.status, 200);
        assert.deepEqual(yield* responseJsonEffect(usageResponse), {
          instanceId,
          status: "cached",
          nextRefreshAt: null,
          quota: null,
        });
        const goalResponse = yield* fetchEffect(yield* getHttpServerUrl(goalPath), {
          headers: readHeaders,
        });
        assert.equal(goalResponse.status, 200);
        const goalBody = yield* responseJsonEffect<typeof goal>(goalResponse);
        assert.deepEqual({ ...goalBody, observedAtMs: goal.observedAtMs }, goal);
        const observationResponse = yield* fetchEffect(yield* getHttpServerUrl(observationPath), {
          headers: readHeaders,
        });
        assert.equal(observationResponse.status, 200);
        assert.deepEqual(yield* responseJsonEffect(observationResponse), observation);
        assert.deepEqual(goalReads, [
          { threadId: defaultThreadId, expectedInstanceId: instanceId },
        ]);
        assert.deepEqual(observationReads, [
          {
            threadId: defaultThreadId,
            commandId: command.commandId,
            messageId: command.messageId,
          },
        ]);
        assert.deepEqual(refreshes, []);

        const adminToken = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
          scope: "orchestration:read access:write",
        });
        assert.equal(adminToken.response.status, 200);
        const adminHeaders = { authorization: `Bearer ${adminToken.body.access_token}` };
        const refreshResponse = yield* fetchEffect(
          yield* getHttpServerUrl(`${queuePath}/refresh`),
          {
            method: "POST",
            headers: adminHeaders,
          },
        );
        assert.equal(refreshResponse.status, 200);
        const refreshed = yield* responseJsonEffect<ProviderQueueRefreshResult>(refreshResponse);
        assertTrue(Schema.is(ProviderQueueRefreshResult)(refreshed));
        assert.isNotNull(refreshed.quota);
        const quota = refreshed.quota!;
        assert.deepEqual(refreshed, {
          instanceId,
          status: "refreshed",
          nextRefreshAt: refreshed.nextRefreshAt,
          quota: {
            schemaVersion: "codex.t3-qualified-quota/v1",
            instanceId,
            probeId: quota.probeId,
            status: "failed",
            attemptedAt: quota.attemptedAt,
            quotaReceivedAt: null,
            probeCompletedAt: quota.probeCompletedAt,
            complete: false,
            rateLimitsByLimitId: null,
            windowProvenance: [],
            capabilityRefs: [],
            failureCode: "refresh_not_observed",
          },
        });
        assert.equal(Date.parse(refreshed.nextRefreshAt!) - Date.parse(quota.attemptedAt), 300_000);
        const cachedResponse = yield* fetchEffect(yield* getHttpServerUrl(`${queuePath}/usage`), {
          headers: adminHeaders,
        });
        assert.equal(cachedResponse.status, 200);
        assert.deepEqual(yield* responseJsonEffect(cachedResponse), {
          ...refreshed,
          status: "cached",
        });
        assert.deepEqual(refreshes, [instanceId]);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("preserves guarded WebSocket command IDs through HTTP command observation", () =>
    Effect.gen(function* () {
      const command = routerMessageDispatch("guarded-observation");
      let guard!: ThreadTurnDispatchGuardV2;
      let services!: Context.Context<RouterV2Services>;
      const dispatched: unknown[] = [];
      const observed: unknown[] = [];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              services = context;
              yield* seedRouterThread(context);
              const observation = yield* Context.get(
                context,
                Orchestrator.OrchestratorV2,
              ).observeCommand(command);
              const target = observation.target;
              assertTrue(target !== null && target.incarnation !== null);
              assert.isTrue(target.complete);
              assert.isTrue(target.idle);
              guard = {
                version: 2,
                observedSnapshotSequence: target.snapshotSequence,
                expectedIncarnation: target.incarnation,
                expectedModelSelection: target.modelSelection,
                expectedActiveRunId: target.activeRunId,
                expectedLatestRunId: target.latestRunId,
                expectedActiveRunAttemptId: target.activeRunAttemptId,
                expectedActiveProviderThreadId: target.activeProviderThreadId,
                expectedProviderSessionId: target.providerSessionId,
                expectedProviderSessionStatus: target.providerSessionStatus,
                ...(target.runtimeGeneration === undefined
                  ? {}
                  : { expectedRuntimeGeneration: target.runtimeGeneration }),
                requireIdle: true,
              };
            }),
          wrapThreadManagement: (service) => ({
            ...service,
            dispatchGuarded: (input, expected) =>
              Effect.sync(() => dispatched.push({ input, expected })).pipe(
                Effect.andThen(service.dispatchGuarded(input, expected)),
              ),
            observeCommand: (input) =>
              Effect.sync(() => observed.push(input)).pipe(
                Effect.andThen(service.observeCommand(input)),
              ),
          }),
        },
      });
      const receipt = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.dispatchGuarded]({ command, guard }),
        ),
      );
      assert.isAbove(receipt.sequence, guard.observedSnapshotSequence);
      assert.deepEqual(dispatched, [{ input: command, expected: guard }]);
      const token = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
        scope: "orchestration:read",
      });
      assert.equal(token.response.status, 200);
      const response = yield* fetchEffect(
        yield* getHttpServerUrl(
          "/api/orchestration/v2/threads/" +
            command.threadId +
            "/commands/" +
            command.commandId +
            "?messageId=" +
            command.messageId,
        ),
        { headers: { authorization: "Bearer " + token.body.access_token } },
      );
      assert.equal(response.status, 200);
      const observation = yield* responseJsonEffect<AwaitedObservation>(response);
      assert.equal(observation.commandId, command.commandId);
      assert.equal(observation.messageId, command.messageId);
      assert.equal(observation.receipt?.resultSequence, receipt.sequence);
      assert.equal(observation.commandStatus, "accepted");
      assert.equal(observation.correlation, "exact");
      assert.deepEqual(observed, [
        { threadId: command.threadId, commandId: command.commandId, messageId: command.messageId },
      ]);
      const durable = yield* Context.get(services, Orchestrator.OrchestratorV2).observeCommand(
        command,
      );
      assert.equal(durable.receipt?.resultSequence, receipt.sequence);
      assert.equal(durable.correlation, "exact");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "rejects guarded HTTP bootstrap while WebSocket prepares before final guarded dispatch",
    () =>
      Effect.gen(function* () {
        const value = yield* qualifiedNativeRouterFixture("guarded-http-bootstrap", false);
        const baseCommand = makeGuardedQueueTransportCommand("bootstrap");
        const command = {
          ...baseCommand,
          commandId: CommandId.make(value.fixture.command.commandId),
          threadId: ThreadId.make(value.fixture.command.threadId),
          bootstrap: value.fixture.command.bootstrap,
        };
        const token = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
          scope: "orchestration:operate",
        });
        assert.equal(token.response.status, 200);
        const rejected = yield* fetchEffect(
          yield* getHttpServerUrl("/api/orchestration/dispatch"),
          {
            method: "POST",
            headers: {
              authorization: `Bearer ${token.body.access_token}`,
              "content-type": "application/json",
            },
            body: jsonRequestBody(command),
          },
        );
        assert.equal(rejected.status, 400);
        const failure = yield* responseJsonEffect<{ reason: string }>(rejected);
        assert.equal(failure.reason, "dispatch_guard_bootstrap_unsupported");
        assert.deepEqual(value.stages, []);
        assert.deepEqual(value.detachments, []);
        assert.deepEqual(value.nativeCalls, []);
        const receipt = yield* Effect.scoped(
          withWsRpcClient(value.url, (client) =>
            client[ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap](value.fixture.submission),
          ),
        );
        assert.equal(receipt.commandAcceptance, "accepted");
        assert.isAbove(receipt.creation?.finalReceipt?.resultSequence ?? 0, 0);
        assert.deepEqual(
          value.stages.map((input) => input.type),
          ["thread.create", "message.dispatch", "prepared-run.release"],
        );
        assert.deepEqual(value.order, [
          "thread.create",
          "message.dispatch",
          "checkout",
          "prepared-run.release",
        ]);
        assert.deepEqual(value.detachments, [{ threadId: command.threadId, status: "stopped" }]);
        const finalCommand = value.stages[2];
        assertTrue(finalCommand?.type === "prepared-run.release");
        assert.deepEqual(value.stages, value.expectedCommands());
        assert.equal(finalCommand.commandId, command.commandId);
        assert.equal(finalCommand.threadId, command.threadId);
        assert.notEqual(value.stages[0]!.commandId, command.commandId);
        assert.notEqual(value.stages[1]!.commandId, command.commandId);
        assert.notEqual(value.stages[0]!.commandId, value.stages[1]!.commandId);
        const creation = receipt.creation;
        assertTrue(creation !== null && creation.incarnation !== null);
        const sequences = creation.stageCommands.map((stage) => {
          assertTrue(stage.receipt !== null && stage.event !== null);
          assertTrue(Number.isFinite(Date.parse(DateTime.formatIso(stage.receipt.acceptedAt))));
          assert.equal(stage.receipt.resultSequence, stage.event.sequence);
          return stage.event.sequence;
        });
        assert.isAbove(sequences[1]!, sequences[0]!);
        assert.isAbove(sequences[2]!, sequences[1]!);
        assert.deepEqual(creation.incarnation, creation.stageCommands[0]!.event);
        const threads = Context.get(value.services, ThreadManagement.ThreadManagementService);
        const birth = Option.getOrThrow(
          yield* threads.getThreadOwnershipIncarnation(command.threadId),
        );
        const leases = yield* threads.listWorktreeOwnershipLeases;
        const checkout = nativeWorktreePath({
          worktreesDir: value.config.worktreesDir,
          cwd: value.fixture.binding.project_cwd,
          branch: value.fixture.historical.requestedBranch,
        });
        assert.isTrue(
          leases.some(
            (lease) =>
              lease.ownerThreadId === command.threadId &&
              lease.ownerIncarnation === birth &&
              lease.resourcePath === checkout &&
              lease.branch === value.fixture.historical.requestedBranch,
          ),
        );
        assert.deepEqual(value.nativeCalls, []);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves snapshots for MCP handoff thread IDs above the router default", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make(
        "thread:mcp:abfba0d2-b591-4b7e-aad1-e943d89811fa:handoff%3A0ae5edf4-2ea3-4ee3-ba7c-48de3ac92896%3A2026-08-24T17%3A08%3A52.138Z:0",
      );
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) => seedRouterThread(context, threadId).pipe(Effect.asVoid),
        },
      });
      const response = yield* fetchEffect(
        yield* getHttpServerUrl("/api/orchestration/threads/" + encodeURIComponent(threadId)),
        {
          headers: {
            cookie: yield* getAuthenticatedSessionCookieHeader(),
            [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
          },
        },
      );
      const snapshot = yield* responseJsonEffect<{ projection: { thread: { id: ThreadId } } }>(
        response,
      );
      assert.equal(response.status, 200);
      assert.equal(snapshot.projection.thread.id, threadId);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("compresses large JSON responses through the composed routes", () =>
    Effect.gen(function* () {
      const descriptor = {
        ...testEnvironmentDescriptor,
        label: "Test environment".repeat(100),
      };
      yield* buildAppUnderTest({
        layers: {
          serverEnvironment: {
            getDescriptor: Effect.succeed(descriptor),
          },
        },
      });

      const url = yield* getHttpServerUrl("/.well-known/t3/environment");
      const response = yield* fetchEffect(url, {
        headers: {
          "accept-encoding": "gzip",
        },
      });
      const body = yield* responseJsonEffect<typeof descriptor>(response);

      assert.equal(response.status, 200);
      assert.equal(response.headers["content-encoding"], "gzip");
      assert.equal(response.headers.vary, "Accept-Encoding");
      assert.deepEqual(body, descriptor);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("includes CORS headers on public environment descriptor responses", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const url = yield* getHttpServerUrl("/.well-known/t3/environment");
      const response = yield* fetchEffect(url, {
        headers: {
          origin: crossOriginClientOrigin,
        },
      });
      const body = yield* responseJsonEffect<typeof testEnvironmentDescriptor>(response);

      assert.equal(response.status, 200);
      assertBrowserApiCorsResponseHeaders(response.headers);
      assert.deepEqual(body, testEnvironmentDescriptor);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("reports unauthenticated session state without requiring auth", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const url = yield* getHttpServerUrl("/api/auth/session");
      const response = yield* fetchEffect(url);
      const body = yield* responseJsonEffect<{
        readonly authenticated: boolean;
        readonly auth: {
          readonly policy: string;
          readonly bootstrapMethods: ReadonlyArray<string>;
          readonly sessionMethods: ReadonlyArray<string>;
          readonly sessionCookieName: string;
        };
      }>(response);

      assert.equal(response.status, 200);
      assert.equal(body.authenticated, false);
      assert.equal(body.auth.policy, "desktop-managed-local");
      assert.deepEqual(body.auth.bootstrapMethods, ["desktop-bootstrap"]);
      assert.deepEqual(body.auth.sessionMethods, [
        "browser-session-cookie",
        "bearer-access-token",
        "dpop-access-token",
      ]);
      // Desktop, so port-scoped: instances scan for a free port and share
      // 127.0.0.1, and cookies are not scoped by port.
      assert.isTrue(body.auth.sessionCookieName.startsWith("t3_session_"));
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("bootstraps a browser session and authenticates the session endpoint via cookie", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const {
        response: bootstrapResponse,
        body: bootstrapBody,
        cookie: setCookie,
      } = yield* bootstrapBrowserSession();

      assert.equal(bootstrapResponse.status, 200);
      assert.equal(bootstrapBody.authenticated, true);
      assert.equal(bootstrapBody.sessionMethod, "browser-session-cookie");
      assert.isUndefined((bootstrapBody as { readonly sessionToken?: string }).sessionToken);
      assert.isDefined(setCookie);

      const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
      const sessionResponse = yield* fetchEffect(sessionUrl, {
        headers: {
          cookie: setCookie?.split(";")[0] ?? "",
        },
      });
      const sessionBody = yield* responseJsonEffect<{
        readonly authenticated: boolean;
        readonly sessionMethod?: string;
      }>(sessionResponse);

      assert.equal(sessionResponse.status, 200);
      assert.equal(sessionBody.authenticated, true);
      assert.equal(sessionBody.sessionMethod, "browser-session-cookie");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect.each(["web", "desktop"] as const)(
    "ordinary Release %s host returns absent Jones state without displacing Release updates",
    (mode) =>
      Effect.gen(function* () {
        yield* buildAppUnderTest({ config: { mode } });
        const url = yield* getHttpServerUrl("/api/jones-updates");
        const response = yield* fetchEffect(url, {
          headers: { cookie: yield* getAuthenticatedSessionCookieHeader() },
        });
        assert.equal(response.status, 200);
        assert.deepEqual(yield* responseJsonEffect(response), null);
        assert.equal(response.headers["cache-control"], "no-store");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("migrates a valid legacy remote-web session cookie", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({ config: { mode: "web", host: "192.168.1.50" } });

      const { cookie } = yield* bootstrapBrowserSession();
      const currentCookie = cookie?.split(";")[0] ?? "";
      const legacyCookie = currentCookie.replace(/^t3_session_[^=]+=/, "t3_session=");
      const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
      const response = yield* fetchEffect(sessionUrl, {
        headers: { cookie: legacyCookie },
      });
      const body = yield* responseJsonEffect<{ readonly authenticated: boolean }>(response);

      assert.equal(body.authenticated, true);
      assert.equal(response.headers["set-cookie"], cookie);
      assert.equal(response.headers["cache-control"], "no-store");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect.each(["cookie", "bearer"])(
    "does not migrate a stale legacy cookie when %s auth succeeds",
    (source) =>
      Effect.gen(function* () {
        yield* buildAppUnderTest({ config: { mode: "web", host: "192.168.1.50" } });

        const { cookie } = yield* bootstrapBrowserSession();
        const sessionCookie = cookie?.split(";")[0] ?? "";
        const sessionToken = extractSessionTokenFromSetCookie(cookie ?? "");
        const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
        const response = yield* fetchEffect(sessionUrl, {
          headers:
            source === "cookie"
              ? { cookie: `${sessionCookie}; t3_session=stale` }
              : { authorization: `Bearer ${sessionToken}`, cookie: "t3_session=stale" },
        });
        const body = yield* responseJsonEffect<{ readonly authenticated: boolean }>(response);

        assert.equal(body.authenticated, true);
        assert.isUndefined(response.headers["set-cookie"]);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("exchanges a bootstrap grant for a scoped bearer access token", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const { response: tokenResponse, body: tokenBody } = yield* exchangeAccessToken();

      assert.equal(tokenResponse.status, 200);
      assert.equal(tokenBody.issued_token_type, AuthAccessTokenType);
      assert.equal(tokenBody.token_type, "Bearer");
      assert.equal(
        tokenBody.scope,
        "orchestration:read orchestration:operate terminal:operate review:write relay:read access:read access:write relay:write",
      );
      assert.equal(typeof tokenBody.access_token, "string");

      const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
      const sessionResponse = yield* fetchEffect(sessionUrl, {
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
        },
      });
      const sessionBody = yield* responseJsonEffect<{
        readonly authenticated: boolean;
        readonly sessionMethod?: string;
        readonly scopes?: ReadonlyArray<string>;
      }>(sessionResponse);

      assert.equal(sessionResponse.status, 200);
      assert.equal(sessionBody.authenticated, true);
      assert.equal(sessionBody.sessionMethod, "bearer-access-token");
      assert.deepEqual(sessionBody.scopes, [
        "orchestration:read",
        "orchestration:operate",
        "terminal:operate",
        "review:write",
        "relay:read",
        "access:read",
        "access:write",
        "relay:write",
      ]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("replaces the local desktop credential on repeated bootstrap exchanges", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();
      const first = yield* exchangeAccessToken();
      const second = yield* exchangeAccessToken();
      const third = yield* exchangeAccessToken();
      assert.equal(first.response.status, 200);
      assert.equal(second.response.status, 200);
      assert.equal(third.response.status, 200);

      const clientsResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: { authorization: `Bearer ${third.body.access_token}` },
      });
      const clients = (yield* clientsResponse.json) as ReadonlyArray<{
        readonly current: boolean;
        readonly subject: string;
      }>;
      assert.equal(clientsResponse.status, 200);
      assert.equal(clients.length, 1);
      assert.equal(clients[0]?.current, true);
      assert.equal(clients[0]?.subject, "desktop-bootstrap");

      for (const previous of [first, second]) {
        const response = yield* HttpClient.get("/api/auth/session", {
          headers: { authorization: `Bearer ${previous.body.access_token}` },
        });
        const state = (yield* response.json) as { readonly authenticated: boolean };
        assert.equal(state.authenticated, false);
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("persists token exchange client display metadata for authorized-client listings", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const pairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const pairingBody = (yield* pairingResponse.json) as {
        readonly credential: string;
      };

      const { response } = yield* exchangeAccessToken(pairingBody.credential, {
        headers: {
          "user-agent": "undici",
        },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
        clientMetadata: {
          label: "T3 Code Mobile",
          deviceType: "mobile",
          os: "iOS",
        },
      });

      const clientsResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const clients = (yield* clientsResponse.json) as ReadonlyArray<{
        readonly current: boolean;
        readonly client: {
          readonly label?: string;
          readonly deviceType: string;
          readonly ipAddress?: string;
          readonly os?: string;
          readonly userAgent?: string;
        };
      }>;
      const mobileClient = clients.find((client) => !client.current);

      assert.equal(pairingResponse.status, 200);
      assert.equal(response.status, 200);
      assert.equal(clientsResponse.status, 200);
      assert.deepInclude(mobileClient?.client, {
        label: "T3 Code Mobile",
        deviceType: "mobile",
        os: "iOS",
        ipAddress: "127.0.0.1",
        userAgent: "undici",
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "exchanges a bootstrap credential for a DPoP-bound access token without bearer downgrade",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
        const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
          headers: { cookie: ownerCookie },
          body: yield* HttpBody.json({}),
        });
        const credential = (yield* credentialResponse.json) as { readonly credential: string };
        const tokenUrl = yield* getHttpServerUrl("/oauth/token");
        const now = yield* DateTime.now;
        const tokenProof = makeDpopProof({
          method: "POST",
          url: tokenUrl,
          iat: Math.floor(now.epochMilliseconds / 1_000),
          jti: "token-exchange-proof",
        });
        const tokenResponse = yield* fetchEffect(tokenUrl, {
          method: "POST",
          headers: {
            "content-type": "application/x-www-form-urlencoded",
            dpop: tokenProof.proof,
          },
          body: new URLSearchParams({
            grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
            subject_token: credential.credential,
            subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
            requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
            scope: "orchestration:read orchestration:operate terminal:operate review:write",
          }).toString(),
        });
        const token = yield* responseJsonEffect<{
          readonly access_token: string;
          readonly token_type: string;
        }>(tokenResponse);

        assert.equal(tokenResponse.status, 200);
        assert.equal(tokenResponse.headers["cache-control"], "no-store");
        assert.equal(token.token_type, "DPoP");

        const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
        const bearerResponse = yield* fetchEffect(sessionUrl, {
          headers: { authorization: `Bearer ${token.access_token}` },
        });
        const bearerState = yield* responseJsonEffect<{ readonly authenticated: boolean }>(
          bearerResponse,
        );
        assert.equal(bearerState.authenticated, false);

        const sessionProof = makeDpopProof({
          method: "GET",
          url: sessionUrl,
          iat: Math.floor(now.epochMilliseconds / 1_000),
          jti: "session-proof",
          accessToken: token.access_token,
          privateKey: tokenProof.privateKey,
          publicJwk: tokenProof.publicJwk,
        });
        const dpopResponse = yield* fetchEffect(sessionUrl, {
          headers: {
            authorization: `DPoP ${token.access_token}`,
            dpop: sessionProof.proof,
          },
        });
        const dpopState = yield* responseJsonEffect<{
          readonly authenticated: boolean;
          readonly sessionMethod?: string;
        }>(dpopResponse);
        assert.equal(dpopState.authenticated, true);
        assert.equal(dpopState.sessionMethod, "dpop-access-token");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("reports clock skew for a future-dated DPoP token exchange proof", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: { cookie: ownerCookie },
        body: yield* HttpBody.json({}),
      });
      const credential = (yield* credentialResponse.json) as { readonly credential: string };
      const tokenUrl = yield* getHttpServerUrl("/oauth/token");
      const now = yield* DateTime.now;
      const dpop = makeDpopProof({
        method: "POST",
        url: tokenUrl,
        iat: Math.floor(now.epochMilliseconds / 1_000) + 25,
      });

      const exchange = yield* exchangeAccessToken(credential.credential, {
        headers: { dpop: dpop.proof },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
      });

      assert.equal(exchange.response.status, 401);
      assert.equal(exchange.body._tag, "EnvironmentAuthInvalidError");
      assert.equal(exchange.body.code, "auth_invalid");
      assert.equal(exchange.body.reason, "invalid_credential");
      assert.equal(exchange.body.dpopFailureReason, "time_window");
      assert.equal(typeof exchange.body.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects replayed DPoP proofs across token exchanges", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const firstCredentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const firstCredential = (yield* firstCredentialResponse.json) as {
        readonly credential: string;
      };
      const secondCredentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const secondCredential = (yield* secondCredentialResponse.json) as {
        readonly credential: string;
      };
      const tokenUrl = yield* getHttpServerUrl("/oauth/token");
      const now = yield* DateTime.now;
      const dpop = makeDpopProof({
        method: "POST",
        url: tokenUrl,
        iat: Math.floor(now.epochMilliseconds / 1_000),
      });

      const firstBootstrap = yield* exchangeAccessToken(firstCredential.credential, {
        headers: {
          dpop: dpop.proof,
        },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
      });
      const replayBootstrap = yield* exchangeAccessToken(secondCredential.credential, {
        headers: {
          dpop: dpop.proof,
        },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
      });

      assert.equal(firstBootstrap.response.status, 200);
      assert.equal(replayBootstrap.response.status, 401);
      assert.equal(replayBootstrap.body._tag, "EnvironmentAuthInvalidError");
      assert.equal(replayBootstrap.body.code, "auth_invalid");
      assert.equal(replayBootstrap.body.reason, "invalid_credential");
      assert.equal(replayBootstrap.body.dpopFailureReason, "replay");
      assert.equal(typeof replayBootstrap.body.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects a DPoP replay by time alone once its marker can be pruned", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: { cookie: ownerCookie },
        body: yield* HttpBody.json({}),
      });
      const credential = (yield* credentialResponse.json) as { readonly credential: string };
      const tokenUrl = yield* getHttpServerUrl("/oauth/token");
      const acceptedAt = yield* DateTime.now;
      // The longest-lived proof: `iat` at the 5 s future skew the verifier allows.
      const dpop = makeDpopProof({
        method: "POST",
        url: tokenUrl,
        iat: Math.floor(acceptedAt.epochMilliseconds / 1_000) + 5,
      });
      const exchange = exchangeAccessToken(credential.credential, {
        headers: { dpop: dpop.proof },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
      });

      assert.equal((yield* exchange).response.status, 200);
      // While the proof is fresh, only the replay marker rejects it.
      assert.equal((yield* exchange).body.dpopFailureReason, "replay");
      // Once the marker can be pruned, the time check rejects the proof by itself.
      yield* TestClock.setTime(
        acceptedAt.epochMilliseconds + Duration.toMillis(REPLAY_MARKER_MAX_AGE),
      );
      assert.equal((yield* exchange).body.dpopFailureReason, "time_window");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("ignores forwarded host headers when validating token exchange DPoP URLs", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const credential = (yield* credentialResponse.json) as {
        readonly credential: string;
      };
      const tokenUrl = yield* getHttpServerUrl("/oauth/token");
      const now = yield* DateTime.now;
      const dpop = makeDpopProof({
        method: "POST",
        url: tokenUrl,
        iat: Math.floor(now.epochMilliseconds / 1_000),
      });

      const bootstrap = yield* exchangeAccessToken(credential.credential, {
        headers: {
          dpop: dpop.proof,
          "x-forwarded-host": "environment.example.test",
        },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
      });

      assert.equal(bootstrap.response.status, 200);
      assert.equal(bootstrap.body.token_type, "DPoP");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects token exchange DPoP proofs bound to spoofed forwarded hosts", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const credential = (yield* credentialResponse.json) as {
        readonly credential: string;
      };
      const tokenUrl = yield* getHttpServerUrl("/oauth/token");
      const spoofedUrl = new URL(tokenUrl);
      spoofedUrl.hostname = "environment.example.test";
      const now = yield* DateTime.now;
      const dpop = makeDpopProof({
        method: "POST",
        url: spoofedUrl.href,
        iat: Math.floor(now.epochMilliseconds / 1_000),
      });

      const bootstrap = yield* exchangeAccessToken(credential.credential, {
        headers: {
          dpop: dpop.proof,
          "x-forwarded-host": spoofedUrl.host,
        },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
      });

      assert.equal(bootstrap.response.status, 401);
      assert.equal(bootstrap.body._tag, "EnvironmentAuthInvalidError");
      assert.equal(bootstrap.body.code, "auth_invalid");
      assert.equal(bootstrap.body.reason, "invalid_credential");
      assert.equal(bootstrap.body.dpopFailureReason, "request_mismatch");
      assert.equal(typeof bootstrap.body.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud link proofs for non-loopback managed endpoint origins", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const linkProofResponse = yield* fetchEffect(linkProofUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          challenge: "relay-link-challenge",
          relayIssuer: "https://relay.example.test",
          endpoint: {
            httpBaseUrl: "https://environment.example.test/",
            wsBaseUrl: "wss://environment.example.test/ws",
            providerKind: "manual",
          },
          origin: {
            localHttpHost: "192.168.1.42",
            localHttpPort: 3773,
          },
        }),
      });
      const body = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(linkProofResponse);

      assert.equal(linkProofResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Invalid managed endpoint origin.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud link proofs for unsupported endpoint providers", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const serverPort = Number(new URL(linkProofUrl).port);
      const linkProofResponse = yield* fetchEffect(linkProofUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          challenge: "relay-link-challenge",
          relayIssuer: "https://relay.example.test",
          endpoint: {
            httpBaseUrl: linkProofUrl.replace("/api/connect/link-proof", ""),
            wsBaseUrl: linkProofUrl
              .replace("http://", "ws://")
              .replace("/api/connect/link-proof", "/ws"),
            // "manual" and "cloudflare_tunnel" are supported; "t3_relay" is not.
            providerKind: "t3_relay",
          },
          origin: {
            localHttpHost: "127.0.0.1",
            localHttpPort: serverPort,
          },
        }),
      });
      const body = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(linkProofResponse);

      assert.equal(linkProofResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Invalid managed endpoint origin.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud link proofs requested through a public managed endpoint", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const serverPort = Number(new URL(linkProofUrl).port);
      const linkProofResponse = yield* HttpClient.post("/api/connect/link-proof", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
          "content-type": "application/json",
          host: "environment.example.test",
          "x-forwarded-host": "environment.example.test",
          "x-forwarded-proto": "https",
        },
        body: HttpBody.text(
          jsonRequestBody({
            challenge: "relay-link-challenge",
            relayIssuer: "https://relay.example.test",
            endpoint: {
              httpBaseUrl: "https://environment.example.test/",
              wsBaseUrl: "wss://environment.example.test/ws",
              providerKind: "manual",
            },
            origin: {
              localHttpHost: "127.0.0.1",
              localHttpPort: serverPort,
            },
          }),
          "application/json",
        ),
      });
      const body = (yield* linkProofResponse.json) as {
        readonly _tag?: string;
        readonly message?: string;
      };

      assert.equal(linkProofResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Invalid managed endpoint origin.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "rejects cloud link proofs when a public request spoofs loopback forwarded headers",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
        const serverPort = Number(new URL(linkProofUrl).port);
        const linkProofResponse = yield* HttpClient.post("/api/connect/link-proof", {
          headers: {
            cookie: yield* getAuthenticatedSessionCookieHeader(),
            "content-type": "application/json",
            host: "environment.example.test",
            "x-forwarded-host": `127.0.0.1:${serverPort}`,
            "x-forwarded-proto": "http",
          },
          body: HttpBody.text(
            jsonRequestBody({
              challenge: "relay-link-challenge",
              relayIssuer: "https://relay.example.test",
              endpoint: {
                httpBaseUrl: "https://environment.example.test/",
                wsBaseUrl: "wss://environment.example.test/ws",
                providerKind: "manual",
              },
              origin: {
                localHttpHost: "127.0.0.1",
                localHttpPort: serverPort,
              },
            }),
            "application/json",
          ),
        });
        const body = (yield* linkProofResponse.json) as {
          readonly _tag?: string;
          readonly message?: string;
        };

        assert.equal(linkProofResponse.status, 400);
        assert.equal(body._tag, "EnvironmentHttpBadRequestError");
        assert.equal(body.message, "Invalid managed endpoint origin.");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud link proofs with malformed forwarded request hosts", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const serverPort = Number(new URL(linkProofUrl).port);
      const linkProofResponse = yield* HttpClient.post("/api/connect/link-proof", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
          "content-type": "application/json",
          host: "bad host",
          "x-forwarded-host": "bad host",
          "x-forwarded-proto": "https",
        },
        body: HttpBody.text(
          jsonRequestBody({
            challenge: "relay-link-challenge",
            relayIssuer: "https://relay.example.test",
            endpoint: {
              httpBaseUrl: "https://environment.example.test/",
              wsBaseUrl: "wss://environment.example.test/ws",
              providerKind: "manual",
            },
            origin: {
              localHttpHost: "127.0.0.1",
              localHttpPort: serverPort,
            },
          }),
          "application/json",
        ),
      });
      const body = (yield* linkProofResponse.json) as {
        readonly _tag?: string;
        readonly message?: string;
      };

      assert.equal(linkProofResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Invalid managed endpoint origin.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects local cloud link proofs for a different loopback port", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const serverPort = Number(new URL(linkProofUrl).port);
      const linkProofResponse = yield* fetchEffect(linkProofUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          challenge: "relay-link-challenge",
          relayIssuer: "https://relay.example.test",
          endpoint: {
            httpBaseUrl: "https://environment.example.test/",
            wsBaseUrl: "wss://environment.example.test/ws",
            providerKind: "manual",
          },
          origin: {
            localHttpHost: "127.0.0.1",
            localHttpPort: serverPort === 65_535 ? serverPort - 1 : serverPort + 1,
          },
        }),
      });
      const body = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(linkProofResponse);

      assert.equal(linkProofResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Invalid managed endpoint origin.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("allows standard clients to read managed relay configuration state", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: { cookie: ownerCookie },
        body: yield* HttpBody.json({}),
      });
      const credential = (yield* credentialResponse.json) as { readonly credential: string };
      const pairedCookie = yield* getAuthenticatedSessionCookieHeader(credential.credential);
      const linkStateUrl = yield* getHttpServerUrl("/api/connect/link-state");
      const response = yield* fetchEffect(linkStateUrl, {
        headers: { cookie: pairedCookie },
      });
      const body = yield* responseJsonEffect<{
        readonly linked?: boolean;
        readonly publishAgentActivity?: boolean;
      }>(response);

      assert.equal(response.status, 200);
      assert.equal(body.linked, false);
      assert.equal(body.publishAgentActivity, false);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "reports relay client status and streams installation progress over environment RPC",
    () =>
      Effect.gen(function* () {
        const installedRelayClient = {
          status: "available" as const,
          executablePath: "/tmp/t3/tools/cloudflared",
          source: "managed" as const,
          version: RelayClient.CLOUDFLARED_VERSION,
        };
        yield* buildAppUnderTest({
          layers: {
            relayClient: {
              resolve: Effect.succeed({
                status: "missing",
                version: RelayClient.CLOUDFLARED_VERSION,
              }),
              install: Effect.succeed(installedRelayClient),
              installWithProgress: (report) =>
                report({ type: "progress", stage: "checking" }).pipe(
                  Effect.andThen(report({ type: "progress", stage: "downloading" })),
                  Effect.as(installedRelayClient),
                ),
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        const status = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) => client[WS_METHODS.cloudGetRelayClientStatus]({})),
        );
        const installEvents = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.cloudInstallRelayClient]({}).pipe(Stream.runCollect),
          ),
        );

        assert.equal(status.status, "missing");
        assert.deepEqual(Array.from(installEvents), [
          { type: "progress", stage: "checking" },
          { type: "progress", stage: "downloading" },
          { type: "complete", status: installedRelayClient },
        ]);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("requires relay write scope to update agent activity publication", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const preferencesUrl = yield* getHttpServerUrl("/api/connect/preferences");
      const ownerResponse = yield* fetchEffect(preferencesUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({ publishAgentActivity: true }),
      });
      const ownerBody = yield* responseJsonEffect<{
        readonly publishAgentActivity?: boolean;
      }>(ownerResponse);
      assert.equal(ownerResponse.status, 200);
      assert.equal(ownerBody.publishAgentActivity, true);

      const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: { cookie: ownerCookie },
        body: yield* HttpBody.json({}),
      });
      const credential = (yield* credentialResponse.json) as { readonly credential: string };
      const pairedCookie = yield* getAuthenticatedSessionCookieHeader(credential.credential);
      const pairedResponse = yield* fetchEffect(preferencesUrl, {
        method: "POST",
        headers: {
          cookie: pairedCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({ publishAgentActivity: false }),
      });
      const pairedBody = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly requiredScope?: string;
      }>(pairedResponse);
      assert.equal(pairedResponse.status, 403);
      assert.equal(pairedBody._tag, "EnvironmentScopeRequiredError");
      assert.equal(pairedBody.requiredScope, "relay:write");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("wakes the agent awareness relay when this server links or changes publishing", () =>
    Effect.gen(function* () {
      let catchUpRequests = 0;
      yield* buildAppUnderTest({
        layers: {
          agentAwarenessRelay: {
            requestCatchUp: () =>
              Effect.sync(() => {
                catchUpRequests += 1;
              }),
          },
        },
      });

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigResponse = yield* fetchEffect(
        yield* getHttpServerUrl("/api/connect/relay-config"),
        {
          method: "POST",
          headers: { cookie: ownerCookie, "content-type": "application/json" },
          body: jsonRequestBody({
            relayUrl: "https://relay.example.test",
            cloudUserId: "user_123",
            environmentCredential: "t3env_test_credential",
            cloudMintPublicKey: cloudKeyPair.publicKey,
            endpointRuntime: null,
          }),
        },
      );
      assert.equal(relayConfigResponse.status, 200);
      assert.equal(catchUpRequests, 1);

      const preferencesResponse = yield* fetchEffect(
        yield* getHttpServerUrl("/api/connect/preferences"),
        {
          method: "POST",
          headers: { cookie: ownerCookie, "content-type": "application/json" },
          body: jsonRequestBody({ publishAgentActivity: true }),
        },
      );
      assert.equal(preferencesResponse.status, 200);
      assert.equal(catchUpRequests, 2);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects relay config with an invalid cloud mint public key", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: "not-a-public-key",
          endpointRuntime: null,
        }),
      });
      const body = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(relayConfigResponse);

      assert.equal(relayConfigResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Cloud mint public key must be a valid Ed25519 public key.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects relay config with insecure relay metadata or empty credentials", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const postRelayConfig = (body: {
        readonly relayUrl: string;
        readonly relayIssuer?: string;
        readonly cloudUserId: string;
        readonly environmentCredential: string;
      }) =>
        fetchEffect(relayConfigUrl, {
          method: "POST",
          headers: {
            cookie: ownerCookie,
            "content-type": "application/json",
          },
          body: jsonRequestBody({
            ...body,
            cloudMintPublicKey: cloudKeyPair.publicKey,
            endpointRuntime: null,
          }),
        });

      const insecureRelayUrl = yield* postRelayConfig({
        relayUrl: "http://relay.example.test",
        cloudUserId: "user_123",
        environmentCredential: "t3env_test_credential",
      });
      const insecureRelayIssuer = yield* postRelayConfig({
        relayUrl: "https://relay.example.test",
        cloudUserId: "user_123",
        relayIssuer: "http://relay.example.test",
        environmentCredential: "t3env_test_credential",
      });
      const nonOriginRelayUrl = yield* postRelayConfig({
        relayUrl: "https://relay.example.test/path",
        cloudUserId: "user_123",
        environmentCredential: "t3env_test_credential",
      });
      const emptyCredential = yield* postRelayConfig({
        relayUrl: "https://relay.example.test",
        cloudUserId: "user_123",
        environmentCredential: "   ",
      });
      const insecureRelayUrlBody = yield* responseJsonEffect<{ readonly message?: string }>(
        insecureRelayUrl,
      );
      const insecureRelayIssuerBody = yield* responseJsonEffect<{ readonly message?: string }>(
        insecureRelayIssuer,
      );
      const nonOriginRelayUrlBody = yield* responseJsonEffect<{ readonly message?: string }>(
        nonOriginRelayUrl,
      );
      const emptyCredentialBody = yield* responseJsonEffect<{ readonly message?: string }>(
        emptyCredential,
      );

      assert.equal(insecureRelayUrl.status, 400);
      assert.equal(insecureRelayUrlBody.message, "Relay URL must be a secure absolute HTTPS URL.");
      assert.equal(insecureRelayIssuer.status, 400);
      assert.equal(
        insecureRelayIssuerBody.message,
        "Relay issuer must be a secure absolute HTTPS URL.",
      );
      assert.equal(nonOriginRelayUrl.status, 400);
      assert.equal(nonOriginRelayUrlBody.message, "Relay URL must be a secure absolute HTTPS URL.");
      assert.equal(emptyCredential.status, 400);
      assert.equal(emptyCredentialBody.message, "Relay environment credential is required.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects relay config replacement from a different cloud account", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const postRelayConfig = (cloudUserId: string, environmentCredential: string) =>
        fetchEffect(relayConfigUrl, {
          method: "POST",
          headers: {
            cookie: ownerCookie,
            "content-type": "application/json",
          },
          body: jsonRequestBody({
            relayUrl: "https://relay.example.test",
            cloudUserId,
            environmentCredential,
            cloudMintPublicKey: cloudKeyPair.publicKey,
            endpointRuntime: null,
          }),
        });

      const firstResponse = yield* postRelayConfig("user_123", "t3env_first_credential");
      const replacementResponse = yield* postRelayConfig("user_456", "t3env_second_credential");
      const replacementBody = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(replacementResponse);

      assert.equal(firstResponse.status, 200);
      assert.equal(replacementResponse.status, 409);
      assert.equal(replacementBody._tag, "EnvironmentHttpConflictError");
      assert.equal(
        replacementBody.message,
        "This environment is already linked to a different cloud account. Unlink it before switching accounts.",
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects a non-Cloudflare managed endpoint runtime without persisting the link", () =>
    Effect.gen(function* () {
      const appliedRuntimeConfigs: Array<unknown> = [];
      yield* buildAppUnderTest({
        layers: {
          cloudManagedEndpointRuntime: {
            applyConfig: (config) =>
              Effect.sync(() => {
                appliedRuntimeConfigs.push(config);
                return config === null
                  ? ({ status: "disabled" } as const)
                  : ({ status: "unsupported", providerKind: config.providerKind } as const);
              }),
          },
        },
      });

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: {
            providerKind: "manual",
            connectorToken: "manual-token",
          },
        }),
      });
      const relayConfigBody = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly endpointRuntimeStatus?: { readonly status?: string };
      }>(relayConfigResponse);
      const linkStateUrl = yield* getHttpServerUrl("/api/connect/link-state");
      const linkStateResponse = yield* fetchEffect(linkStateUrl, {
        headers: { cookie: ownerCookie },
      });
      const linkStateBody = yield* responseJsonEffect<{ readonly linked?: boolean }>(
        linkStateResponse,
      );

      assert.equal(relayConfigResponse.status, 503);
      assert.equal(relayConfigBody._tag, "EnvironmentCloudEndpointUnavailableError");
      assert.equal(relayConfigBody.endpointRuntimeStatus?.status, "unsupported");
      // The connector is never touched for a rejected runtime.
      assert.deepEqual(appliedRuntimeConfigs, []);
      assert.equal(linkStateResponse.status, 200);
      assert.equal(linkStateBody.linked, false);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("reports local cloud link state from persisted relay config", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const linkStateUrl = yield* getHttpServerUrl("/api/connect/link-state");
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");

      const initialResponse = yield* fetchEffect(linkStateUrl, {
        headers: {
          cookie: ownerCookie,
        },
      });
      const initialBody = yield* responseJsonEffect<{
        readonly linked?: boolean;
        readonly cloudUserId?: string | null;
      }>(initialResponse);
      assert.equal(initialResponse.status, 200);
      assert.equal(initialBody.linked, false);
      assert.equal(initialBody.cloudUserId, null);

      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://transport.example.test",
          relayIssuer: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const linkedResponse = yield* fetchEffect(linkStateUrl, {
        headers: {
          cookie: ownerCookie,
        },
      });
      const linkedBody = yield* responseJsonEffect<{
        readonly linked?: boolean;
        readonly cloudUserId?: string | null;
        readonly relayUrl?: string | null;
        readonly relayIssuer?: string | null;
      }>(linkedResponse);

      assert.equal(linkedResponse.status, 200);
      assert.equal(linkedBody.linked, true);
      assert.equal(linkedBody.cloudUserId, "user_123");
      assert.equal(linkedBody.relayUrl, "https://transport.example.test");
      assert.equal(linkedBody.relayIssuer, "https://relay.example.test");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("does not expose internal cloud reconciliation over HTTP", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const reconcileUrl = yield* getHttpServerUrl("/api/connect/reconcile");
      const response = yield* fetchEffect(reconcileUrl, {
        method: "POST",
      });

      assert.equal(response.status, 404);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("unlinks local cloud state and disables the managed endpoint runtime", () =>
    Effect.gen(function* () {
      const appliedRuntimeConfigs: Array<unknown> = [];
      const requestedRecoveryConfigs: Array<unknown> = [];
      yield* buildAppUnderTest({
        layers: {
          cloudManagedEndpointRuntime: {
            applyConfig: (config) => {
              appliedRuntimeConfigs.push(config);
              if (!config) {
                return Effect.succeed({ status: "disabled" });
              }
              return Effect.succeed({
                status: "running",
                providerKind: "cloudflare_tunnel",
                pid: 123,
                ...(config.tunnelId ? { tunnelId: config.tunnelId } : {}),
                ...(config.tunnelName ? { tunnelName: config.tunnelName } : {}),
              });
            },
            requestRecovery: (config) =>
              Effect.sync(() => {
                requestedRecoveryConfigs.push(config);
              }),
          },
          httpClient: HttpClient.make((request) =>
            Effect.succeed(HttpClientResponse.fromWeb(request, Response.json({ status: "ready" }))),
          ),
        },
      });

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const unlinkUrl = yield* getHttpServerUrl("/api/connect/unlink");
      const linkStateUrl = yield* getHttpServerUrl("/api/connect/link-state");

      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://transport.example.test",
          relayIssuer: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: {
            providerKind: "cloudflare_tunnel",
            connectorToken: "connector-token",
            tunnelId: "tunnel-id",
            tunnelName: "tunnel-name",
          },
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const unlinkResponse = yield* fetchEffect(unlinkUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
        },
      });
      const unlinkBody = yield* responseJsonEffect<{
        readonly ok?: boolean;
        readonly endpointRuntimeStatus?: { readonly status?: string };
      }>(unlinkResponse);
      assert.equal(unlinkResponse.status, 200);
      assert.equal(unlinkBody.ok, true);
      assert.equal(unlinkBody.endpointRuntimeStatus?.status, "disabled");

      const linkStateResponse = yield* fetchEffect(linkStateUrl, {
        headers: {
          cookie: ownerCookie,
        },
      });
      const linkStateBody = yield* responseJsonEffect<{
        readonly linked?: boolean;
        readonly cloudUserId?: string | null;
        readonly relayUrl?: string | null;
        readonly relayIssuer?: string | null;
      }>(linkStateResponse);
      assert.equal(linkStateResponse.status, 200);
      assert.equal(linkStateBody.linked, false);
      assert.equal(linkStateBody.cloudUserId, null);
      assert.equal(linkStateBody.relayUrl, null);
      assert.equal(linkStateBody.relayIssuer, null);
      assert.deepEqual(appliedRuntimeConfigs, [
        null,
        {
          providerKind: "cloudflare_tunnel",
          connectorToken: "connector-token",
          tunnelId: "tunnel-id",
          tunnelName: "tunnel-name",
        },
        null,
      ]);
      assert.deepEqual(requestedRecoveryConfigs, []);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects replayed cloud mint requests atomically", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const request = makeCloudMintCredentialRequest({
        privateKey: cloudKeyPair.privateKey,
        environmentId: testEnvironmentDescriptor.environmentId,
        clientProofKeyThumbprint: "client-proof-key-thumbprint",
        nonce: "cloud-mint-nonce-1",
        issuedAt: DateTime.formatIso(now),
        expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
      });
      const mintUrl = yield* getHttpServerUrl("/api/connect/mint-credential");
      const postMint = () =>
        fetchEffect(mintUrl, {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: jsonRequestBody(request),
        });

      const firstResponse = yield* postMint();
      const replayResponse = yield* postMint();
      const replayBody = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(replayResponse);

      assert.equal(firstResponse.status, 200);
      assert.equal(replayResponse.status, 409);
      assert.equal(replayBody._tag, "EnvironmentHttpConflictError");
      assert.equal(replayBody.message, "Cloud mint request was already consumed.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves the documented T3 Connect mint credential endpoint", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const request = makeCloudMintCredentialRequest({
        privateKey: cloudKeyPair.privateKey,
        environmentId: testEnvironmentDescriptor.environmentId,
        clientProofKeyThumbprint: "client-proof-key-thumbprint",
        jti: "cloud-mint-jti-documented-endpoint",
        nonce: "cloud-mint-nonce-documented-endpoint",
        issuedAt: DateTime.formatIso(now),
        expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
      });
      const mintUrl = yield* getHttpServerUrl("/api/t3-connect/mint-credential");
      const response = yield* fetchEffect(mintUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(request),
      });

      assert.equal(response.status, 200);
      const body = yield* responseJsonEffect<{
        readonly credential?: string;
        readonly proof?: string;
      }>(response);
      assert.equal(typeof body.credential, "string");
      assert.equal(typeof body.proof, "string");
      assert.equal(
        decodeCompactJwtPayload<{ readonly requestNonce?: string }>(body.proof!).requestNonce,
        "cloud-mint-nonce-documented-endpoint",
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves signed T3 Connect environment health checks", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const request = makeCloudEnvironmentHealthRequest({
        privateKey: cloudKeyPair.privateKey,
        environmentId: testEnvironmentDescriptor.environmentId,
        jti: "cloud-health-jti-documented-endpoint",
        nonce: "cloud-health-nonce-documented-endpoint",
        issuedAt: DateTime.formatIso(now),
        expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
      });
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const response = yield* fetchEffect(healthUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(request),
      });

      assert.equal(response.status, 200);
      const body = yield* responseJsonEffect<{
        readonly status?: string;
        readonly descriptor?: { readonly environmentId?: string };
        readonly proof?: string;
      }>(response);
      assert.equal(body.status, "online");
      assert.equal(body.descriptor?.environmentId, testEnvironmentDescriptor.environmentId);
      assert.equal(typeof body.proof, "string");
      assert.equal(
        decodeCompactJwtPayload<{ readonly requestNonce?: string }>(body.proof!).requestNonce,
        "cloud-health-nonce-documented-endpoint",
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects replayed cloud health requests atomically", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const request = makeCloudEnvironmentHealthRequest({
        privateKey: cloudKeyPair.privateKey,
        environmentId: testEnvironmentDescriptor.environmentId,
        jti: "cloud-health-jti-replay",
        nonce: "cloud-health-nonce-replay",
        issuedAt: DateTime.formatIso(now),
        expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
      });
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const postHealth = () =>
        fetchEffect(healthUrl, {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: jsonRequestBody(request),
        });

      const firstResponse = yield* postHealth();
      const replayResponse = yield* postHealth();
      const replayBody = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(replayResponse);

      assert.equal(firstResponse.status, 200);
      assert.equal(replayResponse.status, 409);
      assert.equal(replayBody._tag, "EnvironmentHttpConflictError");
      assert.equal(replayBody.message, "Cloud health request was already consumed.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud replays by time alone once their markers can be pruned", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigResponse = yield* fetchEffect(
        yield* getHttpServerUrl("/api/connect/relay-config"),
        {
          method: "POST",
          headers: { cookie: ownerCookie, "content-type": "application/json" },
          body: jsonRequestBody({
            relayUrl: "https://relay.example.test",
            cloudUserId: "user_123",
            environmentCredential: "t3env_test_credential",
            cloudMintPublicKey: cloudKeyPair.publicKey,
            endpointRuntime: null,
          }),
        },
      );
      assert.equal(relayConfigResponse.status, 200);

      const acceptedAt = yield* DateTime.now;
      // The longest-lived proofs: `iat` at the 60 s future skew the handlers
      // allow, and the 5 minute maximum lifetime.
      const issuedAt = DateTime.add(acceptedAt, { minutes: 1 });
      const proofTimes = {
        issuedAt: DateTime.formatIso(issuedAt),
        expiresAt: DateTime.formatIso(DateTime.add(issuedAt, { minutes: 5 })),
      };
      const requests = [
        [
          "/api/t3-connect/health",
          makeCloudEnvironmentHealthRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            nonce: "cloud-health-nonce-pruned",
            ...proofTimes,
          }),
        ],
        [
          "/api/t3-connect/mint-credential",
          makeCloudMintCredentialRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            clientProofKeyThumbprint: "client-proof-key-thumbprint",
            nonce: "cloud-mint-nonce-pruned",
            ...proofTimes,
          }),
        ],
      ] as const;
      const postAll = Effect.forEach(requests, ([pathname, request]) =>
        Effect.gen(function* () {
          const response = yield* fetchEffect(yield* getHttpServerUrl(pathname), {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: jsonRequestBody(request),
          });
          return response.status;
        }),
      );

      assert.deepStrictEqual(yield* postAll, [200, 200]);
      // While the proofs are fresh, only the replay markers reject them (409).
      assert.deepStrictEqual(yield* postAll, [409, 409]);
      // Once the markers can be pruned, the time checks reject the proofs by themselves (401).
      yield* TestClock.setTime(
        acceptedAt.epochMilliseconds + Duration.toMillis(REPLAY_MARKER_MAX_AGE),
      );
      assert.deepStrictEqual(yield* postAll, [401, 401]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "validates cloud proofs against the configured relay issuer, not the transport URL",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
          privateKeyEncoding: { format: "pem", type: "pkcs8" },
          publicKeyEncoding: { format: "pem", type: "spki" },
        });
        const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
        const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
        const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
          method: "POST",
          headers: {
            cookie: ownerCookie,
            "content-type": "application/json",
          },
          body: jsonRequestBody({
            relayUrl: "https://transport.example.test",
            cloudUserId: "user_123",
            relayIssuer: "https://relay.example.test",
            environmentCredential: "t3env_test_credential",
            cloudMintPublicKey: cloudKeyPair.publicKey,
            endpointRuntime: null,
          }),
        });
        assert.equal(relayConfigResponse.status, 200);

        const now = yield* DateTime.now;
        const mintUrl = yield* getHttpServerUrl("/api/t3-connect/mint-credential");
        const postMint = (request: ReturnType<typeof makeCloudMintCredentialRequest>) =>
          fetchEffect(mintUrl, {
            method: "POST",
            headers: {
              "content-type": "application/json",
            },
            body: jsonRequestBody(request),
          });

        const acceptedResponse = yield* postMint(
          makeCloudMintCredentialRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            clientProofKeyThumbprint: "client-proof-key-thumbprint",
            issuer: "https://relay.example.test",
            jti: "cloud-mint-jti-explicit-relay-issuer",
            nonce: "cloud-mint-nonce-explicit-relay-issuer",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          }),
        );
        const rejectedResponse = yield* postMint(
          makeCloudMintCredentialRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            clientProofKeyThumbprint: "client-proof-key-thumbprint",
            issuer: "https://transport.example.test",
            jti: "cloud-mint-jti-transport-url",
            nonce: "cloud-mint-nonce-transport-url",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          }),
        );

        assert.equal(acceptedResponse.status, 200);
        assert.equal(rejectedResponse.status, 401);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps a managed connector stopped when relay registration fails", () =>
    Effect.gen(function* () {
      const appliedRuntimeConfigs: Array<unknown> = [];
      const relayRequests: Array<HttpClientRequest.HttpClientRequest> = [];
      yield* buildAppUnderTest({
        layers: {
          cloudManagedEndpointRuntime: {
            applyConfig: (config) =>
              Effect.sync(() => {
                appliedRuntimeConfigs.push(config);
                return config === null
                  ? ({ status: "disabled" } as const)
                  : ({ status: "running", providerKind: "cloudflare_tunnel", pid: 123 } as const);
              }),
          },
          httpClient: HttpClient.make((request) =>
            Effect.sync(() => {
              relayRequests.push(request);
              return HttpClientResponse.fromWeb(
                request,
                Response.json({ message: "relay unavailable" }, { status: 503 }),
              );
            }),
          ),
        },
      });

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: {
            providerKind: "cloudflare_tunnel",
            connectorToken: "connector-token",
            tunnelId: "tunnel-1",
          },
        }),
      });
      const relayConfigBody = yield* responseJsonEffect<{ readonly _tag?: string }>(
        relayConfigResponse,
      );

      assert.equal(relayConfigResponse.status, 500);
      assert.equal(relayConfigBody._tag, "EnvironmentHttpInternalServerError");
      assert.equal(relayRequests.length, 3);
      assert.deepEqual(appliedRuntimeConfigs, [null]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "queues recovery without starting a connector when relay registration requires it",
    () =>
      Effect.gen(function* () {
        const appliedRuntimeConfigs: Array<unknown> = [];
        const requestedRecoveryConfigs: Array<unknown> = [];
        const relayRequests: Array<HttpClientRequest.HttpClientRequest> = [];
        yield* buildAppUnderTest({
          layers: {
            cloudManagedEndpointRuntime: {
              applyConfig: (config) =>
                Effect.sync(() => {
                  appliedRuntimeConfigs.push(config);
                  return config === null
                    ? ({ status: "disabled" } as const)
                    : ({ status: "running", providerKind: "cloudflare_tunnel", pid: 123 } as const);
                }),
              requestRecovery: (config) =>
                Effect.sync(() => {
                  requestedRecoveryConfigs.push(config);
                }),
            },
            httpClient: HttpClient.make((request) =>
              Effect.sync(() => {
                relayRequests.push(request);
                return HttpClientResponse.fromWeb(
                  request,
                  Response.json({ status: "recovery_required" }),
                );
              }),
            ),
          },
        });

        const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
          privateKeyEncoding: { format: "pem", type: "pkcs8" },
          publicKeyEncoding: { format: "pem", type: "spki" },
        });
        const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
        const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
        const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
          method: "POST",
          headers: {
            cookie: ownerCookie,
            "content-type": "application/json",
          },
          body: jsonRequestBody({
            relayUrl: "https://relay.example.test",
            cloudUserId: "user_123",
            environmentCredential: "t3env_test_credential",
            cloudMintPublicKey: cloudKeyPair.publicKey,
            endpointRuntime: {
              providerKind: "cloudflare_tunnel",
              connectorToken: "connector-token",
              tunnelId: "tunnel-1",
            },
          }),
        });
        const relayConfigBody = yield* responseJsonEffect<{
          readonly _tag?: string;
          readonly endpointRuntimeStatus?: { readonly status?: string };
        }>(relayConfigResponse);

        assert.equal(relayConfigResponse.status, 503);
        assert.equal(relayConfigBody._tag, "EnvironmentCloudEndpointUnavailableError");
        assert.equal(relayConfigBody.endpointRuntimeStatus?.status, "disabled");
        assert.equal(relayRequests.length, 1);
        assert.deepEqual(appliedRuntimeConfigs, [null]);
        assert.deepEqual(requestedRecoveryConfigs, [
          {
            providerKind: "cloudflare_tunnel",
            connectorToken: "connector-token",
            tunnelId: "tunnel-1",
          },
        ]);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("fails relay config when the managed endpoint connector cannot start", () =>
    Effect.gen(function* () {
      const appliedRuntimeConfigs: Array<unknown> = [];
      yield* buildAppUnderTest({
        layers: {
          cloudManagedEndpointRuntime: {
            applyConfig: (config) =>
              Effect.sync(() => {
                appliedRuntimeConfigs.push(config);
                return config === null
                  ? ({ status: "disabled" } as const)
                  : ({
                      status: "failed",
                      providerKind: "cloudflare_tunnel",
                      failure: "not-installed",
                      reason: "cloudflared missing",
                      tunnelId: "tunnel-1",
                    } as const);
              }),
          },
          httpClient: HttpClient.make((request) =>
            Effect.succeed(HttpClientResponse.fromWeb(request, Response.json({ status: "ready" }))),
          ),
        },
      });

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: {
            providerKind: "cloudflare_tunnel",
            connectorToken: "connector-token",
            tunnelId: "tunnel-1",
          },
        }),
      });

      assert.equal(relayConfigResponse.status, 503);
      const relayConfigBody = yield* responseJsonEffect<{
        _tag?: string;
        message?: string;
        endpointRuntimeStatus?: { status?: string; reason?: string };
      }>(relayConfigResponse);
      assert.equal(relayConfigBody._tag, "EnvironmentCloudEndpointUnavailableError");
      assert.equal(relayConfigBody.message, "Managed endpoint runtime could not be started.");
      assert.equal(relayConfigBody.endpointRuntimeStatus?.status, "failed");
      assert.equal(relayConfigBody.endpointRuntimeStatus?.reason, "cloudflared missing");
      assert.deepEqual(appliedRuntimeConfigs, [
        null,
        {
          providerKind: "cloudflare_tunnel",
          connectorToken: "connector-token",
          tunnelId: "tunnel-1",
        },
      ]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud mint requests with the wrong issuer or audience", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const mintUrl = yield* getHttpServerUrl("/api/connect/mint-credential");
      const postMint = (request: ReturnType<typeof makeCloudMintCredentialRequest>) =>
        fetchEffect(mintUrl, {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: jsonRequestBody(request),
        });

      const wrongIssuer = yield* postMint(
        makeCloudMintCredentialRequest({
          privateKey: cloudKeyPair.privateKey,
          environmentId: testEnvironmentDescriptor.environmentId,
          clientProofKeyThumbprint: "client-proof-key-thumbprint",
          issuer: "https://attacker.example.test",
          jti: "cloud-mint-jti-wrong-issuer",
          nonce: "cloud-mint-nonce-wrong-issuer",
          issuedAt: DateTime.formatIso(now),
          expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
        }),
      );
      const wrongAudience = yield* postMint(
        makeCloudMintCredentialRequest({
          privateKey: cloudKeyPair.privateKey,
          environmentId: testEnvironmentDescriptor.environmentId,
          clientProofKeyThumbprint: "client-proof-key-thumbprint",
          audience: "t3-env:other-environment",
          jti: "cloud-mint-jti-wrong-audience",
          nonce: "cloud-mint-nonce-wrong-audience",
          issuedAt: DateTime.formatIso(now),
          expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
        }),
      );

      assert.equal(wrongIssuer.status, 401);
      assert.equal(wrongAudience.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud mint requests for a cloud subject other than the linked user", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const mintUrl = yield* getHttpServerUrl("/api/t3-connect/mint-credential");
      const response = yield* fetchEffect(mintUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(
          makeCloudMintCredentialRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            clientProofKeyThumbprint: "client-proof-key-thumbprint",
            subject: "user_other",
            jti: "cloud-mint-jti-wrong-subject",
            nonce: "cloud-mint-nonce-wrong-subject",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          }),
        ),
      });

      assert.equal(response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud mint requests without the exact connect scope", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const mintUrl = yield* getHttpServerUrl("/api/t3-connect/mint-credential");
      const response = yield* fetchEffect(mintUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(
          makeCloudMintCredentialRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            clientProofKeyThumbprint: "client-proof-key-thumbprint",
            jti: "cloud-mint-jti-duplicate-scope",
            nonce: "cloud-mint-nonce-duplicate-scope",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
            scope: ["environment:connect", "environment:connect"],
          }),
        ),
      });

      assert.equal(response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud health requests with the wrong issuer or audience", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const postHealth = (request: ReturnType<typeof makeCloudEnvironmentHealthRequest>) =>
        fetchEffect(healthUrl, {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: jsonRequestBody(request),
        });

      const wrongIssuer = yield* postHealth(
        makeCloudEnvironmentHealthRequest({
          privateKey: cloudKeyPair.privateKey,
          environmentId: testEnvironmentDescriptor.environmentId,
          issuer: "https://attacker.example.test",
          jti: "cloud-health-jti-wrong-issuer",
          nonce: "cloud-health-nonce-wrong-issuer",
          issuedAt: DateTime.formatIso(now),
          expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
        }),
      );
      const wrongAudience = yield* postHealth(
        makeCloudEnvironmentHealthRequest({
          privateKey: cloudKeyPair.privateKey,
          environmentId: testEnvironmentDescriptor.environmentId,
          audience: "t3-env:other-environment",
          jti: "cloud-health-jti-wrong-audience",
          nonce: "cloud-health-nonce-wrong-audience",
          issuedAt: DateTime.formatIso(now),
          expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
        }),
      );

      assert.equal(wrongIssuer.status, 401);
      assert.equal(wrongAudience.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud health requests for a cloud subject other than the linked user", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const response = yield* fetchEffect(healthUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(
          makeCloudEnvironmentHealthRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            subject: "user_other",
            jti: "cloud-health-jti-wrong-subject",
            nonce: "cloud-health-nonce-wrong-subject",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          }),
        ),
      });

      assert.equal(response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud health requests without the exact status scope", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const response = yield* fetchEffect(healthUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(
          makeCloudEnvironmentHealthRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            jti: "cloud-health-jti-duplicate-scope",
            nonce: "cloud-health-nonce-duplicate-scope",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
            scope: ["environment:status", "environment:status"],
          }),
        ),
      });

      assert.equal(response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("negotiates permessage-deflate with clients that offer it", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const { cookie, url } = parseSessionCookieFromWsUrl(yield* getWsServerUrl("/ws"));
      const openSocket = (perMessageDeflate: boolean) =>
        Effect.acquireRelease(
          Effect.callback<NodeSocket.NodeWS.WebSocket, Error>((resume) => {
            const socket = new NodeSocket.NodeWS.WebSocket(url, {
              perMessageDeflate,
              ...(cookie ? { headers: { cookie } } : {}),
            });
            socket.on("open", () => resume(Effect.succeed(socket)));
            socket.on("error", (error) => resume(Effect.fail(error)));
          }),
          (socket) => Effect.sync(() => socket.close()),
        );

      const compressed = yield* openSocket(true);
      // The ws client records the negotiated extension only when the server's
      // 101 response accepted the offer.
      assert.include(compressed.extensions, "permessage-deflate");

      const plain = yield* openSocket(false);
      assert.notInclude(plain.extensions, "permessage-deflate");
    }).pipe(Effect.scoped, Effect.provide(NodeHttpServerTestWithWsDeflate)),
  );

  it.effect("issues short-lived websocket tickets for authenticated bearer sessions", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const bearerToken = yield* getAuthenticatedBearerSessionToken();
      const wsTicketUrl = yield* getHttpServerUrl("/api/auth/websocket-ticket");
      const wsTicketResponse = yield* fetchEffect(wsTicketUrl, {
        method: "POST",
        headers: {
          authorization: `Bearer ${bearerToken}`,
        },
      });
      const wsTicketBody = yield* responseJsonEffect<{
        readonly ticket: string;
        readonly expiresAt: string;
      }>(wsTicketResponse);

      assert.equal(wsTicketResponse.status, 200);
      assert.equal(typeof wsTicketBody.ticket, "string");
      assert.isTrue(wsTicketBody.ticket.length > 0);
      assert.equal(typeof wsTicketBody.expiresAt, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("does not allow management-only access tokens to operate the environment", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const { response: exchangeResponse, body: tokenBody } = yield* exchangeAccessToken(
        defaultDesktopBootstrapToken,
        { scope: "access:write" },
      );
      assert.equal(exchangeResponse.status, 200);
      assert.equal(tokenBody.scope, "access:write");
      assert.isDefined(tokenBody.access_token);

      const overbroadPairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
        },
        body: yield* HttpBody.json({}),
      });
      const overbroadPairingBody = (yield* overbroadPairingResponse.json) as {
        readonly requiredScope: string;
      };
      const pairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
        },
        body: yield* HttpBody.json({ scopes: ["access:write"] }),
      });
      const wsTicketResponse = yield* HttpClient.post("/api/auth/websocket-ticket", {
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
        },
      });
      const wsTicketBody = (yield* wsTicketResponse.json) as { readonly ticket: string };
      assert.equal(overbroadPairingResponse.status, 403);
      assert.equal(overbroadPairingBody.requiredScope, "orchestration:read");
      assert.equal(pairingResponse.status, 200);
      assert.equal(wsTicketResponse.status, 200);
      const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}&wsTicket=${encodeURIComponent(wsTicketBody.ticket)}`;
      const rpcError = yield* Effect.flip(
        Effect.scoped(withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverGetConfig]({}))),
      );
      assert.equal(rpcError._tag, "EnvironmentAuthorizationError");
      if (rpcError._tag === "EnvironmentAuthorizationError") {
        assert.equal(rpcError.requiredScope, "orchestration:read");
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("returns local unconfigured saved accounting without advertising a reader", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();
      const wsUrl = yield* getWsServerUrl("/ws");
      yield* withWsRpcClient(wsUrl, (client) =>
        Effect.gen(function* () {
          const config = yield* client[WS_METHODS.serverGetConfig]({});
          assert.isUndefined(config.environment.capabilities.savedTokenAccounting);
          const result = yield* client[WS_METHODS.serverReadTokenAccounting]({});
          assert.equal(result.state, "unavailable");
          if (result.state === "unavailable") {
            assert.equal(result.status, "unconfigured");
            assert.equal(result.reason, "reader_unconfigured");
            assert.isNull(result.configuredReportId);
          }
        }),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("denies saved accounting without read scope before reader dispatch", () =>
    Effect.gen(function* () {
      let reads = 0;
      yield* buildAppUnderTest({
        layers: {
          tokenAccounting: {
            isAvailable: Effect.succeed(true),
            read: Effect.sync(() => {
              reads += 1;
              return {
                state: "unavailable" as const,
                status: "unconfigured" as const,
                reason: "reader_unconfigured" as const,
                configuredReportId: null,
                readAt: "2026-10-02T12:00:00Z",
              };
            }),
          },
        },
      });
      const { body: tokenBody } = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
        scope: "access:write",
      });
      const ticketResponse = yield* HttpClient.post("/api/auth/websocket-ticket", {
        headers: { authorization: `Bearer ${tokenBody.access_token ?? ""}` },
      });
      const ticket = (yield* ticketResponse.json) as { readonly ticket: string };
      const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}&wsTicket=${encodeURIComponent(ticket.ticket)}`;
      const denied = yield* Effect.flip(
        Effect.scoped(
          withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverReadTokenAccounting]({})),
        ),
      );
      assert.equal(denied._tag, "EnvironmentAuthorizationError");
      assert.equal(reads, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("includes CORS headers on remote auth success responses", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const origin = crossOriginClientOrigin;
      const { response: tokenResponse, body: tokenBody } = yield* exchangeAccessToken(
        defaultDesktopBootstrapToken,
        {
          headers: { origin },
        },
      );

      assert.equal(tokenResponse.status, 200);
      assertBrowserApiCorsResponseHeaders(tokenResponse.headers);
      assert.equal(tokenBody.token_type, "Bearer");
      assert.equal(typeof tokenBody.access_token, "string");

      const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
      const sessionResponse = yield* fetchEffect(sessionUrl, {
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
          origin,
        },
      });
      const sessionBody = yield* responseJsonEffect<{
        readonly authenticated: boolean;
        readonly sessionMethod?: string;
      }>(sessionResponse);

      assert.equal(sessionResponse.status, 200);
      assertBrowserApiCorsResponseHeaders(sessionResponse.headers);
      assert.equal(sessionBody.authenticated, true);
      assert.equal(sessionBody.sessionMethod, "bearer-access-token");

      const wsTicketUrl = yield* getHttpServerUrl("/api/auth/websocket-ticket");
      const wsTicketResponse = yield* fetchEffect(wsTicketUrl, {
        method: "POST",
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
          origin,
        },
      });
      const wsTicketBody = yield* responseJsonEffect<{
        readonly ticket: string;
      }>(wsTicketResponse);

      assert.equal(wsTicketResponse.status, 200);
      assertBrowserApiCorsResponseHeaders(wsTicketResponse.headers);
      assert.equal(typeof wsTicketBody.ticket, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "responds to remote auth websocket-ticket preflight requests with authorization CORS headers",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const wsTicketUrl = yield* getHttpServerUrl("/api/auth/websocket-ticket");
        const response = yield* fetchEffect(wsTicketUrl, {
          method: "OPTIONS",
          headers: {
            origin: crossOriginClientOrigin,
            "access-control-request-method": "POST",
            "access-control-request-headers": "authorization",
          },
        });

        assert.equal(response.status, 204);
        assertBrowserApiCorsPreflightHeaders(response.headers);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("allows credentialed cloud link proof preflights from the configured dev UI", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: { devUrl: new URL(crossOriginClientOrigin) },
      });

      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const response = yield* fetchEffect(linkProofUrl, {
        method: "OPTIONS",
        headers: {
          origin: crossOriginClientOrigin,
          "access-control-request-method": "POST",
          "access-control-request-headers": "content-type",
        },
      });

      assert.equal(response.status, 204);
      assertBrowserApiCorsPreflightHeaders(response.headers, {
        origin: crossOriginClientOrigin,
        credentials: true,
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("allows configured development origins through ServerConfig", () =>
    Effect.gen(function* () {
      const tailnetOrigin = "https://host.example.ts.net";
      yield* buildAppUnderTest({
        config: {
          devUrl: new URL(crossOriginClientOrigin),
          devAllowedOrigins: [tailnetOrigin],
        },
      });

      const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
      const response = yield* fetchEffect(sessionUrl, {
        method: "OPTIONS",
        headers: {
          origin: tailnetOrigin,
          "access-control-request-method": "GET",
          "access-control-request-headers": "content-type",
        },
      });

      assert.equal(response.status, 204);
      assertBrowserApiCorsPreflightHeaders(response.headers, {
        origin: tailnetOrigin,
        credentials: true,
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  for (const desktopOrigin of ["t3code://app", "t3code-dev://app"]) {
    it.effect(`allows credentialed preflights from ${desktopOrigin} in development`, () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest({
          config: { devUrl: new URL(crossOriginClientOrigin) },
        });

        const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
        const response = yield* fetchEffect(sessionUrl, {
          method: "OPTIONS",
          headers: {
            origin: desktopOrigin,
            "access-control-request-method": "GET",
            "access-control-request-headers": "content-type",
          },
        });

        assert.equal(response.status, 204);
        assertBrowserApiCorsPreflightHeaders(response.headers, {
          origin: desktopOrigin,
          credentials: true,
        });
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
    );
  }

  it.effect("includes CORS headers on remote websocket-ticket auth failures", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const wsTicketUrl = yield* getHttpServerUrl("/api/auth/websocket-ticket");
      const response = yield* fetchEffect(wsTicketUrl, {
        method: "POST",
        headers: {
          origin: crossOriginClientOrigin,
        },
      });
      const body = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly code?: string;
        readonly reason?: string;
        readonly traceId?: string;
      }>(response);

      assert.equal(response.status, 401);
      assertBrowserApiCorsResponseHeaders(response.headers);
      assert.equal(body._tag, "EnvironmentAuthInvalidError");
      assert.equal(body.code, "auth_invalid");
      assert.equal(body.reason, "missing_credential");
      assert.equal(typeof body.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("issues authenticated one-time pairing credentials for additional clients", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const response = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
        },
        body: yield* HttpBody.json({}),
      });
      const body = (yield* response.json) as {
        readonly credential: string;
        readonly expiresAt: string;
      };

      assert.equal(response.status, 200);
      assert.equal(typeof body.credential, "string");
      assert.isTrue(body.credential.length > 0);
      assert.equal(typeof body.expiresAt, "string");

      const bootstrapResult = yield* bootstrapBrowserSession(body.credential);
      assert.equal(bootstrapResult.response.status, 200);

      const reusedResult = yield* bootstrapBrowserSession(body.credential);
      assert.equal(reusedResult.response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("issues pairing credentials for bearer sessions with access management scope", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const bearerToken = yield* getAuthenticatedBearerSessionToken();
      const response = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          authorization: `Bearer ${bearerToken}`,
        },
        body: yield* HttpBody.json({ label: "Hosted web" }),
      });
      const body = (yield* response.json) as {
        readonly credential: string;
        readonly label?: string;
      };

      assert.equal(response.status, 200);
      assert.isTrue(body.credential.length > 0);
      assert.equal(body.label, "Hosted web");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects pairing credentials with an empty scope grant", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const response = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
        },
        body: yield* HttpBody.json({ scopes: [] }),
      });
      const body = (yield* response.json) as {
        readonly code: string;
        readonly reason: string;
      };

      assert.equal(response.status, 400);
      assert.equal(body.code, "invalid_request");
      assert.equal(body.reason, "invalid_scope");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects unauthenticated pairing credential requests", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const response = yield* HttpClient.post("/api/auth/pairing-token", {
        body: yield* HttpBody.json({}),
      });
      assert.equal(response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("returns only pairing metadata to access-read HTTP sessions", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();
      const reader = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
        scope: "access:read",
      });
      assert.equal(reader.response.status, 200);
      assert.equal(reader.body.scope, "access:read");
      const createdResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: { cookie: yield* getAuthenticatedSessionCookieHeader() },
        body: yield* HttpBody.json({ label: "Synthetic phone" }),
      });
      const created = (yield* createdResponse.json) as { id: string; credential: string };
      assert.equal(createdResponse.status, 200);
      const response = yield* HttpClient.get("/api/auth/pairing-links", {
        headers: { authorization: `Bearer ${reader.body.access_token ?? ""}` },
      });
      assert.equal(response.status, 200);
      const responseText = yield* response.text;
      assert.notInclude(responseText, '"credential"');
      assert.notInclude(responseText, created.credential);
      const links = yield* responseJsonEffect<
        ReadonlyArray<{
          readonly id: string;
          readonly label?: string;
          readonly scopes: ReadonlyArray<string>;
        }>
      >(response);
      const listed = links.find((link) => link.id === created.id);
      assert.isDefined(listed);
      assert.deepInclude(listed, {
        label: "Synthetic phone",
        scopes: [...AuthStandardClientScopes],
      });

      const unauthorizedCreate = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: { authorization: `Bearer ${reader.body.access_token ?? ""}` },
        body: yield* HttpBody.json({}),
      });
      assert.equal(unauthorizedCreate.status, 403);
      const idExchange = yield* exchangeAccessToken(created.id, { scope: "terminal:operate" });
      assert.equal(idExchange.response.status, 401);
      const authorized = yield* exchangeAccessToken(created.credential, {
        scope: AuthStandardClientScopes.join(" "),
      });
      assert.equal(authorized.response.status, 200);
      assert.equal(authorized.body.scope, AuthStandardClientScopes.join(" "));
      const reused = yield* exchangeAccessToken(created.credential, { scope: "terminal:operate" });
      assert.equal(reused.response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("returns only pairing metadata in access-read WebSocket snapshots and updates", () =>
    Effect.gen(function* () {
      const changesSubscribed = yield* Deferred.make<void>();
      yield* buildAppUnderTest({
        onPairingChangesSubscribed: Deferred.succeed(changesSubscribed, undefined).pipe(
          Effect.asVoid,
        ),
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const createLink = Effect.gen(function* () {
        const response = yield* HttpClient.post("/api/auth/pairing-token", {
          headers: { cookie: ownerCookie },
          body: yield* HttpBody.json({}),
        });
        assert.equal(response.status, 200);
        return (yield* response.json) as { id: string; credential: string };
      });
      const initialLink = yield* createLink;
      const reader = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
        scope: "access:read",
      });
      assert.equal(reader.body.scope, "access:read");
      const ticketResponse = yield* HttpClient.post("/api/auth/websocket-ticket", {
        headers: { authorization: `Bearer ${reader.body.access_token ?? ""}` },
      });
      assert.equal(ticketResponse.status, 200);
      const { ticket } = (yield* ticketResponse.json) as { ticket: string };
      const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}&wsTicket=${encodeURIComponent(ticket)}`;
      const frames: string[] = [];
      yield* withWsRpcClient(
        wsUrl,
        (client) =>
          Effect.gen(function* () {
            const snapshotReceived = yield* Deferred.make<void>();
            const eventsFiber = yield* client.subscribeAuthAccess({}).pipe(
              Stream.tap((event) =>
                event.type === "snapshot"
                  ? Deferred.succeed(snapshotReceived, undefined)
                  : Effect.void,
              ),
              Stream.takeUntil((event) => event.type === "pairingLinkUpserted"),
              Stream.runCollect,
              Effect.forkChild,
            );
            yield* Deferred.await(snapshotReceived);
            yield* Deferred.await(changesSubscribed);
            const liveLink = yield* createLink;
            const events = yield* Fiber.join(eventsFiber);
            const snapshot = events.find((event) => event.type === "snapshot");
            const update = events.find((event) => event.type === "pairingLinkUpserted");
            assert.isDefined(snapshot);
            assert.isDefined(update);
            assert.isTrue(
              snapshot?.payload.pairingLinks.some((link) => link.id === initialLink.id),
            );
            assert.equal(update?.payload.id, liveLink.id);
            // Inspect the wire frames so client schema decoding cannot hide a leak.
            assert.notInclude(frames.join(""), '"credential"');
            assert.notInclude(frames.join(""), initialLink.credential);
            assert.notInclude(frames.join(""), liveLink.credential);
            const paired = yield* exchangeAccessToken(liveLink.credential, {
              scope: AuthStandardClientScopes.join(" "),
            });
            assert.equal(paired.response.status, 200);
          }),
        (frame) => frames.push(frame),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("lists and revokes pairing links for access management sessions", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const createdResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const createdBody = (yield* createdResponse.json) as {
        readonly id: string;
        readonly credential: string;
      };

      const listResponse = yield* HttpClient.get("/api/auth/pairing-links", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const listedLinks = (yield* listResponse.json) as ReadonlyArray<{
        readonly id: string;
      }>;

      const revokeResponse = yield* HttpClient.post("/api/auth/pairing-links/revoke", {
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: HttpBody.text(jsonRequestBody({ id: createdBody.id }), "application/json"),
      });
      const revokedBootstrap = yield* bootstrapBrowserSession(createdBody.credential);

      assert.equal(createdResponse.status, 200);
      assert.equal(listResponse.status, 200);
      assert.isTrue(listedLinks.some((entry) => entry.id === createdBody.id));
      assert.equal(revokeResponse.status, 200);
      assert.equal(revokedBootstrap.response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects pairing credential requests without access management scope", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
        },
        body: yield* HttpBody.json({}),
      });
      const ownerBody = (yield* ownerResponse.json) as {
        readonly credential: string;
      };
      assert.equal(ownerResponse.status, 200);

      const pairedSessionCookie = yield* getAuthenticatedSessionCookieHeader(ownerBody.credential);
      const pairedResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: pairedSessionCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const pairedBody = (yield* pairedResponse.json) as {
        readonly _tag: string;
        readonly code: string;
        readonly requiredScope: string;
        readonly traceId: string;
      };

      assert.equal(pairedResponse.status, 403);
      assert.equal(pairedBody._tag, "EnvironmentScopeRequiredError");
      assert.equal(pairedBody.code, "insufficient_scope");
      assert.equal(pairedBody.requiredScope, "access:write");
      assert.equal(typeof pairedBody.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("lists paired clients and revokes other sessions while keeping the administrator", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const pairingTokenUrl = yield* getHttpServerUrl("/api/auth/pairing-token");
      const ownerPairingResponse = yield* fetchEffect(pairingTokenUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          label: "Julius iPhone",
        }),
      });
      const ownerPairingBody = yield* responseJsonEffect<{
        readonly credential: string;
        readonly label?: string;
      }>(ownerPairingResponse);
      assert.equal(ownerPairingResponse.status, 200);
      const pairedSessionBootstrap = yield* bootstrapBrowserSession(ownerPairingBody.credential, {
        headers: {
          "user-agent":
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
        },
      });
      const pairedSessionCookie = pairedSessionBootstrap.cookie?.split(";")[0];
      assert.isDefined(pairedSessionCookie);

      const pairedSessionCookieHeader = pairedSessionCookie ?? "";
      const listBeforeResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const clientsBefore = (yield* listBeforeResponse.json) as ReadonlyArray<{
        readonly sessionId: string;
        readonly current: boolean;
        readonly client: {
          readonly label?: string;
          readonly deviceType: string;
          readonly ipAddress?: string;
          readonly os?: string;
          readonly browser?: string;
        };
      }>;
      const pairedClientBefore = clientsBefore.find((entry) => !entry.current);
      const pairedSessionId = clientsBefore.find((entry) => !entry.current)?.sessionId;

      const revokeOthersResponse = yield* HttpClient.post("/api/auth/clients/revoke-others", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const revokeOthersBody = (yield* revokeOthersResponse.json) as {
        readonly revokedCount: number;
      };

      const listAfterResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const clientsAfter = (yield* listAfterResponse.json) as ReadonlyArray<{
        readonly sessionId: string;
        readonly current: boolean;
      }>;

      const pairedClientPairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: pairedSessionCookieHeader,
        },
        body: yield* HttpBody.json({}),
      });
      const pairedClientPairingBody = (yield* pairedClientPairingResponse.json) as {
        readonly _tag: string;
        readonly code: string;
        readonly reason: string;
        readonly traceId: string;
      };

      assert.equal(listBeforeResponse.status, 200);
      assert.equal(ownerPairingBody.label, "Julius iPhone");
      assert.lengthOf(clientsBefore, 2);
      assert.isDefined(pairedSessionId);
      assert.isDefined(pairedClientBefore);
      assert.deepInclude(pairedClientBefore?.client, {
        label: "Julius iPhone",
        deviceType: "mobile",
        os: "iOS",
        browser: "Safari",
        ipAddress: "127.0.0.1",
      });
      assert.equal(revokeOthersResponse.status, 200);
      assert.equal(revokeOthersBody.revokedCount, 1);
      assert.equal(listAfterResponse.status, 200);
      assert.lengthOf(clientsAfter, 1);
      assert.equal(clientsAfter[0]?.current, true);
      assert.equal(pairedClientPairingResponse.status, 401);
      assert.equal(pairedClientPairingBody._tag, "EnvironmentAuthInvalidError");
      assert.equal(pairedClientPairingBody.code, "auth_invalid");
      assert.equal(pairedClientPairingBody.reason, "invalid_credential");
      assert.equal(typeof pairedClientPairingBody.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("separates access inventory reads from credential management writes", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const issueScopedSession = Effect.fnUntraced(function* (
        scope: "access:read" | "access:write",
      ) {
        const pairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
          headers: {
            cookie: ownerCookie,
          },
          body: yield* HttpBody.json({ scopes: [scope] }),
        });
        assert.equal(pairingResponse.status, 200);
        const pairingBody = (yield* pairingResponse.json) as {
          readonly credential: string;
        };
        return yield* getAuthenticatedSessionCookieHeader(pairingBody.credential);
      });

      const readCookie = yield* issueScopedSession("access:read");
      const readListResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: readCookie,
        },
      });
      const readWriteResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: readCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const readWriteBody = (yield* readWriteResponse.json) as {
        readonly requiredScope: string;
      };

      const writeCookie = yield* issueScopedSession("access:write");
      const writeListResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: writeCookie,
        },
      });
      const writeListBody = (yield* writeListResponse.json) as {
        readonly requiredScope: string;
      };

      assert.equal(readListResponse.status, 200);
      assert.equal(readWriteResponse.status, 403);
      assert.equal(readWriteBody.requiredScope, "access:write");
      assert.equal(writeListResponse.status, 403);
      assert.equal(writeListBody.requiredScope, "access:read");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("revokes an individual paired client session", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const pairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const pairingBody = (yield* pairingResponse.json) as {
        readonly credential: string;
      };
      const pairedSessionCookie = yield* getAuthenticatedSessionCookieHeader(
        pairingBody.credential,
      );

      const clientsResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const clients = (yield* clientsResponse.json) as ReadonlyArray<{
        readonly sessionId: string;
        readonly current: boolean;
      }>;
      const pairedSessionId = clients.find((entry) => !entry.current)?.sessionId;
      assert.isDefined(pairedSessionId);

      const revokeResponse = yield* HttpClient.post("/api/auth/clients/revoke", {
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: HttpBody.text(jsonRequestBody({ sessionId: pairedSessionId }), "application/json"),
      });
      const pairedClientPairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: pairedSessionCookie,
        },
        body: yield* HttpBody.json({}),
      });

      assert.equal(revokeResponse.status, 200);
      assert.equal(pairedClientPairingResponse.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("allows reusing the desktop bootstrap credential", () =>
    Effect.gen(function* () {
      // The desktop-bootstrap grant is delivered over trusted IPC at
      // backend launch and needs to stay claimable after a renderer
      // refresh, so it's intentionally reusable (unlike user-facing
      // one-time pairing credentials).
      yield* buildAppUnderTest();

      const first = yield* bootstrapBrowserSession();
      const second = yield* bootstrapBrowserSession();

      assert.equal(first.response.status, 200);
      assert.equal(second.response.status, 200);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("accepts websocket rpc handshake with a bootstrapped browser session cookie", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const { response: bootstrapResponse, cookie } = yield* bootstrapBrowserSession();

      assert.equal(bootstrapResponse.status, 200);
      assert.isDefined(cookie);

      const wsUrl = appendSessionCookieToWsUrl(
        yield* getWsServerUrl("/ws", { authenticated: false }),
        cookie?.split(";")[0] ?? "",
      );
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverGetConfig]({})),
      );

      assert.equal(response.environment.environmentId, testEnvironmentDescriptor.environmentId);
      assert.equal(response.auth.policy, "desktop-managed-local");
      assert.equal(response.shellResumeCompletionMarker, true);
      assert.isUndefined(response.shellRevealInFileManager);
      assert.isUndefined(response.shellRevealInFileManagerKind);
      assert.equal(response.threadResumeCompletionMarker, true);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("creates the Scratch project once and restores its folder on reuse", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      let sql: SqlClient.SqlClient | undefined;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.sync(() => {
              sql = Context.get(context, SqlClient.SqlClient);
            }),
        },
      });
      yield* withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
        Effect.gen(function* () {
          const config = yield* client[WS_METHODS.serverGetConfig]({});
          const scratchRoot = config.scratchWorkspaceRoot ?? "";
          const first = yield* client[WS_METHODS.projectsEnsureScratch]({});
          yield* fs.remove(scratchRoot, { recursive: true });
          const second = yield* client[WS_METHODS.projectsEnsureScratch]({});
          assert.isTrue(scratchRoot.endsWith("scratch"));
          assert.equal(second.projectId, first.projectId);
          assert.isTrue(yield* fs.exists(scratchRoot));
          const rows = yield* sql!<{ id: string; projectIcon: string }>`
          SELECT project_id AS id, project_icon_json AS projectIcon FROM projection_projects
          WHERE workspace_root = ${scratchRoot} AND deleted_at IS NULL
        `;
          assert.lengthOf(rows, 1);
          assert.equal(rows[0]?.id, first.projectId);
          assert.deepEqual(
            yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(rows[0]!.projectIcon),
            {
              kind: "lucide",
              name: "message-square-dashed",
              color: "gray",
            },
          );
        }),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("resolves a lost Scratch create race to the winning project", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();
      const firstClient = yield* getWsServerUrl("/ws");
      const secondClient = yield* getWsServerUrl("/ws");
      const requests = yield* Effect.all(
        [
          withWsRpcClient(firstClient, (client) => client[WS_METHODS.projectsEnsureScratch]({})),
          withWsRpcClient(secondClient, (client) => client[WS_METHODS.projectsEnsureScratch]({})),
        ],
        { concurrency: 2 },
      );
      assert.equal(requests[0]!.projectId, requests[1]!.projectId);
      // Scratch is identified by its configured root; its project title is "No project".
      const config = yield* withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
        client[WS_METHODS.serverGetConfig]({}),
      );
      assert.isString(config.scratchWorkspaceRoot);
      const cookie = yield* getAuthenticatedSessionCookieHeader();
      const response = yield* fetchEffect(yield* getHttpServerUrl("/api/projects"), {
        headers: { cookie },
      });
      const snapshot = yield* responseJsonEffect<{
        projects: ReadonlyArray<{ id: ProjectId; workspaceRoot: string }>;
      }>(response);
      assert.equal(response.status, 200);
      assert.deepEqual(
        snapshot.projects
          .filter((project) => project.workspaceRoot === config.scratchWorkspaceRoot)
          .map((project) => project.id),
        [requests[0]!.projectId],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("gives each new Scratch thread its own folder", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      let context:
        | Context.Context<ProjectService.ProjectService | ThreadManagement.ThreadManagementService>
        | undefined;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.sync(() => {
              context = services;
            }),
        },
      });
      const folders: string[] = [];
      yield* withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
        Effect.gen(function* () {
          const config = yield* client[WS_METHODS.serverGetConfig]({});
          const scratchRoot = config.scratchWorkspaceRoot ?? "";
          const project = yield* client[WS_METHODS.projectsEnsureScratch]({});
          const starts = [
            { id: "scratch-thread-a1b2c3d4", text: "Convert these PNGs to WebP, please!" },
            { id: "other-a1b2c3d4", text: "Convert these PNGs to WebP, please!" },
            { id: "../../escape", text: "Convert these PNGs to WebP, please!" },
            { id: "long-f00dcafe", text: "x".repeat(300) },
          ];
          for (const [index, start] of starts.entries()) {
            const launch = yield* client[ORCHESTRATION_V2_WS_METHODS.launchThread]({
              commandId: CommandId.make("scratch:launch:" + index),
              threadId: ThreadId.make(start.id),
              projectId: project.projectId,
              title: "New thread",
              modelSelection: defaultModelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              workspaceStrategy: { type: "root" },
              initialMessage: { text: start.text, attachments: [] },
            });
            const projection = yield* Context.get(
              context!,
              ThreadManagement.ThreadManagementService,
            ).getThreadProjection(launch.threadId);
            const folder = projection.thread.worktreePath ?? "";
            assert.equal(path.dirname(folder), scratchRoot);
            assert.isTrue(yield* fs.exists(folder));
            folders.push(folder);
          }
        }),
      );
      const names = folders.map((folder) => path.basename(folder));
      assert.match(names[0] ?? "", /^\d{4}-\d{2}-\d{2}-convert-these-pngs-to-webp-a1b2c3d4$/);
      assert.match(names[1] ?? "", /-convert-these-pngs-to-webp-othera1b2c3d4$/);
      assert.match(names[2] ?? "", /-convert-these-pngs-to-webp-escape$/);
      assert.match(names[3] ?? "", /^\d{4}-\d{2}-\d{2}-x{48}-f00dcafe$/);
      assert.equal(new Set(folders).size, 4);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("withholds Scratch when the data dir sits inside a work tree", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        layers: { vcsDriver: { isInsideWorkTree: () => Effect.succeed(true) } },
      });
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          Effect.gen(function* () {
            const config = yield* client[WS_METHODS.serverGetConfig]({});
            const ensure = yield* Effect.flip(client[WS_METHODS.projectsEnsureScratch]({}));

            assert.isUndefined(config.scratchWorkspaceRoot);
            assert.include(String(ensure.message), "not available");
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("creates a project from just a name in the projects folder", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      let projects: ProjectService.ProjectService["Service"] | undefined;
      const gitCalls: Array<string> = [];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.sync(() => {
              projects = Context.get(context, ProjectService.ProjectService);
            }),
          gitVcsDriver: {
            readConfigValue: () => Effect.succeed(null),
            execute: (input) =>
              Effect.sync(() => {
                gitCalls.push(input.args.join(" "));
                return {
                  exitCode: ChildProcessSpawner.ExitCode(0),
                  stdout: "",
                  stderr: "",
                  stdoutTruncated: false,
                  stderrTruncated: false,
                };
              }),
          },
        },
      });

      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          Effect.gen(function* () {
            const root = (yield* client[WS_METHODS.serverGetConfig]({})).newProjectsRoot ?? "";
            const result = yield* client[WS_METHODS.projectsCreateNew]({ name: "Pinball Stats" });

            assert.equal(result.workspaceRoot, path.join(root, "pinball-stats"));
            assert.isUndefined(result.commitError);
            const created = Option.getOrThrow(yield* projects!.getById(result.projectId));
            assert.equal(created.title, "Pinball Stats");
            assert.equal(created.workspaceRoot, result.workspaceRoot);
            assert.deepEqual(gitCalls, [
              "init --initial-branch=main",
              "add --force -- README.md assets/icon.svg",
              "commit --message Initial commit",
            ]);
            assert.isTrue(
              yield* fileSystem.exists(path.join(result.workspaceRoot, "assets", "icon.svg")),
            );
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("advertises the usable file manager and its reveal label", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        layers: {
          externalLauncher: {
            resolveAvailableEditors: () => Effect.succeed(["file-manager"]),
            resolveFileManagerRevealKind: () => Effect.succeed("file-explorer"),
          },
        },
      });

      const { cookie } = yield* bootstrapBrowserSession();
      const wsUrl = appendSessionCookieToWsUrl(
        yield* getWsServerUrl("/ws", { authenticated: false }),
        cookie?.split(";")[0] ?? "",
      );
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverGetConfig]({})),
      );

      assert.deepEqual(response.availableEditors, ["file-manager"]);
      assert.equal(response.shellRevealInFileManager, true);
      assert.equal(response.shellRevealInFileManagerKind, "file-explorer");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("does not block server config when editor discovery never resolves", () =>
    Effect.gen(function* () {
      const discoveryInterrupted = yield* Deferred.make<void>();
      const responseFiber = yield* resolveAvailableEditorsForConfig(
        Effect.never.pipe(
          Effect.onInterrupt(() => Deferred.succeed(discoveryInterrupted, undefined)),
        ),
      ).pipe(Effect.forkChild);

      yield* TestClock.adjust(Duration.seconds(5));

      const availableEditors = yield* Fiber.join(responseFiber);
      yield* Deferred.await(discoveryInterrupted);
      assert.deepEqual(availableEditors, []);
    }),
  );

  it.effect("does not block server config when file manager reveal discovery never resolves", () =>
    Effect.gen(function* () {
      const discoveryInterrupted = yield* Deferred.make<void>();
      const responseFiber = yield* resolveFileManagerRevealKindForConfig(
        Effect.never.pipe(
          Effect.onInterrupt(() => Deferred.succeed(discoveryInterrupted, undefined)),
        ),
      ).pipe(Effect.forkChild);

      yield* TestClock.adjust(Duration.seconds(5));

      const revealKind = yield* Fiber.join(responseFiber);
      yield* Deferred.await(discoveryInterrupted);
      assert.isUndefined(revealKind);
    }),
  );

  it.effect(
    "rejects websocket rpc handshake when a session token is only provided via query string",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const { cookie } = yield* bootstrapBrowserSession();
        assert.isDefined(cookie);
        const sessionToken = extractSessionTokenFromSetCookie(cookie ?? "");
        const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}&token=${encodeURIComponent(sessionToken)}`;

        const error = yield* Effect.flip(
          Effect.scoped(withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverGetConfig]({}))),
        );

        assert.equal(error._tag, "RpcClientError");
        assertInclude(String(error), "SocketOpenError");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "accepts websocket rpc handshake with a dedicated websocket ticket in the query string",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const bearerToken = yield* getAuthenticatedBearerSessionToken();
        const wsTicketUrl = yield* getHttpServerUrl("/api/auth/websocket-ticket");
        const wsTicketResponse = yield* fetchEffect(wsTicketUrl, {
          method: "POST",
          headers: {
            authorization: `Bearer ${bearerToken}`,
          },
        });
        const wsTicketBody = yield* responseJsonEffect<{
          readonly ticket: string;
        }>(wsTicketResponse);
        const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}&wsTicket=${encodeURIComponent(wsTicketBody.ticket)}`;

        const response = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverGetConfig]({})),
        );

        assert.equal(response.environment.environmentId, testEnvironmentDescriptor.environmentId);
        assert.equal(response.auth.policy, "desktop-managed-local");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("proxies browser OTLP trace exports through the server", () =>
    Effect.gen(function* () {
      const upstreamRequests: Array<{
        readonly body: string;
        readonly contentType: string | null;
      }> = [];
      const localTraceRecords: Array<unknown> = [];
      const payload = {
        resourceSpans: [
          {
            resource: {
              attributes: [
                {
                  key: "service.name",
                  value: { stringValue: "t3code-web" },
                },
              ],
            },
            scopeSpans: [
              {
                scope: {
                  name: "effect",
                  version: "4.0.0-beta.43",
                },
                spans: [
                  {
                    traceId: "11111111111111111111111111111111",
                    spanId: "2222222222222222",
                    parentSpanId: "3333333333333333",
                    name: "RpcClient.server.getSettings",
                    kind: 3,
                    startTimeUnixNano: "1000000",
                    endTimeUnixNano: "2000000",
                    attributes: [
                      {
                        key: "rpc.method",
                        value: { stringValue: "server.getSettings" },
                      },
                    ],
                    events: [
                      {
                        name: "http.request",
                        timeUnixNano: "1500000",
                        attributes: [
                          {
                            key: "http.status_code",
                            value: { intValue: "200" },
                          },
                        ],
                      },
                    ],
                    links: [],
                    status: {
                      code: "STATUS_CODE_OK",
                    },
                    flags: 1,
                  },
                ],
              },
            ],
          },
        ],
      };

      const collector = yield* Effect.acquireRelease(
        Effect.promise(async () => {
          const NodeHttp = await import("node:http");

          return await new Promise<{
            readonly close: () => Promise<void>;
            readonly url: string;
          }>((resolve, reject) => {
            const server = NodeHttp.createServer((request, response) => {
              const chunks: Buffer[] = [];
              request.on("data", (chunk) => {
                chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
              });
              request.on("end", () => {
                upstreamRequests.push({
                  body: Buffer.concat(chunks).toString("utf8"),
                  contentType: request.headers["content-type"] ?? null,
                });
                response.statusCode = 204;
                response.end();
              });
            });

            server.on("error", reject);
            server.listen(0, "127.0.0.1", () => {
              const address = server.address();
              if (!address || typeof address === "string") {
                reject(new Error("Expected TCP collector address"));
                return;
              }

              resolve({
                url: `http://127.0.0.1:${address.port}/v1/traces`,
                close: () =>
                  new Promise<void>((resolveClose, rejectClose) => {
                    server.close((error) => {
                      if (error) {
                        rejectClose(error);
                        return;
                      }
                      resolveClose();
                    });
                  }),
              });
            });
          });
        }),
        ({ close }) => Effect.promise(close),
      );

      yield* buildAppUnderTest({
        config: {
          otlpTracesUrl: collector.url,
        },
        layers: {
          browserTraceCollector: {
            record: (records) =>
              Effect.sync(() => {
                localTraceRecords.push(...records);
              }),
          },
        },
      });

      const response = yield* HttpClient.post("/api/observability/v1/traces", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
          "content-type": "application/json",
          origin: "http://localhost:5733",
        },
        // @effect-diagnostics-next-line preferSchemaOverJson:off
        body: HttpBody.text(JSON.stringify(payload), "application/json"),
      });

      assert.equal(response.status, 204);
      assert.equal(response.headers["access-control-allow-origin"], "*");
      assert.deepEqual(localTraceRecords, [
        {
          type: "otlp-span",
          name: "RpcClient.server.getSettings",
          traceId: "11111111111111111111111111111111",
          spanId: "2222222222222222",
          parentSpanId: "3333333333333333",
          sampled: true,
          kind: "client",
          startTimeUnixNano: "1000000",
          endTimeUnixNano: "2000000",
          durationMs: 1,
          attributes: {
            "rpc.method": "server.getSettings",
          },
          resourceAttributes: {
            "service.name": "t3code-web",
          },
          scope: {
            name: "effect",
            version: "4.0.0-beta.43",
            attributes: {},
          },
          events: [
            {
              name: "http.request",
              timeUnixNano: "1500000",
              attributes: {
                "http.status_code": "200",
              },
            },
          ],
          links: [],
          status: {
            code: "STATUS_CODE_OK",
          },
        },
      ]);
      assert.deepEqual(upstreamRequests, [
        {
          body: jsonRequestBody(payload),
          contentType: "application/json",
        },
      ]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("forwards browser OTLP traces as protobuf when the protocol is http/protobuf", () =>
    Effect.gen(function* () {
      const upstreamRequests: Array<{
        readonly body: string;
        readonly contentType: string | null;
      }> = [];
      const localTraceRecords: Array<unknown> = [];
      // Produced by effect's own tracer, so enum fields are numeric and the
      // protobuf encoder accepts them. The hand-written payload in the JSON
      // test uses enum names, which only the JSON path tolerates.
      const payload = yield* makeBrowserOtlpPayload("client.protobuf.test");

      const collector = yield* Effect.acquireRelease(
        Effect.promise(async () => {
          const NodeHttp = await import("node:http");

          return await new Promise<{
            readonly close: () => Promise<void>;
            readonly url: string;
          }>((resolve, reject) => {
            const server = NodeHttp.createServer((request, response) => {
              const chunks: Buffer[] = [];
              request.on("data", (chunk) => {
                chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
              });
              request.on("end", () => {
                upstreamRequests.push({
                  body: Buffer.concat(chunks).toString("utf8"),
                  contentType: request.headers["content-type"] ?? null,
                });
                response.statusCode = 204;
                response.end();
              });
            });

            server.on("error", reject);
            server.listen(0, "127.0.0.1", () => {
              const address = server.address();
              if (!address || typeof address === "string") {
                reject(new Error("Expected TCP collector address"));
                return;
              }

              resolve({
                url: `http://127.0.0.1:${address.port}/v1/traces`,
                close: () =>
                  new Promise<void>((resolveClose, rejectClose) => {
                    server.close((error) => {
                      if (error) {
                        rejectClose(error);
                        return;
                      }
                      resolveClose();
                    });
                  }),
              });
            });
          });
        }),
        ({ close }) => Effect.promise(close),
      );

      yield* buildAppUnderTest({
        config: {
          otlpTracesUrl: collector.url,
          otlpTracesExport: { ...DEFAULT_SIGNAL_EXPORT, protocol: "http/protobuf" },
        },
        layers: {
          browserTraceCollector: {
            record: (records) =>
              Effect.sync(() => {
                localTraceRecords.push(...records);
              }),
          },
        },
      });

      const response = yield* HttpClient.post("/api/observability/v1/traces", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
          "content-type": "application/json",
        },
        // @effect-diagnostics-next-line preferSchemaOverJson:off
        body: HttpBody.text(JSON.stringify(payload), "application/json"),
      });

      assert.equal(response.status, 204);
      // The local collector still decodes the browser's JSON before forwarding.
      assert.equal(localTraceRecords.length, 1);
      assert.equal(upstreamRequests.length, 1);
      const forwarded = upstreamRequests[0];
      assert.notEqual(forwarded, undefined);
      if (!forwarded) {
        return;
      }
      assert.equal(forwarded.contentType, "application/x-protobuf");
      // Protobuf strings are raw UTF-8, so the span and service names survive
      // the stub's utf8 decode even though the surrounding bytes don't.
      assert.notEqual(forwarded.body[0], "{");
      assert.include(forwarded.body, "client.protobuf.test");
      assert.include(forwarded.body, "t3code-web");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("responds to browser OTLP trace preflight requests with CORS headers", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const response = yield* HttpClient.options("/api/observability/v1/traces", {
        headers: {
          origin: "http://localhost:5733",
          "access-control-request-method": "POST",
          "access-control-request-headers": "content-type",
        },
      });

      assert.equal(response.status, 204);
      assert.equal(response.headers["access-control-allow-origin"], "*");
      assert.deepEqual(splitHeaderTokens(response.headers["access-control-allow-methods"]), [
        "GET",
        "OPTIONS",
        "POST",
      ]);
      assert.deepEqual(splitHeaderTokens(response.headers["access-control-allow-headers"]), [
        "authorization",
        "b3",
        "content-type",
        "dpop",
        "traceparent",
        ORCHESTRATION_PROTOCOL_HEADER,
      ]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "stores browser OTLP trace exports locally when no upstream collector is configured",
    () =>
      Effect.gen(function* () {
        const localTraceRecords: Array<unknown> = [];
        const payload = yield* makeBrowserOtlpPayload("client.test");
        const resourceSpan = payload.resourceSpans[0];
        const scopeSpan = resourceSpan?.scopeSpans[0];
        const span = scopeSpan?.spans[0];

        assert.notEqual(resourceSpan, undefined);
        assert.notEqual(scopeSpan, undefined);
        assert.notEqual(span, undefined);
        if (!resourceSpan || !scopeSpan || !span) {
          return;
        }

        yield* buildAppUnderTest({
          layers: {
            browserTraceCollector: {
              record: (records) =>
                Effect.sync(() => {
                  localTraceRecords.push(...records);
                }),
            },
          },
        });

        const response = yield* HttpClient.post("/api/observability/v1/traces", {
          headers: {
            cookie: yield* getAuthenticatedSessionCookieHeader(),
            "content-type": "application/json",
          },
          // @effect-diagnostics-next-line preferSchemaOverJson:off
          body: HttpBody.text(JSON.stringify(payload), "application/json"),
        });

        assert.equal(response.status, 204);
        assert.equal(localTraceRecords.length, 1);
        const record = localTraceRecords[0] as {
          readonly type: string;
          readonly name: string;
          readonly traceId: string;
          readonly spanId: string;
          readonly kind: string;
          readonly attributes: Readonly<Record<string, unknown>>;
          readonly events: ReadonlyArray<unknown>;
          readonly links: ReadonlyArray<unknown>;
          readonly scope: {
            readonly name?: string;
            readonly attributes: Readonly<Record<string, unknown>>;
          };
          readonly resourceAttributes: Readonly<Record<string, unknown>>;
          readonly status?: {
            readonly code?: string;
          };
        };

        assert.equal(record.type, "otlp-span");
        assert.equal(record.name, span.name);
        assert.equal(record.traceId, span.traceId);
        assert.equal(record.spanId, span.spanId);
        assert.equal(record.kind, "internal");
        assert.deepEqual(record.attributes, {});
        assert.deepEqual(record.events, []);
        assert.deepEqual(record.links, []);
        assert.equal(record.scope.name, scopeSpan.scope.name);
        assert.deepEqual(record.scope.attributes, {});
        assert.equal(record.resourceAttributes["service.name"], "t3code-web");
        assert.equal(record.status?.code, String(span.status.code));
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("does not trace browser OTLP trace exports on the server", () =>
    Effect.gen(function* () {
      const spanNames: Array<string> = [];
      const forwardedUrls: Array<string> = [];
      yield* buildAppUnderTest({
        config: { otlpTracesUrl: "http://collector.test/v1/traces" },
        layers: {
          httpClient: HttpClient.make((request) =>
            Effect.sync(() => {
              forwardedUrls.push(request.url);
              return HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }));
            }),
          ),
        },
      }).pipe(
        Effect.provideService(
          Tracer.Tracer,
          Tracer.make({
            span: (options) => {
              spanNames.push(options.name);
              return new Tracer.NativeSpan(options);
            },
          }),
        ),
      );
      const cookie = yield* getAuthenticatedSessionCookieHeader();
      spanNames.length = 0;

      // The query string must not bring back the HTTP server span.
      for (const url of ["/api/observability/v1/traces", "/api/observability/v1/traces?x=1"]) {
        const response = yield* HttpClient.post(url, {
          headers: { cookie, "content-type": "application/json" },
          body: yield* HttpBody.json({ resourceSpans: [] }),
        });
        assert.equal(response.status, 204);
      }

      assert.deepEqual(forwardedUrls, [
        "http://collector.test/v1/traces",
        "http://collector.test/v1/traces",
      ]);
      assert.deepEqual(spanNames, []);

      // Other routes keep their HTTP server span.
      const session = yield* HttpClient.get("/api/auth/session", { headers: { cookie } });
      assert.equal(session.status, 200);
      assert.include(spanNames, "http.server GET");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc server.upsertKeybinding", () =>
    Effect.gen(function* () {
      const rule: KeybindingRule = {
        command: "terminal.toggle",
        key: "ctrl+k",
      };
      const resolved: ResolvedKeybindingRule = {
        command: "terminal.toggle",
        shortcut: {
          key: "k",
          metaKey: false,
          ctrlKey: true,
          shiftKey: false,
          altKey: false,
          modKey: true,
        },
      };

      yield* buildAppUnderTest({
        layers: {
          keybindings: {
            upsertKeybindingRule: () => Effect.succeed([resolved]),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverUpsertKeybinding](rule)),
      );

      assert.deepEqual(response.issues, []);
      assert.deepEqual(response.keybindings, [resolved]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc server.removeKeybinding", () =>
    Effect.gen(function* () {
      const rule: KeybindingRule = {
        command: "terminal.toggle",
        key: "ctrl+k",
      };
      const resolved: ResolvedKeybindingRule = {
        command: "terminal.toggle",
        shortcut: {
          key: "j",
          metaKey: false,
          ctrlKey: false,
          shiftKey: false,
          altKey: false,
          modKey: true,
        },
      };

      yield* buildAppUnderTest({
        layers: {
          keybindings: {
            removeKeybindingRule: () => Effect.succeed([resolved]),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverRemoveKeybinding](rule)),
      );

      assert.deepEqual(response.issues, []);
      assert.deepEqual(response.keybindings, [resolved]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps agent session import project failures structured over websocket rpc", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const projectId = ProjectId.make("missing-import-project");
      const wsUrl = yield* getWsServerUrl("/ws");
      const error = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.agentSessionsImport]({ projectId }).pipe(Effect.flip),
        ),
      );

      assert.equal(error._tag, "AgentSessionImportProjectNotFoundError");
      if (error._tag === "AgentSessionImportProjectNotFoundError") {
        assert.equal(error.projectId, projectId);
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("returns scanner skip counts over websocket rpc", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const codexHome = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-agent-import-rpc-codex-",
      });
      const workspaceRoot = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-agent-import-rpc-workspace-",
      });
      const transcriptDirectory = path.join(codexHome, "sessions", "2026", "08", "31");
      const transcriptPath = path.join(transcriptDirectory, "rollout-skipped.jsonl");
      yield* fileSystem.makeDirectory(transcriptDirectory, { recursive: true });
      yield* fileSystem.writeFileString(
        transcriptPath,
        encodeTestJson({
          timestamp: "2026-08-31T12:00:00.000Z",
          type: "session_meta",
          payload: { id: "rpc-skipped-session", cwd: workspaceRoot },
        }),
      );
      yield* fileSystem.utimes(transcriptPath, 0, 0);

      const projectId = ProjectId.make("agent-import-rpc-project");
      const project = {
        id: projectId,
        title: "Agent import RPC",
        workspaceRoot,
        defaultModelSelection: null,
        scripts: [],
        createdAt: "2026-08-31T12:00:00.000Z",
        updatedAt: "2026-08-31T12:00:00.000Z",
      } as const;
      yield* buildAppUnderTest({
        layers: {
          serverSettings: {
            getSettings: Effect.succeed({
              ...DEFAULT_SERVER_SETTINGS,
              providerInstances: {
                [ProviderInstanceId.make("codex")]: {
                  driver: ProviderDriverKind.make("codex"),
                  config: { homePath: codexHome },
                },
                [ProviderInstanceId.make("claudeAgent")]: {
                  driver: ProviderDriverKind.make("claudeAgent"),
                  enabled: false,
                  config: {},
                },
              },
            }),
          },
          onV2Services: (context) =>
            seedRouterProject(context, projectId, workspaceRoot).pipe(Effect.asVoid),
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const scan = yield* client[WS_METHODS.agentSessionsScan]({});
            assert.deepEqual(
              scan.candidates.map((candidate) => candidate.path),
              [workspaceRoot],
            );
            return yield* client[WS_METHODS.agentSessionsImport]({ projectId });
          }),
        ),
      );

      assert.deepEqual(result, { importedCount: 0, skippedCount: 1 });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("uploads Codex thread feedback through websocket rpc", () =>
    Effect.gen(function* () {
      const input = {
        threadId: ThreadId.make("thread-feedback"),
        reason: "The agent stopped early.",
      };
      const uploadFeedback = vi.fn<NonNullable<ProviderAdapterV2SessionRuntime["uploadFeedback"]>>(
        () => Effect.succeed({ feedbackId: "codex-thread-feedback" }),
      );
      let providerThread:
        | Parameters<
            NonNullable<ProviderAdapterV2SessionRuntime["uploadFeedback"]>
          >[0]["providerThread"]
        | undefined;
      let sessionId: ProviderSessionId | undefined;
      const getRuntime = vi.fn<ProviderSessionManager.ProviderSessionManagerV2["Service"]["get"]>(
        (id) =>
          Effect.succeed(
            id === sessionId
              ? Option.some(makeRouterFeedbackRuntime(id, uploadFeedback))
              : Option.none(),
          ),
      );
      yield* buildAppUnderTest({
        layers: {
          providerSessionManager: { get: getRuntime },
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context, input.threadId);
              sessionId = yield* seedRouterProvider(context, "ready", input.threadId);
              const records = yield* Context.get(
                context,
                ThreadManagement.ThreadManagementService,
              ).getThreadRecords(input.threadId, ["providerThreads"]);
              providerThread = records.providerThreads.at(-1);
            }),
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.providerUploadFeedback](input)),
      );

      assert.deepStrictEqual(response, { feedbackId: "codex-thread-feedback" });
      assertTrue(providerThread !== undefined);
      assert.deepStrictEqual(getRuntime.mock.calls, [[sessionId]]);
      assert.deepStrictEqual(uploadFeedback.mock.calls, [
        [{ providerThread, reason: input.reason }],
      ]);
      assert.strictEqual(providerThread?.appThreadId, input.threadId);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves absolute host media without a local thread and rejects relative media", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-host-media-" });
      const wsUrl = yield* getWsServerUrl("/ws");
      const threadId = ThreadId.make("thread-on-another-environment");

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            for (const [name, mimeType] of [
              ["screenshot.png", "image/png"],
              ["recording.mp4", "video/mp4"],
            ] as const) {
              const filePath = path.join(directory, name);
              yield* fileSystem.writeFileString(filePath, "host media bytes");
              const issued = yield* client[WS_METHODS.assetsCreateUrl]({
                resource: { _tag: "media-file", threadId, path: filePath },
              });
              const response = yield* HttpClient.get(issued.relativeUrl);
              assert.equal(response.status, 200);
              assert.equal(response.headers["content-type"], mimeType);
              assert.equal(yield* response.text, "host media bytes");

              const error = yield* client[WS_METHODS.assetsCreateUrl]({
                resource: { _tag: "media-file", threadId, path: name },
              }).pipe(Effect.flip);
              assert.equal(error._tag, "AssetWorkspaceContextNotFoundError");
            }
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves draft workspace files without a thread", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const wsUrl = yield* getWsServerUrl("/ws");
      const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-draft-media-" });
      yield* fileSystem.writeFileString(path.join(directory, "note.html"), "<p>draft</p>");

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const issued = yield* client[WS_METHODS.assetsCreateUrl]({
              resource: { _tag: "draft-workspace-file", cwd: directory, path: "note.html" },
            });
            const response = yield* HttpClient.get(issued.relativeUrl);
            assert.equal(response.status, 200);
            assert.equal(response.headers["content-type"], "text/html; charset=utf-8");
            assert.equal(yield* response.text, "<p>draft</p>");
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("uploads image bytes through a signed URL issued by websocket rpc", () =>
    Effect.gen(function* () {
      const config = yield* buildAppUnderTest();
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const wsUrl = yield* getWsServerUrl("/ws");

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const issued = yield* client[WS_METHODS.attachmentsCreateUploadUrl]({
              name: "screenshot.png",
              mimeType: "image/png",
              sizeBytes: 6,
            });
            const rejected = yield* HttpClient.post(issued.relativeUrl, {
              body: HttpBody.uint8Array(new Uint8Array([1, 2, 3]), "image/png"),
            });
            assert.equal(rejected.status, 400);

            const response = yield* HttpClient.post(issued.relativeUrl, {
              headers: { origin: crossOriginClientOrigin },
              body: HttpBody.uint8Array(new Uint8Array([1, 2, 3, 4, 5, 6]), "image/png"),
            });
            assert.equal(response.status, 204);
            assertBrowserApiCorsResponseHeaders(response.headers);

            const attachmentPath = path.join(config.attachmentsDir, `${issued.attachmentId}.png`);
            assert.isTrue(yield* fileSystem.exists(attachmentPath));

            yield* client[WS_METHODS.attachmentsDelete]({ attachmentId: issued.attachmentId });
            assert.isFalse(yield* fileSystem.exists(attachmentPath));

            const streamed = yield* client[WS_METHODS.attachmentsCreateUploadUrl]({
              name: "streamed.png",
              mimeType: "image/png",
              sizeBytes: 6,
            });
            const streamedResponse = yield* HttpClient.post(streamed.relativeUrl, {
              body: HttpBody.stream(Stream.make(new Uint8Array([1, 2, 3, 4, 5, 6])), "image/png"),
            });
            assert.equal(streamedResponse.status, 204);
            yield* client[WS_METHODS.attachmentsDelete]({ attachmentId: streamed.attachmentId });

            const uploadedFile = yield* client[WS_METHODS.attachmentsCreateUploadUrl]({
              type: "file",
              name: "report.pdf",
              mimeType: "application/pdf",
              sizeBytes: 6,
            });
            const fileResponse = yield* HttpClient.post(uploadedFile.relativeUrl, {
              body: HttpBody.stream(
                Stream.make(new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])),
                "application/pdf",
              ),
            });
            assert.equal(fileResponse.status, 204);
            const uploadedFilePath = path.join(
              config.attachmentsDir,
              `${uploadedFile.attachmentId}.pdf`,
            );
            assert.isTrue(yield* fileSystem.exists(uploadedFilePath));

            // A mint that carries the attachment's display name and mime
            // serves a real download filename and Content-Type.
            const download = yield* client[WS_METHODS.assetsCreateUrl]({
              resource: {
                _tag: "attachment",
                attachmentId: uploadedFile.attachmentId,
                fileName: "report.pdf",
                mimeType: "application/pdf",
              },
            });
            const downloadResponse = yield* HttpClient.get(download.relativeUrl);
            assert.equal(downloadResponse.status, 200);
            assert.equal(
              downloadResponse.headers["content-disposition"],
              'attachment; filename="report.pdf"',
            );
            assert.equal(downloadResponse.headers["content-type"], "application/pdf");

            // Old clients mint without name or mime and still get a download.
            const bareDownload = yield* client[WS_METHODS.assetsCreateUrl]({
              resource: { _tag: "attachment", attachmentId: uploadedFile.attachmentId },
            });
            const bareResponse = yield* HttpClient.get(bareDownload.relativeUrl);
            assert.equal(bareResponse.status, 200);
            assert.equal(bareResponse.headers["content-disposition"], "attachment");
            assert.equal(bareResponse.headers["content-type"], "application/octet-stream");

            yield* client[WS_METHODS.attachmentsDelete]({
              attachmentId: uploadedFile.attachmentId,
            });
            assert.isFalse(yield* fileSystem.exists(uploadedFilePath));
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects an over-limit chunked upload through the route without hanging", () =>
    Effect.gen(function* () {
      const config = yield* buildAppUnderTest();
      const fileSystem = yield* FileSystem.FileSystem;
      const wsUrl = yield* getWsServerUrl("/ws");

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const issued = yield* client[WS_METHODS.attachmentsCreateUploadUrl]({
              type: "file",
              name: "big.bin",
              mimeType: "application/octet-stream",
              sizeBytes: 6,
            });
            const NodeHttp = yield* Effect.promise(() => import("node:http"));
            const uploadUrl = new URL(issued.relativeUrl, yield* getHttpServerUrl());
            const status = yield* Effect.callback<number, Error>((resume) => {
              let completed = false;
              const complete = (result: Effect.Effect<number, Error>) => {
                if (completed) return;
                completed = true;
                resume(result);
              };
              const request = NodeHttp.request(
                uploadUrl,
                {
                  method: "POST",
                  headers: {
                    "content-type": "application/octet-stream",
                    "transfer-encoding": "chunked",
                  },
                },
                (response) => {
                  request.end();
                  response.resume();
                  response.once("end", () => complete(Effect.succeed(response.statusCode ?? 0)));
                  response.once("error", (error) => complete(Effect.fail(error)));
                },
              );
              request.once("error", (error) => complete(Effect.fail(error)));
              request.flushHeaders();
              request.write(new Uint8Array(4), () => {
                request.write(new Uint8Array(4));
              });

              return Effect.sync(() => request.destroy());
            });
            assert.equal(status, 400);
            assert.deepEqual(yield* fileSystem.readDirectory(config.attachmentsDir), []);
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps feedback errors structured across websocket rpc", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-feedback-failure");
      yield* buildAppUnderTest({
        layers: {
          providerSessionManager: {
            get: (sessionId) =>
              Effect.succeed(
                Option.some(
                  makeRouterFeedbackRuntime(sessionId, () =>
                    Effect.fail(
                      new ProviderAdapterProtocolError({
                        driver: ProviderDriverKind.make("codex"),
                        detail: "private provider detail",
                      }),
                    ),
                  ),
                ),
              ),
          },
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context, threadId);
              yield* seedRouterProvider(context, "ready", threadId);
            }),
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const error = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.providerUploadFeedback]({ threadId }).pipe(Effect.flip),
        ),
      );

      assert.strictEqual(error._tag, "ProviderUploadFeedbackError");
      if (error._tag === "ProviderUploadFeedbackError") {
        assert.strictEqual(error.threadId, threadId);
        assert.strictEqual(error.message, `Failed to upload feedback for thread ${threadId}.`);
        assert.isDefined(error.cause);
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("shares one preview automation broker across websocket sessions", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const wsUrl = yield* getWsServerUrl("/ws");
        const firstConnected = yield* Deferred.make<string>();
        const firstClosed = yield* Deferred.make<void>();
        const host = {
          clientId: "shared-preview-host",
          environmentId: testEnvironmentDescriptor.environmentId,
        } as const;

        yield* withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.previewAutomationConnect](host).pipe(
            Stream.tap((event) =>
              event.type === "connected"
                ? Deferred.succeed(firstConnected, event.connectionId)
                : Effect.void,
            ),
            Stream.runDrain,
            Effect.ensuring(Deferred.succeed(firstClosed, undefined)),
          ),
        ).pipe(Effect.forkScoped);

        const firstConnectionId = yield* Deferred.await(firstConnected);
        const replacementEvent = yield* withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.previewAutomationConnect](host).pipe(Stream.runHead),
        ).pipe(Effect.map(Option.getOrThrow));
        const firstStreamClosed = yield* Deferred.await(firstClosed).pipe(
          Effect.timeoutOption("2 seconds"),
        );

        assert.equal(replacementEvent.type, "connected");
        assert.notEqual(replacementEvent.connectionId, firstConnectionId);
        assert.isTrue(Option.isSome(firstStreamClosed));
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects websocket rpc handshake when session authentication is missing", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-auth-required-" });
      yield* fs.writeFileString(
        path.join(workspaceDir, "needle-file.ts"),
        "export const needle = 1;",
      );

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws", { authenticated: false });
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsSearchEntries]({
            cwd: workspaceDir,
            query: "needle",
            limit: 10,
          }),
        ).pipe(Effect.result),
      );

      assertTrue(result._tag === "Failure");
      const failureMessage = String(result.failure);
      assertTrue(
        failureMessage.includes("SocketOpenError") || failureMessage.includes("SocketCloseError"),
      );
      assertTrue(
        failureMessage.includes("Unauthorized") ||
          failureMessage.includes("An error occurred during Open"),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("provider setup lets read-only clients observe installation but not change setup", () =>
    Effect.gen(function* () {
      let installStarts = 0;
      let authCalls = 0;
      yield* buildAppUnderTest({
        layers: {
          providerInstanceRegistry: {
            getInstance: (instanceId) =>
              Effect.succeed(
                instanceId === providerSetupInstanceId ? providerSetupInstance : undefined,
              ),
          },
          antigravityInstallation: {
            start: Effect.sync(() => {
              installStarts += 1;
              return providerSetupInstallState;
            }),
            changes: Stream.succeed(providerSetupInstallState),
          },
          providerAuth: {
            start: () =>
              Effect.sync(() => {
                authCalls += 1;
                return providerSetupAuthState;
              }),
            subscribe: () =>
              Stream.fromEffect(
                Effect.sync(() => {
                  authCalls += 1;
                  return providerSetupAuthState;
                }),
              ),
          },
        },
      });
      const token = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
        scope: "orchestration:read",
      });
      assert.equal(token.response.status, 200);
      const ticketResponse = yield* HttpClient.post("/api/auth/websocket-ticket", {
        headers: { authorization: `Bearer ${token.body.access_token ?? ""}` },
      });
      const { ticket } = yield* responseJsonEffect<{ readonly ticket: string }>(ticketResponse);
      const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}&wsTicket=${encodeURIComponent(ticket)}`;
      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const observed = yield* client[WS_METHODS.providerInstallSubscribe]({
              instanceId: providerSetupInstanceId,
            }).pipe(Stream.runHead, Effect.map(Option.getOrThrow));
            assert.deepEqual(observed, providerSetupInstallState);
            const errors = [
              yield* client[WS_METHODS.providerInstallStart]({
                instanceId: providerSetupInstanceId,
              }).pipe(Effect.flip),
              yield* client[WS_METHODS.providerAuthStart]({
                instanceId: providerSetupInstanceId,
              }).pipe(Effect.flip),
              yield* client[WS_METHODS.providerAuthSubscribe]({
                instanceId: providerSetupInstanceId,
              }).pipe(Stream.runHead, Effect.flip),
            ];
            for (const error of errors) {
              assert.equal(error._tag, "EnvironmentAuthorizationError");
              if (error._tag === "EnvironmentAuthorizationError") {
                assert.equal(error.requiredScope, "orchestration:operate");
              }
            }
          }),
        ),
      );
      assert.equal(installStarts, 0);
      assert.equal(authCalls, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("provider setup binds private sign-in to the authenticated websocket session", () =>
    Effect.gen(function* () {
      const flowId = "private-sign-in-flow";
      const callbackUrl = "http://127.0.0.1:51234/?state=test-state&code=test-code";
      const waiting: ProviderAuthState = {
        ...providerSetupAuthState,
        phase: "waiting",
        flowId,
        authorizationUrl: "https://accounts.google.com/o/oauth2/v2/auth?state=test-state",
        expiresAt: "2026-09-02T00:05:00.000Z",
      };
      const calls: Array<{
        readonly operation: string;
        readonly instanceId: ProviderInstanceId;
        readonly ownerSessionId: string;
      }> = [];
      const forwardedCallbacks: string[] = [];
      const logoutInstances: ProviderInstanceId[] = [];
      let flowOwner = "";
      yield* buildAppUnderTest({
        layers: {
          providerInstanceRegistry: {
            getInstance: (id) =>
              Effect.succeed(id === providerSetupInstanceId ? providerSetupInstance : undefined),
            listInstances: Effect.succeed([providerSetupInstance]),
          },
          providerAuth: {
            start: (input, ownerSessionId) =>
              Effect.sync(() => {
                flowOwner = ownerSessionId;
                calls.push({ operation: "start", instanceId: input.instanceId, ownerSessionId });
                return waiting;
              }),
            subscribe: (input, ownerSessionId) =>
              Stream.fromEffect(
                Effect.sync(() => {
                  calls.push({
                    operation: "subscribe",
                    instanceId: input.instanceId,
                    ownerSessionId,
                  });
                  return ownerSessionId === flowOwner
                    ? waiting
                    : { ...waiting, flowId: null, authorizationUrl: null, expiresAt: null };
                }),
              ),
            complete: (input, ownerSessionId) =>
              Effect.gen(function* () {
                calls.push({ operation: "complete", instanceId: input.instanceId, ownerSessionId });
                if (ownerSessionId !== flowOwner) {
                  return yield* new ProviderSetupError({
                    instanceId: input.instanceId,
                    operation: "complete",
                    detail: "This sign-in belongs to another client.",
                  });
                }
                assert.equal(input.flowId, flowId);
                forwardedCallbacks.push(input.callbackUrl);
                return { ...waiting, phase: "verifying" as const, authorizationUrl: null };
              }),
            cancel: (input, ownerSessionId) =>
              Effect.sync(() => {
                assert.equal(input.flowId, flowId);
                calls.push({ operation: "cancel", instanceId: input.instanceId, ownerSessionId });
                return { ...providerSetupAuthState, phase: "cancelled" as const, flowId };
              }),
            logout: (input) =>
              Effect.sync(() => {
                logoutInstances.push(input.instanceId);
                return providerSetupAuthState;
              }),
          },
        },
      });
      const firstCookie = yield* getAuthenticatedSessionCookieHeader();
      const secondCookie = yield* getAuthenticatedSessionCookieHeader();
      const firstClients = yield* HttpClient.get("/api/auth/clients", {
        headers: { cookie: firstCookie },
      }).pipe(
        Effect.flatMap(
          responseJsonEffect<
            ReadonlyArray<{ readonly sessionId: string; readonly current: boolean }>
          >,
        ),
      );
      const secondClients = yield* HttpClient.get("/api/auth/clients", {
        headers: { cookie: secondCookie },
      }).pipe(
        Effect.flatMap(
          responseJsonEffect<
            ReadonlyArray<{ readonly sessionId: string; readonly current: boolean }>
          >,
        ),
      );
      const firstOwner = firstClients.find((session) => session.current)?.sessionId;
      const secondOwner = secondClients.find((session) => session.current)?.sessionId;
      assert.isString(firstOwner);
      assert.isString(secondOwner);
      assert.notEqual(firstOwner, secondOwner);
      const baseWsUrl = yield* getWsServerUrl("/ws", { authenticated: false });
      const target = {
        instanceId: providerSetupInstanceId,
        ownerSessionId: "client-supplied-owner",
      };
      yield* Effect.scoped(
        withWsRpcClient(appendSessionCookieToWsUrl(baseWsUrl, firstCookie), (client) =>
          Effect.gen(function* () {
            const started = yield* client[WS_METHODS.providerAuthStart](target);
            assert.equal(started.flowId, flowId);
            const ownState = yield* client[WS_METHODS.providerAuthSubscribe](target).pipe(
              Stream.runHead,
              Effect.map(Option.getOrThrow),
            );
            assert.equal(ownState.authorizationUrl, waiting.authorizationUrl);
            yield* Effect.scoped(
              withWsRpcClient(appendSessionCookieToWsUrl(baseWsUrl, secondCookie), (otherClient) =>
                Effect.gen(function* () {
                  const otherState = yield* otherClient[WS_METHODS.providerAuthSubscribe](
                    target,
                  ).pipe(Stream.runHead, Effect.map(Option.getOrThrow));
                  assert.isNull(otherState.authorizationUrl);
                  assert.isNull(otherState.flowId);
                  const forged = { ...target, ownerSessionId: firstOwner, flowId, callbackUrl };
                  const denied = yield* otherClient[WS_METHODS.providerAuthComplete](forged).pipe(
                    Effect.flip,
                  );
                  assert.equal(denied._tag, "ProviderSetupError");
                  assert.deepEqual(forwardedCallbacks, []);
                }),
              ),
            );
            const completed = yield* client[WS_METHODS.providerAuthComplete]({
              ...target,
              flowId,
              callbackUrl,
            });
            assert.equal(completed.phase, "verifying");
            const cancelled = yield* client[WS_METHODS.providerAuthCancel]({ ...target, flowId });
            assert.equal(cancelled.phase, "cancelled");
            const signedOut = yield* client[WS_METHODS.providerAuthLogout](target);
            assert.equal(signedOut.phase, "idle");
          }),
        ),
      );
      assert.deepEqual(forwardedCallbacks, [callbackUrl]);
      assert.deepEqual(logoutInstances, [providerSetupInstanceId]);
      assert.isTrue(calls.every((call) => call.instanceId === providerSetupInstanceId));
      assert.deepEqual(
        calls.map((call) => call.ownerSessionId),
        [firstOwner, firstOwner, secondOwner, secondOwner, firstOwner, firstOwner],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "provider setup routes installation operations and returns only safe typed errors",
    () =>
      Effect.gen(function* () {
        const calls: string[] = [];
        let state = providerSetupInstallState;
        yield* buildAppUnderTest({
          layers: {
            providerInstanceRegistry: {
              getInstance: (instanceId) =>
                Effect.succeed(
                  instanceId === providerSetupInstanceId ? providerSetupInstance : undefined,
                ),
            },
            antigravityInstallation: {
              start: Effect.sync(() => {
                calls.push("start");
                return state;
              }),
              cancel: (operationId) =>
                Effect.gen(function* () {
                  calls.push(`cancel:${operationId}`);
                  if (operationId !== state.operationId) {
                    return yield* new AntigravityInstallationError({
                      operation: "cancel",
                      detail: "This installation is no longer running.",
                      cause: new Error("Private download diagnostics."),
                    });
                  }
                  state = { ...state, phase: "cancelled" };
                  return state;
                }),
              changes: Stream.fromEffect(Effect.sync(() => state)),
            },
          },
        });
        const wsUrl = yield* getWsServerUrl("/ws");
        yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            Effect.gen(function* () {
              const unknownInstance = yield* client[WS_METHODS.providerInstallStart]({
                instanceId: ProviderInstanceId.make("unknown-instance"),
              }).pipe(Effect.flip);
              assert.equal(unknownInstance._tag, "ProviderSetupError");
              assert.deepEqual(calls, []);
              const started = yield* client[WS_METHODS.providerInstallStart]({
                instanceId: providerSetupInstanceId,
              });
              assert.deepEqual(started, providerSetupInstallState);
              const stale = yield* client[WS_METHODS.providerInstallCancel]({
                instanceId: providerSetupInstanceId,
                operationId: "old-operation",
              }).pipe(Effect.flip);
              assert.equal(stale._tag, "ProviderSetupError");
              if (stale._tag === "ProviderSetupError") {
                assert.equal(stale.instanceId, providerSetupInstanceId);
                assert.equal(stale.operation, "cancel");
                assert.equal(stale.detail, "This installation is no longer running.");
                assert.notProperty(stale, "cause");
              }
              const cancelled = yield* client[WS_METHODS.providerInstallCancel]({
                instanceId: providerSetupInstanceId,
                operationId: "install-operation",
              });
              assert.equal(cancelled.phase, "cancelled");
              const observed = yield* client[WS_METHODS.providerInstallSubscribe]({
                instanceId: providerSetupInstanceId,
              }).pipe(Stream.runHead, Effect.map(Option.getOrThrow));
              assert.deepEqual(observed, cancelled);
            }),
          ),
        );
        assert.deepEqual(calls, ["start", "cancel:old-operation", "cancel:install-operation"]);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc subscribeServerConfig streams snapshot then update", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const providers = [
        {
          instanceId: ProviderInstanceId.make("codex"),
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: "1.0.0",
          status: "ready" as const,
          auth: { status: "authenticated" as const },
          checkedAt: "2026-04-11T00:00:00.000Z",
          models: [],
          slashCommands: [],
          skills: [],
        },
      ] as const;
      const changeEvent = {
        keybindings: [],
        issues: [],
      } as const;

      yield* buildAppUnderTest({
        config: {
          otlpTracesUrl: "http://localhost:4318/v1/traces",
          otlpMetricsUrl: "http://localhost:4318/v1/metrics",
          otlpLogsUrl: "http://localhost:4318/v1/logs",
        },
        layers: {
          keybindings: {
            loadConfigState: Effect.succeed({
              keybindings: [],
              issues: [],
            }),
            streamChanges: Stream.succeed(changeEvent),
          },
          providerRegistry: {
            getProviders: Effect.succeed(providers),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const events = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.subscribeServerConfig]({}).pipe(Stream.take(2), Stream.runCollect),
        ),
      );

      const [first, second] = Array.from(events);
      assert.equal(first?.type, "snapshot");
      if (first?.type === "snapshot") {
        assert.equal(first.version, 1);
        assert.deepEqual(first.config.keybindings, []);
        assert.deepEqual(first.config.issues, []);
        assert.deepEqual(first.config.providers, providers);
        assert.equal(path.basename(first.config.observability.logsDirectoryPath), "logs");
        assert.equal(first.config.observability.localTracingEnabled, true);
        assert.equal(first.config.observability.otlpTracesUrl, "http://localhost:4318/v1/traces");
        assert.equal(first.config.observability.otlpTracesEnabled, true);
        assert.equal(first.config.observability.otlpMetricsUrl, "http://localhost:4318/v1/metrics");
        assert.equal(first.config.observability.otlpMetricsEnabled, true);
        assert.equal(first.config.observability.otlpLogsUrl, "http://localhost:4318/v1/logs");
        assert.equal(first.config.observability.otlpLogsEnabled, true);
        assert.deepEqual(first.config.settings, DEFAULT_SERVER_SETTINGS);
      }
      assert.deepEqual(second, {
        version: 1,
        type: "keybindingsUpdated",
        payload: { keybindings: [], issues: [] },
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  for (const mode of ["all", "targeted", "background"] as const) {
    it.effect(`provider refresh invalidates T3 caches before probing (${mode})`, () => {
      const driver = ProviderDriverKind.make("codex");
      const instanceIds = [ProviderInstanceId.make("codex"), ProviderInstanceId.make("codex_work")];
      const packageNames = ["@example/personal", "@example/work"];
      const versionCache = new Map(
        packageNames.map((name) => [
          name,
          {
            expiresAt: Number.MAX_SAFE_INTEGER,
            version: "1.0.0",
          },
        ]),
      );
      const invalidated: string[] = [];
      const freshMaintenance: string[] = [];
      let manifestRefreshed = false;
      let probed = false;
      const instances = instanceIds.map(
        (instanceId, index) =>
          ({
            instanceId,
            driverKind: driver,
            continuationIdentity: { driverKind: driver, continuationKey: instanceId },
            displayName: undefined,
            enabled: true,
            invalidateCaches: Effect.sync(() => {
              invalidated.push(instanceId);
            }),
            snapshot: {
              resolveMaintenance: (options) =>
                Effect.sync(() => {
                  assert.isTrue(options?.fresh);
                  freshMaintenance.push(instanceId);
                  return makeManualOnlyProviderMaintenanceCapabilities({
                    provider: driver,
                    packageName: packageNames[index]!,
                  });
                }),
              getSnapshot: Effect.never,
              refresh: Effect.never,
              streamChanges: Stream.empty,
              applyUsageLimits: () => Effect.void,
            },
            get orchestrationAdapter(): never {
              throw new Error("Maintenance refresh must not open a provider runtime");
            },
            get textGeneration(): never {
              throw new Error("Maintenance refresh must not generate provider text");
            },
          }) satisfies ProviderInstance,
      );
      const expected =
        mode === "background" ? [] : mode === "targeted" ? [instanceIds[1]!] : instanceIds;
      const probe = Effect.sync(() => {
        probed = true;
        assert.equal(manifestRefreshed, mode !== "background");
        assert.deepEqual(invalidated.toSorted(), expected.toSorted());
        assert.deepEqual(freshMaintenance.toSorted(), expected.toSorted());
        for (let index = 0; index < instanceIds.length; index++) {
          assert.equal(
            versionCache.has(packageNames[index]!),
            !expected.includes(instanceIds[index]!),
          );
        }
        return [];
      });
      return Effect.gen(function* () {
        yield* buildAppUnderTest({
          layers: {
            modelManifest: {
              forceRefresh: Effect.sync(() => {
                manifestRefreshed = true;
                return ModelManifest.BUNDLED_MODEL_MANIFEST;
              }),
            },
            providerInstanceRegistry: { listInstances: Effect.succeed(instances) },
            providerRegistry: { refresh: () => probe, refreshInstance: () => probe },
          },
        });
        const wsUrl = yield* getWsServerUrl("/ws");
        yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.serverRefreshProviders]({
              ...(mode === "targeted" ? { instanceId: instanceIds[1]! } : {}),
              ...(mode !== "background" ? { refreshModels: true } : {}),
            }),
          ),
        );
        assert.isTrue(probed);
      }).pipe(
        Effect.provideService(ProviderVersionCache, versionCache),
        Effect.provide(NodeHttpServer.layerTest),
      );
    });
  }

  it.effect("serves config on reconnect without starting provider probes", () =>
    Effect.gen(function* () {
      const refresh = vi.fn(() => Effect.never);
      yield* buildAppUnderTest({
        layers: { providerRegistry: { refresh } },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      for (let connection = 0; connection < 2; connection += 1) {
        const event = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.subscribeServerConfig]({}).pipe(
              Stream.runHead,
              Effect.map(Option.getOrThrow),
            ),
          ),
        );
        assert.equal(event.type, "snapshot");
      }
      assert.equal(refresh.mock.calls.length, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("returns cached whole-host resources over websocket", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();
      const wsUrl = yield* getWsServerUrl("/ws");
      const [first, second] = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.all(
            [
              client[WS_METHODS.serverGetHostResources]({}),
              client[WS_METHODS.serverGetHostResources]({}),
            ],
            { concurrency: "unbounded" },
          ),
        ),
      );
      assert.deepEqual(first, second);
      assert.isAtLeast(first.sampledAt, 0);
      assert.isAbove(first.cpuCount, 0);
      assert.isAbove(first.totalMemoryBytes, 0);
      assert.isAtLeast(first.availableMemoryBytes, 0);
      assert.isAtMost(first.availableMemoryBytes, first.totalMemoryBytes);
      if (first.cpuUtilization !== null) {
        assert.isAtLeast(first.cpuUtilization, 0);
        assert.isAtMost(first.cpuUtilization, 1);
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("counts macOS reclaimable memory once and shares concurrent samples", () =>
    Effect.gen(function* () {
      const commandCalls = yield* Ref.make(0);
      const hostResources = yield* Layer.build(HostResources.layer).pipe(
        Effect.map((context) => Context.get(context, HostResources.HostResources)),
        Effect.provideService(HostProcessPlatform, "darwin"),
        Effect.provide(
          Layer.mock(ChildProcessSpawner.ChildProcessSpawner)({
            string: () =>
              Ref.update(commandCalls, (count) => count + 1).pipe(
                Effect.as(
                  "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n" +
                    "Pages free: 10.\nPages inactive: 20.\nPages speculative: 5.\n" +
                    "Pages purgeable: 999.\n",
                ),
              ),
          }),
        ),
      );
      const [first, second] = yield* Effect.all([hostResources.read, hostResources.read], {
        concurrency: "unbounded",
      });
      assert.equal(first.availableMemoryBytes, 35 * 16384);
      assert.deepEqual(first, second);
      assert.deepEqual(yield* hostResources.read, first);
      assert.equal(yield* Ref.get(commandCalls), 1);
    }).pipe(TestClock.withLive),
  );

  it.effect("retries host sampling immediately after its caller is interrupted", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const commandCalls = yield* Ref.make(0);
      const hostResources = yield* Layer.build(HostResources.layer).pipe(
        Effect.map((context) => Context.get(context, HostResources.HostResources)),
        Effect.provideService(HostProcessPlatform, "darwin"),
        Effect.provide(
          Layer.mock(ChildProcessSpawner.ChildProcessSpawner)({
            string: () =>
              Effect.gen(function* () {
                const call = yield* Ref.updateAndGet(commandCalls, (count) => count + 1);
                if (call === 1) {
                  yield* Deferred.succeed(started, undefined);
                  return yield* Effect.never;
                }
                return (
                  "Mach Virtual Memory Statistics: (page size of 4096 bytes)\n" +
                  "Pages free: 10.\nPages inactive: 20.\nPages speculative: 5.\n"
                );
              }),
          }),
        ),
      );
      const firstRead = yield* hostResources.read.pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(firstRead);
      const recovered = yield* hostResources.read;
      assert.equal(recovered.availableMemoryBytes, 35 * 4096);
      assert.equal(yield* Ref.get(commandCalls), 2);
    }).pipe(TestClock.withLive),
  );

  it.effect("routes websocket resource telemetry through the subscription", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const snapshot = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.subscribeResourceTelemetry]({}).pipe(Stream.runHead),
        ),
      );

      assertTrue(Option.isSome(snapshot));
      assert.equal(snapshot.value.processes.length, 0);
      assert.equal(snapshot.value.groups.backend.processCount, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  // An already-shipped client decodes this stream against an event union
  // without environmentThemesUpdated, so an ungated emit would kill its whole
  // config subscription. Opting in is the only way to receive them.
  it.effect("subscribeServerConfig sends published themes to an opt-in subscriber", () =>
    Effect.gen(function* () {
      const themes = [
        {
          id: "nightfall",
          name: "Nightfall",
          appearance: "dark" as const,
          canvas: "#1a1b26",
          accent: "#7aa2f7",
        },
      ] as const;

      yield* buildAppUnderTest({
        layers: {
          environmentTheme: {
            current: Effect.succeed(themes),
            streamChanges: Stream.succeed(themes),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const events = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.subscribeServerConfig]({ environmentThemes: true }).pipe(
            Stream.take(2),
            Stream.runCollect,
          ),
        ),
      );

      const [first, second] = Array.from(events);
      assert.equal(first?.type, "snapshot");
      // Not in the snapshot as well, or every opt-in client receives the same
      // array twice on every connect.
      if (first?.type === "snapshot") assert.equal(first.config.environmentThemes, undefined);
      assert.equal(second?.type, "environmentThemesUpdated");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeServerConfig withholds published themes from other subscribers", () =>
    Effect.gen(function* () {
      const themes = [
        {
          id: "nightfall",
          name: "Nightfall",
          appearance: "dark" as const,
          canvas: "#1a1b26",
          accent: "#7aa2f7",
        },
      ] as const;

      yield* buildAppUnderTest({
        layers: {
          environmentTheme: {
            current: Effect.succeed(themes),
            streamChanges: Stream.succeed(themes),
          },
          providerRegistry: { streamChanges: Stream.empty },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const events = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.subscribeServerConfig]({}).pipe(Stream.take(1), Stream.runCollect),
        ),
      );

      const first = Array.from(events)[0];
      assert.equal(first?.type, "snapshot");
      if (first?.type === "snapshot") assert.equal(first.config.environmentThemes, undefined);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect.each([false, true])(
    "routes websocket rpc subscribeServerConfig emits provider status updates (limits: %s)",
    (hasLimits) =>
      Effect.gen(function* () {
        const nextProviders = [
          {
            instanceId: ProviderInstanceId.make("codex"),
            driver: ProviderDriverKind.make("codex"),
            enabled: true,
            installed: true,
            version: "1.0.0",
            status: "ready" as const,
            auth: { status: "authenticated" as const },
            checkedAt: "2026-04-11T00:00:00.000Z",
            models: [],
            slashCommands: [],
            skills: [],
            ...(hasLimits
              ? {
                  usageLimits: {
                    checkedAt: "2026-04-11T00:00:00.000Z",
                    windows: [
                      { id: "weekly", kind: "weekly" as const, label: "Weekly", usedPercent: 25 },
                    ],
                  },
                }
              : {}),
          },
        ] as const;

        yield* buildAppUnderTest({
          layers: {
            keybindings: {
              loadConfigState: Effect.succeed({
                keybindings: [],
                issues: [],
              }),
              streamChanges: Stream.empty,
            },
            providerRegistry: {
              getProviders: Effect.succeed([]),
              streamChanges: Stream.succeed(nextProviders),
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        const events = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.subscribeServerConfig]({ usageLimitsCommand: true }).pipe(
              Stream.take(2),
              Stream.runCollect,
            ),
          ),
        );

        const [first, second] = Array.from(events);
        assert.equal(first?.type, "snapshot");
        if (first?.type === "snapshot") {
          assert.deepEqual(first.config.providers, []);
        }
        assert.deepEqual(second, {
          version: 1,
          type: "providerStatuses",
          payload: {
            providers: hasLimits
              ? [
                  {
                    ...nextProviders[0],
                    slashCommands: [
                      {
                        name: "usage-limits",
                        description: "Show this provider's usage limits",
                      },
                    ],
                  },
                ]
              : nextProviders,
          },
        });
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "routes websocket rpc subscribeServerConfig keeps the limits command from clients that do not ask for it",
    () =>
      Effect.gen(function* () {
        const codex = {
          instanceId: ProviderInstanceId.make("codex"),
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: "1.0.0",
          status: "ready" as const,
          auth: { status: "authenticated" as const },
          checkedAt: "2026-04-11T00:00:00.000Z",
          models: [],
          slashCommands: [],
          skills: [],
          usageLimits: {
            checkedAt: "2026-04-11T00:00:00.000Z",
            windows: [{ id: "weekly", kind: "weekly" as const, label: "Weekly", usedPercent: 25 }],
          },
        };
        yield* buildAppUnderTest({
          layers: {
            keybindings: {
              loadConfigState: Effect.succeed({ keybindings: [], issues: [] }),
              streamChanges: Stream.empty,
            },
            providerRegistry: {
              getProviders: Effect.succeed([codex]),
              streamChanges: Stream.succeed([{ ...codex, version: "1.0.1" }]),
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        const events = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.subscribeServerConfig]({}).pipe(Stream.take(2), Stream.runCollect),
          ),
        );

        const [first, second] = Array.from(events);
        assert.equal(first?.type, "snapshot");
        if (first?.type === "snapshot") {
          assert.deepEqual(first.config.providers, [codex]);
        }
        assert.deepEqual(second, {
          version: 1,
          type: "providerStatuses",
          payload: { providers: [{ ...codex, version: "1.0.1" }] },
        });
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "routes websocket rpc subscribeServerConfig republishes commands when only a limits source changes",
    () =>
      Effect.gen(function* () {
        const codex = {
          instanceId: ProviderInstanceId.make("codex"),
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: "1.0.0",
          status: "ready" as const,
          auth: { status: "authenticated" as const },
          checkedAt: "2026-04-11T00:00:00.000Z",
          models: [],
          slashCommands: [],
          skills: [],
        };
        const hub = {
          id: UsageLimitSourceId.make("hub"),
          kind: "cliproxy" as const,
          label: "Accounts",
          checkedAt: "2026-04-11T00:00:00.000Z",
          accounts: [
            {
              id: "work",
              driver: ProviderDriverKind.make("codex"),
              usageLimits: {
                checkedAt: "2026-04-11T00:00:00.000Z",
                windows: [
                  { id: "weekly", kind: "weekly" as const, label: "Weekly", usedPercent: 25 },
                ],
              },
            },
          ],
        };

        yield* buildAppUnderTest({
          layers: {
            keybindings: {
              loadConfigState: Effect.succeed({ keybindings: [], issues: [] }),
              streamChanges: Stream.empty,
            },
            // The registry emits no change: only the source refresh can carry it.
            providerRegistry: {
              getProviders: Effect.succeed([codex]),
              streamChanges: Stream.empty,
            },
            usageLimitSources: {
              current: Effect.succeed([]),
              // Replay the empty snapshot, then a later refresh, as the live stream does.
              streamChanges: Stream.concat(Stream.make([]), Stream.make([hub])),
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        const events = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.subscribeServerConfig]({ usageLimitsCommand: true }).pipe(
              Stream.take(2),
              Stream.runCollect,
            ),
          ),
        );

        const [first, second] = Array.from(events);
        assert.equal(first?.type, "snapshot");
        if (first?.type === "snapshot") {
          assert.deepEqual(first.config.providers, [codex]);
        }
        assert.deepEqual(second, {
          version: 1,
          type: "providerStatuses",
          payload: {
            providers: [
              {
                ...codex,
                slashCommands: [
                  { name: "usage-limits", description: "Show this provider's usage limits" },
                ],
              },
            ],
          },
        });
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "routes websocket rpc subscribeServerLifecycle replays snapshot and streams updates",
    () =>
      Effect.gen(function* () {
        const lifecycleEvents = [
          {
            version: 1 as const,
            sequence: 1,
            type: "welcome" as const,
            payload: {
              environment: testEnvironmentDescriptor,
              cwd: "/tmp/project",
              projectName: "project",
            },
          },
        ] as const;
        const liveEvents = Stream.make({
          version: 1 as const,
          sequence: 2,
          type: "ready" as const,
          payload: { at: "2026-01-01T00:00:00.000Z", environment: testEnvironmentDescriptor },
        });

        yield* buildAppUnderTest({
          layers: {
            serverLifecycleEvents: {
              snapshot: Effect.succeed({
                sequence: 1,
                events: lifecycleEvents,
              }),
              stream: liveEvents,
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        const events = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.subscribeServerLifecycle]({}).pipe(Stream.take(2), Stream.runCollect),
          ),
        );

        const [first, second] = Array.from(events);
        assert.equal(first?.type, "welcome");
        assert.equal(first?.sequence, 1);
        assert.equal(second?.type, "ready");
        assert.equal(second?.sequence, 2);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeServerLifecycle buffers updates published during snapshot capture", () =>
    Effect.gen(function* () {
      const pubsub = yield* PubSub.unbounded<ServerLifecycleStreamEvent>();
      const streamSubscribed = yield* Deferred.make<void>();
      const snapshotPublished = yield* Deferred.make<void>();
      const bootstrapProjectId = ProjectId.make("project-bootstrap");
      const bootstrapThreadId = ThreadId.make("thread-bootstrap");
      const snapshotEvent = {
        version: 1 as const,
        sequence: 1,
        type: "welcome" as const,
        payload: {
          environment: testEnvironmentDescriptor,
          cwd: "/tmp/project",
          projectName: "project",
          bootstrapStatus: "pending" as const,
        },
      };
      const gapEvent = {
        version: 1 as const,
        sequence: 2,
        type: "welcome" as const,
        payload: {
          environment: testEnvironmentDescriptor,
          cwd: "/tmp/project",
          projectName: "project",
          bootstrapStatus: "complete" as const,
          bootstrapProjectId,
          bootstrapThreadId,
          bootstrapProjectCreated: true,
          bootstrapThreadCreated: true,
        },
      };
      const sentinelEvent = {
        version: 1 as const,
        sequence: 3,
        type: "ready" as const,
        payload: { at: "2026-01-01T00:00:01.000Z", environment: testEnvironmentDescriptor },
      };
      const liveStream = Stream.unwrap(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(pubsub);
          yield* Deferred.succeed(streamSubscribed, undefined);
          return Stream.fromSubscription(subscription);
        }),
      );

      yield* buildAppUnderTest({
        layers: {
          serverLifecycleEvents: {
            snapshot: PubSub.publish(pubsub, gapEvent).pipe(
              Effect.andThen(Deferred.succeed(snapshotPublished, undefined)),
              Effect.as({ sequence: 1, events: [snapshotEvent] }),
            ),
            stream: liveStream,
          },
        },
      });

      yield* Effect.gen(function* () {
        yield* Deferred.await(snapshotPublished);
        yield* Deferred.await(streamSubscribed);
        yield* PubSub.publish(pubsub, sentinelEvent);
      }).pipe(Effect.forkScoped);

      const wsUrl = yield* getWsServerUrl("/ws");
      const events = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.subscribeServerLifecycle]({}).pipe(Stream.take(2), Stream.runCollect),
        ),
      );

      const [first, second] = Array.from(events);
      assert.equal(first?.type, "welcome");
      assert.equal(first?.sequence, 1);
      if (first?.type !== "welcome") {
        assert.fail("expected the pending bootstrap event");
      }
      assert.equal(first.payload.bootstrapStatus, "pending");
      assert.equal(second?.type, "welcome");
      assert.equal(second?.sequence, 2);
      if (second?.type !== "welcome") {
        assert.fail("expected the bootstrap completion event");
      }
      assert.equal(second.payload.bootstrapStatus, "complete");
      assert.equal(second.payload.bootstrapProjectId, bootstrapProjectId);
      assert.equal(second.payload.bootstrapThreadId, bootstrapThreadId);
      assert.equal(second.payload.bootstrapProjectCreated, true);
      assert.equal(second.payload.bootstrapThreadCreated, true);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc projects.searchEntries", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-project-search-" });
      yield* fs.writeFileString(
        path.join(workspaceDir, "needle-file.ts"),
        "export const needle = 1;",
      );

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsSearchEntries]({
            cwd: workspaceDir,
            query: "needle",
            limit: 10,
          }),
        ),
      );

      assert.isAtLeast(response.entries.length, 1);
      assert.isTrue(response.entries.some((entry) => entry.path === "needle-file.ts"));
      assert.equal(response.truncated, false);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("routes websocket rpc projects.listEntries and projects.readFile", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-project-files-" });
      yield* fs.makeDirectory(path.join(workspaceDir, "src"), { recursive: true });
      yield* fs.writeFileString(
        path.join(workspaceDir, "src", "index.ts"),
        "export const answer = 42;\n",
      );

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.all({
            listing: client[WS_METHODS.projectsListEntries]({ cwd: workspaceDir }),
            file: client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "src/index.ts",
            }),
          }),
        ),
      );

      assert.isTrue(response.listing.entries.some((entry) => entry.path === "src/index.ts"));
      assert.deepEqual(response.file, {
        relativePath: "src/index.ts",
        contents: "export const answer = 42;\n",
        byteLength: 26,
        truncated: false,
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("routes websocket rpc projects.searchEntries excludes gitignored files", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-ws-project-search-gitignored-",
      });
      yield* fs.writeFileString(path.join(workspaceDir, ".gitignore"), ".venv/\n");
      yield* fs.makeDirectory(path.join(workspaceDir, ".venv", "lib"), { recursive: true });
      yield* fs.writeFileString(
        path.join(workspaceDir, ".venv", "lib", "ignored-search-target.ts"),
        "export const ignored = true;",
      );
      yield* fs.makeDirectory(path.join(workspaceDir, "src"), { recursive: true });
      yield* fs.writeFileString(
        path.join(workspaceDir, "src", "tracked.ts"),
        "export const ok = 1;",
      );

      yield* buildAppUnderTest({
        layers: {
          vcsDriver: {
            isInsideWorkTree: () => Effect.succeed(true),
            listWorkspaceFiles: () =>
              Effect.succeed({
                paths: ["src/tracked.ts"],
                truncated: false,
                freshness: {
                  source: "live-local",
                  observedAt: TEST_EPOCH,
                  expiresAt: Option.none(),
                },
              }),
            filterIgnoredPaths: (_cwd, relativePaths) =>
              Effect.succeed(
                relativePaths.filter((relativePath) => !relativePath.startsWith(".venv/")),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsSearchEntries]({
            cwd: workspaceDir,
            query: "ignored-search-target",
            limit: 10,
          }),
        ),
      );

      assert.equal(response.entries.length, 0);
      assert.equal(response.truncated, false);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect.skipIf(!symlinksSupported)("preserves structured workspace rpc failures", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-ws-workspace-errors-",
      });
      const outsideDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-ws-workspace-errors-outside-",
      });
      const outsideFile = path.join(outsideDir, "outside.txt");
      yield* fs.writeFileString(outsideFile, "outside\n");
      yield* fs.symlink(outsideFile, path.join(workspaceDir, "linked-outside.txt"));
      const resolvedOutsideFile = yield* fs.realPath(outsideFile);

      yield* buildAppUnderTest();

      const invalidWorkspace = path.join(workspaceDir, "missing-workspace");
      const missingBrowseParent = path.join(workspaceDir, "missing-browse");
      const sensitiveQuery = "authorization: Bearer secret-token";
      const wsUrl = yield* getWsServerUrl("/ws");
      const results = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.all({
            search: client[WS_METHODS.projectsSearchEntries]({
              cwd: invalidWorkspace,
              query: sensitiveQuery,
              limit: 10,
            }).pipe(Effect.result),
            list: client[WS_METHODS.projectsListEntries]({ cwd: invalidWorkspace }).pipe(
              Effect.result,
            ),
            read: client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "linked-outside.txt",
            }).pipe(Effect.result),
            browse: client[WS_METHODS.filesystemBrowse]({
              cwd: workspaceDir,
              partialPath: "./missing-browse/child",
            }).pipe(Effect.result),
          }),
        ),
      );

      if (
        results.search._tag !== "Failure" ||
        results.search.failure._tag !== "ProjectSearchEntriesError"
      ) {
        assert.fail("Expected a ProjectSearchEntriesError");
      }
      const searchError = results.search.failure;
      assert.equal(
        searchError.message,
        `Failed to search workspace entries in '${invalidWorkspace}'.`,
      );
      assert.equal(searchError.cwd, invalidWorkspace);
      assert.equal(searchError.queryLength, sensitiveQuery.length);
      assert.notProperty(searchError, "query");
      assert.notInclude(searchError.message, "Bearer");
      assert.notInclude(searchError.message, "secret-token");
      assert.equal(searchError.limit, 10);
      assert.equal(searchError.failure, "workspace_root_not_found");
      assert.equal(searchError.normalizedCwd, invalidWorkspace);
      assert.isDefined(searchError.cause);

      if (
        results.list._tag !== "Failure" ||
        results.list.failure._tag !== "ProjectListEntriesError"
      ) {
        assert.fail("Expected a ProjectListEntriesError");
      }
      const listError = results.list.failure;
      assert.equal(listError.message, `Failed to list workspace entries in '${invalidWorkspace}'.`);
      assert.equal(listError.cwd, invalidWorkspace);
      assert.equal(listError.failure, "workspace_root_not_found");
      assert.equal(listError.normalizedCwd, invalidWorkspace);
      assert.isDefined(listError.cause);

      if (results.read._tag !== "Failure" || results.read.failure._tag !== "ProjectReadFileError") {
        assert.fail("Expected a ProjectReadFileError");
      }
      const readError = results.read.failure;
      assert.equal(
        readError.message,
        `Failed to read workspace file 'linked-outside.txt' in '${workspaceDir}'.`,
      );
      assert.equal(readError.cwd, workspaceDir);
      assert.equal(readError.relativePath, "linked-outside.txt");
      assert.equal(readError.failure, "resolved_path_outside_root");
      assert.equal(readError.resolvedPath, resolvedOutsideFile);
      assert.isDefined(readError.cause);

      if (
        results.browse._tag !== "Failure" ||
        results.browse.failure._tag !== "FilesystemBrowseError"
      ) {
        assert.fail("Expected a FilesystemBrowseError");
      }
      const browseError = results.browse.failure;
      assert.equal(
        browseError.message,
        `Failed to browse filesystem path './missing-browse/child' from '${workspaceDir}'.`,
      );
      assert.equal(browseError.cwd, workspaceDir);
      assert.equal(browseError.partialPath, "./missing-browse/child");
      assert.equal(browseError.failure, "read_directory_failed");
      assert.equal(browseError.parentPath, missingBrowseParent);
      assert.isDefined(browseError.cause);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("reports workspace root stat failures without relabeling them as missing", () =>
    Effect.gen(function* () {
      if ((yield* HostProcessPlatform) === "win32") return;

      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const blockedRoot = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-ws-workspace-stat-error-",
      });
      const workspaceRoot = path.join(blockedRoot, "workspace");
      yield* fs.makeDirectory(workspaceRoot);
      yield* fs.chmod(blockedRoot, 0o000);

      const result = yield* Effect.gen(function* () {
        yield* buildAppUnderTest();
        const wsUrl = yield* getWsServerUrl("/ws");
        return yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.projectsListEntries]({ cwd: workspaceRoot }).pipe(Effect.result),
          ),
        );
      }).pipe(Effect.ensuring(fs.chmod(blockedRoot, 0o700).pipe(Effect.ignore)));

      if (result._tag !== "Failure" || result.failure._tag !== "ProjectListEntriesError") {
        assert.fail("Expected a ProjectListEntriesError");
      }
      const error = result.failure;
      assert.equal(error.failure, "workspace_root_stat_failed");
      assert.equal(error.normalizedCwd, workspaceRoot);
      assert.equal(error.detail, "validate-existing");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc projects.writeFile", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-project-write-" });

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsWriteFile]({
            cwd: workspaceDir,
            relativePath: "nested/created.txt",
            contents: "written-by-rpc",
          }),
        ),
      );

      assert.equal(response.relativePath, "nested/created.txt");
      const persisted = yield* fs.readFileString(path.join(workspaceDir, "nested", "created.txt"));
      assert.equal(persisted, "written-by-rpc");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("creates a missing workspace root during websocket project.create dispatch", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parentDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-project-create-" });
      const missingWorkspaceRoot = path.join(parentDir, "nested", "new-project");

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsMutate]({
            type: "project.create",
            commandId: CommandId.make("cmd-project-create-missing-root"),
            projectId: ProjectId.make("project-create-missing-root"),
            title: "New Project",
            workspaceRoot: missingWorkspaceRoot,
            createWorkspaceRootIfMissing: true,
            defaultModelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-5-codex",
            },
          }),
        ),
      );
      const stat = yield* fs.stat(missingWorkspaceRoot);

      assert.equal(response.id, ProjectId.make("project-create-missing-root"));
      assert.equal(stat.type, "Directory");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("starts a project clone in the background and blocks threads until it lands", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parent = yield* fs.makeTempDirectoryScoped({ prefix: "t3-router-clone-" });
      const destinationPath = path.join(parent, "repository");
      const projectId = ProjectId.make("router:clone");
      const cloneGate = yield* Deferred.make<void>();
      const updated = yield* Deferred.make<void>();
      let projects!: ProjectService.ProjectService["Service"];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.sync(() => {
              projects = Context.get(context, ProjectService.ProjectService);
            }),
          wrapProjectService: (service) => ({
            ...service,
            update: (input) =>
              service.update(input).pipe(Effect.tap(() => Deferred.succeed(updated, undefined))),
          }),
          sourceControlRepositoryService: {
            prepareClone: (input) =>
              Effect.succeed({
                destinationPath: input.destinationPath,
                remoteUrl: input.remoteUrl ?? "",
                cloneUrl: input.remoteUrl ?? "",
                repository: null,
              }),
            cloneRepository: (input) =>
              Deferred.await(cloneGate).pipe(
                Effect.as({
                  cwd: input.destinationPath,
                  remoteUrl: input.remoteUrl ?? "",
                  repository: null,
                }),
              ),
          },
        },
      });
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          Effect.gen(function* () {
            const started = yield* client[WS_METHODS.projectCloneStart]({
              projectId,
              title: "Clone",
              createdAt: "2026-01-01T00:00:00.000Z",
              remoteUrl: "https://example.invalid/synthetic.git",
              destinationPath,
            });
            assert.equal(started.cwd, destinationPath);
            assertTrue(Option.isSome(yield* projects.getById(projectId)));
            const blocked = yield* client[ORCHESTRATION_V2_WS_METHODS.launchThread]({
              ...routerLaunchInput("clone-blocked"),
              projectId,
            }).pipe(Effect.result);
            assertTrue(blocked._tag === "Failure");
            assert.include(yield* encodeTestJsonEffect(blocked.failure), "still being cloned");
            const snapshots = yield* client[WS_METHODS.subscribeProjectClones]({}).pipe(
              Stream.takeUntil((clones) => clones[0]?.phase === "done"),
              Stream.runCollect,
              Effect.forkChild,
            );
            yield* Deferred.succeed(cloneGate, undefined);
            assert.equal((yield* Fiber.join(snapshots)).at(-1)?.[0]?.phase, "done");
            yield* Deferred.await(updated);
            const launched = yield* client[ORCHESTRATION_V2_WS_METHODS.launchThread]({
              ...routerLaunchInput("clone-ready"),
              projectId,
            });
            assert.equal(launched.projection.thread.projectId, projectId);
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("finds a cloned project's icon once the clone lands", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parent = yield* fs.makeTempDirectoryScoped({ prefix: "t3-router-clone-icon-" });
      const destinationPath = path.join(parent, "repository");
      const projectId = ProjectId.make("router:clone-icon");
      const cloneGate = yield* Deferred.make<void>();
      const updated = yield* Deferred.make<void>();
      yield* buildAppUnderTest({
        layers: {
          wrapProjectService: (service) => ({
            ...service,
            update: (input) =>
              service.update(input).pipe(Effect.tap(() => Deferred.succeed(updated, undefined))),
          }),
          sourceControlRepositoryService: {
            prepareClone: (input) =>
              Effect.succeed({
                destinationPath: input.destinationPath,
                remoteUrl: input.remoteUrl ?? "",
                cloneUrl: input.remoteUrl ?? "",
                repository: null,
              }),
            cloneRepository: (input) =>
              Deferred.await(cloneGate).pipe(
                Effect.andThen(
                  fs
                    .writeFileString(path.join(input.destinationPath, "favicon.svg"), "<svg/>")
                    .pipe(
                      Effect.mapError(
                        (cause) =>
                          new SourceControlRepositoryError({
                            operation: "cloneRepository",
                            provider: "unknown",
                            detail: "Synthetic clone fixture could not create favicon",
                            cause,
                          }),
                      ),
                    ),
                ),
                Effect.as({
                  cwd: input.destinationPath,
                  remoteUrl: input.remoteUrl ?? "",
                  repository: null,
                }),
              ),
          },
        },
      });
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          Effect.gen(function* () {
            yield* client[WS_METHODS.projectCloneStart]({
              projectId,
              title: "Clone",
              createdAt: "2026-01-01T00:00:00.000Z",
              remoteUrl: "https://example.invalid/synthetic.git",
              destinationPath,
            });
            const resource = { _tag: "project-favicon" as const, cwd: destinationPath };
            assert.isTrue(
              (yield* client[WS_METHODS.assetsCreateUrl]({ resource })).relativeUrl.endsWith(
                "/project-favicon-missing",
              ),
            );
            yield* Deferred.succeed(cloneGate, undefined);
            yield* Deferred.await(updated);
            assert.equal(
              (yield* client[WS_METHODS.assetsCreateUrl]({ resource })).sourcePath,
              "favicon.svg",
            );
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("records thread analytics only after a client command succeeds", () =>
    Effect.gen(function* () {
      const effects: string[] = [];
      const properties: Array<Readonly<Record<string, unknown>> | undefined> = [];
      let dispatch!: ThreadManagement.ThreadManagementService["Service"]["dispatch"];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              dispatch = Context.get(context, Orchestrator.OrchestratorV2).dispatch;
              yield* seedRouterThread(context);
              effects.length = 0;
            }),
          threadManagement: {
            dispatch: (command) =>
              Effect.gen(function* () {
                effects.push("dispatch:" + command.commandId);
                return yield* dispatch(command);
              }),
          },
          analyticsService: {
            record: (event, props) =>
              Effect.sync(() => {
                effects.push("analytics:" + event);
                properties.push(props);
              }),
          },
        },
      });
      const wsUrl = yield* getWsServerUrl(
        "/ws?clientSurface=mobile&clientAppVersion=1.2.3&clientDeviceType=phone&clientOs=iOS&clientOsMajorVersion=18&clientDeviceModel=iPhone+15+Pro&connectionMethod=relay",
      );
      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const failed = yield* client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
              ...routerMessageDispatch("analytics-failed"),
              threadId: ThreadId.make("missing-analytics-thread"),
            }).pipe(Effect.result);
            assert.equal(failed._tag, "Failure");
            assert.isFalse(effects.includes("analytics:client.turn.requested"));
            const accepted = yield* client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand](
              routerMessageDispatch("analytics-accepted"),
            );
            assert.isAbove(accepted.sequence, 0);
          }),
        ),
      );
      assert.deepEqual(effects, [
        "analytics:client.connected",
        "dispatch:router:message:analytics-failed",
        "dispatch:router:message:analytics-accepted",
        "analytics:client.turn.requested",
      ]);
      assert.deepEqual(properties[0], properties[1]);
      assert.equal(properties[1]?.surface, "mobile");
      assert.equal(properties[1]?.clientDeviceModel, "iPhone 15 Pro");
      assert.equal(properties[1]?.connectionMethod, "relay");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps telemetry separate for simultaneous clients", () =>
    Effect.gen(function* () {
      const analyticsEvents: Array<{
        event: string;
        properties: Readonly<Record<string, unknown>> | undefined;
      }> = [];

      yield* buildAppUnderTest({
        layers: {
          analyticsService: {
            record: (event, properties) =>
              Effect.sync(() => analyticsEvents.push({ event, properties })),
          },
          onV2Services: (context) =>
            Effect.gen(function* () {
              // Each client works in its own checkout; one local checkout has one owning thread.
              const mobileProjectId = ProjectId.make("project-mobile");
              yield* seedRouterProject(context);
              yield* seedRouterProject(context, mobileProjectId);
              const threads = Context.get(context, ThreadManagement.ThreadManagementService);
              yield* threads.dispatch(routerThreadCreate(ThreadId.make("thread-web")));
              yield* threads.dispatch(
                routerThreadCreate(ThreadId.make("thread-mobile"), mobileProjectId),
              );
            }),
        },
      });

      const webUrl = yield* getWsServerUrl(
        "/ws?clientSurface=web&clientAppVersion=2.0.0&clientDeviceType=desktop&clientOs=Windows&clientWebDeployment=hosted&clientBrowser=Chrome&connectionMethod=direct",
      );
      const mobileUrl = yield* getWsServerUrl(
        "/ws?clientSurface=mobile&clientAppVersion=3.0.0&clientDeviceType=tablet&clientOs=Android&clientOsMajorVersion=15&clientDeviceModel=Pixel+Tablet&connectionMethod=relay",
      );
      const turnCommand = (
        client: string,
      ): Extract<OrchestrationV2Command, { type: "message.dispatch" }> => ({
        ...routerMessageDispatch("telemetry-" + client),

        commandId: CommandId.make(`cmd-${client}-turn`),
        threadId: ThreadId.make(`thread-${client}`),
        messageId: MessageId.make(`message-${client}`),
        text: "hello",
        attachments: [],
        modelSelection: defaultModelSelection,
      });

      yield* Effect.scoped(
        withWsRpcClient(webUrl, (webClient) =>
          withWsRpcClient(mobileUrl, (mobileClient) =>
            Effect.gen(function* () {
              yield* mobileClient[ORCHESTRATION_V2_WS_METHODS.dispatchCommand](
                turnCommand("mobile"),
              );
              yield* webClient[ORCHESTRATION_V2_WS_METHODS.dispatchCommand](turnCommand("web"));
            }),
          ),
        ),
      );

      assert.deepEqual(
        analyticsEvents
          .filter(({ event }) => event === "client.turn.requested")
          .map(({ properties }) => properties),
        [
          {
            surface: "mobile",
            appVersion: "3.0.0",
            clientAppVersion: "3.0.0",
            clientOs: "Android",
            os: "Android",
            clientDeviceType: "tablet",
            osMajorVersion: 15,
            clientOsMajorVersion: 15,
            deviceModel: "Pixel Tablet",
            clientDeviceModel: "Pixel Tablet",
            connectionMethod: "relay",
          },
          {
            surface: "web",
            appVersion: "2.0.0",
            clientAppVersion: "2.0.0",
            clientOs: "Windows",
            clientDeviceType: "desktop",
            webDeployment: "hosted",
            clientBrowser: "Chrome",
            connectionMethod: "direct",
          },
        ],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("ignores invalid client telemetry without rejecting the connection", () =>
    Effect.gen(function* () {
      const connectedProperties: Array<Readonly<Record<string, unknown>> | undefined> = [];

      yield* buildAppUnderTest({
        layers: {
          analyticsService: {
            record: (event, properties) =>
              event === "client.connected"
                ? Effect.sync(() => connectedProperties.push(properties))
                : Effect.void,
          },
        },
      });

      const invalidUrl = yield* getWsServerUrl(
        "/ws?clientSurface=watch&clientDeviceType=television&clientOs=Plan9&clientWebDeployment=cdn&clientBrowser=&clientOsMajorVersion=-1&connectionMethod=teleport",
      );
      yield* Effect.scoped(
        withWsRpcClient(invalidUrl, (client) => client[WS_METHODS.serverGetSettings]({})),
      );

      assert.deepEqual(connectedProperties, [{}]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc projects.writeFile errors", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-project-write-" });

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsWriteFile]({
            cwd: workspaceDir,
            relativePath: "../escape.txt",
            contents: "nope",
          }),
        ).pipe(Effect.result),
      );

      if (result._tag !== "Failure" || result.failure._tag !== "ProjectWriteFileError") {
        assert.fail("Expected a ProjectWriteFileError");
      }
      const writeError = result.failure;
      assert.equal(
        writeError.message,
        `Failed to write workspace file '../escape.txt' in '${workspaceDir}'.`,
      );
      assert.equal(writeError.cwd, workspaceDir);
      assert.equal(writeError.relativePath, "../escape.txt");
      assert.equal(writeError.failure, "workspace_path_outside_root");
      assert.isDefined(writeError.cause);
      assert.notProperty(writeError, "contents");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc shell.openInEditor", () =>
    Effect.gen(function* () {
      let openedInput: { cwd: string; editor: EditorId } | null = null;
      yield* buildAppUnderTest({
        layers: {
          externalLauncher: {
            launchEditor: (input) =>
              Effect.sync(() => {
                openedInput = input;
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.shellOpenInEditor]({
            cwd: "/tmp/project",
            editor: "cursor",
          }),
        ),
      );

      assert.deepEqual(openedInput, { cwd: "/tmp/project", editor: "cursor" });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc shell.openInEditor errors", () =>
    Effect.gen(function* () {
      const externalLauncherError = new ExternalLauncherCommandNotFoundError({
        editor: "cursor",
        command: "cursor",
      });
      yield* buildAppUnderTest({
        layers: {
          externalLauncher: {
            launchEditor: () => Effect.fail(externalLauncherError),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.shellOpenInEditor]({
            cwd: "/tmp/project",
            editor: "cursor",
          }),
        ).pipe(Effect.result),
      );

      assertFailure(result, externalLauncherError);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc git methods", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          cwd: "/tmp/repo",
        },
        layers: {
          vcsDriver: {
            isInsideWorkTree: () => Effect.succeed(true),
          },
          gitManager: {
            invalidateLocalStatus: () => Effect.void,
            invalidateRemoteStatus: () => Effect.void,
            invalidateStatus: () => Effect.void,
            localStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: true,
                refName: "main",
                hasWorkingTreeChanges: false,
                workingTree: { files: [], insertions: 0, deletions: 0 },
              }),
            remoteStatus: () =>
              Effect.succeed({
                hasUpstream: true,
                aheadCount: 0,
                behindCount: 0,
                pr: null,
              }),
            status: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: true,
                refName: "main",
                hasWorkingTreeChanges: false,
                workingTree: { files: [], insertions: 0, deletions: 0 },
                hasUpstream: true,
                aheadCount: 0,
                behindCount: 0,
                pr: null,
              }),
            runStackedAction: (input, options) =>
              Effect.gen(function* () {
                const result = {
                  action: "commit" as const,
                  branch: { status: "skipped_not_requested" as const },
                  commit: {
                    status: "created" as const,
                    commitSha: "abc123",
                    subject: "feat: demo",
                  },
                  push: { status: "skipped_not_requested" as const },
                  pr: { status: "skipped_not_requested" as const },
                  toast: {
                    title: "Committed abc123",
                    description: "feat: demo",
                    cta: {
                      kind: "run_action" as const,
                      label: "Push",
                      action: {
                        kind: "push" as const,
                      },
                    },
                  },
                };

                yield* (
                  options?.progressReporter?.publish({
                    actionId: options.actionId ?? input.actionId,
                    cwd: input.cwd,
                    action: input.action,
                    kind: "phase_started",
                    phase: "commit",
                    label: "Committing...",
                  }) ?? Effect.void
                );

                yield* (
                  options?.progressReporter?.publish({
                    actionId: options.actionId ?? input.actionId,
                    cwd: input.cwd,
                    action: input.action,
                    kind: "action_finished",
                    result,
                  }) ?? Effect.void
                );

                return result;
              }),
            resolvePullRequest: () =>
              Effect.succeed({
                pullRequest: {
                  number: 1,
                  title: "Demo PR",
                  url: "https://example.com/pr/1",
                  baseBranch: "main",
                  headBranch: "feature/demo",
                  state: "open",
                },
              }),
            preparePullRequestThread: () =>
              Effect.succeed({
                pullRequest: {
                  number: 1,
                  title: "Demo PR",
                  url: "https://example.com/pr/1",
                  baseBranch: "main",
                  headBranch: "feature/demo",
                  state: "open",
                },
                branch: "feature/demo",
                worktreePath: null,
                isOnPullRequestHead: true,
              }),
          },
          gitVcsDriver: {
            pullCurrentBranch: () =>
              Effect.succeed({
                status: "pulled",
                refName: "main",
                upstreamRef: "origin/main",
              }),
            listRefs: () =>
              Effect.succeed({
                refs: [
                  {
                    name: "main",
                    current: true,
                    isDefault: true,
                    worktreePath: null,
                  },
                ],
                isRepo: true,
                hasPrimaryRemote: true,
                nextCursor: null,
                totalCount: 1,
              }),
            createWorktree: () =>
              Effect.succeed({
                worktree: { path: "/tmp/wt", refName: "feature/demo" },
              }),
            removeWorktree: () => Effect.void,
            createRef: (input) => Effect.succeed({ refName: input.refName }),
            switchRef: (input) => Effect.succeed({ refName: input.refName }),
          },
          vcsStatusBroadcaster: {
            refreshStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: true,
                refName: "main",
                hasWorkingTreeChanges: false,
                workingTree: { files: [], insertions: 0, deletions: 0 },
                hasUpstream: true,
                aheadCount: 0,
                behindCount: 0,
                pr: null,
              }),
          },
          reviewService: {
            getDiffPreview: (input) =>
              Effect.succeed({
                cwd: input.cwd,
                generatedAt: DateTime.nowUnsafe(),
                sources: [
                  {
                    id: "working-tree",
                    kind: "working-tree",
                    title: "Dirty worktree",
                    baseRef: "HEAD",
                    headRef: null,
                    diff: "dirty-diff",
                    diffHash: "hash-dirty",
                    truncated: false,
                  },
                  {
                    id: "branch-range",
                    kind: "branch-range",
                    title: "Against main",
                    baseRef: "main",
                    headRef: "feature/demo",
                    diff: "base-diff",
                    diffHash: "hash-base",
                    truncated: false,
                  },
                ],
              }),
            getDiffFileContents: () =>
              Effect.succeed({
                oldContents: "before\n",
                newContents: "after\n",
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");

      const pull = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.vcsPull]({ cwd: "/tmp/repo" })),
      );
      assert.equal(pull.status, "pulled");

      const refreshedStatus = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsRefreshStatus]({ cwd: "/tmp/repo" }),
        ),
      );
      assert.equal(refreshedStatus.isRepo, true);

      const stackedEvents = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.gitRunStackedAction]({
            actionId: "action-1",
            cwd: "/tmp/repo",
            action: "commit",
          }).pipe(
            Stream.runCollect,
            Effect.map((events) => Array.from(events)),
          ),
        ),
      );
      const lastStackedEvent = stackedEvents.at(-1);
      assert.equal(lastStackedEvent?.kind, "action_finished");
      if (lastStackedEvent?.kind === "action_finished") {
        assert.equal(lastStackedEvent.result.action, "commit");
      }

      const resolvedPr = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.gitResolvePullRequest]({
            cwd: "/tmp/repo",
            reference: "1",
          }),
        ),
      );
      assert.equal(resolvedPr.pullRequest.number, 1);

      const prepared = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.gitPreparePullRequestThread]({
            cwd: "/tmp/repo",
            reference: "1",
            mode: "local",
          }),
        ),
      );
      assert.equal(prepared.branch, "feature/demo");

      const refs = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.vcsListRefs]({ cwd: "/tmp/repo" })),
      );
      assert.equal(refs.refs[0]?.name, "main");

      const worktree = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsCreateWorktree]({
            cwd: "/tmp/repo",
            refName: "main",
            path: null,
          }),
        ),
      );
      assert.equal(worktree.worktree.refName, "feature/demo");

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsRemoveWorktree]({
            cwd: "/tmp/repo",
            path: "/tmp/wt",
          }),
        ),
      );

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsCreateRef]({
            cwd: "/tmp/repo",
            refName: "feature/new",
          }),
        ),
      );

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsSwitchRef]({
            cwd: "/tmp/repo",
            refName: "main",
          }),
        ),
      );

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsInit]({
            cwd: "/tmp/repo",
          }),
        ),
      );

      const diffPreview = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.reviewGetDiffPreview]({ cwd: "/tmp/repo" }),
        ),
      );
      assert.equal(diffPreview.sources[0]?.diff, "dirty-diff");

      const diffFileContents = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.reviewGetDiffFileContents]({
            cwd: "/tmp/repo",
            sourceKind: "working-tree",
            changeType: "change",
            baseRef: "HEAD",
            headRef: null,
            oldPath: "README.md",
            newPath: "README.md",
          }),
        ),
      );
      assert.equal(diffFileContents.oldContents, "before\n");
      assert.equal(diffFileContents.newContents, "after\n");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc git.pull errors", () =>
    Effect.gen(function* () {
      const gitError = new GitCommandError({
        operation: "pull",
        command: "git pull --ff-only",
        cwd: "/tmp/repo",
        detail: "upstream missing",
      });
      let invalidationCalls = 0;
      let statusCalls = 0;
      yield* buildAppUnderTest({
        layers: {
          gitVcsDriver: {
            pullCurrentBranch: () => Effect.fail(gitError),
          },
          gitManager: {
            invalidateLocalStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            invalidateRemoteStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            invalidateStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            localStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: true,
                refName: "main",
                hasWorkingTreeChanges: true,
                workingTree: { files: [], insertions: 0, deletions: 0 },
              }),
            remoteStatus: () =>
              Effect.sync(() => {
                statusCalls += 1;
                return {
                  hasUpstream: true,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                };
              }),
            status: () =>
              Effect.sync(() => {
                statusCalls += 1;
                return {
                  isRepo: true,
                  hasPrimaryRemote: true,
                  isDefaultRef: true,
                  refName: "main",
                  hasWorkingTreeChanges: true,
                  workingTree: { files: [], insertions: 0, deletions: 0 },
                  hasUpstream: true,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                };
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.vcsPull]({ cwd: "/tmp/repo" })).pipe(
          Effect.result,
        ),
      );

      assertFailure(result, gitError);
      assert.equal(invalidationCalls, 0);
      assert.equal(statusCalls, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc git.runStackedAction errors after refreshing git status", () =>
    Effect.gen(function* () {
      const gitError = new GitCommandError({
        operation: "commit",
        command: "git commit",
        cwd: "/tmp/repo",
        detail: "nothing to commit",
      });
      let invalidationCalls = 0;
      let statusCalls = 0;
      yield* buildAppUnderTest({
        layers: {
          gitManager: {
            invalidateLocalStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            invalidateRemoteStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            invalidateStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            localStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: false,
                refName: "feature/demo",
                hasWorkingTreeChanges: true,
                workingTree: { files: [], insertions: 0, deletions: 0 },
              }),
            remoteStatus: () =>
              Effect.sync(() => {
                statusCalls += 1;
                return {
                  hasUpstream: true,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                };
              }),
            status: () =>
              Effect.sync(() => {
                statusCalls += 1;
                return {
                  isRepo: true,
                  hasPrimaryRemote: true,
                  isDefaultRef: false,
                  refName: "feature/demo",
                  hasWorkingTreeChanges: true,
                  workingTree: { files: [], insertions: 0, deletions: 0 },
                  hasUpstream: true,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                };
              }),
            runStackedAction: () => Effect.fail(gitError),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.gitRunStackedAction]({
            actionId: "action-1",
            cwd: "/tmp/repo",
            action: "commit",
          }).pipe(Stream.runCollect, Effect.result),
        ),
      );

      assertFailure(result, gitError);
      assert.equal(invalidationCalls, 0);
      assert.equal(statusCalls, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("completes websocket rpc git.pull before background git status refresh finishes", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        layers: {
          gitVcsDriver: {
            pullCurrentBranch: () =>
              Effect.succeed({
                status: "pulled" as const,
                refName: "main",
                upstreamRef: "origin/main",
              }),
          },
          gitManager: {
            invalidateLocalStatus: () => Effect.void,
            invalidateRemoteStatus: () => Effect.void,
            invalidateStatus: () => Effect.void,
            localStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: true,
                refName: "main",
                hasWorkingTreeChanges: false,
                workingTree: { files: [], insertions: 0, deletions: 0 },
              }),
            remoteStatus: () =>
              Effect.sleep(Duration.seconds(2)).pipe(
                Effect.as({
                  hasUpstream: true,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                }),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const startedAt = yield* Clock.currentTimeMillis;
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.vcsPull]({ cwd: "/tmp/repo" })),
      );
      const elapsedMs = (yield* Clock.currentTimeMillis) - startedAt;

      assert.equal(result.status, "pulled");
      assertTrue(elapsedMs < 1_000);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "completes websocket rpc git.runStackedAction before background git status refresh finishes",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest({
          layers: {
            vcsDriver: {
              isInsideWorkTree: () => Effect.succeed(true),
            },
            gitManager: {
              invalidateLocalStatus: () => Effect.void,
              invalidateRemoteStatus: () => Effect.void,
              invalidateStatus: () => Effect.void,
              localStatus: () =>
                Effect.succeed({
                  isRepo: true,
                  hasPrimaryRemote: true,
                  isDefaultRef: false,
                  refName: "feature/demo",
                  hasWorkingTreeChanges: false,
                  workingTree: { files: [], insertions: 0, deletions: 0 },
                }),
              remoteStatus: () =>
                Effect.sleep(Duration.seconds(2)).pipe(
                  Effect.as({
                    hasUpstream: true,
                    aheadCount: 0,
                    behindCount: 0,
                    pr: null,
                  }),
                ),
              runStackedAction: () =>
                Effect.succeed({
                  action: "commit" as const,
                  branch: { status: "skipped_not_requested" as const },
                  commit: {
                    status: "created" as const,
                    commitSha: "abc123",
                    subject: "feat: demo",
                  },
                  push: { status: "skipped_not_requested" as const },
                  pr: { status: "skipped_not_requested" as const },
                  toast: {
                    title: "Committed abc123",
                    description: "feat: demo",
                    cta: {
                      kind: "run_action" as const,
                      label: "Push",
                      action: {
                        kind: "push" as const,
                      },
                    },
                  },
                }),
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        const startedAt = yield* Clock.currentTimeMillis;
        yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.gitRunStackedAction]({
              actionId: "action-1",
              cwd: "/tmp/repo",
              action: "commit",
            }).pipe(Stream.runCollect),
          ),
        );
        const elapsedMs = (yield* Clock.currentTimeMillis) - startedAt;

        assertTrue(elapsedMs < 1_000);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "starts a background local git status refresh after a successful git.runStackedAction",
    () =>
      Effect.gen(function* () {
        const localRefreshStarted = yield* Deferred.make<void>();

        yield* buildAppUnderTest({
          layers: {
            vcsDriver: {
              isInsideWorkTree: () => Effect.succeed(true),
            },
            gitManager: {
              invalidateLocalStatus: () => Effect.void,
              invalidateRemoteStatus: () => Effect.void,
              invalidateStatus: () => Effect.void,
              localStatus: () =>
                Deferred.succeed(localRefreshStarted, undefined).pipe(
                  Effect.ignore,
                  Effect.andThen(
                    Effect.succeed({
                      isRepo: true,
                      hasPrimaryRemote: true,
                      isDefaultRef: false,
                      refName: "feature/demo",
                      hasWorkingTreeChanges: false,
                      workingTree: { files: [], insertions: 0, deletions: 0 },
                    }),
                  ),
                ),
              remoteStatus: () =>
                Effect.sleep(Duration.seconds(2)).pipe(
                  Effect.as({
                    hasUpstream: true,
                    aheadCount: 0,
                    behindCount: 0,
                    pr: null,
                  }),
                ),
              runStackedAction: () =>
                Effect.succeed({
                  action: "commit" as const,
                  branch: { status: "skipped_not_requested" as const },
                  commit: {
                    status: "created" as const,
                    commitSha: "abc123",
                    subject: "feat: demo",
                  },
                  push: { status: "skipped_not_requested" as const },
                  pr: { status: "skipped_not_requested" as const },
                  toast: {
                    title: "Committed abc123",
                    description: "feat: demo",
                    cta: {
                      kind: "run_action" as const,
                      label: "Push",
                      action: {
                        kind: "push" as const,
                      },
                    },
                  },
                }),
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.gitRunStackedAction]({
              actionId: "action-1",
              cwd: "/tmp/repo",
              action: "commit",
            }).pipe(Stream.runCollect),
          ),
        );

        yield* Deferred.await(localRefreshStarted);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc orchestration methods", () =>
    Effect.gen(function* () {
      let threads!: ThreadManagement.ThreadManagementService["Service"];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              threads = Context.get(context, ThreadManagement.ThreadManagementService);
            }),
          checkpointDiffQuery: {
            getTurnDiff: () =>
              Effect.succeed({
                threadId: defaultThreadId,
                fromTurnCount: 0,
                toTurnCount: 1,
                diff: "turn-diff",
              }),
            getFullThreadDiff: () =>
              Effect.succeed({
                threadId: defaultThreadId,
                fromTurnCount: 0,
                toTurnCount: 1,
                diff: "full-diff",
              }),
          },
        },
      });
      const wsUrl = yield* getWsServerUrl("/ws");
      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const accepted = yield* client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
              type: "thread.metadata.update",
              commandId: CommandId.make("router:rename"),
              threadId: defaultThreadId,
              title: "RPC persisted title",
            });
            assert.isAbove(accepted.sequence, 0);
            yield* client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
              ...routerMessageDispatch("search"),
              text: "Search reached the final response.",
            });
            const matches = yield* client[ORCHESTRATION_V2_WS_METHODS.searchThreads]({
              query: "final response",
            });
            assert.equal(matches.matches[0]?.threadId, defaultThreadId);
            assert.equal(matches.matches[0]?.source, "user");
            assert.include(matches.matches[0]?.snippet ?? "", "final response");
            const projection = yield* client[ORCHESTRATION_V2_WS_METHODS.getThreadProjection]({
              threadId: defaultThreadId,
            });
            assert.equal(projection.thread.title, "RPC persisted title");
            assert.equal(
              (yield* threads.getThreadProjection(defaultThreadId)).thread.title,
              "RPC persisted title",
            );
            const shell = yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
              requestCompletionMarker: true,
            }).pipe(Stream.runHead);
            assertTrue(Option.isSome(shell) && shell.value.kind === "snapshot");
            if (Option.isSome(shell) && shell.value.kind === "snapshot")
              assert.equal(shell.value.snapshot.threads[0]?.id, defaultThreadId);
            assert.equal(
              (yield* client[ORCHESTRATION_V2_WS_METHODS.getTurnDiff]({
                threadId: defaultThreadId,
                fromTurnCount: 0,
                toTurnCount: 1,
              })).diff,
              "turn-diff",
            );
            assert.equal(
              (yield* client[ORCHESTRATION_V2_WS_METHODS.getFullThreadDiff]({
                threadId: defaultThreadId,
                toTurnCount: 1,
              })).diff,
              "full-diff",
            );
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc orchestration shell snapshot errors", () =>
    Effect.gen(function* () {
      const cause = new Orchestrator.OrchestratorProjectionError({
        threadId: defaultThreadId,
        cause: new Error("synthetic shell read failure"),
      });
      yield* buildAppUnderTest({
        layers: { threadManagement: { getShellSnapshot: () => Effect.fail(cause) } },
      });
      const result = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({}).pipe(Stream.runCollect),
        ),
      ).pipe(Effect.result);
      assertTrue(result._tag === "Failure");
      assert.equal(result.failure._tag, "OrchestrationV2GetShellSnapshotError");
      assert.include(yield* encodeTestJsonEffect(result.failure), "synthetic shell read failure");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("marks an empty shell catch-up replay as synchronized when requested", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
            afterSequence: 0,
            requestCompletionMarker: true,
          }).pipe(
            Stream.takeUntil((item) => item.kind === "synchronized"),
            Stream.runCollect,
          ),
        ),
      );
      assert.deepEqual(items.at(-1), { kind: "synchronized" });
      assert.isFalse(
        items.some((item) => item.kind === "thread.updated" || item.kind === "thread.removed"),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  for (const reasoningMessages of [undefined, true] as const) {
    it.effect(`preserves reasoning wire compatibility with opt-in ${reasoningMessages}`, () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest({
          layers: {
            onV2Services: (context) =>
              Effect.gen(function* () {
                yield* seedRouterThread(context);
                const tool = routerTurnItem(1);
                const {
                  input: _input,
                  output: _output,
                  exitCode: _exit,
                  outputIndicatesFailure: _failed,
                  ...base
                } = tool;
                yield* writeRouterItems(context, [
                  {
                    ...base,
                    type: "reasoning",
                    text: "Checking the available evidence.",
                    streaming: false,
                  },
                ]);
              }),
          },
        });
        const response = yield* fetchEffect(
          yield* getHttpServerUrl(
            "/api/orchestration/threads/" +
              defaultThreadId +
              (reasoningMessages ? "?reasoningMessages=true" : ""),
          ),
          {
            headers: {
              cookie: yield* getAuthenticatedSessionCookieHeader(),
              [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
            },
          },
        );
        assert.equal(response.status, 200);
        const snapshot = yield* responseJsonEffect<{
          projection: { turnItems: ReadonlyArray<{ type: string; text?: string }> };
        }>(response);
        assert.equal(snapshot.projection.turnItems[0]?.type, "reasoning");
        assert.equal(snapshot.projection.turnItems[0]?.text, "Checking the available evidence.");
        const first = yield* Effect.scoped(
          withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
            client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({ threadId: defaultThreadId }).pipe(
              Stream.runHead,
            ),
          ),
        );
        assertTrue(Option.isSome(first) && first.value.kind === "snapshot");
        if (Option.isSome(first) && first.value.kind === "snapshot")
          assert.equal(first.value.projection.turnItems[0]?.type, "reasoning");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
    );
  }

  it.effect("marks a socket thread snapshot as synchronized when requested", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        layers: { onV2Services: (context) => seedRouterThread(context).pipe(Effect.asVoid) },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
            requestCompletionMarker: true,
          }).pipe(Stream.take(2), Stream.runCollect),
        ),
      );
      assertTrue(items[0]?.kind === "snapshot");
      if (items[0]?.kind === "snapshot")
        assert.equal(items[0].projection.thread.id, defaultThreadId);
      assert.deepEqual(items[1], { kind: "synchronized" });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("buffers shell events published while the fallback snapshot loads", () =>
    Effect.gen(function* () {
      let threads!: Orchestrator.OrchestratorV2["Service"];
      let snapshotSequence = 0;
      let snapshotLoaded = false;
      let deletionSequence = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              threads = Context.get(context, Orchestrator.OrchestratorV2);
              snapshotSequence = yield* Context.get(context, OrchestrationEventStore)
                .latestApplicationSequence;
            }),
          wrapThreadManagement: (service) => ({
            ...service,
            getShellSnapshot: (options) =>
              service.getShellSnapshot(options).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    snapshotLoaded = true;
                  }),
                ),
              ),
          }),
          wrapProjectEnrichment: (service) => ({
            ...service,
            getAvailable: (workspaceRoot) =>
              Effect.gen(function* () {
                const enrichment = yield* service.getAvailable(workspaceRoot);
                if (!snapshotLoaded || deletionSequence !== 0) return enrichment;
                snapshotLoaded = false;
                // Shell rows and their sequence share one SQL transaction. Delete
                // during the following enrichment read, before the snapshot is sent.
                const deleted = yield* threads
                  .dispatch({
                    type: "thread.delete",
                    commandId: CommandId.make("router:delete-race"),
                    threadId: defaultThreadId,
                  })
                  .pipe(Effect.orDie);
                const removal = deleted.storedEvents.find(
                  (stored) => stored.event.type === "thread.deleted",
                );
                assertTrue(removal !== undefined);
                deletionSequence = removal.sequence;
                return enrichment;
              }),
          }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
            afterSequence: snapshotSequence + 1,
            requestCompletionMarker: true,
          }).pipe(
            Stream.takeUntil((item) => item.kind === "thread.removed"),
            Stream.runCollect,
          ),
        ),
      ).pipe(Effect.timeout("2 seconds"));
      assert.equal(items[0]?.kind, "snapshot");
      assertTrue(items[0]?.kind === "snapshot");
      assert.equal(items[0].snapshot.snapshotSequence, snapshotSequence);
      assert.isTrue(items[0].snapshot.threads.some((thread) => thread.id === defaultThreadId));
      const removal = items.find((item) => item.kind === "thread.removed");
      assertTrue(removal?.kind === "thread.removed");
      assert.equal(removal.threadId, defaultThreadId);
      assert.equal(removal.sequence, deletionSequence);
      assert.isAbove(deletionSequence, snapshotSequence);
      assert.isTrue(items.some((item) => item.kind === "synchronized"));
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("buffers thread events published while the initial snapshot loads", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      let eventSequence = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterThread(services);
            }),
          wrapThreadManagement: (service) => ({
            ...service,
            getThreadSnapshot: (threadId) =>
              service.getThreadSnapshot(threadId).pipe(
                Effect.tap(() =>
                  writeRouterItems(context, [routerTurnItem(1)]).pipe(
                    Effect.mapError(
                      (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
                    ),
                    Effect.tap((events) =>
                      Effect.sync(() => {
                        eventSequence = events[0]!.sequence;
                      }),
                    ),
                  ),
                ),
              ),
          }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
            requestCompletionMarker: true,
          }).pipe(Stream.take(3), Stream.runCollect),
        ),
      );
      assert.deepEqual(
        items.map((item) => item.kind),
        ["snapshot", "synchronized", "event"],
      );
      assertTrue(items[2]?.kind === "event");
      assert.equal(items[2].sequence, eventSequence);
      assert.equal(items[2].event.type, "turn-item.updated");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  for (const subscription of ["thread", "shell"] as const) {
    it.effect("delivers large raw tool results through the " + subscription + " stream", () =>
      Effect.gen(function* () {
        const output = "Build complete\n" + "x".repeat(9 * 1024 * 1024);
        let afterSequence = 0;
        let threads!: ThreadManagement.ThreadManagementService["Service"];
        yield* buildAppUnderTest({
          layers: {
            onV2Services: (context) =>
              Effect.gen(function* () {
                yield* seedRouterThread(context);
                threads = Context.get(context, ThreadManagement.ThreadManagementService);
                afterSequence = (yield* threads.getThreadSnapshot(defaultThreadId))
                  .snapshotSequence;
                yield* writeRouterItems(context, [routerTurnItem(1, { output })]);
              }),
          },
        });
        const items = yield* Effect.scoped(
          withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
            Effect.gen(function* () {
              if (subscription === "thread") {
                return yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
                  threadId: defaultThreadId,
                  afterSequence,
                  requestCompletionMarker: true,
                }).pipe(
                  Stream.takeUntil((item) => item.kind === "synchronized"),
                  Stream.runCollect,
                );
              }
              return yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
                afterSequence,
                requestCompletionMarker: true,
              }).pipe(
                Stream.takeUntil((item) => item.kind === "synchronized"),
                Stream.runCollect,
              );
            }),
          ),
        );
        const encodedItems = yield* encodeTestJsonEffect(items);
        assert.deepEqual(items.at(-1), { kind: "synchronized" });
        assert.isBelow(Buffer.byteLength(encodedItems), 128 * 1024);
        assert.isFalse(encodedItems.includes(output));
        assert.isTrue(items.some((item) => item.kind === "snapshot"));
        const persisted = (yield* threads.getThreadProjection(defaultThreadId)).turnItems[0];
        assertTrue(persisted?.type === "command_execution");
        assert.equal(persisted.output, output);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
    );
  }

  it.effect("coalesces buffered live tool updates to the latest state", () =>
    Effect.gen(function* () {
      let snapshot!: Effect.Success<
        ReturnType<ThreadManagement.ThreadManagementService["Service"]["getThreadSnapshot"]>
      >;
      let lastSequence = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              snapshot = yield* Context.get(context, Orchestrator.OrchestratorV2).getThreadSnapshot(
                defaultThreadId,
              );
              const written = yield* writeRouterItems(
                context,
                [1, 2, 3].map((version) =>
                  routerTurnItem(1, {
                    status: "running",
                    completedAt: null,
                    output: String(version),
                  }),
                ),
              );
              lastSequence = written.at(-1)!.sequence;
            }),
          threadManagement: { getThreadSnapshot: () => Effect.succeed(snapshot) },
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({ threadId: defaultThreadId }).pipe(
            Stream.take(2),
            Stream.runCollect,
          ),
        ),
      ).pipe(Effect.timeout("2 seconds"));
      assert.equal(items[0]?.kind, "snapshot");
      assertTrue(items[1]?.kind === "event");
      assert.equal(items[1].sequence, lastSequence);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("flushes more than one tool chunk before the synchronization marker", () =>
    Effect.gen(function* () {
      const coalescer = yield* makeThreadLiveEventCoalescer();
      const events: Array<Extract<OrchestrationV2ThreadStreamItem, { kind: "event" }>> = Array.from(
        { length: 514 },
        (_, index) => ({
          kind: "event",
          sequence: index + 1,
          event: routerItemEvent(routerTurnItem(index, { status: "running", completedAt: null })),
        }),
      );
      yield* coalescer.offerAll([...events, { kind: "synchronized" }]);
      const items = yield* coalescer.stream.pipe(
        Stream.takeUntil((item) => item.kind === "synchronized"),
        Stream.runCollect,
      );
      assert.equal(items.filter((item) => item.kind === "event").length, 514);
      assert.deepEqual(items.at(-1), { kind: "synchronized" });
      assert.deepEqual(
        items.flatMap((item) => (item.kind === "event" ? [item.sequence] : [])),
        events.map((event) => event.sequence),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("flushes a tool update before an interleaved message", () =>
    Effect.gen(function* () {
      let snapshot!: Effect.Success<
        ReturnType<ThreadManagement.ThreadManagementService["Service"]["getThreadSnapshot"]>
      >;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              snapshot = yield* Context.get(context, Orchestrator.OrchestratorV2).getThreadSnapshot(
                defaultThreadId,
              );
              const tool = routerTurnItem(1, { status: "running", completedAt: null });
              const message = {
                ...tool,
                type: "assistant_message" as const,
                id: TurnItemId.make("router:answer"),
                messageId: MessageId.make("router:answer"),
                text: "An interleaved answer",
                streaming: false,
              };
              yield* writeRouterItems(context, [tool, message]);
            }),
          threadManagement: { getThreadSnapshot: () => Effect.succeed(snapshot) },
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({ threadId: defaultThreadId }).pipe(
            Stream.take(3),
            Stream.runCollect,
          ),
        ),
      );
      const events = items.filter((item) => item.kind === "event");
      assert.equal(events.length, 2);
      assertTrue(events[0]?.event.type === "turn-item.updated");
      assert.equal(events[0].event.payload.type, "command_execution");
      assertTrue(events[1]?.event.type === "turn-item.updated");
      assert.equal(events[1].event.payload.type, "assistant_message");
      assert.isBelow(events[0].sequence, events[1].sequence);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "subscribeThread sends a fresh snapshot when its event count exceeds the replay limit",
    () =>
      Effect.gen(function* () {
        let afterSequence = 0;
        let decodeCalls = 0;
        yield* buildAppUnderTest({
          layers: {
            onV2Services: (context) =>
              Effect.gen(function* () {
                yield* seedRouterThread(context);
                afterSequence = (yield* Context.get(
                  context,
                  Orchestrator.OrchestratorV2,
                ).getThreadSnapshot(defaultThreadId)).snapshotSequence;
                yield* writeRouterItems(
                  context,
                  Array.from({ length: 129 }, (_, index) => routerTurnItem(index)),
                );
              }),
            wrapApplicationEvents: (service) => ({
              ...service,
              readAgentEvents: (input) => {
                decodeCalls += 1;
                return service.readAgentEvents(input);
              },
            }),
          },
        });
        const items = yield* Effect.scoped(
          withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
            client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
              threadId: defaultThreadId,
              afterSequence,
              requestCompletionMarker: true,
            }).pipe(
              Stream.takeUntil((item) => item.kind === "synchronized"),
              Stream.runCollect,
            ),
          ),
        );
        assert.equal(items[0]?.kind, "snapshot");
        assert.deepEqual(items.at(-1), { kind: "synchronized" });
        assert.equal(decodeCalls, 0);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "stops an overflowing thread producer without an ACK and replays the missing events",
    () =>
      Effect.gen(function* () {
        const ackHeld = yield* Deferred.make<void>();
        const releaseAck = yield* Deferred.make<void>();
        const attached = yield* Deferred.make<void>();
        const detached = yield* Deferred.make<void>();
        const firstApplied = yield* Deferred.make<void>();
        let context!: Context.Context<RouterV2Services>;
        let cursor = 0;
        yield* buildAppUnderTest({
          layers: {
            onV2Services: (services) =>
              Effect.gen(function* () {
                context = services;
                yield* seedRouterThread(services);
                cursor = yield* Context.get(services, OrchestrationEventStore).latestAgentSequence(
                  defaultThreadId,
                );
              }),
            wrapThreadManagement: (service) => ({
              ...service,
              streamStoredEventsFrom: (input) =>
                Stream.unwrap(
                  Deferred.succeed(attached, undefined).pipe(
                    Effect.as(service.streamStoredEventsFrom(input)),
                  ),
                ).pipe(Stream.ensuring(Deferred.succeed(detached, undefined))),
            }),
          },
        });
        const answer = (ordinal: number, text: string): OrchestrationV2TurnItem => {
          const base = routerTurnItem(ordinal);
          return {
            ...base,
            type: "assistant_message",
            messageId: MessageId.make("router:answer:" + ordinal),
            text,
            streaming: false,
          };
        };
        const wsUrl = yield* getWsServerUrl("/ws");
        yield* makeWsRpcClient.pipe(
          Effect.flatMap((client) =>
            Effect.gen(function* () {
              const received: number[] = [];
              const attempt = yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
                threadId: defaultThreadId,
                afterSequence: cursor,
              }).pipe(
                Stream.tap((item) =>
                  item.kind === "event"
                    ? Effect.sync(() => {
                        cursor = item.sequence;
                        received.push(cursor);
                      }).pipe(Effect.andThen(Deferred.succeed(firstApplied, undefined)))
                    : Effect.void,
                ),
                Stream.runDrain,
                Effect.result,
                Effect.forkScoped,
              );
              yield* Deferred.await(attached);
              yield* writeRouterItems(
                context,
                [answer(1, "a".repeat(8 * 1024 * 1024 - 2048))],
                "first-held",
              );
              yield* Deferred.await(ackHeld);
              yield* Deferred.await(firstApplied);
              const missing = yield* writeRouterItems(
                context,
                [2, 3, 4].map((ordinal) => answer(ordinal, "b".repeat(4096))),
                "after-held",
              );
              yield* Deferred.await(detached);
              assert.equal(received.length, 1);
              yield* Deferred.succeed(releaseAck, undefined);
              const failed = yield* Fiber.join(attempt);
              assertTrue(failed._tag === "Failure");
              assert.equal(failed.failure._tag, "OrchestrationV2GetThreadProjectionError");
              const recovered = yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
                threadId: defaultThreadId,
                afterSequence: cursor,
                requestCompletionMarker: true,
              }).pipe(
                Stream.takeUntil((item) => item.kind === "synchronized"),
                Stream.runCollect,
              );
              assert.deepEqual(
                recovered.flatMap((item) => (item.kind === "event" ? [item.sequence] : [])),
                missing.map((stored) => stored.sequence),
              );
              assert.deepEqual(recovered.at(-1), { kind: "synchronized" });
            }),
          ),
          Effect.provide(withFirstWsAckHeld(wsUrl, ackHeld, releaseAck)),
        );
      }).pipe(Effect.scoped, Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("stops an overflowing shell producer without an ACK and recovers deleted entries", () =>
    Effect.gen(function* () {
      const liveEvents = yield* PubSub.unbounded<OrchestrationV2StoredEvent>();
      const detached = yield* Deferred.make<void>();
      const attached = yield* Deferred.make<void>();
      const ackHeld = yield* Deferred.make<void>();
      const releaseAck = yield* Deferred.make<void>();
      let context!: Context.Context<RouterV2Services>;
      let head = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterThread(services);
              head = yield* Context.get(services, OrchestrationEventStore)
                .latestApplicationSequence;
            }),
          wrapApplicationEvents: (service) => ({
            ...service,
            latestApplicationSequence: Effect.sync(() => head),
            streamProjectedApplicationEvents: (input) =>
              Stream.unwrap(
                Effect.gen(function* () {
                  const subscription = yield* PubSub.subscribe(liveEvents);
                  yield* Deferred.succeed(attached, undefined);
                  return Stream.fromSubscription(subscription).pipe(Stream.map(input.project));
                }),
              ).pipe(Stream.ensuring(Deferred.succeed(detached, undefined))),
          }),
        },
      });
      const wsUrl = yield* getWsServerUrl("/ws");
      yield* makeWsRpcClient.pipe(
        Effect.flatMap((client) =>
          Effect.gen(function* () {
            const received: OrchestrationV2ShellStreamItem[] = [];
            // The live producer attaches only after the snapshot's ACK, so the
            // completion marker's ACK is the one held while the burst arrives.
            const attempt = yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
              requestCompletionMarker: true,
            }).pipe(
              Stream.tap((item) =>
                Effect.sync(() => {
                  received.push(item);
                }),
              ),
              Stream.runDrain,
              Effect.result,
              Effect.forkScoped,
            );
            yield* Deferred.await(ackHeld);
            yield* Deferred.await(attached);
            yield* Context.get(context, Orchestrator.OrchestratorV2).dispatch({
              type: "thread.delete",
              commandId: CommandId.make("router:overflow-delete"),
              threadId: defaultThreadId,
            });
            yield* Context.get(context, ProjectService.ProjectService).delete({
              commandId: CommandId.make("router:overflow-project-delete"),
              projectId: defaultProjectId,
              force: true,
            });
            head += 1004;
            const burst = Array.from({ length: 1001 }, (_, index) => ({
              sequence: head - 1001 + index,
              commandId: null,
              event: routerItemEvent(
                routerTurnItem(index, { threadId: ThreadId.make("router:overflow:" + index) }),
              ),
            }));
            yield* PubSub.publishAll(liveEvents, burst);
            yield* Deferred.await(detached);
            assert.equal(yield* PubSub.size(liveEvents), 0);
            yield* Deferred.succeed(releaseAck, undefined);
            const failed = yield* Fiber.join(attempt);
            assertTrue(failed._tag === "Failure");
            assert.equal(failed.failure._tag, "OrchestrationV2GetShellSnapshotError");
            assert.equal(received[0]?.kind, "snapshot");
            const initial = received[0];
            assertTrue(initial?.kind === "snapshot");
            const recovered = yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
              afterSequence: initial.snapshot.snapshotSequence,
              requestCompletionMarker: true,
            }).pipe(
              Stream.takeUntil((item) => item.kind === "synchronized"),
              Stream.runCollect,
            );
            const snapshot = recovered[0];
            assertTrue(snapshot?.kind === "snapshot");
            assert.deepEqual(snapshot.snapshot.projects, []);
            assert.deepEqual(snapshot.snapshot.threads, []);
            assert.deepEqual(recovered.at(-1), { kind: "synchronized" });
          }),
        ),
        Effect.provide(withFirstWsAckHeld(wsUrl, ackHeld, releaseAck, 1)),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("subscribeThread replaces a cursor ahead of the authoritative head", () =>
    Effect.gen(function* () {
      let head = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              head = yield* Context.get(context, OrchestrationEventStore).latestAgentSequence(
                defaultThreadId,
              );
            }),
          wrapApplicationEvents: (service) => ({
            ...service,
            getAgentReplayStats: () =>
              Effect.die("A cursor ahead of the head must not measure replay"),
            readAgentEvents: () => Stream.die("A cursor ahead of the head must not decode replay"),
          }),
        },
      });
      const first = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
            afterSequence: head + 10,
          }).pipe(Stream.runHead),
        ),
      );
      assert.equal(Option.getOrThrow(first).kind, "snapshot");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeThread replays a small thread range across a large global gap", () =>
    Effect.gen(function* () {
      let afterSequence = 0;
      let sequence = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              afterSequence = yield* Context.get(
                context,
                OrchestrationEventStore,
              ).latestAgentSequence(defaultThreadId);
              sequence = (yield* writeRouterItems(context, [routerTurnItem(1)]))[0]!.sequence;
            }),
          wrapApplicationEvents: (service) => ({
            ...service,
            latestApplicationSequence: Effect.succeed(100_000),
            getReplayStats: () => Effect.die("Thread replay must not measure the global log"),
            readApplicationEvents: () => Stream.die("Thread replay must not decode the global log"),
          }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
            afterSequence,
            requestCompletionMarker: true,
          }).pipe(
            Stream.takeUntil((item) => item.kind === "synchronized"),
            Stream.runCollect,
          ),
        ),
      );
      assert.deepEqual(
        items.map((item) => (item.kind === "event" ? item.sequence : item.kind)),
        [sequence, "synchronized"],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeThread resets cached history when its ID is created again", () =>
    Effect.gen(function* () {
      let afterSequence = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              const threads = Context.get(context, Orchestrator.OrchestratorV2);
              afterSequence = (yield* threads.getThreadSnapshot(defaultThreadId)).snapshotSequence;
              yield* threads.dispatch({
                type: "thread.delete",
                commandId: CommandId.make("router:delete"),
                threadId: defaultThreadId,
              });
              yield* threads.dispatch({
                ...routerThreadCreate(),
                commandId: CommandId.make("router:recreate"),
                title: "Recreated thread",
              });
            }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
            afterSequence,
            requestCompletionMarker: true,
          }).pipe(
            Stream.takeUntil((item) => item.kind === "synchronized"),
            Stream.runCollect,
          ),
        ),
      );
      assertTrue(items[0]?.kind === "snapshot");
      assert.equal(items[0].projection.thread.title, "Recreated thread");
      assert.deepEqual(items.at(-1), { kind: "synchronized" });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  for (const { createBeforeDelete, oversized } of [
    { createBeforeDelete: false, oversized: false },
    { createBeforeDelete: true, oversized: false },
    { createBeforeDelete: true, oversized: true },
  ]) {
    it.effect(
      oversized
        ? "keeps the missing-snapshot error when an absent thread exceeds the replay limit"
        : `synchronizes an absent thread and removes its shell after ${createBeforeDelete ? "creation and deletion" : "deletion"}`,
      () =>
        Effect.gen(function* () {
          let afterSequence = 0;
          let missingSnapshotReads = 0;
          yield* buildAppUnderTest({
            layers: {
              // V2 deletion retains a tombstone projection. Only this fallback-error
              // case supplies a missing snapshot; replay still reads real events.
              ...(oversized
                ? {
                    threadManagement: {
                      getThreadSnapshot: (threadId: ThreadId) =>
                        Effect.gen(function* () {
                          missingSnapshotReads += 1;
                          return yield* new Orchestrator.OrchestratorProjectionError({
                            threadId,
                            cause: new ProjectionStore.ProjectionStoreThreadNotFoundError({
                              threadId,
                            }),
                          });
                        }),
                    },
                  }
                : {}),
              onV2Services: (context) =>
                Effect.gen(function* () {
                  yield* seedRouterThread(context);
                  const threads = Context.get(context, Orchestrator.OrchestratorV2);
                  if (!createBeforeDelete)
                    afterSequence = yield* Context.get(
                      context,
                      OrchestrationEventStore,
                    ).latestAgentSequence(defaultThreadId);
                  if (oversized)
                    yield* writeRouterItems(
                      context,
                      Array.from({ length: 129 }, (_, index) => routerTurnItem(index)),
                    );
                  yield* threads.dispatch({
                    type: "thread.delete",
                    commandId: CommandId.make("router:final-delete"),
                    threadId: defaultThreadId,
                  });
                  if (oversized) {
                    const tombstone = yield* threads.getThreadSnapshot(defaultThreadId);
                    assert.isNotNull(tombstone.projection.thread.deletedAt);
                    const stats = yield* Context.get(
                      context,
                      OrchestrationEventStore,
                    ).getAgentReplayStats({
                      threadId: defaultThreadId,
                      afterSequence,
                      throughSequence: tombstone.snapshotSequence,
                      maxEvents: 128,
                    });
                    assert.isAbove(stats.eventCount, 128);
                  }
                }),
            },
          });
          yield* Effect.scoped(
            withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
              Effect.gen(function* () {
                const result = yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
                  threadId: defaultThreadId,
                  afterSequence,
                  requestCompletionMarker: true,
                }).pipe(
                  Stream.takeUntil((item) => item.kind === "synchronized"),
                  Stream.runCollect,
                  Effect.result,
                );
                if (oversized) {
                  assertTrue(result._tag === "Failure");
                  assert.equal(result.failure._tag, "OrchestrationV2GetThreadProjectionError");
                  assert.equal(missingSnapshotReads, 1);
                  return;
                }
                assertTrue(result._tag === "Success");
                assert.deepEqual(result.success.at(-1), { kind: "synchronized" });
                const shell = yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
                  afterSequence,
                  requestCompletionMarker: true,
                }).pipe(
                  Stream.takeUntil((item) => item.kind === "synchronized"),
                  Stream.runCollect,
                );
                assert.isTrue(
                  shell.some(
                    (item) => item.kind === "thread.removed" && item.threadId === defaultThreadId,
                  ),
                );
              }),
            ),
          );
        }).pipe(Effect.provide(NodeHttpServer.layerTest)),
    );
  }

  it.effect("subscribeThread bounds catch-up replay to the captured head", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      let afterSequence = 0;
      let capturedHead = 0;
      let queriedHead: number | undefined;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterThread(services);
              afterSequence = yield* Context.get(
                services,
                OrchestrationEventStore,
              ).latestAgentSequence(defaultThreadId);
              capturedHead = (yield* writeRouterItems(services, [routerTurnItem(1)]))[0]!.sequence;
            }),
          wrapApplicationEvents: (service) => ({
            ...service,
            readAgentEvents: (input) => {
              queriedHead = input?.throughSequence;
              return Stream.fromEffect(
                writeRouterItems(context, [routerTurnItem(2)], "arrives-after-head").pipe(
                  Effect.mapError(
                    (cause) =>
                      new PersistenceSqlError({ operation: "fixture.readAgentEvents", cause }),
                  ),
                ),
              ).pipe(Stream.flatMap(() => service.readAgentEvents(input)));
            },
          }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
            afterSequence,
            requestCompletionMarker: true,
          }).pipe(
            Stream.takeUntil((item) => item.kind === "synchronized"),
            Stream.runCollect,
          ),
        ),
      );
      assert.equal(queriedHead, capturedHead);
      assert.deepEqual(
        items.map((item) => (item.kind === "event" ? item.sequence : item.kind)),
        [capturedHead, "synchronized"],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell sends a fresh snapshot instead of replaying a large gap", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) => seedRouterThread(context).pipe(Effect.asVoid),
          wrapApplicationEvents: (service) => ({
            ...service,
            latestApplicationSequence: Effect.succeed(100_000),
            getReplayStats: () => Effect.die("A large gap must not measure replay"),
            readApplicationEvents: () => Stream.die("A large gap must not decode replay"),
          }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
            afterSequence: 5,
            requestCompletionMarker: true,
          }).pipe(
            Stream.takeUntil((item) => item.kind === "synchronized"),
            Stream.runCollect,
          ),
        ),
      );
      assertTrue(items[0]?.kind === "snapshot");
      assert.equal(items[0].snapshot.snapshotSequence, 100_000);
      assert.equal(items[0].snapshot.threads[0]?.id, defaultThreadId);
      assert.deepEqual(items.at(-1), { kind: "synchronized" });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscriptions snapshot instead of decoding an oversized replay range", () =>
    Effect.gen(function* () {
      let afterSequence = 0;
      let measurements = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              afterSequence = yield* Context.get(context, OrchestrationEventStore)
                .latestApplicationSequence;
              yield* writeRouterItems(context, [
                routerTurnItem(1, { output: "x".repeat(8 * 1024 * 1024 + 1) }),
              ]);
            }),
          wrapApplicationEvents: (service) => ({
            ...service,
            getAgentReplayStats: (input) =>
              service.getAgentReplayStats(input).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    measurements += 1;
                  }),
                ),
              ),
            getReplayStats: (input) =>
              service.getReplayStats(input).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    measurements += 1;
                  }),
                ),
              ),
            readAgentEvents: () => Stream.die("Oversized thread replay must not decode"),
            readApplicationEvents: () => Stream.die("Oversized shell replay must not decode"),
          }),
        },
      });
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          Effect.gen(function* () {
            const thread = yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
              threadId: defaultThreadId,
              afterSequence,
            }).pipe(Stream.runHead);
            const shell = yield* client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
              afterSequence,
            }).pipe(Stream.runHead);
            assert.equal(Option.getOrThrow(thread).kind, "snapshot");
            assert.equal(Option.getOrThrow(shell).kind, "snapshot");
          }),
        ),
      );
      assert.equal(measurements, 2);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell replaces a cursor ahead of the authoritative head", () =>
    Effect.gen(function* () {
      let head = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              head = yield* Context.get(context, OrchestrationEventStore).latestApplicationSequence;
            }),
          wrapApplicationEvents: (service) => ({
            ...service,
            getReplayStats: () =>
              Effect.die("A cursor ahead of the shell head must not measure replay"),
            readApplicationEvents: () =>
              Stream.die("A cursor ahead of the shell head must not decode replay"),
          }),
        },
      });
      const first = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({ afterSequence: head + 10 }).pipe(
            Stream.runHead,
          ),
        ),
      );
      assert.equal(Option.getOrThrow(first).kind, "snapshot");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell coalesces a per-thread burst without stalling other threads", () =>
    Effect.gen(function* () {
      const other = ThreadId.make("router:other");
      let afterSequence = 0;
      const reads: ThreadId[] = [];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              yield* Context.get(context, Orchestrator.OrchestratorV2).dispatch(
                routerThreadCreate(other),
              );
              afterSequence = yield* Context.get(context, OrchestrationEventStore)
                .latestApplicationSequence;
              yield* writeRouterItems(context, [
                ...Array.from({ length: 200 }, (_, index) => routerTurnItem(index)),
                routerTurnItem(201, { threadId: other }),
              ]);
            }),
          wrapThreadManagement: (service) => ({
            ...service,
            getThreadShell: (threadId) =>
              Effect.sync(() => reads.push(threadId)).pipe(
                Effect.andThen(service.getThreadShell(threadId)),
              ),
          }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
            afterSequence,
            requestCompletionMarker: true,
          }).pipe(
            Stream.takeUntil((item) => item.kind === "synchronized"),
            Stream.runCollect,
          ),
        ),
      );
      const updates = items.filter((item) => item.kind === "thread.updated");
      assert.deepEqual(
        updates.map((item) => item.thread.id).sort(),
        [defaultThreadId, other].sort(),
      );
      assert.equal(reads.length, 2);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell coalesces live bursts after the synchronization marker", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      let afterSequence = 0;
      const reads: ThreadId[] = [];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterThread(services);
              afterSequence = yield* Context.get(services, OrchestrationEventStore)
                .latestApplicationSequence;
            }),
          wrapThreadManagement: (service) => ({
            ...service,
            getThreadShell: (threadId) =>
              Effect.sync(() => reads.push(threadId)).pipe(
                Effect.andThen(service.getThreadShell(threadId)),
              ),
          }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
            afterSequence,
            requestCompletionMarker: true,
          }).pipe(
            Stream.tap((item) =>
              item.kind === "synchronized"
                ? writeRouterItems(
                    context,
                    Array.from({ length: 20 }, (_, index) => routerTurnItem(index)),
                  ).pipe(Effect.asVoid)
                : Effect.void,
            ),
            Stream.takeUntil((item) => item.kind === "thread.updated"),
            Stream.runCollect,
          ),
        ),
      ).pipe(Effect.timeout("2 seconds"));
      assert.isTrue(items.some((item) => item.kind === "synchronized"));
      assert.equal(items.at(-1)?.kind, "thread.updated");
      assert.equal(reads.length, 1);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("subscribeShell coalescing still emits a removal for a deleted thread", () =>
    Effect.gen(function* () {
      let afterSequence = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              afterSequence = yield* Context.get(context, OrchestrationEventStore)
                .latestApplicationSequence;
              yield* Context.get(context, Orchestrator.OrchestratorV2).dispatch({
                type: "thread.delete",
                commandId: CommandId.make("router:delete-trailing"),
                threadId: defaultThreadId,
              });
              yield* writeRouterItems(context, [routerTurnItem(1)]);
            }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
            afterSequence,
            requestCompletionMarker: true,
          }).pipe(
            Stream.takeUntil((item) => item.kind === "synchronized"),
            Stream.runCollect,
          ),
        ),
      );
      const removals = items.filter((item) => item.kind === "thread.removed");
      assert.equal(removals.length, 1);
      assert.equal(removals[0]?.threadId, defaultThreadId);
      assert.isFalse(items.some((item) => item.kind === "thread.updated"));
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell retries a transient shell projection refetch failure", () =>
    Effect.gen(function* () {
      let afterSequence = 0;
      let attempts = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              afterSequence = yield* Context.get(context, OrchestrationEventStore)
                .latestApplicationSequence;
              yield* writeRouterItems(context, [routerTurnItem(1)]);
            }),
          wrapThreadManagement: (service) => ({
            ...service,
            getThreadShell: (threadId) =>
              Effect.suspend(() => {
                attempts += 1;
                return attempts === 1
                  ? Effect.fail(
                      new Orchestrator.OrchestratorProjectionError({
                        threadId,
                        cause: new Error("synthetic transient shell refetch"),
                      }),
                    )
                  : service.getThreadShell(threadId);
              }),
          }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
            afterSequence,
            requestCompletionMarker: true,
          }).pipe(
            Stream.takeUntil((item) => item.kind === "synchronized"),
            Stream.runCollect,
          ),
        ),
      );
      assert.isTrue(
        items.some((item) => item.kind === "thread.updated" && item.thread.id === defaultThreadId),
      );
      assert.equal(attempts, 2);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell coalescing still removes a project after a trailing update", () =>
    Effect.gen(function* () {
      let afterSequence = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterProject(context);
              const projects = Context.get(context, ProjectService.ProjectService);
              const events = Context.get(context, OrchestrationEventStore);
              afterSequence = yield* events.latestApplicationSequence;
              yield* projects.delete({
                commandId: CommandId.make("router:project-delete"),
                projectId: defaultProjectId,
              });
              const retained = yield* events
                .readApplicationEvents({
                  afterSequence,
                  throughSequence: yield* events.latestApplicationSequence,
                })
                .pipe(Stream.runCollect);
              const deleted = retained[0];
              assertTrue(
                deleted !== undefined &&
                  "aggregateKind" in deleted &&
                  deleted.aggregateKind === "project",
              );
              if (
                deleted !== undefined &&
                "aggregateKind" in deleted &&
                deleted.aggregateKind === "project"
              ) {
                yield* events.appendProjectEvent({
                  ...deleted,
                  type: "project.meta-updated",
                  eventId: EventId.make("router:trailing-project-update"),
                  payload: {
                    projectId: defaultProjectId,
                    title: "Still deleted",
                    updatedAt: "2026-01-01T00:00:01.000Z",
                  },
                });
              }
            }),
        },
      });
      const items = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
            afterSequence,
            requestCompletionMarker: true,
          }).pipe(
            Stream.takeUntil((item) => item.kind === "synchronized"),
            Stream.runCollect,
          ),
        ),
      );
      assert.isTrue(
        items.some(
          (item) => item.kind === "project.removed" && item.projectId === defaultProjectId,
        ),
      );
      assert.isFalse(items.some((item) => item.kind === "project.updated"));
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("persists V2 provider detach and terminal cleanup when archiving", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      const closed: string[] = [];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterThread(services);
              yield* seedRouterProvider(services);
            }),
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                closed.push(input.threadId);
              }),
          },
        },
      });
      const commandId = CommandId.make("router:archive");
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
            type: "thread.archive",
            commandId,
            threadId: defaultThreadId,
          }),
        ),
      );
      const queued = yield* Context.get(context, EffectOutbox.EffectOutboxV2).listByCommandId(
        commandId,
      );
      assert.deepEqual(queued.map((effect) => effect.request.type).sort(), [
        "provider-session.detach",
        "terminal.cleanup",
      ]);
      assert.deepEqual(closed, []);
      yield* Context.get(context, EffectWorker.OrchestrationEffectWorkerV2).drain(10);
      assert.deepEqual(closed, [defaultThreadId]);
      assert.isNotNull(
        (yield* Context.get(context, ThreadManagement.ThreadManagementService).getThreadProjection(
          defaultThreadId,
        )).thread.archivedAt,
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("captures the V2 attached session before archive removes the active shell", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      let sessionId!: ProviderSessionId;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterThread(services);
              sessionId = yield* seedRouterProvider(services);
            }),
        },
      });
      const commandId = CommandId.make("router:archive-attached");
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
            type: "thread.archive",
            commandId,
            threadId: defaultThreadId,
          }),
        ),
      );
      const effects = yield* Context.get(context, EffectOutbox.EffectOutboxV2).listByCommandId(
        commandId,
      );
      const detach = effects.find((effect) => effect.request.type === "provider-session.detach");
      assertTrue(detach?.request.type === "provider-session.detach");
      assert.equal(detach.request.providerSessionId, sessionId);
      assert.isTrue(detach.request.revokeMcpCredential);
      const active = yield* Context.get(
        context,
        ThreadManagement.ThreadManagementService,
      ).getShellSnapshot({ location: "active" });
      assert.isFalse(active.threads.some((thread) => thread.id === defaultThreadId));
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("archives without dispatching session stop when the thread has no session", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      const closed: string[] = [];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterThread(services);
            }),
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                closed.push(input.threadId);
              }),
          },
        },
      });
      const commandId = CommandId.make("router:archive-no-detach");
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
            type: "thread.archive",
            commandId,
            threadId: defaultThreadId,
          }),
        ),
      );
      const effects = yield* Context.get(context, EffectOutbox.EffectOutboxV2).listByCommandId(
        commandId,
      );
      assert.deepEqual(
        effects.map((effect) => effect.request.type),
        ["terminal.cleanup"],
      );
      yield* Context.get(context, EffectWorker.OrchestrationEffectWorkerV2).drain(10);
      assert.deepEqual(closed, [defaultThreadId]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "archives without dispatching session stop when the thread session is already stopped",
    () =>
      Effect.gen(function* () {
        let context!: Context.Context<RouterV2Services>;
        const closed: string[] = [];
        yield* buildAppUnderTest({
          layers: {
            onV2Services: (services) =>
              Effect.gen(function* () {
                context = services;
                yield* seedRouterThread(services);
                yield* seedRouterProvider(services, "stopped");
              }),
            terminalManager: {
              close: (input) =>
                Effect.sync(() => {
                  closed.push(input.threadId);
                }),
            },
          },
        });
        const commandId = CommandId.make("router:archive-no-detach");
        yield* Effect.scoped(
          withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
            client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
              type: "thread.archive",
              commandId,
              threadId: defaultThreadId,
            }),
          ),
        );
        const effects = yield* Context.get(context, EffectOutbox.EffectOutboxV2).listByCommandId(
          commandId,
        );
        assert.deepEqual(
          effects.map((effect) => effect.request.type),
          ["terminal.cleanup"],
        );
        yield* Context.get(context, EffectWorker.OrchestrationEffectWorkerV2).drain(10);
        assert.deepEqual(closed, [defaultThreadId]);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("leaves V2 settle provider detach to the durable effect worker", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      const closed: string[] = [];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterThread(services);
              yield* seedRouterProvider(services);
            }),
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                closed.push(input.threadId);
              }),
          },
        },
      });
      const commandId = CommandId.make("router:settle");
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
            type: "thread.settle",
            commandId,
            threadId: defaultThreadId,
          }),
        ),
      );
      const effects = yield* Context.get(context, EffectOutbox.EffectOutboxV2).listByCommandId(
        commandId,
      );
      assert.deepEqual(
        effects.map((effect) => effect.request.type),
        ["provider-session.detach"],
      );
      assert.deepEqual(closed, []);
      yield* Context.get(context, EffectWorker.OrchestrationEffectWorkerV2).drain(10);
      assert.deepEqual(closed, []);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("forwards the friendly blocked-settlement message over websocket rpc", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              yield* seedRouterThread(context);
              yield* Context.get(context, ThreadManagement.ThreadManagementService).dispatch({
                ...routerMessageDispatch("blocked-settle"),
                dispatchMode: { type: "defer_start" },
              });
            }),
        },
      });
      const failed = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
            type: "thread.settle",
            commandId: CommandId.make("router:settle-blocked"),
            threadId: defaultThreadId,
          }),
        ),
      ).pipe(Effect.result);
      assertTrue(failed._tag === "Failure");
      assert.equal(failed.failure._tag, "OrchestrationV2DispatchCommandError");
      assert.equal(
        failed.failure.message,
        "Thread " + defaultThreadId + " has active or blocked work and cannot be settled.",
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps V2 archive committed when terminal cleanup fails", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      let attempts = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterThread(services);
            }),
          terminalManager: {
            close: () =>
              Effect.sync(() => {
                attempts += 1;
              }).pipe(
                Effect.andThen(
                  Effect.fail(
                    new TerminalNotRunningError({
                      threadId: defaultThreadId,
                      terminalId: "default",
                    }),
                  ),
                ),
              ),
          },
        },
      });
      const commandId = CommandId.make("router:archive-cleanup-failure");
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
            type: "thread.archive",
            commandId,
            threadId: defaultThreadId,
          }),
        ),
      );
      yield* Context.get(context, EffectWorker.OrchestrationEffectWorkerV2).drain(10);
      const effects = yield* Context.get(context, EffectOutbox.EffectOutboxV2).listByCommandId(
        commandId,
      );
      assert.equal(attempts, 1);
      assert.equal(effects[0]?.request.type, "terminal.cleanup");
      assert.isFalse(effects[0]?.status === "succeeded");
      assert.isNotNull(
        (yield* Context.get(context, ThreadManagement.ThreadManagementService).getThreadProjection(
          defaultThreadId,
        )).thread.archivedAt,
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps V2 archive committed when terminal cleanup defects", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      let attempts = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterThread(services);
            }),
          terminalManager: {
            close: () =>
              Effect.sync(() => {
                attempts += 1;
              }).pipe(Effect.andThen(Effect.die(new Error("synthetic terminal cleanup defect")))),
          },
        },
      });
      const commandId = CommandId.make("router:archive-cleanup-failure");
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand]({
            type: "thread.archive",
            commandId,
            threadId: defaultThreadId,
          }),
        ),
      );
      yield* Context.get(context, EffectWorker.OrchestrationEffectWorkerV2).drain(10);
      const effects = yield* Context.get(context, EffectOutbox.EffectOutboxV2).listByCommandId(
        commandId,
      );
      assert.equal(attempts, 1);
      assert.equal(effects[0]?.request.type, "terminal.cleanup");
      assert.isFalse(effects[0]?.status === "succeeded");
      assert.isNotNull(
        (yield* Context.get(context, ThreadManagement.ThreadManagementService).getThreadProjection(
          defaultThreadId,
        )).thread.archivedAt,
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("prepares a visible V2 worktree thread before releasing its first run", () =>
    Effect.gen(function* () {
      const worktreeGit = makeRouterWorktreeGit();
      const setupStarted = yield* Deferred.make<void>();
      const setupReleased = yield* Deferred.make<void>();
      let context!: Context.Context<RouterV2Services>;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterProject(services);
            }),
          vcsDriver: { isInsideWorkTree: () => Effect.succeed(true) },
          gitVcsDriver: {
            execute: () =>
              Effect.succeed({ ...SUCCESSFUL_GIT_EXECUTION, stdout: "0123456789abcdef" }),
            createWorktree: worktreeGit.createWorktree,
          },
          gitManager: worktreeGit.gitManager,
          projectSetupScriptRunner: {
            runForThread: () =>
              Deferred.succeed(setupStarted, undefined).pipe(
                Effect.andThen(Deferred.await(setupReleased)),
                Effect.as({ status: "no-script" as const }),
              ),
          },
        },
      });
      const input = {
        ...routerLaunchInput("prepared"),
        workspaceStrategy: { type: "worktree" as const, baseRef: "main", branch: "feature/router" },
      };
      const launched = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.launchThread](input),
        ),
      );
      yield* Deferred.await(setupStarted);
      const before = yield* Context.get(
        context,
        ThreadManagement.ThreadManagementService,
      ).getThreadProjection(launched.threadId);
      assert.equal(before.messages[0]?.text, input.initialMessage.text);
      assert.equal(before.runs[0]?.status, "preparing");
      assert.equal(before.thread.worktreePath, worktreeGit.checkoutPath());
      yield* Deferred.succeed(setupReleased, undefined);
      yield* awaitRouterRunStatus(context, launched.threadId, "starting");
      const after = yield* Context.get(
        context,
        ThreadManagement.ThreadManagementService,
      ).getThreadProjection(launched.threadId);
      assert.equal(after.runs[0]?.status, "starting");
      assert.equal(after.messages.length, 1);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect.each([
    { caseName: "the origin remote is missing", hasOrigin: false },
    { caseName: "the base branch exists only locally", hasOrigin: true },
  ])("uses the local V2 worktree base when $caseName", ({ hasOrigin }) =>
    Effect.gen(function* () {
      const worktreeGit = makeRouterWorktreeGit();
      let context!: Context.Context<RouterV2Services>;
      let fetches = 0;
      let remoteChecks = 0;
      let createInput:
        | Parameters<GitVcsDriver.GitVcsDriver["Service"]["createWorktree"]>[0]
        | undefined;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterProject(services);
            }),
          vcsDriver: { isInsideWorkTree: () => Effect.succeed(true) },
          gitVcsDriver: {
            execute: () =>
              Effect.succeed({ ...SUCCESSFUL_GIT_EXECUTION, stdout: "0123456789abcdef" }),
            remoteExists: () => Effect.succeed(hasOrigin),
            fetchRemote: () =>
              Effect.sync(() => {
                fetches += 1;
              }),
            remoteBranchExists: () =>
              Effect.sync(() => {
                remoteChecks += 1;
                return false;
              }),
            resolveRemoteTrackingCommit: () =>
              Effect.die("An absent remote branch must not be resolved"),
            createWorktree: (input, options) =>
              Effect.sync(() => {
                createInput = input;
              }).pipe(Effect.andThen(worktreeGit.createWorktree(input, options))),
          },
          gitManager: worktreeGit.gitManager,
        },
      });
      const input = {
        ...routerLaunchInput("local-base"),
        workspaceStrategy: {
          type: "worktree" as const,
          baseRef: "main",
          branch: "feature/router",
          startFromOrigin: true,
        },
      };
      const launched = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.launchThread](input),
        ),
      );
      yield* awaitRouterRunStatus(context, launched.threadId, "starting");
      assert.equal(fetches, hasOrigin ? 1 : 0);
      assert.equal(remoteChecks, hasOrigin ? 1 : 0);
      assert.equal(createInput?.refName, "main");
      assert.equal(createInput?.baseRefName, "main");
      assert.equal(
        (yield* Context.get(context, ThreadManagement.ThreadManagementService).getThreadProjection(
          launched.threadId,
        )).thread.worktreePath,
        worktreeGit.checkoutPath(),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect.each([
    { caseName: "a non-repository", isRepository: false, failFetch: false },
    { caseName: "a base without a commit", isRepository: true, failFetch: false },
    { caseName: "a fetch failure", isRepository: true, failFetch: true },
  ])(
    "keeps the V2 failed worktree request visible and prevents provider start for $caseName",
    ({ isRepository, failFetch }) =>
      Effect.gen(function* () {
        let context!: Context.Context<RouterV2Services>;
        let worktrees = 0;
        yield* buildAppUnderTest({
          layers: {
            onV2Services: (services) =>
              Effect.gen(function* () {
                context = services;
                yield* seedRouterProject(services);
              }),
            vcsDriver: { isInsideWorkTree: () => Effect.succeed(isRepository) },
            gitVcsDriver: {
              execute: () =>
                Effect.succeed(
                  failFetch
                    ? { ...SUCCESSFUL_GIT_EXECUTION, stdout: "0123456789abcdef" }
                    : {
                        ...SUCCESSFUL_GIT_EXECUTION,
                        exitCode: ChildProcessSpawner.ExitCode(128),
                        stderr: "fatal: Needed a single revision",
                      },
                ),
              remoteExists: () => Effect.succeed(true),
              fetchRemote: (input) =>
                Effect.fail(
                  new GitCommandError({
                    operation: "synthetic.fetch",
                    command: "git",
                    cwd: input.cwd,
                    detail: "synthetic fetch failed",
                  }),
                ),
              createWorktree: () =>
                Effect.sync(() => {
                  worktrees += 1;
                }).pipe(Effect.andThen(Effect.die("Invalid worktree base must not provision"))),
            },
          },
        });
        const input = {
          ...routerLaunchInput("required-worktree"),
          workspaceStrategy: {
            type: "worktree" as const,
            baseRef: "main",
            branch: "feature/router",
            startFromOrigin: failFetch,
          },
        };
        const launched = yield* Effect.scoped(
          withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
            client[ORCHESTRATION_V2_WS_METHODS.launchThread](input),
          ),
        );
        yield* awaitRouterRunStatus(context, launched.threadId, "failed");
        const projection = yield* Context.get(
          context,
          ThreadManagement.ThreadManagementService,
        ).getThreadProjection(launched.threadId);
        assert.equal(projection.messages[0]?.text, input.initialMessage.text);
        assert.equal(projection.runs[0]?.status, "failed");
        assert.isNull(projection.thread.deletedAt);
        if (failFetch) {
          // A transient fetch failure proves no base defect: the accepted planned target
          // stays visible for diagnosis and retry, while nothing was created there.
          assert.isNotNull(launched.projection.thread.worktreePath);
          assert.equal(projection.thread.worktreePath, launched.projection.thread.worktreePath);
        } else assert.isNull(projection.thread.worktreePath);
        const outbox = yield* Context.get(context, EffectOutbox.EffectOutboxV2).listByCommandId(
          CommandId.make(input.commandId + ":release"),
        );
        assert.isFalse(outbox.some((effect) => effect.request.type === "provider-turn.start"));
        assert.equal(worktrees, 0);
        if (failFetch) {
          const replay = yield* Effect.scoped(
            withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
              client[ORCHESTRATION_V2_WS_METHODS.launchThread](input),
            ),
          );
          assert.isTrue(replay.resumed);
          assert.equal(replay.projection.runs[0]?.id, projection.runs[0]?.id);
          assert.equal(replay.projection.runs[0]?.status, "failed");
          assert.equal(
            replay.projection.thread.worktreePath,
            launched.projection.thread.worktreePath,
          );
          const replayed = yield* Context.get(context, EffectOutbox.EffectOutboxV2).listByCommandId(
            CommandId.make(input.commandId + ":release"),
          );
          assert.isFalse(replayed.some((effect) => effect.request.type === "provider-turn.start"));
          assert.equal(worktrees, 0);
        }
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps the V2 failed thread visible when worktree mode targets a non-repository", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      let worktrees = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterProject(services);
            }),
          vcsDriver: { isInsideWorkTree: () => Effect.succeed(false) },
          gitVcsDriver: {
            execute: () => Effect.succeed(SUCCESSFUL_GIT_EXECUTION),
            createWorktree: () =>
              Effect.sync(() => {
                worktrees += 1;
              }).pipe(
                Effect.andThen(Effect.die("Invalid worktree base must not create a worktree")),
              ),
          },
        },
      });
      const input = {
        ...routerLaunchInput("invalid-base"),
        workspaceStrategy: { type: "worktree" as const, baseRef: "main", branch: "feature/router" },
      };
      const launched = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.launchThread](input),
        ),
      );
      yield* awaitRouterRunStatus(context, launched.threadId, "failed");
      const projection = yield* Context.get(
        context,
        ThreadManagement.ThreadManagementService,
      ).getThreadProjection(launched.threadId);
      assert.equal(projection.messages[0]?.text, input.initialMessage.text);
      assert.equal(projection.runs[0]?.status, "failed");
      assert.isNull(projection.thread.deletedAt);
      assert.isNull(projection.thread.worktreePath);
      assert.equal(worktrees, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps the V2 failed thread visible when the worktree base has no commit", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      let worktrees = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterProject(services);
            }),
          vcsDriver: { isInsideWorkTree: () => Effect.succeed(true) },
          gitVcsDriver: {
            execute: () =>
              Effect.succeed({
                ...SUCCESSFUL_GIT_EXECUTION,
                exitCode: ChildProcessSpawner.ExitCode(128),
                stderr: "fatal: Needed a single revision",
              }),
            createWorktree: () =>
              Effect.sync(() => {
                worktrees += 1;
              }).pipe(
                Effect.andThen(Effect.die("Invalid worktree base must not create a worktree")),
              ),
          },
        },
      });
      const input = {
        ...routerLaunchInput("invalid-base"),
        workspaceStrategy: { type: "worktree" as const, baseRef: "main", branch: "feature/router" },
      };
      const launched = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.launchThread](input),
        ),
      );
      yield* awaitRouterRunStatus(context, launched.threadId, "failed");
      const projection = yield* Context.get(
        context,
        ThreadManagement.ThreadManagementService,
      ).getThreadProjection(launched.threadId);
      assert.equal(projection.messages[0]?.text, input.initialMessage.text);
      assert.equal(projection.runs[0]?.status, "failed");
      assert.isNull(projection.thread.deletedAt);
      assert.isNull(projection.thread.worktreePath);
      assert.equal(worktrees, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps the V2 first message visible and fails its run when setup cannot start", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterProject(services);
            }),
          projectSetupScriptRunner: {
            runForThread: (input) =>
              Effect.fail(
                new ProjectSetupScriptRunner.ProjectSetupScriptOperationError({
                  threadId: input.threadId,
                  worktreePath: input.worktreePath,
                  operation: "openTerminal",
                  cause: new Error("synthetic pty unavailable"),
                }),
              ),
          },
        },
      });
      const input = routerLaunchInput("setup-failure");
      const launched = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.launchThread](input),
        ),
      );
      yield* awaitRouterRunStatus(context, launched.threadId, "failed");
      const projection = yield* Context.get(
        context,
        ThreadManagement.ThreadManagementService,
      ).getThreadProjection(launched.threadId);
      assert.equal(projection.runs[0]?.status, "failed");
      assert.equal(projection.messages[0]?.text, input.initialMessage.text);
      assert.isNull(projection.thread.deletedAt);
      assert.isTrue(
        (yield* encodeTestJsonEffect(projection.turnItems)).includes("synthetic pty unavailable"),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps V2 preparation progress failure distinct from setup launch failure", () =>
    Effect.gen(function* () {
      let context!: Context.Context<RouterV2Services>;
      let setupCalls = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterProject(services);
            }),
          wrapThreadManagement: (service) => ({
            ...service,
            dispatch: (command) =>
              command.type === "prepared-run.progress" && command.phase === "setup"
                ? Effect.fail(
                    new Orchestrator.OrchestratorDispatchError({
                      commandId: command.commandId,
                      commandType: command.type,
                      cause: "synthetic preparation progress refused",
                    }),
                  )
                : service.dispatch(command),
          }),
          projectSetupScriptRunner: {
            runForThread: () =>
              Effect.sync(() => {
                setupCalls += 1;
                return { status: "no-script" as const };
              }),
          },
        },
      });
      const launched = yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          client[ORCHESTRATION_V2_WS_METHODS.launchThread](routerLaunchInput("progress-failure")),
        ),
      );
      yield* awaitRouterRunStatus(context, launched.threadId, "failed");
      const projection = yield* Context.get(
        context,
        ThreadManagement.ThreadManagementService,
      ).getThreadProjection(launched.threadId);
      assert.equal(setupCalls, 0);
      assert.equal(projection.runs[0]?.status, "failed");
      assert.isTrue(
        (yield* encodeTestJsonEffect(projection.turnItems)).includes(
          "synthetic preparation progress refused",
        ),
      );
      assert.isTrue((yield* encodeTestJsonEffect(projection.turnItems)).includes("update thread"));
      assert.isFalse(
        (yield* encodeTestJsonEffect(projection.turnItems)).includes("run setup script"),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect.each([
    {
      caseName: "async setup scripts let the turn start before the script exits",
      async: true,
      cancel: false,
    },
    {
      caseName: "sync setup scripts hold the turn until the script exits",
      async: false,
      cancel: false,
    },
    {
      caseName:
        "cancelling worktree setup publishes its outcome and keeps the failed thread visible",
      async: false,
      cancel: true,
    },
  ])("$caseName", ({ async, cancel }) =>
    Effect.gen(function* () {
      const worktreeGit = makeRouterWorktreeGit();
      const completionEntered = yield* Deferred.make<void>();
      const scriptExit = yield* Deferred.make<void>();
      let context!: Context.Context<RouterV2Services>;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterProject(services);
            }),
          vcsDriver: { isInsideWorkTree: () => Effect.succeed(true) },
          gitVcsDriver: {
            execute: () =>
              Effect.succeed({ ...SUCCESSFUL_GIT_EXECUTION, stdout: "0123456789abcdef" }),
            createWorktree: worktreeGit.createWorktree,
            removeWorktree: () => Effect.void,
          },
          gitManager: worktreeGit.gitManager,
          terminalManager: { close: () => Effect.void },
          projectSetupScriptRunner: {
            runForThread: (input) =>
              Effect.succeed({
                status: "started" as const,
                scriptId: "router:setup",
                scriptName: "Synthetic setup",
                scriptCommand: "synthetic",
                terminalId: "router:setup",
                cwd: input.worktreePath,
                async,
                completion: Deferred.succeed(completionEntered, undefined).pipe(
                  Effect.andThen(Deferred.await(scriptExit)),
                  Effect.as({ exitCode: 0, durationMs: 0 }),
                ),
              }),
          },
        },
      });
      const input = {
        ...routerLaunchInput("setup-timing"),
        workspaceStrategy: { type: "worktree" as const, baseRef: "main", branch: "feature/router" },
      };
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          Effect.gen(function* () {
            const launched = yield* client[ORCHESTRATION_V2_WS_METHODS.launchThread](input);
            yield* Deferred.await(completionEntered);
            const before = yield* Context.get(
              context,
              ThreadManagement.ThreadManagementService,
            ).getThreadProjection(launched.threadId);
            assert.equal(before.runs[0]?.status, async ? "starting" : "preparing");
            if (cancel) {
              assert.deepEqual(
                yield* client[WS_METHODS.worktreeSetupCancel]({ threadId: launched.threadId }),
                { cancelled: true },
              );
              yield* awaitRouterRunStatus(context, launched.threadId, "failed");
            } else {
              yield* Deferred.succeed(scriptExit, undefined);
              yield* awaitRouterRunStatus(context, launched.threadId, "starting");
            }
            const after = yield* Context.get(
              context,
              ThreadManagement.ThreadManagementService,
            ).getThreadProjection(launched.threadId);
            assert.isNull(after.thread.deletedAt);
            assert.equal(after.messages.length, 1);
            assert.equal(after.runs[0]?.status, cancel ? "failed" : "starting");
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "keeps a V2 failed worktree thread and its first message visible after a preparation defect",
    () =>
      Effect.gen(function* () {
        let context!: Context.Context<RouterV2Services>;
        yield* buildAppUnderTest({
          layers: {
            onV2Services: (services) =>
              Effect.gen(function* () {
                context = services;
                yield* seedRouterProject(services);
              }),
            vcsDriver: { isInsideWorkTree: () => Effect.succeed(true) },
            gitVcsDriver: {
              execute: () =>
                Effect.succeed({ ...SUCCESSFUL_GIT_EXECUTION, stdout: "0123456789abcdef" }),
              createWorktree: () => Effect.die(new Error("synthetic worktree exploded")),
            },
          },
        });
        const input = {
          ...routerLaunchInput("worktree-defect"),
          workspaceStrategy: {
            type: "worktree" as const,
            baseRef: "main",
            branch: "feature/router",
          },
        };
        const launched = yield* Effect.scoped(
          withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
            client[ORCHESTRATION_V2_WS_METHODS.launchThread](input),
          ),
        );
        yield* awaitRouterRunStatus(context, launched.threadId, "failed");
        const projection = yield* Context.get(
          context,
          ThreadManagement.ThreadManagementService,
        ).getThreadProjection(launched.threadId);
        assert.equal(projection.messages[0]?.text, input.initialMessage.text);
        assert.equal(projection.runs[0]?.status, "failed");
        assert.isNull(projection.thread.deletedAt);
        assert.isTrue(
          (yield* encodeTestJsonEffect(projection.turnItems)).includes(
            "synthetic worktree exploded",
          ),
        );
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("cancels V2 preparation and releases its created worktree and setup terminal", () =>
    Effect.gen(function* () {
      const worktreeGit = makeRouterWorktreeGit();
      const completionEntered = yield* Deferred.make<void>();
      const closes: string[] = [];
      const removals: string[] = [];
      let context!: Context.Context<RouterV2Services>;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterProject(services);
            }),
          vcsDriver: { isInsideWorkTree: () => Effect.succeed(true) },
          gitVcsDriver: {
            execute: () =>
              Effect.succeed({ ...SUCCESSFUL_GIT_EXECUTION, stdout: "0123456789abcdef" }),
            createWorktree: worktreeGit.createWorktree,
            removeWorktree: (input) =>
              Effect.sync(() => {
                removals.push(input.path);
              }).pipe(Effect.andThen(Effect.void)),
          },
          gitManager: worktreeGit.gitManager,
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                closes.push(input.terminalId ?? "all");
              }),
          },
          projectSetupScriptRunner: {
            runForThread: (input) =>
              Effect.succeed({
                status: "started" as const,
                scriptId: "router:setup",
                scriptName: "Synthetic setup",
                scriptCommand: "synthetic",
                terminalId: "router:setup",
                cwd: input.worktreePath,
                async: false,
                completion: Deferred.succeed(completionEntered, undefined).pipe(
                  Effect.andThen(Effect.never),
                ),
              }),
          },
        },
      });
      const input = {
        ...routerLaunchInput("cancel"),
        workspaceStrategy: { type: "worktree" as const, baseRef: "main", branch: "feature/router" },
      };
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          Effect.gen(function* () {
            const launched = yield* client[ORCHESTRATION_V2_WS_METHODS.launchThread](input);
            yield* Deferred.await(completionEntered);
            assert.deepEqual(
              yield* client[WS_METHODS.worktreeSetupCancel]({ threadId: launched.threadId }),
              { cancelled: true },
            );
            yield* awaitRouterRunStatus(context, launched.threadId, "failed");
            const projection = yield* Context.get(
              context,
              ThreadManagement.ThreadManagementService,
            ).getThreadProjection(launched.threadId);
            assert.equal(projection.messages[0]?.text, input.initialMessage.text);
            assert.equal(projection.runs[0]?.status, "failed");
            assert.isNull(projection.thread.deletedAt);
            assert.isNull(projection.thread.worktreePath);
          }),
        ),
      );
      assert.deepEqual(closes, ["router:setup"]);
      assert.deepEqual(removals, [worktreeGit.checkoutPath()]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("keeps the V2 failed thread visible when cancelled worktree cleanup fails", () =>
    Effect.gen(function* () {
      const worktreeGit = makeRouterWorktreeGit();
      const completionEntered = yield* Deferred.make<void>();
      const closes: string[] = [];
      const removals: string[] = [];
      let context!: Context.Context<RouterV2Services>;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (services) =>
            Effect.gen(function* () {
              context = services;
              yield* seedRouterProject(services);
            }),
          vcsDriver: { isInsideWorkTree: () => Effect.succeed(true) },
          gitVcsDriver: {
            execute: () =>
              Effect.succeed({ ...SUCCESSFUL_GIT_EXECUTION, stdout: "0123456789abcdef" }),
            createWorktree: worktreeGit.createWorktree,
            removeWorktree: (input) =>
              Effect.sync(() => {
                removals.push(input.path);
              }).pipe(
                Effect.andThen(
                  Effect.fail(
                    new GitCommandError({
                      operation: "synthetic.removeWorktree",
                      command: "git",
                      cwd: input.cwd,
                      detail: "synthetic cleanup refused",
                    }),
                  ),
                ),
              ),
          },
          gitManager: worktreeGit.gitManager,
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                closes.push(input.terminalId ?? "all");
              }),
          },
          projectSetupScriptRunner: {
            runForThread: (input) =>
              Effect.succeed({
                status: "started" as const,
                scriptId: "router:setup",
                scriptName: "Synthetic setup",
                scriptCommand: "synthetic",
                terminalId: "router:setup",
                cwd: input.worktreePath,
                async: false,
                completion: Deferred.succeed(completionEntered, undefined).pipe(
                  Effect.andThen(Effect.never),
                ),
              }),
          },
        },
      });
      const input = {
        ...routerLaunchInput("cancel"),
        workspaceStrategy: { type: "worktree" as const, baseRef: "main", branch: "feature/router" },
      };
      yield* Effect.scoped(
        withWsRpcClient(yield* getWsServerUrl("/ws"), (client) =>
          Effect.gen(function* () {
            const launched = yield* client[ORCHESTRATION_V2_WS_METHODS.launchThread](input);
            yield* Deferred.await(completionEntered);
            assert.deepEqual(
              yield* client[WS_METHODS.worktreeSetupCancel]({ threadId: launched.threadId }),
              { cancelled: true },
            );
            yield* awaitRouterRunStatus(context, launched.threadId, "failed");
            const projection = yield* Context.get(
              context,
              ThreadManagement.ThreadManagementService,
            ).getThreadProjection(launched.threadId);
            assert.equal(projection.messages[0]?.text, input.initialMessage.text);
            assert.equal(projection.runs[0]?.status, "failed");
            assert.isNull(projection.thread.deletedAt);
            assert.isNull(projection.thread.worktreePath);
          }),
        ),
      );
      assert.deepEqual(closes, ["router:setup"]);
      assert.deepEqual(removals, [worktreeGit.checkoutPath()]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc terminal methods", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-1");
      type OwnershipLease = Effect.Success<
        ReturnType<
          ThreadManagement.ThreadManagementService["Service"]["acquireOrdinaryWorktreeOwnership"]
        >
      >;
      const acquired: Array<{
        threadId: ThreadId;
        cwd: string | undefined;
        lease: OwnershipLease;
      }> = [];
      let workspaceRoot!: string;
      const snapshot = {
        threadId: "thread-1",
        terminalId: "default",
        cwd: "",
        worktreePath: null,
        status: "running" as const,
        pid: 1234,
        history: "",
        exitCode: null,
        exitSignal: null,
        label: "Primary",
        updatedAt: "2026-01-01T00:00:00.000Z",
      };

      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              const project = yield* seedRouterProject(context);
              workspaceRoot = project.workspaceRoot;
              snapshot.cwd = workspaceRoot;
              yield* Context.get(context, ThreadManagement.ThreadManagementService).dispatch(
                routerThreadCreate(threadId),
              );
            }),
          wrapThreadManagement: (service) => ({
            ...service,
            acquireOrdinaryWorktreeOwnership: (ownerThreadId, cwd) =>
              service.acquireOrdinaryWorktreeOwnership(ownerThreadId, cwd).pipe(
                Effect.tap((lease) =>
                  Effect.sync(() => {
                    acquired.push({ threadId: ownerThreadId, cwd, lease });
                  }),
                ),
              ),
          }),
          terminalManager: {
            open: () =>
              Effect.sync(() => {
                assert.equal(acquired.length, 1);
                return snapshot;
              }),
            attachStream: (input, listener) =>
              Effect.gen(function* () {
                assert.equal(acquired.length, 3);
                assert.equal(input.cwd, input.restartIfNotRunning ? undefined : workspaceRoot);
                yield* listener({ type: "snapshot", snapshot });
                return () => {};
              }),
            write: () => Effect.void,
            resize: () => Effect.void,
            clear: () => Effect.void,
            restart: () =>
              Effect.sync(() => {
                assert.equal(acquired.length, 2);
                return snapshot;
              }),
            close: () => Effect.void,
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");

      const opened = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalOpen]({
            threadId: "thread-1",
            terminalId: "default",
            cwd: workspaceRoot,
          }),
        ),
      );
      assert.equal(opened.terminalId, "default");

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalWrite]({
            threadId: "thread-1",
            terminalId: "default",
            data: "echo hi\n",
          }),
        ),
      );

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalResize]({
            threadId: "thread-1",
            terminalId: "default",
            cols: 120,
            rows: 40,
          }),
        ),
      );

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalClear]({
            threadId: "thread-1",
            terminalId: "default",
          }),
        ),
      );

      const restarted = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalRestart]({
            threadId: "thread-1",
            terminalId: "default",
            cwd: workspaceRoot,
            cols: 120,
            rows: 40,
          }),
        ),
      );
      assert.equal(restarted.terminalId, "default");
      assert.deepEqual(
        acquired.map((entry) => entry.threadId),
        [threadId, threadId],
      );

      const attached = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalAttach]({
            threadId,
            terminalId: "default",
            cwd: workspaceRoot,
          }).pipe(Stream.runHead),
        ),
      );
      assertTrue(Option.isSome(attached) && attached.value.type === "snapshot");
      assert.equal(attached.value.snapshot.terminalId, "default");
      const attachedWithoutCwd = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalAttach]({
            threadId,
            terminalId: "default",
            restartIfNotRunning: true,
          }).pipe(Stream.runHead),
        ),
      );
      assertTrue(Option.isSome(attachedWithoutCwd) && attachedWithoutCwd.value.type === "snapshot");
      assert.equal(attachedWithoutCwd.value.snapshot.terminalId, "default");
      assert.deepEqual(
        acquired.map(({ threadId, cwd }) => ({ threadId, cwd })),
        [
          { threadId, cwd: workspaceRoot },
          { threadId, cwd: workspaceRoot },
          { threadId, cwd: workspaceRoot },
        ],
      );
      const firstLease = acquired[0]!.lease;
      for (const { lease } of acquired) {
        assert.equal(lease.resourcePath, workspaceRoot);
        assert.equal(lease.ownerThreadId, threadId);
        assert.equal(lease.leaseId, firstLease.leaseId);
        assert.equal(lease.ownerIncarnation, firstLease.ownerIncarnation);
        assert.equal(lease.acquiredAtMs, firstLease.acquiredAtMs);
        assert.equal(lease.branch, null);
      }

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalClose]({
            threadId: "thread-1",
            terminalId: "default",
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc terminal.write errors", () =>
    Effect.gen(function* () {
      const terminalError = new TerminalNotRunningError({
        threadId: "thread-1",
        terminalId: "default",
      });
      yield* buildAppUnderTest({
        layers: {
          terminalManager: {
            write: () => Effect.fail(terminalError),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalWrite]({
            threadId: "thread-1",
            terminalId: "default",
            data: "echo fail\n",
          }),
        ).pipe(Effect.result),
      );

      assertFailure(result, terminalError);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );
});

it.live(
  "reports thread HTTP and WebSocket transfer budgets",
  () =>
    Effect.gen(function* () {
      const runs = yield* Effect.forEach(
        [ProviderDriverKind.make("codex"), ProviderDriverKind.make("claudeAgent")],
        (provider) => {
          const sqlCounter = makeSqlStatementCounter();
          const providerInstance = makeRouterProviderInstance(provider);
          const modelSelection = {
            instanceId: providerInstance.instanceId,
            model: provider === "codex" ? "gpt-5-codex" : "claude-sonnet-4",
          };
          return Effect.gen(function* () {
            let services!: Context.Context<RouterV2Services>;
            const dynamicResult = (ordinal: number, bytes: number): OrchestrationV2TurnItem => {
              const { input: _input, output: _output, ...base } = routerTurnItem(ordinal);
              return {
                ...base,
                type: "dynamic_tool",
                toolName: "synthetic.result",
                input: {},
                output: diagnosticOutput({
                  provider,
                  turnIndex: ordinal,
                  toolIndex: 0,
                  targetBytes: bytes,
                }),
              };
            };
            const history: Array<OrchestrationV2TurnItem> = [];
            for (let turn = 0; turn < TRANSFER_HISTORY_TURN_COUNT; turn += 1) {
              for (let tool = 0; tool < TRANSFER_HISTORY_TOOLS_PER_TURN; tool += 1) {
                history.push(
                  routerTurnItem(history.length, {
                    title: "History command",
                    output: "Retained command output",
                  }),
                );
              }
              history.push(dynamicResult(history.length, TRANSFER_HISTORY_MCP_RESULT_BYTES));
            }
            yield* buildAppUnderTest({
              layers: {
                providerInstanceRegistry: {
                  getInstance: (id) =>
                    Effect.succeed(
                      id === providerInstance.instanceId ? providerInstance : undefined,
                    ),
                  listInstances: Effect.succeed([providerInstance]),
                },
                onV2Services: (context) =>
                  Effect.gen(function* () {
                    services = context;
                    yield* seedRouterProject(context);
                    yield* Context.get(context, ThreadManagement.ThreadManagementService).dispatch({
                      ...routerThreadCreate(),
                      modelSelection,
                    });
                    yield* writeRouterItems(context, history, "transfer-history");
                  }),
              },
            });
            const threads = Context.get(services, ThreadManagement.ThreadManagementService);
            const baseUrl = yield* getHttpServerUrl();
            const { cookie, url } = parseSessionCookieFromWsUrl(yield* getWsServerUrl("/ws"));
            assert.isDefined(cookie);
            const threadSnapshot = yield* measureHttpGet({
              url: `${baseUrl}/api/orchestration/threads/${defaultThreadId}`,
              headers: {
                cookie: cookie!,
                [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
              },
            });
            assert.equal(threadSnapshot.status, 200);
            assert.equal(threadSnapshot.contentEncoding, "gzip");
            const decodedThread = yield* decodeTransferThreadSnapshot(
              Buffer.from(threadSnapshot.decodedBody).toString("utf8"),
            );
            assert.equal(decodedThread.projection.turnItems.length, history.length);
            const shellSnapshot = yield* measureHttpGet({
              url: `${baseUrl}/api/orchestration/shell`,
              headers: {
                cookie: cookie!,
                [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
              },
            });
            assert.equal(shellSnapshot.status, 200);
            const decodedShell = yield* decodeTransferShellSnapshot(
              Buffer.from(shellSnapshot.decodedBody).toString("utf8"),
            );
            assert.equal(decodedShell.threads.length, 1);
            const threadClient = yield* openMeasuredWsClient({ url, cookie: cookie! });
            const shellClient = yield* openMeasuredWsClient({ url, cookie: cookie! });
            const secondClient = yield* openMeasuredWsClient({ url, cookie: cookie! });
            assert.include(threadClient.recorder.negotiatedExtensions(), "permessage-deflate");
            const subscribeThread = Effect.fnUntraced(function* (
              measured: typeof threadClient,
              afterSequence: number,
            ) {
              const queue = yield* Queue.unbounded<OrchestrationV2ThreadStreamItem>();
              yield* measured.client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
                threadId: defaultThreadId,
                afterSequence,
                requestCompletionMarker: true,
              }).pipe(
                Stream.runForEach((item) => Queue.offer(queue, item)),
                Effect.forkIn(measured.scope),
              );
              return queue;
            });
            const subscribeShell = Effect.fnUntraced(function* (
              measured: typeof shellClient,
              afterSequence: number,
            ) {
              const queue = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
              yield* measured.client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
                afterSequence,
                requestCompletionMarker: true,
              }).pipe(
                Stream.runForEach((item) => Queue.offer(queue, item)),
                Effect.forkIn(measured.scope),
              );
              return queue;
            });
            const synchronized = <A extends { readonly kind: string }>(
              queue: Queue.Queue<A>,
              isSnapshotReset: (item: A) => boolean = (item) => item.kind === "snapshot",
            ) =>
              Effect.gen(function* () {
                let mode: "replay" | "snapshot" = "replay";
                while (true) {
                  const item = yield* Queue.take(queue);
                  if (isSnapshotReset(item)) mode = "snapshot";
                  if (item.kind === "synchronized") return mode;
                }
              }).pipe(Effect.timeout("10 seconds"));
            // A shell cursor resume leads with a metadata-only refresh for repository
            // identities that already resolved. Only an authoritative shell body, which
            // omits resolved roots, resets the cursor.
            const isShellSnapshotReset = (item: OrchestrationV2ShellStreamItem) =>
              item.kind === "snapshot" &&
              (item.resolvedRepositoryIdentityRoots === undefined ||
                item.snapshot.threads.length > 0 ||
                item.snapshot.archivedThreads.length > 0);
            const reach = <A>(queue: Queue.Queue<A>, predicate: (item: A) => boolean) =>
              Effect.gen(function* () {
                while (!predicate(yield* Queue.take(queue))) {}
              }).pipe(Effect.timeout("10 seconds"));
            const threadItems = yield* subscribeThread(
              threadClient,
              decodedThread.snapshotSequence,
            );
            const shellItems = yield* subscribeShell(shellClient, decodedShell.snapshotSequence);
            const secondThreadItems = yield* subscribeThread(
              secondClient,
              decodedThread.snapshotSequence,
            );
            const secondShellItems = yield* subscribeShell(
              secondClient,
              decodedShell.snapshotSequence,
            );
            for (const queue of [threadItems, secondThreadItems])
              assert.equal(yield* synchronized(queue), "replay");
            for (const queue of [shellItems, secondShellItems])
              assert.equal(yield* synchronized(queue, isShellSnapshotReset), "replay");
            const threadStart = threadClient.recorder.totals();
            const shellStart = shellClient.recorder.totals();
            const secondStart = secondClient.recorder.totals();
            const sqlStart = sqlCounter.count();
            const measured: Array<OrchestrationV2TurnItem> = Array.from(
              { length: TRANSFER_MEASURED_TOOLS },
              (_, tool) =>
                routerTurnItem(history.length + tool, {
                  title: "Measured command",
                  output: "Retained measured output",
                }),
            );
            measured.push(
              dynamicResult(history.length + measured.length, TRANSFER_MEASURED_MCP_RESULT_BYTES),
            );
            const committed = yield* writeRouterItems(services, measured, "transfer-measured");
            const sequence = committed.at(-1)!.sequence;
            for (const queue of [threadItems, secondThreadItems]) {
              yield* reach(queue, (item) => item.kind === "event" && item.sequence === sequence);
            }
            for (const queue of [shellItems, secondShellItems]) {
              yield* reach(
                queue,
                (item) => item.kind === "thread.updated" && item.sequence >= sequence,
              );
            }
            const delta = (
              start: ReturnType<typeof threadClient.recorder.totals>,
              end: ReturnType<typeof threadClient.recorder.totals>,
            ) => ({
              wireBytes: end.wireBytes - start.wireBytes,
              decodedBytes: end.decodedBytes - start.decodedBytes,
              messages: end.messages - start.messages,
            });
            const measuredTurnWebSocket = delta(threadStart, threadClient.recorder.totals());
            const measuredTurnShellWebSocket = delta(shellStart, shellClient.recorder.totals());
            const measuredTurnSecondClientWebSocket = delta(
              secondStart,
              secondClient.recorder.totals(),
            );
            const measuredTurnSqlStatements = sqlCounter.count() - sqlStart;
            yield* secondClient.close;
            const reconnectSqlStart = sqlCounter.count();
            const reconnected = yield* openMeasuredWsClient({ url, cookie: cookie! });
            const reconnectStart = reconnected.recorder.totals();
            const reconnectThreadMode = yield* synchronized(
              yield* subscribeThread(reconnected, decodedThread.snapshotSequence),
            );
            const reconnectThreadEnd = reconnected.recorder.totals();
            const reconnectShellMode = yield* synchronized(
              yield* subscribeShell(reconnected, decodedShell.snapshotSequence),
              isShellSnapshotReset,
            );
            const reconnectShellEnd = reconnected.recorder.totals();
            const finalProjection = yield* threads.getThreadProjection(defaultThreadId);
            assert.equal(
              finalProjection.thread.modelSelection.instanceId,
              providerInstance.instanceId,
            );
            assert.equal(finalProjection.turnItems.length, history.length + measured.length);
            const retained = finalProjection.turnItems.find(
              (item) => item.id === measured.at(-1)!.id,
            );
            assertTrue(retained?.type === "dynamic_tool");
            assert.equal(
              Buffer.byteLength(String(retained.output), "utf8"),
              TRANSFER_MEASURED_MCP_RESULT_BYTES,
            );
            assert.isBelow(measuredTurnWebSocket.decodedBytes, TRANSFER_MEASURED_MCP_RESULT_BYTES);
            return {
              provider,
              threadSnapshot,
              shellSnapshot,
              measuredTurnWebSocket,
              measuredTurnShellWebSocket,
              measuredTurnSecondClientWebSocket,
              measuredTurnSqlStatements,
              reconnectThread: {
                mode: reconnectThreadMode,
                ...delta(reconnectStart, reconnectThreadEnd),
              },
              reconnectShell: {
                mode: reconnectShellMode,
                ...delta(reconnectThreadEnd, reconnectShellEnd),
              },
              reconnectSqlStatements: sqlCounter.count() - reconnectSqlStart,
            } satisfies TransferBudgetRun;
          }).pipe(
            Effect.scoped,
            Effect.provideService(Tracer.Tracer, sqlCounter.tracer),
            Effect.provide(NodeHttpServerTestWithWsDeflate),
          );
        },
        { concurrency: 1 },
      );
      yield* Effect.logInfo("\n" + formatTransferBudgetReport(runs));
      const reportPath = yield* Config.String("T3CODE_TRANSFER_BUDGET_REPORT_PATH").pipe(
        Config.option,
      );
      if (Option.isSome(reportPath))
        yield* (yield* FileSystem.FileSystem).writeFileString(
          reportPath.value,
          formatTransferBudgetReport(runs),
        );
      const resultPath = yield* Config.String("T3CODE_TRANSFER_BUDGET_RESULT_PATH").pipe(
        Config.option,
      );
      if (Option.isSome(resultPath))
        yield* (yield* FileSystem.FileSystem).writeFileString(
          resultPath.value,
          formatTransferBudgetResult(runs),
        );
      assert.deepEqual(transferBudgetViolations(runs), []);
    }).pipe(Effect.provide(NodeServices.layer)),
  120_000,
);

const nativeCreationWsFixture = (
  runSetupScript = false,
  projectCwd = "/synthetic/project",
  operationId = "synthetic-native-ws",
) => {
  const binding = Schema.decodeUnknownSync(NativePreparationBinding)({
    backend_instance: "synthetic-backend",
    environment_id: "synthetic-env",
    project_id: defaultProjectId,
    project_cwd: projectCwd,
    account_ref: "synthetic-account",
    runtime_mode: "full-access",
    interaction_mode: "default",
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: runSetupScript,
    provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
  });
  const command = nativePreparationCommand(
    operationId,
    binding,
    "Synthetic native WS text",
    "Synthetic thread",
    "2026-10-02T12:00:00Z",
  );
  const preparation = nativeCreationCanonicalJson({
    schema: "voice.t3-bootstrap-preparation/v1",
    operation_id: operationId,
    preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
    binding,
    command,
    binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
    prompt_digest: nativeCreationSha256(command.message.text),
    command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
  });
  const historical = Schema.decodeUnknownSync(NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "synthetic-account-binding",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  return {
    binding,
    command,
    preparation,
    historical,
    submission: {
      schema: "t3.native-bootstrap-submission/v1" as const,
      preparationBase64: Buffer.from(preparation).toString("base64"),
      creationGuard: {
        schema: "t3.native-creation-guard/v1" as const,
        grantId: "synthetic-native-grant",
        grantRevision: 1,
      },
    },
  };
};
const qualifiedNativeRouterFixture = Effect.fnUntraced(function* (
  name: string,
  runSetupScript: boolean,
) {
  const fs = yield* FileSystem.FileSystem;
  const workspaceRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-router-native-" });
  const fixture = nativeCreationWsFixture(
    runSetupScript,
    workspaceRoot,
    `synthetic-native-${name}`,
  );
  let config!: ServerConfig.ServerConfig["Service"];
  let services!: Context.Context<RouterV2Services>;
  let repository!: NativeCreationRepository["Service"];
  let sql!: SqlClient.SqlClient;
  const stages: OrchestrationV2Command[] = [];
  const order: string[] = [];
  const nativeCalls: string[] = [];
  const detachments: Array<{ threadId: ThreadId; status: string }> = [];
  const closeCalls: Array<Parameters<TerminalManager.TerminalManager["Service"]["close"]>[0]> = [];
  const expectedCommands = () => {
    const original = fixture.command;
    const initial = original.bootstrap.createThread;
    const worktreePath = nativeWorktreePath({
      worktreesDir: config.worktreesDir,
      cwd: workspaceRoot,
      branch: fixture.historical.requestedBranch,
    });
    return [
      Schema.decodeUnknownSync(OrchestrationV2Command)({
        type: "thread.create",
        commandId: `${original.commandId}:native:v2:create`,
        threadId: original.threadId,
        projectId: initial.projectId,
        title: initial.title,
        modelSelection: initial.modelSelection,
        runtimeMode: initial.runtimeMode,
        interactionMode: initial.interactionMode,
        branch: fixture.historical.requestedBranch,
        worktreePath,
        createdBy: "user",
        creationSource: "server",
      }),
      Schema.decodeUnknownSync(OrchestrationV2Command)({
        type: "message.dispatch",
        commandId: `${original.commandId}:native:v2:message`,
        threadId: original.threadId,
        messageId: original.message.messageId,
        text: original.message.text,
        attachments: [],
        modelSelection: initial.modelSelection,
        dispatchMode: { type: "defer_start" },
        createdBy: "user",
        creationSource: "server",
      }),
      Schema.decodeUnknownSync(OrchestrationV2Command)({
        type: "prepared-run.release",
        commandId: original.commandId,
        threadId: original.threadId,
        runId: Context.get(services, IdAllocator.IdAllocatorV2).derive.run({
          threadId: ThreadId.make(original.threadId),
          ordinal: 1,
        }),
      }),
    ];
  };
  const assertReservedInventory = Effect.fnUntraced(function* () {
    const expected = expectedCommands();
    const identities = yield* sql<{ command_id: string; claim_id: string; thread_id: string }>`
      SELECT command_id, claim_id, thread_id FROM native_creation_reserved_command_identities ORDER BY command_id`;
    assert.deepEqual(
      identities.map((row) => row.command_id),
      expected.map((command) => command.commandId).sort(),
    );
    assert.isTrue(
      identities.every(
        (row) =>
          row.claim_id === `native:v2:${fixture.command.commandId}` &&
          row.thread_id === fixture.command.threadId,
      ),
    );
    for (const command of expected) {
      const reservation = yield* repository.getReservedCommand(command.commandId);
      assertTrue(Option.isSome(reservation));
      assert.equal(reservation.value.claimId, `native:v2:${fixture.command.commandId}`);
      assert.equal(reservation.value.threadId, fixture.command.threadId);
      assert.equal(reservation.value.commandDigest, nativeCreationV2CommandDigest(command));
      assert.deepEqual(
        yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2Command))(
          reservation.value.canonicalCommand,
        ),
        command,
      );
    }
  });
  const checkNativeStart = (
    operation: string,
    execution: Parameters<
      ProviderInstance["orchestrationAdapter"]["openSession"]
    >[0]["nativeCreationExecution"],
  ) =>
    Effect.gen(function* () {
      assertTrue(execution !== undefined);
      const reference = getNativeCreationExecutionReference(execution.context);
      assertTrue(reference !== null);
      assert.equal(reference.claimId, `native:v2:${fixture.command.commandId}`);
      assert.equal(reference.stageCommandId, fixture.command.commandId);
      const history = yield* repository.readHistoryByClaim(reference.claimId);
      const starts = history.effectsV2.filter(
        (fact) => fact.effectId === reference.effectId && fact.phase === "started",
      );
      assert.equal(starts.length, 1);
      assert.equal(starts[0]!.commandId, reference.stageCommandId);
      assert.equal(starts[0]!.commandDigest, nativeCreationV2CommandDigest(expectedCommands()[2]!));
      assert.isFalse(
        history.effectsV2.some(
          (fact) => fact.effectId === reference.effectId && fact.phase === "completed",
        ),
      );
      yield* authorizeNativeCreationExecution(execution.context, {
        stage: "native_command",
        resources: execution.resources,
      });
      nativeCalls.push(operation);
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterProtocolError({
            driver: ProviderDriverKind.make("codex"),
            detail: "Synthetic native callee could not validate its original committed start",
            cause,
          }),
      ),
    );
  const baseInstance = makeRouterProviderInstance();
  const nativeSnapshot = yield* Schema.decodeEffect(ServerProvider)({
    instanceId: baseInstance.instanceId,
    driver: baseInstance.driverKind,
    enabled: true,
    installed: false,
    version: null,
    status: "ready",
    auth: { status: "unknown" },
    checkedAt: "2026-10-02T12:00:00Z",
    supportedRuntimeModes: ["full-access", "approval-required"],
    models: [
      {
        slug: fixture.binding.provider_model_selection.model,
        name: "Synthetic native fixture model",
        isCustom: true,
        capabilities: null,
      },
    ],
    skills: [],
    slashCommands: [],
  });
  const nativeInstance: ProviderInstance = {
    instanceId: baseInstance.instanceId,
    driverKind: baseInstance.driverKind,
    enabled: true,
    displayName: baseInstance.displayName,
    continuationIdentity: baseInstance.continuationIdentity,
    snapshot: {
      getSnapshot: Effect.succeed(nativeSnapshot),
      refresh: Effect.die("Native fixture must not probe a live provider"),
      streamChanges: Stream.die("Native fixture must not watch a live provider"),
      applyUsageLimits: () => Effect.die("Native fixture must not mutate provider usage limits"),
      resolveMaintenance: () =>
        Effect.die("Native fixture must not resolve live provider maintenance"),
    },
    get textGeneration(): never {
      throw new Error("Native fixture must not generate provider text");
    },
    orchestrationAdapter: {
      ...baseInstance.orchestrationAdapter,
      openSession: (input) =>
        Effect.gen(function* () {
          yield* checkNativeStart("open_session", input.nativeCreationExecution);
          const now = yield* DateTime.now;
          const runtime: ProviderAdapterV2SessionRuntime = {
            ...(input.nativeOperation?.runtimeGeneration === undefined
              ? {}
              : { runtimeGeneration: input.nativeOperation.runtimeGeneration }),
            instanceId: input.modelSelection.instanceId,
            driver: ProviderDriverKind.make("codex"),
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver: ProviderDriverKind.make("codex"),
              providerInstanceId: input.modelSelection.instanceId,
              status: "ready",
              cwd: input.runtimePolicy.cwd ?? workspaceRoot,
              model: input.modelSelection.model,
              capabilities: CodexProviderCapabilitiesV2,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.never,
            ensureThread: (request) =>
              checkNativeStart("ensure_thread", request.nativeCreationExecution).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    assertTrue(request.existingProviderThread !== undefined);
                    return {
                      ...request.existingProviderThread,
                      providerSessionId: input.providerSessionId,
                      nativeThreadRef: {
                        driver: ProviderDriverKind.make("codex"),
                        nativeId: `synthetic-native-thread:${name}`,
                        strength: "strong" as const,
                      },
                    };
                  }),
                ),
              ),
            resumeThread: (request) =>
              checkNativeStart("resume_thread", request.nativeCreationExecution).pipe(
                Effect.as(request.providerThread),
              ),
            startTurn: (request) => checkNativeStart("start_turn", request.nativeCreationExecution),
            steerTurn: () => Effect.die("Native bootstrap must not steer a turn"),
            interruptTurn: () => Effect.void,
            respondToRuntimeRequest: () =>
              Effect.die("Native bootstrap must not respond to a provider request"),
            readThreadSnapshot: () => Effect.die("Native bootstrap must not read native history"),
            rollbackThread: () => Effect.die("Native bootstrap must not roll back native history"),
            forkThread: () => Effect.die("Native bootstrap must not fork native history"),
          };
          return runtime;
        }),
    },
  };
  config = yield* buildAppUnderTest({
    config: { cwd: workspaceRoot },
    layers: {
      serverEnvironment: { getDescriptor: Effect.succeed(nativeRouterEnvironmentDescriptor) },
      onNativeCreationServices: (current, database) =>
        Effect.sync(() => {
          repository = current;
          sql = database;
        }),
      onV2Services: (context) =>
        Effect.gen(function* () {
          services = context;
          yield* seedRouterProject(context, defaultProjectId, workspaceRoot);
        }),
      nativeCreationGrantResolver: {
        resolveCurrent: ({ actorSessionId, guard }) =>
          Effect.succeed({
            enrolledSessionId: actorSessionId,
            trustedIssuerId: "synthetic-issuer",
            grant: {
              grantId: guard.grantId,
              revision: guard.grantRevision,
              actorSessionId,
              issuerId: "synthetic-issuer",
              expiresAt: DateTime.makeUnsafe("2099-01-01T00:00:00Z"),
              revoked: false,
              operationId: `synthetic-native-${name}`,
              preparationId: fixture.command.commandId.replace(
                "voice-command-",
                "voice-bootstrap-",
              ),
              preparationSha256: nativeCreationSha256(fixture.preparation),
              bindingDigest: nativeCreationSha256(nativeCreationCanonicalJson(fixture.binding)),
              binding: fixture.historical,
              resources: {
                projectCwd: workspaceRoot,
                branch: fixture.historical.requestedBranch,
                worktreePath: nativeWorktreePath({
                  worktreesDir: config.worktreesDir,
                  cwd: workspaceRoot,
                  branch: fixture.historical.requestedBranch,
                }),
              },
              allowedStages: [
                "claim",
                "normalization",
                "tracker_registration",
                "bootstrap_detachment",
                "fetch",
                "worktree",
                "worktree_ownership",
                "native_command",
                "setup",
                "setup_detachment",
                "setup_completion_detachment",
                "cleanup",
                "deletion_drain",
                "git_status_refresh",
              ],
              recoveryScopes: [],
            },
          }),
      },
      nativeCreationBindingResolver: { resolveCurrent: () => Effect.succeed(fixture.historical) },
      providerInstanceRegistry: {
        getInstance: (id) =>
          Effect.succeed(id === nativeInstance.instanceId ? nativeInstance : undefined),
        listInstances: Effect.succeed([nativeInstance]),
      },
      providerAuth: { tryHandlePromptCommand: () => Effect.succeed(false) },
      terminalManager: {
        close: (input) =>
          Effect.sync(() => {
            closeCalls.push(input);
          }),
      },
      projectSetupScriptRunner: {
        runForThread: ({ worktreePath }) =>
          Effect.sync(() => {
            order.push("setup");
            return {
              status: "started" as const,
              scriptId: "synthetic-setup",
              scriptName: "Synthetic setup",
              scriptCommand: "synthetic",
              terminalId: "synthetic-setup-terminal",
              cwd: worktreePath,
              async: false,
              completion: Effect.succeed({ exitCode: 0, durationMs: 1 }),
            };
          }),
      },
      vcsDriver: { isInsideWorkTree: () => Effect.succeed(true) },
      gitVcsDriver: {
        execute: () => Effect.succeed(SUCCESSFUL_GIT_EXECUTION),
        createWorktree: (input) =>
          Effect.gen(function* () {
            yield* assertReservedInventory();
            const history = Option.getOrThrow(
              yield* repository.readHistory(fixture.command.commandId),
            );
            assert.isTrue(
              history.effects.some(
                (fact) =>
                  fact.kind === "worktree" &&
                  fact.phase === "started" &&
                  fact.worktreePath === input.path,
              ),
            );
            assert.deepEqual(nativeCalls, []);
            order.push("checkout");
            assertTrue(typeof input.path === "string");
            yield* fs.makeDirectory(input.path, { recursive: true });
            return { worktree: { refName: fixture.historical.requestedBranch, path: input.path } };
          }).pipe(
            Effect.mapError(
              (cause) =>
                new GitCommandError({
                  operation: "createWorktree",
                  command: "synthetic fixture checkout",
                  cwd: workspaceRoot,
                  detail: "Synthetic SQL or fixture checkout failed",
                  cause,
                }),
            ),
          ),
      },
      wrapThreadManagement: (service) => ({
        ...service,
        readCurrentThreadRuntimeAttachment: (threadId) =>
          service.readCurrentThreadRuntimeAttachment(threadId).pipe(
            Effect.tap((attachment) =>
              Effect.sync(() => {
                detachments.push({ threadId, status: attachment.status });
              }),
            ),
          ),
        dispatchNativeCreationStage: (command, input) =>
          Effect.gen(function* () {
            yield* assertReservedInventory();
            assert.deepEqual(command, expectedCommands()[stages.length]);
            const before = yield* repository.readHistoryByClaim(input.claimId);
            assert.deepEqual(before.effectsV2, []);
            assert.deepEqual(nativeCalls, []);
            stages.push(command);
            order.push(command.type);
            const result = yield* service.dispatchNativeCreationStage(command, input);
            const receipt = (yield* Context.get(
              services,
              EventSink.EventSinkV2,
            ).readCommandReceiptIdentity(command.commandId)).receipt;
            assert.equal(receipt?.status, "accepted");
            assert.equal(receipt?.commandType, command.type);
            assert.equal(receipt?.resultSequence, result.sequence);
            return result;
          }).pipe(
            Effect.mapError((cause) =>
              Schema.is(NativeCreationAuthorityError)(cause)
                ? cause
                : new NativeCreationAuthorityError({
                    code: "unresolved_claim",
                    message: "Synthetic stage reservation or acceptance observer failed",
                  }),
            ),
          ),
      }),
    },
  });
  const url = yield* getWsServerUrl("/ws");
  const sessions = yield* sql<{
    sessionId: string;
  }>`SELECT session_id AS "sessionId" FROM auth_sessions`;
  assert.equal(sessions.length, 1);
  yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES (${sessions[0]!.sessionId}, '2026-10-02T12:00:00Z')`;
  return {
    fixture,
    config,
    services,
    repository,
    sql,
    url,
    stages,
    order,
    nativeCalls,
    detachments,
    closeCalls,
    expectedCommands,
    assertReservedInventory,
  };
});

const nativeWsFixture = nativeCreationWsFixture();
it.effect(
  "actual guarded WS capability and unavailable authority produce zero bootstrap commands",
  () =>
    Effect.gen(function* () {
      let dispatches = 0;
      let capturedSql!: SqlClient.SqlClient;
      yield* buildAppUnderTest({
        layers: {
          serverEnvironment: { getDescriptor: Effect.succeed(nativeRouterEnvironmentDescriptor) },
          onNativeCreationServices: (_repository, sql) =>
            Effect.sync(() => {
              capturedSql = sql;
            }),
          wrapThreadManagement: (service) => ({
            ...service,
            dispatch: (command) =>
              Effect.sync(() => {
                dispatches++;
              }).pipe(Effect.andThen(service.dispatch(command))),
            dispatchNativeCreationStage: (command, input) =>
              Effect.sync(() => {
                dispatches++;
              }).pipe(Effect.andThen(service.dispatchNativeCreationStage(command, input))),
          }),
        },
      });
      const url = yield* getWsServerUrl("/ws");
      const sessions = yield* capturedSql<{
        sessionId: string;
      }>`SELECT session_id AS "sessionId" FROM auth_sessions`;
      assert.equal(sessions.length, 1);
      yield* capturedSql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES (${sessions[0]!.sessionId}, '2026-10-02T12:00:00Z')`;
      yield* withWsRpcClient(url, (client) =>
        Effect.gen(function* () {
          const config = yield* client[WS_METHODS.serverGetConfig]({});
          assert.deepEqual(config.environment.capabilities.nativeBootstrapCreation, {
            submissionSchema: "t3.native-bootstrap-submission/v1",
            preparationSchema: "voice.t3-bootstrap-preparation/v1",
            observationSchema: "t3.native-creation-observation/v2",
            guardRequired: true,
          });
          const result = yield* client[ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap](
            nativeWsFixture.submission,
          ).pipe(Effect.result);
          assert.equal(result._tag, "Failure");
          assertTrue(result._tag === "Failure");
          assert.equal(result.failure._tag, "OrchestrationDispatchCommandError");
          if (result.failure._tag === "OrchestrationDispatchCommandError")
            assert.equal(result.failure.creationRejectionCode, "unsupported_authority");
          assert.equal(dispatches, 0);
        }),
      );
    }).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect(
  "real permanent marker denies actual legacy WS even with unavailable creation grants",
  () =>
    Effect.gen(function* () {
      let capturedSql: SqlClient.SqlClient | undefined;
      let dispatches = 0;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) => seedRouterProject(context).pipe(Effect.asVoid),
          onNativeCreationServices: (_repository, sql) =>
            Effect.sync(() => {
              capturedSql = sql;
            }),
          wrapThreadManagement: (service) => ({
            ...service,
            dispatch: (command) =>
              Effect.sync(() => {
                dispatches++;
              }).pipe(Effect.andThen(service.dispatch(command))),
            dispatchNativeCreationStage: (command, input) =>
              Effect.sync(() => {
                dispatches++;
              }).pipe(Effect.andThen(service.dispatchNativeCreationStage(command, input))),
          }),
        },
      });
      const url = yield* getWsServerUrl("/ws");
      const sql = capturedSql!;
      const sessions = yield* sql<{
        sessionId: string;
      }>`SELECT session_id AS "sessionId" FROM auth_sessions`;
      assert.equal(sessions.length, 1);
      yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES (${sessions[0]!.sessionId}, '2026-10-02T12:00:00Z')`;
      const legacy = routerThreadCreate(ThreadId.make(nativeWsFixture.command.threadId));
      yield* Schema.decodeUnknownEffect(OrchestrationV2Command)(legacy);
      yield* withWsRpcClient(url, (client) =>
        Effect.gen(function* () {
          const result = yield* client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand](legacy).pipe(
            Effect.result,
          );
          assert.equal(result._tag, "Failure");
          assert.equal(dispatches, 0);
        }),
      );
      // The marker is permanent, so a second, unenrolled session shows the same
      // command is otherwise accepted.
      const markers = yield* sql<{
        count: number;
      }>`SELECT count(*) AS count FROM native_creation_automation_enrollments WHERE session_id = ${sessions[0]!.sessionId}`;
      assert.equal(markers[0]?.count, 1);
      const unenrolledUrl = yield* getWsServerUrl("/ws");
      assert.lengthOf(yield* sql`SELECT session_id FROM auth_sessions`, 2);
      yield* withWsRpcClient(unenrolledUrl, (client) =>
        Effect.gen(function* () {
          const allowed = yield* client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand](legacy);
          assert.isAbove(allowed.sequence, 0);
          assert.equal(dispatches, 1);
        }),
      );
    }).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect(
  "qualified synthetic ports drive actual guarded WS only after immutable SQL claim and identities",
  () =>
    Effect.gen(function* () {
      const value = yield* qualifiedNativeRouterFixture("qualified-reservations", true);
      yield* withWsRpcClient(value.url, (client) =>
        Effect.gen(function* () {
          const accepted = yield* client[ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap](
            value.fixture.submission,
          );
          assert.equal(accepted.commandAcceptance, "accepted");
          assert.isAbove(accepted.creation?.finalReceipt?.resultSequence ?? 0, 0);
          assert.deepEqual(value.stages, value.expectedCommands());
          assert.deepEqual(value.order, [
            "thread.create",
            "message.dispatch",
            "checkout",
            "setup",
            "prepared-run.release",
          ]);
          assert.deepEqual(value.nativeCalls, []);
          yield* value.assertReservedInventory();
          const history = yield* value.repository.readHistory(value.fixture.command.commandId);
          assert.isTrue(Option.isSome(history));
          assertTrue(Option.isSome(history));
          assert.isNotNull(history.value.normalizedCommandDigest);
          assert.isAbove(history.value.effects.length, 0);
          assert.isTrue(
            history.value.effects.some(
              (fact) =>
                fact.kind === "setup" &&
                fact.phase === "completed" &&
                fact.terminalId === "synthetic-setup-terminal" &&
                fact.exitCode === 0,
            ),
          );
          assert.deepEqual(history.value.effectsV2, []);
          assert.isTrue(
            value.closeCalls.some((input) => input.terminalId === "synthetic-setup-terminal"),
          );
          yield* Context.get(value.services, EffectWorker.OrchestrationEffectWorkerV2).drain(10);
          assert.deepEqual(value.nativeCalls, ["open_session", "ensure_thread", "start_turn"]);
          const execution = yield* value.repository.readHistoryByClaim(
            history.value.intent.claimId,
          );
          assert.equal(execution.effectsV2.filter((fact) => fact.phase === "started").length, 1);
          const effectsBeforeDuplicate = {
            stages: value.stages.length,
            native: value.nativeCalls.length,
            order: [...value.order],
            history: execution,
          };
          const duplicate = yield* client[ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap](
            value.fixture.submission,
          ).pipe(Effect.result);
          assertTrue(duplicate._tag === "Success");
          assert.equal(duplicate.success.commandAcceptance, "accepted");
          assert.equal(duplicate.success.commandId, accepted.commandId);
          assert.equal(duplicate.success.threadId, accepted.threadId);
          assert.equal(duplicate.success.messageId, accepted.messageId);
          const originalCreation = accepted.creation;
          const replayCreation = duplicate.success.creation;
          assertTrue(originalCreation !== null && replayCreation !== null);
          for (const field of [
            "version",
            "schema",
            "preparationId",
            "operationId",
            "preparationSha256",
            "bindingDigest",
            "promptDigest",
            "commandDigest",
            "normalizedCommandDigest",
            "claimId",
            "claimedBootId",
            "claimedAt",
            "actorSessionId",
            "grantId",
            "grantRevision",
            "binding",
            "incarnation",
          ] as const) {
            assert.deepEqual(replayCreation[field], originalCreation[field]);
          }
          assert.deepEqual(replayCreation.stageCommands, originalCreation.stageCommands);
          assert.deepEqual(replayCreation.finalReceipt, originalCreation.finalReceipt);
          assert.equal(
            replayCreation.finalReceipt?.resultSequence,
            originalCreation.finalReceipt?.resultSequence,
          );
          assert.deepEqual(
            replayCreation.stageCommands.map((stage) => ({
              commandId: stage.commandId,
              commandType: stage.commandType,
              commandDigest: stage.commandDigest,
            })),
            value.expectedCommands().map((command) => ({
              commandId: command.commandId,
              commandType: command.type,
              commandDigest: nativeCreationV2CommandDigest(command),
            })),
          );
          yield* value.assertReservedInventory();
          assert.equal(value.stages.length, effectsBeforeDuplicate.stages);
          assert.equal(value.nativeCalls.length, effectsBeforeDuplicate.native);
          assert.deepEqual(value.order, effectsBeforeDuplicate.order);
          assert.deepEqual(
            yield* value.repository.readHistoryByClaim(history.value.intent.claimId),
            effectsBeforeDuplicate.history,
          );
        }),
      );
    }).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect(
  "actual oversized bootstrap batch closes 1009 before any sibling or creation effect",
  () =>
    Effect.gen(function* () {
      let capturedSql: SqlClient.SqlClient | undefined;
      let nativeHandlerCalls = 0;
      let normalizations = 0;
      let commands = 0;
      let worktrees = 0;
      const config = yield* buildAppUnderTest({
        layers: {
          onNativeCreationServices: (_repository, sql) =>
            Effect.sync(() => {
              capturedSql = sql;
            }),
          onWorkspaceNormalization: () => {
            normalizations++;
          },
          nativeCreationGrantResolver: {
            resolveCurrent: () =>
              Effect.sync(() => {
                nativeHandlerCalls++;
              }).pipe(
                Effect.andThen(
                  Effect.fail(
                    new NativeCreationAuthorityError({
                      code: "unsupported_authority",
                      message: "Synthetic unavailable grant",
                    }),
                  ),
                ),
              ),
          },
          wrapProjectService: (service) => ({
            ...service,
            create: (input) =>
              Effect.sync(() => {
                commands++;
              }).pipe(Effect.andThen(service.create(input))),
          }),
          wrapThreadManagement: (service) => ({
            ...service,
            dispatch: (command) =>
              Effect.sync(() => {
                commands++;
              }).pipe(Effect.andThen(service.dispatch(command))),
            dispatchNativeCreationStage: (command, input) =>
              Effect.sync(() => {
                commands++;
              }).pipe(Effect.andThen(service.dispatchNativeCreationStage(command, input))),
          }),
          gitVcsDriver: {
            createWorktree: () =>
              Effect.sync(() => {
                worktrees++;
                return { worktree: { path: "/synthetic/worktree", refName: "synthetic" } };
              }),
          },
        },
      });
      const { cookie, url } = parseSessionCookieFromWsUrl(yield* getWsServerUrl("/ws"));
      const sql = capturedSql!;
      const sessions = yield* sql<{
        sessionId: string;
      }>`SELECT session_id AS "sessionId" FROM auth_sessions`;
      assert.equal(sessions.length, 1);
      yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES (${sessions[0]!.sessionId}, '2026-10-02T12:00:00Z')`;
      const sibling = {
        _tag: "Request",
        id: "101",
        tag: WS_METHODS.projectsMutate,
        payload: {
          type: "project.create",
          commandId: "oversize-sibling-command",
          projectId: "oversize-sibling-project",
          title: "Synthetic sibling",
          workspaceRoot: config.cwd,
        },
        headers: [],
      };
      const guarded = {
        _tag: "Request",
        id: "102",
        tag: ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap,
        payload: nativeWsFixture.submission,
        headers: [],
      };
      yield* Schema.decodeUnknownEffect(ProjectMutation)(sibling.payload);
      yield* Schema.decodeUnknownEffect(NativeBootstrapSubmission)(guarded.payload);
      const encodedBatch = yield* Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
        [sibling, guarded],
      );
      const wire = " ".repeat(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES) + encodedBatch;
      assert.isAbove(Buffer.byteLength(wire), NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES);
      assert.isBelow(Buffer.byteLength(wire), 3 * 1024 * 1024);
      const before = normalizations;
      const socket = yield* Effect.acquireRelease(
        Effect.sync(
          () => new NodeSocket.NodeWS.WebSocket(url, cookie ? { headers: { cookie } } : {}),
        ),
        (socket) => Effect.sync(() => socket.terminate()),
      );
      const closeCode = yield* Effect.callback<number, Error>((resume) => {
        socket.once("close", (code) => resume(Effect.succeed(code)));
        socket.once("error", (error) => resume(Effect.fail(error)));
        socket.once("open", () => socket.send(wire));
      }).pipe(Effect.timeout("5 seconds"));
      assert.equal(closeCode, 1009);
      assert.equal(nativeHandlerCalls, 0);
      assert.equal(normalizations - before, 0);
      assert.equal(commands, 0);
      assert.equal(worktrees, 0);
      for (const table of [
        "native_creation_intents",
        "native_creation_reserved_command_identities",
        "native_creation_reserved_commands",
        "native_creation_effect_facts",
      ]) {
        const rows = yield* sql.unsafe<{ count: number }>(`SELECT COUNT(*) AS count FROM ${table}`);
        assert.equal(rows[0]?.count, 0);
      }
    }).pipe(
      Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer)),
      TestClock.withLive,
    ),
);

it.effect(
  "observes actual pending and unknown deletion cleanup under Read scope without effects",
  () =>
    Effect.gen(function* () {
      let services!: Context.Context<RouterV2Services>;
      const effects: string[] = [];
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.gen(function* () {
              services = context;
              yield* seedRouterProject(context);
              const threads = Context.get(context, ThreadManagement.ThreadManagementService);
              yield* threads.dispatch({
                ...routerThreadCreate(),
                branch: "feature/deletion-observer",
                worktreePath: "/synthetic/router-project/deletion-observer",
              });
              yield* threads.acquireWorktreeOwnership(defaultThreadId);
            }),
          serverRuntimeStartup: {
            enqueueCommand: () =>
              Effect.sync(() => {
                effects.push("startup");
              }).pipe(Effect.andThen(Effect.die("Read observation must not enqueue a command"))),
          },
          terminalManager: {
            close: () =>
              Effect.sync(() => {
                effects.push("terminal");
              }),
          },
          gitVcsDriver: {
            removeWorktree: () =>
              Effect.sync(() => {
                effects.push("worktree");
              }).pipe(Effect.andThen(Effect.die("Read observation must not remove a worktree"))),
          },
          threadLaunch: {
            dispatchNativeBootstrap: () =>
              Effect.sync(() => {
                effects.push("native");
              }).pipe(
                Effect.andThen(Effect.die("Read observation must not dispatch native bootstrap")),
              ),
          },
        },
      });
      const threads = Context.get(services, ThreadManagement.ThreadManagementService);
      const sink = Context.get(services, EventSink.EventSinkV2);
      const sql = Context.get(services, SqlClient.SqlClient);
      const lease = (yield* threads.listWorktreeOwnershipLeases).find(
        (entry) => entry.ownerThreadId === defaultThreadId,
      );
      assertTrue(lease !== undefined);
      const input = {
        threadId: defaultThreadId,
        commandId: CommandId.make("router:observe-deletion"),
      };
      const deletion = yield* threads.dispatch({
        type: "thread.delete",
        ...input,
        worktreeRemoval: {
          projectId: defaultProjectId,
          path: lease.resourcePath,
          branch: lease.branch,
          force: true,
        },
      });
      const pending = yield* sink.observeThreadDeletionCleanup(input);
      assert.equal(pending.state, "pending");
      assert.equal(pending.receipt?.status, "accepted");
      assert.equal(pending.receipt?.resultSequence, deletion.sequence);
      assert.equal(pending.removalOutcome, null);
      const token = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
        scope: "orchestration:read",
      });
      assert.equal(token.response.status, 200);
      const ticketResponse = yield* HttpClient.post("/api/auth/websocket-ticket", {
        headers: { authorization: `Bearer ${token.body.access_token ?? ""}` },
      });
      assert.equal(ticketResponse.status, 200);
      const ticket = yield* ticketResponse.json;
      const decodedTicket = yield* Schema.decodeUnknownEffect(
        Schema.Struct({ ticket: Schema.String }),
      )(ticket);
      const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}&wsTicket=${encodeURIComponent(decodedTicket.ticket)}`;
      const before = {
        events: yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
        receipts: yield* sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
        outbox: yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        leases: yield* threads.listWorktreeOwnershipLeases,
      };
      const original = yield* sink.readThreadDeletionCommand(input.commandId);
      let unknownOutbox!: typeof before.outbox;
      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            assert.deepEqual(
              yield* client[ORCHESTRATION_V2_WS_METHODS.observeThreadDeletionCleanup](input),
              pending,
            );
            assert.deepEqual(
              yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
              before.outbox,
            );
            const outbox = Context.get(services, EffectOutbox.EffectOutboxV2);
            const effectId = EventSink.deletionWorktreeEffectIdV1(input.commandId, input.threadId);
            const binding = yield* sink.readDeletionWorktreeTask(effectId);
            assertTrue(binding !== null);
            assert.equal(binding.threadId, input.threadId);
            const workerId = "router:deletion-observation-unknown";
            const queued = yield* outbox.listByCommandId(input.commandId);
            assert.isTrue(
              queued.every((effect) => effect.status === "pending" && effect.leaseOwner === null),
            );
            assert.isTrue(
              queued.some(
                (effect) => effect.id === effectId && effect.request.type === "worktree.cleanup",
              ),
            );
            assert.deepEqual(yield* outbox.listHeldByThreadId(input.threadId), []);
            const siblings = queued.filter((effect) => effect.id !== effectId);
            assert.isTrue(siblings.every((effect) => effect.request.type !== "worktree.cleanup"));
            const cancelled = yield* outbox.cancelUnsettled({
              threadId: input.threadId,
              effectTypes: [...new Set(siblings.map((effect) => effect.request.type))],
              reason: "Fixture isolates an unexecuted worktree cleanup observation claim",
            });
            assert.deepEqual([...cancelled].sort(), siblings.map((effect) => effect.id).sort());
            assert.deepEqual(
              yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id = ${input.threadId} AND status = 'running'`,
              [],
            );
            const claimed = Option.getOrUndefined(
              yield* outbox.claimNext({ workerId, leaseDurationMs: 60_000 }),
            );
            assertTrue(claimed !== undefined);
            assert.equal(claimed.id, effectId);
            assert.equal(claimed.commandId, input.commandId);
            assert.equal(claimed.threadId, input.threadId);
            assert.equal(claimed.request.type, "worktree.cleanup");
            const expectedAttempt = claimed.attemptCount;
            yield* sink.recordLeaseCleanupTaskOutcome({
              effectId,
              workerId,
              expectedAttempt,
              outcome: { taskId: effectId, result: null, effect: "unknown" },
              evidence: { reason: "fixture_has_no_physical_completion_observation" },
            });
            const unknown = yield* sink.observeThreadDeletionCleanup(input);
            assert.equal(unknown.state, "unknown");
            assert.equal(unknown.reason, "complete_worktree_removal_proof_unavailable");
            assert.deepEqual(unknown.receipt, pending.receipt);
            assert.deepEqual(unknown.deletion, pending.deletion);
            assert.deepEqual(unknown.worktree, pending.worktree);
            assert.deepEqual(unknown.removalOutcome, { result: null, effect: "unknown" });
            unknownOutbox =
              yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
            const holds = yield* outbox.listHeldByThreadId(input.threadId);
            assert.isTrue(
              holds.some(
                (hold) =>
                  hold.effectId === binding.effectId && hold.expectedAttempt === expectedAttempt,
              ),
            );
            const outcomes =
              yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_outcomes WHERE effect_id = ${effectId} ORDER BY ordinal`;
            assert.deepEqual(
              yield* client[ORCHESTRATION_V2_WS_METHODS.observeThreadDeletionCleanup](input),
              unknown,
            );
            assert.deepEqual(yield* outbox.listHeldByThreadId(input.threadId), holds);
            assert.deepEqual(
              yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_outcomes WHERE effect_id = ${effectId} ORDER BY ordinal`,
              outcomes,
            );
          }),
        ),
      );
      assert.deepEqual(yield* sink.readThreadDeletionCommand(input.commandId), original);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
        before.events,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
        before.receipts,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        unknownOutbox,
      );
      assert.deepEqual(yield* threads.listWorktreeOwnershipLeases, before.leases);
      assert.deepEqual(effects, []);
    }).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect("denies deletion cleanup observation before its SQL read without Read scope", () =>
  Effect.gen(function* () {
    let services!: Context.Context<RouterV2Services>;
    yield* buildAppUnderTest({
      layers: {
        onV2Services: (context) =>
          Effect.sync(() => {
            services = context;
          }),
      },
    });
    const sink = Context.get(services, EventSink.EventSinkV2);
    const originalRead = sink.observeThreadDeletionCleanup;
    const reads: Array<Parameters<typeof originalRead>[0]> = [];
    Object.defineProperty(sink, "observeThreadDeletionCleanup", {
      configurable: true,
      value: (input: Parameters<typeof originalRead>[0]) =>
        Effect.sync(() => {
          reads.push(input);
        }).pipe(Effect.andThen(originalRead(input))),
    });
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        Object.defineProperty(sink, "observeThreadDeletionCleanup", { value: originalRead });
      }),
    );
    const token = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
      scope: "orchestration:operate",
    });
    assert.equal(token.response.status, 200);
    const ticketResponse = yield* HttpClient.post("/api/auth/websocket-ticket", {
      headers: { authorization: `Bearer ${token.body.access_token ?? ""}` },
    });
    assert.equal(ticketResponse.status, 200);
    const ticket = yield* Schema.decodeUnknownEffect(Schema.Struct({ ticket: Schema.String }))(
      yield* ticketResponse.json,
    );
    const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}&wsTicket=${encodeURIComponent(ticket.ticket)}`;
    const input = {
      threadId: ThreadId.make("router:denied-delete-thread"),
      commandId: CommandId.make("router:denied-delete-command"),
    };
    const sql = Context.get(services, SqlClient.SqlClient);
    const before = yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`;
    const failure = yield* Effect.scoped(
      withWsRpcClient(wsUrl, (client) =>
        client[ORCHESTRATION_V2_WS_METHODS.observeThreadDeletionCleanup](input),
      ),
    ).pipe(Effect.flip);
    assert.equal(failure._tag, "EnvironmentAuthorizationError");
    assertTrue(failure._tag === "EnvironmentAuthorizationError");
    assert.equal(failure.requiredScope, "orchestration:read");
    assert.deepEqual(reads, []);
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`, before);
  }).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect(
  "returns requested deletion IDs and null proofs when the actual SQL observation fails",
  () =>
    Effect.gen(function* () {
      let services!: Context.Context<RouterV2Services>;
      yield* buildAppUnderTest({
        layers: {
          onV2Services: (context) =>
            Effect.sync(() => {
              services = context;
            }),
        },
      });
      const sql = Context.get(services, SqlClient.SqlClient);
      const token = yield* exchangeAccessToken(defaultDesktopBootstrapToken, {
        scope: "orchestration:read",
      });
      assert.equal(token.response.status, 200);
      const ticketResponse = yield* HttpClient.post("/api/auth/websocket-ticket", {
        headers: { authorization: `Bearer ${token.body.access_token ?? ""}` },
      });
      assert.equal(ticketResponse.status, 200);
      const ticket = yield* Schema.decodeUnknownEffect(Schema.Struct({ ticket: Schema.String }))(
        yield* ticketResponse.json,
      );
      const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}&wsTicket=${encodeURIComponent(ticket.ticket)}`;
      const input = {
        threadId: ThreadId.make("router:unavailable-delete-thread"),
        commandId: CommandId.make("router:unavailable-delete-command"),
      };
      const before = yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`;
      const outbox = yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
      yield* Effect.acquireUseRelease(
        sql`ALTER TABLE orchestration_command_receipts RENAME TO router_unavailable_deletion_receipts`,
        () =>
          Effect.scoped(
            withWsRpcClient(wsUrl, (client) =>
              Effect.gen(function* () {
                const result =
                  yield* client[ORCHESTRATION_V2_WS_METHODS.observeThreadDeletionCleanup](input);
                assert.deepEqual(result, {
                  ...input,
                  state: "unknown",
                  receipt: null,
                  deletion: null,
                  worktree: null,
                  removalOutcome: null,
                  currentLease: "unavailable",
                  reason: "deletion_cleanup_observation_unavailable",
                });
              }),
            ),
          ),
        () =>
          sql`ALTER TABLE router_unavailable_deletion_receipts RENAME TO orchestration_command_receipts`.pipe(
            Effect.orDie,
          ),
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`, before);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        outbox,
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
    }).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);
