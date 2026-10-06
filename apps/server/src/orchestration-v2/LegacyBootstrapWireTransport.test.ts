import { makeGitVcsDriverCore } from "../vcs/GitVcsDriverCore.ts";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as Sink from "effect/Sink";
import { projectThreadProjectionForWire } from "./WireProjection.ts";
import { makeCommandObservationQuery } from "./CommandObservation.ts";
import * as Deferred from "effect/Deferred";
import * as PtyAdapter from "../terminal/PtyAdapter.ts";
import type { ProjectScript } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as ProcessRunner from "../processRunner.ts";
import { ProviderDriverKind } from "@t3tools/contracts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import { makeProviderRegistryLayer } from "../provider/testUtils/providerRegistryMock.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import {
  legacyBootstrapCreateCommandId,
  legacyNeverInvokedSetupOpen,
  legacyPreparationReleaseBlocker,
} from "./LegacyBootstrap.ts";
import { OrchestrationDispatchCommandError } from "@t3tools/contracts";
import * as WsFileSystem from "effect/FileSystem";
import { it as effectIt } from "@effect/vitest";
import * as WsSocket from "effect/unstable/socket/Socket";
import * as WsQueue from "effect/Queue";
import * as WsFiber from "effect/Fiber";
import * as WsHttpServerRequest from "effect/unstable/http/HttpServerRequest";
import {
  DEFAULT_SERVER_SETTINGS,
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2ThreadProjectionJson,
  WorktreeSetupStreamEvent,
  WS_METHODS,
} from "@t3tools/contracts";
import * as WsTraceDiagnostics from "../diagnostics/TraceDiagnostics.ts";
import * as WsProjectFaviconResolver from "../project/ProjectFaviconResolver.ts";
import * as WsEffectOutbox from "./EffectOutbox.ts";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as WsHttpClient from "effect/unstable/http/HttpClient";
import * as WsWorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as WsVcsProcess from "../vcs/VcsProcess.ts";
import * as WsCodexInstallation from "../provider/CodexInstallation.ts";
import * as WsAntigravityInstallation from "../provider/AntigravityInstallation.ts";
import * as WsApplicationEventStore from "../persistence/Services/OrchestrationEventStore.ts";
import * as WsProjectService from "../project/ProjectService.ts";
import * as WsManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as WsThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as WsProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as WsAnalyticsService from "../telemetry/AnalyticsService.ts";
import * as WsThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as WsScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as WsPullRequestService from "../pullRequest/PullRequestService.ts";
import * as WsPullRequestSyncReactor from "../orchestration-v2/PullRequestSyncReactor.ts";
import * as WsDeviceService from "../device/DeviceService.ts";
import * as WsOrchestrator from "../orchestration-v2/Orchestrator.ts";
import * as WsUsageService from "../usage/UsageService.ts";
import * as WsTokenAccountingService from "../tokenAccounting/TokenAccountingService.ts";
import * as WsUsageLimitSources from "../usage/UsageLimitSources.ts";
import * as WsProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as WsWorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as WsProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as WsRepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as WsAgentSessionImporter from "../project/AgentSessionImporter.ts";
import * as WsCheckpointDiffQuery from "../checkpointing/CheckpointDiffQuery.ts";
import * as WsKeybindings from "../keybindings.ts";
import * as WsEnvironmentTheme from "../environmentTheme.ts";
import * as WsExternalLauncher from "../process/externalLauncher.ts";
import * as WsRemoteOpenTargets from "../environment/RemoteOpenTargets.ts";
import * as WsGitWorkflowService from "../git/GitWorkflowService.ts";
import * as WsReviewService from "../review/ReviewService.ts";
import * as WsVcsProvisioningService from "../vcs/VcsProvisioningService.ts";
import * as WsVcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as WsTerminalManager from "../terminal/Manager.ts";
import * as WsPreviewManager from "../preview/Manager.ts";
import * as WsPortScanner from "../preview/PortScanner.ts";
import * as WsProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import * as WsModelManifest from "../provider/ModelManifest.ts";
import * as WsProviderMaintenance from "../provider/providerMaintenance.ts";
import * as WsProviderInstanceRegistry from "../provider/Services/ProviderInstanceRegistry.ts";
import * as WsAcpRegistrySupport from "../provider/acp/AcpRegistrySupport.ts";
import * as WsAcpRegistryRuntimeCoordinator from "../provider/acp/AcpRegistryRuntimeCoordinator.ts";
import * as WsProviderAuthService from "../provider/Services/ProviderAuthService.ts";
import * as WsServerSelfUpdate from "../cloud/selfUpdate.ts";
import * as WsServerConfig from "../config.ts";
import * as WsServerLifecycleEvents from "../serverLifecycleEvents.ts";
import * as WsServerSettings from "../serverSettings.ts";
import * as WsServerRuntimeStartup from "../serverRuntimeStartup.ts";
import * as WsWorkspaceEntries from "../workspace/WorkspaceEntries.ts";
import * as WsWorkspaceFileSystem from "../workspace/WorkspaceFileSystem.ts";
import * as WsServerEnvironment from "../environment/ServerEnvironment.ts";
import * as WsBackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as WsEnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as WsSourceControlRepositoryService from "../sourceControl/SourceControlRepositoryService.ts";
import * as WsProcessDiagnostics from "../diagnostics/ProcessDiagnostics.ts";
import * as WsHostResources from "../resourceTelemetry/HostResources.ts";
import * as WsProcessResourceMonitor from "../diagnostics/ProcessResourceMonitor.ts";
import * as WsResourceTelemetry from "../resourceTelemetry/ResourceTelemetry.ts";
import * as WsRelayClient from "@t3tools/shared/relayClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { websocketRpcRouteLayer } from "../ws.ts";
import { ORCHESTRATION_PROTOCOL_QUERY_PARAM } from "@t3tools/contracts";
import * as WsPreviewAutomationBroker from "../mcp/PreviewAutomationBroker.ts";

import { expect, it } from "vite-plus/test";
import {
  CommandId,
  GitCommandError,
  EnvironmentOrchestrationHttpApi,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import { environmentAuthenticatedAuthLayer } from "../auth/http.ts";
import {
  SqlitePersistenceMemory,
  makeSqlitePersistenceLive,
} from "../persistence/Layers/Sqlite.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";
import * as ApplicationEvents from "../persistence/Layers/OrchestrationEventStore.ts";
import * as ProjectEnrichment from "../project/ProjectEnrichmentService.ts";
import * as Receipts from "./CommandReceiptStore.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as Threads from "./ThreadManagementService.ts";
import { OrchestratorProjectionError, OrchestratorDispatchError } from "./Orchestrator.ts";
import {
  LegacyReleaseDecision,
  LegacyPreparationFailureDecision,
  RecordedAppThreadJson,
  RecordedRunJson,
} from "./RecordedTypes.ts";
import { orchestrationHttpApiLayer } from "./http.ts";

const decodeThread = Schema.decodeUnknownSync(RecordedAppThreadJson);
const decodeRun = Schema.decodeUnknownSync(RecordedRunJson);
const decodeLegacyRpcFailureCause = Schema.decodeUnknownEffect(
  Schema.Array(
    Schema.Struct({ _tag: Schema.Literal("Fail"), error: OrchestrationDispatchCommandError }),
  ),
);
const encodeSocketRpcRequest = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeSocketRpcResponse = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      _tag: Schema.String,
      requestId: Schema.String,
      exit: Schema.Struct({
        _tag: Schema.String,
        value: Schema.optional(Schema.Unknown),
        cause: Schema.optional(Schema.Unknown),
      }),
    }),
  ),
);

it("serves authenticated full, bounded and shell HTTP snapshots from recorded SQL without private correlation", async () => {
  const threadId = ThreadId.make("recorded-http:T");
  const runId = RunId.make("recorded-http:R");
  const projectId = ProjectId.make("recorded-http:P");
  const timestamp = "2026-10-05T00:00:00.000Z";
  const policy = {
    version: 1 as const,
    createCommandId: CommandId.make("recorded-http:B"),
    birthCommandId: CommandId.make("recorded-http:B:initial-message"),
    releaseCommandId: CommandId.make("recorded-http:C"),
    projectId,
    threadId,
    messageId: MessageId.make("recorded-http:M"),
    payloadHash: "private-http-payload-hash",
    ownsNewThread: false,
    runId,
  };
  const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6" };
  const thread = decodeThread({
    id: threadId,
    projectId,
    title: "Ordinary recorded thread",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: modelSelection.instanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: timestamp,
    updatedAt: timestamp,
    archivedAt: null,
    deletedAt: null,
    settledAt: null,
    settledOverride: null,
    lastVisitedAt: null,
    legacyBootstrapClaim: policy,
  });
  const run = decodeRun({
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId: modelSelection.instanceId,
    modelSelection,
    providerThreadId: null,
    userMessageId: policy.messageId,
    rootNodeId: null,
    activeAttemptId: null,
    status: "preparing",
    requestedAt: timestamp,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
    workspaceRunSetupScript: false,
    legacyBootstrap: policy,
    legacyPreparationFailureKnown: false,
  });
  let snapshotReads = 0;
  let fixtureToken = "";
  let serveFailureFixture = false;
  const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer, Receipts.layer).pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
  );
  const persistence = Layer.mergeAll(stores, EventSink.layer.pipe(Layer.provide(stores)));
  const readers = Layer.unwrap(
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      // Codec storage fixture, not an authenticated bootstrap or a fabricated V1 trace.
      yield* sink.write({
        events: [
          {
            id: EventId.make("recorded-http:thread"),
            type: "thread.created",
            threadId,
            occurredAt: thread.createdAt,
            payload: thread,
          },
          {
            id: EventId.make("recorded-http:run"),
            type: "run.created",
            threadId,
            runId,
            occurredAt: run.requestedAt,
            payload: run,
          },
        ],
      });
      const correlation = {
        policy,
        threadId,
        runId,
        claimEventId: EventId.make("recorded-http:claim"),
        claimSequence: 1,
        claimReceiptSequence: 1,
        birthEventId: EventId.make("recorded-http:birth"),
        birthSequence: 2,
        birthReceiptSequence: 2,
        preparationGeneration: "private-http-generation",
        workspacePath: "/private-http-workspace",
        projectWorkspaceRoot: "/private-http-workspace",
      };
      const decision = yield* Schema.decodeUnknownEffect(LegacyReleaseDecision)({
        version: 1,
        status: "rejected",
        policy,
        claimEventId: correlation.claimEventId,
        claimSequence: 1,
        claimReceiptSequence: 1,
        birthEventId: correlation.birthEventId,
        birthSequence: 2,
        birthReceiptSequence: 2,
        evidenceEventId: EventId.make("recorded-http:C:event"),
        reason: "Private HTTP codec rejection",
        observed: { snapshotSequence: 2, lastEventSequence: 2, target: null },
        guard: {
          observedSnapshotSequence: 0,
          expectedModelSelection: modelSelection,
          expectedSessionStatus: null,
          expectedActiveTurnId: null,
          expectedLatestTurnId: null,
          requireIdle: true,
        },
        deletion: {
          ...correlation,
          version: 1,
          type: "no_control",
          commandId: CommandId.make("recorded-http:D"),
          evidenceEventId: EventId.make("recorded-http:D:event"),
          control: { ...correlation, version: 1, type: "no_control" },
        },
      });
      // This direct recorded DTO fixture tests authenticated serving, not D authority or a V1 trace.
      const payload = yield* Schema.encodeEffect(Schema.fromJsonString(RecordedRunJson))({
        ...run,
        legacyReleaseDecision: decision,
      });
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json = ${payload} WHERE run_id = ${runId}`;
      const failureCommandId = CommandId.make("recorded-http:B:fail");
      const failureEvidenceEventId = EventId.make("recorded-http:failure:event");
      const failure = yield* Schema.decodeUnknownEffect(LegacyPreparationFailureDecision)({
        version: 1,
        status: "known_workspace_failure",
        policy,
        claimEventId: correlation.claimEventId,
        claimSequence: 1,
        claimReceiptSequence: 1,
        birthEventId: correlation.birthEventId,
        birthSequence: 2,
        birthReceiptSequence: 2,
        preparationGeneration: correlation.preparationGeneration,
        projectWorkspaceRoot: correlation.projectWorkspaceRoot,
        workspacePath: correlation.workspacePath,
        failedEffectId: "private-http-failed-effect",
        failedInputHash: "private-http-failed-input",
        outcomeCommandId: CommandId.make("recorded-http:owner:outcome"),
        outcomeEventId: EventId.make("recorded-http:owner:outcome:event"),
        outcomeEventSequence: 3,
        outcomeReceiptSequence: 3,
        failureCommandId,
        evidenceEventId: failureEvidenceEventId,
        deletion: {
          basis: "workspace_failure",
          provenance: decision.deletion,
          failureCommandId,
          failureEvidenceEventId,
          failureEventSequence: 4,
          failureReceiptSequence: 4,
        },
      });
      // Sequential private DTO fixtures prove serving omission, not command authority or receipt authenticity.
      const failurePayload = yield* Schema.encodeEffect(Schema.fromJsonString(RecordedRunJson))({
        ...run,
        status: "failed",
        legacyPreparationFailureDecision: failure,
      });
      const prepareSnapshot = Effect.suspend(() =>
        serveFailureFixture
          ? sql`UPDATE orchestration_v2_projection_runs SET payload_json = ${failurePayload} WHERE run_id = ${runId}`.pipe(
              Effect.asVoid,
            )
          : Effect.void,
      );

      const mapError = (cause: unknown) => new OrchestratorProjectionError({ threadId, cause });
      return Layer.mock(Threads.ThreadManagementService)({
        getThreadSnapshot: (id) =>
          prepareSnapshot.pipe(
            Effect.andThen(projections.getThreadSnapshot(id)),
            Effect.tap(() =>
              Effect.sync(() => {
                snapshotReads++;
              }),
            ),
            Effect.mapError(mapError),
          ),
        getThreadSnapshotWindow: (id, options) =>
          prepareSnapshot.pipe(
            Effect.andThen(projections.getThreadSnapshotWindow(id, options)),
            Effect.tap(() =>
              Effect.sync(() => {
                snapshotReads++;
              }),
            ),
            Effect.mapError(mapError),
          ),
        getShellSnapshot: (options) =>
          prepareSnapshot.pipe(
            Effect.andThen(projections.getShellSnapshot(options)),
            Effect.mapError(mapError),
          ),
      });
    }),
  ).pipe(Layer.provideMerge(persistence));
  const config = WsServerConfig.layerTest(process.cwd(), { prefix: "legacy-wire-http-auth-" });
  const authOwners = WsEnvironmentAuth.layer.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(ServerSecretStore.layer),
    Layer.provideMerge(WsServerEnvironment.identityLayer),
    Layer.provide(config),
    Layer.provide(NodeServices.layer),
  );
  const auth = Layer.unwrap(
    Effect.gen(function* () {
      const owner = yield* WsEnvironmentAuth.EnvironmentAuth;
      const issued = yield* owner.issueSession({ scopes: ["orchestration:read"] });
      fixtureToken = issued.token;
      return environmentAuthenticatedAuthLayer;
    }),
  ).pipe(Layer.provideMerge(authOwners));
  const dependencies = Layer.mergeAll(
    readers,
    Layer.mock(ProjectStore.ProjectStoreV2)({ listShells: () => Effect.succeed([]) }),
    Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({}),
    ApplicationEvents.OrchestrationEventStoreLive.pipe(Layer.provide(SqlitePersistenceMemory)),
  );
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentOrchestrationHttpApi),
  ).pipe(
    Layer.provide(orchestrationHttpApiLayer.pipe(Layer.provide(dependencies))),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  const http = HttpRouter.toWebHandler(routes, { disableLogger: true });
  try {
    const fullUrl = `http://test/api/orchestration/threads/${threadId}`;
    expect((await http.handler(new Request(fullUrl), Context.empty())).status).toBe(401);
    expect(snapshotReads).toBe(0);
    for (const url of [fullUrl, `${fullUrl}/bounded`, "http://test/api/orchestration/shell"]) {
      const response = await http.handler(
        new Request(url, {
          headers: {
            authorization: `Bearer ${fixtureToken}`,
            [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
          },
        }),
        Context.empty(),
      );
      expect(response.status).toBe(200);
      const serialized = await response.text();
      expect(serialized).toContain(threadId);
      expect(serialized).not.toContain("legacyBootstrap");
      expect(serialized).not.toContain("legacyPreparation");
      expect(serialized).not.toContain("legacyReleaseDecision");
      expect(serialized).not.toContain("workspaceRunSetupScript");
      expect(serialized).not.toContain(policy.payloadHash);
      expect(serialized).not.toContain("private-http-workspace");
      expect(serialized).not.toContain("private-http-generation");
      expect(serialized).toContain("Ordinary recorded thread");
    }
    expect(snapshotReads).toBe(2);
    serveFailureFixture = true;
    for (const url of [fullUrl, `${fullUrl}/bounded`, "http://test/api/orchestration/shell"]) {
      const response = await http.handler(
        new Request(url, {
          headers: {
            authorization: `Bearer ${fixtureToken}`,
            [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
          },
        }),
        Context.empty(),
      );
      expect(response.status).toBe(200);
      const serialized = await response.text();
      expect(serialized).toContain(threadId);
      for (const privateField of [
        "legacyPreparationFailureDecision",
        "legacyReleaseDecision",
        "legacyPreparation",
        "legacyBootstrap",
        "workspaceRunSetupScript",
        "private-http-failed-effect",
        "private-http-failed-input",
        policy.payloadHash,
        "private-http-workspace",
        "private-http-generation",
      ])
        expect(serialized).not.toContain(privateField);
      expect(serialized).toContain("Ordinary recorded thread");
    }
    expect(snapshotReads).toBe(4);
  } finally {
    await http.dispose();
  }
});

it("rejects unauthenticated and query-token WebSocket ingress at the actual production auth boundary with zero RPC work", async () => {
  let rpcWork = 0;
  const denyRpcWork = () =>
    Effect.sync(() => {
      rpcWork++;
    }).pipe(Effect.andThen(Effect.die("Unauthenticated request reached an RPC owner.")));
  const config = WsServerConfig.layerTest(process.cwd(), { prefix: "legacy-wire-ws-gate-" });
  const auth = WsEnvironmentAuth.layer.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(ServerSecretStore.layer),
    Layer.provideMerge(WsServerEnvironment.identityLayer),
    Layer.provide(config),
    Layer.provide(NodeServices.layer),
  );
  // Unrelated owners are deliberately unavailable; rejection must precede any RPC layer construction.
  const deniedRpcOwners = Layer.mergeAll(
    WsTokenAccountingService.layer,
    Layer.mock(Threads.ThreadManagementService)({ getThreadSnapshot: denyRpcWork }),
    Layer.mock(WsApplicationEventStore.OrchestrationEventStore)({}),
    Layer.mock(ProjectStore.ProjectStoreV2)({}),
    Layer.mock(WsProjectService.ProjectService)({}),
    Layer.mock(WsManagedProjectFolders.ManagedProjectFolders)({
      namedProjectsRoot: "/synthetic-unused-projects",
    }),
    Layer.mock(WsThreadSearch.ThreadSearch)({}),
    Layer.mock(WsProviderSessionManager.ProviderSessionManagerV2)({}),
    Layer.mock(WsAnalyticsService.AnalyticsService)({}),
    Layer.mock(WsThreadLaunchService.ThreadLaunchService)({ launch: denyRpcWork }),
    Layer.mock(WsScheduledTasks.ScheduledTaskService)({}),
    Layer.mock(WsPullRequestService.PullRequestService)({}),
    Layer.mock(WsPullRequestSyncReactor.PullRequestSyncReactor)({}),
    Layer.mock(WsDeviceService.DeviceService)({}),
    Layer.mock(WsOrchestrator.OrchestratorV2)({ dispatch: denyRpcWork }),
    Layer.mock(WsUsageService.UsageService)({}),
    Layer.mock(WsUsageLimitSources.UsageLimitSources)({}),
    Layer.mock(WsProjectSetupScriptRunner.ProjectSetupScriptRunner)({}),
    Layer.mock(WsWorktreeSetupTracker.WorktreeSetupTracker)({}),
    Layer.mock(WsProjectCloneTracker.ProjectCloneTracker)({}),
    Layer.mock(WsRepositoryIdentityResolver.RepositoryIdentityResolver)({}),
    Layer.mock(WsAgentSessionImporter.AgentSessionImporter)({}),
    Layer.mock(WsCheckpointDiffQuery.CheckpointDiffQuery)({}),
    Layer.mock(WsKeybindings.Keybindings)({}),
    Layer.mock(WsEnvironmentTheme.EnvironmentThemeService)({}),
    Layer.mock(WsExternalLauncher.ExternalLauncher)({}),
    Layer.mock(WsRemoteOpenTargets.RemoteOpenTargets)({}),
    Layer.mock(WsGitWorkflowService.GitWorkflowService)({}),
    Layer.mock(WsReviewService.ReviewService)({}),
    Layer.mock(WsVcsProvisioningService.VcsProvisioningService)({}),
    Layer.mock(WsVcsStatusBroadcaster.VcsStatusBroadcaster)({}),
    Layer.mock(WsTerminalManager.TerminalManager)({}),
    Layer.mock(WsPreviewManager.PreviewManager)({}),
    Layer.mock(WsPortScanner.PortDiscovery)({}),
    Layer.mock(WsProviderRegistry.ProviderRegistry)({}),
    Layer.mock(WsModelManifest.ModelManifest)({}),
    Layer.succeed(WsProviderMaintenance.ProviderVersionCache, new Map()),
    Layer.mock(WsProviderInstanceRegistry.ProviderInstanceRegistry)({}),
    Layer.mock(WsAcpRegistrySupport.AcpRegistryCatalog)({}),
    Layer.mock(WsAcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator)({}),
    Layer.mock(WsProviderAuthService.ProviderAuthService)({}),
    Layer.mock(WsServerSelfUpdate.ServerSelfUpdate)({}),
    Layer.mock(WsServerLifecycleEvents.ServerLifecycleEvents)({}),
    Layer.mock(WsServerSettings.ServerSettingsService)({}),
    Layer.mock(WsServerRuntimeStartup.ServerRuntimeStartup)({}),
    Layer.mock(WsWorkspaceEntries.WorkspaceEntries)({}),
    Layer.mock(WsWorkspaceFileSystem.WorkspaceFileSystem)({}),
    Layer.mock(WsBackgroundPolicy.BackgroundPolicy)({}),
    Layer.mock(WsSourceControlRepositoryService.SourceControlRepositoryService)({}),
    Layer.mock(WsProcessDiagnostics.ProcessDiagnostics)({}),
    Layer.mock(WsHostResources.HostResources)({}),
    Layer.mock(WsProcessResourceMonitor.ProcessResourceMonitor)({}),
    Layer.mock(WsResourceTelemetry.ResourceTelemetry)({}),
    Layer.mock(WsRelayClient.RelayClient)({}),
    Layer.mock(WsPreviewAutomationBroker.PreviewAutomationBroker)({}),
    Layer.mock(WsServerEnvironment.ServerEnvironment)({}),
    Layer.mock(WsAntigravityInstallation.AntigravityInstallation)({
      managedDirectory: "/synthetic-unused-antigravity",
    }),
    Layer.mock(WsCodexInstallation.CodexInstallation)({
      managedDirectory: "/synthetic-unused-codex",
    }),
    Layer.mock(WsVcsProcess.VcsProcess)({}),
    Layer.mock(WsWorkspacePaths.WorkspacePaths)({}),
    Layer.succeed(
      WsHttpClient.HttpClient,
      WsHttpClient.make(() => Effect.die("Auth gate must not call HTTP client.")),
    ),
    Layer.mock(WsTraceDiagnostics.TraceDiagnostics)({}),
    Layer.mock(WsProjectFaviconResolver.ProjectFaviconResolver)({}),
    Layer.mock(EventSink.EventSinkV2)({}),
    Layer.mock(WsEffectOutbox.EffectOutboxV2)({}),
    Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({}),
    Layer.succeed(HostProcessEnvironment, {}),
    Layer.succeed(HostProcessPlatform, "linux"),
  );
  const dependencies = Layer.mergeAll(auth, deniedRpcOwners).pipe(
    Layer.provideMerge(config),
    Layer.provideMerge(NodeServices.layer),
  );
  const routes = websocketRpcRouteLayer.pipe(Layer.provideMerge(dependencies));
  const http = HttpRouter.toWebHandler(routes, { disableLogger: true });
  try {
    const url = `http://test/ws?${ORCHESTRATION_PROTOCOL_QUERY_PARAM}=${ORCHESTRATION_PROTOCOL_VERSION_TEXT}`;
    for (const resource of [
      url,
      `${url}&token=synthetic-query-token`,
      `${url}&wsTicket=invalid-synthetic-ticket`,
    ]) {
      const response = await http.handler(new Request(resource), Context.empty());
      expect(response.status).toBe(401);
      const body = await response.text();
      expect(JSON.parse(body)).toMatchObject({
        _tag: "EnvironmentAuthInvalidError",
        code: "auth_invalid",
      });
      expect(body).not.toContain("synthetic-query-token");
      expect(body).not.toContain("legacyBootstrap");
    }
    expect(rpcWork).toBe(0);
    const incompatible = await http.handler(new Request("http://test/ws"), Context.empty());
    expect(incompatible.status).toBe(426);
    expect(await incompatible.text()).toContain("orchestration_protocol_incompatible");
  } finally {
    await http.dispose();
  }
});

const encodeLegacyCancellationDiagnostic = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

class LegacyCancellationFixtureError extends Schema.TaggedError<LegacyCancellationFixtureError>()(
  "LegacyCancellationFixtureError",
  { message: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}

effectIt.layer(NodeServices.layer, { excludeTestServices: true })(
  "Authenticated legacy WS serialized forwarding",
  (it) => {
    it.effect.each([
      "operate",
      "read_only",
      "private_field",
      "deleted_error",
      "survivor_error",
      "actual_release",
      "actual_guard_rejected",
      "actual_sync",
      "actual_async",
      "actual_disconnect",
      "actual_input_refused",
      "actual_input_intent_lost",
      "actual_input_outcome_lost",
      "actual_entered_error",
      "actual_worktree_started_cancel",
      "actual_worktree_started_cancel_persistence_lost",
      "actual_worktree_started_cancel_readback_lost",
      "actual_worktree_started_cancel_reply_lost",
      "actual_worktree_sync_card",
      "actual_worktree_sync_disconnect_card",
      "actual_required_non_repository",
      "actual_required_missing_base",
      "actual_required_fetch_failed",
      "actual_required_fetch_unknown",
      "actual_worktree_async_card",
      "actual_worktree_cancel_pending",
      "actual_worktree_cancel_partial",
    ] as const)("preserves closed dispatch and authorization for %s", (scenario) =>
      Effect.gen(function* () {
        const fs = yield* WsFileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "legacy-ws-forwarding-" });
        const receivingDatabase = scenario.startsWith("actual_input_")
          ? makeSqlitePersistenceLive(`${cwd}/lexical.sqlite`).pipe(
              Layer.provide(NodeServices.layer),
            )
          : SqlitePersistenceMemory;
        const config = WsServerConfig.layerTest(cwd, `${cwd}/state`);
        const auth = WsEnvironmentAuth.layer.pipe(
          Layer.provideMerge(receivingDatabase),
          Layer.provideMerge(ServerSecretStore.layer),
          Layer.provideMerge(WsServerEnvironment.identityLayer),
          Layer.provide(config),
          Layer.provide(NodeServices.layer),
        );
        const timestamp = "2026-10-05T00:00:00.000Z";
        const threadId = ThreadId.make("wire-forward:T");
        const projectId = ProjectId.make("wire-forward:P");
        const commandId = CommandId.make("wire-forward:C");
        const messageId = MessageId.make("wire-forward:M");
        const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6" };
        const guard = {
          observedSnapshotSequence: 3,
          expectedModelSelection: modelSelection,
          expectedSessionStatus: null,
          expectedActiveTurnId: null,
          expectedLatestTurnId: null,
          requireIdle: true,
        };
        const projection = yield* Schema.decodeUnknownEffect(OrchestrationV2ThreadProjectionJson)({
          thread: {
            id: threadId,
            projectId,
            title: "Forwarding fixture",
            createdBy: "user",
            creationSource: "web",
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: {
              parentThreadId: null,
              relationshipToParent: null,
              rootThreadId: threadId,
            },
            forkedFrom: null,
            createdAt: timestamp,
            updatedAt: timestamp,
            archivedAt: null,
            deletedAt: null,
            settledAt: null,
            settledOverride: null,
            lastVisitedAt: null,
          },
          runs: [],
          attempts: [],
          nodes: [],
          subagents: [],
          providerSessions: [],
          providerThreads: [],
          providerTurns: [],
          runtimeRequests: [],
          messages: [],
          plans: [],
          turnItems: [],
          checkpointScopes: [],
          checkpoints: [],
          contextHandoffs: [],
          contextTransfers: [],
          visibleTurnItems: [],
          updatedAt: timestamp,
        });
        const actualReceiving = scenario.startsWith("actual_");
        const requiredPreflight = scenario.startsWith("actual_required_");
        const failedFetch =
          scenario === "actual_required_fetch_failed" ||
          scenario === "actual_required_fetch_unknown";
        let fetchCalls = 0;
        let worktreeCreateCalls = 0;
        let preflightStore: EventStore.EventStoreV2["Service"] | undefined;
        let preflightReceipts: Receipts.CommandReceiptStoreV2["Service"] | undefined;
        const cancelledWorktree = scenario.startsWith("actual_worktree_cancel_");
        const startedCancellation = scenario.startsWith("actual_worktree_started_cancel");
        const failurePersistenceLost =
          scenario === "actual_worktree_started_cancel_persistence_lost";
        const failureReadbackLost = scenario === "actual_worktree_started_cancel_readback_lost";
        const failureReplyLost = scenario === "actual_worktree_started_cancel_reply_lost";
        let failureReadbackCalls = 0;
        let failureReplyCalls = 0;
        const successfulWorktree =
          scenario === "actual_worktree_sync_card" ||
          scenario === "actual_worktree_sync_disconnect_card" ||
          scenario === "actual_worktree_async_card" ||
          startedCancellation;
        const asyncWorktree = scenario === "actual_worktree_async_card";
        const disconnectedWorktree = scenario === "actual_worktree_sync_disconnect_card";
        const worktreeEntered = yield* Deferred.make<void>();
        const worktreeBarrier = yield* Deferred.make<void>();
        const releaseWorktree = Deferred.succeed(worktreeBarrier, undefined);
        const worktreePath = `${cwd}/unresolved-checkout`;
        const startedSetup =
          scenario === "actual_sync" ||
          scenario === "actual_async" ||
          scenario === "actual_disconnect" ||
          successfulWorktree;
        const lexicalSetup = scenario.startsWith("actual_input_");
        const selectedSetup = startedSetup || lexicalSetup || scenario === "actual_entered_error";
        let lexicalRun: import("./RecordedTypes.ts").RecordedRun | undefined;
        const scripts: ReadonlyArray<ProjectScript> = selectedSetup
          ? [
              {
                id: "setup",
                name: "Captured setup",
                command: "synthetic-no-execution",
                icon: "configure",
                runOnWorktreeCreate: true,
                async: scenario === "actual_async" || asyncWorktree,
              },
            ]
          : [];
        const setupWritten = yield* Deferred.make<string>();
        let written = "";
        const outputListeners = new Set<(chunk: string) => void>();
        const exitListeners = new Set<(event: PtyAdapter.PtyExitEvent) => void>();
        const kills: Array<string | undefined> = [];
        let spawnCalls = 0;
        const process: PtyAdapter.PtyProcess = {
          pid: 92003,
          write: (data) => {
            written = data;
            Deferred.doneUnsafe(setupWritten, Effect.succeed(data));
          },
          resize: () => {},
          kill: (signal) => {
            kills.push(signal);
            for (const exit of exitListeners) exit({ exitCode: 0, signal: 15 });
          },
          onData: (callback) => {
            outputListeners.add(callback);
            return () => {
              outputListeners.delete(callback);
            };
          },
          onExit: (callback) => {
            exitListeners.add(callback);
            return () => {
              exitListeners.delete(callback);
            };
          },
        };
        const completeSetup = Effect.sync(() => {
          const sentinel = written.match(/__T3_SETUP_DONE___[a-f0-9]+:/u)?.[0];
          if (sentinel !== undefined)
            for (const output of outputListeners) output(`${sentinel}0\r\n`);
        });
        yield* Effect.addFinalizer(() => completeSetup);
        yield* Effect.addFinalizer(() => releaseWorktree);

        const receivingOwners = actualReceiving
          ? yield* Effect.gen(function* () {
              const manager = yield* WsTerminalManager.makeWithOptions({
                logsDir: `${cwd}/terminal-logs`,
                env: {},
                shellResolver: () => (lexicalSetup ? "/bin/synthetic\0shell" : "/bin/sh"),
                processTable: Effect.succeed([]),
                subprocessInspector: () =>
                  Effect.succeed({
                    hasRunningSubprocess: false,
                    childCommand: null,
                    processIds: [],
                  }),
                ptyAdapter: {
                  spawn: () => {
                    spawnCalls += 1;
                    if (scenario === "actual_entered_error")
                      return Effect.fail(
                        new PtyAdapter.PtySpawnError({
                          adapter: "synthetic",
                          shell: "/bin/sh",
                          cause: new Error("Synthetic entered failure"),
                        }),
                      );
                    return startedSetup
                      ? Effect.succeed(process)
                      : Effect.die("No setup/provider PTY in receiving RPC fixture");
                  },
                },
              }).pipe(Effect.provide(ProcessRunner.layer));
              const terminal = Layer.succeed(WsTerminalManager.TerminalManager, manager);
              const projectOwner = Layer.mock(WsProjectService.ProjectService)({
                getById: (id) =>
                  Effect.succeed(
                    id === projectId
                      ? Option.some({
                          id: projectId,
                          title: "Fixture",
                          workspaceRoot: cwd,
                          repositoryIdentity: null,
                          faviconPath: null,
                          defaultModelSelection: modelSelection,
                          defaultThreadEnvMode: null,
                          scripts,
                          createdAt: timestamp,
                          updatedAt: timestamp,
                          deletedAt: null,
                        })
                      : Option.none(),
                  ),
              });
              const runner = WsProjectSetupScriptRunner.layer.pipe(
                Layer.provide(
                  Layer.mergeAll(
                    projectOwner,
                    terminal,
                    WsServerSettings.layerTest(),
                    Layer.succeed(HostProcessEnvironment, {}),
                    Layer.succeed(HostProcessPlatform, "linux"),
                  ),
                ),
              );
              const registry = ProviderAdapterRegistry.makeLayer([
                {
                  instanceId: modelSelection.instanceId,
                  driver: ProviderDriverKind.make("codex"),
                  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
                  planSelectionTransition: () =>
                    Effect.succeed({ type: "apply_on_next_turn" as const }),
                  openSession: () => Effect.die("No provider entry in receiving RPC fixture"),
                } as ProviderAdapterV2Shape,
              ]);
              const orchestrator = makeOrchestratorV2ReplayLayerWithRegistry(
                { name: "legacy-authenticated-receiving" },
                registry,
                { databaseLayer: receivingDatabase, runEffectWorker: false },
              ).pipe(Layer.provide(terminal));
              const productionThreads = Threads.layer.pipe(Layer.provide(orchestrator));
              const threads = Layer.effect(
                Threads.ThreadManagementService,
                Effect.gen(function* () {
                  const actual = yield* Threads.ThreadManagementService;
                  return Threads.ThreadManagementService.of({
                    ...actual,
                    dispatch: (command) =>
                      actual.dispatch(command).pipe(
                        Effect.flatMap((result) => {
                          if (failureReplyLost && command.type === "prepared-run.fail") {
                            failureReplyCalls++;
                            return Effect.fail(
                              new OrchestratorDispatchError({
                                commandId: command.commandId,
                                commandType: command.type,
                                cause: "Synthetic accepted preparation failure reply lost",
                              }),
                            );
                          }
                          if (
                            command.type === "prepared-run.progress" &&
                            command.legacyPreparationUpdate?.type ===
                              (scenario === "actual_input_intent_lost" ? "intent" : "outcome") &&
                            "step" in command.legacyPreparationUpdate &&
                            command.legacyPreparationUpdate.step.effect.kind === "setup.open" &&
                            (scenario === "actual_input_intent_lost" ||
                              scenario === "actual_input_outcome_lost")
                          )
                            return Effect.fail(
                              new OrchestratorDispatchError({
                                commandId: command.commandId,
                                commandType: command.type,
                                cause: "Synthetic accepted setup journal reply lost",
                              }),
                            );
                          return Effect.succeed(result);
                        }),
                      ),
                  });
                }),
              ).pipe(Layer.provide(productionThreads));
              const receipts = Receipts.layer.pipe(Layer.provide(receivingDatabase));
              const outbox = WsEffectOutbox.layer.pipe(Layer.provide(receivingDatabase));
              const store = EventStore.layer.pipe(Layer.provide(receivingDatabase));
              const successfulCheckout = successfulWorktree
                ? yield* Effect.gen(function* () {
                    const common = `${cwd}/.git`;
                    const gitDirectory = `${common}/worktrees/card-fixture`;
                    yield* fs.makeDirectory(gitDirectory, { recursive: true });
                    let added = false;
                    const branch = "owned/forwarding";
                    const oid = "9".repeat(40);
                    const spawner = ChildProcessSpawner.make((command) =>
                      Effect.gen(function* () {
                        if (!ChildProcess.isStandardCommand(command))
                          return yield* Effect.die("Synthetic checkout refuses pipelines");
                        const args = [...command.args];
                        if (args.includes("add")) {
                          yield* fs.makeDirectory(worktreePath);
                          yield* fs.writeFileString(
                            `${worktreePath}/.git`,
                            `gitdir: ${gitDirectory}\n`,
                          );
                          added = true;
                        }
                        let stdout = "";
                        if (args.includes("--git-common-dir")) stdout = `${common}\n`;
                        else if (args.includes("--absolute-git-dir")) stdout = `${gitDirectory}\n`;
                        else if (args.includes("symbolic-ref")) stdout = `refs/heads/${branch}\n`;
                        else if (args.includes("rev-parse")) stdout = `${oid}\n`;
                        else if (args.includes("--porcelain"))
                          stdout = added
                            ? `worktree ${worktreePath}\0HEAD ${oid}\0branch refs/heads/${branch}\0\0`
                            : "";
                        else if (args.includes("for-each-ref") && added)
                          stdout = `refs/heads/${branch}\n`;
                        return ChildProcessSpawner.makeHandle({
                          pid: ChildProcessSpawner.ProcessId(1),
                          exitCode: Effect.succeed(
                            ChildProcessSpawner.ExitCode(args.includes("--get-regexp") ? 1 : 0),
                          ),
                          isRunning: Effect.succeed(false),
                          kill: () => Effect.die("No process to kill in synthetic checkout"),
                          unref: Effect.succeed(Effect.void),
                          stdin: Sink.drain,
                          stdout: Stream.encodeText(Stream.make(stdout)),
                          stderr: Stream.empty,
                          all: Stream.empty,
                          getInputFd: () => Sink.drain,
                          getOutputFd: () => Stream.empty,
                        });
                      }),
                    );
                    return yield* makeGitVcsDriverCore().pipe(
                      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
                      Effect.provide(config),
                    );
                  })
                : undefined;
              const external = Layer.mergeAll(
                terminal,
                projectOwner,
                runner,
                WsWorktreeSetupTracker.layer,
                Layer.mock(WsProjectCloneTracker.ProjectCloneTracker)({
                  get: () => Effect.succeed(null),
                }),
                Layer.mock(WsGitWorkflowService.GitWorkflowService)({
                  isRepository: () => Effect.succeed(scenario !== "actual_required_non_repository"),
                  hasCommit: () => Effect.succeed(scenario !== "actual_required_missing_base"),
                  remoteExists: () => Effect.succeed(true),
                  fetchRemote: () =>
                    Effect.gen(function* () {
                      if (
                        !failedFetch ||
                        preflightStore === undefined ||
                        preflightReceipts === undefined
                      )
                        return yield* Effect.die("Unexpected fetch in receiving preflight fixture");
                      const b = legacyBootstrapCreateCommandId(threadId, commandId);
                      const intentId = CommandId.make(`${b}:preflight-intent`);
                      const raw = Array.from(
                        yield* preflightStore
                          .readByCommandId({ commandId: intentId })
                          .pipe(Stream.runCollect),
                      );
                      const accepted = yield* preflightReceipts.getByCommandId(intentId);
                      expect(raw).toHaveLength(1);
                      expect(raw[0]?.event.type).toBe("legacy-bootstrap.preflight-intent");
                      expect(Option.isSome(accepted)).toBe(true);
                      if (Option.isSome(accepted)) {
                        expect(accepted.value.status).toBe("accepted");
                        expect(raw[0]?.sequence).toBe(accepted.value.resultSequence);
                      }
                      expect(Option.isNone(yield* preflightReceipts.getByCommandId(b))).toBe(true);
                      expect(
                        Option.isNone(yield* preflightReceipts.getByCommandId(commandId)),
                      ).toBe(true);
                      fetchCalls++;
                    }).pipe(
                      Effect.orDie,
                      Effect.andThen(
                        Effect.fail(
                          new GitCommandError({
                            operation: "GitWorkflowService.fetchRemote",
                            command: "git fetch origin main",
                            cwd,
                            detail: "Synthetic preflight fetch failed",
                            ...(scenario === "actual_required_fetch_failed" ? { exitCode: 1 } : {}),
                          }),
                        ),
                      ),
                    ),
                  createWorktree: (input, options) => {
                    worktreeCreateCalls++;
                    return successfulCheckout !== undefined
                      ? successfulCheckout.createWorktree({ ...input, path: worktreePath }, options)
                      : Effect.gen(function* () {
                          if (!cancelledWorktree || options?.legacyPreparation === undefined)
                            return yield* Effect.die("No Git create in receiving RPC fixture");
                          if (input.newRefName === undefined)
                            return yield* Effect.die(
                              "Synthetic checkout requires a captured branch",
                            );
                          const parent = yield* fs.stat(cwd);
                          yield* options.legacyPreparation.beforeEffect({
                            kind: "worktree.add",
                            cwd,
                            args: [
                              "worktree",
                              "add",
                              "-b",
                              input.newRefName,
                              worktreePath,
                              input.refName,
                            ],
                            worktreePath,
                            commonDirectory: cwd,
                            baseCommitOid: "9".repeat(40),
                            targetRef: `refs/heads/${input.newRefName}`,
                            before: {
                              parentPath: cwd,
                              parentRealPath: cwd,
                              parentDevice: String(parent.dev),
                              parentInode: String(parent.ino),
                              targetRefAbsent: true,
                              registrationAbsent: true,
                            },
                          });
                          if (scenario === "actual_worktree_cancel_partial") {
                            yield* fs.makeDirectory(worktreePath);
                            yield* fs.writeFileString(
                              `${worktreePath}/partial`,
                              "unresolved owner bytes",
                            );
                          }
                          yield* Deferred.succeed(worktreeEntered, undefined);
                          yield* Deferred.await(worktreeBarrier);
                          return yield* Effect.die(
                            "Unresolved synthetic checkout cannot report success",
                          );
                        }).pipe(
                          Effect.mapError(
                            (cause) =>
                              new GitCommandError({
                                operation: "synthetic unresolved preparation",
                                command: "git",
                                cwd,
                                detail: "Synthetic owner journal or fixture filesystem failed",
                                cause,
                              }),
                          ),
                        );
                  },
                  removeWorktree: () => Effect.die("No Git cleanup in receiving RPC fixture"),
                  renameBranch: () => Effect.die("No Git rename in receiving RPC fixture"),
                }),
                Layer.mock(TextGeneration.TextGeneration)({
                  generateThreadTitle: () => Effect.succeed({ title: "Forwarding fixture" }),
                  generateBranchName: () =>
                    Effect.die("No generated branch in receiving RPC fixture"),
                }),
                WsServerSettings.layerTest(),
                makeProviderRegistryLayer(),
                Layer.mock(WsManagedProjectFolders.ManagedProjectFolders)({
                  namedProjectsRoot: `${cwd}/unused-projects`,
                  folderForThread: () => Effect.succeed(Option.none()),
                }),
              );
              const launchSink = Layer.effect(
                EventSink.EventSinkV2,
                Effect.gen(function* () {
                  const actual = yield* EventSink.EventSinkV2;
                  return EventSink.EventSinkV2.of({
                    ...actual,
                    readByCommandId: (input) => {
                      if (
                        failureReadbackLost &&
                        input.commandId ===
                          `${legacyBootstrapCreateCommandId(threadId, commandId)}:fail`
                      ) {
                        failureReadbackCalls++;
                        return Stream.fail(
                          new EventSink.EventSinkStreamError({
                            threadId,
                            cause: "Synthetic committed preparation failure readback lost",
                          }),
                        );
                      }
                      return actual.readByCommandId(input);
                    },
                  });
                }),
              ).pipe(Layer.provide(orchestrator));
              const launch = WsThreadLaunchService.layer.pipe(
                Layer.provide(
                  Layer.mergeAll(
                    external,
                    threads,
                    receipts,
                    IdAllocator.layer,
                    outbox,
                    orchestrator,
                    store,
                    launchSink,
                  ),
                ),
              );
              return Layer.mergeAll(
                launch,
                threads,
                receipts,
                outbox,
                store,
                orchestrator,
                external,
                ProjectStore.layer.pipe(Layer.provide(receivingDatabase)),
                ProjectionStore.layer.pipe(Layer.provide(receivingDatabase)),
                ApplicationEvents.OrchestrationEventStoreLive.pipe(
                  Layer.provide(receivingDatabase),
                ),
              );
            })
          : Layer.empty;
        const forwarded: WsThreadLaunchService.ThreadLaunchInput[] = [];
        const preflight: Parameters<
          WsThreadLaunchService.ThreadLaunchService["Service"]["preflightLegacyBootstrap"]
        >[0][] = [];
        const order: string[] = [];
        const rpcOwners = Layer.mergeAll(
          WsTokenAccountingService.layer,
          Layer.mock(Threads.ThreadManagementService)({
            getThreadShell: () => Effect.succeed(null),
            dispatch: () => Effect.die("No ordinary dispatch fallback"),
          }),
          Layer.mock(WsApplicationEventStore.OrchestrationEventStore)({}),
          Layer.mock(ProjectStore.ProjectStoreV2)({}),
          Layer.mock(WsProjectService.ProjectService)({
            getById: () =>
              Effect.succeedSome({
                id: projectId,
                title: "Fixture",
                workspaceRoot: cwd,
                repositoryIdentity: null,
                faviconPath: null,
                defaultModelSelection: modelSelection,
                defaultThreadEnvMode: null,
                scripts: [],
                createdAt: timestamp,
                updatedAt: timestamp,
                deletedAt: null,
              }),
          }),
          Layer.mock(WsManagedProjectFolders.ManagedProjectFolders)({
            namedProjectsRoot: "/synthetic-unused-projects",
          }),
          Layer.mock(WsThreadSearch.ThreadSearch)({}),
          Layer.mock(WsProviderSessionManager.ProviderSessionManagerV2)({}),
          Layer.mock(WsAnalyticsService.AnalyticsService)({ record: () => Effect.void }),
          Layer.mock(WsThreadLaunchService.ThreadLaunchService)({
            preflightLegacyBootstrap: (binding) =>
              Effect.sync(() => {
                order.push("preflight");
                preflight.push(binding);
                return {
                  binding,
                  intentCommandId: CommandId.make("wire-forward:mock-preflight-intent"),
                  intentSequence: 1,
                  status: "ready" as const,
                  workspaceStrategy: {
                    type: "worktree" as const,
                    baseRef: "main",
                    branch: "owned/forwarding",
                    startFromOrigin: false,
                  },
                };
              }),
            launch: (input) =>
              Effect.suspend(() => {
                order.push("launch");
                forwarded.push(input);
                if (scenario === "deleted_error" || scenario === "survivor_error")
                  return Effect.fail(
                    new WsThreadLaunchService.ThreadLaunchError({
                      operation: "provision-worktree",
                      commandId: input.commandId,
                      projectId: input.projectId,
                      threadId,
                      cause: new Error("worktree exploded"),
                      ...(scenario === "deleted_error"
                        ? { bootstrapThreadDisposition: "deleted" as const }
                        : {}),
                    }),
                  );
                return Effect.succeed({
                  threadId,
                  projection,
                  resumed: false,
                  legacyReleaseSequence: 3,
                });
              }),
          }),
          Layer.mock(WsScheduledTasks.ScheduledTaskService)({}),
          Layer.mock(WsPullRequestService.PullRequestService)({}),
          Layer.mock(WsPullRequestSyncReactor.PullRequestSyncReactor)({}),
          Layer.mock(WsDeviceService.DeviceService)({}),
          Layer.mock(WsOrchestrator.OrchestratorV2)({
            dispatch: () => Effect.die("No native command fallback"),
          }),
          Layer.mock(WsUsageService.UsageService)({}),
          Layer.mock(WsUsageLimitSources.UsageLimitSources)({}),
          Layer.mock(WsProjectSetupScriptRunner.ProjectSetupScriptRunner)({}),
          Layer.mock(WsWorktreeSetupTracker.WorktreeSetupTracker)({}),
          Layer.mock(WsProjectCloneTracker.ProjectCloneTracker)({
            get: () => Effect.succeed(null),
          }),
          Layer.mock(WsRepositoryIdentityResolver.RepositoryIdentityResolver)({}),
          Layer.mock(WsAgentSessionImporter.AgentSessionImporter)({}),
          Layer.mock(WsCheckpointDiffQuery.CheckpointDiffQuery)({}),
          Layer.mock(WsKeybindings.Keybindings)({}),
          Layer.mock(WsEnvironmentTheme.EnvironmentThemeService)({}),
          Layer.mock(WsExternalLauncher.ExternalLauncher)({}),
          Layer.mock(WsRemoteOpenTargets.RemoteOpenTargets)({}),
          Layer.mock(WsGitWorkflowService.GitWorkflowService)({}),
          Layer.mock(WsReviewService.ReviewService)({}),
          Layer.mock(WsVcsProvisioningService.VcsProvisioningService)({}),
          Layer.mock(WsVcsStatusBroadcaster.VcsStatusBroadcaster)({}),
          Layer.mock(WsTerminalManager.TerminalManager)({}),
          Layer.mock(WsPreviewManager.PreviewManager)({}),
          Layer.mock(WsPortScanner.PortDiscovery)({}),
          Layer.mock(WsProviderRegistry.ProviderRegistry)({}),
          Layer.mock(WsModelManifest.ModelManifest)({}),
          Layer.succeed(WsProviderMaintenance.ProviderVersionCache, new Map()),
          Layer.mock(WsProviderInstanceRegistry.ProviderInstanceRegistry)({}),
          Layer.mock(WsAcpRegistrySupport.AcpRegistryCatalog)({}),
          Layer.mock(WsAcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator)({}),
          Layer.mock(WsProviderAuthService.ProviderAuthService)({}),
          Layer.mock(WsServerSelfUpdate.ServerSelfUpdate)({}),
          Layer.mock(WsServerLifecycleEvents.ServerLifecycleEvents)({}),
          Layer.mock(WsServerSettings.ServerSettingsService)({
            getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
          }),
          Layer.mock(WsServerRuntimeStartup.ServerRuntimeStartup)({
            enqueueCommand: (effect) => effect,
          }),
          Layer.mock(WsWorkspaceEntries.WorkspaceEntries)({}),
          Layer.mock(WsWorkspaceFileSystem.WorkspaceFileSystem)({}),
          Layer.mock(WsBackgroundPolicy.BackgroundPolicy)({}),
          Layer.mock(WsSourceControlRepositoryService.SourceControlRepositoryService)({}),
          Layer.mock(WsProcessDiagnostics.ProcessDiagnostics)({}),
          Layer.mock(WsHostResources.HostResources)({}),
          Layer.mock(WsProcessResourceMonitor.ProcessResourceMonitor)({}),
          Layer.mock(WsResourceTelemetry.ResourceTelemetry)({}),
          Layer.mock(WsRelayClient.RelayClient)({}),
          Layer.mock(WsPreviewAutomationBroker.PreviewAutomationBroker)({}),
          Layer.mock(WsServerEnvironment.ServerEnvironment)({}),
          Layer.mock(WsAntigravityInstallation.AntigravityInstallation)({
            managedDirectory: "/synthetic-unused-antigravity",
          }),
          Layer.mock(WsCodexInstallation.CodexInstallation)({
            managedDirectory: "/synthetic-unused-codex",
          }),
          Layer.mock(WsVcsProcess.VcsProcess)({}),
          Layer.mock(WsWorkspacePaths.WorkspacePaths)({}),
          Layer.succeed(
            WsHttpClient.HttpClient,
            WsHttpClient.make(() => Effect.die("Auth gate must not call HTTP client.")),
          ),
          Layer.mock(WsTraceDiagnostics.TraceDiagnostics)({}),
          Layer.mock(WsProjectFaviconResolver.ProjectFaviconResolver)({}),
          Layer.mock(EventSink.EventSinkV2)({}),
          Layer.mock(WsEffectOutbox.EffectOutboxV2)({
            listByThreadId: () => Effect.succeed([]),
            awaitCompletion: () => Effect.void,
          }),
          Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({}),
          Layer.succeed(HostProcessEnvironment, {}),
          Layer.succeed(HostProcessPlatform, "linux"),
        );

        const dependencies = Layer.mergeAll(auth, rpcOwners, receivingOwners).pipe(
          Layer.provideMerge(config),
          Layer.provideMerge(NodeServices.layer),
        );
        yield* Effect.gen(function* () {
          const owner = yield* WsEnvironmentAuth.EnvironmentAuth;
          const issued = yield* owner.issueSession({
            scopes:
              cancelledWorktree || successfulWorktree
                ? ["orchestration:read", "orchestration:operate"]
                : [scenario === "read_only" ? "orchestration:read" : "orchestration:operate"],
          });
          if (actualReceiving) {
            const sink = yield* EventSink.EventSinkV2;
            if (requiredPreflight) {
              preflightStore = yield* EventStore.EventStoreV2;
              preflightReceipts = yield* Receipts.CommandReceiptStoreV2;
            }
            const projectCommandId = CommandId.make("wire-forward:project-create");
            yield* sink.commitProjectCommand({
              commandId: projectCommandId,
              projectId,
              commandType: "project.create",
              acceptedAt: yield* DateTime.now,
              event: {
                eventId: EventId.make(`${projectCommandId}:event`),
                aggregateKind: "project",
                aggregateId: projectId,
                occurredAt: timestamp,
                commandId: projectCommandId,
                causationEventId: null,
                correlationId: null,
                metadata: {},
                type: "project.created",
                payload: {
                  projectId,
                  title: "Fixture",
                  workspaceRoot: cwd,
                  defaultModelSelection: modelSelection,
                  scripts,
                  createdAt: timestamp,
                  updatedAt: timestamp,
                },
              },
            });
          }
          const input = {
            type: "thread.turn.start",
            commandId,
            threadId,
            createdAt: timestamp,
            runtimeMode: "full-access",
            interactionMode: "default",
            modelSelection,
            message: { messageId, role: "user", text: "Forward only", attachments: [] },
            ...(actualReceiving && scenario !== "actual_guard_rejected"
              ? {}
              : { dispatchGuard: guard }),
            bootstrap: {
              createThread: {
                projectId,
                title: "Forwarding fixture",
                modelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
                branch: null,
                worktreePath: null,
                createdAt: timestamp,
              },
              ...(actualReceiving && !cancelledWorktree && !successfulWorktree && !requiredPreflight
                ? {}
                : {
                    prepareWorktree: {
                      projectCwd: cwd,
                      baseBranch: "main",
                      branch: "owned/forwarding",
                      startFromOrigin: failedFetch,
                      requireWorktree: true,
                    },
                  }),
              runSetupScript: actualReceiving,
            },
            ...(scenario === "private_field" ? { legacyBootstrap: { ownsNewThread: true } } : {}),
          };
          const incoming = yield* WsQueue.unbounded<Uint8Array | string>();
          const outgoing = yield* WsQueue.unbounded<Uint8Array | string>();
          const socket: WsSocket.Socket = {
            [WsSocket.TypeId]: WsSocket.TypeId,
            reader: Effect.succeed({
              pull: WsQueue.take(incoming).pipe(Effect.map((chunk) => [chunk] as const)),
              upgrade: () => Effect.die("No TLS/native socket in fixture"),
            }),
            writer: Effect.succeed({
              write: (chunk) =>
                WsSocket.isCloseEvent(chunk)
                  ? Effect.void
                  : WsQueue.offer(outgoing, chunk).pipe(Effect.asVoid),
              writeAll: (chunks) => WsQueue.offerAll(outgoing, chunks).pipe(Effect.asVoid),
            }),
          };
          const withUpgrade = (
            request: WsHttpServerRequest.HttpServerRequest,
            connectionSocket: WsSocket.Socket = socket,
          ): WsHttpServerRequest.HttpServerRequest =>
            new Proxy(request, {
              get(target, property) {
                if (property === "upgrade") return Effect.succeed(connectionSocket);
                if (property === "modify")
                  return (
                    options: Parameters<WsHttpServerRequest.HttpServerRequest["modify"]>[0],
                  ) => withUpgrade(target.modify(options), connectionSocket);
                return Reflect.get(target, property, target);
              },
            });
          const request = withUpgrade(
            WsHttpServerRequest.fromWeb(
              new Request(
                `http://test/ws?${ORCHESTRATION_PROTOCOL_QUERY_PARAM}=${ORCHESTRATION_PROTOCOL_VERSION_TEXT}`,
                { headers: { authorization: `Bearer ${issued.token}` } },
              ),
            ),
          );
          const handler = yield* HttpRouter.toHttpEffect(websocketRpcRouteLayer);
          const serving = yield* handler.pipe(
            Effect.provideService(WsHttpServerRequest.HttpServerRequest, request),
            Effect.scoped,
            Effect.forkChild,
          );
          yield* Effect.addFinalizer(() => WsFiber.interrupt(serving));
          yield* WsQueue.offer(
            incoming,
            yield* encodeSocketRpcRequest({
              _tag: "Request",
              id: "1",
              tag: ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
              payload: input,
              headers: [],
            }),
          );
          if (requiredPreflight) {
            const rawFrame = yield* Effect.race(
              WsQueue.take(outgoing),
              WsFiber.join(serving).pipe(
                Effect.andThen(Effect.die("Socket ended before preflight failure")),
              ),
            ).pipe(Effect.timeout("5 seconds"));
            const frame = yield* decodeSocketRpcResponse(
              typeof rawFrame === "string" ? rawFrame : new TextDecoder().decode(rawFrame),
            );
            expect(frame.requestId).toBe("1");
            expect(frame.exit._tag).toBe("Failure");
            if (frame.exit._tag !== "Failure")
              return yield* Effect.die("Preflight failure required");
            const failures = yield* Schema.decodeUnknownEffect(
              Schema.Array(
                Schema.Struct({
                  _tag: Schema.Literal("Fail"),
                  error: OrchestrationDispatchCommandError,
                }),
              ),
            )(frame.exit.cause);
            expect(failures).toHaveLength(1);
            expect(failures[0]?.error.bootstrapThreadDisposition).toBe(
              scenario === "actual_required_fetch_unknown" ? undefined : "not-created",
            );
            expect(failures[0]?.error.message).toContain(
              failedFetch ? "fetch failed" : "separate worktree requires",
            );
            const threads = yield* Threads.ThreadManagementService;
            const receipts = yield* Receipts.CommandReceiptStoreV2;
            const store = yield* EventStore.EventStoreV2;
            const b = legacyBootstrapCreateCommandId(threadId, commandId);
            expect(yield* threads.getThreadShell(threadId)).toBeNull();
            for (const id of [b, CommandId.make(`${b}:initial-message`), commandId]) {
              expect(Option.isNone(yield* receipts.getByCommandId(id))).toBe(true);
              expect(Option.isNone(yield* receipts.getProjectByCommandId(id))).toBe(true);
              expect(
                Array.from(yield* store.readByCommandId({ commandId: id }).pipe(Stream.runCollect)),
              ).toHaveLength(0);
            }
            const recorded = Array.from(yield* store.read({ threadId }).pipe(Stream.runCollect));
            expect(recorded.map((entry) => entry.event.type)).toEqual([
              "legacy-bootstrap.preflight-intent",
              "legacy-bootstrap.preflight-outcome",
            ]);
            const intent = recorded[0];
            const outcome = recorded[1];
            if (
              intent?.event.type !== "legacy-bootstrap.preflight-intent" ||
              outcome?.event.type !== "legacy-bootstrap.preflight-outcome"
            )
              return yield* Effect.die("Authentic private preflight records required");
            expect(outcome.event.payload.status).toBe(
              scenario === "actual_required_fetch_unknown" ? "unknown" : "known_failed",
            );
            expect(outcome.event.payload.binding).toEqual(intent.event.payload);
            expect(outcome.event.payload.intentSequence).toBe(intent.sequence);
            expect(intent.event.payload.policy.createCommandId).toBe(b);
            expect(intent.event.payload.policy.releaseCommandId).toBe(commandId);
            expect(intent.event.payload.policy.messageId).toBe(messageId);
            expect(intent.event.payload.policy.threadId).toBe(threadId);
            expect(intent.event.payload.policy.projectId).toBe(projectId);
            expect(intent.event.payload.fetch.cwd).toBe(cwd);
            expect(intent.event.payload.fetch.startFromOrigin).toBe(failedFetch);
            for (const entry of recorded) {
              const recordedCommandId = entry.commandId;
              expect(recordedCommandId).not.toBeNull();
              if (recordedCommandId === null)
                return yield* Effect.die("Private preflight command binding required");
              const receipt = yield* receipts.getByCommandId(recordedCommandId);
              expect(Option.isSome(receipt)).toBe(true);
              if (Option.isSome(receipt)) {
                expect(receipt.value.status).toBe("accepted");
                expect(receipt.value.resultSequence).toBe(entry.sequence);
              }
            }
            const observation = yield* makeCommandObservationQuery();
            const observed = yield* observation.observe({ threadId, commandId, messageId });
            expect(observed.commandStatus).toBe("not_found");
            expect(observed.turn).toBeNull();
            const outbox = yield* WsEffectOutbox.EffectOutboxV2;
            expect(yield* outbox.listByThreadId(threadId)).toEqual([]);
            expect(yield* fs.exists(worktreePath)).toBe(false);
            expect(fetchCalls).toBe(failedFetch ? 1 : 0);
            expect(worktreeCreateCalls).toBe(0);
            expect(spawnCalls).toBe(0);
            expect(written).toBe("");
            expect(kills).toHaveLength(0);
            yield* WsFiber.interrupt(serving);
            return;
          }
          if (successfulWorktree) {
            yield* Deferred.await(setupWritten).pipe(Effect.timeout("5 seconds"));
            const sink = yield* EventSink.EventSinkV2;
            yield* sink.stream({ threadId, eventType: "run.updated" }).pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "run.updated" &&
                  stored.event.payload.legacyPreparation?.steps.some(
                    (step) => step.effect.kind === "setup.write" && step.state === "known_started",
                  ) === true,
              ),
              Stream.runHead,
              Effect.timeout("5 seconds"),
            );
            const receipts = yield* Receipts.CommandReceiptStoreV2;
            const threads = yield* Threads.ThreadManagementService;
            const store = yield* EventStore.EventStoreV2;
            const tracker = yield* WsWorktreeSetupTracker.WorktreeSetupTracker;
            if (asyncWorktree) {
              yield* sink.stream({ threadId, eventType: "run.updated" }).pipe(
                Stream.filter((stored) => stored.commandId === commandId),
                Stream.runHead,
                Effect.timeout("5 seconds"),
              );
            } else {
              expect(Option.isNone(yield* receipts.getByCommandId(commandId))).toBe(true);
            }
            const preparing = yield* threads.getThreadProjection(threadId);
            const steps = preparing.runs[0]?.legacyPreparation?.steps;
            expect(steps?.map((step) => [step.effect.kind, step.state])).toEqual([
              ["worktree.add", "known_succeeded"],
              ["worktree.base-config", "known_succeeded"],
              ["setup.open", "known_succeeded"],
              ["setup.write", "known_started"],
            ]);
            expect(preparing.thread.worktreePath).toBe(worktreePath);
            expect(preparing.thread.branch).toBe("owned/forwarding");
            let setupIncoming = incoming;
            let setupOutgoing = outgoing;
            const readSetupFrame = () =>
              WsQueue.take(setupOutgoing).pipe(
                Effect.flatMap((raw) =>
                  Schema.decodeUnknownEffect(
                    Schema.fromJsonString(
                      Schema.Struct({
                        _tag: Schema.String,
                        requestId: Schema.String,
                        values: Schema.optional(Schema.Array(WorktreeSetupStreamEvent)),
                        exit: Schema.optional(
                          Schema.Struct({
                            _tag: Schema.String,
                            value: Schema.optional(Schema.Struct({ sequence: Schema.Number })),
                          }),
                        ),
                      }),
                    ),
                  )(typeof raw === "string" ? raw : new TextDecoder().decode(raw)),
                ),
                Effect.timeout("5 seconds"),
              );
            yield* WsQueue.offer(
              incoming,
              yield* encodeSocketRpcRequest({
                _tag: "Request",
                id: "2",
                tag: WS_METHODS.subscribeWorktreeSetup,
                payload: { threadId },
                headers: [],
              }),
            );
            let responseSequence: unknown;
            let runningCard: WorktreeSetupStreamEvent | undefined;
            for (
              let count = 0;
              count < 8 &&
              (runningCard === undefined || (asyncWorktree && responseSequence === undefined));
              count++
            ) {
              const frame = yield* readSetupFrame();
              if (frame.requestId === "1") {
                expect(frame.exit?._tag).toBe("Success");
                responseSequence = frame.exit?.value?.sequence;
              }
              if (frame.requestId === "2" && frame.values !== undefined) {
                runningCard = frame.values.find((card) => card?.phase === "running");
                yield* WsQueue.offer(
                  incoming,
                  yield* encodeSocketRpcRequest({ _tag: "Ack", requestId: "2" }),
                );
              }
            }
            expect(runningCard?.phase).toBe("running");
            expect(runningCard?.stages.find((stage) => stage.id === "fetch")?.status).toBe(
              "skipped",
            );
            expect(runningCard?.stages.find((stage) => stage.id === "checkout")?.status).toBe(
              "done",
            );
            expect(runningCard?.stages.find((stage) => stage.id === "setup-script")?.status).toBe(
              "running",
            );
            expect(runningCard?.stages.find((stage) => stage.id === "agent")?.status).toBe(
              asyncWorktree ? "done" : "pending",
            );
            expect(runningCard?.worktreePath).toBe(worktreePath);
            expect(runningCard?.setupScript?.command).toBe("synthetic-no-execution");
            if (!asyncWorktree) expect(responseSequence).toBeUndefined();
            if (startedCancellation) {
              const run = preparing.runs[0];
              const preparation = run?.legacyPreparation;
              if (
                run === undefined ||
                preparation === undefined ||
                preparation.setup.status !== "resolved"
              )
                return yield* Effect.die("Authentic captured setup and control required");
              const actualManager = yield* WsTerminalManager.TerminalManager;
              const ownedGuard = actualManager.withLegacyOwnedControlGuard;
              if (ownedGuard === undefined)
                return yield* Effect.die("Actual owned control guard required");
              const binding = {
                version: 1 as const,
                policy: preparation.policy,
                runId: run.id,
                threadId,
                claimEventId: preparation.claimEventId,
                claimSequence: preparation.claimSequence,
                claimReceiptSequence: preparation.claimReceiptSequence,
                birthEventId: preparation.birthEventId,
                birthSequence: preparation.birthSequence,
                birthReceiptSequence: preparation.birthReceiptSequence,
                preparationGeneration: preparation.generation,
                terminalId: preparation.setup.definition.terminalId,
                generation: preparation.setup.definition.generation,
              };
              expect(
                yield* ownedGuard(binding, Effect.succeed(true)).pipe(
                  Effect.timeout("2 seconds"),
                  Effect.mapError(
                    (cause) =>
                      new LegacyCancellationFixtureError({
                        message: "Owned control guard before cancellation did not settle",
                        cause,
                      }),
                  ),
                ),
              ).toBe(true);
              if (failurePersistenceLost) {
                const sql = yield* SqlClient.SqlClient;
                const failureId = `${legacyBootstrapCreateCommandId(threadId, commandId)}:fail`;
                yield* sql.unsafe(
                  `CREATE TEMP TRIGGER reject_started_cancel_fail BEFORE INSERT ON orchestration_events WHEN NEW.command_id = '${failureId.replaceAll("'", "''")}' BEGIN SELECT RAISE(ABORT, 'synthetic failure transaction refusal'); END`,
                );
              }
              yield* WsQueue.offer(
                incoming,
                yield* encodeSocketRpcRequest({
                  _tag: "Request",
                  id: "3",
                  tag: WS_METHODS.worktreeSetupCancel,
                  payload: { threadId },
                  headers: [],
                }),
              );
              const cancellationFrames: Array<{
                requestId: string;
                tag: string;
                exit?: string;
                phases?: Array<string | null>;
              }> = [];
              const readCancellationFrame = () =>
                WsQueue.take(outgoing).pipe(
                  Effect.flatMap((raw) =>
                    Schema.decodeUnknownEffect(
                      Schema.fromJsonString(
                        Schema.Struct({
                          _tag: Schema.String,
                          requestId: Schema.String,
                          values: Schema.optional(Schema.Array(WorktreeSetupStreamEvent)),
                          exit: Schema.optional(
                            Schema.Struct({
                              _tag: Schema.String,
                              value: Schema.optional(Schema.Unknown),
                              cause: Schema.optional(Schema.Unknown),
                            }),
                          ),
                        }),
                      ),
                    )(typeof raw === "string" ? raw : new TextDecoder().decode(raw)),
                  ),
                  Effect.timeout("5 seconds"),
                  Effect.mapError(
                    (cause) =>
                      new LegacyCancellationFixtureError({
                        message: `Started cancellation RPC/card frame did not arrive: ${encodeLegacyCancellationDiagnostic(cancellationFrames)}`,
                        cause,
                      }),
                  ),
                );
              let cancelAccepted = false;
              let dispatchFailed = false;
              let cancelledCard: WorktreeSetupStreamEvent | undefined;
              for (
                let count = 0;
                count < 20 && (!cancelAccepted || !dispatchFailed || cancelledCard === undefined);
                count++
              ) {
                const frame = yield* readCancellationFrame();
                cancellationFrames.push({
                  requestId: frame.requestId,
                  tag: frame._tag,
                  ...(frame.exit === undefined ? {} : { exit: frame.exit._tag }),
                  ...(frame.values === undefined
                    ? {}
                    : { phases: frame.values.map((card) => card?.phase ?? null) }),
                });
                if (frame.requestId === "3") {
                  expect(frame.exit).toEqual({ _tag: "Success", value: { cancelled: true } });
                  cancelAccepted = true;
                }
                if (frame.requestId === "1") {
                  expect(frame.exit?._tag).toBe("Failure");
                  if (failurePersistenceLost || failureReadbackLost) {
                    expect(encodeLegacyCancellationDiagnostic(frame.exit?.cause ?? null)).toContain(
                      "read-receipt",
                    );
                    expect(
                      encodeLegacyCancellationDiagnostic(frame.exit?.cause ?? null),
                    ).not.toContain("Worktree setup cancelled.");
                  } else {
                    expect(encodeLegacyCancellationDiagnostic(frame.exit?.cause ?? null)).toContain(
                      "Worktree setup cancelled.",
                    );
                    const failures = yield* Schema.decodeUnknownEffect(
                      Schema.Array(
                        Schema.Struct({
                          _tag: Schema.Literal("Fail"),
                          error: OrchestrationDispatchCommandError,
                        }),
                      ),
                    )(frame.exit?.cause);
                    expect(failures).toHaveLength(1);
                    expect(failures[0]?.error.message).toBe("Worktree setup cancelled.");
                  }
                  expect(
                    encodeLegacyCancellationDiagnostic(frame.exit?.cause ?? null),
                  ).not.toContain('"bootstrapThreadDisposition":"deleted"');
                  dispatchFailed = true;
                }
                if (frame.requestId === "2" && frame.values !== undefined) {
                  cancelledCard = frame.values.find((card) => card?.phase === "cancelled");
                  yield* WsQueue.offer(
                    incoming,
                    yield* encodeSocketRpcRequest({ _tag: "Ack", requestId: "2" }),
                  );
                }
              }
              expect(cancelAccepted).toBe(true);
              expect(dispatchFailed).toBe(true);
              expect(cancelledCard?.phase).toBe("cancelled");
              expect(cancelledCard?.stages.find((stage) => stage.id === "agent")?.status).toBe(
                "pending",
              );
              expect(cancelledCard).toEqual(yield* tracker.get(threadId));
              const received = yield* threads.getThreadProjection(threadId);
              expect(received.thread.deletedAt).toBeNull();
              if (failurePersistenceLost) expect(received.runs[0]?.status).toBe("preparing");
              else {
                expect(received.runs[0]?.status).toBe("failed");
              }
              expect(received.runs[0]?.legacyPreparation?.steps).toEqual(steps);
              expect(received.runs[0]?.legacyPreparationFailureDecision).toBeUndefined();
              expect(Option.isNone(yield* receipts.getByCommandId(commandId))).toBe(true);
              const b = legacyBootstrapCreateCommandId(threadId, commandId);
              const failureId = CommandId.make(`${b}:fail`);
              const failure = yield* receipts.getByCommandId(failureId);
              const rawFailure = Array.from(
                yield* store.readByCommandId({ commandId: failureId }).pipe(Stream.runCollect),
              );
              if (failurePersistenceLost) {
                expect(Option.isNone(failure)).toBe(true);
                expect(rawFailure).toHaveLength(0);
              } else {
                expect(Option.isSome(failure)).toBe(true);
                expect(rawFailure.map((entry) => entry.event.type)).toEqual([
                  "run-attempt.updated",
                  "node.updated",
                  "turn-item.updated",
                  "turn-item.updated",
                  "run.updated",
                ]);
                if (Option.isSome(failure)) {
                  expect(failure.value.status).toBe("accepted");
                  expect(failure.value.commandType).toBe("prepared-run.fail");
                  expect(failure.value.threadId).toBe(threadId);
                  expect(rawFailure.at(-1)?.sequence).toBe(failure.value.resultSequence);
                }
                const failed = rawFailure.find((entry) => entry.event.type === "run.updated");
                expect(failed?.event.type === "run.updated" && failed.event.payload).toEqual(
                  received.runs[0],
                );
                const error = rawFailure.find(
                  (entry) =>
                    entry.event.type === "turn-item.updated" &&
                    entry.event.payload.type === "error",
                );
                expect(
                  error?.event.type === "turn-item.updated" &&
                    error.event.payload.type === "error" &&
                    error.event.payload.failure.message,
                ).toBe("Workspace preparation failed: Worktree setup cancelled.");
              }
              expect(failureReadbackCalls).toBe(failureReadbackLost ? 1 : 0);
              expect(failureReplyCalls).toBe(failureReplyLost ? 1 : 0);
              expect(Option.isNone(yield* receipts.getProjectByCommandId(commandId))).toBe(true);
              expect(
                Array.from(yield* store.readByCommandId({ commandId }).pipe(Stream.runCollect)),
              ).toHaveLength(0);
              expect(
                Option.isNone(
                  yield* receipts.getByCommandId(CommandId.make(`${b}:failure-delete`)),
                ),
              ).toBe(true);
              expect(
                Option.isNone(
                  yield* receipts.getByCommandId(CommandId.make(`${b}:workspace-failure-delete`)),
                ),
              ).toBe(true);
              expect(
                Option.isNone(
                  yield* receipts.getByCommandId(CommandId.make(`${b}:guard-rejection-delete`)),
                ),
              ).toBe(true);
              const outbox = yield* WsEffectOutbox.EffectOutboxV2;
              expect(
                (yield* outbox.listByThreadId(threadId)).filter((entry) =>
                  ["provider-turn.start", "terminal.cleanup", "attachment.cleanup"].includes(
                    entry.request.type,
                  ),
                ),
              ).toHaveLength(0);
              expect(
                yield* ownedGuard(binding, Effect.succeed(true)).pipe(
                  Effect.timeout("2 seconds"),
                  Effect.mapError(
                    (cause) =>
                      new LegacyCancellationFixtureError({
                        message: "Owned control guard after cancellation did not settle",
                        cause,
                      }),
                  ),
                ),
              ).toBe(true);
              expect(yield* fs.readFileString(`${worktreePath}/.git`)).toContain("gitdir:");
              expect(spawnCalls).toBe(1);
              expect(kills).toHaveLength(0);
              const observation = yield* makeCommandObservationQuery();
              const observed = yield* observation.observe({ threadId, commandId, messageId });
              expect(observed.commandStatus).toBe("not_found");
              expect(observed.turn).toBeNull();
              yield* WsFiber.interrupt(serving);
              return;
            }
            if (disconnectedWorktree) {
              yield* WsFiber.interrupt(serving);
              expect(Option.isNone(yield* receipts.getByCommandId(commandId))).toBe(true);
              expect(Option.isNone(yield* receipts.getProjectByCommandId(commandId))).toBe(true);
              expect(
                Array.from(yield* store.readByCommandId({ commandId }).pipe(Stream.runCollect)),
              ).toHaveLength(0);
              const retained = yield* threads.getThreadProjection(threadId);
              expect(retained.runs[0]?.status).toBe("preparing");
              expect(retained.runs[0]?.id).toBe(preparing.runs[0]?.id);
              expect(retained.runs[0]?.legacyPreparation).toEqual(
                preparing.runs[0]?.legacyPreparation,
              );
              expect(retained.thread.deletedAt).toBeNull();
              expect(kills).toHaveLength(0);

              setupIncoming = yield* WsQueue.unbounded<Uint8Array | string>();
              setupOutgoing = yield* WsQueue.unbounded<Uint8Array | string>();
              const resumedSocket: WsSocket.Socket = {
                [WsSocket.TypeId]: WsSocket.TypeId,
                reader: Effect.succeed({
                  pull: WsQueue.take(setupIncoming).pipe(Effect.map((chunk) => [chunk] as const)),
                  upgrade: () => Effect.die("No TLS/native socket in resumed fixture"),
                }),
                writer: Effect.succeed({
                  write: (chunk) =>
                    WsSocket.isCloseEvent(chunk)
                      ? Effect.void
                      : WsQueue.offer(setupOutgoing, chunk).pipe(Effect.asVoid),
                  writeAll: (chunks) => WsQueue.offerAll(setupOutgoing, chunks).pipe(Effect.asVoid),
                }),
              };
              const resumedRequest = withUpgrade(
                WsHttpServerRequest.fromWeb(
                  new Request(
                    `http://test/ws?${ORCHESTRATION_PROTOCOL_QUERY_PARAM}=${ORCHESTRATION_PROTOCOL_VERSION_TEXT}`,
                    { headers: { authorization: `Bearer ${issued.token}` } },
                  ),
                ),
                resumedSocket,
              );
              const resumedServing = yield* handler.pipe(
                Effect.provideService(WsHttpServerRequest.HttpServerRequest, resumedRequest),
                Effect.scoped,
                Effect.forkChild,
              );
              yield* Effect.addFinalizer(() => WsFiber.interrupt(resumedServing));
              yield* WsQueue.offer(
                setupIncoming,
                yield* encodeSocketRpcRequest({
                  _tag: "Request",
                  id: "2",
                  tag: WS_METHODS.subscribeWorktreeSetup,
                  payload: { threadId },
                  headers: [],
                }),
              );
            }
            yield* completeSetup;
            let doneCard: WorktreeSetupStreamEvent | undefined;
            for (
              let count = 0;
              count < 20 &&
              (doneCard === undefined || (!disconnectedWorktree && responseSequence === undefined));
              count++
            ) {
              const frame = yield* readSetupFrame();
              if (frame.requestId === "1") {
                expect(frame.exit?._tag).toBe("Success");
                responseSequence = frame.exit?.value?.sequence;
              }
              if (frame.requestId === "2" && frame.values !== undefined) {
                doneCard = frame.values.find((card) => card?.phase === "done");
                yield* WsQueue.offer(
                  setupIncoming,
                  yield* encodeSocketRpcRequest({ _tag: "Ack", requestId: "2" }),
                );
              }
            }
            expect(doneCard?.phase).toBe("done");
            expect(doneCard?.stages.find((stage) => stage.id === "setup-script")?.status).toBe(
              "done",
            );
            expect(doneCard?.stages.find((stage) => stage.id === "agent")?.status).toBe("done");
            expect(doneCard).toEqual(yield* tracker.get(threadId));
            const c = yield* receipts.getByCommandId(commandId);
            expect(Option.isSome(c)).toBe(true);
            if (Option.isNone(c)) return yield* Effect.die("Actual receiving release required");
            expect(c.value.status).toBe("accepted");
            if (disconnectedWorktree) expect(responseSequence).toBeUndefined();
            else expect(responseSequence).toBe(c.value.resultSequence);
            const received = yield* threads.getThreadProjection(threadId);
            const completed = received.runs[0]?.legacyPreparation?.steps;
            expect(completed?.at(-1)?.effect.kind).toBe("setup.completion");
            expect(completed?.at(-1)?.state).toBe("known_succeeded");
            for (const step of completed ?? []) {
              const outcomeId = step.outcomeCommandId;
              expect(outcomeId).toBeDefined();
              if (outcomeId === undefined)
                return yield* Effect.die("Exact settled owner outcome required");
              const receipt = yield* receipts.getByCommandId(outcomeId);
              const raw = Array.from(
                yield* store.readByCommandId({ commandId: outcomeId }).pipe(Stream.runCollect),
              );
              expect(Option.isSome(receipt)).toBe(true);
              expect(raw).toHaveLength(1);
              if (Option.isSome(receipt)) {
                expect(receipt.value.status).toBe("accepted");
                expect(raw[0]?.sequence).toBe(receipt.value.resultSequence);
                if (asyncWorktree && step.effect.kind === "setup.completion")
                  expect(receipt.value.resultSequence).toBeGreaterThan(c.value.resultSequence);
                else expect(receipt.value.resultSequence).toBeLessThan(c.value.resultSequence);
              }
            }
            expect(received.thread.deletedAt).toBeNull();
            expect(received.messages[0]?.id).toBe(messageId);
            if (disconnectedWorktree) {
              expect(received.runs[0]?.id).toBe(preparing.runs[0]?.id);
              expect(received.runs[0]?.status).toBe("starting");
              expect(received.thread.worktreePath).toBe(worktreePath);
              expect(received.thread.branch).toBe("owned/forwarding");
              expect(yield* fs.readFileString(`${worktreePath}/.git`)).toContain("gitdir:");
              const b = legacyBootstrapCreateCommandId(threadId, commandId);
              for (const suffix of [
                "failure-delete",
                "workspace-failure-delete",
                "guard-rejection-delete",
              ]) {
                expect(
                  Option.isNone(yield* receipts.getByCommandId(CommandId.make(`${b}:${suffix}`))),
                ).toBe(true);
              }
              const outbox = yield* WsEffectOutbox.EffectOutboxV2;
              const effects = yield* outbox.listByThreadId(threadId);
              expect(
                effects.filter((entry) =>
                  ["terminal.cleanup", "attachment.cleanup"].includes(entry.request.type),
                ),
              ).toHaveLength(0);
              expect(
                effects.filter(
                  (entry) =>
                    entry.commandId === commandId && entry.request.type === "provider-turn.start",
                ),
              ).toHaveLength(1);
            }
            expect(spawnCalls).toBe(1);
            expect(kills).toHaveLength(0);
            yield* WsFiber.interrupt(serving);
            return;
          }
          if (cancelledWorktree) {
            yield* Deferred.await(worktreeEntered).pipe(Effect.timeout("5 seconds"));
            const tracker = yield* WsWorktreeSetupTracker.WorktreeSetupTracker;
            const threads = yield* Threads.ThreadManagementService;
            const receipts = yield* Receipts.CommandReceiptStoreV2;
            const store = yield* EventStore.EventStoreV2;
            const outbox = yield* WsEffectOutbox.EffectOutboxV2;
            const runningCard = yield* tracker.get(threadId);
            expect(runningCard?.phase).toBe("running");
            expect(runningCard?.stages.find((stage) => stage.id === "checkout")?.status).toBe(
              "running",
            );
            expect(runningCard?.stages.find((stage) => stage.id === "agent")?.status).toBe(
              "pending",
            );
            const preparing = yield* threads.getThreadProjection(threadId);
            const intent = preparing.runs[0]?.legacyPreparation?.steps[0];
            expect(preparing.runs[0]?.status).toBe("preparing");
            expect(intent?.effect.kind).toBe("worktree.add");
            expect(intent?.state).toBe("intent");
            if (intent === undefined) return yield* Effect.die("Exact owner intent required");
            const intentReceipt = yield* receipts.getByCommandId(intent.intentCommandId);
            const intentEvents = Array.from(
              yield* store
                .readByCommandId({ commandId: intent.intentCommandId })
                .pipe(Stream.runCollect),
            );
            expect(Option.isSome(intentReceipt)).toBe(true);
            expect(intentEvents).toHaveLength(1);
            if (Option.isSome(intentReceipt)) {
              expect(intentReceipt.value.status).toBe("accepted");
              expect(intentEvents[0]?.sequence).toBe(intentReceipt.value.resultSequence);
            }
            expect(intent.evidence).toBeUndefined();
            expect(intent.outcomeCommandId).toBeUndefined();
            expect(Option.isNone(yield* receipts.getByCommandId(commandId))).toBe(true);
            const readCardFrame = () =>
              WsQueue.take(outgoing).pipe(
                Effect.flatMap((raw) =>
                  Schema.decodeUnknownEffect(
                    Schema.fromJsonString(
                      Schema.Struct({
                        _tag: Schema.String,
                        requestId: Schema.String,
                        values: Schema.optional(Schema.Array(WorktreeSetupStreamEvent)),
                        exit: Schema.optional(
                          Schema.Struct({
                            _tag: Schema.String,
                            value: Schema.optional(Schema.Unknown),
                          }),
                        ),
                      }),
                    ),
                  )(typeof raw === "string" ? raw : new TextDecoder().decode(raw)),
                ),
                Effect.timeout("5 seconds"),
              );
            yield* WsQueue.offer(
              incoming,
              yield* encodeSocketRpcRequest({
                _tag: "Request",
                id: "2",
                tag: WS_METHODS.subscribeWorktreeSetup,
                payload: { threadId },
                headers: [],
              }),
            );
            const initialCard = yield* readCardFrame();
            expect(initialCard._tag).toBe("Chunk");
            expect(initialCard.requestId).toBe("2");
            expect(initialCard.values?.[0]).toEqual(runningCard);
            yield* WsQueue.offer(
              incoming,
              yield* encodeSocketRpcRequest({ _tag: "Ack", requestId: "2" }),
            );
            yield* WsQueue.offer(
              incoming,
              yield* encodeSocketRpcRequest({
                _tag: "Request",
                id: "3",
                tag: WS_METHODS.worktreeSetupCancel,
                payload: { threadId },
                headers: [],
              }),
            );
            let cancelAccepted = false;
            let cancelledWireCard: typeof WorktreeSetupStreamEvent.Type | undefined;
            for (
              let frameCount = 0;
              frameCount < 8 && (!cancelAccepted || cancelledWireCard === undefined);
              frameCount++
            ) {
              const frame = yield* readCardFrame();
              if (frame.requestId === "3") {
                expect(frame.exit).toEqual({ _tag: "Success", value: { cancelled: true } });
                cancelAccepted = true;
              }
              if (frame.requestId === "2" && frame.values !== undefined) {
                cancelledWireCard = frame.values.find((card) => card?.phase === "cancelled");
                yield* WsQueue.offer(
                  incoming,
                  yield* encodeSocketRpcRequest({ _tag: "Ack", requestId: "2" }),
                );
              }
            }
            expect(cancelAccepted).toBe(true);
            const cancelledCard = yield* tracker.get(threadId);
            expect(cancelledCard?.phase).toBe("cancelled");
            expect(cancelledWireCard).toEqual(cancelledCard);
            expect(cancelledCard?.sequence).toBeGreaterThan(runningCard!.sequence);
            expect(cancelledCard?.stages.find((stage) => stage.id === "checkout")?.status).toBe(
              "skipped",
            );
            expect(cancelledCard?.stages.find((stage) => stage.id === "agent")?.status).toBe(
              "pending",
            );
            const received = yield* threads.getThreadProjection(threadId);
            expect(received.thread.deletedAt).toBeNull();
            expect(received.thread.modelSelection).toEqual(modelSelection);
            expect(received.messages[0]?.id).toBe(messageId);
            expect(received.runs[0]?.status).toBe("failed");
            expect(received.runs[0]?.legacyPreparation?.steps[0]).toEqual(intent);
            expect(Option.isNone(yield* receipts.getByCommandId(commandId))).toBe(true);
            const bId = legacyBootstrapCreateCommandId(threadId, commandId);
            expect(
              Array.from(yield* store.readByCommandId({ commandId }).pipe(Stream.runCollect)),
            ).toHaveLength(0);
            const failedId = CommandId.make(`${bId}:fail`);
            const failed = yield* receipts.getByCommandId(failedId);
            expect(Option.isSome(failed)).toBe(true);
            if (Option.isSome(failed)) expect(failed.value.status).toBe("accepted");
            const rawFailure = Array.from(
              yield* store.readByCommandId({ commandId: failedId }).pipe(Stream.runCollect),
            );
            expect(
              rawFailure.some(
                (entry) =>
                  entry.event.type === "run.updated" && entry.event.payload.status === "failed",
              ),
            ).toBe(true);
            expect(
              Option.isNone(
                yield* receipts.getByCommandId(CommandId.make(`${bId}:failure-delete`)),
              ),
            ).toBe(true);
            const observation = yield* makeCommandObservationQuery();
            const observed = yield* observation.observe({ threadId, commandId, messageId });
            expect(observed.commandStatus).toBe("not_found");
            expect(observed.acceptedSequence).toBeNull();
            expect(observed.turn).toBeNull();
            expect(
              (yield* outbox.listByThreadId(threadId)).filter((entry) =>
                ["provider-turn.start", "terminal.cleanup", "attachment.cleanup"].includes(
                  entry.request.type,
                ),
              ),
            ).toHaveLength(0);
            expect(yield* fs.exists(worktreePath)).toBe(
              scenario === "actual_worktree_cancel_partial",
            );
            if (scenario === "actual_worktree_cancel_partial")
              expect(yield* fs.readFileString(`${worktreePath}/partial`)).toBe(
                "unresolved owner bytes",
              );
            expect(spawnCalls).toBe(0);
            expect(written).toBe("");
            expect(kills).toHaveLength(0);
            yield* WsFiber.interrupt(serving);
            return;
          }
          if (startedSetup) {
            yield* Deferred.await(setupWritten).pipe(Effect.timeout("5 seconds"));
            const sink = yield* EventSink.EventSinkV2;
            yield* sink.stream({ threadId, eventType: "run.updated" }).pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "run.updated" &&
                  stored.event.payload.legacyPreparation?.steps.some(
                    (step) => step.effect.kind === "setup.write" && step.state === "known_started",
                  ) === true,
              ),
              Stream.runHead,
              Effect.timeout("5 seconds"),
            );
            const receipts = yield* Receipts.CommandReceiptStoreV2;
            const threads = yield* Threads.ThreadManagementService;
            expect(written).toContain("synthetic-no-execution");
            if (scenario !== "actual_async") {
              const preparing = yield* threads.getThreadProjection(threadId);
              expect(preparing.runs[0]?.status).toBe("preparing");
              expect(Option.isNone(yield* receipts.getByCommandId(commandId))).toBe(true);
              expect(
                preparing.runs[0]?.legacyPreparation?.steps.map((step) => [
                  step.effect.kind,
                  step.state,
                ]),
              ).toEqual([
                ["setup.open", "known_succeeded"],
                ["setup.write", "known_started"],
              ]);
              if (scenario === "actual_disconnect") {
                yield* WsFiber.interrupt(serving);
                expect(Option.isNone(yield* receipts.getByCommandId(commandId))).toBe(true);
              }
              yield* completeSetup;
            }
            if (scenario === "actual_disconnect") {
              const sink = yield* EventSink.EventSinkV2;
              yield* sink.stream({ threadId, eventType: "run.updated" }).pipe(
                Stream.filter((stored) => stored.commandId === commandId),
                Stream.runHead,
                Effect.timeout("5 seconds"),
              );
              const c = yield* receipts.getByCommandId(commandId);
              expect(Option.isSome(c)).toBe(true);
              if (Option.isSome(c)) expect(c.value.status).toBe("accepted");
              const received = yield* threads.getThreadProjection(threadId);
              expect(received.runs[0]?.status).toBe("starting");
              expect(
                received.runs[0]?.legacyPreparation?.steps.map((step) => [
                  step.effect.kind,
                  step.state,
                ]),
              ).toEqual([
                ["setup.open", "known_succeeded"],
                ["setup.write", "known_started"],
                ["setup.completion", "known_succeeded"],
              ]);
              expect(received.thread.deletedAt).toBeNull();
              expect(kills).toHaveLength(0);
              return;
            }
          }
          const raw = yield* Effect.race(
            WsQueue.take(outgoing),
            WsFiber.join(serving).pipe(Effect.andThen(Effect.die("Socket ended before response"))),
          ).pipe(Effect.timeout("5 seconds"));
          const frame = yield* decodeSocketRpcResponse(
            typeof raw === "string" ? raw : new TextDecoder().decode(raw),
          );
          expect(frame._tag).toBe("Exit");
          expect(frame.requestId).toBe("1");
          if (actualReceiving) {
            const receipts = yield* Receipts.CommandReceiptStoreV2;
            const threads = yield* Threads.ThreadManagementService;
            const store = yield* EventStore.EventStoreV2;
            const outbox = yield* WsEffectOutbox.EffectOutboxV2;
            const c = yield* receipts.getByCommandId(commandId);
            if (
              scenario === "actual_input_intent_lost" ||
              scenario === "actual_input_outcome_lost" ||
              scenario === "actual_entered_error"
            ) {
              expect(frame.exit._tag).toBe("Failure");
              expect(Option.isNone(c)).toBe(true);
              const received = yield* threads.getThreadProjection(threadId);
              expect(received.thread.deletedAt).toBeNull();
              const step = received.runs[0]?.legacyPreparation?.steps.find(
                (entry) => entry.effect.kind === "setup.open",
              );
              expect(step?.state).toBe(
                scenario === "actual_input_outcome_lost" ? "known_no_effect_failure" : "intent",
              );
              expect(step?.evidence).toEqual(
                scenario === "actual_input_outcome_lost"
                  ? {
                      type: "never_invoked",
                      owner: "setup",
                      reason: "input_validation_failed",
                    }
                  : undefined,
              );
              const bId = legacyBootstrapCreateCommandId(threadId, commandId);
              expect(
                Option.isNone(
                  yield* receipts.getByCommandId(CommandId.make(`${bId}:workspace-failure-delete`)),
                ),
              ).toBe(true);
              expect(
                (yield* outbox.listByThreadId(threadId)).filter((entry) =>
                  ["provider-turn.start", "terminal.cleanup", "attachment.cleanup"].includes(
                    entry.request.type,
                  ),
                ),
              ).toHaveLength(0);
              expect(spawnCalls).toBe(scenario === "actual_entered_error" ? 1 : 0);
              expect(written).toBe("");
              expect(kills).toHaveLength(0);
              return;
            }
            expect(Option.isSome(c)).toBe(true);
            if (Option.isNone(c)) return yield* Effect.die("Actual C receipt required");
            expect(c.value.commandType).toBe("prepared-run.release");
            const observation = yield* makeCommandObservationQuery();
            const observed = yield* observation.observe({ threadId, commandId, messageId });
            expect(observed.commandStatus).toBe(c.value.status);
            expect(observed.acceptedSequence).toBe(
              c.value.status === "accepted" ? c.value.resultSequence : null,
            );
            if (scenario === "actual_guard_rejected") expect(observed.turn).toBeNull();
            else {
              expect(observed.correlation).toBe("pending");
              expect(observed.turn?.turnId).toBeNull();
              expect(observed.turn?.state).toBe("pending");
            }
            const bId = legacyBootstrapCreateCommandId(threadId, commandId);
            const b = yield* receipts.getByCommandId(bId);
            const birth = yield* receipts.getByCommandId(CommandId.make(`${bId}:initial-message`));
            expect(Option.isSome(b)).toBe(true);
            expect(Option.isSome(birth)).toBe(true);
            if (Option.isSome(birth))
              expect(birth.value.resultSequence).toBeLessThanOrEqual(c.value.resultSequence);
            const received = yield* threads.getThreadProjection(threadId);
            expect(received.thread.projectId).toBe(projectId);
            expect(received.thread.modelSelection).toEqual(modelSelection);
            expect(received.messages[0]?.id).toBe(messageId);
            expect(received.messages[0]?.text).toBe("Forward only");
            expect(received.runs[0]?.legacyBootstrap?.releaseCommandId).toBe(commandId);
            if (lexicalSetup)
              expect(received.runs[0]?.legacyPreparation?.setup.status).toBe("resolved");
            else
              expect(received.runs[0]?.legacyPreparation?.setup.status).toBe(
                startedSetup ? "resolved" : "no_script",
              );
            expect(order).toEqual([]);
            expect(forwarded).toHaveLength(0);
            const dId = CommandId.make(`${bId}:guard-rejection-delete`);
            const d = yield* receipts.getByCommandId(dId);
            const effects = yield* outbox.listByThreadId(threadId);
            if (scenario !== "actual_guard_rejected") {
              expect(frame.exit).toMatchObject({
                _tag: "Success",
                value: { sequence: c.value.resultSequence },
              });
              expect(c.value.status).toBe("accepted");
              expect(received.thread.deletedAt).toBeNull();
              if (scenario === "actual_input_refused") {
                expect(spawnCalls).toBe(0);
                expect(written).toBe("");
                lexicalRun = received.runs[0]!;
                const preparation = lexicalRun.legacyPreparation;
                expect(preparation?.steps).toHaveLength(1);
                const step = preparation!.steps[0]!;
                expect(step.state).toBe("known_no_effect_failure");
                expect(step.evidence).toEqual({
                  type: "never_invoked",
                  owner: "setup",
                  reason: "input_validation_failed",
                });
                for (const id of [step.intentCommandId, step.outcomeCommandId!]) {
                  const receipt = yield* receipts.getByCommandId(id);
                  expect(Option.isSome(receipt)).toBe(true);
                  const recorded = Array.from(
                    yield* store.readByCommandId({ commandId: id }).pipe(Stream.runCollect),
                  );
                  expect(recorded).toHaveLength(1);
                  if (Option.isSome(receipt)) {
                    expect(receipt.value.status).toBe("accepted");
                    expect(recorded[0]?.sequence).toBe(receipt.value.resultSequence);
                    expect(receipt.value.resultSequence).toBeLessThan(c.value.resultSequence);
                  }
                }
              }
              expect(Option.isNone(d)).toBe(true);
              expect(received.runs[0]?.status).toBe("starting");
              expect(
                effects.filter(
                  (entry) =>
                    entry.commandId === commandId && entry.request.type === "provider-turn.start",
                ),
              ).toHaveLength(1);
              if (scenario === "actual_async") {
                expect(
                  received.runs[0]?.legacyPreparation?.steps.map((step) => [
                    step.effect.kind,
                    step.state,
                  ]),
                ).toEqual([
                  ["setup.open", "known_succeeded"],
                  ["setup.write", "known_started"],
                ]);
                yield* completeSetup;
                const sink = yield* EventSink.EventSinkV2;
                yield* sink
                  .stream({
                    threadId,
                    eventType: "run.updated",
                    afterSequence: c.value.resultSequence,
                  })
                  .pipe(
                    Stream.filter(
                      (stored) =>
                        stored.event.type === "run.updated" &&
                        stored.event.payload.legacyPreparation?.steps.some(
                          (step) =>
                            step.effect.kind === "setup.completion" &&
                            step.state === "known_succeeded",
                        ) === true,
                    ),
                    Stream.runHead,
                    Effect.timeout("5 seconds"),
                  );
                const completed = yield* threads.getThreadProjection(threadId);
                expect(completed.runs[0]?.legacyPreparation?.steps.at(-1)?.state).toBe(
                  "known_succeeded",
                );
                expect(yield* receipts.getByCommandId(commandId)).toEqual(c);
              }
              expect(kills).toHaveLength(0);
            } else {
              expect(frame.exit._tag).toBe("Failure");
              const failures = yield* decodeLegacyRpcFailureCause(frame.exit.cause);
              expect(failures[0]?.error.bootstrapThreadDisposition).toBe("deleted");
              expect(c.value.status).toBe("rejected");
              expect(received.thread.deletedAt).not.toBeNull();
              expect(received.runs[0]?.legacyReleaseDecision?.deletion?.type).toBe("no_control");
              expect(Option.isSome(d)).toBe(true);
              if (Option.isSome(d)) expect(d.value.status).toBe("accepted");
              const deletionEvents = Array.from(
                yield* store.readByCommandId({ commandId: dId }).pipe(Stream.runCollect),
              );
              expect(deletionEvents.map((entry) => entry.event.type)).toEqual([
                "run.updated",
                "thread.deleted",
              ]);
              expect(
                effects.filter(
                  (entry) => entry.commandId === dId && entry.request.type === "terminal.cleanup",
                ),
              ).toHaveLength(0);
              expect(
                effects.filter((entry) => entry.request.type === "provider-turn.start"),
              ).toHaveLength(0);
            }
            expect(
              effects.filter((entry) => entry.request.type === "attachment.cleanup"),
            ).toHaveLength(0);
            expect(typeof raw === "string" ? raw : new TextDecoder().decode(raw)).not.toContain(
              "legacyBootstrap",
            );
            expect(typeof raw === "string" ? raw : new TextDecoder().decode(raw)).not.toContain(
              "legacyPreparation",
            );
            expect(typeof raw === "string" ? raw : new TextDecoder().decode(raw)).not.toContain(
              "legacyReleaseDecision",
            );
          } else if (scenario === "operate") {
            // The mocked sequence is forwarding evidence only; live strict guards remain independently tested rejections.
            expect(frame.exit).toMatchObject({ _tag: "Success", value: { sequence: 3 } });
            expect(order).toEqual(["preflight", "launch"]);
            expect(preflight).toHaveLength(1);
            expect(forwarded).toHaveLength(1);
            const launch = forwarded[0]!;
            expect(launch.preparationReleaseCommandId).toBe(commandId);
            expect(launch.threadId).toBe(threadId);
            expect(launch.projectId).toBe(projectId);
            expect(launch.initialMessage?.messageId).toBe(messageId);
            expect(launch.modelSelection).toEqual(modelSelection);
            expect(launch.legacyBootstrap?.dispatchGuard).toEqual(guard);
            expect(launch.commandId).not.toBe(commandId);
            expect(launch.legacyBootstrap?.birthCommandId).not.toBe(commandId);
          } else if (scenario === "deleted_error" || scenario === "survivor_error") {
            expect(frame.exit._tag).toBe("Failure");
            const failures = yield* decodeLegacyRpcFailureCause(frame.exit.cause);
            expect(failures).toHaveLength(1);
            expect(failures[0]!.error._tag).toBe("OrchestrationDispatchCommandError");
            expect(failures[0]!.error.message).toContain("worktree exploded");
            expect(failures[0]!.error.bootstrapThreadDisposition).toBe(
              scenario === "deleted_error" ? "deleted" : undefined,
            );
            expect(order).toEqual(["preflight", "launch"]);
            expect(forwarded).toHaveLength(1);
          } else {
            expect(frame.exit._tag).toBe("Failure");
            expect(order).toEqual([]);
            expect(preflight).toHaveLength(0);
            expect(forwarded).toHaveLength(0);
          }
        }).pipe(
          Effect.ensuring(completeSetup),
          Effect.ensuring(releaseWorktree),
          Effect.provide(dependencies),
          Effect.scoped,
          Effect.timeout("10 seconds"),
        );
        if (scenario === "actual_input_refused") {
          const expected = lexicalRun;
          if (expected === undefined)
            return yield* Effect.die("Missing authentic refused setup run");
          const stores = Layer.mergeAll(
            EventStore.layer,
            ProjectionStore.layer,
            Receipts.layer,
          ).pipe(Layer.provideMerge(receivingDatabase));
          const reopened = Layer.mergeAll(
            stores,
            EventSink.layer.pipe(Layer.provide(stores)),
            ProjectionMaintenance.layer.pipe(Layer.provide(stores)),
          );
          yield* Effect.gen(function* () {
            const projections = yield* ProjectionStore.ProjectionStoreV2;
            const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
            const receipts = yield* Receipts.CommandReceiptStoreV2;
            const sink = yield* EventSink.EventSinkV2;
            const persisted = yield* projections.getThreadProjection(threadId);
            expect(persisted.runs[0]?.legacyPreparation).toEqual(expected.legacyPreparation);
            expect((yield* maintenance.rebuild).valid).toBe(true);
            const replayed = yield* projections.getThreadProjection(threadId);
            expect(replayed.runs[0]?.legacyPreparation).toEqual(expected.legacyPreparation);
            expect(legacyPreparationReleaseBlocker({ run: replayed.runs[0]! })).toBeUndefined();
            const { legacyPreparation: _preparation, ...ordinary } = replayed.runs[0]!;
            yield* sink.write({
              events: [
                {
                  id: EventId.make("wire-forward:ordinary-stale"),
                  threadId,
                  runId: expected.id,
                  type: "run.updated",
                  occurredAt: expected.requestedAt,
                  payload: ordinary,
                },
              ],
            });
            const stale = yield* projections.getThreadProjection(threadId);
            expect(stale.runs[0]?.legacyPreparation).toEqual(expected.legacyPreparation);
            const preparation = expected.legacyPreparation!;
            for (const changed of [
              { ...preparation, generation: "different" },
              { ...preparation, birthEventId: EventId.make("different") },
              {
                ...preparation,
                policy: { ...preparation.policy, releaseCommandId: CommandId.make("different") },
              },
              { ...preparation, steps: [] },
              {
                ...preparation,
                steps: preparation.steps.map((step) => ({ ...step, inputHash: "different" })),
              },
              {
                ...preparation,
                steps: preparation.steps.map((step) => ({
                  ...step,
                  state: "unknown" as const,
                  evidence: {
                    type: "unknown" as const,
                    reason: "process_result_unavailable" as const,
                  },
                })),
              },
            ])
              expect(
                legacyNeverInvokedSetupOpen({ ...expected, legacyPreparation: changed }),
              ).toBeUndefined();
            const c = yield* receipts.getByCommandId(commandId);
            expect(Option.isSome(c)).toBe(true);
            if (Option.isSome(c)) expect(c.value.status).toBe("accepted");
            const wire = projectThreadProjectionForWire(stale);
            expect("legacyPreparation" in wire.runs[0]!).toBe(false);
          }).pipe(Effect.provide(Layer.fresh(reopened)), Effect.scoped);
        }
      }),
    );
  },
);
