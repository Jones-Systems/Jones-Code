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
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ApplicationEvents from "../persistence/Layers/OrchestrationEventStore.ts";
import * as ProjectEnrichment from "../project/ProjectEnrichmentService.ts";
import * as Receipts from "./CommandReceiptStore.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as Threads from "./ThreadManagementService.ts";
import { OrchestratorProjectionError } from "./Orchestrator.ts";
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

effectIt.layer(NodeServices.layer, { excludeTestServices: true })(
  "Authenticated legacy WS serialized forwarding",
  (it) => {
    it.effect.each([
      "operate",
      "read_only",
      "private_field",
      "deleted_error",
      "survivor_error",
    ] as const)("preserves closed dispatch and authorization for %s", (scenario) =>
      Effect.gen(function* () {
        const fs = yield* WsFileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "legacy-ws-forwarding-" });
        const config = WsServerConfig.layerTest(cwd, `${cwd}/state`);
        const auth = WsEnvironmentAuth.layer.pipe(
          Layer.provideMerge(SqlitePersistenceMemory),
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
        const forwarded: WsThreadLaunchService.ThreadLaunchInput[] = [];
        const preflight: Parameters<
          WsThreadLaunchService.ThreadLaunchService["Service"]["preflightLegacyBootstrap"]
        >[0][] = [];
        const order: string[] = [];
        const rpcOwners = Layer.mergeAll(
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

        const dependencies = Layer.mergeAll(auth, rpcOwners).pipe(
          Layer.provideMerge(config),
          Layer.provideMerge(NodeServices.layer),
        );
        yield* Effect.gen(function* () {
          const owner = yield* WsEnvironmentAuth.EnvironmentAuth;
          const issued = yield* owner.issueSession({
            scopes: [scenario === "read_only" ? "orchestration:read" : "orchestration:operate"],
          });
          const input = {
            type: "thread.turn.start",
            commandId,
            threadId,
            createdAt: timestamp,
            runtimeMode: "full-access",
            interactionMode: "default",
            modelSelection,
            message: { messageId, role: "user", text: "Forward only", attachments: [] },
            dispatchGuard: guard,
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
              prepareWorktree: {
                projectCwd: cwd,
                baseBranch: "main",
                branch: "owned/forwarding",
                startFromOrigin: false,
                requireWorktree: true,
              },
              runSetupScript: false,
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
          ): WsHttpServerRequest.HttpServerRequest =>
            new Proxy(request, {
              get(target, property) {
                if (property === "upgrade") return Effect.succeed(socket);
                if (property === "modify")
                  return (
                    options: Parameters<WsHttpServerRequest.HttpServerRequest["modify"]>[0],
                  ) => withUpgrade(target.modify(options));
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
          const handler = yield* HttpRouter.toHttpEffect(
            websocketRpcRouteLayer.pipe(Layer.provide(dependencies)),
          );
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
          const raw = yield* Effect.race(
            WsQueue.take(outgoing),
            WsFiber.join(serving).pipe(Effect.andThen(Effect.die("Socket ended before response"))),
          ).pipe(Effect.timeout("5 seconds"));
          const frame = yield* decodeSocketRpcResponse(
            typeof raw === "string" ? raw : new TextDecoder().decode(raw),
          );
          expect(frame._tag).toBe("Exit");
          expect(frame.requestId).toBe("1");
          if (scenario === "operate") {
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
        }).pipe(Effect.provide(dependencies), Effect.timeout("10 seconds"));
      }),
    );
  },
);
