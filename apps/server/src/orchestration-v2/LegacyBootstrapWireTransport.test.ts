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
