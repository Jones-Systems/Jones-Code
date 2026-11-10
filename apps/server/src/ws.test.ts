import * as QueueCompatibility from "./orchestration-v2/QueueCompatibility.ts";
import * as SettlementOrigins from "./jones/settlementOrigins/SettlementOrigins.ts";
import * as TokenAccountingService from "./tokenAccounting/TokenAccountingService.ts";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  AuthSessionId,
  AuthSourceControlWriteScope,
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnItemId,
  ProviderDriverKind,
  ProviderInstanceId,
  SourceControlRepositoryError,
  WS_METHODS,
  WsRpcGroup,
  WsServerRefreshProvidersRpc,
  type CompanionDown,
  type PreviewOpenInput,
  type PreviewSessionSnapshot,
  WsSubscribeServerConfigRpc,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2ThreadShell,
  type OrchestrationProjectShell,
  type ServerConfig,
  type ServerConfigStreamEvent,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { Headers } from "effect/http";
import { Rpc, RpcGroup, RpcMessage, RpcServer } from "effect/rpc";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import { vi } from "vite-plus/test";

import * as ShellMaterialization from "./jones/orchestration/shellMaterialization.ts";
import * as AssetSigner from "./assets/AssetSigner.ts";
import * as PreviewAutomationBroker from "./mcp/PreviewAutomationBroker.ts";
import * as ServerBrowser from "./preview/ServerBrowser.ts";
import * as AcpRegistryRuntimeCoordinator from "@t3tools/provider-acp-registry/server/AcpRegistryRuntimeCoordinator";
import * as AcpRegistrySupport from "@t3tools/provider-acp-registry/server/AcpRegistrySupport";
import * as AgentSessionImporter from "./project/AgentSessionImporter.ts";
import * as AgentSessionScanner from "./project/AgentSessionScanner.ts";
import * as AnalyticsService from "./telemetry/AnalyticsService.ts";
import * as AntigravityInstallation from "./provider/AntigravityInstallation.ts";
import * as ProviderInstallation from "./provider/providerInstallation.ts";
import * as BackgroundPolicy from "./background/BackgroundPolicy.ts";
import * as CheckpointDiffQuery from "./checkpointing/CheckpointDiffQuery.ts";
import * as NativeChildWorkQuery from "./orchestration-v2/NativeChildWorkQuery.ts";
import * as CommandOutputHub from "./orchestration-v2/CommandOutputHub.ts";
import * as CodexInstallation from "./provider/CodexInstallation.ts";
import * as DeviceService from "./device/DeviceService.ts";
import * as DocumentText from "./provider/DocumentText.ts";
import * as DirectEndpoints from "./environment/DirectEndpoints.ts";
import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import * as EnvironmentTheme from "./environmentTheme.ts";
import * as GitWorkflowService from "./git/GitWorkflowService.ts";
import * as HostResources from "./resourceTelemetry/HostResources.ts";
import * as ManagedProjectFolders from "./project/ManagedProjectFolders.ts";
import * as ScratchThreadPromotion from "./project/ScratchThreadPromotion.ts";
import * as McpAppRequests from "./mcpApps/McpAppRequests.ts";
import * as ModelManifest from "./provider/ModelManifest.ts";
import * as Orchestrator from "./orchestration-v2/Orchestrator.ts";
import * as OtlpEndpointCheck from "./observability/OtlpEndpointCheck.ts";
import * as PairingGrantStore from "./auth/PairingGrantStore.ts";
import * as RpcAuthorization from "./auth/RpcAuthorization.ts";
import * as PeerLinkRequests from "./peer/PeerLinkRequests.ts";
import * as PeerLinks from "./peer/PeerLinks.ts";
import * as PortScanner from "./preview/PortScanner.ts";
import * as PreviewManager from "./preview/Manager.ts";
import * as DesktopBrowserChannel from "./preview/DesktopBrowserChannel.ts";
import * as JonesHttp from "./jones/http/registration.ts";
import * as CompanionHostRegistry from "./jones/previewCompanion/CompanionHostRegistry.ts";
import * as RenderHostSelection from "./jones/previewCompanion/RenderHostSelection.ts";
import * as RenderPlacement from "./jones/previewCompanion/RenderPlacement.ts";
import * as ProcessDiagnostics from "./diagnostics/ProcessDiagnostics.ts";
import * as ProcessResourceMonitor from "./diagnostics/ProcessResourceMonitor.ts";
import * as ProjectCloneTracker from "./project/ProjectCloneTracker.ts";
import * as ProjectSetupScriptRunner from "./project/ProjectSetupScriptRunner.ts";
import * as ProviderAuthService from "./provider/ProviderAuthService.ts";
import * as ProviderInstanceRegistry from "./provider/ProviderInstanceRegistry.ts";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as ProviderMaintenanceRunner from "./provider/providerMaintenanceRunner.ts";
import * as ProviderRegistry from "./provider/ProviderRegistry.ts";
import * as ProviderSessionManager from "./orchestration-v2/ProviderSessionManager.ts";
import * as PullRequestReviewGuideService from "./pullRequest/PullRequestReviewGuideService.ts";
import * as PullRequestService from "./pullRequest/PullRequestService.ts";
import * as PullRequestSyncReactor from "./orchestration-v2/PullRequestSyncReactor.ts";
import * as RelayClient from "@t3tools/shared/relayClient";
import * as RemoteOpenTargets from "./environment/RemoteOpenTargets.ts";
import * as RepositoryIdentityResolver from "./project/RepositoryIdentityResolver.ts";
import * as ResourceTelemetry from "./resourceTelemetry/ResourceTelemetry.ts";
import * as ReviewService from "./review/ReviewService.ts";
import * as ScheduledTasks from "./scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "./secrets/SecretRequests.ts";
import * as GuestShareService from "./guest/GuestShareService.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import * as ServerLifecycleEvents from "./serverLifecycleEvents.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";
import * as ServerSelfUpdate from "./cloud/selfUpdate.ts";
import * as NpmRelease from "./cloud/npmRelease.ts";
import * as SessionStore from "./auth/SessionStore.ts";
import * as SourceControlDiscovery from "./sourceControl/SourceControlDiscovery.ts";
import * as SourceControlRepositoryService from "./sourceControl/SourceControlRepositoryService.ts";
import * as SourceControlSignIn from "./sourceControl/SourceControlSignIn.ts";
import * as StorageCleanup from "./storageCleanup.ts";
import * as DatabaseStorage from "./persistence/DatabaseStorage.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import * as ThreadLaunchService from "./orchestration-v2/ThreadLaunchService.ts";
import * as ThreadSearch from "./orchestration-v2/ThreadSearch.ts";
import * as UsageLimitSources from "./usage/UsageLimitSources.ts";
import * as UsageService from "./usage/UsageService.ts";
import * as VcsProvisioningService from "./vcs/VcsProvisioningService.ts";
import * as VcsStatusBroadcaster from "./vcs/VcsStatusBroadcaster.ts";
import * as WorkspaceEntries from "./workspace/WorkspaceEntries.ts";
import * as WorkspaceFileSystem from "./workspace/WorkspaceFileSystem.ts";
import * as WorktreeSetupTracker from "./project/WorktreeSetupTracker.ts";
import * as ProjectFaviconResolver from "./project/ProjectFaviconResolver.ts";
import * as TraceDiagnostics from "./diagnostics/TraceDiagnostics.ts";
import * as TriageService from "./triage/TriageService.ts";
import * as TextGenerators from "./textGeneration/TextGenerators.ts";
import * as WorkspacePaths from "./workspace/WorkspacePaths.ts";
import * as ServerSecretStore from "./auth/ServerSecretStore.ts";
import * as ServerConfigModule from "./config.ts";
import * as Keybindings from "./keybindings.ts";
import { USAGE_LIMITS_COMMAND } from "@t3tools/shared/usageLimits";
import * as ServerSettings from "./serverSettings.ts";
import * as ManagedPolicy from "./policy/ManagedPolicy.ts";

import * as ThreadManagement from "./orchestration-v2/ThreadManagementService.ts";
import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import * as OrchestrationEventStore from "./persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "./persistence/Sqlite.ts";
import * as ExternalLauncher from "./process/externalLauncher.ts";
import * as ProjectEnrichment from "./project/ProjectEnrichmentService.ts";
import * as ProjectService from "./project/ProjectService.ts";
import {
  layerWsRpc,
  resolveOpenDiscoveryForConfig,
  subscribeOrchestrationV2Shell,
  subscribeOrchestrationV2Thread,
  WS_RPC_SERVER_OPTIONS,
  withLateConfigDiscovery,
} from "./ws.ts";

const makeConfigDiscoveryRpc = Effect.fn(function* (discoveries: {
  readonly editors: ReturnType<
    ExternalLauncher.ExternalLauncher["Service"]["resolveAvailableEditors"]
  >;
  readonly targets: ReturnType<RemoteOpenTargets.RemoteOpenTargets["Service"]["resolveTargets"]>;
  readonly endpoints: ReturnType<DirectEndpoints.DirectEndpoints["Service"]["resolve"]>;
  readonly available: ReturnType<DirectEndpoints.DirectEndpoints["Service"]["resolveAvailable"]>;
}) {
  const responses = yield* Queue.unbounded<ServerConfigStreamEvent>();
  const services = Layer.mergeAll(
    layerUnusedConfigRpcServices,
    Layer.mock(ExternalLauncher.ExternalLauncher)({
      resolveAvailableEditors: () => discoveries.editors,
      resolveFileManagerRevealKind: () => Effect.succeed("file-explorer"),
    }),
    Layer.mock(RemoteOpenTargets.RemoteOpenTargets)({ resolveTargets: () => discoveries.targets }),
    Layer.mock(DirectEndpoints.DirectEndpoints)({
      resolve: () => discoveries.endpoints,
      resolveAvailable: () => discoveries.available,
    }),
    Layer.mock(ProviderRegistry.ProviderRegistry)({
      getProviders: Effect.succeed([]),
      streamChanges: Stream.never,
    }),
    Layer.mock(UsageLimitSources.UsageLimitSources)({ streamChanges: Stream.make([]) }),
    Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
      scratchRoot: Effect.succeedNone,
      namedProjectsRoot: process.cwd(),
    }),
    Layer.mock(ServerEnvironment.ServerEnvironment)({
      getDescriptor: Effect.succeed({
        environmentId: EnvironmentId.make("discovery-test"),
        label: "Discovery test",
        platform: { os: "linux", arch: "x64" },
        serverVersion: "0.0.0",
        capabilities: { repositoryIdentity: false },
      }),
    }),
    Layer.mock(EnvironmentAuth.EnvironmentAuth)({
      getDescriptor: () =>
        Effect.succeed({
          policy: "loopback-browser",
          bootstrapMethods: [],
          sessionMethods: ["browser-session-cookie"],
          sessionCookieName: "discovery-test-session",
        }),
    }),
  );
  const server = yield* RpcServer.makeNoSerialization(RpcGroup.make(WsSubscribeServerConfigRpc), {
    ...WS_RPC_SERVER_OPTIONS,
    onFromServer: (response) =>
      response._tag === "Chunk"
        ? Effect.forEach(response.values, (event) => Queue.offer(responses, event), {
            discard: true,
          })
        : Effect.void,
  }).pipe(
    Effect.provide(
      layerWsRpc(
        {
          sessionId: AuthSessionId.make("discovery-test"),
          subject: "discovery-test",
          method: "browser-session-cookie",
          scopes: [],
        },
        {},
        yield* ServerBrowser.ServerBrowser,
        () => Effect.void,
        { secretReferences: true },
        () => Effect.succeed({}),
      ).pipe(Layer.provide(services)),
    ),
  );
  const requestId = RpcMessage.RequestId("discovery");
  yield* server.write(0, {
    _tag: "Request",
    id: requestId,
    tag: WS_METHODS.subscribeServerConfig,
    payload: {},
    headers: Headers.empty,
  });
  return {
    responses,
    acknowledge: server.write(0, { _tag: "Ack", requestId }),
    interrupt: server.write(0, { _tag: "Interrupt", requestId, interruptors: [] }),
  };
});

const layerConfigDiscoveryTest = Layer.unwrap(
  Effect.sync(() =>
    Layer.mergeAll(
      Layer.fresh(layerConfigSources),
      Layer.mock(PreviewAutomationBroker.PreviewAutomationBroker)({}),
      Layer.mock(ServerBrowser.ServerBrowser)({}),
    ),
  ),
);

const discoveryTargets = [{ kind: "mdns", host: "discovery.local", username: "test" }] as const;

const discoveryEndpoints = [{ kind: "lan", httpBaseUrl: "http://192.168.1.2:3773/" }] as const;

const discoveryNames = ["editors", "targets", "endpoints"] as const;

const snapshotConfig = (fields: Partial<ServerConfig>) =>
  ({ availableEditors: [], settings: {}, ...fields }) as unknown as ServerConfig;

const settingsUpdated = (settings: object): ServerConfigStreamEvent => ({
  version: 1,
  type: "settingsUpdated",
  payload: { settings: settings as ServerConfig["settings"] },
});

it.effect("resends late editors without rolling back updates already sent", () =>
  Effect.gen(function* () {
    const settingsSent = yield* Deferred.make<void>();
    const events = yield* withLateConfigDiscovery(
      snapshotConfig({ settings: { enableProviderUpdateChecks: true } as never }),
      Stream.make(settingsUpdated({ enableProviderUpdateChecks: false })),
      {
        resolveAvailableEditors: () => Effect.succeed(["file-manager"]),
        resolveFileManagerRevealKind: () =>
          Deferred.await(settingsSent).pipe(Effect.as("file-explorer" as const)),
      },
      { resolveTargets: () => Effect.succeed([]) },
      { resolve: () => Effect.succeed([]) },
    ).pipe(
      Stream.tap((event) =>
        event.type === "settingsUpdated" ? Deferred.succeed(settingsSent, undefined) : Effect.void,
      ),
      Stream.runCollect,
    );

    const [first, second] = Array.from(events);
    assert.equal(events.length, 2);
    assert.equal(first?.type, "settingsUpdated");
    assert.equal(second?.type, "snapshot");
    if (second?.type === "snapshot") {
      assert.deepEqual(second.config.availableEditors, ["file-manager"]);
      assert.equal(second.config.shellRevealInFileManagerKind, "file-explorer");
      assert.deepEqual(second.config.settings, { enableProviderUpdateChecks: false } as never);
    }
  }),
);

it.effect("resends a reveal kind and remote targets that outlast the snapshot timeout", () =>
  Effect.gen(function* () {
    const targets = [{ kind: "mdns" as const, host: "test.local" }];
    const finishDiscovery = yield* Deferred.make<typeof targets>();
    const collecting = yield* withLateConfigDiscovery(
      snapshotConfig({ availableEditors: ["file-manager"] }),
      Stream.empty,
      {
        resolveAvailableEditors: () => Effect.succeed(["file-manager"]),
        resolveFileManagerRevealKind: () => Effect.succeed("file-explorer"),
      },
      { resolveTargets: () => Deferred.await(finishDiscovery) },
      { resolve: () => Effect.succeed([]) },
    ).pipe(Stream.runCollect, Effect.forkScoped({ startImmediately: true }));
    yield* TestClock.adjust("6 seconds");
    yield* Deferred.succeed(finishDiscovery, targets);
    const events = yield* Fiber.join(collecting);

    const [late, remote] = Array.from(events);
    assert.equal(events.length, 2);
    assert.equal(late?.type, "snapshot");
    if (late?.type === "snapshot") {
      assert.equal(late.config.shellRevealInFileManagerKind, "file-explorer");
    }
    assert.equal(remote?.type, "snapshot");
    if (remote?.type === "snapshot") {
      assert.deepEqual(remote.config.remoteOpenTargets, targets);
      assert.equal(remote.config.shellRevealInFileManagerKind, "file-explorer");
    }
  }).pipe(Effect.scoped),
);

it.effect("runs editor, remote open target and direct endpoint discovery side by side", () =>
  Effect.gen(function* () {
    // Each discovery finishes only once the other two have started, so a
    // config that ran them one after another would never resolve.
    const editorsStarted = yield* Deferred.make<void>();
    const targetsStarted = yield* Deferred.make<void>();
    const endpointsStarted = yield* Deferred.make<void>();
    const startThenAwait = <A>(started: Deferred.Deferred<void>, value: A) =>
      Deferred.succeed(started, undefined).pipe(
        Effect.andThen(
          Effect.forEach([editorsStarted, targetsStarted, endpointsStarted], Deferred.await),
        ),
        Effect.as(value),
      );

    const discovery = yield* resolveOpenDiscoveryForConfig({
      editors: startThenAwait(editorsStarted, ["file-manager" as const]),
      fileManagerRevealKind: Effect.succeed("finder" as const),
      remoteOpenTargets: startThenAwait(targetsStarted, []),
      directEndpoints: startThenAwait(endpointsStarted, []),
    });

    assert.deepEqual(discovery, {
      availableEditors: ["file-manager"],
      shellRevealInFileManager: true,
      shellRevealInFileManagerKind: "finder",
      remoteOpenTargets: [],
      directEndpoints: [],
    });
  }),
);

const ReplayTestLayer = OrchestrationEventStore.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

it.live.each(["thread", "shell"] as const)(
  "%s resume skips unknown types, synchronizes, and receives the live tail",
  (scope) =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-01-05T00:00:00.000Z";
      const threadId = ThreadId.make("thread:replay");
      const projectId = ProjectId.make("project:replay");
      const project = {
        id: projectId,
        title: "Replay",
        workspaceRoot: "/tmp/replay",
        defaultModelSelection: null,
        scripts: [],
        createdAt: now,
        updatedAt: now,
      } satisfies OrchestrationProjectShell;
      const insert = (version: number, unknown = false) => sql<{ sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          ${`replay:${version}`}, ${scope === "thread" ? "thread" : "project"},
          ${scope === "thread" ? threadId : projectId}, ${version},
          ${unknown ? "future.event" : scope === "thread" ? "provider-session.detached" : "project.meta-updated"},
          ${now}, 'server', ${JSON.stringify(
            scope === "thread"
              ? { providerSessionId: "session:replay", detachedAt: now }
              : { projectId, updatedAt: now },
          )}, '{}', 2
        ) RETURNING sequence
      `;
      const [before] = yield* insert(0);
      yield* insert(1, true);
      const [after] = yield* insert(2);
      const [highWater] = yield* insert(3, true);
      const updates = yield* PubSub.unbounded();
      let liveSequence = 0;
      const consume = <A extends { readonly kind: string; readonly sequence?: number }, E, R>(
        stream: Stream.Stream<A, E, R>,
      ) =>
        stream.pipe(
          Stream.tap((item) =>
            Effect.gen(function* () {
              if (item.kind !== "synchronized") return;
              const [live] = yield* insert(4);
              liveSequence = live!.sequence;
              const events = yield* store
                .readApplicationEvents({
                  afterSequence: highWater!.sequence,
                  throughSequence: liveSequence,
                })
                .pipe(Stream.runCollect);
              yield* store.publishCommitted(events);
            }),
          ),
          Stream.takeUntil((item) => item.sequence === liveSequence),
          Stream.map((item) => ({ kind: item.kind as string, sequence: item.sequence })),
          Stream.runCollect,
        );
      const subscription = Effect.gen(function* () {
        return scope === "thread"
          ? yield* subscribeOrchestrationV2Thread({
              threadId,
              afterSequence: before!.sequence,
              requestCompletionMarker: true,
            }).pipe(Effect.flatMap(consume))
          : yield* subscribeOrchestrationV2Shell({
              afterSequence: before!.sequence,
              requestCompletionMarker: true,
            }).pipe(Effect.flatMap(consume));
      });
      const items = yield* subscription.pipe(
        Effect.provide(ShellMaterialization.layer),
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ensureLegacyTranscript: () => Effect.void,
          getShellSnapshot: () => Effect.die("shell resume must not load a full snapshot"),
          listActiveSubagentThreadIds: () => Effect.succeed([]),
          streamStoredEventsFrom: (
            input: Parameters<
              ThreadManagement.ThreadManagementService["Service"]["streamStoredEventsFrom"]
            >[0],
          ) => {
            assert.equal(input?.afterSequence, highWater!.sequence);
            return store
              .streamApplicationEvents({ afterSequence: input?.afterSequence ?? 0 })
              .pipe(
                Stream.filter((event): event is OrchestrationV2StoredEvent => "event" in event),
              );
          },
        } as unknown as ThreadManagement.ThreadManagementService["Service"]),
        Effect.provideService(ProjectStore.ProjectStoreV2, {
          listShells: () => Effect.succeed([]),
        } as unknown as ProjectStore.ProjectStoreV2["Service"]),
        Effect.provideService(ProjectService.ProjectService, {
          getShell: () => Effect.succeedSome(project),
        } as unknown as ProjectService.ProjectService["Service"]),
        Effect.provideService(ProjectEnrichment.ProjectEnrichmentService, {
          subscribeChanges: PubSub.subscribe(updates),
        } as ProjectEnrichment.ProjectEnrichmentService["Service"]),
      );
      assert.deepEqual(
        items.map((item) => item.kind),
        scope === "thread"
          ? ["event", "synchronized", "event"]
          : ["project.updated", "synchronized", "project.updated"],
      );
      assert.deepEqual(
        items.flatMap((item) => (item.sequence === undefined ? [] : [item.sequence])),
        [after!.sequence, liveSequence],
      );
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("coalesces streaming replay before synchronization and the live tail", () =>
  Effect.gen(function* () {
    const store = yield* OrchestrationEventStore.OrchestrationEventStore;
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:streaming-replay");
    const now = "2026-10-27T00:00:00.000Z";
    const insert = (version: number, text: string, streaming: boolean) => sql<{
      sequence: number;
    }>`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, actor_kind, payload_json, metadata_json, application_event_version
      ) VALUES (
        ${`streaming-replay:${version}`}, 'thread', ${threadId}, ${version}, 'message.updated',
        ${now}, 'server', ${JSON.stringify({
          createdBy: "agent",
          creationSource: "web",
          id: "message:streaming-replay",
          threadId,
          runId: null,
          nodeId: null,
          role: "assistant",
          text,
          attachments: [],
          streaming,
          createdAt: now,
          updatedAt: now,
        })}, '{}', 2
      ) RETURNING sequence
    `;
    yield* insert(0, "H", true);
    yield* insert(1, "He", true);
    const [latest] = yield* insert(2, "Hello", true);
    const [completed] = yield* insert(3, "Hello!", false);
    const items = yield* subscribeOrchestrationV2Thread({
      threadId,
      afterSequence: 0,
      requestCompletionMarker: true,
    }).pipe(
      Effect.flatMap(Stream.runCollect),
      Effect.provideService(ThreadManagement.ThreadManagementService, {
        ensureLegacyTranscript: () => Effect.void,
        streamStoredEventsFrom: () =>
          Stream.unwrap(
            insert(4, "Next", false).pipe(
              Effect.map(() =>
                store.readAgentEvents({ threadId, afterSequence: completed!.sequence }),
              ),
            ),
          ),
      } as unknown as ThreadManagement.ThreadManagementService["Service"]),
    );
    assert.deepEqual(
      items.map((item) =>
        item.kind === "event"
          ? [item.sequence, item.event.type === "message.updated" ? item.event.payload.text : null]
          : [item.kind],
      ),
      [
        [latest!.sequence, "Hello"],
        [completed!.sequence, "Hello!"],
        ["synchronized"],
        [completed!.sequence + 1, "Next"],
      ],
    );
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("shell groups read distinct threads once and keep each event's own sequence", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const now = "2026-10-27T00:00:00.000Z";
    const insert = (aggregateKind: "thread" | "project", streamId: string, version: number) =>
      sql<{ sequence: number }>`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, actor_kind, payload_json, metadata_json, application_event_version
      ) VALUES (
        ${`shell-group:${streamId}:${version}`}, ${aggregateKind}, ${streamId}, ${version},
        ${aggregateKind === "thread" ? "provider-session.detached" : "project.meta-updated"},
        ${now}, 'server', ${JSON.stringify(
          aggregateKind === "thread"
            ? { providerSessionId: "session:shell-group", detachedAt: now }
            : { projectId: streamId, updatedAt: now },
        )}, '{}', 2
      ) RETURNING sequence
    `.pipe(Effect.map(([row]) => row!.sequence));
    const shell = (id: string, archivedAt: string | null = null) =>
      ({ id: ThreadId.make(id), archivedAt }) as unknown as OrchestrationV2ThreadShell;
    const project = {
      id: ProjectId.make("project:group"),
    } as unknown as OrchestrationProjectShell;

    const after = yield* insert("project", "project:before", 0);
    yield* insert("thread", "thread:active", 0);
    const archived = yield* insert("thread", "thread:archived", 0);
    const projectUpdated = yield* insert("project", project.id, 0);
    const active = yield* insert("thread", "thread:active", 1);
    const missing = yield* insert("thread", "thread:missing", 0);

    const shellReads: Array<ReadonlyArray<string> | undefined> = [];
    let batchFails = false;
    const collect = (afterSequence: number) =>
      subscribeOrchestrationV2Shell({ afterSequence, requestCompletionMarker: true }).pipe(
        Effect.flatMap((stream) =>
          stream.pipe(
            Stream.takeUntil((item) => item.kind === "synchronized"),
            Stream.runCollect,
          ),
        ),
        Effect.provide(ShellMaterialization.layer),
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          listActiveSubagentThreadIds: () => Effect.succeed([]),
          readShellSnapshot: (options?: { readonly threadIds?: ReadonlyArray<string> }) =>
            Effect.suspend(() => {
              shellReads.push(options?.threadIds);
              return batchFails
                ? Effect.fail("batch failed")
                : Effect.succeed(
                    Effect.succeed({
                      schemaVersion: 1,
                      snapshotSequence: Number.MAX_SAFE_INTEGER,
                      threads: [shell("thread:active")],
                      archivedThreads: [shell("thread:archived", now)],
                    }),
                  );
            }),
          getThreadShell: (threadId: string) =>
            Effect.sync(() => {
              shellReads.push([`scalar:${threadId}`]);
              return threadId === "thread:missing"
                ? null
                : shell(threadId, threadId === "thread:archived" ? now : null);
            }),
        } as unknown as ThreadManagement.ThreadManagementService["Service"]),
        Effect.provideService(ProjectStore.ProjectStoreV2, {
          listShells: () => Effect.succeed([]),
        } as unknown as ProjectStore.ProjectStoreV2["Service"]),
        Effect.provideService(ProjectService.ProjectService, {
          getShell: () => Effect.succeedSome(project),
        } as unknown as ProjectService.ProjectService["Service"]),
        Effect.provideServiceEffect(
          ProjectEnrichment.ProjectEnrichmentService,
          PubSub.unbounded().pipe(
            Effect.map(
              (changes) =>
                ({
                  subscribeChanges: PubSub.subscribe(changes),
                }) as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"],
            ),
          ),
        ),
      );

    const collectSummary = (afterSequence: number) =>
      collect(afterSequence).pipe(
        Effect.map((items) =>
          items.flatMap((item): ReadonlyArray<ReadonlyArray<string | number>> =>
            item.kind === "thread.updated"
              ? [[item.kind, item.sequence, item.thread.id]]
              : item.kind === "thread.removed"
                ? [[item.kind, item.sequence, item.threadId]]
                : item.kind === "project.updated"
                  ? [[item.kind, item.sequence, item.project.id]]
                  : [],
          ),
        ),
      );
    const expectedGroup = [
      ["thread.removed", archived, "thread:archived"],
      ["project.updated", projectUpdated, project.id],
      ["thread.updated", active, "thread:active"],
      ["thread.removed", missing, "thread:missing"],
    ];

    assert.deepEqual(yield* collectSummary(after), expectedGroup);
    assert.deepEqual(shellReads, [["thread:archived", "thread:active", "thread:missing"]]);

    shellReads.length = 0;
    assert.deepEqual(yield* collectSummary(active), [
      ["thread.removed", missing, "thread:missing"],
    ]);
    assert.deepEqual(shellReads, [["scalar:thread:missing"]]);

    shellReads.length = 0;
    batchFails = true;
    assert.deepEqual(yield* collectSummary(after), expectedGroup);
    assert.deepEqual(shellReads, [
      ["thread:archived", "thread:active", "thread:missing"],
      ["scalar:thread:archived"],
      ["scalar:thread:active"],
      ["scalar:thread:missing"],
    ]);
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("shares each live event item across subscribers and dedupes bodies per subscriber", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:shared-live");
    const messageId = MessageId.make("message:shared-live");
    const at = DateTime.makeUnsafe("2026-10-31T00:00:00.000Z");
    const owner = { threadId, runId: null, nodeId: null, createdAt: at, updatedAt: at };
    // DateTime memoizes its formatted parts, so only the plain records are frozen.
    const freeze = <A>(value: A): A => {
      if (typeof value === "object" && value !== null && !DateTime.isDateTime(value)) {
        for (const child of Object.values(value)) freeze(child);
        Object.freeze(value);
      }
      return value;
    };
    const live: ReadonlyArray<OrchestrationV2StoredEvent> = freeze([
      {
        sequence: 1,
        commandId: null,
        event: {
          id: EventId.make("event:shared-live:message"),
          type: "message.updated",
          threadId,
          occurredAt: at,
          payload: {
            ...owner,
            id: messageId,
            role: "assistant",
            text: "Done",
            attachments: [],
            streaming: false,
            createdBy: "agent",
            creationSource: "provider",
          },
        },
      },
      {
        sequence: 2,
        commandId: null,
        event: {
          id: EventId.make("event:shared-live:reply"),
          type: "turn-item.updated",
          threadId,
          occurredAt: at,
          payload: {
            ...owner,
            id: TurnItemId.make("item:shared-live:reply"),
            type: "assistant_message",
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1,
            title: null,
            status: "completed",
            messageId,
            text: "Done",
            streaming: false,
            startedAt: at,
            completedAt: at,
          },
        },
      },
    ] as ReadonlyArray<OrchestrationV2StoredEvent>);
    const stringify = yield* Effect.acquireRelease(
      Effect.sync(() => vi.spyOn(JSON, "stringify")),
      (spy) => Effect.sync(() => spy.mockRestore()),
    );
    // The EventSink hands every subscriber the same projected objects.
    const subscribe = (acceptDedupedBodies: boolean) =>
      subscribeOrchestrationV2Thread({ threadId, afterSequence: 0, acceptDedupedBodies }).pipe(
        Effect.flatMap(Stream.runCollect),
        Effect.map((items) => Array.from(items)),
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ensureLegacyTranscript: () => Effect.void,
          streamStoredEventsFrom: () => Stream.fromIterable(live),
        } as unknown as ThreadManagement.ThreadManagementService["Service"]),
      );
    const [deduped, first, second] = yield* Effect.forEach([true, false, false], subscribe);

    assert.lengthOf(first!, live.length);
    first!.forEach((item, index) => {
      assert.strictEqual(item, second![index]);
      assert.strictEqual(item.kind === "event" && item.event, live[index]!.event);
    });
    assert.strictEqual(deduped![0], first![0]);
    const reply = deduped![1];
    assert.deepInclude(reply?.kind === "event" ? reply.event.payload : undefined, {
      text: "",
      textOmitted: true,
    });
    // The live buffer sizes each shared item once, not once per subscriber.
    assert.lengthOf(
      stringify.mock.calls.filter(([value]) => value?.kind === "event"),
      live.length,
    );
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("shell snapshots omit idle children and older settled threads only on request", () =>
  Effect.gen(function* () {
    const shell = (
      id: string,
      parent: string | null,
      activityRunStatus: "running" | null,
      settledDaysAgo: number | null = null,
    ) =>
      ({
        id: ThreadId.make(id),
        projectId: "project",
        archivedAt: null,
        status: activityRunStatus ?? "completed",
        activityRunStatus,
        pendingRuntimeRequest: null,
        pendingBackgroundTasks: [],
        settledOverride: settledDaysAgo === null ? null : "settled",
        settledAt:
          settledDaysAgo === null ? null : DateTime.makeUnsafe(-settledDaysAgo * 86_400_000),
        lineage: {
          parentThreadId: parent === null ? null : ThreadId.make(parent),
          relationshipToParent: parent === null ? null : "subagent",
          rootThreadId: ThreadId.make(parent ?? id),
        },
      }) as unknown as OrchestrationV2ThreadShell;
    const threads = [
      shell("parent", null, null),
      shell("idle", "parent", null),
      shell("live", "parent", "running"),
      ...Array.from({ length: 25 }, (_, index) => shell(`recent-${index}`, null, null, 1)),
      shell("old", null, null, 30),
    ];
    const firstSnapshot = (input: {
      readonly omitIdleSubagents?: boolean;
      readonly omitOlderSettled?: boolean;
    }) =>
      subscribeOrchestrationV2Shell(input).pipe(
        Effect.flatMap((stream) => stream.pipe(Stream.take(1), Stream.runCollect)),
        Effect.map(([item]) => (item?.kind === "snapshot" ? item.snapshot : null)),
        Effect.provide(ShellMaterialization.layer),
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          listActiveShellIndex: () =>
            Effect.succeed(
              threads.map((thread) => ({
                ...thread,
                pinnedAt: null,
                snoozedUntil: null,
                mayBeLive: thread.activityRunStatus !== null,
              })),
            ),
          readShellSnapshot: (options?: { readonly threadIds?: ReadonlyArray<string> }) =>
            Effect.succeed(
              Effect.succeed({
                schemaVersion: 1,
                snapshotSequence: 0,
                threads: threads.filter(
                  (thread) => options?.threadIds?.includes(thread.id) ?? true,
                ),
                archivedThreads: [],
              }),
            ),
        } as unknown as ThreadManagement.ThreadManagementService["Service"]),
        Effect.provideService(ProjectStore.ProjectStoreV2, {
          listShells: () => Effect.succeed([]),
        } as unknown as ProjectStore.ProjectStoreV2["Service"]),
        Effect.provideService(ProjectService.ProjectService, {
          getShell: () => Effect.succeedNone,
        } as unknown as ProjectService.ProjectService["Service"]),
        Effect.provideServiceEffect(
          ProjectEnrichment.ProjectEnrichmentService,
          PubSub.unbounded().pipe(
            Effect.map(
              (changes) =>
                ({
                  subscribeChanges: PubSub.subscribe(changes),
                }) as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"],
            ),
          ),
        ),
      );

    const recent = threads.slice(3, 28).map((thread) => thread.id);
    const full = yield* firstSnapshot({});
    assert.deepEqual(
      full?.threads.map((thread) => thread.id),
      threads.map((thread) => thread.id),
    );
    assert.isUndefined(full?.olderSettled);
    const withoutChildren = yield* firstSnapshot({ omitIdleSubagents: true });
    assert.deepEqual(
      withoutChildren?.threads.map((thread) => thread.id),
      ["parent", "live", ...recent, "old"],
    );
    const withoutOld = yield* firstSnapshot({ omitIdleSubagents: true, omitOlderSettled: true });
    assert.deepEqual(
      withoutOld?.threads.map((thread) => thread.id),
      ["parent", "live", ...recent],
    );
    assert.deepEqual(withoutOld?.olderSettled?.counts, [
      { projectId: ProjectId.make("project"), count: 1 },
    ]);
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

// Build the production RPC layer; unrelated capabilities fail if this test calls them.
const layerUnusedConfigRpcServices = Layer.mergeAll(
  Layer.mock(ScratchThreadPromotion.ScratchThreadPromotion)({}),
  Layer.mock(DocumentText.DocumentText)({}),
  Layer.mock(ShellMaterialization.ShellMaterialization)({}),
  Layer.mock(PeerLinks.PeerLinks)({}),
  Layer.mock(QueueCompatibility.QueueCompatibility)({}),
  Layer.mock(SettlementOrigins.SettlementOrigins)({}),
  Layer.mock(TokenAccountingService.TokenAccountingService)({ isAvailable: Effect.succeed(false) }),
  Layer.mock(ServerSecretStore.ServerSecretStore)({}),
  Layer.mock(WorkspacePaths.WorkspacePaths)({}),
  Layer.mock(TraceDiagnostics.TraceDiagnostics)({}),
  Layer.mock(TriageService.TriageService)({}),
  Layer.mock(TextGenerators.TextGenerators)({}),
  Layer.mock(ProjectFaviconResolver.ProjectFaviconResolver)({}),
  Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({}),
  Layer.mock(AcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator)({}),
  Layer.mock(AcpRegistrySupport.AcpRegistryCatalog)({}),
  Layer.mock(AgentSessionImporter.AgentSessionImporter)({}),
  Layer.mock(AgentSessionScanner.AgentSessionScanner)({}),
  Layer.mock(AnalyticsService.AnalyticsService)({}),
  Layer.mock(AntigravityInstallation.AntigravityInstallation)({ managedDirectory: "/unused" }),
  Layer.mock(BackgroundPolicy.BackgroundPolicy)({}),
  Layer.mock(CheckpointDiffQuery.CheckpointDiffQuery)({}),
  Layer.mock(NativeChildWorkQuery.NativeChildWorkQuery)({}),
  Layer.mock(CommandOutputHub.CommandOutputHub)({}),
  Layer.mock(CodexInstallation.CodexInstallation)({ managedDirectory: "/unused" }),
  Layer.mock(DeviceService.DeviceService)({}),
  Layer.mock(EnvironmentTheme.EnvironmentThemeService)({}),
  Layer.mock(GitWorkflowService.GitWorkflowService)({}),
  Layer.mock(HostResources.HostResources)({}),
  Layer.mock(McpAppRequests.McpAppRequests)({}),
  Layer.mock(ModelManifest.ModelManifest)({}),
  Layer.mock(OrchestrationEventStore.OrchestrationEventStore)({}),
  Layer.mock(Orchestrator.OrchestratorV2)({}),
  Layer.mock(OtlpEndpointCheck.OtlpEndpointCheck)({}),
  Layer.mock(PairingGrantStore.PairingGrantStore)({}),
  Layer.mock(PeerLinks.PeerLinks)({}),
  Layer.mock(PeerLinkRequests.PeerLinkRequests)({}),
  Layer.mock(PortScanner.PortDiscovery)({ trackAgent: () => () => undefined }),
  Layer.mock(PreviewManager.PreviewManager)({}),
  Layer.mock(ProcessDiagnostics.ProcessDiagnostics)({}),
  Layer.mock(ProcessResourceMonitor.ProcessResourceMonitor)({}),
  Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({}),
  Layer.mock(ProjectService.ProjectService)({}),
  Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({}),
  Layer.mock(ProjectStore.ProjectStoreV2)({}),
  Layer.mock(ProviderAuthService.ProviderAuthService)({}),
  Layer.mock(ProviderInstallation.ProviderInstallation)({}),
  Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
    getCatalogSource: () => undefined,
  }),
  Layer.mock(ProviderLatestVersions.ProviderLatestVersions)({}),
  Layer.mock(ProviderMaintenanceRunner.ProviderMaintenanceRunner)({}),
  Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
  Layer.mock(PullRequestReviewGuideService.PullRequestReviewGuideService)({}),
  Layer.mock(PullRequestService.PullRequestService)({}),
  Layer.mock(PullRequestSyncReactor.PullRequestSyncReactor)({}),
  Layer.mock(RelayClient.RelayClient)({}),
  Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({}),
  Layer.mock(ResourceTelemetry.ResourceTelemetry)({}),
  Layer.mock(ReviewService.ReviewService)({}),
  Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
  Layer.mock(SecretRequests.SecretRequests)({}),
  Layer.mock(SecretResolver.SecretResolver)({}),
  Layer.mock(GuestShareService.GuestShareService)({}),
  Layer.mock(ServerLifecycleEvents.ServerLifecycleEvents)({}),
  Layer.mock(ServerRuntimeStartup.ServerRuntimeStartup)({}),
  Layer.mock(ServerSelfUpdate.ServerSelfUpdate)({}),
  Layer.mock(NpmRelease.NpmRelease)({}),
  Layer.mock(SessionStore.SessionStore)({ cookieName: "config-test", legacyCookieName: undefined }),
  Layer.mock(SourceControlDiscovery.SourceControlDiscovery)({}),
  Layer.mock(SourceControlRepositoryService.SourceControlRepositoryService)({}),
  Layer.mock(SourceControlSignIn.SourceControlSignIn)({}),
  Layer.mock(StorageCleanup.StorageCleanup)({}),
  Layer.mock(DatabaseStorage.DatabaseStorage)({}),
  Layer.mock(TerminalManager.TerminalManager)({}),
  Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
  Layer.mock(ThreadManagement.ThreadManagementService)({}),
  Layer.mock(ThreadSearch.ThreadSearch)({}),
  Layer.mock(UsageService.UsageService)({}),
  Layer.mock(VcsProvisioningService.VcsProvisioningService)({}),
  Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({}),
  Layer.mock(WorkspaceEntries.WorkspaceEntries)({}),
  Layer.mock(WorkspaceFileSystem.WorkspaceFileSystem)({}),
  Layer.mock(WorktreeSetupTracker.WorktreeSetupTracker)({}),
);

const layerConfigSources = Layer.mergeAll(
  Keybindings.layer,
  AssetSigner.layer.pipe(Layer.provide(ServerSecretStore.layer)),
  ServerSettings.layer.pipe(
    Layer.provide(Layer.merge(ServerSecretStore.layer, ManagedPolicy.layerTest())),
  ),
).pipe(
  Layer.provideMerge(Layer.fresh(SqlitePersistence.layerMemory)),
  Layer.provideMerge(
    Layer.fresh(
      ServerConfigModule.layerTest(process.cwd(), { prefix: "t3-config-subscription-test-" }),
    ),
  ),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);

it.effect.each([
  "pending",
  "cached",
  "direct-pending",
  "direct-cached",
  "direct-failed",
  "direct-interrupted",
] as const)(
  "%s endpoint discovery preserves initial config and updates through discovery and ACK",
  (discovery) =>
    Effect.gen(function* () {
      const keybindings = yield* Keybindings.Keybindings;
      const settings = yield* ServerSettings.ServerSettingsService;
      const keybindingsSubscribed = yield* Deferred.make<void>();
      const settingsSubscribed = yield* Deferred.make<void>();
      const keybindingsChanges = keybindings.subscribeChanges.pipe(
        Effect.tap(() => Deferred.succeed(keybindingsSubscribed, undefined)),
      );
      const settingsChanges = settings.subscribeChanges.pipe(
        Effect.tap(() => Deferred.succeed(settingsSubscribed, undefined)),
      );
      const initialKeybindings = (yield* keybindings.getSnapshot).keybindings;
      const discoveryStarted = yield* Deferred.make<void>();
      const finishDiscovery = yield* Deferred.make<void>();
      const remoteDiscoveryStarted = yield* Deferred.make<void>();
      const finishRemoteDiscovery = yield* Deferred.make<void>();
      const targets = [{ kind: "mdns" as const, host: "test.local" }];
      const direct = discovery.startsWith("direct-");
      const bound = [{ kind: "lan" as const, httpBaseUrl: "http://192.168.1.10:3773/" }];
      const endpoints = [
        ...bound,
        { kind: "tailnet" as const, httpBaseUrl: "https://test.ts.net/" },
      ];
      const directStarted = yield* Deferred.make<void>();
      const finishDirect = yield* Deferred.make<void>();
      const directStopped = yield* Deferred.make<void>();
      const responses = yield* Queue.unbounded<ServerConfigStreamEvent>();
      const configRpcServices = Layer.mergeAll(
        layerUnusedConfigRpcServices,
        Layer.mock(ExternalLauncher.ExternalLauncher)({
          resolveAvailableEditors: () =>
            Deferred.succeed(discoveryStarted, undefined).pipe(
              Effect.andThen(Deferred.await(finishDiscovery)),
              Effect.as([]),
            ),
          resolveFileManagerRevealKind: () => Effect.undefined,
        }),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
          streamChanges: Stream.never,
        }),
        Layer.mock(UsageLimitSources.UsageLimitSources)({ streamChanges: Stream.make([]) }),
        Layer.mock(RemoteOpenTargets.RemoteOpenTargets)({
          resolveTargets: () =>
            Deferred.succeed(remoteDiscoveryStarted, undefined).pipe(
              Effect.andThen(
                discovery === "cached" || direct
                  ? Effect.void
                  : Deferred.await(finishRemoteDiscovery),
              ),
              Effect.as(targets),
            ),
        }),
        Layer.mock(DirectEndpoints.DirectEndpoints)({
          resolveAvailable: () => Effect.succeed(discovery === "direct-cached" ? endpoints : bound),
          resolve: () =>
            direct
              ? Deferred.succeed(directStarted, undefined).pipe(
                  Effect.andThen(
                    discovery === "direct-cached" ? Effect.void : Deferred.await(finishDirect),
                  ),
                  Effect.andThen(
                    discovery === "direct-failed"
                      ? Effect.die("probe failed")
                      : Effect.succeed(endpoints),
                  ),
                  Effect.onExit(() => Deferred.succeed(directStopped, undefined)),
                )
              : Effect.succeed(bound),
        }),
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
          scratchRoot: Effect.succeedNone,
          namedProjectsRoot: process.cwd(),
        }),
        Layer.mock(ScratchThreadPromotion.ScratchThreadPromotion)({}),
        Layer.mock(ServerEnvironment.ServerEnvironment)({
          getDescriptor: Effect.succeed({
            environmentId: EnvironmentId.make("config-test"),
            label: "Config test",
            platform: { os: "linux", arch: "x64" },
            serverVersion: "0.0.0",
            capabilities: { repositoryIdentity: false },
          }),
        }),
        Layer.mock(EnvironmentAuth.EnvironmentAuth)({
          getDescriptor: () =>
            Effect.succeed({
              policy: "loopback-browser",
              bootstrapMethods: [],
              sessionMethods: ["browser-session-cookie"],
              sessionCookieName: "config-test-session",
            }),
        }),
        // Signal subscription on either accessor so the test also fails, rather
        // than hangs, against a handler that subscribes after the snapshot.
        Layer.succeed(Keybindings.Keybindings, {
          ...keybindings,
          streamChanges: Stream.unwrap(keybindingsChanges),
          subscribeChanges: keybindingsChanges,
        }),
        Layer.succeed(ServerSettings.ServerSettingsService, {
          ...settings,
          streamChanges: Stream.unwrap(settingsChanges),
          subscribeChanges: settingsChanges,
        }),
      );
      const server = yield* RpcServer.makeNoSerialization(
        RpcGroup.make(WsSubscribeServerConfigRpc),
        {
          ...WS_RPC_SERVER_OPTIONS,
          onFromServer: (response) =>
            response._tag === "Chunk"
              ? Effect.forEach(response.values, (event) => Queue.offer(responses, event), {
                  discard: true,
                })
              : Effect.void,
        },
      ).pipe(
        Effect.provide(
          layerWsRpc(
            {
              sessionId: AuthSessionId.make("config-test"),
              subject: "config-test",
              method: "browser-session-cookie",
              scopes: [],
            },
            {},
            yield* ServerBrowser.ServerBrowser,
            () => Effect.void,
            { secretReferences: true },
          ).pipe(Layer.provide(configRpcServices)),
        ),
      );
      const requestId = RpcMessage.RequestId("config");
      const acknowledge = server.write(0, { _tag: "Ack", requestId });
      yield* server.write(0, {
        _tag: "Request",
        id: requestId,
        tag: WS_METHODS.subscribeServerConfig,
        payload: {},
        headers: Headers.empty,
      });
      yield* Deferred.await(discoveryStarted);
      assert.isTrue(yield* Deferred.isDone(remoteDiscoveryStarted));

      const duringDiscovery = yield* settings.updateSettings({ defaultAutoPull: true });
      yield* Deferred.succeed(finishDiscovery, undefined);
      const snapshot = yield* Queue.take(responses);
      assert.equal(snapshot.type, "snapshot");
      if (snapshot.type !== "snapshot") return yield* Effect.die("Missing initial snapshot");
      assert.deepEqual(
        snapshot.config.remoteOpenTargets,
        discovery === "cached" || direct ? targets : [],
      );
      assert.deepEqual(
        snapshot.config.directEndpoints,
        discovery === "direct-cached" ? endpoints : bound,
      );
      assert.isFalse(snapshot.config.settings.defaultAutoPull);
      assert.deepEqual(snapshot.config.keybindings, initialKeybindings);
      const duringAck = yield* keybindings.upsertKeybindingRule({
        key: "mod+alt+y",
        command: "terminal.toggle",
      });
      yield* acknowledge;
      yield* Deferred.await(keybindingsSubscribed);
      yield* Deferred.await(settingsSubscribed);

      // Later updates expose a dropped buffered event as a value mismatch, not a timeout.
      const latestSettings = yield* settings.updateSettings({ enableProviderUpdateChecks: false });
      const latestKeybindings = yield* keybindings.upsertKeybindingRule({
        key: "mod+alt+z",
        command: "terminal.split",
      });
      const expectedSettings = [duringDiscovery, latestSettings].map((value) =>
        ServerSettings.redactServerSettingsForClient(value, { secretReferences: true }),
      );
      const expectedKeybindings = [duringAck.keybindings, latestKeybindings.keybindings];
      // The two sources can interleave, but each must retain its own publication order.
      for (let i = 0; i < 4; i++) {
        const event = yield* Queue.take(responses);
        if (event.type === "settingsUpdated") {
          assert.deepEqual(event.payload.settings, expectedSettings.shift());
        } else {
          assert.equal(event.type, "keybindingsUpdated");
          if (event.type !== "keybindingsUpdated")
            return yield* Effect.die("Unexpected config event");
          assert.deepEqual(event.payload.keybindings, expectedKeybindings.shift());
          assert.deepEqual(event.payload.issues, []);
        }
        yield* acknowledge;
      }
      assert.isEmpty(expectedSettings);
      assert.isEmpty(expectedKeybindings);
      if (discovery === "pending") {
        yield* Deferred.await(remoteDiscoveryStarted);
        yield* Deferred.succeed(finishRemoteDiscovery, undefined);
        const resolved = yield* Queue.take(responses);
        assert.equal(resolved.type, "snapshot");
        if (resolved.type !== "snapshot") return yield* Effect.die("Missing remote targets");
        assert.deepEqual(resolved.config.remoteOpenTargets, targets);
        assert.deepEqual(
          resolved.config.settings,
          ServerSettings.redactServerSettingsForClient(latestSettings, { secretReferences: true }),
        );
        assert.deepEqual(resolved.config.keybindings, latestKeybindings.keybindings);
        yield* acknowledge;
      }
      if (direct) {
        yield* Deferred.await(directStarted);
        if (discovery === "direct-pending") {
          yield* Deferred.succeed(finishDirect, undefined);
          const resolved = yield* Queue.take(responses);
          assert.equal(resolved.type, "snapshot");
          if (resolved.type !== "snapshot") return yield* Effect.die("Missing direct endpoints");
          assert.deepEqual(resolved.config.directEndpoints, endpoints);
          assert.deepEqual(
            resolved.config.settings,
            ServerSettings.redactServerSettingsForClient(latestSettings, {
              secretReferences: true,
            }),
          );
          assert.deepEqual(resolved.config.keybindings, latestKeybindings.keybindings);
          yield* acknowledge;
        } else if (discovery === "direct-failed") {
          yield* Deferred.succeed(finishDirect, undefined);
          yield* Deferred.await(directStopped);
          yield* settings.updateSettings({ defaultAutoPull: false });
          const event = yield* Queue.take(responses);
          assert.equal(event.type, "settingsUpdated");
          yield* acknowledge;
        }
      }
      yield* server.write(0, { _tag: "Interrupt", requestId, interruptors: [] });
      if (direct) yield* Deferred.await(directStopped);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.fresh(layerConfigSources),
          Layer.mock(PreviewAutomationBroker.PreviewAutomationBroker)({}),
          Layer.mock(ServerBrowser.ServerBrowser)({}),
        ),
      ),
      Effect.scoped,
    ),
);

it.effect("on-demand clients get provider config without workspace snapshots", () =>
  Effect.gen(function* () {
    const instanceId = ProviderInstanceId.make("claudeAgent");
    const snapshotFor = (cwd: string, checkedAt: string) => ({
      cwd,
      checkedAt,
      slashCommands: [{ name: `${cwd.slice(1)}-command` }],
      skills: [],
    });
    const provider: ServerProvider = {
      instanceId,
      driver: ProviderDriverKind.make("claudeAgent"),
      enabled: true,
      installed: true,
      version: "1.0.0",
      status: "ready",
      auth: { status: "authenticated" },
      checkedAt: "2026-10-25T00:00:00.000Z",
      models: [],
      slashCommands: [{ name: "machine-command", description: "A machine command" }],
      skills: [
        {
          name: "machine-skill",
          description: "Machine skill",
          path: "/skills/machine-skill",
          enabled: true,
        },
      ],
      workspaceSnapshots: [
        snapshotFor("/a", "2026-10-25T00:00:00.000Z"),
        snapshotFor("/b", "2026-10-25T00:00:00.000Z"),
      ],
    };
    let scansCwd = true;
    const providerChanges = yield* Queue.unbounded<ReadonlyArray<ServerProvider>>();
    const pulled = yield* Queue.unbounded<void>();
    const responses = yield* Queue.unbounded<unknown>();
    const configRpcServices = Layer.mergeAll(
      layerUnusedConfigRpcServices,
      Layer.mock(ExternalLauncher.ExternalLauncher)({
        resolveAvailableEditors: () => Effect.succeed([]),
        resolveFileManagerRevealKind: () => Effect.undefined,
      }),
      Layer.mock(ProviderRegistry.ProviderRegistry)({
        getProviders: Effect.succeed([provider]),
        streamChanges: Stream.fromQueue(providerChanges).pipe(
          Stream.tap(() => Queue.offer(pulled, undefined)),
        ),
        refreshWorkspaceSnapshot: () => Effect.succeed([provider]),
      }),
      Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
        listInstances: Effect.sync(() => [
          (scansCwd
            ? { instanceId, snapshotForCwd: () => Effect.die("unused") }
            : { instanceId }) as never,
        ]),
        getInstance: () =>
          Effect.sync(
            () =>
              (scansCwd
                ? { instanceId, snapshotForCwd: () => Effect.die("unused") }
                : { instanceId }) as never,
          ),
      }),
      Layer.mock(UsageLimitSources.UsageLimitSources)({
        current: Effect.succeed([]),
        streamChanges: Stream.make([]),
      }),
      Layer.mock(RemoteOpenTargets.RemoteOpenTargets)({
        resolveTargets: () => Effect.succeed([]),
      }),
      Layer.mock(DirectEndpoints.DirectEndpoints)({
        resolve: () => Effect.succeed([]),
        resolveAvailable: () => Effect.succeed([]),
      }),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        scratchRoot: Effect.succeedNone,
        namedProjectsRoot: process.cwd(),
      }),
      Layer.mock(ScratchThreadPromotion.ScratchThreadPromotion)({}),
      Layer.mock(ServerEnvironment.ServerEnvironment)({
        getDescriptor: Effect.succeed({
          environmentId: EnvironmentId.make("config-test"),
          label: "Config test",
          platform: { os: "linux", arch: "x64" },
          serverVersion: "0.0.0",
          capabilities: { repositoryIdentity: false },
        }),
      }),
      Layer.mock(EnvironmentAuth.EnvironmentAuth)({
        getDescriptor: () =>
          Effect.succeed({
            policy: "loopback-browser",
            bootstrapMethods: [],
            sessionMethods: ["browser-session-cookie"],
            sessionCookieName: "config-test-session",
          }),
      }),
    );
    const server = yield* RpcServer.makeNoSerialization(
      RpcGroup.make(WsSubscribeServerConfigRpc, WsServerRefreshProvidersRpc),
      {
        ...WS_RPC_SERVER_OPTIONS,
        onFromServer: (response) =>
          response._tag === "Chunk"
            ? Effect.forEach(response.values, (value) => Queue.offer(responses, value), {
                discard: true,
              })
            : response._tag === "Exit" && response.exit._tag === "Success"
              ? Queue.offer(responses, response.exit.value)
              : Effect.void,
      },
    ).pipe(
      Effect.provide(
        layerWsRpc(
          {
            sessionId: AuthSessionId.make("config-test"),
            subject: "config-test",
            method: "browser-session-cookie",
            scopes: [],
          },
          {},
          yield* ServerBrowser.ServerBrowser,
          () => Effect.void,
          { secretReferences: true },
        ).pipe(Layer.provide(configRpcServices)),
      ),
    );
    const configRequest = RpcMessage.RequestId("config");
    yield* server.write(0, {
      _tag: "Request",
      id: configRequest,
      tag: WS_METHODS.subscribeServerConfig,
      payload: { workspaceSnapshotsOnDemand: true, providerCatalogsOnDemand: true },
      headers: Headers.empty,
    });
    const snapshot = (yield* Queue.take(responses)) as ServerConfigStreamEvent;
    if (snapshot.type !== "snapshot") return yield* Effect.die("Missing initial snapshot");
    assert.isUndefined(snapshot.config.providers[0]?.workspaceSnapshots);
    // The cwd snapshot the client fetches carries the catalog, so only command names are sent.
    assert.deepEqual(snapshot.config.providers[0]?.slashCommands, [{ name: "machine-command" }]);
    assert.deepEqual(snapshot.config.providers[0]?.skills, [
      { name: "machine-skill", path: "/skills/machine-skill", enabled: true },
    ]);
    yield* server.write(0, { _tag: "Ack", requestId: configRequest });

    // A rescan that only touches a snapshot sends nothing; the next real change does.
    yield* Queue.offer(providerChanges, [
      { ...provider, workspaceSnapshots: [snapshotFor("/a", "2026-10-25T00:05:00.000Z")] },
    ]);
    yield* Queue.take(pulled);
    yield* TestClock.adjust("1 second");
    // Late /usage-limits discovery still reaches the client, so it refetches its cwd snapshot.
    yield* Queue.offer(providerChanges, [
      {
        ...provider,
        status: "warning",
        slashCommands: [...provider.slashCommands, USAGE_LIMITS_COMMAND],
      },
    ]);
    yield* Queue.take(pulled);
    yield* TestClock.adjust("1 second");
    const update = (yield* Queue.take(responses)) as ServerConfigStreamEvent;
    if (update.type !== "providerStatuses") return yield* Effect.die("Expected provider update");
    assert.equal(update.payload.providers[0]?.status, "warning");
    assert.isUndefined(update.payload.providers[0]?.workspaceSnapshots);
    assert.deepEqual(update.payload.providers[0]?.slashCommands, [
      { name: "machine-command" },
      { name: USAGE_LIMITS_COMMAND.name },
    ]);
    yield* server.write(0, { _tag: "Ack", requestId: configRequest });
    // A replacement that cannot scan a cwd sends its full machine catalog.
    scansCwd = false;
    yield* Queue.offer(providerChanges, [{ ...provider, status: "warning" }]);
    yield* Queue.take(pulled);
    yield* TestClock.adjust("1 second");
    const replaced = (yield* Queue.take(responses)) as ServerConfigStreamEvent;
    if (replaced.type !== "providerStatuses") return yield* Effect.die("Expected provider update");
    assert.deepEqual(replaced.payload.providers[0]?.slashCommands, provider.slashCommands);
    assert.deepEqual(replaced.payload.providers[0]?.skills, provider.skills);

    yield* server.write(0, {
      _tag: "Request",
      id: RpcMessage.RequestId("refresh"),
      tag: WS_METHODS.serverRefreshProviders,
      payload: { instanceId, cwd: "/b", workspaceSnapshotsOnDemand: true },
      headers: Headers.empty,
    });
    const refreshed = (yield* Queue.take(responses)) as {
      readonly providers: ReadonlyArray<ServerProvider>;
    };
    assert.deepEqual(
      refreshed.providers.map((entry) => [entry.workspaceSnapshots, entry.slashCommands]),
      [[[snapshotFor("/b", "2026-10-25T00:00:00.000Z")], []]],
    );
    // A failed scan still gives a client without the machine catalog its commands.
    scansCwd = true;
    yield* server.write(0, {
      _tag: "Request",
      id: RpcMessage.RequestId("refresh-failed-scan"),
      tag: WS_METHODS.serverRefreshProviders,
      payload: {
        instanceId,
        cwd: "/c",
        workspaceSnapshotsOnDemand: true,
        providerCatalogsOnDemand: true,
      },
      headers: Headers.empty,
    });
    const fallback = (yield* Queue.take(responses)) as {
      readonly providers: ReadonlyArray<ServerProvider>;
    };
    assert.deepEqual(fallback.providers[0]?.workspaceSnapshots, [
      {
        cwd: "/c",
        checkedAt: provider.checkedAt,
        slashCommands: provider.slashCommands,
        slashCommandsPending: true,
        skills: provider.skills,
      },
    ]);
    yield* server.write(0, { _tag: "Interrupt", requestId: configRequest, interruptors: [] });
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.fresh(layerConfigSources),
        Layer.mock(PreviewAutomationBroker.PreviewAutomationBroker)({}),
        Layer.mock(ServerBrowser.ServerBrowser)({}),
      ),
    ),
    Effect.scoped,
  ),
);

it.effect.each([true, false, undefined] as const)(
  "preserves provider snapshots unless the client opts in (%s)",
  (workspaceSnapshotsOnDemand) =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("claudeAgent");
      const snapshotFor = (cwd: string, checkedAt: string) => ({
        cwd,
        checkedAt,
        slashCommands: [{ name: `${cwd.slice(1)}-command` }],
        skills: [],
      });
      const provider: ServerProvider = {
        instanceId,
        driver: ProviderDriverKind.make("claudeAgent"),
        enabled: true,
        installed: true,
        version: "1.0.0",
        status: "ready",
        auth: { status: "authenticated" },
        checkedAt: "2026-10-25T00:00:00.000Z",
        models: [],
        slashCommands: [{ name: "machine-command" }],
        skills: [],
        workspaceSnapshots: [
          snapshotFor("/a", "2026-10-25T00:00:00.000Z"),
          snapshotFor("/b", "2026-10-25T00:00:00.000Z"),
        ],
      };
      const otherProvider: ServerProvider = {
        ...provider,
        instanceId: ProviderInstanceId.make("other-claude"),
      };
      const providers = [provider, otherProvider];
      const providerChanges = yield* Queue.unbounded<ReadonlyArray<ServerProvider>>();
      const pulled = yield* Queue.unbounded<void>();
      const responses = yield* Queue.unbounded<unknown>();
      const configRpcServices = Layer.mergeAll(
        layerUnusedConfigRpcServices,
        Layer.mock(ExternalLauncher.ExternalLauncher)({
          resolveAvailableEditors: () => Effect.succeed([]),
          resolveFileManagerRevealKind: () => Effect.undefined,
        }),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed(providers),
          streamChanges: Stream.fromQueue(providerChanges).pipe(
            Stream.tap(() => Queue.offer(pulled, undefined)),
          ),
          refreshWorkspaceSnapshot: () => Effect.succeed(providers),
        }),
        Layer.mock(UsageLimitSources.UsageLimitSources)({
          current: Effect.succeed([]),
          streamChanges: Stream.make([]),
        }),
        Layer.mock(RemoteOpenTargets.RemoteOpenTargets)({
          resolveTargets: () => Effect.succeed([]),
        }),
        Layer.mock(DirectEndpoints.DirectEndpoints)({
          resolve: () => Effect.succeed([]),
          resolveAvailable: () => Effect.succeed([]),
        }),
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
          scratchRoot: Effect.succeedNone,
          namedProjectsRoot: process.cwd(),
        }),
        Layer.mock(ServerEnvironment.ServerEnvironment)({
          getDescriptor: Effect.succeed({
            environmentId: EnvironmentId.make("config-test"),
            label: "Config test",
            platform: { os: "linux", arch: "x64" },
            serverVersion: "0.0.0",
            capabilities: { repositoryIdentity: false },
          }),
        }),
        Layer.mock(EnvironmentAuth.EnvironmentAuth)({
          getDescriptor: () =>
            Effect.succeed({
              policy: "loopback-browser",
              bootstrapMethods: [],
              sessionMethods: ["browser-session-cookie"],
              sessionCookieName: "config-test-session",
            }),
        }),
      );
      const server = yield* RpcServer.makeNoSerialization(
        RpcGroup.make(WsSubscribeServerConfigRpc, WsServerRefreshProvidersRpc),
        {
          ...WS_RPC_SERVER_OPTIONS,
          onFromServer: (response) =>
            response._tag === "Chunk"
              ? Effect.forEach(response.values, (value) => Queue.offer(responses, value), {
                  discard: true,
                })
              : response._tag === "Exit" && response.exit._tag === "Success"
                ? Queue.offer(responses, response.exit.value)
                : Effect.void,
        },
      ).pipe(
        Effect.provide(
          layerWsRpc(
            {
              sessionId: AuthSessionId.make("config-test"),
              subject: "config-test",
              method: "browser-session-cookie",
              scopes: [],
            },
            {},
            yield* ServerBrowser.ServerBrowser,
            () => Effect.void,
            { secretReferences: true },
            () => Effect.succeed({}),
          ).pipe(Layer.provide(configRpcServices)),
        ),
      );
      const configRequest = RpcMessage.RequestId("config");
      yield* server.write(0, {
        _tag: "Request",
        id: configRequest,
        tag: WS_METHODS.subscribeServerConfig,
        payload: workspaceSnapshotsOnDemand === undefined ? {} : { workspaceSnapshotsOnDemand },
        headers: Headers.empty,
      });
      const snapshot = (yield* Queue.take(responses)) as ServerConfigStreamEvent;
      if (snapshot.type !== "snapshot") return yield* Effect.die("Missing initial snapshot");
      if (workspaceSnapshotsOnDemand === true) {
        assert.isUndefined(snapshot.config.providers[0]?.workspaceSnapshots);
      } else {
        assert.deepEqual(snapshot.config.providers, providers);
      }
      yield* server.write(0, { _tag: "Ack", requestId: configRequest });

      // Snapshot-only updates reach legacy clients but disappear before on-demand de-duplication.
      const rescanned = [
        { ...provider, workspaceSnapshots: [snapshotFor("/a", "2026-10-25T00:05:00.000Z")] },
        otherProvider,
      ];
      yield* Queue.offer(providerChanges, rescanned);
      yield* Queue.take(pulled);
      yield* TestClock.adjust("1 second");
      if (workspaceSnapshotsOnDemand !== true) {
        const rescan = (yield* Queue.take(responses)) as ServerConfigStreamEvent;
        if (rescan.type !== "providerStatuses")
          return yield* Effect.die("Expected snapshot update");
        assert.deepEqual(rescan.payload.providers, rescanned);
        yield* server.write(0, { _tag: "Ack", requestId: configRequest });
      }
      yield* Queue.offer(providerChanges, [{ ...provider, status: "warning" }, otherProvider]);
      yield* Queue.take(pulled);
      yield* TestClock.adjust("1 second");
      const update = (yield* Queue.take(responses)) as ServerConfigStreamEvent;
      if (update.type !== "providerStatuses") return yield* Effect.die("Expected provider update");
      assert.equal(update.payload.providers[0]?.status, "warning");
      if (workspaceSnapshotsOnDemand === true) {
        assert.isUndefined(update.payload.providers[0]?.workspaceSnapshots);
      } else {
        assert.deepEqual(
          update.payload.providers[0]?.workspaceSnapshots,
          provider.workspaceSnapshots,
        );
      }

      yield* server.write(0, {
        _tag: "Request",
        id: RpcMessage.RequestId("refresh"),
        tag: WS_METHODS.serverRefreshProviders,
        payload: {
          instanceId,
          cwd: "/b",
          ...(workspaceSnapshotsOnDemand === undefined ? {} : { workspaceSnapshotsOnDemand }),
        },
        headers: Headers.empty,
      });
      const refreshed = (yield* Queue.take(responses)) as {
        readonly providers: ReadonlyArray<ServerProvider>;
      };
      if (workspaceSnapshotsOnDemand === true) {
        assert.deepEqual(
          refreshed.providers.map((entry) => entry.instanceId),
          [instanceId],
        );
        assert.deepEqual(
          refreshed.providers.map((entry) => [entry.workspaceSnapshots, entry.slashCommands]),
          [[[snapshotFor("/b", "2026-10-25T00:00:00.000Z")], []]],
        );
      } else {
        assert.deepEqual(refreshed.providers, providers);
      }
      yield* server.write(0, { _tag: "Interrupt", requestId: configRequest, interruptors: [] });
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.fresh(layerConfigSources),
          Layer.mock(PreviewAutomationBroker.PreviewAutomationBroker)({}),
          Layer.mock(ServerBrowser.ServerBrowser)({}),
        ),
      ),
      Effect.scoped,
    ),
);

it.effect("a publish whose push fails still refreshes the new remote", () =>
  Effect.gen(function* () {
    const refreshes = yield* Queue.unbounded<string>();
    const exits = yield* Queue.unbounded<string>();
    const services = Layer.mergeAll(
      layerUnusedConfigRpcServices,
      Layer.mock(SourceControlRepositoryService.SourceControlRepositoryService)({
        publishRepository: (input) =>
          Effect.fail(
            new SourceControlRepositoryError({
              provider: input.provider,
              operation: "publishRepository",
              detail: 'Created owner/repo and added it as remote "origin", but the push failed.',
            }),
          ),
      }),
      Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
        resolve: (cwd, options) =>
          Queue.offer(refreshes, `identity ${cwd} refresh=${options?.refresh === true}`).pipe(
            Effect.as(null),
          ),
      }),
      Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({
        refreshStatus: (cwd) =>
          Queue.offer(refreshes, `status ${cwd}`).pipe(Effect.andThen(Effect.die("unused"))),
      }),
      Layer.mock(ExternalLauncher.ExternalLauncher)({}),
      Layer.mock(ProviderRegistry.ProviderRegistry)({}),
      Layer.mock(UsageLimitSources.UsageLimitSources)({}),
      Layer.mock(RemoteOpenTargets.RemoteOpenTargets)({}),
      Layer.mock(DirectEndpoints.DirectEndpoints)({}),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({ namedProjectsRoot: "/unused" }),
      Layer.mock(ScratchThreadPromotion.ScratchThreadPromotion)({}),
      Layer.mock(ServerEnvironment.ServerEnvironment)({}),
      Layer.mock(EnvironmentAuth.EnvironmentAuth)({}),
    );
    const server = yield* RpcServer.makeNoSerialization(WsRpcGroup, {
      ...WS_RPC_SERVER_OPTIONS,
      onFromServer: (response) =>
        response._tag === "Exit" ? Queue.offer(exits, response.exit._tag) : Effect.void,
    }).pipe(
      Effect.provide(
        layerWsRpc(
          {
            sessionId: AuthSessionId.make("publish-test"),
            subject: "publish-test",
            method: "browser-session-cookie",
            scopes: [AuthSourceControlWriteScope],
          },
          {},
          yield* ServerBrowser.ServerBrowser,
          () => Effect.void,
          { secretReferences: true },
        ).pipe(
          Layer.provide(services),
          Layer.merge(RpcAuthorization.layer([AuthSourceControlWriteScope])),
        ),
      ),
    );
    yield* server.write(0, {
      _tag: "Request",
      id: RpcMessage.RequestId("publish"),
      tag: WS_METHODS.sourceControlPublishRepository,
      payload: {
        cwd: "/repo",
        provider: "github",
        repository: "owner/repo",
        visibility: "private",
      },
      headers: Headers.empty,
    });
    assert.equal(yield* Queue.take(exits), "Failure");
    assert.deepEqual(
      [yield* Queue.take(refreshes), yield* Queue.take(refreshes)],
      ["identity /repo refresh=true", "status /repo"],
    );
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.fresh(layerConfigSources),
        Layer.mock(PreviewAutomationBroker.PreviewAutomationBroker)({}),
        Layer.mock(ServerBrowser.ServerBrowser)({}),
      ),
    ),
    Effect.scoped,
  ),
);

// Pruned history cannot be replayed: a cursor below the boundary must resync from a snapshot,
// or, for a shell client that loads it over HTTP, be told to without the server building one.
it.effect.each([
  { scope: "thread", httpSnapshotFallback: false },
  { scope: "shell", httpSnapshotFallback: false },
  { scope: "shell", httpSnapshotFallback: true },
] as const)(
  "$scope resume from a cursor below the pruned boundary (HTTP fallback: $httpSnapshotFallback)",
  ({ scope, httpSnapshotFallback }) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-10-13T00:00:00.000Z";
      const threadId = ThreadId.make("thread:pruned");
      const insert = (version: number) => sql<{ sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          ${`pruned:${version}`}, 'thread', ${threadId}, ${version}, 'provider-session.detached',
          ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:pruned", detachedAt: now })},
          '{}', 2
        ) RETURNING sequence
      `;
      const [first] = yield* insert(0);
      const [second] = yield* insert(1);
      yield* insert(2);
      yield* sql`
        INSERT INTO orchestration_v2_projection_metadata (
          projection_name, schema_version, last_sequence, updated_at
        ) VALUES (${OrchestrationEventStore.EVENT_LOG_PRUNED_THROUGH}, 2, ${second!.sequence}, ${now})
      `;
      const subscription = Effect.gen(function* () {
        const items: ReadonlyArray<unknown> =
          scope === "thread"
            ? yield* Stream.runCollect(
                yield* subscribeOrchestrationV2Thread({ threadId, afterSequence: first!.sequence }),
              )
            : yield* Stream.runCollect(
                yield* subscribeOrchestrationV2Shell({
                  afterSequence: first!.sequence,
                  ...(httpSnapshotFallback ? { httpSnapshotFallback: true as const } : {}),
                }),
              );
        return items;
      });
      const exit = yield* subscription.pipe(
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ensureLegacyTranscript: () => Effect.void,
          getThreadSnapshot: () => Effect.die("snapshot"),
          listActiveShellIndex: () => Effect.die("snapshot"),
          readShellSnapshot: () => Effect.die("snapshot"),
          listActiveSubagentThreadIds: () => Effect.die("replay"),
        } as unknown as ThreadManagement.ThreadManagementService["Service"]),
        Effect.provideService(ProjectStore.ProjectStoreV2, {
          listShells: () => Effect.succeed([]),
        } as unknown as ProjectStore.ProjectStoreV2["Service"]),
        Effect.provideService(
          ProjectService.ProjectService,
          {} as ProjectService.ProjectService["Service"],
        ),
        Effect.provideService(ProjectEnrichment.ProjectEnrichmentService, {
          subscribeChanges: PubSub.subscribe(yield* PubSub.unbounded()),
        } as ProjectEnrichment.ProjectEnrichmentService["Service"]),
        Effect.exit,
      );
      assert.deepEqual(
        Exit.isFailure(exit) ? Cause.squash(exit.cause) : Array.from(exit.value),
        httpSnapshotFallback ? [{ kind: "snapshot-required" }] : "snapshot",
      );
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

// A prune pass can delete replayed rows after the boundary check; the read must not be trusted.
it.effect.each(["thread", "shell"] as const)(
  "%s resume loads a snapshot when pruning advances during the replay read",
  (scope) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-10-13T00:00:00.000Z";
      const threadId = ThreadId.make("thread:prune-race");
      const insert = (version: number) => sql<{ sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          ${`prune-race:${version}`}, 'thread', ${threadId}, ${version}, 'provider-session.detached',
          ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:race", detachedAt: now })},
          '{}', 2
        ) RETURNING sequence
      `;
      const [first] = yield* insert(0);
      const [second] = yield* insert(1);
      yield* insert(2);

      const readStarted = yield* Deferred.make<void>();
      const releaseRead = yield* Deferred.make<void>();
      let gated = false;
      const gateReplayRead: Statement.Transformer = (statement) => {
        const [query] = statement.compile();
        // The paged replay read (readApplicationRows), not the stats or high-water queries.
        if (!gated && /^\s*SELECT\s+sequence,\s+event_id,/.test(query)) {
          gated = true;
          return Deferred.succeed(readStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseRead)),
            Effect.as(statement),
          );
        }
        return Effect.succeed(statement);
      };
      const subscription = Effect.gen(function* () {
        const stream =
          scope === "thread"
            ? yield* subscribeOrchestrationV2Thread({ threadId, afterSequence: first!.sequence })
            : yield* subscribeOrchestrationV2Shell({ afterSequence: first!.sequence });
        return stream;
      }).pipe(
        Effect.provideService(Statement.CurrentTransformer, gateReplayRead),
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ensureLegacyTranscript: () => Effect.void,
          getThreadSnapshot: () => Effect.die("snapshot"),
          listActiveShellIndex: () => Effect.die("snapshot"),
          readShellSnapshot: () => Effect.die("snapshot"),
          listActiveSubagentThreadIds: () => Effect.succeed([]),
        } as unknown as ThreadManagement.ThreadManagementService["Service"]),
        Effect.provideService(ProjectStore.ProjectStoreV2, {
          listShells: () => Effect.succeed([]),
        } as unknown as ProjectStore.ProjectStoreV2["Service"]),
        Effect.provideService(
          ProjectService.ProjectService,
          {} as ProjectService.ProjectService["Service"],
        ),
        Effect.provideService(ProjectEnrichment.ProjectEnrichmentService, {
          subscribeChanges: PubSub.subscribe(yield* PubSub.unbounded()),
        } as ProjectEnrichment.ProjectEnrichmentService["Service"]),
        Effect.exit,
        Effect.forkChild,
      );
      const fiber = yield* subscription;
      yield* Deferred.await(readStarted);
      // A pass publishes its boundary, then deletes the rows it covers.
      yield* sql`
        INSERT INTO orchestration_v2_projection_metadata (
          projection_name, schema_version, last_sequence, updated_at
        ) VALUES (${OrchestrationEventStore.EVENT_LOG_PRUNED_THROUGH}, 2, ${second!.sequence}, ${now})
      `;
      yield* sql`DELETE FROM orchestration_events WHERE sequence = ${second!.sequence}`;
      yield* Deferred.succeed(releaseRead, undefined);
      const exit = yield* Fiber.join(fiber);
      assert.deepEqual(Exit.isFailure(exit) ? Cause.squash(exit.cause) : exit, "snapshot");
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

// A thread idle since before the boundary missed nothing; resuming must not resend its body.
it.effect("thread resume at its own high-water below the pruned boundary skips the snapshot", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const now = "2026-10-13T00:00:00.000Z";
    const threadId = ThreadId.make("thread:idle");
    const [last] = yield* sql<{ sequence: number }>`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, actor_kind, payload_json, metadata_json, application_event_version
      ) VALUES (
        'idle:0', 'thread', ${threadId}, 0, 'provider-session.detached',
        ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:idle", detachedAt: now })},
        '{}', 2
      ) RETURNING sequence
    `;
    yield* sql`
      INSERT INTO orchestration_v2_projection_metadata (
        projection_name, schema_version, last_sequence, updated_at
      ) VALUES (${OrchestrationEventStore.EVENT_LOG_PRUNED_THROUGH}, 2, ${last!.sequence + 10}, ${now})
    `;
    const exit = yield* subscribeOrchestrationV2Thread({
      threadId,
      afterSequence: last!.sequence,
    }).pipe(
      Effect.provideService(ThreadManagement.ThreadManagementService, {
        ensureLegacyTranscript: () => Effect.void,
        getThreadSnapshot: () => Effect.die("snapshot"),
        streamStoredEventsFrom: () => Stream.empty,
      } as unknown as ThreadManagement.ThreadManagementService["Service"]),
      Effect.exit,
    );
    // A snapshot load dies with "snapshot", so success means the resume replayed nothing.
    assert.isTrue(Exit.isSuccess(exit));
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect(
  "previewOpen freezes the production render-host selection before the held mirror runs",
  () =>
    Effect.gen(function* () {
      const companionServices = yield* Layer.build(
        JonesHttp.previewCompanionServicesLayer.pipe(
          Layer.provide(DesktopBrowserChannel.layer),
          Layer.provide(
            Layer.mock(ServerEnvironment.ServerEnvironment)({
              getEnvironmentId: Effect.succeed(EnvironmentId.make("open-binding-test")),
            }),
          ),
        ),
      );
      const selection = Context.get(companionServices, RenderHostSelection.RenderHostSelection);
      const placement = Context.get(companionServices, RenderPlacement.RenderPlacement);
      const registry = Context.get(companionServices, CompanionHostRegistry.CompanionHostRegistry);
      const managerContext = yield* Layer.build(PreviewManager.layer);
      const manager = Context.get(managerContext, PreviewManager.PreviewManager);
      const services = Layer.mergeAll(
        layerUnusedConfigRpcServices,
        Layer.succeedContext(companionServices),
        Layer.succeedContext(managerContext),
        Layer.mock(ExternalLauncher.ExternalLauncher)({}),
        Layer.mock(RemoteOpenTargets.RemoteOpenTargets)({}),
        Layer.mock(DirectEndpoints.DirectEndpoints)({}),
        Layer.mock(ProviderRegistry.ProviderRegistry)({}),
        Layer.mock(UsageLimitSources.UsageLimitSources)({}),
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
          namedProjectsRoot: process.cwd(),
        }),
        Layer.mock(ServerEnvironment.ServerEnvironment)({}),
        Layer.mock(EnvironmentAuth.EnvironmentAuth)({}),
      );
      const handlers = yield* Effect.all({
        open: WsRpcGroup.accessHandler(WS_METHODS.previewOpen),
        close: WsRpcGroup.accessHandler(WS_METHODS.previewClose),
      }).pipe(
        Effect.provide(
          layerWsRpc(
            {
              sessionId: AuthSessionId.make("open-binding-test"),
              subject: "open-binding-test",
              method: "browser-session-cookie",
              scopes: [],
            },
            {},
            yield* ServerBrowser.ServerBrowser,
            () => Effect.void,
            { secretReferences: true },
          ).pipe(Layer.provide(services)),
        ),
      );
      const options = {
        client: new Rpc.ServerClient(0),
        requestId: RpcMessage.RequestId("open-binding-test"),
        headers: Headers.empty,
      };
      const open = (input: PreviewOpenInput) =>
        handlers.open(input, options).pipe(
          Effect.filterOrElse(
            (result): result is PreviewSessionSnapshot => !Deferred.isDeferred(result),
            (result) => Deferred.await(result),
          ),
        );
      // Subscribe without consuming: placement cannot race ahead and hide a missing WS hook.
      const mirror = yield* manager.subscribeEvents;
      const threadId = ThreadId.make("thread");
      const first = yield* open({ threadId, runtime: "server" });
      assert.deepEqual(yield* selection.binding(first), { _tag: "server" });
      yield* selection.setDefault({ _tag: "companion", hostId: "mini" });
      const firstEvent = yield* PubSub.take(mirror);
      assert.equal(firstEvent.type, "opened");
      assert.equal(firstEvent.tabId, first.tabId);
      if (firstEvent.type !== "opened") return yield* Effect.die("Missing opened event");
      assert.deepEqual(yield* placement.place(firstEvent.snapshot), { _tag: "headless" });

      const second = yield* open({ threadId, runtime: "server" });
      assert.deepEqual(yield* selection.binding(second), { _tag: "companion", hostId: "mini" });
      yield* PubSub.take(mirror);

      // A real companion handshake followed by disconnect leaves a known offline host.
      const input = yield* Queue.unbounded<ReadonlyArray<string>>();
      const output = yield* Queue.unbounded<CompanionDown>();
      const connection = yield* registry
        .connect({
          read: Queue.take(input).pipe(
            Effect.mapError(
              (cause) => new CompanionHostRegistry.CompanionTransportError({ cause }),
            ),
          ),
          write: (text) =>
            Queue.offer(output, JSON.parse(text) as CompanionDown).pipe(Effect.asVoid),
          close: () => Effect.void,
          writableLength: () => 0,
        })
        .pipe(Effect.scoped, Effect.forkScoped);
      yield* Queue.offer(input, [
        JSON.stringify({
          type: "hello",
          protocol: 1,
          hostId: "mini",
          label: "Mini",
          platform: "test",
          runtimeIdentity: {
            schemaVersion: 1,
            runtimeKind: "electron",
            runtimeInstanceId: "synthetic-mini",
            appVersion: "test",
            buildCommit: null,
          },
          capabilities: {
            cdp: true,
            clipboardText: true,
            uploads: false,
            downloads: false,
            recording: false,
          },
        }),
      ]);
      assert.equal((yield* Queue.take(output)).type, "welcome");
      assert.equal((yield* Queue.take(output)).type, "assignments");
      yield* Fiber.interrupt(connection);
      assert.isFalse((yield* registry.status("mini"))?.online);
      yield* selection.setDefault({ _tag: "server" });
      const unavailable = yield* placement.place(second).pipe(Effect.flip);
      assert.equal(unavailable._tag, "CompanionHostUnavailable");
      assert.equal(unavailable.hostId, "mini");
      assert.equal(unavailable.state, "offline");
      assert.equal(unavailable.outcome, "not_started");

      yield* handlers.close({ threadId, tabId: second.tabId }, options);
      const closed = yield* PubSub.take(mirror);
      assert.equal(closed.type, "closed");
      assert.equal(closed.tabId, second.tabId);
      // Resume the close consumer at the same placement-release seam used by ServerBrowser.
      yield* placement.release(closed);
      assert.isUndefined(yield* selection.binding(second));
      assert.equal((yield* manager.list({ threadId })).sessions.length, 1);
    }).pipe(Effect.scoped, Effect.provide(layerConfigDiscoveryTest)),
);

it.effect("resends a file manager reveal kind that missed the snapshot", () =>
  Effect.gen(function* () {
    const events = yield* withLateConfigDiscovery(
      snapshotConfig({ availableEditors: ["file-manager"] }),
      Stream.empty,
      {
        resolveAvailableEditors: () => Effect.succeed(["file-manager"]),
        resolveFileManagerRevealKind: () => Effect.succeed("file-explorer"),
      },
      { resolveTargets: () => Effect.succeed([]) },
      { resolve: () => Effect.succeed([]) },
    ).pipe(Stream.runCollect);

    const [late] = Array.from(events);
    assert.equal(events.length, 1);
    assert.equal(late?.type, "snapshot");
    if (late?.type === "snapshot") {
      assert.equal(late.config.shellRevealInFileManagerKind, "file-explorer");
    }
  }),
);

it.effect.each(["none", ...discoveryNames, "all"] as const)(
  "starts all config discoveries before release and preserves fields with %s timing out",
  (timeout) =>
    Effect.gen(function* () {
      // endpoints denotes the blocking local/cache read; network discovery has its own lifetime.
      const gates = yield* Effect.forEach(["editors", "targets", "cache", "network"] as const, () =>
        Effect.all({
          started: Deferred.make<void>(),
          release: Deferred.make<void>(),
          completed: Deferred.make<void>(),
          interrupted: Deferred.make<void>(),
        }),
      );
      const calls = [0, 0, 0, 0];
      const waitForLocalTimeout = timeout !== "none";
      const fallbackRemoteTargets =
        timeout === "targets" || timeout === "all" ? [] : discoveryTargets;
      const discover = <A>(index: number, value: A) =>
        Effect.sync(() => {
          calls[index] = calls[index]! + 1;
        }).pipe(
          Effect.andThen(Deferred.succeed(gates[index]!.started, undefined)),
          Effect.andThen(Deferred.await(gates[index]!.release)),
          Effect.andThen(Deferred.succeed(gates[index]!.completed, undefined)),
          Effect.as(value),
          Effect.onInterrupt(() => Deferred.succeed(gates[index]!.interrupted, undefined)),
        );
      const networkEndpoints = [
        ...discoveryEndpoints,
        { kind: "tailnet" as const, httpBaseUrl: "http://100.77.1.9:3773/" },
      ];
      const rpc = yield* makeConfigDiscoveryRpc({
        editors: discover(0, ["file-manager"] as const),
        targets: discover(1, discoveryTargets),
        available: discover(2, discoveryEndpoints),
        endpoints: discover(3, networkEndpoints),
      });
      yield* Deferred.await(gates[0]!.started);
      yield* TestClock.adjust("0 millis");
      for (const gate of gates) assert.isTrue(yield* Deferred.isDone(gate.started));
      assert.deepEqual(calls, [1, 1, 1, 1]);
      yield* TestClock.adjust("4 seconds");
      if (timeout !== "targets" && timeout !== "all") {
        // Complete remote work before allowing the local snapshot to race its completed-result poll.
        yield* Deferred.succeed(gates[1]!.release, undefined);
        yield* Deferred.await(gates[1]!.completed);
        yield* TestClock.adjust("0 millis");
      }
      for (const index of [2, 0]) {
        if (timeout !== "all" && timeout !== discoveryNames[index])
          yield* Deferred.succeed(gates[index]!.release, undefined);
      }
      if (timeout === "none" || timeout === "targets") {
        yield* TestClock.adjust("0 millis");
        assert.equal(yield* Queue.size(rpc.responses), 1);
        if (timeout === "targets") assert.isFalse(yield* Deferred.isDone(gates[1]!.completed));
        const snapshot = yield* Queue.take(rpc.responses);
        assert.equal(snapshot.type, "snapshot");
        if (snapshot.type !== "snapshot")
          return yield* Effect.die("Expected initial config snapshot");
        assert.deepEqual(snapshot.config.availableEditors, ["file-manager"]);
        assert.equal(snapshot.config.shellRevealInFileManager, true);
        assert.equal(snapshot.config.shellRevealInFileManagerKind, "file-explorer");
        assert.deepEqual(
          snapshot.config.remoteOpenTargets,
          timeout === "none" ? discoveryTargets : [],
        );
        assert.deepEqual(snapshot.config.directEndpoints, discoveryEndpoints);
        assert.deepEqual(calls, [1, 1, 1, 1]);
        // Network results may finish while the initial snapshot is waiting for its exact ACK.
        yield* Deferred.succeed(gates[3]!.release, undefined);
        yield* Deferred.await(gates[3]!.completed);
        yield* TestClock.adjust("0 millis");
        assert.equal(yield* Queue.size(rpc.responses), 0);
        yield* rpc.acknowledge;
        const direct = yield* Queue.take(rpc.responses);
        assert.equal(direct.type, "snapshot");
        if (direct.type !== "snapshot")
          return yield* Effect.die("Expected late direct config snapshot");
        assert.deepEqual(direct.config.availableEditors, ["file-manager"]);
        assert.equal(direct.config.shellRevealInFileManager, true);
        assert.equal(direct.config.shellRevealInFileManagerKind, "file-explorer");
        assert.deepEqual(
          direct.config.remoteOpenTargets,
          timeout === "none" ? discoveryTargets : [],
        );
        assert.deepEqual(direct.config.directEndpoints, networkEndpoints);
        yield* rpc.acknowledge;
        yield* TestClock.adjust("0 millis");
        assert.deepEqual(calls, [2, 1, 1, 1]); // Only the documented editor lookup runs again after ACK.
        if (timeout === "targets") {
          yield* TestClock.adjust("1 second");
          assert.isFalse(yield* Deferred.isDone(gates[1]!.completed));
          assert.isFalse(yield* Deferred.isDone(gates[1]!.interrupted));
          assert.equal(calls[1], 1);
          assert.equal(yield* Queue.size(rpc.responses), 0);
          yield* TestClock.adjust("1 second");
          yield* Deferred.succeed(gates[1]!.release, undefined);
          yield* Deferred.await(gates[1]!.completed);
          const remote = yield* Queue.take(rpc.responses);
          assert.equal(remote.type, "snapshot");
          if (remote.type !== "snapshot")
            return yield* Effect.die("Expected late remote config snapshot");
          assert.deepEqual(remote.config.availableEditors, ["file-manager"]);
          assert.equal(remote.config.shellRevealInFileManager, true);
          assert.equal(remote.config.shellRevealInFileManagerKind, "file-explorer");
          assert.deepEqual(remote.config.remoteOpenTargets, discoveryTargets);
          assert.deepEqual(remote.config.directEndpoints, networkEndpoints);
          yield* rpc.acknowledge;
          assert.deepEqual(calls, [2, 1, 1, 1]);
        }
        yield* rpc.interrupt;
        return;
      }
      if (waitForLocalTimeout) {
        yield* TestClock.adjust("999 millis");
        assert.equal(yield* Queue.size(rpc.responses), 0);
        yield* TestClock.adjust("1 millis");
      }
      const snapshot = yield* Queue.take(rpc.responses);
      assert.equal(snapshot.type, "snapshot");
      if (snapshot.type !== "snapshot") return yield* Effect.die("Expected config snapshot");
      const editorsTimedOut = timeout === "editors" || timeout === "all";
      assert.deepEqual(snapshot.config.availableEditors, editorsTimedOut ? [] : ["file-manager"]);
      assert.equal(snapshot.config.shellRevealInFileManager, editorsTimedOut ? undefined : true);
      assert.equal(
        snapshot.config.shellRevealInFileManagerKind,
        editorsTimedOut ? undefined : "file-explorer",
      );
      assert.deepEqual(snapshot.config.remoteOpenTargets, fallbackRemoteTargets);
      assert.deepEqual(
        snapshot.config.directEndpoints,
        timeout === "endpoints" || timeout === "all" ? [] : discoveryEndpoints,
      );
      yield* rpc.interrupt;
    }).pipe(Effect.provide(layerConfigDiscoveryTest), Effect.scoped),
);

it.effect(
  "interrupting config discovery cancels all three probes without publishing a snapshot",
  () =>
    Effect.gen(function* () {
      const probes = yield* Effect.forEach([...discoveryNames, "direct-network"], () =>
        Effect.all({ started: Deferred.make<void>(), interrupted: Deferred.make<void>() }),
      );
      const discover = (index: number) =>
        Deferred.succeed(probes[index]!.started, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() => Deferred.succeed(probes[index]!.interrupted, undefined)),
        );
      const rpc = yield* makeConfigDiscoveryRpc({
        editors: discover(0),
        targets: discover(1),
        available: discover(2),
        endpoints: discover(3),
      });
      yield* Deferred.await(probes[0]!.started);
      yield* TestClock.adjust("0 millis");
      for (const probe of probes) assert.isTrue(yield* Deferred.isDone(probe.started));
      yield* rpc.interrupt;
      yield* TestClock.adjust("0 millis");
      for (const probe of probes) assert.isTrue(yield* Deferred.isDone(probe.interrupted));
      assert.equal(yield* Queue.size(rpc.responses), 0);
    }).pipe(Effect.provide(layerConfigDiscoveryTest), Effect.scoped),
);

it.effect(
  "preserves settings during discovery and keybindings during snapshot ACK at the config RPC boundary",
  () =>
    Effect.gen(function* () {
      const keybindings = yield* Keybindings.Keybindings;
      const settings = yield* ServerSettings.ServerSettingsService;
      const keybindingsSubscribed = yield* Deferred.make<void>();
      const settingsSubscribed = yield* Deferred.make<void>();
      const keybindingsChanges = keybindings.subscribeChanges.pipe(
        Effect.tap(() => Deferred.succeed(keybindingsSubscribed, undefined)),
      );
      const settingsChanges = settings.subscribeChanges.pipe(
        Effect.tap(() => Deferred.succeed(settingsSubscribed, undefined)),
      );
      const initialKeybindings = (yield* keybindings.getSnapshot).keybindings;
      const discoveryStarted = yield* Deferred.make<void>();
      const finishDiscovery = yield* Deferred.make<void>();
      const responses = yield* Queue.unbounded<ServerConfigStreamEvent>();
      const configRpcServices = Layer.mergeAll(
        layerUnusedConfigRpcServices,
        Layer.mock(ExternalLauncher.ExternalLauncher)({
          resolveAvailableEditors: () =>
            Deferred.succeed(discoveryStarted, undefined).pipe(
              Effect.andThen(Deferred.await(finishDiscovery)),
              Effect.as([]),
            ),
          // @effect-diagnostics-next-line effectSucceedWithVoid:off - This optional result requires undefined, not the wider void type.
          resolveFileManagerRevealKind: () => Effect.succeed(undefined),
        }),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
          streamChanges: Stream.never,
        }),
        Layer.mock(UsageLimitSources.UsageLimitSources)({ streamChanges: Stream.make([]) }),
        Layer.mock(RemoteOpenTargets.RemoteOpenTargets)({
          resolveTargets: () => Effect.succeed([]),
        }),
        Layer.mock(DirectEndpoints.DirectEndpoints)({
          resolve: () => Effect.succeed([]),
          resolveAvailable: () => Effect.succeed([]),
        }),
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
          scratchRoot: Effect.succeedNone,
          namedProjectsRoot: process.cwd(),
        }),
        Layer.mock(ServerEnvironment.ServerEnvironment)({
          getDescriptor: Effect.succeed({
            environmentId: EnvironmentId.make("config-test"),
            label: "Config test",
            platform: { os: "linux", arch: "x64" },
            serverVersion: "0.0.0",
            capabilities: { repositoryIdentity: false },
          }),
        }),
        Layer.mock(EnvironmentAuth.EnvironmentAuth)({
          getDescriptor: () =>
            Effect.succeed({
              policy: "loopback-browser",
              bootstrapMethods: [],
              sessionMethods: ["browser-session-cookie"],
              sessionCookieName: "config-test-session",
            }),
        }),
        // Signal subscription on either accessor so the test also fails, rather
        // than hangs, against a handler that subscribes after the snapshot.
        Layer.succeed(Keybindings.Keybindings, {
          ...keybindings,
          streamChanges: Stream.unwrap(keybindingsChanges),
          subscribeChanges: keybindingsChanges,
        }),
        Layer.succeed(ServerSettings.ServerSettingsService, {
          ...settings,
          streamChanges: Stream.unwrap(settingsChanges),
          subscribeChanges: settingsChanges,
        }),
      );
      const server = yield* RpcServer.makeNoSerialization(
        RpcGroup.make(WsSubscribeServerConfigRpc),
        {
          ...WS_RPC_SERVER_OPTIONS,
          onFromServer: (response) =>
            response._tag === "Chunk"
              ? Effect.forEach(response.values, (event) => Queue.offer(responses, event), {
                  discard: true,
                })
              : Effect.void,
        },
      ).pipe(
        Effect.provide(
          layerWsRpc(
            {
              sessionId: AuthSessionId.make("config-test"),
              subject: "config-test",
              method: "browser-session-cookie",
              scopes: [],
            },
            {},
            yield* ServerBrowser.ServerBrowser,
            () => Effect.void,
            { secretReferences: true },
            () => Effect.succeed({}),
          ).pipe(Layer.provide(configRpcServices)),
        ),
      );
      const requestId = RpcMessage.RequestId("config");
      const acknowledge = server.write(0, { _tag: "Ack", requestId });
      yield* server.write(0, {
        _tag: "Request",
        id: requestId,
        tag: WS_METHODS.subscribeServerConfig,
        payload: {},
        headers: Headers.empty,
      });
      yield* Deferred.await(discoveryStarted);

      const duringDiscovery = yield* settings.updateSettings({ defaultAutoPull: true });
      yield* Deferred.succeed(finishDiscovery, undefined);
      const snapshot = yield* Queue.take(responses);
      assert.equal(snapshot.type, "snapshot");
      if (snapshot.type !== "snapshot") return yield* Effect.die("Missing initial snapshot");
      assert.isFalse(snapshot.config.settings.defaultAutoPull);
      assert.deepEqual(snapshot.config.keybindings, initialKeybindings);
      const duringAck = yield* keybindings.upsertKeybindingRule({
        key: "mod+alt+y",
        command: "terminal.toggle",
      });
      yield* acknowledge;
      yield* Deferred.await(keybindingsSubscribed);
      yield* Deferred.await(settingsSubscribed);

      // Later updates expose a dropped buffered event as a value mismatch, not a timeout.
      const latestSettings = yield* settings.updateSettings({ enableProviderUpdateChecks: false });
      const latestKeybindings = yield* keybindings.upsertKeybindingRule({
        key: "mod+alt+z",
        command: "terminal.split",
      });
      const expectedSettings = [duringDiscovery, latestSettings].map(
        ServerSettings.redactServerSettingsForClient,
      );
      const expectedKeybindings = [duringAck.keybindings, latestKeybindings.keybindings];
      // The two sources can interleave, but each must retain its own publication order.
      for (let i = 0; i < 4; i++) {
        const event = yield* Queue.take(responses);
        if (event.type === "settingsUpdated") {
          assert.deepEqual(event.payload.settings, expectedSettings.shift());
        } else {
          assert.equal(event.type, "keybindingsUpdated");
          if (event.type !== "keybindingsUpdated")
            return yield* Effect.die("Unexpected config event");
          assert.deepEqual(event.payload.keybindings, expectedKeybindings.shift());
          assert.deepEqual(event.payload.issues, []);
        }
        yield* acknowledge;
      }
      assert.isEmpty(expectedSettings);
      assert.isEmpty(expectedKeybindings);
      yield* server.write(0, { _tag: "Interrupt", requestId, interruptors: [] });
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.fresh(layerConfigSources),
          Layer.mock(PreviewAutomationBroker.PreviewAutomationBroker)({}),
          Layer.mock(ServerBrowser.ServerBrowser)({}),
        ),
      ),
      Effect.scoped,
    ),
);
