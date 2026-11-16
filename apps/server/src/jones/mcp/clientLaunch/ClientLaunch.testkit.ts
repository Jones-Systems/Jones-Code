import * as ThreadMetadataReads from "../../orchestration/threadMetadataReads.ts";
import * as WorktreeSetupTracker from "../../../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../../../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../../../terminal/Manager.ts";
import { vi } from "@effect/vitest";
import {
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ThreadId,
  type ServerProvider,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as GitDriver from "../../../vcs/GitVcsDriver.ts";
import * as ServerConfig from "../../../config.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as GitWorkflow from "../../../git/GitWorkflowService.ts";
import * as SqlitePersistence from "../../../persistence/Sqlite.ts";
import * as ProjectStore from "../../../orchestration-v2/ProjectStore.ts";
import * as ProjectService from "../../../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../../../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../../../project/ManagedProjectFolders.ts";
import * as ProviderRegistryMock from "../../../provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as TextGeneration from "../../../textGeneration/TextGeneration.ts";
import { CodexProviderCapabilitiesV2 } from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../../../orchestration-v2/EffectOutbox.ts";
import * as EventStore from "../../../orchestration-v2/EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "../../../orchestration-v2/ThreadTitleRegenerationService.ts";
import * as ProviderReplayHarness from "../../../orchestration-v2/testkit/ProviderReplayHarness.ts";

const projectId = ProjectId.make("project:launch-test");
const otherProjectId = ProjectId.make("project:launch-other");
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;
const defaultProject = {
  id: projectId,
  title: "Project",
  workspaceRoot: "/repo",
  repositoryIdentity: null,
  faviconPath: null,
  defaultModelSelection: modelSelection,
  defaultThreadEnvMode: null,
  scripts: [],
  createdAt: "2026-06-20T00:00:00.000Z",
  updatedAt: "2026-06-20T00:00:00.000Z",
  deletedAt: null,
} as const;

const otherProject = {
  ...defaultProject,
  id: otherProjectId,
  title: "Other",
} as const;

const adapter = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("provider execution is disabled in launch tests"),
} as ProviderAdapterV2Shape;

interface ClientLaunchHarnessOptions {
  readonly gitDriver?: GitDriver.GitVcsDriver["Service"];
  readonly projectRecord?: import("@t3tools/contracts").Project;
  readonly readProject?: ProjectService.ProjectService["Service"]["getById"];
  readonly adapters?: ReadonlyArray<ProviderAdapterV2Shape>;
  readonly serverConfigLayer?: Layer.Layer<ServerConfig.ServerConfig>;
  readonly afterCreate?: (threadId: ThreadId) => Effect.Effect<void>;
  readonly managedFolders?: Layer.Layer<ManagedProjectFolders.ManagedProjectFolders>;
  readonly createWorktree?: GitWorkflow.GitWorkflowService["Service"]["createWorktree"];
  readonly remoteExists?: GitWorkflow.GitWorkflowService["Service"]["remoteExists"];
  readonly fetchRemote?: GitWorkflow.GitWorkflowService["Service"]["fetchRemote"];
  readonly listRefs?: GitWorkflow.GitWorkflowService["Service"]["listRefs"];
  readonly resolveRemoteTrackingCommitIfExists?: GitWorkflow.GitWorkflowService["Service"]["resolveRemoteTrackingCommitIfExists"];
  readonly renameBranch?: GitWorkflow.GitWorkflowService["Service"]["renameBranch"];
  readonly runSetup?: ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"];
  readonly generateTitle?: TextGeneration.TextGeneration["Service"]["generateThreadTitle"];
  readonly generateBranchName?: TextGeneration.TextGeneration["Service"]["generateBranchName"];
  readonly serverSettings?: Parameters<typeof ServerSettings.layerTest>[0];
  readonly providers?: ReadonlyArray<ServerProvider>;
}

export function makeClientLaunchHarness(options: ClientLaunchHarnessOptions = {}) {
  const project = options.projectRecord ?? defaultProject;
  const projectId = project.id;
  const database = SqlitePersistence.layerMemory;
  const registry = ProviderAdapterRegistry.layerFromAdapters(options.adapters ?? [adapter]);
  const orchestrator = ProviderReplayHarness.layerWithRegistry(
    { name: "thread-launch" },
    registry,
    {
      databaseLayer: database,
      runEffectWorker: false,
      ...(options.serverConfigLayer === undefined
        ? {}
        : { serverConfigLayer: options.serverConfigLayer }),
    },
  );
  const receipts = CommandReceiptStore.layer.pipe(Layer.provide(database));
  const serverSettings = ServerSettings.layerTest(options.serverSettings);
  const originalThreadManagement = ThreadManagement.layer.pipe(
    Layer.provide(orchestrator),
    Layer.provide(receipts),
    Layer.provide(serverSettings),
  );
  const threadManagement = Layer.effect(
    ThreadManagement.ThreadManagementService,
    Effect.map(ThreadManagement.ThreadManagementService, (service) =>
      ThreadManagement.ThreadManagementService.of({
        ...service,
        dispatch: (command) =>
          service
            .dispatch(command)
            .pipe(
              Effect.tap(() =>
                command.type === "thread.create"
                  ? (options.afterCreate?.(command.threadId) ?? Effect.void)
                  : Effect.void,
              ),
            ),
      }),
    ),
  ).pipe(Layer.provide(originalThreadManagement));
  const outbox = EffectOutbox.layer.pipe(Layer.provide(database));
  const createWorktree = vi.fn(
    options.createWorktree ??
      ((input) =>
        Effect.succeed({
          worktree: { path: "/repo-worktrees/feature", refName: input.newRefName, headSha: "abc" },
        } as never)),
  );
  const renameBranch = vi.fn(
    options.renameBranch ?? ((input) => Effect.succeed({ branch: input.newBranch })),
  );
  const removeWorktree = vi.fn(
    (_input: Parameters<GitWorkflow.GitWorkflowService["Service"]["removeWorktree"]>[0]) =>
      Effect.void,
  );
  const runSetup = vi.fn(
    options.runSetup ?? (() => Effect.succeed({ status: "no-script" as const })),
  );
  const generateBranchName = vi.fn(
    options.generateBranchName ?? (() => Effect.succeed({ branch: "generated-branch" })),
  );
  const generateThreadTitle = vi.fn(
    options.generateTitle ?? (() => Effect.succeed({ title: "Generated title" })),
  );
  const externalServices = Layer.mergeAll(
    WorktreeSetupTracker.layer,
    NodeServices.layer,
    options.gitDriver === undefined
      ? Layer.mock(GitDriver.GitVcsDriver)({})
      : Layer.succeed(GitDriver.GitVcsDriver, options.gitDriver),
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
    Layer.succeed(ProjectService.ProjectService, {
      create: () => Effect.die("unused"),
      bootstrap: () => Effect.die("unused"),
      update: () => Effect.die("unused"),
      delete: () => Effect.die("unused"),
      listPage: () => Effect.die("unused"),
      getById:
        options.readProject ??
        ((id) =>
          Effect.succeed(
            id === projectId
              ? Option.some(project)
              : id === otherProjectId
                ? Option.some(otherProject)
                : Option.none(),
          )),
      getByWorkspaceRoot: () => Effect.succeedSome(project),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree,
      resolveWorktreeBaseRemote: () => Effect.succeed({ remoteName: "origin", branchName: "main" }),
      fetchWorktreeBaseCommit: () =>
        Effect.succeed({ commitSha: "remote-main-sha", remoteRefName: "origin/main" }),
      hasCommit: () => Effect.succeed(false),
      renameBranch,
      fetchRemote: options.fetchRemote ?? (() => Effect.void),
      listRefs:
        options.listRefs ??
        (() =>
          Effect.succeed({
            refs: [],
            isRepo: true,
            hasPrimaryRemote: true,
            nextCursor: null,
            totalCount: 0,
          })),
      resolveRemoteTrackingCommitIfExists:
        options.resolveRemoteTrackingCommitIfExists ??
        (() => Effect.succeed({ commitSha: "remote-main-sha", remoteRefName: "origin/main" })),
      remoteExists: options.remoteExists ?? (() => Effect.succeed(true)),
      remoteBranchExists: () => Effect.die("The origin base must use a single commit lookup"),
      removeWorktree,
      resolveRemoteTrackingCommit: () =>
        Effect.die("The origin base must use a single commit lookup"),
    }),
    Layer.succeed(ProjectSetupScriptRunner.ProjectSetupScriptRunner, {
      runForThread: runSetup,
    }),
    Layer.mock(TextGeneration.TextGeneration)({
      generateThreadTitle,
      generateBranchName,
    }),
    serverSettings,
    ProviderRegistryMock.layer(options.providers),
    options.managedFolders ??
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        isScratchProject: () => Effect.succeed(false),
        folderForThread: () => Effect.succeedNone,
      }),
  );
  const launch = ThreadLaunch.layer.pipe(
    Layer.provide(ThreadMetadataReads.layer.pipe(Layer.provide(database))),
    Layer.provide(
      Layer.mergeAll(
        externalServices,
        threadManagement,
        receipts,
        IdAllocator.layer,
        outbox,
        orchestrator,
        EventStore.layer.pipe(Layer.provide(database)),
      ),
    ),
  );
  const projectedProjects = Layer.mock(ProjectStore.ProjectStoreV2)({
    get: (requestedProjectId) =>
      Effect.succeed(
        requestedProjectId === projectId
          ? Option.some({
              projectId,
              title: project.title,
              workspaceRoot: project.workspaceRoot,
              defaultModelSelection: project.defaultModelSelection,
              defaultThreadEnvMode: null,
              autoPull: false,
              pinnedAt: null,
              archivedAt: null,
              faviconPath: null,
              projectIcon: null,
              scripts: project.scripts,
              createdAt: project.createdAt,
              updatedAt: project.updatedAt,
              deletedAt: project.deletedAt,
            })
          : Option.none(),
      ),
  });
  const titleRegeneration = ThreadTitleRegeneration.layer.pipe(
    Layer.provide(Layer.mergeAll(threadManagement, projectedProjects, externalServices)),
  );
  return {
    layer: Layer.mergeAll(
      launch,
      receipts,
      EventStore.layer.pipe(Layer.provide(database)),
      orchestrator,
      threadManagement,
      titleRegeneration,
      outbox,
      database,
      externalServices,
    ),
    createWorktree,
    removeWorktree,
    renameBranch,
    generateBranchName,
    generateThreadTitle,
    runSetup,
  };
}
