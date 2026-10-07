import * as GitManager from "../git/GitManager.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import {
  readIssuedGitFetchTerminalFailure,
  type IssuedGitFetchTerminalFailure,
} from "../vcs/GitVcsDriverCore.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as ProjectEnrichmentService from "../project/ProjectEnrichmentService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as ProjectFaviconResolver from "../project/ProjectFaviconResolver.ts";
import * as T3ProjectFileLoader from "../project/T3ProjectFileLoader.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as SourceControlRepositoryService from "../sourceControl/SourceControlRepositoryService.ts";
import * as ProviderInstanceRegistryLive from "../provider/Layers/ProviderInstanceRegistryLive.ts";
import * as PortScanner from "../preview/PortScanner.ts";
import * as NativeTelemetryClient from "../resourceTelemetry/NativeTelemetryClient.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import { terminalOwnerObservationLive } from "./ResourceCleanupService.ts";
import type { ReplayDelegatedPreparationOwners } from "./testkit/ProviderReplayHarness.ts";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as Sink from "effect/Sink";
import { makeGitVcsDriverCore } from "../vcs/GitVcsDriverCore.ts";
// @effect-diagnostics nodeBuiltinImport:off - synthetic material claims need lstat device/inode; the retry fixture uses scope-owned real Git.
import * as NodeFS from "node:fs";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as PtyAdapter from "../terminal/PtyAdapter.ts";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Fiber from "effect/Fiber";
import * as EventSink from "./EventSink.ts";
import { makeCommitTransaction } from "./CommitTransaction.ts";
import type { OrdinaryCheckoutLifetime } from "./OrdinaryCheckoutStore.ts";
import {
  ordinaryCheckoutAdmissionRefV1,
  ordinaryCheckoutLeaseIdentityV1,
} from "./OrdinaryCheckoutOwnership.ts";
import * as SqlError from "effect/unstable/sql/SqlError";
import { makeLegacyPreflight } from "./LegacyBootstrapPreflight.ts";
import * as EventStore from "./EventStore.ts";
import {
  canonicalLegacyPayload,
  legacyPayloadHash,
  legacyBootstrapCreateCommandId,
} from "./LegacyBootstrap.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { nativeWorktreePath } from "../vcs/worktreePath.ts";
import * as ServerConfig from "../config.ts";
import { createPendingAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import * as ThreadMessageIntake from "./ThreadMessageIntake.ts";
import { assert, it, vi } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
  ChatAttachmentId,
  ComposerContextId,
  type ChatAttachment,
  type ProjectScript,
  CommandId,
  QueueDispatchCommand,
  EventId,
  DEFAULT_SERVER_SETTINGS,
  GitCommandError,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  OrchestrationV2ThreadProjectionJson,
  ScheduledTaskId,
  type ServerProvider,
  type VcsRef,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Random from "effect/Random";
import { randomUuidV4 } from "./RandomUuid.ts";
import { buildTemporaryWorktreeBranchName } from "@t3tools/shared/git";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import { makeProviderRegistryLayer } from "../provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as IdAllocator from "./IdAllocator.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "./ThreadLaunchService.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "./ThreadTitleRegenerationService.ts";
import {
  makeReplayServerConfig,
  makeOrchestratorV2ReplayLayerWithRegistry,
} from "./testkit/ProviderReplayHarness.ts";

const projectId = ProjectId.make("project:launch-test");
const otherProjectId = ProjectId.make("project:launch-other");
const encodeThreadProjection = Schema.encodeEffect(OrchestrationV2ThreadProjectionJson);
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;
const project = {
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
  ...project,
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

interface HarnessOptions {
  readonly afterPreparedOutcome?: (
    input: Parameters<OrdinaryCheckoutLifetime["recordPreparedOutcome"]>[0],
  ) => Effect.Effect<void>;
  readonly afterDispatch?: (
    command: Parameters<ThreadManagement.ThreadManagementService["Service"]["dispatch"]>[0],
  ) => Effect.Effect<void>;
  readonly physicalFixture?: boolean;
  readonly worktreesDir?: string;
  readonly resolveCommit?: GitWorkflow.GitWorkflowService["Service"]["resolveCommit"];
  readonly privateDUnavailable?: boolean;
  readonly terminalOwner?: Layer.Layer<TerminalManager.TerminalManager>;
  readonly workspaceRoot?: string;
  readonly otherWorkspaceRoot?: string;
  readonly legacyPreflightGit?: Pick<
    GitWorkflow.GitWorkflowService["Service"],
    "remoteBranchExists" | "resolveRemoteTrackingCommit"
  >;
  readonly projectScripts?: ReadonlyArray<ProjectScript>;
  readonly managedFolders?: Layer.Layer<ManagedProjectFolders.ManagedProjectFolders>;
  readonly createWorktree?: GitWorkflow.GitWorkflowService["Service"]["createWorktree"];
  readonly remoteExists?: GitWorkflow.GitWorkflowService["Service"]["remoteExists"];
  readonly isRepository?: GitWorkflow.GitWorkflowService["Service"]["isRepository"];
  readonly hasCommit?: GitWorkflow.GitWorkflowService["Service"]["hasCommit"];
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

function makeDefaultHarness(options: HarnessOptions = {}) {
  const boundProject = {
    ...project,
    workspaceRoot: options.workspaceRoot ?? project.workspaceRoot,
    scripts: options.projectScripts ?? project.scripts,
  };
  const boundOtherProject =
    options.otherWorkspaceRoot === undefined
      ? otherProject
      : { ...otherProject, workspaceRoot: options.otherWorkspaceRoot };
  const database = SqlitePersistenceMemory;
  const branchesByPath = new Map<string, string>();
  const fixtureExists = (path: string) =>
    path === "/repo" ||
    path === "/repo-worktrees" ||
    branchesByPath.has(path) ||
    (path.startsWith("/repo-worktrees/") && !path.startsWith("/repo-worktrees/repo/"));
  const fixtureFileSystem = Layer.effect(
    FileSystem.FileSystem,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      if (options.physicalFixture === true) return fs;
      return FileSystem.FileSystem.of({
        ...fs,
        realPath: (path) => (path.startsWith("/repo") ? Effect.succeed(path) : fs.realPath(path)),
        exists: (path) =>
          path.startsWith("/repo") ? Effect.succeed(fixtureExists(path)) : fs.exists(path),
      });
    }),
  ).pipe(Layer.provide(NodeServices.layer));
  const fixtureConfig = Layer.effect(
    ServerConfig.ServerConfig,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const config = yield* Effect.acquireRelease(
        makeReplayServerConfig("launch-producer"),
        (value) => fs.remove(value.baseDir, { recursive: true }).pipe(Effect.orDie),
      );
      return { ...config, worktreesDir: options.worktreesDir ?? "/repo-worktrees" };
    }),
  ).pipe(Layer.provide(NodeServices.layer));
  const registry = ProviderAdapterRegistry.makeLayer([adapter]);
  const orchestratorBase = makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "thread-launch" },
    registry,
    {
      databaseLayer: database,
      runEffectWorker: false,
      checkoutFixture: {
        projects: [
          options.physicalFixture === true ? boundProject : project,
          boundOtherProject,
        ].map((value) => ({
          projectId: value.id,
          title: value.title,
          workspaceRoot: value.workspaceRoot,
        })),
        // The native Git and project capabilities in this harness are synthetic.
        // Real filesystem checkout/alias observations have separate admission tests.
        resolvePath: (value) => (options.physicalFixture === true ? undefined : value),
        existsPath: (value) =>
          options.physicalFixture === true
            ? undefined
            : value.startsWith("/repo")
              ? fixtureExists(value)
              : undefined,
        worktreesDir: options.worktreesDir ?? "/repo-worktrees",
      },
    },
  );
  const orchestrator =
    options.terminalOwner === undefined
      ? orchestratorBase
      : orchestratorBase.pipe(Layer.provide(options.terminalOwner));
  const threadManagementBase = ThreadManagement.layer.pipe(Layer.provide(orchestrator));
  const threadManagement =
    options.privateDUnavailable !== true
      ? threadManagementBase
      : Layer.effect(
          ThreadManagement.ThreadManagementService,
          Effect.map(
            ThreadManagement.ThreadManagementService,
            ({ dispatchLegacyGuardRejectionDelete: _private, ...ordinary }) => ordinary,
          ),
        ).pipe(Layer.provide(threadManagementBase));
  const observedThreadManagement =
    options.afterDispatch === undefined
      ? threadManagement
      : Layer.effect(
          ThreadManagement.ThreadManagementService,
          Effect.map(ThreadManagement.ThreadManagementService, (actual) => ({
            ...actual,
            dispatch: (command) =>
              actual.dispatch(command).pipe(Effect.tap(() => options.afterDispatch!(command))),
          })),
        ).pipe(Layer.provide(threadManagement));
  const receipts = CommandReceiptStore.layer.pipe(Layer.provide(database));
  const outbox = EffectOutbox.layer.pipe(Layer.provide(database));
  const createWorktree = vi.fn(
    options.createWorktree ??
      ((input) =>
        Effect.succeed({
          worktree: {
            path: input.path ?? "/repo-worktrees/feature",
            refName: input.newRefName,
            headSha: "abc",
          },
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
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    options.terminalOwner ??
      Layer.mock(TerminalManager.TerminalManager)({
        close: () => Effect.void,
        captureOwnedTargets: ({ threadId, ownerBirth }) =>
          Effect.succeed({
            managerId: "synthetic-terminal-manager",
            threadId,
            ownerBirth,
            status: "captured" as const,
            managedTargetsOnly: true as const,
            targets: [],
          }),
      }),
    Layer.succeed(ProjectService.ProjectService, {
      create: () => Effect.die("unused"),
      bootstrap: () => Effect.die("unused"),
      update: () => Effect.die("unused"),
      delete: () => Effect.die("unused"),
      getById: (id) =>
        Effect.succeed(
          id === projectId
            ? Option.some(boundProject)
            : id === otherProjectId
              ? Option.some(boundOtherProject)
              : Option.none(),
        ),
      getByWorkspaceRoot: () => Effect.succeed(Option.some(boundProject)),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree: (input, options) =>
        createWorktree(input, options).pipe(
          Effect.tap((result) =>
            Effect.sync(() => branchesByPath.set(result.worktree.path, result.worktree.refName)),
          ),
        ),
      renameBranch: (input) =>
        renameBranch(input).pipe(
          Effect.tap((result) => Effect.sync(() => branchesByPath.set(input.cwd, result.branch))),
        ),
      localStatus: ({ cwd }) =>
        Effect.succeed({ isRepo: true, refName: branchesByPath.get(cwd) ?? null } as never),
      resolveCommit: options.resolveCommit ?? (() => Effect.succeed({ commitSha: "abc" })),
      invalidateLocalStatus: () => Effect.void,
      isRepository: options.isRepository ?? (() => Effect.succeed(true)),
      hasCommit: options.hasCommit ?? (() => Effect.succeed(true)),
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
      remoteBranchExists:
        options.legacyPreflightGit?.remoteBranchExists ??
        (() => Effect.die("The origin base must use a single commit lookup")),
      removeWorktree: (input) =>
        removeWorktree(input).pipe(
          Effect.tap(() => Effect.sync(() => branchesByPath.delete(input.path))),
        ),
      resolveRemoteTrackingCommit:
        options.legacyPreflightGit?.resolveRemoteTrackingCommit ??
        (() => Effect.die("The origin base must use a single commit lookup")),
    }),
    Layer.succeed(ProjectSetupScriptRunner.ProjectSetupScriptRunner, {
      runForThread: runSetup,
    }),
    Layer.mock(TextGeneration.TextGeneration)({
      generateThreadTitle,
      generateBranchName,
    }),
    ServerSettings.layerTest(options.serverSettings),
    makeProviderRegistryLayer(options.providers),
    options.managedFolders ??
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        folderForThread: () => Effect.succeed(Option.none()),
      }),
  );
  const producerEventSink =
    options.afterPreparedOutcome === undefined
      ? Layer.empty
      : Layer.effect(
          EventSink.EventSinkV2,
          Effect.map(EventSink.EventSinkV2, (actual) => ({
            ...actual,
            ordinaryCheckoutLifetime: {
              ...actual.ordinaryCheckoutLifetime!,
              recordPreparedOutcome: (input) =>
                actual
                  .ordinaryCheckoutLifetime!.recordPreparedOutcome(input)
                  .pipe(Effect.tap(() => options.afterPreparedOutcome?.(input) ?? Effect.void)),
            },
          })),
        ).pipe(Layer.provide(orchestrator));
  const launch = ThreadLaunch.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        externalServices,
        observedThreadManagement,
        receipts,
        IdAllocator.layer,
        orchestrator,
        fixtureFileSystem,
        fixtureConfig,
        outbox,
        EventStore.layer.pipe(Layer.provide(database)),
        producerEventSink,
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
              workspaceRoot: boundProject.workspaceRoot,
              defaultModelSelection: project.defaultModelSelection,
              defaultThreadEnvMode: null,
              autoPull: false,
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
    Layer.provide(Layer.mergeAll(observedThreadManagement, projectedProjects, externalServices)),
  );
  return {
    layer: Layer.mergeAll(
      launch,
      receipts,
      EventStore.layer.pipe(Layer.provide(database)),
      orchestrator,
      observedThreadManagement,
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

// Task-only source to be embedded in the declared test candidate. Not installed.
interface RealRetryAudit {
  factories: number;
  finalized: number;
  controls: string[];
  acquisitions: Array<{
    sql: SqlClient.SqlClient;
    sink: EventSink.EventSinkV2["Service"];
    management: ThreadManagement.ThreadManagementService["Service"];
    projects: ProjectService.ProjectService["Service"];
    terminals: TerminalManager.TerminalManager["Service"];
    receipts: CommandReceiptStore.CommandReceiptStoreV2["Service"];
    outbox: EffectOutbox.EffectOutboxV2["Service"];
    config: ServerConfig.ServerConfig["Service"];
  }>;
  launchers: ThreadLaunch.ThreadLaunchService["Service"][];
  fetchFailures: Array<{
    request: IssuedGitFetchTerminalFailure["request"];
    terminal: IssuedGitFetchTerminalFailure;
    parentHead: string;
    remoteTrackingHead: string;
  }>;
  unsupportedFetchFailures: number;
  observationFailures: string[];
  ownedLockRemoved: boolean;
}

const acquireRealRetryFixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const paths = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-original-retry-" });
  const canonicalRoot = yield* fs.realPath(root);
  const workspaceRoot = paths.join(canonicalRoot, "project");
  const upstream = paths.join(canonicalRoot, "upstream");
  const origin = paths.join(canonicalRoot, "origin.git");
  const worktreesDir = paths.join(canonicalRoot, "worktrees");
  const process = yield* VcsProcess.make.pipe(Effect.provide(ProcessRunner.layer));
  const git = (cwd: string, args: ReadonlyArray<string>) =>
    process
      .run({
        operation: "ThreadLaunch.originalRetryFixture",
        command: "git",
        cwd,
        args,
      })
      .pipe(Effect.map((result) => result.stdout.trim()));
  yield* fs.makeDirectory(worktreesDir);
  yield* git(canonicalRoot, ["init", "--bare", "--initial-branch=main", origin]);
  yield* git(canonicalRoot, ["init", "--initial-branch=main", upstream]);
  yield* fs.writeFileString(paths.join(upstream, "initial.txt"), "original parent\n");
  yield* git(upstream, ["add", "initial.txt"]);
  yield* git(upstream, [
    "-c",
    "user.name=Original Retry Fixture",
    "-c",
    "user.email=original-retry@example.invalid",
    "commit",
    "-m",
    "original parent",
  ]);
  yield* git(upstream, ["remote", "add", "origin", origin]);
  yield* git(upstream, ["push", "origin", "main"]);
  yield* git(canonicalRoot, ["clone", "--no-hardlinks", origin, workspaceRoot]);
  const parentHead = yield* git(workspaceRoot, ["rev-parse", "HEAD"]);
  const originalTrackingHead = yield* git(workspaceRoot, ["rev-parse", "refs/remotes/origin/main"]);
  if (parentHead !== originalTrackingHead)
    return yield* Effect.die(
      "The original fixture clone did not capture its actual parent commit.",
    );
  yield* fs.writeFileString(paths.join(upstream, "upstream.txt"), "new upstream commit\n");
  yield* git(upstream, ["add", "upstream.txt"]);
  yield* git(upstream, [
    "-c",
    "user.name=Original Retry Fixture",
    "-c",
    "user.email=original-retry@example.invalid",
    "commit",
    "-m",
    "new upstream commit",
  ]);
  const upstreamHead = yield* git(upstream, ["rev-parse", "HEAD"]);
  yield* git(upstream, ["push", "origin", "main"]);
  if (upstreamHead === parentHead)
    return yield* Effect.die("The real fixture upstream has no distinct new commit.");
  const uuid = yield* randomUuidV4.pipe(Random.withSeed(originalRetryFixtureSeed));
  const branch = buildTemporaryWorktreeBranchName(() => uuid.replaceAll("-", ""));
  const expectedWorktreePath = nativeWorktreePath({ worktreesDir, cwd: workspaceRoot, branch });
  // Structural parents only. The planned child remains absent before the real add.
  yield* fs.makeDirectory(paths.dirname(expectedWorktreePath), { recursive: true });
  if (yield* fs.exists(expectedWorktreePath))
    return yield* Effect.die("The independent original fixture child is already present.");
  const rawLockPath = yield* git(workspaceRoot, [
    "rev-parse",
    "--git-path",
    "refs/remotes/origin/main.lock",
  ]);
  const lockPath = paths.resolve(workspaceRoot, rawLockPath);
  const gitDirectory = yield* fs.realPath(paths.join(workspaceRoot, ".git"));
  const relativeLock = paths.relative(gitDirectory, lockPath);
  if (relativeLock !== paths.join("refs", "remotes", "origin", "main.lock"))
    return yield* Effect.die("The original ref lock is outside the owned clone.");
  const lockContent = "owned original-retry ref lock\n";
  yield* fs.writeFileString(lockPath, lockContent, { flag: "wx" });
  const audit: RealRetryAudit = {
    factories: 0,
    finalized: 0,
    controls: [],
    acquisitions: [],
    launchers: [],
    fetchFailures: [],
    unsupportedFetchFailures: 0,
    observationFailures: [],
    ownedLockRemoved: false,
  };
  const removeOwnedLock = Effect.gen(function* () {
    if (audit.ownedLockRemoved) return;
    if ((yield* fs.readFileString(lockPath)) !== lockContent)
      return yield* Effect.die("The exact original fixture ref lock changed ownership.");
    yield* fs.remove(lockPath);
    audit.ownedLockRemoved = true;
  });
  return {
    fs,
    paths,
    workspaceRoot,
    worktreesDir,
    expectedWorktreePath,
    uuid,
    branch,
    parentHead,
    originalTrackingHead,
    upstreamHead,
    lockPath,
    git,
    removeOwnedLock,
    audit,
  };
});
type RealRetryFixture = Effect.Success<typeof acquireRealRetryFixture>;

interface OriginalRetryPreparationObservation {
  readonly observedThreadId?: Parameters<
    WorktreeSetupTracker.WorktreeSetupTracker["Service"]["begin"]
  >[0]["threadId"];
  readonly ready: Deferred.Deferred<Fiber.Fiber<unknown, unknown>>;
  trackerAcquisitions: number;
  beginInvocations: number;
  beginEvaluations: number;
  beginTerminals: number;
  actualTracker: WorktreeSetupTracker.WorktreeSetupTracker["Service"] | undefined;
  forwardedTracker: WorktreeSetupTracker.WorktreeSetupTracker["Service"] | undefined;
  capture:
    | {
        readonly input: Parameters<
          WorktreeSetupTracker.WorktreeSetupTracker["Service"]["begin"]
        >[0];
        readonly executingFiber: Fiber.Fiber<unknown, unknown>;
        readonly originalBeginEffect: Effect.Effect<void>;
      }
    | undefined;
  beginExit: Exit.Exit<void> | undefined;
  fiberExit: Exit.Exit<unknown, unknown> | undefined;
  readonly retryReady: Deferred.Deferred<Fiber.Fiber<unknown, unknown>>;
  retryEntryPhase: "not_entered" | "before_retryPreparation";
  retryAwaitState:
    | "handle_unobserved"
    | "awaiting_handle"
    | "awaiting_fiber_exit"
    | "fiber_exit_observed";
  retryCapture: OriginalRetryPreparationObservation["capture"];
  retryBeginExit: Exit.Exit<void> | undefined;
  retryFiberExit: Exit.Exit<unknown, unknown> | undefined;
}

type RetainedRetrySlot = Effect.Success<
  ReturnType<OrdinaryCheckoutLifetime["readOriginalPreparedRetry"]>
>;
type RetainedRetryQuery = Parameters<OrdinaryCheckoutLifetime["readOriginalPreparedRetry"]>[0];
type OriginalRetryRecord = {
  readonly ref: Parameters<OrdinaryCheckoutLifetime["recordPreparedOutcome"]>[0]["ref"];
  readonly outcome: Parameters<
    OrdinaryCheckoutLifetime["recordPreparedOutcome"]
  >[0]["actualProducerOutcome"];
  readonly associationOrdinal: number;
  readonly query: RetainedRetryQuery;
  readonly admission: NonNullable<
    Effect.Success<ReturnType<OrdinaryCheckoutLifetime["readAdmissionForRun"]>>
  >;
  readonly history: Effect.Success<ReturnType<OrdinaryCheckoutLifetime["readExecutionHistory"]>>;
};
type RetrySlotFacts = {
  readonly paths: ReadonlyArray<object>;
  readonly history: ReadonlyArray<object>;
};
interface RetrySlotTransactionProbe {
  readonly mode: "commit" | "rollback";
  acquisitions: number;
  invocations: number;
  evaluations: number;
  beforeRefusal: boolean;
  afterSlot: RetainedRetrySlot | undefined;
  afterSlotQuery: RetainedRetryQuery | undefined;
  readInvocations: Array<{
    readonly input: RetainedRetryQuery;
    slot: RetainedRetrySlot | undefined;
  }>;
  readSuccesses: Array<{
    readonly input: RetainedRetryQuery;
    readonly slot: RetainedRetrySlot;
  }>;
  originalRecord: OriginalRetryRecord | undefined;
  publicationPhase: "not_entered" | "entered" | "completed";
  publicationRecord: OriginalRetryRecord | undefined;
  rollbackFactsEqual: boolean;
  rollbackCause: Cause.Cause<unknown> | undefined;
  rollbackObservation:
    | {
        readonly operationId: string;
        readonly baseline: RetrySlotFacts | undefined;
        readonly exit: Exit.Exit<RetrySlotFacts, SqlError.SqlError>;
      }
    | undefined;
  serving:
    | {
        readonly sql: SqlClient.SqlClient;
        readonly transactionService: SqlClient.SqlClient["transactionService"];
        readonly sink: EventSink.EventSinkV2["Service"];
        readonly facade: EventSink.EventSinkV2["Service"];
        readonly lifetime: OrdinaryCheckoutLifetime;
        readonly originalReader: OrdinaryCheckoutLifetime["readOriginalPreparedRetry"];
        readonly forwardedReader: OrdinaryCheckoutLifetime["readOriginalPreparedRetry"];
        readonly enclosing: Effect.Success<ReturnType<typeof makeCommitTransaction>>;
      }
    | undefined;
}

type RetainedReaderVariant =
  | "messageId"
  | "projectId"
  | "targetSource.worktreePath"
  | "branch"
  | "multiple released operations";
interface RetainedReaderProbe {
  readonly variant: RetainedReaderVariant;
  readonly preparation: OriginalRetryPreparationObservation;
  invocations: number;
  negativeReads: number;
  mutations: number;
  refusal:
    | Effect.Error<ReturnType<OrdinaryCheckoutLifetime["readOriginalPreparedRetry"]>>
    | undefined;
  changedQuery: RetainedRetryQuery | undefined;
  authenticQuery: RetainedRetryQuery | undefined;
  originalSlot: RetainedRetrySlot | undefined;
  rollback: RetainedReaderRollback | undefined;
  rollbackCause: Cause.Cause<unknown> | undefined;
  restoredFacts: boolean;
}
class RetainedReaderRollback extends Schema.TaggedError<RetainedReaderRollback>()(
  "RetainedReaderRollback",
  { operationId: Schema.String },
) {}

function observeRetainedReaderVariant(
  probe: RetainedReaderProbe,
  transactionProbe: RetrySlotTransactionProbe,
  original: OrdinaryCheckoutLifetime,
  input: RetainedRetryQuery,
  actual: ReturnType<OrdinaryCheckoutLifetime["readOriginalPreparedRetry"]>,
  sql: SqlClient.SqlClient,
  enclosing: Effect.Success<ReturnType<typeof makeCommitTransaction>>,
): ReturnType<OrdinaryCheckoutLifetime["readOriginalPreparedRetry"]> {
  probe.invocations++;
  if (probe.invocations !== 1) return actual;
  return Effect.gen(function* () {
    const record = transactionProbe.originalRecord;
    if (record === undefined || probe.preparation.fiberExit === undefined)
      return yield* Effect.die("The authentic original producer has not ended.");
    assert.equal(transactionProbe.publicationPhase, "completed");
    assert.strictEqual(transactionProbe.publicationRecord, record);
    assert.isTrue(transactionProbe.beforeRefusal);
    assert.deepEqual(input, record.query);
    const operationId = record.ref.originalUse.operationId;
    const readFacts = () =>
      Effect.gen(function* () {
        const rows = yield* sql`
        SELECT operation_id, canonical_path, kind, subject_json, state, started_at,
               outcome_json, recorded_at, updated_at
        FROM orchestration_v2_worktree_path_admissions
        WHERE kind = 'native_operation'
          AND json_extract(subject_json, '$.use.admission.admissionId') = ${record.admission.admissionId}
        ORDER BY operation_id`;
        const history =
          yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations WHERE operation_id = ${operationId} ORDER BY ordinal`;
        const leases =
          yield* sql`SELECT * FROM worktree_ownership_leases WHERE resource_path = ${record.admission.capture.lease.resourcePath}`;
        const runs =
          yield* sql`SELECT * FROM orchestration_v2_projection_runs WHERE run_id = ${input.runId}`;
        const links =
          yield* sql`SELECT link.*, effect.status FROM orchestration_v2_ordinary_checkout_effect_links link JOIN orchestration_v2_effect_outbox effect ON effect.effect_id = link.effect_id WHERE link.admission_id = ${record.admission.admissionId} ORDER BY link.effect_id`;
        return { rows, history, leases, runs, links };
      });
    const baseline = yield* readFacts();
    assert.lengthOf(baseline.rows, 1);
    assert.equal(baseline.rows[0]?.operation_id, operationId);
    assert.equal(baseline.rows[0]?.state, "released");
    assert.isString(baseline.rows[0]?.outcome_json);
    const admission = yield* original.readAdmissionForRun({
      threadId: input.threadId,
      runId: input.runId,
    });
    assert.deepEqual(admission, record.admission);
    const history = yield* original.readExecutionHistory(record.ref.originalUse);
    assert.deepEqual(history, record.history);
    assert.equal(history.participants[0]?.state, "retired");
    assert.deepEqual(history.participants[0]?.ref, record.ref);
    assert.lengthOf(baseline.leases, 1);
    assert.equal(baseline.leases[0]?.owner_thread_id, input.threadId);
    assert.equal(baseline.leases[0]?.lease_id, record.ref.originalUse.lease.leaseId);
    assert.equal(baseline.leases[0]?.branch, input.branch);
    assert.isAbove(
      Number(baseline.leases[0]?.expires_at_ms),
      DateTime.toEpochMillis(yield* DateTime.now),
    );
    const currentRun =
      yield* sql`SELECT json_extract(payload_json, '$.status') AS status, json_extract(payload_json, '$.activeAttemptId') AS attempt, json_extract(payload_json, '$.rootNodeId') AS node, json_extract(payload_json, '$.userMessageId') AS message FROM orchestration_v2_projection_runs WHERE run_id = ${input.runId}`;
    assert.lengthOf(currentRun, 1);
    assert.equal(currentRun[0]?.status, "preparing");
    assert.equal(currentRun[0]?.attempt, record.admission.run?.runAttemptId);
    assert.equal(currentRun[0]?.node, record.admission.run?.nodeId);
    assert.equal(currentRun[0]?.message, input.messageId);
    assert.isFalse(
      baseline.links.some((row) => row.status === "pending" || row.status === "running"),
    );
    probe.authenticQuery = input;
    const requireHistoryRefusal = (
      result: Result.Result<
        RetainedRetrySlot,
        Effect.Error<ReturnType<OrdinaryCheckoutLifetime["readOriginalPreparedRetry"]>>
      >,
    ) => {
      if (Result.isSuccess(result))
        throw new Error("The altered original retry query/history was accepted.");
      assert.equal(result.failure._tag, "OrdinaryCheckoutHistoryError");
      assert.equal(
        result.failure.message,
        "Retry has no retained, completed original preparation outcome.",
      );
      probe.refusal = result.failure;
    };
    if (probe.variant !== "multiple released operations") {
      let changed: RetainedRetryQuery;
      switch (probe.variant) {
        case "messageId":
          changed = { ...input, messageId: MessageId.make("message:reader-negative:unrelated") };
          break;
        case "projectId":
          changed = { ...input, projectId: ProjectId.make("project:reader-negative:unrelated") };
          break;
        case "targetSource.worktreePath":
          changed = {
            ...input,
            targetSource: {
              ...input.targetSource,
              worktreePath: `${input.canonicalCheckoutPath}/reader-negative-unowned`,
            },
          };
          break;
        case "branch":
          changed = { ...input, branch: `${input.branch}-reader-negative-unowned` };
          break;
      }
      probe.changedQuery = changed;
      probe.negativeReads++;
      const negative = original.readOriginalPreparedRetry(changed);
      requireHistoryRefusal(yield* negative.pipe(Effect.result));
      assert.deepEqual(yield* readFacts(), baseline);
      probe.restoredFacts = true;
      const slot = yield* actual;
      probe.originalSlot = slot;
      return slot;
    }
    const slot = yield* actual;
    probe.originalSlot = slot;
    const copiedOperationId = `${operationId}:fixture-owned-invalid-copy`;
    const absent =
      yield* sql`SELECT operation_id FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${copiedOperationId}`;
    assert.lengthOf(absent, 0);
    const rollback = yield* enclosing
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`
        INSERT INTO orchestration_v2_worktree_path_admissions
          (operation_id, canonical_path, kind, subject_json, state, started_at,
           outcome_json, recorded_at, updated_at)
        SELECT ${copiedOperationId}, canonical_path, kind, subject_json, state,
               started_at, outcome_json, recorded_at, updated_at
        FROM orchestration_v2_worktree_path_admissions
        WHERE operation_id = ${operationId} AND state = 'released' AND kind = 'native_operation'
          AND json_extract(subject_json, '$.use.admission.admissionId') = ${record.admission.admissionId}`;
          probe.mutations++;
          const changedFacts = yield* readFacts();
          assert.lengthOf(changedFacts.rows, 2);
          assert.deepEqual(changedFacts.history, baseline.history);
          assert.deepEqual(changedFacts.leases, baseline.leases);
          assert.deepEqual(changedFacts.runs, baseline.runs);
          assert.deepEqual(changedFacts.links, baseline.links);
          probe.negativeReads++;
          const negative = original.readOriginalPreparedRetry(input);
          requireHistoryRefusal(yield* negative.pipe(Effect.result));
          return yield* new RetainedReaderRollback({ operationId: copiedOperationId });
        }),
      )
      .pipe(
        Effect.onError((cause) =>
          Effect.sync(() => {
            probe.rollbackCause = cause;
          }),
        ),
        Effect.catchTag("RetainedReaderRollback", (error) => Effect.succeed(error)),
      );
    assert.equal(rollback.operationId, copiedOperationId);
    assert.isDefined(probe.rollbackCause);
    probe.rollback = rollback;
    assert.deepEqual(yield* readFacts(), baseline);
    assert.lengthOf(
      yield* sql`SELECT operation_id FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${copiedOperationId}`,
      0,
    );
    probe.restoredFacts = true;
    return slot;
  });
}

function makeRealRetryHarness(
  fixture: RealRetryFixture,
  originalPreparation?: OriginalRetryPreparationObservation,
  retrySlotProbe?: RetrySlotTransactionProbe,
  retainedReaderProbe?: RetainedReaderProbe,
) {
  const createWorktree = vi.fn<GitWorkflow.GitWorkflowService["Service"]["createWorktree"]>(() =>
    Effect.die("The actual retry createWorktree has not been acquired."),
  );
  const removeWorktree = vi.fn<GitWorkflow.GitWorkflowService["Service"]["removeWorktree"]>(() =>
    Effect.die("The actual retry removeWorktree has not been acquired."),
  );
  const renameBranch = vi.fn<GitWorkflow.GitWorkflowService["Service"]["renameBranch"]>(() =>
    Effect.die("The actual retry renameBranch has not been acquired."),
  );
  const runSetup = vi.fn<
    ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"]
  >(() => Effect.die("The actual retry setup runner has not been acquired."));
  const generateBranchName = vi.fn<TextGeneration.TextGeneration["Service"]["generateBranchName"]>(
    () => Effect.die("The actual retry text generation has not been acquired."),
  );
  const generateThreadTitle = vi.fn<
    TextGeneration.TextGeneration["Service"]["generateThreadTitle"]
  >(() => Effect.die("The actual retry text generation has not been acquired."));
  const providerRegistryLayer = makeProviderRegistryLayer();
  const factory = (owners: ReplayDelegatedPreparationOwners) => {
    fixture.audit.factories++;
    const unexpectedControl = (operation: string) =>
      Effect.sync(() => {
        fixture.audit.controls.push(operation);
      }).pipe(
        Effect.andThen(Effect.die(new Error(`Unexpected retry virtual control: ${operation}`))),
      );
    const base = Layer.mergeAll(
      owners.persistenceLayer,
      owners.legacyImporterLayer,
      owners.managementLayer,
      owners.configLayer,
      owners.settingsLayer,
      owners.platformLayer,
      ThreadCommandExecutor.layer,
      providerRegistryLayer,
    );
    const vcsProcess = VcsProcess.layer.pipe(Layer.provide(owners.platformLayer));
    const drivers = Layer.merge(GitVcsDriver.layer, VcsDriverRegistry.layer).pipe(
      Layer.provide(vcsProcess),
      Layer.provide(base),
    );
    const sourceControl = Layer.effect(
      SourceControlProviderRegistry.SourceControlProviderRegistry,
      SourceControlProviderRegistry.makeWithProviders([]),
    ).pipe(Layer.provide(Layer.merge(drivers, vcsProcess)), Layer.provide(base));
    const repositoryService = SourceControlRepositoryService.layer.pipe(
      Layer.provide(Layer.merge(drivers, sourceControl)),
      Layer.provide(base),
    );
    const clones = ProjectCloneTracker.layer.pipe(Layer.provide(repositoryService));
    // A real, scope-owned empty instance registry. No provider factory or host auth
    // is installed. Actual generation routes report their genuine unavailable error.
    const instances = ProviderInstanceRegistryLive.ProviderInstanceRegistryMutableLayer<never>({
      drivers: [],
      configMap: {},
    });
    const textGenerationBase = TextGeneration.layer.pipe(
      Layer.provide(Layer.merge(instances, sourceControl)),
    );
    const textGeneration = Layer.effect(
      TextGeneration.TextGeneration,
      Effect.gen(function* () {
        const actual = yield* TextGeneration.TextGeneration;
        generateBranchName.mockImplementation(actual.generateBranchName);
        generateThreadTitle.mockImplementation(actual.generateThreadTitle);
        return TextGeneration.TextGeneration.of({
          ...actual,
          generateBranchName,
          generateThreadTitle,
        });
      }),
    ).pipe(Layer.provide(textGenerationBase));
    const common = Layer.mergeAll(base, clones, textGeneration);
    const workspace = WorkspacePaths.layer.pipe(Layer.provide(owners.platformLayer));
    const metadata = ProjectEnrichmentService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          RepositoryIdentityResolver.layer,
          ProjectFaviconResolver.layer.pipe(
            Layer.provide(Layer.merge(workspace, T3ProjectFileLoader.layer)),
          ),
        ),
      ),
      Layer.provide(owners.platformLayer),
    );
    const projects = ProjectService.layer.pipe(
      Layer.provide(Layer.merge(workspace, metadata)),
      Layer.provide(common),
    );
    const terminals = TerminalManager.layer.pipe(
      Layer.provide(terminalOwnerObservationLive.pipe(Layer.provide(owners.eventSinkLayer))),
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(PtyAdapter.PtyAdapter, { spawn: () => unexpectedControl("spawn") }),
          Layer.mock(NativeTelemetryClient.NativeTelemetryClient)({
            processTable: Effect.succeed([]),
          }),
          Layer.mock(PortScanner.PortDiscovery)({
            registerTerminalProcesses: () => unexpectedControl("registerTerminalProcesses"),
            unregisterTerminal: () => unexpectedControl("unregisterTerminal"),
          }),
          ProcessRunner.layer,
        ),
      ),
      Layer.provide(common),
    );
    const setupBase = ProjectSetupScriptRunner.layer.pipe(
      Layer.provide(Layer.merge(projects, terminals)),
      Layer.provide(common),
    );
    const setup = Layer.effect(
      ProjectSetupScriptRunner.ProjectSetupScriptRunner,
      Effect.gen(function* () {
        const actual = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
        runSetup.mockImplementation(actual.runForThread);
        return ProjectSetupScriptRunner.ProjectSetupScriptRunner.of({
          ...actual,
          runForThread: runSetup,
        });
      }),
    ).pipe(Layer.provide(setupBase));
    const manager = GitManager.layer.pipe(
      Layer.provide(Layer.mergeAll(drivers, sourceControl, setup)),
      Layer.provide(common),
    );
    const workflowBase = Layer.effect(GitWorkflow.GitWorkflowService, GitWorkflow.make).pipe(
      Layer.provide(Layer.merge(drivers, manager)),
    );
    const workflow = Layer.effect(
      GitWorkflow.GitWorkflowService,
      Effect.gen(function* () {
        const actual = yield* GitWorkflow.GitWorkflowService;
        createWorktree.mockImplementation(actual.createWorktree);
        removeWorktree.mockImplementation(actual.removeWorktree);
        renameBranch.mockImplementation(actual.renameBranch);
        return GitWorkflow.GitWorkflowService.of({
          ...actual,
          createWorktree,
          removeWorktree,
          renameBranch,
          fetchRemote: (input) =>
            actual.fetchRemote(input).pipe(
              Effect.onError((cause) =>
                Effect.gen(function* () {
                  const terminal = readIssuedGitFetchTerminalFailure(Cause.squash(cause), input);
                  if (terminal === null) {
                    fixture.audit.unsupportedFetchFailures++;
                    return; // The original Cause still propagates; no witness or success.
                  }
                  const parentHead = yield* fixture.git(input.cwd, ["rev-parse", "HEAD"]);
                  const remoteTrackingHead = yield* fixture.git(input.cwd, [
                    "rev-parse",
                    "refs/remotes/origin/main",
                  ]);
                  fixture.audit.fetchFailures.push({
                    request: input,
                    terminal,
                    parentHead,
                    remoteTrackingHead,
                  });
                  // Only the exact fixture lock is removed, after the retained actual
                  // child/scope terminal. Other ref changes remain honest readback.
                  yield* fixture.removeOwnedLock;
                }).pipe(
                  Effect.catchCause((observationCause) =>
                    Effect.sync(() => {
                      fixture.audit.unsupportedFetchFailures++;
                      fixture.audit.observationFailures.push(
                        String(Cause.squash(observationCause)).slice(0, 512),
                      );
                    }),
                  ),
                ),
              ),
            ),
        });
      }),
    ).pipe(Layer.provide(workflowBase));
    const folders = ManagedProjectFolders.layer.pipe(
      Layer.provide(Layer.mergeAll(projects, workflow, drivers)),
      Layer.provide(common),
    );
    const tracker =
      originalPreparation === undefined
        ? WorktreeSetupTracker.layer
        : Layer.effect(
            WorktreeSetupTracker.WorktreeSetupTracker,
            Effect.gen(function* () {
              const actual = yield* WorktreeSetupTracker.WorktreeSetupTracker;
              originalPreparation.trackerAcquisitions++;
              originalPreparation.actualTracker = actual;
              const forwarded = WorktreeSetupTracker.WorktreeSetupTracker.of({
                ...actual,
                begin: (input) => {
                  const originalBeginEffect = actual.begin(input);
                  if (
                    input.threadId !==
                    (originalPreparation.observedThreadId ?? ThreadId.make("thread:launch:retry"))
                  )
                    return originalBeginEffect;
                  originalPreparation.beginInvocations++;
                  return Effect.onExit(
                    Effect.gen(function* () {
                      originalPreparation.beginEvaluations++;
                      const executingFiber = yield* Effect.fiber;
                      if (
                        originalPreparation.capture === undefined &&
                        input.fiber !== null &&
                        input.fiber === executingFiber
                      ) {
                        originalPreparation.capture = {
                          input,
                          executingFiber,
                          originalBeginEffect,
                        };
                        yield* Deferred.succeed(originalPreparation.ready, input.fiber);
                      }
                      if (
                        originalPreparation.retryCapture === undefined &&
                        originalPreparation.retryEntryPhase === "before_retryPreparation" &&
                        originalPreparation.fiberExit !== undefined &&
                        originalPreparation.capture !== undefined &&
                        input.fiber !== null &&
                        input.fiber === executingFiber &&
                        executingFiber !== originalPreparation.capture.executingFiber
                      ) {
                        originalPreparation.retryCapture = {
                          input,
                          executingFiber,
                          originalBeginEffect,
                        };
                        yield* Deferred.succeed(originalPreparation.retryReady, input.fiber);
                      }
                      return yield* originalBeginEffect;
                    }),
                    (exit) =>
                      Effect.sync(() => {
                        originalPreparation.beginTerminals++;
                        if (
                          originalPreparation.capture?.originalBeginEffect === originalBeginEffect
                        )
                          originalPreparation.beginExit = exit;
                        if (
                          originalPreparation.retryCapture?.originalBeginEffect ===
                          originalBeginEffect
                        )
                          originalPreparation.retryBeginExit = exit;
                      }),
                  );
                },
              });
              originalPreparation.forwardedTracker = forwarded;
              return forwarded;
            }),
          ).pipe(Layer.provide(WorktreeSetupTracker.layer));
    const observedSink =
      retrySlotProbe === undefined
        ? Layer.empty
        : Layer.effect(
            EventSink.EventSinkV2,
            Effect.gen(function* () {
              const original = owners.eventSink.ordinaryCheckoutLifetime;
              if (original === undefined)
                return yield* Effect.die("The serving retry lifetime is unavailable.");
              const sql = yield* SqlClient.SqlClient;
              if (sql !== owners.sql)
                return yield* Effect.die("The outer retry transaction changed SQL ownership.");
              const enclosing = yield* makeCommitTransaction();
              retrySlotProbe.acquisitions++;
              const readFacts = (operationId: string) =>
                Effect.gen(function* () {
                  const paths =
                    yield* sql`SELECT * FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${operationId}`;
                  const history =
                    yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations WHERE operation_id = ${operationId} ORDER BY ordinal`;
                  return { paths, history };
                });
              const recordPreparedOutcome: OrdinaryCheckoutLifetime["recordPreparedOutcome"] = (
                input,
              ) => {
                const actual = original.recordPreparedOutcome(input);
                if (
                  input.ref.originalUse.operationId !== "command:launch:retry:ordinary-preparation"
                )
                  return actual;
                retrySlotProbe.invocations++;
                let previous: Effect.Success<ReturnType<typeof readFacts>> | undefined;
                const wrapped = enclosing.withTransaction(
                  Effect.gen(function* () {
                    retrySlotProbe.evaluations++;
                    previous = yield* readFacts(input.ref.originalUse.operationId);
                    const result = yield* actual;
                    const source = input.ref.executor;
                    if (
                      source.kind !== "actual_prepared_producer" ||
                      source.source.kind !== "prepared_run"
                    )
                      return yield* Effect.die(
                        "The actual qualified retry producer source is unavailable.",
                      );
                    const run = source.source.preparation;
                    const admission = yield* original.readAdmissionForRun({
                      threadId: input.ref.originalUse.lease.ownerThreadId,
                      runId: run.runId,
                    });
                    if (admission === null || admission.run === null)
                      return yield* Effect.die(
                        "The authentic original retry admission is unavailable.",
                      );
                    assert.equal(admission.admissionId, source.source.admission.admissionId);
                    assert.deepEqual(
                      ordinaryCheckoutAdmissionRefV1(admission),
                      source.source.admission,
                    );
                    assert.equal(
                      input.ref.originalUse.lease.ownerThreadId,
                      admission.capture.threadId,
                    );
                    assert.deepEqual(
                      ordinaryCheckoutLeaseIdentityV1(input.ref.originalUse.lease),
                      ordinaryCheckoutLeaseIdentityV1(admission.capture.lease),
                    );
                    assert.deepEqual(source.source, input.ref.originalUse.source);
                    assert.deepEqual(admission.run, source.source.preparation);
                    const outcome = input.actualProducerOutcome;
                    assert.equal(outcome.kind, "prepared_failed");
                    assert.equal(outcome.observation.kind, "prepared_precreation_failure_observed");
                    const query = {
                      threadId: admission.capture.threadId,
                      projectId: admission.capture.projectId,
                      runId: admission.run.runId,
                      messageId: admission.run.messageId,
                      canonicalProjectRoot: admission.capture.canonicalProjectRoot,
                      canonicalCheckoutPath: admission.capture.canonicalCheckoutPath,
                      branch: fixture.branch,
                      targetSource: outcome.observation.targetSource,
                    };
                    const facts = yield* readFacts(input.ref.originalUse.operationId);
                    assert.equal(facts.paths.length, 1);
                    assert.equal(facts.paths[0]?.state, "released");
                    assert.isString(facts.paths[0]?.outcome_json);
                    const history = yield* original.readExecutionHistory(input.ref.originalUse);
                    assert.equal(history.latestOrdinal, result.ordinal);
                    assert.equal(history.participants.length, 1);
                    assert.equal(history.participants[0]?.state, "retired");
                    assert.deepEqual(history.participants[0]?.ref, input.ref);
                    assert.equal(history.facts[history.latestOrdinal]?.eventKind, "retire");
                    const leases =
                      yield* sql`SELECT * FROM worktree_ownership_leases WHERE resource_path = ${admission.capture.lease.resourcePath}`;
                    assert.equal(leases.length, 1);
                    assert.equal(leases[0]?.owner_thread_id, admission.capture.threadId);
                    assert.equal(leases[0]?.lease_id, admission.capture.lease.leaseId);
                    assert.equal(leases[0]?.branch, fixture.branch);
                    assert.isAbove(
                      Number(leases[0]?.expires_at_ms),
                      DateTime.toEpochMillis(yield* DateTime.now),
                    );
                    const runs =
                      yield* sql`SELECT json_extract(payload_json, '$.status') AS status, json_extract(payload_json, '$.activeAttemptId') AS attempt, json_extract(payload_json, '$.rootNodeId') AS node, json_extract(payload_json, '$.userMessageId') AS message FROM orchestration_v2_projection_runs WHERE run_id = ${admission.run.runId}`;
                    assert.equal(runs.length, 1);
                    assert.equal(runs[0]?.status, "preparing");
                    assert.equal(runs[0]?.attempt, admission.run.runAttemptId);
                    assert.equal(runs[0]?.node, admission.run.nodeId);
                    assert.equal(runs[0]?.message, admission.run.messageId);
                    const pending =
                      yield* sql`SELECT effect.effect_id FROM orchestration_v2_ordinary_checkout_effect_links link JOIN orchestration_v2_effect_outbox effect ON effect.effect_id = link.effect_id WHERE link.admission_id = ${admission.admissionId} AND effect.status IN ('pending', 'running')`;
                    assert.equal(pending.length, 0);
                    const refusal = yield* original
                      .readOriginalPreparedRetry(query)
                      .pipe(Effect.result);
                    if (Result.isSuccess(refusal))
                      return yield* Effect.die(
                        "The original completion became visible before its enclosing commit.",
                      );
                    assert.equal(refusal.failure._tag, "OrdinaryCheckoutHistoryError");
                    assert.equal(
                      refusal.failure.message,
                      "Retry has no retained, completed original preparation outcome.",
                    );
                    retrySlotProbe.beforeRefusal = true;
                    if (retrySlotProbe.mode === "rollback") {
                      // A genuine duplicate primary key causes a typed SQL rollback, never an accepted replacement row.
                      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions SELECT * FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${input.ref.originalUse.operationId}`;
                      return yield* Effect.die(
                        "The authentic duplicate-key rollback unexpectedly succeeded.",
                      );
                    }
                    const originalRecord = {
                      ref: input.ref,
                      outcome: input.actualProducerOutcome,
                      associationOrdinal: result.ordinal,
                      query,
                      admission,
                      history,
                    };
                    retrySlotProbe.originalRecord = originalRecord;
                    yield* enclosing.afterCommit(
                      Effect.sync(() => {
                        retrySlotProbe.publicationPhase = "entered";
                        retrySlotProbe.publicationRecord = originalRecord;
                        retrySlotProbe.publicationPhase = "completed";
                      }),
                    );
                    return result;
                  }),
                );
                return wrapped.pipe(
                  Effect.onError((cause) =>
                    Effect.gen(function* () {
                      if (retrySlotProbe.mode !== "rollback") return;
                      retrySlotProbe.rollbackCause = cause;
                      const exit = yield* readFacts(input.ref.originalUse.operationId).pipe(
                        Effect.exit,
                      );
                      retrySlotProbe.rollbackObservation = {
                        operationId: input.ref.originalUse.operationId,
                        baseline: previous,
                        exit,
                      };
                    }),
                  ),
                );
              };
              const originalReader = original.readOriginalPreparedRetry;
              const forwardedReader: OrdinaryCheckoutLifetime["readOriginalPreparedRetry"] =
                retrySlotProbe.mode === "rollback"
                  ? originalReader
                  : (input) => {
                      const actual = originalReader(input);
                      const observation: RetrySlotTransactionProbe["readInvocations"][number] = {
                        input,
                        slot: undefined,
                      };
                      retrySlotProbe.readInvocations.push(observation);
                      const tested =
                        retainedReaderProbe === undefined
                          ? actual
                          : observeRetainedReaderVariant(
                              retainedReaderProbe,
                              retrySlotProbe,
                              original,
                              input,
                              actual,
                              sql,
                              enclosing,
                            );
                      return tested.pipe(
                        Effect.tap((slot) =>
                          Effect.sync(() => {
                            observation.slot = slot;
                            retrySlotProbe.readSuccesses.push({ input, slot });
                            retrySlotProbe.afterSlotQuery = input;
                            retrySlotProbe.afterSlot = slot;
                          }),
                        ),
                      );
                    };
              const facade = EventSink.EventSinkV2.of({
                ...owners.eventSink,
                ordinaryCheckoutLifetime: {
                  ...original,
                  recordPreparedOutcome,
                  readOriginalPreparedRetry: forwardedReader,
                },
              });
              retrySlotProbe.serving = {
                sql,
                transactionService: sql.transactionService,
                sink: owners.eventSink,
                facade,
                lifetime: original,
                originalReader,
                forwardedReader,
                enclosing,
              };
              return facade;
            }),
          ).pipe(Layer.provide(common));
    const physical = Layer.mergeAll(
      observedSink,
      projects,
      terminals,
      setup,
      workflow,
      folders,
      tracker,
      clones,
      textGeneration,
      providerRegistryLayer,
    );
    const capture = Layer.effectDiscard(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const sink = yield* EventSink.EventSinkV2;
        const management = yield* ThreadManagement.ThreadManagementService;
        if (sql !== owners.sql || sink !== owners.eventSink || management !== owners.management)
          return yield* Effect.die("The real retry fixture crossed its original owner graph.");
        fixture.audit.acquisitions.push({
          sql,
          sink,
          management,
          projects: yield* ProjectService.ProjectService,
          terminals: yield* TerminalManager.TerminalManager,
          receipts: yield* CommandReceiptStore.CommandReceiptStoreV2,
          outbox: yield* EffectOutbox.EffectOutboxV2,
          config: yield* ServerConfig.ServerConfig,
        });
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            fixture.audit.finalized++;
          }),
        );
      }),
    ).pipe(Layer.provide(Layer.merge(physical, common)));
    return Layer.merge(physical, capture);
  };
  const runtime = makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "original-real-retry" },
    ProviderAdapterRegistry.makeLayer([adapter]),
    {
      databaseLayer: SqlitePersistenceMemory,
      runEffectWorker: false,
      checkoutFixture: {
        projects: [{ projectId, title: project.title, workspaceRoot: fixture.workspaceRoot }],
        worktreesDir: fixture.worktreesDir,
        resolvePath: () => undefined,
      },
      delegatedPreparation: factory,
    },
  );
  const exposeCaptured = Layer.unwrap(
    Effect.gen(function* () {
      const launcher = yield* ThreadLaunch.ThreadLaunchService;
      const captured = fixture.audit.acquisitions[0];
      if (
        captured === undefined ||
        fixture.audit.acquisitions.length !== 1 ||
        fixture.audit.factories !== 1 ||
        (yield* ThreadManagement.ThreadManagementService) !== captured.management ||
        (yield* EventSink.EventSinkV2) !== captured.sink
      )
        return yield* Effect.die(
          "The original retry requires exactly one actual captured owner graph.",
        );
      fixture.audit.launchers.push(launcher);
      return Layer.mergeAll(
        Layer.succeed(ProjectService.ProjectService, captured.projects),
        Layer.succeed(ServerConfig.ServerConfig, captured.config),
        Layer.succeed(EffectOutbox.EffectOutboxV2, captured.outbox),
        Layer.succeed(SqlClient.SqlClient, captured.sql),
      );
    }),
  ).pipe(Layer.provide(runtime));
  return {
    layer: Layer.merge(runtime, exposeCaptured),
    createWorktree,
    removeWorktree,
    renameBranch,
    runSetup,
    generateBranchName,
    generateThreadTitle,
    audit: fixture.audit,
  };
}

function makeHarness(options: {
  readonly realPreparation: RealRetryFixture;
  readonly originalPreparation?: OriginalRetryPreparationObservation;
}): ReturnType<typeof makeRealRetryHarness>;
function makeHarness(options?: HarnessOptions): ReturnType<typeof makeDefaultHarness>;
function makeHarness(
  options:
    | HarnessOptions
    | {
        readonly realPreparation: RealRetryFixture;
        readonly originalPreparation?: OriginalRetryPreparationObservation;
      } = {},
) {
  return "realPreparation" in options
    ? makeRealRetryHarness(options.realPreparation, options.originalPreparation)
    : makeDefaultHarness(options);
}

function launchInput(input: {
  readonly command: string;
  readonly thread: string;
  readonly message?: string;
  readonly workspace?: ThreadLaunch.ThreadLaunchWorkspaceStrategy;
}) {
  return {
    commandId: CommandId.make(input.command),
    threadId: ThreadId.make(input.thread),
    projectId,
    title: "New thread",
    modelSelection,
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    workspaceStrategy: input.workspace ?? { type: "root" as const },
    ...(input.message === undefined
      ? {}
      : {
          initialMessage: {
            messageId: MessageId.make(`${input.message}:id`),
            text: input.message,
            attachments: [],
          },
        }),
    createdBy: "user" as const,
    creationSource: "web" as const,
  };
}

function waitUntil<E, R>(predicate: () => Effect.Effect<boolean, E, R>): Effect.Effect<void, E, R> {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      if (yield* predicate()) return;
      yield* Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            setImmediate(resolve);
          }),
      );
    }
    assert.fail("Condition was not reached before timeout.");
  });
}

function awaitStartingRun(
  threads: ThreadManagement.ThreadManagementService["Service"],
  threadId: ThreadId,
) {
  return threads.streamStoredEventsFrom({ threadId }).pipe(
    Stream.filter(
      (stored) =>
        stored.event.type === "run.updated" &&
        (stored.event.payload.status === "starting" ||
          stored.event.payload.status === "failed" ||
          stored.event.payload.status === "cancelled" ||
          stored.event.payload.status === "interrupted" ||
          stored.event.payload.status === "rolled_back"),
    ),
    Stream.runHead,
    Effect.tap((stored) => {
      const event = Option.getOrNull(stored)?.event;
      assert.equal(event?.type === "run.updated" ? event.payload.status : null, "starting");
      return Effect.void;
    }),
  );
}

it.effect.each(
  (["new", "existing"] as const).flatMap((target) =>
    (["user", "agent"] as const).map((createdBy) => ({ target, createdBy })),
  ),
)(
  "attributes $createdBy-configured automations in $target threads without changing their prompt",
  ({ target, createdBy }) => {
    const harness = makeHarness();
    const scheduledTasks = ScheduledTasks.layer.pipe(
      Layer.provide(Layer.mergeAll(harness.layer, NodeCrypto.layer, Scheduler.layer)),
    );
    return Effect.gen(function* () {
      const tasks = yield* ScheduledTasks.ScheduledTaskService;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const existing =
        target === "existing"
          ? yield* launches.launch(
              launchInput({ command: "command:existing", thread: "thread:existing" }),
            )
          : null;
      const { task } = yield* tasks.upsert({
        id: ScheduledTaskId.make("scheduled-task:attribution"),
        title: "Daily audit",
        prompt: "Audit performance and crashes.",
        enabled: false,
        schedule: { type: "interval", everyMs: 60_000 },
        projectId,
        threadId: existing?.threadId ?? null,
        workspaceStrategy: { type: "root" },
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy,
        creationSource: createdBy === "agent" ? "mcp" : "web",
      });
      const result = yield* tasks.runNow({ id: task.id });
      assert.equal(result.task.lastRunStatus, "succeeded");
      const projectThreads = yield* threads.listProjectThreads({
        projectId,
        includeSubagents: false,
      });
      const thread =
        projectThreads.find((candidate) => candidate.id === existing?.threadId) ??
        projectThreads[0];
      assert.isDefined(thread);
      const projection = yield* threads.getThreadProjection(thread!.id);
      // Encoding the persisted projection exercises both message and turn-item wire schemas.
      const wire = yield* encodeThreadProjection(projection);
      assert.equal(wire.messages[0]?.text, task.prompt);
      assert.equal(wire.messages[0]?.scheduledTaskId, task.id);
      assert.equal(wire.messages[0]?.createdBy, createdBy);
      const turnItem = wire.turnItems.find((item) => item.type === "user_message");
      assert.equal(turnItem?.text, task.prompt);
      assert.equal(turnItem?.scheduledTaskId, task.id);
    }).pipe(Effect.provide(Layer.mergeAll(harness.layer, scheduledTasks)));
  },
);

it.effect("retains automation and sender attribution while a message waits in the queue", () => {
  const harness = makeHarness({ runSetup: () => Effect.never });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:automation:queue",
        thread: "thread:automation:queue",
        message: "First message",
      }),
    );
    const scheduledTaskId = ScheduledTaskId.make("scheduled-task:queued");
    const senderThreadId = ThreadId.make("thread:agent-sender");
    const queued = yield* threads.sendToThread({
      projectId,
      commandId: CommandId.make("command:automation:queued"),
      threadId: launched.threadId,
      messageId: MessageId.make("message:automation:queued"),
      scheduledTaskId,
      senderThreadId,
      text: "Run the audit",
      attachments: [],
      mode: "queue",
      createdBy: "agent",
      creationSource: "mcp",
    });
    assert.equal(queued.delivery, "queued");
    const projection = yield* threads.getThreadProjection(launched.threadId);
    const message = projection.messages.find((item) => item.id === queued.message.id);
    assert.equal(message?.scheduledTaskId, scheduledTaskId);
    assert.equal(message?.senderThreadId, senderThreadId);
    assert.equal(message?.text, "Run the audit");
  }).pipe(Effect.provide(harness.layer));
});

class LegacyLauncherPty implements PtyAdapter.PtyProcess {
  readonly pid = 92001;
  readonly writes: string[] = [];
  readonly kills: (string | undefined)[] = [];
  private readonly exits = new Set<(event: PtyAdapter.PtyExitEvent) => void>();
  write(data: string) {
    this.writes.push(data);
  }
  resize() {}
  kill(signal?: string) {
    this.kills.push(signal);
    for (const exit of this.exits) exit({ exitCode: 0, signal: 15 });
  }
  onData(_callback: (data: string) => void) {
    return () => {};
  }
  onExit(callback: (event: PtyAdapter.PtyExitEvent) => void) {
    this.exits.add(callback);
    return () => {
      this.exits.delete(callback);
    };
  }
}

it.layer(NodeServices.layer, { excludeTestServices: true })(
  "Legacy launcher private D actual owner",
  (it) => {
    it.effect.each(
      (
        [
          "no_script",
          "opted_out",
          "missing_method",
          "lost_reply",
          "reused",
          "started_setup",
        ] as const
      ).map((scenario) => ({ scenario })),
    )("strict guard rejection uses actual private D disposition for $scenario", ({ scenario }) => {
      const runSetupScript = scenario !== "opted_out";
      return Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const workspaceRoot = yield* fs.makeTempDirectoryScoped({ prefix: "legacy-launch-D-" });
        const process = new LegacyLauncherPty();
        const scripts: ReadonlyArray<ProjectScript> =
          scenario === "started_setup"
            ? [
                {
                  id: "setup",
                  name: "Captured synthetic setup",
                  command: "synthetic-no-execution",
                  icon: "configure",
                  runOnWorktreeCreate: true,
                  async: true,
                },
              ]
            : [];
        const manager = yield* TerminalManager.makeWithOptions({
          logsDir: `${workspaceRoot}/terminal-logs`,
          env: {},
          shellResolver: () => "/bin/sh",
          processTable: Effect.succeed([]),
          processKillGraceMs: 1,
          subprocessInspector: () =>
            Effect.succeed({ hasRunningSubprocess: false, childCommand: null, processIds: [] }),
          ptyAdapter: {
            spawn: () =>
              scenario === "started_setup"
                ? Effect.succeed(process)
                : Effect.die("No-control launcher must not spawn"),
          },
        }).pipe(Effect.provide(ProcessRunner.layer));
        const runner = yield* ProjectSetupScriptRunner.make.pipe(
          Effect.provide(
            Layer.mergeAll(
              Layer.mock(ProjectService.ProjectService)({}),
              Layer.succeed(TerminalManager.TerminalManager, manager),
              ServerSettings.layerTest(),
              Layer.succeed(HostProcessEnvironment, {}),
              Layer.succeed(HostProcessPlatform, "linux"),
            ),
          ),
        );
        const harness = makeHarness({
          workspaceRoot,
          projectScripts: scripts,
          ...(scenario === "started_setup" ? { runSetup: runner.runForThread } : {}),
          privateDUnavailable: scenario === "missing_method",
          terminalOwner: Layer.succeed(TerminalManager.TerminalManager, manager),
        });
        yield* Effect.gen(function* () {
          const launch = yield* ThreadLaunch.ThreadLaunchService;
          const sink = yield* EventSink.EventSinkV2;
          const threads = yield* ThreadManagement.ThreadManagementService;
          const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const store = yield* EventStore.EventStoreV2;
          const projectCommandId = CommandId.make(`legacy-launch-D:project:${scenario}`);
          yield* sink.commitProjectCommand({
            commandId: projectCommandId,
            projectId,
            commandType: "project.create",
            acceptedAt: yield* DateTime.now,
            event: {
              eventId: EventId.make(`${projectCommandId}:event`),
              aggregateKind: "project",
              aggregateId: projectId,
              occurredAt: project.createdAt,
              commandId: projectCommandId,
              causationEventId: null,
              correlationId: null,
              metadata: {},
              type: "project.created",
              payload: {
                projectId,
                title: project.title,
                workspaceRoot,
                defaultModelSelection: modelSelection,
                scripts,
                createdAt: project.createdAt,
                updatedAt: project.updatedAt,
              },
            },
          });
          const base = launchInput({
            command: `legacy-launch-D:C:${scenario}`,
            thread: `legacy-launch-D:T:${scenario}`,
            message: "Original guarded prompt",
          });
          if (scenario === "reused") {
            const { initialMessage: _message, ...empty } = base;
            yield* launch.launch({
              ...empty,
              commandId: CommandId.make("legacy-launch-D:empty-reuse"),
            });
          }
          const createCommandId = legacyBootstrapCreateCommandId(base.threadId, base.commandId);
          const policy = {
            version: 1 as const,
            createCommandId,
            birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
            releaseCommandId: base.commandId,
            projectId,
            threadId: base.threadId,
            messageId: base.initialMessage!.messageId,
            payloadHash: `legacy-launch-D:${scenario}`,
            ownsNewThread: scenario !== "reused",
            dispatchGuard: {
              observedSnapshotSequence: yield* sink.latestSequence(),
              expectedModelSelection: modelSelection,
              expectedSessionStatus: null,
              expectedActiveTurnId: null,
              expectedLatestTurnId: null,
              requireIdle: true as const,
            },
          };
          const input = {
            ...base,
            commandId: createCommandId,
            preparationReleaseCommandId: base.commandId,
            legacyBootstrap: policy,
            runSetupScript,
            reuseExistingThread: scenario === "reused",
          };
          const privateDispatch = threads.dispatchLegacyGuardRejectionDelete;
          const lostReply =
            scenario === "lost_reply" && privateDispatch !== undefined
              ? vi
                  .spyOn(threads, "dispatchLegacyGuardRejectionDelete")
                  .mockImplementation((command) =>
                    privateDispatch(command).pipe(
                      Effect.andThen(
                        Effect.fail(
                          new Orchestrator.OrchestratorDispatchError({
                            commandId: command.commandId,
                            commandType: command.type,
                            cause: "Synthetic lost response after authentic commit",
                          }),
                        ),
                      ),
                    ),
                  )
              : undefined;
          const result = yield* launch
            .launch(input)
            .pipe(Effect.result, Effect.ensuring(Effect.sync(() => lostReply?.mockRestore())));
          assert.equal(result._tag, "Failure");
          if (result._tag === "Failure") {
            assert.isTrue(Schema.is(ThreadLaunch.ThreadLaunchError)(result.failure));
            if (Schema.is(ThreadLaunch.ThreadLaunchError)(result.failure))
              assert.equal(
                result.failure.bootstrapThreadDisposition,
                scenario === "missing_method" || scenario === "reused" ? undefined : "deleted",
              );
          }
          const c = yield* receipts.getByCommandId(base.commandId);
          assert.isTrue(Option.isSome(c));
          if (Option.isSome(c)) assert.equal(c.value.status, "rejected");
          const dId = CommandId.make(`${createCommandId}:guard-rejection-delete`);
          const d = yield* receipts.getByCommandId(dId);
          if (scenario === "missing_method" || scenario === "reused") {
            assert.isTrue(Option.isNone(d));
            assert.isNull((yield* threads.getThreadProjection(base.threadId)).thread.deletedAt);
            assert.isEmpty(
              (yield* outbox.listByThreadId(base.threadId)).filter(
                (effect) => effect.commandId === dId,
              ),
            );
            return;
          }
          assert.isTrue(Option.isSome(d));
          if (Option.isSome(d)) assert.equal(d.value.status, "accepted");
          const events = Array.from(
            yield* store.readByCommandId({ commandId: dId }).pipe(Stream.runCollect),
          );
          assert.deepEqual(
            events.map((stored) => stored.event.type),
            ["run.updated", "thread.deleted"],
          );
          const projection = yield* threads.getThreadProjection(base.threadId);
          assert.isNotNull(projection.thread.deletedAt);
          assert.equal(
            projection.runs[0]!.legacyPreparation?.setup.status,
            scenario === "started_setup" ? "resolved" : runSetupScript ? "no_script" : "opted_out",
          );
          const cleanup = (yield* outbox.listByThreadId(base.threadId)).filter(
            (effect) => effect.commandId === dId && effect.request.type === "terminal.cleanup",
          );
          if (scenario === "started_setup") {
            const proof = projection.runs[0]!.legacyReleaseDecision?.deletion;
            assert.equal(proof?.type, "bound_control");
            assert.lengthOf(cleanup, 1);
            if (proof?.type === "bound_control" && cleanup[0]?.request.type === "terminal.cleanup")
              assert.deepEqual(cleanup[0].request.legacyOwnedControl, proof.control);
            assert.lengthOf(process.writes, 1);
            assert.include(process.writes[0]!, "synthetic-no-execution");
            assert.isEmpty(process.kills);
            assert.deepEqual(
              projection.runs[0]!.legacyPreparation?.steps.map((step) => [
                step.effect.kind,
                step.state,
              ]),
              [
                ["setup.open", "known_succeeded"],
                ["setup.write", "known_started"],
              ],
            );
          } else {
            assert.equal(projection.runs[0]!.legacyReleaseDecision?.deletion?.type, "no_control");
            assert.isEmpty(cleanup);
            assert.isEmpty(process.writes);
            assert.isEmpty(process.kills);
          }
          assert.isEmpty(harness.removeWorktree.mock.calls);
          assert.isEmpty(harness.createWorktree.mock.calls);
          if (scenario === "started_setup") {
            const fenced = yield* Deferred.make<void>();
            const actualRead = outbox.listByThreadId;
            const observeFence = vi
              .spyOn(outbox, "listByThreadId")
              .mockImplementation((id) =>
                actualRead(id).pipe(
                  Effect.tap((effects) =>
                    effects.some(
                      (effect) =>
                        effect.commandId === dId &&
                        effect.request.type === "terminal.cleanup" &&
                        effect.status === "pending",
                    )
                      ? Deferred.succeed(fenced, undefined)
                      : Effect.void,
                  ),
                ),
              );
            const retry = yield* launch.launch(input).pipe(Effect.forkChild);
            yield* Deferred.await(fenced).pipe(
              Effect.andThen(Effect.sync(() => retry.pollUnsafe())),
              Effect.tap((result) => Effect.sync(() => assert.isUndefined(result))),
              Effect.ensuring(Fiber.interrupt(retry)),
              Effect.ensuring(Effect.sync(() => observeFence.mockRestore())),
            );
            assert.isEmpty(process.kills);
            return;
          }
          const replay = yield* launch.launch(input).pipe(Effect.result);
          assert.equal(replay._tag, "Failure");
          assert.equal(
            (yield* receipts.getByCommandId(dId)).pipe(
              Option.map((receipt) => receipt.resultSequence),
              Option.getOrThrow,
            ),
            d.pipe(
              Option.map((receipt) => receipt.resultSequence),
              Option.getOrThrow,
            ),
          );
        }).pipe(Effect.provide(harness.layer));
      });
    });
  },
);

it.layer(NodeServices.layer, { excludeTestServices: true })(
  "Legacy worktree actual SQL journal",
  (it) => {
    it.effect.each([
      "verified",
      "unknown",
      "intent_readback_lost",
      "outcome_readback_lost",
      "rename_verified",
      "rename_unknown",
      "rename_intent_readback_lost",
      "rename_outcome_readback_lost",
    ] as const)("legacy worktree journals before mock add and preserves C for %s", (scenario) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const workspaceRoot = yield* fs.makeTempDirectoryScoped({
          prefix: "legacy-worktree-journal-",
        });
        const worktreesDir = `${workspaceRoot}/worktrees`;
        const structuralParent = `${worktreesDir}/${workspaceRoot.slice(workspaceRoot.lastIndexOf("/") + 1)}`;
        yield* fs.makeDirectory(structuralParent, { recursive: true });
        let owned = "";
        const commonDirectory = `${workspaceRoot}/.git`;
        const gitDirectory = `${commonDirectory}/worktrees/owned`;
        yield* fs.makeDirectory(gitDirectory, { recursive: true });
        let addCount = 0;
        let renameCount = 0;
        let renameStage = false;
        const renameCase = scenario.startsWith("rename_");
        const renameDone = yield* Deferred.make<void>();
        const oid = "a".repeat(40);
        const harness = makeHarness({
          workspaceRoot,
          physicalFixture: true,
          worktreesDir,
          resolveCommit: () => Effect.succeed({ commitSha: oid }),
          renameBranch: (input) =>
            Effect.gen(function* () {
              const hooks = input.legacyPreparation;
              if (hooks === undefined)
                return yield* Effect.die("Legacy rename must supply its exact journal");
              renameStage = true;
              const targetRef = `refs/heads/${input.newBranch}-1`;
              const step = {
                claim: hooks.claim,
                oldRef: hooks.claim.headRef,
                oldOid: hooks.claim.headOid,
                targetRef,
                exactName: input.exactName === true,
                args: ["branch", "-m", "--", input.oldBranch, `${input.newBranch}-1`],
              };
              yield* hooks.beforeEffect(step);
              yield* input.revalidateMutation ?? Effect.void;
              renameCount++;
              yield* hooks.afterEffect(
                step,
                scenario === "rename_unknown" ? "failed_or_unknown" : "settled_success",
                scenario === "rename_unknown" ? undefined : { ...hooks.claim, headRef: targetRef },
              );
              yield* input.revalidateMutation ?? Effect.void;
              return { branch: `${input.newBranch}-1` };
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new GitCommandError({
                    operation: "fixture.rename",
                    command: "git",
                    cwd: input.cwd,
                    detail:
                      cause instanceof Error
                        ? cause.message
                        : "Synthetic rename owner is unresolved",
                  }),
              ),
              Effect.ensuring(Deferred.succeed(renameDone, undefined)),
            ),
          createWorktree: (input, options) =>
            Effect.gen(function* () {
              const hooks = options?.legacyPreparation;
              if (hooks === undefined)
                return yield* Effect.die("Legacy producer must supply its private journal");
              if (input.path == null)
                return yield* Effect.die("Original planned worktree path is required");
              owned = input.path;
              const parentPath = owned.slice(0, owned.lastIndexOf("/"));
              const parent = NodeFS.lstatSync(parentPath);
              const step = {
                kind: "worktree.add" as const,
                cwd: workspaceRoot,
                args: ["worktree", "add", "-b", input.newRefName!, owned, oid],
                worktreePath: owned,
                commonDirectory,
                baseCommitOid: oid,
                targetRef: `refs/heads/${input.newRefName}`,
                before: {
                  parentPath,
                  parentRealPath: NodeFS.realpathSync(parentPath),
                  parentDevice: String(parent.dev),
                  parentInode: String(parent.ino),
                  targetRefAbsent: true as const,
                  registrationAbsent: true as const,
                },
              };
              yield* hooks.beforeEffect(step);
              yield* options?.revalidateMutation ?? Effect.void;
              addCount++;
              yield* fs.makeDirectory(owned);
              yield* fs.writeFileString(`${owned}/.git`, `gitdir: ${gitDirectory}\n`);
              const material = NodeFS.lstatSync(owned);
              const claim = {
                path: owned,
                realPath: NodeFS.realpathSync(owned),
                device: String(material.dev),
                inode: String(material.ino),
                parentRealPath: NodeFS.realpathSync(parentPath),
                gitDirectory,
                commonDirectory,
                registeredPath: owned,
                headRef: step.targetRef,
                headOid: oid,
              };
              yield* hooks.afterEffect(
                step,
                scenario === "unknown" ? "failed_or_unknown" : "settled_success",
                scenario === "unknown" ? undefined : claim,
              );
              yield* options?.revalidateMutation ?? Effect.void;
              return { worktree: { path: owned, refName: input.newRefName!, headSha: oid } };
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new GitCommandError({
                    operation: "fixture.legacyWorktree",
                    command: "git",
                    cwd: input.cwd,
                    detail:
                      cause instanceof Error
                        ? cause.message
                        : "Synthetic owner outcome is unavailable",
                  }),
              ),
            ),
        });
        yield* Effect.gen(function* () {
          const launch = yield* ThreadLaunch.ThreadLaunchService;
          const threads = yield* ThreadManagement.ThreadManagementService;
          const sink = yield* EventSink.EventSinkV2;
          const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const projectCommandId = CommandId.make(`legacy-worktree:project:${scenario}`);
          yield* sink.commitProjectCommand({
            commandId: projectCommandId,
            projectId,
            commandType: "project.create",
            acceptedAt: yield* DateTime.now,
            event: {
              eventId: EventId.make(`${projectCommandId}:event`),
              aggregateKind: "project",
              aggregateId: projectId,
              occurredAt: project.createdAt,
              commandId: projectCommandId,
              causationEventId: null,
              correlationId: null,
              metadata: {},
              type: "project.created",
              payload: {
                projectId,
                title: project.title,
                workspaceRoot,
                defaultModelSelection: modelSelection,
                scripts: [],
                createdAt: project.createdAt,
                updatedAt: project.updatedAt,
              },
            },
          });
          const base = launchInput({
            command: `legacy-worktree:C:${scenario}`,
            thread: `legacy-worktree:T:${scenario}`,
            message: "Preserve original worktree delivery",
          });
          const b = legacyBootstrapCreateCommandId(base.threadId, base.commandId);
          const policy = {
            version: 1 as const,
            createCommandId: b,
            birthCommandId: CommandId.make(`${b}:initial-message`),
            releaseCommandId: base.commandId,
            projectId,
            threadId: base.threadId,
            messageId: base.initialMessage!.messageId,
            payloadHash: `legacy-worktree:${scenario}`,
            ownsNewThread: true,
          };
          const input = {
            ...base,
            commandId: b,
            preparationReleaseCommandId: base.commandId,
            legacyBootstrap: policy,
            runSetupScript: false,
            workspaceStrategy: {
              type: "worktree" as const,
              baseRef: "main",
              ...(renameCase ? {} : { branch: "legacy/qualified" }),
              startFromOrigin: false,
            },
          };
          const actualReceipt = receipts.getByCommandId;
          const lostRead = scenario.endsWith("readback_lost")
            ? vi
                .spyOn(receipts, "getByCommandId")
                .mockImplementation((id) =>
                  (!renameCase || renameStage) &&
                  id.endsWith(scenario.includes("intent_readback_lost") ? ":intent" : ":outcome")
                    ? Effect.succeed(Option.none())
                    : actualReceipt(id),
                )
            : undefined;
          const actualDispatch = threads.dispatch;
          const progressFence = renameCase
            ? vi
                .spyOn(threads, "dispatch")
                .mockImplementation((command) =>
                  command.type === "prepared-run.progress" &&
                  command.commandId === `${b}:progress:setup`
                    ? Deferred.await(renameDone).pipe(Effect.andThen(actualDispatch(command)))
                    : actualDispatch(command),
                )
            : undefined;
          const result = yield* launch.launch(input).pipe(
            Effect.timeout("5 seconds"),
            Effect.result,
            Effect.ensuring(Deferred.succeed(renameDone, undefined)),
            Effect.ensuring(
              Effect.sync(() => {
                lostRead?.mockRestore();
                progressFence?.mockRestore();
              }),
            ),
          );
          const projection = yield* threads.getThreadProjection(base.threadId);
          const preparation = projection.runs[0]!.legacyPreparation;
          assert.isDefined(
            preparation,
            Result.isFailure(result) && Schema.is(ThreadLaunch.ThreadLaunchError)(result.failure)
              ? canonicalLegacyPayload(result.failure.cause)
              : "Missing actual journal",
          );
          assert.equal(preparation?.commonDirectory, commonDirectory);
          assert.equal(preparation?.steps[0]?.effect.kind, "worktree.add");
          const step = preparation!.steps[0]!;
          assert.isTrue(Option.isSome(yield* receipts.getByCommandId(step.intentCommandId)));
          const recorded = Array.from(
            yield* sink
              .readByCommandId({ commandId: step.intentCommandId })
              .pipe(Stream.runCollect),
          );
          assert.lengthOf(recorded, 1);
          assert.equal(recorded[0]!.event.type, "run.updated");
          const c = yield* receipts.getByCommandId(base.commandId);
          if (renameCase) {
            const renamed = preparation!.steps.find(
              (entry) => entry.effect.kind === "branch.rename",
            );
            assert.isDefined(renamed);
            assert.equal(
              renamed?.effect.kind === "branch.rename" ? renamed.effect.input.targetRef : null,
              "refs/heads/generated-branch-1",
            );
            assert.equal(addCount, 1);
            assert.isTrue(yield* fs.exists(owned));
            assert.isNull(projection.thread.deletedAt);
            assert.isEmpty(harness.removeWorktree.mock.calls);
            if (scenario === "rename_verified") {
              assert.isTrue(Result.isSuccess(result));
              assert.equal(renameCount, 1);
              assert.equal(renamed?.state, "known_succeeded");
              assert.isTrue(Option.isSome(c));
              if (Option.isSome(c)) assert.equal(c.value.status, "accepted");
              assert.equal(projection.thread.branch, "generated-branch-1");
            } else {
              assert.isTrue(Result.isFailure(result));
              assert.isTrue(Option.isNone(c));
              assert.equal(renameCount, scenario === "rename_intent_readback_lost" ? 0 : 1);
              assert.equal(
                renamed?.state,
                scenario === "rename_unknown"
                  ? "unknown"
                  : scenario === "rename_intent_readback_lost"
                    ? "intent"
                    : "known_succeeded",
              );
              assert.isEmpty(
                (yield* outbox.listByThreadId(base.threadId)).filter(
                  (effect) => effect.request.type === "provider-turn.start",
                ),
              );
              const priorRename = renameCount;
              yield* launch.launch(input).pipe(Effect.result);
              assert.equal(renameCount, priorRename);
              assert.equal(addCount, 1);
              assert.isTrue(Option.isNone(yield* receipts.getByCommandId(base.commandId)));
            }
            return;
          }
          if (scenario === "verified") {
            assert.isTrue(Result.isSuccess(result));
            assert.isTrue(Option.isSome(c));
            if (Option.isSome(c)) assert.equal(c.value.status, "accepted");
            assert.equal(step.state, "known_succeeded");
            assert.equal(step.evidence?.type, "worktree_claim");
            assert.equal(addCount, 1);
            assert.equal(projection.thread.worktreePath, owned);
          } else {
            assert.isTrue(Result.isFailure(result));
            assert.isTrue(Option.isNone(c));
            assert.isEmpty(
              (yield* outbox.listByThreadId(base.threadId)).filter(
                (effect) => effect.request.type === "provider-turn.start",
              ),
            );
            assert.isNull(projection.thread.deletedAt);
            assert.isEmpty(harness.removeWorktree.mock.calls);
            assert.equal(
              step.state,
              scenario === "unknown"
                ? "unknown"
                : scenario === "intent_readback_lost"
                  ? "intent"
                  : "known_succeeded",
            );
            assert.equal(addCount, scenario === "intent_readback_lost" ? 0 : 1);
            const countBefore = addCount;
            yield* launch.launch(input).pipe(Effect.result);
            assert.equal(addCount, countBefore);
            assert.isTrue(Option.isNone(yield* receipts.getByCommandId(base.commandId)));
            if (scenario !== "intent_readback_lost") assert.isTrue(yield* fs.exists(owned));
          }
        }).pipe(Effect.provide(harness.layer));
      }),
    );
  },
);

it.effect(
  "legacy preparation keeps original C absent until the persisted release and replays that receipt",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const continueSetup = yield* Deferred.make<void>();
      const harness = makeHarness({
        runSetup: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(continueSetup)),
            Effect.as({ status: "no-script" as const }),
          ),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const sink = yield* EventSink.EventSinkV2;
        const projectCommandId = CommandId.make("legacy:original-C:project-fixture");
        yield* sink.commitProjectCommand({
          commandId: projectCommandId,
          projectId,
          commandType: "project.create",
          acceptedAt: yield* DateTime.now,
          event: {
            eventId: EventId.make(`${projectCommandId}:event`),
            aggregateKind: "project",
            aggregateId: projectId,
            occurredAt: project.createdAt,
            commandId: projectCommandId,
            causationEventId: null,
            correlationId: null,
            metadata: {},
            type: "project.created",
            payload: {
              projectId,
              title: project.title,
              workspaceRoot: project.workspaceRoot,
              defaultModelSelection: project.defaultModelSelection,
              scripts: [],
              createdAt: project.createdAt,
              updatedAt: project.updatedAt,
            },
          },
        });
        const base = launchInput({
          command: "legacy:original-C",
          thread: "legacy:thread",
          message: "Legacy prompt",
        });
        const createCommandId = legacyBootstrapCreateCommandId(base.threadId, base.commandId);
        const policy = {
          version: 1 as const,
          createCommandId,
          birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
          releaseCommandId: base.commandId,
          projectId,
          threadId: base.threadId,
          messageId: base.initialMessage!.messageId,
          payloadHash: "fixture-hash",
          ownsNewThread: true,
        };
        const input = {
          ...base,
          commandId: createCommandId,
          preparationReleaseCommandId: base.commandId,
          legacyBootstrap: policy,
        };
        const request = yield* launches.launch(input).pipe(Effect.forkChild);
        yield* Effect.race(
          Deferred.await(entered),
          Fiber.join(request).pipe(
            Effect.andThen(Effect.die("Legacy launch completed before its held setup milestone.")),
          ),
        );
        const preparing = yield* threads.getThreadProjection(base.threadId);
        assert.equal(preparing.runs[0]?.status, "preparing");
        assert.deepEqual(preparing.runs[0]?.legacyBootstrap, {
          ...policy,
          runId: preparing.runs[0]!.id,
        });
        assert.isEmpty(yield* outbox.listByCommandId(base.commandId));
        yield* Deferred.succeed(continueSetup, undefined);
        yield* threads.streamStoredEventsFrom({ threadId: base.threadId, afterSequence: 0 }).pipe(
          Stream.filter(
            (stored) =>
              stored.commandId === base.commandId &&
              stored.event.type === "run.updated" &&
              stored.event.payload.status === "starting",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        const launched = yield* Fiber.join(request);
        assert.equal(
          launched.legacyReleaseSequence,
          (yield* (yield* CommandReceiptStore.CommandReceiptStoreV2).getByCommandId(
            base.commandId,
          )).pipe(
            Option.map((receipt) => receipt.resultSequence),
            Option.getOrThrow,
          ),
        );
        const effects = yield* outbox.listByCommandId(base.commandId);
        assert.lengthOf(
          effects.filter(({ request }) => request.type === "provider-turn.start"),
          1,
        );
        const replayed = yield* launches.launch(input);
        assert.isTrue(replayed.resumed);
        assert.equal(replayed.projection.runs[0]?.status, "starting");
        const release = yield* threads.dispatch({
          type: "prepared-run.release",
          commandId: base.commandId,
          threadId: base.threadId,
          runId: launched.projection.runs[0]!.id,
          legacyBootstrap: { ...policy, runId: launched.projection.runs[0]!.id },
        });
        assert.isTrue(
          release.storedEvents.some(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.legacyBootstrap?.releaseCommandId === base.commandId,
          ),
        );
        assert.isEmpty(yield* outbox.listByCommandId(CommandId.make(`${createCommandId}:release`)));
        const changed = yield* launches
          .launch({ ...input, legacyBootstrap: { ...policy, payloadHash: "changed" } })
          .pipe(Effect.flip);
        assert.equal(changed._tag, "ThreadLaunchError");
        assert.lengthOf((yield* threads.getThreadProjection(base.threadId)).runs, 1);
      }).pipe(Effect.provide(harness.layer));
    }),
);
it.effect("returns a visible preparing message while provisioning is still blocked", () =>
  Effect.gen(function* () {
    const worktreeEntered = yield* Deferred.make<void>();
    const allowWorktree = yield* Deferred.make<void>();
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      createWorktree: (input) =>
        Deferred.succeed(worktreeEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowWorktree)),
          Effect.as({
            worktree: { path: input.path!, refName: input.newRefName, headSha: "abc" },
          } as never),
        ),
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:blocked",
        thread: "thread:launch:blocked",
        message: "Build the feature",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      assert.equal(launched.projection.messages[0]?.text, "Build the feature");
      assert.equal(launched.projection.runs[0]?.status, "preparing");
      assert.equal(
        launched.projection.turnItems.find((item) => item.type === "command_execution")?.status,
        "running",
      );
      yield* Deferred.await(worktreeEntered);
      let current = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(
        current.turnItems.find((item) => item.type === "command_execution")?.title,
        "Preparing worktree",
      );
      yield* Deferred.succeed(allowWorktree, undefined);
      const entered = yield* Deferred.await(setupEntered).pipe(
        Effect.timeoutOption(Duration.seconds(2)),
      );
      if (Option.isNone(entered)) {
        current = yield* threads.getThreadProjection(launched.threadId);
        assert.fail(
          `Setup was not reached; run=${current.runs[0]?.status ?? "missing"}, worklog=${current.turnItems.find((item) => item.type === "command_execution")?.title ?? "missing"}.`,
        );
      }
      current = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(
        current.turnItems.find((item) => item.type === "command_execution")?.title,
        "Starting setup script",
      );
      const prematureEffects = yield* outbox.listByCommandId(
        CommandId.make("command:launch:blocked:initial-message"),
      );
      assert.isEmpty(prematureEffects);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("provisions independent launches concurrently instead of behind a global semaphore", () =>
  Effect.gen(function* () {
    const setupCount = yield* Ref.make(0);
    const bothEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Ref.updateAndGet(setupCount, (count) => count + 1).pipe(
          Effect.tap((count) =>
            count === 2 ? Deferred.succeed(bothEntered, undefined) : Effect.void,
          ),
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const results = yield* Effect.all(
        [
          launches.launch(
            launchInput({
              command: "command:launch:concurrent-a",
              thread: "thread:launch:concurrent-a",
              message: "First",
              workspace: { type: "worktree", baseRef: "main", branch: "concurrent-a" },
            }),
          ),
          launches.launch(
            launchInput({
              command: "command:launch:concurrent-b",
              thread: "thread:launch:concurrent-b",
              message: "Second",
              workspace: { type: "worktree", baseRef: "main", branch: "concurrent-b" },
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      assert.deepEqual(
        results.map((result) => result.projection.runs[0]?.status),
        ["preparing", "preparing"],
      );
      yield* Deferred.await(bothEntered);
      assert.equal(yield* Ref.get(setupCount), 2);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("retires completed runless preparation before accepting later native work", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const sql = yield* SqlClient.SqlClient;
    const input = launchInput({
      command: "command:runless:complete",
      thread: "thread:runless:complete",
    });
    const launched = yield* launches.launch(input);
    yield* waitUntil(() =>
      sql<{ readonly state: string }>`SELECT state FROM orchestration_v2_worktree_path_admissions
      WHERE operation_id = ${`${input.commandId}:ordinary-preparation`}`.pipe(
        Effect.map((rows) => rows[0]?.state === "released"),
      ),
    );
    const rows = yield* sql<{
      readonly event_kind: string;
    }>`SELECT event_kind FROM orchestration_v2_ordinary_checkout_execution_associations ORDER BY ordinal`;
    assert.deepEqual(
      rows.map((row) => row.event_kind),
      ["bind", "retire"],
    );
    const use = (yield* sql<{
      readonly state: string;
      readonly outcome_json: string;
    }>`SELECT state, outcome_json FROM orchestration_v2_worktree_path_admissions`)[0]!;
    assert.equal(use.state, "released");
    assert.include(use.outcome_json, '"kind":"prepared_completed"');
    yield* threads.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("command:runless:later"),
      threadId: launched.threadId,
      messageId: MessageId.make("message:runless:later"),
      text: "Use the completed checkout",
      attachments: [],
      modelSelection,
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    assert.equal(
      (yield* threads.getThreadProjection(launched.threadId)).runs[0]?.status,
      "starting",
    );
    assert.equal(harness.createWorktree.mock.calls.length, 0);
    assert.equal(harness.removeWorktree.mock.calls.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect(
  "retires a failed preparation only after its actual checkout and managed-process absence are observed",
  () => {
    const harness = makeHarness({
      runSetup: () => Effect.die("Synthetic setup rejected before any process"),
    });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const sql = yield* SqlClient.SqlClient;
      const input = launchInput({
        command: "command:preparation:known-failure",
        thread: "thread:preparation:known-failure",
        message: "Preserve failure",
      });
      const launched = yield* launches.launch(input);
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((value) => value.runs[0]?.status === "failed")),
      );
      const use = (yield* sql<{
        readonly state: string;
        readonly outcome_json: string;
      }>`SELECT state, outcome_json FROM orchestration_v2_worktree_path_admissions`)[0]!;
      assert.equal(use.state, "released");
      assert.include(use.outcome_json, '"kind":"prepared_failed"');
      assert.deepEqual(
        (yield* sql<{
          readonly event_kind: string;
        }>`SELECT event_kind FROM orchestration_v2_ordinary_checkout_execution_associations ORDER BY ordinal`).map(
          (row) => row.event_kind,
        ),
        ["bind", "retire"],
      );
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).messages[0]?.text,
        "Preserve failure",
      );
      assert.equal(harness.createWorktree.mock.calls.length, 0);
      assert.equal(harness.removeWorktree.mock.calls.length, 0);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("closes original preparation when another launch on the same checkout fails", () =>
  Effect.gen(function* () {
    const entered = yield* Ref.make(false);
    const finalized = yield* Ref.make(false);
    const harness = makeHarness({
      runSetup: () =>
        Ref.set(entered, true).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Ref.set(finalized, true)),
        ),
    });
    const result = yield* Effect.exit(
      Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        yield* launches.launch(
          launchInput({
            command: "command:teardown:original",
            thread: "thread:teardown:original",
            message: "Original preparation",
          }),
        );
        yield* waitUntil(() => Ref.get(entered));
        yield* launches.launch(
          launchInput({
            command: "command:teardown:conflict",
            thread: "thread:teardown:conflict",
            message: "A conflicting physical checkout",
          }),
        );
      }).pipe(Effect.provide(harness.layer)),
    );
    assert.isTrue(Exit.isFailure(result));
    assert.isTrue(yield* Ref.get(entered));
    assert.isTrue(yield* Ref.get(finalized));
    assert.equal(harness.createWorktree.mock.calls.length, 0);
    assert.equal(harness.removeWorktree.mock.calls.length, 0);
  }),
);

it.effect("preserves an explicit bootstrap setup opt-out while releasing provider work", () =>
  Effect.gen(function* () {
    const harness = makeHarness({ runSetup: () => Effect.die("Setup must not run") });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const input = {
        ...launchInput({
          command: "command:launch:no-setup",
          thread: "thread:launch:no-setup",
          message: "Skip setup",
          workspace: { type: "worktree", baseRef: "main" },
        }),
        runSetupScript: false,
      };
      yield* launches.launch(input);
      yield* waitUntil(() =>
        outbox
          .listByCommandId(CommandId.make("command:launch:no-setup:release"))
          .pipe(Effect.map((effects) => effects.length === 1)),
      );
      assert.equal(harness.runSetup.mock.calls.length, 0);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("enqueues provider work only after setup has been initiated", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: ({ worktreePath: cwd }) =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({
            status: "started" as const,
            async: false,
            scriptId: "setup",
            scriptName: "Setup",
            scriptCommand: "vp install",
            terminalId: "setup",
            cwd,
            completion: Effect.succeed({ exitCode: 0, durationMs: 1 }),
          }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:release",
        thread: "thread:launch:release",
        message: "Start after setup",
        workspace: { type: "worktree", baseRef: "main", branch: "feature" },
      });
      const launched = yield* launches.launch(input);
      yield* Deferred.await(setupEntered);
      assert.isEmpty(
        yield* outbox.listByCommandId(CommandId.make("command:launch:release:release")),
      );
      yield* Deferred.succeed(allowSetup, undefined);
      yield* waitUntil(() =>
        outbox
          .listByCommandId(CommandId.make("command:launch:release:release"))
          .pipe(Effect.map((effects) => effects.length === 1)),
      );
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.runs[0]?.status, "starting");
      assert.equal(
        projection.checkpointScopes[0]?.cwd,
        nativeWorktreePath({
          worktreesDir: "/repo-worktrees",
          cwd: project.workspaceRoot,
          branch: "feature",
        }),
      );
      assert.equal(
        projection.turnItems.find((item) => item.type === "command_execution")?.status,
        "completed",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect(
  "queues follow-up messages behind preparation and checkpoints them in the final workspace",
  () =>
    Effect.gen(function* () {
      const setupEntered = yield* Deferred.make<void>();
      const failSetup = yield* Deferred.make<void>();
      const harness = makeHarness({
        runSetup: () =>
          Deferred.succeed(setupEntered, undefined).pipe(
            Effect.andThen(Deferred.await(failSetup)),
            Effect.andThen(Effect.fail(new Error("setup failed") as never)),
          ),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const launchSeed = "jones-queued-preparation-final-workspace-v1";
        const plannedUuid = yield* randomUuidV4.pipe(Random.withSeed(launchSeed));
        const plannedTemporaryBranch = buildTemporaryWorktreeBranchName(() =>
          plannedUuid.replaceAll("-", ""),
        );
        const plannedWorktreePath = nativeWorktreePath({
          worktreesDir: "/repo-worktrees",
          cwd: project.workspaceRoot,
          branch: plannedTemporaryBranch,
        });
        const launched = yield* launches
          .launch(
            launchInput({
              command: "command:launch:queued-during-preparation",
              thread: "thread:launch:queued-during-preparation",
              message: "Prepare the workspace",
              workspace: { type: "worktree", baseRef: "main" },
            }),
          )
          .pipe(Random.withSeed(launchSeed));
        yield* Deferred.await(setupEntered);

        const followUp = yield* threads.sendToThread({
          projectId,
          commandId: CommandId.make("command:launch:queued-follow-up"),
          threadId: launched.threadId,
          messageId: MessageId.make("message:launch:queued-follow-up"),
          text: "Run after preparation",
          attachments: [],
          mode: "auto",
          createdBy: "user",
          creationSource: "web",
        });
        assert.equal(followUp.delivery, "queued");
        assert.equal(followUp.run.status, "queued");
        assert.equal(
          (yield* threads.getThreadRecords(launched.threadId, ["nodes"])).nodes.find(
            (node) => node.runId === followUp.run.id && node.kind === "root_turn",
          )?.checkpointScopeId,
          null,
        );

        yield* Deferred.succeed(failSetup, undefined);
        yield* waitUntil(() =>
          threads
            .getThreadProjection(launched.threadId)
            .pipe(
              Effect.map(
                (projection) =>
                  projection.runs.find((run) => run.id === followUp.run.id)?.status === "starting",
              ),
            ),
        );

        const projection = yield* threads.getThreadProjection(launched.threadId);
        const rootNode = projection.nodes.find(
          (node) => node.runId === followUp.run.id && node.kind === "root_turn",
        );
        assert.isNotNull(rootNode?.checkpointScopeId);
        assert.equal(
          projection.checkpointScopes.find((scope) => scope.id === rootNode?.checkpointScopeId)
            ?.cwd,
          plannedWorktreePath,
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect.each([" /COMPACT ", "/logout"])(
  "uses the first conversation message for a title after %s",
  (nativeCommand) =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const launched = yield* launches.launch({
          ...launchInput({
            command: "compact-title",
            thread: "compact-title-thread",
            message: nativeCommand,
          }),
          generateTitle: true,
        });
        assert.isUndefined(
          (yield* threads.getThreadProjection(launched.threadId)).thread.titleRegeneration,
        );
        assert.isFalse(
          (yield* outbox.listByCommandId(CommandId.make("compact-title:initial-message"))).some(
            (effect) => effect.request.type === "thread-title.generate",
          ),
        );
        const commandId = CommandId.make("compact-title-conversation");
        const messageId = MessageId.make("compact-title-conversation-message");
        yield* threads.dispatch({
          type: "message.dispatch",
          commandId,
          threadId: launched.threadId,
          messageId,
          createdBy: "user",
          creationSource: "web",
          text: "Fix the failing parser",
          attachments: [],
          dispatchMode: { type: "defer_start" },
        });
        assert.equal(
          (yield* threads.getThreadProjection(launched.threadId)).thread.titleRegeneration
            ?.requestId,
          commandId,
        );
        assert.deepEqual(
          (yield* outbox.listByCommandId(commandId))
            .filter((effect) => effect.request.type === "thread-title.generate")
            .map((effect) => effect.request),
          [{ type: "thread-title.generate", kind: { type: "initial", messageId } }],
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect("keeps native maintenance commands out of steering and restart messages", () =>
  Effect.gen(function* () {
    for (const scenario of [
      {
        name: "compact-steer",
        first: "Fix the parser",
        next: " /COMPACT ",
        mode: "steer_active",
      },
      {
        name: "compact-restart",
        first: "Fix the parser",
        next: "/compact",
        mode: "restart_active",
      },
      {
        name: "logout-steer",
        first: "Fix the parser",
        next: "/logout",
        mode: "steer_active",
      },
      {
        name: "logout-restart",
        first: "Fix the parser",
        next: "/logout",
        mode: "restart_active",
      },
      {
        name: "steer-logout",
        first: "/logout",
        next: "Continue with the parser",
        mode: "steer_active",
      },
      {
        name: "steer-compaction",
        first: "/compact",
        next: "Continue with the parser",
        mode: "steer_active",
      },
    ] as const) {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const launched = yield* launches.launch(
          launchInput({
            command: `${scenario.name}:launch`,
            thread: scenario.name,
            message: scenario.first,
          }),
        );
        const before = yield* threads.getThreadProjection(launched.threadId);
        const targetRun = before.runs[0];
        if (targetRun === undefined) return yield* Effect.die("Launch must create a run");
        const commandId = CommandId.make(`${scenario.name}:message`);
        const failure = yield* threads
          .dispatch({
            type: "message.dispatch",
            commandId,
            threadId: launched.threadId,
            messageId: MessageId.make(`${scenario.name}:message`),
            createdBy: "user",
            creationSource: "web",
            text: scenario.next,
            attachments: [],
            dispatchMode: { type: scenario.mode, targetRunId: targetRun.id },
          })
          .pipe(Effect.flip);
        assert.include(
          String(failure.cause).toLowerCase(),
          scenario.name.includes("logout") ? "sign" : "context compaction",
        );
        const after = yield* threads.getThreadProjection(launched.threadId);
        assert.deepEqual(after.messages, before.messages);
        assert.deepEqual(after.thread.titleRegeneration, before.thread.titleRegeneration);
        assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
      }).pipe(Effect.provide(harness.layer), Effect.scoped);
    }
  }),
);

it.effect("arms durable title generation after accepting the first message", () =>
  Effect.gen(function* () {
    const harness = makeHarness({
      generateTitle: (input) =>
        Effect.succeed({
          title: input.previousTitle === undefined ? "Generated title" : "Regenerated title",
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
      const input = {
        ...launchInput({
          command: "command:launch:title-generation",
          thread: "thread:launch:title-generation",
          message: "Generate my title",
        }),
        title: "Generate my title",
        generateTitle: true,
      };
      const launched = yield* launches.launch(input);
      const generationCommandId = CommandId.make("command:launch:title-generation:initial-message");

      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.thread.title, "Generate my title");
      assert.equal(projection.thread.titleRegeneration?.requestId, generationCommandId);
      assert.deepEqual(
        (yield* outbox.listByCommandId(generationCommandId)).map((effect) => effect.request),
        [
          {
            type: "thread-title.generate",
            kind: { type: "initial", messageId: MessageId.make("Generate my title:id") },
          },
        ],
      );
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: generationCommandId,
        kind: { type: "initial", messageId: MessageId.make("Generate my title:id") },
      });
      const generated = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(generated.thread.title, "Generated title");
      assert.deepEqual(
        harness.generateThreadTitle.mock.calls[0]?.[0]?.modelSelection,
        DEFAULT_SERVER_SETTINGS.textGenerationModelSelection,
      );

      const manualRequestId = CommandId.make("command:title-generation:manual");
      yield* threads.dispatch({
        type: "thread.metadata.update",
        commandId: manualRequestId,
        threadId: launched.threadId,
        regenerateTitle: true,
      });
      assert.deepEqual(
        (yield* outbox.listByCommandId(manualRequestId)).map((effect) => effect.request),
        [{ type: "thread-title.generate", kind: { type: "regenerate" } }],
      );
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: manualRequestId,
        kind: { type: "regenerate" },
      });
      const regenerated = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(regenerated.thread.title, "Regenerated title");
      assert.equal(
        harness.generateThreadTitle.mock.calls[1]?.[0]?.previousTitle,
        "Generated title",
      );

      yield* threads.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("command:title-generation:user-rename"),
        threadId: launched.threadId,
        title: "Keep my title",
      });
      const renamed = yield* threads.getThreadProjection(launched.threadId);
      yield* TestClock.adjust(Duration.seconds(1));
      yield* threads.dispatch({
        type: "thread.title.regeneration.complete",
        commandId: CommandId.make("command:title-generation:stale-completion"),
        threadId: launched.threadId,
        requestId: generationCommandId,
        title: "Stale generated title",
      });
      const afterStaleCompletion = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(afterStaleCompletion.thread.title, "Keep my title");
      assert.equal(
        DateTime.toEpochMillis(afterStaleCompletion.thread.updatedAt),
        DateTime.toEpochMillis(renamed.thread.updatedAt),
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("does not update a reused thread title when the initial message is rejected", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make("thread:launch:reused-title-failure");
      yield* threads.dispatch({
        type: "thread.create",
        commandId: CommandId.make("command:launch:reused-title-failure:create"),
        threadId,
        projectId,
        title: "Original title",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });

      const commandId = CommandId.make("command:launch:reused-title-failure");
      const failed = yield* launches
        .launch({
          ...launchInput({
            command: commandId,
            thread: threadId,
            message: "Generate a provisional title",
          }),
          reuseExistingThread: true,
          title: "Generate a provisional title",
          generateTitle: true,
          modelSelection: {
            instanceId: ProviderInstanceId.make("missing-provider"),
            model: "missing-model",
          },
        })
        .pipe(Effect.exit);

      assert.isTrue(Exit.isFailure(failed));
      const projection = yield* threads.getThreadProjection(threadId);
      assert.equal(projection.thread.title, "Original title");
      assert.isUndefined(projection.thread.titleRegeneration);
      assert.isEmpty(projection.messages);
      assert.isEmpty(yield* outbox.listByCommandId(CommandId.make(`${commandId}:initial-message`)));
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("generates an initial title for an attachment-only message", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
      const messageId = MessageId.make("message:image-only");
      const input = {
        ...launchInput({
          command: "command:launch:image-only",
          thread: "thread:launch:image-only",
        }),
        title: "Image: screenshot.png",
        generateTitle: true,
        initialMessage: {
          messageId,
          text: "",
          attachments: [
            {
              type: "image" as const,
              id: "attachment-image-only",
              name: "screenshot.png",
              mimeType: "image/png",
              sizeBytes: 128,
            },
          ],
        },
      };

      const launched = yield* launches.launch(input);
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: CommandId.make("command:launch:image-only:initial-message"),
        kind: { type: "initial", messageId },
      });

      assert.equal(harness.generateThreadTitle.mock.calls[0]?.[0]?.message, "");
      assert.equal(
        harness.generateThreadTitle.mock.calls[0]?.[0]?.attachments?.[0]?.name,
        "screenshot.png",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("uses the available source control writer for generated worktree branches", () =>
  Effect.gen(function* () {
    const writerInstanceId = ProviderInstanceId.make("source-control-writer");
    const writerModelSelection = {
      instanceId: writerInstanceId,
      model: "branch-writer-model",
    } as const;
    const harness = makeHarness({
      serverSettings: {
        providerInstances: {
          [writerInstanceId]: {
            driver: ProviderDriverKind.make("codex"),
            config: {},
          },
        },
        sourceControlWriterModelSelection: writerModelSelection,
      },
      providers: [
        {
          instanceId: writerInstanceId,
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: null,
          status: "ready",
          auth: { status: "authenticated" },
          checkedAt: "2026-07-28T00:00:00.000Z",
          availability: "available",
          models: [],
          slashCommands: [],
          skills: [],
        },
      ],
    });

    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.launch(
        launchInput({
          command: "command:launch:source-control-writer",
          thread: "thread:launch:source-control-writer",
          message: "Generate a branch with the configured writer",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.generateBranchName.mock.calls.length === 1));
      assert.deepEqual(
        harness.generateBranchName.mock.calls[0]?.[0]?.modelSelection,
        writerModelSelection,
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("falls back when the source control writer is unavailable", () =>
  Effect.gen(function* () {
    const writerInstanceId = ProviderInstanceId.make("missing-source-control-writer");
    const harness = makeHarness({
      serverSettings: {
        providerInstances: {
          [writerInstanceId]: {
            driver: ProviderDriverKind.make("missing-driver"),
            config: {},
          },
        },
        sourceControlWriterModelSelection: {
          instanceId: writerInstanceId,
          model: "missing-branch-writer-model",
        },
      },
      providers: [],
    });

    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.launch(
        launchInput({
          command: "command:launch:source-control-writer-fallback",
          thread: "thread:launch:source-control-writer-fallback",
          message: "Generate a branch with the available writer",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.generateBranchName.mock.calls.length === 1));
      assert.deepEqual(
        harness.generateBranchName.mock.calls[0]?.[0]?.modelSelection,
        DEFAULT_SERVER_SETTINGS.textGenerationModelSelection,
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("runs a Scratch thread launched at the root in its own folder", () =>
  Effect.gen(function* () {
    // Only `projectId` stands in for the Scratch project here.
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const disposableRoot = yield* fs.makeTempDirectoryScoped({ prefix: "launch-scratch-" });
    const root = yield* fs.realPath(disposableRoot);
    const workspaceRoot = path.join(root, "project");
    const otherWorkspaceRoot = path.join(root, "other-project");
    const scratchRoot = path.join(root, "scratch");
    const worktreesDir = path.join(root, "worktrees");
    for (const directory of [workspaceRoot, otherWorkspaceRoot, scratchRoot, worktreesDir])
      yield* fs.makeDirectory(directory);
    const firstScratchFolder = path.join(scratchRoot, "folder-1");
    const claimed: Array<{ readonly threadId: ThreadId; readonly text: string }> = [];
    const setupEntered = yield* Deferred.make<void>();
    const harness = makeHarness({
      physicalFixture: true,
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(Effect.as({ status: "no-script" as const })),
      workspaceRoot,
      otherWorkspaceRoot,
      worktreesDir,
      managedFolders: Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: path.join(root, "projects"),
        folderForThread: (input) =>
          Effect.gen(function* () {
            if (input.projectId !== projectId) return Option.none();
            claimed.push({ threadId: input.threadId, text: input.text });
            const folder = path.join(scratchRoot, `folder-${claimed.length}`);
            yield* fs
              .makeDirectory(folder)
              .pipe(
                Effect.mapError(
                  (cause) => new ManagedProjectFolders.ScratchFolderError({ folder, cause }),
                ),
              );
            return Option.some(
              yield* fs
                .realPath(folder)
                .pipe(
                  Effect.mapError(
                    (cause) => new ManagedProjectFolders.ScratchFolderError({ folder, cause }),
                  ),
                ),
            );
          }),
      }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:scratch",
        thread: "thread:launch:scratch",
        message: "Convert these PNGs",
      });
      const launched = yield* launches.launch(input);
      assert.deepEqual(claimed, [{ threadId: launched.threadId, text: "Convert these PNGs" }]);
      assert.equal(launched.projection.thread.worktreePath, firstScratchFolder);
      yield* Deferred.await(setupEntered);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls[0]?.[0]?.worktreePath, firstScratchFolder);
      assert.equal(harness.createWorktree.mock.calls.length, 0);

      // A retry replays the first attempt and claims no second folder.
      const retried = yield* launches.launch(input);
      assert.isTrue(retried.resumed);
      assert.lengthOf(claimed, 1);
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.worktreePath,
        firstScratchFolder,
      );

      const other = yield* launches.launch({
        ...launchInput({
          command: "command:launch:scratch-other",
          thread: "thread:launch:scratch-other",
          message: "Elsewhere",
        }),
        projectId: otherProjectId,
      });
      assert.lengthOf(claimed, 1);
      assert.isNull(other.projection.thread.worktreePath);
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("refuses a retried Scratch root launch whose initial message changed", () =>
  Effect.gen(function* () {
    // Only `projectId` stands in for the Scratch project here.
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const disposableRoot = yield* fs.makeTempDirectoryScoped({ prefix: "launch-scratch-" });
    const root = yield* fs.realPath(disposableRoot);
    const workspaceRoot = path.join(root, "project");
    const otherWorkspaceRoot = path.join(root, "other-project");
    const scratchRoot = path.join(root, "scratch");
    const worktreesDir = path.join(root, "worktrees");
    for (const directory of [workspaceRoot, otherWorkspaceRoot, scratchRoot, worktreesDir])
      yield* fs.makeDirectory(directory);
    const firstScratchFolder = path.join(scratchRoot, "folder-1");
    const claimed: Array<{ readonly threadId: ThreadId; readonly text: string }> = [];
    const setupEntered = yield* Deferred.make<void>();
    const harness = makeHarness({
      physicalFixture: true,
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(Effect.as({ status: "no-script" as const })),
      workspaceRoot,
      otherWorkspaceRoot,
      worktreesDir,
      managedFolders: Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: path.join(root, "projects"),
        folderForThread: (input) =>
          Effect.gen(function* () {
            if (input.projectId !== projectId) return Option.none();
            claimed.push({ threadId: input.threadId, text: input.text });
            const folder = path.join(scratchRoot, `folder-${claimed.length}`);
            yield* fs
              .makeDirectory(folder)
              .pipe(
                Effect.mapError(
                  (cause) => new ManagedProjectFolders.ScratchFolderError({ folder, cause }),
                ),
              );
            return Option.some(
              yield* fs
                .realPath(folder)
                .pipe(
                  Effect.mapError(
                    (cause) => new ManagedProjectFolders.ScratchFolderError({ folder, cause }),
                  ),
                ),
            );
          }),
      }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const input = launchInput({
        command: "command:launch:scratch",
        thread: "thread:launch:scratch",
        message: "Convert these PNGs",
      });
      const launched = yield* launches.launch(input);
      assert.deepEqual(claimed, [{ threadId: launched.threadId, text: "Convert these PNGs" }]);
      assert.equal(launched.projection.thread.worktreePath, firstScratchFolder);
      yield* Deferred.await(setupEntered);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls[0]?.[0]?.worktreePath, firstScratchFolder);
      assert.equal(harness.createWorktree.mock.calls.length, 0);

      if (input.initialMessage === undefined) assert.fail("The initial message is required.");
      const retried = yield* launches
        .launch({
          ...input,
          initialMessage: { ...input.initialMessage, text: "Convert these SVGs" },
        })
        .pipe(Effect.result);
      assert.equal(retried._tag, "Failure");
      if (retried._tag === "Failure") {
        assert.isTrue(Schema.is(ThreadLaunch.ThreadLaunchError)(retried.failure));
        assert.equal(retried.failure.operation, "dispatch-message");
        let cause: unknown = retried.failure.cause;
        while (typeof cause === "object" && cause !== null && "cause" in cause) cause = cause.cause;
        assert.equal(cause, "Replay has another permanent original checkout command.");
      }
      assert.lengthOf(claimed, 1);
      assert.equal(harness.createWorktree.mock.calls.length, 0);
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("names the worktree itself when the client provides no branch", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:server-named-branch",
          thread: "thread:launch:server-named-branch",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.createWorktree.mock.calls.length === 1));
      assert.match(
        harness.createWorktree.mock.calls[0]?.[0]?.newRefName ?? "",
        /^t3code\/[0-9a-f]{8}$/u,
      );
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.thread.branch === "generated-branch")),
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("renames a temporary t3code/<hash> branch off the provisioning critical path", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const disposableRoot = yield* fs.makeTempDirectoryScoped({ prefix: "launch-temp-rename-" });
    const workspaceRoot = yield* fs.realPath(disposableRoot);
    const worktreesDir = path.join(workspaceRoot, "worktrees");
    const expectedWorktreePath = nativeWorktreePath({
      worktreesDir,
      cwd: workspaceRoot,
      branch: "t3code/abcd1234",
    });
    yield* fs.makeDirectory(path.dirname(expectedWorktreePath), { recursive: true });
    const renameInputs: Array<
      Parameters<GitWorkflow.GitWorkflowService["Service"]["renameBranch"]>[0]
    > = [];
    const renameGuards: Array<Effect.Effect<void, Error>> = [];
    let creationGuardCompletions = 0;
    let renameGuardCompletions = 0;
    const branchNameStarted = yield* Deferred.make<void>();
    const allowBranchName = yield* Deferred.make<void>();
    const harness = makeHarness({
      physicalFixture: true,
      workspaceRoot,
      worktreesDir,
      createWorktree: (input, options) =>
        Effect.gen(function* () {
          assert.equal(input.path, expectedWorktreePath);
          assert.isFalse(
            yield* fs.exists(expectedWorktreePath).pipe(
              Effect.mapError(
                (cause) =>
                  new GitCommandError({
                    operation: "GitVcsDriver.createWorktree",
                    command: "git worktree add",
                    cwd: input.cwd,
                    detail: "The scoped checkout absence could not be observed.",
                    cause,
                  }),
              ),
            ),
          );
          if (options?.revalidateMutation === undefined)
            return yield* Effect.fail(
              new GitCommandError({
                operation: "GitVcsDriver.createWorktree.revalidateOriginalActor",
                command: "git worktree add",
                cwd: input.cwd,
                detail: "The original creation actor guard is unavailable.",
              }),
            );
          yield* options.revalidateMutation.pipe(
            Effect.mapError(
              (cause) =>
                new GitCommandError({
                  operation: "GitVcsDriver.createWorktree.revalidateOriginalActor",
                  command: "git worktree add",
                  cwd: input.cwd,
                  detail: "The original creation actor no longer authorizes another mutation.",
                  cause,
                }),
            ),
          );
          creationGuardCompletions += 1;
          yield* fs.makeDirectory(expectedWorktreePath).pipe(
            Effect.mapError(
              (cause) =>
                new GitCommandError({
                  operation: "GitVcsDriver.createWorktree",
                  command: "git worktree add",
                  cwd: input.cwd,
                  detail: "The scoped synthetic checkout material could not be created.",
                  cause,
                }),
            ),
          );
          return {
            worktree: { path: input.path, refName: input.newRefName, headSha: "abc" },
          } as never;
        }),
      renameBranch: (input) =>
        Effect.gen(function* () {
          const guard = input.revalidateMutation;
          if (guard === undefined)
            return yield* Effect.fail(
              new GitCommandError({
                operation: "GitVcsDriver.renameBranch.revalidateOriginalActor",
                command: "git branch rename",
                cwd: input.cwd,
                detail: "The original rename actor guard is unavailable.",
              }),
            );
          renameInputs.push(input);
          renameGuards.push(guard);
          yield* guard.pipe(
            Effect.mapError(
              (cause) =>
                new GitCommandError({
                  operation: "GitVcsDriver.renameBranch.revalidateOriginalActor",
                  command: "git branch rename",
                  cwd: input.cwd,
                  detail: "The original rename actor no longer authorizes mutation or success.",
                  cause,
                }),
            ),
          );
          renameGuardCompletions += 1;
          return { branch: input.newBranch };
        }),
      generateBranchName: () =>
        Deferred.succeed(branchNameStarted, undefined).pipe(
          Effect.andThen(Deferred.await(allowBranchName)),
          Effect.as({ branch: "generated-branch" }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:temp-branch",
          thread: "thread:launch:temp-branch",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main", branch: "t3code/abcd1234" },
        }),
      );
      yield* Deferred.await(branchNameStarted);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "t3code/abcd1234");
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
      );
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.branch,
        "t3code/abcd1234",
      );
      yield* Deferred.succeed(allowBranchName, undefined);
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.thread.branch === "generated-branch")),
      );
      const originalRenameGuard = renameGuards[0];
      if (originalRenameGuard === undefined)
        return yield* Effect.die("The original captured rename guard is unavailable.");
      assert.deepEqual(harness.renameBranch.mock.calls[0]?.[0], {
        cwd: expectedWorktreePath,
        oldBranch: "t3code/abcd1234",
        newBranch: "generated-branch",
        revalidateMutation: originalRenameGuard,
      });
      assert.equal(creationGuardCompletions, 1);
      assert.equal(renameGuardCompletions, 1);
      assert.lengthOf(renameGuards, 1);
      assert.strictEqual(renameInputs[0], harness.renameBranch.mock.calls[0]?.[0]);
      assert.strictEqual(renameInputs[0]?.revalidateMutation, renameGuards[0]);
      assert.equal(yield* fs.realPath(expectedWorktreePath), expectedWorktreePath);
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("keeps an explicit branch name instead of generating one", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.launch(
        launchInput({
          command: "command:launch:explicit-branch",
          thread: "thread:launch:explicit-branch",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main", branch: "my-feature" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.createWorktree.mock.calls.length === 1));
      assert.equal(harness.generateBranchName.mock.calls.length, 0);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "my-feature");
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("keeps the temporary branch when branch generation fails", () =>
  Effect.gen(function* () {
    const harness = makeHarness({
      createWorktree: (input) =>
        Effect.succeed({
          worktree: { path: input.path, refName: input.newRefName, headSha: "abc" },
        } as never),
      generateBranchName: () => Effect.die("branch generation is down"),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:branch-fallback",
          thread: "thread:launch:branch-fallback",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main", branch: "t3code/abcd1234" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.generateBranchName.mock.calls.length === 1));
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "t3code/abcd1234");
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
      );
      assert.equal(harness.renameBranch.mock.calls.length, 0);
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.branch,
        "t3code/abcd1234",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("renames a temporary branch on an existing worktree to a generated name", () =>
  Effect.gen(function* () {
    type OriginalRenameInput = Parameters<NonNullable<HarnessOptions["renameBranch"]>>[0];
    const renameInputs: Array<OriginalRenameInput> = [];
    const renameGuards: Array<NonNullable<OriginalRenameInput["revalidateMutation"]>> = [];
    let renameGuardCompletions = 0;
    const harness = makeHarness({
      renameBranch: (input) =>
        Effect.gen(function* () {
          const guard = input.revalidateMutation;
          if (guard === undefined)
            return yield* new GitCommandError({
              operation: "GitVcsDriver.renameBranch.revalidateOriginalActor",
              command: "git branch rename",
              cwd: input.cwd,
              detail: "The original rename actor guard is unavailable.",
            });
          renameInputs.push(input);
          renameGuards.push(guard);
          yield* guard.pipe(
            Effect.mapError(
              (cause) =>
                new GitCommandError({
                  operation: "GitVcsDriver.renameBranch.revalidateOriginalActor",
                  command: "git branch rename",
                  cwd: input.cwd,
                  detail: "The original rename actor no longer authorizes mutation or success.",
                  cause,
                }),
            ),
          );
          renameGuardCompletions += 1;
          return { branch: input.newBranch };
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:existing-worktree-rename",
          thread: "thread:launch:existing-worktree-rename",
          message: "Build the feature",
          workspace: {
            type: "existing_worktree",
            worktreePath: "/repo-worktrees/t3code-abcd1234",
            branch: "t3code/abcd1234",
          },
        }),
      );
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.thread.branch === "generated-branch")),
      );
      const originalRenameGuard = renameGuards[0];
      if (originalRenameGuard === undefined)
        return yield* Effect.die("The original captured rename guard is unavailable.");
      assert.deepEqual(harness.renameBranch.mock.calls[0]?.[0], {
        cwd: "/repo-worktrees/t3code-abcd1234",
        oldBranch: "t3code/abcd1234",
        newBranch: "generated-branch",
        revalidateMutation: originalRenameGuard,
      });
      assert.lengthOf(renameGuards, 1);
      assert.equal(renameGuardCompletions, 1);
      assert.strictEqual(renameInputs[0], harness.renameBranch.mock.calls[0]?.[0]);
      assert.strictEqual(renameInputs[0]?.revalidateMutation, originalRenameGuard);
    }).pipe(Effect.provide(harness.layer));
  }),
);

function worktreeBaseRef(name: string, overrides: Partial<VcsRef> = {}): VcsRef {
  return {
    name,
    isRemote: false,
    current: false,
    isDefault: false,
    worktreePath: null,
    ...overrides,
  };
}

it.effect("accepts the first message while its automatic worktree base is still loading", () =>
  Effect.gen(function* () {
    const refsEntered = yield* Deferred.make<void>();
    const allowRefs = yield* Deferred.make<void>();
    const harness = makeHarness({
      listRefs: () =>
        Deferred.succeed(refsEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowRefs)),
          Effect.as({
            refs: [worktreeBaseRef("develop", { isDefault: true })],
            isRepo: true,
            hasPrimaryRemote: true,
            nextCursor: null,
            totalCount: 1,
          }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const input = launchInput({
        command: "command:automatic-base-loading",
        thread: "thread:automatic-base-loading",
        message: "Start while branches load",
        workspace: { type: "worktree", branch: "feature" },
      });
      const launched = yield* launches.launch(input);
      assert.equal(launched.projection.messages[0]?.text, "Start while branches load");
      assert.equal(launched.projection.runs[0]?.status, "preparing");
      yield* Deferred.await(refsEntered);
      assert.isNull((yield* threads.getThreadProjection(launched.threadId)).thread.worktreePath);
      const sql = yield* SqlClient.SqlClient;
      const admissions = yield* sql<{
        readonly canonical_path: string;
        readonly resource_path: string;
      }>`
        SELECT json_extract(admission_json, '$.capture.canonicalCheckoutPath') AS canonical_path,
          lease.resource_path
        FROM orchestration_v2_ordinary_checkout_admissions admission
        JOIN worktree_ownership_leases lease
          ON lease.resource_path = json_extract(admission_json, '$.capture.canonicalCheckoutPath')
        WHERE admission.command_id = ${`${input.commandId}:initial-message`}
          AND lease.owner_thread_id = ${launched.threadId}`;
      const plannedPath = nativeWorktreePath({
        worktreesDir: "/repo-worktrees",
        cwd: project.workspaceRoot,
        branch: "feature",
      });
      assert.equal(admissions.length, 1);
      assert.equal(admissions[0]?.canonical_path, plannedPath);
      assert.equal(admissions[0]?.resource_path, plannedPath);
      const replay = yield* launches.launch(input);
      assert.isTrue(replay.resumed);
      assert.equal(replay.threadId, launched.threadId);
      assert.isNull(replay.projection.thread.worktreePath);
      assert.equal(harness.createWorktree.mock.calls.length, 0);
      assert.isNull((yield* tracker.get(launched.threadId))?.baseRef);
      yield* Deferred.succeed(allowRefs, undefined);
      yield* awaitStartingRun(threads, launched.threadId);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0].refName, "develop");
      assert.equal(harness.createWorktree.mock.calls[0]?.[0].path, plannedPath);
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.worktreePath,
        plannedPath,
      );
      assert.equal((yield* tracker.get(launched.threadId))?.baseRef, "develop");
      assert.equal((yield* threads.getThreadProjection(launched.threadId)).messages.length, 1);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each([
  {
    name: "default over checked-out",
    refs: [
      worktreeBaseRef("feature", { current: true }),
      worktreeBaseRef("develop", { isDefault: true }),
    ],
    expected: "develop",
  },
  {
    name: "local-only checked-out",
    refs: [worktreeBaseRef("local", { current: true })],
    expected: "local",
  },
  {
    name: "detached remote default",
    refs: [
      worktreeBaseRef("origin/develop", { isDefault: true, isRemote: true, remoteName: "origin" }),
    ],
    expected: "origin/develop",
  },
])("resolves an omitted V2 base from $name", ({ refs, expected }) => {
  const harness = makeHarness({
    listRefs: () =>
      Effect.succeed({
        refs,
        isRepo: true,
        hasPrimaryRemote: true,
        nextCursor: null,
        totalCount: refs.length,
      }),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:automatic-base",
        thread: "thread:automatic-base",
        message: "Start",
        workspace: { type: "worktree", branch: "feature" },
      }),
    );
    yield* awaitStartingRun(threads, launched.threadId);
    assert.equal(harness.createWorktree.mock.calls[0]?.[0].refName, expected);
    assert.equal(harness.createWorktree.mock.calls[0]?.[0].baseRefName, expected);
  }).pipe(Effect.provide(harness.layer));
});

it.effect.each([
  { name: "non-repository", isRepo: false, refs: [], detail: "requires a Git repository" },
  { name: "empty repository", isRepo: true, refs: [], detail: "Select a base branch" },
  {
    name: "detached local-only repository",
    isRepo: true,
    refs: [worktreeBaseRef("feature")],
    detail: "Select a base branch",
  },
])(
  "keeps the first message visible when automatic base resolution fails for $name",
  ({ isRepo, refs, detail }) => {
    const harness = makeHarness({
      listRefs: () =>
        Effect.succeed({
          refs,
          isRepo,
          hasPrimaryRemote: false,
          nextCursor: null,
          totalCount: refs.length,
        }),
    });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:automatic-base-failed",
          thread: "thread:automatic-base-failed",
          message: "Keep this message",
          workspace: { type: "worktree", branch: "feature" },
        }),
      );
      yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status === "failed",
        ),
        Stream.runHead,
      );
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.messages[0]?.text, "Keep this message");
      assert.isNull(projection.thread.worktreePath);
      assert.include(
        projection.turnItems.find((item) => item.type === "error")?.failure.message ?? "",
        detail,
      );
      assert.equal(harness.createWorktree.mock.calls.length, 0);
      assert.equal(harness.runSetup.mock.calls.length, 0);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("preserves an explicit V2 base without looking up the default", () => {
  const harness = makeHarness({
    listRefs: () => Effect.die("Explicit base must not resolve the default"),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:explicit-base",
        thread: "thread:explicit-base",
        message: "Start",
        workspace: { type: "worktree", baseRef: "release/stable", branch: "feature" },
      }),
    );
    yield* awaitStartingRun(threads, launched.threadId);
    assert.equal(harness.createWorktree.mock.calls[0]?.[0].refName, "release/stable");
  }).pipe(Effect.provide(harness.layer));
});

it.effect.each([
  {
    name: "local base",
    base: "main",
    refs: [],
    expectedBranch: "main",
    expectedStart: "pinned-origin-sha",
    fetchRef: "main",
  },
  {
    name: "origin base",
    base: "origin/release",
    refs: [worktreeBaseRef("origin/release", { isRemote: true, remoteName: "origin" })],
    expectedBranch: "release",
    expectedStart: "pinned-origin-sha",
    fetchRef: "origin/release",
  },
  {
    name: "local origin-prefixed branch",
    base: "origin/release",
    refs: [worktreeBaseRef("origin/release")],
    expectedBranch: "origin/release",
    expectedStart: "pinned-origin-sha",
    fetchRef: undefined,
  },
  {
    name: "other remote",
    base: "upstream/release",
    refs: [worktreeBaseRef("upstream/release", { isRemote: true, remoteName: "upstream" })],
    expectedBranch: null,
    expectedStart: "upstream/release",
    fetchRef: undefined,
  },
  {
    name: "missing origin branch",
    base: "local-only",
    refs: [],
    expectedBranch: "local-only",
    expectedStart: "local-only",
    fetchRef: "local-only",
  },
])(
  "pins the fetched V2 worktree base with one lookup for $name",
  ({ base, refs, expectedBranch, expectedStart, fetchRef }) => {
    const operations: string[] = [];
    const fetchRemote = vi.fn((_: Parameters<NonNullable<HarnessOptions["fetchRemote"]>>[0]) =>
      Effect.sync(() => {
        operations.push("fetch");
      }),
    );
    const resolveRemoteTrackingCommitIfExists = vi.fn(
      (_: Parameters<NonNullable<HarnessOptions["resolveRemoteTrackingCommitIfExists"]>>[0]) =>
        Effect.sync(() => {
          operations.push("resolve");
          return base === "local-only"
            ? null
            : { commitSha: "pinned-origin-sha", remoteRefName: `origin/${expectedBranch}` };
        }),
    );
    const harness = makeHarness({
      listRefs: () =>
        Effect.succeed({
          refs,
          isRepo: true,
          hasPrimaryRemote: true,
          nextCursor: null,
          totalCount: refs.length,
        }),
      fetchRemote,
      resolveRemoteTrackingCommitIfExists,
    });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:pinned-base",
          thread: "thread:pinned-base",
          message: "Start from origin",
          workspace: { type: "worktree", baseRef: base, branch: "feature", startFromOrigin: true },
        }),
      );
      yield* awaitStartingRun(threads, launched.threadId);
      assert.deepEqual(operations, expectedBranch === null ? ["fetch"] : ["fetch", "resolve"]);
      assert.equal(fetchRemote.mock.calls.length, 1);
      assert.equal(fetchRemote.mock.calls[0]?.[0].refName, fetchRef);
      assert.equal(
        resolveRemoteTrackingCommitIfExists.mock.calls.length,
        expectedBranch === null ? 0 : 1,
      );
      if (expectedBranch !== null)
        assert.equal(
          resolveRemoteTrackingCommitIfExists.mock.calls[0]?.[0].branchName,
          expectedBranch,
        );
      assert.equal(harness.createWorktree.mock.calls[0]?.[0].refName, expectedStart);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0].baseRefName, base);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("keeps lookup failures visible without creating a worktree or starting its run", () => {
  const harness = makeHarness({
    resolveRemoteTrackingCommitIfExists: () =>
      Effect.fail(
        new GitCommandError({
          operation: "GitVcsDriver.resolveRemoteTrackingCommitIfExists",
          cwd: "/repo",
          command: "git rev-parse",
          detail: "Remote ref lookup failed",
        }),
      ),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:lookup-failed",
        thread: "thread:lookup-failed",
        message: "Keep this message",
        workspace: { type: "worktree", baseRef: "main", branch: "feature", startFromOrigin: true },
      }),
    );
    yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
      Stream.filter(
        (stored) => stored.event.type === "run.updated" && stored.event.payload.status === "failed",
      ),
      Stream.runHead,
    );
    const projection = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(projection.messages[0]?.text, "Keep this message");
    assert.include(
      projection.turnItems.find((item) => item.type === "error")?.failure.message ?? "",
      "Remote ref lookup failed",
    );
    assert.equal(harness.createWorktree.mock.calls.length, 0);
    assert.equal(harness.runSetup.mock.calls.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("shows the fetch diagnosis when preparing a worktree from origin fails", () => {
  const detail =
    "Git could not authenticate with the remote. Check Git credentials or SSH access on the server, then retry.";
  const harness = makeHarness({
    fetchRemote: () =>
      Effect.fail(
        new GitCommandError({
          operation: "GitVcsDriver.fetchRemote",
          command: "git",
          cwd: project.workspaceRoot,
          detail,
          exitCode: 128,
        }),
      ),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:fetch-failure",
        thread: "thread:launch:fetch-failure",
        message: "Start from origin",
        workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
      }),
    );
    yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
      Stream.filter(
        (stored) => stored.event.type === "run.updated" && stored.event.payload.status === "failed",
      ),
      Stream.runHead,
    );
    const projection = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(projection.messages[0]?.text, "Start from origin");
    assert.equal(projection.runs[0]?.status, "failed");
    assert.isNull(projection.thread.worktreePath);
    assert.equal(
      projection.turnItems.find((item) => item.type === "command_execution")?.status,
      "failed",
    );
    assert.include(
      projection.turnItems.find((item) => item.type === "error")?.failure.message ?? "",
      detail,
    );
    assert.equal(harness.createWorktree.mock.calls.length, 0);
    assert.equal(harness.runSetup.mock.calls.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

const originalRetryFixtureSeed = "jones-original-launch-retry-physical-v1";
const planOriginalRetryFixtureChild = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const config = yield* ServerConfig.ServerConfig;
  const projects = yield* ProjectService.ProjectService;
  const originalProject = Option.getOrNull(yield* projects.getById(projectId));
  if (originalProject === null)
    return yield* Effect.die("The original retry fixture project is unavailable.");
  const canonicalProjectRoot = yield* fs.realPath(originalProject.workspaceRoot);
  const worktreesDir = yield* fs.realPath(config.worktreesDir);
  if (canonicalProjectRoot !== originalProject.workspaceRoot)
    return yield* Effect.die("The independently scoped retry project is not canonical.");
  // A separate seed instance evaluates only the original pure UUID program.
  // Expected identity uses fixture inputs, never accepted target/projection.
  const uuid = yield* randomUuidV4.pipe(Random.withSeed(originalRetryFixtureSeed));
  const branch = buildTemporaryWorktreeBranchName(() => uuid.replaceAll("-", ""));
  return {
    uuid,
    branch,
    canonicalProjectRoot,
    worktreesDir,
    expectedWorktreePath: nativeWorktreePath({
      worktreesDir,
      cwd: originalProject.workspaceRoot,
      branch,
    }),
  };
});

const acquireRetryRegressionObservation = Effect.gen(function* () {
  const observation: OriginalRetryPreparationObservation = {
    ready: yield* Deferred.make<Fiber.Fiber<unknown, unknown>>(),
    trackerAcquisitions: 0,
    beginInvocations: 0,
    beginEvaluations: 0,
    beginTerminals: 0,
    actualTracker: undefined,
    forwardedTracker: undefined,
    capture: undefined,
    beginExit: undefined,
    fiberExit: undefined,
    retryReady: yield* Deferred.make<Fiber.Fiber<unknown, unknown>>(),
    retryEntryPhase: "not_entered",
    retryAwaitState: "handle_unobserved",
    retryCapture: undefined,
    retryBeginExit: undefined,
    retryFiberExit: undefined,
  };
  return observation;
});

it.effect.each(["commit", "rollback"] as const)(
  "authentic original retry completion respects its enclosing %s transaction",
  (mode) =>
    Effect.gen(function* () {
      const realFixture = yield* acquireRealRetryFixture;
      const observation = yield* acquireRetryRegressionObservation;
      const probe: RetrySlotTransactionProbe = {
        mode,
        acquisitions: 0,
        invocations: 0,
        evaluations: 0,
        beforeRefusal: false,
        afterSlot: undefined,
        afterSlotQuery: undefined,
        readInvocations: [],
        readSuccesses: [],
        originalRecord: undefined,
        publicationPhase: "not_entered",
        publicationRecord: undefined,
        rollbackFactsEqual: false,
        rollbackCause: undefined,
        rollbackObservation: undefined,
        serving: undefined,
      };
      const harness = makeRealRetryHarness(realFixture, observation, probe);
      return yield* Effect.gen(function* () {
        const expected = yield* planOriginalRetryFixtureChild;
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const launched = yield* launches
          .launch(
            launchInput({
              command: "command:launch:retry",
              thread: "thread:launch:retry",
              message: "Retry me",
              workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
            }),
          )
          .pipe(Random.withSeed(originalRetryFixtureSeed));
        const fiber = yield* Deferred.await(observation.ready);
        observation.fiberExit = yield* Fiber.await(fiber);
        yield* waitUntil(() =>
          threads
            .getThreadProjection(launched.threadId)
            .pipe(Effect.map((projection) => projection.runs[0]?.status === "failed")),
        );
        assert.equal(probe.acquisitions, 1);
        assert.equal(probe.invocations, 1);
        assert.equal(probe.evaluations, 1);
        assert.isTrue(probe.beforeRefusal);
        const serving = probe.serving;
        if (serving === undefined)
          return yield* Effect.die("The transaction probe owner capture is unavailable.");
        assert.strictEqual(serving.sql, realFixture.audit.acquisitions[0]?.sql);
        assert.strictEqual(serving.sink, realFixture.audit.acquisitions[0]?.sink);
        assert.strictEqual(serving.lifetime, serving.sink.ordinaryCheckoutLifetime);
        assert.notStrictEqual(serving.facade, serving.sink);
        assert.notStrictEqual(serving.facade.ordinaryCheckoutLifetime, serving.lifetime);
        if (mode === "commit") {
          assert.strictEqual(serving.originalReader, serving.lifetime.readOriginalPreparedRetry);
          assert.strictEqual(
            serving.forwardedReader,
            serving.facade.ordinaryCheckoutLifetime?.readOriginalPreparedRetry,
          );
          assert.notStrictEqual(serving.forwardedReader, serving.originalReader);
        } else {
          assert.strictEqual(
            serving.facade.ordinaryCheckoutLifetime?.readOriginalPreparedRetry,
            serving.lifetime.readOriginalPreparedRetry,
          );
        }
        assert.strictEqual(
          serving.facade.ordinaryCheckoutLifetime?.revalidateExecution,
          serving.lifetime.revalidateExecution,
        );
        assert.strictEqual(serving.facade.readByCommandId, serving.sink.readByCommandId);
        assert.strictEqual(serving.transactionService, serving.sql.transactionService);
        assert.equal(realFixture.audit.factories, 1);
        assert.equal(realFixture.audit.acquisitions.length, 1);
        assert.equal(realFixture.audit.launchers.length, 1);
        assert.equal(realFixture.audit.fetchFailures.length, 1);
        assert.equal(realFixture.audit.unsupportedFetchFailures, 0);
        assert.deepEqual(realFixture.audit.observationFailures, []);
        assert.deepEqual(realFixture.audit.controls, []);
        assert.equal(harness.createWorktree.mock.calls.length, 0);
        assert.equal(harness.runSetup.mock.calls.length, 0);
        assert.isFalse(yield* realFixture.fs.exists(expected.expectedWorktreePath));
        assert.equal(
          yield* realFixture.git(realFixture.workspaceRoot, ["rev-parse", "HEAD"]),
          realFixture.parentHead,
        );
        if (mode === "commit") {
          assert.isDefined(observation.fiberExit);
          assert.strictEqual(observation.capture?.executingFiber, fiber);
          assert.equal(probe.publicationPhase, "completed");
          const originalRecord = probe.originalRecord;
          if (originalRecord === undefined)
            return yield* Effect.die("The original committed preparation identity is unavailable.");
          assert.strictEqual(probe.publicationRecord, originalRecord);
          assert.isTrue(probe.beforeRefusal);
          const failed = yield* threads.getThreadProjection(launched.threadId);
          const retry = {
            commandId: CommandId.make("command:launch:retry:1"),
            threadId: launched.threadId,
            runId: failed.runs[0]!.id,
          };
          observation.retryEntryPhase = "before_retryPreparation";
          yield* launches.retryPreparation(retry);
          observation.retryAwaitState = "awaiting_handle";
          const retryFiber = yield* Deferred.await(observation.retryReady);
          assert.notStrictEqual(retryFiber, fiber);
          assert.strictEqual(observation.retryCapture?.executingFiber, retryFiber);
          observation.retryAwaitState = "awaiting_fiber_exit";
          observation.retryFiberExit = yield* Fiber.await(retryFiber);
          observation.retryAwaitState = "fiber_exit_observed";
          assert.lengthOf(probe.readInvocations, 2);
          assert.lengthOf(probe.readSuccesses, 2);
          const firstInvocation = probe.readInvocations[0];
          const secondInvocation = probe.readInvocations[1];
          const firstSuccess = probe.readSuccesses[0];
          const secondSuccess = probe.readSuccesses[1];
          if (
            firstInvocation === undefined ||
            secondInvocation === undefined ||
            firstSuccess === undefined ||
            secondSuccess === undefined
          )
            return yield* Effect.die("Both original retry reads and successes are required.");
          assert.strictEqual(firstInvocation.input, secondInvocation.input);
          assert.strictEqual(firstSuccess.input, firstInvocation.input);
          assert.strictEqual(secondSuccess.input, secondInvocation.input);
          assert.deepEqual(firstInvocation.input, originalRecord.query);
          assert.deepEqual(secondInvocation.input, originalRecord.query);
          assert.deepEqual(firstInvocation.input.targetSource, originalRecord.query.targetSource);
          assert.deepEqual(secondInvocation.input.targetSource, originalRecord.query.targetSource);
          assert.strictEqual(firstInvocation.slot, firstSuccess.slot);
          assert.strictEqual(secondInvocation.slot, secondSuccess.slot);
          assert.strictEqual(firstSuccess.slot, secondSuccess.slot);
          assert.strictEqual(firstSuccess.slot.ref, secondSuccess.slot.ref);
          for (const successfulRead of probe.readSuccesses) {
            assert.deepEqual(successfulRead.slot.ref, originalRecord.ref);
            assert.equal(
              successfulRead.slot.ref.originalUse.operationId,
              originalRecord.ref.originalUse.operationId,
            );
            assert.deepEqual(successfulRead.slot.ref.originalUse, originalRecord.ref.originalUse);
            assert.deepEqual(successfulRead.slot.ref.executor, originalRecord.ref.executor);
            assert.deepEqual(
              successfulRead.slot.ref.originalUse.admission,
              ordinaryCheckoutAdmissionRefV1(originalRecord.admission),
            );
            assert.deepEqual(
              ordinaryCheckoutLeaseIdentityV1(successfulRead.slot.ref.originalUse.lease),
              ordinaryCheckoutLeaseIdentityV1(originalRecord.admission.capture.lease),
            );
            assert.deepEqual(successfulRead.slot.ref, originalRecord.history.participants[0]?.ref);
            assert.deepEqual(
              successfulRead.slot.ref,
              originalRecord.history.facts[originalRecord.history.latestOrdinal]?.ref,
            );
            assert.strictEqual(successfulRead.slot.outcome, originalRecord.outcome);
            assert.equal(successfulRead.slot.associationOrdinal, originalRecord.associationOrdinal);
          }
          assert.strictEqual(probe.afterSlotQuery, firstInvocation.input);
          assert.strictEqual(probe.afterSlotQuery, secondInvocation.input);
          assert.strictEqual(probe.afterSlot, firstSuccess.slot);
          assert.strictEqual(probe.afterSlot, secondSuccess.slot);
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          yield* waitUntil(() =>
            outbox
              .listByCommandId(CommandId.make("command:launch:retry:1:release"))
              .pipe(Effect.map((effects) => effects.length === 1)),
          );
          const retried = yield* threads.getThreadProjection(launched.threadId);
          assert.equal(retried.runs.length, 1);
          assert.equal(retried.runs[0]?.status, "starting");
          assert.equal(retried.thread.worktreePath, expected.expectedWorktreePath);
          assert.equal(harness.createWorktree.mock.calls.length, 1);
          assert.isTrue(yield* realFixture.fs.exists(expected.expectedWorktreePath));
          assert.deepEqual(realFixture.audit.controls, []);
          assert.isFalse(probe.rollbackFactsEqual);
        } else {
          assert.isUndefined(probe.afterSlot);
          const rollbackObservation = probe.rollbackObservation;
          if (
            rollbackObservation === undefined ||
            rollbackObservation.baseline === undefined ||
            probe.rollbackCause === undefined
          )
            return yield* Effect.die("The authentic original rollback observation is unavailable.");
          assert.equal(
            rollbackObservation.operationId,
            "command:launch:retry:ordinary-preparation",
          );
          if (Exit.isFailure(rollbackObservation.exit))
            return yield* Effect.failCause(rollbackObservation.exit.cause);
          assert.deepEqual(rollbackObservation.exit.value, rollbackObservation.baseline);
          probe.rollbackFactsEqual = true;
          assert.isTrue(probe.rollbackFactsEqual);
          const rollbackError = Cause.findError(probe.rollbackCause);
          if (Result.isFailure(rollbackError) || !SqlError.isSqlError(rollbackError.success))
            return yield* Effect.die("The original enclosing rollback has no typed SQL cause.");
          assert.equal(rollbackError.success.reason._tag, "ConstraintError");
        }
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

function runAuthenticRetainedReaderRegression(variant: RetainedReaderVariant) {
  return Effect.gen(function* () {
    const realFixture = yield* acquireRealRetryFixture;
    const observation = yield* acquireRetryRegressionObservation;
    const probe: RetrySlotTransactionProbe = {
      mode: "commit",
      acquisitions: 0,
      invocations: 0,
      evaluations: 0,
      beforeRefusal: false,
      afterSlot: undefined,
      afterSlotQuery: undefined,
      readInvocations: [],
      readSuccesses: [],
      originalRecord: undefined,
      publicationPhase: "not_entered",
      publicationRecord: undefined,
      rollbackFactsEqual: false,
      rollbackCause: undefined,
      rollbackObservation: undefined,
      serving: undefined,
    };
    const readerProbe: RetainedReaderProbe = {
      variant,
      preparation: observation,
      invocations: 0,
      negativeReads: 0,
      mutations: 0,
      refusal: undefined,
      changedQuery: undefined,
      authenticQuery: undefined,
      originalSlot: undefined,
      rollback: undefined,
      rollbackCause: undefined,
      restoredFacts: false,
    };
    const harness = makeRealRetryHarness(realFixture, observation, probe, readerProbe);
    return yield* Effect.gen(function* () {
      const expected = yield* planOriginalRetryFixtureChild;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches
        .launch(
          launchInput({
            command: "command:launch:retry",
            thread: "thread:launch:retry",
            message: "Retry me",
            workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
          }),
        )
        .pipe(Random.withSeed(originalRetryFixtureSeed));
      const fiber = yield* Deferred.await(observation.ready);
      observation.fiberExit = yield* Fiber.await(fiber);
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "failed")),
      );
      assert.equal(probe.acquisitions, 1);
      assert.equal(probe.invocations, 1);
      assert.equal(probe.evaluations, 1);
      assert.isTrue(probe.beforeRefusal);
      const serving = probe.serving;
      const record = probe.originalRecord;
      if (serving === undefined || record === undefined)
        return yield* Effect.die("The authentic original reader foundation is unavailable.");
      assert.strictEqual(serving.sql, realFixture.audit.acquisitions[0]?.sql);
      assert.strictEqual(serving.sink, realFixture.audit.acquisitions[0]?.sink);
      assert.strictEqual(serving.lifetime, serving.sink.ordinaryCheckoutLifetime);
      assert.strictEqual(serving.originalReader, serving.lifetime.readOriginalPreparedRetry);
      assert.notStrictEqual(serving.facade, serving.sink);
      assert.notStrictEqual(serving.forwardedReader, serving.originalReader);
      assert.strictEqual(
        serving.forwardedReader,
        serving.facade.ordinaryCheckoutLifetime?.readOriginalPreparedRetry,
      );
      assert.strictEqual(
        serving.facade.ordinaryCheckoutLifetime?.revalidateExecution,
        serving.lifetime.revalidateExecution,
      );
      assert.strictEqual(serving.transactionService, serving.sql.transactionService);
      assert.equal(probe.publicationPhase, "completed");
      assert.strictEqual(probe.publicationRecord, record);
      assert.strictEqual(observation.capture?.executingFiber, fiber);
      assert.equal(realFixture.audit.factories, 1);
      assert.lengthOf(realFixture.audit.acquisitions, 1);
      assert.lengthOf(realFixture.audit.launchers, 1);
      assert.lengthOf(realFixture.audit.fetchFailures, 1);
      assert.equal(realFixture.audit.unsupportedFetchFailures, 0);
      assert.deepEqual(realFixture.audit.observationFailures, []);
      assert.deepEqual(realFixture.audit.controls, []);
      assert.equal(harness.createWorktree.mock.calls.length, 0);
      assert.equal(harness.runSetup.mock.calls.length, 0);
      assert.isFalse(yield* realFixture.fs.exists(expected.expectedWorktreePath));
      assert.equal(
        yield* realFixture.git(realFixture.workspaceRoot, ["rev-parse", "HEAD"]),
        realFixture.parentHead,
      );
      const failed = yield* threads.getThreadProjection(launched.threadId);
      observation.retryEntryPhase = "before_retryPreparation";
      yield* launches.retryPreparation({
        commandId: CommandId.make("command:launch:retry:1"),
        threadId: launched.threadId,
        runId: failed.runs[0]!.id,
      });
      const retryFiber = yield* Deferred.await(observation.retryReady);
      assert.notStrictEqual(retryFiber, fiber);
      assert.strictEqual(observation.retryCapture?.executingFiber, retryFiber);
      observation.retryFiberExit = yield* Fiber.await(retryFiber);
      assert.equal(readerProbe.invocations, 2);
      assert.equal(readerProbe.negativeReads, 1);
      assert.isDefined(readerProbe.refusal);
      assert.isTrue(readerProbe.restoredFacts);
      assert.equal(readerProbe.mutations, variant === "multiple released operations" ? 1 : 0);
      if (variant === "multiple released operations") {
        assert.isUndefined(readerProbe.changedQuery);
        assert.isDefined(readerProbe.rollback);
      } else {
        const changed = readerProbe.changedQuery;
        if (changed === undefined)
          return yield* Effect.die("The single altered query is unavailable.");
        const expectedQuery =
          variant === "messageId"
            ? { ...record.query, messageId: changed.messageId }
            : variant === "projectId"
              ? { ...record.query, projectId: changed.projectId }
              : variant === "branch"
                ? { ...record.query, branch: changed.branch }
                : {
                    ...record.query,
                    targetSource: {
                      ...record.query.targetSource,
                      worktreePath: changed.targetSource.worktreePath,
                    },
                  };
        assert.deepEqual(changed, expectedQuery);
        assert.notDeepEqual(changed, record.query);
      }
      assert.lengthOf(probe.readInvocations, 2);
      assert.lengthOf(probe.readSuccesses, 2);
      const first = probe.readSuccesses[0];
      const second = probe.readSuccesses[1];
      if (first === undefined || second === undefined)
        return yield* Effect.die("Both genuine retry reader results are required.");
      assert.strictEqual(first.input, second.input);
      assert.strictEqual(readerProbe.authenticQuery, first.input);
      assert.deepEqual(first.input, record.query);
      assert.deepEqual(second.input, record.query);
      assert.strictEqual(first.slot, second.slot);
      assert.strictEqual(first.slot, readerProbe.originalSlot);
      assert.strictEqual(first.slot.ref, second.slot.ref);
      assert.deepEqual(first.slot.ref, record.ref);
      assert.deepEqual(
        first.slot.ref.originalUse.admission,
        ordinaryCheckoutAdmissionRefV1(record.admission),
      );
      assert.deepEqual(first.slot.ref.originalUse, record.ref.originalUse);
      assert.deepEqual(first.slot.ref.executor, record.ref.executor);
      assert.deepEqual(
        ordinaryCheckoutLeaseIdentityV1(first.slot.ref.originalUse.lease),
        ordinaryCheckoutLeaseIdentityV1(record.admission.capture.lease),
      );
      assert.deepEqual(first.slot.ref, record.history.participants[0]?.ref);
      assert.strictEqual(first.slot.outcome, record.outcome);
      assert.strictEqual(second.slot.outcome, record.outcome);
      assert.equal(first.slot.associationOrdinal, record.associationOrdinal);
      assert.equal(second.slot.associationOrdinal, record.associationOrdinal);
      assert.strictEqual(probe.afterSlot, first.slot);
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      yield* waitUntil(() =>
        outbox
          .listByCommandId(CommandId.make("command:launch:retry:1:release"))
          .pipe(Effect.map((effects) => effects.length === 1)),
      );
      const retried = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(retried.runs.length, 1);
      assert.equal(retried.runs[0]?.status, "starting");
      assert.equal(retried.thread.worktreePath, expected.expectedWorktreePath);
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.isTrue(yield* realFixture.fs.exists(expected.expectedWorktreePath));
      assert.deepEqual(realFixture.audit.controls, []);
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));
}

it.effect("authentic retained retry reader refuses an unrelated messageId", () =>
  runAuthenticRetainedReaderRegression("messageId"),
);
it.effect("authentic retained retry reader refuses an unrelated projectId", () =>
  runAuthenticRetainedReaderRegression("projectId"),
);
it.effect("authentic retained retry reader refuses an unrelated targetSource.worktreePath", () =>
  runAuthenticRetainedReaderRegression("targetSource.worktreePath"),
);
it.effect("authentic retained retry reader refuses an unrelated branch", () =>
  runAuthenticRetainedReaderRegression("branch"),
);
it.effect("authentic retained retry reader refuses multiple released native operation rows", () =>
  runAuthenticRetainedReaderRegression("multiple released operations"),
);

it.effect("retries a failed workspace preparation on the same run", () =>
  Effect.gen(function* () {
    const realFixture = yield* acquireRealRetryFixture;
    const originalPreparation: OriginalRetryPreparationObservation = {
      ready: yield* Deferred.make<Fiber.Fiber<unknown, unknown>>(),
      trackerAcquisitions: 0,
      beginInvocations: 0,
      beginEvaluations: 0,
      beginTerminals: 0,
      actualTracker: undefined,
      forwardedTracker: undefined,
      capture: undefined,
      beginExit: undefined,
      fiberExit: undefined,
      retryReady: yield* Deferred.make<Fiber.Fiber<unknown, unknown>>(),
      retryEntryPhase: "not_entered",
      retryAwaitState: "handle_unobserved",
      retryCapture: undefined,
      retryBeginExit: undefined,
      retryFiberExit: undefined,
    };
    const harness = makeHarness({ realPreparation: realFixture, originalPreparation });
    return yield* Effect.gen(function* () {
      const fixture = yield* planOriginalRetryFixtureChild;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches
        .launch(
          launchInput({
            command: "command:launch:retry",
            thread: "thread:launch:retry",
            message: "Retry me",
            workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
          }),
        )
        .pipe(Random.withSeed(originalRetryFixtureSeed));
      const originalPreparationFiber = yield* Deferred.await(originalPreparation.ready);
      originalPreparation.fiberExit = yield* Fiber.await(originalPreparationFiber);
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "failed")),
      );
      const failed = yield* threads.getThreadProjection(launched.threadId);
      const runId = failed.runs[0]!.id;
      assert.equal(
        failed.turnItems.find((item) => item.type === "error")?.failure.code,
        ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
      );

      const retry = {
        commandId: CommandId.make("command:launch:retry:1"),
        threadId: launched.threadId,
        runId,
      };
      originalPreparation.retryEntryPhase = "before_retryPreparation";
      yield* launches.retryPreparation(retry);
      originalPreparation.retryAwaitState = "awaiting_handle";
      const retryPreparationFiber = yield* Deferred.await(originalPreparation.retryReady);
      originalPreparation.retryAwaitState = "awaiting_fiber_exit";
      originalPreparation.retryFiberExit = yield* Fiber.await(retryPreparationFiber);
      originalPreparation.retryAwaitState = "fiber_exit_observed";
      yield* waitUntil(() =>
        outbox
          .listByCommandId(CommandId.make("command:launch:retry:1:release"))
          .pipe(Effect.map((effects) => effects.length === 1)),
      );
      const retried = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(retried.runs.length, 1);
      assert.equal(retried.runs[0]?.status, "starting");
      assert.equal(retried.thread.worktreePath, fixture.expectedWorktreePath);
      assert.equal(retried.turnItems.find((item) => item.type === "error")?.status, "cancelled");
      assert.equal(
        retried.turnItems.find((item) => item.type === "command_execution")?.status,
        "completed",
      );
      assert.equal(harness.createWorktree.mock.calls.length, 1);

      // The run left preparation, so a second retry has nothing to do.
      const rejected = yield* launches
        .retryPreparation({ ...retry, commandId: CommandId.make("command:launch:retry:2") })
        .pipe(Effect.flip);
      assert.equal(rejected._tag, "OrchestratorDispatchError");
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("retains the explicit setup opt-out when a failed bootstrap is retried", () =>
  Effect.gen(function* () {
    const realFixture = yield* acquireRealRetryFixture;
    const originalPreparation: OriginalRetryPreparationObservation = {
      ...(yield* acquireRetryRegressionObservation),
      observedThreadId: ThreadId.make("thread:launch:retry-no-setup"),
    };
    const harness = makeHarness({ realPreparation: realFixture, originalPreparation });
    return yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const launched = yield* launches
        .launch({
          ...launchInput({
            command: "command:launch:retry-no-setup",
            thread: "thread:launch:retry-no-setup",
            message: "Retry without setup",
            workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
          }),
          runSetupScript: false,
        })
        .pipe(Random.withSeed(originalRetryFixtureSeed));
      const originalPreparationFiber = yield* Deferred.await(originalPreparation.ready);
      originalPreparation.fiberExit = yield* Fiber.await(originalPreparationFiber);
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((p) => p.runs[0]?.status === "failed")),
      );
      const failed = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(failed.runs[0]?.workspaceRunSetupScript, false);
      const commandId = CommandId.make("command:launch:retry-no-setup:retry");
      originalPreparation.retryEntryPhase = "before_retryPreparation";
      yield* launches.retryPreparation({
        commandId,
        threadId: launched.threadId,
        runId: failed.runs[0]!.id,
      });
      originalPreparation.retryAwaitState = "awaiting_handle";
      const retryPreparationFiber = yield* Deferred.await(originalPreparation.retryReady);
      originalPreparation.retryAwaitState = "awaiting_fiber_exit";
      originalPreparation.retryFiberExit = yield* Fiber.await(retryPreparationFiber);
      originalPreparation.retryAwaitState = "fiber_exit_observed";
      yield* waitUntil(() =>
        outbox
          .listByCommandId(CommandId.make(`${commandId}:release`))
          .pipe(Effect.map((effects) => effects.length === 1)),
      );
      assert.equal(harness.runSetup.mock.calls.length, 0);
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).runs[0]?.status,
        "starting",
      );
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("joins the original failed producer before reusing its created checkout", () =>
  Effect.gen(function* () {
    const failedPublished = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>();
    const allowProducerEnd = yield* Deferred.make<void>();
    let setupFailures = 1;
    const failures: string[] = [];
    let originalOutcome:
      | Parameters<OrdinaryCheckoutLifetime["recordPreparedOutcome"]>[0]
      | undefined;
    const harness = makeHarness({
      afterPreparedOutcome: (input) =>
        Effect.sync(() => {
          originalOutcome = input;
        }),
      runSetup: () =>
        setupFailures-- > 0
          ? Effect.fail(new Error("setup failed") as never)
          : Effect.succeed({ status: "no-script" as const }),
      afterDispatch: (command) =>
        Effect.gen(function* () {
          if (command.type !== "prepared-run.fail") return;
          failures.push(command.failure.message);
          if (command.commandId !== "command:producer-join:fail") return;
          yield* Deferred.succeed(failedPublished, yield* Effect.fiber);
          yield* Deferred.await(allowProducerEnd);
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:producer-join",
          thread: "thread:producer-join",
          message: "Retry after the original producer ends",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      const producer = yield* Deferred.await(failedPublished);
      assert.isUndefined(producer.pollUnsafe());
      assert.isDefined(originalOutcome);
      const physical = originalOutcome!.actualProducerOutcome.observation;
      assert.equal(physical.kind, "prepared_failure_observed");
      assert.strictEqual(ThreadLaunch.readIssuedOrdinaryPreparedPhysicalResult(physical), physical);
      assert.isNull(ThreadLaunch.readIssuedRetiredPreparedFailureResult(physical));
      const failed = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(failed.runs[0]?.status, "failed");
      const runId = failed.runs[0]!.id;
      const retry = {
        commandId: CommandId.make("command:producer-join:retry"),
        threadId: launched.threadId,
        runId,
      };
      yield* threads.dispatch({ type: "prepared-run.retry", ...retry });
      const lifetime = sink.ordinaryCheckoutLifetime!;
      const admission = (yield* lifetime.readAdmissionForRun({
        threadId: launched.threadId,
        runId,
      }))!;
      const query = {
        threadId: launched.threadId,
        projectId,
        runId,
        messageId: admission.run!.messageId,
        canonicalProjectRoot: admission.capture.canonicalProjectRoot,
        canonicalCheckoutPath: admission.capture.canonicalCheckoutPath,
        branch: failed.thread.branch!,
        targetSource: {
          projectWorkspaceRoot: project.workspaceRoot,
          worktreePath: failed.thread.worktreePath,
        },
      };
      const beforeRetirement = yield* lifetime.readOriginalPreparedRetry(query).pipe(Effect.flip);
      assert.equal(
        beforeRetirement.message,
        "Retry has no retained, completed original preparation outcome.",
      );
      const retryFiber = yield* launches.retryPreparation(retry).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.isUndefined(retryFiber.pollUnsafe());
      assert.equal((yield* sql`SELECT 1 AS available`)[0]?.available, 1);
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      yield* Deferred.succeed(allowProducerEnd, undefined);
      const producerExit = yield* Fiber.await(producer);
      assert.isTrue(Exit.isSuccess(producerExit));
      assert.strictEqual(
        ThreadLaunch.readIssuedRetiredPreparedFailureResult(physical),
        physical,
        "actual issued result after original producer end",
      );
      assert.isNull(ThreadLaunch.readIssuedRetiredPreparedFailureResult({ ...physical }));
      assert.isNull(
        ThreadLaunch.readIssuedRetiredPreparedFailureResult(
          yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(
            yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(physical),
          ),
        ),
      );
      yield* Fiber.join(retryFiber);
      const settled = yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.commandId === "command:producer-join:retry:release" ||
            stored.commandId === "command:producer-join:retry:fail",
        ),
        Stream.runHead,
      );
      assert.equal(
        Option.getOrNull(settled)?.commandId,
        "command:producer-join:retry:release",
        failures.join("; "),
      );
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.renameBranch.mock.calls.length, 1);
    }).pipe(
      Effect.ensuring(Deferred.succeed(allowProducerEnd, undefined)),
      Effect.provide(harness.layer),
    );
  }),
);

it.effect("a producer retirement timeout refuses retry without interrupting the producer", () =>
  Effect.gen(function* () {
    const failedPublished = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>();
    const retryDispatched = yield* Deferred.make<void>();
    const allowProducerEnd = yield* Deferred.make<void>();
    const retryRefused = yield* Deferred.make<string>();
    const harness = makeHarness({
      runSetup: () => Effect.fail(new Error("setup failed") as never),
      afterDispatch: (command) =>
        Effect.gen(function* () {
          if (command.type === "prepared-run.retry") {
            yield* Deferred.succeed(retryDispatched, undefined);
            return;
          }
          if (command.type !== "prepared-run.fail") return;
          if (command.commandId === "command:producer-timeout:retry:fail") {
            yield* Deferred.succeed(retryRefused, command.failure.message);
            return;
          }
          if (command.commandId !== "command:producer-timeout:fail") return;
          yield* Deferred.succeed(failedPublished, yield* Effect.fiber);
          yield* Deferred.await(allowProducerEnd);
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:producer-timeout",
          thread: "thread:producer-timeout",
          message: "Keep the original producer",
          workspace: { type: "worktree", baseRef: "main", branch: "feature" },
        }),
      );
      const producer = yield* Deferred.await(failedPublished);
      const retry = yield* launches
        .retryPreparation({
          commandId: CommandId.make("command:producer-timeout:retry"),
          threadId: launched.threadId,
          runId: launched.projection.runs[0]!.id,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(retryDispatched);
      yield* TestClock.adjust("5 seconds");
      assert.include(
        yield* Deferred.await(retryRefused),
        "The original preparation producer has not retired.",
      );
      yield* Fiber.join(retry);
      assert.isUndefined(producer.pollUnsafe());
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).runs[0]?.status,
        "failed",
      );
      yield* Deferred.succeed(allowProducerEnd, undefined);
      assert.isTrue(Exit.isSuccess(yield* Fiber.await(producer)));
    }).pipe(
      Effect.ensuring(Deferred.succeed(allowProducerEnd, undefined)),
      Effect.provide(harness.layer),
    );
  }),
);

it.effect("the original failed producer refuses to await itself during retry", () =>
  Effect.gen(function* () {
    const selfRetryDone = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>();
    const retryRefused = yield* Deferred.make<string>();
    let launches: ThreadLaunch.ThreadLaunchService["Service"] | undefined;
    const harness = makeHarness({
      runSetup: () => Effect.fail(new Error("setup failed") as never),
      afterDispatch: (command) =>
        Effect.gen(function* () {
          if (command.type !== "prepared-run.fail") return;
          if (command.commandId === "command:producer-self:retry:fail") {
            yield* Deferred.succeed(retryRefused, command.failure.message);
            return;
          }
          if (command.commandId !== "command:producer-self:fail") return;
          yield* launches!
            .retryPreparation({
              commandId: CommandId.make("command:producer-self:retry"),
              threadId: command.threadId,
              runId: command.runId,
            })
            .pipe(Effect.orDie);
          yield* Deferred.succeed(selfRetryDone, yield* Effect.fiber);
        }),
    });
    yield* Effect.gen(function* () {
      launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.launch(
        launchInput({
          command: "command:producer-self",
          thread: "thread:producer-self",
          message: "No self wait",
          workspace: { type: "worktree", baseRef: "main", branch: "feature" },
        }),
      );
      assert.include(
        yield* Deferred.await(retryRefused),
        "The original preparation producer cannot await itself.",
      );
      const producer = yield* Deferred.await(selfRetryDone);
      assert.isTrue(Exit.isSuccess(yield* Fiber.await(producer)));
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.runSetup.mock.calls.length, 1);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each(["wrong birth", "live owner", "wrong manager"] as const)(
  "a created-worktree retry refuses a fresh %s observation",
  (variant) =>
    Effect.gen(function* () {
      const failedPublished = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>();
      const retryRefused = yield* Deferred.make<string>();
      let captures = 0;
      const harness = makeHarness({
        runSetup: () => Effect.fail(new Error("setup failed") as never),
        terminalOwner: Layer.mock(TerminalManager.TerminalManager)({
          close: () => Effect.void,
          captureOwnedTargets: ({ threadId, ownerBirth }) =>
            Effect.sync(() => {
              captures++;
              const changed = captures > 1;
              return {
                managerId:
                  changed && variant === "wrong manager" ? "foreign-manager" : "original-manager",
                threadId,
                ownerBirth:
                  changed && variant === "wrong birth"
                    ? { ...ownerBirth, sequence: ownerBirth.sequence + 1 }
                    : ownerBirth,
                status: "captured" as const,
                managedTargetsOnly: true as const,
                targets:
                  changed && variant === "live owner"
                    ? [{ threadId, ownerBirth, terminalId: "live", handleId: "live-handle" }]
                    : [],
              };
            }),
        }),
        afterDispatch: (command) =>
          Effect.gen(function* () {
            if (command.type !== "prepared-run.fail") return;
            if (command.commandId === `command:owner-recheck:${variant}:fail`)
              yield* Deferred.succeed(failedPublished, yield* Effect.fiber);
            if (command.commandId === `command:owner-recheck:${variant}:retry:fail`)
              yield* Deferred.succeed(retryRefused, command.failure.message);
          }),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const launched = yield* launches.launch(
          launchInput({
            command: `command:owner-recheck:${variant}`,
            thread: `thread:owner-recheck:${variant}`,
            message: "Recheck the original owner",
            workspace: { type: "worktree", baseRef: "main", branch: "feature" },
          }),
        );
        const producer = yield* Deferred.await(failedPublished);
        assert.isTrue(Exit.isSuccess(yield* Fiber.await(producer)));
        assert.equal(captures, 1);
        yield* launches.retryPreparation({
          commandId: CommandId.make(`command:owner-recheck:${variant}:retry`),
          threadId: launched.threadId,
          runId: launched.projection.runs[0]!.id,
        });
        assert.include(
          yield* Deferred.await(retryRefused),
          "The original created checkout or managed owner changed before retry.",
        );
        assert.equal(captures, 2);
        assert.equal(harness.createWorktree.mock.calls.length, 1);
        assert.equal(harness.runSetup.mock.calls.length, 1);
        assert.equal(
          (yield* threads.getThreadProjection(launched.threadId)).runs[0]?.status,
          "failed",
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect("a wrong captured owner birth cannot issue a created failure outcome", () =>
  Effect.gen(function* () {
    const failedPublished = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>();
    let issued = 0;
    const harness = makeHarness({
      runSetup: () => Effect.fail(new Error("setup failed") as never),
      afterPreparedOutcome: () =>
        Effect.sync(() => {
          issued++;
        }),
      terminalOwner: Layer.mock(TerminalManager.TerminalManager)({
        close: () => Effect.void,
        captureOwnedTargets: ({ threadId, ownerBirth }) =>
          Effect.succeed({
            managerId: "original-manager",
            threadId,
            ownerBirth: { ...ownerBirth, sequence: ownerBirth.sequence + 1 },
            status: "captured" as const,
            managedTargetsOnly: true as const,
            targets: [],
          }),
      }),
      afterDispatch: (command) =>
        command.type === "prepared-run.fail"
          ? Effect.fiber.pipe(
              Effect.flatMap((producer) => Deferred.succeed(failedPublished, producer)),
              Effect.asVoid,
            )
          : Effect.void,
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const sql = yield* SqlClient.SqlClient;
      yield* launches.launch(
        launchInput({
          command: "command:owner-birth-issue",
          thread: "thread:owner-birth-issue",
          message: "Require original birth",
          workspace: { type: "worktree", baseRef: "main", branch: "feature" },
        }),
      );
      const producer = yield* Deferred.await(failedPublished);
      assert.isTrue(Exit.isSuccess(yield* Fiber.await(producer)));
      assert.equal(issued, 0);
      assert.equal(
        (yield* sql`SELECT state FROM orchestration_v2_worktree_path_admissions`)[0]?.state,
        "unknown",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("a retry reuses a recorded worktree without undoing its branch rename", () => {
  let setupFailures = 1;
  const harness = makeHarness({
    runSetup: () =>
      setupFailures-- > 0
        ? Effect.fail(new Error("setup failed") as never)
        : Effect.succeed({ status: "no-script" as const }),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
    const launchSeed = "jones-recorded-worktree-reuse-explicit-base-v1";
    const plannedUuid = yield* randomUuidV4.pipe(Random.withSeed(launchSeed));
    const plannedTemporaryBranch = buildTemporaryWorktreeBranchName(() =>
      plannedUuid.replaceAll("-", ""),
    );
    const plannedWorktreePath = nativeWorktreePath({
      worktreesDir: "/repo-worktrees",
      cwd: project.workspaceRoot,
      branch: plannedTemporaryBranch,
    });
    const launched = yield* launches
      .launch(
        launchInput({
          command: "command:launch:reuse",
          thread: "thread:launch:reuse",
          message: "Reuse the worktree",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      )
      .pipe(Random.withSeed(launchSeed));
    yield* waitUntil(() =>
      threads
        .getThreadProjection(launched.threadId)
        .pipe(
          Effect.map(
            (projection) =>
              projection.runs[0]?.status === "failed" &&
              projection.thread.branch === "generated-branch",
          ),
        ),
    );
    const failed = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(failed.thread.worktreePath, plannedWorktreePath);

    yield* launches.retryPreparation({
      commandId: CommandId.make("command:launch:reuse:retry"),
      threadId: launched.threadId,
      runId: failed.runs[0]!.id,
    });
    yield* waitUntil(() =>
      outbox
        .listByCommandId(CommandId.make("command:launch:reuse:retry:release"))
        .pipe(Effect.map((effects) => effects.length === 1)),
    );
    const retried = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(retried.runs[0]?.status, "starting");
    // The retry neither checks out again nor puts back the temporary branch.
    assert.equal(harness.createWorktree.mock.calls.length, 1);
    assert.equal(harness.renameBranch.mock.calls.length, 1);
    assert.equal(retried.thread.branch, "generated-branch");
    assert.equal(retried.thread.worktreePath, plannedWorktreePath);
    // Clients see the retry's setup, not the failed one it replaced.
    const snapshot = yield* tracker.get(launched.threadId);
    assert.equal(snapshot?.phase, "done");
    assert.deepEqual(
      snapshot?.stages.map((stage) => stage.id),
      ["setup-script", "agent"],
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect("removes a worktree that failed before the thread recorded it", () => {
  const harness = makeHarness({
    // A checkout that dies after claiming its directory.
    createWorktree: (input, options) =>
      (options?.progress?.onWorktreeClaimed?.(input.path!) ?? Effect.void).pipe(
        Effect.andThen(Effect.fail(new Error("checkout failed") as never)),
      ),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launchSeed = "jones-partial-unrecorded-checkout-v1";
    const plannedUuid = yield* randomUuidV4.pipe(Random.withSeed(launchSeed));
    const plannedBranch = buildTemporaryWorktreeBranchName(() => plannedUuid.replaceAll("-", ""));
    const plannedWorktreePath = nativeWorktreePath({
      worktreesDir: "/repo-worktrees",
      cwd: project.workspaceRoot,
      branch: plannedBranch,
    });
    const launched = yield* launches
      .launch(
        launchInput({
          command: "command:launch:partial-worktree",
          thread: "thread:launch:partial-worktree",
          message: "Partial checkout",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      )
      .pipe(Random.withSeed(launchSeed));
    yield* waitUntil(() =>
      threads
        .getThreadProjection(launched.threadId)
        .pipe(Effect.map((projection) => projection.runs[0]?.status === "failed")),
    );
    assert.equal(harness.createWorktree.mock.calls.length, 1);
    assert.equal(harness.createWorktree.mock.calls[0]![0].path, plannedWorktreePath);
    const projection = yield* threads.getThreadProjection(launched.threadId);
    // Unrecorded, so a retry would create a second checkout beside it.
    assert.equal(projection.thread.worktreePath, null);
    assert.deepEqual(
      harness.removeWorktree.mock.calls.map(([input]) => input.path),
      [plannedWorktreePath],
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect("does not remove an alternate claimed checkout outside the planned target", () => {
  const harness = makeHarness({
    // A checkout that dies after claiming its directory.
    createWorktree: (_input, options) =>
      (options?.progress?.onWorktreeClaimed?.("/repo-worktrees/partial") ?? Effect.void).pipe(
        Effect.andThen(Effect.fail(new Error("checkout failed") as never)),
      ),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:partial-worktree",
        thread: "thread:launch:partial-worktree",
        message: "Partial checkout",
        workspace: { type: "worktree", baseRef: "main" },
      }),
    );
    yield* waitUntil(() =>
      threads
        .getThreadProjection(launched.threadId)
        .pipe(Effect.map((projection) => projection.runs[0]?.status === "failed")),
    );
    const projection = yield* threads.getThreadProjection(launched.threadId);
    // Unrecorded, so a retry would create a second checkout beside it.
    assert.equal(projection.thread.worktreePath, null);
    assert.deepEqual(
      harness.removeWorktree.mock.calls.map(([input]) => input.path),
      [],
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect.each(["worktree", "setup"] as const)(
  "%s failure keeps the thread and message visible and emits failure items",
  (failurePoint) =>
    Effect.gen(function* () {
      const failure = new Error(`${failurePoint} failed`);
      const harness = makeHarness(
        failurePoint === "worktree"
          ? { createWorktree: () => Effect.fail(failure as never) }
          : { runSetup: () => Effect.fail(failure as never) },
      );
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const input = launchInput({
          command: `command:launch:${failurePoint}-failure`,
          thread: `thread:launch:${failurePoint}-failure`,
          message: `Fail during ${failurePoint}`,
          workspace: { type: "worktree", baseRef: "main" },
        });
        const launched = yield* launches.launch(input);
        yield* waitUntil(() =>
          threads
            .getThreadProjection(launched.threadId)
            .pipe(Effect.map((projection) => projection.runs[0]?.status === "failed")),
        );
        const projection = yield* threads.getThreadProjection(launched.threadId);
        assert.equal(projection.messages[0]?.text, `Fail during ${failurePoint}`);
        assert.equal(projection.runs[0]?.status, "failed");
        assert.equal(
          projection.turnItems.find((item) => item.type === "command_execution")?.status,
          "failed",
        );
        assert.match(
          projection.turnItems.find((item) => item.type === "error")?.failure.message ?? "",
          new RegExp(`${failurePoint} failed`, "u"),
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect("replays a server-allocated launch", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const { threadId: _unusedThreadId, ...rest } = launchInput({
        command: "command:launch:allocated-retry",
        thread: "unused",
        message: "Only once",
      });
      const first = yield* launches.launch(rest);
      yield* Deferred.await(setupEntered);
      const retry = yield* launches.launch(rest);
      assert.equal(first.threadId, retry.threadId);
      assert.isFalse(first.resumed);
      assert.isTrue(retry.resumed);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      assert.equal(retry.projection.messages.length, 1);
      assert.equal(retry.projection.runs.length, 1);
      assert.equal(retry.projection.messages[0]?.id, first.projection.messages[0]?.id);
      assert.equal(retry.projection.runs[0]?.id, first.projection.runs[0]?.id);
      yield* Deferred.succeed(allowSetup, undefined);
      yield* threads.streamStoredEventsFrom({ threadId: first.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.commandId === CommandId.make(`${rest.commandId}:release`) &&
            stored.event.type === "run.updated",
        ),
        Stream.runHead,
      );
      const settled = yield* launches.launch(rest);
      assert.equal(settled.threadId, first.threadId);
      assert.isTrue(settled.resumed);
      assert.equal(settled.projection.messages[0]?.id, first.projection.messages[0]?.id);
      assert.equal(settled.projection.runs[0]?.id, first.projection.runs[0]?.id);
      assert.equal(harness.runSetup.mock.calls.length, 1);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("rejects a server-allocated launch replay with a mismatching thread id", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:allocated-mismatch",
      thread: "unused",
      message: "Mismatch",
    });
    const first = yield* launches.launch(rest);
    const failed = yield* launches
      .launch({
        ...rest,
        threadId: ThreadId.make("thread:launch:allocated-mismatch"),
      })
      .pipe(Effect.flip);
    assert.notEqual(first.threadId, ThreadId.make("thread:launch:allocated-mismatch"));
    assert.equal(failed._tag, "ThreadLaunchError");
    assert.equal(failed.operation, "create-thread");
    assert.include(String(failed.cause), "cannot be replayed");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("rejects a server-allocated launch receipt from another project", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:allocated-wrong-project",
      thread: "unused",
      message: "Wrong project",
    });
    const first = yield* launches.launch(rest);
    const failed = yield* launches
      .launch({
        ...rest,
        projectId: otherProjectId,
      })
      .pipe(Effect.flip);
    assert.equal(failed._tag, "ThreadLaunchError");
    assert.equal(failed.operation, "resolve-project");
    assert.equal(failed.threadId, first.threadId);
    assert.equal(failed.cause, "Project identity changed.");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("rejects a server-allocated launch retry after the thread is deleted", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:allocated-deleted",
      thread: "unused",
      message: "Deleted before retry",
    });
    const first = yield* launches.launch(rest);
    yield* threads.dispatch({
      type: "thread.delete",
      commandId: CommandId.make("command:launch:allocated-deleted:delete"),
      threadId: first.threadId,
    });
    const failed = yield* launches.launch(rest).pipe(Effect.flip);
    assert.equal(failed._tag, "ThreadLaunchError");
    assert.equal(failed.operation, "create-thread");
    assert.equal(failed.threadId, first.threadId);
    assert.equal(failed.cause, "Thread not found.");
    const shells = yield* threads.listProjectThreads({ projectId, includeSubagents: true });
    assert.equal(shells.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("does not treat an unrelated accepted command receipt as a launch", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const threadId = ThreadId.make("thread:launch:unrelated-receipt");
    yield* threads.dispatch({
      type: "thread.create",
      commandId: CommandId.make("command:launch:unrelated-receipt:create"),
      threadId,
      projectId,
      title: "Existing",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* threads.dispatch({
      type: "thread.metadata.update",
      commandId: CommandId.make("command:launch:unrelated-receipt"),
      threadId,
      expectedEmpty: true,
    });
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:unrelated-receipt",
      thread: "unused",
      message: "Should not become a launch",
    });
    const failed = yield* launches.launch(rest).pipe(Effect.flip);
    assert.equal(failed._tag, "ThreadLaunchError");
    assert.equal(failed.operation, "create-thread");
    assert.include(String(failed.cause), "cannot be replayed");
    const projection = yield* threads.getThreadProjection(threadId);
    assert.equal(projection.messages.length, 0);
    assert.equal(projection.runs.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("bounds concurrent first launches to one thread per command", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const { threadId: _unusedThreadId, ...rest } = launchInput({
        command: "command:launch:concurrent-allocated",
        thread: "unused",
        message: "Race me",
      });
      // The command receipt is reserved atomically with the winning create, so
      // a loser either replays the winner's stored events or surfaces a
      // transient replay conflict that the next attempt resolves — the race
      // can never persist a second thread.
      const results = yield* Effect.all(
        [launches.launch(rest).pipe(Effect.exit), launches.launch(rest).pipe(Effect.exit)],
        { concurrency: "unbounded" },
      );
      const winner = results.find(Exit.isSuccess);
      assert.isDefined(winner);
      const threadId = winner!.value.threadId;
      for (const result of results) {
        if (Exit.isSuccess(result)) {
          assert.equal(result.value.threadId, threadId);
          continue;
        }
        const error = Cause.findErrorOption(result.cause).pipe(Option.getOrThrow);
        assert.equal(error._tag, "ThreadLaunchError");
        assert.equal(error.operation, "create-thread");
        assert.include(String(error.cause), "cannot be replayed");
        const retried = yield* launches.launch(rest);
        assert.equal(retried.threadId, threadId);
      }
      const projectThreads = yield* threads.listProjectThreads({
        projectId,
        includeSubagents: false,
      });
      assert.equal(projectThreads.length, 1);
      const projection = yield* threads.getThreadProjection(threadId);
      assert.equal(projection.messages.length, 1);
      assert.equal(projection.runs.length, 1);
      yield* Deferred.await(setupEntered);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("deduplicates retried launch side effects in-process", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const input = launchInput({
        command: "command:launch:retry",
        thread: "thread:launch:retry",
        message: "Only once",
      });
      const [first, retry] = yield* Effect.all([launches.launch(input), launches.launch(input)], {
        concurrency: "unbounded",
      });
      yield* Deferred.await(setupEntered);
      assert.equal(first.threadId, retry.threadId);
      assert.isFalse(first.resumed);
      assert.isTrue(retry.resumed);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("does not let a failing same-command caller strand a concurrent durable launch", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const command = "command:launch:failed-owner-race";
      const [failed, launched] = yield* Effect.all(
        [
          launches
            .launch({
              ...launchInput({
                command,
                thread: "thread:launch:failed-owner-race",
                message: "This invalid reuse fails",
              }),
              reuseExistingThread: true,
            })
            .pipe(Effect.exit),
          launches.launch(
            launchInput({
              command,
              thread: "thread:launch:successful-peer",
              message: "This peer persists",
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      assert.isTrue(Exit.isFailure(failed));
      assert.equal(launched.projection.runs[0]?.status, "preparing");
      const entered = yield* Deferred.await(setupEntered).pipe(
        Effect.timeoutOption(Duration.seconds(2)),
      );
      assert.isTrue(Option.isSome(entered));
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("schedules an accepted preparing message exactly once across concurrent retries", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const messageId = MessageId.make("message:launch:accepted-before-fork");
      const input = {
        ...launchInput({
          command: "command:launch:accepted-before-fork",
          thread: "thread:launch:accepted-before-fork",
          message: "Resume preparation",
        }),
        initialMessage: { messageId, text: "Resume preparation", attachments: [] },
      };

      yield* threads.dispatch({
        type: "thread.create",
        commandId: input.commandId,
        threadId: input.threadId,
        projectId: input.projectId,
        title: input.title,
        modelSelection: input.modelSelection,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
        branch: null,
        worktreePath: null,
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      });
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`${input.commandId}:initial-message`),
        threadId: input.threadId,
        messageId,
        text: "Resume preparation",
        attachments: [],
        modelSelection: input.modelSelection,
        dispatchMode: { type: "defer_start", workspaceStrategy: { type: "root" } },
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      });
      const preparing = yield* threads.getThreadProjection(input.threadId);
      assert.equal(preparing.runs[0]?.status, "preparing");

      const [first, second] = yield* Effect.all([launches.launch(input), launches.launch(input)], {
        concurrency: "unbounded",
      });
      yield* Deferred.await(setupEntered);
      assert.isTrue(first.resumed);
      assert.isTrue(second.resumed);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("creates a strong provider-thread mapping for an imported native session", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const input = {
      ...launchInput({
        command: "command:launch:imported-native-session",
        thread: "thread:launch:imported-native-session",
      }),
      importedNativeThread: {
        ref: {
          driver: ProviderDriverKind.make("codex"),
          nativeId: "native-session-42",
          strength: "strong" as const,
        },
        metadata: {
          title: "Native session",
          updatedAt: "2026-08-23T00:00:00Z",
        },
      },
    };

    const launched = yield* launches.launch(input);

    assert.deepInclude(launched.projection.providerThreads[0], {
      id: IdAllocator.deriveProviderThread({
        driver: input.importedNativeThread.ref.driver,
        providerInstanceId: modelSelection.instanceId,
        nativeThreadId: input.importedNativeThread.ref.nativeId,
      }),
      driver: input.importedNativeThread.ref.driver,
      providerInstanceId: modelSelection.instanceId,
      appThreadId: input.threadId,
      nativeThreadRef: input.importedNativeThread.ref,
      status: "not_loaded",
      nativeMetadata: input.importedNativeThread.metadata,
    });
    assert.equal(
      launched.projection.thread.activeProviderThreadId,
      launched.projection.providerThreads[0]?.id,
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect("shared intake preserves durable attachment bytes after a lost launch result", () => {
  const harness = makeHarness();
  const files = ServerConfig.layerTest(process.cwd(), { prefix: "t3-message-intake-" }).pipe(
    Layer.provideMerge(NodeServices.layer),
  );
  return Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const pendingId = createPendingAttachmentId();
    assert.isNotNull(pendingId);
    const attachment: ChatAttachment = {
      type: "image",
      id: ChatAttachmentId.make(pendingId),
      name: "image.png",
      mimeType: "image/png",
      sizeBytes: 4,
    };
    const pendingPath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment,
    });
    assert.isNotNull(pendingPath);
    yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
    yield* fs.writeFile(pendingPath, new Uint8Array([1, 2, 3, 4]));
    const input = {
      ...launchInput({ command: "intake-launch", thread: "intake-thread" }),
      initialMessage: {
        messageId: MessageId.make("intake-first"),
        text: "First [file](t3-context://v1/file/intake-file)",
        context: {
          version: 1 as const,
          records: [
            {
              version: 1 as const,
              contextId: ComposerContextId.make("intake-file"),
              kind: "file" as const,
              label: attachment.name,
              attachmentId: attachment.id,
              name: attachment.name,
              mimeType: attachment.mimeType,
              sizeBytes: attachment.sizeBytes,
            },
          ],
        },
        attachments: [attachment],
      },
    };
    const failed = yield* ThreadMessageIntake.launchThread(input).pipe(
      Effect.provideService(ThreadLaunch.ThreadLaunchService, {
        preflightLegacyBootstrap: launches.preflightLegacyBootstrap,
        launch: (request) =>
          launches.launch(request).pipe(
            Effect.andThen(
              new ThreadLaunch.ThreadLaunchError({
                operation: "create-thread",
                commandId: request.commandId,
                projectId,
                cause: "lost result after acceptance",
              }),
            ),
          ),
        retryPreparation: launches.retryPreparation,
      }),
      Effect.flip,
    );
    assert.equal(failed._tag, "ThreadLaunchError");
    // The observer failed, but the real V2 message and its bytes were accepted.
    const accepted = yield* threads.getThreadProjection(input.threadId);
    const stored = accepted.messages.find(
      (message) => message.id === input.initialMessage.messageId,
    );
    assert.isDefined(stored);
    assert.notEqual(stored.attachments[0]?.id, attachment.id);
    assert.equal(
      (stored.context?.records[0] as { attachmentId: string }).attachmentId,
      stored.attachments[0]?.id,
    );
    const userItem = accepted.turnItems.find(
      (item) => item.type === "user_message" && item.messageId === stored.id,
    );
    assert.ok(userItem?.type === "user_message");
    assert.deepEqual(userItem.context, stored.context);
    const storedPath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: stored.attachments[0]!,
    });
    assert.isNotNull(storedPath);
    assert.deepEqual(yield* fs.readFile(storedPath), new Uint8Array([1, 2, 3, 4]));
    assert.deepEqual(yield* fs.readFile(pendingPath), new Uint8Array([1, 2, 3, 4]));

    const replayed = yield* ThreadMessageIntake.launchThread(input);
    assert.equal(replayed.projection.messages[0]?.id, stored.id);
    assert.deepEqual(replayed.projection.messages[0]?.attachments, stored.attachments);
    const claimedFiles = Effect.map(fs.readDirectory(config.attachmentsDir), (files) =>
      files.filter((name) => !name.startsWith("pending-")),
    );
    assert.equal((yield* claimedFiles).length, 1);

    const missingProject = yield* ThreadMessageIntake.launchThread({
      ...input,
      commandId: CommandId.make("intake-no-project"),
      projectId: ProjectId.make("missing-project"),
    }).pipe(Effect.flip);
    assert.equal(missingProject._tag, "ThreadLaunchError");
    assert.equal((yield* claimedFiles).length, 1);
    const missingThread = yield* ThreadMessageIntake.dispatchCommand({
      type: "message.dispatch",
      commandId: CommandId.make("intake-no-thread"),
      threadId: ThreadId.make("missing-thread"),
      messageId: MessageId.make("intake-missing"),
      text: "Missing",
      attachments: [attachment],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    }).pipe(Effect.flip);
    assert.equal(missingThread._tag, "OrchestratorProjectionError");
    assert.equal((yield* claimedFiles).length, 1);

    // Both ordinary command intake (RPC) and send intake (MCP) use the same store.
    const dispatch = ThreadMessageIntake.dispatchCommand({
      type: "message.dispatch",
      commandId: CommandId.make("intake-dispatch"),
      threadId: input.threadId,
      messageId: MessageId.make("intake-second"),
      text: "Second",
      attachments: [attachment],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* dispatch;
    yield* dispatch;
    const queuedProjection = yield* threads.getThreadProjection(input.threadId);
    const queuedRun = queuedProjection.runs.find(
      (run) => run.userMessageId === MessageId.make("intake-second"),
    );
    assert.isDefined(queuedRun);
    assert.equal(queuedRun.status, "queued");
    const queuedMessage = queuedProjection.messages.find(
      (message) => message.id === queuedRun.userMessageId,
    );
    assert.isDefined(queuedMessage);
    const file: ChatAttachment = {
      type: "file",
      id: ChatAttachmentId.make(createPendingAttachmentId("pdf")),
      name: "queued.pdf",
      mimeType: "application/pdf",
      sizeBytes: 4,
    };
    const filePath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: file,
    });
    assert.isNotNull(filePath);
    yield* fs.writeFile(filePath, new Uint8Array([5, 6, 7, 8]));
    const edit = ThreadMessageIntake.dispatchCommand({
      type: "queued-run.edit",
      commandId: CommandId.make("intake-edit"),
      threadId: input.threadId,
      runId: queuedRun.id,
      text: "Edited with a file",
      attachments: [...queuedMessage.attachments, file],
    });
    yield* edit;
    yield* edit;
    const editedProjection = yield* threads.getThreadProjection(input.threadId);
    const editedMessage = editedProjection.messages.find(
      (message) => message.id === queuedRun.userMessageId,
    );
    assert.isDefined(editedMessage);
    assert.equal(editedMessage.attachments.length, 2);
    assert.deepEqual(editedMessage.attachments[0], queuedMessage.attachments[0]);
    assert.notEqual(editedMessage.attachments[1]?.id, file.id);
    const durableFilePath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: editedMessage.attachments[1]!,
    });
    assert.isNotNull(durableFilePath);
    assert.deepEqual(yield* fs.readFile(durableFilePath), new Uint8Array([5, 6, 7, 8]));
    assert.deepEqual(yield* fs.readFile(filePath), new Uint8Array([5, 6, 7, 8]));
    const beforeRejectedEdit = (yield* claimedFiles).length;
    const rejectedEdit = yield* ThreadMessageIntake.dispatchCommand({
      type: "queued-run.edit",
      commandId: CommandId.make("intake-edit-rejected"),
      threadId: input.threadId,
      runId: queuedRun.id,
      text: "",
      attachments: [file],
    }).pipe(Effect.flip);
    assert.equal(rejectedEdit._tag, "OrchestratorCommandRejectedError");
    assert.equal((yield* claimedFiles).length, beforeRejectedEdit);
    assert.deepEqual(yield* fs.readFile(filePath), new Uint8Array([5, 6, 7, 8]));
    const send = ThreadMessageIntake.sendToThread({
      commandId: CommandId.make("intake-send"),
      projectId,
      threadId: input.threadId,
      messageId: MessageId.make("intake-third"),
      text: "Third",
      attachments: [attachment],
      mode: "queue",
      createdBy: "agent",
      creationSource: "mcp",
    });
    yield* send;
    yield* send;
    assert.equal((yield* claimedFiles).length, 4);
    const final = yield* threads.getThreadProjection(input.threadId);
    assert.equal(final.messages.length, 3);
    for (const message of final.messages) {
      const path = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: message.attachments[0]!,
      });
      assert.isNotNull(path);
      assert.deepEqual(yield* fs.readFile(path), new Uint8Array([1, 2, 3, 4]));
    }
  }).pipe(Effect.provide(Layer.mergeAll(harness.layer, files)));
});

it.effect("cancels tracked setup before provider work is released", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const input = launchInput({
        command: "launch:cancel-tracked",
        thread: "thread:cancel-tracked",
        message: "Start",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      yield* Deferred.await(entered);
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "running");
      assert.isTrue(yield* tracker.cancel(launched.threadId));
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "cancelled");
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.runs[0]?.status, "failed");
      assert.isNull(projection.thread.worktreePath);
      assert.isEmpty(yield* outbox.listByCommandId(CommandId.make(`${input.commandId}:release`)));
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("cancellation retains a checkout whose managed setup target is still live", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      terminalOwner: Layer.mock(TerminalManager.TerminalManager)({
        close: () => Effect.void,
        captureOwnedTargets: ({ threadId, ownerBirth }) =>
          Effect.succeed({
            managerId: "synthetic-terminal-manager",
            threadId,
            ownerBirth,
            status: "captured" as const,
            managedTargetsOnly: true as const,
            targets: [{ threadId, ownerBirth, terminalId: "live", handleId: "live-handle" }],
          }),
      }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launchSeed = "jones-cancel-live-managed-setup-v1";
      const plannedUuid = yield* randomUuidV4.pipe(Random.withSeed(launchSeed));
      const plannedBranch = buildTemporaryWorktreeBranchName(() => plannedUuid.replaceAll("-", ""));
      const plannedWorktreePath = nativeWorktreePath({
        worktreesDir: "/repo-worktrees",
        cwd: project.workspaceRoot,
        branch: plannedBranch,
      });
      const launched = yield* launches
        .launch(
          launchInput({
            command: "launch:cancel-live-managed",
            thread: "thread:cancel-live-managed",
            message: "Start",
            workspace: { type: "worktree", baseRef: "main" },
          }),
        )
        .pipe(Random.withSeed(launchSeed));
      yield* Deferred.await(entered);
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.createWorktree.mock.calls[0]![0].path, plannedWorktreePath);
      assert.isTrue(yield* tracker.cancel(launched.threadId));
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.runs[0]?.status, "failed");
      assert.deepEqual(harness.removeWorktree.mock.calls, []);
      assert.equal(projection.thread.worktreePath, plannedWorktreePath);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each([0, 1])("releases an async setup before its completion with exit %s", (exitCode) =>
  Effect.gen(function* () {
    const completion = yield* Deferred.make<{ exitCode: number | null; durationMs: number }>();
    const harness = makeHarness({
      runSetup: (setupInput) =>
        Effect.succeed({
          status: "started" as const,
          async: true,
          scriptId: "setup",
          scriptName: "Setup",
          scriptCommand: "vp install",
          terminalId: "setup",
          cwd: setupInput.worktreePath,
          completion: Deferred.await(completion),
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const launched = yield* launches.launch(
        launchInput({
          command: `command:launch:async-${exitCode}`,
          thread: `thread:launch:async-${exitCode}`,
          message: "Start during setup",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* tracker.stream(launched.threadId).pipe(
        Stream.filter(
          (snapshot) =>
            snapshot?.stages.some((stage) => stage.id === "agent" && stage.status === "done") ===
            true,
        ),
        Stream.runHead,
      );
      const running = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(running.runs[0]?.status, "starting");
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "running");
      yield* Deferred.succeed(completion, { exitCode, durationMs: 1 });
      yield* tracker.stream(launched.threadId).pipe(
        Stream.filter((snapshot) => snapshot?.phase === "done"),
        Stream.runHead,
      );
      const settled = yield* tracker.get(launched.threadId);
      assert.equal(
        settled?.stages.find((stage) => stage.id === "setup-script")?.status,
        exitCode === 0 ? "done" : "failed",
      );
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).runs[0]?.status,
        "starting",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect(
  "an automatic-base retry reuses the recorded worktree without selecting another base",
  () => {
    let setupFailures = 1;
    let lookups = 0;
    const harness = makeHarness({
      listRefs: () =>
        Effect.sync(() => {
          lookups += 1;
          return {
            refs: [worktreeBaseRef("main", { isDefault: true })],
            isRepo: true,
            hasPrimaryRemote: false,
            nextCursor: null,
            totalCount: 1,
          };
        }),
      runSetup: () =>
        setupFailures-- > 0
          ? Effect.fail(new Error("setup failed") as never)
          : Effect.succeed({ status: "no-script" as const }),
    });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const launchSeed = "jones-recorded-worktree-reuse-automatic-base-v1";
      const plannedUuid = yield* randomUuidV4.pipe(Random.withSeed(launchSeed));
      const plannedTemporaryBranch = buildTemporaryWorktreeBranchName(() =>
        plannedUuid.replaceAll("-", ""),
      );
      const plannedWorktreePath = nativeWorktreePath({
        worktreesDir: "/repo-worktrees",
        cwd: project.workspaceRoot,
        branch: plannedTemporaryBranch,
      });
      const launched = yield* launches
        .launch(
          launchInput({
            command: "command:launch:reuse",
            thread: "thread:launch:reuse",
            message: "Reuse the worktree",
            workspace: { type: "worktree" },
          }),
        )
        .pipe(Random.withSeed(launchSeed));
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(
            Effect.map(
              (projection) =>
                projection.runs[0]?.status === "failed" &&
                projection.thread.branch === "generated-branch",
            ),
          ),
      );
      const failed = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(failed.thread.worktreePath, plannedWorktreePath);

      yield* launches.retryPreparation({
        commandId: CommandId.make("command:launch:reuse:retry"),
        threadId: launched.threadId,
        runId: failed.runs[0]!.id,
      });
      yield* waitUntil(() =>
        outbox
          .listByCommandId(CommandId.make("command:launch:reuse:retry:release"))
          .pipe(Effect.map((effects) => effects.length === 1)),
      );
      const retried = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(retried.runs[0]?.status, "starting");
      // The retry neither checks out again nor puts back the temporary branch.
      assert.equal(harness.createWorktree.mock.calls.length, 1);
      assert.equal(harness.renameBranch.mock.calls.length, 1);
      assert.equal(retried.thread.branch, "generated-branch");
      assert.equal(retried.thread.worktreePath, plannedWorktreePath);
      // Clients see the retry's setup, not the failed one it replaced.
      const snapshot = yield* tracker.get(launched.threadId);
      assert.equal(snapshot?.phase, "done");
      assert.isNull(snapshot?.baseRef);
      assert.equal(lookups, 1);
      assert.deepEqual(
        snapshot?.stages.map((stage) => stage.id),
        ["setup-script", "agent"],
      );
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect.each([
  { caseName: "the origin remote is missing", hasOrigin: false },
  { caseName: "the base branch exists only locally", hasOrigin: true },
])("uses the local V2 worktree base when $caseName", ({ hasOrigin }) => {
  const expectedWorktreePath = nativeWorktreePath({
    worktreesDir: "/repo-worktrees",
    cwd: project.workspaceRoot,
    branch: "feature/router",
  });
  const fetchRemote = vi.fn(() => Effect.void);
  const remoteLookup = vi.fn(() => Effect.succeed(null));
  const harness = makeHarness({
    remoteExists: () => Effect.succeed(hasOrigin),
    fetchRemote,
    resolveRemoteTrackingCommitIfExists: remoteLookup,
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:local-base",
        thread: "thread:local-base",
        message: "Start locally",
        workspace: {
          type: "worktree",
          baseRef: "main",
          branch: "feature/router",
          startFromOrigin: true,
        },
      }),
    );
    yield* waitUntil(() =>
      threads
        .getThreadProjection(launched.threadId)
        .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
    );
    assert.equal(fetchRemote.mock.calls.length, hasOrigin ? 1 : 0);
    assert.equal(remoteLookup.mock.calls.length, hasOrigin ? 1 : 0);
    assert.equal(harness.createWorktree.mock.calls[0]?.[0].refName, "main");
    assert.equal(harness.createWorktree.mock.calls[0]?.[0].baseRefName, "main");
    assert.equal(
      (yield* threads.getThreadProjection(launched.threadId)).thread.worktreePath,
      expectedWorktreePath,
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect(
  "prefers an exact local branch found on a later ref page over an ambiguous origin ref",
  () => {
    const listRefs = vi.fn((input: Parameters<NonNullable<HarnessOptions["listRefs"]>>[0]) =>
      Effect.succeed({
        refs:
          input.cursor === undefined
            ? [worktreeBaseRef("origin/release", { isRemote: true, remoteName: "origin" })]
            : [worktreeBaseRef("origin/release")],
        isRepo: true,
        hasPrimaryRemote: true,
        nextCursor: input.cursor === undefined ? 1 : null,
        totalCount: 2,
      }),
    );
    const remoteLookup = vi.fn(
      (input: Parameters<NonNullable<HarnessOptions["resolveRemoteTrackingCommitIfExists"]>>[0]) =>
        Effect.succeed({
          commitSha: "local-origin-prefixed-sha",
          remoteRefName: `origin/${input.branchName}`,
        }),
    );
    const fetchRemote = vi.fn(
      (_: Parameters<NonNullable<HarnessOptions["fetchRemote"]>>[0]) => Effect.void,
    );
    const harness = makeHarness({
      listRefs,
      resolveRemoteTrackingCommitIfExists: remoteLookup,
      fetchRemote,
    });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:paged-local",
          thread: "thread:paged-local",
          message: "Use the local spelling",
          workspace: {
            type: "worktree",
            baseRef: "origin/release",
            branch: "feature",
            startFromOrigin: true,
          },
        }),
      );
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
      );
      assert.deepEqual(
        listRefs.mock.calls.map(([input]) => input.cursor),
        [undefined, 1],
      );
      assert.equal(remoteLookup.mock.calls.length, 1);
      assert.equal(remoteLookup.mock.calls[0]?.[0].branchName, "origin/release");
      assert.isUndefined(fetchRemote.mock.calls[0]?.[0].refName);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0].refName, "local-origin-prefixed-sha");
      assert.equal(harness.createWorktree.mock.calls[0]?.[0].baseRefName, "origin/release");
    }).pipe(Effect.provide(harness.layer));
  },
);

function legacyPreflightBinding(name: string, requireWorktree = true) {
  const threadId = ThreadId.make(`preflight:${name}`);
  const releaseCommandId = CommandId.make(`preflight:${name}:C`);
  const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
  const messageId = MessageId.make(`preflight:${name}:M`);
  const payload = {
    type: "thread.turn.start",
    commandId: releaseCommandId,
    threadId,
    message: { messageId, role: "user", text: "Preflight prompt", attachments: [] },
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    createdAt: "2026-10-05T00:00:00.000Z",
    bootstrap: {
      createThread: {
        projectId,
        title: "Preflight",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdAt: "2026-10-05T00:00:00.000Z",
      },
      prepareWorktree: {
        projectCwd: "/repo",
        baseBranch: "main",
        branch: "feature/preflight",
        startFromOrigin: true,
        requireWorktree,
      },
    },
  };
  const canonicalPayload = canonicalLegacyPayload(payload);
  return {
    policy: {
      version: 1 as const,
      createCommandId,
      birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
      releaseCommandId,
      projectId,
      threadId,
      messageId,
      payloadHash: legacyPayloadHash(canonicalPayload),
      ownsNewThread: true,
    },
    canonicalPayload,
    fetch: {
      cwd: "/repo",
      baseRef: "main",
      startFromOrigin: true,
      requireWorktree,
      remote: "origin",
    },
  };
}

it.effect.each([true, false])(
  "legacy preflight validates repository before shell birth and preserves requireWorktree=%s fallback",
  (required) => {
    const fetch = vi.fn(() => Effect.void);
    const h = makeHarness({ isRepository: () => Effect.succeed(false), fetchRemote: fetch });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const binding = legacyPreflightBinding(`nonrepo-${required}`, required);
      const result = yield* launches.preflightLegacyBootstrap(binding);
      assert.equal(result.status, required ? "known_failed" : "ready");
      if (!required) assert.deepEqual(result.workspaceStrategy, { type: "root" });
      assert.isNull(yield* threads.getThreadShell(binding.policy.threadId));
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(binding.policy.releaseCommandId)));
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(binding.policy.createCommandId)));
      assert.equal(fetch.mock.calls.length, 0);
      assert.equal(h.createWorktree.mock.calls.length, 0);
      assert.deepEqual(yield* launches.preflightLegacyBootstrap(binding), result);
    }).pipe(Effect.provide(h.layer));
  },
);

it.effect(
  "legacy preflight commits exact intent before fetch and concurrent retries join its durable outcome",
  () => {
    let actualStore: EventStore.EventStoreV2["Service"] | undefined;
    const binding = legacyPreflightBinding("joined-fetch");
    const fetch = vi.fn(() =>
      Effect.gen(function* () {
        assert.isDefined(actualStore);
        const stored = Array.from(
          yield* actualStore!
            .readByCommandId({
              commandId: CommandId.make(`${binding.policy.createCommandId}:preflight-intent`),
            })
            .pipe(Stream.runCollect),
        );
        assert.equal(stored.length, 1);
        assert.equal(stored[0]!.event.type, "legacy-bootstrap.preflight-intent");
      }).pipe(Effect.orDie),
    );
    const h = makeHarness({
      fetchRemote: fetch,
      legacyPreflightGit: {
        remoteBranchExists: (input) =>
          Effect.sync(() => {
            assert.deepEqual(input, {
              cwd: binding.fetch.cwd,
              refName: binding.fetch.baseRef,
              remoteName: "origin",
            });
            return true;
          }),
        resolveRemoteTrackingCommit: (input) =>
          Effect.sync(() => {
            assert.deepEqual(input, {
              cwd: binding.fetch.cwd,
              refName: binding.fetch.baseRef,
              fallbackRemoteName: "origin",
            });
            return { commitSha: "remote-main-sha", remoteRefName: "origin/main" };
          }),
      },
    });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      actualStore = yield* EventStore.EventStoreV2;
      const joined = yield* Effect.all(
        [launches.preflightLegacyBootstrap(binding), launches.preflightLegacyBootstrap(binding)],
        { concurrency: 2 },
      );
      assert.deepEqual(joined[0], joined[1]);
      assert.equal(joined[0]!.status, "ready");
      assert.deepEqual(joined[0]!.workspaceStrategy, {
        type: "worktree",
        baseRef: "remote-main-sha",
        branch: "feature/preflight",
        startFromOrigin: false,
      });
      assert.equal(fetch.mock.calls.length, 1);
      assert.equal(h.createWorktree.mock.calls.length, 0);
      const records = Array.from(
        yield* actualStore.read({ threadId: binding.policy.threadId }).pipe(Stream.runCollect),
      );
      assert.deepEqual(
        records.map((stored) => stored.event.type),
        ["legacy-bootstrap.preflight-intent", "legacy-bootstrap.preflight-outcome"],
      );
      assert.isNull(
        yield* (yield* ThreadManagement.ThreadManagementService).getThreadShell(
          binding.policy.threadId,
        ),
      );
    }).pipe(Effect.provide(h.layer));
  },
);

it.effect.each(["known", "lost"] as const)(
  "legacy preflight journals %s fetch failure and never repeats it",
  (kind) => {
    const fetch = vi.fn(() =>
      Effect.fail(
        new GitCommandError({
          operation: "GitVcsDriver.fetchRemote",
          command: "git fetch",
          cwd: "/repo",
          detail: "synthetic failure",
          ...(kind === "known" ? { exitCode: 1 } : {}),
        }),
      ),
    );
    const h = makeHarness({ fetchRemote: fetch });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const binding = legacyPreflightBinding(`fetch-${kind}`);
      const result = yield* launches.preflightLegacyBootstrap(binding);
      assert.equal(result.status, kind === "known" ? "known_failed" : "unknown");
      assert.deepEqual(yield* launches.preflightLegacyBootstrap(binding), result);
      assert.equal(fetch.mock.calls.length, 1);
      const threads = yield* ThreadManagement.ThreadManagementService;
      assert.isNull(yield* threads.getThreadShell(binding.policy.threadId));
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(binding.policy.releaseCommandId)));
      if (kind === "lost") {
        const changed = legacyPreflightBinding(`fetch-${kind}`);
        const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(QueueDispatchCommand))(
          changed.canonicalPayload,
        );
        const payload = {
          ...decoded,
          commandId: CommandId.make(`${changed.policy.releaseCommandId}:replacement`),
        };
        const createCommandId = legacyBootstrapCreateCommandId(
          changed.policy.threadId,
          payload.commandId,
        );
        const canonicalPayload = canonicalLegacyPayload(payload);
        const rejected = yield* launches
          .preflightLegacyBootstrap({
            ...changed,
            canonicalPayload,
            policy: {
              ...changed.policy,
              releaseCommandId: payload.commandId,
              createCommandId,
              birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
              payloadHash: legacyPayloadHash(canonicalPayload),
            },
          })
          .pipe(Effect.flip);
        assert.equal(rejected._tag, "ThreadLaunchError");
        assert.equal(fetch.mock.calls.length, 1);
      }
    }).pipe(Effect.provide(h.layer));
  },
);

it.effect(
  "legacy preflight restart treats an accepted intent without outcome as unknown without fetching or replacing it",
  () => {
    const fetch = vi.fn(() => Effect.void);
    const h = makeHarness({ fetchRemote: fetch });
    return Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const binding = legacyPreflightBinding("missing-outcome");
      const intentId = CommandId.make(`${binding.policy.createCommandId}:preflight-intent`);
      yield* sink.commitLegacyPreflight({
        commandId: intentId,
        event: {
          id: EventId.make(`${intentId}:event`),
          type: "legacy-bootstrap.preflight-intent",
          threadId: binding.policy.threadId,
          occurredAt: yield* DateTime.now,
          payload: binding,
        },
      });
      const receivingAfterRestart = yield* makeLegacyPreflight;
      const result = yield* receivingAfterRestart(binding);
      assert.equal(result.status, "unknown");
      assert.equal(fetch.mock.calls.length, 0);
      assert.equal(h.createWorktree.mock.calls.length, 0);
      assert.isNull(
        yield* (yield* ThreadManagement.ThreadManagementService).getThreadShell(
          binding.policy.threadId,
        ),
      );
      assert.isTrue(
        Option.isNone(
          yield* (yield* CommandReceiptStore.CommandReceiptStoreV2).getByCommandId(
            binding.policy.releaseCommandId,
          ),
        ),
      );
    }).pipe(Effect.provide(h.layer));
  },
);

it.layer(NodeServices.layer, { excludeTestServices: true })(
  "Legacy launcher never-invoked failure owner",
  (it) => {
    const qualifyFailure = (
      deletePersistenceFails: boolean,
      changedBinding?: "project_root" | "current_target" | "copied_owner_journal",
    ) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const workspaceRoot = yield* fs.makeTempDirectoryScoped({
          prefix: "legacy-launch-failure-owner-",
        });
        const commonDirectory = `${workspaceRoot}/.git`;
        yield* fs.makeDirectory(commonDirectory);
        const commands: string[][] = [];
        const spawner = ChildProcessSpawner.make((command) =>
          Effect.gen(function* () {
            if (!ChildProcess.isStandardCommand(command))
              return yield* Effect.die("Unexpected synthetic pipeline");
            const args = [...command.args];
            commands.push(args);
            if (args.includes("add"))
              return yield* Effect.die("Never-invoked owner must not enter worktree mutation");
            const stdout = args.includes("--git-common-dir")
              ? `${commonDirectory}\n`
              : args.includes("rev-parse")
                ? `${"a".repeat(40)}\n`
                : "";
            return ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(1),
              exitCode: Effect.succeed(
                ChildProcessSpawner.ExitCode(args.includes("--get-regexp") ? 1 : 0),
              ),
              isRunning: Effect.succeed(false),
              kill: () => Effect.void,
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
        const files = ServerConfig.layerTest(workspaceRoot, `${workspaceRoot}/state`);
        const driverConfig = yield* ServerConfig.ServerConfig.pipe(Effect.provide(files));
        const worktreesDir = driverConfig.worktreesDir;
        const structuralParent = `${worktreesDir}/${workspaceRoot.slice(workspaceRoot.lastIndexOf("/") + 1)}`;
        yield* fs.makeDirectory(structuralParent, { recursive: true });
        const driver = yield* makeGitVcsDriverCore().pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provide(files),
        );
        const manager = yield* TerminalManager.makeWithOptions({
          logsDir: `${workspaceRoot}/terminal-logs`,
          env: {},
          shellResolver: () => "/bin/sh",
          processTable: Effect.succeed([]),
          processKillGraceMs: 1,
          subprocessInspector: () =>
            Effect.succeed({ hasRunningSubprocess: false, childCommand: null, processIds: [] }),
          ptyAdapter: { spawn: () => Effect.die("No-control failure must never spawn") },
        }).pipe(Effect.provide(ProcessRunner.layer));
        const harness = makeHarness({
          workspaceRoot,
          physicalFixture: true,
          worktreesDir,
          resolveCommit: () => Effect.succeed({ commitSha: "a".repeat(40) }),
          terminalOwner: Layer.succeed(TerminalManager.TerminalManager, manager),
          createWorktree: (input, options) => driver.createWorktree(input, options),
        });
        yield* Effect.gen(function* () {
          const launch = yield* ThreadLaunch.ThreadLaunchService;
          const sink = yield* EventSink.EventSinkV2;
          const threads = yield* ThreadManagement.ThreadManagementService;
          const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          let rejectedFailureCommit: Cause.Cause<unknown> | undefined;
          if (changedBinding !== undefined) {
            const currentSql = yield* SqlClient.SqlClient;
            const readEvents = sink.readByCommandId.bind(sink);
            const dispatch = threads.dispatch.bind(threads);
            let authenticFailureStarted = false;
            const eventRead = vi.spyOn(sink, "readByCommandId").mockImplementation((...args) =>
              readEvents(...args).pipe(
                Stream.map((stored) => {
                  if (
                    !authenticFailureStarted ||
                    changedBinding !== "copied_owner_journal" ||
                    stored.event.type !== "run.updated" ||
                    stored.event.payload.legacyPreparation === undefined ||
                    stored.commandId?.endsWith(":outcome") !== true
                  )
                    return stored;
                  return {
                    ...stored,
                    event: {
                      ...stored.event,
                      payload: {
                        ...stored.event.payload,
                        legacyPreparation: {
                          ...stored.event.payload.legacyPreparation,
                          policy: {
                            ...stored.event.payload.legacyPreparation.policy,
                            threadId: ThreadId.make("copied-owner:T"),
                          },
                        },
                      },
                    },
                  };
                }),
              ),
            );
            const failureDispatch = vi.spyOn(threads, "dispatch").mockImplementation((...args) =>
              Effect.gen(function* () {
                if (args[0].type === "prepared-run.fail") {
                  authenticFailureStarted = true;
                  if (changedBinding === "project_root")
                    yield* currentSql`UPDATE projection_projects SET workspace_root = ${`${workspaceRoot}/changed-project`} WHERE project_id = ${projectId}`.pipe(
                      Effect.orDie,
                    );
                  if (changedBinding === "current_target")
                    yield* currentSql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${`${worktreesDir}/unrelated-target`}) WHERE thread_id = ${args[0].threadId}`.pipe(
                      Effect.orDie,
                    );
                }
                return yield* dispatch(...args).pipe(
                  Effect.tapCause((cause) =>
                    Effect.sync(() => {
                      if (args[0].type === "prepared-run.fail") rejectedFailureCommit = cause;
                    }),
                  ),
                );
              }),
            );
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                eventRead.mockRestore();
                failureDispatch.mockRestore();
              }),
            );
          }
          const projectCommandId = CommandId.make("legacy-launch-failure:project");
          yield* sink.commitProjectCommand({
            commandId: projectCommandId,
            projectId,
            commandType: "project.create",
            acceptedAt: yield* DateTime.now,
            event: {
              eventId: EventId.make(`${projectCommandId}:event`),
              aggregateKind: "project",
              aggregateId: projectId,
              occurredAt: project.createdAt,
              commandId: projectCommandId,
              causationEventId: null,
              correlationId: null,
              metadata: {},
              type: "project.created",
              payload: {
                projectId,
                title: project.title,
                workspaceRoot,
                defaultModelSelection: project.defaultModelSelection,
                scripts: [],
                createdAt: project.createdAt,
                updatedAt: project.updatedAt,
              },
            },
          });
          const base = launchInput({
            command: "legacy-launch-failure:C",
            thread: "legacy-launch-failure:T",
            message: "Retain original upload",
          });
          const createCommandId = legacyBootstrapCreateCommandId(base.threadId, base.commandId);
          const policy = {
            version: 1 as const,
            createCommandId,
            birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
            releaseCommandId: base.commandId,
            projectId,
            threadId: base.threadId,
            messageId: base.initialMessage!.messageId,
            payloadHash: "fixture-hash",
            ownsNewThread: true,
          };
          const config = yield* ServerConfig.ServerConfig;
          const attachmentId = createPendingAttachmentId();
          const attachment = {
            id: ChatAttachmentId.make(attachmentId),
            type: "image" as const,
            name: "original.png",
            mimeType: "image/png",
            sizeBytes: 8,
          };
          const uploaded = resolveAttachmentPath({
            attachmentsDir: config.attachmentsDir,
            attachment,
          })!;
          yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
          const bytes = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10]);
          yield* fs.writeFile(uploaded, bytes);
          const sql = yield* SqlClient.SqlClient;
          if (deletePersistenceFails)
            yield* sql`CREATE TEMP TRIGGER fail_launcher_failure_D BEFORE INSERT ON orchestration_events
                WHEN NEW.event_type = 'thread.deleted'
                BEGIN SELECT RAISE(ABORT, 'synthetic launcher D persistence failure'); END`;
          const result = yield* launch
            .launch({
              ...base,
              commandId: createCommandId,
              preparationReleaseCommandId: base.commandId,
              legacyBootstrap: policy,
              runSetupScript: false,
              initialMessage: { ...base.initialMessage!, attachments: [attachment] },
              workspaceStrategy: {
                type: "worktree",
                baseRef: "main",
                branch: "invalid?",
                startFromOrigin: false,
              },
            })
            .pipe(
              Effect.result,
              Effect.ensuring(
                deletePersistenceFails
                  ? sql`DROP TRIGGER fail_launcher_failure_D`.pipe(Effect.orDie, Effect.asVoid)
                  : Effect.void,
              ),
            );
          if (changedBinding !== undefined) {
            assert.isTrue(Result.isFailure(result));
            assert.isDefined(rejectedFailureCommit);
            assert.match(
              Cause.pretty(rejectedFailureCommit!),
              /Never-invoked workspace failure lacks exact birth, owner journal or absent C receipts/,
            );
            const retained = yield* threads.getThreadProjection(base.threadId);
            assert.equal(retained.runs[0]?.status, "preparing");
            assert.isUndefined(retained.runs[0]?.legacyPreparationFailureDecision);
            assert.isNull(retained.thread.deletedAt);
            assert.isTrue(Option.isNone(yield* receipts.getByCommandId(base.commandId)));
            assert.isTrue(
              Option.isNone(
                yield* receipts.getByCommandId(CommandId.make(`${createCommandId}:failure-delete`)),
              ),
            );
            assert.isFalse(commands.some((args) => args.includes("add")));
            assert.deepEqual(Array.from(yield* fs.readFile(uploaded)), Array.from(bytes));
            assert.isFalse(
              (yield* outbox.listByThreadId(base.threadId)).some(
                ({ request }) =>
                  request.type === "provider-turn.start" ||
                  request.type === "attachment.cleanup" ||
                  request.type === "terminal.cleanup",
              ),
            );
            return;
          }
          assert.isTrue(Result.isFailure(result));
          if (Result.isFailure(result)) {
            assert.isTrue(Schema.is(ThreadLaunch.ThreadLaunchError)(result.failure));
            if (Schema.is(ThreadLaunch.ThreadLaunchError)(result.failure)) {
              if (deletePersistenceFails)
                assert.isUndefined(result.failure.bootstrapThreadDisposition);
              else
                assert.equal(
                  result.failure.bootstrapThreadDisposition,
                  "deleted",
                  canonicalLegacyPayload({
                    cause: result.failure.cause,
                    run: (yield* threads.getThreadProjection(base.threadId)).runs[0],
                    D: yield* receipts.getByCommandId(
                      CommandId.make(`${createCommandId}:failure-delete`),
                    ),
                  }),
                );
            }
          }
          const projection = yield* threads.getThreadProjection(base.threadId);
          if (deletePersistenceFails) {
            assert.isNull(projection.thread.deletedAt);
            assert.equal(projection.runs[0]?.status, "failed");
            assert.isDefined(projection.runs[0]?.legacyPreparationFailureDecision);
            assert.isUndefined(projection.runs[0]?.legacyPreparationFailureDecision?.deletion);
          } else {
            assert.isNotNull(projection.thread.deletedAt);
            assert.isDefined(projection.runs[0]?.legacyPreparationFailureDecision?.deletion);
          }
          assert.isTrue(Option.isNone(yield* receipts.getByCommandId(base.commandId)));
          assert.isTrue(Option.isNone(yield* receipts.getProjectByCommandId(base.commandId)));
          const d = yield* receipts.getByCommandId(
            CommandId.make(`${createCommandId}:failure-delete`),
          );
          if (deletePersistenceFails) {
            assert.isFalse(Option.isSome(d) && d.value.status === "accepted");
            assert.deepEqual(
              Array.from(
                yield* (yield* EventStore.EventStoreV2)
                  .readByCommandId({
                    commandId: CommandId.make(`${createCommandId}:failure-delete`),
                  })
                  .pipe(Stream.runCollect),
              ),
              [],
            );
          } else {
            assert.isTrue(Option.isSome(d));
            if (Option.isSome(d)) assert.equal(d.value.status, "accepted");
          }
          assert.deepEqual(
            yield* outbox.listByCommandId(CommandId.make(`${createCommandId}:failure-delete`)),
            [],
          );
          assert.deepEqual(yield* fs.readFile(uploaded), bytes);
          assert.isFalse(commands.some((args) => args.includes("add")));
          assert.isEmpty(harness.removeWorktree.mock.calls);
          assert.isEmpty(harness.runSetup.mock.calls);
          const effects = yield* outbox.listByThreadId(base.threadId);
          assert.isFalse(
            effects.some(
              ({ request }) =>
                request.type === "provider-turn.start" ||
                request.type === "attachment.cleanup" ||
                request.type === "terminal.cleanup",
            ),
          );
        }).pipe(Effect.provide(harness.layer.pipe(Layer.provideMerge(files))));
      });
    it.effect(
      "reports deleted only after actual no-control failure D and retains uploaded bytes",
      () => qualifyFailure(false),
    );
    it.effect(
      "retains the durable failed shell and undefined disposition when actual SQL failure D cannot commit",
      () => qualifyFailure(true),
    );
    it.effect.each(["project_root", "current_target", "copied_owner_journal"] as const)(
      "refuses original failure-D authority for changed %s",
      (changedBinding) => qualifyFailure(false, changedBinding),
    );
  },
);
