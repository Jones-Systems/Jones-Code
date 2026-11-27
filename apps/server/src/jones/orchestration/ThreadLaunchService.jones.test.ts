import * as NodeServices from "@effect/platform-node/NodeServices";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as Logger from "effect/Logger";
import * as ThreadMetadataReads from "./threadMetadataReads.ts";
import * as RepoFreshness from "../git/RepoFreshness.ts";
import * as WorktreeSetupTracker from "../../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import { assert, it, vi } from "@effect/vitest";
import {
  CommandId,
  GitCommandError,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
  type VcsRef,
  ThreadId,
} from "@t3tools/contracts";
import {
  LaunchDeliveryEvidence,
  initialMessageCommandId,
} from "@t3tools/contracts/jones/deliveryEvidence";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../../project/ManagedProjectFolders.ts";
import * as ProviderRegistryMock from "../../provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "../../orchestration-v2/ThreadLaunchService.ts";
import { layerNoTaskFolders } from "../../orchestration-v2/ProviderTurnStartService.testkit.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "../../orchestration-v2/ThreadTitleRegenerationService.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { launchDeliveryEvidence } from "./deliveryEvidence.ts";

const projectId = ProjectId.make("project:launch-test");
const otherProjectId = ProjectId.make("project:launch-other");
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
  readonly managedFolders?: Layer.Layer<ManagedProjectFolders.ManagedProjectFolders>;
  readonly createWorktree?: GitWorkflow.GitWorkflowService["Service"]["createWorktree"];
  readonly hasCommit?: GitWorkflow.GitWorkflowService["Service"]["hasCommit"];
  readonly resolveWorktreeBaseRemote?: GitWorkflow.GitWorkflowService["Service"]["resolveWorktreeBaseRemote"];
  readonly fetchWorktreeBaseCommit?: GitWorkflow.GitWorkflowService["Service"]["fetchWorktreeBaseCommit"];
  readonly listRefs?: GitWorkflow.GitWorkflowService["Service"]["listRefs"];
  readonly renameBranch?: GitWorkflow.GitWorkflowService["Service"]["renameBranch"];
  readonly runSetup?: ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"];
  readonly generateTitle?: TextGeneration.TextGeneration["Service"]["generateThreadTitle"];
  readonly generateBranchName?: TextGeneration.TextGeneration["Service"]["generateBranchName"];
  readonly serverSettings?: Parameters<typeof ServerSettings.layerTest>[0];
  readonly providers?: ReadonlyArray<ServerProvider>;
  readonly repoFreshness?: RepoFreshness.RepoFreshness["Service"];
}

function makeHarness(options: HarnessOptions = {}) {
  const database = SqlitePersistence.layerMemory;
  const registry = ProviderAdapterRegistry.layerFromAdapters([adapter]);
  const orchestrator = ProviderReplayHarness.layerWithRegistry(
    { name: "thread-launch" },
    registry,
    { databaseLayer: database, runEffectWorker: false },
  );
  const receipts = CommandReceiptStore.layer.pipe(Layer.provide(database));
  const serverSettings = ServerSettings.layerTest(options.serverSettings);
  const threadManagement = ThreadManagement.layer.pipe(
    Layer.provide(orchestrator),
    Layer.provide(receipts),
    Layer.provide(serverSettings),
  );
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
    layerNoTaskFolders,
    options.repoFreshness === undefined
      ? Layer.empty
      : Layer.succeed(RepoFreshness.RepoFreshness, options.repoFreshness),
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
    // The project at /repo is a Git repository, as the Git workflow below reports.
    Layer.mock(CheckpointStore.CheckpointStore)({ isGitRepository: () => Effect.succeed(true) }),
    Layer.succeed(ProjectService.ProjectService, {
      create: () => Effect.die("unused"),
      bootstrap: () => Effect.die("unused"),
      update: () => Effect.die("unused"),
      delete: () => Effect.die("unused"),
      listPage: () => Effect.die("unused"),
      getById: (id) =>
        Effect.succeed(
          id === projectId
            ? Option.some(project)
            : id === otherProjectId
              ? Option.some(otherProject)
              : Option.none(),
        ),
      getByWorkspaceRoot: () => Effect.succeedSome(project),
      workspaceScanDirectory: () => Effect.die("unused"),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      repositoryRoot: () => Effect.succeed(project.workspaceRoot),
      currentBranch: () => Effect.succeed(null),
      listLocalBranchNames: () => Effect.succeed([]),
      createWorktree,
      // Every named base exists in this repository; no local branch occupies a
      // generated branch's parent path, so neither the fallback base nor flattening applies.
      hasCommit:
        options.hasCommit ?? (({ refName }) => Effect.succeed(!refName.startsWith("refs/heads/"))),
      renameBranch,
      resolveWorktreeBaseRemote:
        options.resolveWorktreeBaseRemote ??
        (({ refName }) => Effect.succeed({ remoteName: "origin", branchName: refName })),
      fetchWorktreeBaseCommit:
        options.fetchWorktreeBaseCommit ??
        (() => Effect.succeed({ commitSha: "remote-main-sha", remoteRefName: "origin/main" })),
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
      removeWorktree,
      deleteLocalBranch: () => Effect.void,
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
  const metadata = ThreadMetadataReads.layer.pipe(Layer.provide(database));
  // The production owner, as runtimeLayer composes it: no ordinary checkout custody.
  const launch = ThreadLaunch.layerProduction.pipe(
    Layer.provide(metadata),
    Layer.provide(
      Layer.mergeAll(
        externalServices,
        threadManagement,
        receipts,
        IdAllocator.layer,
        outbox,
        orchestrator,
        database,
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
              hasCustomTitle: false,
              autoPull: false,
              pinnedAt: null,
              archivedAt: null,
              faviconPath: null,
              projectIcon: null,
              scripts: project.scripts,
              trackedFolders: [],
              createdAt: project.createdAt,
              updatedAt: project.updatedAt,
              deletedAt: project.deletedAt,
            })
          : Option.none(),
      ),
  });
  const titleRegeneration = ThreadTitleRegeneration.layer.pipe(
    Layer.provide(
      Layer.mergeAll(threadManagement, projectedProjects, externalServices, NodeServices.layer),
    ),
  );
  return {
    layer: Layer.mergeAll(
      metadata,
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

function waitUntil<E, R>(threadId: ThreadId, predicate: () => Effect.Effect<boolean, E, R>) {
  return Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    const reached = yield* threads.streamStoredEventsFrom({ threadId }).pipe(
      Stream.filterEffect(() => predicate()),
      Stream.runHead,
    );
    if (Option.isNone(reached))
      assert.fail("Thread events ended before the condition was reached.");
  });
}

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

it.effect("defers an omitted worktree base to launch without reading Git refs", () => {
  const hasCommit = vi.fn(() => Effect.die("Preflight must not substitute HEAD"));
  const listRefs = vi.fn(() => Effect.die("Launch owns automatic base selection"));
  const harness = makeHarness({ listRefs });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const git = yield* GitWorkflow.GitWorkflowService;
    const commitRead = vi.spyOn(git, "hasCommit").mockImplementation(hasCommit);
    yield* launches
      .checkWorktreeBase({
        projectId,
        workspaceStrategy: { type: "worktree", branch: "feature" },
      })
      .pipe(Effect.ensuring(Effect.sync(() => commitRead.mockRestore())));
    assert.equal(hasCommit.mock.calls.length, 0);
    assert.equal(listRefs.mock.calls.length, 0);
    assert.equal(harness.createWorktree.mock.calls.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

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
      const launched = yield* launches.launch(
        launchInput({
          command: "command:automatic-base-loading",
          thread: "thread:automatic-base-loading",
          message: "Start while branches load",
          workspace: { type: "worktree", branch: "feature" },
        }),
      );
      assert.equal(launched.projection.messages[0]?.text, "Start while branches load");
      assert.equal(launched.projection.runs[0]?.status, "preparing");
      yield* Deferred.await(refsEntered);
      assert.equal(harness.createWorktree.mock.calls.length, 0);
      assert.isNull((yield* tracker.get(launched.threadId))?.baseRef);
      yield* Deferred.succeed(allowRefs, undefined);
      yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status === "starting",
        ),
        Stream.runHead,
      );
      assert.equal(harness.createWorktree.mock.calls[0]?.[0].refName, "develop");
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
    yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "run.updated" && stored.event.payload.status === "starting",
      ),
      Stream.runHead,
    );
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
    yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "run.updated" && stored.event.payload.status === "starting",
      ),
      Stream.runHead,
    );
    assert.equal(harness.createWorktree.mock.calls[0]?.[0].refName, "release/stable");
  }).pipe(Effect.provide(harness.layer));
});

it.effect.each([
  { name: "local base", base: "main", remoteName: "origin", branchName: "main", missing: false },
  {
    name: "origin base",
    base: "origin/release",
    remoteName: "origin",
    branchName: "release",
    missing: false,
  },
  {
    name: "local origin-prefixed branch",
    base: "origin/release",
    remoteName: "origin",
    branchName: "origin/release",
    missing: false,
  },
  {
    name: "other remote",
    base: "upstream/release",
    remoteName: "upstream",
    branchName: "release",
    missing: false,
  },
  {
    name: "missing origin branch",
    base: "local-only",
    remoteName: "origin",
    branchName: "local-only",
    missing: true,
  },
])(
  "pins the fetched V2 worktree base with one lookup for $name",
  ({ base, remoteName, branchName, missing }) => {
    const operations: string[] = [];
    const resolveWorktreeBaseRemote = vi.fn(
      (_: Parameters<NonNullable<HarnessOptions["resolveWorktreeBaseRemote"]>>[0]) =>
        Effect.sync(() => {
          operations.push("resolve");
          return { remoteName, branchName };
        }),
    );
    const fetchWorktreeBaseCommit = vi.fn(
      (_: Parameters<NonNullable<HarnessOptions["fetchWorktreeBaseCommit"]>>[0]) =>
        Effect.sync(() => {
          operations.push("fetch");
          return missing
            ? null
            : { commitSha: "pinned-origin-sha", remoteRefName: `${remoteName}/${branchName}` };
        }),
    );
    const harness = makeHarness({ resolveWorktreeBaseRemote, fetchWorktreeBaseCommit });
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
      yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status === "starting",
        ),
        Stream.runHead,
      );
      assert.deepEqual(operations, ["resolve", "fetch"]);
      assert.deepEqual(resolveWorktreeBaseRemote.mock.calls, [[{ cwd: "/repo", refName: base }]]);
      assert.deepEqual(fetchWorktreeBaseCommit.mock.calls, [
        [{ cwd: "/repo", refName: base, remote: { remoteName, branchName } }],
      ]);
      assert.equal(
        harness.createWorktree.mock.calls[0]?.[0].refName,
        missing ? base : "pinned-origin-sha",
      );
      assert.equal(
        harness.createWorktree.mock.calls[0]?.[0].baseRefName,
        missing ? base : `refs/remotes/${remoteName}/${branchName}`,
      );
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect.each([
  { mode: "verified", fails: false, sync: false, missing: false },
  { mode: "verified", fails: false, sync: false, missing: true },
  { mode: "fetch-now", fails: false, sync: false, missing: false },
  { mode: "fetch-now", fails: false, sync: true, missing: false },
  { mode: "fetch-now", fails: true, sync: false, missing: false },
  { mode: "fetch-now", fails: false, sync: false, missing: true },
] as const)(
  "prepares $mode with fetch failure $fails, primary sync $sync, and missing commit $missing",
  ({ mode, fails, sync, missing }) => {
    const verified = {
      remoteName: "origin",
      branchName: "main",
      commitSha: "a".repeat(40),
      checkedAtMs: 10,
    };
    const newer = { ...verified, commitSha: "b".repeat(40), checkedAtMs: 11 };
    const status = {
      projectId,
      cadenceSeconds: 120,
      target: { status: "resolved", remoteName: "origin" },
      latestVerified: verified,
      latestAttempt: { status: "succeeded", verification: verified },
      freshness: "fresh",
    } satisfies RepoFreshness.RepoFreshnessStatus;
    const read = vi.fn(() => Effect.succeed(status));
    const checked = {
      ...status,
      latestAttempt: fails
        ? { status: "failed" as const, checkedAtMs: 11 }
        : { status: "succeeded" as const, verification: newer },
    };
    const checkForLaunch = vi.fn(() => Effect.succeed(checked));
    const refresh = vi.fn(() =>
      Effect.succeed({
        status: checked,
        checkout: { status: "skipped" as const, reason: "auto-pull-disabled" as const },
      }),
    );
    const ordinaryFetch = vi.fn(() => Effect.die("must use the selected freshness mode"));
    const rejected = fails || (mode === "fetch-now" && missing);
    const preparedBase = missing ? "HEAD" : "refs/remotes/origin/main";
    const harness = makeHarness({
      repoFreshness: { status: read, checkForLaunch, refresh },
      fetchWorktreeBaseCommit: ordinaryFetch,
      hasCommit: ({ refName }) =>
        Effect.succeed(!missing || (refName !== verified.commitSha && refName !== newer.commitSha)),
    });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const launched = yield* launches.launch({
        ...launchInput({
          command: "command:freshness-base",
          thread: "thread:freshness-base",
          message: "Use the verified base",
          workspace: { type: "worktree", branch: "feature", baseFreshness: mode },
        }),
        ...(sync ? { syncPrimaryOnFreshFetch: true as const } : {}),
      });
      yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
        Stream.filter(
          ({ event }) =>
            event.type === "run.updated" &&
            event.payload.status === (rejected ? "failed" : "starting"),
        ),
        Stream.runHead,
      );
      assert.lengthOf(ordinaryFetch.mock.calls, 0);
      assert.lengthOf(read.mock.calls, mode === "verified" ? 1 : 0);
      assert.lengthOf(checkForLaunch.mock.calls, mode === "fetch-now" && !sync ? 1 : 0);
      assert.lengthOf(refresh.mock.calls, mode === "fetch-now" && sync ? 1 : 0);
      assert.lengthOf(harness.createWorktree.mock.calls, rejected ? 0 : 1);
      const snapshot = yield* tracker.get(launched.threadId);
      const fetchStage = snapshot?.stages.find(({ id }) => id === "fetch");
      if (rejected) {
        assert.equal(fetchStage?.status, "failed");
      } else {
        assert.equal(
          harness.createWorktree.mock.calls[0]?.[0].refName,
          missing ? "HEAD" : mode === "verified" ? verified.commitSha : newer.commitSha,
        );
        assert.equal(harness.createWorktree.mock.calls[0]?.[0].baseRefName, preparedBase);
        assert.equal(snapshot?.baseRef, preparedBase);
        if (missing) assert.include(fetchStage?.detail, "Using the local base.");
      }
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("keeps lookup failures visible without creating a worktree or starting its run", () => {
  const harness = makeHarness({
    fetchWorktreeBaseCommit: () =>
      Effect.fail(
        new GitCommandError({
          operation: "GitVcsDriver.fetchWorktreeBaseCommit",
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

it.effect(
  "an automatic-base retry reuses the recorded worktree without selecting another base",
  () =>
    Effect.gen(function* () {
      const renamedWorkspaceRecorded = yield* Deferred.make<void>();
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
            ? Deferred.await(renamedWorkspaceRecorded).pipe(
                Effect.andThen(Effect.fail(new Error("setup failed") as never)),
              )
            : Effect.succeed({ status: "no-script" as const }),
      });
      return yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
        yield* threads
          .streamStoredEventsFrom({ threadId: ThreadId.make("thread:launch:reuse") })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "thread.metadata-updated" &&
                stored.event.payload.branch === "generated-branch",
            ),
            Stream.runHead,
            Effect.andThen(Deferred.succeed(renamedWorkspaceRecorded, undefined)),
            Effect.forkChild,
          );
        const launched = yield* launches.launch(
          launchInput({
            command: "command:launch:reuse",
            thread: "thread:launch:reuse",
            message: "Reuse the worktree",
            workspace: { type: "worktree" },
          }),
        );
        yield* waitUntil(launched.threadId, () =>
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
        assert.equal(failed.thread.worktreePath, "/repo-worktrees/feature");

        yield* launches.retryPreparation({
          commandId: CommandId.make("command:launch:reuse:retry"),
          threadId: launched.threadId,
          runId: failed.runs[0]!.id,
        });
        yield* waitUntil(launched.threadId, () =>
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
        assert.equal(retried.thread.worktreePath, "/repo-worktrees/feature");
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
    }),
);

it.effect.each([
  { caseName: "the origin remote is missing", hasOrigin: false },
  { caseName: "the base branch exists only locally", hasOrigin: true },
])("uses the local V2 worktree base when $caseName", ({ hasOrigin }) => {
  const fetchWorktreeBaseCommit = vi.fn(() => Effect.succeed(null));
  const resolveWorktreeBaseRemote = vi.fn(() =>
    Effect.succeed(hasOrigin ? { remoteName: "origin", branchName: "main" } : null),
  );
  const harness = makeHarness({
    resolveWorktreeBaseRemote,
    fetchWorktreeBaseCommit,
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
    yield* waitUntil(launched.threadId, () =>
      threads
        .getThreadProjection(launched.threadId)
        .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
    );
    assert.equal(fetchWorktreeBaseCommit.mock.calls.length, hasOrigin ? 1 : 0);
    assert.equal(resolveWorktreeBaseRemote.mock.calls.length, 1);
    assert.equal(harness.createWorktree.mock.calls[0]?.[0].refName, "main");
    assert.equal(harness.createWorktree.mock.calls[0]?.[0].baseRefName, "main");
    assert.equal(
      (yield* threads.getThreadProjection(launched.threadId)).thread.worktreePath,
      "/repo-worktrees/feature",
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect(
  "prefers an exact local branch found on a later ref page over an ambiguous origin ref",
  () => {
    const listRefs = vi.fn(() => Effect.die("Exact base resolution must not page display refs"));
    const resolveWorktreeBaseRemote = vi.fn(() =>
      Effect.succeed({ remoteName: "origin", branchName: "origin/release" }),
    );
    const fetchWorktreeBaseCommit = vi.fn(
      (_: Parameters<NonNullable<HarnessOptions["fetchWorktreeBaseCommit"]>>[0]) =>
        Effect.succeed({
          commitSha: "local-origin-prefixed-sha",
          remoteRefName: "origin/origin/release",
        }),
    );
    const harness = makeHarness({ listRefs, resolveWorktreeBaseRemote, fetchWorktreeBaseCommit });
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
      yield* waitUntil(launched.threadId, () =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
      );
      assert.equal(listRefs.mock.calls.length, 0);
      assert.equal(resolveWorktreeBaseRemote.mock.calls.length, 1);
      assert.deepEqual(fetchWorktreeBaseCommit.mock.calls, [
        [
          {
            cwd: "/repo",
            refName: "origin/release",
            remote: { remoteName: "origin", branchName: "origin/release" },
          },
        ],
      ]);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0].refName, "local-origin-prefixed-sha");
      assert.equal(
        harness.createWorktree.mock.calls[0]?.[0].baseRefName,
        "refs/remotes/origin/origin/release",
      );
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect(
  "observes preparation ownership while active, after scope release, and in a fresh layer",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const hold = yield* Deferred.make<void>();
      const commandId = CommandId.make("command:reservation-observer");
      const harness = makeHarness({
        runSetup: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(hold)),
            Effect.as({ status: "no-script" as const }),
          ),
      });
      const observed = yield* Effect.scoped(
        Effect.gen(function* () {
          const launches = yield* ThreadLaunch.ThreadLaunchService;
          assert.isFalse(yield* launches.preparationActive(commandId));
          // Setup runs only in a checkout; an existing worktree holds preparation there.
          yield* launches.launch(
            launchInput({
              command: commandId,
              thread: "thread:reservation-observer",
              message: "Observe",
              workspace: {
                type: "existing_worktree",
                worktreePath: "/repo-worktrees/reservation-observer",
              },
            }),
          );
          yield* Deferred.await(entered);
          assert.isTrue(yield* launches.preparationActive(commandId));
          return launches;
        }).pipe(Effect.provide(harness.layer)),
      );
      assert.isFalse(yield* observed.preparationActive(commandId));
      yield* Effect.gen(function* () {
        const fresh = yield* ThreadLaunch.ThreadLaunchService;
        assert.isFalse(yield* fresh.preparationActive(commandId));
      }).pipe(Effect.provide(makeHarness().layer));
    }),
);

it.effect.each([
  { hold: true, progress: true, schedules: false },
  { hold: true, progress: false, schedules: true },
  { hold: false, progress: true, schedules: true },
])("T15: preparation reservation policy %j", ({ hold, progress, schedules }) =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const resume = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(resume)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      // Setup runs only in a checkout; an existing worktree holds preparation there.
      const worktreePath = "/repo-worktrees/hold-progress";
      const input = launchInput({
        command: "command:hold-progress",
        thread: "thread:hold-progress",
        message: "Prepare",
        workspace: { type: "existing_worktree", worktreePath },
      });
      // An earlier attempt's claim and split first message, before it scheduled preparation.
      yield* threads.dispatch({
        type: "thread.create",
        commandId: input.commandId,
        threadId: input.threadId,
        projectId,
        title: input.title,
        modelSelection,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
        branch: null,
        worktreePath,
        createdBy: "user",
        creationSource: "web",
      });
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`${input.commandId}:initial-message`),
        threadId: input.threadId,
        messageId: input.initialMessage!.messageId,
        text: "Prepare",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "defer_start", workspaceStrategy: input.workspaceStrategy },
        createdBy: "user",
        creationSource: "web",
      });
      if (progress)
        yield* receipts.insertIfAbsent({
          commandId: CommandId.make(`${input.commandId}:progress:setup`),
          threadId: input.threadId,
          commandType: "prepared-run.progress",
          acceptedAt: yield* DateTime.now,
          resultSequence: 0,
          status: "accepted",
          error: null,
        });
      const request = {
        ...input,
        ...(hold ? { holdPreparationAfterProgress: true as const } : {}),
      };
      yield* launches.launch(request);
      if (schedules) {
        yield* Deferred.await(entered);
        assert.isTrue(yield* launches.preparationActive(input.commandId));
        yield* launches.launch(request);
        assert.equal(harness.runSetup.mock.calls.length, 1);
      } else {
        assert.isFalse(yield* launches.preparationActive(input.commandId));
        assert.equal(harness.runSetup.mock.calls.length, 0);
      }
      assert.equal(harness.createWorktree.mock.calls.length, 0);
    }).pipe(Effect.provide(harness.layer));
  }),
);

const launchEvidenceCodec = Schema.toCodecJson(LaunchDeliveryEvidence);
const userMessageIds = (
  messages: ReadonlyArray<{ readonly id: MessageId; readonly role: string }>,
) => messages.filter((message) => message.role === "user").map((message) => message.id);

it.effect(
  "a combined first-message claim proves delivery only from its own exact receipt and events",
  () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const input = launchInput({
        command: "command:combined-evidence",
        thread: "thread:combined-evidence",
        message: "Combined first prompt",
      });
      const messageId = input.initialMessage!.messageId;
      const derived = initialMessageCommandId(input.commandId);
      const launched = yield* launches.launch(input);
      assert.isFalse(launched.resumed);
      // The new thread's own claim carried its first prompt; the derived key holds no command.
      const claim = yield* receipts.getByCommandId(input.commandId);
      assert.isTrue(Option.isSome(claim));
      assert.deepInclude(Option.getOrThrow(claim), {
        commandType: "thread.create",
        status: "accepted",
        threadId: launched.threadId,
      });
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(derived)));
      const claimEvents = Array.from(
        yield* eventSink.readByCommandId({ commandId: input.commandId }).pipe(Stream.runCollect),
      );
      assert.isTrue(claimEvents.some(({ event }) => event.type === "thread.created"));
      assert.isTrue(
        claimEvents.some(
          ({ event }) =>
            event.type === "message.updated" &&
            event.payload.role === "user" &&
            event.payload.id === messageId,
        ),
      );
      assert.isTrue(
        claimEvents.some(
          ({ event }) => event.type === "run.created" && event.payload.userMessageId === messageId,
        ),
      );
      const expected: LaunchDeliveryEvidence = {
        kind: "launch",
        commandId: input.commandId,
        projectId,
        creation: { status: "accepted", threadId: launched.threadId },
        initialMessage: { status: "accepted", commandId: derived, messageId },
      };
      const evidence = yield* launches.getDeliveryEvidence(input);
      assert.deepEqual(evidence, expected);
      // The wire contract names the first prompt by its derived key.
      assert.deepEqual(
        yield* Schema.decodeEffect(launchEvidenceCodec)(
          yield* Schema.encodeEffect(launchEvidenceCodec)(evidence),
        ),
        expected,
      );

      // Another first prompt, project, thread or claim type is never this launch's evidence.
      const unknownMessage = { status: "unknown", commandId: derived, messageId } as const;
      const otherMessageId = MessageId.make("combined-evidence:another-first");
      const otherMessage = yield* launches.getDeliveryEvidence({
        ...input,
        initialMessage: { ...input.initialMessage!, messageId: otherMessageId },
      });
      assert.deepEqual(otherMessage.creation, expected.creation);
      assert.deepEqual(otherMessage.initialMessage, {
        status: "unknown",
        commandId: derived,
        messageId: otherMessageId,
      });
      for (const mismatched of [
        { ...input, projectId: otherProjectId },
        { ...input, threadId: ThreadId.make("thread:combined-evidence:other") },
        { ...input, reuseExistingThread: true },
      ]) {
        const refused = yield* launches.getDeliveryEvidence(mismatched);
        assert.deepEqual(refused.creation, { status: "unknown" });
        assert.deepEqual(refused.initialMessage, unknownMessage);
      }
      const { initialMessage: _firstMessage, ...withoutMessage } = input;
      assert.deepEqual((yield* launches.getDeliveryEvidence(withoutMessage)).initialMessage, {
        status: "not-requested",
      });

      // The claim's own events must carry the run; another command's events prove nothing.
      const withoutRun = yield* launchDeliveryEvidence(
        receipts,
        {
          readByCommandId: (request) =>
            eventSink
              .readByCommandId(request)
              .pipe(Stream.filter(({ event }) => event.type !== "run.created")),
        },
        input,
      );
      assert.deepEqual(withoutRun.initialMessage, unknownMessage);
      const other = launchInput({
        command: "command:combined-evidence:foreign",
        thread: "thread:combined-evidence:foreign",
        message: "Another thread's prompt",
      });
      yield* launches.launch(other);
      const foreign = yield* launchDeliveryEvidence(
        receipts,
        { readByCommandId: () => eventSink.readByCommandId({ commandId: other.commandId }) },
        input,
      );
      assert.deepEqual(foreign.creation, { status: "unknown" });
      assert.deepEqual(foreign.initialMessage, unknownMessage);

      // A same-key replay returns the recorded claim: one first message, one run, same evidence.
      const replayed = yield* launches.launch(input);
      assert.isTrue(replayed.resumed);
      assert.equal(replayed.threadId, launched.threadId);
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.deepEqual(userMessageIds(projection.messages), [messageId]);
      assert.lengthOf(projection.runs, 1);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(derived)));
      assert.deepEqual(yield* launches.getDeliveryEvidence(input), expected);

      // An unreadable or occupied derived key can neither confirm nor rule out another delivery.
      const unreadable = yield* launchDeliveryEvidence(
        {
          getByCommandId: (commandId) =>
            commandId === derived
              ? Effect.fail(new CommandReceiptStore.CommandReceiptStoreReadError({ commandId }))
              : receipts.getByCommandId(commandId),
        },
        eventSink,
        input,
      );
      assert.deepEqual(unreadable.creation, expected.creation);
      assert.deepEqual(unreadable.initialMessage, unknownMessage);
      yield* receipts.insertIfAbsent({
        commandId: derived,
        commandType: "message.dispatch",
        threadId: launched.threadId,
        status: "accepted",
        acceptedAt: yield* DateTime.now,
        resultSequence: 0,
        error: null,
      });
      const conflicting = yield* launches.getDeliveryEvidence(input);
      assert.deepEqual(conflicting.creation, expected.creation);
      assert.deepEqual(conflicting.initialMessage, unknownMessage);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("a shell claim proves its first message only through its derived dispatch", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    const input = launchInput({
      command: "command:split-evidence",
      thread: "thread:split-evidence",
      message: "Split first prompt",
    });
    const messageId = input.initialMessage!.messageId;
    const derived = initialMessageCommandId(input.commandId);
    // A claim recorded without its first prompt, as an interrupted or older launch left it.
    yield* threads.dispatch({
      type: "thread.create",
      commandId: input.commandId,
      threadId: input.threadId,
      projectId,
      title: input.title,
      modelSelection,
      runtimeMode: input.runtimeMode,
      interactionMode: input.interactionMode,
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    const shell = yield* launches.getDeliveryEvidence(input);
    assert.deepEqual(shell.creation, { status: "accepted", threadId: input.threadId });
    assert.deepEqual(shell.initialMessage, { status: "unknown", commandId: derived, messageId });

    const resumed = yield* launches.launch(input);
    assert.isFalse(resumed.resumed);
    const messageReceipt = yield* receipts.getByCommandId(derived);
    assert.isTrue(Option.isSome(messageReceipt));
    assert.deepInclude(Option.getOrThrow(messageReceipt), {
      commandType: "message.dispatch",
      status: "accepted",
      threadId: input.threadId,
    });
    const expected: LaunchDeliveryEvidence = {
      kind: "launch",
      commandId: input.commandId,
      projectId,
      creation: { status: "accepted", threadId: input.threadId },
      initialMessage: { status: "accepted", commandId: derived, messageId },
    };
    assert.deepEqual(yield* launches.getDeliveryEvidence(input), expected);
    const otherMessageId = MessageId.make("split-evidence:another-first");
    assert.deepEqual(
      (yield* launches.getDeliveryEvidence({
        ...input,
        initialMessage: { ...input.initialMessage!, messageId: otherMessageId },
      })).initialMessage,
      { status: "unknown", commandId: derived, messageId: otherMessageId },
    );

    // A same-key replay reuses both receipts: one first message, one run, same evidence.
    const replayed = yield* launches.launch(input);
    assert.isTrue(replayed.resumed);
    const projection = yield* threads.getThreadProjection(input.threadId);
    assert.deepEqual(userMessageIds(projection.messages), [messageId]);
    assert.lengthOf(projection.runs, 1);
    assert.deepEqual(yield* launches.getDeliveryEvidence(input), expected);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("a reused empty thread takes its first message in its own command, not its claim", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    const draftCommandId = CommandId.make("command:reuse-evidence:draft");
    const threadId = ThreadId.make("thread:reuse-evidence");
    yield* threads.dispatch({
      type: "thread.create",
      commandId: draftCommandId,
      threadId,
      projectId,
      title: "Draft",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    const input = {
      ...launchInput({
        command: "command:reuse-evidence",
        thread: threadId,
        message: "Reuse first prompt",
      }),
      reuseExistingThread: true,
    };
    const messageId = input.initialMessage!.messageId;
    const derived = initialMessageCommandId(input.commandId);
    yield* launches.launch(input);
    assert.deepInclude(Option.getOrThrow(yield* receipts.getByCommandId(input.commandId)), {
      commandType: "thread.metadata.update",
      status: "accepted",
      threadId,
    });
    assert.deepInclude(Option.getOrThrow(yield* receipts.getByCommandId(derived)), {
      commandType: "message.dispatch",
      status: "accepted",
      threadId,
    });
    const expected: LaunchDeliveryEvidence = {
      kind: "launch",
      commandId: input.commandId,
      projectId,
      creation: { status: "accepted", threadId },
      initialMessage: { status: "accepted", commandId: derived, messageId },
    };
    assert.deepEqual(yield* launches.getDeliveryEvidence(input), expected);
    // The same key read as a new-thread claim, or the draft's own creation, is not this claim.
    for (const mismatched of [
      { ...input, reuseExistingThread: false },
      { ...input, commandId: draftCommandId },
    ]) {
      const refused = yield* launches.getDeliveryEvidence(mismatched);
      assert.deepEqual(refused.creation, { status: "unknown" });
      assert.equal(refused.initialMessage.status, "unknown");
    }

    const replayed = yield* launches.launch(input);
    assert.isTrue(replayed.resumed);
    const projection = yield* threads.getThreadProjection(threadId);
    assert.deepEqual(userMessageIds(projection.messages), [messageId]);
    assert.lengthOf(projection.runs, 1);
    assert.deepEqual(yield* launches.getDeliveryEvidence(input), expected);
  }).pipe(Effect.provide(harness.layer));
});

it.live(
  "rename discovers only matching IDs across passes and continues after a guarded child rejects",
  () =>
    Effect.gen(function* () {
      const generation = yield* Deferred.make<void>();
      const allowGeneration = yield* Deferred.make<void>();
      const createdWorktree = yield* Deferred.make<string>();
      const discoveryDone = yield* Deferred.make<void>();
      const harness = makeHarness({
        createWorktree: (input) => {
          if (input.newRefName === undefined || input.newRefName === null)
            return Effect.die("Expected a reserved launch branch");
          const refName = input.newRefName;
          return Deferred.succeed(createdWorktree, refName).pipe(
            Effect.as({
              worktree: { path: "/repo-worktrees/feature", refName, headSha: "abc" },
            }),
          );
        },
        generateBranchName: () =>
          Deferred.succeed(generation, undefined).pipe(
            Effect.andThen(Deferred.await(allowGeneration)),
            Effect.as({ branch: "generated-branch" }),
          ),
      });
      const warnings: unknown[] = [];
      const logger = Logger.make((options) => {
        warnings.push(options.message);
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const metadata = yield* ThreadMetadataReads.ThreadMetadataReads;
        const sql = yield* SqlClient.SqlClient;
        const parent = ThreadId.make("metadata-rename:parent");
        const child = ThreadId.make("metadata-rename:child");
        const grandchild = ThreadId.make("metadata-rename:grandchild");
        const archived = ThreadId.make("metadata-rename:archived");
        const rejected = ThreadId.make("metadata-rename:rejected");
        const otherPath = ThreadId.make("metadata-rename:other-path");
        const otherBranch = ThreadId.make("metadata-rename:other-branch");
        const invalid = ThreadId.make("metadata-rename:invalid");
        let oldBranch = "";
        const create = (id: ThreadId, path = "/repo-worktrees/feature", branch = oldBranch) =>
          threads
            .dispatch({
              type: "thread.create",
              commandId: CommandId.make(`${id}:create`),
              threadId: id,
              projectId,
              title: id,
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch,
              worktreePath: path,
              createdBy: "user",
              creationSource: "web",
            })
            .pipe(
              Effect.tap(
                () => sql`UPDATE orchestration_v2_projection_threads
        SET payload_json = json_set(payload_json, '$.lineage.parentThreadId', ${id === grandchild ? child : parent},
          '$.lineage.relationshipToParent', 'subagent', '$.lineage.rootThreadId', ${parent})
        WHERE thread_id = ${id}`,
              ),
            );
        const input = launchInput({
          command: "metadata-rename:launch",
          thread: parent,
          message: "Rename this shared worktree",
          workspace: { type: "worktree", baseRef: "main" },
        });
        yield* launches.launch(input);
        yield* Deferred.await(generation);
        oldBranch = yield* Deferred.await(createdWorktree);
        yield* create(child);
        yield* create(archived);
        yield* threads.dispatch({
          type: "thread.archive",
          commandId: CommandId.make(`${archived}:archive`),
          threadId: archived,
        });
        yield* create(rejected);
        yield* create(otherPath, "/different");
        yield* create(otherBranch, "/repo-worktrees/feature", "different");
        yield* create(invalid, "/different");
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = '{}'
        WHERE thread_id = ${invalid}`;
        const actualDispatch = threads.dispatch;
        const actualListIds = metadata.listIds;
        const dispatched: ThreadId[] = [];
        const discoveryStatements: string[] = [];
        const capture: Statement.Transformer = (statement) => {
          discoveryStatements.push(statement.compile()[0]);
          return Effect.succeed(statement);
        };
        const dispatchSpy = vi.spyOn(threads, "dispatch").mockImplementation((command) =>
          Effect.gen(function* () {
            if (
              command.type === "thread.metadata.update" &&
              command.commandId.includes(":branch-rename")
            ) {
              dispatched.push(command.threadId);
              if (command.threadId === child) yield* create(grandchild).pipe(Effect.orDie);
              if (command.threadId === rejected) {
                // Keep the row discoverable after a real guard rejection. Only the
                // visited set prevents this unchanged row from being dispatched again.
                return yield* actualDispatch({ ...command, expectedBranch: "stale-selection" });
              }
            }
            return yield* actualDispatch(command);
          }),
        );
        const listSpy = vi.spyOn(metadata, "listIds").mockImplementation((query) =>
          actualListIds(query).pipe(
            Effect.provideService(Statement.CurrentTransformer, capture),
            Effect.tap((ids) =>
              ids.every((id) => id === rejected) && dispatched.includes(grandchild)
                ? Deferred.succeed(discoveryDone, undefined)
                : Effect.void,
            ),
          ),
        );
        yield* Deferred.succeed(allowGeneration, undefined).pipe(
          Effect.andThen(Deferred.await(discoveryDone).pipe(Effect.timeout("5 seconds"))),
          Effect.ensuring(
            Effect.sync(() => {
              dispatchSpy.mockRestore();
              listSpy.mockRestore();
            }),
          ),
        );
        // Parent first; a grandchild inserted during dispatch requires a second discovery pass.
        assert.equal(dispatched[0], parent);
        assert.sameMembers(dispatched, [parent, child, grandchild, archived, rejected]);
        assert.equal(new Set(dispatched).size, dispatched.length);
        for (const id of [parent, child, grandchild, archived]) {
          assert.equal((yield* threads.getThreadProjection(id)).thread.branch, "generated-branch");
        }
        assert.equal((yield* threads.getThreadProjection(rejected)).thread.branch, oldBranch);
        assert.equal((yield* threads.getThreadProjection(otherPath)).thread.branch, oldBranch);
        assert.equal((yield* threads.getThreadProjection(otherBranch)).thread.branch, "different");
        assert.isTrue(
          discoveryStatements.some((query) =>
            query.includes("orchestration_v2_projection_threads"),
          ),
        );
        assert.isFalse(
          discoveryStatements.some((query) =>
            /orchestration_events|projection_(runs|turn_items|provider_threads)/.test(query),
          ),
        );
        assert.include(JSON.stringify(warnings), "Shared thread worktree branch update failed");
      }).pipe(
        Effect.provide(
          harness.layer.pipe(
            Layer.provideMerge(Logger.layer([logger], { mergeWithExisting: false })),
          ),
        ),
      );
    }),
);
