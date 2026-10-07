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
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../../project/ManagedProjectFolders.ts";
import { makeProviderRegistryLayer } from "../../provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import type { ProviderAdapterV2Shape } from "../../orchestration-v2/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "../../orchestration-v2/ThreadTitleRegenerationService.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

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

function makeHarness(options: HarnessOptions = {}) {
  const database = SqlitePersistenceMemory;
  const registry = ProviderAdapterRegistry.makeLayer([adapter]);
  const orchestrator = makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "thread-launch" },
    registry,
    { databaseLayer: database, runEffectWorker: false },
  );
  const threadManagement = ThreadManagement.layer.pipe(Layer.provide(orchestrator));
  const receipts = CommandReceiptStore.layer.pipe(Layer.provide(database));
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
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
    Layer.succeed(ProjectService.ProjectService, {
      create: () => Effect.die("unused"),
      bootstrap: () => Effect.die("unused"),
      update: () => Effect.die("unused"),
      delete: () => Effect.die("unused"),
      getById: (id) =>
        Effect.succeed(
          id === projectId
            ? Option.some(project)
            : id === otherProjectId
              ? Option.some(otherProject)
              : Option.none(),
        ),
      getByWorkspaceRoot: () => Effect.succeed(Option.some(project)),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree,
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
    ServerSettings.layerTest(options.serverSettings),
    makeProviderRegistryLayer(options.providers),
    options.managedFolders ??
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        folderForThread: () => Effect.succeed(Option.none()),
      }),
  );
  const launch = ThreadLaunch.layer.pipe(
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
      yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status === "starting",
        ),
        Stream.runHead,
      );
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
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:reuse",
          thread: "thread:launch:reuse",
          message: "Reuse the worktree",
          workspace: { type: "worktree" },
        }),
      );
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
      assert.equal(failed.thread.worktreePath, "/repo-worktrees/feature");

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
  },
);

it.effect.each([
  { caseName: "the origin remote is missing", hasOrigin: false },
  { caseName: "the base branch exists only locally", hasOrigin: true },
])("uses the local V2 worktree base when $caseName", ({ hasOrigin }) => {
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
      "/repo-worktrees/feature",
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
