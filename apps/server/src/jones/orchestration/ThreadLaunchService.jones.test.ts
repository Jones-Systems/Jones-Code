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
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
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
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "../../orchestration-v2/ThreadTitleRegenerationService.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

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
  readonly resolveWorktreeBaseRemote?: GitWorkflow.GitWorkflowService["Service"]["resolveWorktreeBaseRemote"];
  readonly fetchWorktreeBaseCommit?: GitWorkflow.GitWorkflowService["Service"]["fetchWorktreeBaseCommit"];
  readonly listRefs?: GitWorkflow.GitWorkflowService["Service"]["listRefs"];
  readonly renameBranch?: GitWorkflow.GitWorkflowService["Service"]["renameBranch"];
  readonly runSetup?: ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"];
  readonly generateTitle?: TextGeneration.TextGeneration["Service"]["generateThreadTitle"];
  readonly generateBranchName?: TextGeneration.TextGeneration["Service"]["generateBranchName"];
  readonly serverSettings?: Parameters<typeof ServerSettings.layerTest>[0];
  readonly providers?: ReadonlyArray<ServerProvider>;
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
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
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
      getByWorkspaceRoot: () => Effect.succeed(Option.some(project)),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree,
      hasCommit: () => Effect.succeed(false),
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
      assert.deepEqual(fetchWorktreeBaseCommit.mock.calls, [[{ cwd: "/repo", refName: base }]]);
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
    yield* waitUntil(() =>
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
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
      );
      assert.equal(listRefs.mock.calls.length, 0);
      assert.equal(resolveWorktreeBaseRemote.mock.calls.length, 1);
      assert.deepEqual(fetchWorktreeBaseCommit.mock.calls, [
        [{ cwd: "/repo", refName: "origin/release" }],
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
          yield* launches.launch(
            launchInput({
              command: commandId,
              thread: "thread:reservation-observer",
              message: "Observe",
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
      const input = launchInput({
        command: "command:hold-progress",
        thread: "thread:hold-progress",
        message: "Prepare",
      });
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
