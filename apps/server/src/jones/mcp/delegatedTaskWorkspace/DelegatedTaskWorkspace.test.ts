import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, describe, it, vi } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  GitCommandError,
  MessageId,
  OrchestratorMcpFailure,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ServerProvider,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as GitWorkflow from "../../../git/GitWorkflowService.ts";
import * as OrchestratorMcpService from "../../../mcp/OrchestratorMcpService.ts";
import { CodexProviderCapabilitiesV2 } from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../../../orchestration-v2/EffectOutbox.ts";
import * as EventStore from "../../../orchestration-v2/EventStore.ts";
import * as ProjectStore from "../../../orchestration-v2/ProjectStore.ts";
import * as ProviderAdapterRegistry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ProviderReplayHarness from "../../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../../../persistence/Sqlite.ts";
import * as RemoteDelegation from "../../../peer/RemoteDelegation.ts";
import * as ManagedProjectFolders from "../../../project/ManagedProjectFolders.ts";
import * as ProjectCloneTracker from "../../../project/ProjectCloneTracker.ts";
import * as ProjectService from "../../../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../../../project/ProjectSetupScriptRunner.ts";
import * as WorktreeSetupTracker from "../../../project/WorktreeSetupTracker.ts";
import * as ProviderRegistryMock from "../../../provider/testUtils/providerRegistryMock.ts";
import * as ScheduledTasks from "../../../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../../../secrets/SecretRequests.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as TerminalManager from "../../../terminal/Manager.ts";
import * as TextGeneration from "../../../textGeneration/TextGeneration.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import {
  resolveDelegatedTaskProject,
  resolveExplicitDelegatedTaskWorkspace,
} from "./DelegatedTaskWorkspace.ts";

const projectId = ProjectId.make("project:delegated-workspace");
const project = { id: projectId, workspaceRoot: "/repo" };

describe("explicit delegated task workspace", () => {
  const unusedGit = {
    listWorktreePaths: () => Effect.die("worktree listing is not expected"),
  };
  const unusedLaunches = {
    checkWorktreeBase: () => Effect.die("base lookup is not expected"),
  };

  it.effect("leaves legacy isolation to its own resolution without reading the project", () =>
    Effect.gen(function* () {
      for (const isolation of [undefined, "shared", "worktree"] as const) {
        const resolved = yield* resolveExplicitDelegatedTaskWorkspace({
          request: isolation === undefined ? {} : { isolation },
          isRetryOfAcceptedDelegation: false,
          project: Effect.die("project is not needed"),
          git: unusedGit,
          launches: unusedLaunches,
        });
        assert.isUndefined(resolved);
      }
    }),
  );

  it.effect("refuses isolation with workspaceStrategy, including on a replayed request", () =>
    Effect.gen(function* () {
      for (const isRetryOfAcceptedDelegation of [false, true]) {
        const error = yield* resolveExplicitDelegatedTaskWorkspace({
          request: {
            isolation: "worktree",
            workspaceStrategy: { type: "worktree", baseRef: "main" },
          },
          isRetryOfAcceptedDelegation,
          project: Effect.die("project is not needed"),
          git: unusedGit,
          launches: unusedLaunches,
        }).pipe(Effect.flip);
        assert.equal(error.code, "invalid_request");
        assert.include(error.message, "not both");
      }
    }),
  );

  it.effect("replays an accepted delegation without validating its workspace again", () =>
    Effect.gen(function* () {
      const resolved = yield* resolveExplicitDelegatedTaskWorkspace({
        request: { workspaceStrategy: { type: "existing_worktree", worktreePath: "/gone" } },
        isRetryOfAcceptedDelegation: true,
        project: Effect.die("project is not needed"),
        git: unusedGit,
        launches: unusedLaunches,
      });
      assert.isUndefined(resolved);
    }),
  );

  it.effect("admits only the project's git worktrees and fails closed when listing fails", () =>
    Effect.gen(function* () {
      const listWorktreePaths = vi.fn((cwd: string) =>
        cwd === "/repo"
          ? Effect.succeed(["/repo", "/repo-worktrees/lane"])
          : Effect.die(`unexpected listing cwd ${cwd}`),
      );
      const admitted = yield* resolveExplicitDelegatedTaskWorkspace({
        request: {
          workspaceStrategy: {
            type: "existing_worktree",
            worktreePath: "/repo-worktrees/lane",
            branch: "work/lane",
          },
        },
        isRetryOfAcceptedDelegation: false,
        project: Effect.succeed(project),
        git: { listWorktreePaths },
        launches: unusedLaunches,
      });
      assert.deepEqual(admitted, {
        type: "existing_worktree",
        worktreePath: "/repo-worktrees/lane",
        branch: "work/lane",
      });
      const foreign = yield* resolveExplicitDelegatedTaskWorkspace({
        request: { workspaceStrategy: { type: "existing_worktree", worktreePath: "/other/repo" } },
        isRetryOfAcceptedDelegation: false,
        project: Effect.succeed(project),
        git: { listWorktreePaths },
        launches: unusedLaunches,
      }).pipe(Effect.flip);
      assert.equal(foreign.code, "invalid_request");
      assert.include(foreign.message, "No task was created");
      const unlisted = yield* resolveExplicitDelegatedTaskWorkspace({
        request: {
          workspaceStrategy: { type: "existing_worktree", worktreePath: "/repo-worktrees/lane" },
        },
        isRetryOfAcceptedDelegation: false,
        project: Effect.succeed(project),
        git: {
          listWorktreePaths: () =>
            Effect.fail(
              new GitCommandError({
                operation: "listWorktreePaths",
                command: "git",
                cwd: "/repo",
                detail: "not a repository",
              }),
            ),
        },
        launches: unusedLaunches,
      }).pipe(Effect.flip);
      assert.equal(unlisted.code, "invalid_request");
    }),
  );

  it.effect("checks a named base against the parent's project before accepting it", () =>
    Effect.gen(function* () {
      const checkWorktreeBase = vi.fn(
        (input: Parameters<ThreadLaunch.ThreadLaunchService["Service"]["checkWorktreeBase"]>[0]) =>
          input.workspaceStrategy.type === "worktree" &&
          input.workspaceStrategy.baseRef === "missing"
            ? Effect.fail(
                new ThreadLaunch.ThreadLaunchBaseRefError({
                  projectId,
                  projectTitle: "Project",
                  workspaceRoot: "/repo",
                  baseRef: "missing",
                }),
              )
            : Effect.void,
      );
      const accepted = yield* resolveExplicitDelegatedTaskWorkspace({
        request: {
          workspaceStrategy: { type: "worktree", baseRef: "feature/base", branch: "feature/lane" },
        },
        isRetryOfAcceptedDelegation: false,
        project: Effect.succeed(project),
        git: unusedGit,
        launches: { checkWorktreeBase },
      });
      assert.deepEqual(accepted, {
        type: "worktree",
        baseRef: "feature/base",
        branch: "feature/lane",
      });
      assert.equal(checkWorktreeBase.mock.calls[0]?.[0].projectId, projectId);
      const missing = yield* resolveExplicitDelegatedTaskWorkspace({
        request: { workspaceStrategy: { type: "worktree", baseRef: "missing" } },
        isRetryOfAcceptedDelegation: false,
        project: Effect.succeed(project),
        git: unusedGit,
        launches: { checkWorktreeBase },
      }).pipe(Effect.flip);
      assert.instanceOf(missing, OrchestratorMcpFailure);
      assert.equal(missing.code, "invalid_request");
      assert.include(missing.message, "No task was created");
      assert.include(missing.message, "startFromOrigin:true");
    }),
  );
});

describe("delegated task project", () => {
  const otherProjectId = ProjectId.make("project:delegated-other");
  const strategy = { type: "existing_worktree" as const, worktreePath: "/other/lane" };
  const unusedLoad = () => Effect.die("project is not needed");

  it.effect("keeps the parent's project without a command field", () =>
    Effect.gen(function* () {
      for (const request of [{}, { projectId }]) {
        const resolved = yield* resolveDelegatedTaskProject({
          request,
          parentProjectId: projectId,
          isRetryOfAcceptedDelegation: false,
          loadProject: unusedLoad,
        });
        assert.deepEqual(resolved, { projectId, commandProjectId: undefined });
      }
    }),
  );

  it.effect("requires an explicit workspace for another project, even on replay", () =>
    Effect.gen(function* () {
      for (const isRetryOfAcceptedDelegation of [false, true]) {
        const error = yield* resolveDelegatedTaskProject({
          request: { projectId: otherProjectId },
          parentProjectId: projectId,
          isRetryOfAcceptedDelegation,
          loadProject: unusedLoad,
        }).pipe(Effect.flip);
        assert.equal(error.code, "invalid_request");
        assert.include(error.message, "workspaceStrategy");
      }
    }),
  );

  it.effect("rejects missing and deleted projects but replays an accepted one unread", () =>
    Effect.gen(function* () {
      const missing = yield* resolveDelegatedTaskProject({
        request: { projectId: otherProjectId, workspaceStrategy: strategy },
        parentProjectId: projectId,
        isRetryOfAcceptedDelegation: false,
        loadProject: (id) =>
          Effect.fail(
            new OrchestratorMcpFailure({
              code: "invalid_request",
              message: `Project ${id} was not found.`,
            }),
          ),
      }).pipe(Effect.flip);
      assert.include(missing.message, "was not found");
      const deleted = yield* resolveDelegatedTaskProject({
        request: { projectId: otherProjectId, workspaceStrategy: strategy },
        parentProjectId: projectId,
        isRetryOfAcceptedDelegation: false,
        loadProject: () => Effect.succeed({ deletedAt: "2026-10-20T00:00:00.000Z" }),
      }).pipe(Effect.flip);
      assert.include(deleted.message, "was deleted");
      const accepted = yield* resolveDelegatedTaskProject({
        request: { projectId: otherProjectId, workspaceStrategy: strategy },
        parentProjectId: projectId,
        isRetryOfAcceptedDelegation: false,
        loadProject: () => Effect.succeed({ deletedAt: null }),
      });
      assert.deepEqual(accepted, { projectId: otherProjectId, commandProjectId: otherProjectId });
      const replayed = yield* resolveDelegatedTaskProject({
        request: { projectId: otherProjectId, workspaceStrategy: strategy },
        parentProjectId: projectId,
        isRetryOfAcceptedDelegation: true,
        loadProject: unusedLoad,
      });
      assert.deepEqual(replayed, accepted);
    }),
  );
});

const parentSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;
const claudeInstanceId = ProviderInstanceId.make("claudeAgent");
const claudeDriver = ProviderDriverKind.make("claudeAgent");

const adapter = {
  instanceId: parentSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("provider execution is disabled in delegated workspace tests"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

const claudeProvider: ServerProvider = {
  instanceId: claudeInstanceId,
  driver: claudeDriver,
  enabled: true,
  installed: true,
  version: "test",
  checkedAt: "2026-10-20T00:00:00.000Z",
  slashCommands: [],
  skills: [],
  status: "ready",
  auth: { status: "authenticated" },
  models: [
    { slug: "claude-opus-5-5", name: "Claude Opus 5.5", isCustom: true, capabilities: null },
  ],
};

const projectRecord = {
  id: projectId,
  title: "Project",
  workspaceRoot: "/repo",
  repositoryIdentity: null,
  faviconPath: null,
  defaultModelSelection: parentSelection,
  defaultThreadEnvMode: null,
  scripts: [],
  createdAt: "2026-10-20T00:00:00.000Z",
  updatedAt: "2026-10-20T00:00:00.000Z",
  deletedAt: null,
} as const;

// A second registered repository, as in the HQ lanes delegated from another project.
const hqProjectId = ProjectId.make("project:delegated-workspace-hq");
const hqProjectRecord = {
  ...projectRecord,
  id: hqProjectId,
  title: "HQ",
  workspaceRoot: "/hq",
} as const;

/**
 * Real orchestrator, thread management and launch preparation over in-memory
 * SQLite; Git, setup scripts and provider execution are fakes. As with the
 * real runner, setup reports a completion only when preparation observes it;
 * that completion blocks until released so each case observes the prepared
 * child before it starts.
 */
function makeHarness(
  listWorktreePaths: GitVcsDriver.GitVcsDriver["Service"]["listWorktreePaths"],
  options: { readonly claudeDisabledIn?: ProjectId; readonly setupAsync?: boolean } = {},
) {
  const setupEntered = Deferred.makeUnsafe<void>();
  const allowSetup = Deferred.makeUnsafe<void>();
  const terminalClosed = Deferred.makeUnsafe<void>();
  const database = SqlitePersistence.layerMemory;
  const registry = ProviderAdapterRegistry.layerFromAdapters([
    adapter,
    { ...adapter, instanceId: claudeInstanceId, driver: claudeDriver },
  ]);
  const orchestrator = ProviderReplayHarness.layerWithRegistry(
    { name: "delegated-task-workspace" },
    registry,
    { databaseLayer: database, runEffectWorker: false },
  );
  const serverSettings = ServerSettings.layerTest({
    providerInstances: { [claudeInstanceId]: { driver: claudeDriver, config: {} } },
    ...(options.claudeDisabledIn === undefined
      ? {}
      : {
          projectSettingsOverrides: {
            [options.claudeDisabledIn]: {
              providerInstances: { [claudeInstanceId]: { enabled: false } },
            },
          },
        }),
  });
  const projectStore = ProjectStore.layer.pipe(Layer.provide(database));
  const receipts = CommandReceiptStore.layer.pipe(Layer.provide(database));
  const events = EventStore.layer.pipe(Layer.provide(database));
  const threadManagement = ThreadManagement.layer.pipe(
    Layer.provide(orchestrator),
    Layer.provide(serverSettings),
    Layer.provide(receipts),
  );
  const outbox = EffectOutbox.layer.pipe(Layer.provide(database));
  const createWorktree = vi.fn(
    (input: Parameters<GitWorkflow.GitWorkflowService["Service"]["createWorktree"]>[0]) =>
      Effect.succeed({
        worktree: { path: "/repo-worktrees/lane", refName: input.newRefName, headSha: "abc" },
      } as never),
  );
  const removeWorktree = vi.fn(
    (_input: Parameters<GitWorkflow.GitWorkflowService["Service"]["removeWorktree"]>[0]) =>
      Effect.void,
  );
  const hasCommit = vi.fn(
    (input: Parameters<GitWorkflow.GitWorkflowService["Service"]["hasCommit"]>[0]) =>
      Effect.succeed(input.refName !== "refs/heads/t3"),
  );
  const listWorktrees = vi.fn(listWorktreePaths);
  // The runner's project fields are optional strings; an omitted one is recorded as null.
  const setupInputs: Array<{
    readonly projectId: string | null;
    readonly projectCwd: string | null;
    readonly worktreePath: string;
  }> = [];
  // Whether each setup run was observed, which is when the real runner returns a completion.
  const setupObserved: Array<boolean> = [];
  const closeTerminal = vi.fn(
    (_input: Parameters<TerminalManager.TerminalManager["Service"]["close"]>[0]) =>
      Deferred.succeed(terminalClosed, undefined).pipe(Effect.asVoid),
  );
  const externalServices = Layer.mergeAll(
    WorktreeSetupTracker.layer,
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: closeTerminal }),
    Layer.succeed(ProjectService.ProjectService, {
      create: () => Effect.die("unused"),
      bootstrap: () => Effect.die("unused"),
      update: () => Effect.die("unused"),
      delete: () => Effect.die("unused"),
      getById: (id) =>
        Effect.succeed(
          id === projectId
            ? Option.some(projectRecord)
            : id === hqProjectId
              ? Option.some(hqProjectRecord)
              : Option.none(),
        ),
      getByWorkspaceRoot: () => Effect.succeed(Option.some(projectRecord)),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
      listPage: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree,
      removeWorktree,
      hasCommit,
      isRepository: () => Effect.succeed(true),
      renameBranch: (input) => Effect.succeed({ branch: input.newBranch }),
      fetchRemote: () => Effect.void,
      remoteExists: () => Effect.succeed(true),
      listRefs: () =>
        Effect.succeed({
          refs: [],
          isRepo: true,
          hasPrimaryRemote: true,
          nextCursor: null,
          totalCount: 0,
        }),
    }),
    Layer.succeed(ProjectSetupScriptRunner.ProjectSetupScriptRunner, {
      runForThread: (input) => {
        const observed =
          input.observeCompletion !== undefined ||
          input.legacyPreparation !== undefined ||
          input.nativePreparation !== undefined;
        return Effect.sync(() => {
          setupInputs.push({
            projectId: input.projectId ?? null,
            projectCwd: input.projectCwd ?? null,
            worktreePath: input.worktreePath,
          });
          setupObserved.push(observed);
        }).pipe(
          Effect.andThen(observed ? Effect.void : Deferred.succeed(setupEntered, undefined)),
          Effect.as({
            status: "started" as const,
            async: options.setupAsync === true,
            scriptId: "setup",
            scriptName: "Setup",
            scriptCommand: "setup",
            terminalId: "setup",
            cwd: input.worktreePath,
            ...(observed
              ? {
                  completion: Deferred.succeed(setupEntered, undefined).pipe(
                    Effect.andThen(Deferred.await(allowSetup)),
                    Effect.as({ exitCode: 0, durationMs: 0 }),
                  ),
                }
              : {}),
          }),
        );
      },
    }),
    Layer.mock(TextGeneration.TextGeneration)({
      generateThreadTitle: () => Effect.succeed({ title: "Generated title" }),
      generateBranchName: () => Effect.succeed({ branch: "generated-branch" }),
    }),
    serverSettings,
    ProviderRegistryMock.layer([claudeProvider]),
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
        events,
      ),
    ),
  );
  const base = Layer.mergeAll(
    launch,
    receipts,
    events,
    orchestrator,
    registry,
    threadManagement,
    outbox,
    database,
    projectStore,
    externalServices,
  );
  const service = OrchestratorMcpService.layer.pipe(
    Layer.provide(NodeCrypto.layer),
    Layer.provide(base),
    Layer.provide(
      Layer.mock(GitVcsDriver.GitVcsDriver)({
        listWorktreePaths: listWorktrees,
        resolveCommit: () => Effect.die("explicit strategies do not use the parent commit"),
      }),
    ),
    Layer.provide(Layer.mock(ScheduledTasks.ScheduledTaskService)({})),
    Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
    Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
  );
  return {
    layer: Layer.merge(base, service),
    setupEntered,
    allowSetup,
    terminalClosed,
    createWorktree,
    removeWorktree,
    hasCommit,
    listWorktrees,
    closeTerminal,
    setupInputs,
    setupObserved,
  };
}

/** Registers a project in the orchestrator's own project projection. */
const seedProject = (
  record: {
    readonly id: ProjectId;
    readonly title: string;
    readonly workspaceRoot: string;
    readonly defaultModelSelection: typeof parentSelection;
    readonly createdAt: string;
    readonly updatedAt: string;
  },
  sequence: number,
  deleted = false,
) =>
  Effect.gen(function* () {
    const projects = yield* ProjectStore.ProjectStoreV2;
    const envelope = {
      aggregateKind: "project" as const,
      aggregateId: record.id,
      occurredAt: record.createdAt,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
    };
    yield* projects.apply({
      ...envelope,
      sequence,
      eventId: EventId.make(`event:seed-project:${record.id}`),
      type: "project.created",
      payload: {
        projectId: record.id,
        title: record.title,
        workspaceRoot: record.workspaceRoot,
        defaultModelSelection: record.defaultModelSelection,
        scripts: [],
        createdAt: record.createdAt,
        updatedAt: record.updatedAt,
      },
    });
    if (deleted)
      yield* projects.apply({
        ...envelope,
        sequence: sequence + 1,
        eventId: EventId.make(`event:delete-project:${record.id}`),
        type: "project.deleted",
        payload: { projectId: record.id, deletedAt: record.updatedAt },
      });
  });

const startParent = Effect.gen(function* () {
  const threads = yield* ThreadManagement.ThreadManagementService;
  const parentThreadId = ThreadId.make("thread:delegated-workspace-parent");
  yield* threads.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create-parent"),
    threadId: parentThreadId,
    projectId,
    title: "Parent",
    modelSelection: parentSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "parent-feature",
    worktreePath: "/repo-worktrees/parent",
    createdBy: "user",
    creationSource: "web",
  });
  yield* threads.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("start-parent"),
    threadId: parentThreadId,
    messageId: MessageId.make("parent-message"),
    text: "Delegate implementation lanes",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
  });
  const scope = {
    environmentId: EnvironmentId.make("delegated-workspace-test"),
    requestNamespace: "parent-session",
    thread: {
      threadId: parentThreadId,
      providerSessionId: "parent-session",
      providerInstanceId: parentSelection.instanceId,
    },
    client: undefined,
    capabilities: new Set(["orchestration"] as const),
    issuedAt: 1,
  };
  return { parentThreadId, scope };
});

const enqueuedRequests = (childThreadId: ThreadId) =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    return (yield* outbox.listByThreadId(childThreadId)).map((effect) => effect.request.type);
  });

/**
 * While a delegated child's observed setup is still running in its existing
 * worktree: the run is still preparing and no provider turn has been enqueued.
 */
const assertHeldInSetup = (harness: ReturnType<typeof makeHarness>, childThreadId: ThreadId) =>
  Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    assert.deepEqual(harness.setupObserved, [true]);
    assert.equal((yield* threads.getThreadProjection(childThreadId)).runs[0]?.status, "preparing");
    assert.notInclude(yield* enqueuedRequests(childThreadId), "provider-turn.start");
  });

/** Waits until the child's run is released to its agent. */
const awaitReleased = (childThreadId: ThreadId) =>
  Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    yield* threads
      .streamStoredEventsFrom({ threadId: childThreadId, eventType: "run.updated" })
      .pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status === "starting",
        ),
        Stream.runHead,
      );
  });

/**
 * After a cancelled delegated child's setup terminal closed (only its
 * preparation's cancellation cleanup closes it, so preparation has ended):
 * the run is interrupted, no provider turn was ever enqueued for the child,
 * and its existing checkout was not removed.
 */
const assertCancelledInExistingWorktree = (
  harness: ReturnType<typeof makeHarness>,
  childThreadId: ThreadId,
) =>
  Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    assert.deepEqual(
      harness.closeTerminal.mock.calls.map(([input]) => [input.threadId, input.terminalId]),
      [[childThreadId, "setup"]],
    );
    const cancelled = yield* threads.getThreadProjection(childThreadId);
    assert.equal(cancelled.runs[0]?.status, "interrupted");
    assert.notInclude(yield* enqueuedRequests(childThreadId), "provider-turn.start");
    assert.equal(harness.removeWorktree.mock.calls.length, 0);
  });

const claudeTarget = { providerInstanceId: claudeInstanceId, model: "claude-opus-5-5" };

describe("delegate_task workspaceStrategy through the orchestrator", () => {
  it.effect(
    "prepares a Claude child in a named new worktree as a cancellable subagent of the caller",
    () => {
      const harness = makeHarness(() => Effect.die("a new worktree does not list worktrees"));
      return Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const { parentThreadId, scope } = yield* startParent;
        const input = {
          task: "Implement lane T1",
          target: claudeTarget,
          workspaceStrategy: {
            type: "worktree" as const,
            baseRef: "work/hq-foundation",
            branch: "work/hq-t1",
            startFromOrigin: false,
          },
          clientRequestId: "hq-t1",
        };
        const delegated = yield* service.delegateTask(scope, input);
        if (delegated.childThreadId === null)
          return yield* Effect.die("Local delegation must return a child thread");
        yield* Deferred.await(harness.setupEntered);

        // Separate provider session and thread, linked to the caller as its subagent.
        const child = yield* threads.getThreadProjection(delegated.childThreadId);
        assert.notEqual(delegated.childThreadId, parentThreadId);
        assert.equal(child.thread.lineage.parentThreadId, parentThreadId);
        assert.equal(child.thread.lineage.relationshipToParent, "subagent");
        assert.equal(child.thread.modelSelection.instanceId, claudeInstanceId);
        assert.equal(child.thread.modelSelection.model, "claude-opus-5-5");
        assert.equal(child.thread.projectId, projectId);
        assert.equal(child.thread.worktreePath, "/repo-worktrees/lane");
        assert.equal(child.thread.branch, "work/hq-t1");
        assert.equal(child.runs[0]?.status, "preparing");
        const parent = yield* threads.getThreadProjection(parentThreadId);
        const task = parent.subagents.find((candidate) => candidate.id === delegated.taskId);
        assert.equal(task?.origin, "app_owned");
        assert.equal(task?.childThreadId, delegated.childThreadId);
        assert.equal(task?.providerInstanceId, claudeInstanceId);
        assert.equal(parent.thread.worktreePath, "/repo-worktrees/parent");
        assert.equal(parent.thread.branch, "parent-feature");

        const created = harness.createWorktree.mock.calls[0]?.[0];
        assert.equal(created?.refName, "work/hq-foundation");
        assert.equal(created?.newRefName, "work/hq-t1");
        assert.equal(created?.cwd, "/repo");

        // A retry replays the accepted task without re-checking or re-creating.
        const baseChecks = harness.hasCommit.mock.calls.length;
        const replayed = yield* service.delegateTask(scope, input);
        assert.equal(replayed.taskId, delegated.taskId);
        assert.equal(replayed.childThreadId, delegated.childThreadId);
        assert.equal(harness.createWorktree.mock.calls.length, 1);
        assert.equal(harness.hasCommit.mock.calls.length, baseChecks);

        const status = yield* service.taskStatus(scope, delegated.taskId);
        assert.equal(status.childThreadId, delegated.childThreadId);
        assert.equal(status.providerInstanceId, claudeInstanceId);

        yield* service.cancelTask(scope, { taskId: delegated.taskId });
        yield* Deferred.await(harness.terminalClosed);
        yield* threads.streamStoredEventsFrom({ threadId: delegated.childThreadId }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "thread.metadata-updated" &&
              stored.event.payload.worktreePath === null,
          ),
          Stream.runHead,
        );
        // The worktree this task created is removed when it is cancelled mid-setup.
        assert.equal(harness.removeWorktree.mock.calls.length, 1);
        assert.equal(harness.removeWorktree.mock.calls[0]?.[0].path, "/repo-worktrees/lane");
        const cancelled = yield* threads.getThreadProjection(delegated.childThreadId);
        assert.equal(cancelled.runs[0]?.status, "interrupted");
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect(
    "binds a Claude child to an existing project worktree and never removes it on cancel",
    () => {
      const harness = makeHarness((cwd) =>
        Effect.succeed(cwd === "/repo" ? ["/repo", "/repo-worktrees/lane"] : []),
      );
      return Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const { parentThreadId, scope } = yield* startParent;
        const delegated = yield* service.delegateTask(scope, {
          task: "Continue lane T2",
          target: claudeTarget,
          workspaceStrategy: {
            type: "existing_worktree",
            worktreePath: "/repo-worktrees/lane",
            branch: "work/hq-t2",
          },
          clientRequestId: "hq-t2",
        });
        if (delegated.childThreadId === null)
          return yield* Effect.die("Local delegation must return a child thread");
        yield* Deferred.await(harness.setupEntered);
        assert.deepEqual(harness.listWorktrees.mock.calls, [["/repo"]]);
        assert.equal(harness.createWorktree.mock.calls.length, 0);
        const child = yield* threads.getThreadProjection(delegated.childThreadId);
        assert.equal(child.thread.lineage.parentThreadId, parentThreadId);
        assert.equal(child.thread.lineage.relationshipToParent, "subagent");
        assert.equal(child.thread.worktreePath, "/repo-worktrees/lane");
        assert.equal(child.thread.branch, "work/hq-t2");
        yield* assertHeldInSetup(harness, delegated.childThreadId);

        // Cancelling stops setup in the delegated child's existing worktree and
        // keeps the checkout; the interrupted run is never released to its agent.
        yield* service.cancelTask(scope, { taskId: delegated.taskId });
        yield* Deferred.await(harness.terminalClosed);
        yield* assertCancelledInExistingWorktree(harness, delegated.childThreadId);
        const cancelled = yield* threads.getThreadProjection(delegated.childThreadId);
        assert.equal(cancelled.thread.worktreePath, "/repo-worktrees/lane");
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect("refuses invalid workspaces before any child or subagent exists", () => {
    const harness = makeHarness((cwd) => Effect.succeed(cwd === "/repo" ? ["/repo"] : []));
    return Effect.gen(function* () {
      const service = yield* OrchestratorMcpService.OrchestratorMcpService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const { parentThreadId, scope } = yield* startParent;
      const foreign = yield* service
        .delegateTask(scope, {
          task: "Work in another repository",
          target: claudeTarget,
          workspaceStrategy: { type: "existing_worktree", worktreePath: "/hq/repo" },
          clientRequestId: "foreign-worktree",
        })
        .pipe(Effect.flip);
      assert.equal(foreign.code, "invalid_request");
      const combined = yield* service
        .delegateTask(scope, {
          task: "Ambiguous checkout",
          target: claudeTarget,
          isolation: "worktree",
          workspaceStrategy: { type: "worktree", baseRef: "main" },
          clientRequestId: "combined",
        })
        .pipe(Effect.flip);
      assert.equal(combined.code, "invalid_request");
      const parent = yield* threads.getThreadProjection(parentThreadId);
      assert.isEmpty(parent.subagents);
      assert.equal(harness.createWorktree.mock.calls.length, 0);
    }).pipe(Effect.provide(harness.layer));
  });
});

const hqWorktrees = (cwd: string) =>
  Effect.succeed(
    cwd === "/hq"
      ? ["/hq", "/hq-worktrees/t1"]
      : cwd === "/repo"
        ? ["/repo", "/repo-worktrees/lane"]
        : [],
  );

const hqExistingWorktree = {
  type: "existing_worktree" as const,
  worktreePath: "/hq-worktrees/t1",
  branch: "work/hq-t1",
};

describe("cross-project delegate_task", () => {
  it.effect(
    "binds a Claude child to another project's worktree while the caller keeps the task",
    () => {
      const harness = makeHarness(hqWorktrees);
      return Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        yield* seedProject(hqProjectRecord, 1);
        const { parentThreadId, scope } = yield* startParent;
        const input = {
          task: "Implement HQ T1",
          target: claudeTarget,
          projectId: hqProjectId,
          workspaceStrategy: hqExistingWorktree,
          clientRequestId: "hq-cross-t1",
        };
        const delegated = yield* service.delegateTask(scope, input);
        if (delegated.childThreadId === null)
          return yield* Effect.die("Local delegation must return a child thread");
        yield* Deferred.await(harness.setupEntered);

        // Admission and setup use the target project, not the caller's.
        assert.deepEqual(harness.listWorktrees.mock.calls, [["/hq"]]);
        assert.deepEqual(harness.setupInputs, [
          { projectId: hqProjectId, projectCwd: "/hq", worktreePath: "/hq-worktrees/t1" },
        ]);
        const created = yield* threads
          .streamStoredEventsFrom({
            threadId: delegated.childThreadId,
            eventType: "thread.created",
          })
          .pipe(Stream.runHead);
        assert.isTrue(Option.isSome(created));
        if (Option.isSome(created) && created.value.event.type === "thread.created")
          assert.equal(created.value.event.payload.projectId, hqProjectId);
        const child = yield* threads.getThreadProjection(delegated.childThreadId);
        assert.equal(child.thread.projectId, hqProjectId);
        assert.equal(child.thread.lineage.parentThreadId, parentThreadId);
        assert.equal(child.thread.lineage.relationshipToParent, "subagent");
        assert.equal(child.thread.modelSelection.instanceId, claudeInstanceId);
        assert.equal(child.thread.worktreePath, "/hq-worktrees/t1");
        assert.equal(child.thread.branch, "work/hq-t1");
        // Mode limits come from the parent.
        assert.equal(child.thread.runtimeMode, "full-access");
        const parent = yield* threads.getThreadProjection(parentThreadId);
        assert.equal(parent.thread.projectId, projectId);
        const task = parent.subagents.find((candidate) => candidate.id === delegated.taskId);
        assert.equal(task?.origin, "app_owned");
        assert.equal(task?.childThreadId, delegated.childThreadId);

        // An accepted retry replays without reading the target project again.
        const listings = harness.listWorktrees.mock.calls.length;
        const replayed = yield* service.delegateTask(scope, input);
        assert.equal(replayed.taskId, delegated.taskId);
        assert.equal(replayed.childThreadId, delegated.childThreadId);
        assert.equal(harness.listWorktrees.mock.calls.length, listings);
        assert.lengthOf((yield* threads.getThreadProjection(parentThreadId)).subagents, 1);

        const status = yield* service.taskStatus(scope, delegated.taskId);
        assert.equal(status.childThreadId, delegated.childThreadId);
        assert.equal(status.providerInstanceId, claudeInstanceId);

        // As in the same project: setup stops, the checkout stays, no release.
        yield* assertHeldInSetup(harness, delegated.childThreadId);
        yield* service.cancelTask(scope, { taskId: delegated.taskId });
        yield* Deferred.await(harness.terminalClosed);
        yield* assertCancelledInExistingWorktree(harness, delegated.childThreadId);
        const cancelled = yield* threads.getThreadProjection(delegated.childThreadId);
        assert.equal(cancelled.thread.worktreePath, "/hq-worktrees/t1");
        // The task still belongs to, and is read through, the caller.
        const afterCancel = yield* service.taskStatus(scope, delegated.taskId);
        assert.equal(afterCancel.childThreadId, delegated.childThreadId);
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect("checks provider enablement in the target project, not the caller's", () => {
    const harness = makeHarness(hqWorktrees, { claudeDisabledIn: hqProjectId });
    return Effect.gen(function* () {
      const service = yield* OrchestratorMcpService.OrchestratorMcpService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      yield* seedProject(hqProjectRecord, 1);
      const { parentThreadId, scope } = yield* startParent;
      const refused = yield* service
        .delegateTask(scope, {
          task: "Implement HQ T2",
          target: claudeTarget,
          projectId: hqProjectId,
          workspaceStrategy: hqExistingWorktree,
          clientRequestId: "hq-disabled",
        })
        .pipe(Effect.flip);
      assert.equal(refused.code, "invalid_request");
      assert.include(refused.message, `disabled for project ${hqProjectId}`);
      assert.isEmpty((yield* threads.getThreadProjection(parentThreadId)).subagents);

      // The same provider remains available in the caller's own project.
      yield* service.delegateTask(scope, {
        task: "Implement lane in this project",
        target: claudeTarget,
        workspaceStrategy: { type: "existing_worktree", worktreePath: "/repo-worktrees/lane" },
        clientRequestId: "same-project-enabled",
      });
      yield* Deferred.await(harness.setupEntered);
      assert.lengthOf((yield* threads.getThreadProjection(parentThreadId)).subagents, 1);
    }).pipe(Effect.provide(harness.layer));
  });

  it.effect(
    "refuses another project without its own workspace, with a foreign checkout, or missing",
    () => {
      const harness = makeHarness(hqWorktrees);
      return Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        yield* seedProject(hqProjectRecord, 1);
        const { parentThreadId, scope } = yield* startParent;
        const attempt = (
          clientRequestId: string,
          extra: Partial<Parameters<typeof service.delegateTask>[1]>,
        ) =>
          service
            .delegateTask(scope, {
              task: "Implement HQ lane",
              target: claudeTarget,
              clientRequestId,
              ...extra,
            })
            .pipe(Effect.flip);

        const shared = yield* attempt("no-workspace", { projectId: hqProjectId });
        assert.equal(shared.code, "invalid_request");
        assert.include(shared.message, "workspaceStrategy");
        const copied = yield* attempt("parent-commit", {
          projectId: hqProjectId,
          isolation: "worktree",
        });
        assert.equal(copied.code, "invalid_request");
        assert.include(copied.message, "workspaceStrategy");
        // The caller's own worktree is not a worktree of the target project.
        const foreign = yield* attempt("foreign-checkout", {
          projectId: hqProjectId,
          workspaceStrategy: { type: "existing_worktree", worktreePath: "/repo-worktrees/lane" },
        });
        assert.equal(foreign.code, "invalid_request");
        assert.include(foreign.message, hqProjectId);
        const missing = yield* attempt("missing-project", {
          projectId: ProjectId.make("project:delegated-workspace-missing"),
          workspaceStrategy: hqExistingWorktree,
        });
        assert.equal(missing.code, "invalid_request");
        assert.include(missing.message, "was not found");

        assert.isEmpty((yield* threads.getThreadProjection(parentThreadId)).subagents);
        assert.equal(harness.createWorktree.mock.calls.length, 0);
        assert.isEmpty(harness.setupInputs);
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect(
    "enforces the target project in delegated_task.request itself, without the MCP resolver",
    () => {
      const harness = makeHarness(hqWorktrees);
      const deletedProjectRecord = {
        ...projectRecord,
        id: ProjectId.make("project:delegated-workspace-deleted"),
        title: "Deleted",
        workspaceRoot: "/deleted",
      };
      return Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        yield* seedProject(hqProjectRecord, 1);
        yield* seedProject(deletedProjectRecord, 10, true);
        const { parentThreadId } = yield* startParent;
        const parentRun = (yield* threads.getThreadProjection(parentThreadId)).runs[0];
        if (parentRun === undefined || parentRun.rootNodeId === null)
          return assert.fail("The parent run must be active with a root node.");
        const parentRunId = parentRun.id;
        const parentNodeId = parentRun.rootNodeId;
        const request = (
          commandId: string,
          fields: {
            readonly projectId: ProjectId;
            readonly workspaceStrategy?: typeof hqExistingWorktree;
          },
        ) =>
          threads.dispatch({
            type: "delegated_task.request",
            createdBy: "agent",
            creationSource: "mcp",
            commandId: CommandId.make(commandId),
            parentThreadId,
            parentRunId,
            parentNodeId,
            task: "Direct delegated request",
            modelSelection: { instanceId: claudeInstanceId, model: "claude-opus-5-5" },
            runtimeMode: "full-access",
            interactionMode: "default",
            ...fields,
          });

        yield* request("direct-no-workspace", { projectId: hqProjectId }).pipe(Effect.flip);
        yield* request("direct-missing", {
          projectId: ProjectId.make("project:delegated-workspace-missing"),
          workspaceStrategy: hqExistingWorktree,
        }).pipe(Effect.flip);
        yield* request("direct-deleted", {
          projectId: deletedProjectRecord.id,
          workspaceStrategy: hqExistingWorktree,
        }).pipe(Effect.flip);
        assert.isEmpty((yield* threads.getThreadProjection(parentThreadId)).subagents);

        // Control: the same command shape for a live project creates the child there.
        const accepted = yield* request("direct-live", {
          projectId: hqProjectId,
          workspaceStrategy: hqExistingWorktree,
        });
        const childCreated = accepted.storedEvents.find(
          (stored) => stored.event.type === "thread.created",
        );
        assert.equal(
          childCreated?.event.type === "thread.created"
            ? childCreated.event.payload.projectId
            : undefined,
          hqProjectId,
        );
        const parent = yield* threads.getThreadProjection(parentThreadId);
        assert.lengthOf(parent.subagents, 1);
        assert.equal(parent.thread.projectId, projectId);
      }).pipe(Effect.provide(harness.layer));
    },
  );
});

const existingWorktreeCases = [
  {
    name: "its own project",
    seedHq: false,
    request: {
      workspaceStrategy: {
        type: "existing_worktree" as const,
        worktreePath: "/repo-worktrees/lane",
      },
    },
  },
  {
    name: "another project",
    seedHq: true,
    request: { projectId: hqProjectId, workspaceStrategy: hqExistingWorktree },
  },
];

describe("delegated existing-worktree setup", () => {
  it.effect.each(existingWorktreeCases)(
    "releases a child in $name only after its synchronous setup succeeds",
    ({ seedHq, request }) => {
      const harness = makeHarness(hqWorktrees);
      return Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        if (seedHq) yield* seedProject(hqProjectRecord, 1);
        const { scope } = yield* startParent;
        const delegated = yield* service.delegateTask(scope, {
          task: "Continue lane",
          target: claudeTarget,
          clientRequestId: "sync-setup",
          ...request,
        });
        if (delegated.childThreadId === null)
          return yield* Effect.die("Local delegation must return a child thread");
        yield* Deferred.await(harness.setupEntered);
        yield* assertHeldInSetup(harness, delegated.childThreadId);

        yield* Deferred.succeed(harness.allowSetup, undefined);
        yield* awaitReleased(delegated.childThreadId);
        assert.include(yield* enqueuedRequests(delegated.childThreadId), "provider-turn.start");
        assert.equal(harness.closeTerminal.mock.calls.length, 0);
        assert.equal(harness.removeWorktree.mock.calls.length, 0);
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect("releases a child before its async setup completes and still stops that setup", () => {
    const harness = makeHarness(hqWorktrees, { setupAsync: true });
    return Effect.gen(function* () {
      const service = yield* OrchestratorMcpService.OrchestratorMcpService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const { scope } = yield* startParent;
      const delegated = yield* service.delegateTask(scope, {
        task: "Continue lane",
        target: claudeTarget,
        clientRequestId: "async-setup",
        workspaceStrategy: { type: "existing_worktree", worktreePath: "/repo-worktrees/lane" },
      });
      if (delegated.childThreadId === null)
        return yield* Effect.die("Local delegation must return a child thread");
      // The async setup is never allowed to finish, yet the agent is released.
      yield* awaitReleased(delegated.childThreadId);
      assert.deepEqual(harness.setupObserved, [true]);
      assert.include(yield* enqueuedRequests(delegated.childThreadId), "provider-turn.start");
      assert.equal(harness.closeTerminal.mock.calls.length, 0);

      // Cancelling after release still closes the observed setup's terminal, once,
      // and keeps the checkout.
      yield* service.cancelTask(scope, { taskId: delegated.taskId });
      yield* Deferred.await(harness.terminalClosed);
      assert.deepEqual(
        harness.closeTerminal.mock.calls.map(([input]) => [input.threadId, input.terminalId]),
        [[delegated.childThreadId, "setup"]],
      );
      assert.equal(harness.removeWorktree.mock.calls.length, 0);
      const cancelled = yield* threads.getThreadProjection(delegated.childThreadId);
      assert.equal(cancelled.runs[0]?.status, "interrupted");
      assert.equal(cancelled.thread.worktreePath, "/repo-worktrees/lane");
    }).pipe(Effect.provide(harness.layer));
  });
});
