import * as WorktreeSetupTracker from "../../../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../../../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../../../terminal/Manager.ts";
import { assert, it, vi } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  RunId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Exit from "effect/Exit";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as GitDriver from "../../../vcs/GitVcsDriver.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import { type Caller } from "../../../mcp/threadAccess.ts";
import type { ThreadStartGate } from "../../../mcp/McpToolAccess.ts";
import * as ClientLaunch from "./ClientLaunchService.ts";
import { clientLaunchIds } from "./ClientLaunchIdentity.ts";
import * as Deferred from "effect/Deferred";
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
    NodeServices.layer,
    Layer.mock(GitDriver.GitVcsDriver)({}),
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

const namespace = "synthetic-launch-service";
const modes = { runtimeMode: "full-access", interactionMode: "default" } as const;
const gate: ThreadStartGate = {
  start: Effect.succeed(modes),
  continueWithin: (_target, effect) => effect,
};
const input = { clientRequestId: "key", projectId, title: "Launch", message: "Do the task" };
const caller = Effect.gen(function* () {
  return {
    scope: {
      environmentId: EnvironmentId.make("synthetic"),
      requestNamespace: namespace,
      issuedAt: 0,
      capabilities: new Set(["orchestration" as const]),
      thread: undefined,
      client: { sessionId: "synthetic", label: "test", access: "full-access" as const },
    },
    threads: yield* ThreadManagement.ThreadManagementService,
    caller: undefined,
    limits: modes,
  } satisfies Caller;
});
const failAfter = (
  launches: ThreadLaunch.ThreadLaunchService["Service"],
  step: "claim" | "thread" | "message",
) => ({
  ...launches,
  launch: (request: ThreadLaunch.ThreadLaunchInput) =>
    Effect.gen(function* () {
      const threads = yield* ThreadManagement.ThreadManagementService;
      if (step !== "claim")
        yield* threads
          .dispatch({
            type: "thread.create",
            commandId: request.commandId,
            threadId: request.threadId!,
            projectId,
            title: request.title,
            modelSelection: request.modelSelection,
            runtimeMode: request.runtimeMode,
            interactionMode: request.interactionMode,
            branch: null,
            worktreePath: null,
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(Effect.orDie);
      if (step === "message") {
        const message = request.initialMessage;
        yield* threads
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`${request.commandId}:initial-message`),
            threadId: request.threadId!,
            messageId: message!.messageId!,
            text: message!.text,
            attachments: [],
            modelSelection: request.modelSelection,
            dispatchMode: { type: "defer_start", workspaceStrategy: request.workspaceStrategy },
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(Effect.orDie);
      }
      return yield* new ThreadLaunch.ThreadLaunchError({
        operation: "create-thread",
        commandId: request.commandId,
        projectId,
        cause: "synthetic interruption",
      });
    }),
});

it.effect(
  "T1/T2/T4: replays one thread and message, conflicts on canonical changes, ignores changed defaults",
  () =>
    Effect.gen(function* () {
      const context = yield* caller;
      const first = yield* ClientLaunch.launch(input, context, modes, gate);
      const second = yield* ClientLaunch.launch(input, context, modes, gate).pipe(
        Effect.provideService(ProjectService.ProjectService, {
          ...(yield* ProjectService.ProjectService),
          getById: () => Effect.die("Replay must not resolve defaults"),
        }),
      );
      assert.equal(first.threadId, second.threadId);
      assert.isTrue(second.replayed);
      const threads = yield* ThreadManagement.ThreadManagementService;
      const projection = yield* threads.getThreadProjection(first.threadId);
      assert.lengthOf(projection.messages, 1);
      assert.lengthOf(projection.runs, 1);
      for (const change of [
        { title: "other" },
        { message: "other" },
        { modelSelection },
        { runtimeMode: "full-access" as const },
        { interactionMode: "default" as const },
        { workspaceStrategy: { type: "worktree" as const, branch: "other" } },
      ]) {
        const error = yield* ClientLaunch.launch(
          { ...input, ...change },
          context,
          modes,
          gate,
        ).pipe(Effect.flip);
        assert.equal(error.code, "request_conflict");
      }
      assert.lengthOf((yield* threads.getThreadProjection(first.threadId)).messages, 1);
    }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect.each(["claim", "thread", "message"] as const)(
  "T5/T7/T12: reconciles interruption after %s",
  (step) =>
    Effect.gen(function* () {
      const context = yield* caller;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const fault = failAfter(launches, step);
      const result = yield* Effect.exit(
        ClientLaunch.launch(input, context, modes, gate).pipe(
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...launches,
            launch: (request) =>
              fault
                .launch(request)
                .pipe(Effect.provideService(ThreadManagement.ThreadManagementService, threads)),
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(result));
      const state = yield* ClientLaunch.lookup(namespace, input.clientRequestId);
      assert.isTrue(state.claimed);
      assert.equal(state.state, step === "claim" ? "absent" : "launched");
      assert.equal(state.initialMessage, step === "message" ? "accepted" : "pending");
      if (step === "message") assert.equal(state.preparation, "unscheduled");
      const [first, second] = yield* Effect.all(
        [
          ClientLaunch.launch(input, context, modes, gate),
          ClientLaunch.launch(input, context, modes, gate),
        ],
        { concurrency: 2 },
      );
      assert.equal(first.threadId, second.threadId);
      const projection = yield* threads.getThreadProjection(first.threadId);
      assert.lengthOf(projection.messages, 1);
      assert.lengthOf(projection.runs, 1);
    }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect("T5/T6: absent is not proof, fence is authoritative, launch cannot cross it", () =>
  Effect.gen(function* () {
    assert.equal((yield* ClientLaunch.lookup(namespace, "key")).state, "absent");
    assert.equal((yield* ClientLaunch.lookup(namespace, "key")).initialMessage, "unknown");
    assert.equal((yield* ClientLaunch.fence(namespace, "key")).basis, "fence");
    assert.equal(
      (yield* ClientLaunch.launch(input, yield* caller, modes, gate).pipe(Effect.flip)).code,
      "launch_fenced",
    );
    const threads = yield* ThreadManagement.ThreadManagementService;
    assert.isNull(yield* threads.getThreadShell(clientLaunchIds(namespace, "key").threadId));
    const next = yield* ClientLaunch.launch(
      { ...input, clientRequestId: "next" },
      yield* caller,
      modes,
      gate,
    );
    assert.equal((yield* ClientLaunch.fence(namespace, "next")).threadId, next.threadId);
  }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect("T6: concurrent fence and launch have one durable winner", () =>
  Effect.gen(function* () {
    const context = yield* caller;
    const [launched, fenced] = yield* Effect.all(
      [
        Effect.exit(ClientLaunch.launch(input, context, modes, gate)),
        ClientLaunch.fence(namespace, "key"),
      ],
      { concurrency: 2 },
    );
    const state = yield* ClientLaunch.lookup(namespace, "key");
    assert.equal(state.state, fenced.state);
    if (state.state === "no_effect") {
      assert.isTrue(Exit.isFailure(launched));
      assert.isNull(
        yield* context.threads.getThreadShell(clientLaunchIds(namespace, "key").threadId),
      );
    } else {
      assert.equal(state.state, "launched");
      assert.lengthOf((yield* context.threads.getThreadProjection(state.threadId!)).messages, 1);
    }
  }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect(
  "T12/T13: progress without an active preparation holds, and later runs do not replace the bound run",
  () =>
    Effect.gen(function* () {
      const context = yield* caller;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const ids = clientLaunchIds(namespace, "key");
      const fault = failAfter(launches, "message");
      yield* Effect.exit(
        ClientLaunch.launch(input, context, modes, gate).pipe(
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...launches,
            launch: (request) =>
              fault
                .launch(request)
                .pipe(Effect.provideService(ThreadManagement.ThreadManagementService, threads)),
          }),
        ),
      );
      const before = yield* ClientLaunch.lookup(namespace, "key");
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      yield* receipts.insertIfAbsent({
        commandId: CommandId.make(`${ids.commandId}:progress:setup`),
        threadId: ids.threadId,
        commandType: "prepared-run.progress",
        acceptedAt: yield* DateTime.now,
        resultSequence: 0,
        status: "accepted",
        error: null,
      });
      const held = yield* ClientLaunch.lookup(namespace, "key");
      assert.equal(held.preparation, "unknown_effect");
      yield* ClientLaunch.launch(input, context, modes, gate).pipe(
        Effect.provideService(ThreadLaunch.ThreadLaunchService, {
          ...launches,
          launch: () => Effect.die("Unknown progress must never be retried"),
        }),
      );
      const original = yield* threads.getThreadProjection(ids.threadId);
      const later = {
        ...original.runs[0]!,
        id: RunId.make("unrelated-run"),
        userMessageId: MessageId.make("unrelated-message"),
      };
      const bound = yield* ClientLaunch.lookup(namespace, "key").pipe(
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ...threads,
          getThreadProjection: () =>
            Effect.succeed({ ...original, runs: [...original.runs, later] }),
        }),
      );
      assert.equal(bound.runId, before.runId);
      assert.equal(bound.runStatus, before.runStatus);
    }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect(
  "T13: omitted message, rejected message, deleted thread and rejected launch remain distinct",
  () =>
    Effect.gen(function* () {
      const context = yield* caller;
      yield* ClientLaunch.launch(
        { clientRequestId: "idle", projectId, title: "Idle" },
        context,
        modes,
        gate,
      );
      assert.equal((yield* ClientLaunch.lookup(namespace, "idle")).initialMessage, "not_requested");
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const fault = failAfter(launches, "thread");
      yield* Effect.exit(
        ClientLaunch.launch(input, context, modes, gate).pipe(
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...launches,
            launch: (request) =>
              fault
                .launch(request)
                .pipe(
                  Effect.provideService(ThreadManagement.ThreadManagementService, context.threads),
                ),
          }),
        ),
      );
      const ids = clientLaunchIds(namespace, "key");
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.commitRejectedCommand({
        commandId: CommandId.make(`${ids.commandId}:initial-message`),
        threadId: ids.threadId,
        commandType: "message.dispatch",
        rejectedAt: yield* DateTime.now,
        error: "synthetic refusal",
      });
      const rejected = yield* ClientLaunch.lookup(namespace, "key");
      assert.equal(rejected.initialMessage, "rejected");
      assert.equal(rejected.reason, "synthetic refusal");
      yield* ClientLaunch.launch(input, context, modes, gate).pipe(
        Effect.provideService(ThreadLaunch.ThreadLaunchService, {
          ...launches,
          launch: () => Effect.die("Rejected message must remain held"),
        }),
      );
      const deleted = yield* ClientLaunch.lookup(namespace, "key").pipe(
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ...context.threads,
          getThreadShell: () => Effect.succeed(null),
        }),
      );
      assert.isTrue(deleted.threadDeleted);
      const other = clientLaunchIds(namespace, "reject");
      yield* sink.commitRejectedCommand({
        commandId: other.commandId,
        threadId: other.threadId,
        commandType: "thread.create",
        rejectedAt: yield* DateTime.now,
        error: "ordinary rejection",
      });
      assert.equal((yield* ClientLaunch.lookup(namespace, "reject")).basis, "rejection");
    }).pipe(Effect.provide(makeHarness().layer)),
);

it.effect(
  "T4/T12: accepted worktree replay skips a deleted base and keeps one active preparation",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const hold = yield* Deferred.make<void>();
      const h = makeHarness({
        runSetup: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(hold)),
            Effect.as({ status: "no-script" as const }),
          ),
      });
      yield* Effect.gen(function* () {
        const context = yield* caller;
        const request = {
          ...input,
          workspaceStrategy: {
            type: "worktree" as const,
            baseRef: "main",
            startFromOrigin: true,
            branch: "synthetic-feature",
          },
        };
        const first = yield* ClientLaunch.launch(request, context, modes, gate);
        yield* Deferred.await(entered);
        assert.equal((yield* ClientLaunch.lookup(namespace, "key")).preparation, "in_progress");
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const replay = ClientLaunch.launch(request, context, modes, gate).pipe(
          Effect.provideService(ThreadLaunch.ThreadLaunchService, {
            ...launches,
            checkWorktreeBase: () => Effect.die("Accepted replay must not inspect a deleted base"),
            launch: () => Effect.die("Active preparation must not be relaunched"),
          }),
        );
        const results = yield* Effect.all([replay, replay], { concurrency: 2 });
        assert.isTrue(
          results.every((result) => result.replayed && result.threadId === first.threadId),
        );
        assert.lengthOf(h.runSetup.mock.calls, 1);
        assert.lengthOf(h.createWorktree.mock.calls, 1);
      }).pipe(Effect.provide(h.layer));
    }),
);
