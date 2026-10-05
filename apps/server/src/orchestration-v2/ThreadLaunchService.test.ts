import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as Sink from "effect/Sink";
import { makeGitVcsDriverCore } from "../vcs/GitVcsDriverCore.ts";
// @effect-diagnostics nodeBuiltinImport:off - synthetic material claims need lstat device/inode; no real Git or setup executes.
import * as NodeFS from "node:fs";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProcessRunner from "../processRunner.ts";
import type * as PtyAdapter from "../terminal/PtyAdapter.ts";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Fiber from "effect/Fiber";
import * as EventSink from "./EventSink.ts";
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
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

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
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";

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
  readonly privateDUnavailable?: boolean;
  readonly terminalOwner?: Layer.Layer<TerminalManager.TerminalManager>;
  readonly workspaceRoot?: string;
  readonly projectScripts?: ReadonlyArray<ProjectScript>;
  readonly managedFolders?: Layer.Layer<ManagedProjectFolders.ManagedProjectFolders>;
  readonly createWorktree?: GitWorkflow.GitWorkflowService["Service"]["createWorktree"];
  readonly isRepository?: GitWorkflow.GitWorkflowService["Service"]["isRepository"];
  readonly hasCommit?: GitWorkflow.GitWorkflowService["Service"]["hasCommit"];
  readonly fetchRemote?: GitWorkflow.GitWorkflowService["Service"]["fetchRemote"];
  readonly renameBranch?: GitWorkflow.GitWorkflowService["Service"]["renameBranch"];
  readonly runSetup?: ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"];
  readonly generateTitle?: TextGeneration.TextGeneration["Service"]["generateThreadTitle"];
  readonly generateBranchName?: TextGeneration.TextGeneration["Service"]["generateBranchName"];
  readonly serverSettings?: Parameters<typeof ServerSettings.layerTest>[0];
  readonly providers?: ReadonlyArray<ServerProvider>;
}

function makeHarness(options: HarnessOptions = {}) {
  const boundProject = {
    ...project,
    workspaceRoot: options.workspaceRoot ?? project.workspaceRoot,
    scripts: options.projectScripts ?? project.scripts,
  };
  const database = SqlitePersistenceMemory;
  const registry = ProviderAdapterRegistry.makeLayer([adapter]);
  const orchestratorBase = makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "thread-launch" },
    registry,
    { databaseLayer: database, runEffectWorker: false },
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
    options.terminalOwner ??
      Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
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
              ? Option.some(otherProject)
              : Option.none(),
        ),
      getByWorkspaceRoot: () => Effect.succeed(Option.some(boundProject)),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree,
      isRepository: options.isRepository ?? (() => Effect.succeed(true)),
      hasCommit: options.hasCommit ?? (() => Effect.succeed(true)),
      renameBranch,
      fetchRemote: options.fetchRemote ?? (() => Effect.void),
      remoteExists: () => Effect.succeed(true),
      remoteBranchExists: () => Effect.succeed(true),
      removeWorktree,
      resolveRemoteTrackingCommit: () =>
        Effect.succeed({ commitSha: "remote-main-sha", remoteRefName: "origin/main" }),
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
        const owned = `${workspaceRoot}/owned`;
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
              renameCount++;
              yield* hooks.afterEffect(
                step,
                scenario === "rename_unknown" ? "failed_or_unknown" : "settled_success",
                scenario === "rename_unknown" ? undefined : { ...hooks.claim, headRef: targetRef },
              );
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
              const parent = NodeFS.lstatSync(workspaceRoot);
              const step = {
                kind: "worktree.add" as const,
                cwd: workspaceRoot,
                args: ["worktree", "add", "-b", input.newRefName!, owned, oid],
                worktreePath: owned,
                commonDirectory,
                baseCommitOid: oid,
                targetRef: `refs/heads/${input.newRefName}`,
                before: {
                  parentPath: workspaceRoot,
                  parentRealPath: NodeFS.realpathSync(workspaceRoot),
                  parentDevice: String(parent.dev),
                  parentInode: String(parent.ino),
                  targetRefAbsent: true as const,
                  registrationAbsent: true as const,
                },
              };
              yield* hooks.beforeEffect(step);
              addCount++;
              yield* fs.makeDirectory(owned);
              yield* fs.writeFileString(`${owned}/.git`, `gitdir: ${gitDirectory}\n`);
              const material = NodeFS.lstatSync(owned);
              const claim = {
                path: owned,
                realPath: NodeFS.realpathSync(owned),
                device: String(material.dev),
                inode: String(material.ino),
                parentRealPath: NodeFS.realpathSync(workspaceRoot),
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
      createWorktree: () =>
        Deferred.succeed(worktreeEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowWorktree)),
          Effect.as({
            worktree: { path: "/repo-worktrees/feature", refName: "feature", headSha: "abc" },
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
            }),
          ),
          launches.launch(
            launchInput({
              command: "command:launch:concurrent-b",
              thread: "thread:launch:concurrent-b",
              message: "Second",
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
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({
            status: "started" as const,
            async: false,
            scriptId: "setup",
            scriptName: "Setup",
            scriptCommand: "vp install",
            terminalId: "setup",
            cwd: "/repo",
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
        workspace: { type: "worktree", baseRef: "main" },
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
      assert.equal(projection.checkpointScopes[0]?.cwd, "/repo-worktrees/feature");
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
        const launched = yield* launches.launch(
          launchInput({
            command: "command:launch:queued-during-preparation",
            thread: "thread:launch:queued-during-preparation",
            message: "Prepare the workspace",
            workspace: { type: "worktree", baseRef: "main" },
          }),
        );
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
          "/repo-worktrees/feature",
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
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
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
      }
    }).pipe(Effect.provide(harness.layer));
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
    const claimed: Array<{ readonly threadId: ThreadId; readonly text: string }> = [];
    const harness = makeHarness({
      managedFolders: Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        folderForThread: (input) =>
          Effect.sync(() => {
            if (input.projectId !== projectId) return Option.none();
            claimed.push({ threadId: input.threadId, text: input.text });
            return Option.some(`/scratch/folder-${claimed.length}`);
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
      assert.equal(launched.projection.thread.worktreePath, "/scratch/folder-1");
      yield* waitUntil(() => Effect.sync(() => harness.runSetup.mock.calls.length === 1));
      assert.equal(harness.runSetup.mock.calls[0]?.[0]?.worktreePath, "/scratch/folder-1");
      assert.equal(harness.createWorktree.mock.calls.length, 0);

      // A retry replays the first attempt and claims no second folder.
      const retried = yield* launches.launch(input);
      assert.isTrue(retried.resumed);
      assert.lengthOf(claimed, 1);
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.worktreePath,
        "/scratch/folder-1",
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
  }),
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
    const branchNameStarted = yield* Deferred.make<void>();
    const allowBranchName = yield* Deferred.make<void>();
    const harness = makeHarness({
      createWorktree: (input) =>
        Effect.succeed({
          worktree: { path: "/repo-worktrees/temp", refName: input.newRefName, headSha: "abc" },
        } as never),
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
      assert.deepEqual(harness.renameBranch.mock.calls[0]?.[0], {
        cwd: "/repo-worktrees/temp",
        oldBranch: "t3code/abcd1234",
        newBranch: "generated-branch",
      });
    }).pipe(Effect.provide(harness.layer));
  }),
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
          worktree: { path: "/repo-worktrees/temp", refName: input.newRefName, headSha: "abc" },
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
    const harness = makeHarness();
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
      assert.deepEqual(harness.renameBranch.mock.calls[0]?.[0], {
        cwd: "/repo-worktrees/t3code-abcd1234",
        oldBranch: "t3code/abcd1234",
        newBranch: "generated-branch",
      });
    }).pipe(Effect.provide(harness.layer));
  }),
);

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
    assert.equal(projection.thread.worktreePath, null);
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

it.effect("retries a failed workspace preparation on the same run", () => {
  let fetchFailures = 1;
  const harness = makeHarness({
    fetchRemote: () =>
      fetchFailures-- > 0
        ? Effect.fail(
            new GitCommandError({
              operation: "GitVcsDriver.fetchRemote",
              command: "git",
              cwd: project.workspaceRoot,
              detail: "Git could not update a local reference.",
              exitCode: 1,
            }),
          )
        : Effect.void,
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:retry",
        thread: "thread:launch:retry",
        message: "Retry me",
        workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
      }),
    );
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
    yield* launches.retryPreparation(retry);
    yield* waitUntil(() =>
      outbox
        .listByCommandId(CommandId.make("command:launch:retry:1:release"))
        .pipe(Effect.map((effects) => effects.length === 1)),
    );
    const retried = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(retried.runs.length, 1);
    assert.equal(retried.runs[0]?.status, "starting");
    assert.equal(retried.thread.worktreePath, "/repo-worktrees/feature");
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
});

it.effect("retains the explicit setup opt-out when a failed bootstrap is retried", () => {
  let fetchFailures = 1;
  const harness = makeHarness({
    fetchRemote: () =>
      fetchFailures-- > 0
        ? Effect.fail(
            new GitCommandError({
              operation: "GitVcsDriver.fetchRemote",
              command: "git",
              cwd: project.workspaceRoot,
              detail: "Local reference failure",
              exitCode: 1,
            }),
          )
        : Effect.void,
    runSetup: () => Effect.die("Setup must stay opted out"),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const launched = yield* launches.launch({
      ...launchInput({
        command: "command:launch:retry-no-setup",
        thread: "thread:launch:retry-no-setup",
        message: "Retry without setup",
        workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
      }),
      runSetupScript: false,
    });
    yield* waitUntil(() =>
      threads
        .getThreadProjection(launched.threadId)
        .pipe(Effect.map((p) => p.runs[0]?.status === "failed")),
    );
    const failed = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(failed.runs[0]?.workspaceRunSetupScript, false);
    const commandId = CommandId.make("command:launch:retry-no-setup:retry");
    yield* launches.retryPreparation({
      commandId,
      threadId: launched.threadId,
      runId: failed.runs[0]!.id,
    });
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
});

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
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:reuse",
        thread: "thread:launch:reuse",
        message: "Reuse the worktree",
        workspace: { type: "worktree", baseRef: "main" },
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
    assert.deepEqual(
      snapshot?.stages.map((stage) => stage.id),
      ["setup-script", "agent"],
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect("removes a worktree that failed before the thread recorded it", () => {
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
      ["/repo-worktrees/partial"],
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
      const input = launchInput({
        command: "command:launch:accepted-before-fork",
        thread: "thread:launch:accepted-before-fork",
        message: "Resume preparation",
      });
      const messageId = MessageId.make("message:launch:accepted-before-fork");

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
        dispatchMode: { type: "defer_start" },
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

it.effect.each([0, 1])("releases an async setup before its completion with exit %s", (exitCode) =>
  Effect.gen(function* () {
    const completion = yield* Deferred.make<{ exitCode: number | null; durationMs: number }>();
    const harness = makeHarness({
      runSetup: () =>
        Effect.succeed({
          status: "started" as const,
          async: true,
          scriptId: "setup",
          scriptName: "Setup",
          scriptCommand: "vp install",
          terminalId: "setup",
          cwd: "/repo-worktrees/feature",
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
    const h = makeHarness({ fetchRemote: fetch });
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
    const qualifyFailure = (deletePersistenceFails: boolean) =>
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
          terminalOwner: Layer.succeed(TerminalManager.TerminalManager, manager),
          createWorktree: (input, options) => driver.createWorktree(input, options),
        });
        yield* Effect.gen(function* () {
          const launch = yield* ThreadLaunch.ThreadLaunchService;
          const sink = yield* EventSink.EventSinkV2;
          const threads = yield* ThreadManagement.ThreadManagementService;
          const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
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
  },
);
