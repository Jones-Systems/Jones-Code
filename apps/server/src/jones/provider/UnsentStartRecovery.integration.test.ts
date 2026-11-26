import { assert, it } from "@effect/vitest";
import {
  type ChatAttachment,
  ChatAttachmentId,
  CheckpointScopeId,
  CommandId,
  ComposerContextId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationMessageContext,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type Project,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ScheduledTaskId,
  type ServerProvider,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import { createDeterministicAttachmentId, resolveAttachmentPath } from "../../attachmentStore.ts";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../../config.ts";
import * as Environment from "../../environment/ServerEnvironment.ts";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as McpScope from "../../mcp/McpInvocationContext.ts";
import { HandlersLayer } from "../../mcp/McpToolAccess.ts";
import * as OrchestratorMcp from "../../mcp/OrchestratorMcpService.ts";
import * as OrchestratorHandlers from "../../mcp/toolkits/orchestrator/handlers.ts";
import { OrchestratorToolkit } from "../../mcp/toolkits/orchestrator/tools.ts";
import { ClaudeProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as AdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRuntimeRecovery from "../../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as ThreadLaunch from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as RemoteDelegation from "../../peer/RemoteDelegation.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ManagedFolders from "../../project/ManagedProjectFolders.ts";
import * as CloneTracker from "../../project/ProjectCloneTracker.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as SetupScripts from "../../project/ProjectSetupScriptRunner.ts";
import * as SetupTracker from "../../project/WorktreeSetupTracker.ts";
import * as ProviderRegistry from "../../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../../secrets/SecretRequests.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as Terminal from "../../terminal/Manager.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import * as Goals from "../goals/continuity/GoalContinuity.ts";
import * as ContinuityAdmissionTestkit from "../goals/continuity/admission.testkit.ts";
import * as NativeStartAdmission from "../nativeCreation/NativeStartAdmission.ts";
import * as Control from "../orchestration/ControlAdmission.ts";
import * as Restart from "../orchestration/RestartPreparation.ts";
import { EVENT_LOG_PRUNED_THROUGH_PROJECTION } from "../persistence/EventLogPruneBoundary.ts";
import * as Refusals from "../startRefusal/Store.ts";
import * as PreparedCheckout from "../workspaceLifecycle/PreparedStartCheckout.ts";
import * as RecoveryStartDenial from "./RecoveryStartDenial.ts";
import { StartAdmission } from "./StartAdmission.ts";
import * as UnattendedStartAdmission from "./UnattendedStartAdmission.ts";
import { recoveryResendEffect } from "./UnsentStartProof.ts";

const recoveryStores = Layer.mergeAll(
  IdAllocator.layer,
  EventStore.layer,
  EffectOutbox.layer,
  ProjectionStore.layer,
  Receipts.layer,
  Refusals.layer,
  Control.layer,
).pipe(Layer.provideMerge(SqlitePersistence.layerMemory));
const recoverySink = EventSink.layer.pipe(Layer.provideMerge(recoveryStores));
const unattendedRecoveryLayer = Layer.mergeAll(
  ProviderRuntimeRecovery.layer,
  NativeStartAdmission.layer,
).pipe(
  Layer.provide(UnattendedStartAdmission.layer),
  Layer.provideMerge(
    Layer.mergeAll(
      recoverySink,
      ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
      Layer.mock(Restart.RestartPreparation)({ assertThreadOpen: () => Effect.void }),
      Layer.mock(Goals.GoalContinuity)({
        context: () => {
          throw new Error("Unexpected goal continuity context in unattended recovery.");
        },
        requirementFor: () =>
          Effect.succeed({ outcome: "absent", gate: { outcome: "clear", fenceRevision: "0" } }),
      }),
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("recovery-regression")),
      }),
    ),
  ),
);

const initialTime = Date.parse("2026-11-18T15:00:00.000Z");
const parentId = ThreadId.make("prepared-release-parent");
const delegatedText = "Deliver the original prepared input";
const parentStopId = CommandId.make("prepared-release-parent-stop");
type StopTiming = "before birth" | "between birth and release";

const fixtureGit = Effect.fnUntraced(function* (cwd: string, ...args: string[]) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const code = yield* spawner.exitCode(ChildProcess.make("git", args, { cwd }));
  assert.equal(Number(code), 0, `git ${args.join(" ")}`);
});

const replacePreparedDirectory = Effect.fnUntraced(function* (cwd: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  // Keep the Git registration, admin directory and branch; replace only the physical checkout.
  const displaced = `${cwd}-displaced`;
  yield* fs.rename(cwd, displaced);
  yield* fs.makeDirectory(cwd);
  yield* fs.copyFile(path.join(displaced, ".git"), path.join(cwd, ".git"));
  yield* fixtureGit(cwd, "rev-parse", "--verify", "HEAD");
});

const preparedRecovery = ProviderRuntimeRecovery.layer.pipe(
  Layer.provide(UnattendedStartAdmission.layer),
  Layer.provide(
    Layer.mergeAll(
      IdAllocator.layer,
      Receipts.layer,
      Refusals.layer,
      Control.layer,
      Restart.layer.pipe(Layer.provide(EventStore.layer)),
      ContinuityAdmissionTestkit.layerAbsent,
    ),
  ),
);

const delegatePrepared = Effect.fnUntraced(function* (fixture: {
  readonly project: Project;
  readonly cwd: string;
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly model: string;
  readonly stop?: StopTiming;
  readonly newWorktree?: boolean;
}) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const sql = yield* SqlClient.SqlClient;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const setupReached = yield* Deferred.make<void>();
  const releaseSetup = yield* Deferred.make<void>();
  const registry: ServerProvider = {
    instanceId: fixture.instanceId,
    driver: fixture.driver,
    enabled: true,
    installed: true,
    version: "test",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: fixture.project.createdAt,
    models: [{ slug: fixture.model, name: fixture.model, isCustom: false, capabilities: null }],
    slashCommands: [],
    skills: [],
  };
  const local = Layer.mergeAll(
    Layer.succeed(GitVcsDriver.GitVcsDriver, git),
    NodeCrypto.layer,
    IdAllocator.layer,
    Receipts.layer,
    Layer.succeed(ThreadManagement.ThreadManagementService, {
      ...threads,
      getThreadEventSequence: orchestrator.getThreadEventSequence,
    }),
    Layer.mock(ProjectService.ProjectService)({
      getById: (id) => Effect.succeed(Option.liftPredicate(fixture.project, (p) => p.id === id)),
    }),
    Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([registry]) }),
    Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
    Layer.mock(SecretRequests.SecretRequests)({}),
    Layer.mock(RemoteDelegation.RemoteDelegation)({}),
    Layer.mock(ManagedFolders.ManagedProjectFolders)({
      namedProjectsRoot: fixture.project.workspaceRoot,
      isScratchProject: () => Effect.succeed(false),
    }),
    Layer.mock(CloneTracker.ProjectCloneTracker)({}),
    Layer.mock(Terminal.TerminalManager)({}),
    Layer.mock(CheckpointStore.CheckpointStore)({}),
    Layer.mock(TextGeneration.TextGeneration)({}),
    SetupTracker.layer,
    Layer.mock(SetupScripts.ProjectSetupScriptRunner)({
      runForThread: ({ worktreePath }) =>
        Effect.gen(function* () {
          assert.equal(worktreePath, fixture.cwd);
          yield* Deferred.succeed(setupReached, undefined);
          yield* Deferred.await(releaseSetup);
          return { status: "no-script" } as const;
        }),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      repositoryRoot: (cwd) => Effect.succeed(cwd),
      hasCommit: ({ cwd, refName }) =>
        fixtureGit(cwd, "rev-parse", "--verify", refName).pipe(
          Effect.provide(NodeServices.layer),
          Effect.as(true),
          Effect.orDie,
        ),
      listLocalBranchNames: () => Effect.succeed(["main"]),
      createWorktree: ({ cwd, newRefName, refName }, options) =>
        Effect.gen(function* () {
          if (newRefName !== "prepared-branch")
            return yield* Effect.die("Unexpected fixture branch");
          yield* fixtureGit(cwd, "worktree", "add", "-b", newRefName, fixture.cwd, refName);
          const onClaimed = options?.progress?.onWorktreeClaimed;
          if (onClaimed !== undefined) yield* onClaimed(fixture.cwd);
          return { worktree: { path: fixture.cwd, refName: newRefName } };
        }).pipe(Effect.provide(NodeServices.layer), Effect.orDie),
    }),
  );
  const dependencies = OrchestratorMcp.layer.pipe(
    Layer.provide(ThreadLaunch.layerProduction),
    Layer.provide(local),
  );
  const context = yield* Layer.build(
    HandlersLayer.layer(OrchestratorHandlers.layer).pipe(Layer.provideMerge(dependencies)),
  );
  const toolkit = yield* OrchestratorToolkit.pipe(Effect.provide(context));
  const startParent = (key: string) =>
    orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(key),
      threadId: parentId,
      messageId: MessageId.make(key),
      text: "Delegate work",
      attachments: [],
      modelSelection: { instanceId: fixture.instanceId, model: fixture.model },
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "start_immediately" },
    });
  const stopParent = Effect.gen(function* () {
    const stop = yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: parentStopId,
      threadId: parentId,
    });
    assert.isNotEmpty(stop.storedEvents);
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    assert.isTrue(Option.isSome(yield* outbox.get(`effect:${parentStopId}:delegated-tasks.stop`)));
  });
  const invoke = Effect.fnUntraced(function* (
    args: Parameters<typeof toolkit.handle<"delegate_task">>[1],
  ) {
    const { thread, providerThreads } = yield* orchestrator.getThreadRecords(parentId, [
      "providerThreads",
    ]);
    const providerSessionId = providerThreads.find(
      ({ id }) => id === thread.activeProviderThreadId,
    )?.providerSessionId;
    assert.isString(providerSessionId);
    return yield* toolkit.handle("delegate_task", args).pipe(
      Stream.unwrap,
      Stream.runLast,
      Effect.map((value) => {
        const response = Option.getOrThrow(value);
        const result = response.result;
        if (response.isFailure || !("childThreadId" in result))
          throw new Error(JSON.stringify(result));
        const { childThreadId } = result;
        if (childThreadId === null) throw new Error("Expected an admitted local child");
        return childThreadId;
      }),
      Effect.provideService(McpScope.McpInvocationContext, {
        environmentId: EnvironmentId.make("prepared-release-recovery"),
        capabilities: new Set<McpScope.McpCapability>(["orchestration"]),
        issuedAt: initialTime,
        requestNamespace: "prepared-start-recovery",
        thread: {
          threadId: parentId,
          providerSessionId: providerSessionId!,
          providerInstanceId: fixture.instanceId,
        },
        client: undefined,
      }),
      Effect.provide(context),
    );
  });
  yield* startParent("parent-input");
  if (fixture.stop === "before birth") {
    const prior = yield* invoke({
      task: "Previous delegated work",
      clientRequestId: "before-stop",
      mode: "async",
    });
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop-previous-child"),
      threadId: prior,
    });
    yield* stopParent;
    yield* startParent("parent-input-after-stop");
  }
  const threadId = yield* invoke({
    task: delegatedText,
    workspaceStrategy: fixture.newWorktree
      ? { type: "worktree", baseRef: "main", branch: "prepared-branch", startFromOrigin: false }
      : { type: "existing_worktree", worktreePath: fixture.cwd, branch: "prepared-branch" },
    clientRequestId: "prepared-child",
    mode: "async",
  });
  yield* Effect.raceFirst(
    Deferred.await(setupReached),
    orchestrator
      .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.updated" })
      .pipe(
        Stream.filter(
          ({ event }) => event.type === "run.updated" && event.payload.status !== "preparing",
        ),
        Stream.runHead,
        Effect.flatMap(() =>
          orchestrator
            .getThreadRecords(threadId, ["turnItems"])
            .pipe(
              Effect.flatMap(({ turnItems }) => Effect.die(new Error(JSON.stringify(turnItems)))),
            ),
        ),
      ),
  );
  const { runs, messages } = yield* orchestrator.getThreadRecords(threadId, ["runs", "messages"]);
  const run = runs[0]!;
  assert.equal(run.status, "preparing");
  assert.equal(messages[0]?.text, delegatedText);
  const events = yield* sql<{ command_id: string }>`
    SELECT command_id FROM orchestration_events WHERE stream_id = ${threadId} AND event_type = 'run.created'
  `;
  assert.lengthOf(events, 1);
  const birthCommandId = CommandId.make(events[0]!.command_id);
  assert.deepInclude(Option.getOrThrow(yield* threads.getCommandReceipt(birthCommandId)), {
    commandType: "delegated_task.request",
    threadId: parentId,
    status: "accepted",
  });
  if (fixture.stop === "between birth and release") yield* stopParent;
  yield* Deferred.succeed(releaseSetup, undefined);
  const releaseId = CommandId.make(`${birthCommandId}:release`);
  yield* orchestrator
    .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.updated" })
    .pipe(
      Stream.filter((event) => event.commandId === releaseId),
      Stream.runHead,
    );
  return { threadId, run, birthCommandId, releaseId, messageId: messages[0]!.id };
});

const launchPreparedInput = Effect.fnUntraced(function* (
  source: "mcp" | "scheduled" | "delegated",
  options: {
    readonly stop?: StopTiming;
    readonly newWorktree?: true;
    readonly auto?: true;
    readonly swapAtNativeBoundary?: true;
  } = {},
) {
  yield* TestClock.setTime(initialTime);
  const config = yield* Replay.makeReplayServerConfig("prepared-release-recovery");
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const projectRoot = yield* fs.realPath(config.baseDir);
  const projectId = ProjectId.make("prepared-release-project");
  const launchId = CommandId.make("prepared-release-launch");
  const threadId = ThreadId.make("prepared-release-thread");
  const driver = ProviderDriverKind.make(source === "scheduled" ? "codex" : "claudeAgent");
  const instanceId = ProviderInstanceId.make(driver);
  const modelSelection = { instanceId, model: "synthetic" };
  const capabilities =
    source === "scheduled" ? CodexProviderCapabilitiesV2 : ClaudeProviderCapabilitiesV2;
  const cwd = source === "scheduled" ? projectRoot : path.join(projectRoot, "prepared");
  yield* fixtureGit(projectRoot, "init", "-b", "main");
  yield* fixtureGit(
    projectRoot,
    "-c",
    "user.name=Recovery test",
    "-c",
    "user.email=recovery-test@example.invalid",
    "-c",
    "maintenance.auto=false",
    "commit",
    "--allow-empty",
    "-m",
    "Initial checkout",
  );
  if (source !== "scheduled" && !options.newWorktree)
    yield* fixtureGit(projectRoot, "worktree", "add", "-b", "prepared-branch", cwd, "main");
  const sends: Array<ProviderAdapter.ProviderAdapterV2TurnInput> = [];
  const opens: string[] = [];
  const beforeNative: string[] = [];
  const unsupported = () => Effect.die("Unexpected provider operation");
  const database = SqlitePersistence.layerMemory;
  const configLayer = Layer.succeed(ServerConfig.ServerConfig, config);
  const git = GitVcsDriver.layer.pipe(
    Layer.provide(VcsProcess.layer),
    Layer.provide(configLayer),
    Layer.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
  );
  const checkout = PreparedCheckout.layer.pipe(
    Layer.provide(git),
    Layer.provide(Environment.layerIdentity),
    Layer.provide(configLayer),
    Layer.provide(database),
    Layer.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
  );
  const adapters = AdapterRegistry.layerFromAdapters([
    {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(capabilities),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          opens.push(input.providerSessionId);
          const now = yield* DateTime.now;
          return yield* Replay.withReplayThreadPublication(input, {
            instanceId,
            driver,
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd,
              model: modelSelection.model,
              capabilities,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.never,
            ensureThread: ({ existingProviderThread }) =>
              existingProviderThread === undefined
                ? Effect.die("Expected the original provider thread")
                : Effect.succeed({
                    ...existingProviderThread,
                    nativeThreadRef: { driver, nativeId: "native:prepared", strength: "strong" },
                    status: "idle",
                  }),
            resumeThread: unsupported,
            startTurn: (turn) =>
              Effect.gen(function* () {
                beforeNative.push(turn.attemptId);
                if (options.swapAtNativeBoundary)
                  yield* replacePreparedDirectory(cwd).pipe(
                    Effect.provide(NodeServices.layer),
                    Effect.orDie,
                  );
                if (turn.revalidateStartAdmission !== undefined)
                  yield* turn.revalidateStartAdmission;
                sends.push(turn);
              }),
            steerTurn: unsupported,
            interruptTurn: unsupported,
            respondToRuntimeRequest: unsupported,
            readThreadSnapshot: unsupported,
            rollbackThread: unsupported,
            forkThread: unsupported,
          });
        }),
    },
  ]);
  const replay = Replay.layerWithRegistryAndServices(
    {
      name: "prepared-release-recovery",
      ...(source === "scheduled" ? { runtimePolicyOverride: { cwd } } : {}),
    },
    adapters,
    {
      continueThreadsAfterServerUpdate: true,
      runEffectWorker: false,
      serverConfigLayer: configLayer,
      databaseLayer: database,
    },
  );
  const context = yield* Layer.build(
    Layer.merge(replay, adapters).pipe(Layer.provideMerge(checkout), Layer.provideMerge(git)),
  );
  return yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const now = DateTime.formatIso(yield* DateTime.now);
    yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at)
      VALUES(${projectId},'Synthetic',${projectRoot},'[]',${now},${now})`;
    const workspaceStrategy =
      source === "scheduled"
        ? ({ type: options.auto ? "auto" : "root" } as const)
        : ({ type: "worktree", baseRef: "main" } as const);
    const imageBytes = Buffer.from(
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aGaQAAAAASUVORK5CYII=",
      "base64",
    );
    const image = {
      type: "image",
      id: ChatAttachmentId.make(createDeterministicAttachmentId(threadId, "prepared")!),
      name: "prepared.png",
      mimeType: "image/png",
      sizeBytes: imageBytes.length,
    } satisfies ChatAttachment;
    if (source === "mcp") {
      const attachmentPath = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: image,
      });
      assert.isNotNull(attachmentPath);
      yield* fs.writeFile(attachmentPath!, imageBytes);
    }
    const input: Pick<
      ProviderAdapter.ProviderAdapterV2TurnMessage,
      "messageId" | "text" | "attachments" | "scheduledTaskId"
    > & { readonly context?: OrchestrationMessageContext } = {
      messageId: MessageId.make("prepared-release-input"),
      text: delegatedText,
      ...(source === "delegated"
        ? { attachments: [] }
        : source === "scheduled"
          ? { attachments: [], scheduledTaskId: ScheduledTaskId.make("schedule:prepared") }
          : {
              attachments: [image],
              context: {
                version: 1 as const,
                records: [
                  {
                    version: 1 as const,
                    contextId: ComposerContextId.make("prepared-image"),
                    kind: "image" as const,
                    label: image.name,
                    attachmentId: image.id,
                    name: image.name,
                    mimeType: image.mimeType,
                    sizeBytes: image.sizeBytes,
                  },
                ],
              },
            }),
    };
    const actor =
      source === "scheduled"
        ? { createdBy: "user" as const, creationSource: "web" as const }
        : { createdBy: "agent" as const, creationSource: "mcp" as const };
    // ThreadLaunch carries a scheduled user input in its create command and
    // records an MCP agent's initial prompt through a distinct dispatch.
    const created = yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: source === "delegated" ? CommandId.make("create-parent") : launchId,
      threadId: source === "delegated" ? parentId : threadId,
      projectId,
      title: "Prepared input",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      ...actor,
      ...(source === "scheduled" ? { initialMessage: { ...input, workspaceStrategy } } : {}),
    });
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    if (source === "delegated") {
      const delegated = yield* delegatePrepared({
        project: {
          id: projectId,
          title: "Synthetic",
          workspaceRoot: projectRoot,
          scripts: [],
          defaultModelSelection: null,
          createdAt: now,
          updatedAt: now,
          deletedAt: null,
        },
        cwd,
        instanceId,
        driver,
        model: modelSelection.model,
        ...options,
      });
      const originalId = `effect:${delegated.releaseId}:provider-turn.start:${delegated.run.id}`;
      assert.deepInclude(Option.getOrThrow(yield* outbox.get(originalId)), {
        status: "pending",
        attemptCount: 0,
      });
      return {
        ...delegated,
        context,
        actor,
        modelSelection,
        cwd,
        sends,
        opens,
        beforeNative,
        originalId,
        input: { ...input, messageId: delegated.messageId },
        release: undefined,
      };
    }
    const birthCommandId =
      source !== "mcp" ? launchId : CommandId.make(`${launchId}:initial-message`);
    const born =
      source === "scheduled"
        ? created
        : yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: birthCommandId,
            threadId,
            ...actor,
            ...input,
            modelSelection,
            dispatchMode: { type: "defer_start", workspaceStrategy },
          });
    const run = born.storedEvents.flatMap(({ event }) =>
      event.type === "run.created" ? [event.payload] : [],
    )[0]!;
    assert.equal(run.status, "preparing");
    assert.isNotNull(run.activeAttemptId);
    assert.isEmpty(yield* outbox.listByThreadId(threadId));
    if (source === "mcp") {
      yield* orchestrator.dispatch({
        type: "prepared-run.progress",
        commandId: CommandId.make(`${launchId}:progress:worktree`),
        threadId,
        runId: run.id,
        phase: "worktree",
      });
    }
    yield* orchestrator.dispatch({
      type: "thread.metadata.update",
      commandId: CommandId.make(`${launchId}:workspace`),
      threadId,
      worktreePath: source !== "scheduled" ? cwd : null,
      branch: source !== "scheduled" ? "prepared-branch" : "main",
    });
    if (source === "mcp") {
      yield* orchestrator.dispatch({
        type: "prepared-run.progress",
        commandId: CommandId.make(`${launchId}:progress:setup`),
        threadId,
        runId: run.id,
        phase: "setup",
      });
    }
    const releaseId = CommandId.make(`${launchId}:release`);
    const release = yield* orchestrator.dispatch({
      type: "prepared-run.release",
      commandId: releaseId,
      threadId,
      runId: run.id,
    });
    const originalId = `effect:${releaseId}:provider-turn.start:${run.id}`;
    const rows = yield* outbox.listByThreadId(threadId);
    assert.lengthOf(rows, 1);
    assert.deepInclude(rows[0], {
      id: originalId,
      status: "pending",
      attemptCount: 0,
      request: { type: "provider-turn.start", runId: run.id },
    });
    return {
      context,
      run,
      input,
      actor,
      modelSelection,
      cwd,
      sends,
      opens,
      beforeNative,
      originalId,
      releaseId,
      release,
      birthCommandId,
    };
  }).pipe(Effect.provide(context));
});

const reconcilePrepared = (elapsed: number) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(initialTime + elapsed);
    const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
    yield* recovery.reconcile("startup");
  }).pipe(Effect.provide(preparedRecovery, { local: true }));

it.effect.each([
  { name: "scheduled root", source: "scheduled", options: {} },
  { name: "MCP with attachment", source: "mcp", options: {} },
  { name: "MCP delegation to an existing worktree", source: "delegated", options: {} },
  {
    name: "MCP delegation to a new worktree after a retained parent Stop",
    source: "delegated",
    options: { stop: "before birth", newWorktree: true },
  },
] as const)(
  "dispatches the original $name input once after two reconciliations",
  ({ source, options }) =>
    Effect.gen(function* () {
      const prepared = yield* launchPreparedInput(source, options);
      const { run, originalId, input, actor, sends } = prepared;
      yield* Effect.gen(function* () {
        yield* reconcilePrepared(1);
        yield* reconcilePrepared(2);
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const rows = yield* outbox.listByThreadId(run.threadId);
        assert.lengthOf(rows, 3);
        assert.deepInclude(
          rows.find((row) => row.id === originalId),
          {
            status: "cancelled",
            attemptCount: 0,
          },
        );
        assert.lengthOf(
          rows.filter((row) => row.status === "cancelled" && row.attemptCount === 0),
          2,
        );
        const pending = rows.find((row) => row.status === "pending");
        assert.isDefined(pending);
        assert.isEmpty(sends);
        if ("stop" in options) {
          const parked = yield* outbox.claimNext({
            workerId: "parked-parent-stop",
            urgentOnly: true,
          });
          assert.equal(Option.getOrThrow(parked).id, `effect:${parentStopId}:delegated-tasks.stop`);
        }
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        assert.isTrue(yield* worker.runOnce());
        assert.isFalse(yield* worker.runOnce());
        assert.deepInclude(Option.getOrUndefined(yield* outbox.get(pending!.id)), {
          status: "succeeded",
          attemptCount: 1,
        });
        assert.lengthOf(sends, 1);
        const [sent] = sends;
        assert.deepInclude(sent, {
          threadId: run.threadId,
          runId: run.id,
          attemptId: run.activeAttemptId!,
          modelSelection: prepared.modelSelection,
        });
        assert.equal(sent?.runtimePolicy.cwd, prepared.cwd);
        const { context, ...nativeMessage } = input;
        assert.deepEqual(sent?.message, {
          ...nativeMessage,
          ...actor,
          ...(source === "delegated"
            ? { senderThreadId: ThreadId.make("prepared-release-parent") }
            : {}),
        });
        const projection = yield* ProjectionStore.ProjectionStoreV2;
        const records = yield* projection.getThreadRecords(run.threadId, [
          "runs",
          "attempts",
          "messages",
        ]);
        assert.deepEqual(
          records.runs.map((row) => row.id),
          [run.id],
        );
        assert.deepEqual(
          records.attempts.map((row) => row.id),
          [run.activeAttemptId],
        );
        assert.deepEqual(
          records.messages.map((row) => row.id),
          [input.messageId],
        );
        assert.deepEqual(records.messages[0]?.attachments, input.attachments);
        assert.deepEqual(records.messages[0]?.context, context);
      }).pipe(Effect.provide(prepared.context));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each([
  "missing birth receipt",
  "missing release receipt",
  "unresolved preparation",
  "changed captured selection",
  "changed workspace",
  "replacement checkout at the same path",
  "missing checkout witness",
  "mismatched checkout witness",
  "auto workspace without durable checkout identity",
  "wrong delegated parent receipt",
  "different delegated task child",
  "pruned delegated birth evidence",
  "later checkpoint scope",
  "claimed original",
  "native creation guard",
  "Stop",
] as const)("does not resend a prepared input with %s", (scenario) =>
  Effect.gen(function* () {
    const prepared = yield* scenario === "auto workspace without durable checkout identity"
      ? launchPreparedInput("scheduled", { auto: true })
      : launchPreparedInput(
          scenario === "wrong delegated parent receipt" ||
            scenario === "different delegated task child" ||
            scenario === "pruned delegated birth evidence"
            ? "delegated"
            : "mcp",
        );
    const { run, sends, originalId, release, releaseId, birthCommandId } = prepared;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      yield* TestClock.setTime(initialTime + 1);
      switch (scenario) {
        case "missing birth receipt":
        case "missing release receipt":
          yield* sql`DELETE FROM orchestration_command_receipts
            WHERE command_id = ${scenario === "missing birth receipt" ? birthCommandId : releaseId}`;
          break;
        case "unresolved preparation":
          yield* sql`UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.status', 'running')
            WHERE command_id = ${releaseId} AND event_type = 'turn-item.updated'`;
          break;
        case "changed captured selection":
          yield* sql`UPDATE orchestration_events SET payload_json = json_set(payload_json,
              '$.modelSelection.model', 'older-model')
            WHERE command_id = ${birthCommandId} AND event_type = 'run.created'`;
          break;
        case "changed workspace": {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* orchestrator.dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("prepared-release-changed-branch"),
            threadId: run.threadId,
            branch: "other-branch",
          });
          break;
        }
        case "replacement checkout at the same path":
          yield* replacePreparedDirectory(prepared.cwd);
          break;
        case "missing checkout witness":
          yield* sql`DELETE FROM jones_prepared_start_checkouts WHERE run_id = ${run.id}`;
          break;
        case "mismatched checkout witness":
          yield* sql`UPDATE jones_prepared_start_checkouts
            SET release_event_id = 'event:another-release' WHERE run_id = ${run.id}`;
          break;
        case "auto workspace without durable checkout identity":
          assert.isEmpty(
            yield* sql`SELECT 1 FROM jones_prepared_start_checkouts WHERE run_id = ${run.id}`,
          );
          break;
        case "wrong delegated parent receipt":
          yield* sql`UPDATE orchestration_command_receipts SET aggregate_id = ${run.threadId}
            WHERE command_id = ${birthCommandId}`;
          break;
        case "different delegated task child":
          yield* sql`UPDATE orchestration_v2_projection_subagents SET child_thread_id = 'some-other-child'
            WHERE thread_id = ${parentId} AND child_thread_id = ${run.threadId}`;
          break;
        case "pruned delegated birth evidence":
          yield* sql`DELETE FROM orchestration_events
            WHERE command_id = ${birthCommandId} AND event_type = 'run-attempt.created'`;
          break;
        case "later checkpoint scope": {
          const sink = yield* EventSink.EventSinkV2;
          const scope = release?.storedEvents.find(
            ({ event }) => event.type === "checkpoint-scope.created",
          )?.event;
          if (scope?.type !== "checkpoint-scope.created")
            return yield* Effect.die("Expected released root scope");
          yield* sink.writeWithEffects({
            events: [
              {
                ...scope,
                id: EventId.make("event:later-checkpoint-scope"),
                occurredAt: yield* DateTime.now,
                payload: { ...scope.payload, id: CheckpointScopeId.make("scope:later-checkpoint") },
              },
            ],
            effects: [],
          });
          break;
        }
        case "claimed original": {
          const claimed = yield* outbox.claimNext({ workerId: "prepared-interrupted-after-claim" });
          assert.equal(Option.getOrUndefined(claimed)?.id, originalId);
          break;
        }
        case "native creation guard":
          yield* sql`UPDATE orchestration_v2_effect_outbox
            SET payload_json = json_object('request', json(payload_json),
              'nativeCreationExecutionReference', json_object(
                'version', 2, 'claimId', 'native-claim', 'stageCommandId', ${releaseId},
                'stage', 'native_command', 'effectId', effect_id
              ))
            WHERE effect_id = ${originalId}`;
          break;
        case "Stop": {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("prepared-release-interrupt"),
            threadId: run.threadId,
            runId: run.id,
          });
          break;
        }
      }
      yield* reconcilePrepared(2);
      const rows = (yield* outbox.listByThreadId(run.threadId)).filter(
        ({ request }) => request.type === "provider-turn.start",
      );
      assert.lengthOf(rows, 1);
      assert.deepInclude(rows[0], { id: originalId, status: "cancelled" });
      const projection = yield* ProjectionStore.ProjectionStoreV2;
      assert.equal(
        (yield* projection.getRun(run.threadId, run.id))?.status,
        scenario === "Stop" ? "interrupted" : "cancelled",
      );
      assert.isEmpty(sends);
    }).pipe(Effect.provide(prepared.context));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each([
  { name: "at startup when Stop fell between birth and release", at: "startup" },
  { name: "at claim when Stop fell between birth and release", at: "claim" },
  { name: "at claim when the earlier Stop event was pruned", at: "pruned" },
] as const)("blocks a prepared delegated input $name", ({ at }) =>
  Effect.gen(function* () {
    const prepared = yield* launchPreparedInput("delegated", {
      stop: at === "pruned" ? "before birth" : "between birth and release",
    });
    yield* Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const rows = outbox
        .listByThreadId(prepared.run.threadId)
        .pipe(
          Effect.map((rows) =>
            rows.filter(({ request }) => request.type === "provider-turn.start"),
          ),
        );
      if (at === "startup") {
        yield* reconcilePrepared(1);
        assert.lengthOf(yield* rows, 1);
      } else {
        // Exercise claim admission independently of the startup Stop check.
        if (at === "pruned") {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`DELETE FROM orchestration_events WHERE sequence = (
              SELECT result_sequence FROM orchestration_command_receipts WHERE command_id = ${parentStopId}
            )`;
        }
        yield* outbox.reconcileAfterProcessLoss;
        const candidate = recoveryResendEffect(
          prepared.run,
          CommandId.make("stale-reconciliation"),
        );
        const sink = yield* EventSink.EventSinkV2;
        yield* sink.writeWithEffects({ events: [], effects: [candidate] });
        const parked = yield* outbox.claimNext({
          workerId: "parked-parent-stop",
          urgentOnly: true,
        });
        assert.equal(Option.getOrThrow(parked).id, `effect:${parentStopId}:delegated-tasks.stop`);
        const ran = yield* Effect.flatMap(EffectWorker.OrchestrationEffectWorkerV2, (worker) =>
          worker.runOnce(),
        ).pipe(
          Effect.provide(
            EffectWorker.layerWithOptions({ workerId: "prepared-stale-reconciliation" }).pipe(
              Layer.provide(
                RecoveryStartDenial.layer.pipe(
                  Layer.provide(Layer.merge(IdAllocator.layer, Receipts.layer)),
                ),
              ),
            ),
          ),
        );
        assert.isFalse(ran);
        assert.deepInclude(Option.getOrThrow(yield* outbox.get(candidate.id)), {
          status: "failed",
          attemptCount: 0,
          leaseOwner: null,
        });
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        assert.equal(
          (yield* projections.getRun(prepared.run.threadId, prepared.run.id))?.status,
          "starting",
        );
      }
      assert.deepInclude(
        (yield* rows).find((row) => row.id === prepared.originalId),
        {
          status: "cancelled",
          attemptCount: 0,
        },
      );
      assert.isEmpty(prepared.opens);
      assert.isEmpty(prepared.sends);
    }).pipe(Effect.provide(prepared.context));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each([
  "after enqueue",
  "at the adapter boundary",
  "when the witness disappears",
] as const)("holds a prepared replacement %s", (scenario) =>
  Effect.gen(function* () {
    const prepared = yield* launchPreparedInput(
      "mcp",
      scenario === "at the adapter boundary" ? { swapAtNativeBoundary: true } : {},
    );
    yield* Effect.gen(function* () {
      yield* reconcilePrepared(1);
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const pending = (yield* outbox.listByThreadId(prepared.run.threadId)).find(
        (row) => row.status === "pending",
      );
      assert.isDefined(pending);
      assert.equal(pending?.attemptCount, 0);
      if (scenario === "after enqueue") yield* replacePreparedDirectory(prepared.cwd);
      if (scenario === "when the witness disappears") {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DELETE FROM jones_prepared_start_checkouts WHERE run_id = ${prepared.run.id}`;
      }
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      assert.isTrue(yield* worker.runOnce());
      assert.isFalse(yield* worker.runOnce());
      const settled = Option.getOrThrow(yield* outbox.get(pending!.id));
      assert.equal(settled.attemptCount, 1);
      assert.isEmpty(prepared.sends);
      const projection = yield* ProjectionStore.ProjectionStoreV2;
      assert.equal(
        (yield* projection.getRun(prepared.run.threadId, prepared.run.id))?.status,
        "failed",
      );
      const adapterWasReached = scenario === "at the adapter boundary";
      assert.lengthOf(prepared.opens, adapterWasReached ? 1 : 0);
      assert.lengthOf(prepared.beforeNative, adapterWasReached ? 1 : 0);
    }).pipe(Effect.provide(prepared.context));
  }).pipe(Effect.provide(NodeServices.layer)),
);

const seedUnattendedStart = Effect.fnUntraced(function* (source: "mcp" | "scheduled") {
  yield* TestClock.setTime(initialTime);
  const now = yield* DateTime.now;
  const sink = yield* EventSink.EventSinkV2;
  const bornQueued = source === "scheduled";
  const threadId = ThreadId.make("thread:unattended-recovery");
  const runId = RunId.make("run:unattended-recovery");
  const attemptId = RunAttemptId.make("attempt:unattended-recovery");
  const rootNodeId = NodeId.make("node:unattended-recovery");
  const providerThreadId = ProviderThreadId.make("provider-thread:unattended-recovery");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const modelSelection = { instanceId: providerInstanceId, model: "gpt-5.4" };
  const driver = ProviderDriverKind.make("codex");
  const actor = bornQueued
    ? { createdBy: "user" as const, creationSource: "web" as const }
    : { createdBy: "agent" as const, creationSource: "mcp" as const };
  const thread: OrchestrationV2AppThread = {
    ...actor,
    id: threadId,
    projectId: ProjectId.make("project:unattended-recovery"),
    title: "Unattended input",
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThreadId,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const providerThread: OrchestrationV2ProviderThread = {
    id: providerThreadId,
    appThreadId: threadId,
    ownerNodeId: null,
    providerInstanceId,
    providerSessionId: ProviderSessionId.make("session:unattended-recovery"),
    driver,
    nativeThreadRef: null,
    nativeConversationHeadRef: null,
    status: "not_loaded",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const message: OrchestrationV2ConversationMessage = {
    ...actor,
    ...(bornQueued ? { scheduledTaskId: ScheduledTaskId.make("scheduled:recovery") } : {}),
    id: MessageId.make("message:unattended-recovery"),
    threadId,
    runId,
    nodeId: rootNodeId,
    role: "user",
    text: "Deliver the original unattended input",
    attachments: [],
    streaming: false,
    createdAt: now,
    updatedAt: now,
  };
  const run: OrchestrationV2Run = {
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId,
    modelSelection,
    providerThreadId,
    userMessageId: message.id,
    rootNodeId,
    activeAttemptId: attemptId,
    status: "starting",
    queuePosition: null,
    requestedAt: now,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const attempt: OrchestrationV2RunAttempt = {
    id: attemptId,
    runId,
    attemptOrdinal: 1,
    rootNodeId,
    providerInstanceId,
    providerThreadId,
    providerTurnId: null,
    reason: "initial",
    status: "pending",
    startedAt: null,
    completedAt: null,
  };
  const birthId = CommandId.make("command:unattended-recovery:birth");
  const startId = bornQueued ? CommandId.make("command:unattended-recovery:promote") : birthId;
  const original: EffectOutbox.PendingOrchestrationEffectV2 = {
    id: `effect:${startId}:provider-turn.start:${runId}`,
    commandId: startId,
    threadId,
    request: { type: "provider-turn.start", runId },
  };
  const base = { threadId, providerInstanceId, occurredAt: now };
  const runBase = { ...base, runId, nodeId: rootNodeId };
  const event = (name: string) => EventId.make(`event:unattended-recovery:${name}`);
  const birth = yield* sink.commitCommand({
    commandId: birthId,
    commandType: "message.dispatch",
    threadId,
    acceptedAt: now,
    events: [
      { ...base, id: event("thread"), type: "thread.created", payload: thread },
      { ...base, id: event("provider"), type: "provider-thread.updated", payload: providerThread },
      {
        ...runBase,
        id: event("run"),
        type: "run.created",
        payload: bornQueued ? { ...run, status: "queued", queuePosition: 1 } : run,
      },
      { ...runBase, id: event("attempt"), type: "run-attempt.created", payload: attempt },
      { ...runBase, id: event("message"), type: "message.updated", payload: message },
    ],
    effects: bornQueued ? [] : [original],
  });
  if (bornQueued) {
    yield* sink.writeWithEffects({
      commandId: startId,
      queuePromotion: { commandId: startId, threadId, runId },
      events: [
        {
          ...runBase,
          id: event("promotion-input"),
          type: "turn-item.updated",
          payload: {
            ...actor,
            id: TurnItemId.make("item:unattended-recovery"),
            threadId,
            runId,
            nodeId: rootNodeId,
            providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 100,
            status: "completed",
            title: null,
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            type: "user_message",
            messageId: message.id,
            scheduledTaskId: message.scheduledTaskId!,
            text: message.text,
            attachments: message.attachments,
            inputIntent: "queued_turn",
          },
        },
        { ...runBase, id: event("promotion"), type: "run.updated", payload: run },
      ],
      effects: [original],
    });
  }
  return { run, attempt, message, providerThread, original, birth };
});

it.effect.each(["mcp", "scheduled"] as const)(
  "delivers one %s start after two reconciliations that the worker never claimed",
  (source) =>
    Effect.gen(function* () {
      const { run, attempt, message, providerThread, original } =
        yield* seedUnattendedStart(source);
      const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const admission = yield* StartAdmission;
      const sends: Array<{ readonly effectId: string; readonly text: string }> = [];
      for (const elapsed of [1, 2]) {
        yield* TestClock.setTime(initialTime + elapsed);
        assert.equal((yield* recovery.reconcile("startup")).terminalizedRuns, 0);
      }
      const rows = yield* outbox.listByThreadId(run.threadId);
      assert.lengthOf(rows, 3);
      assert.deepInclude(
        rows.find((row) => row.id === original.id),
        {
          status: "cancelled",
          attemptCount: 0,
        },
      );
      assert.lengthOf(
        rows.filter((row) => row.status === "cancelled" && row.attemptCount === 0),
        2,
      );
      const pending = rows.find((row) => row.status === "pending");
      assert.isDefined(pending);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const executor = Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
        execute: (row) =>
          Effect.gen(function* () {
            assert.deepEqual(row.request, { type: "provider-turn.start", runId: run.id });
            const claim = yield* admission.capture({
              threadId: run.threadId,
              run,
              attemptId: attempt.id,
              providerThread,
            });
            yield* claim.revalidate;
            const messages = yield* projections.getMessagesById(run.threadId, [run.userMessageId]);
            sends.push({ effectId: row.id, text: messages.get(run.userMessageId)!.text });
          }).pipe(
            Effect.mapError(
              (cause) =>
                new EffectWorker.OrchestrationEffectExecutionError({
                  effectId: row.id,
                  effectType: row.request.type,
                  cause,
                }),
            ),
          ),
      });
      yield* Effect.gen(function* () {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        assert.isTrue(yield* worker.runOnce());
        assert.isFalse(yield* worker.runOnce());
      }).pipe(Effect.provide(EffectWorker.layer.pipe(Layer.provide(executor))));
      assert.deepEqual(sends, [{ effectId: pending?.id, text: message.text }]);
      assert.deepInclude(Option.getOrUndefined(yield* outbox.get(pending!.id)), {
        status: "succeeded",
        attemptCount: 1,
      });
    }).pipe(Effect.provide(unattendedRecoveryLayer)),
);

it.effect.each([
  "claimed",
  "missing outbox row",
  "missing receipt",
  "missing event",
  "pruned history",
  "conflicting message",
  "native guard",
  "control hold",
] as const)("refuses to resend when a replacement has %s", (scenario) =>
  Effect.gen(function* () {
    const { run, original, birth } = yield* seedUnattendedStart("mcp");
    const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const sql = yield* SqlClient.SqlClient;
    yield* TestClock.setTime(initialTime + 1);
    assert.equal((yield* recovery.reconcile("startup")).terminalizedRuns, 0);
    const replacement = (yield* outbox.listByThreadId(run.threadId)).find(
      (row) => row.status === "pending",
    );
    assert.isDefined(replacement);
    const effectId = replacement!.id;
    const reconciliationId = effectId.slice(`effect:restart-resend:${run.id}:`.length);
    switch (scenario) {
      case "claimed": {
        const claimed = yield* outbox.claimNext({ workerId: "interrupted-before-run-start" });
        assert.equal(Option.getOrUndefined(claimed)?.id, effectId);
        break;
      }
      case "missing outbox row":
        yield* sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = ${effectId}`;
        break;
      case "missing receipt":
        yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${reconciliationId}`;
        break;
      case "missing event":
        yield* sql`DELETE FROM orchestration_events
          WHERE command_id = ${reconciliationId} AND event_type = 'run.updated'`;
        break;
      case "pruned history":
        yield* sql`INSERT OR REPLACE INTO orchestration_v2_projection_metadata
          (projection_name, schema_version, last_sequence, updated_at)
          VALUES (${EVENT_LOG_PRUNED_THROUGH_PROJECTION}, 1, ${birth.receipt.resultSequence},
            ${DateTime.formatIso(birth.receipt.acceptedAt)})`;
        break;
      case "conflicting message":
        yield* sql`UPDATE orchestration_events
          SET payload_json = json_set(payload_json, '$.userMessageId', 'message:other')
          WHERE command_id = ${reconciliationId} AND event_type = 'run.updated'`;
        break;
      case "native guard":
        yield* sql`UPDATE orchestration_v2_effect_outbox
          SET payload_json = json_object('request', json(payload_json),
            'nativeCreationExecutionReference', json_object(
              'version', 2, 'claimId', 'native-claim', 'stageCommandId', ${original.commandId},
              'stage', 'native_command', 'effectId', effect_id
            ))
          WHERE effect_id = ${effectId}`;
        break;
      case "control hold":
        yield* (yield* Control.ControlAdmission).holdStartup({
          instanceId: run.providerInstanceId,
          threadId: run.threadId,
          nativeThreadId: "native:unknown-outcome",
          producer: null,
        });
        break;
    }
    yield* TestClock.setTime(initialTime + 2);
    assert.equal((yield* recovery.reconcile("startup")).terminalizedRuns, 1);
    const rows = yield* outbox.listByThreadId(run.threadId);
    assert.isTrue(rows.every((row) => row.status === "cancelled"));
    assert.isTrue(rows.every((row) => row.id === original.id || row.id === effectId));
    const projection = yield* ProjectionStore.ProjectionStoreV2;
    assert.equal((yield* projection.getRun(run.threadId, run.id))?.status, "cancelled");
  }).pipe(Effect.provide(unattendedRecoveryLayer)),
);

it.effect("rolls back the recovery receipt and events when the replacement write fails", () =>
  Effect.gen(function* () {
    const { run, original } = yield* seedUnattendedStart("mcp");
    const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const receipts = yield* Receipts.CommandReceiptStoreV2;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TEMP TRIGGER fail_recovery_outbox
      BEFORE INSERT ON orchestration_v2_effect_outbox
      WHEN NEW.command_id LIKE 'command:restart-resend:%'
      BEGIN SELECT RAISE(ABORT, 'replacement write failed'); END`;
    yield* TestClock.setTime(initialTime + 1);
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    const commandId = CommandId.make(
      `command:runtime-reconcile:startup:${run.threadId}:${timestamp}`,
    );
    yield* recovery.reconcile("startup").pipe(Effect.flip);
    assert.isTrue(Option.isNone(yield* receipts.getByCommandId(commandId)));
    const events = yield* sql`SELECT 1 FROM orchestration_events WHERE command_id = ${commandId}`;
    assert.lengthOf(events, 0);
    const rows = yield* outbox.listByThreadId(run.threadId);
    assert.lengthOf(rows, 1);
    assert.deepInclude(rows[0], { id: original.id, status: "cancelled", attemptCount: 0 });

    yield* sql`DROP TRIGGER fail_recovery_outbox`;
    assert.equal((yield* recovery.reconcile("startup")).terminalizedRuns, 0);
    assert.isTrue(Option.isSome(yield* receipts.getByCommandId(commandId)));
    const recovered = yield* outbox.listByThreadId(run.threadId);
    assert.lengthOf(
      recovered.filter((row) => row.status === "pending"),
      1,
    );
  }).pipe(Effect.provide(unattendedRecoveryLayer)),
);
