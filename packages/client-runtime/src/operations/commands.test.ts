import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  CommandId,
  EnvironmentId,
  MessageId,
  GitCommandError,
  OrchestrationDispatchCommandError,
  type VcsListRefsInput,
  type VcsListRefsResult,
  NodeId,
  ORCHESTRATION_V2_WS_METHODS,
  PlanId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  WS_METHODS,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadLaunchInput,
  type OrchestrationV2ThreadProjection,
  type ProjectMutation,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as RpcSession from "../rpc/session.ts";
import type { EnvironmentRpcFailure } from "../rpc/client.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import { v2Now, v2Projection, v2ThreadId } from "../state/orchestrationV2TestFixtures.ts";
import {
  archiveThread,
  cancelQueuedRun,
  createProject,
  dismissThreadUserInput,
  editQueuedRun,
  forkThreadFromRun,
  interruptThreadTurn,
  mergeThreadBack,
  promoteQueuedRun,
  reorderActiveThread,
  reorderQueuedRun,
  revertThreadCheckpoint,
  settleThread,
  startThreadTurn,
  unsettleThread,
  updateProject,
  updateThreadMetadata,
} from "./commands.ts";

const TEST_CRYPTO_LAYER = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => new Uint8Array(size),
    digest: (_algorithm, data) => Effect.succeed(data),
  }),
);

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Test environment",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});

const makeSupervisor = Effect.fn("TestEnvironmentCommands.makeSupervisor")(function* (input: {
  readonly listRefs?: (
    input: VcsListRefsInput,
  ) => Effect.Effect<VcsListRefsResult, EnvironmentRpcFailure<typeof WS_METHODS.vcsListRefs>>;
  readonly onLaunch?: () => Effect.Effect<void>;
  readonly commands: OrchestrationV2Command[];
  readonly projects: ProjectMutation[];
  readonly launches?: OrchestrationV2ThreadLaunchInput[];
  readonly projection?: OrchestrationV2ThreadProjection;
  readonly projectionRequests?: ThreadId[];
  readonly advertiseServerResolvedCommandContext?: boolean;
}) {
  const client = {
    [WS_METHODS.vcsListRefs]: input.listRefs ?? (() => Effect.never),
    [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: (command: OrchestrationV2Command) =>
      Effect.sync(() => {
        input.commands.push(command);
        return { sequence: input.commands.length };
      }),
    [ORCHESTRATION_V2_WS_METHODS.getThreadProjection]: (requestInput: {
      readonly threadId: ThreadId;
    }) =>
      Effect.sync(() => {
        input.projectionRequests?.push(requestInput.threadId);
        return input.projection ?? v2Projection;
      }),
    [ORCHESTRATION_V2_WS_METHODS.launchThread]: (launchInput: OrchestrationV2ThreadLaunchInput) =>
      Effect.gen(function* () {
        input.launches?.push(launchInput);
        yield* input.onLaunch?.() ?? Effect.void;
        return {
          threadId: launchInput.threadId ?? v2ThreadId,
          projection: input.projection ?? v2Projection,
          resumed: false,
        };
      }),
    [WS_METHODS.projectsMutate]: (mutation: ProjectMutation) =>
      Effect.sync(() => {
        input.projects.push(mutation);
        return {
          id: mutation.projectId,
          title: mutation.type === "project.create" ? mutation.title : "Project",
          workspaceRoot:
            mutation.type === "project.create" ? mutation.workspaceRoot : "/workspace/project",
          repositoryIdentity: null,
          faviconPath: null,
          defaultModelSelection: null,
          scripts: [],
          createdAt: "2026-06-06T00:00:00.000Z",
          updatedAt: "2026-06-06T00:00:00.000Z",
          deletedAt: null,
        };
      }),
  } as unknown as WsRpcProtocolClient;
  const session: RpcSession.RpcSession = {
    client,
    initialConfig: Effect.succeed({
      environment: {
        capabilities: {
          repositoryIdentity: true,
          ...(input.advertiseServerResolvedCommandContext === false
            ? {}
            : { serverResolvedCommandContext: true }),
        },
      },
    } as never),
    subscribeServerConfig: (input) => client.subscribeServerConfig(input),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
  return EnvironmentSupervisor.EnvironmentSupervisor.of({
    target: TARGET,
    state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
    session: yield* SubscriptionRef.make(Option.some(session)),
    prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Effect.void,
  } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
});

describe("V2 environment commands", () => {
  it.effect("routes projects through the event-sourced project transport", () =>
    Effect.gen(function* () {
      const projects: ProjectMutation[] = [];
      const supervisor = yield* makeSupervisor({ commands: [], projects });

      yield* createProject({
        projectId: ProjectId.make("project-1"),
        title: "Project",
        workspaceRoot: "/workspace/project",
        createWorkspaceRootIfMissing: true,
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(projects).toEqual([
        {
          type: "project.create",
          commandId: "00000000-0000-4000-8000-000000000000",
          projectId: "project-1",
          title: "Project",
          workspaceRoot: "/workspace/project",
          createWorkspaceRootIfMissing: true,
        },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("persists and clears project presentation and environment settings", () =>
    Effect.gen(function* () {
      const projects: ProjectMutation[] = [];
      const supervisor = yield* makeSupervisor({ commands: [], projects });
      const projectId = ProjectId.make("project-1");
      const projectIcon = { kind: "emoji", emoji: "🌲" } as const;
      yield* updateProject({
        projectId,
        autoPull: true,
        projectIcon,
        faviconPath: "/workspace/project/icon.png",
        defaultThreadEnvMode: "worktree",
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));
      yield* updateProject({
        projectId,
        autoPull: false,
        projectIcon: null,
        faviconPath: null,
        defaultThreadEnvMode: null,
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(projects).toEqual([
        {
          type: "project.update",
          commandId: "00000000-0000-4000-8000-000000000000",
          projectId,
          autoPull: true,
          projectIcon,
          faviconPath: "/workspace/project/icon.png",
          defaultThreadEnvMode: "worktree",
        },
        {
          type: "project.update",
          commandId: "00000000-0000-4000-8000-000000000000",
          projectId,
          autoPull: false,
          projectIcon: null,
          faviconPath: null,
          defaultThreadEnvMode: null,
        },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("preserves caller command ids for idempotent V2 commands", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const supervisor = yield* makeSupervisor({ commands, projects: [] });

      yield* archiveThread({
        commandId: CommandId.make("queued-command"),
        threadId: ThreadId.make("thread-1"),
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(commands).toEqual([
        { type: "thread.archive", commandId: "queued-command", threadId: "thread-1" },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("resolves run ordinal zero to the persisted thread-start checkpoint", () =>
    Effect.gen(function* () {
      const scopeId = CheckpointScopeId.make("checkpoint-scope-root");
      const checkpointId = CheckpointId.make("checkpoint-thread-start");
      const projection: OrchestrationV2ThreadProjection = {
        ...v2Projection,
        checkpoints: [
          {
            id: checkpointId,
            threadId: v2ThreadId,
            scopeId,
            runId: null,
            nodeId: NodeId.make("node-run-1"),
            parentCheckpointId: null,
            ordinalWithinScope: 0,
            appRunOrdinal: null,
            ref: CheckpointRef.make("refs/t3/thread-start"),
            status: "ready",
            files: [],
            capturedAt: v2Now,
          },
        ],
      };
      const commands: OrchestrationV2Command[] = [];
      const supervisor = yield* makeSupervisor({ commands, projects: [], projection });

      yield* revertThreadCheckpoint({
        commandId: CommandId.make("rollback-thread-start"),
        threadId: v2ThreadId,
        turnCount: 0,
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(commands).toEqual([
        {
          type: "checkpoint.rollback",
          commandId: "rollback-thread-start",
          threadId: v2ThreadId,
          scopeId,
          checkpointId,
        },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("preserves plan implementation provenance on V2 runs", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const supervisor = yield* makeSupervisor({ commands, projects: [] });

      yield* startThreadTurn({
        commandId: CommandId.make("implement-plan"),
        threadId: v2ThreadId,
        message: {
          messageId: MessageId.make("message-implementation"),
          role: "user",
          text: "Implement the plan",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        },
        runtimeMode: "full-access",
        interactionMode: "default",
        titleSeed: "Implement the plan",
        sourceProposedPlan: {
          threadId: ThreadId.make("thread-plan"),
          planId: PlanId.make("plan-1"),
        },
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(commands).toHaveLength(1);
      expect(commands[0]).toMatchObject({
        type: "message.dispatch",
        commandId: "implement-plan",
        threadId: v2ThreadId,
        titleSeed: "Implement the plan",
        sourcePlanRef: { threadId: "thread-plan", planId: "plan-1" },
        deliveryIntent: "auto",
        dispatchMode: { type: "start_immediately" },
      });
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("preserves an existing worktree and branch during first-message launch", () =>
    Effect.gen(function* () {
      const launches: OrchestrationV2ThreadLaunchInput[] = [];
      const supervisor = yield* makeSupervisor({ commands: [], projects: [], launches });

      yield* startThreadTurn({
        commandId: CommandId.make("launch-existing-worktree"),
        threadId: v2ThreadId,
        message: {
          messageId: MessageId.make("message-existing-worktree"),
          role: "user",
          text: "Continue here",
          attachments: [],
        },
        runtimeMode: "full-access",
        interactionMode: "default",
        titleSeed: "Continue here",
        bootstrap: {
          createThread: {
            projectId: ProjectId.make("project-1"),
            title: "Thread",
            modelSelection: v2Projection.thread.modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: "feature",
            worktreePath: "/workspace/project-worktrees/feature",
            createdAt: "2026-06-20T00:00:00.000Z",
          },
        },
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(launches[0]).toMatchObject({
        threadId: v2ThreadId,
        title: "Continue here",
        generateTitle: true,
        workspaceStrategy: {
          type: "existing_worktree",
          worktreePath: "/workspace/project-worktrees/feature",
          branch: "feature",
        },
      });
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("provisions an origin-based worktree for an existing empty thread", () =>
    Effect.gen(function* () {
      const launches: OrchestrationV2ThreadLaunchInput[] = [];
      const supervisor = yield* makeSupervisor({ commands: [], projects: [], launches });

      yield* startThreadTurn({
        commandId: CommandId.make("launch-existing-thread-worktree"),
        threadId: v2ThreadId,
        message: {
          messageId: MessageId.make("message-existing-thread-worktree"),
          role: "user",
          text: "Move to a worktree",
          attachments: [],
        },
        runtimeMode: "full-access",
        interactionMode: "default",
        bootstrap: {
          prepareWorktree: {
            projectCwd: "/workspace/project",
            baseBranch: "main",
            branch: "feature",
            startFromOrigin: true,
          },
        },
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(launches[0]).toMatchObject({
        threadId: v2ThreadId,
        reuseExistingThread: true,
        projectId: v2Projection.thread.projectId,
        workspaceStrategy: {
          type: "worktree",
          baseRef: "main",
          branch: "feature",
          startFromOrigin: true,
        },
      });
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("uses server-resolved delivery intent without fetching the full projection", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const projectionRequests: ThreadId[] = [];
      const supervisor = yield* makeSupervisor({ commands, projects: [], projectionRequests });

      for (const [mode, expectedType] of [
        ["queue", "queue_after_active"],
        ["auto", "start_immediately"],
        ["steer", "start_immediately"],
        ["restart", "start_immediately"],
      ] as const) {
        yield* startThreadTurn({
          commandId: CommandId.make(`command-${mode}`),
          threadId: v2ThreadId,
          message: {
            messageId: MessageId.make(`message-${mode}`),
            role: "user",
            text: mode,
            attachments: [],
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          dispatchMode: mode,
        }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

        expect(commands.at(-1)).toMatchObject({
          type: "message.dispatch",
          dispatchMode: { type: expectedType },
          ...(mode === "queue" ? {} : { deliveryIntent: mode }),
        });
      }
      expect(projectionRequests).toEqual([]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("retains projection-shaped delivery for servers without command context support", () =>
    Effect.gen(function* () {
      const activeRunId = RunId.make("legacy-active-run");
      const projection: OrchestrationV2ThreadProjection = {
        ...v2Projection,
        runs: [
          {
            id: activeRunId,
            threadId: v2ThreadId,
            ordinal: 1,
            providerInstanceId: v2Projection.thread.providerInstanceId,
            modelSelection: v2Projection.thread.modelSelection,
            providerThreadId: null,
            userMessageId: MessageId.make("legacy-active-message"),
            rootNodeId: null,
            activeAttemptId: null,
            status: "running",
            requestedAt: v2Now,
            startedAt: v2Now,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
          },
        ],
      };
      const commands: OrchestrationV2Command[] = [];
      const projectionRequests: ThreadId[] = [];
      const supervisor = yield* makeSupervisor({
        commands,
        projects: [],
        projection,
        projectionRequests,
        advertiseServerResolvedCommandContext: false,
      });

      for (const [mode, expectedMode] of [
        ["auto", { type: "queue_after_active" }],
        ["queue", { type: "queue_after_active" }],
        ["steer", { type: "steer_active", targetRunId: activeRunId }],
        ["restart", { type: "restart_active", targetRunId: activeRunId }],
      ] as const) {
        yield* startThreadTurn({
          commandId: CommandId.make(`legacy-command-${mode}`),
          threadId: v2ThreadId,
          message: {
            messageId: MessageId.make(`legacy-message-${mode}`),
            role: "user",
            text: mode,
            attachments: [],
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          dispatchMode: mode,
        }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

        expect(commands.at(-1)).toMatchObject({
          type: "message.dispatch",
          dispatchMode: expectedMode,
        });
        expect(commands.at(-1)).not.toHaveProperty("deliveryIntent");
      }
      expect(projectionRequests).toEqual([v2ThreadId, v2ThreadId, v2ThreadId, v2ThreadId]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect.each([
    "waiting",
    "completed",
    "failed",
    "interrupted",
    "cancelled",
    "rolled_back",
  ] as const)("dispatches Stop for %s runs with background commands except rollback", (status) =>
    Effect.gen(function* () {
      const waitingRunId = RunId.make("run-waiting");
      const projection: OrchestrationV2ThreadProjection = {
        ...v2Projection,
        runs: [
          {
            id: waitingRunId,
            threadId: v2ThreadId,
            ordinal: 1,
            providerInstanceId: v2Projection.thread.providerInstanceId,
            modelSelection: v2Projection.thread.modelSelection,
            providerThreadId: null,
            userMessageId: MessageId.make("message-waiting"),
            rootNodeId: null,
            activeAttemptId: null,
            status,
            requestedAt: v2Now,
            startedAt: v2Now,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
          },
        ],
        turnItems: [
          {
            id: TurnItemId.make("background-command"),
            threadId: v2ThreadId,
            runId: waitingRunId,
            nodeId: null,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1,
            status: "running",
            title: null,
            startedAt: v2Now,
            completedAt: null,
            updatedAt: v2Now,
            type: "command_execution",
            input: "vp run dev",
          },
        ],
      };
      const commands: OrchestrationV2Command[] = [];
      const supervisor = yield* makeSupervisor({ commands, projects: [], projection });

      const result = yield* interruptThreadTurn({ threadId: v2ThreadId }).pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
      );

      expect(result).toEqual({ sequence: status === "rolled_back" ? 0 : 1 });
      expect(commands).toEqual(
        status === "rolled_back"
          ? []
          : [
              {
                type: "run.interrupt",
                commandId: expect.any(String),
                threadId: v2ThreadId,
                runId: waitingRunId,
                holdQueue: true,
              },
            ],
      );
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect(
    "dispatches V2-native relationship and queue commands without compatibility shaping",
    () =>
      Effect.gen(function* () {
        const commands: OrchestrationV2Command[] = [];
        const supervisor = yield* makeSupervisor({ commands, projects: [] });
        const provide = Effect.provideService(
          EnvironmentSupervisor.EnvironmentSupervisor,
          supervisor,
        );

        yield* forkThreadFromRun({
          commandId: CommandId.make("fork"),
          sourceThreadId: v2ThreadId,
          targetThreadId: ThreadId.make("thread-fork"),
          runId: RunId.make("run-1"),
        }).pipe(provide);
        yield* mergeThreadBack({
          commandId: CommandId.make("merge"),
          sourceThreadId: ThreadId.make("thread-fork"),
          targetThreadId: v2ThreadId,
          runId: RunId.make("run-2"),
        }).pipe(provide);
        yield* reorderQueuedRun({
          commandId: CommandId.make("reorder"),
          threadId: v2ThreadId,
          runId: RunId.make("run-3"),
          beforeRunId: RunId.make("run-4"),
        }).pipe(provide);
        yield* promoteQueuedRun({
          commandId: CommandId.make("promote"),
          threadId: v2ThreadId,
          queuedRunId: RunId.make("run-3"),
          targetRunId: RunId.make("run-active"),
        }).pipe(provide);
        yield* cancelQueuedRun({
          commandId: CommandId.make("cancel"),
          threadId: v2ThreadId,
          runId: RunId.make("run-3"),
        }).pipe(provide);
        yield* editQueuedRun({
          commandId: CommandId.make("edit"),
          threadId: v2ThreadId,
          runId: RunId.make("run-3"),
          text: "updated queued text",
        }).pipe(provide);
        yield* editQueuedRun({
          commandId: CommandId.make("edit-attachments"),
          threadId: v2ThreadId,
          runId: RunId.make("run-3"),
          text: "updated queued text with attachments",
          edit: {
            messageId: MessageId.make("message-3"),
            attachments: [
              {
                type: "image",
                id: "attachment-kept",
                name: "kept.png",
                mimeType: "image/png",
                sizeBytes: 64,
              },
            ],
          },
        }).pipe(provide);

        expect(commands).toMatchObject([
          { type: "thread.fork", sourcePoint: { type: "run", runId: "run-1" } },
          { type: "thread.merge_back", sourcePoint: { type: "run", runId: "run-2" } },
          { type: "queued-run.reorder", runId: "run-3", beforeRunId: "run-4" },
          {
            type: "queued-message.promote-to-steer",
            queuedRunId: "run-3",
            targetRunId: "run-active",
          },
          { type: "queued-run.cancel", runId: "run-3" },
          { type: "queued-run.edit", runId: "run-3", text: "updated queued text" },
          {
            type: "queued-run.edit",
            runId: "run-3",
            text: "updated queued text with attachments",
            attachments: [{ id: "attachment-kept" }],
          },
        ]);
        // A text-only edit must not send an attachments replacement list.
        expect(commands[5]).not.toHaveProperty("attachments");
      }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("delegates model selection to the server without fetching the full projection", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const projectionRequests: ThreadId[] = [];
      const supervisor = yield* makeSupervisor({ commands, projects: [], projectionRequests });

      yield* updateThreadMetadata({
        commandId: CommandId.make("same-provider"),
        threadId: v2ThreadId,
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "another-model",
        },
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      yield* updateThreadMetadata({
        commandId: CommandId.make("switch-provider"),
        threadId: v2ThreadId,
        modelSelection: {
          instanceId: ProviderInstanceId.make("claude"),
          model: "claude-sonnet-4-6",
        },
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(commands).toEqual([
        {
          type: "thread.model-selection.set",
          commandId: "same-provider",
          threadId: v2ThreadId,
          modelSelection: { instanceId: "codex", model: "another-model" },
        },
        {
          type: "thread.model-selection.set",
          commandId: "switch-provider",
          threadId: v2ThreadId,
          modelSelection: { instanceId: "claude", model: "claude-sonnet-4-6" },
        },
      ]);
      expect(projectionRequests).toEqual([]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("retains provider-switch shaping for servers without command context support", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const projectionRequests: ThreadId[] = [];
      const supervisor = yield* makeSupervisor({
        commands,
        projects: [],
        projectionRequests,
        advertiseServerResolvedCommandContext: false,
      });

      yield* updateThreadMetadata({
        commandId: CommandId.make("legacy-switch-provider"),
        threadId: v2ThreadId,
        modelSelection: {
          instanceId: ProviderInstanceId.make("claude"),
          model: "claude-sonnet-4-6",
        },
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(projectionRequests).toEqual([v2ThreadId]);
      expect(commands).toEqual([
        {
          type: "provider.switch",
          commandId: "legacy-switch-provider",
          threadId: v2ThreadId,
          modelSelection: { instanceId: "claude", model: "claude-sonnet-4-6" },
        },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect.each([true, false])(
    "rolls back an identified checkpoint without fetching the full projection, restoreFiles=%s",
    (restoreFiles) =>
      Effect.gen(function* () {
        const commands: OrchestrationV2Command[] = [];
        const projectionRequests: ThreadId[] = [];
        const supervisor = yield* makeSupervisor({ commands, projects: [], projectionRequests });

        yield* revertThreadCheckpoint({
          commandId: CommandId.make("rollback-known-checkpoint"),
          threadId: v2ThreadId,
          checkpointId: CheckpointId.make("checkpoint-known"),
          scopeId: CheckpointScopeId.make("scope-known"),
          restoreFiles,
        }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

        expect(projectionRequests).toEqual([]);
        expect(commands).toEqual([
          {
            type: "checkpoint.rollback",
            commandId: "rollback-known-checkpoint",
            threadId: v2ThreadId,
            checkpointId: "checkpoint-known",
            scopeId: "scope-known",
            restoreFiles,
          },
        ]);
      }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("validates identified checkpoints locally for older servers", () =>
    Effect.gen(function* () {
      const checkpointId = CheckpointId.make("legacy-checkpoint");
      const scopeId = CheckpointScopeId.make("legacy-checkpoint-scope");
      for (const status of ["ready", "missing", "error", "stale", null] as const) {
        const commands: OrchestrationV2Command[] = [];
        const projectionRequests: ThreadId[] = [];
        const supervisor = yield* makeSupervisor({
          commands,
          projects: [],
          projectionRequests,
          advertiseServerResolvedCommandContext: false,
          projection: {
            ...v2Projection,
            checkpoints:
              status === null
                ? []
                : [
                    {
                      id: checkpointId,
                      threadId: v2ThreadId,
                      scopeId,
                      runId: null,
                      nodeId: NodeId.make("legacy-checkpoint-node"),
                      parentCheckpointId: null,
                      ordinalWithinScope: 0,
                      appRunOrdinal: null,
                      ref: CheckpointRef.make("refs/t3/legacy-checkpoint"),
                      status,
                      files: [],
                      capturedAt: v2Now,
                    },
                  ],
          },
        });
        const rollback = revertThreadCheckpoint({
          commandId: CommandId.make(`legacy-rollback-${status}`),
          threadId: v2ThreadId,
          checkpointId,
          scopeId,
        }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

        if (status === "ready") {
          yield* rollback;
          expect(commands).toEqual([
            {
              type: "checkpoint.rollback",
              commandId: "legacy-rollback-ready",
              threadId: v2ThreadId,
              checkpointId,
              scopeId,
            },
          ]);
        } else {
          const error = yield* rollback.pipe(Effect.flip);
          expect(error._tag).toBe("OrchestrationV2CheckpointUnavailableError");
          expect(commands).toEqual([]);
        }
        expect(projectionRequests).toEqual([v2ThreadId]);
      }
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("dispatches settle and unsettle commands without timestamps", () =>
    Effect.gen(function* () {
      const dispatched: OrchestrationV2Command[] = [];
      const supervisor = yield* makeSupervisor({ commands: dispatched, projects: [] });

      yield* settleThread({
        commandId: CommandId.make("settle-command"),
        threadId: ThreadId.make("thread-1"),
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));
      yield* unsettleThread({
        commandId: CommandId.make("unsettle-command"),
        threadId: ThreadId.make("thread-1"),
        reason: "user",
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(dispatched).toEqual([
        {
          type: "thread.settle",
          commandId: "settle-command",
          threadId: "thread-1",
        },
        {
          type: "thread.unsettle",
          commandId: "unsettle-command",
          threadId: "thread-1",
          reason: "user",
        },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("sends an active order key without changing activity timestamps", () =>
    Effect.gen(function* () {
      const dispatched: OrchestrationV2Command[] = [];
      const supervisor = yield* makeSupervisor({ commands: dispatched, projects: [] });
      yield* reorderActiveThread({
        commandId: CommandId.make("reorder-command"),
        threadId: ThreadId.make("thread-1"),
        orderKey: "mf",
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));
      expect(dispatched).toEqual([
        {
          type: "thread.active.reorder",
          commandId: "reorder-command",
          threadId: "thread-1",
          orderKey: "mf",
        },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("dismisses a pending user-input request", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const supervisor = yield* makeSupervisor({ commands, projects: [] });

      yield* dismissThreadUserInput({
        commandId: CommandId.make("dismiss-command"),
        threadId: v2ThreadId,
        requestId: RuntimeRequestId.make("request-1"),
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(commands).toEqual([
        {
          type: "thread.user-input.dismiss",
          commandId: "dismiss-command",
          threadId: v2ThreadId,
          requestId: "request-1",
        },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("dispatches an explicit idle start without fetching the full projection", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const projectionRequests: ThreadId[] = [];
      const supervisor = yield* makeSupervisor({
        commands,
        projects: [],
        projectionRequests,
      });

      yield* startThreadTurn({
        commandId: CommandId.make("direct-start"),
        threadId: v2ThreadId,
        message: {
          messageId: MessageId.make("direct-start-message"),
          role: "user",
          text: "continue",
          attachments: [],
        },
        runtimeMode: "full-access",
        interactionMode: "default",
        dispatchMode: "start",
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(projectionRequests).toEqual([]);
      expect(commands).toMatchObject([
        {
          type: "message.dispatch",
          threadId: v2ThreadId,
          dispatchMode: { type: "start_immediately" },
        },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("interrupts a known run without fetching the full projection", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const projectionRequests: ThreadId[] = [];
      const supervisor = yield* makeSupervisor({
        commands,
        projects: [],
        projectionRequests,
      });

      yield* interruptThreadTurn({
        commandId: CommandId.make("direct-interrupt"),
        threadId: v2ThreadId,
        runId: RunId.make("active-run"),
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(projectionRequests).toEqual([]);
      expect(commands).toEqual([
        {
          type: "run.interrupt",
          commandId: "direct-interrupt",
          threadId: v2ThreadId,
          runId: "active-run",
          holdQueue: true,
        },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );
});

const automaticWorktreeTurn = {
  commandId: CommandId.make("captured-command"),
  threadId: ThreadId.make("captured-thread"),
  message: {
    messageId: MessageId.make("captured-message"),
    role: "user" as const,
    text: "Start the task",
    attachments: [],
  },
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
  bootstrap: {
    createThread: {
      projectId: ProjectId.make("captured-project"),
      title: "Captured draft",
      modelSelection: v2Projection.thread.modelSelection,
      runtimeMode: "full-access" as const,
      interactionMode: "default" as const,
      branch: null,
      worktreePath: null,
      createdAt: DateTime.formatIso(v2Now),
    },
    prepareWorktree: {
      projectCwd: "/workspace/project",
      branch: "t3/captured-branch",
      startFromOrigin: true,
    },
    runSetupScript: true,
  },
};

const automaticWorktreeLaunch = {
  commandId: automaticWorktreeTurn.commandId,
  creationSource: "web" as const,
  threadId: automaticWorktreeTurn.threadId,
  projectId: automaticWorktreeTurn.bootstrap.createThread.projectId,
  title: automaticWorktreeTurn.bootstrap.createThread.title,
  generateTitle: false,
  modelSelection: automaticWorktreeTurn.bootstrap.createThread.modelSelection,
  runtimeMode: automaticWorktreeTurn.runtimeMode,
  interactionMode: automaticWorktreeTurn.interactionMode,
  workspaceStrategy: {
    type: "worktree" as const,
    branch: automaticWorktreeTurn.bootstrap.prepareWorktree.branch,
    startFromOrigin: true,
  },
  initialMessage: {
    messageId: automaticWorktreeTurn.message.messageId,
    text: automaticWorktreeTurn.message.text,
    attachments: [],
  },
};

const refsResult = (refs: VcsListRefsResult["refs"], isRepo = true): VcsListRefsResult => ({
  refs,
  isRepo,
  hasPrimaryRemote: refs.some((ref) => ref.isRemote === true),
  nextCursor: null,
  totalCount: refs.length,
});

const withBaseRef = (baseRef: string) => ({
  ...automaticWorktreeLaunch,
  workspaceStrategy: { ...automaticWorktreeLaunch.workspaceStrategy, baseRef },
});

describe("v2 worktree start compatibility", () => {
  it.effect("waits for one unfiltered default-ref lookup before launching on older servers", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const launches: OrchestrationV2ThreadLaunchInput[] = [];
      const lookups: VcsListRefsInput[] = [];
      const firstOperation = yield* Deferred.make<"refs" | "launch">();
      const refs = yield* Deferred.make<VcsListRefsResult>();
      const supervisor = yield* makeSupervisor({
        commands,
        projects: [],
        launches,
        onLaunch: () => Deferred.succeed(firstOperation, "launch").pipe(Effect.asVoid),
        listRefs: (input) =>
          Effect.gen(function* () {
            lookups.push(input);
            yield* Deferred.succeed(firstOperation, "refs");
            return yield* Deferred.await(refs);
          }),
      });
      const start = yield* startThreadTurn(automaticWorktreeTurn).pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.forkChild,
      );

      expect(yield* Deferred.await(firstOperation)).toBe("refs");
      expect(lookups).toEqual([{ cwd: "/workspace/project", limit: 100 }]);
      expect(launches).toEqual([]);
      yield* Deferred.succeed(
        refs,
        refsResult([
          { name: "feature/current", current: true, isDefault: false, worktreePath: null },
          {
            name: "origin/develop",
            current: false,
            isDefault: true,
            isRemote: true,
            worktreePath: null,
          },
        ]),
      );
      expect(yield* Fiber.join(start)).toMatchObject({ threadId: automaticWorktreeTurn.threadId });
      expect(launches).toEqual([withBaseRef("origin/develop")]);
      expect(commands).toEqual([]);
      expect(automaticWorktreeTurn.bootstrap.prepareWorktree).not.toHaveProperty("baseBranch");
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER), Effect.scoped),
  );

  it.effect("passes automatic intent directly to capable servers without the client hint", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const launches: OrchestrationV2ThreadLaunchInput[] = [];
      const lookups: VcsListRefsInput[] = [];
      const supervisor = yield* makeSupervisor({
        commands,
        projects: [],
        launches,
        listRefs: (input) =>
          Effect.sync(() => {
            lookups.push(input);
            return refsResult([]);
          }),
      });
      yield* startThreadTurn({ ...automaticWorktreeTurn, serverResolvesWorktreeBase: true }).pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
      );
      expect(lookups).toEqual([]);
      expect(launches).toEqual([automaticWorktreeLaunch]);
      expect(commands).toEqual([]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect(
    "keeps independent command and thread identities for automatic multi-model launches",
    () =>
      Effect.gen(function* () {
        const launches: OrchestrationV2ThreadLaunchInput[] = [];
        const lookups: VcsListRefsInput[] = [];
        const supervisor = yield* makeSupervisor({
          commands: [],
          projects: [],
          launches,
          listRefs: (input) =>
            Effect.sync(() => {
              lookups.push(input);
              return refsResult([]);
            }),
        });
        for (const suffix of ["one", "two"]) {
          yield* startThreadTurn({
            ...automaticWorktreeTurn,
            commandId: CommandId.make(`multi-model-command-${suffix}`),
            threadId: ThreadId.make(`multi-model-thread-${suffix}`),
            serverResolvesWorktreeBase: true,
            message: {
              ...automaticWorktreeTurn.message,
              messageId: MessageId.make(`multi-model-message-${suffix}`),
            },
            bootstrap: {
              ...automaticWorktreeTurn.bootstrap,
              prepareWorktree: {
                projectCwd: "/workspace/project",
                requireWorktree: true,
                startFromOrigin: true,
              },
            },
          }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));
        }
        expect(lookups).toEqual([]);
        expect(launches).toEqual(
          ["one", "two"].map((suffix) => ({
            ...automaticWorktreeLaunch,
            commandId: CommandId.make(`multi-model-command-${suffix}`),
            threadId: ThreadId.make(`multi-model-thread-${suffix}`),
            initialMessage: {
              ...automaticWorktreeLaunch.initialMessage,
              messageId: MessageId.make(`multi-model-message-${suffix}`),
            },
            workspaceStrategy: { type: "worktree", startFromOrigin: true },
          })),
        );
      }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("preserves an explicit base and skips lookup for current-checkout launches", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const launches: OrchestrationV2ThreadLaunchInput[] = [];
      const lookups: VcsListRefsInput[] = [];
      const supervisor = yield* makeSupervisor({
        commands,
        projects: [],
        launches,
        listRefs: (input) =>
          Effect.sync(() => {
            lookups.push(input);
            return refsResult([]);
          }),
      });
      yield* startThreadTurn({
        ...automaticWorktreeTurn,
        serverResolvesWorktreeBase: false,
        bootstrap: {
          ...automaticWorktreeTurn.bootstrap,
          prepareWorktree: {
            ...automaticWorktreeTurn.bootstrap.prepareWorktree,
            baseBranch: "upstream/release",
          },
        },
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));
      yield* startThreadTurn({
        ...automaticWorktreeTurn,
        serverResolvesWorktreeBase: false,
        bootstrap: { createThread: automaticWorktreeTurn.bootstrap.createThread },
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));
      expect(lookups).toEqual([]);
      expect(launches).toEqual([
        withBaseRef("upstream/release"),
        { ...automaticWorktreeLaunch, workspaceStrategy: { type: "root" } },
      ]);
      expect(commands).toEqual([]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("uses the current local branch when an older server reports no default", () =>
    Effect.gen(function* () {
      const launches: OrchestrationV2ThreadLaunchInput[] = [];
      const supervisor = yield* makeSupervisor({
        commands: [],
        projects: [],
        launches,
        listRefs: () =>
          Effect.succeed(
            refsResult([
              { name: "feature/local", current: true, isDefault: false, worktreePath: null },
            ]),
          ),
      });
      yield* startThreadTurn({ ...automaticWorktreeTurn, serverResolvesWorktreeBase: false }).pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
      );
      expect(launches).toEqual([withBaseRef("feature/local")]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("fails clearly without launching when no valid automatic base exists", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const launches: OrchestrationV2ThreadLaunchInput[] = [];
      for (const isRepo of [true, false]) {
        const supervisor = yield* makeSupervisor({
          commands,
          projects: [],
          launches,
          listRefs: () => Effect.succeed(refsResult([], isRepo)),
        });
        const failure = yield* startThreadTurn(automaticWorktreeTurn).pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          Effect.flip,
        );
        expect(failure).toBeInstanceOf(OrchestrationDispatchCommandError);
        expect(failure.message).toContain("base branch");
      }
      expect(launches).toEqual([]);
      expect(commands).toEqual([]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect("propagates lookup failure without launching or retrying", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const launches: OrchestrationV2ThreadLaunchInput[] = [];
      let lookupCount = 0;
      const refsFailure = new GitCommandError({
        operation: "listRefs",
        command: "git",
        cwd: "/workspace/project",
        detail: "Repository unavailable",
      });
      const supervisor = yield* makeSupervisor({
        commands,
        projects: [],
        launches,
        listRefs: () =>
          Effect.suspend(() => {
            lookupCount += 1;
            return Effect.fail(refsFailure);
          }),
      });
      const failure = yield* startThreadTurn(automaticWorktreeTurn).pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.flip,
      );
      expect(failure).toBe(refsFailure);
      expect(lookupCount).toBe(1);
      expect(launches).toEqual([]);
      expect(commands).toEqual([]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );
});
