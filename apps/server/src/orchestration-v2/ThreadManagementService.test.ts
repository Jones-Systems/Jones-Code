import { expect, it } from "@effect/vitest";
import {
  CommandId,
  ChatAttachmentId,
  type AuthEnvironmentScope,
  AuthSessionId,
  EnvironmentAuthenticatedPrincipal,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2Command,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ScheduledTaskId,
  ThreadId,
  type ThreadTurnDispatchGuardV2,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import { DispatchGuardRejectedError } from "./DispatchGuard.ts";
import {
  NormalizationWitnessCarrier,
  type NormalizationWitnessPreparation,
} from "./NormalizationWitness.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "./Orchestrator.ts";
import { ProviderOperatingCountsError } from "./ProviderThreadRuntimeObservation.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

it.effect("keeps guarded dispatch and native observation off transcript hydration", () => {
  const threadId = ThreadId.make("thread:thread-management:native-direct");
  const commandId = CommandId.make("command:thread-management:native-direct");
  const messageId = MessageId.make("message:thread-management:native-direct");
  const dispatchError = new Orchestrator.OrchestratorDispatchError({
    commandId,
    commandType: "message.dispatch",
  });
  const observationError = new Orchestrator.OrchestratorProjectionError({ threadId });
  const countsError = new ProviderOperatingCountsError({
    cause: "Current runtime counts unavailable",
  });
  const attachment = {
    status: "unknown" as const,
    reason: "registered binding unavailable",
    observedAt: "2026-10-03T00:00:00Z",
  };
  const runtime = { status: "unknown" as const, reason: "probe coverage unavailable" };
  let hydrations = 0;
  const testLayer = ThreadManagementService.layerWithLegacyImporter.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(Orchestrator.OrchestratorV2)({
          dispatchGuarded: () => Effect.fail(dispatchError),
          observeCommand: () => Effect.fail(observationError),
          observeCurrentThreadRuntimeStop: () => Effect.fail(observationError),
          readCurrentThreadRuntimeAttachment: () => Effect.succeed(attachment),
          observeCurrentThreadRuntime: () => Effect.succeed(runtime),
          getOperatingCounts: () => Effect.fail(countsError),
        }),
        Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
          ensureTranscript: () =>
            Effect.sync(() => {
              hydrations += 1;
              return { importedThreadCount: 0, importedMessageCount: 0 };
            }),
        }),
      ),
    ),
  );
  return Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService;
    const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.1-codex" };
    const guard: ThreadTurnDispatchGuardV2 = {
      version: 2,
      observedSnapshotSequence: 1,
      expectedIncarnation: { eventId: EventId.make("birth:native-direct"), sequence: 1 },
      expectedModelSelection: modelSelection,
      expectedActiveRunId: null,
      expectedLatestRunId: null,
      expectedActiveRunAttemptId: null,
      expectedActiveProviderThreadId: null,
      expectedProviderSessionId: null,
      expectedProviderSessionStatus: null,
      requireIdle: true,
    };
    const rejected = yield* service
      .dispatchGuarded(
        {
          type: "message.dispatch",
          commandId,
          threadId,
          messageId,
          text: "Guarded",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        },
        guard,
      )
      .pipe(Effect.flip);
    expect(rejected).toBe(dispatchError);
    expect(
      yield* service.observeCommand({ threadId, commandId, messageId }).pipe(Effect.flip),
    ).toBe(observationError);
    expect(
      yield* service.observeCurrentThreadRuntimeStop({ threadId, commandId }).pipe(Effect.flip),
    ).toBe(observationError);
    expect(yield* service.readCurrentThreadRuntimeAttachment(threadId)).toBe(attachment);
    expect(yield* service.observeCurrentThreadRuntime(threadId)).toBe(runtime);
    expect(yield* service.getOperatingCounts().pipe(Effect.flip)).toBe(countsError);
    expect(hydrations).toBe(0);
  }).pipe(
    Effect.provide(testLayer),
    Effect.provideService(EnvironmentAuthenticatedPrincipal, {
      sessionId: AuthSessionId.make("session:native-direct"),
      subject: "user:synthetic",
      method: "browser-session-cookie",
      scopes: new Set<AuthEnvironmentScope>(["orchestration:operate"]),
    }),
  );
});

it("stamps authoritative provenance on commands that create threads or messages", () => {
  const command: OrchestrationV2Command = {
    type: "thread.create",
    createdBy: "agent",
    creationSource: "mcp",
    commandId: CommandId.make("command:thread-management:create"),
    threadId: ThreadId.make("thread:thread-management:create"),
    projectId: ProjectId.make("project:thread-management"),
    title: "Thread management",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5-codex",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
  };

  expect(
    ThreadManagementService.withCreationProvenance(command, {
      createdBy: "user",
      creationSource: "web",
    }),
  ).toMatchObject({
    createdBy: "user",
    creationSource: "web",
  });
});

it("leaves commands that do not create durable authored content unchanged", () => {
  const command: OrchestrationV2Command = {
    type: "run.interrupt",
    commandId: CommandId.make("command:thread-management:interrupt"),
    threadId: ThreadId.make("thread:thread-management:interrupt"),
    runId: RunId.make("run:thread-management:interrupt"),
  };

  expect(
    ThreadManagementService.withCreationProvenance(command, {
      createdBy: "user",
      creationSource: "web",
    }),
  ).toBe(command);
});

it("identifies every existing thread that must be hydrated before dispatch", () => {
  const sourceThreadId = ThreadId.make("thread:thread-management:source");
  const targetThreadId = ThreadId.make("thread:thread-management:target");
  const parentThreadId = ThreadId.make("thread:thread-management:parent");

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("command:thread-management:create"),
      threadId: targetThreadId,
      projectId: ProjectId.make("project:thread-management"),
      title: "Created thread",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    }),
  ).toEqual([]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.archive",
      commandId: CommandId.make("command:thread-management:archive"),
      threadId: targetThreadId,
    }),
  ).toEqual([targetThreadId]);

  // Read-state commands skip transcript hydration entirely: they fire on
  // every activity bump while a thread is open and never touch messages.
  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.visit",
      commandId: CommandId.make("command:thread-management:visit"),
      threadId: targetThreadId,
      visitedAt: "2026-07-30T00:00:00.000Z",
    }),
  ).toEqual([]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.mark-unread",
      commandId: CommandId.make("command:thread-management:mark-unread"),
      threadId: targetThreadId,
    }),
  ).toEqual([]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.fork",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("command:thread-management:fork"),
      sourceThreadId,
      targetThreadId,
      sourcePoint: {
        type: "run",
        runId: RunId.make("run:thread-management:source"),
      },
    }),
  ).toEqual([sourceThreadId]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.merge_back",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("command:thread-management:merge"),
      sourceThreadId,
      targetThreadId,
      sourcePoint: {
        type: "run",
        runId: RunId.make("run:thread-management:source"),
      },
    }),
  ).toEqual([sourceThreadId, targetThreadId]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "delegated_task.request",
      createdBy: "agent",
      creationSource: "provider",
      commandId: CommandId.make("command:thread-management:delegate"),
      parentThreadId,
      parentRunId: RunId.make("run:thread-management:parent"),
      parentNodeId: NodeId.make("node:thread-management:parent"),
      task: "Inspect the migration",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      runtimeMode: "full-access",
      interactionMode: "default",
    }),
  ).toEqual([parentThreadId]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "delegated_task.wake-policy",
      commandId: CommandId.make("command:thread-management:wake-policy"),
      parentThreadId,
      taskId: NodeId.make("node:thread-management:delegated"),
      completionWake: "always",
    }),
  ).toEqual([parentThreadId]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.created.record",
      commandId: CommandId.make("command:thread-management:record"),
      parentThreadId,
      parentRunId: RunId.make("run:thread-management:parent"),
      parentNodeId: NodeId.make("node:thread-management:parent"),
      targetThreadId,
      targetRunId: null,
    }),
  ).toEqual([parentThreadId, targetThreadId]);
});

it("derives thread management messages from structural error attributes", () => {
  const projectId = ProjectId.make("project:thread-management:errors");
  const threadId = ThreadId.make("thread:thread-management:errors");
  const runId = RunId.make("run:thread-management:errors");
  const messageId = MessageId.make("message:thread-management:errors");
  const infrastructureCause = new Error("private sqlite detail");

  const threadNotFound = new ThreadManagementService.ThreadManagementThreadNotFoundError({
    projectId,
    threadId,
  });
  expect(threadNotFound).toMatchObject({ projectId, threadId });
  expect(threadNotFound.message).toBe(`Thread ${threadId} was not found in project ${projectId}.`);

  const runNotFound = new ThreadManagementService.ThreadManagementRunNotFoundError({
    threadId,
    runId,
  });
  expect(runNotFound).toMatchObject({ threadId, runId });
  expect(runNotFound.message).toBe(`Run ${runId} does not belong to thread ${threadId}.`);

  const archived = new ThreadManagementService.ThreadManagementThreadArchivedError({
    threadId,
  });
  expect(archived).toMatchObject({ threadId });
  expect(archived.message).toBe(`Thread ${threadId} is archived and cannot receive messages.`);

  const notSteerable = new ThreadManagementService.ThreadManagementNoSteerableRunError({
    threadId,
    mode: "restart",
  });
  expect(notSteerable).toMatchObject({
    threadId,
    mode: "restart",
  });
  expect(notSteerable.message).toBe(
    `Thread ${threadId} has no running turn that can be restarted.`,
  );

  const notInterruptible = new ThreadManagementService.ThreadManagementThreadNotInterruptibleError({
    threadId,
    runId,
  });
  expect(notInterruptible).toMatchObject({ threadId, runId });
  expect(notInterruptible.message).toBe(`Run ${runId} is not currently interruptible.`);

  const listFailure = new ThreadManagementService.ThreadManagementProjectThreadsListError({
    projectId,
    cause: infrastructureCause,
  });
  expect(listFailure).toMatchObject({ projectId, cause: infrastructureCause });
  expect(listFailure.message).toBe(`Unable to list threads in project ${projectId}.`);
  expect(listFailure.message).not.toContain(infrastructureCause.message);

  const durableProjectionFailure =
    new ThreadManagementService.ThreadManagementDurableRunProjectionError({
      threadId,
      messageId,
    });
  expect(durableProjectionFailure).toMatchObject({ threadId, messageId });
  expect(durableProjectionFailure.message).toBe(
    `Message ${messageId} was accepted on thread ${threadId} without a durable run projection.`,
  );
});

it.effect("classifies projection infrastructure failures separately from a missing thread", () => {
  const projectId = ProjectId.make("project:thread-management:projection-failure");
  const threadId = ThreadId.make("thread:thread-management:projection-failure");
  const infrastructureCause = new Error("sqlite read failed");
  const projectionError = new Orchestrator.OrchestratorProjectionError({
    threadId,
    cause: infrastructureCause,
  });
  const testLayer = ThreadManagementService.layer.pipe(
    Layer.provide(
      Layer.mock(Orchestrator.OrchestratorV2)({
        getThreadProjection: () => Effect.fail(projectionError),
      }),
    ),
  );

  return Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService;
    const error = yield* Effect.flip(service.getProjectThread({ projectId, threadId }));

    expect(error).toBeInstanceOf(ThreadManagementService.ThreadManagementProjectionLoadError);
    expect(error).toMatchObject({
      projectId,
      threadId,
      cause: projectionError,
    });
    expect(error.message).toBe(`Unable to load thread ${threadId} in project ${projectId}.`);
  }).pipe(Effect.provide(testLayer));
});

it.effect("uses thread-not-found only after a projection loads outside the project", () => {
  const projectId = ProjectId.make("project:thread-management:requested");
  const otherProjectId = ProjectId.make("project:thread-management:other");
  const threadId = ThreadId.make("thread:thread-management:wrong-project");
  const projection = {
    thread: {
      id: threadId,
      projectId: otherProjectId,
      deletedAt: null,
    },
  } as OrchestrationV2ThreadProjection;
  const testLayer = ThreadManagementService.layer.pipe(
    Layer.provide(
      Layer.mock(Orchestrator.OrchestratorV2)({
        getThreadProjection: () => Effect.succeed(projection),
      }),
    ),
  );

  return Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService;
    const error = yield* Effect.flip(service.getProjectThread({ projectId, threadId }));

    expect(error).toBeInstanceOf(ThreadManagementService.ThreadManagementThreadNotFoundError);
    expect(error).toMatchObject({ projectId, threadId });
    expect("cause" in error).toBe(false);
  }).pipe(Effect.provide(testLayer));
});

it.effect("preserves failed legacy materialization when reading checkpoint context", () => {
  const threadId = ThreadId.make("thread:thread-management:checkpoint-import-failure");
  const importError = new LegacyV1ThreadImporter.LegacyV1ThreadImportError({
    threadId,
    operation: "hydrate transcript for",
    cause: new Error("checkpoint import failed"),
  });
  const testLayer = ThreadManagementService.layerWithLegacyImporter.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getCheckpointContext: () =>
            Effect.succeed({ runs: [], checkpointScopes: [], checkpoints: [] }),
        }),
        Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
          ensureTranscript: () => Effect.fail(importError),
        }),
      ),
    ),
  );
  return Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService;
    const error = yield* service.getCheckpointContext(threadId).pipe(Effect.flip);
    expect(error).toBeInstanceOf(Orchestrator.OrchestratorProjectionError);
    expect(error).toMatchObject({ threadId, cause: importError });
  }).pipe(Effect.provide(testLayer));
});

for (const scenario of [
  { finalStatus: "completed" as const, timedOut: false },
  { finalStatus: "failed" as const, timedOut: false },
  { finalStatus: "cancelled" as const, timedOut: false },
  { finalStatus: "interrupted" as const, timedOut: false },
  { finalStatus: "rolled_back" as const, timedOut: false },
  { finalStatus: "running" as const, timedOut: true },
  { finalStatus: "missing" as const },
]) {
  it.effect(`waitForThread timeout final read when selected run is ${scenario.finalStatus}`, () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("project:thread-management:wait-timeout");
      const threadId = ThreadId.make("thread:thread-management:wait-timeout");
      const runId = RunId.make("run:thread-management:wait-timeout");
      const loopRead = yield* Deferred.make<void>();
      let reads = 0;
      const projection = (status: OrchestrationV2Run["status"] | "missing") =>
        ({
          thread: { id: threadId, projectId, deletedAt: null },
          runs: status === "missing" ? [] : [{ id: runId, status }],
        }) as unknown as OrchestrationV2ThreadProjection;
      const testLayer = ThreadManagementService.layer.pipe(
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadRecords: () =>
              Effect.gen(function* () {
                reads += 1;
                if (reads === 1) {
                  return projection("running");
                }
                if (reads === 2) {
                  // Park inside the wait loop so the timeout path runs while a
                  // final projection read can still observe a terminal run.
                  yield* Deferred.succeed(loopRead, undefined);
                  return yield* Effect.never;
                }
                return projection(scenario.finalStatus);
              }),
          }),
        ),
      );
      const service = yield* ThreadManagementService.ThreadManagementService.pipe(
        Effect.provide(testLayer),
      );
      const fiber = yield* service
        .waitForThread({
          projectId,
          threadId,
          runId,
          timeoutMs: 1,
        })
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(loopRead);
      yield* TestClock.adjust(Duration.millis(1));
      const result = yield* Fiber.join(fiber);

      if (scenario.finalStatus === "missing") {
        expect(result._tag).toBe("Failure");
        expect(result).toMatchObject({
          failure: expect.any(ThreadManagementService.ThreadManagementRunNotFoundError),
        });
        expect(result).toMatchObject({
          failure: { threadId, runId },
        });
      } else {
        expect(result._tag).toBe("Success");
        expect(result).toMatchObject({
          success: {
            threadId,
            timedOut: scenario.timedOut,
            run: { id: runId, status: scenario.finalStatus },
          },
        });
      }
    }),
  );
}

const normalizationSendInput: ThreadManagementService.ThreadManagementSendInput = {
  projectId: ProjectId.make("project:normalization-send"),
  commandId: CommandId.make("command:normalization-send"),
  threadId: ThreadId.make("thread:normalization-send"),
  messageId: MessageId.make("message:normalization-send"),
  scheduledTaskId: ScheduledTaskId.make("task:normalization-send"),
  senderThreadId: ThreadId.make("thread:normalization-sender"),
  text: "Normalized message",
  attachments: [],
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.1-codex" },
  mode: "auto",
  createdBy: "user",
  creationSource: "web",
};
const normalizationRunId = RunId.make("run:normalization-send");
const normalizationPreparation: NormalizationWitnessPreparation = {
  commandId: normalizationSendInput.commandId,
  requestDigest: "a".repeat(64),
  attachments: [],
  contextRemaps: [],
  mode: "fresh",
};
const normalizationAcceptedCommand: Extract<OrchestrationV2Command, { type: "message.dispatch" }> =
  {
    type: "message.dispatch",
    commandId: normalizationSendInput.commandId,
    threadId: normalizationSendInput.threadId,
    messageId: normalizationSendInput.messageId,
    scheduledTaskId: normalizationSendInput.scheduledTaskId,
    senderThreadId: normalizationSendInput.senderThreadId,
    text: normalizationSendInput.text,
    attachments: normalizationSendInput.attachments,
    modelSelection: normalizationSendInput.modelSelection,
    dispatchMode: { type: "restart_active", targetRunId: normalizationRunId },
    createdBy: normalizationSendInput.createdBy,
    creationSource: normalizationSendInput.creationSource,
  };
const normalizationReplay: ThreadManagementService.ThreadManagementSendNormalization = {
  preparation: { ...normalizationPreparation, mode: "replay" },
  acceptedCommand: normalizationAcceptedCommand,
};

const makeNormalizationSendHarness = (
  options: {
    readonly steerable?: boolean;
    readonly thread?: Partial<OrchestrationV2ThreadProjection["thread"]>;
    readonly durable?: boolean;
    readonly queued?: boolean;
  } = {},
) => {
  const observed: Array<{ readonly stage: string; readonly carrier: unknown }> = [];
  const commands: OrchestrationV2ServerCommand[] = [];
  const thread = {
    id: normalizationSendInput.threadId,
    projectId: normalizationSendInput.projectId,
    archivedAt: null,
    deletedAt: null,
    ...options.thread,
  };
  const initial = {
    thread,
    runs: [
      {
        id: normalizationRunId,
        ordinal: 1,
        status: options.steerable ? "running" : "completed",
        activeAttemptId: "attempt:normalization",
      },
    ],
    providerTurns: options.steerable
      ? [{ runAttemptId: "attempt:normalization", status: "running" }]
      : [],
  } as unknown as OrchestrationV2ThreadProjection;
  const durable = {
    ...initial,
    runs: [{ id: normalizationRunId, ordinal: 1, status: options.queued ? "queued" : "completed" }],
    messages:
      options.durable === false
        ? []
        : [{ id: normalizationSendInput.messageId, runId: normalizationRunId }],
    turnItems: options.queued
      ? []
      : [
          {
            type: "user_message",
            messageId: normalizationSendInput.messageId,
            inputIntent: "steer",
          },
        ],
  } as unknown as OrchestrationV2ThreadProjection;
  const layer = ThreadManagementService.layer.pipe(
    Layer.provide(
      Layer.mock(Orchestrator.OrchestratorV2)({
        getThreadRecords: (_threadId, fields) =>
          Effect.gen(function* () {
            const isDurableRead = fields.some((field) => field === "messages");
            observed.push({
              stage: isDurableRead ? "durable" : "target",
              carrier: yield* NormalizationWitnessCarrier,
            });
            return isDurableRead ? durable : initial;
          }),
        dispatch: (command) =>
          Effect.gen(function* () {
            commands.push(command);
            observed.push({ stage: "dispatch", carrier: yield* NormalizationWitnessCarrier });
            return { sequence: 7, storedEvents: [] };
          }),
      }),
    ),
  );
  return { layer, observed, commands, durable };
};

for (const mode of ["auto", "queue", "steer", "restart"] as const) {
  it.effect(`forwards fresh normalization only during ${mode} send dispatch`, () => {
    const harness = makeNormalizationSendHarness({ steerable: true, queued: mode === "queue" });
    return Effect.gen(function* () {
      const service = yield* ThreadManagementService.ThreadManagementService;
      const result = yield* service.sendToThread(
        { ...normalizationSendInput, mode },
        {
          preparation: normalizationPreparation,
        },
      );
      expect(harness.commands).toHaveLength(1);
      const command = harness.commands[0];
      expect(command).toMatchObject({
        dispatchMode:
          mode === "queue"
            ? { type: "queue_after_active" }
            : {
                type: mode === "restart" ? "restart_active" : "steer_active",
                targetRunId: normalizationRunId,
              },
      });
      expect(harness.observed).toEqual([
        { stage: "target", carrier: undefined },
        { stage: "dispatch", carrier: { ...normalizationPreparation, acceptedCommand: command } },
        { stage: "durable", carrier: undefined },
      ]);
      expect(result.dispatch.sequence).toBe(7);
      expect(result.message).toBe(harness.durable.messages[0]);
      expect(result.run).toBe(harness.durable.runs[0]);
      expect(result.delivery).toBe(
        mode === "queue" ? "queued" : mode === "restart" ? "restarted" : "steered",
      );
      expect(yield* NormalizationWitnessCarrier).toBeUndefined();
    }).pipe(Effect.provide(harness.layer));
  });
}

it.effect(
  "replays the accepted send mode after the steerable run ends and still reads durable results",
  () => {
    const harness = makeNormalizationSendHarness();
    return Effect.gen(function* () {
      const service = yield* ThreadManagementService.ThreadManagementService;
      const result = yield* service.sendToThread(
        { ...normalizationSendInput, mode: "restart" },
        normalizationReplay,
      );
      expect(harness.commands).toEqual([normalizationAcceptedCommand]);
      expect(harness.commands[0]).toBe(normalizationAcceptedCommand);
      expect(harness.observed).toEqual([
        { stage: "target", carrier: undefined },
        {
          stage: "dispatch",
          carrier: {
            ...normalizationReplay.preparation,
            acceptedCommand: normalizationAcceptedCommand,
          },
        },
        { stage: "durable", carrier: undefined },
      ]);
      expect(result.message).toBe(harness.durable.messages[0]);
      expect(result.delivery).toBe("restarted");
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("rejects normalization identity mismatches before dispatch", () => {
  const harness = makeNormalizationSendHarness();
  return Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService;
    const cases: ReadonlyArray<ThreadManagementService.ThreadManagementSendNormalization> = [
      { preparation: { ...normalizationPreparation, commandId: CommandId.make("other-command") } },
      { preparation: normalizationPreparation, acceptedCommand: normalizationAcceptedCommand },
      { preparation: { ...normalizationPreparation, mode: "replay" } },
      ...[
        { commandId: CommandId.make("other-command") },
        { threadId: ThreadId.make("other-thread") },
        { messageId: MessageId.make("other-message") },
        { scheduledTaskId: undefined },
        { senderThreadId: undefined },
        { text: "Changed message" },
        {
          attachments: [
            {
              type: "image" as const,
              id: ChatAttachmentId.make("changed-image"),
              name: "image.png",
              mimeType: "image/png",
              sizeBytes: 1,
            },
          ],
        },
        { modelSelection: undefined },
        { createdBy: "agent" as const },
        { creationSource: "mcp" as const },
      ].map((changed) => ({
        preparation: normalizationReplay.preparation,
        acceptedCommand: { ...normalizationAcceptedCommand, ...changed },
      })),
    ];
    for (const normalization of cases) {
      const failure = yield* service
        .sendToThread(normalizationSendInput, normalization)
        .pipe(Effect.flip);
      expect(failure).toBeInstanceOf(DispatchGuardRejectedError);
      expect(failure).toMatchObject({ reason: "identity_conflict" });
    }
    expect(harness.commands).toEqual([]);
    expect(
      harness.observed.every((item) => item.stage === "target" && item.carrier === undefined),
    ).toBe(true);
  }).pipe(Effect.provide(harness.layer));
});

for (const guard of ["project", "deleted", "archive"] as const) {
  it.effect(`preserves the ${guard} guard before normalized replay`, () => {
    const harness = makeNormalizationSendHarness({
      thread:
        guard === "project"
          ? { projectId: ProjectId.make("different-project") }
          : guard === "deleted"
            ? { deletedAt: DateTime.makeUnsafe("2026-10-01T00:00:00Z") }
            : { archivedAt: DateTime.makeUnsafe("2026-10-01T00:00:00Z") },
    });
    return Effect.gen(function* () {
      const service = yield* ThreadManagementService.ThreadManagementService;
      const failure = yield* service
        .sendToThread(normalizationSendInput, normalizationReplay)
        .pipe(Effect.flip);
      expect(failure).toBeInstanceOf(
        guard === "archive"
          ? ThreadManagementService.ThreadManagementThreadArchivedError
          : ThreadManagementService.ThreadManagementThreadNotFoundError,
      );
      expect(harness.commands).toEqual([]);
    }).pipe(Effect.provide(harness.layer));
  });
}

it.effect("does not report replay success without its durable message", () => {
  const harness = makeNormalizationSendHarness({ durable: false });
  return Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService;
    const failure = yield* service
      .sendToThread(normalizationSendInput, normalizationReplay)
      .pipe(Effect.flip);
    expect(failure).toBeInstanceOf(
      ThreadManagementService.ThreadManagementDurableRunProjectionError,
    );
    expect(harness.commands).toEqual([normalizationAcceptedCommand]);
    expect(harness.observed.map((item) => item.stage)).toEqual(["target", "dispatch", "durable"]);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("clears an ambient normalization carrier only for an ordinary send dispatch", () => {
  const harness = makeNormalizationSendHarness();
  const ambient = {
    ...normalizationReplay.preparation,
    acceptedCommand: normalizationAcceptedCommand,
  };
  return Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService;
    yield* service.sendToThread(normalizationSendInput);
    expect(harness.commands[0]).toMatchObject({ dispatchMode: { type: "start_immediately" } });
    expect(harness.observed).toEqual([
      { stage: "target", carrier: ambient },
      { stage: "dispatch", carrier: undefined },
      { stage: "durable", carrier: ambient },
    ]);
    expect(yield* NormalizationWitnessCarrier).toBe(ambient);
  }).pipe(
    Effect.provide(harness.layer),
    Effect.provideService(NormalizationWitnessCarrier, ambient),
  );
});
