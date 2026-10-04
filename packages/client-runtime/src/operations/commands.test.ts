import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  CommandId,
  ComposerContextId,
  EnvironmentId,
  MessageId,
  NodeId,
  OrchestrationV2ImportedHistoryReviewBasis,
  ORCHESTRATION_V2_WS_METHODS,
  PlanId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  WS_METHODS,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadLaunchInput,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ImportedHistoryDelivery,
  type OrchestrationV2ImportedHistoryReviewResult,
  type OrchestrationV2ImportedHistoryStartReceipt,
  type OrchestrationV2ObserveImportedHistoryStartInput,
  type OrchestrationV2ReviewImportedHistoryStartInput,
  type OrchestrationV2StartWithImportedHistoryCommand,
  type OrchestrationV2StopCurrentThreadRuntimeInput,
  type OrchestrationV2ObserveCurrentThreadRuntimeStopInput,
  type OrchestrationV2ObserveThreadDeletionCleanupInput,
  type OrchestrationV2ThreadDeletionCleanupObservation,
  type OrchestrationV2StopCurrentThreadRuntimeResult,
  type OrchestrationV2GetThreadRuntimeObservationInput,
  type OrchestrationV2ThreadRuntimeObservationResult,
  type OrchestrationV2GetOperatingCountsInput,
  type OrchestrationV2OperatingCountsResult,
  type ChatAttachment,
  type PersistChatAttachmentsInput,
  type ProjectMutation,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
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
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import { v2Now, v2Projection, v2ThreadId } from "../state/orchestrationV2TestFixtures.ts";
import {
  archiveThread,
  deleteThread,
  observeThreadDeletionCleanup,
  deliverImportedContinuation,
  prepareImportedContinuation,
  reviewImportedHistoryStart,
  observeImportedHistoryStart,
  stopCurrentThreadRuntime,
  observeCurrentThreadRuntimeStop,
  getThreadRuntimeObservation,
  getOperatingCounts,
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
  readonly commands: OrchestrationV2Command[];
  readonly projects: ProjectMutation[];
  readonly launches?: OrchestrationV2ThreadLaunchInput[];
  readonly projection?: OrchestrationV2ThreadProjection;
  readonly projectionRequests?: ThreadId[];
  readonly advertiseServerResolvedCommandContext?: boolean;
  readonly importedReviews?: OrchestrationV2ReviewImportedHistoryStartInput[];
  readonly importedStarts?: OrchestrationV2StartWithImportedHistoryCommand[];
  readonly importedObservations?: OrchestrationV2ObserveImportedHistoryStartInput[];
  readonly importedReview?: OrchestrationV2ImportedHistoryReviewResult;
  readonly importedReceipt?: OrchestrationV2ImportedHistoryStartReceipt;
  readonly loseImportedStartResponse?: boolean;
  readonly attachmentRequests?: PersistChatAttachmentsInput[];
  readonly persistedAttachments?: ReadonlyArray<ChatAttachment>;
  readonly runtimeStops?: OrchestrationV2StopCurrentThreadRuntimeInput[];
  readonly runtimeStopObservations?: OrchestrationV2ObserveCurrentThreadRuntimeStopInput[];
  readonly runtimeStopResult?: OrchestrationV2StopCurrentThreadRuntimeResult;
  readonly loseRuntimeStopResponse?: boolean;
  readonly runtimeObservationRequests?: OrchestrationV2GetThreadRuntimeObservationInput[];
  readonly runtimeObservation?: OrchestrationV2ThreadRuntimeObservationResult;
  readonly operatingCountRequests?: OrchestrationV2GetOperatingCountsInput[];
  readonly operatingCounts?: OrchestrationV2OperatingCountsResult;
  readonly failOperatingCounts?: boolean;
  readonly deletionObservations?: OrchestrationV2ObserveThreadDeletionCleanupInput[];
  readonly deletionObservation?: OrchestrationV2ThreadDeletionCleanupObservation;
  readonly loseDeleteResponse?: boolean;
}) {
  const client = {
    [ORCHESTRATION_V2_WS_METHODS.observeThreadDeletionCleanup]: (
      observation: OrchestrationV2ObserveThreadDeletionCleanupInput,
    ) =>
      Effect.sync(() => {
        input.deletionObservations?.push(observation);
        if (input.deletionObservation === undefined)
          throw new Error("Unexpected deletion observation.");
        return input.deletionObservation;
      }),
    [ORCHESTRATION_V2_WS_METHODS.getThreadRuntimeObservation]: (
      observationInput: OrchestrationV2GetThreadRuntimeObservationInput,
    ) =>
      Effect.sync(() => {
        input.runtimeObservationRequests?.push(observationInput);
        if (input.runtimeObservation === undefined)
          throw new Error("Unexpected runtime observation.");
        return input.runtimeObservation;
      }),
    [ORCHESTRATION_V2_WS_METHODS.getOperatingCounts]: (
      countsInput: OrchestrationV2GetOperatingCountsInput,
    ) =>
      Effect.sync(() => {
        input.operatingCountRequests?.push(countsInput);
        if (input.failOperatingCounts) throw new Error("Operating counts manager unavailable.");
        if (input.operatingCounts === undefined)
          throw new Error("Unexpected Operating counts read.");
        return input.operatingCounts;
      }),
    [ORCHESTRATION_V2_WS_METHODS.stopCurrentThreadRuntime]: (
      stopInput: OrchestrationV2StopCurrentThreadRuntimeInput,
    ) =>
      Effect.sync(() => {
        input.runtimeStops?.push(stopInput);
        if (input.loseRuntimeStopResponse) throw new Error("Stop response was lost.");
        if (input.runtimeStopResult === undefined)
          throw new Error("Unexpected current runtime stop.");
        return input.runtimeStopResult;
      }),
    [ORCHESTRATION_V2_WS_METHODS.observeCurrentThreadRuntimeStop]: (
      observation: OrchestrationV2ObserveCurrentThreadRuntimeStopInput,
    ) =>
      Effect.sync(() => {
        input.runtimeStopObservations?.push(observation);
        if (input.runtimeStopResult === undefined)
          throw new Error("Unexpected runtime stop observation.");
        return input.runtimeStopResult;
      }),
    [ORCHESTRATION_V2_WS_METHODS.reviewImportedHistoryStart]: (
      reviewInput: OrchestrationV2ReviewImportedHistoryStartInput,
    ) =>
      Effect.sync(() => {
        input.importedReviews?.push(reviewInput);
        if (input.importedReview === undefined) throw new Error("Unexpected imported review.");
        return input.importedReview;
      }),
    [ORCHESTRATION_V2_WS_METHODS.startWithImportedHistory]: (
      command: OrchestrationV2StartWithImportedHistoryCommand,
    ) =>
      Effect.sync(() => {
        input.importedStarts?.push(command);
        if (input.loseImportedStartResponse) throw new Error("Start response was lost.");
        if (input.importedReceipt === undefined) throw new Error("Unexpected imported start.");
        return input.importedReceipt;
      }),
    [ORCHESTRATION_V2_WS_METHODS.observeImportedHistoryStart]: (
      observation: OrchestrationV2ObserveImportedHistoryStartInput,
    ) =>
      Effect.sync(() => {
        input.importedObservations?.push(observation);
        if (input.importedReceipt === undefined)
          throw new Error("Unexpected imported observation.");
        return input.importedReceipt;
      }),
    [WS_METHODS.assetsPersistChatAttachments]: (attachmentInput: PersistChatAttachmentsInput) =>
      Effect.sync(() => {
        input.attachmentRequests?.push(attachmentInput);
        return { attachments: input.persistedAttachments ?? [] };
      }),
    [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: (command: OrchestrationV2Command) =>
      Effect.sync(() => {
        input.commands.push(command);
        if (input.loseDeleteResponse && command.type === "thread.delete")
          throw new Error("Delete response was lost.");
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
      Effect.sync(() => {
        input.launches?.push(launchInput);
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

const IMPORTED_MESSAGE = MessageId.make("reviewed-message");
const IMPORTED_BASIS = OrchestrationV2ImportedHistoryReviewBasis.make("reviewed-imported-basis");
const IMPORTED_COMMAND = CommandId.make("reviewed-imported-command");
const importedReview = (
  delivery: OrchestrationV2ImportedHistoryDelivery,
): OrchestrationV2ImportedHistoryReviewResult => ({
  version: 2,
  threadId: v2ThreadId,
  target:
    delivery.type === "queued_run" ? delivery : { type: "message", messageId: delivery.messageId },
  capability: { startWithImportedHistory: true },
  applicability: "imported",
  qualification: { type: "unsupported", reason: "Use the explicit imported action." },
  restoredBinding: { type: "missing", reason: "No native binding." },
  nativeEffects: { type: "clear" },
  transcriptEligibility: { type: "eligible" },
  reviewedBasis: IMPORTED_BASIS,
});
const importedReceipt = (
  delivery: OrchestrationV2ImportedHistoryDelivery,
): OrchestrationV2ImportedHistoryStartReceipt => ({
  version: 2,
  threadId: v2ThreadId,
  target: importedReview(delivery).target,
  commandId: IMPORTED_COMMAND,
  reviewedBasis: IMPORTED_BASIS,
  intentStatus: "accepted",
  receipt: {
    commandId: IMPORTED_COMMAND,
    threadId: v2ThreadId,
    commandType: "thread.imported-history.start",
    acceptedAt: v2Now,
    resultSequence: 7,
    status: "accepted",
    error: null,
  },
  rejectionReason: null,
  execution: {
    status: "pending",
    runId: delivery.type === "queued_run" ? delivery.runId : null,
    providerThreadId: null,
    providerSessionId: null,
    nativeThreadId: null,
    effectOutcome: null,
    error: null,
  },
});

describe("unified imported continuation commands", () => {
  const deliveries: ReadonlyArray<OrchestrationV2ImportedHistoryDelivery> = [
    {
      type: "message",
      messageId: IMPORTED_MESSAGE,
      text: "Continue this reviewed conversation.",
      attachments: [
        {
          type: "image",
          id: "stored-image",
          name: "image.png",
          mimeType: "image/png",
          sizeBytes: 10,
        },
      ],
      context: { version: 1, records: [] },
      modelSelection: { instanceId: ProviderInstanceId.make("claude"), model: "claude-sonnet-4" },
      runtimeMode: "full-access",
      interactionMode: "default",
      sourcePlanRef: { threadId: v2ThreadId, planId: PlanId.make("reviewed-plan") },
      titleSeed: "Reviewed title",
      dispatchMode: { type: "start_immediately" },
      deliveryIntent: "auto",
    },
    { type: "queued_run", runId: RunId.make("held-run"), messageId: IMPORTED_MESSAGE },
  ];

  for (const delivery of deliveries) {
    it.effect(
      `delivers the reviewed ${delivery.type} through one unified RPC without ordinary dispatch`,
      () =>
        Effect.gen(function* () {
          const commands: OrchestrationV2Command[] = [];
          const importedReviews: OrchestrationV2ReviewImportedHistoryStartInput[] = [];
          const importedStarts: OrchestrationV2StartWithImportedHistoryCommand[] = [];
          const receipt = importedReceipt(delivery);
          const supervisor = yield* makeSupervisor({
            commands,
            projects: [],
            importedReviews,
            importedStarts,
            importedReview: importedReview(delivery),
            importedReceipt: receipt,
          });
          const input = { threadId: v2ThreadId, delivery };
          const prepared = yield* prepareImportedContinuation(input).pipe(
            Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          );
          const review = yield* reviewImportedHistoryStart(prepared).pipe(
            Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          );
          expect(importedStarts).toEqual([]);
          const result = yield* deliverImportedContinuation({
            ...prepared,
            commandId: IMPORTED_COMMAND,
            reviewedBasis: review.reviewedBasis!,
          }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

          expect(importedReviews).toEqual([input]);
          expect(importedStarts).toEqual([
            {
              type: "thread.imported-history.start",
              commandId: IMPORTED_COMMAND,
              reviewedBasis: IMPORTED_BASIS,
              ...input,
            },
          ]);
          expect(commands).toEqual([]);
          expect(result).toBe(receipt);
          if (delivery.type === "queued_run") expect(importedStarts[0]!.delivery).toBe(delivery);
        }),
    );
  }

  it.effect(
    "persists uploads against the reviewed message before review and preserves context correlation",
    () =>
      Effect.gen(function* () {
        const commands: OrchestrationV2Command[] = [];
        const attachmentRequests: PersistChatAttachmentsInput[] = [];
        const stored: ChatAttachment = {
          type: "image",
          id: "already-stored",
          name: "stored.png",
          mimeType: "image/png",
          sizeBytes: 10,
        };
        const persisted: ChatAttachment = {
          type: "image",
          id: "persisted-upload",
          name: "upload.png",
          mimeType: "image/png",
          sizeBytes: 12,
        };
        const upload = {
          type: "image" as const,
          id: "draft-upload",
          name: "upload.png",
          mimeType: "image/png",
          sizeBytes: 12,
          dataUrl: "data:image/png;base64,AQ==",
        };
        const delivery = {
          ...deliveries[0]!,
          type: "message" as const,
          messageId: IMPORTED_MESSAGE,
          text: "Reviewed text",
          runtimeMode: "full-access" as const,
          interactionMode: "default" as const,
          dispatchMode: { type: "start_immediately" as const },
          attachments: [stored, upload],
          context: {
            version: 1 as const,
            records: [
              {
                version: 1 as const,
                kind: "image" as const,
                contextId: ComposerContextId.make("upload-context"),
                label: "Uploaded image",
                attachmentId: "draft-upload",
                name: "upload.png",
                mimeType: "image/png",
                sizeBytes: 12,
              },
            ],
          },
        };
        const supervisor = yield* makeSupervisor({
          commands,
          projects: [],
          attachmentRequests,
          persistedAttachments: [persisted],
        });
        const prepared = yield* prepareImportedContinuation({
          threadId: v2ThreadId,
          delivery,
        }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

        expect(attachmentRequests).toEqual([
          {
            threadId: v2ThreadId,
            messageId: IMPORTED_MESSAGE,
            attachments: [upload],
          },
        ]);
        expect(prepared.delivery).toEqual({
          ...delivery,
          attachments: [stored, persisted],
          context: {
            ...delivery.context,
            records: [{ ...delivery.context.records[0]!, attachmentId: persisted.id }],
          },
        });
        expect(commands).toEqual([]);
      }),
  );

  it.effect(
    "observes the original command after a lost start response without resending or allocating an ID",
    () =>
      Effect.gen(function* () {
        const delivery = deliveries[1]!;
        const commands: OrchestrationV2Command[] = [];
        const importedStarts: OrchestrationV2StartWithImportedHistoryCommand[] = [];
        const importedObservations: OrchestrationV2ObserveImportedHistoryStartInput[] = [];
        const receipt = importedReceipt(delivery);
        const supervisor = yield* makeSupervisor({
          commands,
          projects: [],
          importedStarts,
          importedObservations,
          importedReceipt: receipt,
          loseImportedStartResponse: true,
        });
        const result = yield* Effect.exit(
          deliverImportedContinuation({
            threadId: v2ThreadId,
            commandId: IMPORTED_COMMAND,
            reviewedBasis: IMPORTED_BASIS,
            delivery,
          }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor)),
        );
        expect(result._tag).toBe("Failure");
        const observed = yield* observeImportedHistoryStart({
          threadId: v2ThreadId,
          commandId: IMPORTED_COMMAND,
        }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

        expect(importedStarts).toHaveLength(1);
        expect(importedStarts[0]!.commandId).toBe(IMPORTED_COMMAND);
        expect(importedObservations).toEqual([
          { threadId: v2ThreadId, commandId: IMPORTED_COMMAND },
        ]);
        expect(observed).toBe(receipt);
        expect(commands).toEqual([]);
      }),
  );
});

describe("dedicated current runtime stop commands", () => {
  const input: OrchestrationV2StopCurrentThreadRuntimeInput = {
    commandId: CommandId.make("captured-runtime-stop"),
    threadId: v2ThreadId,
    target: {
      binding: {
        threadId: v2ThreadId,
        providerThreadId: ProviderThreadId.make("current-provider-thread"),
        providerSessionId: ProviderSessionId.make("current-provider-session"),
        instanceId: ProviderInstanceId.make("codex"),
        runtimeGeneration: "current-generation",
        nativeThreadId: "current-native-thread",
      },
      driver: ProviderDriverKind.make("codex"),
      evidenceRevision: 8,
    },
  };
  const result: OrchestrationV2StopCurrentThreadRuntimeResult = {
    version: 2,
    ...input,
    commandStatus: "accepted",
    receipt: {
      commandId: input.commandId,
      threadId: v2ThreadId,
      commandType: ORCHESTRATION_V2_WS_METHODS.stopCurrentThreadRuntime,
      acceptedAt: v2Now,
      resultSequence: 8,
      status: "accepted",
      error: null,
    },
    queueFence: { status: "installed", affectedRunIds: [RunId.make("fenced-queued-run")] },
    runtimeStop: { status: "pending" },
    reason: null,
  };

  it.effect(
    "stops only the captured current tuple through the dedicated RPC without reading historical sessions",
    () =>
      Effect.gen(function* () {
        const commands: OrchestrationV2Command[] = [];
        const projectionRequests: ThreadId[] = [];
        const runtimeStops: OrchestrationV2StopCurrentThreadRuntimeInput[] = [];
        const supervisor = yield* makeSupervisor({
          commands,
          projects: [],
          projectionRequests,
          runtimeStops,
          runtimeStopResult: result,
        });
        const actual = yield* stopCurrentThreadRuntime(input).pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        );

        expect(runtimeStops).toEqual([input]);
        expect(actual).toBe(result);
        expect(actual.runtimeStop.status).toBe("pending");
        expect(commands).toEqual([]);
        expect(projectionRequests).toEqual([]);
      }),
  );

  it.effect(
    "observes the same stop command after a lost response without issuing another stop",
    () =>
      Effect.gen(function* () {
        const commands: OrchestrationV2Command[] = [];
        const runtimeStops: OrchestrationV2StopCurrentThreadRuntimeInput[] = [];
        const runtimeStopObservations: OrchestrationV2ObserveCurrentThreadRuntimeStopInput[] = [];
        const unknownResult: OrchestrationV2StopCurrentThreadRuntimeResult = {
          ...result,
          runtimeStop: { status: "unknown" },
          reason: "Native stop needs reconciliation.",
        };
        const supervisor = yield* makeSupervisor({
          commands,
          projects: [],
          runtimeStops,
          runtimeStopObservations,
          runtimeStopResult: unknownResult,
          loseRuntimeStopResponse: true,
        });
        const lost = yield* Effect.exit(
          stopCurrentThreadRuntime(input).pipe(
            Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          ),
        );
        expect(lost._tag).toBe("Failure");
        const observed = yield* observeCurrentThreadRuntimeStop({
          threadId: v2ThreadId,
          commandId: input.commandId,
        }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

        expect(runtimeStops).toEqual([input]);
        expect(runtimeStopObservations).toEqual([
          { threadId: v2ThreadId, commandId: input.commandId },
        ]);
        expect(observed).toBe(unknownResult);
        expect(commands).toEqual([]);
      }),
  );
});

describe("current runtime observation reads", () => {
  const counts: OrchestrationV2OperatingCountsResult = {
    total: 7,
    operating: 2,
    foregroundWaitingApproval: 1,
    foregroundWaitingInput: 1,
    foregroundWaitingPlan: 1,
    backgroundOperating: 1,
    backgroundUnknown: 1,
    snapshotSequence: 12,
    observedAt: "2026-10-03T02:28:34Z",
    backgroundSampledAt: "2026-10-03T02:28:30Z",
  };

  it.effect(
    "reads runtime and scoped Operating counts through dedicated read ports without hydration or dispatch",
    () =>
      Effect.gen(function* () {
        const commands: OrchestrationV2Command[] = [];
        const projectionRequests: ThreadId[] = [];
        const runtimeObservationRequests: OrchestrationV2GetThreadRuntimeObservationInput[] = [];
        const operatingCountRequests: OrchestrationV2GetOperatingCountsInput[] = [];
        const runtimeObservation: OrchestrationV2ThreadRuntimeObservationResult = {
          threadId: v2ThreadId,
          observation: { status: "unknown", reason: "Current registration is unavailable." },
        };
        const supervisor = yield* makeSupervisor({
          commands,
          projects: [],
          projectionRequests,
          runtimeObservationRequests,
          operatingCountRequests,
          runtimeObservation,
          operatingCounts: counts,
        });
        const observation = yield* getThreadRuntimeObservation({ threadId: v2ThreadId }).pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        );
        const allCounts = yield* getOperatingCounts({}).pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        );
        const scopedCounts = yield* getOperatingCounts({
          projectId: ProjectId.make("selected-project"),
        }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

        expect(observation).toBe(runtimeObservation);
        expect(allCounts).toBe(counts);
        expect(scopedCounts).toBe(counts);
        expect(runtimeObservationRequests).toEqual([{ threadId: v2ThreadId }]);
        expect(operatingCountRequests).toEqual([{}, { projectId: "selected-project" }]);
        expect(commands).toEqual([]);
        expect(projectionRequests).toEqual([]);
      }),
  );

  it.effect("propagates count-read failures without synthesizing healthy zeros", () =>
    Effect.gen(function* () {
      const supervisor = yield* makeSupervisor({
        commands: [],
        projects: [],
        failOperatingCounts: true,
      });
      const result = yield* Effect.exit(
        getOperatingCounts({}).pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        ),
      );
      expect(result._tag).toBe("Failure");
    }),
  );
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

  for (const status of [
    "waiting",
    "completed",
    "failed",
    "interrupted",
    "cancelled",
    "rolled_back",
  ] as const) {
    it.effect(`dispatches Stop for ${status} runs with background commands except rollback`, () =>
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
  }

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

describe("correlated thread deletion cleanup", () => {
  it.effect("forwards exact optional consent with the caller's stable command ID", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const supervisor = yield* makeSupervisor({ commands, projects: [] });
      const commandId = CommandId.make("delete-original");
      const worktreeRemoval = {
        projectId: ProjectId.make("project-1"),
        path: "/worktree/exact",
        branch: "task",
        force: true,
      } as const;
      yield* deleteThread({ threadId: v2ThreadId, commandId, worktreeRemoval }).pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
      );
      yield* deleteThread({ threadId: v2ThreadId, commandId: CommandId.make("delete-only") }).pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
      );
      expect(commands).toEqual([
        { type: "thread.delete", commandId, threadId: v2ThreadId, worktreeRemoval },
        { type: "thread.delete", commandId: "delete-only", threadId: v2ThreadId },
      ]);
    }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );

  it.effect(
    "observes the original delete after response loss without dispatching or hydrating again",
    () =>
      Effect.gen(function* () {
        const commands: OrchestrationV2Command[] = [];
        const deletionObservations: OrchestrationV2ObserveThreadDeletionCleanupInput[] = [];
        const projectionRequests: ThreadId[] = [];
        const commandId = CommandId.make("delete-lost");
        const observation: OrchestrationV2ThreadDeletionCleanupObservation = {
          threadId: v2ThreadId,
          commandId,
          receipt: null,
          deletion: null,
          worktree: null,
          state: "unknown",
          removalOutcome: null,
          currentLease: "unavailable",
          reason: "Readback unavailable",
        };
        const supervisor = yield* makeSupervisor({
          commands,
          projects: [],
          deletionObservations,
          projectionRequests,
          deletionObservation: observation,
          loseDeleteResponse: true,
        });
        const result = yield* Effect.exit(
          deleteThread({ threadId: v2ThreadId, commandId }).pipe(
            Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          ),
        );
        expect(result._tag).toBe("Failure");
        const observed = yield* observeThreadDeletionCleanup({
          threadId: v2ThreadId,
          commandId,
        }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));
        expect(observed).toBe(observation);
        expect(deletionObservations).toEqual([{ threadId: v2ThreadId, commandId }]);
        expect(commands).toHaveLength(1);
        expect(projectionRequests).toEqual([]);
      }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
  );
});
