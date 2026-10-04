import { makeAssistantStreamingFilter } from "./assistantStreaming.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  isOrchestrationV2WorkActive,
  CommandId,
  RunId,
  RunAttemptId,
  ThreadId,
  type EventId,
  type ModelSelection,
  type NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  type ProviderSessionId,
  type ProviderThreadId,
  type ProviderTurnId,
  type TurnItemId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import type {
  ImportedHistoryStartExecutionReferenceV2,
  QueuedRunExecutionIntentV2,
} from "./Orchestrator.ts";
import {
  getNativeCreationExecutionReference,
  type NativeCreationExecutionContextV2,
} from "./NativeCreationAuthority.ts";
import type {
  ProviderAdapterV2Event,
  ProviderAdapterV2RuntimePolicy,
  ProviderAdapterV2SessionRuntime,
  ProviderAdapterV2TurnMessage,
} from "./ProviderAdapter.ts";
import {
  ProviderAdapterTurnStartError,
  withProviderNativeEffect,
  type ProviderNativeOperationContext,
} from "./ProviderAdapter.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import {
  makeProviderFailure,
  makeProviderFailureTurnItem,
  nativeEffectEvidenceFor,
  ProviderNativeOperationUnknownError,
} from "./ProviderFailure.ts";
import {
  copyProviderEventOrigin,
  readProviderEventOrigin,
  type ProviderEventOrigin,
} from "./ProviderEventOrigin.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import type {
  ProviderOrdinaryExecutionAttachmentV1,
  ProviderPinnedRuntimeStopResultV1,
  ProviderSessionActivityError,
} from "./ProviderSessionManager.ts";
import * as ProviderManagedActorCompletion from "./ProviderManagedActorCompletion.ts";

export type OrdinaryManagedRunExecutorV1 = Extract<
  OrdinaryCheckout.OrdinaryCheckoutExecutionExecutorV1,
  { readonly kind: "captured_managed_run" }
>;

export interface OrdinaryManagedRunStartObservationV1 {
  readonly kind: "dispatch_returned";
  readonly settlementMode?: "primary_terminal_checkpoint" | "managed_actor_completion";
  readonly startExecution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
  readonly managedExecutor: OrdinaryManagedRunExecutorV1;
  readonly observedAt: string;
}

type OrdinaryManagedRunRevalidationError =
  | RunExecutionStartError
  | ProviderSessionActivityError
  | ProviderManagedActorCompletion.ProviderManagedActorCompletionError;

const issuedOrdinaryManagedStarts = new WeakMap<
  object,
  {
    readonly bytes: string;
    readonly revalidateIssued: Effect.Effect<void, OrdinaryManagedRunRevalidationError>;
  }
>();

function issueOrdinaryManagedRunStartObservation(
  observation: OrdinaryManagedRunStartObservationV1,
  revalidateIssued: Effect.Effect<void, OrdinaryManagedRunRevalidationError>,
): void {
  issuedOrdinaryManagedStarts.set(observation, {
    bytes: JSON.stringify(observation),
    revalidateIssued,
  });
}

// Encoded JSON text compares exact persisted checkout identities. An encoding failure stays a defect.
const encodeOrdinaryCheckoutUseJson = (use: OrdinaryCheckout.OrdinaryCheckoutUseV1) =>
  Schema.encodeEffect(Schema.fromJsonString(OrdinaryCheckout.OrdinaryCheckoutUseV1))(use).pipe(
    Effect.orDie,
  );

const encodeOrdinaryCheckoutExecutionRefJson = (
  ref: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
) =>
  Schema.encodeEffect(Schema.fromJsonString(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1))(
    ref,
  ).pipe(Effect.orDie);

// Descriptive fields cannot issue a start: only the original returned object has this retained producer closure.
export function readIssuedOrdinaryManagedRunStartObservation(input: unknown): {
  readonly observation: OrdinaryManagedRunStartObservationV1;
  readonly revalidateIssued: Effect.Effect<void, OrdinaryManagedRunRevalidationError>;
} | null {
  if (typeof input !== "object" || input === null) return null;
  const issued = issuedOrdinaryManagedStarts.get(input);
  if (issued === undefined || JSON.stringify(input) !== issued.bytes) return null;
  return {
    observation: input as OrdinaryManagedRunStartObservationV1,
    revalidateIssued: issued.revalidateIssued,
  };
}

function freezeOrdinaryExecutionFacts<A>(value: A): A {
  if (typeof value === "object" && value !== null) {
    if (DateTime.isDateTime(value)) DateTime.formatIso(value);
    for (const child of Object.values(value)) freezeOrdinaryExecutionFacts(child);
    Object.freeze(value);
  }
  return value;
}

export interface OrdinaryManagedRunExecutionHandleV1 {
  readonly startExecution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
  readonly managedExecutor: OrdinaryManagedRunExecutorV1;
  readonly actualStartObservation: OrdinaryManagedRunStartObservationV1;
  readonly attachment: ProviderOrdinaryExecutionAttachmentV1;
  readonly revalidateCaptured: Effect.Effect<void, OrdinaryManagedRunRevalidationError>;
  readonly revalidateMutation: Effect.Effect<
    void,
    RunExecutionStartError | ProviderSessionActivityError
  >;
  readonly revalidateCompletionBinding: Effect.Effect<void, OrdinaryManagedRunRevalidationError>;
  readonly nativeCompletion?: ProviderManagedActorCompletion.ProviderManagedActorRunReaderV1;
  readonly requireActivatedExecution: Effect.Effect<
    OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
    RunExecutionStartError
  >;
  readonly activate: (
    ref: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
  ) => Effect.Effect<void, RunExecutionStartError>;
  readonly lose: (reason: string) => Effect.Effect<ProviderPinnedRuntimeStopResultV1 | undefined>;
  readonly awaitIngestionExit: Effect.Effect<Exit.Exit<void, RunExecutionIngestError>>;
  /** Closing this owned output scope does not certify adapter mutation completion or retire its SQL participant. */
  readonly close: Effect.Effect<void>;
}

export interface ImportedHistoryStartExecution {
  readonly reference: ImportedHistoryStartExecutionReferenceV2;
  readonly executionIntent: Extract<
    QueuedRunExecutionIntentV2,
    { readonly kind: "imported_history_choice" }
  >;
  readonly workerId: string;
  readonly expectedAttempt: number;
}

export interface QueuedRunStartExecution {
  readonly effectId: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly workerId: string;
  readonly expectedAttempt: number;
}

export const readClaimedQueuedRunStartExecution = (
  input: QueuedRunStartExecution,
  sink: EventSink.EventSinkV2Shape,
) =>
  Effect.gen(function* () {
    const proof = yield* sink.readClaimedQueuedRunStart({
      effectId: input.effectId,
      threadId: input.threadId,
      runId: input.runId,
      workerId: input.workerId,
      expectedAttempt: input.expectedAttempt,
    });
    if (
      proof === null ||
      proof.incarnation.threadId !== input.threadId ||
      proof.executionIntent.kind !== "queued" ||
      proof.executionIntent.commandId !== input.commandId ||
      proof.executionIntent.effectId !== input.effectId ||
      proof.executionIntent.runId !== input.runId ||
      proof.basis.runId !== input.runId ||
      proof.basis.runAttemptId !== proof.executionIntent.runAttemptId
    )
      return yield* new ProviderNativeOperationUnknownError({
        nativeEffect: {
          operationId: input.effectId,
          operation: "start_turn",
          threadId: input.threadId,
          outcome: "unknown",
        },
        cause: "The reserved queued start has no matching current claimed source proof.",
      });
    return proof;
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderNativeOperationUnknownError({
          nativeEffect: {
            operationId: input.effectId,
            operation: "start_turn",
            threadId: input.threadId,
            outcome: "unknown",
          },
          cause,
        }),
    ),
  );

export const readImportedHistoryStartExecution = (
  input: ImportedHistoryStartExecution,
  sink: EventSink.EventSinkV2Shape,
) =>
  Effect.gen(function* () {
    const reference = input.reference;
    const unknown = (cause: unknown) =>
      new ProviderNativeOperationUnknownError({
        nativeEffect: {
          operationId: reference.effectId,
          operation: "start_turn",
          threadId: reference.threadId,
          attemptId: input.executionIntent.runAttemptId,
          outcome: "unknown",
        },
        cause,
      });
    const choice = yield* sink.readImportedHistoryStartChoice(reference);
    if (
      choice === null ||
      choice.receipt.status !== "accepted" ||
      choice.commandId !== reference.commandId ||
      choice.threadId !== reference.threadId ||
      choice.runId !== reference.runId ||
      (choice.effectId !== null && choice.effectId !== reference.effectId) ||
      input.executionIntent.commandId !== reference.commandId ||
      input.executionIntent.runId !== reference.runId ||
      input.executionIntent.effectId !== reference.effectId ||
      input.executionIntent.reviewedBasis !== choice.command.reviewedBasis
    )
      return yield* unknown("The imported choice differs from its prepared execution intent.");
    const facts = yield* sink.readNativeCommandFacts({
      threadId: reference.threadId,
      commandId: reference.commandId,
    });
    const reservations =
      facts.commitSnapshot.records.start_reservations?.filter(
        (row) => row.effect_id === reference.effectId,
      ) ?? [];
    const claims =
      facts.commitSnapshot.records.effects?.filter((row) => row.effect_id === reference.effectId) ??
      [];
    const now = DateTime.formatIso(yield* DateTime.now);
    if (
      reservations.length !== 1 ||
      claims.length !== 1 ||
      claims[0]!.command_id !== reference.commandId ||
      claims[0]!.status !== "running" ||
      claims[0]!.lease_owner !== input.workerId ||
      claims[0]!.attempt_count !== input.expectedAttempt ||
      typeof claims[0]!.lease_expires_at !== "string" ||
      !Number.isFinite(Date.parse(claims[0]!.lease_expires_at)) ||
      Date.parse(claims[0]!.lease_expires_at) <= Date.parse(now) ||
      (facts.commitSnapshot.records.unknown_effect_holds?.length ?? 0) > 0
    )
      return yield* unknown("The prepared imported choice no longer owns its current claim.");
    const intentSchema = Schema.fromJsonString(
      Schema.Struct({
        kind: Schema.Literal("imported_history_choice"),
        commandId: CommandId,
        runId: RunId,
        runAttemptId: RunAttemptId,
        effectId: Schema.NonEmptyString,
        reviewedBasis: Schema.NonEmptyString,
      }),
    );
    const storedIntent = yield* Schema.decodeUnknownEffect(intentSchema)(
      reservations[0]!.execution_intent_json,
    );
    if (
      (["kind", "commandId", "runId", "runAttemptId", "effectId", "reviewedBasis"] as const).some(
        (key) => storedIntent[key] !== input.executionIntent[key],
      )
    )
      return yield* unknown("The imported choice reservation changed before provider execution.");
    const preparedProviders = facts.events.flatMap((stored) =>
      stored.commandId === reference.commandId &&
      stored.event.type === "provider-thread.updated" &&
      stored.event.payload.providerSessionId !== null &&
      stored.event.payload.nativeThreadRef === null &&
      stored.event.payload.nativeConversationHeadRef === null
        ? [stored.event.payload]
        : [],
    );
    const preparedProvider = preparedProviders[0];
    const preparedScopes = facts.events.flatMap((stored) =>
      stored.commandId === reference.commandId &&
      stored.event.type === "checkpoint-scope.created" &&
      stored.event.payload.runId === reference.runId &&
      stored.event.payload.providerThreadId === preparedProvider?.id
        ? [stored.event.payload]
        : [],
    );
    const current = facts.projection;
    const run = current?.runs.find((candidate) => candidate.id === reference.runId);
    const attempt = current?.attempts.find(
      (candidate) => candidate.id === input.executionIntent.runAttemptId,
    );
    const root = current?.nodes.find((candidate) => candidate.id === run?.rootNodeId);
    const provider = current?.providerThreads.find(
      (candidate) => candidate.id === run?.providerThreadId,
    );
    if (
      facts.eventMetadataOverflow ||
      preparedProviders.length !== 1 ||
      preparedScopes.length !== 1 ||
      current === null ||
      preparedProvider === undefined ||
      preparedProvider.appThreadId !== reference.threadId ||
      run === undefined ||
      (run.status !== "starting" && run.status !== "running") ||
      run.userMessageId !== choice.messageId ||
      run.activeAttemptId !== input.executionIntent.runAttemptId ||
      run.providerThreadId !== preparedProvider.id ||
      current.thread.activeProviderThreadId !== preparedProvider.id ||
      attempt?.runId !== reference.runId ||
      attempt.providerThreadId !== preparedProvider.id ||
      root?.checkpointScopeId !== preparedScopes[0]!.id ||
      preparedScopes[0]!.nodeId !== root.id ||
      provider?.providerSessionId !== preparedProvider.providerSessionId ||
      provider.providerInstanceId !== preparedProvider.providerInstanceId ||
      provider.driver !== preparedProvider.driver
    )
      return yield* unknown(
        "The imported choice no longer owns its command-attributed prepared target.",
      );
    for (const fence of facts.commitSnapshot.records.stop_fences ?? []) {
      if (fence.run_id !== reference.runId) continue;
      const fencedIntent = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
        fence.execution_intent_json,
      );
      if (
        typeof fencedIntent === "object" &&
        fencedIntent !== null &&
        "effectId" in fencedIntent &&
        fencedIntent.effectId === reference.effectId
      )
        return yield* unknown("The prepared imported choice is fenced by a current stop intent.");
    }
    return choice;
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderNativeOperationUnknownError({
          nativeEffect: {
            operationId: input.reference.effectId,
            operation: "start_turn",
            threadId: input.reference.threadId,
            attemptId: input.executionIntent.runAttemptId,
            outcome: "unknown",
          },
          cause,
        }),
    ),
  );

export interface ProviderEventRoutingState {
  readonly ownedThreadIds: ReadonlySet<ThreadId>;
  // Set once this run's root turn ended. A child thread created after that
  // belongs to the run that is live then, so this one no longer adopts it.
  readonly rootTurnEnded: boolean;
  readonly ownedProviderThreadIds: ReadonlySet<ProviderThreadId>;
  readonly ownedProviderTurnIds: ReadonlySet<ProviderTurnId>;
  readonly inheritedBackgroundTurnItems: ReadonlyMap<TurnItemId, OrchestrationV2Run["id"]>;
  readonly rootProviderTurnId: ProviderTurnId | null;
}

export interface ProviderEventRouteIdentity {
  readonly threadId: ThreadId;
  readonly runId: OrchestrationV2Run["id"];
  readonly attemptId: RunAttemptId;
  readonly providerThreadId: ProviderThreadId;
}

export interface InheritedBackgroundTurnItemRoute {
  readonly id: TurnItemId;
  readonly runId: OrchestrationV2Run["id"];
}

type ProviderTerminalEvent = Extract<ProviderAdapterV2Event, { readonly type: "turn.terminal" }>;

function isTerminalProviderTurnStatus(status: OrchestrationV2ProviderTurn["status"]): boolean {
  return (
    status === "completed" ||
    status === "interrupted" ||
    status === "failed" ||
    status === "cancelled"
  );
}

function isSettledSubagentStatus(status: OrchestrationV2Subagent["status"]): boolean {
  return !isOrchestrationV2WorkActive(status);
}

// Turn item types whose lifecycle can outlive the root turn (background
// commands, monitors/dynamic tools, subagent rows). Ingestion must not stop
// while one of these is still non-terminal, or the late completion event is
// dropped and the item spins forever in the projection.
const backgroundCapableTurnItemTypes: ReadonlySet<OrchestrationV2TurnItem["type"]> = new Set([
  "command_execution",
  "dynamic_tool",
  "subagent",
]);

function isSettledTurnItemStatus(status: OrchestrationV2TurnItem["status"]): boolean {
  return !isOrchestrationV2WorkActive(status);
}

function isSettledRunEligibleForInheritedBackground(status: OrchestrationV2Run["status"]): boolean {
  return status === "interrupted" || status === "failed" || status === "cancelled";
}

/**
 * Transfer delivery permission for exact live background items whose original
 * run no longer has a subscriber. Provider sessions are runtime containers and
 * can host multiple provider threads, so the durable provider-thread lineage is
 * the discriminator. Completed, rolled-back, and already-terminal items remain
 * excluded.
 */
export function selectInheritedBackgroundTurnItems(input: {
  readonly threadId: ThreadId;
  readonly currentProviderThreadId: ProviderThreadId;
  readonly currentRunOrdinal: number;
  readonly runs: ReadonlyArray<OrchestrationV2Run>;
  readonly turnItems: ReadonlyArray<OrchestrationV2TurnItem>;
}): ReadonlyArray<InheritedBackgroundTurnItemRoute> {
  const settledPriorRunIds = new Set(
    input.runs
      .filter(
        (run) =>
          run.threadId === input.threadId &&
          run.ordinal < input.currentRunOrdinal &&
          isSettledRunEligibleForInheritedBackground(run.status),
      )
      .map((run) => run.id),
  );
  return input.turnItems.flatMap((turnItem) =>
    turnItem.threadId === input.threadId &&
    turnItem.providerThreadId === input.currentProviderThreadId &&
    turnItem.runId !== null &&
    settledPriorRunIds.has(turnItem.runId) &&
    backgroundCapableTurnItemTypes.has(turnItem.type) &&
    !isSettledTurnItemStatus(turnItem.status)
      ? [{ id: turnItem.id, runId: turnItem.runId }]
      : [],
  );
}

type SubagentTurnItem = Extract<OrchestrationV2TurnItem, { readonly type: "subagent" }>;

type OpenRunOwnedSubagentProjection = {
  readonly subagents: ReadonlyMap<NodeId, OrchestrationV2Subagent>;
  readonly turnItems: ReadonlyMap<NodeId, SubagentTurnItem>;
  readonly childTurnItems: ReadonlyMap<TurnItemId, OrchestrationV2TurnItem>;
  readonly nodes: ReadonlyMap<NodeId, OrchestrationV2ExecutionNode>;
  /** Child threads once linked by a root-run subagent row; kept for cascade. */
  readonly linkedChildThreadIds: ReadonlySet<ThreadId>;
};

type RunOwnedSubagentTerminalStatus = Extract<
  OrchestrationV2Subagent["status"],
  "interrupted" | "failed" | "cancelled"
>;

function isOpenExecutionNodeStatus(status: OrchestrationV2ExecutionNode["status"]): boolean {
  return status === "pending" || status === "running" || status === "waiting";
}

function isRunOwnedSubagentTerminalStatus(
  status: ProviderTerminalEvent["status"],
): status is RunOwnedSubagentTerminalStatus {
  return status === "interrupted" || status === "failed" || status === "cancelled";
}

/**
 * Whether a new run takes over a subagent's child thread, so a later message
 * can resume it there. A running subagent stays with the run that launched it,
 * which keeps ingesting until it ends; taking it over too would store its
 * events twice. An interrupted, failed or cancelled one is never resumed.
 */
export function canRouteRelatedSubagent(status: OrchestrationV2Subagent["status"]): boolean {
  return status === "completed";
}

function emptyOpenRunOwnedSubagentProjection(): OpenRunOwnedSubagentProjection {
  return {
    subagents: new Map(),
    turnItems: new Map(),
    childTurnItems: new Map(),
    nodes: new Map(),
    linkedChildThreadIds: new Set(),
  };
}

function withLinkedChildThreadId(
  current: OpenRunOwnedSubagentProjection,
  childThreadId: ThreadId | null,
): OpenRunOwnedSubagentProjection {
  if (childThreadId === null || current.linkedChildThreadIds.has(childThreadId)) {
    return current;
  }
  const linkedChildThreadIds = new Set(current.linkedChildThreadIds);
  linkedChildThreadIds.add(childThreadId);
  return { ...current, linkedChildThreadIds };
}

export function cascadeTerminalizeRunOwnedSubagents(input: {
  readonly run: OrchestrationV2Run;
  readonly open: OpenRunOwnedSubagentProjection;
  readonly status: RunOwnedSubagentTerminalStatus;
  readonly completedAt: DateTime.Utc;
  readonly allocateEventId: () => Effect.Effect<EventId, IdAllocator.IdAllocatorV2AllocationError>;
}): Effect.Effect<
  ReadonlyArray<OrchestrationV2DomainEvent>,
  IdAllocator.IdAllocatorV2AllocationError
> {
  return Effect.gen(function* () {
    const events: Array<OrchestrationV2DomainEvent> = [];
    // Prefer lifetime linkage over currently-open rows: subagent/turn-item
    // snapshots may terminalize before the linked child-thread node settles.
    const childThreadIds = new Set(input.open.linkedChildThreadIds);
    for (const item of [...input.open.subagents.values(), ...input.open.turnItems.values()]) {
      if (item.childThreadId !== null) {
        childThreadIds.add(item.childThreadId);
      }
    }
    const keys = new Set<NodeId>([
      ...input.open.subagents.keys(),
      ...input.open.turnItems.keys(),
      ...input.open.nodes.keys(),
    ]);
    for (const key of keys) {
      const subagent = input.open.subagents.get(key);
      if (subagent !== undefined && !isSettledSubagentStatus(subagent.status)) {
        events.push({
          id: yield* input.allocateEventId(),
          type: "subagent.updated",
          threadId: subagent.threadId,
          runId: input.run.id,
          nodeId: subagent.id,
          driver: subagent.driver,
          providerInstanceId: subagent.providerInstanceId,
          occurredAt: input.completedAt,
          payload: {
            ...subagent,
            status: input.status,
            completedAt: input.completedAt,
            updatedAt: input.completedAt,
          },
        });
      }
      const node = input.open.nodes.get(key);
      if (
        node !== undefined &&
        ((node.threadId === input.run.threadId && node.runId === input.run.id) ||
          childThreadIds.has(node.threadId)) &&
        isOpenExecutionNodeStatus(node.status)
      ) {
        events.push({
          id: yield* input.allocateEventId(),
          type: "node.updated",
          threadId: node.threadId,
          runId: node.runId ?? input.run.id,
          nodeId: node.id,
          providerInstanceId: input.run.providerInstanceId,
          occurredAt: input.completedAt,
          payload: {
            ...node,
            status: input.status,
            completedAt: input.completedAt,
          },
        });
      }
      const turnItem = input.open.turnItems.get(key);
      if (
        turnItem !== undefined &&
        turnItem.runId === input.run.id &&
        !isSettledTurnItemStatus(turnItem.status)
      ) {
        events.push({
          id: yield* input.allocateEventId(),
          type: "turn-item.updated",
          threadId: turnItem.threadId,
          runId: input.run.id,
          ...(turnItem.nodeId === null ? {} : { nodeId: turnItem.nodeId }),
          providerInstanceId: input.run.providerInstanceId,
          occurredAt: input.completedAt,
          payload: {
            ...turnItem,
            status: input.status,
            completedAt: input.completedAt,
            updatedAt: input.completedAt,
          },
        });
      }
    }
    for (const turnItem of input.open.childTurnItems.values()) {
      if (!childThreadIds.has(turnItem.threadId) || isSettledTurnItemStatus(turnItem.status)) {
        continue;
      }
      events.push({
        id: yield* input.allocateEventId(),
        type: "turn-item.updated",
        threadId: turnItem.threadId,
        runId: turnItem.runId ?? input.run.id,
        ...(turnItem.nodeId === null ? {} : { nodeId: turnItem.nodeId }),
        providerInstanceId: input.run.providerInstanceId,
        occurredAt: input.completedAt,
        payload: {
          ...turnItem,
          ...("streaming" in turnItem ? { streaming: false } : {}),
          status: input.status,
          completedAt: input.completedAt,
          updatedAt: input.completedAt,
        },
      });
    }
    return events;
  });
}

export function finalProviderThreadStatus(
  disposition: ProviderTerminalEvent["threadDisposition"],
): OrchestrationV2ProviderThread["status"] {
  return disposition === "broken" ? "error" : "idle";
}

export function makeProviderEventRoutingState(input: {
  readonly identity: ProviderEventRouteIdentity;
  readonly inheritedBackgroundTurnItems?: ReadonlyArray<InheritedBackgroundTurnItemRoute>;
  readonly providerTurnId: ProviderTurnId | null;
  readonly relatedThreadIds?: ReadonlyArray<ThreadId>;
  readonly relatedProviderThreadIds?: ReadonlyArray<ProviderThreadId>;
}): ProviderEventRoutingState {
  return {
    ownedThreadIds: new Set([input.identity.threadId, ...(input.relatedThreadIds ?? [])]),
    rootTurnEnded: false,
    ownedProviderThreadIds: new Set([
      input.identity.providerThreadId,
      ...(input.relatedProviderThreadIds ?? []),
    ]),
    ownedProviderTurnIds:
      input.providerTurnId === null ? new Set() : new Set([input.providerTurnId]),
    inheritedBackgroundTurnItems: new Map(
      (input.inheritedBackgroundTurnItems ?? []).map((item) => [item.id, item.runId]),
    ),
    rootProviderTurnId: input.providerTurnId,
  };
}

export function routeProviderEvent(
  event: ProviderAdapterV2Event,
  input: ProviderEventRouteIdentity,
  state: ProviderEventRoutingState,
): readonly [boolean, ProviderEventRoutingState] {
  const ownsThread = (threadId: ThreadId): boolean => state.ownedThreadIds.has(threadId);
  const ownsChildThread = (threadId: ThreadId): boolean =>
    threadId !== input.threadId && ownsThread(threadId);
  const ownsRun = (runId: string | null): boolean => runId === input.runId;
  const addProviderThread = (providerThreadId: ProviderThreadId): ProviderEventRoutingState => ({
    ...state,
    ownedProviderThreadIds: new Set([...state.ownedProviderThreadIds, providerThreadId]),
  });
  const addProviderTurn = (
    providerTurnId: ProviderTurnId,
    root: boolean,
  ): ProviderEventRoutingState => ({
    ...state,
    ownedProviderTurnIds: new Set([...state.ownedProviderTurnIds, providerTurnId]),
    rootProviderTurnId: root ? providerTurnId : state.rootProviderTurnId,
  });

  switch (event.type) {
    case "runtime_identity.observed":
      return [false, state];
    case "provider_session.updated":
      // The session manager persists process-wide status once for every
      // attached app thread before broadcasting the adapter event.
      return [false, state];
    case "app_thread.created": {
      if (event.appThread.id === input.threadId) {
        return [true, state];
      }
      const isOwnedSubagent =
        !state.rootTurnEnded &&
        event.appThread.lineage.relationshipToParent === "subagent" &&
        event.appThread.lineage.parentThreadId !== null &&
        ownsThread(event.appThread.lineage.parentThreadId);
      if (!isOwnedSubagent) {
        return [false, state];
      }
      return [
        true,
        {
          ...state,
          ownedThreadIds: new Set([...state.ownedThreadIds, event.appThread.id]),
        },
      ];
    }
    case "provider_thread.updated": {
      const belongs =
        state.ownedProviderThreadIds.has(event.providerThread.id) ||
        (event.providerThread.appThreadId !== null && ownsThread(event.providerThread.appThreadId));
      return belongs ? [true, addProviderThread(event.providerThread.id)] : [false, state];
    }
    case "provider_turn.updated": {
      const isRoot = event.providerTurn.runAttemptId === input.attemptId;
      const belongs =
        isRoot ||
        (event.providerTurn.providerThreadId !== input.providerThreadId &&
          state.ownedProviderThreadIds.has(event.providerTurn.providerThreadId)) ||
        state.ownedProviderTurnIds.has(event.providerTurn.id) ||
        (event.threadId !== undefined && ownsChildThread(event.threadId));
      return belongs ? [true, addProviderTurn(event.providerTurn.id, isRoot)] : [false, state];
    }
    case "node.updated": {
      const belongs = ownsRun(event.node.runId) || ownsChildThread(event.node.threadId);
      if (!belongs || event.node.providerThreadId === null) {
        return [belongs, state];
      }
      return [true, addProviderThread(event.node.providerThreadId)];
    }
    case "subagent.updated":
      return [ownsRun(event.subagent.runId) || ownsChildThread(event.subagent.threadId), state];
    case "message.updated":
      return [ownsRun(event.message.runId) || ownsChildThread(event.message.threadId), state];
    case "turn_item.updated": {
      if (ownsRun(event.turnItem.runId) || ownsChildThread(event.turnItem.threadId)) {
        return [true, state];
      }
      const inheritedRunId = state.inheritedBackgroundTurnItems.get(event.turnItem.id);
      // Preserve the item's original ownership while allowing the one live run
      // to deliver an exact carryover identity selected from the projection.
      const isInheritedBackgroundItem =
        event.turnItem.threadId === input.threadId &&
        event.turnItem.runId !== null &&
        event.turnItem.runId === inheritedRunId &&
        backgroundCapableTurnItemTypes.has(event.turnItem.type);
      if (!isInheritedBackgroundItem) {
        return [false, state];
      }
      if (!isSettledTurnItemStatus(event.turnItem.status)) {
        return [true, state];
      }
      const inheritedBackgroundTurnItems = new Map(state.inheritedBackgroundTurnItems);
      inheritedBackgroundTurnItems.delete(event.turnItem.id);
      return [true, { ...state, inheritedBackgroundTurnItems }];
    }
    case "plan.updated":
      return [ownsRun(event.plan.runId) || ownsChildThread(event.plan.threadId), state];
    case "runtime_request.updated":
      return [
        (event.threadId !== undefined && ownsChildThread(event.threadId)) ||
          (event.runtimeRequest.providerTurnId !== null &&
            state.ownedProviderTurnIds.has(event.runtimeRequest.providerTurnId)),
        state,
      ];
    case "turn.terminal":
      return event.providerTurnId === state.rootProviderTurnId
        ? [true, { ...state, rootTurnEnded: true }]
        : [false, state];
  }
}

/**
 * ERRORS
 */
export class RunExecutionStartError extends Schema.TaggedError<RunExecutionStartError>()(
  "RunExecutionStartError",
  {
    commandId: CommandId,
    runId: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to start orchestration V2 run execution ${this.runId}.`;
  }
}

export class RunExecutionIngestError extends Schema.TaggedError<RunExecutionIngestError>()(
  "RunExecutionIngestError",
  {
    runId: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed while ingesting orchestration V2 run execution ${this.runId}.`;
  }
}

export const RunExecutionServiceV2Error = Schema.Union([
  RunExecutionStartError,
  RunExecutionIngestError,
]);
export type RunExecutionServiceV2Error = typeof RunExecutionServiceV2Error.Type;

/**
 * SERVICE DEFINITION
 */
export interface RunExecutionServiceV2StartRootRunInput {
  readonly commandId: CommandId;
  readonly ordinaryCheckoutUse?: OrdinaryCheckout.OrdinaryCheckoutUseV1;
  readonly ordinaryCheckoutExecution?: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
  readonly captureOrdinaryAttachment?: () => Effect.Effect<
    ProviderOrdinaryExecutionAttachmentV1,
    unknown
  >;
  readonly prepareOrdinaryManagedActorRun?: (
    admission: ProviderManagedActorCompletion.ProviderManagedActorAdmissionV1,
  ) => Effect.Effect<ProviderManagedActorCompletion.ProviderManagedActorRunReaderV1, unknown>;
  readonly nativeCreationExecutionContext?: NativeCreationExecutionContextV2;
  readonly importedHistoryStartExecution?: ImportedHistoryStartExecution;
  readonly appThread: OrchestrationV2AppThread;
  readonly providerSessionId: ProviderSessionId;
  readonly session: ProviderAdapterV2SessionRuntime;
  readonly run: OrchestrationV2Run;
  readonly rootNode: OrchestrationV2ExecutionNode;
  readonly checkpointScope: OrchestrationV2CheckpointScope;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly attempt: OrchestrationV2RunAttempt;
  readonly attemptId: RunAttemptId;
  readonly providerTurnOrdinal: number;
  readonly loadInheritedBackgroundTurnItems?: () => Effect.Effect<
    ReadonlyArray<InheritedBackgroundTurnItemRoute>,
    unknown
  >;
  readonly relatedThreadIds?: ReadonlyArray<ThreadId>;
  readonly relatedProviderThreadIds?: ReadonlyArray<ProviderThreadId>;
  readonly shouldStartProviderTurn?: () => Effect.Effect<boolean, never>;
  readonly shouldFinalizeRun?: () => Effect.Effect<boolean, never>;
  readonly hasUnpairedRunInterruptRequest?: () => Effect.Effect<boolean, never>;
  readonly message: ProviderAdapterV2TurnMessage;
  readonly modelSelection: ModelSelection;
  readonly runtimePolicy: ProviderAdapterV2RuntimePolicy;
}

export interface RunExecutionServiceV2Shape {
  readonly startRootRun: (
    input: RunExecutionServiceV2StartRootRunInput,
  ) => Effect.Effect<void | OrdinaryManagedRunExecutionHandleV1, RunExecutionServiceV2Error>;
}

export class RunExecutionServiceV2 extends Context.Service<
  RunExecutionServiceV2,
  RunExecutionServiceV2Shape
>()("t3/orchestration-v2/RunExecutionService/RunExecutionServiceV2") {}

/**
 * IMPLEMENTATIONS
 */
export const layer: Layer.Layer<
  RunExecutionServiceV2,
  never,
  | CheckpointService.CheckpointServiceV2
  | EventSink.EventSinkV2
  | IdAllocator.IdAllocatorV2
  | ProviderEventIngestor.ProviderEventIngestorV2
  | ServerSettings.ServerSettingsService
> = Layer.effect(
  RunExecutionServiceV2,
  Effect.gen(function* () {
    const serviceScope = yield* Effect.scope;
    const checkpointService = yield* CheckpointService.CheckpointServiceV2;
    const eventSink = yield* EventSink.EventSinkV2;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const providerEventIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
    const serverSettings = yield* ServerSettings.ServerSettingsService;
    const finalizationObserver = yield* RunFinalizationService.RunFinalizationObserver;

    const writeFinalRunEvents = (input: {
      readonly run: OrchestrationV2Run;
      readonly ordinaryCheckoutExecution?: Effect.Effect<
        OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
        RunExecutionStartError
      >;
      readonly rootNode: OrchestrationV2ExecutionNode;
      readonly checkpointScope: OrchestrationV2CheckpointScope;
      readonly providerThread: OrchestrationV2ProviderThread;
      readonly attempt: OrchestrationV2RunAttempt;
      readonly acceptedPrimaryState?: Effect.Effect<{
        readonly attempt: OrchestrationV2RunAttempt;
        readonly rootNode: OrchestrationV2ExecutionNode;
      }>;
      readonly shouldFinalizeRun?: () => Effect.Effect<boolean, never>;
      readonly hasUnpairedRunInterruptRequest?: () => Effect.Effect<boolean, never>;
      readonly openRunOwnedSubagents?: OpenRunOwnedSubagentProjection;
      readonly terminal: ProviderTerminalEvent;
      readonly failureItemPersisted: boolean;
      readonly refreshAfterTurn: Effect.Effect<void>;
      readonly writeIfRunCurrent?: {
        readonly activeAttemptId: RunAttemptId;
        readonly expectedStatus: OrchestrationV2Run["status"];
      };
    }) =>
      Effect.gen(function* () {
        const completedAt = yield* DateTime.now;
        const shouldFinalizeRun =
          input.shouldFinalizeRun === undefined ? true : yield* input.shouldFinalizeRun();
        if (!shouldFinalizeRun) {
          // Superseded attempt (steer / selection restart). Emit
          // run_interrupt_result only when hard Stop left an unpaired request
          // for this run; plain steers and already-paired stops emit nothing.
          if (input.terminal.status === "interrupted") {
            const hasUnpairedRequest =
              input.hasUnpairedRunInterruptRequest === undefined
                ? false
                : yield* input.hasUnpairedRunInterruptRequest();
            if (hasUnpairedRequest) {
              yield* eventSink.writeWithEffects({
                effects: [],
                events: [
                  {
                    id: yield* idAllocator.allocate.event({ threadId: input.run.threadId }),
                    type: "turn-item.updated" as const,
                    threadId: input.run.threadId,
                    runId: input.run.id,
                    nodeId: input.rootNode.id,
                    providerInstanceId: input.run.providerInstanceId,
                    occurredAt: completedAt,
                    payload: makeInterruptResultTurnItem({
                      idAllocator,
                      run: input.run,
                      rootNode: input.rootNode,
                      providerThread: input.providerThread,
                      completedAt,
                    }),
                  },
                ],
              });
              yield* input.refreshAfterTurn;
            }
          }
          return;
        }
        const acceptedPrimary =
          input.acceptedPrimaryState === undefined
            ? { attempt: input.attempt, rootNode: input.rootNode }
            : yield* input.acceptedPrimaryState;
        const finalizedAttempt: OrchestrationV2RunAttempt | null = {
          ...acceptedPrimary.attempt,
          status: input.terminal.status,
          completedAt,
        };
        const allocateEventId = () => idAllocator.allocate.event({ threadId: input.run.threadId });
        const open = input.openRunOwnedSubagents ?? emptyOpenRunOwnedSubagentProjection();
        const hasOpenSubagentProjection =
          open.subagents.size > 0 ||
          open.turnItems.size > 0 ||
          open.childTurnItems.size > 0 ||
          open.nodes.size > 0;
        const cascadedSubagentEvents =
          isRunOwnedSubagentTerminalStatus(input.terminal.status) && hasOpenSubagentProjection
            ? yield* cascadeTerminalizeRunOwnedSubagents({
                run: input.run,
                open,
                status: input.terminal.status,
                completedAt,
                allocateEventId,
              })
            : [];
        const persistedStatus =
          input.terminal.status === "completed" ? "waiting" : input.terminal.status;
        // Completion cohorts are advanced by Orchestrator while a provider
        // turn is in flight. Do not replay the run snapshot captured at start
        // over a newer acknowledgement, successor, or Stop barrier.
        const { delegatedCompletion: _delegatedCompletion, ...runWithoutDelegatedCompletion } =
          input.run;
        const finalizedRun: OrchestrationV2Run = {
          ...runWithoutDelegatedCompletion,
          status: persistedStatus,
          completedAt: input.terminal.status === "completed" ? null : completedAt,
        };
        const finalizedRootNode: OrchestrationV2ExecutionNode = {
          ...acceptedPrimary.rootNode,
          status: persistedStatus,
          completedAt: input.terminal.status === "completed" ? null : completedAt,
          checkpointScopeId: input.checkpointScope.id,
        };
        const finalizedProviderThread: OrchestrationV2ProviderThread = {
          ...input.providerThread,
          status: finalProviderThreadStatus(input.terminal.threadDisposition),
          updatedAt: completedAt,
        };
        const runEventId = yield* allocateEventId();
        const nodeEventId = yield* allocateEventId();
        const providerThreadEventId = yield* allocateEventId();
        const checkpointCaptureCommandId = CommandId.make(
          `command:effect:checkpoint.capture:${input.run.id}`,
        );
        // Stopped runs capture too: their checkpoint is the rollback point for
        // the next message. The capture is enqueued with these terminal events,
        // ahead of any later run's start on this thread's effect lane.
        // Failed captured runs also need this physical result to settle their
        // original managed checkout participant while retaining failed status.
        const ordinaryExecution =
          input.ordinaryCheckoutExecution === undefined
            ? undefined
            : yield* input.ordinaryCheckoutExecution;
        const ordinaryUseRecord =
          ordinaryExecution === undefined
            ? null
            : yield* eventSink.readOrdinaryCheckoutUse(ordinaryExecution.originalUse.operationId);
        if (ordinaryExecution !== undefined && ordinaryUseRecord === null)
          return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
            reason: "stale_admission",
            threadId: input.run.threadId,
            path: ordinaryExecution.originalUse.lease.resourcePath,
            message: "Run finalization has no original checkout admission",
          });
        const capturesTerminalCheckpoint =
          input.terminal.status === "completed" ||
          input.terminal.status === "interrupted" ||
          input.terminal.status === "cancelled" ||
          (input.terminal.status === "failed" &&
            ordinaryExecution?.executor.kind === "captured_managed_run");
        const finalization = {
          ...(ordinaryExecution === undefined ||
          ordinaryUseRecord === null ||
          !capturesTerminalCheckpoint
            ? {}
            : {
                ordinaryCheckoutEffects: [
                  {
                    admission: ordinaryExecution.originalUse.admission,
                    runId: input.run.id,
                    source: ordinaryUseRecord.subject.source,
                    joinedUse: ordinaryExecution.originalUse,
                    ordinaryCheckoutExecution: ordinaryExecution,
                  },
                ],
              }),
          effects: capturesTerminalCheckpoint
            ? [
                {
                  id: `effect:checkpoint.capture:${input.run.id}`,
                  commandId: checkpointCaptureCommandId,
                  threadId: input.run.threadId,
                  request: {
                    type: "checkpoint.capture" as const,
                    runId: input.run.id,
                    scopeId: input.checkpointScope.id,
                  },
                },
              ]
            : [],
          events: [
            // Terminalize open run-owned subagent rows before the root run
            // settles so projections never keep a forever-running subagent card.
            ...cascadedSubagentEvents,
            ...(finalizedAttempt === null
              ? []
              : [
                  {
                    id: yield* allocateEventId(),
                    type: "run-attempt.updated" as const,
                    threadId: input.run.threadId,
                    runId: input.run.id,
                    nodeId: input.rootNode.id,
                    providerInstanceId: input.run.providerInstanceId,
                    occurredAt: completedAt,
                    payload: finalizedAttempt,
                  },
                ]),
            ...(input.terminal.status === "interrupted"
              ? [
                  {
                    id: yield* allocateEventId(),
                    type: "turn-item.updated" as const,
                    threadId: input.run.threadId,
                    runId: input.run.id,
                    nodeId: input.rootNode.id,
                    providerInstanceId: input.run.providerInstanceId,
                    occurredAt: completedAt,
                    payload: makeInterruptResultTurnItem({
                      idAllocator,
                      run: input.run,
                      rootNode: input.rootNode,
                      providerThread: input.providerThread,
                      completedAt,
                    }),
                  },
                ]
              : []),
            ...(input.terminal.status === "failed" && !input.failureItemPersisted
              ? [
                  {
                    id: yield* allocateEventId(),
                    type: "turn-item.updated" as const,
                    threadId: input.run.threadId,
                    runId: input.run.id,
                    nodeId: input.rootNode.id,
                    providerInstanceId: input.run.providerInstanceId,
                    occurredAt: completedAt,
                    payload: makeProviderFailureTurnItem({
                      idAllocator,
                      driver: input.terminal.driver,
                      threadId: input.run.threadId,
                      runId: input.run.id,
                      nodeId: input.rootNode.id,
                      providerThreadId: input.terminal.providerThreadId,
                      providerTurnId: input.terminal.providerTurnId,
                      itemOrdinal: input.terminal.failureItemOrdinal,
                      failure: input.terminal.failure,
                      occurredAt: completedAt,
                    }),
                  },
                ]
              : []),
            {
              id: runEventId,
              type: "run.updated",
              threadId: input.run.threadId,
              runId: input.run.id,
              nodeId: input.rootNode.id,
              providerInstanceId: input.run.providerInstanceId,
              occurredAt: completedAt,
              payload: finalizedRun,
            },
            {
              id: nodeEventId,
              type: "node.updated",
              threadId: input.run.threadId,
              runId: input.run.id,
              nodeId: input.rootNode.id,
              providerInstanceId: input.run.providerInstanceId,
              occurredAt: completedAt,
              payload: finalizedRootNode,
            },
            {
              id: providerThreadEventId,
              type: "provider-thread.updated",
              threadId: input.run.threadId,
              providerInstanceId: input.run.providerInstanceId,
              occurredAt: completedAt,
              payload: finalizedProviderThread,
            },
          ],
        } satisfies Parameters<typeof eventSink.writeWithEffects>[0];
        if (input.writeIfRunCurrent !== undefined) {
          const result = yield* eventSink.writeIfRunCurrent({
            threadId: input.run.threadId,
            runId: input.run.id,
            activeAttemptId: input.writeIfRunCurrent.activeAttemptId,
            expectedStatus: input.writeIfRunCurrent.expectedStatus,
            events: finalization.events,
          });
          if (!result.committed) {
            return;
          }
        } else {
          yield* eventSink.writeWithEffects(finalization);
        }
        yield* input.refreshAfterTurn;
      });

    return RunExecutionServiceV2.of({
      startRootRun: (input) =>
        Effect.gen(function* () {
          const ordinaryStartExecution = input.ordinaryCheckoutExecution;
          const ordinaryFailure = (cause: unknown) =>
            new RunExecutionStartError({
              commandId: input.commandId,
              runId: input.run.id,
              cause,
            });
          const ordinaryAdmission =
            ordinaryStartExecution === undefined
              ? null
              : yield* eventSink
                  .readOrdinaryCheckoutAdmissionForRun({
                    threadId: input.run.threadId,
                    runId: input.run.id,
                  })
                  .pipe(Effect.mapError(ordinaryFailure));
          if (ordinaryStartExecution !== undefined) {
            if (
              ordinaryStartExecution.executor.kind !== "actual_outbox_claim" ||
              ordinaryAdmission?.run === null ||
              ordinaryAdmission?.run === undefined ||
              input.captureOrdinaryAttachment === undefined ||
              ordinaryAdmission.run.runId !== input.run.id ||
              ordinaryAdmission.run.runAttemptId !== input.attemptId ||
              ordinaryAdmission.run.nodeId !== input.rootNode.id ||
              ordinaryAdmission.run.messageId !== input.message.messageId ||
              (input.ordinaryCheckoutUse !== undefined &&
                (yield* encodeOrdinaryCheckoutUseJson(input.ordinaryCheckoutUse)) !==
                  (yield* encodeOrdinaryCheckoutUseJson(ordinaryStartExecution.originalUse)))
            )
              return yield* ordinaryFailure(
                "The ordinary run has no exact original start actor, accepted run, or captured runtime producer.",
              );
            yield* eventSink
              .revalidateOrdinaryCheckoutExecution(ordinaryStartExecution)
              .pipe(Effect.mapError(ordinaryFailure));
          }
          const runScope = yield* Scope.fork(serviceScope);
          const activated = yield* Deferred.make<
            OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
            RunExecutionStartError
          >();
          let scopeCurrent = true;
          let lossSignaled = false;
          let activatedRef: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1 | undefined;
          const currentOrdinaryExecution =
            ordinaryStartExecution === undefined ? undefined : Deferred.await(activated);
          yield* Scope.addFinalizer(
            runScope,
            Effect.sync(() => {
              scopeCurrent = false;
            }).pipe(
              Effect.andThen(
                Deferred.fail(
                  activated,
                  ordinaryFailure("The owned run scope closed before activation."),
                ),
              ),
              Effect.asVoid,
            ),
          );
          const executionReference =
            input.nativeCreationExecutionContext === undefined
              ? null
              : getNativeCreationExecutionReference(input.nativeCreationExecutionContext);
          if (input.nativeCreationExecutionContext !== undefined && executionReference === null) {
            return yield* new RunExecutionStartError({
              commandId: input.commandId,
              runId: input.run.id,
              cause: "The native execution context has no issued effect reference.",
            });
          }
          if (input.importedHistoryStartExecution !== undefined) {
            if (
              executionReference !== null ||
              input.importedHistoryStartExecution.reference.threadId !== input.run.threadId ||
              input.importedHistoryStartExecution.reference.runId !== input.run.id ||
              input.importedHistoryStartExecution.executionIntent.runAttemptId !== input.attemptId
            )
              return yield* new RunExecutionStartError({
                commandId: input.commandId,
                runId: input.run.id,
                cause: "The prepared application choice differs from this provider execution.",
              });
            yield* readImportedHistoryStartExecution(
              input.importedHistoryStartExecution,
              eventSink,
            ).pipe(Effect.mapError(ordinaryFailure));
          }
          // Startup failure and stream shutdown can report the same attempt.
          const refreshAfterTurn = yield* Effect.cached(
            finalizationObserver.refreshAfterTurn(input.appThread.projectId).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("failed to refresh pull requests after run termination", {
                  threadId: input.run.threadId,
                  runId: input.run.id,
                  cause,
                }),
              ),
            ),
          );
          const makeFailedTerminalEvent = (
            failure: OrchestrationV2ProviderFailure,
            failureItemOrdinal: number,
          ): ProviderTerminalEvent => ({
            type: "turn.terminal",
            driver: input.providerThread.driver,
            providerThreadId: input.providerThread.id,
            providerTurnId:
              input.attempt.providerTurnId ??
              idAllocator.derive.providerTurn({
                driver: input.providerThread.driver,
                nativeTurnId: `failed:${input.attempt.id}`,
              }),
            runOrdinal: input.run.ordinal,
            failureItemOrdinal,
            status: "failed",
            failure,
            threadDisposition: "reusable",
          });
          const responseStreamingMode = yield* Effect.gen(function* () {
            const responseStreamingMode = yield* serverSettings.getSettings.pipe(
              Effect.map(
                (settings) =>
                  resolveProjectSettings(settings, input.appThread.projectId).settings
                    .responseStreamingMode,
              ),
            );
            yield* checkpointService
              .captureBaseline({
                scope: input.checkpointScope,
                ordinalWithinScope: Math.max(0, input.run.ordinal - 1),
                ...(ordinaryStartExecution === undefined
                  ? {}
                  : {
                      ordinaryCheckoutUse: ordinaryStartExecution.originalUse,
                      ordinaryCheckoutExecution: ordinaryStartExecution,
                    }),
              })
              .pipe(
                Effect.provideService(EventSink.EventSinkV2, eventSink),
                Effect.catchCause((cause) =>
                  Cause.hasInterruptsOnly(cause) ||
                  cause.reasons.some(
                    (reason) =>
                      Cause.isFailReason(reason) &&
                      CheckpointService.isOrdinaryCheckoutMutationError(reason.error),
                  )
                    ? Effect.failCause(cause)
                    : Effect.logWarning(
                        "orchestration V2 checkpoint baseline capture failed; starting provider without a baseline",
                        { runId: input.run.id },
                      ),
                ),
              );
            if (
              input.shouldStartProviderTurn !== undefined &&
              !(yield* input.shouldStartProviderTurn())
            ) {
              if (ordinaryStartExecution !== undefined) {
                yield* Scope.close(runScope, Exit.void);
                return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                  reason: "unknown_use",
                  threadId: input.run.threadId,
                  path: ordinaryStartExecution.originalUse.lease.resourcePath,
                  message:
                    "The original run attempt was superseded after baseline preparation and before native dispatch.",
                });
              }
              return null;
            }
            return responseStreamingMode;
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.gen(function* () {
                if (
                  Cause.hasInterruptsOnly(cause) ||
                  cause.reasons.some(
                    (reason) =>
                      Cause.isFailReason(reason) &&
                      CheckpointService.isOrdinaryCheckoutMutationError(reason.error),
                  )
                ) {
                  return yield* Effect.failCause(cause);
                }
                yield* Effect.logError("orchestration V2 run preparation failed", {
                  runId: input.run.id,
                  cause,
                });
                yield* writeFinalRunEvents({
                  ...(ordinaryStartExecution === undefined
                    ? {}
                    : { ordinaryCheckoutExecution: Effect.succeed(ordinaryStartExecution) }),
                  run: input.run,
                  rootNode: input.rootNode,
                  checkpointScope: input.checkpointScope,
                  providerThread: input.providerThread,
                  attempt: input.attempt,
                  terminal: makeFailedTerminalEvent(
                    makeProviderFailure({
                      cause: Cause.squash(cause),
                      // Keep exact underlying text in the logged cause only;
                      // the persisted turn item gets a bounded curated message.
                      message: "Run preparation failed.",
                      class: "unknown",
                    }),
                    input.providerTurnOrdinal * 100 + 1,
                  ),
                  failureItemPersisted: false,
                  refreshAfterTurn,
                  writeIfRunCurrent: {
                    activeAttemptId: input.attemptId,
                    expectedStatus: "running",
                  },
                });
                return null;
              }),
            ),
            Effect.mapError(
              (cause) =>
                new RunExecutionStartError({
                  commandId: input.commandId,
                  runId: input.run.id,
                  cause,
                }),
            ),
          );
          if (responseStreamingMode === null) {
            return;
          }
          const terminalEvent = yield* Ref.make<ProviderTerminalEvent | null>(null);
          const assistantOutputHeld = yield* Ref.make(false);
          const capturedAssistantOrigin = yield* Ref.make<ProviderEventOrigin | undefined>(
            undefined,
          );
          const latestTurnItemOrdinal = yield* Ref.make(input.providerTurnOrdinal * 100);
          const latestProviderThread = yield* Ref.make(input.providerThread);
          const acceptedPrimaryState = yield* Ref.make({
            attempt: input.attempt,
            rootNode: input.rootNode,
          });
          const routeIdentity: ProviderEventRouteIdentity = {
            threadId: input.run.threadId,
            runId: input.run.id,
            attemptId: input.attempt.id,
            providerThreadId: input.providerThread.id,
          };
          const eventSubscription =
            input.session.subscribeEvents === undefined
              ? { events: input.session.events, close: Effect.void }
              : yield* input.session.subscribeEvents;
          // Loss can close the scope before its child fiber starts; acquisition owns cleanup immediately.
          const closeEventSubscription = yield* Effect.cached(eventSubscription.close);
          yield* Scope.addFinalizer(runScope, closeEventSubscription);
          const inheritedBackgroundTurnItems = yield* (
            input.loadInheritedBackgroundTurnItems?.() ?? Effect.succeed([])
          ).pipe(
            Effect.onError(() => closeEventSubscription),
            Effect.mapError(
              (cause) =>
                new RunExecutionStartError({
                  commandId: input.commandId,
                  runId: input.run.id,
                  cause,
                }),
            ),
          );
          const inheritedBackgroundTurnItemsById = new Map(
            inheritedBackgroundTurnItems.map((item) => [item.id, item.runId]),
          );
          const eventRouting = yield* Ref.make<ProviderEventRoutingState>(
            makeProviderEventRoutingState({
              identity: routeIdentity,
              inheritedBackgroundTurnItems,
              providerTurnId: input.attempt.providerTurnId,
              ...(input.relatedThreadIds === undefined
                ? {}
                : { relatedThreadIds: input.relatedThreadIds }),
              ...(input.relatedProviderThreadIds === undefined
                ? {}
                : { relatedProviderThreadIds: input.relatedProviderThreadIds }),
            }),
          );
          const rootTerminalSeen = yield* Ref.make(false);
          const rootRunFinalized = yield* Ref.make(false);
          const providerThreadOwnerLost = yield* Ref.make(false);
          const activeChildProviderTurns = yield* Ref.make<ReadonlySet<ProviderTurnId>>(new Set());
          const activeChildSubagents = yield* Ref.make<ReadonlySet<NodeId>>(new Set());
          const activeBackgroundTurnItems = yield* Ref.make<
            ReadonlySet<OrchestrationV2TurnItem["id"]>
          >(new Set(inheritedBackgroundTurnItemsById.keys()));
          const openRunOwnedSubagents = yield* Ref.make(emptyOpenRunOwnedSubagentProjection());
          const finalizeRootRun = (terminal: ProviderTerminalEvent) =>
            Effect.gen(function* () {
              if (yield* Ref.get(rootRunFinalized)) {
                return;
              }
              const providerThread = yield* Ref.get(latestProviderThread);
              const openSubagents = yield* Ref.get(openRunOwnedSubagents);
              yield* writeFinalRunEvents({
                ...(currentOrdinaryExecution === undefined
                  ? {}
                  : { ordinaryCheckoutExecution: currentOrdinaryExecution }),
                run: input.run,
                rootNode: input.rootNode,
                checkpointScope: input.checkpointScope,
                providerThread,
                attempt: input.attempt,
                acceptedPrimaryState: Ref.get(acceptedPrimaryState),
                ...(input.shouldFinalizeRun === undefined
                  ? {}
                  : { shouldFinalizeRun: input.shouldFinalizeRun }),
                ...(input.hasUnpairedRunInterruptRequest === undefined
                  ? {}
                  : {
                      hasUnpairedRunInterruptRequest: input.hasUnpairedRunInterruptRequest,
                    }),
                openRunOwnedSubagents: openSubagents,
                terminal,
                failureItemPersisted: terminal.status === "failed",
                refreshAfterTurn,
              }).pipe(
                Effect.mapError(
                  (cause) => new RunExecutionIngestError({ runId: input.run.id, cause }),
                ),
              );
              if (isRunOwnedSubagentTerminalStatus(terminal.status)) {
                yield* Ref.set(openRunOwnedSubagents, emptyOpenRunOwnedSubagentProjection());
              }
              yield* Ref.set(rootRunFinalized, true);
            });
          const trackChildLifecycle = (event: ProviderAdapterV2Event, deliverable: boolean) =>
            Effect.gen(function* () {
              const routing = yield* Ref.get(eventRouting);
              if (event.type === "provider_turn.updated") {
                const isRoot =
                  event.providerTurn.runAttemptId === input.attempt.id ||
                  event.providerTurn.id === routing.rootProviderTurnId;
                if (!isRoot) {
                  yield* Ref.update(activeChildProviderTurns, (current) => {
                    const next = new Set(current);
                    if (isTerminalProviderTurnStatus(event.providerTurn.status)) {
                      next.delete(event.providerTurn.id);
                    } else {
                      next.add(event.providerTurn.id);
                    }
                    return next;
                  });
                }
              }
              if (event.type === "subagent.updated") {
                const belongsToRootRun = event.subagent.runId === input.run.id;
                const belongsToOwnedChildThread =
                  event.subagent.threadId !== input.run.threadId &&
                  routing.ownedThreadIds.has(event.subagent.threadId);
                if (belongsToRootRun || belongsToOwnedChildThread) {
                  yield* Ref.update(activeChildSubagents, (current) => {
                    const next = new Set(current);
                    if (isSettledSubagentStatus(event.subagent.status)) {
                      next.delete(event.subagent.id);
                    } else {
                      next.add(event.subagent.id);
                    }
                    return next;
                  });
                }
                // Snapshot run-owned subagents for interrupt cascade.
                // Preserve childThreadId linkage for the root-run lifetime even
                // after the subagent row terminalizes, so open child-thread
                // nodes can still be proven linked on a later root interrupt.
                if (belongsToRootRun) {
                  yield* Ref.update(openRunOwnedSubagents, (current) => {
                    const withLink = withLinkedChildThreadId(current, event.subagent.childThreadId);
                    const subagents = new Map(withLink.subagents);
                    if (isSettledSubagentStatus(event.subagent.status)) {
                      subagents.delete(event.subagent.id);
                    } else {
                      subagents.set(event.subagent.id, event.subagent);
                    }
                    return { ...withLink, subagents };
                  });
                }
              }
              if (event.type === "node.updated") {
                const belongsToRootSubagent =
                  event.node.kind === "subagent" && event.node.runId === input.run.id;
                const belongsToOwnedChildThread =
                  event.node.threadId !== input.run.threadId &&
                  routing.ownedThreadIds.has(event.node.threadId);
                if (!belongsToRootSubagent && !belongsToOwnedChildThread) {
                  return;
                }
                yield* Ref.update(openRunOwnedSubagents, (current) => {
                  const nodes = new Map(current.nodes);
                  if (isOpenExecutionNodeStatus(event.node.status)) {
                    nodes.set(event.node.id, event.node);
                  } else {
                    nodes.delete(event.node.id);
                  }
                  return { ...current, nodes };
                });
              }
              if (event.type === "turn_item.updated") {
                const belongsToRootRun = event.turnItem.runId === input.run.id;
                const belongsToOwnedChildThread =
                  event.turnItem.threadId !== input.run.threadId &&
                  routing.ownedThreadIds.has(event.turnItem.threadId);
                const belongsToInheritedBackgroundItem =
                  event.turnItem.threadId === input.run.threadId &&
                  event.turnItem.runId !== null &&
                  inheritedBackgroundTurnItemsById.get(event.turnItem.id) === event.turnItem.runId;
                if (
                  backgroundCapableTurnItemTypes.has(event.turnItem.type) &&
                  (belongsToRootRun ||
                    belongsToOwnedChildThread ||
                    belongsToInheritedBackgroundItem)
                ) {
                  yield* Ref.update(activeBackgroundTurnItems, (current) => {
                    const next = new Set(current);
                    if (isSettledTurnItemStatus(event.turnItem.status)) {
                      next.delete(event.turnItem.id);
                    } else {
                      next.add(event.turnItem.id);
                    }
                    return next;
                  });
                }
                if (belongsToOwnedChildThread && deliverable) {
                  yield* Ref.update(openRunOwnedSubagents, (current) => {
                    const childTurnItems = new Map(current.childTurnItems);
                    if (isSettledTurnItemStatus(event.turnItem.status)) {
                      childTurnItems.delete(event.turnItem.id);
                    } else {
                      childTurnItems.set(event.turnItem.id, event.turnItem);
                    }
                    return { ...current, childTurnItems };
                  });
                }
                if (belongsToRootRun && event.turnItem.type === "subagent") {
                  const subagentItem = event.turnItem;
                  yield* Ref.update(openRunOwnedSubagents, (current) => {
                    const withLink = withLinkedChildThreadId(current, subagentItem.childThreadId);
                    const turnItems = new Map(withLink.turnItems);
                    if (isSettledTurnItemStatus(subagentItem.status)) {
                      turnItems.delete(subagentItem.subagentId);
                    } else {
                      turnItems.set(subagentItem.subagentId, subagentItem);
                    }
                    return { ...withLink, turnItems };
                  });
                }
              }
            });
          const shouldStopProviderEventIngestion = Effect.gen(function* () {
            if (!(yield* Ref.get(rootTerminalSeen))) {
              return false;
            }
            const terminal = yield* Ref.get(terminalEvent);
            // Non-completed terminals drop background tracking immediately.
            if (terminal !== null && terminal.status !== "completed") {
              return true;
            }
            const childProviderTurns = yield* Ref.get(activeChildProviderTurns);
            if (childProviderTurns.size > 0) {
              return false;
            }
            const childSubagents = yield* Ref.get(activeChildSubagents);
            if (childSubagents.size > 0) {
              return false;
            }
            // Keep ingesting past root settlement while background-capable
            // items owned by this run (or an owned child thread) are still
            // non-terminal, so their late completion events reach the
            // projection (stuck-spinner fix). Only for completed runs:
            // interrupted/failed turns intentionally drop background tracking
            // rather than pinning the stream open. Newly owned items depend on
            // adapters emitting a non-terminal event before the root terminal.
            // Exact inherited items are seeded from their selected durable rows.
            //
            // Owner loss (a newer run claimed lastRunOrdinal) must not close
            // this stream while these sets are non-empty: turn_item.updated
            // writes are not ownership-gated, so late completions still land.
            const backgroundItems = yield* Ref.get(activeBackgroundTurnItems);
            if (backgroundItems.size > 0) {
              return false;
            }
            // Owner loss means do not hold the stream open solely for the
            // roster probe; once background sets are empty, release.
            if (yield* Ref.get(providerThreadOwnerLost)) {
              return true;
            }
            // Claude background Bash has no turn-item projection. Keep the
            // stream open while this root's provider thread still reports
            // pending roster work so late empty updates can clear Waiting.
            // Use only the thread-scoped probe: session-wide pending work
            // (siblings, wake buffers, session subagents) must not pin this
            // root subscription. Session idle release still uses
            // hasPendingBackgroundWork via ProviderSessionManager.
            const latestProviderThreadSnapshot = yield* Ref.get(latestProviderThread);
            if (input.session.hasPendingBackgroundWorkForThread !== undefined) {
              const hasPendingWork = yield* input.session
                .hasPendingBackgroundWorkForThread(latestProviderThreadSnapshot)
                .pipe(Effect.catchCause(() => Effect.succeed(false)));
              if (hasPendingWork) {
                return false;
              }
            }
            return true;
          });
          const filterAssistantEvent = makeAssistantStreamingFilter(responseStreamingMode);
          const revalidateOutputRuntime = (
            binding: ProviderEventIngestor.ProviderAssistantOutputOwner["binding"],
          ) =>
            Effect.gen(function* () {
              const providerThread = yield* Ref.get(latestProviderThread);
              if (
                input.session.runtimeGeneration !== binding.runtimeGeneration ||
                input.session.providerSessionId !== binding.providerSessionId ||
                providerThread.id !== binding.providerThreadId ||
                providerThread.nativeThreadRef?.nativeId !== binding.nativeThreadId ||
                input.session.instanceId !== binding.instanceId
              )
                return yield* Effect.fail(
                  "Assistant output's emitting runtime was replaced before publication.",
                );
            });
          const providerEventFiber = yield* eventSubscription.events.pipe(
            Stream.filterEffect((event) =>
              Effect.gen(function* () {
                const origin = readProviderEventOrigin(event);
                if (origin !== undefined) {
                  if (
                    origin.producer.driver !== event.driver ||
                    origin.producer.driver !== input.session.driver ||
                    origin.producer.instanceId !== input.run.providerInstanceId ||
                    origin.producer.providerSessionId !== input.providerSessionId
                  )
                    return false;
                  const owner = origin.turn;
                  if (
                    owner !== undefined &&
                    (owner.binding.instanceId !== origin.producer.instanceId ||
                      owner.binding.providerSessionId !== origin.producer.providerSessionId ||
                      (origin.producer.runtimeGeneration !== undefined &&
                        origin.producer.runtimeGeneration !== owner.binding.runtimeGeneration))
                  )
                    return false;
                  const current = yield* origin.producer.revalidateCurrent.pipe(Effect.exit);
                  if (current._tag === "Failure") return false;
                  if (origin.derivation !== undefined) {
                    const d = origin.derivation;
                    const executor = d.executor;
                    if (
                      origin.turn !== undefined ||
                      executor.runId !== input.run.id ||
                      executor.attemptId !== input.attempt.id ||
                      executor.binding.threadId !== input.run.threadId ||
                      executor.binding.providerThreadId !== input.providerThread.id ||
                      executor.binding.instanceId !== input.run.providerInstanceId ||
                      executor.binding.providerSessionId !== input.providerSessionId ||
                      d.subject.parentThreadId !== input.run.threadId
                    )
                      return false;
                    const derivation = yield* d.revalidateDerivation.pipe(Effect.exit);
                    if (derivation._tag === "Failure") {
                      yield* Ref.set(assistantOutputHeld, true);
                      return yield* Effect.fail(
                        new RunExecutionIngestError({
                          runId: input.run.id,
                          cause: derivation.cause,
                        }),
                      );
                    }
                    return true;
                  }
                  if (
                    owner !== undefined &&
                    (event.type === "turn.terminal" || event.type === "provider_turn.updated")
                  ) {
                    const providerTurnId =
                      event.type === "turn.terminal" ? event.providerTurnId : event.providerTurn.id;
                    if (providerTurnId !== owner.providerTurnId) return false;
                    const providerThreadId =
                      event.type === "turn.terminal"
                        ? event.providerThreadId
                        : event.providerTurn.providerThreadId;
                    if (
                      providerThreadId !== owner.binding.providerThreadId ||
                      (event.type === "provider_turn.updated" &&
                        event.providerTurn.runAttemptId !== owner.attemptId)
                    )
                      return false;
                    if (
                      owner.binding.threadId === input.run.threadId &&
                      (owner.runId !== input.run.id || owner.attemptId !== input.attempt.id)
                    )
                      return false;
                  }
                }
                return yield* Ref.modify(eventRouting, (state) =>
                  routeProviderEvent(event, routeIdentity, state),
                );
              }),
            ),
            Stream.tap((event) =>
              Effect.gen(function* () {
                let storedEventCount = 0;
                const origin = readProviderEventOrigin(event);
                const owner = origin?.turn;
                if (origin?.derivation !== undefined) {
                  const d = origin.derivation;
                  const committed = yield* providerEventIngestor
                    .ingestNormalized({
                      providerSessionId: input.providerSessionId,
                      providerInstanceId: input.run.providerInstanceId,
                      threadId: input.run.threadId,
                      runId: input.run.id,
                      nodeId: input.rootNode.id,
                      event,
                      revalidateCurrentOwner: origin.producer.revalidateCurrent.pipe(
                        Effect.andThen(revalidateOutputRuntime(d.executor.binding)),
                      ),
                    })
                    .pipe(Effect.onError(() => Ref.set(assistantOutputHeld, true)));
                  if (committed.length === 0) return;
                  // Only the qualified committed family can clear child and pending-work tracking.
                  yield* Ref.update(eventRouting, (state) => ({
                    ...state,
                    ownedThreadIds: new Set([...state.ownedThreadIds, d.subject.childThreadId]),
                  }));
                  for (const { event: stored } of committed) {
                    if (stored.type === "node.updated") {
                      yield* trackChildLifecycle(
                        { type: "node.updated", driver: event.driver, node: stored.payload },
                        true,
                      );
                    } else if (stored.type === "subagent.updated") {
                      yield* trackChildLifecycle(
                        {
                          type: "subagent.updated",
                          driver: event.driver,
                          subagent: stored.payload,
                        },
                        true,
                      );
                    } else if (stored.type === "turn-item.updated") {
                      yield* trackChildLifecycle(
                        {
                          type: "turn_item.updated",
                          driver: event.driver,
                          turnItem: stored.payload,
                        },
                        true,
                      );
                    }
                  }
                  return;
                }
                const isRootAssistantOutput =
                  (event.type === "node.updated" &&
                    (event.node.kind === "assistant_message" || event.node.kind === "reasoning") &&
                    event.node.threadId === input.run.threadId &&
                    event.node.runId === input.run.id) ||
                  (event.type === "message.updated" &&
                    event.message.role === "assistant" &&
                    event.message.threadId === input.run.threadId &&
                    event.message.runId === input.run.id) ||
                  (event.type === "turn_item.updated" &&
                    (event.turnItem.type === "assistant_message" ||
                      event.turnItem.type === "reasoning") &&
                    event.turnItem.threadId === input.run.threadId &&
                    event.turnItem.runId === input.run.id);
                if (
                  owner !== undefined &&
                  owner.runId === input.run.id &&
                  owner.attemptId === input.attempt.id &&
                  isRootAssistantOutput
                ) {
                  yield* providerEventIngestor
                    .captureAssistantOutput({
                      providerSessionId: input.providerSessionId,
                      providerInstanceId: input.run.providerInstanceId,
                      threadId: input.run.threadId,
                      runId: input.run.id,
                      nodeId: input.rootNode.id,
                      event,
                      owner,
                      revalidateCurrentOwner: revalidateOutputRuntime(owner.binding),
                    })
                    .pipe(Effect.onError(() => Ref.set(assistantOutputHeld, true)));
                  yield* Ref.set(capturedAssistantOrigin, origin);
                }
                const terminalOutputTarget =
                  event.type === "turn.terminal"
                    ? event.providerTurnId
                    : event.type === "provider_turn.updated" &&
                        event.providerTurn.providerThreadId === input.providerThread.id &&
                        event.providerTurn.runAttemptId === input.attempt.id &&
                        ["completed", "interrupted", "failed", "cancelled"].includes(
                          event.providerTurn.status,
                        )
                      ? event.providerTurn.id
                      : undefined;
                const capturedOwner = (yield* Ref.get(capturedAssistantOrigin))?.turn;
                if (
                  terminalOutputTarget !== undefined &&
                  owner === undefined &&
                  capturedOwner !== undefined &&
                  (yield* providerEventIngestor.hasBufferedAssistantOutput({
                    binding: capturedOwner.binding,
                    providerTurnId: terminalOutputTarget,
                  }))
                ) {
                  yield* Ref.set(assistantOutputHeld, true);
                  return yield* Effect.fail(
                    new ProviderEventIngestor.ProviderEventPublishError({
                      providerSessionId: input.providerSessionId,
                      eventCount: 0,
                      cause:
                        "A terminal without captured native ownership cannot omit held assistant output.",
                    }),
                  );
                }
                if (owner !== undefined && terminalOutputTarget !== undefined) {
                  // Persist the full raw snapshot before terminal ingestion can settle the run.
                  yield* providerEventIngestor
                    .flushAssistantOutput({
                      binding: owner.binding,
                      providerTurnId: terminalOutputTarget,
                      revalidateCurrentOwner: origin!.producer.revalidateCurrent.pipe(
                        Effect.andThen(revalidateOutputRuntime(owner.binding)),
                      ),
                    })
                    .pipe(Effect.onError(() => Ref.set(assistantOutputHeld, true)));
                }
                const filteredEvent = filterAssistantEvent(
                  event,
                  DateTime.toEpochMillis(yield* DateTime.now),
                );
                const deliveredEvent =
                  filteredEvent === null ? null : copyProviderEventOrigin(event, filteredEvent);
                if (deliveredEvent) {
                  // Root provider_thread.updated always uses an ownership gate:
                  // pre-terminal writeIfRunCurrent (attempt still running), or
                  // post-terminal writeIfProviderThreadOwner so late roster
                  // clears still land while this attempt owns the run and this
                  // run owns lastRunOrdinal.
                  const rootTerminalAlreadySeen = yield* Ref.get(rootTerminalSeen);
                  const isRootProviderThreadUpdate =
                    event.type === "provider_thread.updated" &&
                    event.providerThread.id === input.providerThread.id;
                  const isRootProviderTurnUpdate =
                    !rootTerminalAlreadySeen &&
                    event.type === "provider_turn.updated" &&
                    event.providerTurn.runAttemptId === input.attempt.id &&
                    event.providerTurn.nodeId === input.rootNode.id &&
                    event.providerTurn.providerThreadId === input.providerThread.id &&
                    (event.threadId === undefined || event.threadId === input.run.threadId);
                  const storedEvents = yield* providerEventIngestor
                    .ingestNormalized({
                      analyticsContext: {
                        modelSelection: input.modelSelection,
                        runtimeMode: input.runtimePolicy.runtimeMode,
                        interactionMode: input.runtimePolicy.interactionMode,
                      },
                      providerSessionId: input.providerSessionId,
                      providerInstanceId: input.run.providerInstanceId,
                      threadId: input.run.threadId,
                      runId: input.run.id,
                      nodeId: input.rootNode.id,
                      event: deliveredEvent,
                      ...(origin === undefined
                        ? {}
                        : { revalidateCurrentOwner: origin.producer.revalidateCurrent }),
                      ...(isRootProviderThreadUpdate || isRootProviderTurnUpdate
                        ? rootTerminalAlreadySeen
                          ? {
                              writeIfProviderThreadOwner: {
                                providerThreadId: input.providerThread.id,
                                runId: input.run.id,
                                activeAttemptId: input.attempt.id,
                                expectedLastRunOrdinal: input.run.ordinal,
                              },
                            }
                          : {
                              writeIfRunCurrent: {
                                runId: input.run.id,
                                activeAttemptId: input.attempt.id,
                                expectedStatus: "running" as const,
                              },
                            }
                        : {}),
                    })
                    .pipe(
                      Effect.onError(() =>
                        isRootAssistantOutput ? Ref.set(assistantOutputHeld, true) : Effect.void,
                      ),
                    );
                  storedEventCount = storedEvents.length;
                  if (isRootProviderTurnUpdate) {
                    const acceptedAttempt = storedEvents
                      .map((stored) => stored.event)
                      .find(
                        (stored) =>
                          stored.type === "run-attempt.updated" &&
                          stored.payload.id === input.attempt.id &&
                          stored.payload.runId === input.run.id &&
                          stored.payload.rootNodeId === input.rootNode.id,
                      );
                    const acceptedRoot = storedEvents
                      .map((stored) => stored.event)
                      .find(
                        (stored) =>
                          stored.type === "node.updated" &&
                          stored.payload.id === input.rootNode.id &&
                          stored.payload.runId === input.run.id,
                      );
                    if (
                      acceptedAttempt?.type === "run-attempt.updated" &&
                      acceptedRoot?.type === "node.updated"
                    ) {
                      // Only committed companion rows may replace the preparation snapshots.
                      yield* Ref.set(acceptedPrimaryState, {
                        attempt: acceptedAttempt.payload,
                        rootNode: acceptedRoot.payload,
                      });
                    }
                  }
                  if (
                    isRootProviderThreadUpdate &&
                    rootTerminalAlreadySeen &&
                    storedEventCount === 0
                  ) {
                    // Ownership lost (or thread row missing). Stop pinning the
                    // stream on this run's background probe.
                    yield* Ref.set(providerThreadOwnerLost, true);
                  }
                }
                if (event.type === "provider_thread.updated") {
                  if (event.providerThread.id === input.providerThread.id && storedEventCount > 0) {
                    yield* Ref.set(latestProviderThread, event.providerThread);
                  }
                }
                if (
                  event.type === "turn_item.updated" &&
                  event.turnItem.providerTurnId ===
                    (yield* Ref.get(eventRouting)).rootProviderTurnId
                ) {
                  yield* Ref.update(latestTurnItemOrdinal, (current) =>
                    Math.max(current, event.turnItem.ordinal),
                  );
                }
                if (event.type === "turn.terminal") {
                  yield* Ref.set(terminalEvent, event);
                  yield* Ref.set(rootTerminalSeen, true);
                  yield* finalizeRootRun(event);
                }
                yield* trackChildLifecycle(event, deliveredEvent !== null);
              }),
            ),
            Stream.takeUntilEffect(() => shouldStopProviderEventIngestion),
            Stream.runDrain,
            Effect.mapError((cause) => new RunExecutionIngestError({ runId: input.run.id, cause })),
            Effect.flatMap(() =>
              Effect.gen(function* () {
                const terminal = yield* Ref.get(terminalEvent);
                if (terminal === null) {
                  return;
                }
                yield* finalizeRootRun(terminal);
              }),
            ),
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.failCause(cause)
                : Ref.get(rootRunFinalized).pipe(
                    Effect.flatMap((finalized) =>
                      Effect.logWarning("orchestration V2 provider event ingestion failed", {
                        runId: input.run.id,
                        cause,
                      }).pipe(
                        Effect.andThen(
                          Ref.get(assistantOutputHeld).pipe(
                            Effect.flatMap((outputHeld) =>
                              // A rejected output transaction must not be followed by a terminal write.
                              finalized || outputHeld
                                ? Effect.void
                                : Ref.get(latestProviderThread).pipe(
                                    Effect.flatMap((providerThread) =>
                                      Ref.get(latestTurnItemOrdinal).pipe(
                                        Effect.flatMap((latestItemOrdinal) =>
                                          Ref.get(openRunOwnedSubagents).pipe(
                                            Effect.flatMap((openSubagents) =>
                                              writeFinalRunEvents({
                                                ...(currentOrdinaryExecution === undefined
                                                  ? {}
                                                  : {
                                                      ordinaryCheckoutExecution:
                                                        currentOrdinaryExecution,
                                                    }),
                                                run: input.run,
                                                rootNode: input.rootNode,
                                                checkpointScope: input.checkpointScope,
                                                providerThread,
                                                attempt: input.attempt,
                                                acceptedPrimaryState: Ref.get(acceptedPrimaryState),
                                                ...(input.shouldFinalizeRun === undefined
                                                  ? {}
                                                  : { shouldFinalizeRun: input.shouldFinalizeRun }),
                                                ...(input.hasUnpairedRunInterruptRequest ===
                                                undefined
                                                  ? {}
                                                  : {
                                                      hasUnpairedRunInterruptRequest:
                                                        input.hasUnpairedRunInterruptRequest,
                                                    }),
                                                openRunOwnedSubagents: openSubagents,
                                                terminal: makeFailedTerminalEvent(
                                                  makeProviderFailure({
                                                    cause: Cause.squash(cause),
                                                    class: "unknown",
                                                  }),
                                                  latestItemOrdinal + 1,
                                                ),
                                                failureItemPersisted: false,
                                                refreshAfterTurn,
                                              }),
                                            ),
                                          ),
                                        ),
                                      ),
                                    ),
                                  ),
                            ),
                          ),
                        ),
                        Effect.mapError(
                          (writeCause) =>
                            new RunExecutionIngestError({
                              runId: input.run.id,
                              cause: { ingest: cause, write: writeCause },
                            }),
                        ),
                      ),
                    ),
                  ),
            ),
            Effect.ensuring(closeEventSubscription),
            Effect.forkIn(runScope),
          );

          if (
            input.shouldStartProviderTurn !== undefined &&
            !(yield* input.shouldStartProviderTurn())
          ) {
            yield* Fiber.interrupt(providerEventFiber);
            if (ordinaryStartExecution !== undefined) {
              yield* Scope.close(runScope, Exit.void);
              return yield* ordinaryFailure(
                new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                  reason: "unknown_use",
                  threadId: input.run.threadId,
                  path: ordinaryStartExecution.originalUse.lease.resourcePath,
                  message:
                    "The original run attempt was superseded after setup and before native dispatch.",
                }),
              );
            }
            return;
          }

          // A provider turn is a sign that the session is still alive. Keep
          // its already-issued MCP credential valid even when the agent goes
          // a long time between browser-tool calls.
          yield* McpSessionRegistry.touchActiveMcpThread(input.run.threadId);
          const nativeCompletion =
            ordinaryStartExecution === undefined ||
            input.prepareOrdinaryManagedActorRun === undefined
              ? undefined
              : yield* input
                  .prepareOrdinaryManagedActorRun({
                    startExecution: ordinaryStartExecution,
                    admission: ordinaryAdmission!,
                    checkpointScopeId: input.checkpointScope.id,
                    providerThreadId: input.providerThread.id,
                  })
                  .pipe(
                    Effect.mapError(ordinaryFailure),
                    Effect.onError(() => Scope.close(runScope, Exit.void)),
                  );
          const compact =
            input.message.attachments.length === 0 &&
            input.message.text.trim().toLowerCase() === "/compact";
          const nativeOperation: ProviderNativeOperationContext = {
            operationId:
              executionReference?.effectId ??
              input.importedHistoryStartExecution?.reference.effectId ??
              `${input.commandId}:${input.attemptId}:start`,
            operation: compact ? "compact_thread" : "start_turn",
            instanceId: input.modelSelection.instanceId,
            threadId: input.run.threadId,
            providerSessionId: input.providerSessionId,
            providerThreadId: input.providerThread.id,
            ...(input.session.runtimeGeneration === undefined
              ? {}
              : { runtimeGeneration: input.session.runtimeGeneration }),
            attemptId: input.attemptId,
          };
          const turnInput = {
            nativeOperation,
            appThread: input.appThread,
            threadId: input.run.threadId,
            runId: input.run.id,
            runOrdinal: input.run.ordinal,
            providerTurnOrdinal: input.providerTurnOrdinal,
            ...(input.run.restartContinuationOfRunId === undefined
              ? {}
              : {
                  restartContinuationOfRunId: input.run.restartContinuationOfRunId,
                }),
            attemptId: input.attemptId,
            rootNodeId: input.rootNode.id,
            providerThread: input.providerThread,
            message: input.message,
            modelSelection: input.modelSelection,
            runtimePolicy: input.runtimePolicy,
          };
          const startTurn = compact
            ? (input.session.compactThread?.(turnInput) ??
              Effect.fail(
                new ProviderAdapterTurnStartError({
                  driver: input.session.driver,
                  threadId: input.run.threadId,
                  providerThreadId: input.providerThread.id,
                  runId: input.run.id,
                  cause: "This provider does not support context compaction.",
                }),
              ))
            : input.session.startTurn(turnInput);
          if (ordinaryStartExecution !== undefined)
            yield* eventSink
              .revalidateOrdinaryCheckoutExecution(ordinaryStartExecution)
              .pipe(Effect.mapError(ordinaryFailure));
          let dispatchReturned = false;
          const actualDispatch =
            ordinaryStartExecution === undefined || nativeCompletion === undefined
              ? startTurn
              : ProviderManagedActorCompletion.withProviderManagedActorExecution(
                  ordinaryStartExecution,
                  startTurn,
                );
          yield* withProviderNativeEffect(actualDispatch, nativeOperation).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                dispatchReturned = true;
              }),
            ),
            Effect.catchCause((cause) => {
              const evidence = nativeEffectEvidenceFor(cause, nativeOperation);
              if (evidence.outcome === "confirmed_success")
                return Effect.sync(() => {
                  dispatchReturned = true;
                });
              if (evidence.outcome === "unknown")
                return Effect.fail(
                  new RunExecutionStartError({
                    commandId: input.commandId,
                    runId: input.run.id,
                    cause: new ProviderNativeOperationUnknownError({
                      nativeEffect: evidence,
                      cause,
                    }),
                  }),
                );
              return Effect.logError("orchestration V2 provider turn start failed", {
                runId: input.run.id,
                cause,
              }).pipe(
                Effect.andThen(Fiber.interrupt(providerEventFiber)),
                Effect.andThen(Ref.get(latestProviderThread)),
                Effect.flatMap((providerThread) =>
                  Ref.get(latestTurnItemOrdinal).pipe(
                    Effect.flatMap((latestItemOrdinal) =>
                      Ref.get(openRunOwnedSubagents).pipe(
                        Effect.flatMap((openSubagents) =>
                          writeFinalRunEvents({
                            ...(ordinaryStartExecution === undefined
                              ? {}
                              : {
                                  ordinaryCheckoutExecution: Effect.succeed(ordinaryStartExecution),
                                }),
                            run: input.run,
                            rootNode: input.rootNode,
                            checkpointScope: input.checkpointScope,
                            providerThread,
                            attempt: input.attempt,
                            acceptedPrimaryState: Ref.get(acceptedPrimaryState),
                            ...(input.shouldFinalizeRun === undefined
                              ? {}
                              : { shouldFinalizeRun: input.shouldFinalizeRun }),
                            ...(input.hasUnpairedRunInterruptRequest === undefined
                              ? {}
                              : {
                                  hasUnpairedRunInterruptRequest:
                                    input.hasUnpairedRunInterruptRequest,
                                }),
                            openRunOwnedSubagents: openSubagents,
                            terminal: makeFailedTerminalEvent(
                              makeProviderFailure({
                                cause: Cause.squash(cause),
                                class: "provider_error",
                              }),
                              latestItemOrdinal + 1,
                            ),
                            failureItemPersisted: false,
                            refreshAfterTurn,
                          }),
                        ),
                      ),
                    ),
                  ),
                ),
                Effect.mapError(
                  (writeCause) =>
                    new RunExecutionStartError({
                      commandId: input.commandId,
                      runId: input.run.id,
                      cause: { start: cause, write: writeCause },
                    }),
                ),
              );
            }),
            Effect.onError((cause) =>
              ordinaryStartExecution === undefined
                ? Effect.void
                : Ref.set(assistantOutputHeld, true).pipe(
                    Effect.andThen(Deferred.fail(activated, ordinaryFailure(Cause.squash(cause)))),
                    Effect.asVoid,
                  ),
            ),
          );
          if (ordinaryStartExecution === undefined) return;
          if (!dispatchReturned) {
            yield* Deferred.fail(
              activated,
              ordinaryFailure("The native dispatch did not return successfully."),
            );
            yield* Scope.close(runScope, Exit.void);
            return;
          }
          const captured = yield* input.captureOrdinaryAttachment!().pipe(
            Effect.mapError(ordinaryFailure),
          );
          const managedExecutor = freezeOrdinaryExecutionFacts(
            yield* Schema.decodeUnknownEffect(OrdinaryCheckout.OrdinaryCheckoutExecutionExecutorV1)(
              {
                kind: "captured_managed_run",
                captureId: captured.captureId,
                run: ordinaryAdmission!.run,
                checkpointScopeId: input.checkpointScope.id,
                driver: captured.driver,
                binding: {
                  threadId: captured.binding.threadId,
                  providerThreadId: captured.binding.providerThreadId,
                  providerSessionId: captured.binding.providerSessionId,
                  instanceId: captured.binding.instanceId,
                },
                ...(captured.binding.runtimeGeneration === undefined
                  ? {}
                  : { runtimeGeneration: captured.binding.runtimeGeneration }),
                ...(captured.binding.nativeThreadId === undefined
                  ? {}
                  : { nativeThreadId: captured.binding.nativeThreadId }),
                ...(captured.binding.evidenceRevision === undefined
                  ? {}
                  : { evidenceRevision: captured.binding.evidenceRevision }),
                ...(captured.providerTurnId === undefined
                  ? {}
                  : { providerTurnId: captured.providerTurnId }),
              },
            ).pipe(Effect.orDie),
          ) as OrdinaryManagedRunExecutorV1;
          if (
            captured.runId !== input.run.id ||
            captured.attemptId !== input.attemptId ||
            captured.binding.threadId !== input.run.threadId ||
            captured.binding.providerThreadId !== input.providerThread.id ||
            captured.binding.providerSessionId !== input.providerSessionId ||
            captured.binding.instanceId !== input.run.providerInstanceId
          )
            return yield* ordinaryFailure(
              "The returned dispatch capture differs from this exact run target.",
            );
          const revalidateMutation = Effect.suspend(
            (): Effect.Effect<void, RunExecutionStartError | ProviderSessionActivityError> =>
              !scopeCurrent || lossSignaled
                ? Effect.fail(ordinaryFailure("The captured run scope is no longer current."))
                : captured.revalidateCaptured,
          );
          const revalidateCompletionBinding = Effect.suspend(
            (): Effect.Effect<void, OrdinaryManagedRunRevalidationError> =>
              !scopeCurrent || lossSignaled
                ? Effect.fail(ordinaryFailure("The captured run completion binding was lost."))
                : nativeCompletion === undefined
                  ? revalidateMutation
                  : captured.revalidateCompletionBinding.pipe(
                      Effect.andThen(nativeCompletion.revalidateCompletionBinding),
                    ),
          );
          // The optional native producer can qualify historical completion; default runs keep their current captured owner.
          const revalidateCaptured =
            nativeCompletion === undefined
              ? revalidateMutation
              : revalidateMutation.pipe(Effect.catch(() => revalidateCompletionBinding));
          const startExecution = freezeOrdinaryExecutionFacts(
            OrdinaryCheckout.decodeOrdinaryCheckoutExecutionRefV1(
              yield* Schema.encodeEffect(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1)(
                ordinaryStartExecution,
              ).pipe(Effect.orDie),
            ),
          );
          const observation = Object.freeze({
            kind: "dispatch_returned" as const,
            settlementMode:
              input.prepareOrdinaryManagedActorRun === undefined
                ? ("primary_terminal_checkpoint" as const)
                : ("managed_actor_completion" as const),
            startExecution,
            managedExecutor,
            observedAt: DateTime.formatIso(yield* DateTime.now),
          });
          issueOrdinaryManagedRunStartObservation(observation, revalidateCaptured);
          return Object.freeze({
            startExecution,
            managedExecutor,
            actualStartObservation: observation,
            attachment: captured,
            revalidateCaptured,
            revalidateMutation,
            revalidateCompletionBinding,
            ...(nativeCompletion === undefined ? {} : { nativeCompletion }),
            requireActivatedExecution: Effect.suspend(() =>
              activatedRef === undefined
                ? Effect.fail(
                    ordinaryFailure("The actual managed activation has not been committed."),
                  )
                : Effect.succeed(activatedRef),
            ),
            activate: (ref: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1) =>
              Effect.gen(function* () {
                const expected = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
                  originalUse: startExecution.originalUse,
                  executor: managedExecutor,
                });
                if (
                  (yield* encodeOrdinaryCheckoutExecutionRefJson(ref)) !==
                  (yield* encodeOrdinaryCheckoutExecutionRefJson(expected))
                )
                  return yield* ordinaryFailure(
                    "Activation returned a different managed checkout participant.",
                  );
                yield* revalidateCaptured.pipe(Effect.mapError(ordinaryFailure));
                yield* eventSink
                  .revalidateOrdinaryCheckoutExecution(ref)
                  .pipe(Effect.mapError(ordinaryFailure));
                const retainedRef = freezeOrdinaryExecutionFacts(
                  OrdinaryCheckout.decodeOrdinaryCheckoutExecutionRefV1(
                    yield* Schema.encodeEffect(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1)(
                      ref,
                    ).pipe(Effect.orDie),
                  ),
                );
                yield* (
                  nativeCompletion === undefined
                    ? Effect.void
                    : nativeCompletion.bindManagedExecution(retainedRef)
                ).pipe(
                  Effect.mapError(ordinaryFailure),
                  Effect.andThen(
                    Effect.sync(() => {
                      activatedRef = retainedRef;
                    }),
                  ),
                  Effect.andThen(Deferred.succeed(activated, retainedRef)),
                  Effect.uninterruptible,
                );
              }),
            lose: (reason: string) =>
              Effect.gen(function* () {
                if (lossSignaled) return undefined;
                lossSignaled = true;
                yield* Deferred.fail(activated, ordinaryFailure(reason));
                return yield* captured
                  .stopCaptured({
                    operationId: `${startExecution.originalUse.operationId}:captured-loss`,
                  })
                  .pipe(Effect.ensuring(Scope.close(runScope, Exit.void)));
              }),
            awaitIngestionExit: Fiber.await(providerEventFiber),
            close: Scope.close(runScope, Exit.void),
          } satisfies OrdinaryManagedRunExecutionHandleV1);
        }),
    } satisfies RunExecutionServiceV2Shape);
  }),
);

function makeInterruptResultTurnItem(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2Shape;
  readonly run: OrchestrationV2Run;
  readonly rootNode: OrchestrationV2ExecutionNode;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly completedAt: DateTime.Utc;
}): OrchestrationV2TurnItem {
  return {
    id: input.idAllocator.derive.runSignalTurnItem({
      runId: input.run.id,
      signal: "interrupt-result",
    }),
    threadId: input.run.threadId,
    runId: input.run.id,
    nodeId: input.rootNode.id,
    providerThreadId: input.providerThread.id,
    providerTurnId: input.rootNode.providerTurnId,
    nativeItemRef: null,
    parentItemId: input.idAllocator.derive.runSignalTurnItem({
      runId: input.run.id,
      signal: "interrupt-request",
    }),
    ordinal: input.run.ordinal * 100 + 98,
    status: "interrupted",
    title: "Interrupted",
    startedAt: input.completedAt,
    completedAt: input.completedAt,
    updatedAt: input.completedAt,
    type: "run_interrupt_result",
    message: "Run interrupted by user",
  };
}
