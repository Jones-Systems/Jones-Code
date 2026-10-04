import type { OrchestrationV2HistoricalMessage } from "@t3tools/contracts";
import {
  ChatAttachment,
  CheckpointId,
  MessageId,
  ModelSelection,
  NodeId,
  OrchestrationV2AppThread,
  OrchestrationV2ConversationMessage,
  OrchestrationV2ExecutionNode,
  OrchestrationV2ProviderSession,
  OrchestrationV2PlanArtifact,
  OrchestrationV2ProviderCapabilities,
  OrchestrationV2ProviderFailure,
  OrchestrationV2ProviderRetry,
  OrchestrationV2ProviderThread,
  OrchestrationV2ProviderTurn,
  OrchestrationV2RuntimeRequest,
  OrchestrationV2Subagent,
  OrchestrationV2TurnItem,
  ProviderApprovalDecision,
  ProviderInteractionMode,
  ProviderDriverKind,
  ProviderInstanceId,
  PositiveInt,
  ProviderUserInputAnswers,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RuntimeMode,
  RuntimeRequestId,
  RuntimeIdentityAttestation,
  RunAttemptId,
  RunId,
  ThreadId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

import type { ProviderGoalReadResult } from "../provider/providerGoal.ts";
import type { NativeEffectConfirmationV1 } from "../persistence/Services/NativeCreationRepository.ts";
import {
  authorizeNativeCreationExecution,
  type NativeCreationExecutionContextV2,
  type NativeCreationResources,
} from "./NativeCreationAuthority.ts";

import type {
  ProviderSelectionTransitionInput,
  ProviderSelectionTransitionPlan,
} from "./ProviderSelectionTransition.ts";

export const ProviderNativeEffectOperation = Schema.Literals([
  "open_session",
  "close_session",
  "read_thread_snapshot",
  "ensure_thread",
  "resume_thread",
  "inject_history",
  "start_turn",
  "compact_thread",
  "steer_turn",
  "interrupt_turn",
  "respond_to_request",
  "unload_thread",
  "rollback_thread",
  "fork_thread",
]);
export type ProviderNativeEffectOperation = typeof ProviderNativeEffectOperation.Type;

export const ProviderNativeOperationContext = Schema.Struct({
  operationId: TrimmedNonEmptyString,
  operation: ProviderNativeEffectOperation,
  instanceId: Schema.optional(ProviderInstanceId),
  threadId: Schema.optional(ThreadId),
  providerSessionId: Schema.optional(ProviderSessionId),
  providerThreadId: Schema.optional(ProviderThreadId),
  runtimeGeneration: Schema.optional(TrimmedNonEmptyString),
  attemptId: Schema.optional(RunAttemptId),
});
export type ProviderNativeOperationContext = typeof ProviderNativeOperationContext.Type;

/**
 * Evidence covers the complete operation, including eager activation, lazy
 * initialization, registration and history injection before the final RPC.
 * Missing or mismatched evidence is unknown; a safe last request cannot prove
 * that an earlier stage had no effect.
 */
export const ProviderNativeEffectEvidence = Schema.Struct({
  ...ProviderNativeOperationContext.fields,
  outcome: Schema.Literals(["confirmed_success", "known_no_effect", "unknown"]),
});
export type ProviderNativeEffectEvidence = typeof ProviderNativeEffectEvidence.Type;

export interface ProviderDeclaredHandoffDelivery {
  readonly canConsumeHandoffSummaries: boolean;
  readonly supportsFullThreadHandoff: boolean;
  readonly supportsProviderSwitchingViaHandoff: boolean;
}

/** Declared delivery support only; this proves no native resume or store accessibility. */
export function makeProviderDeclaredHandoffDelivery(
  capabilities: OrchestrationV2ProviderCapabilities,
): ProviderDeclaredHandoffDelivery {
  return Object.freeze({
    canConsumeHandoffSummaries: capabilities.context.canConsumeHandoffSummaries,
    supportsFullThreadHandoff: capabilities.context.supportsFullThreadHandoff,
    supportsProviderSwitchingViaHandoff: capabilities.sessions.supportsProviderSwitchingViaHandoff,
  });
}

export const ProviderContinuationSourceIdentity = Schema.Struct({
  driverKind: ProviderDriverKind,
  continuationKey: TrimmedNonEmptyString,
  runtimeGeneration: TrimmedNonEmptyString,
});
export type ProviderContinuationSourceIdentity = typeof ProviderContinuationSourceIdentity.Type;

export const ProviderRuntimeBinding = Schema.Struct({
  threadId: ThreadId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
  instanceId: ProviderInstanceId,
  runtimeGeneration: TrimmedNonEmptyString,
  nativeThreadId: Schema.optional(TrimmedNonEmptyString),
});
export type ProviderRuntimeBinding = typeof ProviderRuntimeBinding.Type;

export interface ProviderPendingStartStopInput {
  readonly binding: ProviderRuntimeBinding & { readonly nativeThreadId: string };
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  /** Original start operation correlation; binding carries the actual dispatch incarnation. */
  readonly startOperation: ProviderNativeOperationContext;
}

/** Logical cancellation and provider-session cleanup only; no OS descendant-exit claim. */
export type ProviderPendingStartStopResult =
  | {
      readonly status: "not_pending";
      readonly binding: ProviderPendingStartStopInput["binding"];
      readonly runId: RunId;
      readonly attemptId: RunAttemptId;
      readonly startOperationId: string;
    }
  | {
      readonly status: "cancelled";
      readonly binding: ProviderPendingStartStopInput["binding"];
      readonly runId: RunId;
      readonly attemptId: RunAttemptId;
      readonly startOperationId: string;
    }
  | { readonly status: "unknown"; readonly reason: string };

export const ProviderOwnedRuntimeIdentity = Schema.Struct({
  instanceId: ProviderInstanceId,
  providerSessionId: ProviderSessionId,
  runtimeGeneration: TrimmedNonEmptyString,
  handleToken: TrimmedNonEmptyString,
  pid: Schema.Int,
});
export type ProviderOwnedRuntimeIdentity = typeof ProviderOwnedRuntimeIdentity.Type;

/** An actual numeric exit code proves leader exit only, not group, wake or drain completion. */
export const ProviderOwnedRuntimeExitObservation = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("leader_exited"),
    identity: ProviderOwnedRuntimeIdentity,
    exitCode: Schema.Int,
    observedAt: Schema.String,
  }),
  Schema.Struct({
    status: Schema.Literal("unknown"),
    reason: TrimmedNonEmptyString,
    identity: Schema.optional(ProviderOwnedRuntimeIdentity),
  }),
]);
export type ProviderOwnedRuntimeExitObservation = typeof ProviderOwnedRuntimeExitObservation.Type;

export const ProviderRuntimeObservation = Schema.Union([
  Schema.Struct({
    status: Schema.Literals(["working", "monitoring", "busy", "idle"]),
    binding: ProviderRuntimeBinding,
    observedAt: Schema.String,
  }),
  Schema.Struct({
    status: Schema.Literal("unknown"),
    binding: Schema.optional(ProviderRuntimeBinding),
    reason: Schema.String,
  }),
]);
export type ProviderRuntimeObservation = typeof ProviderRuntimeObservation.Type;

export const ProviderAdapterV2RuntimePolicy = Schema.Struct({
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  cwd: Schema.NullOr(Schema.String),
  approvalPolicy: Schema.optional(Schema.Unknown),
  sandboxPolicy: Schema.optional(Schema.Unknown),
  reasoningEffort: Schema.optional(Schema.String),
});
export type ProviderAdapterV2RuntimePolicy = typeof ProviderAdapterV2RuntimePolicy.Type;

export const ProviderAdapterV2TurnMessage = Schema.Struct({
  messageId: MessageId,
  text: Schema.String,
  attachments: Schema.Array(ChatAttachment),
  createdBy: OrchestrationV2ConversationMessage.fields.createdBy,
  creationSource: OrchestrationV2ConversationMessage.fields.creationSource,
  scheduledTaskId: OrchestrationV2ConversationMessage.fields.scheduledTaskId,
  senderThreadId: OrchestrationV2ConversationMessage.fields.senderThreadId,
});
export type ProviderAdapterV2TurnMessage = typeof ProviderAdapterV2TurnMessage.Type;

export const ProviderAdapterV2SessionStatus = Schema.Literals([
  "starting",
  "ready",
  "running",
  "waiting",
  "stopped",
  "error",
]);
export type ProviderAdapterV2SessionStatus = typeof ProviderAdapterV2SessionStatus.Type;

export const ProviderAdapterV2Event = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("runtime_identity.observed"),
    driver: ProviderDriverKind,
    binding: ProviderRuntimeBinding,
    attestation: RuntimeIdentityAttestation,
  }),
  Schema.Struct({
    type: Schema.Literal("app_thread.created"),
    driver: ProviderDriverKind,
    appThread: OrchestrationV2AppThread,
  }),
  Schema.Struct({
    type: Schema.Literal("provider_session.updated"),
    driver: ProviderDriverKind,
    providerSession: OrchestrationV2ProviderSession,
  }),
  Schema.Struct({
    type: Schema.Literal("provider_thread.updated"),
    driver: ProviderDriverKind,
    providerThread: OrchestrationV2ProviderThread,
  }),
  Schema.Struct({
    type: Schema.Literal("provider_turn.updated"),
    driver: ProviderDriverKind,
    threadId: Schema.optional(ThreadId),
    providerTurn: OrchestrationV2ProviderTurn,
  }),
  Schema.Struct({
    type: Schema.Literal("node.updated"),
    driver: ProviderDriverKind,
    node: OrchestrationV2ExecutionNode,
  }),
  Schema.Struct({
    type: Schema.Literal("subagent.updated"),
    driver: ProviderDriverKind,
    subagent: OrchestrationV2Subagent,
  }),
  Schema.Struct({
    type: Schema.Literal("message.updated"),
    driver: ProviderDriverKind,
    message: OrchestrationV2ConversationMessage,
  }),
  Schema.Struct({
    type: Schema.Literal("turn_item.updated"),
    driver: ProviderDriverKind,
    turnItem: OrchestrationV2TurnItem,
  }),
  Schema.Struct({
    type: Schema.Literal("runtime_request.updated"),
    driver: ProviderDriverKind,
    threadId: Schema.optional(ThreadId),
    runtimeRequest: OrchestrationV2RuntimeRequest,
  }),
  Schema.Struct({
    type: Schema.Literal("plan.updated"),
    driver: ProviderDriverKind,
    plan: OrchestrationV2PlanArtifact,
  }),
  Schema.Struct({
    type: Schema.Literal("turn.terminal"),
    driver: ProviderDriverKind,
    providerThreadId: ProviderThreadId,
    providerTurnId: ProviderTurnId,
    runOrdinal: PositiveInt,
    status: Schema.Literals(["completed", "interrupted", "cancelled"]),
    failure: Schema.Null,
    threadDisposition: Schema.Literals(["reusable", "broken"]),
  }),
  Schema.Struct({
    type: Schema.Literal("turn.terminal"),
    driver: ProviderDriverKind,
    providerThreadId: ProviderThreadId,
    providerTurnId: ProviderTurnId,
    runOrdinal: PositiveInt,
    failureItemOrdinal: PositiveInt,
    status: Schema.Literal("failed"),
    failure: OrchestrationV2ProviderFailure,
    retry: Schema.optional(OrchestrationV2ProviderRetry),
    retryStartedAt: Schema.optional(Schema.DateTimeUtc),
    threadDisposition: Schema.Literals(["reusable", "broken"]),
  }),
]);
export type ProviderAdapterV2Event = typeof ProviderAdapterV2Event.Type;

export class ProviderAdapterCapabilitiesError extends Schema.TaggedError<ProviderAdapterCapabilitiesError>()(
  "ProviderAdapterCapabilitiesError",
  {
    driver: ProviderDriverKind,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to read ${this.driver} provider capabilities.`;
  }
}

export class ProviderAdapterOpenSessionError extends Schema.TaggedError<ProviderAdapterOpenSessionError>()(
  "ProviderAdapterOpenSessionError",
  {
    driver: ProviderDriverKind,
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to open ${this.driver} provider session ${this.providerSessionId}.`;
  }
}

export class ProviderAdapterCloseSessionError extends Schema.TaggedError<ProviderAdapterCloseSessionError>()(
  "ProviderAdapterCloseSessionError",
  {
    driver: ProviderDriverKind,
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to close ${this.driver} provider session ${this.providerSessionId}.`;
  }
}

export class ProviderAdapterResumeThreadError extends Schema.TaggedError<ProviderAdapterResumeThreadError>()(
  "ProviderAdapterResumeThreadError",
  {
    driver: ProviderDriverKind,
    providerSessionId: ProviderSessionId,
    providerThreadId: ProviderThreadId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to resume ${this.driver} provider thread ${this.providerThreadId}.`;
  }
}

export class ProviderAdapterEnsureThreadError extends Schema.TaggedError<ProviderAdapterEnsureThreadError>()(
  "ProviderAdapterEnsureThreadError",
  {
    driver: ProviderDriverKind,
    threadId: ThreadId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to ensure ${this.driver} provider thread for app thread ${this.threadId}.`;
  }
}

export class ProviderAdapterReadThreadSnapshotError extends Schema.TaggedError<ProviderAdapterReadThreadSnapshotError>()(
  "ProviderAdapterReadThreadSnapshotError",
  {
    driver: ProviderDriverKind,
    providerThreadId: ProviderThreadId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to read ${this.driver} provider thread snapshot ${this.providerThreadId}.`;
  }
}

export class ProviderAdapterRollbackThreadError extends Schema.TaggedError<ProviderAdapterRollbackThreadError>()(
  "ProviderAdapterRollbackThreadError",
  {
    driver: ProviderDriverKind,
    providerThreadId: ProviderThreadId,
    checkpointId: Schema.optional(CheckpointId),
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to roll back ${this.driver} provider thread ${this.providerThreadId}.`;
  }
}

export class ProviderAdapterForkThreadError extends Schema.TaggedError<ProviderAdapterForkThreadError>()(
  "ProviderAdapterForkThreadError",
  {
    driver: ProviderDriverKind,
    providerThreadId: ProviderThreadId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to fork ${this.driver} provider thread ${this.providerThreadId}.`;
  }
}

export class ProviderAdapterTurnStartError extends Schema.TaggedError<ProviderAdapterTurnStartError>()(
  "ProviderAdapterTurnStartError",
  {
    driver: ProviderDriverKind,
    threadId: ThreadId,
    providerThreadId: ProviderThreadId,
    runId: RunId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to start run ${this.runId} on ${this.driver} provider thread ${this.providerThreadId}.`;
  }
}

export class ProviderAdapterSteerRunUnsupportedError extends Schema.TaggedError<ProviderAdapterSteerRunUnsupportedError>()(
  "ProviderAdapterSteerRunUnsupportedError",
  {
    driver: ProviderDriverKind,
    providerThreadId: ProviderThreadId,
  },
) {
  override get message(): string {
    return `${this.driver} provider thread ${this.providerThreadId} does not support active-run steering.`;
  }
}

export class ProviderAdapterSteerRunError extends Schema.TaggedError<ProviderAdapterSteerRunError>()(
  "ProviderAdapterSteerRunError",
  {
    driver: ProviderDriverKind,
    providerThreadId: ProviderThreadId,
    providerTurnId: ProviderTurnId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to steer active run ${this.providerTurnId} on ${this.driver} provider thread ${this.providerThreadId}.`;
  }
}

export class ProviderAdapterInterruptError extends Schema.TaggedError<ProviderAdapterInterruptError>()(
  "ProviderAdapterInterruptError",
  {
    driver: ProviderDriverKind,
    providerThreadId: ProviderThreadId,
    providerTurnId: ProviderTurnId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to interrupt ${this.driver} provider turn ${this.providerTurnId}.`;
  }
}

export class ProviderAdapterRuntimeRequestResponseError extends Schema.TaggedError<ProviderAdapterRuntimeRequestResponseError>()(
  "ProviderAdapterRuntimeRequestResponseError",
  {
    driver: ProviderDriverKind,
    requestId: RuntimeRequestId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed to respond to ${this.driver} runtime request ${this.requestId}.`;
  }
}

export class ProviderAdapterEventStreamError extends Schema.TaggedError<ProviderAdapterEventStreamError>()(
  "ProviderAdapterEventStreamError",
  {
    driver: ProviderDriverKind,
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
  },
) {
  override get message(): string {
    return `Failed while streaming ${this.driver} provider session ${this.providerSessionId} events.`;
  }
}

export class ProviderAdapterProtocolError extends Schema.TaggedError<ProviderAdapterProtocolError>()(
  "ProviderAdapterProtocolError",
  {
    driver: ProviderDriverKind,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
    nativeEffect: Schema.optional(ProviderNativeEffectEvidence),
    payload: Schema.optional(Schema.Unknown),
  },
) {
  override get message(): string {
    return `${this.driver} provider protocol error: ${this.detail}.`;
  }
}

export const ProviderAdapterV2Error = Schema.Union([
  ProviderAdapterCapabilitiesError,
  ProviderAdapterOpenSessionError,
  ProviderAdapterCloseSessionError,
  ProviderAdapterResumeThreadError,
  ProviderAdapterEnsureThreadError,
  ProviderAdapterReadThreadSnapshotError,
  ProviderAdapterRollbackThreadError,
  ProviderAdapterForkThreadError,
  ProviderAdapterTurnStartError,
  ProviderAdapterSteerRunUnsupportedError,
  ProviderAdapterSteerRunError,
  ProviderAdapterInterruptError,
  ProviderAdapterRuntimeRequestResponseError,
  ProviderAdapterEventStreamError,
  ProviderAdapterProtocolError,
]);
export type ProviderAdapterV2Error = typeof ProviderAdapterV2Error.Type;

/** Recheck the current opaque grant with unchanged resources at the actual native callee. */
export function authorizeProviderNativeCreation(
  execution: ProviderAdapterV2OpenSessionInput["nativeCreationExecution"],
  driver: ProviderDriverKind,
  actualDirectory: string | null | undefined,
): Effect.Effect<void, ProviderAdapterV2Error> {
  if (execution === undefined) return Effect.void;
  if (
    actualDirectory == null ||
    actualDirectory.trim().length === 0 ||
    actualDirectory !== execution.resources.worktreePath
  ) {
    return Effect.fail(
      new ProviderAdapterProtocolError({
        driver,
        detail: "Native creation resources do not match the actual runtime directory.",
      }),
    );
  }
  return authorizeNativeCreationExecution(execution.context, {
    stage: "native_command",
    resources: execution.resources,
  }).pipe(
    Effect.asVoid,
    Effect.mapError(
      (cause) =>
        new ProviderAdapterProtocolError({
          driver,
          detail: "Native creation authorization failed at the actual native callee.",
          cause,
        }),
    ),
  );
}

/** Wrap the complete exposed operation, never just its final native request. */
export function withProviderNativeEffect<A, E extends ProviderAdapterV2Error, R>(
  effect: Effect.Effect<A, E, R>,
  nativeOperation: ProviderNativeOperationContext | undefined,
): Effect.Effect<A, E, R> {
  if (nativeOperation === undefined) return effect;
  return effect.pipe(
    Effect.mapError((error) => {
      const evidence = "nativeEffect" in error ? error.nativeEffect : undefined;
      if (
        Schema.is(ProviderNativeEffectEvidence)(evidence) &&
        evidence.operationId === nativeOperation.operationId &&
        evidence.operation === nativeOperation.operation &&
        (
          [
            "instanceId",
            "threadId",
            "providerSessionId",
            "providerThreadId",
            "runtimeGeneration",
            "attemptId",
          ] as const
        ).every(
          (key) => nativeOperation[key] === undefined || nativeOperation[key] === evidence[key],
        )
      ) {
        return error;
      }
      const enriched = Object.create(
        Object.getPrototypeOf(error),
        Object.getOwnPropertyDescriptors(error),
      ) as E;
      Object.defineProperty(enriched, "nativeEffect", {
        value: { ...nativeOperation, outcome: "unknown" },
        enumerable: true,
        configurable: true,
        writable: true,
      });
      return enriched;
    }),
  );
}

export interface ProviderAdapterV2OpenSessionInput {
  readonly nativeOperation?: ProviderNativeOperationContext;
  /**
   * Runtime-only opaque context and exact issued resources. Retain for lazy
   * replacements and reauthorize immediately before each native stage effect;
   * this bundle is not serialized and grants no other stage or resource.
   */
  readonly nativeCreationExecution?: {
    readonly context: NativeCreationExecutionContextV2;
    readonly resources: NativeCreationResources;
  };
  /**
   * Await before native spawn, connect or query effects for each reserved
   * incarnation, including lazy replacements. The manager fences old registered
   * evidence; allocating or registering this generation does not attest a live
   * process. New observations still require the actual new native handle/query.
   */
  readonly beforeRuntimeReplacement?: (
    nextGeneration: string,
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
  /**
   * Managed internal refresh only. The manager reserves the single resident
   * owner and excludes new attachments through replacement and registration.
   * The supplied effect fences old evidence before closing or opening a native
   * handle, then initializes and restores its native binding. Existing unknown
   * effects never authorize replay; this reservation is not live-process proof.
   */
  readonly withRuntimeReplacement?: (
    nextGeneration: string,
    replace: Effect.Effect<void, ProviderAdapterV2Error>,
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
  /**
   * Reserve the exact single resident owner before cancelling an unreturned
   * start and closing its captured incarnation. Exclude attachments and reuse
   * through cleanup; this close-only span requires no new initialized runtime.
   * Missing ownership leaves the pending native operation unknown.
   */
  readonly withPendingStartStop?: (
    input: ProviderPendingStartStopInput,
    stop: Effect.Effect<void, ProviderAdapterV2Error>,
  ) => Effect.Effect<ProviderPendingStartStopResult, ProviderAdapterV2Error>;
  readonly threadId: ThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly modelSelection: ModelSelection;
  readonly runtimePolicy: ProviderAdapterV2RuntimePolicy;
  readonly resumeFromSession?: OrchestrationV2ProviderSession;
  /** Native thread to activate while an eager adapter opens its provider process. */
  readonly initialNativeThreadId?: string;
  /** Preserves provider item identity across eager activation of a persisted thread. */
  readonly initialProviderItemIdentityVersion?: 2;
}

export interface ProviderAdapterV2EnsureThreadInput {
  readonly nativeOperation?: ProviderNativeOperationContext;
  /** Current operation only; a prior caller's open context cannot authorize this mutation. */
  readonly nativeCreationExecution?: {
    readonly context: NativeCreationExecutionContextV2;
    readonly resources: NativeCreationResources;
  };
  readonly threadId: ThreadId;
  readonly modelSelection: ModelSelection;
  readonly runtimePolicy: ProviderAdapterV2RuntimePolicy;
  readonly providerSessionId?: ProviderSessionId;
  readonly existingProviderThread?: OrchestrationV2ProviderThread;
}

export interface ProviderAdapterV2TurnInput {
  readonly nativeOperation?: ProviderNativeOperationContext;
  /** Current configured default for native option resolution; never persisted as the user's selection. */
  readonly configuredDefaultModelSelection?: {
    readonly modelSelection: ModelSelection;
    readonly driver: ProviderDriverKind;
  };
  /** Current operation only; a prior caller's open context cannot authorize this mutation. */
  readonly nativeCreationExecution?: {
    readonly context: NativeCreationExecutionContextV2;
    readonly resources: NativeCreationResources;
  };
  readonly appThread: OrchestrationV2AppThread;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly runOrdinal: number;
  readonly providerTurnOrdinal: number;
  readonly restartContinuationOfRunId?: RunId;
  readonly attemptId: RunAttemptId;
  readonly rootNodeId: NodeId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly message: ProviderAdapterV2TurnMessage;
  readonly modelSelection: ModelSelection;
  readonly runtimePolicy: ProviderAdapterV2RuntimePolicy;
}

export interface ProviderAdapterV2SteerInput {
  readonly nativeOperation?: ProviderNativeOperationContext;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly providerTurnId: ProviderTurnId;
  readonly message: ProviderAdapterV2TurnMessage;
}

export interface ProviderAdapterV2InterruptInput {
  readonly nativeOperation?: ProviderNativeOperationContext;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly providerTurnId: ProviderTurnId;
  /** When true, the next `startTurn` may respawn the provider runtime (Grok Stop recovery). */
  readonly requestRuntimeRestart?: boolean;
}

export interface ProviderAdapterV2RuntimeRequestResponseInput {
  readonly nativeOperation?: ProviderNativeOperationContext;
  readonly requestId: RuntimeRequestId;
  readonly decision?: ProviderApprovalDecision;
  readonly answers?: ProviderUserInputAnswers;
  readonly response?: unknown;
}

export interface ProviderAdapterV2ThreadSnapshot {
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
  readonly messages: ReadonlyArray<OrchestrationV2ConversationMessage>;
  readonly runtimeRequests: ReadonlyArray<OrchestrationV2RuntimeRequest>;
  readonly providerPayload?: unknown;
}

export interface ProviderAdapterV2ReadThreadSnapshotInput {
  readonly nativeOperation?: ProviderNativeOperationContext;
  readonly providerThread: OrchestrationV2ProviderThread;
}

export type ProviderAdapterV2RollbackTarget =
  | {
      readonly type: "thread_start";
      readonly checkpointId: CheckpointId;
      readonly appRunOrdinal: 0;
    }
  | {
      readonly type: "provider_turn";
      readonly checkpointId: CheckpointId;
      readonly appRunOrdinal: number;
      readonly providerTurn: OrchestrationV2ProviderTurn;
    };

export interface ProviderAdapterV2RollbackThreadInput {
  readonly nativeOperation?: ProviderNativeOperationContext;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly target: ProviderAdapterV2RollbackTarget;
  readonly providerThreadTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
}

export interface ProviderAdapterV2ForkThreadInput {
  readonly nativeOperation?: ProviderNativeOperationContext;
  readonly sourceProviderThread: OrchestrationV2ProviderThread;
  readonly sourceProviderTurns?: ReadonlyArray<OrchestrationV2ProviderTurn>;
  readonly providerTurnId?: ProviderTurnId;
  readonly targetThreadId: ThreadId;
  readonly ownerNodeId?: NodeId;
  readonly modelSelection?: ModelSelection;
  readonly runtimePolicy?: ProviderAdapterV2RuntimePolicy;
}

export interface ProviderAdapterV2EventSubscription {
  readonly events: Stream.Stream<ProviderAdapterV2Event, ProviderAdapterV2Error>;
  readonly close: Effect.Effect<void>;
}

export interface ProviderAdapterV2HistoricalContext {
  readonly messages: ReadonlyArray<OrchestrationV2HistoricalMessage>;
  readonly context: string;
}

export interface ProviderAdapterV2SessionRuntime {
  /** Incarnation correlation; allocating it alone does not prove a live native handle. */
  readonly runtimeGeneration?: string;
  /** Frozen identity of the actual owned handle; retaining it alone proves no live process. */
  readonly ownedRuntimeIdentity?: ProviderOwnedRuntimeIdentity | undefined;
  /**
   * Read only the captured handle, checking the exact expected identity before
   * and after observation, including after scope closure. Missing ownership,
   * mismatches and read failures are unknown; leader exit is not a STOP proof.
   */
  readonly observeOwnedRuntimeExit?: (
    expected: ProviderOwnedRuntimeIdentity,
  ) => Effect.Effect<ProviderOwnedRuntimeExitObservation>;
  /**
   * Immutable source-store provenance for one successfully initialized native
   * incarnation. Absent before initialize/query success and during replacement
   * reservation; persist only with a native-bound tuple matching its generation.
   * This internal identity does not attest a live handle or grant store access.
   */
  readonly continuationSourceIdentity?: ProviderContinuationSourceIdentity | undefined;
  /**
   * Notify only after committed confirmation readback and current binding,
   * effect, attempt and issuer-reference checks. This conveys correlation, not
   * execution permission; remove only the matching covered-unresolved record.
   * Missing or mismatched proof retains unresolved state and does not gate reads.
   */
  readonly onNativeEffectConfirmed?: (input: {
    readonly context: NativeCreationExecutionContextV2;
    readonly confirmation: NativeEffectConfirmationV1;
  }) => Effect.Effect<void, ProviderAdapterV2Error>;
  readonly observeThreadRuntime?: (
    binding: ProviderRuntimeBinding,
  ) => Effect.Effect<ProviderRuntimeObservation, ProviderAdapterV2Error>;
  readonly getGoal?: (
    binding: ProviderRuntimeBinding,
  ) => Effect.Effect<ProviderGoalReadResult, ProviderAdapterV2Error>;
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly providerSessionId: ProviderSessionId;
  readonly providerSession: OrchestrationV2ProviderSession;
  readonly events: Stream.Stream<ProviderAdapterV2Event, ProviderAdapterV2Error>;
  /**
   * Manager-owned runtimes expose a synchronous subscription so concurrent
   * provider threads receive independent copies of the process event stream.
   * Adapter runtimes may omit this and expose only their single-consumer event stream.
   */
  readonly subscribeEvents?: Effect.Effect<ProviderAdapterV2EventSubscription>;
  /**
   * Adapters whose native runtime can hold pending work outside an active
   * turn (for example Claude background tasks and their wake turns) report it
   * here so the session manager defers idle release while it is pending.
   */
  readonly hasPendingBackgroundWork?: Effect.Effect<boolean>;
  /**
   * Per-provider-thread pending work for root-run ingestion stop gates. When
   * present, RunExecutionService uses only this probe (never the session-wide
   * hasPendingBackgroundWork) so sibling native threads cannot pin an
   * unrelated root subscription open.
   */
  readonly hasPendingBackgroundWorkForThread?: (
    providerThread: OrchestrationV2ProviderThread,
  ) => Effect.Effect<boolean>;
  /**
   * Capacity for the requested model/options, independent of native thread usage.
   * `cwd` is the thread's working directory, for providers whose project config
   * can change a model's limits.
   */
  readonly getModelContextWindow?: (
    modelSelection: ModelSelection,
    cwd?: string | null,
  ) => number | undefined;
  /** Whether an option-only change preserves measured native usage and capacity.
   * Compaction thresholds are still discarded. Unknown transitions invalidate usage.
   */
  readonly canReuseContextUsage?: (previous: ModelSelection, next: ModelSelection) => boolean;
  readonly ensureThread: (
    input: ProviderAdapterV2EnsureThreadInput,
  ) => Effect.Effect<OrchestrationV2ProviderThread, ProviderAdapterV2Error>;
  readonly resumeThread: (input: {
    readonly nativeOperation?: ProviderNativeOperationContext;
    /** Current operation only; a prior caller's open context cannot authorize this mutation. */
    readonly nativeCreationExecution?: {
      readonly context: NativeCreationExecutionContextV2;
      readonly resources: NativeCreationResources;
    };
    /**
     * Await after actual native initialization and source capture, before native
     * resume or attach. The manager validates historical driver/key against the
     * current initialized incarnation and rechecks current binding/run authority;
     * a historical process generation need not equal the new generation.
     * Rejection after open/initialize leaves the complete operation unknown;
     * skipping the resume RPC alone does not prove no effect or allow fallback.
     */
    readonly beforeNativeResume?: (
      actual: ProviderContinuationSourceIdentity | undefined,
    ) => Effect.Effect<void, ProviderAdapterV2Error>;
    readonly providerThread: OrchestrationV2ProviderThread;
    readonly threadId?: ThreadId;
    readonly modelSelection?: ModelSelection;
    readonly runtimePolicy?: ProviderAdapterV2RuntimePolicy;
  }) => Effect.Effect<OrchestrationV2ProviderThread, ProviderAdapterV2Error>;
  /** False means the native protocol explicitly does not support history injection. */
  readonly injectHistory?: (
    input: ProviderAdapterV2HistoricalContext & {
      readonly nativeOperation?: ProviderNativeOperationContext;
      /** Current operation only; a prior caller's open context cannot authorize this mutation. */
      readonly nativeCreationExecution?: {
        readonly context: NativeCreationExecutionContextV2;
        readonly resources: NativeCreationResources;
      };
      readonly providerThread: OrchestrationV2ProviderThread;
    },
  ) => Effect.Effect<boolean, ProviderAdapterV2Error>;
  readonly startTurn: (
    input: ProviderAdapterV2TurnInput,
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
  readonly compactThread?: (
    input: ProviderAdapterV2TurnInput,
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
  readonly steerTurn: (
    input: ProviderAdapterV2SteerInput,
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
  readonly interruptTurn: (
    input: ProviderAdapterV2InterruptInput,
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
  /**
   * Stop an actually dispatched start before its native turn ID is returned.
   * Match the original operation ID and run/attempt against the retained
   * request, and the binding against its actual dispatch generation. A managed
   * refresh may have changed generation since the original context was issued.
   * Closing an incarnation requires withPendingStartStop; absent means unknown.
   */
  readonly stopPendingStart?: (
    input: ProviderPendingStartStopInput,
  ) => Effect.Effect<ProviderPendingStartStopResult, ProviderAdapterV2Error>;
  /**
   * Lets a runtime shared by several app threads unload one provider thread's
   * native state (and its MCP servers) when that app thread detaches, while
   * the runtime keeps serving the others. A later resume reloads it.
   */
  readonly unloadThread?: (input: {
    readonly nativeOperation?: ProviderNativeOperationContext;
    readonly providerThread: OrchestrationV2ProviderThread;
  }) => Effect.Effect<void, ProviderAdapterV2Error>;
  readonly respondToRuntimeRequest: (
    input: ProviderAdapterV2RuntimeRequestResponseInput,
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
  readonly readThreadSnapshot: (
    input: ProviderAdapterV2ReadThreadSnapshotInput,
  ) => Effect.Effect<ProviderAdapterV2ThreadSnapshot, ProviderAdapterV2Error>;
  /**
   * Providers that accept product feedback for a thread (#7949, Codex → OpenAI)
   * expose it here; absent means the driver has no feedback channel.
   */
  readonly uploadFeedback?: (input: {
    readonly providerThread: OrchestrationV2ProviderThread;
    readonly reason?: string;
  }) => Effect.Effect<{ readonly feedbackId: string }, ProviderAdapterV2Error>;
  readonly rollbackThread: (
    input: ProviderAdapterV2RollbackThreadInput,
  ) => Effect.Effect<ProviderAdapterV2ThreadSnapshot, ProviderAdapterV2Error>;
  readonly forkThread: (
    input: ProviderAdapterV2ForkThreadInput,
  ) => Effect.Effect<OrchestrationV2ProviderThread, ProviderAdapterV2Error>;
}

/** Preserve incarnation getters while adding conservative whole-operation error evidence. */
export function withProviderNativeEffects(
  runtime: ProviderAdapterV2SessionRuntime,
): ProviderAdapterV2SessionRuntime {
  const injectHistory = runtime.injectHistory;
  const compactThread = runtime.compactThread;
  const unloadThread = runtime.unloadThread;
  const methods = {
    ensureThread: (input: ProviderAdapterV2EnsureThreadInput) =>
      withProviderNativeEffect(runtime.ensureThread(input), input.nativeOperation),
    resumeThread: (input: Parameters<ProviderAdapterV2SessionRuntime["resumeThread"]>[0]) =>
      withProviderNativeEffect(runtime.resumeThread(input), input.nativeOperation),
    startTurn: (input: ProviderAdapterV2TurnInput) =>
      withProviderNativeEffect(runtime.startTurn(input), input.nativeOperation),
    steerTurn: (input: ProviderAdapterV2SteerInput) =>
      withProviderNativeEffect(runtime.steerTurn(input), input.nativeOperation),
    interruptTurn: (input: ProviderAdapterV2InterruptInput) =>
      withProviderNativeEffect(runtime.interruptTurn(input), input.nativeOperation),
    respondToRuntimeRequest: (input: ProviderAdapterV2RuntimeRequestResponseInput) =>
      withProviderNativeEffect(runtime.respondToRuntimeRequest(input), input.nativeOperation),
    readThreadSnapshot: (input: ProviderAdapterV2ReadThreadSnapshotInput) =>
      withProviderNativeEffect(runtime.readThreadSnapshot(input), input.nativeOperation),
    rollbackThread: (input: ProviderAdapterV2RollbackThreadInput) =>
      withProviderNativeEffect(runtime.rollbackThread(input), input.nativeOperation),
    forkThread: (input: ProviderAdapterV2ForkThreadInput) =>
      withProviderNativeEffect(runtime.forkThread(input), input.nativeOperation),
    ...(injectHistory === undefined
      ? {}
      : {
          injectHistory: (input: Parameters<typeof injectHistory>[0]) =>
            withProviderNativeEffect(injectHistory.call(runtime, input), input.nativeOperation),
        }),
    ...(compactThread === undefined
      ? {}
      : {
          compactThread: (input: ProviderAdapterV2TurnInput) =>
            withProviderNativeEffect(compactThread.call(runtime, input), input.nativeOperation),
        }),
    ...(unloadThread === undefined
      ? {}
      : {
          unloadThread: (input: Parameters<typeof unloadThread>[0]) =>
            withProviderNativeEffect(unloadThread.call(runtime, input), input.nativeOperation),
        }),
  };
  return Object.create(Object.getPrototypeOf(runtime), {
    ...Object.getOwnPropertyDescriptors(runtime),
    ...Object.getOwnPropertyDescriptors(methods),
  }) as ProviderAdapterV2SessionRuntime;
}

export interface ProviderAdapterV2Shape {
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  /** Factory-captured declarations; reading them performs no native capability probe. */
  readonly declaredHandoffDelivery?: ProviderDeclaredHandoffDelivery | undefined;
  readonly getCapabilities: () => Effect.Effect<
    OrchestrationV2ProviderCapabilities,
    ProviderAdapterV2Error
  >;
  readonly planSelectionTransition: (
    input: ProviderSelectionTransitionInput,
  ) => Effect.Effect<ProviderSelectionTransitionPlan, ProviderAdapterV2Error>;
  readonly openSession: (
    input: ProviderAdapterV2OpenSessionInput,
  ) => Effect.Effect<ProviderAdapterV2SessionRuntime, ProviderAdapterV2Error, Scope.Scope>;
}

export class ProviderAdapterV2 extends Context.Service<ProviderAdapterV2, ProviderAdapterV2Shape>()(
  "t3/orchestration-v2/ProviderAdapter/ProviderAdapterV2",
) {}
