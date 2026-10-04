import {
  CheckpointId,
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  ProviderSessionId,
  RunAttemptId,
  ProviderApprovalDecision,
  ProviderUserInputAnswers,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  OrchestrationV2StartWithImportedHistoryCommand,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaIssue from "effect/SchemaIssue";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { decodeJson, decodeTaskKind, jsonCause } from "./EventSinkJsonCodec.ts";
import { ProviderNativeEffectEvidence } from "./ProviderAdapter.ts";
import { NativeCreationExecutionReferenceV2 } from "./NativeCreationAuthority.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";
import {
  ImportedApplicationAttachmentRetentionEvidenceV1,
  makeImportedApplicationAttachmentRetentionEvidenceV1,
} from "./ImportedApplicationAttachmentInventory.ts";

export const OrchestrationEffectRequestV2 = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("provider-runtime.continue"),
    sourceRunId: RunId,
  }),
  Schema.Struct({
    type: Schema.Literal("provider-session.detach"),
    providerSessionId: ProviderSessionId,
    detail: Schema.optional(Schema.String),
    /** Set on terminal detaches (thread archive/delete): revoke the thread's MCP credentials. */
    revokeMcpCredential: Schema.optional(Schema.Boolean),
  }),
  Schema.Struct({
    type: Schema.Literal("provider-turn.start"),
    runId: RunId,
  }),
  Schema.Struct({
    type: Schema.Literal("provider-turn.interrupt"),
    providerSessionId: ProviderSessionId,
    providerThreadId: ProviderThreadId,
    providerTurnId: ProviderTurnId,
  }),
  Schema.Struct({
    type: Schema.Literal("provider-turn.steer"),
    providerSessionId: ProviderSessionId,
    providerThreadId: ProviderThreadId,
    providerTurnId: ProviderTurnId,
    messageId: MessageId,
  }),
  Schema.Struct({
    type: Schema.Literal("provider-turn.restart"),
    providerSessionId: ProviderSessionId,
    providerThreadId: ProviderThreadId,
    providerTurnId: ProviderTurnId,
    interruptedAttemptId: RunAttemptId,
    runId: RunId,
    sessionTransition: Schema.optional(
      Schema.Union([
        Schema.Struct({
          type: Schema.Literal("replace"),
          replacementProviderSessionId: ProviderSessionId,
        }),
        Schema.Struct({ type: Schema.Literal("detach") }),
      ]),
    ),
  }),
  Schema.Struct({
    type: Schema.Literal("runtime-request.respond"),
    providerSessionId: ProviderSessionId,
    requestId: RuntimeRequestId,
    decision: Schema.optional(ProviderApprovalDecision),
    answers: Schema.optional(ProviderUserInputAnswers),
  }),
  Schema.Struct({
    type: Schema.Literal("provider-thread.rollback"),
    restoreFiles: Schema.optional(Schema.Boolean),
    providerThreadId: ProviderThreadId,
    checkpointId: CheckpointId,
    scopeId: CheckpointScopeId,
  }),
  Schema.Struct({
    type: Schema.Literal("checkpoint.capture"),
    runId: RunId,
    scopeId: CheckpointScopeId,
  }),
  Schema.Struct({
    type: Schema.Literal("terminal.cleanup"),
  }),
  Schema.Struct({
    type: Schema.Literal("worktree.cleanup"),
  }),
  Schema.Struct({
    type: Schema.Literal("attachment.cleanup"),
    attachmentIds: Schema.Array(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("thread-title.generate"),
    kind: Schema.Union([
      Schema.Struct({ type: Schema.Literal("initial"), messageId: MessageId }),
      Schema.Struct({ type: Schema.Literal("regenerate") }),
    ]),
  }),
]);
export type OrchestrationEffectRequestV2 = typeof OrchestrationEffectRequestV2.Type;

export const NativeOrchestrationEffectPayloadV2 = Schema.Struct({
  request: OrchestrationEffectRequestV2,
  nativeCreationExecutionReference: NativeCreationExecutionReferenceV2,
});

const decodeNativePayload = Schema.decodeUnknownEffect(NativeOrchestrationEffectPayloadV2, {
  onExcessProperty: "error",
});
const encodeNativePayload = Schema.encodeSync(
  Schema.fromJsonString(NativeOrchestrationEffectPayloadV2),
);

const AttachmentNamespaceOwnerBirthV1 = Schema.Struct({
  kind: Schema.Literal("application_v2_thread_birth"),
  threadId: ThreadId,
  eventId: EventId,
  sequence: Schema.Int.check(Schema.isGreaterThan(0)),
});
export const AttachmentNamespaceCleanupReferenceV1 = Schema.Union([
  Schema.Struct({
    version: Schema.Literal(1),
    mode: Schema.Literal("delete_thread"),
    ownerBirth: AttachmentNamespaceOwnerBirthV1,
    triggerEventId: EventId,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    mode: Schema.Literal("prune_thread"),
    ownerBirth: AttachmentNamespaceOwnerBirthV1,
    triggerEventId: EventId,
    rollbackEffectId: Schema.NonEmptyString,
  }),
]);
export type AttachmentNamespaceCleanupReferenceV1 =
  typeof AttachmentNamespaceCleanupReferenceV1.Type;
const AttachmentNamespaceSha256V1 = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
export const AttachmentNamespaceCleanupTaskV1 = Schema.Struct({
  version: Schema.Literal(1),
  effectId: Schema.NonEmptyString,
  commandId: CommandId,
  threadId: ThreadId,
  reference: AttachmentNamespaceCleanupReferenceV1,
  triggerSequence: Schema.Int.check(Schema.isGreaterThan(0)),
  bindingSha256: AttachmentNamespaceSha256V1,
});
export type AttachmentNamespaceCleanupTaskV1 = typeof AttachmentNamespaceCleanupTaskV1.Type;
const AttachmentNamespaceClaimV1 = Schema.Struct({
  workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)),
  leaseExpiresAt: Schema.NonEmptyString,
});
const AttachmentNamespaceBasisFieldsV1 = {
  task: AttachmentNamespaceCleanupTaskV1,
  claim: AttachmentNamespaceClaimV1,
  basisEventSequence: Schema.Int.check(Schema.isGreaterThan(0)),
};
export const QualifiedAttachmentNamespaceCleanupBasisV1 = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("ready"),
    ...AttachmentNamespaceBasisFieldsV1,
    retainedRelativePaths: Schema.Array(Schema.NonEmptyString),
    retentionSourceEvidence: Schema.optionalKey(ImportedApplicationAttachmentRetentionEvidenceV1),
  }),
  Schema.Struct({
    status: Schema.Literal("superseded"),
    ...AttachmentNamespaceBasisFieldsV1,
    replacementBirth: AttachmentNamespaceOwnerBirthV1,
  }),
  Schema.Struct({
    status: Schema.Literal("unsafe_namespace"),
    ...AttachmentNamespaceBasisFieldsV1,
  }),
]);
export const AttachmentNamespaceCleanupObservationV1 = Schema.Struct({
  version: Schema.Literal(1),
  producer: Schema.Literal("attachment_namespace"),
  effectId: Schema.NonEmptyString,
  bindingSha256: AttachmentNamespaceSha256V1,
  workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)),
  basisEventSequence: Schema.Int.check(Schema.isGreaterThan(0)),
  configuredRoot: Schema.NonEmptyString,
  namespaceSegment: Schema.NullOr(Schema.NonEmptyString),
  observedAt: Schema.NonEmptyString,
  outcome: Schema.Union([
    Schema.Struct({
      status: Schema.Literal("completed"),
      matchingPaths: Schema.Array(Schema.NonEmptyString),
      removedPaths: Schema.Array(Schema.NonEmptyString),
      retainedPaths: Schema.Array(Schema.NonEmptyString),
      rootAbsent: Schema.Boolean,
    }),
    Schema.Struct({
      status: Schema.Literal("superseded"),
      replacementBirth: AttachmentNamespaceOwnerBirthV1,
    }),
    Schema.Struct({ status: Schema.Literal("unsafe_namespace") }),
    Schema.Struct({
      status: Schema.Literal("retryable_failure"),
      removedPaths: Schema.Array(Schema.NonEmptyString),
      remainingPaths: Schema.Array(Schema.NonEmptyString),
      reason: Schema.NonEmptyString,
    }),
    Schema.Struct({
      status: Schema.Literal("unknown"),
      removedPaths: Schema.Array(Schema.NonEmptyString),
      reason: Schema.NonEmptyString,
    }),
  ]),
});
export type AttachmentNamespaceCleanupObservationV1 =
  typeof AttachmentNamespaceCleanupObservationV1.Type;
export const AttachmentNamespaceCleanupCorrelationV1 = Schema.Struct({
  version: Schema.Literal(1),
  basis: QualifiedAttachmentNamespaceCleanupBasisV1,
  observationSha256: AttachmentNamespaceSha256V1,
});
export const AttachmentNamespaceOrchestrationEffectPayloadV1 = Schema.Struct({
  request: Schema.Struct({
    type: Schema.Literal("attachment.cleanup"),
    attachmentIds: Schema.Array(Schema.String),
  }),
  attachmentNamespaceCleanup: AttachmentNamespaceCleanupReferenceV1,
});
const decodeAttachmentPayload = Schema.decodeUnknownEffect(
  AttachmentNamespaceOrchestrationEffectPayloadV1,
  { onExcessProperty: "error" },
);
const encodeAttachmentPayload = Schema.encodeSync(
  Schema.fromJsonString(AttachmentNamespaceOrchestrationEffectPayloadV1),
);

export const REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS = [
  "provider-runtime.continue",
  "provider-session.detach",
  "provider-thread.rollback",
  "checkpoint.capture",
  "terminal.cleanup",
  "attachment.cleanup",
  "thread-title.generate",
] as const satisfies ReadonlyArray<OrchestrationEffectRequestV2["type"]>;

export const PROCESS_BOUND_EFFECT_TYPES = [
  "worktree.cleanup",
  "provider-turn.start",
  "provider-turn.interrupt",
  "provider-turn.steer",
  "provider-turn.restart",
  "runtime-request.respond",
] as const satisfies ReadonlyArray<OrchestrationEffectRequestV2["type"]>;

export const OrchestrationEffectStatusV2 = Schema.Literals([
  "pending",
  "running",
  "succeeded",
  "failed",
  "cancelled",
]);
export type OrchestrationEffectStatusV2 = typeof OrchestrationEffectStatusV2.Type;

export interface OrchestrationEffectV2 {
  readonly id: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly request: OrchestrationEffectRequestV2;
  readonly nativeCreationExecutionReference?: NativeCreationExecutionReferenceV2;
  readonly attachmentNamespaceCleanup?: AttachmentNamespaceCleanupReferenceV1;
  readonly status: OrchestrationEffectStatusV2;
  readonly attemptCount: number;
  readonly availableAt: string;
  readonly leaseOwner: string | null;
  readonly leaseExpiresAt: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly completedAt: string | null;
  readonly lastError: string | null;
}

export interface PendingOrchestrationEffectV2 {
  readonly id: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly request: OrchestrationEffectRequestV2;
  readonly nativeCreationExecutionReference?: NativeCreationExecutionReferenceV2;
  readonly attachmentNamespaceCleanup?: AttachmentNamespaceCleanupReferenceV1;
  readonly availableAt?: DateTime.Utc;
}

// This records unresolved cleanup; a missing binding grants no access to a resource target.
const ResourceCleanupUnknownSubjectV1 = {
  version: Schema.Literal(1),
  kind: Schema.Literal("resource_cleanup"),
  operationId: Schema.NonEmptyString,
  threadId: ThreadId,
  taskKind: Schema.Literals(["terminal", "attachment", "worktree"]),
  outcome: Schema.Literal("unknown"),
};
export const ResourceCleanupUnknownEvidenceV1 = Schema.Union([
  Schema.Struct({
    ...ResourceCleanupUnknownSubjectV1,
    bindingSha256: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  }),
  Schema.Struct({
    ...ResourceCleanupUnknownSubjectV1,
    bindingSha256: Schema.Null,
    reason: Schema.Literal("task_binding_unavailable"),
  }),
]);
export type ResourceCleanupUnknownEvidenceV1 = typeof ResourceCleanupUnknownEvidenceV1.Type;
const UnknownEffectHoldEvidenceV2 = Schema.Union([
  ProviderNativeEffectEvidence,
  ResourceCleanupUnknownEvidenceV1,
]);

export interface UnknownEffectHoldV2 {
  readonly effectId: string;
  readonly threadId: ThreadId;
  readonly workerId: string;
  readonly operationId: string;
  readonly evidence: ProviderNativeEffectEvidence | ResourceCleanupUnknownEvidenceV1;
  readonly expectedAttempt: number;
  readonly heldAt: string;
}
export const UnknownEffectHoldSchemaV2 = Schema.Struct({
  effectId: Schema.NonEmptyString,
  threadId: ThreadId,
  workerId: Schema.NonEmptyString,
  operationId: Schema.NonEmptyString,
  evidence: UnknownEffectHoldEvidenceV2,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)),
  heldAt: Schema.NonEmptyString,
});
export const QualifiedDeletionCleanupEvidenceV1 = Schema.Struct({
  version: Schema.Literal(1),
  schema: Schema.Literal("t3.deletion-cleanup-observation/v1"),
  producer: Schema.Literals(["worktree", "managed_terminal", "managed_provider"]),
  observation: Schema.Record(Schema.String, Schema.Unknown),
  coveredHolds: Schema.Array(UnknownEffectHoldSchemaV2),
});

export interface ParkedImportedHistoryDeliveryV1 {
  readonly status: "parked";
  readonly effectId: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly runAttemptId: RunAttemptId;
  readonly commandDigest: string;
  readonly schedulingAttempt: number;
}

export class EffectOutboxError extends Schema.TaggedError<EffectOutboxError>()(
  "EffectOutboxError",
  {
    operation: Schema.String,
    effectId: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Orchestration effect outbox ${this.operation} failed${this.effectId === undefined ? "" : ` for ${this.effectId}`}.`;
  }
}

const isEffectOutboxError = Schema.is(EffectOutboxError);

export interface EffectOutboxV2Shape {
  readonly listAttachmentNamespaceCleanupRetryCandidates: (input?: {
    readonly limit?: number;
  }) => Effect.Effect<
    ReadonlyArray<{
      readonly effectId: string;
      readonly bindingSha256: string;
      readonly observationOrdinal: number;
    }>,
    EffectOutboxError
  >;
  readonly holdAttachmentNamespaceCleanupUnknown: (input: {
    readonly effectId: string;
    readonly bindingSha256: string;
    readonly workerId: string;
    readonly expectedAttempt: number;
    readonly expectedLatestOrdinal: number;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly claimAttachmentNamespaceCleanup: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly leaseDurationMs: number;
    readonly expectedBindingSha256: string;
    readonly expectedObservationOrdinal: number;
  }) => Effect.Effect<OrchestrationEffectV2 | null, EffectOutboxError>;
  readonly completeAttachmentNamespaceCleanupObservation: (input: {
    readonly effectId: string;
    readonly bindingSha256: string;
    readonly workerId: string;
    readonly expectedAttempt: number;
    readonly expectedLatestOrdinal: number;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly awaitAvailable: Effect.Effect<void>;
  readonly notifyAvailable: (count?: number) => Effect.Effect<void>;
  /** Persist rows only. Notify workers after the surrounding transaction commits. */
  readonly enqueue: (
    effects: ReadonlyArray<PendingOrchestrationEffectV2>,
  ) => Effect.Effect<void, EffectOutboxError>;
  readonly get: (
    effectId: string,
  ) => Effect.Effect<Option.Option<OrchestrationEffectV2>, EffectOutboxError>;
  readonly listByCommandId: (
    commandId: CommandId,
  ) => Effect.Effect<ReadonlyArray<OrchestrationEffectV2>, EffectOutboxError>;
  readonly listHeldByThreadId: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<UnknownEffectHoldV2>, EffectOutboxError>;
  readonly cancelUnsettled: (input: {
    readonly threadId: ThreadId;
    readonly effectTypes: ReadonlyArray<OrchestrationEffectRequestV2["type"]>;
    readonly reason: string;
  }) => Effect.Effect<ReadonlyArray<string>, EffectOutboxError>;
  readonly signalCancellations: (effectIds: ReadonlyArray<string>) => Effect.Effect<void>;
  readonly awaitCancellation: (effectId: string) => Effect.Effect<void>;
  readonly clearCancellation: (effectId: string) => Effect.Effect<void>;
  readonly reconcileAfterProcessLoss: Effect.Effect<
    { readonly requeued: number; readonly cancelled: number },
    EffectOutboxError
  >;
  readonly reconcileAfterProcessLossExcluding: (input: {
    readonly excludeThreadIds: ReadonlyArray<ThreadId>;
  }) => Effect.Effect<{ readonly requeued: number; readonly cancelled: number }, EffectOutboxError>;
  readonly claimNext: (input: {
    readonly workerId: string;
    readonly leaseDurationMs: number;
    readonly excludeRestartContinuations?: boolean;
  }) => Effect.Effect<Option.Option<OrchestrationEffectV2>, EffectOutboxError>;
  readonly nextClaimableAt: Effect.Effect<Option.Option<DateTime.Utc>, EffectOutboxError>;
  readonly renewClaim: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly expectedAttempt: number;
    readonly expectedLeaseExpiresAt: string;
    readonly leaseExpiresAt: string;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly settleOrdinaryCheckoutStartClaim: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly expectedAttempt: number;
    readonly expectedLeaseExpiresAt: string;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly claimOrdinaryFinalCheckpointRow: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly expectedAttemptCount: number;
    readonly leaseDurationMs: number;
  }) => Effect.Effect<OrchestrationEffectV2 | null, EffectOutboxError>;
  readonly parkImportedHistoryDelivery: (input: {
    readonly effectId: string;
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly runAttemptId: RunAttemptId;
    readonly workerId: string;
    readonly expectedAttempt: number;
  }) => Effect.Effect<
    | ParkedImportedHistoryDeliveryV1
    | {
        readonly status: "rejected";
        readonly reason:
          | "claim_changed"
          | "choice_changed"
          | "queue_changed"
          | "prior_effect"
          | "already_head";
      },
    EffectOutboxError
  >;
  readonly holdUnknown: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly operationId: string;
    readonly evidence: ProviderNativeEffectEvidence;
    readonly expectedAttempt: number;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly holdResourceCleanupUnknown: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly expectedAttempt: number;
    readonly evidence: ResourceCleanupUnknownEvidenceV1;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly succeed: (input: {
    readonly effectId: string;
    readonly workerId: string;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly completeObservedDeletionCleanup: (input: {
    readonly effectId: string;
    readonly bindingSha256: string;
    readonly expectedLatestOrdinal: number;
    readonly workerId: string;
    readonly expectedAttempt: number;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly retry: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly error: string;
    readonly delayMs: number;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly fail: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly error: string;
  }) => Effect.Effect<boolean, EffectOutboxError>;
}

export class EffectOutboxV2 extends Context.Service<EffectOutboxV2, EffectOutboxV2Shape>()(
  "t3/orchestration-v2/EffectOutbox/EffectOutboxV2",
) {}

type EffectRow = {
  readonly effect_id: string;
  readonly command_id: string;
  readonly thread_id: string;
  readonly effect_type: string;
  readonly payload_json: string;
  readonly status: string;
  readonly attempt_count: number;
  readonly available_at: string;
  readonly lease_owner: string | null;
  readonly lease_expires_at: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly completed_at: string | null;
  readonly last_error: string | null;
};

const encodeRequest = Schema.encodeSync(Schema.fromJsonString(OrchestrationEffectRequestV2));
const decodeRequest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationEffectRequestV2),
);

const decodeTaskProviderSessionId = Schema.decodeEffect(
  Schema.String.pipe(
    Schema.decodeTo(Schema.Unknown, {
      decode: SchemaGetter.onSome<unknown, string>((input, options) => {
        try {
          const providerSessionId: unknown = JSON.parse(input).expectedBinding.providerSessionId;
          return Effect.succeed(Option.some(providerSessionId));
        } catch (cause) {
          return Effect.fail(
            new SchemaIssue.InvalidValue({ nativeJsonCause: cause }, input, options),
          );
        }
      }),
      encode: SchemaGetter.forbiddenEncoding,
    }),
  ),
);

// Envelope payloads must retain their reference; malformed envelopes cannot downgrade to plain work.
export const decodeOrchestrationEffectPayloadV2 = (payload: string) =>
  Effect.gen(function* () {
    const raw = yield* decodeJson(payload).pipe(
      Effect.mapError(
        (error) => new Cause.UnknownError(jsonCause(error), "An error occurred in Effect.try"),
      ),
    );
    if (typeof raw === "object" && raw !== null && Object.hasOwn(raw, "attachmentNamespaceCleanup"))
      return yield* decodeAttachmentPayload(raw);
    if (
      typeof raw === "object" &&
      raw !== null &&
      (Object.hasOwn(raw, "request") || Object.hasOwn(raw, "nativeCreationExecutionReference"))
    ) {
      return yield* decodeNativePayload(raw);
    }
    return { request: yield* decodeRequest(payload) };
  });

class EffectOutboxPayloadError extends Schema.TaggedError<EffectOutboxPayloadError>()(
  "EffectOutboxPayloadError",
  { message: Schema.String, cause: Schema.Defect() },
) {}

const invalidEffectPayload = (message: string) =>
  new EffectOutboxPayloadError({ message, cause: new Error(message) });

const validateAttachmentAssociation = (input: {
  readonly id: string;
  readonly commandId: string;
  readonly threadId: string;
  readonly request: OrchestrationEffectRequestV2;
  readonly nativeCreationExecutionReference?: NativeCreationExecutionReferenceV2;
  readonly attachmentNamespaceCleanup?: AttachmentNamespaceCleanupReferenceV1;
}) =>
  Effect.gen(function* () {
    const reference = input.attachmentNamespaceCleanup;
    if (reference === undefined) return;
    if (
      input.nativeCreationExecutionReference !== undefined ||
      input.request.type !== "attachment.cleanup" ||
      reference.ownerBirth.threadId !== input.threadId ||
      (reference.mode === "delete_thread"
        ? input.id !== `effect:${input.commandId}:attachment.cleanup`
        : input.id !== `${reference.rollbackEffectId}:attachment.cleanup:prune`)
    )
      return yield* Effect.fail(
        invalidEffectPayload("Attachment namespace reference differs from its effect association"),
      );
  });

const validateNativeAssociation = (input: {
  readonly id: string;
  readonly commandId: string;
  readonly request: OrchestrationEffectRequestV2;
  readonly nativeCreationExecutionReference?: NativeCreationExecutionReferenceV2;
}) =>
  Effect.gen(function* () {
    const reference = input.nativeCreationExecutionReference;
    if (reference === undefined) return;
    if (
      reference.effectId !== input.id ||
      reference.stageCommandId !== input.commandId ||
      reference.stage !== "native_command" ||
      (input.request.type === "provider-turn.start" &&
        input.id !== `effect:${input.commandId}:provider-turn.start:${input.request.runId}`) ||
      (input.request.type !== "provider-turn.start" &&
        input.request.type !== "thread-title.generate")
    ) {
      return yield* Effect.fail(
        invalidEffectPayload("Native outbox reference differs from its effect association"),
      );
    }
  });

const rowToEffect = (row: EffectRow) =>
  decodeOrchestrationEffectPayloadV2(row.payload_json).pipe(
    Effect.tap((payload) =>
      payload.request.type === row.effect_type
        ? Effect.void
        : Effect.fail(
            invalidEffectPayload("Outbox effect type differs from its persisted payload"),
          ),
    ),
    Effect.tap((payload) =>
      validateNativeAssociation({ id: row.effect_id, commandId: row.command_id, ...payload }),
    ),
    Effect.tap((payload) =>
      validateAttachmentAssociation({
        id: row.effect_id,
        commandId: row.command_id,
        threadId: row.thread_id,
        ...payload,
      }),
    ),
    Effect.map((payload): OrchestrationEffectV2 => ({
      id: row.effect_id,
      commandId: CommandId.make(row.command_id),
      threadId: ThreadId.make(row.thread_id),
      ...payload,
      status: row.status as OrchestrationEffectStatusV2,
      attemptCount: row.attempt_count,
      availableAt: row.available_at,
      leaseOwner: row.lease_owner,
      leaseExpiresAt: row.lease_expires_at,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: row.completed_at,
      lastError: row.last_error,
    })),
  );

export const layer: Layer.Layer<EffectOutboxV2, never, SqlClient.SqlClient> = Layer.effect(
  EffectOutboxV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    // Availability is only a bounded latency hint; durable rows remain
    // authoritative. Retaining a small burst lets multiple worker slots wake
    // for distinct threads without allowing notifications to grow unbounded.
    const available = yield* Queue.dropping<void>(64);
    const cancellationSignals = new Map<string, Deferred.Deferred<void>>();
    const notifyAvailable = (count = 1) =>
      Queue.offerAll(
        available,
        Array.from({ length: Math.min(64, Math.max(0, Math.floor(count))) }, () => undefined),
      ).pipe(Effect.asVoid);
    const ordinaryFinalCheckpointPredicate =
      () => sql`candidate.effect_type = 'checkpoint.capture' AND EXISTS (
      SELECT 1 FROM orchestration_v2_ordinary_checkout_effect_links link
      JOIN orchestration_v2_ordinary_checkout_execution_associations association ON association.admission_id = link.admission_id
      WHERE link.effect_id = candidate.effect_id AND association.executor_kind = 'captured_managed_run'
        AND json_extract(association.association_json, '$.executor.run.runId') = COALESCE(json_extract(candidate.payload_json, '$.runId'), json_extract(candidate.payload_json, '$.request.runId'))
        AND json_extract(association.association_json, '$.executor.checkpointScopeId') = COALESCE(json_extract(candidate.payload_json, '$.scopeId'), json_extract(candidate.payload_json, '$.request.scopeId'))
        AND NOT EXISTS (SELECT 1 FROM orchestration_v2_ordinary_checkout_execution_associations activation
          WHERE activation.operation_id = association.operation_id AND activation.association_id = association.association_id
            AND activation.event_kind = 'activate'
            AND json_extract(activation.evidence_json, '$.actualStartObservation.settlementMode') = 'primary_terminal_checkpoint')) `;
    // Title generation is correlated metadata work, so it has its own
    // per-thread lane and cannot delay provider lifecycle effects.
    const claimableCandidatePredicate = (
      availableBefore?: string,
      includeOrdinaryFinalCheckpoint = false,
    ) =>
      sql`
        ${
          availableBefore === undefined
            ? sql`1 = 1`
            : sql`candidate.available_at <= ${availableBefore}`
        }
        AND candidate.status = 'pending'
        AND ${includeOrdinaryFinalCheckpoint ? sql`1 = 1` : sql`NOT (${ordinaryFinalCheckpointPredicate()})`}
        AND (json_type(candidate.payload_json, '$.attachmentNamespaceCleanup') IS NULL OR candidate.attempt_count = 0)
        AND NOT EXISTS (
          SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
          WHERE hold.effect_id = candidate.effect_id
        )
        AND NOT EXISTS (
          SELECT 1 FROM orchestration_v2_imported_history_start_choices choice
          JOIN orchestration_v2_imported_history_start_outcomes outcome ON outcome.command_id = choice.command_id
          JOIN orchestration_v2_projection_runs target ON target.thread_id = choice.thread_id AND target.run_id = outcome.run_id
          WHERE choice.command_id = candidate.command_id AND choice.thread_id = candidate.thread_id
            AND outcome.intent_status = 'accepted' AND outcome.effect_id = candidate.effect_id
            AND candidate.effect_type = 'provider-turn.start' AND target.status = 'queued'
            AND json_extract(choice.canonical_command_json, '$.delivery.type') = 'queued_run'
            AND target.run_id = json_extract(candidate.payload_json, '$.runId')
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_queued_start_reservations reservation WHERE reservation.effect_id = candidate.effect_id)
            AND target.run_id IS NOT (
              SELECT queued.run_id FROM orchestration_v2_projection_runs queued
              LEFT JOIN orchestration_v2_projection_messages message ON message.message_id = json_extract(queued.payload_json, '$.userMessageId')
                AND message.thread_id = queued.thread_id
              WHERE queued.thread_id = target.thread_id AND queued.status = 'queued'
              ORDER BY (json_type(message.payload_json, '$.delegatedCompletion') IS NOT NULL) DESC,
                COALESCE(json_extract(queued.payload_json, '$.queuePosition'), queued.ordinal) ASC, queued.ordinal ASC LIMIT 1
            )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM orchestration_v2_effect_outbox AS active
          WHERE active.thread_id = candidate.thread_id
            AND active.status = 'running'
            AND (
              (
                candidate.effect_type = 'thread-title.generate'
                AND active.effect_type = 'thread-title.generate'
              )
              OR
              (
                candidate.effect_type != 'thread-title.generate'
                AND active.effect_type != 'thread-title.generate'
              )
            )
        )
      `;

    const cancellationSignal = (effectId: string) => {
      const existing = cancellationSignals.get(effectId);
      if (existing !== undefined) return existing;
      const created = Deferred.makeUnsafe<void>();
      cancellationSignals.set(effectId, created);
      return created;
    };

    const decodeRows = (operation: string, rows: ReadonlyArray<EffectRow>) =>
      Effect.forEach(rows, rowToEffect).pipe(
        Effect.mapError((cause) => new EffectOutboxError({ operation, cause })),
      );

    const reconcileAfterProcessLossExcluding: EffectOutboxV2Shape["reconcileAfterProcessLossExcluding"] =
      ({ excludeThreadIds }) =>
        Effect.gen(function* () {
          const excluded = [...new Set(excludeThreadIds)];
          const includedThread =
            excluded.length === 0
              ? sql`1 = 1`
              : sql`(thread_id IS NULL OR thread_id NOT IN ${sql.in(excluded)})`;
          const result = yield* sql.withTransaction(
            Effect.gen(function* () {
              const now = DateTime.formatIso(yield* DateTime.now);
              const cancelledRows = yield* sql<{ readonly effect_id: string }>`
            UPDATE orchestration_v2_effect_outbox
            SET status = 'cancelled', lease_owner = NULL, lease_expires_at = NULL,
              completed_at = ${now}, updated_at = ${now},
              last_error = 'Cancelled because the server process ended before the effect completed.'
            WHERE status IN ('pending', 'running') AND ${includedThread}
              AND effect_type IN ${sql.in(PROCESS_BOUND_EFFECT_TYPES)}
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
                WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
            RETURNING effect_id
          `;
              const requeuedRows = yield* sql<{ readonly effect_id: string }>`
            UPDATE orchestration_v2_effect_outbox
            SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL,
              available_at = ${now}, updated_at = ${now},
              last_error = 'Requeued after the previous server process ended.'
            WHERE status = 'running' AND ${includedThread}
              AND effect_type IN ${sql.in(REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS)}
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
                WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
            RETURNING effect_id
          `;
              return { requeued: requeuedRows.length, cancelled: cancelledRows.length };
            }),
          );
          if (result.requeued > 0) yield* notifyAvailable(result.requeued);
          return result;
        }).pipe(
          Effect.mapError(
            (cause) => new EffectOutboxError({ operation: "reconcile-process-loss", cause }),
          ),
        );

    const readAttachmentNamespaceHistory = Effect.fnUntraced(function* (effectId: string) {
      const rows = yield* sql<{
        readonly ordinal: number;
        readonly binding_sha256: string;
        readonly canonical_task_json: string;
        readonly observation_json: string;
        readonly correlation_json: string;
        readonly recorded_at: string;
      }>`
        SELECT * FROM orchestration_v2_attachment_cleanup_observations WHERE effect_id = ${effectId} ORDER BY ordinal`;
      if (rows.length === 0) return null;
      const entries = yield* Effect.forEach(
        rows,
        (row, ordinal) =>
          Effect.gen(function* () {
            const task = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(AttachmentNamespaceCleanupTaskV1),
            )(row.canonical_task_json, { onExcessProperty: "error" });
            const observation = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(AttachmentNamespaceCleanupObservationV1),
            )(row.observation_json, { onExcessProperty: "error" });
            const correlation = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(AttachmentNamespaceCleanupCorrelationV1),
            )(row.correlation_json, { onExcessProperty: "error" });
            const { bindingSha256: _digest, ...subject } = task;
            const basis = correlation.basis;
            if (
              basis.status === "ready" &&
              basis.retentionSourceEvidence !== undefined &&
              nativeCreationCanonicalJson(
                makeImportedApplicationAttachmentRetentionEvidenceV1({
                  ...basis.retentionSourceEvidence,
                  relativePaths: basis.retainedRelativePaths,
                }),
              ) !== nativeCreationCanonicalJson(basis.retentionSourceEvidence)
            )
              return yield* new EffectOutboxError({
                operation: "read-attachment-namespace-history",
                effectId,
              });
            const status = observation.outcome.status;
            if (
              row.ordinal !== ordinal ||
              task.effectId !== effectId ||
              row.binding_sha256 !== task.bindingSha256 ||
              nativeCreationSha256(nativeCreationCanonicalJson(subject)) !== task.bindingSha256 ||
              nativeCreationCanonicalJson(basis.task) !== nativeCreationCanonicalJson(task) ||
              correlation.observationSha256 !==
                nativeCreationSha256(nativeCreationCanonicalJson(observation)) ||
              observation.effectId !== effectId ||
              observation.bindingSha256 !== task.bindingSha256 ||
              observation.workerId !== basis.claim.workerId ||
              observation.expectedAttempt !== basis.claim.expectedAttempt ||
              observation.basisEventSequence !== basis.basisEventSequence ||
              row.recorded_at !== observation.observedAt ||
              !(
                (basis.status === "ready" &&
                  ["completed", "retryable_failure", "unknown"].includes(status)) ||
                (basis.status === "superseded" &&
                  status === "superseded" &&
                  observation.outcome.status === "superseded" &&
                  nativeCreationCanonicalJson(basis.replacementBirth) ===
                    nativeCreationCanonicalJson(observation.outcome.replacementBirth)) ||
                (basis.status === "unsafe_namespace" && status === "unsafe_namespace")
              )
            )
              return yield* new EffectOutboxError({
                operation: "read-attachment-namespace-history",
                effectId,
              });
            return { ordinal, task, observation, basis };
          }),
        { concurrency: 1 },
      );
      const task = entries[0]!.task;
      const effect = Option.getOrNull(yield* service.get(effectId));
      if (
        entries.some(
          (entry) => nativeCreationCanonicalJson(entry.task) !== nativeCreationCanonicalJson(task),
        ) ||
        effect === null ||
        effect.commandId !== task.commandId ||
        effect.threadId !== task.threadId ||
        effect.request.type !== "attachment.cleanup" ||
        effect.attachmentNamespaceCleanup === undefined ||
        nativeCreationCanonicalJson(effect.attachmentNamespaceCleanup) !==
          nativeCreationCanonicalJson(task.reference)
      )
        return yield* new EffectOutboxError({
          operation: "read-attachment-namespace-history",
          effectId,
        });
      return { effect, task, entries, latest: entries[entries.length - 1]! };
    });
    const attachmentNamespaceHoldsMatch = (
      history: NonNullable<Effect.Success<ReturnType<typeof readAttachmentNamespaceHistory>>>,
      holds: ReadonlyArray<UnknownEffectHoldV2>,
    ) =>
      holds.every(
        (hold) =>
          hold.effectId === history.task.effectId &&
          hold.threadId === history.task.threadId &&
          hold.operationId === history.task.effectId &&
          "kind" in hold.evidence &&
          hold.evidence.kind === "resource_cleanup" &&
          hold.evidence.taskKind === "attachment" &&
          hold.evidence.operationId === history.task.effectId &&
          hold.evidence.threadId === history.task.threadId &&
          hold.evidence.bindingSha256 === history.task.bindingSha256 &&
          history.entries.some(
            (entry) =>
              entry.observation.outcome.status === "unknown" &&
              entry.observation.workerId === hold.workerId &&
              entry.observation.expectedAttempt === hold.expectedAttempt,
          ),
      );

    const service: EffectOutboxV2Shape = {
      listAttachmentNamespaceCleanupRetryCandidates: (input = {}) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const limit = input.limit ?? 16;
              if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100)
                return yield* new EffectOutboxError({
                  operation: "list-attachment-namespace-retries",
                });
              const now = DateTime.formatIso(yield* DateTime.now);
              const rows = yield* sql<{ readonly effect_id: string }>`SELECT candidate.effect_id
          FROM orchestration_v2_effect_outbox candidate
          JOIN orchestration_v2_attachment_cleanup_observations observation ON observation.effect_id = candidate.effect_id
            AND observation.ordinal = (SELECT MAX(latest.ordinal) FROM orchestration_v2_attachment_cleanup_observations latest
              WHERE latest.effect_id = candidate.effect_id)
          WHERE candidate.effect_type = 'attachment.cleanup'
            AND json_extract(candidate.payload_json, '$.attachmentNamespaceCleanup.version') = 1
            AND json_extract(observation.observation_json, '$.outcome.status') IN ('retryable_failure', 'unknown')
            AND candidate.available_at <= ${now}
            AND ((candidate.status = 'pending' AND candidate.lease_owner IS NULL AND candidate.lease_expires_at IS NULL) OR
              (candidate.status = 'running' AND candidate.lease_owner IS NOT NULL AND candidate.lease_expires_at <= ${now}))
          ORDER BY candidate.available_at, candidate.created_at, candidate.effect_id LIMIT ${limit}`;
              return yield* Effect.forEach(
                rows,
                (row) =>
                  Effect.gen(function* () {
                    const history = yield* readAttachmentNamespaceHistory(row.effect_id);
                    if (
                      history === null ||
                      !["unknown", "retryable_failure"].includes(
                        history.latest.observation.outcome.status,
                      )
                    )
                      return yield* new EffectOutboxError({
                        operation: "list-attachment-namespace-retries",
                        effectId: row.effect_id,
                      });
                    return {
                      effectId: row.effect_id,
                      bindingSha256: history.task.bindingSha256,
                      observationOrdinal: history.latest.ordinal,
                    };
                  }),
                { concurrency: 1 },
              );
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new EffectOutboxError({ operation: "list-attachment-namespace-retries", cause }),
            ),
          ),
      holdAttachmentNamespaceCleanupUnknown: (input) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const history = yield* readAttachmentNamespaceHistory(input.effectId);
              if (
                history === null ||
                history.latest.ordinal !== input.expectedLatestOrdinal ||
                history.task.bindingSha256 !== input.bindingSha256 ||
                history.latest.observation.outcome.status !== "unknown" ||
                history.latest.observation.workerId !== input.workerId ||
                history.latest.observation.expectedAttempt !== input.expectedAttempt
              )
                return false;
              const holds = (yield* service.listHeldByThreadId(history.task.threadId)).filter(
                (hold) => hold.effectId === input.effectId,
              );
              if (!attachmentNamespaceHoldsMatch(history, holds)) return false;
              if (holds.length > 0) return true;
              const now = DateTime.formatIso(yield* DateTime.now);
              const evidence: ResourceCleanupUnknownEvidenceV1 = {
                version: 1,
                kind: "resource_cleanup",
                operationId: input.effectId,
                threadId: history.task.threadId,
                taskKind: "attachment",
                bindingSha256: input.bindingSha256,
                outcome: "unknown",
              };
              const rows = yield* sql`INSERT INTO orchestration_v2_unknown_effect_holds
          (effect_id, worker_id, operation_id, evidence_json, expected_attempt, held_at)
          SELECT effect_id, ${input.workerId}, ${input.effectId}, ${yield* Schema.encodeEffect(Schema.fromJsonString(ResourceCleanupUnknownEvidenceV1))(evidence).pipe(Effect.orDie)},
            ${input.expectedAttempt}, ${now} FROM orchestration_v2_effect_outbox WHERE effect_id = ${input.effectId}
            AND status = 'running' AND lease_owner = ${input.workerId} AND attempt_count = ${input.expectedAttempt} AND lease_expires_at > ${now}
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = ${input.effectId}) RETURNING effect_id`;
              return rows.length === 1;
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new EffectOutboxError({
                  operation: "hold-attachment-namespace",
                  effectId: input.effectId,
                  cause,
                }),
            ),
          ),
      claimAttachmentNamespaceCleanup: (input) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              if (
                input.workerId.length === 0 ||
                !Number.isSafeInteger(input.leaseDurationMs) ||
                input.leaseDurationMs < 1 ||
                !Number.isSafeInteger(input.expectedObservationOrdinal) ||
                input.expectedObservationOrdinal < 0
              )
                return yield* new EffectOutboxError({
                  operation: "claim-attachment-namespace",
                  effectId: input.effectId,
                });
              const history = yield* readAttachmentNamespaceHistory(input.effectId);
              if (
                history === null ||
                history.latest.ordinal !== input.expectedObservationOrdinal ||
                history.task.bindingSha256 !== input.expectedBindingSha256 ||
                !["unknown", "retryable_failure"].includes(
                  history.latest.observation.outcome.status,
                )
              )
                return null;
              const holds = (yield* service.listHeldByThreadId(history.task.threadId)).filter(
                (hold) => hold.effectId === input.effectId,
              );
              if (!attachmentNamespaceHoldsMatch(history, holds)) return null;
              const now = yield* DateTime.now;
              const nowIso = DateTime.formatIso(now);
              const expiresAt = DateTime.formatIso(
                DateTime.add(now, { milliseconds: input.leaseDurationMs }),
              );
              const current = history.effect;
              if (
                current.attemptCount < history.latest.observation.expectedAttempt ||
                !(
                  (current.status === "pending" &&
                    current.leaseOwner === null &&
                    current.leaseExpiresAt === null &&
                    Number.isFinite(Date.parse(current.availableAt)) &&
                    current.availableAt <= nowIso) ||
                  (current.status === "running" &&
                    current.leaseOwner !== null &&
                    current.leaseExpiresAt !== null &&
                    Number.isFinite(Date.parse(current.leaseExpiresAt)) &&
                    current.leaseExpiresAt <= nowIso)
                )
              )
                return null;
              const rows =
                yield* sql<EffectRow>`UPDATE orchestration_v2_effect_outbox SET status = 'running', attempt_count = attempt_count + 1,
          lease_owner = ${input.workerId}, lease_expires_at = ${expiresAt}, updated_at = ${nowIso}, last_error = NULL
          WHERE effect_id = ${input.effectId} AND attempt_count = ${current.attemptCount}
            AND ((status = 'pending' AND lease_owner IS NULL AND lease_expires_at IS NULL AND available_at <= ${nowIso}) OR
              (status = 'running' AND lease_owner = ${current.leaseOwner} AND lease_expires_at = ${current.leaseExpiresAt} AND lease_expires_at <= ${nowIso}))
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_attachment_cleanup_observations WHERE effect_id = ${input.effectId}
              AND ordinal > ${input.expectedObservationOrdinal}) RETURNING *`;
              if (rows.length !== 1) return null;
              cancellationSignals.set(input.effectId, Deferred.makeUnsafe<void>());
              return yield* rowToEffect(rows[0]!);
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new EffectOutboxError({
                  operation: "claim-attachment-namespace",
                  effectId: input.effectId,
                  cause,
                }),
            ),
          ),
      completeAttachmentNamespaceCleanupObservation: (input) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const history = yield* readAttachmentNamespaceHistory(input.effectId);
              if (
                history === null ||
                history.latest.ordinal !== input.expectedLatestOrdinal ||
                history.task.bindingSha256 !== input.bindingSha256 ||
                !["completed", "superseded", "unsafe_namespace"].includes(
                  history.latest.observation.outcome.status,
                ) ||
                history.latest.observation.workerId !== input.workerId ||
                history.latest.observation.expectedAttempt !== input.expectedAttempt ||
                history.effect.attemptCount !== input.expectedAttempt
              )
                return false;
              const holds = (yield* service.listHeldByThreadId(history.task.threadId)).filter(
                (hold) => hold.effectId === input.effectId,
              );
              if (!attachmentNamespaceHoldsMatch(history, holds)) return false;
              if (history.effect.status === "succeeded") return history.effect.completedAt !== null;
              const now = DateTime.formatIso(yield* DateTime.now);
              const changed =
                yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
          completed_at = ${now}, updated_at = ${now}, last_error = NULL WHERE effect_id = ${input.effectId} AND status = 'running'
            AND lease_owner = ${input.workerId} AND attempt_count = ${input.expectedAttempt} AND lease_expires_at > ${now} RETURNING effect_id`;
              return changed.length === 1;
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new EffectOutboxError({
                  operation: "complete-attachment-namespace",
                  effectId: input.effectId,
                  cause,
                }),
            ),
          ),
      enqueue: (effects) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const now = yield* DateTime.now;
              const nowIso = DateTime.formatIso(now);
              yield* Effect.forEach(
                effects,
                (effect) =>
                  Effect.gen(function* () {
                    yield* validateAttachmentAssociation(effect);
                    const payload =
                      effect.attachmentNamespaceCleanup !== undefined
                        ? encodeAttachmentPayload(
                            yield* decodeAttachmentPayload({
                              request: effect.request,
                              attachmentNamespaceCleanup: effect.attachmentNamespaceCleanup,
                            }),
                          )
                        : effect.nativeCreationExecutionReference === undefined
                          ? encodeRequest(effect.request)
                          : encodeNativePayload(
                              yield* decodeNativePayload({
                                request: effect.request,
                                nativeCreationExecutionReference:
                                  effect.nativeCreationExecutionReference,
                              }),
                            );
                    yield* validateNativeAssociation(effect);
                    yield* sql`
              INSERT INTO orchestration_v2_effect_outbox (
                effect_id,
                command_id,
                thread_id,
                effect_type,
                payload_json,
                status,
                attempt_count,
                available_at,
                created_at,
                updated_at
              )
              VALUES (
                ${effect.id},
                ${effect.commandId},
                ${effect.threadId},
                ${effect.request.type},
                ${payload},
                'pending',
                0,
                ${DateTime.formatIso(effect.availableAt ?? now)},
                ${nowIso},
                ${nowIso}
              )
              ON CONFLICT(effect_id) DO NOTHING
              `;
                    const existing =
                      yield* sql<EffectRow>`SELECT * FROM orchestration_v2_effect_outbox
                WHERE effect_id = ${effect.id}
                  AND (${effect.nativeCreationExecutionReference !== undefined || effect.attachmentNamespaceCleanup !== undefined ? sql`1 = 1` : sql`0 = 1`} OR
                    CASE WHEN json_valid(payload_json) THEN
                      json_type(payload_json, '$.request') IS NOT NULL OR
                      json_type(payload_json, '$.nativeCreationExecutionReference') IS NOT NULL OR
                      json_type(payload_json, '$.attachmentNamespaceCleanup') IS NOT NULL
                    ELSE 0 END)`;
                    if (
                      existing.length !== 0 &&
                      (existing.length !== 1 ||
                        existing[0]!.command_id !== effect.commandId ||
                        existing[0]!.thread_id !== effect.threadId ||
                        existing[0]!.effect_type !== effect.request.type ||
                        existing[0]!.payload_json !== payload)
                    ) {
                      return yield* Effect.fail(
                        invalidEffectPayload(
                          "Referenced outbox effect identity is already bound differently",
                        ),
                      );
                    }
                  }),
                { concurrency: 1, discard: true },
              );
              // Do not signal here: callers enqueue inside a larger transaction,
              // and workers must only observe availability after that transaction
              // commits. EventSink owns the corresponding post-commit notification.
            }),
          )
          .pipe(Effect.mapError((cause) => new EffectOutboxError({ operation: "enqueue", cause }))),
      get: (effectId) =>
        sql<EffectRow>`
          SELECT *
          FROM orchestration_v2_effect_outbox
          WHERE effect_id = ${effectId}
          LIMIT 1
        `.pipe(
          Effect.flatMap((rows) => {
            const row = rows[0];
            return row === undefined
              ? Effect.succeed(Option.none())
              : rowToEffect(row).pipe(Effect.map(Option.some));
          }),
          Effect.mapError((cause) => new EffectOutboxError({ operation: "get", effectId, cause })),
        ),
      awaitAvailable: Queue.take(available),
      notifyAvailable,
      listByCommandId: (commandId) =>
        sql<EffectRow>`
          SELECT *
          FROM orchestration_v2_effect_outbox
          WHERE command_id = ${commandId}
          ORDER BY created_at ASC, effect_id ASC
        `.pipe(
          Effect.flatMap((rows) => decodeRows("list", rows)),
          Effect.mapError((cause) =>
            isEffectOutboxError(cause)
              ? cause
              : new EffectOutboxError({ operation: "list", cause }),
          ),
        ),
      listHeldByThreadId: (threadId) =>
        sql<{
          readonly effect_id: string;
          readonly worker_id: string;
          readonly operation_id: string;
          readonly evidence_json: string;
          readonly expected_attempt: number;
          readonly held_at: string;
        }>`
          SELECT hold.* FROM orchestration_v2_unknown_effect_holds hold
          JOIN orchestration_v2_effect_outbox effect ON effect.effect_id = hold.effect_id
          WHERE effect.thread_id = ${threadId} ORDER BY hold.held_at, hold.effect_id
        `.pipe(
          Effect.flatMap((rows) =>
            Effect.forEach(rows, (row) =>
              Schema.decodeUnknownEffect(Schema.fromJsonString(UnknownEffectHoldEvidenceV2))(
                row.evidence_json,
                { onExcessProperty: "error" },
              ).pipe(
                Effect.map((evidence): UnknownEffectHoldV2 => ({
                  effectId: row.effect_id,
                  threadId,
                  workerId: row.worker_id,
                  operationId: row.operation_id,
                  evidence,
                  expectedAttempt: row.expected_attempt,
                  heldAt: row.held_at,
                })),
              ),
            ),
          ),
          Effect.mapError((cause) => new EffectOutboxError({ operation: "list-held", cause })),
        ),
      cancelUnsettled: ({ threadId, effectTypes, reason }) =>
        Effect.gen(function* () {
          if (effectTypes.length === 0) return [];
          const now = DateTime.formatIso(yield* DateTime.now);
          const rows = yield* sql<{ readonly effect_id: string }>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = 'cancelled',
              lease_owner = NULL,
              lease_expires_at = NULL,
              completed_at = ${now},
              updated_at = ${now},
              last_error = ${reason}
            WHERE thread_id = ${threadId}
              AND status IN ('pending', 'running')
              AND effect_type IN ${sql.in(effectTypes)}
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
                WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
            RETURNING effect_id
          `;
          return rows.map(({ effect_id }) => effect_id);
        }).pipe(
          Effect.mapError(
            (cause) => new EffectOutboxError({ operation: "cancel-unsettled", cause }),
          ),
        ),
      signalCancellations: (effectIds) =>
        Effect.gen(function* () {
          yield* Effect.forEach(
            effectIds,
            (effectId) => {
              const signal = cancellationSignals.get(effectId);
              return signal === undefined ? Effect.void : Deferred.succeed(signal, undefined);
            },
            { discard: true },
          );
          // Cancellation can unblock other pending work on the same thread.
          // This method is deliberately post-commit, so it is also the safe
          // place to wake claimers after the durable status change.
          if (effectIds.length > 0) yield* notifyAvailable(effectIds.length);
        }),
      awaitCancellation: (effectId) => Deferred.await(cancellationSignal(effectId)),
      clearCancellation: (effectId) =>
        Effect.sync(() => {
          cancellationSignals.delete(effectId);
        }),
      reconcileAfterProcessLoss: reconcileAfterProcessLossExcluding({ excludeThreadIds: [] }),
      reconcileAfterProcessLossExcluding,
      claimNext: ({ workerId, leaseDurationMs, excludeRestartContinuations = false }) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const nowIso = DateTime.formatIso(now);
          const leaseExpiresAt = DateTime.formatIso(
            DateTime.add(now, { milliseconds: Math.max(1, leaseDurationMs) }),
          );
          const ordinaryLeaseExpiresAt = DateTime.formatIso(DateTime.add(now, { minutes: 5 }));
          // Empty safety claims are expected while the daemon is idle. Tracing
          // this query records the full SQL text on every poll and can dominate
          // the local trace without adding actionable information.
          const claimStatement = sql<EffectRow>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = 'running',
              attempt_count = attempt_count + 1,
              lease_owner = ${workerId},
              lease_expires_at = CASE WHEN EXISTS (SELECT 1 FROM orchestration_v2_ordinary_checkout_effect_links ordinary_link
                WHERE ordinary_link.effect_id = orchestration_v2_effect_outbox.effect_id) THEN ${ordinaryLeaseExpiresAt} ELSE ${leaseExpiresAt} END,
              updated_at = ${nowIso},
              last_error = NULL
            WHERE effect_id = (
              SELECT candidate.effect_id
              FROM orchestration_v2_effect_outbox AS candidate
              WHERE ${claimableCandidatePredicate(nowIso)}
                AND ${excludeRestartContinuations ? sql`candidate.effect_type != 'provider-runtime.continue'` : sql`1 = 1`}
              ORDER BY candidate.available_at ASC, candidate.created_at ASC, candidate.effect_id ASC
              LIMIT 1
            )
            RETURNING *
          `;
          const rows = yield* claimStatement.pipe(Effect.withTracerEnabled(false));
          const row = rows[0];
          if (row === undefined) return Option.none();
          cancellationSignals.set(row.effect_id, Deferred.makeUnsafe<void>());
          return Option.some(yield* rowToEffect(row));
        }).pipe(Effect.mapError((cause) => new EffectOutboxError({ operation: "claim", cause }))),
      renewClaim: (input) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const currentTime = yield* DateTime.now;
              const now = DateTime.formatIso(currentTime);
              const horizon = DateTime.formatIso(DateTime.add(currentTime, { minutes: 5 }));
              const previous = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
                input.expectedLeaseExpiresAt,
              );
              const next = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
                input.leaseExpiresAt,
              );
              if (
                DateTime.formatIso(previous) !== input.expectedLeaseExpiresAt ||
                DateTime.formatIso(next) !== input.leaseExpiresAt ||
                input.expectedLeaseExpiresAt <= now ||
                input.leaseExpiresAt < input.expectedLeaseExpiresAt ||
                input.leaseExpiresAt > horizon ||
                input.workerId.length === 0 ||
                !Number.isSafeInteger(input.expectedAttempt) ||
                input.expectedAttempt < 1
              )
                return false;
              const rows =
                yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at = ${input.leaseExpiresAt}, updated_at = ${now}
          WHERE effect_id = ${input.effectId} AND status = 'running' AND lease_owner = ${input.workerId}
            AND attempt_count = ${input.expectedAttempt} AND lease_expires_at = ${input.expectedLeaseExpiresAt} AND lease_expires_at > ${now}
            AND EXISTS (SELECT 1 FROM orchestration_v2_ordinary_checkout_effect_links link WHERE link.effect_id = ${input.effectId})
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = ${input.effectId}) RETURNING effect_id`;
              return rows.length === 1;
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new EffectOutboxError({
                  operation: "renew-ordinary-claim",
                  effectId: input.effectId,
                  cause,
                }),
            ),
          ),
      settleOrdinaryCheckoutStartClaim: (input) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const now = DateTime.formatIso(yield* DateTime.now);
              const rows =
                yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
          completed_at = ${now}, updated_at = ${now}, last_error = NULL WHERE effect_id = ${input.effectId}
            AND effect_type IN ('provider-turn.start', 'provider-turn.restart') AND status = 'running'
            AND lease_owner = ${input.workerId} AND attempt_count = ${input.expectedAttempt}
            AND lease_expires_at = ${input.expectedLeaseExpiresAt} AND lease_expires_at > ${now}
            AND EXISTS (SELECT 1 FROM orchestration_v2_ordinary_checkout_effect_links link WHERE link.effect_id = ${input.effectId})
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = ${input.effectId}) RETURNING effect_id`;
              return rows.length === 1;
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new EffectOutboxError({
                  operation: "settle-ordinary-start",
                  effectId: input.effectId,
                  cause,
                }),
            ),
          ),
      claimOrdinaryFinalCheckpointRow: (input) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              if (
                input.workerId.length === 0 ||
                !Number.isSafeInteger(input.expectedAttemptCount) ||
                input.expectedAttemptCount < 0 ||
                input.leaseDurationMs !== 300_000
              )
                return null;
              const current = yield* DateTime.now;
              const now = DateTime.formatIso(current);
              const expiry = DateTime.formatIso(DateTime.add(current, { minutes: 5 }));
              const rows =
                yield* sql<EffectRow>`UPDATE orchestration_v2_effect_outbox SET status = 'running', attempt_count = attempt_count + 1,
          lease_owner = ${input.workerId}, lease_expires_at = ${expiry}, updated_at = ${now}, last_error = NULL
          WHERE effect_id IN (SELECT candidate.effect_id FROM orchestration_v2_effect_outbox candidate
            WHERE candidate.effect_id = ${input.effectId} AND candidate.attempt_count = ${input.expectedAttemptCount}
              AND candidate.lease_owner IS NULL AND candidate.lease_expires_at IS NULL AND ${ordinaryFinalCheckpointPredicate()}
              AND ${claimableCandidatePredicate(now, true)}) RETURNING *`;
              if (rows.length !== 1) return null;
              return yield* rowToEffect(rows[0]!);
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new EffectOutboxError({
                  operation: "claim-ordinary-final-checkpoint",
                  effectId: input.effectId,
                  cause,
                }),
            ),
          ),
      nextClaimableAt: Effect.gen(function* () {
        const rows = yield* sql<{ readonly available_at: string | null }>`
          SELECT MIN(candidate.available_at) AS available_at
          FROM orchestration_v2_effect_outbox AS candidate
          WHERE ${claimableCandidatePredicate()}
        `.pipe(Effect.withTracerEnabled(false));
        const availableAt = rows[0]?.available_at;
        if (availableAt === undefined || availableAt === null) return Option.none();
        const parsed = DateTime.make(availableAt);
        if (Option.isNone(parsed)) {
          return yield* new EffectOutboxError({
            operation: "next-claimable",
            cause: `Invalid available_at timestamp: ${availableAt}`,
          });
        }
        return parsed;
      }).pipe(
        Effect.mapError((cause) =>
          isEffectOutboxError(cause)
            ? cause
            : new EffectOutboxError({ operation: "next-claimable", cause }),
        ),
      ),
      parkImportedHistoryDelivery: (input) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const reject = (
                reason:
                  | "claim_changed"
                  | "choice_changed"
                  | "queue_changed"
                  | "prior_effect"
                  | "already_head",
              ) => ({ status: "rejected" as const, reason });
              const now = DateTime.formatIso(yield* DateTime.now);
              if (!Number.isSafeInteger(input.expectedAttempt) || input.expectedAttempt < 1)
                return reject("claim_changed");
              const effects =
                yield* sql<EffectRow>`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id = ${input.effectId}
          AND command_id = ${input.commandId} AND thread_id = ${input.threadId} AND effect_type = 'provider-turn.start'`;
              if (effects.length !== 1) return reject("claim_changed");
              const effect = effects[0]!;
              const alreadyParked =
                effect.status === "pending" &&
                effect.lease_owner === null &&
                effect.lease_expires_at === null &&
                effect.attempt_count === input.expectedAttempt - 1 &&
                effect.last_error === "imported-history.waiting-for-head/v1";
              if (
                !alreadyParked &&
                (effect.status !== "running" ||
                  effect.lease_owner !== input.workerId ||
                  effect.attempt_count !== input.expectedAttempt ||
                  effect.lease_expires_at === null ||
                  !Number.isFinite(Date.parse(effect.lease_expires_at)) ||
                  Date.parse(effect.lease_expires_at) <= Date.parse(now))
              )
                return reject("claim_changed");
              const payload = yield* decodeOrchestrationEffectPayloadV2(effect.payload_json);
              if (
                "nativeCreationExecutionReference" in payload ||
                payload.request.type !== "provider-turn.start" ||
                payload.request.runId !== input.runId ||
                input.effectId !== `effect:${input.commandId}:provider-turn.start:${input.runId}`
              )
                return reject("choice_changed");
              const choices = yield* sql<{
                readonly command_digest: string;
                readonly canonical_command_json: string;
                readonly basis_json: string;
                readonly reviewed_basis: string;
                readonly message_id: string;
                readonly result_sequence: number;
              }>`
          SELECT choice.*, outcome.message_id, receipt.result_sequence FROM orchestration_v2_imported_history_start_choices choice
          JOIN orchestration_v2_imported_history_start_outcomes outcome ON outcome.command_id = choice.command_id AND outcome.intent_status = 'accepted'
          JOIN orchestration_command_receipts receipt ON receipt.command_id = choice.command_id AND receipt.status = 'accepted'
            AND receipt.command_type = 'thread.imported-history.start' AND receipt.aggregate_kind = 'thread' AND receipt.aggregate_id = choice.thread_id
          WHERE choice.command_id = ${input.commandId} AND choice.thread_id = ${input.threadId}
            AND outcome.run_id = ${input.runId} AND outcome.effect_id = ${input.effectId}`;
              if (choices.length !== 1) return reject("choice_changed");
              const row = choices[0]!;
              const command = yield* Schema.decodeUnknownEffect(
                Schema.fromJsonString(OrchestrationV2StartWithImportedHistoryCommand),
              )(row.canonical_command_json, { onExcessProperty: "error" });
              if (
                command.commandId !== input.commandId ||
                command.threadId !== input.threadId ||
                command.delivery.type !== "queued_run" ||
                command.delivery.runId !== input.runId ||
                command.delivery.messageId !== row.message_id ||
                command.reviewedBasis !== row.reviewed_basis ||
                nativeCreationSha256(nativeCreationCanonicalJson(command)) !== row.command_digest
              )
                return reject("choice_changed");
              const basis = yield* Schema.decodeUnknownEffect(
                Schema.fromJsonString(
                  Schema.Struct({
                    snapshot: Schema.Struct({
                      threadId: ThreadId,
                      records: Schema.Record(
                        Schema.String,
                        Schema.Array(Schema.Record(Schema.String, Schema.Unknown)),
                      ),
                    }),
                  }),
                ),
              )(row.basis_json);
              if (basis.snapshot.threadId !== input.threadId) return reject("choice_changed");
              const runs =
                yield* sql`SELECT * FROM orchestration_v2_projection_runs WHERE thread_id = ${input.threadId} AND run_id = ${input.runId}
          AND status = 'queued' AND json_extract(payload_json, '$.activeAttemptId') = ${input.runAttemptId}
          AND json_extract(payload_json, '$.userMessageId') = ${row.message_id}`;
              const attempts =
                yield* sql`SELECT * FROM orchestration_v2_projection_run_attempts WHERE thread_id = ${input.threadId}
          AND run_id = ${input.runId} AND attempt_id = ${input.runAttemptId} AND status = 'pending'`;
              if (runs.length !== 1 || attempts.length !== 1) return reject("queue_changed");
              const originalRun = basis.snapshot.records.runs?.find(
                (run) => run.run_id === input.runId,
              );
              const originalAttempt = basis.snapshot.records.run_attempts?.find(
                (attempt) => attempt.attempt_id === input.runAttemptId,
              );
              const providers =
                yield* sql`SELECT * FROM orchestration_v2_projection_provider_threads WHERE thread_id = ${input.threadId}
          AND provider_thread_id = ${attempts[0]!.provider_thread_id}`;
              const messages =
                yield* sql`SELECT * FROM orchestration_v2_projection_messages WHERE thread_id = ${input.threadId} AND message_id = ${row.message_id}`;
              const originalProvider = basis.snapshot.records.provider_threads?.find(
                (provider) => provider.provider_thread_id === attempts[0]!.provider_thread_id,
              );
              const originalMessage = basis.snapshot.records.messages?.find(
                (message) => message.message_id === row.message_id,
              );
              if (
                originalRun === undefined ||
                originalAttempt === undefined ||
                originalProvider === undefined ||
                originalMessage === undefined ||
                providers.length !== 1 ||
                messages.length !== 1 ||
                nativeCreationCanonicalJson(runs[0]) !== nativeCreationCanonicalJson(originalRun) ||
                nativeCreationCanonicalJson(attempts[0]) !==
                  nativeCreationCanonicalJson(originalAttempt) ||
                nativeCreationCanonicalJson(providers[0]) !==
                  nativeCreationCanonicalJson(originalProvider) ||
                nativeCreationCanonicalJson(messages[0]) !==
                  nativeCreationCanonicalJson(originalMessage)
              )
                return reject("queue_changed");
              const prior =
                yield* sql`SELECT effect_id FROM orchestration_v2_queued_start_reservations WHERE effect_id = ${input.effectId} OR command_id = ${input.commandId}
          UNION ALL SELECT effect_id FROM orchestration_v2_native_effect_confirmations WHERE effect_id = ${input.effectId} OR command_id = ${input.commandId}
          UNION ALL SELECT hold.effect_id FROM orchestration_v2_unknown_effect_holds hold JOIN orchestration_v2_effect_outbox held ON held.effect_id = hold.effect_id
            WHERE held.thread_id = ${input.threadId}
          UNION ALL SELECT event_id FROM orchestration_events WHERE application_event_version = 2 AND command_id = ${input.commandId}
            AND sequence > ${row.result_sequence} AND event_type IN ('provider-thread.updated', 'provider-session.attached', 'provider-session.updated',
              'checkpoint-scope.created', 'node.updated', 'run.updated', 'run-attempt.updated', 'context-handoff.updated')
          UNION ALL SELECT started.effect_id FROM native_creation_effect_facts started JOIN native_creation_intents claim ON claim.claim_id = started.claim_id
            WHERE claim.thread_id = ${input.threadId} AND started.phase = 'started' AND NOT EXISTS (
              SELECT 1 FROM native_creation_effect_facts completed WHERE completed.claim_id = started.claim_id
                AND completed.effect_id = started.effect_id AND completed.phase = 'completed')`;
              if (prior.length !== 0) return reject("prior_effect");
              const heads = yield* sql<{
                readonly run_id: string;
              }>`SELECT queued.run_id FROM orchestration_v2_projection_runs queued
          LEFT JOIN orchestration_v2_projection_messages message ON message.message_id = json_extract(queued.payload_json, '$.userMessageId') AND message.thread_id = queued.thread_id
          WHERE queued.thread_id = ${input.threadId} AND queued.status = 'queued'
          ORDER BY (json_type(message.payload_json, '$.delegatedCompletion') IS NOT NULL) DESC,
            COALESCE(json_extract(queued.payload_json, '$.queuePosition'), queued.ordinal) ASC, queued.ordinal ASC LIMIT 1`;
              if (heads.length !== 1) return reject("queue_changed");
              if (!alreadyParked && heads[0]!.run_id === input.runId) return reject("already_head");
              // A proven scheduling wait never entered preparation; only this claim's scheduling budget is restored.
              if (!alreadyParked) {
                const changed =
                  yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'pending', attempt_count = attempt_count - 1,
            lease_owner = NULL, lease_expires_at = NULL, updated_at = ${now}, last_error = 'imported-history.waiting-for-head/v1'
            WHERE effect_id = ${input.effectId} AND status = 'running' AND lease_owner = ${input.workerId}
              AND attempt_count = ${input.expectedAttempt} AND lease_expires_at > ${now} RETURNING effect_id`;
                if (changed.length !== 1) return reject("claim_changed");
              }
              return Object.freeze({
                status: "parked" as const,
                effectId: input.effectId,
                commandId: input.commandId,
                threadId: input.threadId,
                runId: input.runId,
                runAttemptId: input.runAttemptId,
                commandDigest: row.command_digest,
                schedulingAttempt: input.expectedAttempt - 1,
              }) satisfies ParkedImportedHistoryDeliveryV1;
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new EffectOutboxError({
                  operation: "park-imported-delivery",
                  effectId: input.effectId,
                  cause,
                }),
            ),
          ),
      holdUnknown: ({ effectId, workerId, operationId, evidence, expectedAttempt }) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const decoded = yield* Schema.decodeUnknownEffect(ProviderNativeEffectEvidence)(
                evidence,
              );
              if (
                decoded.outcome !== "unknown" ||
                decoded.operationId !== operationId ||
                !Number.isSafeInteger(expectedAttempt) ||
                expectedAttempt < 1
              )
                return yield* new EffectOutboxError({ operation: "hold-unknown", effectId });
              const now = DateTime.formatIso(yield* DateTime.now);
              const rows = yield* sql<{ readonly effect_id: string }>`
            INSERT INTO orchestration_v2_unknown_effect_holds
              (effect_id, worker_id, operation_id, evidence_json, expected_attempt, held_at)
            SELECT effect_id, ${workerId}, ${operationId},
              ${yield* Schema.encodeEffect(Schema.fromJsonString(ProviderNativeEffectEvidence))(decoded).pipe(Effect.orDie)},
              ${expectedAttempt}, ${now}
            FROM orchestration_v2_effect_outbox
            WHERE effect_id = ${effectId} AND status = 'running'
              AND lease_owner = ${workerId} AND attempt_count = ${expectedAttempt}
              AND lease_expires_at > ${now}
              AND (${decoded.threadId ?? null} IS NULL OR thread_id = ${decoded.threadId ?? null})
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
                WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
            RETURNING effect_id
          `;
              return rows.length === 1;
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isEffectOutboxError(cause)
                ? cause
                : new EffectOutboxError({ operation: "hold-unknown", effectId, cause }),
            ),
          ),
      holdResourceCleanupUnknown: ({ effectId, workerId, expectedAttempt, evidence }) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const decoded = yield* Schema.decodeUnknownEffect(ResourceCleanupUnknownEvidenceV1)(
                evidence,
                { onExcessProperty: "error" },
              );
              if (
                decoded.operationId !== effectId ||
                workerId.length === 0 ||
                !Number.isSafeInteger(expectedAttempt) ||
                expectedAttempt < 1
              )
                return yield* new EffectOutboxError({
                  operation: "hold-resource-cleanup-unknown",
                  effectId,
                });
              const now = DateTime.formatIso(yield* DateTime.now);
              const rows = yield* sql<{ readonly effect_id: string }>`
            INSERT INTO orchestration_v2_unknown_effect_holds
              (effect_id, worker_id, operation_id, evidence_json, expected_attempt, held_at)
            SELECT effect.effect_id, ${workerId}, ${effectId},
              ${yield* Schema.encodeEffect(Schema.fromJsonString(ResourceCleanupUnknownEvidenceV1))(decoded).pipe(Effect.orDie)}, ${expectedAttempt}, ${now}
            FROM orchestration_v2_effect_outbox effect
            LEFT JOIN orchestration_v2_lease_cleanup_task_bindings binding ON binding.effect_id = effect.effect_id
            WHERE effect.effect_id = ${effectId} AND effect.status = 'running' AND effect.lease_owner = ${workerId}
              AND effect.attempt_count = ${expectedAttempt} AND effect.lease_expires_at > ${now}
              AND effect.thread_id = ${decoded.threadId}
              AND (${decoded.bindingSha256 === null ? 1 : 0} = 1 OR (binding.thread_id = ${decoded.threadId}
                AND binding.binding_sha256 = ${decoded.bindingSha256}
                AND json_extract(binding.task_json, '$.kind') = ${decoded.taskKind}))
              AND effect.effect_type = ${`${decoded.taskKind}.cleanup`}
              AND json_extract(effect.payload_json, '$.type') = effect.effect_type
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = effect.effect_id)
            RETURNING effect_id`;
              return rows.length === 1;
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isEffectOutboxError(cause)
                ? cause
                : new EffectOutboxError({
                    operation: "hold-resource-cleanup-unknown",
                    effectId,
                    cause,
                  }),
            ),
          ),
      completeObservedDeletionCleanup: (input) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const rows = yield* sql<{
                readonly ordinal: number;
                readonly outcome_json: string;
                readonly correlation_json: string;
                readonly binding_sha256: string;
                readonly thread_id: string;
                readonly task_json: string;
              }>`
          SELECT outcome.*, binding.binding_sha256, binding.thread_id, binding.task_json
          FROM orchestration_v2_lease_cleanup_task_outcomes outcome
          JOIN orchestration_v2_lease_cleanup_task_bindings binding ON binding.effect_id = outcome.effect_id
          WHERE outcome.effect_id = ${input.effectId} ORDER BY outcome.ordinal DESC LIMIT 1`;
              if (
                rows.length !== 1 ||
                rows[0]!.ordinal !== input.expectedLatestOrdinal ||
                rows[0]!.binding_sha256 !== input.bindingSha256
              )
                return false;
              const row = rows[0]!;
              const outcome = yield* Schema.decodeUnknownEffect(
                Schema.fromJsonString(
                  Schema.Struct({
                    taskId: Schema.NonEmptyString,
                    result: Schema.NullOr(Schema.Literals(["succeeded", "failed"])),
                    effect: Schema.Literals(["confirmed", "absent", "no_effect", "unknown"]),
                  }),
                ),
              )(row.outcome_json, { onExcessProperty: "error" });
              const correlation = yield* Schema.decodeUnknownEffect(
                Schema.fromJsonString(
                  Schema.Struct({
                    workerId: Schema.NonEmptyString,
                    expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)),
                    bindingSha256: Schema.NonEmptyString,
                    evidence: QualifiedDeletionCleanupEvidenceV1,
                  }),
                ),
              )(row.correlation_json, { onExcessProperty: "error" });
              const kind = yield* decodeTaskKind(row.task_json).pipe(
                Effect.catch((error) => Effect.die(jsonCause(error))),
              );
              if (
                outcome.taskId !== input.effectId ||
                outcome.result !== "succeeded" ||
                !["confirmed", "absent"].includes(outcome.effect) ||
                correlation.workerId !== input.workerId ||
                correlation.expectedAttempt !== input.expectedAttempt ||
                correlation.bindingSha256 !== input.bindingSha256 ||
                correlation.evidence.producer !==
                  (kind === "terminal"
                    ? "managed_terminal"
                    : kind === "provider"
                      ? "managed_provider"
                      : kind)
              )
                return false;
              const holds = (yield* service.listHeldByThreadId(
                ThreadId.make(row.thread_id),
              )).filter((hold) => hold.effectId === input.effectId);
              if (
                correlation.evidence.coveredHolds.length !== holds.length ||
                holds.some(
                  (hold) =>
                    !correlation.evidence.coveredHolds.some(
                      (covered) =>
                        nativeCreationCanonicalJson(covered) === nativeCreationCanonicalJson(hold),
                    ),
                )
              )
                return false;
              const effect = Option.getOrNull(yield* service.get(input.effectId));
              if (
                effect === null ||
                effect.threadId !== row.thread_id ||
                effect.request.type !==
                  (kind === "worktree"
                    ? "worktree.cleanup"
                    : kind === "terminal"
                      ? "terminal.cleanup"
                      : kind === "provider"
                        ? "provider-session.detach"
                        : "unsupported") ||
                (kind === "provider" &&
                  (effect.request.type !== "provider-session.detach" ||
                    effect.request.providerSessionId !==
                      (yield* decodeTaskProviderSessionId(row.task_json).pipe(
                        Effect.catch((error) => Effect.die(jsonCause(error))),
                      )))) ||
                effect.attemptCount !== input.expectedAttempt
              )
                return false;
              if (effect.status === "succeeded") return true;
              const now = DateTime.formatIso(yield* DateTime.now);
              const completed =
                yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded', lease_owner = NULL,
          lease_expires_at = NULL, completed_at = ${now}, updated_at = ${now}, last_error = NULL
          WHERE effect_id = ${input.effectId} AND status = 'running' AND lease_owner = ${input.workerId}
            AND attempt_count = ${input.expectedAttempt} RETURNING effect_id`;
              return completed.length === 1;
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new EffectOutboxError({
                  operation: "complete-observed-deletion-cleanup",
                  effectId: input.effectId,
                  cause,
                }),
            ),
          ),
      succeed: ({ effectId, workerId }) =>
        Effect.gen(function* () {
          const now = DateTime.formatIso(yield* DateTime.now);
          const rows = yield* sql<{ readonly effect_id: string }>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = 'succeeded',
              lease_owner = NULL,
              lease_expires_at = NULL,
              completed_at = ${now},
              updated_at = ${now},
              last_error = NULL
            WHERE effect_id = ${effectId}
              AND status = 'running'
              AND lease_owner = ${workerId}
              AND json_type(payload_json, '$.attachmentNamespaceCleanup') IS NULL
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
                WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
            RETURNING effect_id
          `;
          if (rows.length === 1) {
            cancellationSignals.delete(effectId);
            yield* notifyAvailable();
          }
          return rows.length === 1;
        }).pipe(
          Effect.mapError(
            (cause) => new EffectOutboxError({ operation: "succeed", effectId, cause }),
          ),
        ),
      retry: ({ effectId, workerId, error, delayMs }) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const nowIso = DateTime.formatIso(now);
          const availableAt = DateTime.formatIso(
            DateTime.add(now, { milliseconds: Math.max(0, delayMs) }),
          );
          const rows = yield* sql<{ readonly effect_id: string }>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = 'pending',
              available_at = ${availableAt},
              lease_owner = NULL,
              lease_expires_at = NULL,
              updated_at = ${nowIso},
              last_error = ${error}
            WHERE effect_id = ${effectId}
              AND status = 'running'
              AND lease_owner = ${workerId}
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
                WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
            RETURNING effect_id
          `;
          if (rows.length === 1) {
            cancellationSignals.delete(effectId);
            yield* notifyAvailable();
          }
          return rows.length === 1;
        }).pipe(
          Effect.mapError(
            (cause) => new EffectOutboxError({ operation: "retry", effectId, cause }),
          ),
        ),
      fail: ({ effectId, workerId, error }) =>
        Effect.gen(function* () {
          const now = DateTime.formatIso(yield* DateTime.now);
          const rows = yield* sql<{ readonly effect_id: string }>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = 'failed',
              lease_owner = NULL,
              lease_expires_at = NULL,
              completed_at = ${now},
              updated_at = ${now},
              last_error = ${error}
            WHERE effect_id = ${effectId}
              AND status = 'running'
              AND lease_owner = ${workerId}
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
                WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
            RETURNING effect_id
          `;
          if (rows.length === 1) {
            cancellationSignals.delete(effectId);
            yield* notifyAvailable();
          }
          return rows.length === 1;
        }).pipe(
          Effect.mapError((cause) => new EffectOutboxError({ operation: "fail", effectId, cause })),
        ),
    };

    return service;
  }),
);
