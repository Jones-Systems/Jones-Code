import { NativeCreationExecutionReferenceV2 } from "../nativeCreation/NativeCreationExecutionTypes.ts";
import {
  CheckpointId,
  CheckpointScopeId,
  CommandId,
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
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { DelegatedCheckoutPlanV1 } from "./DelegatedCheckoutPolicy.ts";
import { ProviderNativeEffectEvidence } from "./ProviderAdapter.ts";

export const OrchestrationEffectRequestV2 = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("delegated-workspace.prepare"),
    runId: RunId,
    plan: DelegatedCheckoutPlanV1,
  }),
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
  "delegated-workspace.prepare",
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
  readonly nativeCreationExecutionReference?: NativeCreationExecutionReferenceV2;
  readonly id: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly request: OrchestrationEffectRequestV2;
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
  readonly nativeCreationExecutionReference?: NativeCreationExecutionReferenceV2;
  readonly id: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly request: OrchestrationEffectRequestV2;
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
  readonly awaitAvailable: Effect.Effect<void>;
  readonly notifyAvailable: (count?: number) => Effect.Effect<void>;
  readonly claimOrdinaryFinalCheckpointRow?: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly expectedAttemptCount: number;
    readonly leaseDurationMs: number;
  }) => Effect.Effect<OrchestrationEffectV2 | null, EffectOutboxError>;
  readonly settleOrdinaryCheckoutStartClaim?: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly expectedAttempt: number;
    readonly expectedLeaseExpiresAt: string;
  }) => Effect.Effect<boolean, EffectOutboxError>;
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
  readonly claimNext: (input: {
    readonly workerId: string;
    readonly leaseDurationMs: number;
    readonly excludeRestartContinuations?: boolean;
  }) => Effect.Effect<Option.Option<OrchestrationEffectV2>, EffectOutboxError>;
  readonly nextClaimableAt: Effect.Effect<Option.Option<DateTime.Utc>, EffectOutboxError>;
  readonly listHeldByThreadId: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<UnknownEffectHoldV2>, EffectOutboxError>;
  readonly renewClaim: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly expectedAttempt: number;
    readonly expectedLeaseExpiresAt: string;
    readonly leaseExpiresAt: string;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly holdUnknown: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly operationId: string;
    readonly evidence: ProviderNativeEffectEvidence;
    readonly expectedAttempt: number;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly succeed: (input: {
    readonly effectId: string;
    readonly workerId: string;
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
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
// A malformed envelope cannot lose its authority lineage by decoding as ordinary work.
export const decodeOrchestrationEffectPayloadV2 = (payload: string) =>
  Effect.gen(function* () {
    const raw = yield* decodeJson(payload);
    if (
      typeof raw === "object" &&
      raw !== null &&
      (Object.hasOwn(raw, "request") || Object.hasOwn(raw, "nativeCreationExecutionReference"))
    )
      return yield* decodeNativePayload(raw);
    return { request: yield* decodeRequest(payload) };
  });
const invalidEffectPayload = (message: string) =>
  new EffectOutboxError({ operation: "payload", cause: new Error(message) });

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
    Effect.map((payload): OrchestrationEffectV2 => ({
      ...payload,

      id: row.effect_id,
      commandId: CommandId.make(row.command_id),
      threadId: ThreadId.make(row.thread_id),
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
    // Each thread runs its effects one at a time, in enqueue (rowid) order. An earlier
    // effect waiting out a retry backoff still blocks later ones, so a turn
    // cannot start while a failed rollback is about to restore files. A claim
    // that skips restart continuations is not blocked by them either.
    // Title generation is correlated metadata work, so it has its own
    // per-thread lane and cannot delay provider lifecycle effects.
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

    const claimableCandidatePredicate = (
      availableBefore?: string,
      excludeRestartContinuations = false,
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
        AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
          WHERE hold.effect_id = candidate.effect_id)
        AND NOT EXISTS (
          SELECT 1
          FROM orchestration_v2_effect_outbox AS active
          WHERE active.thread_id = candidate.thread_id
            AND (
              active.status = 'running'
              OR (
                active.status = 'pending'
                AND active.rowid < candidate.rowid
                AND ${
                  excludeRestartContinuations
                    ? sql`active.effect_type != 'provider-runtime.continue'`
                    : sql`1 = 1`
                }
              )
            )
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

    const service: EffectOutboxV2Shape = {
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
              AND ${claimableCandidatePredicate(now, false, true)}) RETURNING *`;
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
                    yield* validateNativeAssociation(effect);
                    const payload =
                      effect.nativeCreationExecutionReference === undefined
                        ? encodeRequest(effect.request)
                        : encodeNativePayload(
                            yield* decodeNativePayload({
                              request: effect.request,
                              nativeCreationExecutionReference:
                                effect.nativeCreationExecutionReference,
                            }),
                          );
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
                AND (${effect.nativeCreationExecutionReference !== undefined ? sql`1 = 1` : sql`0 = 1`} OR
                  CASE WHEN json_valid(payload_json) THEN
                    json_type(payload_json, '$.request') IS NOT NULL OR json_type(payload_json, '$.nativeCreationExecutionReference') IS NOT NULL
                  ELSE 0 END)`;
                    if (
                      existing.length !== 0 &&
                      (existing.length !== 1 ||
                        existing[0]!.command_id !== effect.commandId ||
                        existing[0]!.thread_id !== effect.threadId ||
                        existing[0]!.effect_type !== effect.request.type ||
                        existing[0]!.payload_json !== payload)
                    )
                      return yield* invalidEffectPayload(
                        "Referenced outbox effect identity is already bound differently",
                      );
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
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
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
      reconcileAfterProcessLoss: Effect.gen(function* () {
        const now = DateTime.formatIso(yield* DateTime.now);
        const cancelledRows = yield* sql<{ readonly effect_id: string }>`
          UPDATE orchestration_v2_effect_outbox
          SET
            status = 'cancelled',
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = ${now},
            updated_at = ${now},
            last_error = 'Cancelled because the server process ended before the effect completed.'
          WHERE status IN ('pending', 'running')
            AND effect_type IN ${sql.in(PROCESS_BOUND_EFFECT_TYPES)}
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
          RETURNING effect_id
        `;
        const requeuedRows = yield* sql<{ readonly effect_id: string }>`
          UPDATE orchestration_v2_effect_outbox
          SET
            status = 'pending',
            lease_owner = NULL,
            lease_expires_at = NULL,
            available_at = ${now},
            updated_at = ${now},
            last_error = 'Requeued after the previous server process ended.'
          WHERE status = 'running'
            AND effect_type IN ${sql.in(REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS)}
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
          RETURNING effect_id
        `;
        if (requeuedRows.length > 0) yield* notifyAvailable(requeuedRows.length);
        return { requeued: requeuedRows.length, cancelled: cancelledRows.length };
      }).pipe(
        Effect.mapError(
          (cause) => new EffectOutboxError({ operation: "reconcile-process-loss", cause }),
        ),
      ),
      claimNext: ({ workerId, leaseDurationMs, excludeRestartContinuations = false }) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const nowIso = DateTime.formatIso(now);
          const leaseExpiresAt = DateTime.formatIso(
            DateTime.add(now, { milliseconds: Math.max(1, leaseDurationMs) }),
          );
          // Empty safety claims are expected while the daemon is idle. Tracing
          // this query records the full SQL text on every poll and can dominate
          // the local trace without adding actionable information.
          const claimStatement = sql<EffectRow>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = 'running',
              attempt_count = attempt_count + 1,
              lease_owner = ${workerId},
              lease_expires_at = ${leaseExpiresAt},
              updated_at = ${nowIso},
              last_error = NULL
            WHERE effect_id = (
              SELECT candidate.effect_id
              FROM orchestration_v2_effect_outbox AS candidate
              WHERE ${claimableCandidatePredicate(nowIso, excludeRestartContinuations)}
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
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
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
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
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
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
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
