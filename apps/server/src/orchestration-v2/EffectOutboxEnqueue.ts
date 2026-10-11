import { NativeCreationExecutionReferenceV2 } from "../jones/nativeCreation/NativeCreationExecutionTypes.ts";
import { LegacyOwnedTerminalControl } from "./RecordedTypes.ts";
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
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export const OrchestrationEffectRequestV2 = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("provider-runtime.continue"),
    sourceRunId: RunId,
    preparedForRestart: Schema.optional(Schema.Boolean),
    continueWithoutPreference: Schema.optional(Schema.Boolean),
  }),
  Schema.Struct({
    type: Schema.Literal("provider-session.detach"),
    runtimeStopCommandId: Schema.optional(CommandId),
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
    legacyOwnedControl: Schema.optional(LegacyOwnedTerminalControl),
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
  /** Follows a Stop: sends `thread.stop` to every delegated task under the stopped thread. */
  Schema.Struct({
    type: Schema.Literal("delegated-tasks.stop"),
    reason: Schema.optional(Schema.String),
  }),
]);
export type OrchestrationEffectRequestV2 = typeof OrchestrationEffectRequestV2.Type;

export interface PendingOrchestrationEffectV2 {
  readonly id: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly nativeCreationExecutionReference?: NativeCreationExecutionReferenceV2;
  readonly request: OrchestrationEffectRequestV2;
  readonly availableAt?: DateTime.Utc;
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

export const NativeOrchestrationEffectPayloadV2 = Schema.Struct({
  request: OrchestrationEffectRequestV2,
  nativeCreationExecutionReference: NativeCreationExecutionReferenceV2,
});
const encodeRequest = Schema.encodeSync(Schema.fromJsonString(OrchestrationEffectRequestV2));
const encodeNativePayload = Schema.encodeSync(
  Schema.fromJsonString(NativeOrchestrationEffectPayloadV2),
);

/** Persist rows through the caller's SQL context; notification belongs after its transaction commits. */
export const enqueue: (
  effects: ReadonlyArray<PendingOrchestrationEffectV2>,
) => Effect.Effect<void, EffectOutboxError, SqlClient.SqlClient> = Effect.fnUntraced(
  function* (effects: ReadonlyArray<PendingOrchestrationEffectV2>) {
    const sql = yield* SqlClient.SqlClient;
    const now = yield* DateTime.now;
    const nowIso = DateTime.formatIso(now);
    yield* Effect.forEach(
      effects,
      (effect) => sql`
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
          ${effect.nativeCreationExecutionReference === undefined ? encodeRequest(effect.request) : encodeNativePayload({ request: effect.request, nativeCreationExecutionReference: effect.nativeCreationExecutionReference })},
          'pending',
          0,
          ${DateTime.formatIso(effect.availableAt ?? now)},
          ${nowIso},
          ${nowIso}
        )
        ON CONFLICT(effect_id) DO NOTHING
      `,
      { concurrency: 1, discard: true },
    );
    // Do not signal here: callers enqueue inside a larger transaction,
    // and workers must only observe availability after that transaction
    // commits. EventSink owns the corresponding post-commit notification.
  },
  Effect.mapError((cause) => new EffectOutboxError({ operation: "enqueue", cause })),
);
