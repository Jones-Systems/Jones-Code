import {
  NativeCreationEffect,
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
  ThreadTurnStartCommand,
  CommandId, ThreadId, EventId, RunId, RunAttemptId, IsoDateTime,
  NativeThreadIncarnationV2, NativeCreationEffectV2,
  type NativeCreationObservationV2, type NativeCommandIdentityV2,
  type AuthSessionId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  type NativeCreationAuthorityError,
  type NativeCreationResources,
} from "../../orchestration-v2/NativeCreationAuthority.ts";
import type { ProviderRuntimeBinding, ProviderNativeEffectEvidence } from "../../orchestration-v2/ProviderAdapter.ts";
import { type NativeCreationExecutionReferenceV2 } from "../../orchestration-v2/NativeCreationAuthority.ts";
import { type ValidatedNativeCreationPreparation } from "../../orchestration-v2/NativeCreationPreparation.ts";

// Retained V1 activity bodies are historical ledger input, never V2 execution commands.
const LegacyNativeActivityCommand = Schema.Struct({
  type: Schema.Literal("thread.activity.append"), commandId: CommandId, threadId: ThreadId,
  activity: Schema.Struct({ id: Schema.String, tone: Schema.String, kind: Schema.String,
    summary: Schema.String, payload: Schema.Record(Schema.String, Schema.Unknown),
    turnId: Schema.NullOr(Schema.String), createdAt: IsoDateTime }), createdAt: IsoDateTime,
});
export const NativeCreationCommand = Schema.Union([OrchestrationV2Command, ThreadTurnStartCommand, LegacyNativeActivityCommand]);
export type NativeCreationCommand = typeof NativeCreationCommand.Type;
export type NativeCreationStageCommandV2 = Extract<OrchestrationV2Command,
  { type: "thread.create" | "message.dispatch" | "prepared-run.release" }>;

export type NativeCreationBoundedHistoryV2 = Omit<NativeCreationObservationV2,
  "version" | "schema" | "incarnation" | "outcome"> & {
  readonly originalCommandId: CommandId; readonly threadId: ThreadId; readonly messageId: string;
};
export interface NativeCreationResolvedExecutionV2 {
  readonly reference: NativeCreationExecutionReferenceV2;
  readonly command: NativeCreationStageCommandV2;
  readonly nativeIdentity: NativeCommandIdentityV2;
  readonly preparation: ValidatedNativeCreationPreparation;
  readonly history: NativeCreationHistory;
}
export const NativeCreationThreadRecoveryCommandV2 = Schema.Struct({
  version: Schema.Literal(2), claimId: Schema.NonEmptyString, commandId: CommandId, threadId: ThreadId,
  commandType: Schema.Literal("thread.delete"),
  canonicalCommand: Schema.Struct({ type: Schema.Literal("thread.delete"), commandId: CommandId, threadId: ThreadId }),
  commandDigest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  commandStartEffectId: Schema.NonEmptyString, cleanupStartEffectId: Schema.NonEmptyString,
  cleanupStartOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)), recoveryScopeId: Schema.NonEmptyString,
  resource: Schema.Struct({ kind: Schema.Literal("thread"), threadId: ThreadId, incarnation: NativeThreadIncarnationV2 }),
});
export type NativeCreationThreadRecoveryCommandV2 = typeof NativeCreationThreadRecoveryCommandV2.Type;
export interface NativeEffectConfirmationV1 {
  readonly version: 1; readonly effectId: string; readonly commandId: CommandId; readonly threadId: ThreadId;
  readonly workerId: string; readonly operationId: string; readonly runId: RunId; readonly attemptId: RunAttemptId;
  readonly expectedAttempt: number; readonly binding: ProviderRuntimeBinding; readonly evidence: ProviderNativeEffectEvidence;
  readonly evidenceRevision: number; readonly nativeExecutionReference: NativeCreationExecutionReferenceV2 | null;
  readonly commandEventId: EventId; readonly commandEventSequence: number; readonly confirmedAt: string;
}

export class NativeCreationRepositoryError extends Schema.TaggedError<NativeCreationRepositoryError>()(
  "NativeCreationRepositoryError",
  { code: Schema.Literals(["conflict", "unresolved_claim"]), message: Schema.String },
) {}

export const NativeCreationStoredIntent = Schema.Struct({
  claimId: Schema.String,
  claimedBootId: Schema.String,
  claimedAt: Schema.String,
  actorSessionId: Schema.String,
  grantId: Schema.String,
  grantRevision: Schema.Int,
  preparationId: Schema.String,
  operationId: Schema.String,
  preparationSha256: Schema.String,
  bindingDigest: Schema.String,
  promptDigest: Schema.String,
  commandDigest: Schema.String,
  commandId: Schema.String,
  threadId: Schema.String,
  messageId: Schema.String,
  canonicalPreparation: Schema.String,
  binding: NativeCreationHistoricalBinding,
  resources: Schema.Struct({
    projectCwd: Schema.String,
    branch: Schema.String,
    worktreePath: Schema.String,
  }),
});
export type NativeCreationStoredIntent = typeof NativeCreationStoredIntent.Type;

export interface NativeCreationHistory {
  readonly intent: NativeCreationStoredIntent;
  readonly normalizedCommandDigest: string | null;
  readonly effects: ReadonlyArray<NativeCreationEffect>;
  readonly effectsV2: ReadonlyArray<NativeCreationEffectV2>;
  readonly effectOverflow: boolean;
}

export interface NativeCreationClaimInput {
  readonly preparation: ValidatedNativeCreationPreparation;
  readonly resources: NativeCreationResources;
  readonly claimId: string;
  readonly claimedBootId: string;
  readonly claimedAt: string;
  readonly actorSessionId: string;
  readonly grantId: string;
  readonly grantRevision: number;
}

export interface NativeCreationReservedCommandIdentity {
  readonly claimId: string;
  readonly commandId: string;
  readonly threadId: string;
}

export interface NativeCreationReservedCommand {
  readonly claimId: string;
  readonly commandId: string;
  readonly threadId: string;
  readonly commandType: string;
  readonly commandDigest: string;
  readonly canonicalCommand: string;
}

type WithoutOrdinal<Fact> = Fact extends NativeCreationEffect ? Omit<Fact, "ordinal"> : never;
export type NativeCreationStartedFact = Extract<NativeCreationEffect, { phase: "started" }>;
export type NativeCreationCompletedFact = Extract<NativeCreationEffect, { phase: "completed" }>;

export class NativeCreationRepository extends Context.Service<
  NativeCreationRepository,
  {
    readonly readBoundedHistoryByThread: (threadId: ThreadId) => Effect.Effect<NativeCreationBoundedHistoryV2 | null, NativeCreationRepositoryError>;
    readonly readThreadRecoveryCommand: (commandId: string) => Effect.Effect<NativeCreationThreadRecoveryCommandV2 | null, NativeCreationRepositoryError>;
    readonly reserveThreadRecoveryCommand: (input: NativeCreationThreadRecoveryCommandV2) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly readNativeEffectConfirmation: (effectId: string) => Effect.Effect<NativeEffectConfirmationV1 | null, NativeCreationRepositoryError>;
    readonly recordNativeEffectConfirmation: (input: {
      readonly effectId: string; readonly workerId: string; readonly expectedAttempt: number;
      readonly runId: RunId; readonly attemptId: RunAttemptId; readonly binding: ProviderRuntimeBinding;
      readonly expectedEvidenceRevision: number; readonly evidence: ProviderNativeEffectEvidence;
    }) => Effect.Effect<NativeEffectConfirmationV1, NativeCreationRepositoryError>;
    readonly validateCommandAcceptanceV2: (input: {
      readonly commandId: CommandId; readonly threadId: ThreadId; readonly commandType: NativeCreationStageCommandV2["type"];
      readonly commandDigest: string; readonly bindingDigest: string; readonly eventId: EventId; readonly sequence: number;
    }) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly readExecutionReference: (reference: NativeCreationExecutionReferenceV2) => Effect.Effect<NativeCreationResolvedExecutionV2, NativeCreationRepositoryError>;
    readonly startEffectV2: (reference: NativeCreationExecutionReferenceV2, timestamp: string,
      authorize: Effect.Effect<NativeCreationHistoricalBinding, NativeCreationAuthorityError>) => Effect.Effect<{
        readonly status: "started"; readonly fact: Extract<NativeCreationEffectV2, {phase: "started"}>;
      }, NativeCreationRepositoryError | NativeCreationAuthorityError>;
    readonly completeEffectV2: (reference: NativeCreationExecutionReferenceV2, input: {
      readonly timestamp: string; readonly eventId: EventId; readonly sequence: number;
    }) => Effect.Effect<Extract<NativeCreationEffectV2, {phase: "completed"}>, NativeCreationRepositoryError>;
    readonly hasAutomationEnrollment: (
      actorSessionId: AuthSessionId,
    ) => Effect.Effect<boolean, NativeCreationRepositoryError>;
    readonly claim: (
      input: NativeCreationClaimInput,
      authorize: Effect.Effect<NativeCreationHistoricalBinding, NativeCreationAuthorityError>,
    ) => Effect.Effect<
      {
        readonly status: "claimed" | "duplicate";
        readonly history: NativeCreationHistory;
      },
      NativeCreationRepositoryError | NativeCreationAuthorityError
    >;
    readonly readHistory: (
      commandId: string,
    ) => Effect.Effect<Option.Option<NativeCreationHistory>, NativeCreationRepositoryError>;
    readonly readHistoryByClaim: (
      claimId: string,
    ) => Effect.Effect<NativeCreationHistory, NativeCreationRepositoryError>;
    readonly reserveCommandIdentities: (
      claimId: string,
      commandIds: ReadonlyArray<string>,
    ) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly getReservedCommandIdentity: (
      commandId: string,
    ) => Effect.Effect<
      Option.Option<NativeCreationReservedCommandIdentity>,
      NativeCreationRepositoryError
    >;
    readonly recordNormalizedCommand: (
      claimId: string,
      command: NativeCreationCommand,
    ) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly reserveCommand: (
      claimId: string,
      command: NativeCreationCommand,
    ) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly getReservedCommand: (
      commandId: string,
    ) => Effect.Effect<Option.Option<NativeCreationReservedCommand>, NativeCreationRepositoryError>;
    readonly startEffect: (
      claimId: string,
      fact: WithoutOrdinal<NativeCreationStartedFact>,
      authorize: Effect.Effect<NativeCreationHistoricalBinding, NativeCreationAuthorityError>,
    ) => Effect.Effect<
      NativeCreationStartedFact,
      NativeCreationRepositoryError | NativeCreationAuthorityError
    >;
    readonly completeEffect: (
      claimId: string,
      fact: WithoutOrdinal<NativeCreationCompletedFact>,
    ) => Effect.Effect<NativeCreationCompletedFact, NativeCreationRepositoryError>;
  }
>()("t3/persistence/Services/NativeCreationRepository") {}
