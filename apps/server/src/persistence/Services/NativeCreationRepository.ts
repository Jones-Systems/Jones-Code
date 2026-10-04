import {
  NativeCreationEffect,
  NativeCreationEffectV2,
  NativeCreationHistoricalBinding,
  type NativeCreationObservationV2,
  CommandId,
  ThreadId,
  type MessageId,
  type EventId,
  type RunId,
  type RunAttemptId,
  type NativeCommandIdentityV2,
  type OrchestrationV2Command,
  type AuthSessionId,
  NativeThreadIncarnationV2,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  type NativeCreationAuthorityError,
  type NativeCreationResources,
  type NativeCreationExecutionReferenceV2,
} from "../../orchestration-v2/NativeCreationAuthority.ts";
import { type ValidatedNativeCreationPreparation } from "../../orchestration-v2/NativeCreationPreparation.ts";
import {
  type ProviderRuntimeBinding,
  type ProviderNativeEffectEvidence,
} from "../../orchestration-v2/ProviderAdapter.ts";

declare const nativeEffectConfirmationProof: unique symbol;
export interface NativeEffectConfirmationV1 {
  readonly [nativeEffectConfirmationProof]: true;
  readonly version: 1;
  readonly effectId: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly workerId: string;
  readonly expectedAttempt: number;
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly binding: ProviderRuntimeBinding;
  readonly evidenceRevision: number;
  readonly evidence: ProviderNativeEffectEvidence;
  readonly nativeExecutionReference: NativeCreationExecutionReferenceV2 | null;
  /** Correlates the accepted command; the complete native evidence proves the ACK. */
  readonly commandEvent: { readonly eventId: EventId; readonly sequence: number };
  readonly confirmedAt: string;
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

export type NativeCreationBoundedHistoryV2 = Omit<
  NativeCreationObservationV2,
  "version" | "schema" | "incarnation" | "outcome"
> & {
  readonly originalCommandId: CommandId;
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
};

export interface NativeCreationResolvedExecutionV2 {
  readonly reference: NativeCreationExecutionReferenceV2;
  readonly history: NativeCreationHistory;
  readonly preparation: ValidatedNativeCreationPreparation;
  readonly command: OrchestrationV2Command;
  readonly nativeIdentity: NativeCommandIdentityV2;
}

export const NativeCreationThreadRecoveryCommandV2 = Schema.Struct({
  version: Schema.Literal(2),
  claimId: Schema.NonEmptyString,
  commandId: CommandId,
  threadId: ThreadId,
  commandType: Schema.Literal("thread.delete"),
  canonicalCommand: Schema.Struct({
    type: Schema.Literal("thread.delete"),
    commandId: CommandId,
    threadId: ThreadId,
  }),
  commandDigest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  commandStartEffectId: Schema.NonEmptyString,
  cleanupStartEffectId: Schema.NonEmptyString,
  cleanupStartOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  recoveryScopeId: Schema.NonEmptyString,
  resource: Schema.Struct({
    kind: Schema.Literal("thread"),
    threadId: ThreadId,
    incarnation: NativeThreadIncarnationV2,
  }),
});
export type NativeCreationThreadRecoveryCommandV2 =
  typeof NativeCreationThreadRecoveryCommandV2.Type;

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
export type NativeCreationStartedFactV2 = Extract<NativeCreationEffectV2, { phase: "started" }>;
export type NativeCreationCompletedFactV2 = Extract<NativeCreationEffectV2, { phase: "completed" }>;

export class NativeCreationRepository extends Context.Service<
  NativeCreationRepository,
  {
    readonly reserveThreadRecoveryCommand: (
      reference: NativeCreationThreadRecoveryCommandV2,
    ) => Effect.Effect<NativeCreationThreadRecoveryCommandV2, NativeCreationRepositoryError>;
    readonly readThreadRecoveryCommand: (
      commandId: string,
    ) => Effect.Effect<NativeCreationThreadRecoveryCommandV2 | null, NativeCreationRepositoryError>;
    readonly recordNativeEffectConfirmation: (input: {
      readonly effectId: string;
      readonly workerId: string;
      readonly expectedAttempt: number;
      readonly runId: RunId;
      readonly attemptId: RunAttemptId;
      readonly binding: ProviderRuntimeBinding;
      readonly expectedEvidenceRevision: number;
      readonly evidence: ProviderNativeEffectEvidence;
    }) => Effect.Effect<NativeEffectConfirmationV1, NativeCreationRepositoryError>;
    readonly readNativeEffectConfirmation: (
      effectId: string,
    ) => Effect.Effect<NativeEffectConfirmationV1 | null, NativeCreationRepositoryError>;
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
    readonly readExecutionReference: (
      reference: NativeCreationExecutionReferenceV2,
    ) => Effect.Effect<NativeCreationResolvedExecutionV2, NativeCreationRepositoryError>;
    readonly readBoundedHistoryByThread: (
      threadId: ThreadId,
    ) => Effect.Effect<NativeCreationBoundedHistoryV2 | null, NativeCreationRepositoryError>;
    readonly validateCommandAcceptanceV2: (input: {
      readonly commandId: CommandId;
      readonly threadId: ThreadId;
      readonly commandType: "thread.create" | "message.dispatch";
      readonly commandDigest: string;
      readonly bindingDigest: string;
      readonly eventId: EventId;
      readonly sequence: number;
    }) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly startEffectV2: (
      reference: NativeCreationExecutionReferenceV2,
      timestamp: string,
      authorize: Effect.Effect<NativeCreationHistoricalBinding, NativeCreationAuthorityError>,
    ) => Effect.Effect<
      { readonly status: "started"; readonly fact: NativeCreationStartedFactV2 },
      NativeCreationRepositoryError | NativeCreationAuthorityError
    >;
    readonly completeEffectV2: (
      reference: NativeCreationExecutionReferenceV2,
      completion: {
        readonly timestamp: string;
        readonly eventId: EventId;
        readonly sequence: number;
      },
    ) => Effect.Effect<NativeCreationCompletedFactV2, NativeCreationRepositoryError>;
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
      command: OrchestrationV2Command,
    ) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly reserveCommand: (
      claimId: string,
      command: OrchestrationV2Command,
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
