import {
  NativeCreationEffect,
  NativeCreationHistoricalBinding,
  type OrchestrationV2Command,
  type AuthSessionId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  type NativeCreationAuthorityError,
  type NativeCreationResources,
} from "./NativeCreationAuthority.ts";
import { type ValidatedNativeCreationPreparation } from "./NativeCreationPreparation.ts";

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

// Stored historical reservations remain readable; new reservations decode current V2 commands.
export interface NativeCreationReservedCommand {
  readonly claimId: string;
  readonly commandId: string;
  readonly threadId: string;
  readonly commandType:
    | OrchestrationV2Command["type"]
    | Extract<NativeCreationEffect, { kind: "native_command" }>["commandType"];
  readonly commandDigest: string;
  readonly canonicalCommand: string;
}

type WithoutOrdinal<Fact> = Fact extends NativeCreationEffect ? Omit<Fact, "ordinal"> : never;
export type NativeCreationStartedFact = Extract<NativeCreationEffect, { phase: "started" }>;
export type NativeCreationCompletedFact = Extract<NativeCreationEffect, { phase: "completed" }>;

export class NativeCreationRepository extends Context.Service<
  NativeCreationRepository,
  {
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
  // @effect-diagnostics-next-line deterministicKeys:off - Preserve the pre-extraction service key for compatibility.
>()("t3/nativeCreation/NativeCreationRepository") {}
