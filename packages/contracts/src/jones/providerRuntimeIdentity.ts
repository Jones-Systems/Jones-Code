import * as Schema from "effect/Schema";
import {
  NonNegativeInt,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  TrimmedNonEmptyString,
} from "../baseSchemas.ts";
import { ProviderDriverKind, ProviderInstanceId } from "../providerInstance.ts";

/** Missing native evidence must never be filled from routing or authentication metadata. */
export const RuntimeIdentityObservation = Schema.Union([
  Schema.Struct({ status: Schema.Literal("unknown") }),
  Schema.Struct({ status: Schema.Literal("unavailable"), reason: TrimmedNonEmptyString }),
  Schema.Struct({
    status: Schema.Literal("observed"),
    value: TrimmedNonEmptyString,
    sourceEvent: TrimmedNonEmptyString,
  }),
]);
export type RuntimeIdentityObservation = typeof RuntimeIdentityObservation.Type;

export const ObservedRuntimeIdentity = Schema.Struct({
  backend: RuntimeIdentityObservation,
  model: RuntimeIdentityObservation,
  account: RuntimeIdentityObservation,
  serviceTier: RuntimeIdentityObservation,
});
export type ObservedRuntimeIdentity = typeof ObservedRuntimeIdentity.Type;

export const RequestedRuntimeIdentity = Schema.Struct({
  providerInstanceId: ProviderInstanceId,
  providerDriver: ProviderDriverKind,
  model: TrimmedNonEmptyString,
  serviceTier: Schema.NullOr(TrimmedNonEmptyString),
});
export type RequestedRuntimeIdentity = typeof RequestedRuntimeIdentity.Type;

export const RuntimeIdentityAttestation = Schema.Struct({
  runtimeGeneration: Schema.optional(TrimmedNonEmptyString),
  evidenceRevision: Schema.optional(NonNegativeInt),
  requested: RequestedRuntimeIdentity,
  observed: ObservedRuntimeIdentity,
});
export type RuntimeIdentityAttestation = typeof RuntimeIdentityAttestation.Type;

/** A process incarnation and the native conversation it actually owns are independent IDs. */
export const ProviderRuntimeBinding = Schema.Struct({
  threadId: ThreadId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
  providerInstanceId: ProviderInstanceId,
  driver: ProviderDriverKind,
  nativeThreadId: TrimmedNonEmptyString,
  runtimeGeneration: TrimmedNonEmptyString,
});
export type ProviderRuntimeBinding = typeof ProviderRuntimeBinding.Type;

export const ProviderRuntimeEvidenceCapture = Schema.Struct({
  ...ProviderRuntimeBinding.fields,
  runtimeGeneration: Schema.optional(TrimmedNonEmptyString),
  evidenceRevision: Schema.optional(NonNegativeInt),
});
export type ProviderRuntimeEvidenceCapture = typeof ProviderRuntimeEvidenceCapture.Type;
