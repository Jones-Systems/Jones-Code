import * as Schema from "effect/Schema";
import { TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

/**
 * One provider-reported runtime identity dimension.
 *
 * `unknown` means no authoritative provider evidence has arrived for the
 * current request. `unavailable` means the provider boundary does not safely
 * attest the dimension. Neither state may be filled from routing settings,
 * authentication metadata, or a model catalog.
 */
export const RuntimeIdentityObservation = Schema.Union([
  Schema.Struct({ status: Schema.Literal("unknown") }),
  Schema.Struct({
    status: Schema.Literal("unavailable"),
    reason: TrimmedNonEmptyString,
  }),
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

export const RuntimeIdentityAttestation = Schema.Struct({
  /** Opaque launch correlation. Observations must match this exact runtime. */
  runtimeGeneration: Schema.optional(TrimmedNonEmptyString),
  requested: Schema.Struct({
    providerInstanceId: ProviderInstanceId,
    providerDriver: TrimmedNonEmptyString,
    model: TrimmedNonEmptyString,
    serviceTier: Schema.NullOr(TrimmedNonEmptyString),
  }),
  observed: ObservedRuntimeIdentity,
});
export type RuntimeIdentityAttestation = typeof RuntimeIdentityAttestation.Type;
