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

// OS units and namespace are explicit; executable metadata is not a build attestation.
export const PhysicalProcessIdentity = Schema.Struct({
  platform: Schema.Literal("linux"),
  bootId: Schema.String,
  pidNamespace: Schema.String,
  pid: NonNegativeInt,
  startTime: Schema.String,
  startTimeUnit: Schema.Literal("linux-clock-ticks-since-boot"),
  executableIdentity: Schema.String,
  build: Schema.Literal("unknown"),
});
export type PhysicalProcessIdentity = typeof PhysicalProcessIdentity.Type;

export const PhysicalProcessObservation = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("observed"),
    runtimeGeneration: Schema.String,
    launcherPid: NonNegativeInt,
    responder: PhysicalProcessIdentity,
    source: Schema.Literal("codex.server/diagnostics+local-os"),
  }),
  Schema.Struct({
    status: Schema.Literal("unknown"),
    runtimeGeneration: Schema.String,
    reason: Schema.Literals([
      "unsupported",
      "capability_disabled",
      "malformed",
      "transport",
      "timeout",
      "retired",
      "os_unavailable",
      "process_absent",
      "launcher_unavailable",
      "provenance_unavailable",
      "responder_mismatch",
      "incarnation_changed",
      "storage_unavailable",
      "not_captured",
    ]),
    diagnostics: Schema.optional(
      Schema.Struct({ status: Schema.Literal("supported"), reportedPid: NonNegativeInt }),
    ),
    localLauncher: Schema.optional(PhysicalProcessIdentity),
  }),
]);
export type PhysicalProcessObservation = typeof PhysicalProcessObservation.Type;
