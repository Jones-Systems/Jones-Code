import type { ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";

export interface ProviderContinuationSource {
  readonly provenance: "v2_binding" | "legacy_row" | "native_import";
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId | null;
  readonly driver: ProviderDriverKind;
  readonly nativeThreadId: string | null;
  readonly continuationKey?: string | null;
  readonly status?: string;
}

export interface ProviderContinuationTarget {
  readonly providerInstanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly continuationKey: string;
  readonly supportsNativeResume: boolean;
}

/** Only the persistence owner may derive this from an actual decoded stopped row. */
export interface LegacyStoppedRuntimeProofV1 {
  readonly schema: "t3.legacy-stopped-runtime-proof/v1";
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId | null;
  readonly driver: ProviderDriverKind;
  readonly nativeThreadId: string;
  readonly status: "stopped";
  readonly source: "persisted_runtime_row";
}

/**
 * The owning reader supplies proof of the historical store or exact native
 * thread's accessibility to the target. Current settings and raw cursors do
 * not produce this evidence, and accessibility alone does not prove stopped.
 */
export interface ProviderContinuationAccessibility {
  readonly providerInstanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly nativeThreadId: string;
  readonly continuationKey: string;
  readonly source: "historical_store" | "native_read";
}

export type ProviderContinuationQualification =
  | { readonly type: "qualified"; readonly nativeThreadId: string; readonly continuationKey: string }
  | { readonly type: "unknown" | "unsupported"; readonly reason: string };

const nonempty = (value: string | null | undefined): value is string =>
  typeof value === "string" && value.trim().length > 0;

export function qualifyProviderContinuation(input: {
  readonly source: ProviderContinuationSource;
  readonly target: ProviderContinuationTarget;
  readonly accessibility?: ProviderContinuationAccessibility;
  readonly stoppedProof?: LegacyStoppedRuntimeProofV1;
}): ProviderContinuationQualification {
  const { source, target, accessibility, stoppedProof } = input;
  const legacy = source.provenance === "legacy_row" || source.provenance === "native_import";
  if (legacy) {
    if (source.status !== "stopped") return { type: "unknown", reason: "source_not_stopped" };
    if (!stoppedProof) return { type: "unknown", reason: "stopped_proof_missing" };
    if (
      stoppedProof.schema !== "t3.legacy-stopped-runtime-proof/v1" ||
      stoppedProof.source !== "persisted_runtime_row" ||
      stoppedProof.status !== "stopped" ||
      stoppedProof.threadId !== source.threadId ||
      stoppedProof.providerInstanceId !== source.providerInstanceId ||
      stoppedProof.driver !== source.driver ||
      stoppedProof.nativeThreadId !== source.nativeThreadId
    ) {
      return { type: "unknown", reason: "stopped_proof_mismatch" };
    }
  }
  if (!nonempty(source.nativeThreadId)) return { type: "unknown", reason: "native_reference_missing" };
  if (!nonempty(source.driver)) return { type: "unknown", reason: "source_driver_unproved" };
  if (!nonempty(target.continuationKey) || !nonempty(target.driver)) {
    return { type: "unknown", reason: "target_identity_missing" };
  }
  if (!legacy && !nonempty(source.continuationKey)) {
    return { type: "unknown", reason: "historical_identity_unproved" };
  }
  if (source.driver !== target.driver) return { type: "unsupported", reason: "driver_incompatible" };
  if (!target.supportsNativeResume) return { type: "unsupported", reason: "native_resume_unsupported" };
  if (nonempty(source.continuationKey) && source.continuationKey !== target.continuationKey) {
    return { type: "unsupported", reason: "store_incompatible" };
  }
  if (legacy) {
    if (!accessibility) return { type: "unknown", reason: "accessibility_missing" };
    if (
      (accessibility.source !== "historical_store" && accessibility.source !== "native_read") ||
      accessibility.providerInstanceId !== target.providerInstanceId ||
      accessibility.driver !== source.driver ||
      accessibility.driver !== target.driver ||
      accessibility.nativeThreadId !== source.nativeThreadId ||
      accessibility.continuationKey !== target.continuationKey
    ) {
      return { type: "unknown", reason: "accessibility_mismatch" };
    }
  }
  return {
    type: "qualified",
    nativeThreadId: source.nativeThreadId,
    continuationKey: target.continuationKey,
  };
}
