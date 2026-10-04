import { ProviderRequestKind, type OrchestrationV2ProviderSession, type OrchestrationV2ThreadShell, type ProviderDriverKind } from "@t3tools/contracts";
import type { ProviderRuntimeBinding, ProviderRuntimeObservation } from "./ProviderAdapter.ts";
import * as Schema from "effect/Schema";

export class ProviderOperatingCountsError extends Schema.TaggedError<ProviderOperatingCountsError>()(
  "ProviderOperatingCountsError", { cause: Schema.optional(Schema.Defect()) },
) {}

export type ProviderThreadRuntimeAttachment =
  | { readonly status: "attached"; readonly binding: ProviderRuntimeBinding; readonly driver: ProviderDriverKind;
      readonly runtimeStatus: OrchestrationV2ProviderSession["status"]; readonly evidenceRevision: number; readonly observedAt: string }
  | { readonly status: "stopped"; readonly reason: "runtime_not_resident"; readonly observedAt: string }
  | { readonly status: "unknown"; readonly reason: string; readonly observedAt: string };

export type ProviderThreadForegroundActivity = "working" | "waiting_approval" | "waiting_input" | "waiting_plan" | null;
export interface ProviderThreadActivityObservation {
  readonly foreground: ProviderThreadForegroundActivity;
  readonly background: "working" | "monitoring" | null;
  readonly backgroundStatus: "known" | "unknown";
  readonly reason?: string;
}
export type ProviderThreadActivitySource = Pick<OrchestrationV2ThreadShell,
  "id" | "activeProviderThreadId" | "archivedAt" | "deletedAt" | "activityRunStatus" | "interactionMode" |
  "hasActionableProposedPlan" | "latestRunCompletedAt"> & {
    readonly pendingRuntimeRequest: Pick<NonNullable<OrchestrationV2ThreadShell["pendingRuntimeRequest"]>, "kind"> | null;
  };
const isApprovalRequestKind = Schema.is(ProviderRequestKind);

export function providerThreadActivityObservation(
  thread: ProviderThreadActivitySource,
  native: ProviderRuntimeObservation,
  sampledAtMs: number,
): ProviderThreadActivityObservation {
  if (thread.archivedAt !== null || thread.deletedAt !== null)
    return { foreground: null, background: null, backgroundStatus: "known" };
  const active = thread.activityRunStatus === "preparing" || thread.activityRunStatus === "starting" || thread.activityRunStatus === "running";
  const foreground: ProviderThreadForegroundActivity = isApprovalRequestKind(thread.pendingRuntimeRequest?.kind)
    ? "waiting_approval" : thread.pendingRuntimeRequest?.kind === "user_input"
      ? "waiting_input" : active ? "working"
        : thread.interactionMode === "plan" && thread.hasActionableProposedPlan && thread.latestRunCompletedAt != null
          ? "waiting_plan" : null;
  if (native.status === "unknown")
    return { foreground: foreground === "working" ? null : foreground, background: null, backgroundStatus: native.reason === "runtime_not_resident" ? "known" : "unknown", reason: native.reason };
  if (!Number.isFinite(Date.parse(native.observedAt)) || Date.parse(native.observedAt) < sampledAtMs)
    return { foreground: foreground === "working" ? null : foreground, background: null, backgroundStatus: "unknown", reason: "native_activity_stale" };
  if (native.binding.threadId !== thread.id || native.binding.providerThreadId !== thread.activeProviderThreadId)
    return { foreground: foreground === "working" ? null : foreground, background: null, backgroundStatus: "unknown", reason: "runtime_binding_changed" };
  if (native.status === "busy")
    return { foreground, background: null, backgroundStatus: "unknown", reason: "native_background_coverage_incomplete" };
  return {
    foreground: foreground === "working" && native.status !== "working" ? null : foreground,
    background: native.status === "monitoring" || (foreground === null && native.status === "working") ? native.status : null,
    backgroundStatus: "known",
  };
}

export interface ProviderOperatingCounts {
  readonly total: number;
  readonly operating: number;
  readonly foregroundWaitingApproval: number;
  readonly foregroundWaitingInput: number;
  readonly foregroundWaitingPlan: number;
  readonly backgroundOperating: number;
  readonly backgroundUnknown: number;
  readonly snapshotSequence: number;
  readonly observedAt: string;
  readonly backgroundSampledAt: string;
}
