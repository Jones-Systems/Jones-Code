import {
  ProviderRequestKind,
  type OrchestrationV2ThreadShell,
  type ProviderRuntimeBinding,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export type ProviderRuntimeObservation =
  | {
      readonly status: "working" | "monitoring" | "busy" | "idle";
      readonly binding: ProviderRuntimeBinding;
      readonly observedAt: string;
      readonly backgroundCoverage?: "complete" | "partial";
    }
  | { readonly status: "unknown"; readonly reason: string };

export type ProviderThreadForegroundActivity =
  | "working"
  | "waiting_approval"
  | "waiting_input"
  | "waiting_plan"
  | null;

export interface ProviderThreadActivityObservation {
  readonly foreground: ProviderThreadForegroundActivity;
  readonly background: "working" | "monitoring" | null;
  readonly backgroundStatus: "known" | "unknown";
  readonly reason?: string;
}

export type ProviderThreadActivitySource = Pick<
  OrchestrationV2ThreadShell,
  | "id"
  | "activeProviderThreadId"
  | "archivedAt"
  | "deletedAt"
  | "activityRunStatus"
  | "interactionMode"
  | "hasActionableProposedPlan"
  | "latestRunCompletedAt"
> & {
  readonly pendingRuntimeRequest: Pick<
    NonNullable<OrchestrationV2ThreadShell["pendingRuntimeRequest"]>,
    "kind"
  > | null;
};

const isApprovalRequestKind = Schema.is(ProviderRequestKind);

export function providerThreadForegroundActivity(
  thread: ProviderThreadActivitySource,
): ProviderThreadForegroundActivity {
  if (thread.archivedAt !== null || thread.deletedAt !== null) return null;
  const active =
    thread.activityRunStatus === "preparing" ||
    thread.activityRunStatus === "starting" ||
    thread.activityRunStatus === "running";
  return isApprovalRequestKind(
    thread.pendingRuntimeRequest?.kind,
  )
    ? "waiting_approval"
    : thread.pendingRuntimeRequest?.kind === "user_input"
      ? "waiting_input"
      : active
        ? "working"
        : thread.interactionMode === "plan" &&
            thread.hasActionableProposedPlan &&
            thread.latestRunCompletedAt != null
          ? "waiting_plan"
          : null;
}

export function providerThreadActivityObservation(
  thread: ProviderThreadActivitySource,
  native: ProviderRuntimeObservation | null,
  sampledAtMs: number,
): ProviderThreadActivityObservation {
  if (thread.archivedAt !== null || thread.deletedAt !== null) {
    return { foreground: null, background: null, backgroundStatus: "known" };
  }
  const foreground = providerThreadForegroundActivity(thread);
  if (native === null || native.status === "unknown") {
    const reason = native?.reason ?? "native_activity_unavailable";
    return {
      foreground: foreground === "working" ? null : foreground,
      background: null,
      backgroundStatus: reason === "runtime_not_resident" ? "known" : "unknown",
      reason,
    };
  }
  const observedAtMs = Date.parse(native.observedAt);
  if (
    !Number.isFinite(sampledAtMs) ||
    !Number.isFinite(observedAtMs) ||
    observedAtMs < sampledAtMs
  ) {
    return {
      foreground: foreground === "working" ? null : foreground,
      background: null,
      backgroundStatus: "unknown",
      reason: "native_activity_stale",
    };
  }
  if (
    native.binding.threadId !== thread.id ||
    native.binding.providerThreadId !== thread.activeProviderThreadId
  ) {
    return {
      foreground: foreground === "working" ? null : foreground,
      background: null,
      backgroundStatus: "unknown",
      reason: "runtime_binding_changed",
    };
  }
  if (native.status === "busy") {
    return {
      foreground,
      background: null,
      backgroundStatus: "unknown",
      reason: "native_background_coverage_incomplete",
    };
  }
  return {
    foreground: foreground === "working" && native.status !== "working" ? null : foreground,
    background:
      native.status === "monitoring" || (foreground === null && native.status === "working")
        ? native.status
        : null,
    backgroundStatus: native.backgroundCoverage === "complete" ? "known" : "unknown",
    ...(native.backgroundCoverage !== "complete" ? { reason: "native_background_coverage_incomplete" } : {}),
  };
}
