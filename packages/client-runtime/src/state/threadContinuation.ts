import {
  ORCHESTRATION_V2_WS_METHODS,
  type CommandId,
  type OrchestrationV2ImportedHistoryReviewBasis,
  type OrchestrationV2ImportedHistoryReviewResult,
  type OrchestrationV2ImportedHistoryStartReceipt,
  type OrchestrationV2ThreadRuntimeAttachmentResult,
  type OrchestrationV2CurrentThreadRuntimeTarget,
  type OrchestrationV2StopCurrentThreadRuntimeInput,
  type OrchestrationV2StopCurrentThreadRuntimeResult,
  type OrchestrationV2ThreadRuntimeObservation,
  type OrchestrationV2ThreadRuntimeObservationResult,
  type OrchestrationV2OperatingCountsResult,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ThreadShell,
  type ScopedThreadRef,
  type EnvironmentId,
  type ThreadId,
} from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import { createEnvironmentRpcQueryAtomFamily } from "./runtime.ts";
import type { EnvironmentThreadShell } from "./models.ts";

export type ImportedContinuationTarget = OrchestrationV2ImportedHistoryReviewResult["target"];

export type ImportedContinuationReviewState =
  | {
      readonly status: "available";
      readonly reviewedBasis: OrchestrationV2ImportedHistoryReviewBasis;
      readonly reason: null;
    }
  | { readonly status: "ordinary" | "native"; readonly reason: null }
  | { readonly status: "held" | "unknown" | "unavailable"; readonly reason: string };

function sameTarget(a: ImportedContinuationTarget, b: ImportedContinuationTarget): boolean {
  return (
    a.type === b.type &&
    a.messageId === b.messageId &&
    (a.type !== "queued_run" || (b.type === "queued_run" && a.runId === b.runId))
  );
}

export function resolveImportedContinuationReview(
  review: OrchestrationV2ImportedHistoryReviewResult | null | undefined,
  expected: { readonly threadId: ThreadId; readonly target: ImportedContinuationTarget },
): ImportedContinuationReviewState {
  if (review == null) {
    return { status: "unavailable", reason: "Continuation review is unavailable." };
  }
  if (review.threadId !== expected.threadId || !sameTarget(review.target, expected.target)) {
    return { status: "unavailable", reason: "Continuation review belongs to another target." };
  }
  if (review.nativeEffects.type === "unknown") {
    return { status: "unknown", reason: review.nativeEffects.reason };
  }
  if (review.applicability === "not_imported") {
    return { status: "ordinary", reason: null };
  }
  if (review.applicability === "unknown") {
    return { status: "unknown", reason: "Imported conversation applicability is unknown." };
  }
  if (review.qualification.type === "qualified") {
    return review.restoredBinding.type === "ready"
      ? { status: "native", reason: null }
      : { status: "held", reason: review.restoredBinding.reason };
  }
  if (!review.capability.startWithImportedHistory) {
    return { status: "unavailable", reason: "This server cannot start with imported history." };
  }
  if (review.transcriptEligibility.type !== "eligible") {
    return {
      status: review.transcriptEligibility.type === "unknown" ? "unknown" : "unavailable",
      reason: review.transcriptEligibility.reason,
    };
  }
  if (review.reviewedBasis === null) {
    return { status: "unavailable", reason: "A current continuation review is required." };
  }
  return { status: "available", reviewedBasis: review.reviewedBasis, reason: null };
}

export type ImportedContinuationReceiptState = {
  readonly status: "pending" | "started" | "rejected" | "unknown";
  readonly intentAccepted: boolean;
  readonly reason: string | null;
};

export function resolveImportedContinuationReceipt(
  receipt: OrchestrationV2ImportedHistoryStartReceipt | null | undefined,
  expected: {
    readonly threadId: ThreadId;
    readonly commandId: CommandId;
    readonly target: ImportedContinuationTarget;
    readonly reviewedBasis: OrchestrationV2ImportedHistoryReviewBasis;
  },
): ImportedContinuationReceiptState {
  const unknown = (reason: string, intentAccepted = false): ImportedContinuationReceiptState => ({
    status: "unknown",
    intentAccepted,
    reason,
  });
  if (receipt == null)
    return unknown("The start response is unavailable; observe the same command.");
  if (
    receipt.threadId !== expected.threadId ||
    receipt.commandId !== expected.commandId ||
    receipt.target === null ||
    !sameTarget(receipt.target, expected.target) ||
    receipt.reviewedBasis !== expected.reviewedBasis
  ) {
    return unknown("The start response belongs to another reviewed command.");
  }
  const observation = receipt.receipt;
  if (
    observation !== null &&
    (observation.threadId !== expected.threadId ||
      observation.commandId !== expected.commandId ||
      observation.commandType !== "thread.imported-history.start")
  ) {
    return unknown("The command observation belongs to another start request.");
  }
  const intentAccepted = receipt.intentStatus === "accepted" && observation?.status === "accepted";
  if (receipt.execution.status === "unknown" || receipt.execution.effectOutcome === "unknown") {
    return unknown(
      receipt.execution.error ?? "Native execution needs reconciliation.",
      intentAccepted,
    );
  }
  if (
    receipt.intentStatus === "rejected" &&
    receipt.execution.status === "not_started" &&
    receipt.execution.effectOutcome !== "confirmed_success" &&
    observation?.status !== "accepted"
  ) {
    return {
      status: "rejected",
      intentAccepted: false,
      reason: receipt.rejectionReason ?? observation?.error ?? "The reviewed start was rejected.",
    };
  }
  if (!intentAccepted) return unknown("Start admission has not been confirmed.");
  if (receipt.execution.status === "started") {
    const execution = receipt.execution;
    if (
      execution.effectOutcome !== "confirmed_success" ||
      execution.runId === null ||
      execution.providerThreadId === null ||
      execution.providerSessionId === null ||
      execution.nativeThreadId === null ||
      (expected.target.type === "queued_run" && execution.runId !== expected.target.runId)
    ) {
      return unknown("The new conversation has not been correlated to this request.", true);
    }
    return { status: "started", intentAccepted: true, reason: null };
  }
  return { status: "pending", intentAccepted: true, reason: receipt.execution.error };
}

export function resolveCurrentThreadRuntimeAttachment(
  result: OrchestrationV2ThreadRuntimeAttachmentResult | null | undefined,
  threadId: ThreadId,
): OrchestrationV2ThreadRuntimeAttachmentResult["attachment"] | null {
  return result?.threadId === threadId ? result.attachment : null;
}

export function createThreadContinuationAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
  options: {
    readonly threadRefreshAtom?: (ref: ScopedThreadRef) => Atom.Atom<unknown>;
    readonly snapshotAtom?: (
      environmentId: EnvironmentId,
    ) => Atom.Atom<OrchestrationV2ShellSnapshot | null>;
  } = {},
) {
  return {
    runtimeAttachment: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:thread:runtime-attachment",
      tag: ORCHESTRATION_V2_WS_METHODS.getThreadRuntimeAttachment,
      staleTimeMs: 0,
    }),
    importedHistoryStartReceipt: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:thread:imported-history-start-receipt",
      tag: ORCHESTRATION_V2_WS_METHODS.observeImportedHistoryStart,
      staleTimeMs: 0,
    }),
    currentThreadRuntimeStop: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:thread:current-runtime-stop",
      tag: ORCHESTRATION_V2_WS_METHODS.observeCurrentThreadRuntimeStop,
      staleTimeMs: 0,
    }),
    runtimeObservation: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:thread:runtime-observation",
      tag: ORCHESTRATION_V2_WS_METHODS.getThreadRuntimeObservation,
      staleTimeMs: 0,
      refreshTrigger: ({ environmentId, input }) =>
        options.threadRefreshAtom?.({ environmentId, threadId: input.threadId }),
    }),
    operatingCounts: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:operating-counts",
      tag: ORCHESTRATION_V2_WS_METHODS.getOperatingCounts,
      staleTimeMs: 0,
      refreshTrigger: ({ environmentId }) => options.snapshotAtom?.(environmentId),
    }),
  };
}

export function captureCurrentThreadRuntimeStopTarget(
  result: OrchestrationV2ThreadRuntimeAttachmentResult | null | undefined,
  threadId: ThreadId,
): OrchestrationV2CurrentThreadRuntimeTarget | null {
  const attachment = resolveCurrentThreadRuntimeAttachment(result, threadId);
  if (attachment?.status !== "attached" || attachment.binding.threadId !== threadId) return null;
  return {
    binding: { ...attachment.binding },
    driver: attachment.driver,
    evidenceRevision: attachment.evidenceRevision,
  };
}

function sameRuntimeTarget(
  a: OrchestrationV2CurrentThreadRuntimeTarget,
  b: OrchestrationV2CurrentThreadRuntimeTarget,
): boolean {
  return (
    a.driver === b.driver &&
    a.evidenceRevision === b.evidenceRevision &&
    a.binding.threadId === b.binding.threadId &&
    a.binding.providerThreadId === b.binding.providerThreadId &&
    a.binding.providerSessionId === b.binding.providerSessionId &&
    a.binding.instanceId === b.binding.instanceId &&
    a.binding.runtimeGeneration === b.binding.runtimeGeneration &&
    a.binding.nativeThreadId === b.binding.nativeThreadId
  );
}

export type CurrentThreadRuntimeStopState = {
  readonly status: "pending" | "stopped" | "rejected" | "unknown";
  readonly commandAccepted: boolean;
  readonly queueFenceInstalled: boolean;
  readonly reason: string | null;
};

export function resolveCurrentThreadRuntimeStop(
  result: OrchestrationV2StopCurrentThreadRuntimeResult | null | undefined,
  expected: OrchestrationV2StopCurrentThreadRuntimeInput,
): CurrentThreadRuntimeStopState {
  const unknown = (
    reason: string,
    commandAccepted = false,
    queueFenceInstalled = false,
  ): CurrentThreadRuntimeStopState => ({
    status: "unknown",
    commandAccepted,
    queueFenceInstalled,
    reason,
  });
  if (result == null) return unknown("The stop response is unavailable; observe the same command.");
  if (
    result.threadId !== expected.threadId ||
    result.commandId !== expected.commandId ||
    expected.target.binding.threadId !== expected.threadId ||
    result.target === null ||
    !sameRuntimeTarget(result.target, expected.target)
  ) {
    return unknown("The stop response does not match the captured current runtime.");
  }
  const receipt = result.receipt;
  if (
    receipt !== null &&
    (receipt.commandId !== expected.commandId ||
      receipt.threadId !== expected.threadId ||
      receipt.commandType !== "provider-session.detach")
  ) {
    return unknown("The stop receipt belongs to another command.");
  }
  const commandAccepted = result.commandStatus === "accepted" && receipt?.status === "accepted";
  const queueFenceInstalled = result.queueFence.status === "installed";
  if (result.queueFence.status === "unknown" || result.runtimeStop.status === "unknown") {
    return unknown(
      result.reason ?? "Runtime stop needs reconciliation.",
      commandAccepted,
      queueFenceInstalled,
    );
  }
  if (
    result.commandStatus === "rejected" &&
    result.queueFence.status === "not_installed" &&
    result.runtimeStop.status === "not_started" &&
    receipt?.status !== "accepted"
  ) {
    return {
      status: "rejected",
      commandAccepted: false,
      queueFenceInstalled: false,
      reason: result.reason ?? receipt?.error ?? "The captured runtime stop was rejected.",
    };
  }
  if (!commandAccepted)
    return unknown("Runtime stop admission has not been confirmed.", false, queueFenceInstalled);
  if (result.runtimeStop.status === "stopped") {
    return queueFenceInstalled
      ? {
          status: "stopped",
          commandAccepted: true,
          queueFenceInstalled: true,
          reason: result.reason,
        }
      : unknown("Runtime stop has no confirmed queue fence.", true);
  }
  return { status: "pending", commandAccepted: true, queueFenceInstalled, reason: result.reason };
}

export type ThreadRuntimeObservationOwner = Pick<
  OrchestrationV2ThreadShell,
  "id" | "activeProviderThreadId"
>;

export function resolveThreadRuntimeObservation<E>(
  query: AsyncResult.AsyncResult<OrchestrationV2ThreadRuntimeObservationResult, E>,
  thread: ThreadRuntimeObservationOwner,
): OrchestrationV2ThreadRuntimeObservation {
  if (query._tag !== "Success") {
    return { status: "unknown", reason: "Current runtime observation is unavailable." };
  }
  if (query.waiting) {
    return { status: "unknown", reason: "Current runtime observation is refreshing." };
  }
  const result = query.value;
  const binding = result.observation.binding;
  if (
    result.threadId !== thread.id ||
    (binding !== undefined &&
      (binding.threadId !== thread.id ||
        binding.providerThreadId !== thread.activeProviderThreadId))
  ) {
    return { status: "unknown", reason: "runtime_binding_changed" };
  }
  return result.observation;
}

type ThreadOperatingSource =
  | Pick<
      OrchestrationV2ThreadShell,
      | "id"
      | "activeProviderThreadId"
      | "status"
      | "pendingRuntimeRequest"
      | "hasActionableProposedPlan"
      | "archivedAt"
    >
  | Pick<
      EnvironmentThreadShell,
      | "id"
      | "activeProviderThreadId"
      | "runtime"
      | "hasPendingApprovals"
      | "hasPendingUserInput"
      | "hasActionableProposedPlan"
      | "archivedAt"
    >;

export function resolveThreadOperatingState(
  thread: ThreadOperatingSource,
  observation: OrchestrationV2ThreadRuntimeObservation,
) {
  const request = "pendingRuntimeRequest" in thread ? thread.pendingRuntimeRequest : null;
  const hasPendingApprovals =
    "hasPendingApprovals" in thread
      ? thread.hasPendingApprovals
      : request !== null && request.kind !== "user_input" && request.kind !== "auth_refresh";
  const hasPendingInput =
    "hasPendingUserInput" in thread ? thread.hasPendingUserInput : request?.kind === "user_input";
  const foregroundAttention = hasPendingApprovals
    ? ("approval" as const)
    : hasPendingInput
      ? ("input" as const)
      : thread.hasActionableProposedPlan
        ? ("plan" as const)
        : null;
  const foregroundStatus = "status" in thread ? thread.status : thread.runtime?.status;
  const foregroundWorking = foregroundStatus === "starting" || foregroundStatus === "running";
  const backgroundStatus =
    observation.status !== "unknown" &&
    (observation.binding.threadId !== thread.id ||
      observation.binding.providerThreadId !== thread.activeProviderThreadId)
      ? ("unknown" as const)
      : observation.status;
  const backgroundDisplay =
    backgroundStatus === "working" || backgroundStatus === "monitoring" ? backgroundStatus : null;
  const visible = thread.archivedAt === null;
  return {
    foregroundAttention,
    backgroundStatus,
    backgroundDisplay,
    operating: visible && (foregroundWorking || backgroundDisplay !== null),
    workstreamRunning: visible && (foregroundWorking || backgroundStatus === "working"),
  };
}

export type OperatingCountsState =
  | { readonly status: "available"; readonly counts: OrchestrationV2OperatingCountsResult }
  | { readonly status: "unavailable" | "stale"; readonly counts: null };

export function resolveOperatingCounts<E>(
  query: AsyncResult.AsyncResult<OrchestrationV2OperatingCountsResult, E>,
  minimumSnapshotSequence = 0,
): OperatingCountsState {
  if (query._tag !== "Success") return { status: "unavailable", counts: null };
  if (query.waiting || query.value.snapshotSequence < minimumSnapshotSequence) {
    return { status: "stale", counts: null };
  }
  return { status: "available", counts: query.value };
}
