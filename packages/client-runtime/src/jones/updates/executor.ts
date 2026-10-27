// @effect-diagnostics globalDate:off - Promise entrypoints use injected clocks for reviews and native lease admission; Date only parses/formats those clocks.
import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlanGrant, JonesInstallPlanReview } from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationAction, JonesOperationReceipt, JonesOperationStartInput,
  JonesRecoveryPolicy, JonesRecoveryPreflight, JonesRecoveryPreflightInput,
} from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import {
  jonesInstallRefusal, jonesUpdatePresentation, resolveJonesExactSourceAction,
  type JonesAction, type JonesExactSourceInput, type JonesUpdateObservation,
} from "./jonesUpdates.ts";
import { validateReceipt } from "../installPlan/model.ts";

export type JonesDownloadResolution =
  | { readonly kind: "ready"; readonly action: JonesOperationAction }
  | { readonly kind: "refused"; readonly code: string; readonly message: string };

/** No replacement or installation is implicit in Download. Matching legacy offers still work. */
export function resolveDownloadAction(input: JonesExactSourceInput): JonesDownloadResolution {
  const operation = input.observation.state?.operation;
  if (operation !== undefined && (operation.code === "launcher-unconfirmed" ||
    ["accepted", "running", "stop-requested", "adopting", "handed-off", "held"].includes(operation.phase)))
    return { kind: "refused", code: "outstanding-operation", message: "An operation is still outstanding. Read its exact receipt before another download." };
  const decision = resolveJonesExactSourceAction({ ...input, allowInstall: false, allowReplace: false });
  if (decision.status !== "ready") return { kind: "refused", code: decision.reason, message: decision.message };
  if (decision.step.kind === "download-exact") return { kind: "ready", action: decision.step };
  if (decision.step.kind === "download") return { kind: "ready", action: { kind: "download", ...decision.step.input } };
  return { kind: "refused", code: "unsupported", message: "Review the existing downloaded build separately." };
}

export interface JonesExecutorSnapshot {
  readonly observation: JonesUpdateObservation;
  readonly canOperate: boolean;
}

function sameAction(left: JonesOperationAction, right: JonesOperationAction): boolean {
  if (left.kind === "download" && right.kind === "download") return left.artifactId === right.artifactId && left.sourceSha === right.sourceSha;
  if (left.kind === "download-exact" && right.kind === "download-exact") return left.targetSource === right.targetSource;
  return left.kind === "install" && right.kind === "install" && left.targetSource === right.targetSource &&
    left.stagedHandle === right.stagedHandle && left.currentVersion === right.currentVersion &&
    left.continueRunningThreads === right.continueRunningThreads && left.recovery?.policy === right.recovery?.policy &&
    left.recovery?.preflightId === right.recovery?.preflightId;
}
export type JonesPlanGateResult =
  | { readonly kind: "grant"; readonly grant: JonesInstallPlanGrant }
  | { readonly kind: "handled"; readonly message: string }
  | { readonly kind: "refused"; readonly message: string };

export interface JonesExecutorHost {
  readonly readLatest: () => JonesExecutorSnapshot;
  readonly legacy: (request: JonesAction) => Promise<JonesUpdateState | null>;
  readonly start: (input: JonesOperationStartInput) => Promise<JonesOperationReceipt>;
  readonly confirm: (message: string) => Promise<boolean>;
  readonly preflight?: (input: JonesRecoveryPreflightInput) => Promise<JonesRecoveryPreflight>;
  readonly chooseRecovery?: (preflight: JonesRecoveryPreflight) => Promise<JonesRecoveryPolicy | null>;
  /** Desktop supplies this native handoff. A renderer assertion cannot open the gate. */
  readonly planGate?: (environmentId: EnvironmentId, targetSource: string) => Promise<JonesPlanGateResult>;
}

export type JonesReviewedInstallResult =
  | { readonly kind: "ready"; readonly review: JonesInstallPlanReview; readonly action: Extract<JonesOperationAction, { kind: "install" }> }
  | { readonly kind: "refused"; readonly code: string; readonly message: string };

/** Review is a proposal, revalidated again immediately before dispatch. */
export async function prepareReviewedInstall(input: {
  readonly environmentId: EnvironmentId; readonly targetSource: string; readonly label: string;
  readonly host: JonesExecutorHost; readonly now: () => number; readonly continueRunningThreads?: boolean;
}): Promise<JonesReviewedInstallResult> {
  const refuse = (code: string, message: string): JonesReviewedInstallResult => ({ kind: "refused", code, message });
  const initial = input.host.readLatest();
  const state = initial.observation.state;
  if (initial.observation.freshness !== "fresh" || !initial.canOperate || state?.environmentId !== input.environmentId ||
    state.stagedHandle === undefined || state.currentVersion === undefined || state.installedSource === undefined ||
    state.provenance?.sourceSha !== input.targetSource || !state.capability.install)
    return refuse("changed", "Fresh host identity and a matching downloaded source are required.");
  const clicked = { environmentId: input.environmentId, stagedHandle: state.stagedHandle, currentVersion: state.currentVersion };
  if (!(await input.host.confirm(`Install source ${input.targetSource.slice(0, 7)} on ${input.label}? The host restarts and disconnects briefly.`)))
    return refuse("cancelled", "Nothing was sent.");
  let recovery: JonesInstallPlanReview["recovery"];
  const afterConfirm = input.host.readLatest();
  if (!afterConfirm.canOperate || jonesInstallRefusal(clicked, afterConfirm.observation) !== undefined ||
    afterConfirm.observation.state?.installedSource?.sha !== state.installedSource.sha ||
    afterConfirm.observation.state.provenance?.sourceSha !== input.targetSource)
    return refuse("changed", "The host or downloaded build changed during review.");
  if (afterConfirm.observation.state.capability.recoveryPolicy === 1) {
    if (input.host.preflight === undefined || input.host.chooseRecovery === undefined)
      return refuse("unsupported", "This client cannot review the host's recovery policy.");
    const binding: JonesRecoveryPreflightInput = { ...clicked, expectedInstalledSource: state.installedSource.sha };
    const measured = await input.host.preflight(binding);
    if (measured.protocol !== 1 || measured.binding.environmentId !== binding.environmentId ||
      measured.binding.stagedHandle !== binding.stagedHandle || measured.binding.currentVersion !== binding.currentVersion ||
      measured.binding.expectedInstalledSource !== binding.expectedInstalledSource ||
      !Number.isFinite(Date.parse(measured.expiresAt)) || Date.parse(measured.expiresAt) <= input.now())
      return refuse("stale-preflight", "Recovery preflight changed or expired. Review again.");
    const policy = await input.host.chooseRecovery(measured);
    if (policy === null) return refuse("cancelled", "Nothing was sent.");
    if ((policy === "clone-required" ? measured.clone.status !== "proved" || !measured.policies.cloneRequired.admissible :
      !measured.policies.cloneOrCopy.admissible) || Date.parse(measured.expiresAt) <= input.now())
      return refuse("recovery-refused", "The selected recovery policy is unavailable or expired.");
    recovery = { policy, preflightId: measured.preflightId };
  }
  const latest = input.host.readLatest();
  if (!latest.canOperate || jonesInstallRefusal(clicked, latest.observation) !== undefined ||
    latest.observation.state?.installedSource?.sha !== state.installedSource.sha ||
    latest.observation.state.provenance?.sourceSha !== input.targetSource ||
    (recovery !== undefined && latest.observation.state.capability.recoveryPolicy !== 1))
    return refuse("changed", "The host changed during review. Nothing was sent.");
  const review: JonesInstallPlanReview = {
    stagedHandle: clicked.stagedHandle, currentVersion: clicked.currentVersion,
    reviewedAt: new Date(input.now()).toISOString(), targetSource: input.targetSource,
    expectedInstalledSource: state.installedSource.sha,
    ...(input.continueRunningThreads === undefined ? {} : { continueRunningThreads: input.continueRunningThreads }),
    ...(recovery === undefined ? {} : { recovery }),
  };
  return { kind: "ready", review, action: {
    kind: "install", stagedHandle: review.stagedHandle, currentVersion: review.currentVersion, targetSource: review.targetSource,
    ...(review.continueRunningThreads === undefined ? {} : { continueRunningThreads: review.continueRunningThreads }),
    ...(review.recovery === undefined ? {} : { recovery: review.recovery }),
  } };
}

export type JonesExecutionResult = {
  readonly kind: "accepted" | "refused" | "unknown";
  readonly receipt?: JonesOperationReceipt; readonly state?: JonesUpdateState;
  readonly code?: string; readonly message: string;
};

export async function executeJonesAction(input: {
  readonly environmentId: EnvironmentId; readonly targetSource: string; readonly action: JonesOperationAction;
  readonly operationId: string; readonly host: JonesExecutorHost; readonly review?: JonesInstallPlanReview;
  /** Retained by the entrypoint until receipt reconciliation, independent of revision changes. */
  readonly unresolved?: boolean;
  readonly cancelled?: () => boolean;
  readonly now?: () => number;
}): Promise<JonesExecutionResult> {
  const refused = (code: string, message: string): JonesExecutionResult => ({ kind: "refused", code, message });
  if (input.unresolved) return { kind: "unknown", code: "unresolved", message: "The previous request is not confirmed. Read its receipt before any new action." };
  if (input.cancelled?.()) return refused("cancelled", "Nothing was sent.");
  const target = input.action.kind === "download" ? input.action.sourceSha : input.action.targetSource;
  if (target !== input.targetSource) return refused("binding", "The action does not match the reviewed target.");
  const validate = (): JonesExecutionResult | undefined => {
    const latest = input.host.readLatest();
    const state = latest.observation.state;
    if (!latest.canOperate || latest.observation.freshness !== "fresh" || state?.environmentId !== input.environmentId)
      return refused("stale", "Fresh authorized host status is required.");
    if (state.operation !== undefined && (state.operation.code === "launcher-unconfirmed" ||
      ["accepted", "running", "stop-requested", "adopting", "handed-off", "held"].includes(state.operation.phase)))
      return { kind: "unknown", code: "outstanding-operation", receipt: state.operation, message: "The host still has an outstanding operation. Read its exact outcome before another action." };
    if (input.action.kind === "install") {
      const review = input.review;
      if (review === undefined || review.targetSource !== input.targetSource || review.stagedHandle !== input.action.stagedHandle ||
        review.currentVersion !== input.action.currentVersion || state.installedSource?.sha !== review.expectedInstalledSource ||
        state.provenance?.sourceSha !== input.targetSource ||
        review.recovery?.policy !== input.action.recovery?.policy || review.recovery?.preflightId !== input.action.recovery?.preflightId ||
        review.continueRunningThreads !== input.action.continueRunningThreads ||
        (input.action.recovery !== undefined && state.capability.recoveryPolicy !== 1))
        return refused("review-required", "The downloaded build or recovery review changed.");
      const reason = jonesInstallRefusal({ environmentId: input.environmentId, stagedHandle: review.stagedHandle, currentVersion: review.currentVersion }, latest.observation);
      if (reason !== undefined) return refused("changed", reason);
    } else {
      const resolved = resolveDownloadAction({ environmentId: input.environmentId, targetSource: input.targetSource, ...latest });
      if (resolved.kind !== "ready") return refused(resolved.code, resolved.message);
      if (resolved.action.kind !== input.action.kind ||
        (input.action.kind === "download" && (resolved.action.kind !== "download" || resolved.action.artifactId !== input.action.artifactId)))
        return refused("changed", "The offered artifact changed. Review again.");
    }
    return undefined;
  };
  const problem = validate();
  if (problem !== undefined) return problem;
  let grant: JonesInstallPlanGrant | undefined;
  if (input.host.planGate !== undefined) {
    let gated: JonesPlanGateResult;
    try {
      gated = await input.host.planGate(input.environmentId, input.targetSource);
    } catch {
      return { kind: "unknown", code: "plan-handoff-unknown", message: "The native plan handoff was not confirmed. Read the plan before any new action." };
    }
    if (gated.kind === "handled") return { kind: "accepted", message: gated.message };
    if (gated.kind === "refused") return refused("plan-gate", gated.message);
    grant = gated.grant;
    if (grant.environmentId !== input.environmentId || grant.request.kind !== "operation-start" ||
      grant.request.input.environmentId !== input.environmentId || grant.request.input.operationId !== grant.operationId ||
      !sameAction(grant.request.input.action, input.action) ||
      !Number.isFinite(Date.parse(grant.leaseUntil)) || Date.parse(grant.leaseUntil) <= (input.now ?? Date.now)())
      return refused("plan-gate", "The native grant does not match this action.");
  }
  if (input.cancelled?.()) return refused("cancelled", "Nothing was sent.");
  const changed = validate();
  if (changed !== undefined) return changed;
  const state = input.host.readLatest().observation.state!;
  let dispatched = false;
  try {
    if (state.capability.operations === 1) {
      if (state.installedSource === undefined) return refused("binding", "The installed source is unavailable.");
      const request: JonesOperationStartInput = grant?.request.kind === "operation-start" ? grant.request.input : {
        operationId: input.operationId, environmentId: input.environmentId,
        expectedInstalledSource: state.installedSource.sha, action: input.action,
      };
      if (request.expectedInstalledSource !== state.installedSource.sha) return refused("changed", "The host changed after the native grant.");
      dispatched = true;
      const receipt = await input.host.start(request);
      if (!validateReceipt(request, receipt)) return { kind: "unknown", code: "invalid-receipt", message: "The returned receipt could not be bound to this request. Read the exact operation before continuing." };
      return { kind: receipt.phase === "refused" ? "refused" : "accepted", receipt,
        ...(receipt.code === undefined ? {} : { code: receipt.code }), message: receipt.message ?? `Operation ${receipt.operationId}: ${receipt.phase}.` };
    }
    if (grant !== undefined) return refused("unsupported", "The host no longer supports durable operations.");
    const action = input.action;
    const request: JonesAction = action.kind === "download-exact" ? { action: "downloadExact", input: { targetSource: action.targetSource } } :
      action.kind === "download" ? { action: "download", input: { artifactId: action.artifactId, sourceSha: action.sourceSha } } :
        { action: "install", input: { environmentId: input.environmentId, stagedHandle: action.stagedHandle, currentVersion: action.currentVersion,
          ...(action.continueRunningThreads === undefined ? {} : { continueRunningThreads: action.continueRunningThreads }) } };
    if (action.kind === "install" && action.recovery !== undefined) return refused("unsupported", "This host cannot bind recovery policy to an install.");
    dispatched = true;
    const result = await input.host.legacy(request);
    if (result === null) return { kind: "unknown", message: "The host returned no result. Check status; nothing is resent automatically." };
    if (action.kind !== "install" && (["downloading", "verifying", "staged"].includes(result.phase) ? result.provenance?.sourceSha !== input.targetSource : true))
      return refused("not-accepted", result.message ?? "The host did not confirm the selected download.");
    if (["error", "blocked", "rolled-back"].includes(result.phase)) return { kind: "refused", state: result, message: jonesUpdatePresentation(result).message };
    return { kind: "accepted", state: result, message: "Request accepted. Install always requires a separate review." };
  } catch {
    return dispatched ? { kind: "unknown", message: "The request may have been accepted. Read its exact receipt or host status; nothing is resent automatically." } :
      refused("not-sent", "Nothing was sent.");
  }
}
