import { msg } from "gt-react";
import { m } from "../../i18n/messages";
import type { DesktopBridge, EnvironmentId } from "@t3tools/contracts";
import type {
  JonesAction,
  JonesOperationBeforeSend,
  JonesOperationCommand,
  JonesStopCommand,
} from "@t3tools/client-runtime/jones/updates";
import type { JonesExecutorHost } from "@t3tools/client-runtime/jones/updates/executor";
import type { FleetHostRequest } from "@t3tools/client-runtime/jones/fleet-updates";
import {
  allowsOperationRecovery,
  canFenceWithUnavailableFleet,
  deviceActiveOperations,
  exactOperationRequest,
  matchesForwardRoute,
  matchPlanEnrollment,
  sameOperationBinding,
  supportsInstallPlanMutations,
  supportsOperationReceiptReads,
  type PlanEnrollmentMatch,
} from "@t3tools/client-runtime/jones/install-plan";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import type {
  JonesOperationReadResult,
  JonesOperationReceipt,
  JonesOperationStartInput,
  JonesAbandonStageResult,
  JonesRecoverCleanupResult,
  JonesRecoveryPreflight,
} from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type {
  JonesInstallPlan,
  JonesInstallPlanDevice,
  JonesInstallPlanGrant,
  JonesInstallPlanRequest,
  JonesInstallPlanReview,
  JonesInstallPlanResult,
  JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import type { InstallStatusDevice, InstallStatusStore } from "./panelUi";
import { archivedPlanFrom, installStatusStore, isOlderNativeState, statusTurn } from "./panelUi";
import { operationAdmissionMessage } from "./copy";

export { exactOperationRequest } from "@t3tools/client-runtime/jones/install-plan";

export interface InstallPlanServices {
  readonly bridge: Pick<
    DesktopBridge,
    "installPlan" | "onInstallPlan" | "getUpdateState" | "onUpdateState"
  > &
    Partial<Pick<DesktopBridge, "checkForUpdate">>;
  readonly devices: () => readonly InstallStatusDevice[];
  readonly observe: (environmentId: EnvironmentId) => Promise<InstallStatusDevice>;
  /** Current read scope for receipt reads and late reporting over the authenticated transport. */
  readonly canReadReceipts?: (environmentId: EnvironmentId) => boolean;
  readonly readStatus?: (environmentId: EnvironmentId) => Promise<JonesUpdateState | null>;
  readonly legacy: (
    environmentId: EnvironmentId,
    request: JonesAction,
  ) => Promise<JonesUpdateState | null>;
  readonly operation: (
    environmentId: EnvironmentId,
    request: JonesOperationCommand,
  ) => Promise<
    | JonesOperationReceipt
    | JonesOperationReadResult
    | JonesRecoveryPreflight
    | JonesAbandonStageResult
    | JonesRecoverCleanupResult
  >;
  readonly stop: (
    environmentId: EnvironmentId,
    request: JonesStopCommand,
  ) => Promise<JonesOperationReceipt>;
  readonly host: (
    environmentId: EnvironmentId,
    request: FleetHostRequest,
  ) => Promise<FleetHostStatus>;
  readonly confirm: JonesExecutorHost["confirm"];
  readonly chooseRecovery: NonNullable<JonesExecutorHost["chooseRecovery"]>;
  readonly now: () => number;
  /** Ask a waiting update status stream to read now; it never interrupts a live long-poll. */
  readonly kick?: (environmentId: EnvironmentId) => void;
}

export function authorizedUpdateStatus(
  device: InstallStatusDevice | undefined,
  environmentId: EnvironmentId,
): JonesUpdateState | undefined {
  return device?.canOperate ? matchingUpdateStatus(device, environmentId) : undefined;
}

function matchingUpdateStatus(
  device: InstallStatusDevice | undefined,
  environmentId: EnvironmentId,
) {
  if (
    device?.environmentId === environmentId &&
    device.observation.freshness === "fresh" &&
    device.observation.state?.environmentId === environmentId
  )
    return device.observation.state;
}

export function currentAuthorizedUpdateStatus(
  services: InstallPlanServices,
  environmentId: EnvironmentId,
  observed?: InstallStatusDevice,
): JonesUpdateState | undefined {
  const current = services.devices().find((device) => device.environmentId === environmentId);
  if (current?.canOperate !== true) return undefined;
  if (
    current.observation.state?.environmentId !== undefined &&
    current.observation.state.environmentId !== environmentId
  )
    return undefined;
  if (observed?.directStatus) {
    const earlier = statusTurn(current.observation) < statusTurn(observed.observation);
    if (earlier) return authorizedUpdateStatus(observed, environmentId);
    if (
      statusTurn(current.observation) === statusTurn(observed.observation) &&
      !sameStatus(current.observation.state, observed.observation.state)
    )
      return undefined;
  }
  return authorizedUpdateStatus(current, environmentId);
}

function sameStatus(left: JonesUpdateState | null, right: JonesUpdateState | null) {
  return (
    left?.environmentId === right?.environmentId &&
    left?.revision === right?.revision &&
    left?.phase === right?.phase &&
    left?.currentVersion === right?.currentVersion &&
    left?.installedSource?.sha === right?.installedSource?.sha &&
    left?.installedSource?.tree === right?.installedSource?.tree &&
    left?.stagedHandle === right?.stagedHandle &&
    left?.operation?.operationId === right?.operation?.operationId &&
    left?.operation?.phase === right?.operation?.phase &&
    left?.capability.operations === right?.capability.operations &&
    left?.capability.operationReceipts === right?.capability.operationReceipts &&
    left?.capability.boundAdmission === right?.capability.boundAdmission &&
    left?.capability.abandonStage === right?.capability.abandonStage
  );
}

export function planEnrollmentFor(
  device: JonesInstallPlanDevice,
  observed: InstallStatusDevice | undefined,
): PlanEnrollmentMatch {
  if (observed === undefined) return "unknown";
  const { environmentId } = device;
  if (
    observed.environmentId !== environmentId ||
    (observed.observation.state?.environmentId !== undefined &&
      observed.observation.state.environmentId !== environmentId)
  )
    return "identity-changed";
  return matchPlanEnrollment(
    device,
    matchingUpdateStatus(observed, environmentId) ?? null,
    observed.fleetStatus,
  );
}

export function hasPlanEnrollment(device: JonesInstallPlanDevice, observed: InstallStatusDevice) {
  return (
    device.environmentId === observed.environmentId &&
    matchPlanEnrollment(device, observed.observation.state, observed.fleetStatus) === "match"
  );
}

export function hasCurrentPlanEnrollment(
  services: InstallPlanServices,
  device: JonesInstallPlanDevice,
  observed: InstallStatusDevice,
) {
  const current = services.devices().find((item) => item.environmentId === device.environmentId);
  return (
    current !== undefined &&
    hasPlanEnrollment(device, observed) &&
    hasPlanEnrollment(device, current)
  );
}

export function exactFleetEnrollment(
  environmentId: EnvironmentId,
  status: FleetHostStatus | null | undefined,
) {
  return status?.environmentId === environmentId &&
    status.enrollment?.environmentId === environmentId &&
    (status.update?.environmentId === undefined || status.update.environmentId === environmentId)
    ? status.enrollment
    : undefined;
}

export function supportsSetupFence(
  environmentId: EnvironmentId,
  enrollmentId: string | undefined,
  status: FleetHostStatus | null | undefined,
) {
  const enrollment = exactFleetEnrollment(environmentId, status);
  return (
    enrollmentId !== undefined &&
    enrollment !== undefined &&
    (enrollment.enabled && enrollment.enrollmentId === enrollmentId
      ? status?.fencePlanDirective === 1
      : status?.fenceHistoricalPlanDirective === 1)
  );
}

export interface InstallPlanBinding {
  readonly services: InstallPlanServices;
  readonly store: InstallStatusStore;
}

export function hasOperationReadScope(services: InstallPlanServices, environmentId: EnvironmentId) {
  return services.canReadReceipts?.(environmentId) ?? true;
}

interface PlanHostObservation {
  readonly sequence: number;
  readonly direct: InstallStatusDevice | undefined;
  readonly knownFleet: FleetHostStatus | null | undefined;
}

interface ActiveInstallPlanBinding extends InstallPlanBinding {
  readonly latestObservation: (environmentId: EnvironmentId) => PlanHostObservation | undefined;
}

export function currentPlanHostAuthority(
  binding: InstallPlanBinding,
  device: JonesInstallPlanDevice,
) {
  const { services, store } = binding;
  const { environmentId } = device;
  const current = services.devices().find((item) => item.environmentId === environmentId);
  const status = matchingUpdateStatus(current, environmentId);
  const latest = active === binding ? active.latestObservation(environmentId) : undefined;
  const direct = latest?.direct;
  const directStatus = matchingUpdateStatus(direct, environmentId);
  const projectedEnrollment = planEnrollmentFor(device, current);
  const directEnrollment = planEnrollmentFor(device, direct);
  const retainedEnrollment = matchPlanEnrollment(device, status ?? null, latest?.knownFleet);
  // Missing fleet status removes forward proof without clearing a retained contradiction.
  const enrollment = [projectedEnrollment, directEnrollment, retainedEnrollment].includes(
    "identity-changed",
  )
    ? "identity-changed"
    : directEnrollment === "route-changed"
      ? "route-changed"
      : projectedEnrollment === "route-changed" ||
          retainedEnrollment === "route-changed" ||
          (current?.fleetStatus === null && projectedEnrollment !== "match")
        ? "unknown"
        : directEnrollment;
  const { native, nativeRefusal } = store.get();
  const writable = active === binding && native?.readOnly === undefined && nativeRefusal === null;
  const hasOperationScope =
    writable &&
    current?.canOperate === true &&
    direct?.canOperate !== false &&
    status !== undefined;
  const hasReadScope = hasOperationReadScope(services, environmentId);
  const canRecoverOperation =
    hasOperationScope &&
    status.capability.operations === 1 &&
    directStatus?.capability.operations === 1;
  const currentFleet = current?.fleetStatus;
  const directFleet = direct?.fleetStatus;
  return {
    status,
    canDispatch:
      hasOperationScope &&
      supportsInstallPlanMutations(status.capability) &&
      (directStatus === undefined || supportsInstallPlanMutations(directStatus.capability)),
    canRecoverOperation,
    canFenceSetup:
      canRecoverOperation &&
      direct?.canOperate === true &&
      device.route === "follower-operations" &&
      enrollment !== "identity-changed" &&
      supportsSetupFence(environmentId, device.enrollmentId, currentFleet) &&
      supportsSetupFence(environmentId, device.enrollmentId, directFleet) &&
      currentFleet?.enrollment?.enrollmentId === directFleet?.enrollment?.enrollmentId &&
      currentFleet?.enrollment?.enabled === directFleet?.enrollment?.enabled,
    canReadReceipt:
      active === binding &&
      hasReadScope &&
      status !== undefined &&
      supportsOperationReceiptReads(status.capability) &&
      allowsOperationRecovery(enrollment, "read"),
    canReportReceipt: hasReadScope,
    enrollment,
    allowUnavailableFence:
      enrollment === "unknown" &&
      projectedEnrollment !== "route-changed" &&
      retainedEnrollment !== "route-changed" &&
      canFenceWithUnavailableFleet(device, status ?? null, direct?.fleetStatus),
    ...(hasOperationScope &&
    status.capability.boundAdmission === 1 &&
    (directStatus === undefined || directStatus.capability.boundAdmission === 1)
      ? { boundAdmission: 1 as const }
      : {}),
    fleetStatus: direct?.fleetStatus,
    reviewTarget: status?.policy?.reviewTarget,
  } as const;
}

let active: ActiveInstallPlanBinding | undefined;
const bindingListeners = new Set<() => void>();

export function subscribeInstallPlanServices(listener: () => void) {
  bindingListeners.add(listener);
  return () => bindingListeners.delete(listener);
}

export function bindInstallPlanServices(source: InstallPlanServices, store = installStatusStore) {
  let sequence = 0;
  const latest = new Map<EnvironmentId, PlanHostObservation>();
  const services: InstallPlanServices = {
    ...source,
    async observe(environmentId) {
      const started = ++sequence;
      const remember = (device?: InstallStatusDevice) => {
        const previous = latest.get(environmentId);
        if (started > (previous?.sequence ?? 0))
          latest.set(environmentId, {
            sequence: started,
            direct: device,
            knownFleet: device?.fleetStatus ?? previous?.knownFleet,
          });
      };
      try {
        const device = await source.observe(environmentId);
        remember(device);
        return device;
      } catch (error) {
        remember();
        throw error;
      }
    },
  };
  const binding: ActiveInstallPlanBinding = {
    services,
    store,
    latestObservation: (environmentId) => latest.get(environmentId),
  };
  active = binding;
  store.bindDesk(services.bridge.installPlan);
  for (const listener of bindingListeners) listener();
  return () => {
    if (active !== binding) return;
    active = undefined;
    for (const listener of bindingListeners) listener();
  };
}
export function installPlanServices(): InstallPlanBinding | undefined {
  return active;
}

export function nativePlanIssue(result: JonesInstallPlanResult) {
  return result.refused?.message ?? result.state.readOnly?.message;
}

export function requireWritablePlan(binding: InstallPlanBinding, result?: JonesInstallPlanResult) {
  if (active !== binding) throw new Error("Install status connection changed.");
  const { native, nativeRefusal } = binding.store.get();
  const problem =
    (result === undefined ? undefined : nativePlanIssue(result)) ??
    native?.readOnly?.message ??
    nativeRefusal;
  if (problem !== null && problem !== undefined) throw new Error(problem);
  if (result !== undefined && isOlderNativeState(native, result.state))
    throw new Error("The update plan changed. Refresh Install status.");
}

export function revisionFor(state: JonesInstallPlanState, plan: JonesInstallPlan) {
  return state.view.recoveryHistory === 1 ? { expectedRevision: plan.revision } : {};
}

export function claimAdmission(
  state: JonesInstallPlanState,
  authority: ReturnType<typeof currentPlanHostAuthority>,
) {
  return state.view.boundAdmission === 1 && authority.boundAdmission === 1
    ? { boundAdmission: 1 as const }
    : {};
}

export function issuedDevice(
  plan: JonesInstallPlan | null | undefined,
  original: JonesInstallPlan,
  selected: JonesInstallPlanDevice,
  includeRemoved = false,
) {
  if (
    plan?.planId !== original.planId ||
    plan.target.repository !== original.target.repository ||
    plan.target.sourceSha !== original.target.sourceSha ||
    plan.target.sourceTree !== original.target.sourceTree
  )
    return undefined;
  const device = plan.devices.find((item) => item.environmentId === selected.environmentId);
  return device !== undefined &&
    (device.membership === "selected" ||
      (includeRemoved && device.membership === selected.membership)) &&
    device.route === selected.route &&
    device.enrollmentId === selected.enrollmentId &&
    device.abandoning !== true
    ? device
    : undefined;
}

export function planStartIssue(
  binding: InstallPlanBinding,
  plan: JonesInstallPlan,
  device: JonesInstallPlanDevice,
  grant: JonesInstallPlanGrant,
  claim: Pick<
    Extract<JonesInstallPlanRequest, { type: "claim" }>,
    "claimant" | "boundAdmission" | "review"
  >,
  input: JonesOperationStartInput,
) {
  const { native } = binding.store.get();
  const current = native?.current;
  const member = issuedDevice(current, plan, device);
  const operation = member === undefined ? undefined : deviceActiveOperations(member).at(-1);
  const original = operation && exactOperationRequest(plan.planId, device.environmentId, operation);
  if (
    current == null ||
    member === undefined ||
    operation === undefined ||
    grant.request.kind !== "operation-start" ||
    grant.planId !== plan.planId ||
    grant.environmentId !== device.environmentId ||
    grant.operationId !== input.operationId ||
    !sameOperationBinding(grant.request.input, input) ||
    original === undefined ||
    !sameOperationBinding(original, input) ||
    operation.step !== grant.step ||
    operation.lease.claimant !== claim.claimant ||
    operation.lease.leaseUntil !== grant.leaseUntil ||
    !(Date.parse(grant.leaseUntil) > binding.services.now()) ||
    operation.receipt !== undefined ||
    operation.fence !== undefined ||
    member.ended !== undefined ||
    member.verifiedElsewhere !== undefined ||
    current.cancelled === true ||
    current.dismissedAt !== undefined ||
    current.supersededAt !== undefined ||
    current.laptop.gate === undefined ||
    current.laptop.gateClosed !== undefined ||
    current.laptop.gatePause === true ||
    native?.laptop.activationBlocked !== false
  )
    return m(
      msg("This plan's exact reservation changed or expired. Check the original operation."),
    );
  const authority = currentPlanHostAuthority(binding, device);
  if (authority.enrollment === "route-changed")
    return operationAdmissionMessage("admission-route-changed");
  if (
    !authority.canDispatch ||
    !allowsOperationRecovery(authority.enrollment, "start") ||
    authority.status?.installedSource?.sha !== input.expectedInstalledSource
  )
    return m(msg("Fresh control authority and the selected device's enrollment are required."));
  const { admission, action } = input;
  const newlyReserved = !deviceActiveOperations(device).some(
    (item) => item.operationId === input.operationId,
  );
  if (
    (admission !== undefined && authority.boundAdmission !== 1) ||
    (newlyReserved && (claim.boundAdmission === 1) !== (admission !== undefined))
  )
    return operationAdmissionMessage("admission-binding-required");
  if (!matchesForwardRoute(current, member, input, authority.fleetStatus, authority.reviewTarget))
    return operationAdmissionMessage("admission-route-changed");
  if (
    action.kind === "install" &&
    (!sameReview(operation.review, claim.review) ||
      authority.status.stagedHandle !== action.stagedHandle ||
      authority.status.currentVersion !== action.currentVersion ||
      authority.status.provenance?.sourceSha !== plan.target.sourceSha ||
      authority.status.provenance.sourceTree !== plan.target.sourceTree)
  )
    return m(msg("The reviewed stage changed. Check the original operation and review again."));
  return undefined;
}

function sameReview(
  recorded: JonesInstallPlanReview | undefined,
  review: JonesInstallPlanReview | undefined,
) {
  return (
    recorded !== undefined &&
    review !== undefined &&
    recorded.reviewedAt === review.reviewedAt &&
    recorded.stagedHandle === review.stagedHandle &&
    recorded.targetSource === review.targetSource &&
    recorded.currentVersion === review.currentVersion &&
    recorded.expectedInstalledSource === review.expectedInstalledSource &&
    recorded.continueRunningThreads === review.continueRunningThreads &&
    recorded.recovery?.policy === review.recovery?.policy &&
    recorded.recovery?.preflightId === review.recovery?.preflightId
  );
}

export function guardPlanStart(
  binding: InstallPlanBinding,
  issue: () => string | undefined,
  beforeSend?: JonesOperationBeforeSend,
): JonesOperationBeforeSend {
  return async () => {
    if (issue() !== undefined) return false;
    const read = await requestInstallPlan({ type: "read" }, binding);
    requireWritablePlan(binding, read);
    if (beforeSend !== undefined && !(await beforeSend())) return false;
    return issue() === undefined;
  };
}

export type PlanTarget = {
  readonly planId: string;
  readonly archived?: boolean;
  readonly expectedRevision?: number;
};

export function exactPlan(result: JonesInstallPlanResult, planId: string, archived: boolean) {
  if (!archived) return result.state.current?.planId === planId ? result.state.current : null;
  if (result.historyPlan !== undefined) {
    const detail = result.historyPlan;
    const summary = result.state.recoveryHistory?.find((entry) => entry.planId === planId);
    return detail.planId === planId &&
      (detail.dismissedAt !== undefined || detail.supersededAt !== undefined) &&
      (summary === undefined || summary.revision === detail.revision)
      ? detail
      : null;
  }
  return archivedPlanFrom(result.state, planId);
}

export async function readTargetPlan(binding: InstallPlanBinding, target: PlanTarget) {
  const result = await requestInstallPlan(
    target.archived && binding.store.get().native?.view.recoveryHistory === 1
      ? { type: "read-history", planId: target.planId }
      : { type: "read" },
    binding,
  );
  return { result, plan: exactPlan(result, target.planId, target.archived === true) };
}

export async function readWritableTarget(
  binding: InstallPlanBinding,
  target: PlanTarget,
  changed = "This update plan or attempt changed. Refresh Install status.",
) {
  const { result, plan } = await readTargetPlan(binding, target);
  requireWritablePlan(binding, result);
  if (
    plan === null ||
    (target.expectedRevision !== undefined && plan.revision !== target.expectedRevision)
  )
    throw new Error(changed);
  return { state: result.state, plan };
}

export async function confirmClosedPlan(
  binding: InstallPlanBinding,
  planId: string,
  matches: (plan: JonesInstallPlan) => boolean,
  result?: JonesInstallPlanResult,
) {
  if (active !== binding) throw new Error("Install status connection changed.");
  const direct = result === undefined ? null : exactPlan(result, planId, true);
  if (direct !== null && matches(direct)) return direct;
  const read = await readTargetPlan(binding, { planId, archived: true });
  if (active !== binding) throw new Error("Install status connection changed.");
  if (read.plan !== null && matches(read.plan)) return read.plan;
  const issue =
    (result === undefined ? undefined : nativePlanIssue(result)) ?? nativePlanIssue(read.result);
  throw new Error(issue ?? "Closing this plan was not confirmed. Refresh Install status.");
}

export async function requestInstallPlan(
  request: Parameters<NonNullable<DesktopBridge["installPlan"]>>[0],
  binding: InstallPlanBinding | undefined = active,
) {
  if (binding?.services.bridge.installPlan === undefined)
    throw new Error("Install status is unavailable in this desktop build.");
  const selection = binding.store.historySelection();
  const wasActive = active;
  const result = await binding.services.bridge.installPlan(request);
  if (
    active === wasActive &&
    active?.services === binding.services &&
    active.store === binding.store
  ) {
    const isRead = request.type === "read" || request.type === "read-history";
    const matchesHistory =
      request.type === "read-history" &&
      selection === binding.store.historySelection() &&
      binding.store.get().historyPlanId === request.planId &&
      result.refused === undefined;
    const history = matchesHistory ? exactPlan(result, request.planId, true) : null;
    binding.store.update({
      native: result.state,
      ...(isRead && !(request.type === "read-history" && result.refused?.code === "plan-mismatch")
        ? { nativeRefusal: result.refused?.message ?? null }
        : {}),
      ...(history === null ? {} : { historyPlan: history }),
      ...(isRead || (request.type === "report" && result.refused === undefined)
        ? {}
        : { error: result.refused?.message ?? null }),
    });
  }
  return result;
}
