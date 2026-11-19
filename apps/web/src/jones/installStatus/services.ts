import type { DesktopBridge, EnvironmentId } from "@t3tools/contracts";
import type {
  JonesAction,
  JonesOperationCommand,
  JonesStopCommand,
} from "@t3tools/client-runtime/jones/updates";
import type { JonesExecutorHost } from "@t3tools/client-runtime/jones/updates/executor";
import type { FleetHostRequest } from "@t3tools/client-runtime/jones/fleet-updates";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import type {
  JonesOperationReadResult,
  JonesOperationReceipt,
  JonesAbandonStageResult,
  JonesRecoverCleanupResult,
  JonesRecoveryPreflight,
} from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type {
  JonesInstallPlan,
  JonesInstallPlanDevice,
  JonesInstallPlanHostOperation,
  JonesInstallPlanResult,
  JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import type { InstallStatusDevice, InstallStatusStore } from "./panelUi";
import { archivedPlanFrom, installStatusStore, isOlderNativeState, statusTurn } from "./panelUi";

export interface InstallPlanServices {
  readonly bridge: Pick<
    DesktopBridge,
    "installPlan" | "onInstallPlan" | "getUpdateState" | "onUpdateState"
  > &
    Partial<Pick<DesktopBridge, "checkForUpdate">>;
  readonly devices: () => readonly InstallStatusDevice[];
  readonly observe: (environmentId: EnvironmentId) => Promise<InstallStatusDevice>;
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
  if (
    device?.environmentId === environmentId &&
    device.canOperate &&
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
    left?.capability.abandonStage === right?.capability.abandonStage
  );
}

export function hasPlanEnrollment(device: JonesInstallPlanDevice, observed: InstallStatusDevice) {
  const { fleetStatus } = observed;
  return (
    device.environmentId === observed.environmentId &&
    (device.route === "operations"
      ? fleetStatus == null ||
        (fleetStatus.environmentId === device.environmentId &&
          fleetStatus.enrollment?.enabled !== true)
      : fleetStatus?.environmentId === device.environmentId &&
        fleetStatus.enrollment?.enabled === true &&
        fleetStatus.enrollment.environmentId === device.environmentId &&
        fleetStatus.enrollment.enrollmentId === device.enrollmentId)
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
    (current.fleetStatus === undefined || hasPlanEnrollment(device, current))
  );
}

export function exactOperationRequest(
  planId: string,
  environmentId: EnvironmentId,
  operation: JonesInstallPlanHostOperation,
) {
  const input = operation.request.kind === "operation-start" ? operation.request.input : undefined;
  return input?.environmentId === environmentId &&
    input.operationId === operation.operationId &&
    input.correlation?.planId === planId &&
    input.correlation.deviceKey === environmentId
    ? input
    : undefined;
}

export interface InstallPlanBinding {
  readonly services: InstallPlanServices;
  readonly store: InstallStatusStore;
}
let active: InstallPlanBinding | undefined;
const bindingListeners = new Set<() => void>();

export function subscribeInstallPlanServices(listener: () => void) {
  bindingListeners.add(listener);
  return () => bindingListeners.delete(listener);
}

export function bindInstallPlanServices(services: InstallPlanServices, store = installStatusStore) {
  const binding = { services, store };
  active = binding;
  store.bindDesk(services.bridge.installPlan);
  for (const listener of bindingListeners) listener();
  return () => {
    if (active !== binding) return;
    active = undefined;
    for (const listener of bindingListeners) listener();
  };
}
export function installPlanServices() {
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
  binding = active,
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
