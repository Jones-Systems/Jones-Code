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
import type { InstallStatusDevice, InstallStatusStore } from "./panelUi";
import { installStatusStore } from "./panelUi";

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

export function canOperateInstallDevice(
  services: InstallPlanServices,
  environmentId: EnvironmentId,
) {
  return services.devices().some((d) => d.environmentId === environmentId && d.canOperate);
}

let active: { services: InstallPlanServices; store: InstallStatusStore } | undefined;
export function bindInstallPlanServices(services: InstallPlanServices, store = installStatusStore) {
  const binding = { services, store };
  active = binding;
  return () => {
    if (active === binding) active = undefined;
  };
}
export function installPlanServices() {
  return active;
}

export async function requestInstallPlan(
  request: Parameters<NonNullable<DesktopBridge["installPlan"]>>[0],
  binding = active,
) {
  if (binding?.services.bridge.installPlan === undefined)
    throw new Error("Install status is unavailable in this desktop build.");
  const result = await binding.services.bridge.installPlan(request);
  if (active?.services === binding.services && active.store === binding.store)
    binding.store.update({
      native: result.state,
      ...(request.type === "read" || (request.type === "report" && result.refused === undefined)
        ? {}
        : { error: result.refused?.message ?? null }),
    });
  return result;
}
