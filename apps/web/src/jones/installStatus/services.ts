import type { DesktopBridge, EnvironmentId } from "@t3tools/contracts";
import type { JonesAction, JonesOperationCommand, JonesStopCommand } from "@t3tools/client-runtime/jones/updates";
import type { JonesExecutorHost } from "@t3tools/client-runtime/jones/updates/executor";
import type { FleetHostRequest } from "@t3tools/client-runtime/jones/fleet-updates";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import type { JonesOperationReadResult, JonesOperationReceipt, JonesRecoveryPreflight } from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type { InstallStatusDevice, InstallStatusStore } from "./panelUi";
import { installStatusStore } from "./panelUi";

export interface InstallPlanServices {
  readonly bridge: Pick<DesktopBridge, "installPlan" | "onInstallPlan" | "getUpdateState" | "onUpdateState">;
  readonly devices: () => readonly InstallStatusDevice[];
  readonly observe: (environmentId: EnvironmentId) => Promise<InstallStatusDevice>;
  readonly legacy: (environmentId: EnvironmentId, request: JonesAction) => Promise<JonesUpdateState | null>;
  readonly operation: (environmentId: EnvironmentId, request: JonesOperationCommand) => Promise<JonesOperationReceipt | JonesOperationReadResult | JonesRecoveryPreflight>;
  readonly stop: (environmentId: EnvironmentId, request: JonesStopCommand) => Promise<JonesOperationReceipt>;
  readonly host: (environmentId: EnvironmentId, request: FleetHostRequest) => Promise<FleetHostStatus>;
  readonly confirm: JonesExecutorHost["confirm"];
  readonly chooseRecovery: NonNullable<JonesExecutorHost["chooseRecovery"]>;
  readonly now: () => number;
}
let active: { services: InstallPlanServices; store: InstallStatusStore } | undefined;
export function bindInstallPlanServices(services: InstallPlanServices, store = installStatusStore) {
  const binding = { services, store };
  active = binding;
  return () => { if (active === binding) active = undefined; };
}
export function installPlanServices() { return active; }

export async function requestInstallPlan(request: Parameters<NonNullable<DesktopBridge["installPlan"]>>[0],
  binding = active) {
  if (binding?.services.bridge.installPlan === undefined) throw new Error("Install status is unavailable in this desktop build.");
  const result = await binding.services.bridge.installPlan(request);
  if (active?.services === binding.services && active.store === binding.store)
    binding.store.update({ native: result.state, ...(request.type === "read" ? {} : { error: result.refused?.message ?? null }) });
  return result;
}
