// @effect-diagnostics globalDate:off - Promise driver supplies scheduling time; Date only parses native lease timestamps.
import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlanGrant, JonesInstallPlanRequest, JonesInstallPlanResult,
} from "@t3tools/contracts/jones/installPlan";
import type { JonesOperationReadResult, JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import type { JonesOperationCommand, JonesUpdateObservation } from "../updates/jonesUpdates.ts";
import type { FleetHostRequest } from "../fleetUpdates/hostBridge.ts";
import { nextRunnerStep, validateDirectiveEvidence, validateReceipt } from "./model.ts";

export interface InstallPlanDriver {
  readonly claimant: string;
  readonly now: () => number;
  readonly cancelled: () => boolean;
  readonly desktop: (request: JonesInstallPlanRequest) => Promise<JonesInstallPlanResult>;
  /** A fresh read, including a fresh fleet status for enrolled hosts. It never mutates. */
  readonly observe: (environmentId: EnvironmentId) => Promise<{
    readonly observation: JonesUpdateObservation; readonly fleetStatus?: FleetHostStatus | null;
  }>;
  readonly operation: (environmentId: EnvironmentId, request: Exclude<JonesOperationCommand, { action: "recoveryPreflight" }>) => Promise<JonesOperationReceipt | JonesOperationReadResult>;
  readonly host: (environmentId: EnvironmentId, request: FleetHostRequest) => Promise<FleetHostStatus>;
}

/** One pass, bounded by the persisted 16-device limit. The caller owns wakeups and lifetime. */
export async function advanceInstallPlan(driver: InstallPlanDriver): Promise<void> {
  const initial = await driver.desktop({ type: "read" });
  const plan = initial.state.current;
  if (driver.cancelled() || initial.state.readOnly !== undefined || plan === null) return;
  for (const device of plan.devices.slice(0, 16)) {
    if (driver.cancelled()) return;
    try {
      const { observation, fleetStatus } = await driver.observe(device.environmentId);
      if (driver.cancelled()) return;
      const step = nextRunnerStep(plan, device, observation, fleetStatus, driver.now());
      const report = async (operationId: string, receipt: JonesOperationReceipt) => {
        if (!driver.cancelled()) await driver.desktop({ type: "report", planId: plan.planId,
          environmentId: device.environmentId, operationId, evidence: { kind: "receipt", receipt } });
      };
      if (step.kind === "wait") continue;
      if (step.kind === "read") {
        const result = await driver.operation(device.environmentId, { action: "operationRead", input: {
          environmentId: device.environmentId, operationIds: [step.operation.operationId],
        } });
        if (!("receipts" in result)) continue;
        const receipt = result.receipts.find((r) => r.operationId === step.operation.operationId)?.receipt;
        if (receipt !== undefined && receipt !== null && step.operation.request.kind === "operation-start" &&
          validateReceipt(step.operation.request.input, receipt) &&
          (step.operation.receipt === undefined || validateReceipt(step.operation.receipt, receipt)))
          await report(step.operation.operationId, receipt);
        // Null is absence, never proof of no effects or permission to send again.
        continue;
      }
      if (step.kind === "reservation" && step.operation.step === "directive" &&
        Date.parse(step.operation.lease.leaseUntil) <= driver.now()) {
        if (step.operation.request.kind === "plan-directive" && fleetStatus !== undefined && fleetStatus !== null &&
          validateDirectiveEvidence(step.operation.request.input, fleetStatus))
          await driver.desktop({ type: "report", planId: plan.planId, environmentId: device.environmentId,
            operationId: step.operation.operationId, evidence: { kind: "directive", status: fleetStatus } });
        continue;
      }
      // Reclaim through native main even for this window's own reservation: it rechecks the
      // gate and current membership, and returns the exact original request for idempotency.
      if (step.kind === "reservation" && (step.operation.lease.claimant !== driver.claimant ||
        Date.parse(step.operation.lease.leaseUntil) <= driver.now() || step.operation.step === "install")) continue;
      const claim: JonesInstallPlanRequest = {
        type: "claim", planId: plan.planId, environmentId: device.environmentId,
        claimant: driver.claimant, explicit: false,
        ...(step.kind === "claim-fence" ? { step: "fence", operationId: step.operationId } :
          step.kind === "claim-directive" ? { step: "directive", enrollmentId: step.enrollmentId, expectedDirectiveId: step.expectedDirectiveId } :
          step.kind === "claim-download" ? { step: "download", action: step.action, expectedInstalledSource: step.expectedInstalledSource } :
          step.operation.request.kind === "plan-directive" ? {
            step: "directive", enrollmentId: step.operation.request.input.enrollmentId,
            expectedDirectiveId: step.operation.request.input.expectedDirectiveId,
          } : { step: "download", action: step.operation.request.input.action,
            expectedInstalledSource: step.operation.request.input.expectedInstalledSource }),
      };
      const claimed = await driver.desktop(claim);
      const grant = claimed.grant;
      if (driver.cancelled() || claimed.refused !== undefined || grant === undefined ||
        grant.planId !== plan.planId || grant.environmentId !== device.environmentId ||
        grant.step === "install" || grant.step === "stop" || !Number.isFinite(Date.parse(grant.leaseUntil)) ||
        Date.parse(grant.leaseUntil) <= driver.now()) continue;
      await dispatchGrant(driver, grant, report);
    } catch {
      // No mutation retry here. A later pass reads the reserved ID, or fences after expiry.
      // A disconnected host does not prevent other selected hosts from making progress.
    }
  }
}

async function dispatchGrant(driver: InstallPlanDriver, grant: JonesInstallPlanGrant,
  report: (operationId: string, receipt: JonesOperationReceipt) => Promise<void>): Promise<void> {
  const request = grant.request;
  if (request.input.environmentId !== grant.environmentId ||
    (request.kind === "plan-directive" ? request.input.directiveId : request.input.operationId) !== grant.operationId) return;
  if (request.kind === "plan-directive") {
    try {
      const status = await driver.host(grant.environmentId, { action: "planDirective", input: request.input });
      if (!driver.cancelled() && validateDirectiveEvidence(request.input, status))
        await driver.desktop({ type: "report", planId: grant.planId, environmentId: grant.environmentId,
          operationId: grant.operationId, evidence: { kind: "directive", status } });
    } catch (error) {
      // Only a typed target refusal can end the directive. Transport/auth failures stay unknown.
      if (!driver.cancelled() && typeof error === "object" && error !== null && "_tag" in error &&
        error._tag === "FleetHostError" && "reason" in error && "message" in error &&
        typeof error.reason === "string" && typeof error.message === "string" &&
        ["binding", "not-enrolled", "bootstrap-required", "conflict", "storage"].includes(error.reason))
        await driver.desktop({ type: "report", planId: grant.planId, environmentId: grant.environmentId,
          operationId: grant.operationId, evidence: { kind: "directive-refused",
            reason: error.reason as "binding" | "not-enrolled" | "bootstrap-required" | "conflict" | "storage", message: error.message.slice(0, 1024) } });
    }
    return;
  }
  if (request.kind === "operation-stop" || (request.kind === "operation-start" && request.input.action.kind === "install")) return;
  const receipt = await driver.operation(grant.environmentId, request.kind === "operation-start" ?
    { action: "operationStart", input: request.input } : { action: "operationFence", input: request.input });
  if (!("receipts" in receipt) && receipt.operationId === grant.operationId && receipt.environmentId === grant.environmentId &&
    (request.kind !== "operation-start" || validateReceipt(request.input, receipt))) await report(grant.operationId, receipt);
}
