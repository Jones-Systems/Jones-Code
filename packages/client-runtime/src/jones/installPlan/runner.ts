// @effect-diagnostics globalDate:off - Promise driver supplies scheduling time; Date only parses native lease timestamps.
import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlanGrant,
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationReadResult,
  JonesOperationReceipt,
} from "@t3tools/contracts/jones/jonesOperations";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import type { JonesOperationCommand, JonesUpdateObservation } from "../updates/jonesUpdates.ts";
import type { FleetHostRequest } from "../fleetUpdates/hostBridge.ts";
import {
  canReplayDirective,
  nextRunnerStep,
  validateDirectiveEvidence,
  validateReceipt,
} from "./model.ts";

export interface InstallPlanDriver {
  readonly claimant: string;
  readonly now: () => number;
  readonly cancelled: () => boolean;
  readonly desktop: (request: JonesInstallPlanRequest) => Promise<JonesInstallPlanResult>;
  /** A fresh read, including a fresh fleet status for enrolled hosts. It never mutates. */
  readonly observe: (environmentId: EnvironmentId) => Promise<{
    readonly observation: JonesUpdateObservation;
    readonly canOperate: boolean;
    readonly fleetStatus?: FleetHostStatus | null;
  }>;
  /** Rechecks fresh host capability and session authority before claims and dispatch. */
  readonly canDispatch: (environmentId: EnvironmentId) => boolean;
  readonly operation: (
    environmentId: EnvironmentId,
    request: Exclude<JonesOperationCommand, { action: "recoveryPreflight" | "recoverCleanup" }>,
  ) => Promise<JonesOperationReceipt | JonesOperationReadResult>;
  readonly host: (
    environmentId: EnvironmentId,
    request: FleetHostRequest,
  ) => Promise<FleetHostStatus>;
}

/** One pass, bounded by the persisted 16-device limit. The caller owns wakeups and lifetime. */
export async function advanceInstallPlan(driver: InstallPlanDriver): Promise<void> {
  const initial = await driver.desktop({ type: "read" });
  const plan = initial.state.current;
  if (
    driver.cancelled() ||
    initial.refused !== undefined ||
    initial.state.readOnly !== undefined ||
    plan === null
  )
    return;
  for (const device of plan.devices.slice(0, 16)) {
    if (driver.cancelled()) return;
    try {
      const { observation, canOperate, fleetStatus } = await driver.observe(device.environmentId);
      if (driver.cancelled()) return;
      const canMutate = () =>
        !driver.cancelled() && canOperate && driver.canDispatch(device.environmentId);
      const step = nextRunnerStep(plan, device, observation, fleetStatus, driver.now());
      const report = async (operationId: string, receipt: JonesOperationReceipt) => {
        // Work already sent can settle after this window stops scheduling.
        await driver.desktop({
          type: "report",
          planId: plan.planId,
          environmentId: device.environmentId,
          operationId,
          evidence: { kind: "receipt", receipt },
        });
      };
      if (step.kind === "wait") continue;
      if (
        step.kind === "read" ||
        (step.kind === "reservation" &&
          step.operation.step === "download" &&
          step.operation.lease.claimant === driver.claimant &&
          initial.state.view.recoveryHistory !== 1)
      ) {
        // Older native coordinators can turn a lost download reclaim into a new lease.
        // Read the original receipt until it is observed or the reservation can be fenced.
        const { operationId, request, receipt: prior } = step.operation;
        if (
          request.kind !== "operation-start" ||
          request.input.environmentId !== device.environmentId ||
          request.input.operationId !== operationId
        )
          continue;
        const result = await driver.operation(device.environmentId, {
          action: "operationRead",
          input: {
            environmentId: device.environmentId,
            operationIds: [operationId],
          },
        });
        if (!("receipts" in result)) continue;
        const receipt = result.receipts.find((r) => r.operationId === operationId)?.receipt;
        if (
          receipt != null &&
          validateReceipt(request.input, receipt) &&
          (prior === undefined || validateReceipt(prior, receipt))
        )
          await report(operationId, receipt);
        // Null is absence, never proof of no effects or permission to send again.
        continue;
      }
      if (step.kind === "report-directive") {
        await driver.desktop({
          type: "report",
          planId: plan.planId,
          environmentId: device.environmentId,
          operationId: step.operationId,
          evidence: { kind: "directive", status: step.status },
        });
        continue;
      }
      const reservation = step.kind === "reservation" ? step.operation : undefined;
      const expired =
        reservation !== undefined && Date.parse(reservation.lease.leaseUntil) <= driver.now();
      const directive =
        reservation?.request.kind === "plan-directive" ? reservation.request.input : undefined;
      if (directive !== undefined && !canReplayDirective(directive, fleetStatus)) continue;
      // Reclaim through native main even for this window's own reservation: it rechecks the
      // gate and current membership, and returns the exact original request for idempotency.
      if (
        reservation !== undefined &&
        (expired
          ? directive === undefined
          : reservation.lease.claimant !== driver.claimant || reservation.step === "install")
      )
        continue;
      const claim: JonesInstallPlanRequest = {
        type: "claim",
        planId: plan.planId,
        environmentId: device.environmentId,
        claimant: driver.claimant,
        explicit: false,
        ...(step.kind === "claim-fence"
          ? { step: "fence", operationId: step.operationId }
          : step.kind === "claim-directive"
            ? {
                step: "directive",
                enrollmentId: step.enrollmentId,
                expectedDirectiveId: step.expectedDirectiveId,
              }
            : step.kind === "claim-download"
              ? {
                  step: "download",
                  action: step.action,
                  expectedInstalledSource: step.expectedInstalledSource,
                }
              : step.operation.request.kind === "plan-directive"
                ? {
                    step: "directive",
                    operationId: step.operation.operationId,
                    enrollmentId: step.operation.request.input.enrollmentId,
                    expectedDirectiveId: step.operation.request.input.expectedDirectiveId,
                  }
                : {
                    step: "download",
                    operationId: step.operation.operationId,
                    action: step.operation.request.input.action,
                    expectedInstalledSource: step.operation.request.input.expectedInstalledSource,
                  }),
      };
      if (!canMutate()) continue;
      const claimed = await driver.desktop(claim);
      const grant = claimed.grant;
      if (
        !canMutate() ||
        claimed.refused !== undefined ||
        claimed.state.readOnly !== undefined ||
        (claim.step === "download" &&
          claim.operationId !== undefined &&
          claimed.state.view.recoveryHistory !== 1) ||
        grant === undefined ||
        grant.planId !== plan.planId ||
        grant.environmentId !== device.environmentId ||
        (claim.operationId !== undefined && grant.operationId !== claim.operationId) ||
        grant.step !== claim.step ||
        !Number.isFinite(Date.parse(grant.leaseUntil)) ||
        Date.parse(grant.leaseUntil) <= driver.now()
      )
        continue;
      await dispatchGrant(driver, grant, report, directive !== undefined);
    } catch {
      // A later pass reconciles only the exact pending operation, under a native lease.
      // A disconnected host does not prevent other selected hosts from making progress.
    }
  }
}

async function dispatchGrant(
  driver: InstallPlanDriver,
  grant: JonesInstallPlanGrant,
  report: (operationId: string, receipt: JonesOperationReceipt) => Promise<void>,
  replayingDirective: boolean,
): Promise<void> {
  const request = grant.request;
  if (
    request.input.environmentId !== grant.environmentId ||
    (request.kind === "plan-directive" ? request.input.directiveId : request.input.operationId) !==
      grant.operationId
  )
    return;
  if (request.kind === "plan-directive") {
    if (grant.step !== "directive") return;
    try {
      const status = await driver.host(grant.environmentId, {
        action: "planDirective",
        input: request.input,
      });
      if (validateDirectiveEvidence(request.input, status))
        await driver.desktop({
          type: "report",
          planId: grant.planId,
          environmentId: grant.environmentId,
          operationId: grant.operationId,
          evidence: { kind: "directive", status },
        });
    } catch (error) {
      // A storage error can follow a durable write. A replay refusal can also race the original
      // dispatch, so only settle an initial request on a definitive refusal.
      if (
        !driver.cancelled() &&
        !replayingDirective &&
        typeof error === "object" &&
        error !== null &&
        "_tag" in error &&
        error._tag === "FleetHostError" &&
        "reason" in error &&
        "message" in error &&
        typeof error.reason === "string" &&
        typeof error.message === "string" &&
        ["binding", "not-enrolled", "bootstrap-required", "conflict"].includes(error.reason)
      )
        await driver.desktop({
          type: "report",
          planId: grant.planId,
          environmentId: grant.environmentId,
          operationId: grant.operationId,
          evidence: {
            kind: "directive-refused",
            reason: error.reason as "binding" | "not-enrolled" | "bootstrap-required" | "conflict",
            message: error.message.slice(0, 1024),
          },
        });
    }
    return;
  }
  if (
    request.kind === "operation-stop" ||
    (request.kind === "operation-start" &&
      (grant.step !== "download" || request.input.action.kind === "install")) ||
    (request.kind === "operation-fence" && grant.step !== "fence")
  )
    return;
  const receipt = await driver.operation(
    grant.environmentId,
    request.kind === "operation-start"
      ? { action: "operationStart", input: request.input }
      : { action: "operationFence", input: request.input },
  );
  if (
    !("receipts" in receipt) &&
    receipt.operationId === grant.operationId &&
    receipt.environmentId === grant.environmentId &&
    validateReceipt(request.input, receipt)
  )
    await report(grant.operationId, receipt);
}
