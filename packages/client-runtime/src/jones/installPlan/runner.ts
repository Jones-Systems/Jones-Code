// @effect-diagnostics globalDate:off - Promise driver supplies scheduling time; Date only parses native lease timestamps.
import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlanDevice,
  JonesInstallPlanGrant,
  JonesInstallPlanHostOperation,
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
  JonesInstallPlanTarget,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationReadResult,
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import type { JonesOperationCommand, JonesUpdateObservation } from "../updates/jonesUpdates.ts";
import type { FleetHostRequest } from "../fleetUpdates/hostBridge.ts";
import {
  canReplayDirective,
  deviceActiveOperations,
  exactOperationRequest,
  matchPlanEnrollment,
  nextRunnerStep,
  sameDirectiveBinding,
  sameOperationBinding,
  supportsOperationReceiptReads,
  validateDirectiveEvidence,
  validateReceipt,
  type PlanEnrollmentMatch,
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
  /** Live authorization and the latest route evidence, independent of the pass's original read. */
  readonly authority: (device: JonesInstallPlanDevice) => {
    readonly canDispatch: boolean;
    readonly canReadReceipt: boolean;
    readonly enrollment: PlanEnrollmentMatch;
    readonly fleetStatus: FleetHostStatus | null | undefined;
  };
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
      const authority = () => driver.authority(device);
      const enrollment = matchPlanEnrollment(device, observation.state, fleetStatus);
      const step = nextRunnerStep(plan, device, observation, fleetStatus, driver.now());
      const report = async (operationId: string, receipt: JonesOperationReceipt) => {
        // Work already sent can settle after this window stops scheduling.
        if (!authority().canReadReceipt) return;
        await driver.desktop({
          type: "report",
          planId: plan.planId,
          environmentId: device.environmentId,
          operationId,
          evidence: { kind: "receipt", receipt },
        });
      };
      if (step.kind === "wait") continue;
      const readReceipt = async (
        input: JonesOperationStartInput,
        prior?: JonesOperationReceipt,
      ) => {
        const status = observation.state;
        if (
          driver.cancelled() ||
          status?.environmentId !== device.environmentId ||
          !supportsOperationReceiptReads(status.capability) ||
          !authority().canReadReceipt
        )
          return;
        const { operationId } = input;
        const result = await driver.operation(device.environmentId, {
          action: "operationRead",
          input: { environmentId: device.environmentId, operationIds: [operationId] },
        });
        if (!("receipts" in result)) return;
        const receipt = result.receipts.find((r) => r.operationId === operationId)?.receipt;
        if (
          receipt != null &&
          validateReceipt(input, receipt) &&
          (prior === undefined || validateReceipt(prior, receipt))
        )
          await report(operationId, receipt);
        // Null is absence, never proof of no effects or permission to send again.
      };
      const originalRequest = (operation: JonesInstallPlanHostOperation) =>
        exactOperationRequest(plan.planId, device.environmentId, operation);
      const readReservation = async (operation: JonesInstallPlanHostOperation) => {
        const input = originalRequest(operation);
        if (input !== undefined) await readReceipt(input, operation.receipt);
      };
      if (
        step.kind === "read" ||
        (step.kind === "reservation" &&
          step.operation.step === "download" &&
          step.operation.lease.claimant === driver.claimant &&
          initial.state.view.recoveryHistory !== 1)
      ) {
        // Older native coordinators can turn a lost download reclaim into a new lease.
        // Read the original receipt until it is observed or the reservation can be fenced.
        await readReservation(step.operation);
        continue;
      }
      if (step.kind === "report-directive") {
        if (!authority().canReadReceipt) continue;
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
      const fence =
        step.kind === "claim-fence"
          ? deviceActiveOperations(device).find((op) => op.operationId === step.operationId)
          : undefined;
      const pending = fence ?? reservation;
      const original = pending === undefined ? undefined : originalRequest(pending);
      if (
        step.kind === "claim-fence" &&
        (fence === undefined ||
          original === undefined ||
          fence.step === "directive" ||
          (fence.step === "install") !== (original.action.kind === "install") ||
          actionSource(original) !== plan.target.sourceSha)
      )
        continue;
      const expired =
        reservation !== undefined && Date.parse(reservation.lease.leaseUntil) <= driver.now();
      const directive =
        reservation?.request.kind === "plan-directive" ? reservation.request.input : undefined;
      if (directive !== undefined && !canReplayDirective(directive, fleetStatus)) continue;
      const routeAllows = (current: ReturnType<typeof authority>) =>
        step.kind === "claim-fence"
          ? enrollment !== "changed" && current.enrollment !== "changed"
          : enrollment === "match" && current.enrollment === "match";
      const canMutate = (current: ReturnType<typeof authority>) =>
        !driver.cancelled() &&
        canOperate &&
        current.canDispatch &&
        routeAllows(current) &&
        (step.kind === "claim-directive"
          ? current.fleetStatus?.planDirectiveProtocol === 1 &&
            (current.fleetStatus.follower?.planDirective?.directiveId ?? null) ===
              step.expectedDirectiveId
          : directive === undefined || canReplayDirective(directive, current.fleetStatus));
      const before = authority();
      if (!canMutate(before)) {
        if (pending !== undefined && !routeAllows(before)) await readReservation(pending);
        continue;
      }
      // Reclaim through native main even for this window's own reservation: it rechecks the
      // gate and current membership, and returns the exact original request for idempotency.
      if (
        reservation !== undefined &&
        (expired
          ? directive === undefined
          : reservation.lease.claimant !== driver.claimant ||
            reservation.step === "install" ||
            (directive === undefined && original === undefined))
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
      const claimed = await driver.desktop(claim);
      const grant = claimed.grant;
      if (
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
      if (!matchesGrantRequest(grant, claim, plan.target, pending)) continue;
      const latest = authority();
      if (!canMutate(latest)) {
        const request = grant.request;
        if (
          !routeAllows(latest) &&
          (request.kind === "operation-start" || request.kind === "operation-fence")
        )
          await readReceipt(request.input);
        continue;
      }
      await dispatchGrant(driver, grant, report);
    } catch {
      // A later pass reconciles only the exact pending operation, under a native lease.
      // A disconnected host does not prevent other selected hosts from making progress.
    }
  }
}

function actionSource({ action }: JonesOperationStartInput) {
  return action.kind === "download" ? action.sourceSha : action.targetSource;
}

function matchesGrantRequest(
  grant: JonesInstallPlanGrant,
  claim: Extract<JonesInstallPlanRequest, { type: "claim" }>,
  target: JonesInstallPlanTarget,
  pending?: JonesInstallPlanHostOperation,
) {
  const { request } = grant;
  if (request.input.environmentId !== grant.environmentId) return false;
  if (request.kind === "plan-directive") {
    const { input } = request;
    return (
      claim.step === "directive" &&
      input.directiveId === grant.operationId &&
      input.enrollmentId === claim.enrollmentId &&
      input.expectedDirectiveId === claim.expectedDirectiveId &&
      input.repository === target.repository &&
      input.targetSource === target.sourceSha &&
      input.targetTree === target.sourceTree &&
      (pending === undefined ||
        (pending.request.kind === "plan-directive" &&
          sameDirectiveBinding(pending.request.input, input)))
    );
  }
  if (request.kind !== "operation-start" && request.kind !== "operation-fence") return false;
  const { input } = request;
  if (
    input.operationId !== grant.operationId ||
    input.correlation?.planId !== grant.planId ||
    input.correlation.deviceKey !== grant.environmentId ||
    actionSource(input) !== target.sourceSha
  )
    return false;
  if (
    pending !== undefined &&
    (pending.request.kind !== "operation-start" ||
      !sameOperationBinding(pending.request.input, input))
  )
    return false;
  return request.kind === "operation-fence"
    ? claim.step === "fence" && pending !== undefined
    : claim.step === "download" && input.action.kind !== "install";
}

async function dispatchGrant(
  driver: InstallPlanDriver,
  grant: JonesInstallPlanGrant,
  report: (operationId: string, receipt: JonesOperationReceipt) => Promise<void>,
): Promise<void> {
  const request = grant.request;
  if (request.kind === "plan-directive") {
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
    } catch {
      // A refusal or lost reply cannot settle every captured send of this ID.
    }
    return;
  }
  if (request.kind !== "operation-start" && request.kind !== "operation-fence") return;
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
