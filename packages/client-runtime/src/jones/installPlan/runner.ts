// @effect-diagnostics globalDate:off - Promise driver supplies scheduling time; Date only parses native lease timestamps.
import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlan,
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
import {
  isJonesOperationNotSentError,
  type JonesOperationBeforeSend,
  type JonesOperationCommand,
  type JonesUpdateObservation,
} from "../updates/jonesUpdates.ts";
import type { FleetHostRequest } from "../fleetUpdates/hostBridge.ts";
import {
  allowsOperationRecovery,
  canFenceWithUnavailableFleet,
  canReplayDirective,
  deviceActiveOperations,
  exactOperationRequest,
  isPreacceptanceOperationConflict,
  isProvisionalUnsupportedReceipt,
  matchPlanEnrollment,
  nextRunnerStep,
  sameDirectiveBinding,
  sameOperationBinding,
  supportsOperationReceiptReads,
  validateDirectiveEvidence,
  validateOperationEvidence,
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
    readonly canReportReceipt?: boolean;
    readonly enrollment: PlanEnrollmentMatch;
    readonly fleetStatus: FleetHostStatus | null | undefined;
    readonly boundAdmission?: 1;
    readonly allowUnavailableFence?: boolean;
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
      const report = async (receipt: JonesOperationReceipt, ledgerRead?: true) => {
        // Work already sent can settle after this window stops scheduling.
        const { canReadReceipt, canReportReceipt = canReadReceipt } = authority();
        if (
          !canReportReceipt ||
          (receipt.staged !== undefined && receipt.staged.sourceTree !== plan.target.sourceTree)
        )
          return;
        await driver.desktop({
          type: "report",
          planId: plan.planId,
          environmentId: device.environmentId,
          operationId: receipt.operationId,
          evidence: { kind: "receipt", receipt, ...(ledgerRead ? { ledgerRead } : {}) },
        });
      };
      if (step.kind === "wait") continue;
      const readReceipt = async (
        input: JonesOperationStartInput,
        prior?: JonesOperationReceipt,
      ) => {
        const status = observation.state;
        const current = authority();
        if (
          driver.cancelled() ||
          status?.environmentId !== device.environmentId ||
          !supportsOperationReceiptReads(status.capability) ||
          !current.canReadReceipt ||
          !allowsOperationRecovery(enrollment, "read") ||
          !allowsOperationRecovery(current.enrollment, "read")
        )
          return;
        const { operationId } = input;
        const result = await driver.operation(device.environmentId, {
          action: "operationRead",
          input: { environmentId: device.environmentId, operationIds: [operationId] },
        });
        if (!("receipts" in result) || result.receipts.length !== 1) return;
        const exact = result.receipts[0];
        if (exact?.operationId !== operationId) return;
        const { receipt } = exact;
        if (
          receipt !== null &&
          validateOperationEvidence(input, receipt, { ledgerRead: true }) &&
          (prior === undefined || validateOperationEvidence(prior, receipt, { ledgerRead: true }))
        )
          await report(receipt, true);
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
        const current = authority();
        if (!current.canReadReceipt || current.enrollment !== "match") continue;
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
      const recovery = step.kind === "claim-fence" ? "fence" : "start";
      const allowUnavailableFence = canFenceWithUnavailableFleet(
        device,
        observation.state,
        fleetStatus,
      );
      const routeAllows = (current: ReturnType<typeof authority>) =>
        allowsOperationRecovery(enrollment, recovery, allowUnavailableFence) &&
        allowsOperationRecovery(
          current.enrollment,
          recovery,
          current.allowUnavailableFence ??
            (allowUnavailableFence && current.fleetStatus === undefined),
        );
      const canTargetBound = (current: ReturnType<typeof authority>) =>
        observation.state?.capability.boundAdmission === 1 && current.boundAdmission === 1;
      const canMutate = (
        current: ReturnType<typeof authority>,
        input: JonesOperationStartInput | undefined,
      ) => {
        if (
          driver.cancelled() ||
          !canOperate ||
          !current.canDispatch ||
          !routeAllows(current) ||
          (input?.admission !== undefined && !canTargetBound(current))
        )
          return false;
        if (step.kind === "claim-directive")
          return (
            current.fleetStatus?.planDirectiveProtocol === 1 &&
            (current.fleetStatus.follower?.planDirective?.directiveId ?? null) ===
              step.expectedDirectiveId
          );
        if (directive !== undefined) return canReplayDirective(directive, current.fleetStatus);
        return (
          recovery === "fence" ||
          (matchesForwardRoute(plan, device, input, fleetStatus) &&
            matchesForwardRoute(plan, device, input, current.fleetStatus))
        );
      };
      const before = authority();
      if (!canMutate(before, original)) {
        if (
          pending !== undefined &&
          (!routeAllows(before) || (original?.admission !== undefined && !canTargetBound(before)))
        )
          await readReservation(pending);
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
                  ...(initial.state.view.boundAdmission === 1 && canTargetBound(before)
                    ? { boundAdmission: 1 }
                    : {}),
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
      const request = grant.request;
      const grantedInput =
        request.kind === "operation-start" || request.kind === "operation-fence"
          ? request.input
          : undefined;
      const latest = authority();
      if (!canMutate(latest, grantedInput)) {
        if (
          grantedInput !== undefined &&
          (!routeAllows(latest) ||
            (grantedInput.admission !== undefined && !canTargetBound(latest)))
        )
          await readReceipt(grantedInput);
        continue;
      }
      const stillEligible = () =>
        Date.parse(grant.leaseUntil) > driver.now() && canMutate(authority(), grantedInput);
      const beforeSend: JonesOperationBeforeSend = async () => {
        if (!stillEligible()) return false;
        const native = await driver.desktop({ type: "read" });
        return matchesActiveReservation(native, plan, device, grant, driver) && stillEligible();
      };
      try {
        const needsLedgerRead = await dispatchGrant(driver, grant, beforeSend, report);
        if (needsLedgerRead && grantedInput !== undefined) await readReceipt(grantedInput);
      } catch (error) {
        if (grantedInput?.admission !== undefined && !isJonesOperationNotSentError(error))
          await readReceipt(grantedInput);
      }
    } catch {
      // A later pass reconciles only the exact pending operation, under a native lease.
      // A disconnected host does not prevent other selected hosts from making progress.
    }
  }
}

function actionSource({ action }: JonesOperationStartInput) {
  return action.kind === "download" ? action.sourceSha : action.targetSource;
}

/** Qualifies the matched route against the plan's exact applied follower directive. */
export function matchesForwardRoute(
  plan: JonesInstallPlan,
  device: JonesInstallPlanDevice,
  input: JonesOperationStartInput | undefined,
  status: FleetHostStatus | null | undefined,
): boolean {
  const admission = input?.admission;
  if (device.route === "operations") return admission?.kind !== "follower";
  const applied = deviceActiveOperations(device).findLast(
    (op) => op.request.kind === "plan-directive" && op.directive?.kind === "applied",
  );
  if (
    applied?.request.kind !== "plan-directive" ||
    applied.directive?.kind !== "applied" ||
    status == null
  )
    return false;
  const original = applied.request.input;
  return (
    original.environmentId === device.environmentId &&
    original.enrollmentId === device.enrollmentId &&
    original.directiveId === applied.operationId &&
    original.directiveId === applied.directive.directive.directiveId &&
    original.targetSource === plan.target.sourceSha &&
    original.targetTree === plan.target.sourceTree &&
    validateDirectiveEvidence(original, status) &&
    (admission === undefined ||
      (admission.kind === "follower" &&
        admission.enrollmentId === original.enrollmentId &&
        admission.directiveId === original.directiveId &&
        admission.targetSource === original.targetSource &&
        admission.targetTree === original.targetTree))
  );
}

function matchesActiveReservation(
  native: JonesInstallPlanResult,
  originalPlan: JonesInstallPlan,
  originalDevice: JonesInstallPlanDevice,
  grant: JonesInstallPlanGrant,
  driver: Pick<InstallPlanDriver, "claimant" | "now">,
): boolean {
  const request = grant.request;
  if (request.kind !== "operation-start" && request.kind !== "operation-fence") return false;
  const plan = native.state.current;
  if (
    native.refused !== undefined ||
    native.state.readOnly !== undefined ||
    plan?.planId !== originalPlan.planId ||
    plan.dismissedAt !== undefined ||
    plan.supersededAt !== undefined ||
    plan.cancelled === true ||
    plan.target.repository !== originalPlan.target.repository ||
    plan.target.sourceSha !== originalPlan.target.sourceSha ||
    plan.target.sourceTree !== originalPlan.target.sourceTree
  )
    return false;
  const device = plan.devices.find((d) => d.environmentId === grant.environmentId);
  if (
    device?.membership !== "selected" ||
    device.abandoning === true ||
    device.route !== originalDevice.route ||
    device.enrollmentId !== originalDevice.enrollmentId
  )
    return false;
  const operation = deviceActiveOperations(device).find(
    (op) => op.operationId === grant.operationId,
  );
  const original = operation && exactOperationRequest(plan.planId, device.environmentId, operation);
  if (
    operation === undefined ||
    original === undefined ||
    operation.step !== (request.input.action.kind === "install" ? "install" : "download") ||
    operation.receipt !== undefined ||
    !sameOperationBinding(original, request.input)
  )
    return false;
  const fence = request.kind === "operation-fence";
  const lease = fence ? operation.fence?.lease : operation.lease;
  if (lease?.claimant !== driver.claimant || lease.leaseUntil !== grant.leaseUntil) return false;
  if (fence) {
    const originalExpiry = Date.parse(operation.lease.leaseUntil);
    return Number.isFinite(originalExpiry) && originalExpiry <= driver.now();
  }
  return (
    !native.state.laptop.activationBlocked &&
    plan.laptop.gate !== undefined &&
    plan.laptop.gateClosed === undefined &&
    plan.laptop.gatePause !== true &&
    device.ended === undefined &&
    device.verifiedElsewhere === undefined &&
    operation.fence === undefined
  );
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
  if (request.kind === "operation-fence") return claim.step === "fence" && pending !== undefined;
  return (
    claim.step === "download" &&
    claim.action !== undefined &&
    input.action.kind !== "install" &&
    input.expectedInstalledSource === claim.expectedInstalledSource &&
    sameOperationBinding(input, { ...input, action: claim.action }) &&
    (pending !== undefined || (claim.boundAdmission === 1) === (input.admission !== undefined))
  );
}

async function dispatchGrant(
  driver: InstallPlanDriver,
  grant: JonesInstallPlanGrant,
  beforeSend: JonesOperationBeforeSend,
  report: (receipt: JonesOperationReceipt) => Promise<void>,
): Promise<boolean> {
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
    return false;
  }
  if (request.kind !== "operation-start" && request.kind !== "operation-fence") return false;
  const receipt = await driver.operation(
    grant.environmentId,
    request.kind === "operation-start"
      ? { action: "operationStart", input: request.input, beforeSend }
      : { action: "operationFence", input: request.input, beforeSend },
  );
  if ("receipts" in receipt) return false;
  if (validateOperationEvidence(request.input, receipt)) await report(receipt);
  return isProvisionalUnsupportedReceipt(receipt) || isPreacceptanceOperationConflict(receipt);
}
