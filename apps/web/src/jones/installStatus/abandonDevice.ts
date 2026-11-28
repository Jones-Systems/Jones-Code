import { msg } from "gt-react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { FleetHostStatus, FleetPlanDirective } from "@t3tools/contracts/jones/fleet-updates";
import type {
  JonesInstallPlan,
  JonesInstallPlanDevice,
} from "@t3tools/contracts/jones/installPlan";
import {
  deviceActiveOperations,
  deviceStagedReceipt,
  matchesPlanTarget,
  operationOutstanding,
  sameDirectiveBinding,
  validateReceipt,
} from "@t3tools/client-runtime/jones/install-plan";
import { m } from "../../i18n/messages";
import {
  authorizedUpdateStatus,
  currentAuthorizedUpdateStatus,
  currentPlanHostAuthority,
  exactFleetEnrollment,
  exactOperationRequest,
  exactPlan,
  hasCurrentPlanEnrollment,
  hasOperationReadScope,
  installPlanServices,
  nativePlanIssue,
  readTargetPlan,
  requestInstallPlan,
  requireWritablePlan,
  type PlanTarget,
} from "./services";

function retainedAttempt(device: JonesInstallPlanDevice) {
  const operations = deviceActiveOperations(device);
  const staged = operations.findLast(
    (operation) => operation.step === "download" && operation.receipt?.phase === "completed",
  )?.receipt;
  const directive = operations.findLast((operation) => operation.directive?.kind === "applied");
  return {
    addedAt: device.addedAt,
    route: device.route,
    enrollmentId: device.enrollmentId,
    lastOperationId: operations.at(-1)?.operationId,
    stageOperationId: staged?.operationId,
    stagedHandle: staged?.staged?.stagedHandle,
    directive:
      directive?.request.kind === "plan-directive" && directive.directive?.kind === "applied"
        ? {
            operationId: directive.operationId,
            input: directive.request.input,
            applied: directive.directive.directive,
          }
        : undefined,
  };
}

type AbandonTarget = PlanTarget & {
  readonly environmentId: EnvironmentId;
  readonly attempt: ReturnType<typeof retainedAttempt>;
  readonly resume: boolean;
};

export function deviceAbandonTarget(
  plan: JonesInstallPlan,
  environmentId: EnvironmentId,
  archived: boolean,
): AbandonTarget | undefined {
  const device = plan.devices.find((item) => item.environmentId === environmentId);
  if (device === undefined || (archived && device.abandoning !== true)) return undefined;
  return {
    planId: plan.planId,
    expectedRevision: plan.revision,
    archived,
    environmentId,
    attempt: retainedAttempt(device),
    resume: device.abandoning === true,
  };
}

function sameAppliedDirective(left: FleetPlanDirective | undefined, right: FleetPlanDirective) {
  return (
    left?.directiveId === right.directiveId &&
    left.targetSource === right.targetSource &&
    left.targetTree === right.targetTree &&
    left.mode === right.mode &&
    left.issuedAt === right.issuedAt
  );
}

export async function abandonDevice(target: AbandonTarget, binding = installPlanServices()) {
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { services } = binding;
  const { environmentId, planId, archived, attempt } = target;
  const readExact = async (phase: "review" | "dispatch" | "finish") => {
    const reviewing = phase === "review";
    let readArchive = archived === true;
    let { result, plan } = await readTargetPlan(binding, target);
    requireWritablePlan(binding, result);
    // Finishing records evidence from the original attempt even if it closed in flight.
    if (
      phase === "finish" &&
      !readArchive &&
      plan === null &&
      result.state.view.recoveryHistory === 1
    ) {
      readArchive = true;
      ({ result, plan } = await readTargetPlan(binding, { planId, archived: true }));
      requireWritablePlan(binding, result);
    }
    if (plan === null || (reviewing && plan.revision !== target.expectedRevision))
      throw new Error("This update plan or attempt changed. Refresh Install status.");
    const device = plan.devices.find((item) => item.environmentId === environmentId);
    const latest = device === undefined ? undefined : retainedAttempt(device);
    if (
      (readArchive
        ? result.state.view.recoveryHistory !== 1
        : result.state.view.deviceRecovery !== 1) ||
      device?.membership !== "selected" ||
      (device.abandoning === true) !== (reviewing ? target.resume : true) ||
      latest?.addedAt !== attempt.addedAt ||
      latest.route !== attempt.route ||
      latest.enrollmentId !== attempt.enrollmentId ||
      latest.lastOperationId !== attempt.lastOperationId ||
      latest.stageOperationId !== attempt.stageOperationId ||
      latest.stagedHandle !== attempt.stagedHandle ||
      (attempt.directive === undefined
        ? latest.directive !== undefined
        : latest.directive === undefined ||
          latest.directive.operationId !== attempt.directive.operationId ||
          !sameDirectiveBinding(latest.directive.input, attempt.directive.input) ||
          !sameAppliedDirective(latest.directive.applied, attempt.directive.applied))
    )
      throw new Error("The reviewed attempt or staged selection changed. Refresh Install status.");
    return { plan, device, archived: readArchive };
  };
  const first = await readExact("review");
  if (
    !(await services.confirm(
      target.resume
        ? `Resume abandonment of the recorded update on ${first.device.label}?`
        : `Abandon this update on ${first.device.label}? This discards the staged selection created by this attempt and restores a follower's previous update policy. Installed builds and operation history are kept.`,
    ))
  )
    return;

  const observed = await services.observe(environmentId);
  const { plan, device } = await readExact("review");
  const operations = deviceActiveOperations(device);
  if (operations.some(operationOutstanding))
    throw new Error("Check the outstanding operation before abandoning this device.");
  const staged = deviceStagedReceipt(operations);
  const creator = operations.find(
    (operation) => operation.operationId === attempt.stageOperationId,
  );
  const original =
    creator === undefined ? undefined : exactOperationRequest(planId, environmentId, creator);
  const directive = attempt.directive?.input;
  const applied = attempt.directive?.applied;
  if (
    (directive === undefined && operations.some((item) => item.directive?.kind === "applied")) ||
    (directive !== undefined &&
      (device.route !== "follower-operations" ||
        directive.directiveId !== attempt.directive?.operationId ||
        directive.environmentId !== environmentId ||
        directive.enrollmentId !== device.enrollmentId ||
        directive.repository !== plan.target.repository ||
        directive.targetSource !== plan.target.sourceSha ||
        directive.targetTree !== plan.target.sourceTree ||
        applied?.directiveId !== directive.directiveId ||
        applied.targetSource !== directive.targetSource ||
        applied.targetTree !== directive.targetTree))
  )
    throw new Error(m(msg("The original follower directive is unconfirmed.")));
  const authorizedStatus = () => {
    const status = currentAuthorizedUpdateStatus(services, environmentId);
    if (
      status?.capability.operations !== 1 ||
      !hasCurrentPlanEnrollment(services, device, observed)
    )
      throw new Error("Refresh the device and its permissions before abandoning its update.");
    return status;
  };
  const state = authorizedStatus();
  const observedAt = observed.observation.heardAt ?? services.now();
  const stageReplacement =
    staged?.staged?.createdByOperation === true &&
    state.stagedHandle !== undefined &&
    state.stagedHandle !== staged.staged.stagedHandle
      ? { operationId: staged.operationId, stagedHandle: state.stagedHandle, observedAt }
      : undefined;
  const targetRunning = matchesPlanTarget(plan, device, state);
  if (targetRunning && !target.resume)
    throw new Error(
      "The selected build is already running on this device. Finish the plan to record it.",
    );
  const runningTarget =
    targetRunning && state.currentVersion !== undefined
      ? {
          build: {
            version: state.currentVersion,
            sourceSha: plan.target.sourceSha,
            sourceTree: plan.target.sourceTree,
          },
          observedAt,
        }
      : undefined;
  const retireStage =
    staged !== undefined && runningTarget === undefined && stageReplacement === undefined;
  if (
    retireStage &&
    (original === undefined ||
      staged.staged?.createdByOperation !== true ||
      state.capability.abandonStage !== 1)
  )
    throw new Error("This host cannot prove ownership or retire the staged selection.");
  const fleet = observed.fleetStatus;
  const sameEnrollment = (status: FleetHostStatus | null | undefined) => {
    const enrollment = exactFleetEnrollment(environmentId, status);
    return enrollment?.enabled === true && enrollment.enrollmentId === directive?.enrollmentId;
  };
  const releaseMatches = (status: FleetHostStatus | null | undefined) =>
    directive !== undefined &&
    sameEnrollment(status) &&
    status?.releasedDirectiveId === directive.directiveId &&
    status.follower?.planDirective?.directiveId !== directive.directiveId;
  const canRelease = (status: FleetHostStatus | null | undefined) => {
    if (directive === undefined || applied === undefined || status?.releasePlanDirective !== 1)
      return false;
    const current = status.follower?.planDirective;
    return (
      sameEnrollment(status) &&
      ((status.releasableDirectiveId === directive.directiveId &&
        sameAppliedDirective(current, applied)) ||
        (target.resume &&
          status.follower?.protocol === 1 &&
          (current?.directiveId ?? null) === directive.expectedDirectiveId))
    );
  };
  // The exact release can have committed before its reply reached this plan.
  if (directive !== undefined && !releaseMatches(fleet) && !canRelease(fleet))
    throw new Error("This host cannot restore the original follower directive.");

  if (!target.resume) {
    const begun = await requestInstallPlan(
      {
        type: "abandon-device",
        phase: "begin",
        planId,
        environmentId,
        expectedRevision: plan.revision,
      },
      binding,
    );
    requireWritablePlan(binding, begun);
    if (
      exactPlan(begun, planId, false)?.devices.find((d) => d.environmentId === environmentId)
        ?.abandoning !== true
    )
      throw new Error("Abandonment was not recorded. Refresh the device.");
  }

  if (retireStage && staged.staged !== undefined && original !== undefined) {
    await readExact("dispatch");
    const live = authorizedStatus();
    if (
      live.capability.abandonStage !== 1 ||
      (live.stagedHandle !== undefined && live.stagedHandle !== staged.staged.stagedHandle)
    )
      throw new Error("The current staged selection changed. Review the original attempt again.");
    const reply = await services.operation(environmentId, {
      action: "abandonStage",
      input: {
        environmentId,
        planId,
        operationId: staged.operationId,
        stagedHandle: staged.staged.stagedHandle,
        confirmed: true,
      },
    });
    const receipt = "receipt" in reply ? reply.receipt : null;
    if (
      receipt?.stageRetiredAt === undefined ||
      !validateReceipt(original, receipt) ||
      !validateReceipt(staged, receipt)
    )
      throw new Error("The host did not return an exact retirement receipt.");
    requireWritablePlan(binding);
    authorizedStatus();
    const report = await requestInstallPlan(
      {
        type: "report",
        planId,
        environmentId,
        operationId: staged.operationId,
        evidence: { kind: "receipt", receipt },
      },
      binding,
    );
    const issue = nativePlanIssue(report);
    if (issue !== undefined)
      throw new Error(`The host confirmed retirement. Saving it failed: ${issue}`);
  }

  let directiveRelease: FleetHostStatus | undefined;
  if (directive !== undefined) {
    if (fleet != null && releaseMatches(fleet)) directiveRelease = fleet;
    else {
      directiveRelease = await services.host(environmentId, {
        action: "releaseDirective",
        input: directive,
        beforeSend: async () => {
          const fresh = await services.observe(environmentId);
          const { device: reserved } = await readExact("dispatch");
          const authority = currentPlanHostAuthority(binding, reserved);
          const currentFleet = services
            .devices()
            .find((item) => item.environmentId === environmentId)?.fleetStatus;
          return (
            !deviceActiveOperations(reserved).some(operationOutstanding) &&
            authorizedUpdateStatus(fresh, environmentId)?.capability.operations === 1 &&
            authority.canRecoverOperation &&
            authority.enrollment === "match" &&
            canRelease(fresh.fleetStatus) &&
            canRelease(authority.fleetStatus) &&
            canRelease(currentFleet)
          );
        },
      });
      if (!releaseMatches(directiveRelease))
        throw new Error("The original follower directive release is unconfirmed.");
    }
  }
  const latest = await readExact("finish");
  if (directiveRelease === undefined) authorizedStatus();
  else if (!hasOperationReadScope(services, environmentId))
    throw new Error(m(msg("The host confirmed release. Read permission is required to save it.")));
  const finished = await requestInstallPlan(
    {
      type: "abandon-device",
      phase: "finish",
      planId,
      environmentId,
      expectedRevision: latest.plan.revision,
      ...(directiveRelease === undefined ? {} : { directiveRelease }),
      ...(runningTarget === undefined ? {} : { runningTarget }),
      ...(stageReplacement === undefined || runningTarget !== undefined
        ? {}
        : { stageReplacement }),
    },
    binding,
  );
  const issue = nativePlanIssue(finished);
  if (issue !== undefined) throw new Error(issue);
  const verified = latest.archived
    ? (await readTargetPlan(binding, { planId, archived: true })).plan
    : exactPlan(finished, planId, false);
  const outcome = verified?.devices.find((item) => item.environmentId === environmentId);
  if (
    !(runningTarget === undefined
      ? outcome?.membership === "removed"
      : outcome?.verifiedElsewhere !== undefined)
  )
    throw new Error("Abandonment was not confirmed. Refresh the device.");
  services.kick?.(environmentId);
}
