import type { EnvironmentId } from "@t3tools/contracts";
import type {
  FleetHostStatus,
  FleetPlanDirectiveInput,
} from "@t3tools/contracts/jones/fleet-updates";
import type {
  JonesInstallPlan,
  JonesInstallPlanDevice,
} from "@t3tools/contracts/jones/installPlan";
import {
  deviceActiveOperations,
  deviceStagedReceipt,
  matchesPlanTarget,
  operationOutstanding,
  validateReceipt,
} from "@t3tools/client-runtime/jones/install-plan";
import {
  currentAuthorizedUpdateStatus,
  exactOperationRequest,
  exactPlan,
  hasCurrentPlanEnrollment,
  installPlanServices,
  nativePlanIssue,
  readTargetPlan,
  readWritableTarget,
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
    directive: directive?.request.kind === "plan-directive" ? directive.request.input : undefined,
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

function sameDirective(
  left: FleetPlanDirectiveInput | undefined,
  right: FleetPlanDirectiveInput | undefined,
) {
  return (
    left?.environmentId === right?.environmentId &&
    left?.enrollmentId === right?.enrollmentId &&
    left?.directiveId === right?.directiveId &&
    left?.expectedDirectiveId === right?.expectedDirectiveId &&
    left?.repository === right?.repository &&
    left?.targetSource === right?.targetSource &&
    left?.targetTree === right?.targetTree
  );
}

export async function abandonDevice(target: AbandonTarget, binding = installPlanServices()) {
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { services } = binding;
  const { environmentId, planId, archived, attempt } = target;
  const readExact = async (resume: boolean, expectDisplayedRevision: boolean) => {
    const { state, plan } = await readWritableTarget(
      binding,
      expectDisplayedRevision ? target : { planId, archived: archived === true },
    );
    const device = plan.devices.find((item) => item.environmentId === environmentId);
    const latest = device === undefined ? undefined : retainedAttempt(device);
    if (
      (archived ? state.view.recoveryHistory !== 1 : state.view.deviceRecovery !== 1) ||
      device?.membership !== "selected" ||
      (device.abandoning === true) !== resume ||
      latest?.addedAt !== attempt.addedAt ||
      latest.route !== attempt.route ||
      latest.enrollmentId !== attempt.enrollmentId ||
      latest.lastOperationId !== attempt.lastOperationId ||
      latest.stageOperationId !== attempt.stageOperationId ||
      latest.stagedHandle !== attempt.stagedHandle ||
      !sameDirective(latest.directive, attempt.directive)
    )
      throw new Error("The reviewed attempt or staged selection changed. Refresh Install status.");
    return { plan, device };
  };
  const first = await readExact(target.resume, true);
  if (
    !(await services.confirm(
      target.resume
        ? `Resume abandonment of the recorded update on ${first.device.label}?`
        : `Abandon this update on ${first.device.label}? This discards the staged selection created by this attempt and restores a follower's previous update policy. Installed builds and operation history are kept.`,
    ))
  )
    return;

  const observed = await services.observe(environmentId);
  const { plan, device } = await readExact(target.resume, true);
  const operations = deviceActiveOperations(device);
  if (operations.some(operationOutstanding))
    throw new Error("Check the outstanding operation before abandoning this device.");
  const staged = deviceStagedReceipt(operations);
  const creator = operations.find(
    (operation) => operation.operationId === attempt.stageOperationId,
  );
  const original =
    creator === undefined ? undefined : exactOperationRequest(planId, environmentId, creator);
  const directive = attempt.directive;
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
  const releaseMatches = (status: FleetHostStatus | null | undefined) =>
    directive !== undefined &&
    status?.environmentId === environmentId &&
    status.enrollment?.enabled === true &&
    status.enrollment.environmentId === environmentId &&
    status.enrollment.enrollmentId === directive.enrollmentId &&
    status.releasedDirectiveId === directive.directiveId &&
    status.follower?.planDirective?.directiveId !== directive.directiveId;
  if (
    directive !== undefined &&
    !releaseMatches(fleet) &&
    (fleet?.releasePlanDirective !== 1 || fleet.releasableDirectiveId !== directive.directiveId)
  )
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
    await readExact(true, false);
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
      await readExact(true, false);
      authorizedStatus();
      directiveRelease = await services.host(environmentId, {
        action: "releaseDirective",
        input: directive,
      });
      if (!releaseMatches(directiveRelease))
        throw new Error("The original follower directive release is unconfirmed.");
    }
  }
  const latest = await readExact(true, false);
  authorizedStatus();
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
  const verified = archived
    ? (await readTargetPlan(binding, target)).plan
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
