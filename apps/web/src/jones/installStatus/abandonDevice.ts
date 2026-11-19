import type { EnvironmentId } from "@t3tools/contracts";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import {
  deviceActiveOperations,
  deviceStagedReceipt,
  matchesPlanTarget,
  operationOutstanding,
  validateReceipt,
} from "@t3tools/client-runtime/jones/install-plan";
import { installPlanServices, requestInstallPlan } from "./services";

export async function abandonDevice(environmentId: EnvironmentId, planId: string) {
  const binding = installPlanServices();
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { services } = binding;
  const current = async () => {
    if (installPlanServices() !== binding) throw new Error("Install status connection changed.");
    const { state, refused } = await requestInstallPlan({ type: "read" }, binding);
    if (refused !== undefined) throw new Error(refused.message);
    const plan = state.current;
    const device = plan?.devices.find((item) => item.environmentId === environmentId);
    if (
      state.view.deviceRecovery !== 1 ||
      plan?.planId !== planId ||
      device?.membership !== "selected"
    )
      throw new Error("The update plan or device changed. Refresh Install status.");
    return { plan, device };
  };
  const first = await current();
  if (
    !(await services.confirm(
      `Abandon this update on ${first.device.label}? Any staged selection created by this attempt will be discarded. A follower's previous update policy will be restored. Installed builds and operation history will be kept.`,
    ))
  )
    return;

  const { plan, device } = await current();
  const operations = deviceActiveOperations(device);
  if (operations.some(operationOutstanding))
    throw new Error("Check the outstanding operation before abandoning this device.");
  const staged = deviceStagedReceipt(operations);
  const directive = operations.findLast((operation) => operation.directive?.kind === "applied");
  const host = await services.observe(environmentId);
  const liveRead = services.readStatus;
  const state = liveRead === undefined ? host.observation.state : await liveRead(environmentId);
  const observedAt =
    liveRead === undefined ? (host.observation.heardAt ?? services.now()) : services.now();
  if (
    !host.canOperate ||
    (liveRead === undefined && host.observation.freshness !== "fresh") ||
    state?.environmentId !== environmentId
  )
    throw new Error("Refresh the device before abandoning its update.");
  const needsStage = staged !== undefined;
  const stageReplacement =
    staged?.staged?.createdByOperation === true &&
    state.stagedHandle !== undefined &&
    state.stagedHandle !== staged.staged.stagedHandle
      ? { operationId: staged.operationId, stagedHandle: state.stagedHandle, observedAt }
      : undefined;
  const targetRunning = matchesPlanTarget(plan, device, state);
  if (targetRunning && !device.abandoning)
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
  if (
    needsStage &&
    runningTarget === undefined &&
    stageReplacement === undefined &&
    (staged.staged?.createdByOperation !== true || state.capability.abandonStage !== 1)
  )
    throw new Error("This host cannot prove ownership or retire the staged selection.");
  if (
    directive !== undefined &&
    (host.fleetStatus?.releasePlanDirective !== 1 ||
      (!device.abandoning && host.fleetStatus.releasableDirectiveId !== directive.operationId))
  )
    throw new Error("This host cannot restore the previous follower directive.");

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
  if (begun.refused !== undefined) throw new Error(begun.refused.message);
  if (
    begun.state.current?.planId !== planId ||
    begun.state.current.devices.find((item) => item.environmentId === environmentId)?.abandoning !==
      true
  )
    throw new Error("Abandonment was not recorded. Refresh the device.");

  if (
    needsStage &&
    staged.staged !== undefined &&
    runningTarget === undefined &&
    stageReplacement === undefined
  ) {
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
    if (!("receipt" in reply) || reply.receipt === null)
      throw new Error(
        ("reason" in reply && reply.reason) || "Staged selection retirement was not confirmed.",
      );
    if (reply.receipt.stageRetiredAt === undefined || !validateReceipt(staged, reply.receipt))
      throw new Error("The retirement receipt does not match this device's staged selection.");
    const report = await requestInstallPlan(
      {
        type: "report",
        planId,
        environmentId,
        operationId: staged.operationId,
        evidence: { kind: "receipt", receipt: reply.receipt },
      },
      binding,
    );
    if (report.refused !== undefined) throw new Error(report.refused.message);
  }
  let directiveRelease: FleetHostStatus | undefined;
  if (directive?.request.kind === "plan-directive") {
    directiveRelease = await services.host(environmentId, {
      action: "releaseDirective",
      input: directive.request.input,
    });
  }
  const latest = await current();
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
  if (finished.refused !== undefined) throw new Error(finished.refused.message);
  if (
    finished.state.current?.planId !== planId ||
    !(runningTarget === undefined
      ? finished.state.current.devices.find((item) => item.environmentId === environmentId)
          ?.membership === "removed"
      : finished.state.current.devices.find((item) => item.environmentId === environmentId)
          ?.verifiedElsewhere !== undefined)
  )
    throw new Error("Abandonment was not confirmed. Refresh the device.");
  services.kick?.(environmentId);
}
