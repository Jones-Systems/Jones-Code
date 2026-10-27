import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlan, JonesInstallPlanSelection, JonesInstallPlanState, JonesInstallPlanTarget } from "@t3tools/contracts/jones/installPlan";
import type { JonesRecoveryPreflight } from "@t3tools/contracts/jones/jonesOperations";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import type { JonesUpdateObservation } from "../updates/jonesUpdates.ts";
import { jonesTargetStatus } from "../updates/targetStatus.ts";
import { deviceSettled, deviceState, isPlanComplete, selectability, startPlan, type InstallPlanDeviceState } from "./model.ts";

const copy: Record<InstallPlanDeviceState, string> = {
  removed: "Removed · never started", "not-started": "Not started · Laptop didn't update", queued: "Waits for Laptop",
  directive: "Getting device ready…", reserved: "Starting…", downloading: "Downloading…", "stop-requested": "Stop requested…",
  verifying: "Checking download · can't stop now", installing: "Installing · restarting", staged: "Downloaded · install when ready",
  done: "Updated · verified", "updated-elsewhere": "Already on the selected source", stopped: "Stopped · nothing installed",
  ended: "Stopped", "not-updated": "Not updated", "rolled-back": "Rolled back", blocked: "Didn't finish",
  held: "Stop not confirmed", unknown: "Not confirmed", fenced: "Waiting for a new download reservation",
};

export interface InstallPlanDeviceObservation {
  readonly observation: JonesUpdateObservation;
  readonly fleetStatus?: FleetHostStatus | null;
  readonly canOperate: boolean;
}

/** A client-side preview; this never creates native evidence or dispatch authority. */
export function previewInstallPlan(input: {
  readonly planId: string; readonly target: JonesInstallPlanTarget; readonly from: JonesInstallPlan["from"];
  readonly laptopEnvironmentId: EnvironmentId; readonly now: string;
  readonly devices: readonly { readonly environmentId: EnvironmentId; readonly label: string; readonly status: InstallPlanDeviceObservation }[];
}) {
  const select: JonesInstallPlanSelection[] = [];
  const unavailable: { environmentId: EnvironmentId; label: string; reason: string }[] = [];
  for (const device of input.devices) {
    if (device.environmentId === input.laptopEnvironmentId) continue;
    const selectable = selectability(device.status.observation, device.status.fleetStatus);
    if (selectable.selectable && device.status.canOperate && select.length < 16)
      select.push({ environmentId: device.environmentId, label: device.label, route: selectable.route,
        ...(selectable.enrollmentId === undefined ? {} : { enrollmentId: selectable.enrollmentId }) });
    else unavailable.push({ environmentId: device.environmentId, label: device.label,
      reason: !device.status.canOperate ? "This session cannot change updates." : selectable.selectable ? "The plan already has 16 devices." : selectable.reason });
  }
  return { plan: startPlan({ ...input, origin: "update-button", select }), unavailable };
}

export function installPlanPresentation(state: JonesInstallPlanState, options: {
  readonly now: number;
  readonly devices?: ReadonlyMap<EnvironmentId, InstallPlanDeviceObservation>;
  readonly laptop?: JonesUpdateObservation;
} ) {
  const plan = state.current ?? state.last;
  const readOnly = state.readOnly !== undefined || state.current === null;
  if (plan === null) return { plan: null, readOnly, rows: [], complete: false, headerDoneVisible: false,
    gateText: "Others wait until Laptop is updated and verified.", summary: "No update plan" };
  const rows = plan.devices.map((device) => {
    const phase = deviceState(plan, device, options.now);
    const observed = options.devices?.get(device.environmentId);
    const status = observed === undefined ? undefined : jonesTargetStatus(observed.observation, {
      canOperate: observed.canOperate, targetSource: plan.target.sourceSha, planId: plan.planId,
    });
    const nativeGate = plan.laptop.gate !== undefined && plan.laptop.gateClosed === undefined;
    const mutable = !readOnly && device.membership === "selected" && nativeGate && observed?.observation.freshness === "fresh" && observed.canOperate &&
      observed.observation.state?.capability.operations === 1;
    const latest = device.operations.at(-1);
    const lastDownload = device.operations.filter((o) => o.step === "download").at(-1);
    let text = copy[phase];
    if (phase === "directive") text = `Getting ${device.label} ready…`;
    if (phase === "ended") text = lastDownload?.receipt?.staged !== undefined && observed?.observation.freshness === "fresh" &&
      observed.observation.state?.stagedHandle === lastDownload?.receipt?.staged?.stagedHandle ?
      "Stopped · kept staged files" : "Stopped · nothing installed";
    if (["held", "blocked", "not-updated"].includes(phase) && latest?.receipt?.code !== undefined) text += ` · ${latest.receipt.code}`;
    return {
      environmentId: device.environmentId, label: device.label, phase, copyKey: `device.${phase}`, text,
      settled: deviceSettled(plan, device), status,
      ...(latest === undefined ? {} : { operationId: latest.operationId }),
      actions: {
        remove: !readOnly && device.membership === "selected" && device.operations.length === 0,
        download: mutable && ["stopped", "not-updated"].includes(phase) && status?.actions.download === true,
        install: mutable && phase === "staged" && status?.actions.install === true,
        stop: mutable && phase === "downloading" && latest?.receipt !== undefined && ["accepted", "running"].includes(latest.receipt.phase) && status?.actions.stop === true,
        end: mutable && phase === "staged", checkAgain: true,
      },
    };
  });
  const laptopOperation = plan.laptop.operations.at(-1);
  const laptopPhase = plan.laptop.gateClosed !== undefined ? "gateClosed" : plan.laptop.gate !== undefined ? "verified" :
    laptopOperation?.result?.status === "committed" ? "committedGatePending" :
    laptopOperation?.result?.status === "completed" ? "staged" :
    laptopOperation?.result?.status === "running" ? laptopOperation.step === "install" ? "installing" :
      options.laptop?.freshness === "fresh" && options.laptop.state?.phase === "verifying" ? "verifying" : "downloading" :
    laptopOperation?.result?.status ?? "preview";
  const laptopCopy: Record<string, string> = {
    gateClosed: "Laptop changed · others won't start", verified: "Updated · verified", committedGatePending: "Checking after restart…",
    staged: "Downloaded · ready to install", installing: "Installing · restarting", downloading: "Downloading…",
    verifying: "Checking download · can't stop now",
    "stop-requested": "Stopping download…", stopped: "Download stopped · nothing installed", interrupted: "Download didn't finish · nothing installed",
    held: "Stop not confirmed", "rolled-back": `Rolled back · still on ${plan.from.version}`, blocked: "Didn't finish",
    failed: "Download didn't finish", refused: "Not updated", preview: `Goes first · ${plan.target.version} available`,
  };
  const complete = isPlanComplete(plan);
  const laptopState = options.laptop?.freshness === "fresh" ? options.laptop.state : null;
  const laptopActions = {
    download: !readOnly && ["preview", "stopped", "interrupted", "failed", "refused"].includes(laptopPhase) &&
      laptopState?.capability.download === true && laptopState.provenance?.sourceSha === plan.target.sourceSha,
    install: !readOnly && laptopPhase === "staged" && laptopState?.capability.install === true &&
      laptopState.stagedHandle !== undefined && laptopState.provenance?.sourceSha === plan.target.sourceSha,
    stop: !readOnly && laptopPhase === "downloading" && laptopState?.phase === "downloading" && laptopState.capability.operations === 1,
    checkAgain: true,
  };
  const selected = rows.filter((row) => plan.devices.find((d) => d.environmentId === row.environmentId)?.membership === "selected");
  return { plan, readOnly, rows, complete, headerDoneVisible: !readOnly && complete,
    laptop: { phase: laptopPhase, copyKey: `laptop.${laptopPhase}`, text: laptopCopy[laptopPhase] ?? "Not confirmed", actions: laptopActions,
      ...(laptopOperation === undefined ? {} : { operationId: laptopOperation.operationId }) },
    gateText: "Others wait until Laptop is updated and verified.",
    summary: complete ? "Update plan finished" : `${selected.filter((r) => r.settled).length} of ${selected.length} devices settled`,
  };
}

export function recoveryPreflightPresentation(preflight: JonesRecoveryPreflight, label: string) {
  return {
    text: preflight.firstInstall ? `First install on ${label} · no existing data to back up` : "Recovery covers the database, WAL, SHM and required settings.",
    clone: preflight.clone.status === "proved" && preflight.policies.cloneRequired.admissible,
    cloneOrCopy: preflight.policies.cloneOrCopy.admissible,
    reason: preflight.policies.cloneOrCopy.reason ?? preflight.clone.reason,
    stop: true,
  };
}
