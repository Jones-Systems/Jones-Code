import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import type { JonesRecoveryPreflight } from "@t3tools/contracts/jones/jonesOperations";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import type { JonesUpdateObservation } from "../updates/jonesUpdates.ts";
import { jonesTargetStatus } from "../updates/targetStatus.ts";
import {
  deviceSettled,
  deviceStateWithRunningTarget,
  isPlanComplete,
  type InstallPlanDeviceState,
} from "./model.ts";
import { installProgressView, progressForOperation, type InstallProgressView } from "./progress.ts";

const copy: Record<InstallPlanDeviceState, string> = {
  removed: "Removed · never started",
  "not-started": "Not started · Laptop didn't update",
  queued: "Waits for Laptop",
  directive: "Getting device ready…",
  reserved: "Starting…",
  downloading: "Downloading…",
  "stop-requested": "Stop requested…",
  verifying: "Checking download · can't stop now",
  installing: "Installing · restarting",
  staged: "Downloaded · install when ready",
  done: "Updated · verified",
  "updated-elsewhere": "Already on the selected source",
  stopped: "Stopped · nothing installed",
  ended: "Stopped",
  "not-updated": "Not updated",
  "rolled-back": "Rolled back",
  blocked: "Didn't finish",
  held: "Stop not confirmed",
  unknown: "Not confirmed",
  fenced: "Waiting for a new download reservation",
};

export interface InstallPlanDeviceObservation {
  readonly observation: JonesUpdateObservation;
  readonly fleetStatus?: FleetHostStatus | null;
  readonly canOperate: boolean;
}

/** Ledger states whose own text must stay visible over host step progress. */
const LEDGER_TEXT_WINS = new Set<InstallPlanDeviceState>(["held", "unknown", "stop-requested"]);
const ALREADY_RUNNING_TEXT = "Downloaded · already running this build";

function rowProgress(
  observation: JonesUpdateObservation | undefined,
  latestOperationId: string | undefined,
  planless: boolean,
): InstallProgressView | undefined {
  const progress = progressForOperation(observation?.state?.progress, latestOperationId, planless);
  return progress === undefined ? undefined : installProgressView(progress, observation?.heardAt);
}

type ActiveWork = "install" | "download" | undefined;
function activeWork(phase: string, progress: InstallProgressView | undefined): ActiveWork {
  if (progress?.active) return progress.kind;
  if (phase === "installing") return "install";
  if (phase === "downloading" || phase === "verifying") return "download";
  return undefined;
}

export function installPlanPresentation(
  state: JonesInstallPlanState,
  options: {
    readonly now: number;
    readonly devices?: ReadonlyMap<EnvironmentId, InstallPlanDeviceObservation>;
    readonly laptop?: JonesUpdateObservation;
    /** The plan is a local preview with no ledger, so any host progress is shown. */
    readonly planless?: boolean;
  },
) {
  const planless = options.planless === true;
  const plan = state.current ?? state.last;
  const readOnly = state.readOnly !== undefined || state.current === null;
  if (plan === null)
    return {
      plan: null,
      readOnly,
      rows: [],
      complete: false,
      headerDoneVisible: false,
      gateText: "Others wait until Laptop is updated and verified.",
      summary: "No update plan",
    };
  const rows = plan.devices.map((device) => {
    const observed = options.devices?.get(device.environmentId);
    const status =
      observed === undefined
        ? undefined
        : jonesTargetStatus(observed.observation, {
            canOperate: observed.canOperate,
            targetSource: plan.target.sourceSha,
            planId: plan.planId,
          });
    const nativeGate = plan.laptop.gate !== undefined && plan.laptop.gateClosed === undefined;
    const mutable =
      !readOnly &&
      device.membership === "selected" &&
      nativeGate &&
      observed?.observation.freshness === "fresh" &&
      observed.canOperate &&
      observed.observation.state?.capability.operations === 1;
    const latest = device.operations.at(-1);
    const lastDownload = device.operations.filter((o) => o.step === "download").at(-1);
    const receipt = latest?.receipt;
    const recoveredDownload =
      receipt?.code === "cleanup-recovered" &&
      receipt.jobId !== undefined &&
      (receipt.phase === "interrupted" || receipt.phase === "stopped");
    const retryDownload =
      latest?.step === "download" &&
      (recoveredDownload || receipt?.phase === "failed" || receipt?.phase === "refused");
    const retryInstall =
      latest?.step === "install" && (receipt?.phase === "failed" || receipt?.phase === "refused");
    const stage = lastDownload?.receipt?.staged;
    const hostState = observed?.observation.state;
    const installed = hostState?.installedSource;
    const confirmedInstall = device.operations.some(
      ({ step, operationId, receipt }) =>
        step === "install" &&
        receipt?.phase === "completed" &&
        receipt.updateId === operationId &&
        receipt.outcome?.status === "committed",
    );
    const runningTarget =
      observed?.observation.freshness === "fresh" &&
      hostState?.environmentId === device.environmentId &&
      installed?.repository === plan.target.repository &&
      installed.sha === plan.target.sourceSha &&
      installed.tree === plan.target.sourceTree;
    const targetBuildRunning = !confirmedInstall && runningTarget;
    // A fresh running target outranks an older settled ledger record, such as an interrupted
    // download that a later install through another operation made moot.
    const phase = deviceStateWithRunningTarget(plan, device, runningTarget, options.now);
    const matchingRetryStage =
      lastDownload?.receipt?.phase === "completed" &&
      stage !== undefined &&
      stage.stagedHandle === hostState?.stagedHandle &&
      stage.sourceSha === plan.target.sourceSha &&
      stage.sourceTree === plan.target.sourceTree &&
      hostState.provenance?.sourceSha === stage.sourceSha &&
      hostState.provenance.sourceTree === stage.sourceTree;
    let text = copy[phase];
    if (latest?.step === "download" && latest.receipt?.phase === "refused")
      text = "Download request did not start";
    if (
      runningTarget &&
      phase === "updated-elsewhere" &&
      latest?.receipt?.code !== "already-installed"
    )
      text = "Updated · running the selected source";
    if (phase === "directive") text = `Getting ${device.label} ready…`;
    if (phase === "ended")
      text =
        lastDownload?.receipt?.staged !== undefined &&
        observed?.observation.freshness === "fresh" &&
        observed.observation.state?.stagedHandle === lastDownload?.receipt?.staged?.stagedHandle
          ? "Stopped · kept staged files"
          : "Stopped · nothing installed";
    if (["held", "blocked", "not-updated"].includes(phase) && latest?.receipt?.code !== undefined)
      text += ` · ${latest.receipt.code}`;
    const alreadyRunning =
      phase === "staged" &&
      observed?.observation.freshness === "fresh" &&
      hostState?.stagedAlreadyRunning === true;
    if (alreadyRunning) text = ALREADY_RUNNING_TEXT;
    const progress = rowProgress(observed?.observation, latest?.operationId, planless);
    if (progress?.text !== undefined && !LEDGER_TEXT_WINS.has(phase)) text = progress.text;
    return {
      environmentId: device.environmentId,
      label: device.label,
      phase,
      copyKey: `device.${phase}`,
      text,
      targetBuildRunning,
      downloadLabel: retryDownload ? "Retry download" : `Download on ${device.label}`,
      installLabel: retryInstall ? "Retry install" : `Install on ${device.label}`,
      settled: phase === "updated-elsewhere" || deviceSettled(plan, device),
      status,
      activeWork: activeWork(phase, progress),
      ...(progress === undefined ? {} : { progress }),
      ...(latest === undefined ? {} : { operationId: latest.operationId }),
      actions: {
        remove: !readOnly && device.membership === "selected" && device.operations.length === 0,
        download:
          mutable &&
          ["stopped", "not-updated"].includes(phase) &&
          status?.actions.download === true,
        install:
          mutable &&
          phase === "staged" &&
          !alreadyRunning &&
          status?.actions.install === true &&
          (!retryInstall || matchingRetryStage),
        stop:
          mutable &&
          phase === "downloading" &&
          latest?.receipt !== undefined &&
          ["accepted", "running"].includes(latest.receipt.phase) &&
          status?.actions.stop === true,
        end: mutable && phase === "staged",
        checkAgain: true,
      },
    };
  });
  const laptopOperation = plan.laptop.operations.at(-1);
  const retryLaptopDownload =
    laptopOperation?.step === "download" &&
    ["failed", "refused"].includes(laptopOperation.result?.status ?? "");
  const laptopState = options.laptop?.freshness === "fresh" ? options.laptop.state : null;
  const coldStage =
    !readOnly &&
    laptopOperation === undefined &&
    laptopState?.phase === "staged" &&
    laptopState.stagedHandle !== undefined &&
    laptopState.provenance?.sourceSha === plan.target.sourceSha &&
    laptopState.provenance.sourceTree === plan.target.sourceTree;
  const retryLaptopInstall =
    laptopOperation?.step === "install" &&
    ["failed", "refused", "interrupted"].includes(laptopOperation.result?.status ?? "") &&
    !state.laptop.activationBlocked &&
    laptopState?.phase === "staged" &&
    laptopState.stagedHandle !== undefined &&
    laptopState.provenance?.sourceSha === plan.target.sourceSha &&
    laptopState.provenance.sourceTree === plan.target.sourceTree;
  const laptopPhase =
    plan.laptop.gateClosed !== undefined
      ? "gateClosed"
      : plan.laptop.gate !== undefined
        ? "verified"
        : laptopOperation?.result?.status === "committed"
          ? "committedGatePending"
          : laptopOperation?.result?.status === "completed"
            ? "staged"
            : laptopOperation?.result?.status === "running"
              ? laptopOperation.step === "install"
                ? "installing"
                : options.laptop?.freshness === "fresh" &&
                    options.laptop.state?.phase === "verifying"
                  ? "verifying"
                  : "downloading"
              : coldStage
                ? "staged"
                : (laptopOperation?.result?.status ?? "preview");
  const laptopCopy: Record<string, string> = {
    gateClosed: "Laptop changed · others won't start",
    verified: "Updated · verified",
    committedGatePending: "Checking after restart…",
    staged: "Downloaded · ready to install",
    installing: "Installing · restarting",
    downloading: "Downloading…",
    verifying: "Checking download · can't stop now",
    "stop-requested": "Stopping download…",
    stopped: "Download stopped · nothing installed",
    interrupted: "Download didn't finish · nothing installed",
    held: "Stop not confirmed",
    "rolled-back": `Rolled back · still on ${plan.from.version}`,
    blocked: "Didn't finish",
    failed: "Download didn't finish",
    refused: "Not updated",
    preview: `Goes first · ${plan.target.version} available`,
  };
  const complete = isPlanComplete(plan);
  const laptopAlreadyRunning =
    laptopPhase === "staged" && laptopState?.stagedAlreadyRunning === true;
  const laptopProgress = rowProgress(
    options.laptop?.freshness === "fresh" ? options.laptop : undefined,
    laptopOperation?.operationId,
    planless,
  );
  const laptopText =
    laptopOperation?.step === "download" && laptopPhase === "refused"
      ? "Download request did not start"
      : laptopOperation?.step === "install" && ["failed", "interrupted"].includes(laptopPhase)
        ? "Install didn't finish"
        : laptopAlreadyRunning
          ? ALREADY_RUNNING_TEXT
          : (laptopCopy[laptopPhase] ?? "Not confirmed");
  const laptopActions = {
    download:
      !readOnly &&
      ["preview", "stopped", "interrupted", "failed", "refused"].includes(laptopPhase) &&
      laptopState?.capability.download === true &&
      laptopState.stagedHandle === undefined &&
      laptopState.provenance?.sourceSha === plan.target.sourceSha,
    install:
      !readOnly &&
      (laptopPhase === "staged" || retryLaptopInstall) &&
      !laptopAlreadyRunning &&
      plan.laptop.gate === undefined &&
      plan.laptop.gateClosed === undefined &&
      laptopState?.capability.install === true &&
      laptopState.stagedHandle !== undefined &&
      laptopState.provenance?.sourceSha === plan.target.sourceSha,
    stop:
      !readOnly &&
      laptopPhase === "downloading" &&
      laptopState?.phase === "downloading" &&
      laptopState.capability.operations === 1,
    checkAgain: true,
  };
  const selected = rows.filter(
    (row) =>
      plan.devices.find((d) => d.environmentId === row.environmentId)?.membership === "selected",
  );
  return {
    plan,
    readOnly,
    rows,
    complete,
    headerDoneVisible: !readOnly && complete,
    laptop: {
      phase: laptopPhase,
      copyKey: `laptop.${laptopPhase}`,
      text:
        laptopProgress?.text !== undefined && laptopPhase !== "held"
          ? laptopProgress.text
          : laptopText,
      downloadLabel: retryLaptopDownload ? "Retry download" : "Download on Laptop",
      actions: laptopActions,
      ...(laptopProgress === undefined ? {} : { progress: laptopProgress }),
      ...(laptopOperation === undefined ? {} : { operationId: laptopOperation.operationId }),
    },
    gateText: "Others wait until Laptop is updated and verified.",
    summary: complete ? "Update plan finished" : planSummary(selected),
  };
}

/** Header summary such as "1 installing · 2 of 3 done". */
export function planSummary(
  rows: readonly { readonly settled: boolean; readonly activeWork: ActiveWork }[],
) {
  const installing = rows.filter((row) => row.activeWork === "install").length;
  const downloading = rows.filter((row) => row.activeWork === "download").length;
  return [
    ...(installing > 0 ? [`${installing} installing`] : []),
    ...(downloading > 0 ? [`${downloading} downloading`] : []),
    `${rows.filter((row) => row.settled).length} of ${rows.length} done`,
  ].join(" · ");
}

export function recoveryPreflightPresentation(preflight: JonesRecoveryPreflight, label: string) {
  return {
    text: preflight.firstInstall
      ? `First install on ${label} · no existing data to back up`
      : "Recovery covers the database, WAL, SHM and required settings.",
    clone: preflight.clone.status === "proved" && preflight.policies.cloneRequired.admissible,
    cloneOrCopy: preflight.policies.cloneOrCopy.admissible,
    reason: preflight.policies.cloneOrCopy.reason ?? preflight.clone.reason,
    stop: true,
  };
}
