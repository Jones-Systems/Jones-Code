// @effect-diagnostics globalDate:off - Pure model parses recorded timestamps; scheduling time is supplied by the caller.
import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlan,
  JonesInstallPlanDevice,
  JonesInstallPlanCommandReceipt,
  JonesInstallPlanHostOperation,
  JonesInstallPlanOrigin,
  JonesInstallPlanSelection,
  JonesInstallPlanTarget,
  JonesInstallPlanResult,
} from "@t3tools/contracts/jones/installPlan";
import { JonesOperationReceipt as ReceiptSchema } from "@t3tools/contracts/jones/jonesOperations";
import type {
  JonesOperationAction,
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import type {
  FleetHostStatus,
  FleetPlanDirectiveFenceEvidence,
  FleetPlanDirectiveInput,
} from "@t3tools/contracts/jones/fleet-updates";
import * as Schema from "effect/Schema";
import type { JonesUpdateObservation } from "../updates/jonesUpdates.ts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";

const terminal = new Set<JonesOperationReceipt["phase"]>([
  "completed",
  "rolled-back",
  "stopped",
  "refused",
  "failed",
  "blocked",
  "fenced",
  "interrupted",
]);

/** Structural equality without depending on JSON object insertion order. */
function equal(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null)
    return false;
  const a = Object.keys(left).sort();
  const b = Object.keys(right).sort();
  return (
    a.length === b.length &&
    a.every(
      (key, i) =>
        key === b[i] &&
        equal((left as Record<string, unknown>)[key], (right as Record<string, unknown>)[key]),
    )
  );
}

const isReceipt = Schema.is(ReceiptSchema);

/** Validate against the original request first, then against the last accepted receipt. */
export function validateReceipt(
  prev: JonesOperationStartInput | JonesOperationReceipt,
  next: JonesOperationReceipt,
): boolean {
  if (!isReceipt(next)) return false;
  const updatedAt = Date.parse(next.updatedAt);
  if (!Number.isFinite(updatedAt)) return false;
  if (!["refused", "fenced"].includes(next.phase) && next.acceptedAt === undefined) return false;
  for (const key of [
    "operationId",
    "environmentId",
    "action",
    "expectedInstalledSource",
    "correlation",
  ] as const) {
    if (!equal(prev[key], next[key])) return false;
  }
  const action = next.action;
  if (
    next.staged !== undefined &&
    (next.staged.sourceSha !==
      (action.kind === "download" ? action.sourceSha : action.targetSource) ||
      (action.kind === "download" && next.staged.artifactId !== action.artifactId))
  )
    return false;
  if (next.phase === "fenced" && next.acceptedAt !== undefined) return false;
  if (next.phase === "completed") {
    if (action.kind === "install") {
      if (next.updateId !== next.operationId || next.outcome?.status !== "committed") return false;
    } else if (next.staged === undefined) return false;
  }
  if (
    next.phase === "rolled-back" &&
    (action.kind !== "install" || next.outcome?.status !== "rolled-back")
  )
    return false;
  if (
    next.updateId !== undefined &&
    (action.kind !== "install" || next.updateId !== next.operationId)
  )
    return false;
  if (!("phase" in prev)) return true;
  const previousUpdate = Date.parse(prev.updatedAt);
  if (updatedAt < previousUpdate) return false;
  // A stable field may first appear as work advances, but cannot be removed or rebound.
  for (const key of ["acceptedAt", "jobId", "staged", "updateId", "outcome", "recovery"] as const) {
    if (prev[key] !== undefined && !equal(prev[key], next[key])) {
      // Recovery's observed method can arrive after its requested policy.
      if (
        key !== "recovery" ||
        prev.recovery?.observed !== undefined ||
        prev.recovery?.requested !== next.recovery?.requested
      )
        return false;
    }
  }
  if (prev.stop !== undefined) {
    const stop = next.stop;
    if (
      stop === undefined ||
      prev.stop.stopId !== stop.stopId ||
      prev.stop.requestedAt !== stop.requestedAt ||
      (prev.stop.result !== undefined &&
        prev.stop.result !== "held" &&
        prev.stop.result !== stop.result)
    )
      return false;
  }
  if (terminal.has(prev.phase)) {
    const stopRequestedAt = Date.parse(next.stop?.requestedAt ?? "");
    const expectedStopResult =
      prev.phase === "completed" && prev.staged !== undefined ? "too-late" : "not-stoppable-phase";
    const lateStop =
      prev.stop === undefined &&
      next.stop?.result === expectedStopResult &&
      stopRequestedAt >= previousUpdate &&
      stopRequestedAt <= updatedAt;
    const retiredAt = Date.parse(next.stageRetiredAt ?? "");
    const stageRetired =
      prev.phase === "completed" &&
      prev.staged?.createdByOperation === true &&
      prev.stageRetiredAt === undefined &&
      retiredAt >= previousUpdate &&
      retiredAt <= updatedAt;
    if (!lateStop && !stageRetired) return equal(prev, next);
    // A read may observe both annotations at once. Ignore only the first valid addition of each.
    const finalEvidence = { ...next, updatedAt: prev.updatedAt };
    if (lateStop) delete finalEvidence.stop;
    if (stageRetired) delete finalEvidence.stageRetiredAt;
    return equal(prev, finalEvidence);
  }
  if (next.phase === "fenced") return false; // acceptance can never be erased by a fence
  if (terminal.has(next.phase)) return true;
  const later: Record<string, readonly string[]> = {
    accepted: ["accepted", "running", "stop-requested", "adopting", "handed-off", "held"],
    running: ["running", "stop-requested", "adopting", "handed-off", "held"],
    "stop-requested": ["stop-requested", "adopting", "held"],
    adopting: ["adopting", "handed-off", "held"],
    "handed-off": ["handed-off", "held"],
    held: ["held", "handed-off"],
  };
  return later[prev.phase]?.includes(next.phase) === true;
}

export function validateDirectiveEvidence(
  input: FleetPlanDirectiveInput,
  status: FleetHostStatus,
): boolean {
  const directive = status.follower?.planDirective;
  return (
    status.environmentId === input.environmentId &&
    status.planDirectiveProtocol === 1 &&
    status.enrollment?.enabled === true &&
    status.enrollment.enrollmentId === input.enrollmentId &&
    directive?.directiveId === input.directiveId &&
    directive.targetSource === input.targetSource &&
    directive.targetTree === input.targetTree &&
    directive.mode === "manual-review"
  );
}

export function validateDirectiveFenceEvidence(
  input: FleetPlanDirectiveInput,
  proof: FleetPlanDirectiveFenceEvidence,
): boolean {
  return (
    proof.protocol === 1 &&
    sameDirectiveBinding(input, proof.input) &&
    (proof.outcome !== "applied" ||
      (proof.directive.directiveId === input.directiveId &&
        proof.directive.targetSource === input.targetSource &&
        proof.directive.targetTree === input.targetTree &&
        proof.directive.mode === "manual-review" &&
        Number.isFinite(Date.parse(proof.directive.issuedAt))))
  );
}

export function sameDirectiveBinding(
  left: FleetPlanDirectiveInput,
  right: FleetPlanDirectiveInput,
) {
  return equal(left, right);
}

/** The V1 replay record can identify a remote receipt, but cannot recreate a dispatch body. */
export function orphanReceiptCheck(receipt: JonesInstallPlanCommandReceipt) {
  const { command, planId, operationId, bodyHash } = receipt;
  if (
    receipt.outcome !== "reserved" ||
    receipt.requestType !== "claim" ||
    planId === undefined ||
    command.planId !== planId ||
    !Number.isSafeInteger(command.expectedRevision) ||
    operationId === undefined ||
    bodyHash === undefined ||
    (command.action === "install") !== (command.stagedHandle !== undefined)
  )
    return undefined;
  return { command, planId, operationId, bodyHash };
}

/** A fresh host status still shows the predecessor required by this exact request. */
export function canReplayDirective(
  input: FleetPlanDirectiveInput,
  status: FleetHostStatus | null | undefined,
): boolean {
  return (
    status?.environmentId === input.environmentId &&
    status.planDirectiveProtocol === 1 &&
    status.enrollment?.enabled === true &&
    status.enrollment.environmentId === input.environmentId &&
    status.enrollment.enrollmentId === input.enrollmentId &&
    status.follower !== undefined &&
    (status.follower.planDirective?.directiveId ?? null) === input.expectedDirectiveId
  );
}

export type InstallPlanSelectability =
  | {
      readonly selectable: true;
      readonly route: JonesInstallPlanSelection["route"];
      readonly enrollmentId?: string;
    }
  | { readonly selectable: false; readonly reason: string };

export function supportsInstallPlanMutations(capability: JonesUpdateState["capability"]): boolean {
  return capability.operations === 1 && capability.download && capability.install;
}

export function selectability(
  observation: JonesUpdateObservation,
  fleetStatus?: FleetHostStatus | null,
): InstallPlanSelectability {
  const state = observation.state;
  if (observation.freshness !== "fresh" || state === null)
    return { selectable: false, reason: "Waiting for fresh update status." };
  if (!supportsInstallPlanMutations(state.capability) || state.installedSource === undefined)
    return {
      selectable: false,
      reason: "This host needs durable update operations and a verified installed source.",
    };
  if (state.environmentId === undefined)
    return { selectable: false, reason: "This host has no verified environment identity." };
  if (fleetStatus === undefined)
    return { selectable: false, reason: "Waiting for fleet enrollment status." };
  if (
    fleetStatus !== undefined &&
    fleetStatus !== null &&
    fleetStatus.environmentId !== state.environmentId
  )
    return { selectable: false, reason: "The host identity changed." };
  const enrollment = fleetStatus?.enrollment;
  if (enrollment?.enabled) {
    if (fleetStatus?.planDirectiveProtocol !== 1)
      return { selectable: false, reason: "This enrolled host needs plan directive support." };
    return {
      selectable: true,
      route: "follower-operations",
      enrollmentId: enrollment.enrollmentId,
    };
  }
  // A follower policy without fresh enrollment evidence cannot be treated as a manual host.
  if (state.policy !== undefined)
    return { selectable: false, reason: "Waiting for this following host's enrollment status." };
  return { selectable: true, route: "operations" };
}

export function startPlan(input: {
  readonly planId: string;
  readonly origin: JonesInstallPlanOrigin;
  readonly target: JonesInstallPlanTarget;
  readonly from: JonesInstallPlan["from"];
  readonly laptopEnvironmentId: EnvironmentId;
  readonly select: readonly JonesInstallPlanSelection[];
  readonly now: string;
}): JonesInstallPlan {
  if (
    input.select.length > 16 ||
    new Set(input.select.map((s) => s.environmentId)).size !== input.select.length ||
    input.select.some((s) => s.environmentId === input.laptopEnvironmentId)
  )
    throw new Error("Invalid install plan selection.");
  return {
    schema: 1,
    planId: input.planId,
    revision: 0,
    createdAt: input.now,
    origin: input.origin,
    target: input.target,
    from: input.from,
    laptop: { environmentId: input.laptopEnvironmentId, operations: [] },
    devices: input.select.map((selection) => ({
      ...selection,
      membership: "selected",
      addedAt: input.now,
      operations: [],
    })),
  };
}

export function deviceActiveOperations(device: JonesInstallPlanDevice) {
  return device.operations.slice(device.archivedOperationCount ?? 0);
}

export function deviceStagedReceipt(operations: readonly JonesInstallPlanHostOperation[]) {
  const receipt = operations.findLast(
    (operation) => operation.step === "download" && operation.receipt?.phase === "completed",
  )?.receipt;
  return receipt?.stageRetiredAt === undefined ? receipt : undefined;
}

/** Verifying a running build never releases host admission or settles its launcher receipt. */
export function canRecordRunningTarget(device: JonesInstallPlanDevice): boolean {
  return (
    !device.abandoning &&
    device.operations.every(
      (operation) =>
        !outstanding(operation) ||
        (operation.step === "install" &&
          operation.receipt?.phase === "running" &&
          operation.receipt.updateId === undefined &&
          operation.receipt.code === "launcher-unconfirmed"),
    )
  );
}

/** Selection edits never undo host effects or lose operation history. */
export function deviceSelectionLocked(plan: JonesInstallPlan, device: JonesInstallPlanDevice) {
  if (device.membership === "removed") return false;
  const operations = deviceActiveOperations(device);
  return (
    device.abandoning === true ||
    operations.some(
      (operation) => outstanding(operation) || operation.directive?.kind === "applied",
    ) ||
    ["blocked", "held", "unknown", "staged"].includes(deviceState(plan, device))
  );
}

export function applyEdit(
  plan: JonesInstallPlan,
  edit: {
    readonly expectedRevision: number;
    readonly select: readonly JonesInstallPlanSelection[];
    readonly deselect: readonly EnvironmentId[];
  },
  now: string,
): {
  plan: JonesInstallPlan;
  applied: NonNullable<JonesInstallPlanResult["applied"]>;
  refused?: "stale-revision" | "bounds";
} {
  if (
    edit.expectedRevision !== plan.revision ||
    plan.dismissedAt !== undefined ||
    plan.supersededAt !== undefined
  )
    return { plan, applied: [], refused: "stale-revision" };
  const ids = new Set([...edit.select.map((s) => s.environmentId), ...edit.deselect]);
  if (
    ids.size > 16 ||
    new Set([
      ...plan.devices.map((d) => d.environmentId),
      ...edit.select.map((s) => s.environmentId),
    ]).size > 16
  )
    return { plan, applied: [], refused: "bounds" };
  const devices = [...plan.devices];
  const applied: NonNullable<JonesInstallPlanResult["applied"]>[number][] = [];
  for (const id of ids) {
    const index = devices.findIndex((d) => d.environmentId === id);
    const old = devices[index];
    const selection = edit.select.find((s) => s.environmentId === id);
    if (
      id === plan.laptop.environmentId ||
      (selection !== undefined && edit.deselect.includes(id))
    ) {
      applied.push({ environmentId: id, result: "not-selectable", reason: "Invalid selection." });
    } else if (old !== undefined && deviceSelectionLocked(plan, old)) {
      applied.push({
        environmentId: id,
        result: selection === undefined ? "locked-started" : "unchanged",
      });
    } else if (selection !== undefined) {
      if (old?.membership === "selected") applied.push({ environmentId: id, result: "unchanged" });
      else {
        const next: JonesInstallPlanDevice =
          old === undefined
            ? { ...selection, membership: "selected", addedAt: now, operations: [] }
            : {
                ...selection,
                membership: "selected",
                addedAt: now,
                operations: old.operations,
                ...(old.archivedOperationCount === undefined
                  ? {}
                  : { archivedOperationCount: old.archivedOperationCount }),
              };
        if (index < 0) devices.push(next);
        else devices[index] = next;
        applied.push({ environmentId: id, result: "added" });
      }
    } else if (old?.membership === "selected") {
      devices[index] = { ...old, membership: "removed", removedAt: now };
      applied.push({ environmentId: id, result: "removed" });
    } else applied.push({ environmentId: id, result: "unchanged" });
  }
  const changed = applied.some((a) => a.result === "added" || a.result === "removed");
  return { plan: changed ? { ...plan, devices, revision: plan.revision + 1 } : plan, applied };
}

/** Whether a ledger operation still awaits its directive or a terminal receipt. */
export function operationOutstanding(operation: JonesInstallPlanHostOperation): boolean {
  return outstanding(operation);
}

export function hasUnconfirmedLauncherOutcome(receipt: JonesOperationReceipt | undefined): boolean {
  return receipt?.code === "launcher-unconfirmed" || receipt?.code === "launcher-unreadable";
}

export function isSettledOperationReceipt(receipt: JonesOperationReceipt): boolean {
  return terminal.has(receipt.phase) && !hasUnconfirmedLauncherOutcome(receipt);
}

function outstanding(operation: JonesInstallPlanHostOperation): boolean {
  return operation.step === "directive"
    ? operation.directive === undefined && operation.directiveFence === undefined
    : operation.receipt === undefined || !isSettledOperationReceipt(operation.receipt);
}

/** Recovery follows all retained work, even after visual completion or a membership change. */
export function deviceHasRecoveryDuty(device: JonesInstallPlanDevice): boolean {
  // Stop and fence can affect only their original operation. Its exact terminal receipt
  // releases that hold once there is no unconfirmed launcher outcome.
  return device.abandoning === true || device.operations.some(outstanding);
}

/** A paused rollout gate alone does not indicate outstanding native work. */
export function planHasLaptopRecoveryDuty(plan: JonesInstallPlan): boolean {
  return (
    (plan.laptop.stageAbandonment !== undefined &&
      plan.laptop.stageAbandonment.status !== "retired") ||
    plan.laptop.operations.some(
      (o) =>
        o.result === undefined ||
        o.result.code === "native-unconfirmed" ||
        ["running", "stop-requested", "held"].includes(o.result.status),
    )
  );
}

export function laptopSettled(plan: JonesInstallPlan): boolean {
  if (plan.laptop.gatePause === true || planHasLaptopRecoveryDuty(plan)) return false;
  if (plan.laptop.gate !== undefined && plan.laptop.gateClosed === undefined) return true;
  const latest = plan.laptop.operations.at(-1);
  return (
    latest?.result !== undefined &&
    ["stopped", "rolled-back", "blocked", "failed", "refused", "interrupted"].includes(
      latest.result.status,
    )
  );
}

export type InstallPlanDeviceState =
  | "removed"
  | "not-started"
  | "queued"
  | "directive"
  | "reserved"
  | "downloading"
  | "stop-requested"
  | "verifying"
  | "installing"
  | "staged"
  | "done"
  | "updated-elsewhere"
  | "stopped"
  | "ended"
  | "not-updated"
  | "rolled-back"
  | "blocked"
  | "held"
  | "unknown"
  | "fenced";

/** The attempt's ledger state, independently of recorded proof of a running build. */
export function deviceLedgerState(
  plan: JonesInstallPlan,
  device: JonesInstallPlanDevice,
  now: number = Number.POSITIVE_INFINITY,
): InstallPlanDeviceState {
  if (device.membership === "removed") return "removed";
  const operations = deviceActiveOperations(device);
  const pending = operations.find(outstanding);
  const latest = pending ?? operations.at(-1);
  if (latest !== undefined) {
    if (latest.step === "directive") {
      if (latest.directive?.kind === "refused" || latest.directiveFence !== undefined)
        return "not-updated";
      if (latest.directive === undefined)
        return Date.parse(latest.lease.leaseUntil) > now ? "directive" : "unknown";
    } else {
      const receipt = latest.receipt;
      if (receipt === undefined)
        return Date.parse(latest.lease.leaseUntil) > now ? "reserved" : "unknown";
      if (hasUnconfirmedLauncherOutcome(receipt)) return "unknown";
      if (receipt.phase === "held") return "held";
      if (receipt.phase === "fenced") return "fenced";
      if (receipt.phase === "stop-requested") return "stop-requested";
      if (["accepted", "running", "adopting", "handed-off"].includes(receipt.phase))
        return latest.step === "install"
          ? "installing"
          : receipt.phase === "adopting"
            ? "verifying"
            : "downloading";
      if (receipt.stageRetiredAt !== undefined) return "stopped";
      if (receipt.phase === "completed")
        return latest.step === "install" ? "done" : device.ended !== undefined ? "ended" : "staged";
      if (receipt.phase === "rolled-back") return "rolled-back";
      if (receipt.phase === "refused" && receipt.code === "already-installed")
        return "updated-elsewhere";
      if (receipt.phase === "blocked" && latest.step === "install") return "blocked";
      if (latest.step === "install") return device.ended !== undefined ? "ended" : "staged";
      return receipt.phase === "stopped" ? "stopped" : "not-updated";
    }
  }
  return operations.length === 0 &&
    laptopSettled(plan) &&
    (plan.laptop.gate === undefined || plan.laptop.gateClosed !== undefined)
    ? "not-started"
    : "queued";
}

export function deviceState(
  plan: JonesInstallPlan,
  device: JonesInstallPlanDevice,
  now: number = Number.POSITIVE_INFINITY,
): InstallPlanDeviceState {
  const ledger = deviceLedgerState(plan, device, now);
  return ledger !== "rolled-back" &&
    device.membership === "selected" &&
    device.verifiedElsewhere !== undefined &&
    canRecordRunningTarget(device)
    ? "updated-elsewhere"
    : ledger;
}

/**
 * Settled ledger states that fresh proof of the target running on the host overrides. A later
 * install by another operation can leave an older interrupted or staged download as the latest
 * record. Outstanding operations still require exact evidence before their receipts change.
 */
const SUPERSEDABLE_BY_RUNNING_TARGET: ReadonlySet<InstallPlanDeviceState> = new Set([
  "not-updated",
  "not-started",
  "queued",
  "stopped",
  "ended",
  "staged",
  "blocked",
  "rolled-back",
]);

/** The row state once fresh host status proves the plan target is the running source. */
export function deviceStateWithRunningTarget(
  plan: JonesInstallPlan,
  device: JonesInstallPlanDevice,
  targetRunning: boolean,
  now: number = Number.POSITIVE_INFINITY,
): InstallPlanDeviceState {
  const state = deviceState(plan, device, now);
  return targetRunning &&
    !device.operations.some(outstanding) &&
    SUPERSEDABLE_BY_RUNNING_TARGET.has(state)
    ? "updated-elsewhere"
    : state;
}

export function deviceSettled(plan: JonesInstallPlan, device: JonesInstallPlanDevice): boolean {
  if (device.abandoning) return false;
  if (device.verifiedElsewhere !== undefined && canRecordRunningTarget(device)) return true;
  if (device.operations.some(outstanding)) return false;
  return [
    "removed",
    "not-started",
    "done",
    "updated-elsewhere",
    "stopped",
    "ended",
    "not-updated",
    "rolled-back",
    "blocked",
  ].includes(deviceState(plan, device));
}

export function isPlanComplete(plan: JonesInstallPlan): boolean {
  return (
    plan.dismissedAt === undefined &&
    plan.supersededAt === undefined &&
    laptopSettled(plan) &&
    plan.devices.every((d) =>
      d.membership === "removed" ? !d.operations.some(outstanding) : deviceSettled(plan, d),
    )
  );
}

export function matchesPlanTarget(
  plan: JonesInstallPlan,
  device: JonesInstallPlanDevice,
  state: JonesUpdateState | null | undefined,
): boolean {
  return (
    state?.environmentId === device.environmentId &&
    state.installedSource?.repository === plan.target.repository &&
    state.installedSource.sha === plan.target.sourceSha &&
    state.installedSource.tree === plan.target.sourceTree
  );
}

/** Fresh proof can finish retained stages; completion itself remains ledger-only. */
export function planCompletion(
  plan: JonesInstallPlan,
  observations: ReadonlyMap<EnvironmentId, JonesUpdateObservation>,
  deviceRecovery = false,
) {
  const candidates = plan.devices.filter((device) => {
    const observation = observations.get(device.environmentId);
    return (
      device.membership === "selected" &&
      (deviceRecovery
        ? canRecordRunningTarget(device) &&
          device.verifiedElsewhere === undefined &&
          (deviceStateWithRunningTarget(plan, device, true) === "updated-elsewhere" ||
            deviceState(plan, device) === "fenced" ||
            device.operations.some(outstanding))
        : deviceState(plan, device) === "staged" && !device.operations.some(outstanding)) &&
      observation?.freshness === "fresh" &&
      (!deviceRecovery || observation.state?.currentVersion !== undefined) &&
      matchesPlanTarget(plan, device, observation.state)
    );
  });
  const completable = isPlanComplete({
    ...plan,
    devices: plan.devices.map((device) =>
      candidates.includes(device)
        ? {
            ...device,
            ...(deviceRecovery
              ? { verifiedElsewhere: { version: plan.target.version, observedAt: 0 } }
              : { ended: { at: plan.createdAt } }),
          }
        : device,
    ),
  });
  return { candidates, completable };
}

export function canSupersede(plan: JonesInstallPlan): boolean {
  return (
    !plan.laptop.operations.some((o) => o.step === "install") &&
    plan.devices.every((d) => d.operations.length === 0)
  );
}

export type InstallPlanRunnerStep =
  | { readonly kind: "wait"; readonly reason: string }
  | { readonly kind: "reservation"; readonly operation: JonesInstallPlanHostOperation }
  | { readonly kind: "read"; readonly operation: JonesInstallPlanHostOperation }
  | {
      readonly kind: "report-directive";
      readonly operationId: string;
      readonly status: FleetHostStatus;
    }
  | { readonly kind: "claim-fence"; readonly operationId: string }
  | {
      readonly kind: "claim-directive";
      readonly enrollmentId: string;
      readonly expectedDirectiveId: string | null;
    }
  | {
      readonly kind: "claim-download";
      readonly action: JonesOperationAction;
      readonly expectedInstalledSource: string;
    };

export function supportsOperationReceiptReads(capability: JonesUpdateState["capability"]): boolean {
  return capability.operations === 1 || capability.operationReceipts === 1;
}

/** Pure scheduling advice. Only the native coordinator can grant dispatch authority. */
export function nextRunnerStep(
  plan: JonesInstallPlan,
  device: JonesInstallPlanDevice,
  observation: JonesUpdateObservation,
  fleetStatus: FleetHostStatus | null | undefined,
  now: number,
): InstallPlanRunnerStep {
  const wait = (reason: string): InstallPlanRunnerStep => ({ kind: "wait", reason });
  if (
    plan.dismissedAt !== undefined ||
    plan.supersededAt !== undefined ||
    device.membership !== "selected" ||
    device.abandoning === true
  )
    return wait("Plan is inactive for this device.");
  const { state } = observation;
  if (observation.freshness !== "fresh" || state?.environmentId !== device.environmentId)
    return wait("Fresh matching update status is required.");
  const capability = state.capability;
  const mutationsSupported = supportsInstallPlanMutations(capability);
  const operations = deviceActiveOperations(device);
  const pending = operations.find(outstanding);
  if (
    pending?.request.kind === "operation-start" &&
    supportsOperationReceiptReads(capability) &&
    (pending.receipt !== undefined || !mutationsSupported)
  )
    return { kind: "read", operation: pending };
  if (
    pending?.step === "directive" &&
    pending.request.kind === "plan-directive" &&
    pending.request.input.environmentId === device.environmentId &&
    pending.request.input.directiveId === pending.operationId &&
    capability.operations === 1 &&
    fleetStatus != null &&
    validateDirectiveEvidence(pending.request.input, fleetStatus)
  )
    return { kind: "report-directive", operationId: pending.operationId, status: fleetStatus };
  if (!mutationsSupported) return wait("Fresh download and install capability is required.");
  if (pending !== undefined) {
    if (pending.step === "directive") {
      // Only an explicit recovery action on a closed plan can fence follower setup.
      return { kind: "reservation", operation: pending };
    }
    return Date.parse(pending.lease.leaseUntil) > now
      ? { kind: "reservation", operation: pending }
      : { kind: "claim-fence", operationId: pending.operationId };
  }
  if (plan.laptop.gate === undefined || plan.laptop.gateClosed !== undefined)
    return wait("Waits for Laptop.");
  if (device.ended !== undefined || device.verifiedElsewhere !== undefined)
    return wait("This device's update was ended.");
  if (matchesPlanTarget(plan, device, state))
    return wait("The selected source is already running.");
  const selectable = selectability(observation, fleetStatus);
  if (
    !selectable.selectable ||
    selectable.route !== device.route ||
    selectable.enrollmentId !== device.enrollmentId
  )
    return wait(selectable.selectable ? "The selected host route changed." : selectable.reason);
  const lastDownload = operations.findLast((o) => o.step === "download");
  if (lastDownload !== undefined && lastDownload.receipt?.phase !== "fenced")
    return wait("Download already attempted; another attempt needs an explicit action.");
  if (
    device.route === "follower-operations" &&
    state.installedSource?.sha !== plan.target.sourceSha &&
    !operations.some((o) => o.step === "directive" && o.directive?.kind === "applied")
  ) {
    if (operations.some((o) => o.step === "directive" && o.directive?.kind === "refused"))
      return wait("The host refused its directive.");
    if (
      fleetStatus == null ||
      device.enrollmentId === undefined ||
      fleetStatus.enrollment?.enrollmentId !== device.enrollmentId
    )
      return wait("Enrollment changed.");
    return {
      kind: "claim-directive",
      enrollmentId: device.enrollmentId,
      expectedDirectiveId: fleetStatus.follower?.planDirective?.directiveId ?? null,
    };
  }
  if (state.installedSource === undefined) return wait("The host cannot download now.");
  if (
    state.stagedHandle !== undefined ||
    ["downloading", "verifying", "preparing", "installing"].includes(state.phase)
  )
    return wait("The host has existing work or a stage to review.");
  const action: JonesOperationAction | undefined =
    state.capability.exactSource === true || state.installedSource.sha === plan.target.sourceSha
      ? { kind: "download-exact", targetSource: plan.target.sourceSha }
      : state.phase === "available" && state.provenance?.sourceSha === plan.target.sourceSha
        ? {
            kind: "download",
            artifactId: state.provenance.artifactId,
            sourceSha: plan.target.sourceSha,
          }
        : undefined;
  return action === undefined
    ? wait("This host cannot select the plan's source.")
    : { kind: "claim-download", action, expectedInstalledSource: state.installedSource.sha };
}
