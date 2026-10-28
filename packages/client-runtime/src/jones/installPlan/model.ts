// @effect-diagnostics globalDate:off - Pure model parses recorded timestamps; scheduling time is supplied by the caller.
import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlan,
  JonesInstallPlanDevice,
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
  FleetPlanDirectiveInput,
} from "@t3tools/contracts/jones/fleet-updates";
import * as Schema from "effect/Schema";
import type { JonesUpdateObservation } from "../updates/jonesUpdates.ts";

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

/** Validate against the original request first, then against the last accepted receipt. */
export function validateReceipt(
  prev: JonesOperationStartInput | JonesOperationReceipt,
  next: JonesOperationReceipt,
): boolean {
  if (!Schema.is(ReceiptSchema)(next)) return false;
  if (!Number.isFinite(Date.parse(next.updatedAt))) return false;
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
  if (
    !Number.isFinite(Date.parse(next.updatedAt)) ||
    Date.parse(next.updatedAt) < Date.parse(prev.updatedAt)
  )
    return false;
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
  if (terminal.has(prev.phase)) return equal(prev, next);
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

export type InstallPlanSelectability =
  | {
      readonly selectable: true;
      readonly route: JonesInstallPlanSelection["route"];
      readonly enrollmentId?: string;
    }
  | { readonly selectable: false; readonly reason: string };

export function selectability(
  observation: JonesUpdateObservation,
  fleetStatus?: FleetHostStatus | null,
): InstallPlanSelectability {
  const state = observation.state;
  if (observation.freshness !== "fresh" || state === null)
    return { selectable: false, reason: "Waiting for fresh update status." };
  if (
    state.capability.operations !== 1 ||
    !state.capability.download ||
    !state.capability.install ||
    state.installedSource === undefined
  )
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
    } else if (old !== undefined && old.operations.length > 0) {
      applied.push({
        environmentId: id,
        result: selection === undefined ? "locked-started" : "unchanged",
      });
    } else if (selection !== undefined) {
      if (old?.membership === "selected") applied.push({ environmentId: id, result: "unchanged" });
      else {
        const next: JonesInstallPlanDevice = {
          ...selection,
          membership: "selected",
          addedAt: now,
          operations: [],
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

function outstanding(operation: JonesInstallPlanHostOperation): boolean {
  return operation.step === "directive"
    ? operation.directive === undefined
    : operation.receipt === undefined || !terminal.has(operation.receipt.phase);
}

export function laptopSettled(plan: JonesInstallPlan): boolean {
  if (
    plan.laptop.operations.some(
      (o) =>
        o.result === undefined || ["running", "stop-requested", "held"].includes(o.result.status),
    )
  )
    return false;
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

export function deviceState(
  plan: JonesInstallPlan,
  device: JonesInstallPlanDevice,
  now: number = Number.POSITIVE_INFINITY,
): InstallPlanDeviceState {
  const pending = device.operations.find(outstanding);
  const latest = pending ?? device.operations.at(-1);
  if (latest !== undefined) {
    if (latest.step === "directive") {
      if (latest.directive?.kind === "refused") return "not-updated";
      if (latest.directive === undefined)
        return Date.parse(latest.lease.leaseUntil) > now ? "directive" : "unknown";
    } else {
      const receipt = latest.receipt;
      if (receipt === undefined)
        return Date.parse(latest.lease.leaseUntil) > now ? "reserved" : "unknown";
      if (receipt.code === "launcher-unconfirmed") return "unknown";
      if (receipt.phase === "held") return "held";
      if (receipt.phase === "fenced") return "fenced";
      if (receipt.phase === "stop-requested") return "stop-requested";
      if (["accepted", "running", "adopting", "handed-off"].includes(receipt.phase))
        return latest.step === "install"
          ? "installing"
          : receipt.phase === "adopting"
            ? "verifying"
            : "downloading";
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
  if (device.membership === "removed") return "removed";
  return device.operations.length === 0 &&
    laptopSettled(plan) &&
    (plan.laptop.gate === undefined || plan.laptop.gateClosed !== undefined)
    ? "not-started"
    : "queued";
}

export function deviceSettled(plan: JonesInstallPlan, device: JonesInstallPlanDevice): boolean {
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
    plan.devices.every(
      (d) =>
        !d.operations.some(outstanding) && (d.membership === "removed" || deviceSettled(plan, d)),
    )
  );
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
    device.membership !== "selected"
  )
    return wait("Plan is inactive for this device.");
  if (
    observation.freshness !== "fresh" ||
    observation.state?.environmentId !== device.environmentId ||
    observation.state.capability.operations !== 1
  )
    return wait("Fresh operation support is required.");
  const pending = device.operations.find(outstanding);
  if (pending !== undefined) {
    if (pending.receipt !== undefined) return { kind: "read", operation: pending };
    if (pending.step === "directive") {
      // A directive has no operation ledger fence; read its exact CAS result, never invent one.
      return { kind: "reservation", operation: pending };
    }
    return Date.parse(pending.lease.leaseUntil) > now
      ? { kind: "reservation", operation: pending }
      : { kind: "claim-fence", operationId: pending.operationId };
  }
  if (plan.laptop.gate === undefined || plan.laptop.gateClosed !== undefined)
    return wait("Waits for Laptop.");
  if (device.ended !== undefined) return wait("This device's update was ended.");
  const selectable = selectability(observation, fleetStatus);
  if (
    !selectable.selectable ||
    selectable.route !== device.route ||
    selectable.enrollmentId !== device.enrollmentId
  )
    return wait(selectable.selectable ? "The selected host route changed." : selectable.reason);
  const lastDownload = device.operations.filter((o) => o.step === "download").at(-1);
  if (lastDownload !== undefined && lastDownload.receipt?.phase !== "fenced")
    return wait("Download already attempted; another attempt needs an explicit action.");
  const state = observation.state;
  if (
    device.route === "follower-operations" &&
    state.installedSource?.sha !== plan.target.sourceSha &&
    !device.operations.some((o) => o.step === "directive" && o.directive?.kind === "applied")
  ) {
    if (device.operations.some((o) => o.step === "directive" && o.directive?.kind === "refused"))
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
  if (
    state.installedSource === undefined ||
    !state.capability.download ||
    !state.capability.install
  )
    return wait("The host cannot download now.");
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
