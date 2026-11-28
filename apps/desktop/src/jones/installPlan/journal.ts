import type { EnvironmentId } from "@t3tools/contracts";
import {
  JONES_INSTALL_PLAN_CLI_RESPONSE_BYTES,
  JONES_INSTALL_PLAN_HISTORY_LIMIT,
  type JonesInstallPlan,
  type JonesInstallPlanCommandReceipt,
  type JonesInstallPlanHostOperation,
  type JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import {
  deviceHasRecoveryDuty,
  isProvisionalUnsupportedReceipt,
  isSettledOperationReceipt,
  operationOutstanding,
  orphanReceiptCheck,
  planHasLaptopRecoveryDuty,
  validateOperationEvidence,
} from "@t3tools/client-runtime/jones/install-plan/model";
import { closedPlans, requiresV3Schema, retainedPlans, type PlanDocument } from "./store.ts";

const KiB = 1024;
const ADMISSION_BYTES = JONES_INSTALL_PLAN_CLI_RESPONSE_BYTES - 256 * KiB;
const RECEIPT_DETAILS_BYTES = 24 * KiB;
const RECOVERY_ANNOTATION_BYTES = 5 * KiB;
const CLOSURE_BYTES = 32 * KiB;

const bytes = (value: unknown) =>
  value === undefined ? 0 : Buffer.byteLength(JSON.stringify(value));

export const documentRevision = (document: PlanDocument) =>
  document.schema === 1 ? 0 : document.documentRevision;

const asRecoveryDocument = (document: PlanDocument) =>
  document.schema === 1
    ? { ...document, schema: 2 as const, documentRevision: 0, older: [] }
    : document;

export function commitRevision(previous: PlanDocument, next: PlanDocument): PlanDocument {
  if (next === previous) return previous;
  const recovery = asRecoveryDocument(next);
  // Older readers cannot preserve bound intent or proof of a durable legacy refusal.
  const schema =
    previous.schema === 3 || recovery.schema === 3 || requiresV3Schema(recovery) ? 3 : 2;
  return { ...recovery, schema, documentRevision: documentRevision(previous) + 1 };
}

export function archiveCurrent(document: PlanDocument, closed: JonesInstallPlan): PlanDocument {
  return {
    ...asRecoveryDocument(document),
    current: null,
    last: closed,
    older: closedPlans(document),
  };
}

export function replaceRetainedPlan(document: PlanDocument, plan: JonesInstallPlan): PlanDocument {
  if (document.current?.planId === plan.planId) return { ...document, current: plan };
  if (document.last?.planId === plan.planId) return { ...document, last: plan };
  if (document.schema === 1) return document;
  return {
    ...document,
    older: document.older.map((entry) => (entry.planId === plan.planId ? plan : entry)),
  };
}

function reservedForPlan(document: PlanDocument, planId: string) {
  return (document.commands ?? []).filter(
    ({ receipt }) => receipt.planId === planId && receipt.outcome === "reserved",
  );
}

/** Expose reservations that outlived their plan through V1 rotation or legacy eviction. */
export function orphanedPlanCommands(document: PlanDocument) {
  const retained = new Set(retainedPlans(document).map((plan) => plan.planId));
  return (document.commands ?? []).filter(
    ({ receipt }) =>
      receipt.outcome === "reserved" &&
      (receipt.planId === undefined || !retained.has(receipt.planId)),
  );
}

function matchesTerminalClaimReceipt(
  commandReceipt: JonesInstallPlanCommandReceipt,
  receipt: JonesOperationReceipt,
  input?: JonesOperationStartInput,
): boolean {
  const check = orphanReceiptCheck(commandReceipt);
  if (check === undefined) return false;
  const { command, planId, operationId } = check;
  const { action, correlation } = receipt;
  return (
    receipt.operationId === operationId &&
    receipt.environmentId === command.environmentId &&
    correlation?.planId === planId &&
    correlation.deviceKey === command.environmentId &&
    action.kind === command.action &&
    (action.kind === "download" ? action.sourceSha : action.targetSource) ===
      command.targetSource &&
    (action.kind === "install" ? action.stagedHandle : undefined) === command.stagedHandle &&
    isSettledOperationReceipt(receipt) &&
    // An evicted claim identifies a ledger read through its saved command metadata.
    validateOperationEvidence(
      input ?? {
        operationId,
        environmentId: command.environmentId,
        correlation,
        expectedInstalledSource: receipt.expectedInstalledSource,
        action,
      },
      receipt,
      { ledgerRead: true },
    )
  );
}

export function matchesOrphanReceipt(
  commandReceipt: JonesInstallPlanCommandReceipt,
  receipt: JonesOperationReceipt,
): boolean {
  return (
    matchesTerminalClaimReceipt(commandReceipt, receipt) && bytes(receipt) <= RECEIPT_DETAILS_BYTES
  );
}

export function unpersistedVerifiedUnsupportedReceipt(
  plan: JonesInstallPlan,
  entry: NonNullable<PlanDocument["commands"]>[number],
): JonesOperationReceipt | undefined {
  const { receipt: claim, body, orphanReceipt } = entry;
  const { command, planId, operationId, outcome, code, bodyHash } = claim;
  if (
    orphanReceipt !== undefined ||
    claim.requestType !== "claim" ||
    (outcome !== "reserved" && (outcome !== "terminal" || code !== "unsupported")) ||
    planId !== plan.planId ||
    command.targetSource !== plan.target.sourceSha ||
    (bodyHash !== undefined && bodyHash !== body)
  )
    return undefined;

  const device = plan.devices.find(({ environmentId }) => environmentId === command.environmentId);
  const operation = device?.operations.find((entry) => entry.operationId === operationId);
  if (
    operation?.request.kind !== "operation-start" ||
    operation.unsupportedReceiptVerified !== true ||
    operation.step !== (command.action === "install" ? "install" : "download") ||
    operation.review?.stagedHandle !== command.stagedHandle
  )
    return undefined;
  const { input } = operation.request;
  const { receipt } = operation;
  return receipt !== undefined &&
    isProvisionalUnsupportedReceipt(receipt) &&
    (input.admission?.kind !== "follower" ||
      input.admission.targetTree === plan.target.sourceTree) &&
    matchesTerminalClaimReceipt({ ...claim, bodyHash: body, outcome: "reserved" }, receipt, input)
    ? receipt
    : undefined;
}

export function recoveryHistory(
  document: PlanDocument,
): NonNullable<JonesInstallPlanState["recoveryHistory"]> {
  return closedPlans(document).flatMap((plan) => {
    const reserved = new Set(
      reservedForPlan(document, plan.planId).map(({ receipt }) => receipt.command.environmentId),
    );
    const pendingLaptop =
      planHasLaptopRecoveryDuty(plan) || reserved.has(plan.laptop.environmentId);
    const pendingDevices = plan.devices
      .filter((device) => deviceHasRecoveryDuty(device) || reserved.has(device.environmentId))
      .map(({ environmentId, label }) => ({ environmentId, label }));
    return pendingLaptop || pendingDevices.length > 0
      ? [
          {
            planId: plan.planId,
            revision: plan.revision,
            targetVersion: plan.target.version.slice(0, 128),
            closedAt: plan.dismissedAt ?? plan.supersededAt ?? plan.createdAt,
            pendingLaptop,
            pendingDevices,
          },
        ]
      : [];
  });
}

export function heldByClosedPlan(document: PlanDocument, environmentId: EnvironmentId): boolean {
  return (
    closedPlans(document).some((plan) =>
      plan.devices.some(
        (device) => device.environmentId === environmentId && deviceHasRecoveryDuty(device),
      ),
    ) ||
    document.commands?.some(
      ({ receipt }) =>
        receipt.outcome === "reserved" &&
        (document.current === null || receipt.planId !== document.current.planId) &&
        receipt.command.environmentId === environmentId,
    ) === true
  );
}

function protectedPlan(document: PlanDocument, plan: JonesInstallPlan): boolean {
  return (
    planHasLaptopRecoveryDuty(plan) ||
    plan.devices.some(deviceHasRecoveryDuty) ||
    document.commands?.some(
      (entry) =>
        (entry.receipt.planId === plan.planId && entry.receipt.outcome === "reserved") ||
        unpersistedVerifiedUnsupportedReceipt(plan, entry) !== undefined,
    ) === true
  );
}

// Receipt identity duplicates the intent. Supplementary strings from the host (other than the
// schema-bounded diagnostics) have fixed native forms. Grandfather exact legacy values so they can
// acquire later Stop, cleanup and launcher evidence without changing their identity.
export function receiptDetailsFit(
  prior: JonesOperationReceipt | undefined,
  receipt: JonesOperationReceipt,
): boolean {
  return [
    [prior?.acceptedAt, receipt.acceptedAt],
    [prior?.updatedAt, receipt.updatedAt],
    [prior?.stageRetiredAt, receipt.stageRetiredAt],
    [prior?.stop?.requestedAt, receipt.stop?.requestedAt],
    [prior?.staged?.stagedHandle, receipt.staged?.stagedHandle],
    [prior?.staged?.version, receipt.staged?.version],
    [prior?.outcome?.fromVersion, receipt.outcome?.fromVersion],
    [prior?.outcome?.targetVersion, receipt.outcome?.targetVersion],
  ].every(([old, next]) => old === next || bytes(next) <= KiB);
}

function operationRoom(operation: JonesInstallPlanHostOperation): number {
  if (operation.request.kind === "plan-directive")
    return operationOutstanding(operation) ? 8 * KiB : 0;
  // A terminal operation can still learn its exact late Stop or stage retirement annotation.
  if (!operationOutstanding(operation)) return RECOVERY_ANNOTATION_BYTES;
  // Reserve the repeated intent, including admission, in the eventual receipt.
  return (
    Math.max(0, bytes(operation.request.input) + RECEIPT_DETAILS_BYTES - bytes(operation.receipt)) +
    RECOVERY_ANNOTATION_BYTES
  );
}

function fitsAdmission(document: PlanDocument): boolean {
  let projected = bytes(document);
  for (const plan of retainedPlans(document)) {
    // Reserve the manual review (including repeated IDs), native gate/operation results and summary.
    if (plan === document.current)
      projected += CLOSURE_BYTES + bytes(plan.devices.map((device) => device.environmentId));
    if (planHasLaptopRecoveryDuty(plan)) projected += 16 * KiB;
    for (const device of plan.devices) {
      if (deviceHasRecoveryDuty(device) || plan === document.current)
        projected += bytes(device.environmentId) + KiB;
      for (const operation of device.operations) projected += operationRoom(operation);
    }
  }
  return projected <= ADMISSION_BYTES;
}

/** A closing plan already owns its slot. Only admission can remove settled, older detail. */
export function admitPlanWork(
  document: PlanDocument,
  evictSettled: boolean,
): PlanDocument | undefined {
  if (retainedPlans(document).length <= JONES_INSTALL_PLAN_HISTORY_LIMIT && fitsAdmission(document))
    return document;
  if (!evictSettled || document.schema === 1) return undefined;
  const index = document.older.findLastIndex((plan) => !protectedPlan(document, plan));
  return index < 0
    ? undefined
    : admitPlanWork({ ...document, older: document.older.filter((_, i) => i !== index) }, true);
}
