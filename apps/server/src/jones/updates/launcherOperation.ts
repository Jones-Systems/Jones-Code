// @effect-diagnostics nodeBuiltinImport:off globalDate:off - The detached launcher validates durable timestamps without an Effect runtime.
// The detached launcher owns these receipts outside the rollback database.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import type { ServiceUpdateRecord } from "../../cloud/serviceProtocol.ts";
import type { StagedQualifiedRuntime } from "../cloud/qualifiedRuntime.ts";

export interface OperationBinding {
  readonly environmentId: string;
  readonly currentVersion: string;
  readonly expectedInstalledSource: string;
  readonly targetSource: string;
  readonly stagedHandle: string;
}

export interface NativeOperationBinding extends OperationBinding {
  readonly baseDir: string;
  readonly dbPath: string;
  readonly targetVersion: string;
}

export interface OperationReconciliation {
  /** `rejected`: the launcher proved this reservation finished without acceptance. */
  readonly state: "absent" | "pending" | "committed" | "rolled-back" | "rejected" | "blocked";
  readonly operationId: string;
  readonly binding?: OperationBinding;
  readonly updateId?: string;
  readonly reason?: string;
  readonly settlement?: {
    readonly basis: UpdateSettlementBasis;
    readonly recordedAt: string;
  };
}

export type UpdateSettlementBasis = "launcher-recorded" | "legacy-reconciled" | "retired-exception";

interface OperationReceiptDigests {
  readonly reservationSha256: string;
  readonly outcomeSha256: string;
}

interface FailedSettlementEvidence {
  readonly phaseAtFailure: "accepted";
  readonly nativeReason: "launcher-restarted";
  readonly candidateStarted: false;
  readonly activeVersion: string;
  readonly activeSourceSha: string;
  readonly recoveryPayload: "absent" | "removed";
  readonly restorePending: false;
  readonly cause: string | null;
  readonly writerLauncherVersion: string;
  readonly recordedAt: string;
}

export type UpdateOperationSettlement = OperationReceiptDigests & {
  readonly schema: 1;
  readonly operationId: string;
  readonly status: "failed";
  readonly settlement: "no-effect";
} & (
    | {
        readonly basis: "launcher-recorded" | "legacy-reconciled";
        readonly evidence: FailedSettlementEvidence;
      }
    | {
        readonly basis: "retired-exception";
        readonly evidence: {
          readonly retiredLocation: string;
          readonly recordedAt: string;
          readonly writerLauncherVersion: string;
        };
      }
  );

export interface UpdateOperationCause {
  readonly schema: 1;
  readonly operationId: string;
  readonly step: "reverify" | "backup" | "ownership";
  readonly code: string;
  readonly message: string;
}

export class FailedOperationUnsettledError extends Error {
  readonly code = "failed-operation-unsettled";
  readonly operationId: string;
  constructor(operationId: string) {
    super("A failed native operation requires settlement before another update.");
    this.name = "FailedOperationUnsettledError";
    this.operationId = operationId;
  }
}

interface ReceiptPublicationOptions {
  readonly staging?: string;
  /** The launcher queue owns publication; this seam exercises its durable crash prefixes. */
  readonly onPhase?: (
    phase: "written" | "file-synced" | "linked" | "directory-synced",
  ) => Promise<void>;
}

interface OperationReservation {
  readonly schema: 1;
  readonly operationId: string;
  readonly binding: NativeOperationBinding;
}

interface OperationOutcome {
  readonly schema: 1;
  readonly operationId: string;
  readonly binding: NativeOperationBinding;
  readonly status: "committed" | "rolled-back" | "failed" | "rejected";
  readonly reason?: string;
}

/** How the launcher answers a server asking whether its handoff was accepted. */
export interface UpdateOperationOutcome {
  readonly outcome: "accepted" | "rejected" | "unknown";
  readonly reason?: string;
}

const ABANDONED = "The launcher finished this request without accepting it.";

export const isUpdateOperationId = (id: string): boolean =>
  /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(id);

export function operationBinding(staged: StagedQualifiedRuntime): NativeOperationBinding {
  return {
    environmentId: staged.binding.environmentId,
    currentVersion: staged.binding.activeVersion,
    expectedInstalledSource: staged.binding.activeSourceSha,
    targetSource: staged.receipt.sourceSha,
    stagedHandle: staged.stagedHandle,
    baseDir: staged.binding.baseDir,
    dbPath: staged.binding.dbPath,
    targetVersion: staged.receipt.version,
  };
}

export function sameOperationBinding(
  a: NativeOperationBinding,
  b: NativeOperationBinding,
): boolean {
  return (
    Object.keys(a).length === Object.keys(b).length &&
    (Object.keys(a) as Array<keyof NativeOperationBinding>).every((key) => a[key] === b[key])
  );
}

function publicBinding(binding: NativeOperationBinding): OperationBinding {
  return {
    environmentId: binding.environmentId,
    currentVersion: binding.currentVersion,
    expectedInstalledSource: binding.expectedInstalledSource,
    targetSource: binding.targetSource,
    stagedHandle: binding.stagedHandle,
  };
}

function receiptPath(baseDir: string, operationId: string, outcome = false): string {
  if (!isUpdateOperationId(operationId)) throw new Error("Update operation ID must be a UUID v4.");
  return NodePath.join(
    baseDir,
    "runtime",
    "jones-update-operations",
    `${operationId}${outcome ? ".outcome" : ""}.json`,
  );
}

async function readJson(file: string): Promise<unknown | undefined> {
  try {
    const stat = await NodeFSP.lstat(file);
    if (!stat.isFile() || stat.size > 64 * 1024)
      throw new Error("Invalid update operation receipt.");
    return JSON.parse(await NodeFSP.readFile(file, "utf8"));
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw cause;
  }
}

async function syncDirectory(directory: string): Promise<void> {
  const handle = await NodeFSP.open(directory, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

const stagingDirectory = (receipts: string) => `${receipts}.staging`;

/** Publishes a receipt once: a crash leaves no receipt or a complete one, never a torn one. */
async function writeExclusive(
  file: string,
  value: unknown,
  options?: ReceiptPublicationOptions,
): Promise<void> {
  const directory = NodePath.dirname(file);
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  await syncDirectory(NodePath.dirname(directory));
  // A sibling on the same file system: the receipt directory admits only receipts.
  const staging = options?.staging ?? stagingDirectory(directory);
  await NodeFSP.mkdir(staging, { recursive: true, mode: 0o700 });
  const temporary = NodePath.join(staging, NodeCrypto.randomUUID());
  try {
    const handle = await NodeFSP.open(temporary, "wx", 0o600);
    try {
      await handle.writeFile(`${JSON.stringify(value)}\n`);
      await options?.onPhase?.("written");
      await handle.sync();
      await options?.onPhase?.("file-synced");
    } finally {
      await handle.close();
    }
    // Unlike rename, link never replaces a receipt that already exists.
    await NodeFSP.link(temporary, file);
    await options?.onPhase?.("linked");
  } finally {
    await NodeFSP.rm(temporary, { force: true });
  }
  await syncDirectory(directory);
  await options?.onPhase?.("directory-synced");
}

export async function readOperationReservation(
  baseDir: string,
  operationId: string,
): Promise<OperationReservation | undefined> {
  baseDir = await NodeFSP.realpath(baseDir);
  const value = await readJson(receiptPath(baseDir, operationId));
  if (value === undefined) return undefined;
  return decodeReservation(value, baseDir, operationId);
}

function decodeReservation(
  value: unknown,
  baseDir: string,
  operationId: string,
): OperationReservation {
  if (typeof value !== "object" || value === null)
    throw new Error("Invalid update operation reservation.");
  const row = value as Partial<OperationReservation>;
  const keys = [
    "environmentId",
    "currentVersion",
    "expectedInstalledSource",
    "targetSource",
    "stagedHandle",
    "baseDir",
    "dbPath",
    "targetVersion",
  ] as const;
  if (
    row.schema !== 1 ||
    row.operationId !== operationId ||
    row.binding == null ||
    keys.some((key) => typeof row.binding![key] !== "string" || row.binding![key] === "") ||
    Object.keys(row.binding).length !== keys.length ||
    row.binding.baseDir !== baseDir
  )
    throw new Error("Invalid or mismatched update operation reservation.");
  return row as OperationReservation;
}

async function readOperationOutcome(
  baseDir: string,
  reserved: OperationReservation,
): Promise<OperationOutcome | undefined> {
  const value = await readJson(receiptPath(baseDir, reserved.operationId, true));
  if (value === undefined) return undefined;
  return decodeOutcome(value, reserved);
}

function decodeOutcome(value: unknown, reserved: OperationReservation): OperationOutcome {
  if (typeof value !== "object" || value === null)
    throw new Error("Invalid update operation outcome.");
  const row = value as Partial<OperationOutcome>;
  if (
    row.schema !== 1 ||
    row.operationId !== reserved.operationId ||
    typeof row.binding !== "object" ||
    row.binding === null ||
    !sameOperationBinding(reserved.binding, row.binding) ||
    !["committed", "rolled-back", "failed", "rejected"].includes(String(row.status)) ||
    (row.reason !== undefined && typeof row.reason !== "string")
  )
    throw new Error("Invalid or mismatched update operation outcome.");
  return row as OperationOutcome;
}

/**
 * Only the request that reserves an operation can accept it; a later request for the same
 * operation is refused. Until native state names it or an outcome is recorded, it is unresolved.
 */
export async function reserveUpdateOperation(
  baseDir: string,
  operationId: string,
  binding: NativeOperationBinding,
): Promise<void> {
  baseDir = await NodeFSP.realpath(baseDir);
  if (binding.baseDir !== baseDir)
    throw new Error("Update operation binding must use the canonical runtime root.");
  // A damaged or unverifiable tombstone must never make an old ID available again.
  if ((await readSettlementFile(baseDir, operationId)) !== undefined)
    throw Object.assign(new Error("Update operation ID already holds settlement evidence."), {
      code: "EEXIST",
    });
  await writeExclusive(receiptPath(baseDir, operationId), { schema: 1, operationId, binding });
}

export async function archiveUpdateOperation(
  baseDir: string,
  update: ServiceUpdateRecord | undefined,
): Promise<void> {
  if (update === undefined || update.status === "pending" || !isUpdateOperationId(update.id))
    return;
  baseDir = await NodeFSP.realpath(baseDir);
  const reserved = await readOperationReservation(baseDir, update.id);
  if (reserved === undefined) return;
  if (
    update.qualified === undefined ||
    !sameOperationBinding(reserved.binding, operationBinding(update.qualified))
  )
    throw new Error("Cannot archive an update with mismatched operation identity.");
  await archiveOutcome(baseDir, reserved, update.status);
}

async function archiveOutcome(
  baseDir: string,
  reserved: OperationReservation,
  status: OperationOutcome["status"],
): Promise<void> {
  // Preserve the original archive's property order and omission of reason, including on failure.
  const outcome: OperationOutcome = {
    schema: 1,
    operationId: reserved.operationId,
    binding: reserved.binding,
    status,
  };
  const file = receiptPath(baseDir, reserved.operationId, true);
  const existing = await readJson(file);
  if (existing !== undefined) {
    if (JSON.stringify(existing) !== JSON.stringify(outcome))
      throw new Error("Update operation outcome conflicts with its durable receipt.");
    return;
  }
  await writeExclusive(file, outcome);
}

/**
 * Records that a reserved operation finished without acceptance. Caller must hold the launcher
 * transition queue and know that no handler will still accept the operation. Repeating it is
 * harmless; it refuses an operation the native state names or one with another outcome.
 */
export async function rejectUpdateOperation(
  baseDir: string,
  operationId: string,
  current: ServiceUpdateRecord | undefined,
  reason: string,
): Promise<void> {
  baseDir = await NodeFSP.realpath(baseDir);
  if (current?.id === operationId) throw new Error("Native state names this update operation.");
  const reserved = await readOperationReservation(baseDir, operationId);
  if (reserved === undefined) throw new Error("The update operation has no reservation.");
  const existing = await readOperationOutcome(baseDir, reserved);
  if (existing !== undefined) {
    if (existing.status !== "rejected")
      throw new Error("Update operation outcome conflicts with its durable receipt.");
    return;
  }
  await writeExclusive(receiptPath(baseDir, operationId, true), {
    schema: 1,
    operationId,
    binding: reserved.binding,
    status: "rejected",
    reason,
  } satisfies OperationOutcome);
}

export async function reconcileUpdateOperation(
  baseDir: string,
  operationId: string,
  current: ServiceUpdateRecord | undefined,
): Promise<OperationReconciliation> {
  try {
    baseDir = await NodeFSP.realpath(baseDir);
    const reserved = await readOperationReservation(baseDir, operationId);
    if (reserved === undefined) {
      return current?.id === operationId
        ? {
            state: "blocked",
            operationId,
            reason: "Native state has no matching operation reservation.",
          }
        : { state: "absent", operationId };
    }
    const binding = reserved.binding;
    if (current?.id === operationId) {
      if (
        current.qualified === undefined ||
        !sameOperationBinding(binding, operationBinding(current.qualified))
      )
        throw new Error("Native state differs from the reserved update operation.");
      return attachSettlement(baseDir, {
        state: current.status === "failed" ? "blocked" : current.status,
        operationId,
        binding: publicBinding(binding),
        updateId: operationId,
      });
    }
    const outcome = await readOperationOutcome(baseDir, reserved);
    if (outcome?.status === "rejected")
      return {
        state: "rejected",
        operationId,
        binding: publicBinding(binding),
        reason: outcome.reason ?? ABANDONED,
      };
    if (outcome !== undefined)
      return attachSettlement(baseDir, {
        state: outcome.status === "failed" ? "blocked" : outcome.status,
        operationId,
        binding: publicBinding(binding),
        updateId: operationId,
      });
    return {
      state: "blocked",
      operationId,
      binding: publicBinding(binding),
      reason: "Reserved update has no proven native outcome; reconcile before retrying.",
    };
  } catch {
    return {
      state: "blocked",
      operationId,
      reason: "Update operation receipt is invalid or unreadable.",
    };
  }
}

async function listReceipts(
  baseDir: string,
): Promise<{ readonly reservations: Set<string>; readonly outcomes: Set<string> } | undefined> {
  let directory: Awaited<ReturnType<typeof NodeFSP.opendir>>;
  try {
    const path = NodePath.join(baseDir, "runtime", "jones-update-operations");
    if (!(await NodeFSP.lstat(path)).isDirectory())
      throw new Error("Invalid native receipt directory.");
    directory = await NodeFSP.opendir(path);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw cause;
  }
  const reservations = new Set<string>();
  const outcomes = new Set<string>();
  let count = 0;
  for await (const entry of directory) {
    if (++count > 4096) throw new Error("Native operation receipt inspection limit exceeded.");
    const match = /^([a-f0-9-]+)(\.outcome)?\.json$/.exec(entry.name);
    if (!entry.isFile() || match === null || !isUpdateOperationId(match[1]!))
      throw new Error("Unexpected native operation receipt entry.");
    (match[2] === undefined ? reservations : outcomes).add(match[1]!);
  }
  if ([...outcomes].some((id) => !reservations.has(id)))
    throw new Error("Native operation outcome has no reservation.");
  return { reservations, outcomes };
}

/**
 * Records every abandoned reservation as rejected. Caller must hold the launcher transition queue:
 * only the request that reserved an operation can accept it, so a reservation without an outcome
 * that native state does not name belongs to a request that ended, or a launcher that is gone,
 * without accepting it. It can never start.
 */
export async function settleAbandonedUpdateOperations(
  baseDir: string,
  current: ServiceUpdateRecord | undefined,
): Promise<void> {
  baseDir = await NodeFSP.realpath(baseDir);
  const receipts = NodePath.join(baseDir, "runtime", "jones-update-operations");
  // No receipt is being written while the caller holds the queue; drop a crashed write's leftovers.
  await NodeFSP.rm(stagingDirectory(receipts), { recursive: true, force: true });
  const listed = await listReceipts(baseDir);
  if (listed === undefined) return;
  for (const id of listed.reservations)
    if (!listed.outcomes.has(id) && current?.id !== id)
      await rejectUpdateOperation(baseDir, id, current, ABANDONED);
}

/**
 * The launcher's answer to its server about one handoff. Caller must hold the transition queue
 * after every earlier message from that server, so no earlier request for the operation is still
 * running: an operation without a reservation can no longer be accepted by any of them.
 */
export async function resolveUpdateOperation(
  baseDir: string,
  operationId: string,
  current: ServiceUpdateRecord | undefined,
): Promise<UpdateOperationOutcome> {
  if (current?.id === operationId) return { outcome: "accepted" };
  const receipt = await reconcileUpdateOperation(baseDir, operationId, current);
  if (receipt.state === "absent")
    return { outcome: "rejected", reason: "The launcher holds no reservation for this operation." };
  if (receipt.state === "rejected")
    return {
      outcome: "rejected",
      ...(receipt.reason === undefined ? {} : { reason: receipt.reason }),
    };
  if (receipt.updateId === operationId) return { outcome: "accepted" };
  if (receipt.binding === undefined)
    return {
      outcome: "unknown",
      ...(receipt.reason === undefined ? {} : { reason: receipt.reason }),
    };
  try {
    await rejectUpdateOperation(baseDir, operationId, current, ABANDONED);
    return { outcome: "rejected", reason: ABANDONED };
  } catch {
    return { outcome: "unknown", reason: "The abandoned reservation could not be recorded." };
  }
}

/** Refuse a new operation while any retained receipt lacks a proven terminal outcome. */
export async function assertNoUnreconciledUpdateOperations(
  baseDir: string,
  current: ServiceUpdateRecord | undefined,
): Promise<void> {
  baseDir = await NodeFSP.realpath(baseDir);
  const listed = await listReceipts(baseDir);
  if (listed === undefined) return;
  for (const id of listed.reservations) {
    const receipt = await reconcileUpdateOperation(baseDir, id, current);
    if (receipt.state === "blocked" && receipt.updateId === id && receipt.binding !== undefined) {
      if (receipt.settlement !== undefined) continue;
      // Native failure alone remains proved, but a contradictory/corrupt archive is a durability hold.
      const reserved = await readOperationReservation(baseDir, id);
      if (reserved === undefined) throw new Error("Update operation reservation disappeared.");
      const outcome = await readOperationOutcome(baseDir, reserved);
      if (outcome !== undefined && outcome.status !== "failed")
        throw new Error("Native failure conflicts with the durable update outcome.");
      throw new FailedOperationUnsettledError(id);
    }
    if (!["committed", "rolled-back", "rejected"].includes(receipt.state))
      throw new Error("A retained native operation requires reconciliation before another update.");
  }
}

const settlementDirectory = (baseDir: string) =>
  NodePath.join(baseDir, "runtime", "jones-update-settlements");
const HASH = /^[a-f0-9]{64}$/;
const SOURCE = /^[a-f0-9]{40}$/;
const VERSION =
  /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)-preview\.[0-9]+\.[0-9]+(?:\.[0-9]+)?$/;
const LAUNCHER_VERSION =
  /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;
const object = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
const closed = (row: Record<string, unknown>, keys: readonly string[]) =>
  Object.keys(row).length === keys.length && keys.every((key) => Object.hasOwn(row, key));
const version = (value: unknown): value is string =>
  typeof value === "string" && value.length <= 256 && VERSION.test(value);
const timestamp = (value: unknown): value is string =>
  typeof value === "string" &&
  /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(value) &&
  Number.isFinite(Date.parse(value)) &&
  new Date(value).toISOString() === value;

function settlementPath(baseDir: string, operationId: string, cause = false): string {
  if (!isUpdateOperationId(operationId)) throw new Error("Update operation ID must be a UUID v4.");
  return NodePath.join(
    settlementDirectory(baseDir),
    ...(cause ? ["causes"] : []),
    `${operationId}.json`,
  );
}

/** Read and hash the same bounded raw bytes; equivalent JSON is not an equivalent digest. */
async function readReceiptSnapshot(
  file: string,
  privateMode = false,
): Promise<{ readonly value: unknown; readonly sha256: string } | undefined> {
  let handle: Awaited<ReturnType<typeof NodeFSP.open>>;
  try {
    const stat = await NodeFSP.lstat(file);
    if (!stat.isFile()) throw new Error("Update evidence is not a regular file.");
    handle = await NodeFSP.open(file, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw cause;
  }
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.size > 64 * 1024 || (privateMode && (stat.mode & 0o777) !== 0o600))
      throw new Error("Invalid update evidence size or mode.");
    const bytes = Buffer.alloc(64 * 1024 + 1);
    let length = 0;
    while (length < bytes.length) {
      const next = await handle.read(bytes, length, bytes.length - length, length);
      if (next.bytesRead === 0) break;
      length += next.bytesRead;
    }
    if (length > 64 * 1024) throw new Error("Update evidence exceeds its byte limit.");
    const raw = bytes.subarray(0, length);
    return {
      value: JSON.parse(raw.toString("utf8")),
      sha256: NodeCrypto.createHash("sha256").update(raw).digest("hex"),
    };
  } finally {
    await handle.close();
  }
}

async function realDirectory(path: string, privateMode = false): Promise<boolean> {
  try {
    const stat = await NodeFSP.lstat(path);
    if (!stat.isDirectory() || (privateMode && (stat.mode & 0o777) !== 0o700))
      throw new Error("Invalid update evidence directory.");
    return true;
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw cause;
  }
}

async function readSettlementFile(baseDir: string, operationId: string, cause = false) {
  const file = settlementPath(baseDir, operationId, cause);
  if (
    !(await realDirectory(NodePath.join(baseDir, "runtime"))) ||
    !(await realDirectory(settlementDirectory(baseDir), true)) ||
    (cause && !(await realDirectory(NodePath.dirname(file), true)))
  )
    return undefined;
  await assertSettlementDevice(baseDir);
  return readReceiptSnapshot(file, true);
}

async function assertSettlementDevice(baseDir: string): Promise<void> {
  const directory = settlementDirectory(baseDir);
  const device = (await NodeFSP.stat(directory)).dev;
  const ledger = NodePath.join(baseDir, "runtime", "jones-update-operations");
  if ((await realDirectory(ledger)) && (await NodeFSP.stat(ledger)).dev !== device)
    throw new Error("Update settlement and ledger must share a filesystem.");
  for (const path of [stagingDirectory(directory), NodePath.join(directory, "causes")]) {
    if ((await realDirectory(path, true)) && (await NodeFSP.stat(path)).dev !== device)
      throw new Error("Update evidence publication must stay on one filesystem.");
  }
}

async function prepareSettlementDirectory(baseDir: string, cause = false): Promise<string> {
  const runtime = NodePath.join(baseDir, "runtime");
  if (!(await realDirectory(runtime))) await NodeFSP.mkdir(runtime, { mode: 0o700 });
  const directory = settlementDirectory(baseDir);
  if (!(await realDirectory(directory, true))) await NodeFSP.mkdir(directory, { mode: 0o700 });
  const staging = stagingDirectory(directory);
  if (!(await realDirectory(staging, true))) await NodeFSP.mkdir(staging, { mode: 0o700 });
  if (cause && !(await realDirectory(NodePath.join(directory, "causes"), true)))
    await NodeFSP.mkdir(NodePath.join(directory, "causes"), { mode: 0o700 });
  await assertSettlementDevice(baseDir);
  await syncDirectory(runtime);
  return staging;
}

async function syncReceipt(
  file: string,
  options?: Pick<ReceiptPublicationOptions, "onPhase">,
): Promise<void> {
  const handle = await NodeFSP.open(file, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    await handle.sync();
    await options?.onPhase?.("file-synced");
  } finally {
    await handle.close();
  }
  await syncDirectory(NodePath.dirname(file));
  await options?.onPhase?.("directory-synced");
}

function decodeSettlement(value: unknown, operationId: string): UpdateOperationSettlement {
  const row = object(value);
  const evidence = object(row?.evidence);
  if (
    row === undefined ||
    evidence === undefined ||
    !closed(row, [
      "schema",
      "operationId",
      "basis",
      "status",
      "settlement",
      "reservationSha256",
      "outcomeSha256",
      "evidence",
    ]) ||
    row.schema !== 1 ||
    row.operationId !== operationId ||
    !isUpdateOperationId(operationId) ||
    row.status !== "failed" ||
    row.settlement !== "no-effect" ||
    typeof row.reservationSha256 !== "string" ||
    !HASH.test(row.reservationSha256) ||
    typeof row.outcomeSha256 !== "string" ||
    !HASH.test(row.outcomeSha256) ||
    !timestamp(evidence.recordedAt) ||
    typeof evidence.writerLauncherVersion !== "string" ||
    evidence.writerLauncherVersion.length > 256 ||
    !LAUNCHER_VERSION.test(evidence.writerLauncherVersion)
  )
    throw new Error("Invalid update settlement receipt.");
  if (row.basis === "retired-exception") {
    if (
      !closed(evidence, ["retiredLocation", "recordedAt", "writerLauncherVersion"]) ||
      !retiredParts(evidence.retiredLocation)
    )
      throw new Error("Invalid retired update evidence.");
  } else if (
    (row.basis !== "launcher-recorded" && row.basis !== "legacy-reconciled") ||
    !closed(evidence, [
      "phaseAtFailure",
      "nativeReason",
      "candidateStarted",
      "activeVersion",
      "activeSourceSha",
      "recoveryPayload",
      "restorePending",
      "cause",
      "writerLauncherVersion",
      "recordedAt",
    ]) ||
    evidence.phaseAtFailure !== "accepted" ||
    evidence.nativeReason !== "launcher-restarted" ||
    evidence.candidateStarted !== false ||
    evidence.restorePending !== false ||
    !version(evidence.activeVersion) ||
    typeof evidence.activeSourceSha !== "string" ||
    !SOURCE.test(evidence.activeSourceSha) ||
    (evidence.recoveryPayload !== "absent" && evidence.recoveryPayload !== "removed") ||
    (evidence.cause !== null &&
      (typeof evidence.cause !== "string" || redactCauseMessage(evidence.cause) !== evidence.cause))
  )
    throw new Error("Invalid failed update settlement evidence.");
  return row as unknown as UpdateOperationSettlement;
}

async function failedEvidence(baseDir: string, operationId: string, directory?: string) {
  if (
    !(await realDirectory(NodePath.join(baseDir, "runtime"))) ||
    (directory === undefined &&
      !(await realDirectory(NodePath.dirname(receiptPath(baseDir, operationId)))))
  )
    throw new Error("Missing failed update evidence directory.");
  const reservationFile =
    directory === undefined
      ? receiptPath(baseDir, operationId)
      : NodePath.join(directory, `${operationId}.json`);
  const outcomeFile =
    directory === undefined
      ? receiptPath(baseDir, operationId, true)
      : NodePath.join(directory, `${operationId}.outcome.json`);
  const reservation = await readReceiptSnapshot(reservationFile);
  const outcome = await readReceiptSnapshot(outcomeFile);
  if (reservation === undefined || outcome === undefined)
    throw new Error("Failed update evidence is incomplete.");
  const reserved = decodeReservation(reservation.value, baseDir, operationId);
  if (decodeOutcome(outcome.value, reserved).status !== "failed")
    throw new Error("Update evidence is not failed.");
  return { reserved, reservationSha256: reservation.sha256, outcomeSha256: outcome.sha256 };
}

/** Restart recovery calls this before cleanup while native state still durably says accepted. */
export async function writeFailedUpdateOperation(
  baseDir: string,
  update: Pick<ServiceUpdateRecord, "id" | "qualified">,
): Promise<OperationReceiptDigests> {
  baseDir = await NodeFSP.realpath(baseDir);
  const reserved = await readOperationReservation(baseDir, update.id);
  if (
    reserved === undefined ||
    update.qualified === undefined ||
    !sameOperationBinding(reserved.binding, operationBinding(update.qualified))
  )
    throw new Error("Cannot record a failure with mismatched operation identity.");
  await archiveOutcome(baseDir, reserved, "failed");
  await syncReceipt(receiptPath(baseDir, update.id, true));
  const { reservationSha256, outcomeSha256 } = await failedEvidence(baseDir, update.id);
  return { reservationSha256, outcomeSha256 };
}

function retiredParts(value: unknown): string[] | undefined {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > 1024 ||
    NodePath.isAbsolute(value) ||
    // oxlint-disable-next-line no-control-regex -- Retained evidence paths must exclude control bytes.
    /[\\\x00-\x1f\x7f]/.test(value)
  )
    return undefined;
  const parts = value.split("/");
  if (
    parts.some((part) => part === "" || part === "." || part === "..") ||
    [
      "jones-update-operations",
      "jones-update-operations.staging",
      "jones-update-settlements",
      "jones-update-settlements.staging",
    ].includes(parts[0]!)
  )
    return undefined;
  return parts;
}

/** Retired directories retain the original UUID reservation and UUID.outcome filenames. */
export async function verifyRetiredUpdateOperation(input: {
  readonly baseDir: string;
  readonly operationId: string;
  readonly retiredLocation: string;
}): Promise<
  | ({ readonly result: "verified"; readonly retiredLocation: string } & OperationReceiptDigests)
  | { readonly result: "refused"; readonly reason: "retired-evidence-mismatch" }
> {
  try {
    const baseDir = await NodeFSP.realpath(input.baseDir);
    const parts = retiredParts(input.retiredLocation);
    if (parts === undefined || !isUpdateOperationId(input.operationId))
      throw new Error("Invalid retired path.");
    if (!(await realDirectory(NodePath.join(baseDir, "runtime"))))
      throw new Error("Missing runtime evidence directory.");
    await realDirectory(NodePath.dirname(receiptPath(baseDir, input.operationId)));
    if (
      (await readReceiptSnapshot(receiptPath(baseDir, input.operationId))) !== undefined ||
      (await readReceiptSnapshot(receiptPath(baseDir, input.operationId, true))) !== undefined
    )
      throw new Error("Retired evidence still has a live ledger entry.");
    let directory = NodePath.join(baseDir, "runtime");
    if (!(await realDirectory(directory))) throw new Error("Missing runtime evidence directory.");
    for (const part of parts) {
      directory = NodePath.join(directory, part);
      if (!(await realDirectory(directory))) throw new Error("Missing retired evidence directory.");
    }
    const { reservationSha256, outcomeSha256 } = await failedEvidence(
      baseDir,
      input.operationId,
      directory,
    );
    return {
      result: "verified",
      retiredLocation: input.retiredLocation,
      reservationSha256,
      outcomeSha256,
    };
  } catch {
    return { result: "refused", reason: "retired-evidence-mismatch" };
  }
}

async function validateSettlement(
  baseDir: string,
  receipt: UpdateOperationSettlement,
): Promise<void> {
  const evidence =
    receipt.basis === "retired-exception"
      ? await verifyRetiredUpdateOperation({
          baseDir,
          operationId: receipt.operationId,
          retiredLocation: receipt.evidence.retiredLocation,
        })
      : await failedEvidence(baseDir, receipt.operationId);
  if (
    ("result" in evidence && evidence.result !== "verified") ||
    !("reservationSha256" in evidence) ||
    evidence.reservationSha256 !== receipt.reservationSha256 ||
    evidence.outcomeSha256 !== receipt.outcomeSha256
  )
    throw new Error("Update settlement digests differ from retained evidence.");
  if (
    receipt.basis !== "retired-exception" &&
    "reserved" in evidence &&
    (receipt.evidence.activeVersion !== evidence.reserved.binding.currentVersion ||
      receipt.evidence.activeSourceSha !== evidence.reserved.binding.expectedInstalledSource)
  )
    throw new Error("Update settlement active identity differs from its reservation.");
}

export async function readUpdateOperationSettlement(
  baseDir: string,
  operationId: string,
): Promise<UpdateOperationSettlement | undefined> {
  baseDir = await NodeFSP.realpath(baseDir);
  const raw = await readSettlementFile(baseDir, operationId);
  if (raw === undefined) return undefined;
  const receipt = decodeSettlement(raw.value, operationId);
  await validateSettlement(baseDir, receipt);
  return receipt;
}

/** Caller proves the pre-trial cleanup and holds the launcher's transition/ownership scope. */
export async function publishUpdateOperationSettlement(
  baseDir: string,
  input: UpdateOperationSettlement,
  options?: Pick<ReceiptPublicationOptions, "onPhase">,
): Promise<{
  readonly result: "settled" | "already-settled";
  readonly receipt: UpdateOperationSettlement;
}> {
  baseDir = await NodeFSP.realpath(baseDir);
  const receipt = decodeSettlement(input, input.operationId);
  await validateSettlement(baseDir, receipt);
  const file = settlementPath(baseDir, receipt.operationId);
  const existing = await readUpdateOperationSettlement(baseDir, receipt.operationId);
  if (existing !== undefined) {
    if (
      existing.basis !== receipt.basis ||
      existing.reservationSha256 !== receipt.reservationSha256 ||
      existing.outcomeSha256 !== receipt.outcomeSha256
    )
      throw new Error("Update settlement conflicts with its durable receipt.");
    await syncReceipt(file, options);
    return { result: "already-settled", receipt: existing };
  }
  const staging = await prepareSettlementDirectory(baseDir);
  await writeExclusive(file, receipt, { ...options, staging });
  return { result: "settled", receipt };
}

async function attachSettlement(
  baseDir: string,
  receipt: OperationReconciliation,
): Promise<OperationReconciliation> {
  if (receipt.state !== "blocked" || receipt.updateId !== receipt.operationId) return receipt;
  try {
    const settlement = await readUpdateOperationSettlement(baseDir, receipt.operationId);
    return settlement === undefined
      ? receipt
      : {
          ...receipt,
          settlement: { basis: settlement.basis, recordedAt: settlement.evidence.recordedAt },
        };
  } catch {
    // Invalid supplemental evidence cannot erase independently proved native acceptance/failure.
    return receipt;
  }
}

function redactCauseMessage(input: string): string {
  const redacted = input
    .slice(0, 4096)
    // oxlint-disable-next-line no-control-regex -- Persisted causes must not contain terminal control bytes.
    .replace(/[\x00-\x1f\x7f]/g, " ")
    .replace(/\b[A-Za-z]:[\\/][^\s"'<>]*/g, "[path]")
    .replace(/(^|[\s("'=])(?:\/|\\\\|\.{1,2}[\\/]|~[\\/])[^\s"'<>]*/g, "$1[path]");
  let result = "";
  let bytes = 0;
  for (const character of redacted) {
    bytes += Buffer.byteLength(character);
    if (bytes > 512) break;
    result += character;
  }
  return result;
}

function decodeCause(value: unknown, operationId: string): UpdateOperationCause {
  const row = object(value);
  if (
    row === undefined ||
    !closed(row, ["schema", "operationId", "step", "code", "message"]) ||
    row.schema !== 1 ||
    row.operationId !== operationId ||
    !isUpdateOperationId(operationId) ||
    !["reverify", "backup", "ownership"].includes(String(row.step)) ||
    typeof row.code !== "string" ||
    !/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(row.code) ||
    typeof row.message !== "string" ||
    redactCauseMessage(row.message) !== row.message
  )
    throw new Error("Invalid update cause record.");
  return row as unknown as UpdateOperationCause;
}

export async function readUpdateOperationCause(
  baseDir: string,
  operationId: string,
): Promise<UpdateOperationCause | undefined> {
  baseDir = await NodeFSP.realpath(baseDir);
  const raw = await readSettlementFile(baseDir, operationId, true);
  return raw === undefined ? undefined : decodeCause(raw.value, operationId);
}

export async function writeUpdateOperationCause(
  baseDir: string,
  input: UpdateOperationCause,
): Promise<void> {
  baseDir = await NodeFSP.realpath(baseDir);
  const cause = decodeCause(
    { ...input, message: redactCauseMessage(input.message) },
    input.operationId,
  );
  const file = settlementPath(baseDir, cause.operationId, true);
  const existing = await readUpdateOperationCause(baseDir, cause.operationId);
  if (existing !== undefined) {
    if (JSON.stringify(existing) !== JSON.stringify(cause))
      throw new Error("Update cause conflicts with its durable receipt.");
    await syncReceipt(file);
    return;
  }
  const staging = await prepareSettlementDirectory(baseDir, true);
  await writeExclusive(file, cause, { staging });
}

type LegacySettlementRefusal =
  | "not-failed"
  | "not-found"
  | "insufficient-evidence"
  | "restore-pending"
  | "active-changed"
  | "trial-evidence-present";

export async function evaluateLegacyUpdateSettlement(input: {
  readonly baseDir: string;
  readonly operationId: string;
  readonly current: ServiceUpdateRecord | undefined;
  readonly active: { readonly activeVersion: string; readonly activeSourceSha: string };
}): Promise<
  | {
      readonly result: "settle";
      readonly binding: NativeOperationBinding;
      readonly current: ServiceUpdateRecord & { readonly qualified: StagedQualifiedRuntime };
    }
  | { readonly result: "refused"; readonly reason: LegacySettlementRefusal }
> {
  const refuse = (reason: LegacySettlementRefusal) => ({ result: "refused" as const, reason });
  try {
    const baseDir = await NodeFSP.realpath(input.baseDir);
    if (!(await realDirectory(NodePath.join(baseDir, "runtime")))) return refuse("not-found");
    if (!(await realDirectory(NodePath.dirname(receiptPath(baseDir, input.operationId)))))
      return refuse("not-found");
    const reserved = await readOperationReservation(baseDir, input.operationId);
    if (reserved === undefined) return refuse("not-found");
    const current = input.current;
    if (current === undefined || current.id !== input.operationId)
      return refuse("insufficient-evidence");
    if (current.status !== "failed") return refuse("not-failed");
    if (
      current.reason !== "launcher-restarted" ||
      current.qualified === undefined ||
      !sameOperationBinding(reserved.binding, operationBinding(current.qualified))
    )
      return refuse("insufficient-evidence");
    const outcome = await readOperationOutcome(baseDir, reserved);
    if (outcome !== undefined && outcome.status !== "failed")
      return refuse("insufficient-evidence");
    if (
      input.active.activeVersion !== reserved.binding.currentVersion ||
      input.active.activeSourceSha !== reserved.binding.expectedInstalledSource
    )
      return refuse("active-changed");
    if (
      current.startupReceipt !== undefined ||
      ("phase" in current && current.phase === "trial-ready")
    )
      return refuse("trial-evidence-present");
    const backups = NodePath.join(baseDir, "runtime", "db-backup");
    if (await realDirectory(backups)) {
      const directory = NodePath.join(backups, input.operationId);
      if (await realDirectory(directory)) {
        let count = 0;
        let trial = false;
        for await (const entry of await NodeFSP.opendir(directory)) {
          if (++count > 4096) return refuse("insufficient-evidence");
          if (
            entry.name === ".restore-pending" ||
            /^\..+-restore\.json(?:\.complete)?$/.test(entry.name)
          )
            return refuse("restore-pending");
          if (
            entry.name === "advanced-state" ||
            entry.name === "resume-dispatched.json" ||
            entry.name.startsWith(".resume-dispatched.")
          )
            trial = true;
        }
        if (trial) return refuse("trial-evidence-present");
      }
    }
    return {
      result: "settle",
      binding: reserved.binding,
      current: { ...current, qualified: current.qualified },
    };
  } catch {
    return refuse("insufficient-evidence");
  }
}
