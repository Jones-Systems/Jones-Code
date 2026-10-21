// @effect-diagnostics nodeBuiltinImport:off
// The detached launcher owns these receipts outside the rollback database.
import * as NodeCrypto from "node:crypto";
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
async function writeExclusive(file: string, value: unknown): Promise<void> {
  const directory = NodePath.dirname(file);
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  await syncDirectory(NodePath.dirname(directory));
  // A sibling on the same file system: the receipt directory admits only receipts.
  const staging = stagingDirectory(directory);
  await NodeFSP.mkdir(staging, { recursive: true, mode: 0o700 });
  const temporary = NodePath.join(staging, NodeCrypto.randomUUID());
  try {
    const handle = await NodeFSP.open(temporary, "wx", 0o600);
    try {
      await handle.writeFile(`${JSON.stringify(value)}\n`);
      await handle.sync();
    } finally {
      await handle.close();
    }
    // Unlike rename, link never replaces a receipt that already exists.
    await NodeFSP.link(temporary, file);
  } finally {
    await NodeFSP.rm(temporary, { force: true });
  }
  await syncDirectory(directory);
}

export async function readOperationReservation(
  baseDir: string,
  operationId: string,
): Promise<OperationReservation | undefined> {
  baseDir = await NodeFSP.realpath(baseDir);
  const value = await readJson(receiptPath(baseDir, operationId));
  if (value === undefined) return undefined;
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
  const outcome = {
    schema: 1,
    operationId: update.id,
    binding: reserved.binding,
    status: update.status,
  };
  const existing = await readJson(receiptPath(baseDir, update.id, true));
  if (existing !== undefined) {
    if (JSON.stringify(existing) !== JSON.stringify(outcome))
      throw new Error("Update operation outcome conflicts with its durable receipt.");
    return;
  }
  await writeExclusive(receiptPath(baseDir, update.id, true), outcome);
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
      return {
        state: current.status === "failed" ? "blocked" : current.status,
        operationId,
        binding: publicBinding(binding),
        updateId: operationId,
      };
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
      return {
        state: outcome.status === "failed" ? "blocked" : outcome.status,
        operationId,
        binding: publicBinding(binding),
        updateId: operationId,
      };
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
    if (!["committed", "rolled-back", "rejected"].includes(receipt.state))
      throw new Error("A retained native operation requires reconciliation before another update.");
  }
}
