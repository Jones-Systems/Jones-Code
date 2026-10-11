// @effect-diagnostics nodeBuiltinImport:off
// The detached launcher owns these receipts outside the rollback database.
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
  readonly state: "absent" | "pending" | "committed" | "rolled-back" | "blocked";
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

async function writeExclusive(file: string, value: unknown): Promise<void> {
  const directory = NodePath.dirname(file);
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  const parent = await NodeFSP.open(NodePath.dirname(directory), "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
  const handle = await NodeFSP.open(file, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(value)}\n`);
    await handle.sync();
  } finally {
    await handle.close();
  }
  const dir = await NodeFSP.open(directory, "r");
  try {
    await dir.sync();
  } finally {
    await dir.close();
  }
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

/** A reservation without matching native state is ambiguous and must never be resubmitted. */
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
  const file = receiptPath(baseDir, update.id, true);
  const outcome = {
    schema: 1,
    operationId: update.id,
    binding: reserved.binding,
    status: update.status,
  };
  const existing = await readJson(file);
  if (existing !== undefined) {
    if (JSON.stringify(existing) !== JSON.stringify(outcome))
      throw new Error("Update operation outcome conflicts with its durable receipt.");
    return;
  }
  await writeExclusive(file, outcome);
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
    const archived = (await readJson(receiptPath(baseDir, operationId, true))) as
      | {
          schema?: unknown;
          operationId?: unknown;
          binding?: NativeOperationBinding;
          status?: unknown;
        }
      | undefined;
    if (
      archived?.schema === 1 &&
      archived.operationId === operationId &&
      archived.binding !== undefined &&
      sameOperationBinding(binding, archived.binding) &&
      ["committed", "rolled-back", "failed"].includes(String(archived.status))
    ) {
      return {
        state:
          archived.status === "committed"
            ? "committed"
            : archived.status === "rolled-back"
              ? "rolled-back"
              : "blocked",
        operationId,
        binding: publicBinding(binding),
        updateId: operationId,
      };
    }
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

/** Refuse a new operation while any retained receipt lacks a proven terminal outcome. */
export async function assertNoUnreconciledUpdateOperations(
  baseDir: string,
  current: ServiceUpdateRecord | undefined,
): Promise<void> {
  baseDir = await NodeFSP.realpath(baseDir);
  let directory: Awaited<ReturnType<typeof NodeFSP.opendir>>;
  try {
    const path = NodePath.join(baseDir, "runtime", "jones-update-operations");
    if (!(await NodeFSP.lstat(path)).isDirectory())
      throw new Error("Invalid native receipt directory.");
    directory = await NodeFSP.opendir(path);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return;
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
  for (const id of reservations) {
    const receipt = await reconcileUpdateOperation(baseDir, id, current);
    if (receipt.state !== "committed" && receipt.state !== "rolled-back")
      throw new Error("A retained native operation requires reconciliation before another update.");
  }
}
