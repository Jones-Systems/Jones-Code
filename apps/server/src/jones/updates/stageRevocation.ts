// @effect-diagnostics nodeBuiltinImport:off
// Only the launcher acceptance queue publishes revocations or retires stage pointers.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import type { ServiceUpdateRecord, ServiceUpdateRetirement } from "../../cloud/serviceProtocol.ts";
import { verifyStagedQualifiedRuntime } from "../cloud/qualifiedRuntime.ts";
import { retireStagedSelection, restoreStagedSelection } from "./stagedSelection.ts";
import {
  assertNoUnreconciledUpdateOperations,
  isUpdateOperationId,
  operationBinding,
  reconcileUpdateOperation,
  sameOperationBinding,
  type NativeOperationBinding,
} from "./launcherOperation.ts";

interface Revocation {
  readonly schema: 1;
  readonly operationId: string;
  readonly binding: NativeOperationBinding;
}
const bindingKeys = [
  "environmentId",
  "currentVersion",
  "expectedInstalledSource",
  "targetSource",
  "stagedHandle",
  "baseDir",
  "dbPath",
  "targetVersion",
] as const;
const directoryPath = (baseDir: string) =>
  NodePath.join(baseDir, "runtime", "jones-update-revocations");

async function readRevocations(baseDir: string): Promise<ReadonlyArray<Revocation>> {
  const path = directoryPath(baseDir);
  let entries: string[];
  try {
    if (!(await NodeFSP.lstat(path)).isDirectory())
      throw new Error("Invalid stage revocation directory.");
    entries = await NodeFSP.readdir(path);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw cause;
  }
  if (entries.length > 4096) throw new Error("Stage revocation inspection limit exceeded.");
  const records: Revocation[] = [];
  for (const name of entries) {
    const id = String(name).replace(/\.json$/, "");
    if (name !== `${id}.json` || !isUpdateOperationId(id))
      throw new Error("Unknown stage revocation entry.");
    const file = NodePath.join(path, String(name));
    const stat = await NodeFSP.lstat(file);
    if (
      !stat.isFile() ||
      stat.size > 65536 ||
      stat.uid !== NodeOS.userInfo().uid ||
      (stat.mode & 0o077) !== 0
    )
      throw new Error("Invalid stage revocation file.");
    const row = JSON.parse(await NodeFSP.readFile(file, "utf8")) as Revocation;
    if (
      row?.schema !== 1 ||
      row.operationId !== id ||
      row.binding == null ||
      Object.keys(row.binding).length !== bindingKeys.length ||
      bindingKeys.some((key) => typeof row.binding[key] !== "string" || row.binding[key] === "") ||
      row.binding.baseDir !== baseDir
    )
      throw new Error("Stage revocation binding is unknown; acceptance held.");
    records.push(row);
  }
  return records;
}

/** Retired payloads remain on disk but can never authorize another delayed install. */
export async function assertStageNotRevoked(
  baseDir: string,
  stagedHandle: string,
  operationId?: string,
): Promise<void> {
  baseDir = await NodeFSP.realpath(baseDir);
  if (
    (await readRevocations(baseDir)).some(
      (row) => row.binding.stagedHandle === stagedHandle || row.operationId === operationId,
    )
  )
    throw new Error(
      "The staged handle was durably retired; stage a new selection before installing.",
    );
}

/** Caller must hold the same launcher transition queue used for native acceptance. */
export async function retireNativeStage(
  baseDir: string,
  activeVersion: string,
  current: ServiceUpdateRecord | undefined,
  input: ServiceUpdateRetirement,
): Promise<void> {
  baseDir = await NodeFSP.realpath(baseDir);
  if (!isUpdateOperationId(input.operationId) || input.currentVersion !== activeVersion)
    throw new Error("Retirement does not match the active operation binding.");
  if ((await reconcileUpdateOperation(baseDir, input.operationId, current)).state !== "absent")
    throw new Error("The operation may have been accepted; retirement held.");
  await assertNoUnreconciledUpdateOperations(baseDir, current);
  if (current?.status === "pending") throw new Error("An update is pending; retirement held.");
  const staged = await verifyStagedQualifiedRuntime(baseDir, activeVersion, input.stagedHandle);
  const binding = operationBinding(staged);
  if (
    [
      "environmentId",
      "currentVersion",
      "expectedInstalledSource",
      "targetSource",
      "stagedHandle",
    ].some(
      (key) =>
        binding[key as keyof NativeOperationBinding] !==
        input[key as keyof ServiceUpdateRetirement],
    )
  )
    throw new Error("Retirement differs from the immutable staged binding.");
  const selected = await restoreStagedSelection(baseDir, activeVersion);
  if (selected !== undefined && !sameOperationBinding(operationBinding(selected), binding))
    throw new Error("Another staged selection occupies the active binding; retirement held.");
  const records = await readRevocations(baseDir);
  const prior = records.find((row) => row.operationId === input.operationId);
  if (prior !== undefined && !sameOperationBinding(prior.binding, binding))
    throw new Error("Retirement operation ID already names another immutable binding.");
  if (
    records.some(
      (row) =>
        row.binding.stagedHandle === input.stagedHandle &&
        !sameOperationBinding(row.binding, binding),
    )
  )
    throw new Error("Retired stage binding differs; reconciliation required.");
  if (prior === undefined) {
    const directory = directoryPath(baseDir);
    await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
    const parent = await NodeFSP.open(NodePath.dirname(directory), "r");
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
    const file = await NodeFSP.open(
      NodePath.join(directory, `${input.operationId}.json`),
      "wx",
      0o600,
    );
    try {
      await file.writeFile(
        JSON.stringify({ schema: 1, operationId: input.operationId, binding }) + "\n",
      );
      await file.sync();
    } finally {
      await file.close();
    }
    const handle = await NodeFSP.open(directory, "r");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  }
  // A prior response may have been lost at fsync: replay establishes durability
  // again before it may acknowledge retirement or clear a pointer.
  const retained = await NodeFSP.open(
    NodePath.join(directoryPath(baseDir), `${input.operationId}.json`),
    "r",
  );
  try {
    await retained.sync();
  } finally {
    await retained.close();
  }
  const retainedDirectory = await NodeFSP.open(directoryPath(baseDir), "r");
  try {
    await retainedDirectory.sync();
  } finally {
    await retainedDirectory.close();
  }
  // Publication must succeed before pointer removal. An interrupted publication
  // remains a fail-closed receipt; it never licenses supersession.
  await retireStagedSelection(baseDir, activeVersion, input);
}
