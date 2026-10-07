// @effect-diagnostics nodeBuiltinImport:off
// The launcher has no Effect runtime; this shared boundary uses Node durability primitives.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeNet from "node:net";
import type * as NetAddress from "effect/unstable/net/NetAddress";
import type { StagedQualifiedRuntime } from "./qualifiedRuntime.ts";

export interface QualifiedTrialRuntimeWitness {
  readonly home: string;
  readonly databasePath: string;
  readonly serviceUserdata: string;
  readonly environmentId: string;
  readonly version: string;
  readonly buildMetadata: unknown;
  readonly listener: NetAddress.SocketAddress;
  readonly processId: number;
}

export interface QualifiedTrialReceipt {
  readonly protocol: 4;
  readonly startupGateProtocol: 1;
  readonly updateId: string;
  readonly stagedHandle: string;
  readonly home: string;
  readonly databasePath: string;
  readonly serviceUserdata: string;
  readonly environmentId: string;
  readonly version: string;
  readonly sourceSha: string;
  readonly sourceTree: string;
  readonly listener: {
    readonly family: "IPv4" | "IPv6";
    readonly address: string;
    readonly port: number;
    readonly scopeId: 0;
  };
  readonly processId: number;
  readonly resumeHeld: true;
}

export interface QualifiedTrialGrant extends QualifiedTrialReceipt {
  readonly generation: string;
}

const record = (input: unknown): Record<string, unknown> | undefined =>
  typeof input === "object" && input !== null && !Array.isArray(input)
    ? input as Record<string, unknown>
    : undefined;
const transactionId = (id: unknown): id is string =>
  typeof id === "string" && /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(id);
const source = (value: unknown): value is string =>
  typeof value === "string" && /^[a-f0-9]{40}$/.test(value);
const absolute = (value: unknown): value is string =>
  typeof value === "string" && NodePath.isAbsolute(value) && NodePath.normalize(value) === value;

export function decodeQualifiedTrialReceipt(input: unknown): QualifiedTrialReceipt | undefined {
  const value = record(input);
  const listener = record(value?.listener);
  if (
    value?.protocol !== 4 || value.startupGateProtocol !== 1 ||
    !transactionId(value.updateId) ||
    typeof value.stagedHandle !== "string" ||
    !/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(value.stagedHandle) ||
    !absolute(value.home) || !absolute(value.databasePath) || !absolute(value.serviceUserdata) ||
    value.serviceUserdata !== NodePath.join(value.home, "userdata") ||
    value.databasePath !== NodePath.join(value.serviceUserdata, "statev2.sqlite") ||
    typeof value.environmentId !== "string" || value.environmentId.trim() === "" ||
    typeof value.version !== "string" || value.version.trim() === "" ||
    !source(value.sourceSha) || !source(value.sourceTree) ||
    !Number.isSafeInteger(value.processId) || (value.processId as number) < 1 ||
    value.resumeHeld !== true || listener === undefined ||
    (listener.family !== "IPv4" && listener.family !== "IPv6") ||
    typeof listener.address !== "string" ||
    NodeNet.isIP(listener.address) !== (listener.family === "IPv4" ? 4 : 6) ||
    !Number.isSafeInteger(listener.port) || (listener.port as number) < 1 || (listener.port as number) > 65535 ||
    listener.scopeId !== 0
  ) return undefined;
  return {
    protocol: 4, startupGateProtocol: 1, updateId: value.updateId,
    stagedHandle: value.stagedHandle, home: value.home, databasePath: value.databasePath,
    serviceUserdata: value.serviceUserdata, environmentId: value.environmentId,
    version: value.version, sourceSha: value.sourceSha, sourceTree: value.sourceTree,
    listener: {
      family: listener.family, address: listener.address, port: listener.port as number, scopeId: 0,
    },
    processId: value.processId as number, resumeHeld: true,
  };
}

export function decodeQualifiedTrialGrant(input: unknown): QualifiedTrialGrant | undefined {
  const receipt = decodeQualifiedTrialReceipt(input);
  const value = record(input);
  return receipt !== undefined && value?.generation === receipt.updateId
    ? { ...receipt, generation: receipt.updateId }
    : undefined;
}

export function sameQualifiedTrialIdentity(
  left: QualifiedTrialReceipt,
  right: QualifiedTrialReceipt,
): boolean {
  const a = decodeQualifiedTrialReceipt(left);
  const b = decodeQualifiedTrialReceipt(right);
  return a !== undefined && b !== undefined && JSON.stringify(a) === JSON.stringify(b);
}

export function assertQualifiedTrialBinding(input: {
  readonly updateId: string;
  readonly qualified: StagedQualifiedRuntime;
  readonly receipt: QualifiedTrialReceipt;
}): void {
  const { qualified, receipt } = input;
  if (
    decodeQualifiedTrialReceipt(receipt) === undefined ||
    input.updateId !== receipt.updateId || qualified.stagedHandle !== receipt.stagedHandle ||
    qualified.binding.baseDir !== receipt.home || qualified.binding.dbPath !== receipt.databasePath ||
    qualified.binding.environmentId !== receipt.environmentId ||
    qualified.receipt.version !== receipt.version || qualified.receipt.sourceSha !== receipt.sourceSha ||
    qualified.receipt.sourceTree !== receipt.sourceTree
  ) throw new Error("Qualified startup proof does not match the retained transaction.");
}

const cancelled = (signal?: AbortSignal): void => {
  if (signal?.aborted) throw new Error("Qualified startup was cancelled; its prior effects require reconciliation.");
};

export async function makeQualifiedTrialReceipt(input: {
  readonly updateId: string;
  readonly qualified: StagedQualifiedRuntime;
  readonly witness: QualifiedTrialRuntimeWitness;
  readonly signal?: AbortSignal;
}): Promise<QualifiedTrialReceipt> {
  cancelled(input.signal);
  const witness = input.witness;
  const metadata = record(witness.buildMetadata);
  const identity = record(metadata?.jonesSource);
  if (identity?.repository !== "Jones-Systems/Jones-Code")
    throw new Error("Qualified startup requires the bundled Jones source identity.");
  const listener = witness.listener;
  if (listener._tag !== "InetAddressV4" && listener._tag !== "InetAddressV6")
    throw new Error("Qualified startup requires an observed internet listener.");
  // No requested host or port is used here: the bound server supplies its actual socket address.
  const receipt = decodeQualifiedTrialReceipt({
    protocol: 4, startupGateProtocol: 1, updateId: input.updateId,
    stagedHandle: input.qualified.stagedHandle,
    home: await NodeFSP.realpath(witness.home),
    databasePath: await NodeFSP.realpath(witness.databasePath),
    serviceUserdata: await NodeFSP.realpath(witness.serviceUserdata),
    environmentId: witness.environmentId, version: witness.version,
    sourceSha: identity.sha, sourceTree: identity.tree,
    listener: {
      family: listener._tag === "InetAddressV4" ? "IPv4" : "IPv6",
      address: listener.address.toString(), port: listener.port,
      scopeId: listener._tag === "InetAddressV6" ? listener.scopeId : 0,
    },
    processId: witness.processId, resumeHeld: true,
  });
  if (receipt === undefined || witness.processId !== process.pid)
    throw new Error("Qualified startup runtime witness is incomplete or belongs to another process.");
  assertQualifiedTrialBinding({ ...input, receipt });
  cancelled(input.signal);
  return receipt;
}

export function qualifiedResumeReservationPath(receipt: QualifiedTrialReceipt): string {
  if (decodeQualifiedTrialReceipt(receipt) === undefined)
    throw new Error("Invalid qualified resume reservation identity.");
  return NodePath.join(receipt.home, "runtime", "db-backup", receipt.updateId, "resume-dispatched.json");
}

export interface QualifiedReservationAdapter {
  readonly before?: (phase: "write" | "file-sync" | "link" | "directory-sync") => Promise<void>;
}

export async function assertQualifiedResumeUnreserved(receipt: QualifiedTrialReceipt): Promise<void> {
  const path = qualifiedResumeReservationPath(receipt);
  await assertQualifiedResumeDirectory(receipt);
  const present = await NodeFSP.lstat(path).then(() => true, (cause: NodeJS.ErrnoException) => {
    if (cause.code === "ENOENT") return false;
    throw cause;
  });
  if (present) throw new Error("Qualified resume is already reserved; replay requires reconciliation.");
}

async function assertQualifiedResumeDirectory(receipt: QualifiedTrialReceipt): Promise<void> {
  const directory = NodePath.dirname(qualifiedResumeReservationPath(receipt));
  for (const path of [
    NodePath.join(receipt.home, "runtime"),
    NodePath.join(receipt.home, "runtime", "db-backup"), directory,
  ]) {
    const stat = await NodeFSP.lstat(path);
    if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o022) !== 0 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid()) ||
      await NodeFSP.realpath(path) !== path)
      throw new Error("Qualified resume transaction path has unknown ownership.");
  }
}

export async function reserveQualifiedResume(input: {
  readonly receipt: QualifiedTrialReceipt;
  readonly signal?: AbortSignal;
  readonly adapter?: QualifiedReservationAdapter;
}): Promise<void> {
  const target = qualifiedResumeReservationPath(input.receipt);
  const directory = NodePath.dirname(target);
  cancelled(input.signal);
  // The retained paired snapshot owns this transaction path; startup never creates another root.
  await assertQualifiedResumeUnreserved(input.receipt);
  const temp = NodePath.join(directory, `.resume-dispatched.${process.pid}.${NodeCrypto.randomUUID()}`);
  let handle: NodeFSP.FileHandle | undefined;
  let ownedTemp = false;
  let linked = false;
  try {
    cancelled(input.signal);
    handle = await NodeFSP.open(temp, "wx", 0o600);
    ownedTemp = true;
    await input.adapter?.before?.("write");
    cancelled(input.signal);
    await handle.writeFile(JSON.stringify(input.receipt) + "\n");
    await input.adapter?.before?.("file-sync");
    await handle.sync();
    await handle.close();
    handle = undefined;
    await input.adapter?.before?.("link");
    cancelled(input.signal);
    await NodeFSP.link(temp, target);
    linked = true;
    await input.adapter?.before?.("directory-sync");
    const parent = await NodeFSP.open(directory, "r");
    try { await parent.sync(); } finally { await parent.close(); }
    cancelled(input.signal);
  } catch (cause) {
    if (linked)
      throw new Error("Qualified resume reservation may be durable; retained for reconciliation.", { cause });
    throw cause;
  } finally {
    try { await handle?.close(); }
    finally {
      if (ownedTemp) await NodeFSP.unlink(temp).catch((cause: NodeJS.ErrnoException) => {
        if (cause.code !== "ENOENT") throw cause;
      });
    }
  }
}
