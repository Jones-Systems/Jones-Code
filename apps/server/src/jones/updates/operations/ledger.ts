// @effect-diagnostics nodeBuiltinImport:off globalDate:off
// Receipts are outside rollback state. No receipt is removed: external plan references are unknown.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as Schema from "effect/Schema";
import {
  JonesOperationId,
  JonesOperationReceipt,
  type JonesOperationStartInput,
  type JonesOperationPhase,
} from "@t3tools/contracts/jones/jonesOperations";
import { withQualifiedRuntimeLock } from "../../cloud/qualifiedRuntime.ts";
import {
  ownedDirectory,
  read,
  replace,
  serialized,
  syncDirectory,
} from "../../fleetUpdates/store.ts";

const SCAN_BATCH = 32;
const isReceipt = Schema.is(JonesOperationReceipt);
const isId = Schema.is(JonesOperationId);
const unsettled = new Set<JonesOperationPhase>([
  "accepted",
  "running",
  "stop-requested",
  "adopting",
  "handed-off",
  "held",
]);
const legal: Partial<Record<JonesOperationPhase, readonly JonesOperationPhase[]>> = {
  accepted: [
    "running",
    "stop-requested",
    "completed",
    "stopped",
    "failed",
    "interrupted",
    "refused",
    "held",
  ],
  running: [
    "adopting",
    "stop-requested",
    "handed-off",
    "held",
    "completed",
    "rolled-back",
    "refused",
    "failed",
    "blocked",
    "interrupted",
  ],
  "stop-requested": ["adopting", "held", "completed", "stopped", "failed", "interrupted"],
  adopting: ["completed", "held", "blocked"],
  "handed-off": ["completed", "rolled-back", "refused", "blocked", "held"],
  held: ["completed", "rolled-back", "stopped", "interrupted", "handed-off", "blocked", "refused"],
};
const sameIntent = (a: JonesOperationReceipt, b: JonesOperationStartInput) =>
  a.environmentId === b.environmentId &&
  a.expectedInstalledSource === b.expectedInstalledSource &&
  NodeUtil.isDeepStrictEqual(a.action, b.action) &&
  NodeUtil.isDeepStrictEqual(a.correlation, b.correlation) &&
  NodeUtil.isDeepStrictEqual(a.admission, b.admission);
function operationReceipt(
  input: JonesOperationStartInput,
  phase: JonesOperationPhase,
): JonesOperationReceipt {
  const now = new Date().toISOString();
  return {
    ...input,
    protocol: 1,
    phase,
    updatedAt: now,
    ...(phase === "accepted" ? { acceptedAt: now } : {}),
  };
}
export function replayOperationReceipt(
  old: JonesOperationReceipt,
  input: JonesOperationStartInput,
) {
  if (
    sameIntent(old, input) ||
    (old.phase === "fenced" && old.environmentId === input.environmentId)
  )
    return old;
  return {
    ...operationReceipt(input, "refused"),
    code: "operation-conflict",
    message: "This operation ID is already bound to another request.",
  } satisfies JonesOperationReceipt;
}

export function replayOperationFence(old: JonesOperationReceipt, input: JonesOperationStartInput) {
  if (old.environmentId !== input.environmentId)
    throw new Error("Fence environment binding changed.");
  if (old.phase !== "fenced" && !sameIntent(old, input))
    throw new Error("Fence conflicts with the operation's original request.");
  return old;
}
export function createOperationLedger(baseDir: string) {
  const directory = NodePath.join(baseDir, "runtime", "jones-operations");
  const path = (id: string) => {
    if (!isId(id)) throw new Error("Invalid Jones operation ID.");
    return NodePath.join(directory, `${id}.json`);
  };
  const locked = <A>(f: () => Promise<A>) =>
    serialized(directory, async () => {
      await ownedDirectory(NodePath.dirname(directory), false);
      await ownedDirectory(directory, true);
      return withQualifiedRuntimeLock(baseDir, "jones-operations", async () => {
        // Any writer may have renamed a receipt before its directory sync failed.
        await syncDirectory(directory);
        return f();
      });
    });
  const load = async (id: string) => {
    const receipt = await read(path(id), isReceipt, 16 * 1024);
    if (receipt !== null && receipt.operationId !== id)
      throw new Error("Operation filename binding changed.");
    return receipt;
  };
  const save = async (receipt: JonesOperationReceipt) => {
    if (!isReceipt(receipt) || Buffer.byteLength(JSON.stringify(receipt)) + 1 > 16 * 1024)
      throw new Error("Invalid Jones operation receipt.");
    await replace(path(receipt.operationId), receipt);
    return receipt;
  };
  const create = (input: JonesOperationStartInput, refusal?: { code: string; message: string }) =>
    locked(async () => {
      const old = await load(input.operationId);
      if (old !== null) return { created: false, receipt: replayOperationReceipt(old, input) };
      const receipt =
        refusal === undefined
          ? operationReceipt(input, "accepted")
          : { ...operationReceipt(input, "refused"), ...refusal };
      return { created: true, receipt: await save(receipt) };
    });
  const readMany = (ids: readonly string[]) =>
    locked(async () => {
      const receipts = [];
      for (const operationId of ids)
        receipts.push({ operationId, receipt: await load(operationId) });
      return receipts;
    });
  return {
    read: (id: string) => locked(() => load(id)),
    readMany,
    accept: (input: JonesOperationStartInput) => create(input),
    refuse: (input: JonesOperationStartInput, code: string, message: string) =>
      create(input, { code, message }),
    fence: (input: JonesOperationStartInput) =>
      locked(async () => {
        const old = await load(input.operationId);
        return old === null
          ? save(operationReceipt(input, "fenced"))
          : replayOperationFence(old, input);
      }),
    transition: (id: string, change: (old: JonesOperationReceipt) => JonesOperationReceipt) =>
      locked(async () => {
        const old = await load(id);
        if (old === null) throw new Error("Operation was never recorded.");
        const next = change(old);
        if (
          (next.phase === "stopped" && old.action.kind === "install") ||
          (old.phase === "accepted" &&
            old.action.kind === "install" &&
            (next.phase === "completed" || next.phase === "failed")) ||
          next.operationId !== id ||
          !sameIntent(old, next) ||
          next.acceptedAt !== old.acceptedAt ||
          (old.phase !== next.phase && !legal[old.phase]?.includes(next.phase))
        )
          throw new Error("Illegal operation transition.");
        for (const key of ["jobId", "staged", "updateId", "recovery"] as const)
          if (old[key] !== undefined && !NodeUtil.isDeepStrictEqual(old[key], next[key]))
            throw new Error("Stable operation evidence changed.");
        if (
          old.stop !== undefined &&
          (next.stop?.stopId !== old.stop.stopId || next.stop.requestedAt !== old.stop.requestedAt)
        )
          throw new Error("Stop identity changed.");
        // A background readback of the same hold must keep a cleanup review bound to this receipt.
        if (NodeUtil.isDeepStrictEqual(old, next)) return old;
        return save({ ...next, updatedAt: new Date().toISOString() });
      }),
    // Streaming validation keeps memory bounded. The history is never capped or silently pruned:
    // age/count alone cannot prove that a plan no longer references an operation ID.
    listUnsettled: async function* () {
      const entries = await locked(() => NodeFSP.opendir(directory));
      let ids: string[] = [];
      for await (const entry of entries) {
        if (!entry.name.endsWith(".json") || !isId(entry.name.slice(0, -5))) continue;
        ids.push(entry.name.slice(0, -5));
        if (ids.length < SCAN_BATCH) continue;
        for (const { receipt } of await readMany(ids))
          if (receipt !== null && unsettled.has(receipt.phase)) yield receipt;
        ids = [];
      }
      if (ids.length > 0)
        for (const { receipt } of await readMany(ids))
          if (receipt !== null && unsettled.has(receipt.phase)) yield receipt;
    },
  };
}
export type OperationLedger = ReturnType<typeof createOperationLedger>;
