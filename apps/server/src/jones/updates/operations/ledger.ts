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
import { ownedDirectory, read, replace, serialized } from "../../fleetUpdates/store.ts";

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
  accepted: ["running", "stop-requested", "stopped", "interrupted", "refused", "held"],
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
  NodeUtil.isDeepStrictEqual(a.correlation, b.correlation);
export function operationReceipt(
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
export function operationTombstone(input: JonesOperationStartInput): JonesOperationReceipt {
  return operationReceipt(input, "fenced");
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
      return withQualifiedRuntimeLock(baseDir, "jones-operations", f);
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
  const existing = (old: JonesOperationReceipt, input: JonesOperationStartInput) =>
    (old.phase === "fenced" && old.environmentId === input.environmentId) || sameIntent(old, input)
      ? old
      : ({
          ...operationReceipt(input, "refused"),
          code: "operation-conflict",
          message: "This operation ID is already bound to another request.",
        } satisfies JonesOperationReceipt);
  const create = (input: JonesOperationStartInput, refusal?: { code: string; message: string }) =>
    locked(async () => {
      const old = await load(input.operationId);
      if (old !== null) return { created: false, receipt: existing(old, input) };
      const receipt =
        refusal === undefined
          ? operationReceipt(input, "accepted")
          : { ...operationReceipt(input, "refused"), ...refusal };
      return { created: true, receipt: await save(receipt) };
    });
  return {
    read: (id: string) => locked(() => load(id)),
    accept: (input: JonesOperationStartInput) => create(input),
    refuse: (input: JonesOperationStartInput, code: string, message: string) =>
      create(input, { code, message }),
    fence: (input: JonesOperationStartInput) =>
      locked(async () => (await load(input.operationId)) ?? save(operationTombstone(input))),
    transition: (id: string, change: (old: JonesOperationReceipt) => JonesOperationReceipt) =>
      locked(async () => {
        const old = await load(id);
        if (old === null) throw new Error("Operation was never recorded.");
        const next = change(old);
        if (
          (next.phase === "stopped" && old.action.kind === "install") ||
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
        return save({ ...next, updatedAt: new Date().toISOString() });
      }),
    // Streaming validation keeps memory bounded. The history is never capped or silently pruned:
    // age/count alone cannot prove that a plan no longer references an operation ID.
    listUnsettled: async function* () {
      await locked(async () => undefined);
      for await (const entry of await NodeFSP.opendir(directory)) {
        if (!entry.name.endsWith(".json") || !isId(entry.name.slice(0, -5))) continue;
        const receipt = await locked(() => load(entry.name.slice(0, -5)));
        if (receipt !== null && unsettled.has(receipt.phase)) yield receipt;
      }
    },
  };
}
export type OperationLedger = ReturnType<typeof createOperationLedger>;
