import type { EnvironmentId } from "@t3tools/contracts";
import {
  advancePlanCreation,
  decodePlanCreationOperation,
  encodePlanCreationOperation,
  planCreationSlotKey,
  type PlanCreationOperation,
  type PlanCreationSlot,
} from "@t3tools/client-runtime/jones/planCreationOperation";
import type { StateStorage } from "../../lib/storage";

interface PlanStorage extends StateStorage<void | Promise<void>> {
  readonly withLock?: <A>(name: string, work: () => Promise<A>) => Promise<A>;
}
interface Document {
  readonly schemaVersion: 1;
  readonly current: readonly PlanCreationOperation[];
  readonly history: readonly PlanCreationOperation[];
}
const maxDocumentBytes = 1024 * 1024;
const maxOperations = 64;
export const planCreationStorageKey = (environmentId: EnvironmentId) =>
  `jones.plan-creation.v1:${environmentId}`;

export function createPlanCreationStorage(storage: PlanStorage) {
  const queues = new Map<EnvironmentId, Promise<unknown>>();
  const listeners = new Map<EnvironmentId, Set<() => void>>();
  const publish = (environmentId: EnvironmentId) => {
    for (const listener of listeners.get(environmentId) ?? []) listener();
  };
  const exclusive = <A>(environmentId: EnvironmentId, work: () => Promise<A>): Promise<A> => {
    const run = (queues.get(environmentId) ?? Promise.resolve()).then(() =>
      storage.withLock ? storage.withLock(planCreationStorageKey(environmentId), work) : work(),
    );
    queues.set(environmentId, run.catch(() => undefined));
    return run;
  };
  const read = async (environmentId: EnvironmentId): Promise<Document> => {
    const raw = await storage.getItem(planCreationStorageKey(environmentId));
    if (raw === null) return { schemaVersion: 1, current: [], history: [] };
    if (new TextEncoder().encode(raw).byteLength > maxDocumentBytes)
      throw new Error("Saved plan actions need reconciliation. No further work was sent.");
    const value: unknown = JSON.parse(raw);
    if (
      typeof value !== "object" || value === null ||
      !("schemaVersion" in value) || value.schemaVersion !== 1 ||
      !("current" in value) || !Array.isArray(value.current) ||
      !("history" in value) || !Array.isArray(value.history) ||
      Object.keys(value).some((key) => !["schemaVersion", "current", "history"].includes(key)) ||
      value.current.length + value.history.length > maxOperations
    ) throw new Error("Saved plan actions need reconciliation. No further work was sent.");
    const current = value.current.map(decodePlanCreationOperation);
    const history = value.history.map(decodePlanCreationOperation);
    const all = [...current, ...history];
    if (
      all.some((operation) => operation.environmentId !== environmentId) ||
      history.some((operation) => operation.phase !== "create-rejected") ||
      new Set(all.flatMap((operation) => [operation.create.commandId, operation.prepared.payload.commandId])).size !== all.length * 2 ||
      new Set(all.map((operation) => operation.create.threadId)).size !== all.length ||
      new Set(all.map((operation) => operation.prepared.payload.messageId)).size !== all.length ||
      new Set(current.map(planCreationSlotKey)).size !== current.length
    ) throw new Error("Saved plan action identities conflict. No further work was sent.");
    return { schemaVersion: 1, current, history };
  };
  const write = async (environmentId: EnvironmentId, document: Document) => {
    if (document.current.length + document.history.length > maxOperations)
      throw new Error("Plan action history is full. Earlier actions were retained.");
    const raw = JSON.stringify({
      schemaVersion: 1,
      current: document.current.map(encodePlanCreationOperation),
      history: document.history.map(encodePlanCreationOperation),
    });
    if (new TextEncoder().encode(raw).byteLength > maxDocumentBytes)
      throw new Error("Plan action history is full. Earlier actions were retained.");
    // StateStorage may be asynchronous. A debounced/memory acknowledgement is not sufficient.
    await storage.setItem(planCreationStorageKey(environmentId), raw);
    if (await storage.getItem(planCreationStorageKey(environmentId)) !== raw)
      throw new Error("Plan action could not be saved. Nothing further was sent.");
    publish(environmentId);
  };
  return {
    subscribe(environmentId: EnvironmentId, listener: () => void) {
      const set = listeners.get(environmentId) ?? new Set<() => void>();
      set.add(listener);
      listeners.set(environmentId, set);
      return () => {
        set.delete(listener);
        if (set.size === 0) listeners.delete(environmentId);
      };
    },
    readSource(environmentId: EnvironmentId, sourceThreadId: PlanCreationSlot["sourceThreadId"]) {
      return exclusive(environmentId, async () =>
        (await read(environmentId)).current.filter((operation) => operation.sourceThreadId === sourceThreadId),
      );
    },
    read(slot: PlanCreationSlot) {
      return exclusive(slot.environmentId, async () =>
        (await read(slot.environmentId)).current.find(
          (operation) => planCreationSlotKey(operation) === planCreationSlotKey(slot),
        ),
      );
    },
    admit(slot: PlanCreationSlot, allocate: () => Promise<PlanCreationOperation>) {
      return exclusive(slot.environmentId, async () => {
        const document = await read(slot.environmentId);
        const prior = document.current.find(
          (operation) => planCreationSlotKey(operation) === planCreationSlotKey(slot),
        );
        if (prior && prior.phase !== "create-rejected") return prior;
        if (document.current.length + document.history.length >= maxOperations)
          throw new Error("Plan action history is full. Earlier actions were retained.");
        const operation = decodePlanCreationOperation(await allocate());
        if (operation.phase !== "create-pending" || planCreationSlotKey(operation) !== planCreationSlotKey(slot))
          throw new Error("Plan action does not match its source.");
        if ([...document.current, ...document.history].some((row) =>
          row.create.threadId === operation.create.threadId ||
          row.prepared.payload.messageId === operation.prepared.payload.messageId ||
          [row.create.commandId, row.prepared.payload.commandId].some((id) =>
            id === operation.create.commandId || id === operation.prepared.payload.commandId)))
          throw new Error("Plan action identity is already retained.");
        await write(slot.environmentId, {
          schemaVersion: 1,
          current: [...document.current.filter((row) => row !== prior), operation],
          history: prior ? [...document.history, prior] : document.history,
        });
        return operation;
      });
    },
    advance(operation: PlanCreationOperation, phase: PlanCreationOperation["phase"]) {
      return exclusive(operation.environmentId, async () => {
        const document = await read(operation.environmentId);
        const prior = document.current.find((row) => row.create.commandId === operation.create.commandId);
        if (!prior || JSON.stringify({ ...prior, phase: operation.phase }) !== JSON.stringify(operation))
          throw new Error("Saved plan action changed. Reconcile its existing identity.");
        const next = advancePlanCreation(prior, phase);
        if (next !== prior) await write(operation.environmentId, {
          ...document,
          current: document.current.map((row) => row === prior ? next : row),
        });
        return next;
      });
    },
    clearEnvironment(environmentId: EnvironmentId) {
      return exclusive(environmentId, async () => {
        await storage.removeItem(planCreationStorageKey(environmentId));
        if (await storage.getItem(planCreationStorageKey(environmentId)) !== null)
          throw new Error("Saved plan actions could not be removed.");
        publish(environmentId);
      });
    },
  };
}

// Use the platform's real storage directly, without resolveStorage's memory fallback
// or createDeferredStorage's delayed write. Browser setItem completion is the acknowledgement.
export const planCreationStorage = createPlanCreationStorage({
  getItem: (key) => window.localStorage.getItem(key),
  setItem: (key, value) => window.localStorage.setItem(key, value),
  removeItem: (key) => window.localStorage.removeItem(key),
  withLock: (name, work) =>
    typeof navigator !== "undefined" && navigator.locks
      ? navigator.locks.request(name, work)
      : work(),
});
