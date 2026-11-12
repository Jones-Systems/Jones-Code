import {
  isOrchestrationV2WorkActive,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Subagent,
  type ProviderThreadId,
  type ThreadId,
} from "@t3tools/contracts";
import { pendingBackgroundTurnItems } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as Effect from "effect/Effect";

import * as Projections from "../../orchestration-v2/ProjectionStore.ts";

const fields = ["runs", "providerThreads", "subagents", "turnItems", "providerTurns"] as const;
type Records = Projections.ProjectionRecords<(typeof fields)[number]>;
type Provider = OrchestrationV2ProviderThread;
type OwnershipRecords = Pick<Records, "thread" | "providerThreads" | "subagents">;

const nativeProvider = (provider: Provider | undefined): provider is Provider =>
  provider?.driver === "codex" &&
  provider.nativeThreadRef?.driver === "codex" &&
  provider.nativeThreadRef.strength === "strong" &&
  !!provider.nativeThreadRef.nativeId?.trim();

// CodexAdapterV2 records all three links; lineage alone also describes app-owned tasks.
const owns = (
  parent: OwnershipRecords,
  provider: Provider | undefined,
  task: OrchestrationV2Subagent,
  child: OwnershipRecords,
): boolean => {
  const childProvider = child.providerThreads.find((row) => row.id === task.providerThreadId);
  return (
    nativeProvider(provider) &&
    nativeProvider(childProvider) &&
    provider.appThreadId === parent.thread.id &&
    task.origin === "provider_native" &&
    task.driver === "codex" &&
    task.threadId === parent.thread.id &&
    task.providerInstanceId === provider.providerInstanceId &&
    task.nativeTaskRef?.driver === "codex" &&
    task.nativeTaskRef.strength === "strong" &&
    !!task.nativeTaskRef.nativeId?.trim() &&
    task.childThreadId === child.thread.id &&
    child.thread.lineage.relationshipToParent === "subagent" &&
    child.thread.lineage.parentThreadId === parent.thread.id &&
    child.thread.lineage.rootThreadId === parent.thread.lineage.rootThreadId &&
    child.thread.forkedFrom?.type === "node" &&
    child.thread.forkedFrom.nodeId === task.id &&
    childProvider.appThreadId === child.thread.id &&
    childProvider.providerInstanceId === provider.providerInstanceId &&
    childProvider.forkedFrom?.providerThreadId === provider.id
  );
};

const locallyQuiet = (records: Records, provider: Provider, root: boolean) =>
  !records.subagents.some(
    (task) =>
      (root || task.origin === "provider_native") && isOrchestrationV2WorkActive(task.status),
  ) &&
  (provider.pendingBackgroundTasks?.length ?? 0) === 0 &&
  pendingBackgroundTurnItems({ runs: records.runs, turnItems: records.turnItems }).filter(
    (item) => root || item.type !== "subagent" || item.origin !== "app_owned",
  ).length === 0 &&
  (root ||
    (!records.providerTurns.some(
      (turn) => turn.providerThreadId === provider.id && isOrchestrationV2WorkActive(turn.status),
    ) &&
      !records.runs.some(
        (run) =>
          run.providerThreadId === provider.id &&
          ["preparing", "starting", "running", "waiting"].includes(run.status),
      )));

export const make = Effect.fn("CodexFailoverLineage.make")(function* () {
  const projections = yield* Projections.ProjectionStoreV2;
  const read = (threadId: ThreadId) =>
    projections
      .getThreadRecords(threadId, fields)
      .pipe(Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }));
  // Settle notifications need ownership only; avoid transcript reads on that path.
  const readOwnership = (threadId: ThreadId) =>
    projections
      .getThreadRecords(threadId, ["providerThreads", "subagents"])
      .pipe(Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }));
  const sourceQuiet = Effect.fn("CodexFailoverLineage.sourceQuiet")(function* (
    root: Records,
    providerThreadId: ProviderThreadId | null | undefined,
  ) {
    const provider = root.providerThreads.find((row) => row.id === providerThreadId);
    if (!nativeProvider(provider)) return false;
    const seen = new Set<ThreadId>();
    const pending = [{ records: root, provider }];
    // Visit only durable owned edges, including completed parents with live descendants.
    // A finite visited set bounds the walk without declaring a truncated tree quiet.
    while (pending.length > 0) {
      const current = pending.pop()!;
      const { records } = current;
      if (seen.has(records.thread.id)) return false;
      seen.add(records.thread.id);
      if (!locallyQuiet(records, current.provider, records.thread.id === root.thread.id))
        return false;
      for (const task of records.subagents) {
        if (
          task.origin !== "provider_native" ||
          task.driver !== "codex" ||
          task.providerInstanceId !== current.provider.providerInstanceId
        )
          continue;
        if (task.childThreadId === null) return false;
        const child = yield* read(task.childThreadId);
        if (child === null || !owns(records, current.provider, task, child)) return false;
        pending.push({
          records: child,
          provider: child.providerThreads.find((row) => row.id === task.providerThreadId)!,
        });
      }
    }
    return true;
  });
  const settleThreadIds = Effect.fn("CodexFailoverLineage.settleThreadIds")(function* (
    threadId: ThreadId,
  ) {
    const ids = [threadId];
    const seen = new Set(ids);
    let child = yield* readOwnership(threadId);
    while (
      child !== null &&
      child.thread.lineage.relationshipToParent === "subagent" &&
      child.thread.lineage.parentThreadId !== null &&
      child.thread.forkedFrom?.type === "node"
    ) {
      const parentId = child.thread.lineage.parentThreadId;
      if (seen.has(parentId)) break;
      const parent = yield* readOwnership(parentId);
      if (parent === null) break;
      const nodeId = child.thread.forkedFrom.nodeId;
      const task = parent.subagents.find((row) => row.id === nodeId);
      if (task === undefined || task.origin !== "provider_native") break;
      const provider = child.providerThreads.find((row) => row.id === task.providerThreadId);
      const parentProvider = parent.providerThreads.find(
        (row) => row.id === provider?.forkedFrom?.providerThreadId,
      );
      if (!owns(parent, parentProvider, task, child)) break;
      ids.push(parentId);
      seen.add(parentId);
      child = parent;
    }
    return ids;
  });
  return { sourceQuiet, settleThreadIds };
});
