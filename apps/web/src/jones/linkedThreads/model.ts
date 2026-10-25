import {
  endpointKey,
  sameEndpoint,
  type LinkedGroup,
  type LinkedRelation,
  type LinkedSnapshot,
} from "@t3tools/client-runtime/jones/linked-threads";
import { isParentThreadRelationship } from "@t3tools/client-runtime/state/thread-relationships";
import type { ThreadEndpointRef } from "@t3tools/contracts/jones/linkedThreads";
import type { LineageRow } from "../remoteAgentLineage/model";

export type LinkedEntry = {
  readonly key: string;
  readonly endpoint: ThreadEndpointRef;
  group: LinkedGroup;
  readonly labels: string[];
  readonly lineage: LineageRow[];
  readonly relations: LinkedRelation[];
  remoteParent: boolean;
  snapshot?: LinkedSnapshot;
};
const rank: Record<LinkedGroup, number> = {
  delegated: -1,
  continuations: 0,
  parents: 1,
  children: 2,
  siblings: 3,
};

export function buildLinkedEntries(input: {
  readonly subject: ThreadEndpointRef;
  readonly related: readonly LineageRow[];
  readonly active: readonly LineageRow[];
  readonly previous: readonly LineageRow[];
  readonly relations: readonly LinkedRelation[];
  readonly snapshots: ReadonlyMap<string, LinkedSnapshot>;
  readonly remoteParent?: ThreadEndpointRef;
}) {
  const entries = new Map<string, LinkedEntry>();
  const add = (endpoint: ThreadEndpointRef, group: LinkedGroup, label: string) => {
    const key = endpointKey(endpoint);
    let entry = entries.get(key);
    if (!entry) {
      entry = {
        key,
        endpoint,
        group,
        labels: [],
        lineage: [],
        relations: [],
        remoteParent: false,
        ...(input.snapshots.get(key) ? { snapshot: input.snapshots.get(key)! } : {}),
      };
      entries.set(key, entry);
    }
    if (rank[group] < rank[entry.group]) entry.group = group;
    if (!entry.labels.includes(label)) entry.labels.push(label);
    return entry;
  };
  for (const row of [...input.active, ...input.previous, ...input.related]) {
    const endpoint =
      row.kind === "remote"
        ? row.agent.remoteChild
        : { environmentId: input.subject.environmentId, threadId: row.row.threadId };
    const delegated = input.active.includes(row) || input.previous.includes(row);
    const edge = row.kind === "local" ? row.row.edge : null;
    const parent = edge !== null && isParentThreadRelationship(edge, input.subject.threadId);
    const group = delegated
      ? "delegated"
      : edge?.kind === "transfer"
        ? "siblings"
        : parent
          ? "parents"
          : "children";
    const label = delegated
      ? "Delegated task"
      : edge?.kind === "transfer"
        ? "Context transfer"
        : edge?.kind === "subagent"
          ? "Parent agent"
          : parent
            ? "Forked from"
            : "Fork";
    const entry = add(endpoint, group, label);
    if (delegated) entry.lineage.push(row);
    else if (
      entry.group !== "delegated" &&
      (entry.lineage.length === 0 || (edge?.kind === "fork" && parent))
    )
      entry.lineage.splice(0, entry.lineage.length, row);
  }
  if (input.remoteParent) add(input.remoteParent, "parents", "Parent agent").remoteParent = true;
  for (const relation of input.relations) {
    if (sameEndpoint(relation.endpoint, input.subject)) continue;
    add(relation.endpoint, relation.group, relation.label).relations.push(relation);
  }
  return [...entries.values()].map((entry) => ({
    ...entry,
    labels: [...entry.labels].sort((a, b) => {
      const priority = (label: string) =>
        label === "Delegated task"
          ? -1
          : label.startsWith("Continue")
            ? 0
            : label.includes("Parent") || label === "Forked from" || label === "Launched by"
              ? 1
              : label === "Fork" || label === "Launched"
                ? 2
                : label === "Context transfer"
                  ? 4
                  : 3;
      return priority(a) - priority(b);
    }),
  }));
}
