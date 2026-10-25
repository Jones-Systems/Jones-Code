import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  isParentThreadRelationship,
  type ThreadRelationshipWalkRow,
} from "@t3tools/client-runtime/state/thread-relationships";
import type {
  EnvironmentId,
  EnvironmentMachineKind,
  ScopedThreadRef,
  OrchestrationV2Subagent,
  OrchestrationV2RemoteTaskChild,
  OrchestrationV2ThreadShell,
  ThreadId,
} from "@t3tools/contracts";

export function resolveMachineIdentity(
  label: string | null | undefined,
  kind?: EnvironmentMachineKind | null,
) {
  const fullLabel = label?.trim() || "Unknown environment";
  const normalized = fullLabel.toLowerCase().replace(/[-_\s]+/g, " ");
  const aliases: Readonly<Record<string, "vps" | "test" | "laptop" | "mini">> = {
    vps: "vps",
    "test server": "test",
    tsv: "test",
    laptop: "laptop",
    lap: "laptop",
    mini: "mini",
    "mac mini": "mini",
  };
  const known = Object.hasOwn(aliases, normalized) ? aliases[normalized] : undefined;
  const identity =
    known ?? (kind === "laptop" ? "laptop" : kind === "mac-mini" ? "mini" : "unknown");
  const shortLabel = { vps: "VPS", test: "TSV", laptop: "LAP", mini: "MINI", unknown: fullLabel }[
    identity
  ];
  return { identity, label: fullLabel, shortLabel, kind: kind ?? "server" } as const;
}

export type ParentSource = {
  readonly lineage: OrchestrationV2ThreadShell["lineage"];
  readonly delegatedFrom?: OrchestrationV2ThreadShell["delegatedFrom"] | null;
  readonly linkOrigin?: OrchestrationV2ThreadShell["linkOrigin"] | null;
};

export function resolveParentRef(
  environmentId: EnvironmentId,
  thread: ParentSource | null | undefined,
): { ref: ScopedThreadRef; remote: boolean; fallbackTitle: string; fallbackLabel: string } | null {
  if (thread?.lineage.relationshipToParent === "subagent" && thread.lineage.parentThreadId) {
    return {
      ref: scopeThreadRef(environmentId, thread.lineage.parentThreadId),
      remote: false,
      fallbackTitle: "Parent thread",
      fallbackLabel: "",
    };
  }
  const delegated = thread?.delegatedFrom;
  return delegated
    ? {
        ref: scopeThreadRef(delegated.environmentId, delegated.threadId),
        remote: true,
        fallbackTitle: delegated.title,
        fallbackLabel: thread?.linkOrigin?.label ?? "",
      }
    : null;
}

export type RemoteAgent = OrchestrationV2Subagent & {
  readonly remoteChild: OrchestrationV2RemoteTaskChild;
};
export type LineageRow =
  | {
      readonly kind: "local";
      readonly row: ThreadRelationshipWalkRow;
      readonly status: string | null;
    }
  | { readonly kind: "remote"; readonly agent: RemoteAgent; readonly status: string };

export function groupLineageRows(
  local: readonly ThreadRelationshipWalkRow[],
  agents: readonly OrchestrationV2Subagent[],
  threadId: ThreadId,
) {
  const rows: LineageRow[] = [
    ...local.map((row): LineageRow => ({ kind: "local", row, status: row.edge.status ?? null })),
    ...agents
      .filter((agent): agent is RemoteAgent => agent.remoteChild !== undefined)
      .map((agent): LineageRow => ({ kind: "remote", agent, status: agent.status })),
  ];
  const related: LineageRow[] = [],
    active: LineageRow[] = [],
    previous: LineageRow[] = [];
  for (const row of rows) {
    if (
      row.kind === "local" &&
      (row.row.edge.kind !== "subagent" || isParentThreadRelationship(row.row.edge, threadId))
    )
      related.push(row);
    else if (
      ["completed", "failed", "error", "cancelled", "interrupted", "idle"].includes(
        row.status ?? "",
      )
    )
      previous.push(row);
    else active.push(row);
  }
  const runningCount =
    active.filter((row) => row.status === "running").length +
    agents.filter(
      (agent) =>
        agent.childThreadId === null &&
        agent.remoteChild === undefined &&
        agent.status === "running",
    ).length;
  return { related, active, previous, runningCount };
}
