import { isParentThreadRelationship } from "@t3tools/client-runtime/state/thread-relationships";
import type { ThreadId } from "@t3tools/contracts";
import type { ReactNode } from "react";

import type { LineageRow } from "../remoteAgentLineage/model";

export function LinkedLineageGroups(props: {
  readonly threadId: ThreadId;
  readonly related: ReadonlyArray<LineageRow>;
  readonly active: ReadonlyArray<LineageRow>;
  readonly previous: ReadonlyArray<LineageRow>;
  readonly remoteParent: ReactNode;
  readonly renderGroup: (group: {
    readonly id: string;
    readonly label: string | null;
    readonly rows: ReadonlyArray<LineageRow>;
    readonly expanded: boolean;
  }) => ReactNode;
}) {
  const parents: LineageRow[] = [];
  const forksFrom: LineageRow[] = [];
  const forks: LineageRow[] = [];
  const transfers: LineageRow[] = [];
  for (const row of props.related) {
    if (row.kind !== "local") continue;
    if (row.row.edge.kind === "subagent") parents.push(row);
    else if (row.row.edge.kind === "transfer") transfers.push(row);
    else if (isParentThreadRelationship(row.row.edge, props.threadId)) forksFrom.push(row);
    else forks.push(row);
  }

  return (
    <>
      {props.remoteParent}
      {[
        { id: "parents", label: "Parent agents", rows: parents },
        { id: "forked-from", label: "Forked from", rows: forksFrom },
        { id: "forks", label: "Forks", rows: forks },
        { id: "transfers", label: "Context transfers", rows: transfers },
      ].map((group) => props.renderGroup({ ...group, expanded: true }))}
      {props.active.length > 0 || props.previous.length > 0 ? (
        <div role="group" aria-label="Delegated tasks">
          <h4 className="flex min-h-8 items-center px-2 text-2xs font-medium text-muted-foreground">
            Delegated tasks
          </h4>
          {props.renderGroup({ id: "active", label: null, rows: props.active, expanded: true })}
          {props.renderGroup({
            id: "previous",
            label: "Previous agents",
            rows: props.previous,
            expanded: false,
          })}
        </div>
      ) : null}
    </>
  );
}
