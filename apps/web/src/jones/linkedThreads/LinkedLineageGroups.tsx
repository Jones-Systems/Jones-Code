import type { ReactNode } from "react";
import type { LinkedEntry } from "./model";

export function LinkedLineageGroups(props: {
  readonly entries: readonly LinkedEntry[];
  readonly renderGroup: (group: {
    readonly id: string;
    readonly label: string | null;
    readonly rows: readonly LinkedEntry[];
    readonly expanded: boolean;
  }) => ReactNode;
}) {
  const delegated = props.entries.filter((entry) => entry.group === "delegated");
  const previous = (entry: LinkedEntry) =>
    entry.lineage.every((row) =>
      ["completed", "failed", "error", "cancelled", "interrupted", "idle"].includes(
        row.status ?? "",
      ),
    );
  return (
    <>
      {props.renderGroup({
        id: "continuations",
        label: "Continuations",
        rows: props.entries.filter((e) => e.group === "continuations"),
        expanded: true,
      })}
      {props.renderGroup({
        id: "parents",
        label: "Parents",
        rows: props.entries.filter((e) => e.group === "parents"),
        expanded: true,
      })}
      {delegated.length > 0 ? (
        <div role="group" aria-label="Delegated tasks">
          <h4 className="flex min-h-8 items-center px-2 text-2xs font-medium text-muted-foreground">
            Delegated tasks
          </h4>
          {props.renderGroup({
            id: "active",
            label: null,
            rows: delegated.filter((e) => !previous(e)),
            expanded: true,
          })}
          {props.renderGroup({
            id: "previous",
            label: "Previous agents",
            rows: delegated.filter(previous),
            expanded: false,
          })}
        </div>
      ) : null}
      {props.renderGroup({
        id: "children",
        label: "Children",
        rows: props.entries.filter((e) => e.group === "children"),
        expanded: false,
      })}
      {props.renderGroup({
        id: "siblings",
        label: "Siblings & peers",
        rows: props.entries.filter((e) => e.group === "siblings"),
        expanded: false,
      })}
    </>
  );
}
