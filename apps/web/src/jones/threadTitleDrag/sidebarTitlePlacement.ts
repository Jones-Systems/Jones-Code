import {
  resolveSidebarDropTarget,
  sidebarListItemId,
  type SidebarListItem,
  type SidebarSection,
} from "../../components/Sidebar.logic";

export function locateSidebarTitlePlacement(
  list: HTMLUListElement,
  point: { x: number; y: number },
  items: readonly SidebarListItem[],
  key: string,
  section: SidebarSection,
) {
  const row = list.ownerDocument
    .elementFromPoint(point.x, point.y)
    ?.closest<HTMLElement>("[data-thread-item]");
  const neighbor = row?.dataset.threadItem;
  if (!row || row.parentElement !== list || !neighbor || neighbor === key) return null;

  const remaining = items.filter((item) => sidebarListItemId(item) !== key);
  const neighborIndex = remaining.findIndex(
    (item) => item.kind === "thread" && item.key === neighbor,
  );
  const item = remaining[neighborIndex];
  if (item?.kind !== "thread" || (item.section !== "active" && item.section !== "pinned"))
    return null;

  const bounds = row.getBoundingClientRect();
  const after = point.y >= (bounds.top + bounds.bottom) / 2;
  // The planner takes a final index. Put the source at the end so the target
  // at the desired insertion index can also represent the final slot.
  const withSource = [...remaining, { kind: "thread", key, section } as const];
  const target = resolveSidebarDropTarget(
    withSource,
    key,
    sidebarListItemId(withSource[neighborIndex + Number(after)]!),
  );
  return target ? { row, after, target } : null;
}

export function showSidebarTitlePlacement(
  previous: HTMLElement | null,
  position: { row: HTMLElement; after: boolean } | null,
) {
  if (previous !== position?.row) previous?.removeAttribute("data-thread-title-insertion");
  if (position) position.row.dataset.threadTitleInsertion = position.after ? "after" : "before";
  return position?.row ?? null;
}
