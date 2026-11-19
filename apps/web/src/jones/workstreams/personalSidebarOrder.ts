import type {
  PersonalThreadRef,
  PersonalWorkstreamsGrouping,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import * as Schema from "effect/Schema";
import { useLayoutEffect, useRef, useSyncExternalStore } from "react";
import {
  readLocalStorageValue,
  subscribeLocalStorageKey,
  writeLocalStorageValue,
} from "../../hooks/useLocalStorage";
import {
  personalThreadLimitKey,
  personalThreadExpandedKey,
  readPersonalThreadWindow,
  windowPersonalThreads,
  type PersonalThreadLimit,
} from "./personalThreadWindow";

export const personalGroupCollapsedKey = (id: string | null) =>
  id === null ? "jones:workstreams:unassigned-collapsed" : `jones:workstreams:collapsed:${id}`;

/** Hides every named group and its threads; each group's own collapsed state is left untouched. */
export const personalNamedGroupsHiddenKey = "jones:workstreams:named-hidden";

/**
 * Expands or collapses every given group. Collapsing also returns a group's
 * thread window to its saved default, exactly as collapsing one group does.
 */
export function setPersonalGroupsCollapsed(ids: readonly string[], collapsed: boolean) {
  for (const id of ids) {
    writeLocalStorageValue(personalGroupCollapsedKey(id), collapsed, false, Schema.Boolean);
    if (collapsed)
      writeLocalStorageValue(personalThreadExpandedKey(id), false, false, Schema.Boolean);
  }
}

export function usePersonalSidebarActiveThreads<T extends PersonalThreadRef>(
  grouping: PersonalWorkstreamsGrouping<T>,
  currentThread: PersonalThreadRef | null,
): { visible: readonly T[]; traversal: readonly T[] } {
  const groups = [
    { id: null, threads: grouping.ungrouped },
    ...grouping.groups.map((group) => ({ id: group.workstream.id, threads: group.threads })),
  ];
  const keys = [
    personalNamedGroupsHiddenKey,
    ...groups.flatMap((group) => [
      personalGroupCollapsedKey(group.id),
      ...(group.id === null
        ? []
        : [personalThreadLimitKey(group.id), personalThreadExpandedKey(group.id)]),
    ]),
  ];
  const subscribe = (onChange: () => void) => {
    const stops = keys.map((key) => subscribeLocalStorageKey(key, onChange));
    return () => stops.forEach((stop) => stop());
  };
  // A primitive snapshot stays stable between writes, including when groups are reordered.
  const snapshot = useSyncExternalStore(subscribe, () =>
    JSON.stringify({
      hidden: readLocalStorageValue(personalNamedGroupsHiddenKey, false, Schema.Boolean),
      windows: groups.map((group) => ({
        collapsed: readLocalStorageValue(
          personalGroupCollapsedKey(group.id),
          false,
          Schema.Boolean,
        ),
        ...(group.id === null ? { limit: 5, expanded: true } : readPersonalThreadWindow(group.id)),
      })),
    }),
  );
  const { hidden, windows } = JSON.parse(snapshot) as {
    hidden: boolean;
    windows: Array<{
      collapsed: boolean;
      limit: PersonalThreadLimit;
      expanded: boolean;
    }>;
  };
  const currentKey = currentThread === null ? null : personalThreadKey(currentThread);
  // Hiding the named groups removes their rows like collapsing each of them, without writing their state.
  const visibleGroups = groups.map((group, index) =>
    windows[index]!.collapsed || (hidden && group.id !== null)
      ? []
      : windowPersonalThreads(group.threads, windows[index]!.limit, windows[index]!.expanded),
  );
  return {
    visible: visibleGroups.flat(),
    traversal: groups.flatMap((group, index) => {
      const visible = visibleGroups[index]!;
      if (visible.length === group.threads.length || currentKey === null) return visible;
      const anchor = group.threads
        .slice(visible.length)
        .find((thread) => personalThreadKey(thread) === currentKey);
      return anchor ? [...visible, anchor] : visible;
    }),
  };
}

export function useHiddenPersonalSidebarNeighbor(
  currentThreadId: string | null,
  traversalThreadIds: readonly string[],
  getUnwindowedThreadIds: () => readonly string[],
) {
  const lastPosition = useRef<{
    threadId: string;
    getOrder: () => readonly string[];
  } | null>(null);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Remember the committed order for the current route before a status filter removes its row.
  useLayoutEffect(() => {
    if (lastPosition.current?.threadId !== currentThreadId) lastPosition.current = null;
    if (currentThreadId !== null && traversalThreadIds.includes(currentThreadId)) {
      lastPosition.current = { threadId: currentThreadId, getOrder: getUnwindowedThreadIds };
    }
  }, [currentThreadId, traversalThreadIds, getUnwindowedThreadIds]);

  return (direction: "next" | "previous") => {
    const position = lastPosition.current;
    if (position === null || traversalThreadIds.includes(position.threadId)) return null;

    const order = position.getOrder();
    const currentIndex = order.indexOf(position.threadId);
    if (currentIndex === -1) return null;
    const visible = new Set(traversalThreadIds);
    const step = direction === "next" ? 1 : -1;
    for (let index = currentIndex + step; index >= 0 && index < order.length; index += step) {
      const candidate = order[index]!;
      if (visible.has(candidate)) return candidate;
    }
    return null;
  };
}
