import type {
  PersonalThreadRef,
  PersonalWorkstreamsGrouping,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import * as Schema from "effect/Schema";
import { useSyncExternalStore } from "react";
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
    ...grouping.groups.map((group) => ({ id: group.workstream.id, threads: group.threads })),
    { id: null, threads: grouping.ungrouped },
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
