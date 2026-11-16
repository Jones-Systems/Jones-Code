import type {
  PersonalThreadRef,
  PersonalWorkstreamsGrouping,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import * as Schema from "effect/Schema";
import { useSyncExternalStore } from "react";
import { readLocalStorageValue, subscribeLocalStorageKey } from "../../hooks/useLocalStorage";
import {
  personalThreadLimitKey,
  personalThreadExpandedKey,
  readPersonalThreadWindow,
  windowPersonalThreads,
  type PersonalThreadLimit,
} from "./personalThreadWindow";

export const personalGroupCollapsedKey = (id: string | null) =>
  id === null ? "jones:workstreams:unassigned-collapsed" : `jones:workstreams:collapsed:${id}`;

export function usePersonalSidebarActiveThreads<T extends PersonalThreadRef>(
  grouping: PersonalWorkstreamsGrouping<T>,
  currentThread: PersonalThreadRef | null,
): { visible: readonly T[]; traversal: readonly T[] } {
  const groups = [
    ...grouping.groups.map((group) => ({ id: group.workstream.id, threads: group.threads })),
    { id: null, threads: grouping.ungrouped },
  ];
  const keys = groups.flatMap((group) => [
    personalGroupCollapsedKey(group.id),
    ...(group.id === null
      ? []
      : [personalThreadLimitKey(group.id), personalThreadExpandedKey(group.id)]),
  ]);
  const subscribe = (onChange: () => void) => {
    const stops = keys.map((key) => subscribeLocalStorageKey(key, onChange));
    return () => stops.forEach((stop) => stop());
  };
  // A primitive snapshot stays stable between writes, including when groups are reordered.
  const snapshot = useSyncExternalStore(subscribe, () =>
    JSON.stringify(
      groups.map((group) => ({
        collapsed: readLocalStorageValue(
          personalGroupCollapsedKey(group.id),
          false,
          Schema.Boolean,
        ),
        ...(group.id === null ? { limit: 5, expanded: true } : readPersonalThreadWindow(group.id)),
      })),
    ),
  );
  const windows = JSON.parse(snapshot) as Array<{
    collapsed: boolean;
    limit: PersonalThreadLimit;
    expanded: boolean;
  }>;
  const currentKey = currentThread === null ? null : personalThreadKey(currentThread);
  const visibleGroups = groups.map((group, index) =>
    windows[index]!.collapsed
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
