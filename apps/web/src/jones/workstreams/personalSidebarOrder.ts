import type {
  PersonalThreadRef,
  PersonalWorkstreamsGrouping,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import * as Schema from "effect/Schema";
import { useSyncExternalStore } from "react";
import { readLocalStorageValue, subscribeLocalStorageKey } from "../../hooks/useLocalStorage";

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
  const keys = groups.map((group) => personalGroupCollapsedKey(group.id));
  const subscribe = (onChange: () => void) => {
    const stops = keys.map((key) => subscribeLocalStorageKey(key, onChange));
    return () => stops.forEach((stop) => stop());
  };
  // A primitive snapshot stays stable between writes, including when groups are reordered.
  const snapshot = useSyncExternalStore(subscribe, () =>
    keys.map((key) => (readLocalStorageValue(key, false, Schema.Boolean) ? "1" : "0")).join(""),
  );
  const currentKey = currentThread === null ? null : personalThreadKey(currentThread);
  return {
    visible: groups.flatMap((group, index) => (snapshot[index] === "1" ? [] : group.threads)),
    traversal: groups.flatMap((group, index) =>
      snapshot[index] === "1"
        ? group.threads.filter((thread) => personalThreadKey(thread) === currentKey)
        : group.threads,
    ),
  };
}
