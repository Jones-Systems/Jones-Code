import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  groupPersonalThreads,
  personalThreadKey,
  type PersonalThreadRef,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import * as Schema from "effect/Schema";
import { useSyncExternalStore } from "react";
import {
  readLocalStorageValue,
  subscribeLocalStorageKey,
  useLocalStorage,
} from "../../hooks/useLocalStorage";
import type { SidebarThreadSummary } from "../../types";
import type { ThreadStatusFilter } from "../threadStatusFilter/model";
import { useThreadStatusSelection } from "../threadStatusFilter/useThreadStatusFilter";

export type PersonalStatusFilter = ThreadStatusFilter | "hide-input" | "hide-done";

const PersonalStatusFilterSchema = Schema.NullOr(
  Schema.Literals(["all", "working", "input", "done", "hide-working", "hide-input", "hide-done"]),
);

export const personalStatusFilterKey = (id: string) => `jones:workstreams:status-filter:${id}`;

export function readPersonalStatusFilter(id: string): PersonalStatusFilter | null {
  return readLocalStorageValue(personalStatusFilterKey(id), null, PersonalStatusFilterSchema);
}

export function usePersonalStatusFilter(id: string) {
  const [filter, setFilter] = useLocalStorage(
    personalStatusFilterKey(id),
    null,
    PersonalStatusFilterSchema,
  );
  return { filter, setFilter };
}

function usePersonalStatusFilters(state: PersonalWorkstreamsState) {
  const ids = state.workstreams.map((workstream) => workstream.id);
  const subscribe = (onChange: () => void) => {
    if (typeof window === "undefined") return () => {};
    const stops = ids.map((id) => subscribeLocalStorageKey(personalStatusFilterKey(id), onChange));
    return () => stops.forEach((stop) => stop());
  };
  const getSnapshot = () => JSON.stringify(ids.map((id) => readPersonalStatusFilter(id)));
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  const filters = JSON.parse(snapshot) as Array<PersonalStatusFilter | null>;
  return new Map(ids.map((id, index) => [id, filters[index] ?? null]));
}

type LocalStatusSelections<T> = Readonly<Record<ThreadStatusFilter, readonly T[]>>;

export function selectPersonalStatusThreads<T extends PersonalThreadRef>(
  scopedThreads: readonly T[],
  state: PersonalWorkstreamsState,
  globalVisibleThreads: readonly T[],
  filters: ReadonlyMap<string, PersonalStatusFilter | null>,
  selections: LocalStatusSelections<T>,
): readonly T[] {
  const { workstreamIdByThreadKey } = groupPersonalThreads(state, scopedThreads);
  const globalKeys = new Set(globalVisibleThreads.map(personalThreadKey));
  const localKeys = {
    all: new Set(selections.all.map(personalThreadKey)),
    working: new Set(selections.working.map(personalThreadKey)),
    input: new Set(selections.input.map(personalThreadKey)),
    done: new Set(selections.done.map(personalThreadKey)),
    "hide-working": new Set(selections["hide-working"].map(personalThreadKey)),
  };
  return scopedThreads.filter((thread) => {
    const key = personalThreadKey(thread);
    const id = workstreamIdByThreadKey.get(key);
    const filter = id === undefined ? null : (filters.get(id) ?? null);
    if (filter === null) return globalKeys.has(key);
    if (filter === "hide-input") return !localKeys.input.has(key);
    if (filter === "hide-done") return !localKeys.done.has(key);
    return localKeys[filter].has(key);
  });
}

export function usePersonalStatusSelection(
  scopedThreads: readonly SidebarThreadSummary[],
  controllerState: PersonalWorkstreamsState,
  globalVisibleThreads: readonly SidebarThreadSummary[],
  now?: string,
  activeThreadKey: string | null = null,
): readonly SidebarThreadSummary[] {
  const filters = usePersonalStatusFilters(controllerState);
  const { workstreamIdByThreadKey } = groupPersonalThreads(controllerState, scopedThreads);
  const filterFor = (thread: SidebarThreadSummary) => {
    const id = workstreamIdByThreadKey.get(personalThreadKey(thread));
    return id === undefined ? null : (filters.get(id) ?? null);
  };
  const candidates = (filter: PersonalStatusFilter, inverse?: PersonalStatusFilter) =>
    scopedThreads.filter((thread) => {
      const selected = filterFor(thread);
      return selected === filter || (inverse !== undefined && selected === inverse);
    });
  const retainedKey = (filter: PersonalStatusFilter) =>
    scopedThreads.some(
      (thread) =>
        scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id)) === activeThreadKey &&
        filterFor(thread) === filter,
    )
      ? activeThreadKey
      : null;
  // Scope each fixed hook to its consumers so changing a local filter resets route retention.
  const all = useThreadStatusSelection(candidates("all"), "all", "current", now).threads;
  const working = useThreadStatusSelection(
    candidates("working"),
    "working",
    "current",
    now,
  ).threads;
  const input = useThreadStatusSelection(
    candidates("input", "hide-input"),
    "input",
    "current",
    now,
    retainedKey("input"),
  ).threads;
  const done = useThreadStatusSelection(
    candidates("done", "hide-done"),
    "done",
    "current",
    now,
    retainedKey("done"),
  ).threads;
  const hideWorking = useThreadStatusSelection(
    candidates("hide-working"),
    "hide-working",
    "current",
    now,
  ).threads;
  return selectPersonalStatusThreads(
    scopedThreads,
    controllerState,
    globalVisibleThreads,
    filters,
    {
      all,
      working,
      input,
      done,
      "hide-working": hideWorking,
    },
  );
}
