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
  writeLocalStorageValue,
} from "../../hooks/useLocalStorage";
import type { SidebarThreadSummary } from "../../types";
import type { ThreadStatusFilter } from "../threadStatusFilter/model";
import { useThreadStatusSelection } from "../threadStatusFilter/useThreadStatusFilter";

export type PersonalStatusFilter = ThreadStatusFilter | "hide-input" | "hide-done";

/**
 * A stored choice carries a revision so the shared "Workstreams" pill and each
 * group's own pill order themselves by recency instead of by who wrote last
 * into one key. `filter: null` is a tombstone: "follow the global filter".
 */
export interface PersonalStatusChoice {
  readonly filter: PersonalStatusFilter | null;
  readonly rev: number;
}

const FilterSchema = Schema.Literals([
  "all",
  "working",
  "input",
  "done",
  "hide-working",
  "hide-input",
  "hide-done",
]);
const ChoiceSchema = Schema.Struct({
  filter: Schema.NullOr(FilterSchema),
  rev: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
// Earlier builds stored the bare filter literal; read it as revision 0 and write only the struct.
const StoredLocalSchema = Schema.NullOr(Schema.Union([FilterSchema, ChoiceSchema]));
const StoredChoiceSchema = Schema.NullOr(ChoiceSchema);

export const personalStatusFilterKey = (id: string) => `jones:workstreams:status-filter:${id}`;
export const personalSharedStatusFilterKey = "jones:workstreams:shared-status-filter";

export function readPersonalStatusChoice(id: string): PersonalStatusChoice | null {
  const stored = readLocalStorageValue(personalStatusFilterKey(id), null, StoredLocalSchema);
  return typeof stored === "string" ? { filter: stored, rev: 0 } : stored;
}

export function readSharedPersonalStatusChoice(): PersonalStatusChoice | null {
  return readLocalStorageValue(personalSharedStatusFilterKey, null, StoredChoiceSchema);
}

/** The group's own stored filter, ignoring the shared choice. */
export function readPersonalStatusFilter(id: string): PersonalStatusFilter | null {
  return readPersonalStatusChoice(id)?.filter ?? null;
}

/**
 * The group's own choice wins while it is at least as recent as the shared
 * one; otherwise the shared choice applies. `null` follows the global filter.
 */
export function effectivePersonalStatusFilter(
  local: PersonalStatusChoice | null,
  shared: PersonalStatusChoice | null,
): PersonalStatusFilter | null {
  if (local !== null && local.rev >= (shared?.rev ?? 0)) return local.filter;
  return shared?.filter ?? null;
}

/** A group's write never ticks the sequence; it is only never older than the shared choice. */
export function stampLocalPersonalStatusChoice(
  filter: PersonalStatusFilter | null,
  shared: PersonalStatusChoice | null,
  own: PersonalStatusChoice | null,
): PersonalStatusChoice {
  return { filter, rev: Math.max(shared?.rev ?? 0, own?.rev ?? 0) };
}

/** A shared write is newer than every present group's choice, including after the shared key was lost. */
export function nextSharedPersonalStatusChoice(
  filter: PersonalStatusFilter | null,
  shared: PersonalStatusChoice | null,
  locals: readonly (PersonalStatusChoice | null)[],
): PersonalStatusChoice {
  return {
    filter,
    rev: 1 + Math.max(shared?.rev ?? 0, ...locals.map((local) => local?.rev ?? 0)),
  };
}

function writeChoice(key: string, choice: PersonalStatusChoice) {
  // A struct never equals null, so clearing keeps its revision instead of removing the key.
  writeLocalStorageValue(key, choice, null, StoredChoiceSchema);
}

export function setPersonalStatusFilter(id: string, filter: PersonalStatusFilter | null) {
  writeChoice(
    personalStatusFilterKey(id),
    stampLocalPersonalStatusChoice(
      filter,
      readSharedPersonalStatusChoice(),
      readPersonalStatusChoice(id),
    ),
  );
}

export function setSharedPersonalStatusFilter(
  ids: readonly string[],
  filter: PersonalStatusFilter | null,
) {
  writeChoice(
    personalSharedStatusFilterKey,
    nextSharedPersonalStatusChoice(
      filter,
      readSharedPersonalStatusChoice(),
      ids.map(readPersonalStatusChoice),
    ),
  );
}

type FilterUpdate =
  | PersonalStatusFilter
  | null
  | ((current: PersonalStatusFilter | null) => PersonalStatusFilter | null);

function usePersonalStatusChoices(ids: readonly string[]) {
  const subscribe = (onChange: () => void) => {
    if (typeof window === "undefined") return () => {};
    const stops = [personalSharedStatusFilterKey, ...ids.map(personalStatusFilterKey)].map((key) =>
      subscribeLocalStorageKey(key, onChange),
    );
    return () => stops.forEach((stop) => stop());
  };
  const getSnapshot = () =>
    JSON.stringify([readSharedPersonalStatusChoice(), ...ids.map(readPersonalStatusChoice)]);
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  const [shared, ...locals] = JSON.parse(snapshot) as Array<PersonalStatusChoice | null>;
  return { shared: shared ?? null, locals };
}

export function usePersonalStatusFilter(id: string) {
  const { shared, locals } = usePersonalStatusChoices([id]);
  const filter = effectivePersonalStatusFilter(locals[0] ?? null, shared);
  const setFilter = (update: FilterUpdate) => {
    const next =
      typeof update === "function"
        ? update(
            effectivePersonalStatusFilter(
              readPersonalStatusChoice(id),
              readSharedPersonalStatusChoice(),
            ),
          )
        : update;
    setPersonalStatusFilter(id, next);
  };
  return { filter, setFilter };
}

/** The Workstreams header pill: one choice applied to every named group. */
export function useSharedPersonalStatusFilter(ids: readonly string[]) {
  const [shared] = useLocalStorage(personalSharedStatusFilterKey, null, StoredChoiceSchema);
  const setFilter = (filter: PersonalStatusFilter | null) =>
    setSharedPersonalStatusFilter(ids, filter);
  return { filter: shared?.filter ?? null, setFilter };
}

function usePersonalStatusFilters(state: PersonalWorkstreamsState) {
  const ids = state.workstreams.map((workstream) => workstream.id);
  const { shared, locals } = usePersonalStatusChoices(ids);
  return new Map(
    ids.map((id, index) => [id, effectivePersonalStatusFilter(locals[index] ?? null, shared)]),
  );
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
