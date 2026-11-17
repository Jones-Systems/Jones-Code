import * as Schema from "effect/Schema";
import { XIcon } from "lucide-react";
import { useState } from "react";
import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import {
  Menu,
  MenuRadioGroup,
  MenuRadioItem,
  MenuPopup,
  MenuTrigger,
} from "../../components/ui/menu";
import { Toggle, ToggleGroup } from "../../components/ui/toggle-group";
import { useLocalStorage } from "../../hooks/useLocalStorage";
import type { PromptStage } from "./promptItems";
import {
  filterPromptItems,
  groupPromptItems,
  promptSearchIdentifiers,
  searchPromptItems,
  promptFilterOptions,
  promptGroupOptions,
  type PromptFilter,
  type PromptGroupBy,
  type PromptOrganizeContext,
} from "./promptOrganize";
import type { PromptItem } from "./promptItems";
import type { PromptNavigation } from "./promptSearch";
import type { PromptWorkstreamLine } from "./promptWorkstreams";

const filterLabels = {
  all: "All",
  "needs-you": "Needs you",
  paused: "Paused",
  waiting: "Waiting",
} as const;
const groupLabels = {
  destination: "Destination",
  "queue-state": "Queue state",
  none: "None",
} as const;
const prefsSchema = Schema.Struct({
  filter: Schema.Literals(["all", "needs-you", "paused", "waiting"]),
  group: Schema.Literals(["default", "destination", "queue-state", "none"]),
});
const defaults: typeof prefsSchema.Type = { filter: "all", group: "default" };

export interface PromptListView {
  readonly filter: PromptFilter;
  readonly group: PromptGroupBy;
  readonly query: string;
  readonly key: string;
  readonly workstream: { readonly key: string; readonly label: string } | null;
  readonly filterWorkstream: (line: PromptWorkstreamLine) => void;
  readonly clear: () => void;
}

export function usePromptListView(stage: PromptStage, navigation?: PromptNavigation) {
  const [prefs, setPrefs] = useLocalStorage(
    `jones.prompts.list.${stage}.v1`,
    defaults,
    prefsSchema,
  );
  const [query, setQuery] = useState("");
  const [localWorkstream, setLocalWorkstream] = useState<{ key: string; label: string } | null>(
    null,
  );
  const filter = navigation?.search.filter ?? prefs.filter;
  const group = navigation?.search.group ?? prefs.group;
  const workstreamKey = navigation ? navigation.search.ws : localWorkstream?.key;
  const workstream = workstreamKey
    ? {
        key: workstreamKey,
        label: localWorkstream?.key === workstreamKey ? localWorkstream.label : "Workstream",
      }
    : null;
  const setFilter = (filter: PromptFilter) => {
    setPrefs({ ...prefs, filter });
    navigation?.replace({ filter, stage });
  };
  const setGroup = (group: PromptGroupBy) => {
    setPrefs({ ...prefs, group });
    navigation?.replace({ group, stage });
  };
  const clearWorkstream = () => {
    setLocalWorkstream(null);
    navigation?.replace({ ws: undefined });
  };
  const view: PromptListView = {
    filter: promptFilterOptions[stage].includes(filter) ? filter : "all",
    group: promptGroupOptions[stage].includes(group) ? group : "default",
    query,
    key: JSON.stringify([filter, group, query, workstreamKey]),
    workstream,
    filterWorkstream: (line) => {
      setLocalWorkstream({ key: line.filterKey, label: line.name ?? line.id });
      navigation?.replace({ ws: line.filterKey });
    },
    clear: () => {
      setQuery("");
      setLocalWorkstream(null);
      setPrefs(defaults);
      navigation?.replace({ filter: "all", group: "default", ws: undefined, stage });
    },
  };
  return { view, setFilter, setGroup, setQuery, clearWorkstream };
}

export function PromptToolbar({
  stage,
  controls,
}: {
  readonly stage: PromptStage;
  readonly controls: ReturnType<typeof usePromptListView>;
}) {
  const { view, setFilter, setGroup, setQuery, clearWorkstream } = controls;
  const groups = promptGroupOptions[stage];
  const groupLabel = (value: PromptGroupBy) =>
    value === "default" ? (stage === "pending" ? "Attention" : "Workstream") : groupLabels[value];
  return (
    <div
      role="group"
      aria-label="Prompt filters"
      className="flex min-w-0 flex-wrap items-center gap-2"
    >
      <ToggleGroup
        aria-label="Filter prompts"
        value={[view.filter]}
        variant="segmented"
        onValueChange={(next) => {
          const filter = promptFilterOptions[stage].find((value) => value === next[0]);
          if (filter) setFilter(filter);
        }}
      >
        {promptFilterOptions[stage].map((filter) => (
          <Toggle key={filter} value={filter}>
            {filterLabels[filter]}
          </Toggle>
        ))}
      </ToggleGroup>
      {view.workstream ? (
        <Button variant="outline" size="compact" className="max-w-full" onClick={clearWorkstream}>
          <span className="truncate">{view.workstream.label}</span>
          <XIcon aria-hidden />
          <span className="sr-only">Clear Workstream filter</span>
        </Button>
      ) : null}
      <div className="w-full sm:w-56">
        <Input
          size="sm"
          type="search"
          aria-label="Search prompts"
          placeholder="Search prompts"
          value={view.query}
          onChange={(event) => setQuery(event.target.value)}
        />
      </div>
      <Menu>
        <MenuTrigger
          render={<Button size="compact" variant="outline" />}
          aria-label="Group prompts"
        >
          Group by {groupLabel(view.group)}
        </MenuTrigger>
        <MenuPopup align="start">
          <MenuRadioGroup
            value={view.group}
            onValueChange={(next) => {
              const group = groups.find((value) => value === next);
              if (group) setGroup(group);
            }}
          >
            {groups.map((group) => (
              <MenuRadioItem key={group} value={group}>
                {groupLabel(group)}
              </MenuRadioItem>
            ))}
          </MenuRadioGroup>
        </MenuPopup>
      </Menu>
    </div>
  );
}

export function filteredPromptGroups(
  items: readonly PromptItem[],
  stage: PromptStage,
  view: PromptListView | undefined,
  context: PromptOrganizeContext,
  lines: (item: PromptItem) => readonly PromptWorkstreamLine[],
) {
  if (!view) return groupPromptItems(items, stage, "default", context);
  const filtered = filterPromptItems(
    items,
    view.filter,
    context,
    view.workstream?.key ?? null,
    (item) => lines(item).map((line) => line.filterKey),
  );
  const found = searchPromptItems(filtered, view.query, (item) => [
    ...promptSearchIdentifiers(item),
    context.destinationLabel(item),
    ...lines(item).flatMap((line) => [line.name, line.id]),
  ]);
  return groupPromptItems(found, stage, view.group, context);
}

export function PromptFilteredEmpty({ view }: { readonly view: PromptListView }) {
  return (
    <div className="space-y-2 rounded-md border p-4 text-sm">
      <p>No prompts match.</p>
      <Button variant="outline" size="compact" onClick={view.clear}>
        Clear filters
      </Button>
    </div>
  );
}
