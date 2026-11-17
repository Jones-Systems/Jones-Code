import type { PromptStage } from "./promptItems";
import {
  promptFilterOptions,
  promptGroupOptions,
  type PromptFilter,
  type PromptGroupBy,
} from "./promptOrganize";

export interface PromptSearch {
  readonly source?: string | undefined;
  readonly stage?: PromptStage | undefined;
  readonly prompt?: string | undefined;
  readonly filter?: PromptFilter | undefined;
  readonly group?: PromptGroupBy | undefined;
  readonly ws?: string | undefined;
}

export interface PromptNavigation {
  readonly search: PromptSearch;
  readonly replace: (patch: PromptSearch) => void;
}

function bounded(value: unknown) {
  return typeof value === "string" && value.length > 0 && value.length <= 1024 ? value : undefined;
}

export function validatePromptSearch(raw: Record<string, unknown>): PromptSearch {
  const stage =
    raw.stage === "pending" || raw.stage === "queued" || raw.stage === "sent"
      ? raw.stage
      : undefined;
  const filter = bounded(raw.filter) as PromptFilter | undefined;
  const group = bounded(raw.group) as PromptGroupBy | undefined;
  const prompt = bounded(raw.prompt);
  return {
    source: bounded(raw.source),
    stage,
    prompt: prompt && /^(voice|queue):.+$/.test(prompt) ? prompt : undefined,
    filter: filter && promptFilterOptions[stage ?? "pending"].includes(filter) ? filter : undefined,
    group: group && promptGroupOptions[stage ?? "pending"].includes(group) ? group : undefined,
    ws: bounded(raw.ws),
  };
}

export function workstreamFilterKey(environmentId: string, id: string) {
  return `${encodeURIComponent(environmentId)}:${encodeURIComponent(id)}`;
}

export function promptLink(search: PromptSearch) {
  const url = new URL(window.location.href);
  url.search = "";
  for (const [key, value] of Object.entries(search)) {
    if (value !== undefined) url.searchParams.set(key, value);
  }
  return url.toString();
}
