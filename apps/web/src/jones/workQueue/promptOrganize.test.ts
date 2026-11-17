import { describe, expect, it } from "vite-plus/test";
import type { PromptAttention } from "./promptAttention";
import type { PromptItem } from "./promptItems";
import {
  combinePromptCounts,
  filterPromptItems,
  groupPromptItems,
  promptFilterOptions,
  promptGroupOptions,
  promptSearchIdentifiers,
  searchPromptItems,
  sortPromptItems,
  type PromptOrganizeContext,
} from "./promptOrganize";

const item = (key: string, overrides: Partial<PromptItem> = {}): PromptItem => ({
  key,
  stage: "queued",
  origin: "queue",
  draft: null,
  entry: null,
  text: { kind: "not_carried" },
  destinations: [],
  unlinkedDeliveries: null,
  time: null,
  timeKind: null,
  ...overrides,
});
const bands = new Map<string, PromptAttention>();
const workstreams = new Map<string, string>();
const context: PromptOrganizeContext = {
  attention: (prompt) => bands.get(prompt.key) ?? { needsYou: false, band: "other" },
  destinationLabel: (prompt) => `Destination ${prompt.key.at(-1)}`,
  workstream: (prompt) => {
    const label = workstreams.get(prompt.key);
    return label ? { key: label, label } : null;
  },
  queueStateLabel: () => "Queued",
};

describe("organizing prompts", () => {
  it("offers only the groupings and filters each stage supports", () => {
    expect(promptGroupOptions.pending).toEqual(["default", "destination"]);
    expect(promptGroupOptions.queued).toContain("queue-state");
    expect(promptFilterOptions.sent).toEqual(["all", "needs-you"]);
  });
  it("orders Pending by band, then soonest release, then oldest", () => {
    bands.clear();
    bands.set("a", { needsYou: false, band: "paused" });
    bands.set("b", { needsYou: true, band: "needs-you" });
    bands.set("c", { needsYou: false, band: "releasing" });
    bands.set("d", { needsYou: false, band: "releasing" });
    const pending = (key: string, remaining: number | null, time: number) =>
      item(key, {
        stage: "pending",
        time,
        draft: { remaining_ms: remaining } as PromptItem["draft"],
      });
    const sorted = sortPromptItems(
      [pending("a", null, 1), pending("c", 9000, 2), pending("d", 1000, 3), pending("b", null, 4)],
      "pending",
      context,
    );
    expect(sorted.map((prompt) => prompt.key)).toEqual(["b", "d", "c", "a"]);
    const groups = groupPromptItems(sorted, "pending", "default", context);
    expect(groups.map((group) => [group.label, group.items.length])).toEqual([
      ["Needs you", 1],
      ["Releasing", 2],
      ["Paused", 1],
    ]);
  });
  it("puts high-lane queued work first and newest Sent first", () => {
    const high = item("h", {
      time: 1,
      destinations: [{ queueItem: { lane: "high" } } as PromptItem["destinations"][number]],
    });
    const sorted = sortPromptItems(
      [item("n", { time: 5 }), high, item("o", { time: 2 })],
      "queued",
      context,
    );
    expect(sorted.map((prompt) => prompt.key)).toEqual(["h", "n", "o"]);
    expect(
      sortPromptItems([item("x", { time: 1 }), item("y", { time: 3 })], "sent", context).map(
        (prompt) => prompt.key,
      ),
    ).toEqual(["y", "x"]);
  });
  it("groups Queued by Workstream with No workstream last", () => {
    workstreams.clear();
    workstreams.set("a", "Beta");
    workstreams.set("b", "Alpha");
    const groups = groupPromptItems(
      [item("a"), item("b"), item("c")],
      "queued",
      "default",
      context,
    );
    expect(groups.map((group) => group.label)).toEqual(["Alpha", "Beta", "No workstream"]);
    expect(groupPromptItems([item("a")], "queued", "destination", context)[0]!.label).toBe(
      "Destination a",
    );
    expect(groupPromptItems([item("a")], "queued", "queue-state", context)[0]!.label).toBe(
      "Queued",
    );
    expect(groupPromptItems([item("a"), item("b")], "queued", "none", context)).toHaveLength(1);
    expect(groupPromptItems([], "queued", "none", context)).toEqual([]);
  });
  it("filters by attention band and Workstream", () => {
    bands.clear();
    bands.set("a", { needsYou: true, band: "needs-you" });
    bands.set("b", { needsYou: false, band: "waiting" });
    bands.set("c", { needsYou: false, band: "paused" });
    const items = [item("a"), item("b"), item("c")];
    const keys = (list: readonly PromptItem[]) => list.map((prompt) => prompt.key);
    expect(keys(filterPromptItems(items, "all", context))).toEqual(["a", "b", "c"]);
    expect(keys(filterPromptItems(items, "needs-you", context))).toEqual(["a"]);
    expect(keys(filterPromptItems(items, "waiting", context))).toEqual(["b"]);
    expect(keys(filterPromptItems(items, "paused", context))).toEqual(["c"]);
    expect(
      keys(
        filterPromptItems(items, "all", context, "ws", (prompt) =>
          prompt.key === "b" ? ["ws"] : [],
        ),
      ),
    ).toEqual(["b"]);
  });
  it("searches literal text and exact identifiers", () => {
    const prompt = item("queue:request", {
      text: { kind: "available", text: "Fix the Login page" },
      destinations: [
        {
          key: "request",
          deliveryId: "delivery-7",
          queueRequestId: "request-7",
          threadId: "thread-7",
          targetEnvironmentRef: null,
          targetHostId: null,
          backend: null,
          instance: null,
          queueItem: null,
          handoff: null,
        },
      ],
    });
    expect(searchPromptItems([prompt], "login", promptSearchIdentifiers)).toEqual([prompt]);
    expect(searchPromptItems([prompt], "thread-7", promptSearchIdentifiers)).toEqual([prompt]);
    expect(searchPromptItems([prompt], "absent", promptSearchIdentifiers)).toEqual([]);
    expect(searchPromptItems([prompt], "  ", promptSearchIdentifiers)).toEqual([prompt]);
  });
});

describe("stage counts", () => {
  it("is unknown until a source reports", () => {
    expect(combinePromptCounts([])).toBeNull();
    expect(combinePromptCounts([null])).toBeNull();
  });
  it("sums known counts and keeps the weakest qualifier", () => {
    expect(
      combinePromptCounts([
        { count: 2, qualifier: "" },
        { count: 3, qualifier: "" },
      ]),
    ).toEqual({
      count: 5,
      qualifier: "",
    });
    expect(
      combinePromptCounts([
        { count: 2, qualifier: "+" },
        { count: 1, qualifier: "" },
      ]),
    ).toEqual({
      count: 3,
      qualifier: "+",
    });
    expect(combinePromptCounts([{ count: 2, qualifier: "+" }, null])).toEqual({
      count: 2,
      qualifier: "?",
    });
    expect(combinePromptCounts([{ count: 1, qualifier: "?" }])).toEqual({
      count: 1,
      qualifier: "?",
    });
  });
});
