import { describe, expect, it } from "vite-plus/test";
import type { PromptItem } from "./promptItems";
import { promptWorkstreams } from "./promptWorkstreams";
import { validatePromptSearch } from "./promptSearch";

const threadKey = JSON.stringify(["mini", "thread-1"]);
const prompt: PromptItem = {
  key: "voice:speech",
  stage: "sent",
  origin: "voice",
  draft: null,
  entry: null,
  text: { kind: "unavailable" },
  destinations: [
    {
      key: "destination",
      deliveryId: "delivery",
      queueRequestId: "request",
      threadId: "thread-1",
      targetEnvironmentRef: "mini",
      targetHostId: null,
      backend: null,
      instance: null,
      queueItem: null,
      handoff: null,
    },
  ],
  unlinkedDeliveries: null,
  time: null,
  timeKind: null,
};

const sources = {
  environmentId: "mini",
  queueBound: true,
  names: new Map([["docs", { name: "Docs", color: "#123456" }]]),
  threadAssignments: new Map([[threadKey, { workstreamId: "docs" }]]),
};

describe("Prompt Workstream provenance", () => {
  it("only marks the observed assignment from the matching environment and thread", () => {
    expect(promptWorkstreams(prompt, sources)).toMatchObject([
      { provenance: "from_thread", id: "docs", assignedAutomatically: false },
    ]);
    expect(
      promptWorkstreams(prompt, { ...sources, autoAssignedThreadKeys: new Set() })[0]
        ?.assignedAutomatically,
    ).toBe(false);
    expect(
      promptWorkstreams(prompt, { ...sources, autoAssignedThreadKeys: new Set([threadKey]) })[0]
        ?.assignedAutomatically,
    ).toBe(true);
    expect(
      promptWorkstreams(prompt, {
        ...sources,
        environmentId: "laptop",
        autoAssignedThreadKeys: new Set([threadKey]),
      }),
    ).toEqual([]);
  });
});

describe("Prompt deep links", () => {
  it("binds a valid stage, prompt and compatible controls and discards invalid values", () => {
    expect(
      validatePromptSearch({
        source: "mini",
        stage: "queued",
        prompt: "queue:request",
        filter: "waiting",
        group: "queue-state",
        ws: "mini:docs",
        query: "transient",
      }),
    ).toEqual({
      source: "mini",
      stage: "queued",
      prompt: "queue:request",
      filter: "waiting",
      group: "queue-state",
      ws: "mini:docs",
    });
    expect(
      validatePromptSearch({
        source: "x".repeat(1025),
        stage: "pending",
        prompt: "request",
        filter: "waiting",
        group: "queue-state",
        ws: ["mini", "docs"],
      }),
    ).toEqual({
      source: undefined,
      stage: "pending",
      prompt: undefined,
      filter: undefined,
      group: undefined,
      ws: undefined,
    });
  });
});
