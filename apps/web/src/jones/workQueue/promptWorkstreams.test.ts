import { describe, expect, it } from "vite-plus/test";
import type { PersonalWorkstreamsState } from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { PromptItem } from "./promptItems";
import { promptWorkstreams } from "./promptWorkstreams";
import { configuredHostedAppUrl } from "../../hostedPairing";
import { promptLink, validatePromptSearch } from "./promptSearch";
import { sentThreadWorkstream, sentWorkstreamGroup, sentWorkstreamLines } from "./sentWorkstreams";

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

describe("Sent Workstream membership", () => {
  const environmentId = EnvironmentId.make("mini");
  const threadId = ThreadId.make("thread-1");
  const source = { environmentId, hostId: "host", expiresAt: 100 };
  const destination = {
    ...prompt.destinations[0]!,
    targetHostId: source.hostId,
    handoff: {
      id: "request",
      target: { threadId, label: null },
      sentAt: 20,
      text: null,
      sourceLabel: null,
      workstreams: ["requested-by-queue"],
      confirmation: {
        status: "confirmed",
        kind: "handoff-receipt",
        receiptId: "receipt",
        source: "queue / host / mini",
      },
    },
  } as const;
  const state: PersonalWorkstreamsState = {
    environments: [{ environmentId, status: "ready", confirmed: true, workstreamCount: 0 }],
    workstreams: [],
    assignmentsByThreadKey: new Map(),
    pendingThreadKeys: new Set(),
    canCreate: true,
  };

  it("requires a current exact receipt and a live observation of the receiving environment", () => {
    const live = new Map();
    expect(sentThreadWorkstream(destination, source, state, live, 99)).toMatchObject({
      kind: "unassigned",
      revision: 0,
    });
    for (const variant of [
      { ...destination, targetEnvironmentRef: "other" },
      { ...destination, targetHostId: "other" },
      { ...destination, threadId: "other" },
      { ...destination, handoff: null },
    ]) {
      expect(sentThreadWorkstream(variant, source, state, live, 99).kind).toBe("unknown");
    }
    expect(sentThreadWorkstream(destination, source, state, live, 100).kind).toBe("unknown");
    expect(sentThreadWorkstream(destination, source, state, null, 99).kind).toBe("unknown");
    for (const status of ["sync_failed", "offline", "update_required"] as const) {
      const stale = { ...state, environments: [{ ...state.environments[0]!, status }] };
      expect(sentThreadWorkstream(destination, source, stale, live, 99).kind).toBe("unknown");
    }
  });

  it("keeps assignments whose group definition is masked, and ignores optimistic moves", () => {
    const assignment = { threadId, workstreamId: "missing-group", revision: 4, updatedAt: null };
    const masked = {
      ...state,
      assignmentsByThreadKey: new Map([[threadKey, { ...assignment, workstreamId: null }]]),
    };
    const observed = sentThreadWorkstream(
      destination,
      source,
      masked,
      new Map([[threadId, assignment]]),
      99,
    );
    expect(observed).toMatchObject({ kind: "assigned", line: { id: "missing-group", name: null } });
    expect(sentWorkstreamLines([observed, observed])).toHaveLength(1);
    expect(sentWorkstreamGroup([observed, observed]).label).toBe("missing-group");
    expect(sentThreadWorkstream(destination, source, masked, new Map(), 99).kind).toBe("unknown");
    const pending = { ...masked, pendingThreadKeys: new Set([threadKey]) };
    expect(sentThreadWorkstream(destination, source, pending, new Map(), 99)).toMatchObject({
      kind: "unassigned",
      revision: 0,
    });
  });
});

describe("Prompt deep links", () => {
  it("accepts Unassigned only on Sent", () => {
    expect(validatePromptSearch({ stage: "sent", filter: "unassigned" }).filter).toBe("unassigned");
    expect(validatePromptSearch({ stage: "queued", filter: "unassigned" }).filter).toBeUndefined();
  });
  it("routes hosted links to live Prompts and preserves the locally served route", () => {
    const search = { source: "mini", stage: "queued", prompt: "queue:request" } as const;
    const hosted = new URL(
      promptLink(search, new URL("/work-queue", configuredHostedAppUrl()).href),
    );
    const local = new URL(promptLink(search, "http://127.0.0.1:7375/work-queue"));
    expect(hosted.pathname).toBe("/voice-review");
    expect(local.pathname).toBe("/work-queue");
    expect(Object.fromEntries(hosted.searchParams)).toEqual(search);
    expect(Object.fromEntries(local.searchParams)).toEqual(search);
  });
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
