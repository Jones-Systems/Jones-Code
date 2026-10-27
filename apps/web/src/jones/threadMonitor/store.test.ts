import { CommandId, MessageId, RuntimeRequestId, ThreadId } from "@t3tools/contracts";
import { monitorDraftKey } from "@t3tools/client-runtime/jones/thread-monitor";
import { describe, expect, it } from "vite-plus/test";
import { MonitorStore, emptyCounts, refOf } from "./store";
import { MonitorPeerStore } from "./peerStore";
import { monitorSnapshot } from "./snapshots";
import { testEnvironment, testRow } from "./fixtures.test-support";

function setup() {
  const store = new MonitorStore("generation");
  store.register(testEnvironment.environmentId);
  const row = testRow();
  store.setCohort([testEnvironment], [row], { ...emptyCounts(), waiting: 1 });
  return { store, row };
}
describe("main-owned monitor lifetime and drafts", () => {
  it("replays an unresolved input draft when an approval takes selection, then prunes only resolved requests", async () => {
    const { store, row } = setup();
    const input = RuntimeRequestId.make("input-a");
    const approval = RuntimeRequestId.make("approval-b");
    const selection = { ...refOf(row), selectionId: "selected" };
    store.select(selection);
    const detail = {
      ...selection,
      revision: 1,
      entries: [],
      request: null,
      plan: null,
      unavailableReason: null,
    };
    const draft = {
      ...store.draft(row, input),
      revision: 1,
      answers: { note: "unsent independent answer" },
    };
    store.setDetail(detail, new Set([input]));
    expect(store.edit(draft)).toBe(true);
    store.setDetail(
      {
        ...detail,
        revision: 2,
        request: {
          kind: "approval",
          requestId: approval,
          prompt: "Approve B?",
          options: [],
          availability: { available: true, reason: null },
        },
      },
      new Set([input, approval]),
    );
    const peer = new MonitorPeerStore(async () => undefined);
    peer.receive({ type: "reset", publisherGeneration: "generation" });
    for (const frame of monitorSnapshot({
      publisherGeneration: "generation",
      revision: 1,
      environments: store.environments,
      rows: store.visibleRows(),
      counts: store.counts,
    }))
      peer.receive(frame);
    for (const retained of store.drafts.values()) peer.receive({ type: "draft", draft: retained });
    expect(peer.draft(row, input).answers).toEqual(draft.answers);
    store.setDetail({ ...detail, revision: 3 }, new Set([input]));
    expect(store.draft(row, input).answers).toEqual(draft.answers);
    store.setDetail({ ...detail, revision: 4, unavailableReason: "Disconnected" });
    expect(store.draft(row, input).answers).toEqual(draft.answers);
    store.setDetail({ ...detail, revision: 5 }, new Set());
    expect(store.drafts.has(monitorDraftKey(row, input))).toBe(false);
    expect(store.edit({ ...draft, revision: 2 })).toBe(false);
    await store.retire(row.environmentId);
    store.setDetail({ ...detail, revision: 6 }, new Set([input]));
    expect(store.edit({ ...draft, revision: 3 })).toBe(false);
  });
  it("keeps both drafts across hide/disconnect and clears only the submitted revision", () => {
    const { store, row } = setup();
    const first = { ...store.draft(row), revision: 1, text: "submitted" };
    const decision = {
      ...store.draft(row, RuntimeRequestId.make("question")),
      revision: 1,
      answers: { actualQuestion: "custom answer" },
    };
    expect(store.edit(first)).toBe(true);
    expect(store.edit(decision)).toBe(true);
    store.setCohort(
      [{ ...testEnvironment, transport: "disconnected", freshness: "cached" }],
      [row],
      { ...emptyCounts(), waiting: 1 },
    );
    expect(store.draft(row).text).toBe("submitted");
    const action = {
      ...refOf(row),
      type: "send" as const,
      actionId: "send",
      commandId: CommandId.make("command"),
      messageId: MessageId.make("message"),
      draftRevision: 1,
      text: first.text,
    };
    store.recordIdentity(action);
    store.edit({ ...first, revision: 2, text: "new typing" });
    store.result({
      ...refOf(row),
      type: "actionResult",
      actionId: action.actionId,
      commandId: action.commandId,
      status: "accepted",
      reason: null,
      submittedDraftRevision: 1,
    });
    expect(store.draft(row).text).toBe("new typing");
    expect(store.draft(row, decision.requestId).answers).toEqual(decision.answers);
  });
  it("preserves an edit racing the accepted clear at the next revision", () => {
    const { store, row } = setup();
    store.edit({ ...store.draft(row), revision: 4, text: "sent" });
    store.recordIdentity({
      ...refOf(row),
      type: "send",
      actionId: "a",
      commandId: CommandId.make("c"),
      messageId: MessageId.make("m"),
      draftRevision: 4,
      text: "sent",
    });
    store.result({
      ...refOf(row),
      type: "actionResult",
      actionId: "a",
      commandId: CommandId.make("c"),
      status: "accepted",
      reason: null,
      submittedDraftRevision: 4,
    });
    expect(store.draft(row).text).toBe("");
    expect(
      store.edit({
        ...store.draft(row),
        revision: 5,
        text: "typed while acceptance was in flight",
      }),
    ).toBe(true);
    expect(store.draft(row).text).toContain("while acceptance");
  });
  it("retires synchronously, releases selected detail, and awaits the exact native ack", async () => {
    const { store, row } = setup();
    store.edit({ ...store.draft(row), revision: 1, text: "private draft" });
    store.edit({
      ...store.draft(row, RuntimeRequestId.make("q")),
      revision: 1,
      answers: { q: "private answer" },
    });
    store.select({ ...refOf(row), selectionId: "selected" });
    let released = false;
    store.bindDetailRelease(() => {
      released = true;
    });
    let ack: (() => void) | undefined;
    store.bindInvalidator(
      (frame) =>
        new Promise((resolve) => {
          ack = () => resolve({ ...frame, type: "environmentInvalidated" });
        }),
    );
    const retired = store.retire(row.environmentId);
    expect(released).toBe(true);
    expect(store.drafts.size).toBe(0);
    expect(store.rows).toHaveLength(0);
    expect(store.selection).toBeNull();
    expect(store.fence.accepts(row)).toBe(false);
    expect(store.edit({ ...store.draft(row), revision: 9, text: "late" })).toBe(false);
    expect(store.register(row.environmentId)).toBeNull();
    ack!();
    await retired;
    store.observeCatalog(new Set());
    const next = store.register(row.environmentId)!;
    expect(next.environmentEpoch).toBe(1);
    expect(store.fence.accepts(row)).toBe(false);
    expect(store.drafts.size).toBe(0);
    store.result({
      ...refOf(row),
      type: "actionResult",
      actionId: "late",
      commandId: CommandId.make("late"),
      status: "accepted",
      reason: null,
    });
    expect(store.results.size).toBe(0);
  });
  it("keeps registration fenced when native invalidation fails instead of leaking the old account", async () => {
    const { store, row } = setup();
    store.bindInvalidator(async () => {
      throw new Error("peer not safely retired");
    });
    await expect(store.retire(row.environmentId)).rejects.toThrow("not safely retired");
    store.observeCatalog(new Set());
    expect(store.register(row.environmentId)).toBeNull();
    expect(store.drafts.has(monitorDraftKey(row))).toBe(false);
  });
  it("grows chronological materialization by 50 without evicting an earlier visible row", () => {
    const { store } = setup();
    const distinct = Array.from({ length: 80 }, (_, index) =>
      testRow({ threadId: ThreadId.make(`thread-${index}`) }),
    );
    store.setCohort([testEnvironment], distinct, { ...emptyCounts(), waiting: 80 });
    expect(store.visibleRows()).toHaveLength(1);
    const first = store.visibleRows()[0]!;
    store.more("waiting");
    expect(store.visibleRows()).toHaveLength(51);
    store.setCohort([testEnvironment], [...distinct].reverse(), { ...emptyCounts(), waiting: 80 });
    expect(store.visibleRows()).toContain(first);
  });
});
