import { CommandId, RuntimeRequestId, ThreadId } from "@t3tools/contracts";
import { monitorDraftKey, monitorThreadKey } from "@t3tools/client-runtime/jones/thread-monitor";
import { describe, expect, it } from "vite-plus/test";
import type { ThreadMonitorPeerIntent } from "@t3tools/contracts/jones/desktopThreadMonitor";
import { MonitorPeerStore } from "./peerStore";
import { monitorSnapshot } from "./snapshots";
import { emptyCounts, refOf } from "./store";
import { testEnvironment, testRow } from "./fixtures.test-support";
import { emptyMonitorViewState } from "./viewState";

function setup() {
  const intents: ThreadMonitorPeerIntent[] = [];
  const peer = new MonitorPeerStore(async (intent) => {
    intents.push(intent);
  });
  peer.receive({ type: "reset", publisherGeneration: "generation" });
  return { peer, intents };
}
describe("monitor peer atomic snapshots and retirement", () => {
  it("keeps a legacy sender replay usable when an older broker rejects the opt-in request", async () => {
    const peer = new MonitorPeerStore(async () => {
      throw new Error("Legacy strict decoder rejects the optional request");
    });
    peer.receive({ type: "reset", publisherGeneration: "generation" });
    for (const frame of monitorSnapshot({
      publisherGeneration: "generation",
      revision: 1,
      environments: [testEnvironment],
      rows: [testRow()],
      counts: { needsYou: 0, working: 0, waiting: 1, done: 0 },
    }))
      peer.receive(frame);
    await Promise.resolve();
    expect(peer.countsReady).toBe(true);
    expect(peer.rows.size).toBe(1);
    expect(peer.error).toBeNull();
  });
  it("hydrates exact replayed Restore identity without inferring old unknown results or regressing outcomes", () => {
    const { peer, intents } = setup();
    const row = testRow();
    const snapshot = (environment = testEnvironment) => {
      for (const frame of monitorSnapshot({
        publisherGeneration: "generation",
        revision: peer.viewRevision + 1,
        environments: [environment],
        rows: [],
        counts: { needsYou: 0, working: 0, waiting: 0, done: 0 },
      }))
        peer.receive(frame);
    };
    snapshot();
    expect(intents).toContainEqual({
      type: "requestSnapshot",
      restoreIdentityReplay: { publisherGeneration: "generation" },
    });
    const result = {
      ...refOf(row),
      type: "actionResult" as const,
      actionId: "restore",
      commandId: CommandId.make("restore-command"),
      status: "unknown" as const,
      reason: null,
    };
    const restoreIdentity = {
      type: "restore" as const,
      settleCommandId: CommandId.make("settle-command"),
    };
    peer.receive(result);
    expect(peer.actions.size).toBe(0);
    peer.receive({ ...result, restoreIdentity });
    const expected = {
      ...refOf(row),
      type: "restore",
      actionId: result.actionId,
      commandId: result.commandId,
      settleCommandId: restoreIdentity.settleCommandId,
    };
    expect(peer.actions.get(result.actionId)).toEqual(expected);
    expect(peer.retainedRows.size).toBe(0);
    for (const patch of [
      { commandId: CommandId.make("other-command") },
      { threadId: ThreadId.make("other-thread") },
      { environmentEpoch: 1 },
      { publisherGeneration: "other-generation" },
      { restoreIdentity: { ...restoreIdentity, settleCommandId: CommandId.make("other-settle") } },
    ])
      peer.receive({ ...result, restoreIdentity, ...patch });
    expect(peer.actions.get(result.actionId)).toEqual(expected);
    expect(peer.results.get(result.actionId)).toEqual({ ...result, restoreIdentity });
    peer.receive({ ...result, status: "pending", restoreIdentity });
    expect(peer.results.get(result.actionId)?.status).toBe("unknown");
    peer.receive({ ...result, status: "accepted" });
    peer.receive({ ...result, restoreIdentity });
    expect(peer.results.get(result.actionId)?.status).toBe("accepted");
    peer.receive({ ...testEnvironment, type: "invalidateEnvironment", requestId: "retire" });
    peer.receive({ ...result, restoreIdentity });
    expect(peer.actions.size).toBe(0);
    expect(peer.results.size).toBe(0);
    snapshot({ ...testEnvironment, environmentEpoch: 1 });
    peer.receive({ ...result, restoreIdentity });
    expect(peer.actions.size).toBe(0);
    peer.receive({ type: "reset", publisherGeneration: "next-generation" });
    peer.receive({ ...result, restoreIdentity });
    expect(peer.actions.size).toBe(0);
  });
  it("retains an active expired row outside the cohort until handoff or authoritative retirement", () => {
    const { peer } = setup();
    const row = testRow({ group: "done", status: "completed" });
    const key = monitorThreadKey(row);
    const snapshot = (revision: number, rows: ReadonlyArray<typeof row>) => {
      for (const frame of monitorSnapshot({
        publisherGeneration: "generation",
        revision,
        environments: [testEnvironment],
        rows,
        counts: { ...emptyCounts(), done: rows.length },
      }))
        peer.receive(frame);
    };
    snapshot(1, [row]);
    const initial = peer.getSnapshot();
    expect(peer.getSnapshot()).toBe(initial);
    peer.setQuick(key);
    peer.edit({ ...peer.draft(row), revision: 1, text: "unsent after expiry" });
    snapshot(2, []);
    expect(peer.counts.done).toBe(0);
    expect(peer.rows.has(key)).toBe(false);
    expect(initial.rows.get(key)).toEqual(row);
    expect(initial.quick).toBeNull();
    expect(peer.getSnapshot()).not.toBe(initial);
    expect(peer.getSnapshot().rows.has(key)).toBe(false);
    expect(peer.retainedRows.get(key)).toEqual(row);
    expect(peer.quick).toBe(key);
    expect(peer.draft(row).text).toBe("unsent after expiry");
    peer.select(row);
    peer.setQuick(null);
    expect(peer.retainedRows.get(key)).toEqual(row);
    peer.release();
    expect(peer.retainedRows.has(key)).toBe(false);
    expect(peer.draft(row).text).toBe("unsent after expiry");
    snapshot(3, [row]);
    peer.setQuick(key);
    snapshot(4, []);
    peer.receive({ ...testEnvironment, type: "invalidateEnvironment", requestId: "retire" });
    expect(peer.retainedRows.size).toBe(0);
    expect(peer.quick).toBeNull();
    expect(peer.drafts.size).toBe(0);
    peer.setQuick(key);
    expect(peer.quick).toBeNull();
  });
  it("applies authoritative presentation once and leaves local edits out of the replay snapshot", () => {
    const { peer, intents } = setup();
    const row = testRow();
    for (const frame of monitorSnapshot({
      publisherGeneration: "generation",
      revision: 1,
      environments: [testEnvironment],
      rows: [row],
      counts: { ...emptyCounts(), waiting: 1 },
    }))
      peer.receive(frame);
    const saved = {
      ...emptyMonitorViewState("generation"),
      revision: 8,
      collapsed: { needsYou: true, working: false, waiting: false, done: true },
      editor: { ref: refOf(row), quick: true, focused: true, selectionStart: 2, selectionEnd: 4 },
    };
    peer.receive({ type: "viewState", state: saved });
    const applyVersion = peer.viewStateApplyVersion;
    expect(peer.quick).toBe(monitorThreadKey(row));
    expect(peer.expanded).toBe(true);
    expect(peer.collapsed).toEqual(saved.collapsed);
    expect(intents.filter((intent) => intent.type === "setViewState")).toEqual([]);
    peer.toggleGroup("done");
    peer.updateViewState({ ...saved, collapsed: peer.collapsed });
    const local = peer.viewState!;
    peer.receive({ type: "viewState", state: local });
    expect(peer.viewStateApplyVersion).toBe(applyVersion);
    expect(peer.authoritativeViewState).toBe(saved);
    expect(peer.collapsed.done).toBe(false);
    peer.receive({
      type: "viewState",
      state: { ...saved, revision: local.revision + 1, editor: null },
    });
    expect(peer.viewStateApplyVersion).toBe(applyVersion + 1);
    expect(peer.quick).toBeNull();
    expect(peer.collapsed.done).toBe(true);
  });
  it("commits complete snapshots only, retains complete counts and refuses old epoch data", () => {
    const { peer, intents } = setup();
    const rows = Array.from({ length: 140 }, (_, index) =>
      testRow({ threadId: ThreadId.make(`thread-${index}`) }),
    );
    const frames = monitorSnapshot({
      publisherGeneration: "generation",
      revision: 1,
      environments: [testEnvironment],
      rows,
      counts: { ...emptyCounts(), waiting: 900 },
    });
    expect(frames).toHaveLength(3);
    peer.receive(frames[0]!);
    expect(peer.rows.size).toBe(0);
    peer.receive(frames[1]!);
    expect(peer.rows.size).toBe(0);
    peer.receive(frames[2]!);
    expect(peer.rows.size).toBe(140);
    expect(peer.counts.waiting).toBe(900);
    peer.receive({ ...testEnvironment, type: "invalidateEnvironment", requestId: "retire" });
    expect(peer.rows.size).toBe(0);
    for (const frame of monitorSnapshot({
      publisherGeneration: "generation",
      revision: 2,
      environments: [testEnvironment],
      rows,
      counts: { ...emptyCounts(), waiting: 900 },
    }))
      peer.receive(frame);
    expect(peer.rows.size).toBe(0);
    expect(intents).toContainEqual({ type: "requestSnapshot" });
  });
  it("purges both mirrors, selected text and late acknowledgments while preserving a surviving route", () => {
    const { peer } = setup();
    const row = testRow();
    for (const frame of monitorSnapshot({
      publisherGeneration: "generation",
      revision: 1,
      environments: [testEnvironment],
      rows: [row],
      counts: { ...emptyCounts(), waiting: 1 },
    }))
      peer.receive(frame);
    peer.edit({ ...peer.draft(row), revision: 1, text: "reply" });
    peer.edit({
      ...peer.draft(row, RuntimeRequestId.make("question")),
      revision: 1,
      answers: { q: "choice" },
    });
    peer.select(row);
    peer.receive({
      type: "detail",
      detail: {
        ...refOf(row),
        selectionId: peer.selection!.selectionId,
        revision: 1,
        entries: [{ id: "text", role: "assistant", text: "private text", excerpt: false }],
        request: null,
        plan: null,
        unavailableReason: null,
      },
    });
    for (const frame of monitorSnapshot({
      publisherGeneration: "generation",
      revision: 2,
      environments: [{ ...testEnvironment, transport: "disconnected", freshness: "cached" }],
      rows: [row],
      counts: { ...emptyCounts(), waiting: 1 },
    }))
      peer.receive(frame);
    expect(peer.drafts.size).toBe(2);
    expect(peer.detail?.entries[0]?.text).toBe("private text");
    peer.receive({ ...refOf(row), type: "invalidateEnvironment", requestId: "retire" });
    expect(peer.drafts.size).toBe(0);
    expect(peer.selection).toBeNull();
    expect(peer.detail).toBeNull();
    peer.receive({
      type: "draft",
      draft: {
        ...refOf(row),
        revision: 20,
        kind: "reply",
        requestId: null,
        text: "late private text",
        answers: {},
      },
    });
    peer.receive({
      ...refOf(row),
      type: "actionResult",
      actionId: "late",
      commandId: CommandId.make("late"),
      status: "accepted",
      reason: null,
    });
    expect(peer.drafts.size).toBe(0);
    expect(peer.results.size).toBe(0);
    for (const frame of monitorSnapshot({
      publisherGeneration: "generation",
      revision: 3,
      environments: [{ ...testEnvironment, environmentEpoch: 1 }],
      rows: [{ ...row, environmentEpoch: 1 }],
      counts: { ...emptyCounts(), waiting: 1 },
    }))
      peer.receive(frame);
    expect(peer.rows.has(monitorThreadKey(row))).toBe(true);
    expect(peer.drafts.has(monitorDraftKey(row))).toBe(false);
  });
  it("keeps newer typing when an older send is accepted and rejects identity-mismatched results", () => {
    const { peer } = setup();
    const row = testRow();
    for (const frame of monitorSnapshot({
      publisherGeneration: "generation",
      revision: 1,
      environments: [testEnvironment],
      rows: [row],
      counts: { ...emptyCounts(), waiting: 1 },
    }))
      peer.receive(frame);
    peer.edit({ ...peer.draft(row), revision: 3, text: "newer typing" });
    peer.receive({
      ...refOf(row),
      type: "actionResult",
      actionId: "send",
      commandId: CommandId.make("command"),
      status: "pending",
      reason: null,
    });
    peer.receive({
      ...refOf(row),
      type: "actionResult",
      actionId: "send",
      commandId: CommandId.make("wrong-command"),
      status: "accepted",
      reason: null,
      submittedDraftRevision: 3,
    });
    expect(peer.draft(row).text).toBe("newer typing");
    peer.receive({
      ...refOf(row),
      type: "actionResult",
      actionId: "send",
      commandId: CommandId.make("command"),
      status: "accepted",
      reason: null,
      submittedDraftRevision: 2,
    });
    expect(peer.draft(row).text).toBe("newer typing");
  });
});
