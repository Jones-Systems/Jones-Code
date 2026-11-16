import { EnvironmentId } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { monitorThreadKey } from "@t3tools/client-runtime/jones/thread-monitor";
import { MonitorStore, emptyCounts, refOf } from "./store";
import { MonitorPeerStore } from "./peerStore";
import { monitorSnapshot } from "./snapshots";
import { emptyMonitorViewState } from "./viewState";
import { testEnvironment, testRow } from "./fixtures.test-support";
const NOW = Date.parse("2026-10-26T08:00:00.000Z");
beforeEach(() => {
  vi.spyOn(Date, "now").mockReturnValue(NOW);
});
afterEach(() => {
  vi.restoreAllMocks();
});
describe("main-owned content-free monitor view state", () => {
  it("replays disclosure, scroll and exact editor identity into a replacement peer", () => {
    const main = new MonitorStore("generation");
    main.register(testEnvironment.environmentId);
    const row = testRow();
    main.setCohort([testEnvironment], [row], { ...emptyCounts(), waiting: 1 });
    const saved = {
      ...emptyMonitorViewState("generation"),
      revision: 8,
      collapsed: { needsYou: true, working: false, waiting: false, done: true },
      list: { scrollTop: 200, anchors: [{ key: `row:${monitorThreadKey(row)}`, offset: -7 }] },
      editor: { ref: refOf(row), quick: true, focused: true, selectionStart: 2, selectionEnd: 4 },
    };
    expect(main.editViewState(saved)).toBe(true);
    const peer = new MonitorPeerStore(async () => undefined);
    peer.receive({ type: "reset", publisherGeneration: "generation" });
    for (const frame of monitorSnapshot({
      publisherGeneration: "generation",
      revision: 1,
      environments: main.environments,
      rows: main.visibleRows(),
      counts: main.counts,
    }))
      peer.receive(frame);
    peer.receive({ type: "viewState", state: main.viewState });
    expect(peer.viewState).toEqual(saved);
    expect(peer.viewStateApplyVersion).toBeGreaterThan(0);
    expect(main.editViewState({ ...saved, revision: 7 })).toBe(false);
    expect(main.editViewState({ ...saved, publisherGeneration: "foreign", revision: 9 })).toBe(
      false,
    );
  });
  it("retains an aged-out editor without adding it to Done counts, then purges its lifetime", async () => {
    const main = new MonitorStore("generation");
    main.register(testEnvironment.environmentId);
    const row = testRow({
      group: "done",
      status: "completed",
      completedAt: new Date(NOW - 3_600_000).toISOString(),
    });
    main.setCohort([testEnvironment], [row], { ...emptyCounts(), done: 1 });
    main.editViewState({
      ...emptyMonitorViewState("generation"),
      revision: 1,
      editor: { ref: refOf(row), quick: true, focused: true, selectionStart: 0, selectionEnd: 0 },
    });
    main.setCohort([testEnvironment], [], emptyCounts());
    expect(main.counts.done).toBe(0);
    expect(main.visibleRows()).toEqual([row]);
    await main.retire(row.environmentId);
    expect(main.visibleRows()).toHaveLength(0);
    expect(main.viewState.editor).toBeNull();
    expect(main.viewState.list.anchors).toHaveLength(0);
    expect(
      main.editViewState({
        ...emptyMonitorViewState("generation"),
        revision: 100,
        editor: { ref: refOf(row), quick: true, focused: true, selectionStart: 0, selectionEnd: 0 },
      }),
    ).toBe(false);
  });
  it("preserves a surviving direct environment editor when another environment is removed", async () => {
    const main = new MonitorStore("generation");
    main.register(testEnvironment.environmentId);
    const other = EnvironmentId.make("relay-only");
    main.register(other);
    const row = testRow();
    main.setCohort([testEnvironment, { ...testEnvironment, environmentId: other }], [row], {
      ...emptyCounts(),
      waiting: 1,
    });
    main.editViewState({
      ...emptyMonitorViewState("generation"),
      revision: 1,
      editor: { ref: refOf(row), quick: true, focused: true, selectionStart: 3, selectionEnd: 3 },
    });
    await main.retire(other);
    expect(main.viewState.editor?.ref.environmentId).toBe(testEnvironment.environmentId);
    expect(main.viewState.editor?.selectionStart).toBe(3);
    expect(main.nextDetailRevision()).toBe(1);
    expect(main.nextDetailRevision()).toBe(2);
  });
  it("stages the newest selected detail until view-state replay establishes its exact selection", () => {
    const peer = new MonitorPeerStore(async () => undefined);
    const row = testRow();
    peer.receive({ type: "reset", publisherGeneration: "generation" });
    for (const frame of monitorSnapshot({
      publisherGeneration: "generation",
      revision: 1,
      environments: [testEnvironment],
      rows: [row],
      counts: { ...emptyCounts(), waiting: 1 },
    }))
      peer.receive(frame);
    const detail = {
      ...refOf(row),
      selectionId: "surviving-selection",
      revision: 20,
      entries: [
        { id: "message", role: "assistant" as const, text: "Current detail", excerpt: false },
      ],
      request: null,
      plan: null,
      unavailableReason: null,
    };
    peer.receive({ type: "detail", detail });
    peer.receive({ type: "detail", detail: { ...detail, revision: 19, entries: [] } });
    expect(peer.selection).toBeNull();
    peer.receive({
      type: "viewState",
      state: {
        ...emptyMonitorViewState("generation"),
        revision: 2,
        editor: {
          ref: refOf(row),
          quick: false,
          focused: true,
          selectionStart: 1,
          selectionEnd: 2,
        },
      },
    });
    expect(peer.selection?.selectionId).toBe("surviving-selection");
    expect(peer.detail?.revision).toBe(20);
    peer.receive({ type: "detail", detail: { ...detail, revision: 19, entries: [] } });
    expect(peer.detail?.entries[0]?.text).toBe("Current detail");
    peer.receive({ type: "detail", detail: { ...detail, revision: 21, entries: [] } });
    expect(peer.detail?.revision).toBe(21);
  });
});
