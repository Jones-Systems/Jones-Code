import { describe, expect, it } from "vite-plus/test";
import type { PreviewEvent, PreviewListResult } from "@t3tools/contracts";
import { applyCompanionSessionEvent } from "./snapshot.ts";
const snapshot = {
  threadId: "thread",
  tabId: "tab",
  runtime: "server" as const,
  navStatus: { _tag: "Idle" as const },
  canGoBack: false,
  canGoForward: false,
  updatedAt: "2026-10-08T00:00:00Z",
};
const list: PreviewListResult = { serverEpoch: "epoch", revision: 10, sessions: [snapshot] };
const closed: PreviewEvent = {
  type: "closed",
  threadId: "thread",
  tabId: "tab",
  serverEpoch: "epoch",
  revision: 11,
  createdAt: snapshot.updatedAt,
};
describe("assigned session inventory ordering", () => {
  it("rejects stale events and requires a new list after a changed server epoch", () => {
    expect(applyCompanionSessionEvent(list, { ...closed, revision: 9 })).toBe(list);
    expect(applyCompanionSessionEvent(list, { ...closed, serverEpoch: "new-epoch" })).toBeNull();
    expect(applyCompanionSessionEvent(null, closed)).toBeNull();
  });
  it("removes closed sessions and follows authoritative navigation snapshots", () => {
    expect(applyCompanionSessionEvent(list, closed)?.sessions).toEqual([]);
    const next = {
      ...snapshot,
      navStatus: { _tag: "Success" as const, url: "https://fixture.test", title: "Next" },
    };
    expect(
      applyCompanionSessionEvent(list, { ...closed, type: "navigated", snapshot: next })?.sessions,
    ).toEqual([next]);
  });
});
