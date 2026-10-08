import { describe, expect, it } from "vite-plus/test";
import {
  EnvironmentId,
  ThreadId,
  type DesktopCompanionState,
  type PreviewSessionSnapshot,
  type PreviewCompanionThreadSelectionResponse,
} from "@t3tools/contracts";
import { nativeCompanionRendering, mergeCompanionSessions } from "./inventory.ts";
import type { CompanionQuery } from "./state.ts";
const environmentId = EnvironmentId.make("env");
const threadId = ThreadId.make("thread");
const snapshot: PreviewSessionSnapshot = {
  threadId,
  tabId: "tab",
  runtime: "server",
  navStatus: { _tag: "Idle" },
  canGoBack: false,
  canGoForward: false,
  updatedAt: "2026-10-08T00:00:00Z",
};
const state: DesktopCompanionState = {
  config: { enabled: true, environmentId, hostId: "mini", label: "Mini", browserOnly: true },
  status: "online",
  connectionGeneration: 2,
  assignments: [{ threadId, tabId: "tab" }],
};
const binding = (
  hostId: string | null,
): CompanionQuery<PreviewCompanionThreadSelectionResponse> => ({
  status: "ready",
  value: { selection: null, effective: { _tag: "server" }, tabs: [{ tabId: "tab", hostId }] },
});
const renders = (
  value: CompanionQuery<PreviewCompanionThreadSelectionResponse> | null,
  companion = state,
) =>
  nativeCompanionRendering({
    environmentId,
    primaryEnvironmentId: environmentId,
    snapshot,
    binding: value,
    companion,
  });
describe("immutable companion bindings", () => {
  it("waits on unresolved/error endpoints but preserves explicit older-server 404", () => {
    expect(renders(null)).toBe(false);
    expect(renders({ status: "unavailable" })).toBe(false);
    expect(renders({ status: "unsupported" })).toBe(true);
    expect(renders(binding(null))).toBe(true);
  });
  it("requires this host's active exact assignment before native mounting", () => {
    expect(renders(binding("another-mini"))).toBe(false);
    expect(renders(binding("mini"))).toBe(true);
    expect(renders(binding("mini"), { ...state, assignments: [] })).toBe(false);
    expect(renders(binding("mini"), { ...state, status: "reconnecting" })).toBe(false);
    expect(renders(binding("mini"), { ...state, connectionGeneration: null })).toBe(false);
  });
  it("does not transfer a bound tab when new-tab selection changes", () => {
    expect(
      renders({
        status: "ready",
        value: {
          selection: { _tag: "server" },
          effective: { _tag: "server" },
          tabs: [{ tabId: "tab", hostId: "another-mini" }],
        },
      }),
    ).toBe(false);
  });
  it("mounts one guest for overlapping route and assignment inventories", () => {
    const ordinary = {
      threadRef: { environmentId, threadId },
      snapshot,
      runtimeTabId: "runtime-tab",
      pictureInPicture: false,
      zoomFactor: 1,
    };
    const merged = mergeCompanionSessions(
      [ordinary],
      [{ ...ordinary, snapshot: { ...snapshot, zoomFactor: 2 } }],
    );
    expect(merged).toHaveLength(1);
    expect(merged[0]?.companion).toBe(true);
    expect(merged[0]?.snapshot.zoomFactor).toBe(2);
  });
});
