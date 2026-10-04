import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { makeThreadShellFixture } from "../../test-fixtures";
import { nativeWorkstreamThreadKey } from "@t3tools/client-runtime/state/workstreams";
import {
  buildThreadListV2Items,
  buildThreadListV2ListItems,
  getThreadListV2OrderedSection,
} from "../threads/threadListV2";
import { mobileThreadOrderScope, type MobileThreadOrderSnapshot } from "../../lib/threadOrderScope";
import { projectMobileWorkstreamList, mobileWorkstreamMoveDestination } from "./listProjection";

const now = "2026-10-01T12:00:00Z";
function thread(id: string, pinned: boolean, activeOrderKey: string): EnvironmentThreadShell {
  return makeThreadShellFixture({
    environmentId: EnvironmentId.make("env:one"),
    id: ThreadId.make(id),
    projectId: ProjectId.make("repo"),
    title: id,
    archivedAt: null,
    createdAt: "2026-09-30T12:00:00Z",
    updatedAt: "2026-09-30T12:00:00Z",
    settledOverride: null,
    pinnedAt: pinned ? "2026-09-29T12:00:00Z" : null,
    pinOrderKey: pinned ? "zz" : null,
    activeOrderKey,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
  });
}
const pin = thread("same:id", true, "aa");
const member = thread("member", false, "bb");
const free = thread("free", true, "cc");
const group = {
  key: "group",
  name: "Delivery",
  color: "#123456",
  threadKeys: new Set(
    [pin, member].map((row) => nativeWorkstreamThreadKey(row.environmentId, row.id)),
  ),
};
const snapshot: MobileThreadOrderSnapshot = {
  enabled: true,
  revision: "ready",
  primaryGroupByThreadKey: new Map([...group.threadKeys].map((key) => [key, group.key])),
};
const projection = {
  enabled: true,
  groups: [group],
  collapsedKeys: new Set<string>(),
  secondaryLabelsByKey: new Map<string, readonly string[]>(),
};
function rows(threads: readonly EnvironmentThreadShell[], current = snapshot) {
  const layout = buildThreadListV2Items({
    threads,
    snapshot: current,
    environmentId: null,
    searchQuery: "",
    now,
    snoozedShelfExpanded: true,
  });
  return projectMobileWorkstreamList(
    buildThreadListV2ListItems({ ...layout, pendingTasks: [], snoozedShelfExpanded: true }),
    projection,
  );
}

describe("mixed pinned workstream rows", () => {
  it("renders members once and permits pinned neighbors with active scope", () => {
    const items = rows([member, free, pin]);
    expect(items.filter((row) => row.type === "v2-thread").map((row) => row.item.thread)).toEqual([
      free,
      pin,
      member,
    ]);
    expect(mobileThreadOrderScope(pin, snapshot)).toEqual({
      kind: "workstream",
      groupKey: "group",
    });
    expect(mobileWorkstreamMoveDestination(items, pin, "down")).toEqual({
      targetId: `${member.environmentId}:${member.id}`,
      placement: "after",
    });
    expect(mobileWorkstreamMoveDestination(items, free, "down")).toBeNull();
  });
  it("returns a removed pin to its retained native slot without editing native metadata", () => {
    const removed = {
      ...snapshot,
      primaryGroupByThreadKey: new Map([
        [nativeWorkstreamThreadKey(member.environmentId, member.id), "group"],
      ]),
    };
    expect(
      getThreadListV2OrderedSection({
        threads: [pin, member, free],
        section: "pinned",
        snapshot: removed,
        now,
      }),
    ).toEqual([free, pin]);
    expect(pin.pinOrderKey).toBe("zz");
    expect(pin.pinnedAt).toBe("2026-09-29T12:00:00Z");
  });
  it("keeps settled and snoozed members inactive and restores their existing group", () => {
    const settled = { ...pin, settledOverride: "settled" as const, settledAt: now };
    const snoozed = { ...member, snoozedUntil: "2026-10-02T12:00:00Z" };
    expect(rows([settled, snoozed]).filter((row) => row.type === "v2-workstream")).toEqual([]);
    expect(rows([pin, member]).filter((row) => row.type === "v2-workstream")).toMatchObject([
      { groupKey: "group", count: 2 },
    ]);
    expect(snapshot.primaryGroupByThreadKey.size).toBe(2);
  });
  it("rejects collapsed group movement and scopes equal IDs by their whole identity", () => {
    const items = projectMobileWorkstreamList(rows([pin, member]), {
      ...projection,
      collapsedKeys: new Set(["group"]),
      selectedThreadKey: `${pin.environmentId}:${pin.id}`,
    });
    expect(mobileWorkstreamMoveDestination(items, pin, "down")).toBeNull();
    const foreign = { ...pin, environmentId: EnvironmentId.make("env:two") };
    expect(mobileThreadOrderScope(foreign, snapshot)).toEqual({ kind: "shelf", section: "pinned" });
  });
});
