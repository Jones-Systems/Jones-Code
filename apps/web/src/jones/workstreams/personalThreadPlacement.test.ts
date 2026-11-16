import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  groupPersonalThreads,
  personalThreadKey,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsFailure,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { sortActiveThreadsByOrderKey } from "@t3tools/client-runtime/state/thread-sort";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { PersonalWorkstreamAssignment } from "@t3tools/contracts/jones/workstreamsPersonal";
import { describe, expect, it } from "vite-plus/test";

import { placePersonalThread, type PersonalThreadPlacementInput } from "./personalThreadPlacement";

const environmentId = EnvironmentId.make("placement-env");
const at = "2026-11-16T12:00:00.000Z";
const shellKey = (thread: EnvironmentThreadShell) =>
  scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));
const failure: PersonalWorkstreamsFailure = {
  action: "assign",
  reason: "changed_elsewhere",
  environmentId,
  dedupeKey: "assign:moved:changed_elsewhere",
};

function fixture(
  ids: readonly string[] = ["u1", "a1", "b1", "moved", "a2", "u2"],
  initialGroups: ReadonlyArray<readonly [string, string]> = [
    ["a1", "a"],
    ["a2", "a"],
    ["b1", "b"],
  ],
  pinnedId?: string,
) {
  let rows = ids.map(
    (id, index) =>
      ({
        environmentId,
        id: ThreadId.make(id),
        createdAt: new Date(Date.UTC(2026, 0, 20 - index)).toISOString(),
        pinnedAt: id === pinnedId ? at : null,
        activeOrderKey: null,
      }) as EnvironmentThreadShell,
  );
  const assignments = new Map<string, PersonalWorkstreamAssignment>(
    initialGroups.map(([id, workstreamId]) => [
      personalThreadKey({ environmentId, id: ThreadId.make(id) }),
      { threadId: ThreadId.make(id), workstreamId, revision: 1, updatedAt: at },
    ]),
  );
  const state: PersonalWorkstreamsState = {
    workstreams: ["a", "b", "empty"].map((id) => ({
      id,
      name: id,
      lifecycle: "active",
      color: null,
      orderKey: id,
      createdAt: at,
      updatedAt: at,
    })),
    environments: [{ environmentId, status: "ready", confirmed: true, workstreamCount: 3 }],
    assignmentsByThreadKey: assignments,
    pendingThreadKeys: new Set(),
    canCreate: true,
  };
  const assignmentWrites: Array<string | null> = [];
  const orderWrites: Array<{ id: string; orderKey: string }> = [];
  const assignmentFailures: PersonalWorkstreamsFailure[] = [];
  const positionFailures: Array<{ assignmentSaved: boolean; cause: unknown }> = [];
  const denied = new Set<string>();
  let assignAllowed = true;
  let rejectAssignment = false;
  let assignmentError: Error | undefined;
  let failOrderAt = Number.POSITIVE_INFINITY;
  let throwOrderAt = Number.POSITIVE_INFINITY;
  let revokeAfterWrites = Number.POSITIVE_INFINITY;
  let revokeAfterAssignment = false;
  let orderAttempts = 0;
  const row = (id: string) => rows.find((thread) => thread.id === id)!;
  const activeRows = () => sortActiveThreadsByOrderKey(rows.filter((thread) => !thread.pinnedAt));
  const canOperate = (threads: readonly EnvironmentThreadShell[]) =>
    orderWrites.length < revokeAfterWrites &&
    !(revokeAfterAssignment && assignmentWrites.length > 0) &&
    threads.every((thread) => !denied.has(thread.id));
  const controller: Pick<PersonalWorkstreamsController, "assign" | "canAssign"> = {
    canAssign: () => assignAllowed,
    assign: async (thread, workstreamId) => {
      if (assignmentError) throw assignmentError;
      if (rejectAssignment) return { ok: false, failure };
      assignmentWrites.push(workstreamId);
      assignments.set(personalThreadKey(thread), {
        threadId: thread.id,
        workstreamId,
        revision: 2,
        updatedAt: at,
      });
      return { ok: true, value: undefined };
    },
  };
  const input = (
    destinationId: string | null,
    neighborId: string | null,
    after = false,
    overrides: Partial<PersonalThreadPlacementInput> = {},
  ): PersonalThreadPlacementInput => ({
    thread: row("moved"),
    destinationId,
    neighbor: neighborId === null ? null : row(neighborId),
    after,
    state,
    activeThreads: activeRows(),
    activeKeys: activeRows().map(shellKey),
    activeKeysById: new Map(rows.map((thread) => [shellKey(thread), thread.activeOrderKey])),
    reorderableKeys: new Set(activeRows().map(shellKey)),
    threadByKey: new Map(rows.map((thread) => [shellKey(thread), thread])),
    allowReorder: true,
    controller,
    canOperate,
    reorder: async (thread, orderKey) => {
      orderAttempts += 1;
      if (orderAttempts === throwOrderAt) throw new Error("order transport failed");
      if (orderAttempts === failOrderAt) return false;
      orderWrites.push({ id: thread.id, orderKey });
      rows = rows.map((candidate) =>
        shellKey(candidate) === shellKey(thread)
          ? { ...candidate, activeOrderKey: orderKey }
          : candidate,
      );
      return true;
    },
    onAssignmentFailure: (cause) => assignmentFailures.push(cause),
    onPositionFailure: (assignmentSaved, cause) =>
      positionFailures.push({ assignmentSaved, cause }),
    ...overrides,
  });
  const groupOrder = (id: string | null) => {
    const grouped = groupPersonalThreads(state, activeRows());
    return (
      id === null
        ? grouped.ungrouped
        : grouped.groups.find((group) => group.workstream.id === id)!.threads
    ).map((thread) => thread.id);
  };
  return {
    row,
    input,
    groupOrder,
    assignmentWrites,
    orderWrites,
    assignmentFailures,
    positionFailures,
    denied,
    setAssignAllowed: (allowed: boolean) => (assignAllowed = allowed),
    rejectAssignment: () => (rejectAssignment = true),
    throwAssignment: (cause: Error) => (assignmentError = cause),
    failOrderAt: (count: number) => (failOrderAt = count),
    throwOrderAt: (count: number) => (throwOrderAt = count),
    revokeAfterWrites: (count: number) => (revokeAfterWrites = count),
    revokeAfterAssignment: () => (revokeAfterAssignment = true),
    setOrderKey: (id: string, key: string) => {
      rows = rows.map((thread) => (thread.id === id ? { ...thread, activeOrderKey: key } : thread));
    },
    unpin: () => {
      rows = rows.map((thread) => (thread.id === "moved" ? { ...thread, pinnedAt: null } : thread));
    },
  };
}

describe("placePersonalThread", () => {
  it.each([
    { after: false, expected: ["a1", "moved", "a2"] },
    { after: true, expected: ["a1", "a2", "moved"] },
  ])("places Unassigned into a workstream at the exact row target: $after", async (scenario) => {
    const f = fixture();
    const untouchedKey = f.row("b1").activeOrderKey;
    expect(await placePersonalThread(f.input("a", "a2", scenario.after))).toBe(true);
    expect(f.groupOrder("a")).toEqual(scenario.expected);
    expect(f.groupOrder(null)).toEqual(["u1", "u2"]);
    expect(f.groupOrder("b")).toEqual(["b1"]);
    expect(f.row("b1").activeOrderKey).toBe(untouchedKey);
    expect(f.orderWrites.every(({ id }) => ["a1", "a2", "moved"].includes(id))).toBe(true);
    expect(f.assignmentWrites).toEqual(["a"]);
  });

  it("unassigns at natural order without honoring a row slot or changing any keys", async () => {
    const f = fixture(undefined, [
      ["moved", "a"],
      ["a1", "a"],
      ["a2", "a"],
      ["b1", "b"],
    ]);
    expect(await placePersonalThread(f.input(null, "u1", false, { allowReorder: false }))).toBe(
      true,
    );
    expect(f.groupOrder(null)).toEqual(["u1", "moved", "u2"]);
    expect(f.groupOrder("a")).toEqual(["a1", "a2"]);
    expect([f.row("a1").activeOrderKey, f.row("a2").activeOrderKey]).toEqual([null, null]);
    expect(f.assignmentWrites).toEqual([null]);
    expect(f.orderWrites).toEqual([]);
  });

  it.each([false, true])("ignores internal Unassigned row drops: after=%s", async (after) => {
    const f = fixture(["moved", "u1", "u2"], []);
    expect(await placePersonalThread(f.input(null, "u1", after))).toBe(true);
    expect(f.groupOrder(null)).toEqual(["moved", "u1", "u2"]);
    expect(f.assignmentWrites).toEqual([]);
    expect(f.orderWrites).toEqual([]);
  });

  it("plans a pinned arrival as an active writable participant without unpinning it", async () => {
    const f = fixture(undefined, undefined, "moved");
    expect(await placePersonalThread(f.input("a", "a1", true))).toBe(true);
    expect(f.row("moved").pinnedAt).toBe(at);
    expect(f.row("moved").activeOrderKey).not.toBeNull();
    f.unpin();
    expect(f.groupOrder("a")).toEqual(["a1", "moved", "a2"]);
    expect(f.groupOrder(null)).toEqual(["u1", "u2"]);
  });

  it.each([false, true])(
    "positions a pinned arrival at a keyed group's edge: after=%s",
    async (after) => {
      const f = fixture(
        undefined,
        [
          ["a1", "a"],
          ["a2", "a"],
          ["moved", "a"],
        ],
        "moved",
      );
      f.setOrderKey("a1", "a0");
      f.setOrderKey("a2", "a1");
      expect(await placePersonalThread(f.input("a", after ? "a2" : "a1", after))).toBe(true);
      expect(f.assignmentWrites).toEqual([]);
      expect(f.orderWrites.some((write) => write.id === "moved")).toBe(true);
      f.unpin();
      expect(f.groupOrder("a")).toEqual(after ? ["a1", "a2", "moved"] : ["moved", "a1", "a2"]);
    },
  );

  it.each([true, false])(
    "uses natural group-drop order with allowReorder=%s",
    async (allowReorder) => {
      const f = fixture(["moved", "a1", "a2"]);
      expect(await placePersonalThread(f.input("a", null, false, { allowReorder }))).toBe(true);
      expect(f.groupOrder("a")).toEqual(["moved", "a1", "a2"]);
      expect(f.orderWrites).toEqual([]);
    },
  );

  it("treats a same-group header drop as a no-op even when assignment and order are unavailable", async () => {
    const f = fixture(undefined, [
      ["moved", "a"],
      ["a1", "a"],
      ["a2", "a"],
    ]);
    f.setAssignAllowed(false);
    expect(await placePersonalThread(f.input("a", null, false, { allowReorder: false }))).toBe(
      true,
    );
    expect(f.groupOrder("a")).toEqual(["a1", "moved", "a2"]);
    expect(f.assignmentWrites).toEqual([]);
    expect(f.orderWrites).toEqual([]);
  });

  it("assigns into an empty group without writing an order key", async () => {
    const f = fixture();
    expect(await placePersonalThread(f.input("empty", null))).toBe(true);
    expect(f.groupOrder("empty")).toEqual(["moved"]);
    expect(f.orderWrites).toEqual([]);
  });

  it.each([
    "permission",
    "assignment",
    "destination",
    "neighbor-group",
    "neighbor-stale",
    "reorder-disabled",
    "moved-frozen",
    "neighbor-frozen",
  ])("rejects %s before changing membership or order", async (kind) => {
    const f = fixture();
    let input = f.input("a", "a1");
    if (kind === "permission") f.denied.add("moved");
    if (kind === "assignment") f.setAssignAllowed(false);
    if (kind === "destination") input = { ...input, destinationId: "deleted" };
    if (kind === "neighbor-group") input = { ...input, neighbor: f.row("b1") };
    if (kind === "neighbor-stale")
      input = {
        ...input,
        activeThreads: input.activeThreads.filter((thread) => thread.id !== "a1"),
      };
    if (kind === "reorder-disabled") input = { ...input, allowReorder: false };
    if (kind === "moved-frozen" || kind === "neighbor-frozen") {
      const keys = new Set(input.reorderableKeys);
      keys.delete(shellKey(f.row(kind === "moved-frozen" ? "moved" : "a1")));
      input = { ...input, reorderableKeys: keys };
    }
    expect(await placePersonalThread(input)).toBe(false);
    expect(f.groupOrder("a")).toEqual(["a1", "a2"]);
    expect(f.groupOrder(null)).toEqual(["u1", "moved", "u2"]);
    expect(f.assignmentWrites).toEqual([]);
    expect(f.orderWrites).toEqual([]);
  });

  it("rejects infeasible placement before a frozen keyless row without saving membership", async () => {
    const f = fixture(
      ["a1", "frozen", "moved", "a2"],
      [
        ["a1", "a"],
        ["frozen", "a"],
        ["a2", "a"],
      ],
    );
    const input = f.input("a", "a1");
    const writable = new Set(input.reorderableKeys);
    writable.delete(shellKey(f.row("frozen")));
    expect(await placePersonalThread({ ...input, reorderableKeys: writable })).toBe(false);
    expect(f.groupOrder("a")).toEqual(["a1", "frozen", "a2"]);
    expect(f.groupOrder(null)).toEqual(["moved"]);
    expect(f.assignmentWrites).toEqual([]);
    expect(f.orderWrites).toEqual([]);
  });

  it("does not write order keys after an assignment failure", async () => {
    const f = fixture();
    f.rejectAssignment();
    expect(await placePersonalThread(f.input("a", "a1"))).toBe(false);
    expect(f.groupOrder("a")).toEqual(["a1", "a2"]);
    expect(f.groupOrder(null)).toEqual(["u1", "moved", "u2"]);
    expect(f.orderWrites).toEqual([]);
    expect(f.assignmentFailures).toEqual([failure]);
    expect(f.positionFailures).toEqual([]);
  });

  it("propagates an unexpected assignment exception without claiming its outcome", async () => {
    const f = fixture();
    const cause = new Error("assignment transport failed");
    f.throwAssignment(cause);
    await expect(placePersonalThread(f.input("a", "a1"))).rejects.toBe(cause);
    expect(f.orderWrites).toEqual([]);
    expect(f.assignmentFailures).toEqual([]);
    expect(f.positionFailures).toEqual([]);
  });

  it.each(["false", "throw", "permission"])(
    "reports saved membership after partial order failure: %s",
    async (kind) => {
      const f = fixture();
      if (kind === "false") f.failOrderAt(2);
      if (kind === "throw") f.throwOrderAt(2);
      if (kind === "permission") f.revokeAfterWrites(1);
      expect(await placePersonalThread(f.input("a", "a1"))).toBe(false);
      expect(f.assignmentWrites).toEqual(["a"]);
      expect(f.orderWrites).toHaveLength(1);
      expect(f.orderWrites[0]!.id).toBe("moved");
      expect(f.groupOrder("a")).toEqual(["a1", "a2", "moved"]);
      expect(f.groupOrder(null)).toEqual(["u1", "u2"]);
      expect(f.positionFailures).toHaveLength(1);
      expect(f.positionFailures[0]!.assignmentSaved).toBe(true);
      expect(f.positionFailures[0]!.cause).toBeInstanceOf(Error);
    },
  );

  it("rechecks operation permission after assignment and before the first order write", async () => {
    const f = fixture();
    f.revokeAfterAssignment();
    expect(await placePersonalThread(f.input("a", "a1"))).toBe(false);
    expect(f.assignmentWrites).toEqual(["a"]);
    expect(f.groupOrder("a")).toEqual(["a1", "moved", "a2"]);
    expect(f.orderWrites).toEqual([]);
    expect(f.positionFailures[0]!.assignmentSaved).toBe(true);
  });

  it("does not claim a saved assignment for a same-group order failure", async () => {
    const f = fixture(undefined, [
      ["moved", "a"],
      ["a1", "a"],
      ["a2", "a"],
    ]);
    f.failOrderAt(1);
    expect(await placePersonalThread(f.input("a", "a1"))).toBe(false);
    expect(f.groupOrder("a")).toEqual(["a1", "moved", "a2"]);
    expect(f.assignmentWrites).toEqual([]);
    expect(f.positionFailures[0]!.assignmentSaved).toBe(false);
  });
});
