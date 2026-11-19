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
  movedId = "moved",
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
  const positionRejections: undefined[] = [];
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
    thread: row(movedId),
    destinationId,
    neighbor: neighborId === null ? null : row(neighborId),
    after,
    state,
    savedOrder: { state, readRoster: () => rows },
    activeThreads: activeRows(),
    orderRoster: rows,
    activeKeysById: new Map(rows.map((thread) => [shellKey(thread), thread.activeOrderKey])),
    reorderableKeys: new Set(rows.map(shellKey)),
    allowReorder: true,
    controller,
    canOperate,
    reorder: async (thread, orderKey) => {
      orderAttempts += 1;
      if (row(thread.id).pinnedAt != null) return false;
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
    onPositionRejected: () => positionRejections.push(undefined),
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
    positionRejections,
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
      rows = rows.map((thread) => (thread.id === movedId ? { ...thread, pinnedAt: null } : thread));
    },
  };
}

function snoozedAlpha(input: PersonalThreadPlacementInput, sleepers: readonly string[]) {
  return {
    ...input,
    state: { ...input.state, workstreams: input.state.workstreams.filter(({ id }) => id !== "a") },
    activeThreads: input.activeThreads.filter(({ id }) => !sleepers.includes(id)),
    orderRoster: input.orderRoster.filter(({ id }) => !sleepers.includes(id)),
  };
}

describe("placePersonalThread", () => {
  it.each(["same project", "filtered project", "pin shelf"])(
    "rejects a drop that would reverse stationary sleeping members (%s)",
    async (origin) => {
      const pinned = origin === "pin shelf";
      const f = fixture(
        ["other", "anchor", "tail", "moved"],
        [["anchor", "a"], ["tail", "a"], ...(pinned ? [] : ([["moved", "b"]] as const))],
        pinned ? "moved" : undefined,
      );
      f.setOrderKey("moved", "t");
      if (origin === "filtered project") f.denied.add("tail");
      const input = snoozedAlpha(f.input(null, "other"), ["tail"]);
      let unpins = 0;
      const placed = await placePersonalThread({
        ...input,
        unpin: async () => {
          unpins += 1;
          f.unpin();
          return true;
        },
      });
      expect({
        placed,
        assignments: f.assignmentWrites,
        unpins,
        writes: f.orderWrites,
        saved: f.groupOrder("a"),
        notices: f.positionRejections.length,
        receiptFailures: f.positionFailures.length,
      }).toEqual({
        placed: false,
        assignments: [],
        unpins: 0,
        writes: [],
        saved: ["anchor", "tail"],
        notices: 1,
        receiptFailures: 0,
      });
    },
  );

  it("rejects the whole plan when only its second stationary write would invert a sleeping lane", async () => {
    const f = fixture(
      ["a1", "a2", "a3", "moved"],
      [
        ["a1", "a"],
        ["a2", "a"],
        ["a3", "a"],
      ],
    );
    f.setOrderKey("moved", "t");
    f.failOrderAt(3);
    expect(await placePersonalThread(snoozedAlpha(f.input(null, "a1"), ["a2"]))).toBe(false);
    expect({
      writes: f.orderWrites,
      saved: f.groupOrder("a"),
      failures: f.positionFailures,
    }).toEqual({
      writes: [],
      saved: ["a1", "a2", "a3"],
      failures: [],
    });
    expect(f.positionRejections).toHaveLength(1);
  });

  it.each([false, true])(
    "allows a harmless sleeping-lane rekey, including an RPC failure after it (%s)",
    async (failAfterFirst) => {
      const f = fixture(
        ["other", "tail", "anchor", "moved"],
        [
          ["tail", "a"],
          ["anchor", "a"],
        ],
      );
      f.setOrderKey("moved", "t");
      if (failAfterFirst) f.failOrderAt(2);
      expect(await placePersonalThread(snoozedAlpha(f.input(null, "other"), ["tail"]))).toBe(
        !failAfterFirst,
      );
      expect(f.orderWrites[0]?.id).toBe("anchor");
      expect(f.groupOrder("a")).toEqual(["tail", "anchor"]);
      expect(f.positionRejections).toEqual([]);
      expect(f.positionFailures).toHaveLength(failAfterFirst ? 1 : 0);
    },
  );

  it("can rekey a single stationary sleeping member", async () => {
    const f = fixture(["other", "anchor", "moved"], [["anchor", "a"]]);
    f.setOrderKey("moved", "t");
    expect(await placePersonalThread(snoozedAlpha(f.input(null, "other"), []))).toBe(true);
    expect(f.orderWrites[0]?.id).toBe("anchor");
    expect(f.groupOrder("a")).toEqual(["anchor"]);
    expect(f.assignmentWrites).toEqual([]);
  });

  it("allows the woken mover to change its own saved position while retaining assignment", async () => {
    const f = fixture(
      ["moved", "tail", "other"],
      [
        ["moved", "a"],
        ["tail", "a"],
      ],
    );
    expect(await placePersonalThread(snoozedAlpha(f.input(null, "other", true), ["tail"]))).toBe(
      true,
    );
    expect(f.orderWrites.map(({ id }) => id)).toEqual(["moved"]);
    expect(f.groupOrder("a")).toEqual(["tail", "moved"]);
    expect(f.assignmentWrites).toEqual([]);
  });

  it("reorders an unsettled historical member in Unassigned without removing its saved Workstream", async () => {
    const f = fixture(["u1", "moved", "u2"], [["moved", "a"]]);
    const input = f.input(null, "u1");
    const state: PersonalWorkstreamsState = {
      ...input.state,
      workstreams: input.state.workstreams.map((workstream) =>
        workstream.id === "a" ? { ...workstream, lifecycle: "completed" } : workstream,
      ),
    };
    expect(await placePersonalThread({ ...input, state })).toBe(true);
    expect(f.assignmentWrites).toEqual([]);
    expect(f.orderWrites.length).toBeGreaterThan(0);
    expect(state.assignmentsByThreadKey.get(personalThreadKey(input.thread))?.workstreamId).toBe(
      "a",
    );
  });

  it.each([
    { after: false, neighbor: "u1", expected: ["moved", "u1", "u2"] },
    { after: true, neighbor: "u2", expected: ["u1", "u2", "moved"] },
  ])("persists supported internal Unassigned placement: $after", async (scenario) => {
    const f = fixture(["u1", "moved", "u2"], []);
    expect(await placePersonalThread(f.input(null, scenario.neighbor, scenario.after))).toBe(true);
    expect(f.groupOrder(null)).toEqual(scenario.expected);
    expect(f.assignmentWrites).toEqual([]);
    expect(f.orderWrites.length).toBeGreaterThan(0);
  });

  it("saves an active move within a workstream that also shows a pinned member", async () => {
    const f = fixture(
      ["first", "moved", "pinned"],
      [
        ["first", "a"],
        ["moved", "a"],
        ["pinned", "a"],
      ],
      "pinned",
    );
    const input = f.input("a", "first");
    const members = [...input.activeThreads, f.row("pinned")];
    expect(
      await placePersonalThread({
        ...input,
        activeThreads: members,
        reorderableKeys: new Set(members.map(shellKey)),
      }),
    ).toBe(true);
    expect(f.groupOrder("a")).toEqual(["moved", "first"]);
    expect(f.orderWrites.map(({ id }) => id)).toEqual(["first", "moved"]);
    expect(f.row("pinned").activeOrderKey).toBeNull();
  });

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

  it.each([
    { destinationId: "a", rejectSecond: false },
    { destinationId: "a", rejectSecond: true },
    { destinationId: null, rejectSecond: false },
    { destinationId: null, rejectSecond: true },
  ])(
    "preserves destination siblings when arriving between equal keys (lane=$destinationId, rejectSecond=$rejectSecond)",
    async ({ destinationId, rejectSecond }) => {
      const siblings = ["a1", "a2", "b1", "b2"];
      const ids = [...siblings, "moved", "control"];
      const f = fixture(ids, [
        ...(destinationId === null
          ? [["moved", "a"] as const]
          : siblings.map((id) => [id, destinationId] as const)),
        ["control", "b"],
      ]);
      f.setOrderKey("b1", "m");
      f.setOrderKey("b2", "m");
      f.setOrderKey("moved", "t");
      if (rejectSecond) f.failOrderAt(2);

      const input = f.input(destinationId, "b1", true);
      const siblingOrderAfterWrites: string[][] = [];
      expect(
        await placePersonalThread({
          ...input,
          reorder: async (thread, key) => {
            const saved = await input.reorder(thread, key);
            if (saved) {
              siblingOrderAfterWrites.push(
                f.groupOrder(destinationId).filter((id) => id !== "moved"),
              );
            }
            return saved;
          },
        }),
      ).toBe(!rejectSecond);

      expect(f.assignmentWrites).toEqual([destinationId]);
      expect(f.positionFailures.map(({ assignmentSaved }) => assignmentSaved)).toEqual(
        rejectSecond ? [true] : [],
      );
      expect(siblingOrderAfterWrites).toHaveLength(rejectSecond ? 1 : 5);
      for (const order of siblingOrderAfterWrites) expect(order).toEqual(siblings);
      expect(f.row("control").activeOrderKey).toBeNull();

      const reloadedRows = JSON.parse(
        JSON.stringify(ids.map(f.row).reverse()),
      ) as EnvironmentThreadShell[];
      const reloaded = groupPersonalThreads(input.state, sortActiveThreadsByOrderKey(reloadedRows));
      const lane =
        destinationId === null
          ? reloaded.ungrouped
          : reloaded.groups.find(({ workstream }) => workstream.id === destinationId)!.threads;
      expect(lane.map(({ id }) => id).filter((id) => id !== "moved")).toEqual(siblings);
      if (!rejectSecond) {
        expect(lane.map(({ id }) => id)).toEqual(["a1", "a2", "b1", "moved", "b2"]);
      }
    },
  );

  it.each(
    [
      { destinationId: null, endpoint: "tail", upper: "z", keyless: ["a"] },
      { destinationId: "a", endpoint: "fixed", upper: "z", keyless: ["a1", "a2"] },
      { destinationId: "a", endpoint: "fixed", upper: "n", keyless: ["a1", "a2"] },
    ].flatMap((scenario) => [false, true].map((rejectSecond) => ({ ...scenario, rejectSecond }))),
  )(
    "splits tied keys below a hidden read-only endpoint (lane=$destinationId, upper=$upper, rejectSecond=$rejectSecond)",
    async ({ destinationId, endpoint, upper, keyless, rejectSecond }) => {
      const siblings = [...keyless, "b1", "b2", endpoint];
      const ids = [...keyless, "b1", "b2", "u", endpoint];
      const f = fixture(
        ids,
        destinationId === null ? [] : siblings.map((id) => [id, destinationId]),
        undefined,
        "u",
      );
      f.setOrderKey("b1", "m");
      f.setOrderKey("b2", "m");
      f.setOrderKey("u", "t");
      f.setOrderKey(endpoint, upper);
      f.denied.add(endpoint);
      if (rejectSecond) f.failOrderAt(2);

      const input = f.input(destinationId, "b1", true);
      const siblingOrders: string[][] = [];
      expect(
        await placePersonalThread({
          ...input,
          activeThreads: input.activeThreads.filter(({ id }) => id !== endpoint),
          reorder: async (thread, key) => {
            const saved = await input.reorder(thread, key);
            if (saved) siblingOrders.push(f.groupOrder(destinationId).filter((id) => id !== "u"));
            return saved;
          },
        }),
      ).toBe(!rejectSecond);

      expect(f.assignmentWrites).toEqual(destinationId === null ? [] : [destinationId]);
      expect(f.positionFailures.map(({ assignmentSaved }) => assignmentSaved)).toEqual(
        rejectSecond ? [destinationId !== null] : [],
      );
      expect(f.orderWrites.map(({ id }) => id)).toEqual(rejectSecond ? ["b2"] : ["b2", "u"]);
      for (const order of siblingOrders) expect(order).toEqual(siblings);
      for (const id of keyless) expect(f.row(id).activeOrderKey).toBeNull();
      expect(f.row("b1").activeOrderKey).toBe("m");
      expect(f.row(endpoint).activeOrderKey).toBe(upper);

      const savedRows = structuredClone(ids.toReversed().map(f.row));
      const reloaded = groupPersonalThreads(
        structuredClone(input.state),
        sortActiveThreadsByOrderKey(savedRows),
      );
      const lane =
        destinationId === null
          ? reloaded.ungrouped
          : reloaded.groups.find(({ workstream }) => workstream.id === destinationId)!.threads;
      const reloadedIds = lane.map(({ id }) => id);
      expect(reloadedIds.filter((id) => id !== "u")).toEqual(siblings);
      if (!rejectSecond) expect(reloadedIds).toEqual([...keyless, "b1", "u", "b2", endpoint]);
    },
  );

  it("rejects a tie between equal fixed bounds before saving membership, unpinning, or writing keys", async () => {
    const f = fixture(
      ["b0", "b1", "b2", "u"],
      [
        ["b0", "a"],
        ["b1", "a"],
        ["b2", "a"],
      ],
      "u",
      "u",
    );
    for (const id of ["b0", "b1", "b2"]) f.setOrderKey(id, "m");
    f.setOrderKey("u", "t");
    f.denied.add("b0");
    f.denied.add("b2");
    let unpins = 0;
    expect(
      await placePersonalThread(
        f.input("a", "b1", true, {
          unpin: async () => {
            unpins += 1;
            return true;
          },
        }),
      ),
    ).toBe(false);
    expect(f.assignmentWrites).toEqual([]);
    expect(unpins).toBe(0);
    expect(f.orderWrites).toEqual([]);
    expect(f.groupOrder("a")).toEqual(["b0", "b1", "b2"]);
    expect(f.row("u").pinnedAt).toBe(at);
  });

  it.each([
    { ids: ["anchor", "moving", "tail"], fixed: ["tail"], key: null },
    {
      ids: ["a-fixed", "anchor", "moving", "z-fixed"],
      fixed: ["a-fixed", "z-fixed"],
      key: "m",
    },
  ])(
    "assigns and unpins into the natural slot with retained key $key",
    async ({ ids, fixed, key }) => {
      const f = fixture(
        ids,
        ids.filter((id) => id !== "moving").map((id) => [id, "a"]),
        "moving",
        "moving",
      );
      if (key !== null) for (const id of ids) f.setOrderKey(id, key);
      for (const id of fixed) f.denied.add(id);
      const input = f.input("a", "anchor", true);
      expect(
        await placePersonalThread({
          ...input,
          activeThreads: input.activeThreads.filter(({ id }) => !f.denied.has(id)),
          unpin: async () => {
            expect(f.assignmentWrites).toEqual(["a"]);
            expect(f.orderWrites).toEqual([]);
            f.unpin();
            return true;
          },
        }),
      ).toBe(true);
      expect(f.row("moving").pinnedAt).toBeNull();
      expect(f.groupOrder("a")).toEqual(ids);
      expect(f.orderWrites).toEqual([]);
    },
  );

  it("places a grouped thread into a mixed Unassigned prefix and keeps the other keys", async () => {
    const f = fixture(
      ["u1", "a1", "b1", "moved", "a2", "u2", "u3"],
      [
        ["moved", "a"],
        ["a1", "a"],
        ["a2", "a"],
        ["b1", "b"],
      ],
    );
    f.setOrderKey("u3", "m");
    f.setOrderKey("moved", "t");
    expect(await placePersonalThread(f.input(null, "u1", true))).toBe(true);
    expect(f.groupOrder(null)).toEqual(["u1", "moved", "u2", "u3"]);
    expect(f.groupOrder("a")).toEqual(["a1", "a2"]);
    expect([f.row("a1").activeOrderKey, f.row("a2").activeOrderKey]).toEqual([null, null]);
    expect(f.row("u3").activeOrderKey).toBe("m");
    expect(f.orderWrites.map(({ id }) => id)).toEqual(["u2", "moved"]);
    expect(f.assignmentWrites).toEqual([null]);
  });

  it("admits a pinned arrival to active ordering only after unpinning it", async () => {
    const f = fixture(undefined, undefined, "moved");
    f.setOrderKey("moved", "t");
    expect(
      await placePersonalThread(
        f.input("a", "a1", true, {
          unpin: async () => {
            expect(f.assignmentWrites).toEqual(["a"]);
            expect(f.orderWrites).toEqual([]);
            f.unpin();
            return true;
          },
        }),
      ),
    ).toBe(true);
    expect(f.row("moved").pinnedAt).toBeNull();
    expect(f.row("moved").activeOrderKey).not.toBeNull();
    expect(f.groupOrder("a")).toEqual(["a1", "moved", "a2"]);
    expect(f.groupOrder(null)).toEqual(["u1", "u2"]);
  });

  it.each(["source capability", "unpin callback"])(
    "rejects a pinned ordinal before any changes when missing %s",
    async (missing) => {
      const f = fixture(undefined, undefined, "moved");
      const input = f.input("a", "a1", true);
      const unpin = async () => {
        throw new Error("An invalid placement must leave the thread pinned");
      };
      const reorderableKeys = new Set(input.reorderableKeys);
      if (missing === "source capability") reorderableKeys.delete(shellKey(input.thread));
      expect(
        await placePersonalThread({
          ...input,
          reorderableKeys,
          unpin: missing === "unpin callback" ? undefined : unpin,
        }),
      ).toBe(false);
      expect(f.assignmentWrites).toEqual([]);
      expect(f.orderWrites).toEqual([]);
      expect(f.row("moved").pinnedAt).toBe(at);
      expect(f.positionRejections).toEqual([]);
    },
  );

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
      f.setOrderKey("moved", "a0m");
      expect(
        await placePersonalThread(
          f.input("a", after ? "a2" : "a1", after, {
            unpin: async () => {
              f.unpin();
              return true;
            },
          }),
        ),
      ).toBe(true);
      expect(f.assignmentWrites).toEqual([]);
      expect(f.orderWrites.some((write) => write.id === "moved")).toBe(true);
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

  it.each([false, true])(
    "keeps a pinned group-header drop assignment-only for order keys (external=%s)",
    async (external) => {
      const f = fixture(undefined, undefined, "moved");
      const input = f.input("a", null, false, {
        allowReorder: false,
        unpin: external
          ? async () => {
              expect(f.assignmentWrites).toEqual(["a"]);
              f.unpin();
              return true;
            }
          : undefined,
      });
      expect(await placePersonalThread(input)).toBe(true);
      expect(f.assignmentWrites).toEqual(["a"]);
      expect(f.orderWrites).toEqual([]);
      expect(f.row("moved").pinnedAt).toBe(external ? null : at);
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
    expect(f.positionRejections).toEqual([]);
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
    expect(f.positionRejections).toHaveLength(1);
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
      const f = fixture(
        ["u1", "a1", "b1", "moved", "a2", "hidden", "u2"],
        [
          ["a1", "a"],
          ["a2", "a"],
          ["hidden", "a"],
          ["b1", "b"],
        ],
      );
      f.setOrderKey("hidden", "m");
      f.setOrderKey("moved", "t");
      if (kind === "false") f.failOrderAt(2);
      if (kind === "throw") f.throwOrderAt(2);
      if (kind === "permission") f.revokeAfterWrites(1);
      const input = f.input("a", "a1");
      expect(
        await placePersonalThread({
          ...input,
          activeThreads: input.activeThreads.filter(({ id }) => id !== "hidden"),
        }),
      ).toBe(false);
      expect(f.assignmentWrites).toEqual(["a"]);
      expect(f.orderWrites).toHaveLength(1);
      expect(f.orderWrites[0]!.id).toBe("a2");
      expect(f.groupOrder("a")).toEqual(["a1", "a2", "hidden", "moved"]);
      expect(f.row("hidden").activeOrderKey).toBe("m");
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
