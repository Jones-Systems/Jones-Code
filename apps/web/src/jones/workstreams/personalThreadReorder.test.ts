import {
  generateSpreadPinOrderKeys,
  pinOrderKeyBetween,
  sortActiveThreadsByOrderKey,
} from "@t3tools/client-runtime/state/thread-sort";
import { describe, expect, it } from "vite-plus/test";

import { planWorkstreamThreadReorder } from "./personalThreadReorder";

function fixture(
  ids: readonly string[],
  groups: ReadonlyArray<readonly [string, string | null | undefined]>,
  keys: ReadonlyArray<readonly [string, string | null | undefined]> = [],
) {
  const keysById = new Map(keys);
  const rows = ids.map((id, index) => ({
    id,
    createdAt: new Date(Date.UTC(2026, 0, 20 - index)).toISOString(),
    activeOrderKey: keysById.get(id),
  }));
  return {
    rows,
    activeOrder: sortActiveThreadsByOrderKey(rows).map(({ id }) => id),
    keysById,
    writableKeys: new Set(ids),
    groupIdByKey: new Map(groups),
  };
}

const groupOrder = (
  rows: ReturnType<typeof fixture>["rows"],
  groups: ReadonlyMap<string, string | null | undefined>,
  groupId: string | null,
) =>
  sortActiveThreadsByOrderKey(rows)
    .filter(({ id }) => (groups.get(id) ?? null) === groupId)
    .map(({ id }) => id);

describe("planWorkstreamThreadReorder", () => {
  it("writes only the source workstream and preserves Unassigned order after every assignment", () => {
    const input = fixture(
      ["a1", "u1", "a2", "u2", "b1", "u3", "a3"],
      [
        ["a1", "a"],
        ["a2", "a"],
        ["a3", "a"],
        ["b1", "b"],
      ],
    );
    let rows = input.rows;
    for (const [movedId, neighborId] of [
      ["a3", "a1"],
      ["a2", "a3"],
    ] as const) {
      const assignments = planWorkstreamThreadReorder({
        ...input,
        activeOrder: sortActiveThreadsByOrderKey(rows).map(({ id }) => id),
        keysById: new Map(rows.map(({ id, activeOrderKey }) => [id, activeOrderKey])),
        movedId,
        neighborId,
        after: false,
      });
      expect(assignments).not.toBeNull();
      expect(assignments!.length).toBeGreaterThan(0);
      for (const { id, orderKey } of assignments!) {
        expect(input.groupIdByKey.get(id)).toBe("a");
        rows = rows.map((row) => (row.id === id ? { ...row, activeOrderKey: orderKey } : row));
        expect(groupOrder(rows, input.groupIdByKey, null)).toEqual(["u1", "u2", "u3"]);
        expect(groupOrder(rows, input.groupIdByKey, "b")).toEqual(["b1"]);
      }
    }
    expect(groupOrder(rows, input.groupIdByKey, "a")).toEqual(["a2", "a3", "a1"]);
  });

  it("reorders only Unassigned, normalizing absent, undefined and null group IDs", () => {
    const input = fixture(
      ["a1", "u1", "u2", "a2", "u3"],
      [
        ["a1", "a"],
        ["a2", "a"],
        ["u1", null],
        ["u3", undefined],
      ],
    );
    const assignments = planWorkstreamThreadReorder({
      ...input,
      movedId: "u3",
      neighborId: "u1",
      after: false,
    });
    expect(assignments).not.toBeNull();
    expect(assignments!.map(({ id }) => id)).toEqual(["u3", "u1", "u2"]);
    let rows = input.rows;
    for (const { id, orderKey } of assignments!) {
      rows = rows.map((row) => (row.id === id ? { ...row, activeOrderKey: orderKey } : row));
      expect(groupOrder(rows, input.groupIdByKey, "a")).toEqual(["a1", "a2"]);
    }
    expect(groupOrder(rows, input.groupIdByKey, null)).toEqual(["u3", "u1", "u2"]);
  });

  it.each([
    { movedId: "a1", neighborId: "a1", after: true },
    { movedId: "a1", neighborId: "a2", after: false },
    { movedId: "a2", neighborId: "a1", after: true },
  ])("does not materialize keyless rows for a no-op: $movedId / $neighborId / $after", (move) => {
    const input = fixture(
      ["a1", "u1", "a2"],
      [
        ["a1", "a"],
        ["a2", "a"],
      ],
    );
    expect(planWorkstreamThreadReorder({ ...input, ...move })).toEqual([]);
  });

  it.each([
    ["a1", "b1"],
    ["a1", "u1"],
    ["u1", "a1"],
  ])("rejects moving %s across the group boundary to %s", (movedId, neighborId) => {
    const input = fixture(
      ["a1", "b1", "u1"],
      [
        ["a1", "a"],
        ["b1", "b"],
      ],
    );
    expect(planWorkstreamThreadReorder({ ...input, movedId, neighborId, after: false })).toBeNull();
  });

  it.each([
    { movedId: "missing", neighborId: "a1", writable: ["a1", "a2", "missing"] },
    { movedId: "a1", neighborId: "missing", writable: ["a1", "a2", "missing"] },
    { movedId: "a2", neighborId: "a1", writable: ["a1"] },
    { movedId: "a2", neighborId: "a1", writable: ["a2"] },
  ])("rejects nonmembers or unwritable endpoints: $movedId / $neighborId / $writable", (move) => {
    const input = fixture(["a1", "a2"], []);
    expect(
      planWorkstreamThreadReorder({
        ...input,
        movedId: move.movedId,
        neighborId: move.neighborId,
        after: false,
        writableKeys: new Set(move.writable),
      }),
    ).toBeNull();
  });

  it.each(["keyed", "keyless"] as const)(
    "reserves hidden and other-group keys during %s planning",
    (kind) => {
      const hiddenKey =
        kind === "keyed" ? pinOrderKeyBetween(null, "m")! : generateSpreadPinOrderKeys(4)[0]!;
      const otherKey =
        kind === "keyed" ? pinOrderKeyBetween(hiddenKey, "m")! : generateSpreadPinOrderKeys(4)[1]!;
      const input = fixture(
        ["a1", "a2", "b1"],
        [
          ["a1", "a"],
          ["a2", "a"],
          ["b1", "b"],
        ],
        [
          ["a1", kind === "keyed" ? "m" : null],
          ["a2", kind === "keyed" ? "t" : null],
          ["hidden", hiddenKey],
          ["b1", otherKey],
        ],
      );
      const assignments = planWorkstreamThreadReorder({
        ...input,
        movedId: "a2",
        neighborId: "a1",
        after: false,
      });
      expect(assignments).not.toBeNull();
      expect(assignments!.length).toBeGreaterThan(0);
      for (const { id, orderKey } of assignments!) {
        expect(input.groupIdByKey.get(id)).toBe("a");
        expect([hiddenKey, otherKey]).not.toContain(orderKey);
      }
      const updates = new Map(assignments!.map(({ id, orderKey }) => [id, orderKey]));
      const rows = input.rows.map((row) => ({
        ...row,
        activeOrderKey: updates.get(row.id) ?? row.activeOrderKey,
      }));
      expect(groupOrder(rows, input.groupIdByKey, "a")).toEqual(["a2", "a1"]);
    },
  );

  it("places a row after its neighbor within the same group", () => {
    const input = fixture(["a1", "a2", "a3"], []);
    const assignments = planWorkstreamThreadReorder({
      ...input,
      movedId: "a1",
      neighborId: "a3",
      after: true,
    });
    expect(assignments).not.toBeNull();
    const updates = new Map(assignments!.map(({ id, orderKey }) => [id, orderKey]));
    const rows = input.rows.map((row) => ({
      ...row,
      activeOrderKey: updates.get(row.id) ?? row.activeOrderKey,
    }));
    expect(groupOrder(rows, input.groupIdByKey, null)).toEqual(["a2", "a3", "a1"]);
  });

  it("refuses materialization ahead of a frozen keyless row in the same group", () => {
    const input = fixture(["frozen", "a1", "a2"], []);
    expect(
      planWorkstreamThreadReorder({
        ...input,
        movedId: "a2",
        neighborId: "a1",
        after: false,
        writableKeys: new Set(["a1", "a2"]),
      }),
    ).not.toBeNull();
    const crossing = fixture(["a1", "frozen", "a2"], []);
    expect(
      planWorkstreamThreadReorder({
        ...crossing,
        movedId: "a2",
        neighborId: "a1",
        after: false,
        writableKeys: new Set(["a1", "a2"]),
      }),
    ).toBeNull();
  });
});
