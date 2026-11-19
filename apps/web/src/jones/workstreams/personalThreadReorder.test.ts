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
  it.each([false, true])(
    "preserves every other member's order as a short move streams back (keyed tail: %s)",
    (keyedTail) => {
      const members = ["a1", "a2", "a3", "a4", "a5", "moved", "a6", "a7"];
      const input = fixture(
        ["a1", "u1", "a2", "a3", "a4", "a5", "moved", "a6", "a7"],
        members.map((id) => [id, "a"]),
        keyedTail
          ? [
              ["a6", "m"],
              ["a7", "t"],
              ["hidden", "g"],
            ]
          : [],
      );
      const assignments = planWorkstreamThreadReorder({
        ...input,
        movedId: "moved",
        neighborId: "a3",
        after: false,
      });
      expect(assignments).not.toBeNull();
      const stationary = members.filter((id) => id !== "moved");
      let rows = input.rows;
      for (const { id, orderKey } of assignments!) {
        rows = rows.map((row) => (row.id === id ? { ...row, activeOrderKey: orderKey } : row));
        expect(groupOrder(rows, input.groupIdByKey, "a").filter((id) => id !== "moved")).toEqual(
          stationary,
        );
        expect(orderKey).not.toBe(input.keysById.get("hidden"));
      }
      expect(groupOrder(rows, input.groupIdByKey, "a")).toEqual([
        "a1",
        "a2",
        "moved",
        "a3",
        "a4",
        "a5",
        "a6",
        "a7",
      ]);
      expect(assignments!.map(({ id }) => id)).toEqual([
        ...(keyedTail ? [] : ["a7", "a6"]),
        "a5",
        "a4",
        "a3",
        "moved",
      ]);
    },
  );

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
    expect(assignments!.map(({ id }) => id)).toEqual(["u2", "u1", "u3"]);
    let rows = input.rows;
    for (const { id, orderKey } of assignments!) {
      rows = rows.map((row) => (row.id === id ? { ...row, activeOrderKey: orderKey } : row));
      expect(groupOrder(rows, input.groupIdByKey, "a")).toEqual(["a1", "a2"]);
      expect(groupOrder(rows, input.groupIdByKey, null).filter((id) => id !== "u3")).toEqual([
        "u1",
        "u2",
      ]);
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

  it("preserves fixed members when a tied-key fallback moves siblings in both directions", () => {
    const stationary = ["first", "a1", "a2", "b1", "b2", "b3", "b4", "last"];
    const input = fixture(
      [...stationary, "moved"],
      [],
      [
        ...["b1", "b2", "b3", "b4"].map((id) => [id, "r"] as const),
        ["last", generateSpreadPinOrderKeys(8).at(-1)],
        ["moved", "t"],
      ],
    );
    input.writableKeys.delete("first");
    input.writableKeys.delete("last");
    const assignments = planWorkstreamThreadReorder({
      ...input,
      movedId: "moved",
      neighborId: "b2",
      after: true,
    });
    expect(assignments?.map(({ id }) => id)).toEqual(["b1", "b2", "b4", "b3", "a2", "a1", "moved"]);
    let rows = input.rows;
    for (const { id, orderKey } of assignments!) {
      rows = rows.map((row) => (row.id === id ? { ...row, activeOrderKey: orderKey } : row));
      expect(groupOrder(rows, input.groupIdByKey, null).filter((id) => id !== "moved")).toEqual(
        stationary,
      );
    }
    expect(groupOrder(rows, input.groupIdByKey, null)).toEqual([
      "first",
      "a1",
      "a2",
      "b1",
      "b2",
      "moved",
      "b3",
      "b4",
      "last",
    ]);
  });

  it("splits a tie to the left between fixed keyed bounds and skips a reserved key", () => {
    const input = fixture(
      ["lo", "b1", "b2", "u"],
      [],
      [
        ["lo", "c"],
        ["b1", "m"],
        ["b2", "m"],
        ["u", "t"],
        ["retained", pinOrderKeyBetween("c", "m")],
      ],
    );
    input.writableKeys.delete("lo");
    input.writableKeys.delete("b2");
    const assignments = planWorkstreamThreadReorder({
      ...input,
      movedId: "u",
      neighborId: "b1",
      after: true,
    });
    expect(assignments?.map(({ id }) => id)).toEqual(["b1", "u"]);
    let rows = input.rows;
    for (const { id, orderKey } of assignments!) {
      expect(orderKey).not.toBe(input.keysById.get("retained"));
      rows = rows.map((row) => (row.id === id ? { ...row, activeOrderKey: orderKey } : row));
      expect(groupOrder(rows, input.groupIdByKey, null).filter((id) => id !== "u")).toEqual([
        "lo",
        "b1",
        "b2",
      ]);
    }
    expect(groupOrder(rows, input.groupIdByKey, null)).toEqual(["lo", "b1", "u", "b2"]);
  });

  it("can split a tie beyond a frozen keyless prefix", () => {
    const input = fixture(
      ["a", "frozen", "b1", "b2", "u"],
      [],
      [
        ["b1", "m"],
        ["b2", "m"],
        ["u", "t"],
      ],
    );
    input.writableKeys.delete("frozen");
    expect(
      planWorkstreamThreadReorder({ ...input, movedId: "u", neighborId: "b1", after: true })?.map(
        ({ id }) => id,
      ),
    ).toEqual(["b2", "u"]);
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

describe("cross-group placement planning", () => {
  it.each([
    {
      movedId: "u2",
      neighborId: "a2",
      after: false,
      destinationGroupId: "a",
      expected: ["a1", "u2", "a2"],
    },
    {
      movedId: "a1",
      neighborId: "u1",
      after: true,
      destinationGroupId: null,
      expected: ["u1", "a1", "u2"],
    },
  ])("plans membership and position together for $movedId", (scenario) => {
    const input = fixture(
      ["u1", "a1", "b1", "u2", "a2"],
      [
        ["a1", "a"],
        ["a2", "a"],
        ["b1", "b"],
      ],
    );
    const assignments = planWorkstreamThreadReorder({ ...input, ...scenario });
    expect(assignments).not.toBeNull();
    const membership = new Map(input.groupIdByKey);
    membership.set(scenario.movedId, scenario.destinationGroupId);
    let rows = input.rows;
    for (const { id, orderKey } of assignments!) {
      expect(membership.get(id) ?? null).toBe(scenario.destinationGroupId);
      rows = rows.map((row) => (row.id === id ? { ...row, activeOrderKey: orderKey } : row));
      expect(groupOrder(rows, membership, "b")).toEqual(["b1"]);
    }
    expect(groupOrder(rows, membership, scenario.destinationGroupId)).toEqual(scenario.expected);
  });

  it("includes a pinned thread entering the active list in the indicated destination position", () => {
    const input = fixture(
      ["a1", "a2", "pinned"],
      [
        ["a1", "a"],
        ["a2", "a"],
      ],
    );
    const assignments = planWorkstreamThreadReorder({
      ...input,
      movedId: "pinned",
      neighborId: "a1",
      after: true,
      destinationGroupId: "a",
    });
    expect(assignments).not.toBeNull();
    const membership = new Map(input.groupIdByKey).set("pinned", "a");
    const keys = new Map(assignments!.map(({ id, orderKey }) => [id, orderKey]));
    const rows = input.rows.map((row) => ({
      ...row,
      activeOrderKey: keys.get(row.id) ?? row.activeOrderKey,
    }));
    expect(groupOrder(rows, membership, "a")).toEqual(["a1", "pinned", "a2"]);
  });

  it("rejects a neighbor from a different destination before any assignment", () => {
    const input = fixture(
      ["u1", "a1", "b1"],
      [
        ["a1", "a"],
        ["b1", "b"],
      ],
    );
    expect(
      planWorkstreamThreadReorder({
        ...input,
        movedId: "u1",
        neighborId: "b1",
        after: false,
        destinationGroupId: "a",
      }),
    ).toBeNull();
  });
});
