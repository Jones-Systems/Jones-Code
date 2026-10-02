import { describe, expect, it } from "vite-plus/test";
import {
  canEditWorkstreams,
  executeMobileShelfMove,
  moveNativeThreadOrder,
  planNativeMembership,
  workstreamTint,
} from "./actions";

import { data, input, placements, primary, reference } from "./actions.fixtures";

describe("native Workstream membership planning", () => {
  it("moves only primary membership across repositories and preserves native and secondary state", () => {
    const before = structuredClone(input);
    expect(planNativeMembership(input)).toEqual({
      operation: "move_primary",
      source_workstream_id: "alpha",
      expected_source_version: 3,
      source_membership_id: "membership",
      destination_workstream_id: "beta",
      expected_destination_version: 3,
    });
    expect(input).toEqual(before);
  });
  it("removes the primary episode instead of removing history or changing settlement", () => {
    expect(planNativeMembership({ ...input, destination: null })).toEqual({
      operation: "remove_membership",
      workstream_id: "alpha",
      expected_version: 3,
      membership_id: "membership",
    });
  });
  it("assigns a registered Unassigned thread without registering or enrolling it", () => {
    expect(planNativeMembership({ ...input, placements: { ...placements, items: [] } })).toEqual({
      operation: "attach_primary",
      workstream_id: "beta",
      expected_version: 3,
      native_reference_id: "reference",
    });
  });
  it("reattaches a removed primary membership as a new episode", () => {
    const boundary = {
      at: "2026-09-30T11:00:00Z",
      actor: { principal_id: "owner" },
      command_id: "history-command",
      registry_version: 1,
    };
    const episode = {
      membership_id: "old",
      workstream_id: "beta",
      native_reference_id: "reference",
      kind: "primary" as const,
      opened: boundary,
      closed: { ...boundary, reason: "removed" as const, other_reason: null },
    };
    expect(
      planNativeMembership({
        ...input,
        placements: { ...placements, items: [] },
        destinationMemberships: [episode],
      })?.operation,
    ).toBe("reattach_primary");
    expect(episode.closed.reason).toBe("removed");
  });
  it("does nothing when dropped into its current group", () => {
    expect(planNativeMembership({ ...input, destination: "alpha" })).toBeNull();
  });
  it("rejects ambiguous, expired and foreign-environment references", () => {
    for (const references of [
      [],
      [reference, reference],
      [{ ...reference, identity: { ...reference.identity, source_instance_id: "env" } }],
      [
        {
          ...reference,
          registration: { ...reference.registration, expires_at: "2026-09-30T11:59:00Z" },
        },
      ],
    ])
      expect(() => planNativeMembership({ ...input, references })).toThrow("verified reference");
  });
  it("rejects stale or conflicting primary placement", () => {
    for (const items of [
      [{ ...primary, expires_at: "2026-09-30T11:59:00Z" }],
      [primary, { ...primary, membership_id: "other", workstream_id: "beta" }],
    ])
      expect(() =>
        planNativeMembership({ ...input, placements: { ...placements, items } }),
      ).toThrow("stale or conflicting");
  });
  it("rejects replaced authorization and registry snapshots", () => {
    expect(() =>
      planNativeMembership({
        ...input,
        placements: { ...placements, context: { ...placements.context, registry_version: 12 } },
      }),
    ).toThrow("binding changed");
    expect(() =>
      planNativeMembership({
        ...input,
        placements: {
          ...placements,
          context: { ...placements.context, authorization_revision: 2 },
        },
      }),
    ).toThrow("binding changed");
  });
  it("gates mutations for cached, stale, incomplete and read-only lists", () => {
    for (const candidate of [
      { ...data, source: "cache" as const },
      { ...data, stale: true },
      { ...data, nextCursor: "next" },
      { ...data, binding: { ...data.binding, permissions: ["workstreams:read" as const] } },
    ]) {
      expect(canEditWorkstreams(candidate)).toBe(false);
      expect(() => planNativeMembership({ ...input, data: candidate })).toThrow("write access");
    }
  });
});

describe("native member order and decoration", () => {
  it("moves the selected native row while preserving every other row and the input", () => {
    const order = ["a:one", "b:two", "c:three", "d:four"];
    expect(moveNativeThreadOrder(order, "d:four", "b:two", false)).toEqual([
      "a:one",
      "d:four",
      "b:two",
      "c:three",
    ]);
    expect(moveNativeThreadOrder(order, "a:one", "c:three", true)).toEqual([
      "b:two",
      "c:three",
      "a:one",
      "d:four",
    ]);
    expect(order).toEqual(["a:one", "b:two", "c:three", "d:four"]);
    expect(moveNativeThreadOrder(order, "missing", "a:one", false)).toBe(order);
  });
  it("gives a stable ID the same pale light/dark border palette independent of name or position", () => {
    expect(workstreamTint("alpha")).toBe(workstreamTint("alpha"));
    expect(workstreamTint("alpha")).toMatch(/dark:border-/);
    expect(workstreamTint("alpha")).toContain("-50/70");
  });
});

describe("explicit mobile shelf move", () => {
  it("waits for primary removal before native writes", async () => {
    const events: string[] = [];
    let commit: (() => void) | undefined;
    const committed = new Promise<void>((resolve) => {
      commit = resolve;
    });
    const move = executeMobileShelfMove({
      removePrimary: async () => {
        events.push("remove");
        await committed;
        events.push("committed");
      },
      moveNative: async () => {
        events.push("native");
        return true;
      },
    });
    expect(events).toEqual(["remove"]);
    commit!();
    expect(await move).toBe(true);
    expect(events).toEqual(["remove", "committed", "native"]);
  });
  it.each(["unknown", "partial", "rejected"])(
    "stops native effects after %s removal",
    async (state) => {
      const nativeWrites: string[] = [];
      await expect(
        executeMobileShelfMove({
          removePrimary: () => Promise.reject(new Error(state)),
          moveNative: async () => {
            nativeWrites.push("pin");
            return true;
          },
        }),
      ).rejects.toThrow(state);
      expect(nativeWrites).toEqual([]);
    },
  );
  it("removes membership of a pinned thread without changing its pin or active slot", () => {
    const pinned = {
      ...input.thread,
      pinnedAt: "2026-09-30T10:00:00Z",
      pinOrderKey: "bb",
      activeOrderKey: "zz",
    };
    const before = structuredClone(pinned);
    expect(planNativeMembership({ ...input, thread: pinned, destination: null })?.operation).toBe(
      "remove_membership",
    );
    expect(pinned).toEqual(before);
  });
});
