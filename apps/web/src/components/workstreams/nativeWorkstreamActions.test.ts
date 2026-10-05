import type { WorkstreamReceipt, WorkstreamCommand } from "@t3tools/contracts";
import type { WorkstreamListView } from "../../state/workstreams";
import {
  planSelectedShelfDrop,
  runSelectedThreadSteps,
  runSelectedShelfSteps,
  type SelectedShelfThread,
} from "../sidebar/selectedThreadMovement";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  captureDraggedThreadKeys,
  projectWorkstreamShelves,
  moveNativeThreadBlock,
  moveNativeMembershipThreads,
  submitNativeMembershipBatch,
  ThreadMovementError,
  type NativeMembershipAction,
  canEditWorkstreams,
  moveNativeThreadOrder,
  planNativeMembership,
  workstreamTint,
} from "./nativeWorkstreamActions";

import { data, input, placements, primary, reference } from "./nativeWorkstreamActions.fixtures";

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
    expect(workstreamTint("alpha")).toContain("dark:border-");
    expect(workstreamTint("alpha")).toContain("-50/70");
  });
});

describe("selected thread movement", () => {
  it("captures selected rows in rendered order and ignores hidden selection", () => {
    expect(captureDraggedThreadKeys("c", new Set(["hidden", "c", "a"]), ["a", "b", "c"])).toEqual([
      "a",
      "c",
    ]);
    expect(captureDraggedThreadKeys("b", new Set(["a", "c"]), ["a", "b", "c"])).toEqual(["b"]);
  });
  it("inserts one stable selected block without duplicating destination members", () => {
    expect(moveNativeThreadBlock(["a", "b", "c", "d", "e"], ["d", "b"], "c", false)).toEqual([
      "a",
      "d",
      "b",
      "c",
      "e",
    ]);
    expect(moveNativeThreadBlock(["a", "b", "c"], ["c", "a"], null, true)).toEqual(["b", "c", "a"]);
    const order = ["a", "b", "c"];
    expect(moveNativeThreadBlock(order, ["a", "b"], "b", true)).toBe(order);
  });

  const commandId = async () => "command" as WorkstreamCommand["command_id"];
  const action: NativeMembershipAction = {
    operation: "move_primary",
    source_workstream_id: "alpha",
    expected_source_version: 3,
    source_membership_id: "first",
    destination_workstream_id: "beta",
    expected_destination_version: 3,
  };
  const receipt = (registryVersion: number, version: number) =>
    ({
      state: "committed",
      registry_version: registryVersion,
      effects: {
        workstream_versions: [
          { workstream_id: "alpha", version },
          { workstream_id: "beta", version },
        ],
      },
    }) as unknown as WorkstreamReceipt;

  it("advances registry and both Workstream versions from each serial committed receipt", async () => {
    const submit = vi.fn(async (_command: WorkstreamCommand) => receipt(12, 9));
    submit.mockResolvedValueOnce(receipt(12, 9)).mockResolvedValueOnce(receipt(13, 10));
    const completed = await submitNativeMembershipBatch({
      data,
      commandId,
      submit,
      steps: [
        { key: "first", action },
        { key: "second", action: { ...action, source_membership_id: "second" } },
      ],
    });
    expect(completed).toEqual(["first", "second"]);
    expect(submit.mock.calls.map(([command]) => command)).toEqual([
      expect.objectContaining({
        expected_registry_version: 11,
        action: expect.objectContaining({
          expected_source_version: 3,
          expected_destination_version: 3,
        }),
      }),
      expect.objectContaining({
        expected_registry_version: 12,
        action: expect.objectContaining({
          expected_source_version: 9,
          expected_destination_version: 9,
        }),
      }),
    ]);
  });

  it("stops on unresolved and rejected receipts and reports completed, stopped and unprocessed identities", async () => {
    for (const state of ["unresolved", "rejected"] as const) {
      const submit = vi.fn(async (_command: WorkstreamCommand) => receipt(12, 4));
      submit
        .mockResolvedValueOnce(receipt(12, 4))
        .mockResolvedValueOnce({ state } as WorkstreamReceipt);
      const error = await submitNativeMembershipBatch({
        data,
        commandId,
        submit,
        steps: ["first", "second", "third"].map((key) => ({ key, action })),
      }).catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(ThreadMovementError);
      expect(error).toMatchObject({
        completedKeys: ["first"],
        stoppedKey: "second",
        unprocessedKeys: ["third"],
        commandId: "command",
      });
      expect(submit).toHaveBeenCalledTimes(2);
    }
  });

  it("does not retry a transport failure with an unknown effect", async () => {
    const submit = vi.fn(async (_command: WorkstreamCommand): Promise<WorkstreamReceipt> => {
      throw new Error("connection lost");
    });
    await expect(
      submitNativeMembershipBatch({
        data,
        commandId,
        submit,
        steps: ["first", "second"].map((key) => ({ key, action })),
      }),
    ).rejects.toMatchObject({
      completedKeys: [],
      stoppedKey: "first",
      unprocessedKeys: ["second"],
    });
    expect(submit).toHaveBeenCalledOnce();
  });

  it("preflights all selected environments before any membership submission", async () => {
    const submit = vi.fn();
    const controller = {
      data,
      placements,
      loading: false,
      placementInventory: {
        identities: [{ source_instance_id: "env:a", native_thread_id: "thread" }],
      },
      submit,
      runBindingOperation: vi.fn(),
      loadDetail: vi.fn(),
    } as unknown as WorkstreamListView;
    await expect(
      moveNativeMembershipThreads({
        controller,
        threads: [input.thread, { ...input.thread, environmentId: "unavailable" }],
        destination: "beta",
        commandId,
        now: input.now,
      }),
    ).rejects.toThrow("No threads were moved");
    expect(controller.loadDetail).not.toHaveBeenCalled();
    expect(controller.runBindingOperation).not.toHaveBeenCalled();
    expect(submit).not.toHaveBeenCalled();
  });

  const shelfThread = (
    key: string,
    section: SelectedShelfThread["section"],
  ): SelectedShelfThread => ({
    key,
    section,
    pinned: section === "pinned",
    settled: section === "settled",
    supportsPinning: true,
    supportsSettlement: true,
    supportsSnooze: true,
  });
  const shelfInput = {
    initiator: "c",
    threads: [shelfThread("c", "active"), shelfThread("a", "active")],
    target: { section: "pinned" as const, pinnedOrder: ["x", "c", "y"], activeOrder: [] },
    currentOrder: ["x", "y"],
    keysById: new Map<string, string | null>([
      ["x", null],
      ["y", null],
      ["hidden", "V"],
    ]),
    reorderableKeys: new Set(["x", "y", "a", "c"]),
  };
  it("plans selected shelf pinning in captured order and reserves hidden row keys", () => {
    const steps = planSelectedShelfDrop(shelfInput);
    expect(steps.filter((step) => step.operation === "pin").map((step) => step.key)).toEqual([
      "c",
      "a",
    ]);
    const keyById = new Map(
      steps.filter((step) => step.orderKey).map((step) => [step.key, step.orderKey!]),
    );
    expect([...keyById].toSorted((a, b) => a[1].localeCompare(b[1])).map(([key]) => key)).toEqual([
      "x",
      "c",
      "a",
      "y",
    ]);
    expect(steps.some((step) => step.key === "hidden" || step.orderKey === "V")).toBe(false);
  });
  it("orders an already pinned Workstream member at the chosen pin slot without pinning it again", () => {
    const steps = planSelectedShelfDrop({
      ...shelfInput,
      threads: [{ ...shelfThread("c", "active"), pinned: true }],
    });
    expect(steps.some((step) => step.operation === "pin")).toBe(false);
    expect(steps.find((step) => step.key === "c")?.operation).toBe("order-pinned");
    const ordered = steps
      .filter((step) => step.orderKey)
      .toSorted((a, b) => a.orderKey!.localeCompare(b.orderKey!));
    expect(ordered.map((step) => step.key)).toEqual(["x", "c", "y"]);
  });
  it("stops all native shelf commands when membership removal fails", async () => {
    const run = vi.fn(async () => {});
    const failure = new ThreadMovementError(
      "Membership effect unknown",
      ["a"],
      "b",
      ["c"],
      "command",
    );
    const removeMembership = vi.fn(async () => {
      throw failure;
    });
    const error = await runSelectedShelfSteps({
      selectedKeys: ["a", "b", "c"],
      steps: [{ key: "a", operation: "unpin" }],
      removeMembership,
      run,
    }).catch((cause: unknown) => cause);
    expect(run).not.toHaveBeenCalled();
    expect(error).toMatchObject({ completedKeys: [], stoppedKey: "b", commandId: "command" });
    expect(String(error)).toContain("Membership effect unknown");
  });
  it("commits membership removals before the first native shelf command", async () => {
    const calls: string[] = [];
    await runSelectedShelfSteps({
      selectedKeys: ["a", "b"],
      steps: [
        { key: "a", operation: "unpin" },
        { key: "b", operation: "order-active", orderKey: "V" },
      ],
      removeMembership: async () => {
        calls.push("remove-a", "remove-b");
      },
      run: async (step) => {
        calls.push(step.operation);
      },
    });
    expect(calls).toEqual(["remove-a", "remove-b", "unpin", "order-active"]);
  });
  it("requires explicit shelf movement before restoring or unpinning", () => {
    const steps = planSelectedShelfDrop({
      ...shelfInput,
      threads: [shelfThread("c", "pinned"), shelfThread("a", "snoozed")],
      target: { section: "active", activeOrder: ["x", "c", "y"], pinnedOrder: [] },
    });
    expect(
      steps
        .filter((step) => !step.operation.startsWith("order"))
        .map((step) => [step.key, step.operation]),
    ).toEqual([
      ["c", "unpin"],
      ["a", "unsnooze"],
    ]);
  });
  it("rejects unsupported selected targets and neighboring key writes before effects", () => {
    expect(() =>
      planSelectedShelfDrop({
        ...shelfInput,
        threads: [
          shelfThread("c", "active"),
          { ...shelfThread("a", "active"), supportsPinning: false },
        ],
      }),
    ).toThrow("No threads were moved");
    expect(() =>
      planSelectedShelfDrop({ ...shelfInput, reorderableKeys: new Set(["a", "c"]) }),
    ).toThrow("No threads were moved");
  });
  it("retains failed and unprocessed selected rows when a serial native operation fails", async () => {
    const run = vi.fn(async (step: { key: string }) => {
      if (step.key === "b") throw new Error("receipt failed");
    });
    const error = await runSelectedThreadSteps({
      selectedKeys: ["a", "b", "c"],
      steps: ["a", "b", "c"].map((key) => ({ key, operation: "order-active", orderKey: key })),
      run,
    }).catch((cause: unknown) => cause);
    expect(error).toMatchObject({ completedKeys: ["a"], stoppedKey: "b", unprocessedKeys: ["c"] });
    expect(run).toHaveBeenCalledTimes(2);
  });
});

describe("active Workstream shelf projection", () => {
  it("keeps trusted grouped pins exclusively in mixed active groups and fallback pins on Pinned", () => {
    const pinned = {
      environmentId: "env",
      id: "pinned",
      createdAt: "2026-10-01",
      pinnedAt: "2026-10-01",
      activeOrderKey: "a",
      pinOrderKey: "z",
    };
    const active = {
      environmentId: "env",
      id: "active",
      createdAt: "2026-10-01",
      pinnedAt: null,
      activeOrderKey: "b",
      pinOrderKey: null,
    };
    const fallback = { ...pinned, id: "fallback" };
    const unassigned = { ...active, id: "unassigned" };
    const grouping = {
      groups: [{ workstream: data.items[0]!, threads: [active, pinned] }],
      ungrouped: [fallback, unassigned],
      ordered: [pinned, active, fallback, unassigned],
      groupedKeys: new Set([JSON.stringify(["env", "pinned"]), JSON.stringify(["env", "active"])]),
      secondaryWorkstreamIdsByKey: new Map(),
      secondaryWorkstreamLabelsByKey: new Map(),
      conflictingKeys: new Set<string>(),
    };
    const before = structuredClone([pinned, active, fallback, unassigned]);
    const projected = projectWorkstreamShelves(grouping, [fallback, pinned]);
    expect(projected.grouping.groups[0]!.threads).toEqual([pinned, active]);
    expect(projected.grouping.ungrouped).toEqual([unassigned]);
    expect(projected.pinnedThreads).toEqual([fallback]);
    expect([pinned, active, fallback, unassigned]).toEqual(before);
  });
});
