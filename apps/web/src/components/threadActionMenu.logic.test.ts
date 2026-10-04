import { describe, expect, it, vi } from "vite-plus/test";
import * as DateTime from "effect/DateTime";
import { CommandId } from "@t3tools/contracts";
import type {
  OrchestrationV2CurrentThreadRuntimeTarget,
  OrchestrationV2StopCurrentThreadRuntimeInput,
  OrchestrationV2StopCurrentThreadRuntimeResult,
  OrchestrationV2ThreadRuntimeAttachmentResult,
  ScopedThreadRef,
} from "@t3tools/contracts";

import {
  buildThreadActionMenuItems,
  createCurrentRuntimeStopController,
  currentRuntimeStopMenuTarget,
  type ThreadActionMenuState,
} from "./threadActionMenu.logic";

const baseState: ThreadActionMenuState = {
  branch: null,
  projectFilter: null,
  isPinned: false,
  isSettled: false,
  autoSettleEnabled: true,
  isSnoozed: false,
  canSnoozeNow: true,
  isRegeneratingTitle: false,
  isRunning: false,
  canStopSession: true,
  supports: {
    settlement: true,
    autoSettleOptOut: true,
    snooze: true,
    pinning: true,
    titleRegeneration: true,
  },
  snoozePresets: [
    { id: "hour", label: "In 1 hour", whenLabel: "3:00 PM", snoozedUntil: "2026-08-07T15:00:00Z" },
  ],
};

function ids(state: ThreadActionMenuState): string[] {
  return buildThreadActionMenuItems(state).map((item) => item.id);
}

function allIds(state: ThreadActionMenuState): string[] {
  const flatten = (items: ReturnType<typeof buildThreadActionMenuItems>): string[] =>
    items.flatMap((item) => [item.id, ...(item.children ? flatten(item.children) : [])]);
  return flatten(buildThreadActionMenuItems(state));
}

describe("buildThreadActionMenuItems", () => {
  it("hides lifecycle items when the environment lacks the capabilities", () => {
    expect(
      ids({
        ...baseState,
        supports: {
          settlement: false,
          autoSettleOptOut: false,
          snooze: false,
          pinning: false,
          titleRegeneration: false,
        },
      }),
    ).toEqual([
      "kill-thread",
      "rename",
      "mark-unread",
      "copy",
      "project-settings",
      "archive",
      "delete",
    ]);
  });

  it("groups project settings with utility actions before archive", () => {
    const items = buildThreadActionMenuItems(baseState);
    const copyIndex = items.findIndex((item) => item.id === "copy");
    expect(items[copyIndex + 1]).toMatchObject({
      id: "project-settings",
      label: "Project settings",
      icon: "settings",
    });
    expect(items[copyIndex + 2]?.id).toBe("archive");
  });

  it("offers project filtering only for surfaces with a scoped thread list", () => {
    expect(ids(baseState)).not.toContain("filter-by-project");
    expect(
      buildThreadActionMenuItems({
        ...baseState,
        projectFilter: { label: "Beta Project", isActive: false },
      }).find((item) => item.id === "filter-by-project"),
    ).toMatchObject({ label: "Filter by Beta Project", icon: "folder-tree" });
  });

  it("offers the way back to all projects once the list is scoped", () => {
    const items = buildThreadActionMenuItems({
      ...baseState,
      projectFilter: { label: "Beta Project", isActive: true },
    });
    const filterIndex = items.findIndex((candidate) => candidate.id === "filter-by-project");
    expect(items[filterIndex]).toMatchObject({ label: "Show all projects", icon: "folder-tree" });
    expect(items[filterIndex - 1]?.id).toBe("mark-unread");
    expect(items[filterIndex + 1]?.id).toBe("auto-settle");
  });

  it("includes branch items only for threads with a branch", () => {
    const withBranch = allIds({ ...baseState, branch: "feat/menu" });
    expect(withBranch).toContain("new-thread-on-branch");
    expect(withBranch).toContain("copy-branch");
    expect(allIds(baseState)).not.toContain("new-thread-on-branch");
    expect(allIds(baseState)).not.toContain("copy-branch");
  });

  it("flips lifecycle labels with thread state", () => {
    expect(ids({ ...baseState, isPinned: true, isSettled: true, isSnoozed: true })).toEqual(
      expect.arrayContaining(["unpin", "unsettle", "unsnooze"]),
    );
    expect(ids(baseState)).toEqual(expect.arrayContaining(["pin", "settle", "snooze"]));
  });

  it("places Kill Thread below Settle and disables it after the session stops", () => {
    const items = buildThreadActionMenuItems(baseState);
    const settleIndex = items.findIndex((item) => item.id === "settle");
    expect(items[settleIndex + 1]).toMatchObject({
      id: "kill-thread",
      label: "Kill Thread",
      disabled: false,
    });
    expect(
      buildThreadActionMenuItems({ ...baseState, canStopSession: false }).find(
        (item) => item.id === "kill-thread",
      ),
    ).toMatchObject({ disabled: true });
  });

  it("offers auto-settle as a submenu with the current option checked", () => {
    const find = (state: ThreadActionMenuState) =>
      buildThreadActionMenuItems(state).find((item) => item.id === "auto-settle");
    const on = find(baseState);
    expect(on?.label).toBe("Auto-settle behavior");
    expect(on?.children?.map((child) => [child.id, child.checked])).toEqual([
      ["auto-settle:enabled", true],
      ["auto-settle:disabled", false],
    ]);
    const off = find({ ...baseState, autoSettleEnabled: false });
    expect(off?.children?.map((child) => child.checked)).toEqual([false, true]);
    // Sits with the per-thread settings after Mark unread, not the lifecycle verbs.
    const items = buildThreadActionMenuItems(baseState);
    expect(items[items.findIndex((item) => item.id === "mark-unread") + 1]?.id).toBe("auto-settle");
    expect(
      ids({ ...baseState, supports: { ...baseState.supports, autoSettleOptOut: false } }),
    ).not.toContain("auto-settle");
  });

  it("disables snooze when the thread cannot snooze, keeping presets visible", () => {
    const snooze = buildThreadActionMenuItems({ ...baseState, canSnoozeNow: false }).find(
      (item) => item.id === "snooze",
    );
    expect(snooze?.disabled).toBe(true);
    expect(snooze?.children?.map((child) => child.id)).toEqual(["snooze:hour", "snooze:custom"]);
  });

  it("disables title regeneration while one is in flight", () => {
    const item = buildThreadActionMenuItems({ ...baseState, isRegeneratingTitle: true }).find(
      (candidate) => candidate.id === "regenerate-title",
    );
    expect(item).toMatchObject({ label: "Regenerating…", disabled: true });
  });

  it("marks delete as destructive and keeps it last", () => {
    const items = buildThreadActionMenuItems({ ...baseState, branch: "main" });
    expect(items.at(-1)).toMatchObject({ id: "delete", destructive: true });
  });
  it("offers archive as a non-destructive action right before delete", () => {
    const items = buildThreadActionMenuItems(baseState);
    const archiveItem = items.at(-2);
    expect(archiveItem?.id).toBe("archive");
    expect(archiveItem?.icon).toBe("archive");
    expect(archiveItem?.separatorBefore).toBe(true);
    expect(archiveItem?.destructive).toBeFalsy();
    expect(items.at(-1)?.id).toBe("delete");
  });

  it("keeps archive available even when the environment lacks every other capability", () => {
    expect(
      ids({
        ...baseState,
        supports: {
          settlement: false,
          autoSettleOptOut: false,
          snooze: false,
          pinning: false,
          titleRegeneration: false,
        },
      }),
    ).toContain("archive");
  });

  it("disables archive while the thread is running", () => {
    const archiveItem = buildThreadActionMenuItems({ ...baseState, isRunning: true }).find(
      (item) => item.id === "archive",
    );
    expect(archiveItem?.disabled).toBe(true);
  });
});

describe("current runtime stop command correlation", () => {
  const threadRef = {
    environmentId: "environment:current",
    threadId: "thread:current",
  } as ScopedThreadRef;
  const target = {
    binding: {
      threadId: threadRef.threadId,
      providerThreadId: "provider:current",
      providerSessionId: "session:current",
      instanceId: "codex",
      runtimeGeneration: "generation:current",
      nativeThreadId: "native:current",
    },
    driver: "codex",
    evidenceRevision: 7,
  } as OrchestrationV2CurrentThreadRuntimeTarget;
  const result = (
    input: OrchestrationV2StopCurrentThreadRuntimeInput,
    patch = {},
  ): OrchestrationV2StopCurrentThreadRuntimeResult =>
    ({
      version: 2,
      commandId: input.commandId,
      threadId: input.threadId,
      target: input.target,
      commandStatus: "accepted",
      receipt: {
        commandId: input.commandId,
        threadId: input.threadId,
        commandType: "provider-session.detach",
        acceptedAt: DateTime.makeUnsafe("2026-10-03T00:00:00Z"),
        resultSequence: 1,
        status: "accepted",
        error: null,
      },
      queueFence: { status: "installed", affectedRunIds: ["run:held"] },
      runtimeStop: { status: "pending" },
      reason: null,
      ...patch,
    }) as never;
  const attachment = (
    runtimeStatus = "idle",
    patch = {},
  ): OrchestrationV2ThreadRuntimeAttachmentResult =>
    ({
      threadId: threadRef.threadId,
      stopCapability: { version: 2 },
      attachment: {
        status: "attached",
        binding: target.binding,
        driver: target.driver,
        evidenceRevision: target.evidenceRevision,
        runtimeStatus,
        observedAt: "2026-10-03T00:00:00Z",
      },
      ...patch,
    }) as never;
  const harness = () => {
    let pointer: OrchestrationV2StopCurrentThreadRuntimeInput | null = null;
    const options = {
      read: () => pointer,
      reserve: vi.fn(
        (_ref: ScopedThreadRef, input: OrchestrationV2StopCurrentThreadRuntimeInput) => {
          pointer = input;
        },
      ),
      clear: vi.fn(() => {
        pointer = null;
      }),
      stop: vi.fn(
        async (_ref: ScopedThreadRef, input: OrchestrationV2StopCurrentThreadRuntimeInput) =>
          result(input),
      ),
      observe: vi.fn(
        async (
          _ref: ScopedThreadRef,
          _input: Pick<OrchestrationV2StopCurrentThreadRuntimeInput, "commandId" | "threadId">,
        ) => result(pointer!),
      ),
    };
    return { options, read: () => pointer, run: createCurrentRuntimeStopController(options) };
  };

  it.each(["idle", "error"])(
    "enables Kill only for an advertised capability and exact attached %s runtime",
    (runtimeStatus) => {
      const captured = currentRuntimeStopMenuTarget(attachment(runtimeStatus), threadRef.threadId);
      expect(captured).toEqual(target);
      expect(captured!.binding).not.toBe(target.binding);
      expect(
        buildThreadActionMenuItems({ ...baseState, canStopSession: captured !== null }).find(
          (item) => item.id === "kill-thread",
        )?.disabled,
      ).toBe(false);
    },
  );

  it.each([
    null,
    attachment("idle", { stopCapability: undefined }),
    attachment("idle", { stopCapability: null }),
    attachment("idle", { stopCapability: { version: 1 } }),
    attachment("idle", { threadId: "thread:other" }),
    attachment("idle", {
      attachment: {
        status: "stopped",
        reason: "runtime_not_resident",
        observedAt: "2026-10-03T00:00:00Z",
      },
    }),
    attachment("idle", {
      attachment: {
        status: "unknown",
        reason: "Current binding is unknown.",
        observedAt: "2026-10-03T00:00:00Z",
      },
    }),
  ])("keeps Kill disabled without current target and canonical capability %#", (value) => {
    const captured = currentRuntimeStopMenuTarget(value, threadRef.threadId);
    expect(captured).toBeNull();
    expect(
      buildThreadActionMenuItems({ ...baseState, canStopSession: captured !== null }).find(
        (item) => item.id === "kill-thread",
      )?.disabled,
    ).toBe(true);
  });

  it("captures one exact current target, persists before RPC and shares simultaneous calls", async () => {
    const { run, options, read } = harness();
    let complete!: (value: OrchestrationV2StopCurrentThreadRuntimeResult) => void;
    options.stop.mockImplementation(
      () =>
        new Promise((resolve) => {
          complete = resolve;
        }),
    );
    const first = run(threadRef, target);
    expect(run(threadRef, target)).toBe(first);
    await Promise.resolve();
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(options.stop.mock.calls[0]![1].target).toEqual(target);
    expect(options.reserve.mock.invocationCallOrder[0]).toBeLessThan(
      options.stop.mock.invocationCallOrder[0]!,
    );
    complete(result(read()!));
    expect((await first).status).toBe("pending");
    expect(options.clear).not.toHaveBeenCalled();
    expect(read()).not.toBeNull();
  });

  it("observes the saved command and original target after response loss and attachment replacement", async () => {
    const { run, options, read } = harness();
    options.stop.mockRejectedValue(new Error("Lost response"));
    expect((await run(threadRef, target)).status).toBe("unknown");
    const original = read()!;
    const replacement = {
      ...target,
      evidenceRevision: 8,
      binding: { ...target.binding, providerSessionId: "session:replacement" as never },
    };
    expect((await run(threadRef, replacement)).status).toBe("pending");
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(options.observe).toHaveBeenCalledWith(threadRef, {
      threadId: original.threadId,
      commandId: original.commandId,
    });
    expect(read()).toEqual(original);
    expect(options.clear).not.toHaveBeenCalled();
  });

  it("does not create a stop operation when observing without a saved pointer", async () => {
    const { run, options } = harness();
    expect(await run.observe(threadRef)).toBeNull();
    expect(options.reserve).not.toHaveBeenCalled();
    expect(options.stop).not.toHaveBeenCalled();
    expect(options.observe).not.toHaveBeenCalled();
  });

  it("observes the original command after controller remount without a current attachment", async () => {
    const { run, options, read } = harness();
    options.stop.mockRejectedValue(new Error("Lost response"));
    expect((await run(threadRef, target)).status).toBe("unknown");
    const original = read()!;
    const remounted = createCurrentRuntimeStopController(options);
    expect((await remounted.observe(threadRef))?.status).toBe("pending");
    expect(options.observe).toHaveBeenCalledWith(threadRef, {
      threadId: original.threadId,
      commandId: original.commandId,
    });
    expect(options.reserve).toHaveBeenCalledTimes(1);
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(read()).toEqual(original);
  });

  it("shares an in-flight request with observation instead of issuing another RPC", async () => {
    const { run, options, read } = harness();
    let complete!: (value: OrchestrationV2StopCurrentThreadRuntimeResult) => void;
    options.stop.mockImplementation(
      () =>
        new Promise((resolve) => {
          complete = resolve;
        }),
    );
    const pending = run(threadRef, target);
    expect(run.observe(threadRef)).toBe(pending);
    await Promise.resolve();
    complete(result(read()!));
    expect((await pending).status).toBe("pending");
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(options.observe).not.toHaveBeenCalled();
  });

  it("does not retry a failed pre-RPC save while observing its in-memory pointer", async () => {
    const { run, options, read } = harness();
    const save = options.reserve.getMockImplementation()!;
    options.reserve.mockImplementationOnce((ref, input) => {
      save(ref, input);
      throw new Error("Durable readback failed");
    });
    await expect(run(threadRef, target)).rejects.toThrow("Durable readback failed");
    const original = read()!;
    options.observe.mockImplementation(async () =>
      result(original, {
        target: null,
        commandStatus: "not_found",
        receipt: null,
        queueFence: { status: "unknown", affectedRunIds: [] },
        runtimeStop: { status: "unknown" },
      }),
    );
    expect((await run.observe(threadRef))?.status).toBe("unknown");
    expect(options.reserve).toHaveBeenCalledTimes(1);
    expect(options.stop).not.toHaveBeenCalled();
    expect(options.observe).toHaveBeenCalledWith(threadRef, {
      threadId: original.threadId,
      commandId: original.commandId,
    });
    expect(read()).toEqual(original);
    expect((await run(threadRef, target)).status).toBe("pending");
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(read()!.commandId).toBe(original.commandId);
  });

  it("never redispatches when a captured observation races pointer cleanup", async () => {
    const { run, options, read } = harness();
    await run(threadRef, target);
    const original = read()!;
    options.observe.mockImplementation(async () => result(original));
    const pending = run.observe(threadRef);
    options.clear();
    expect((await pending)?.status).toBe("pending");
    expect(options.observe).toHaveBeenCalledWith(threadRef, {
      threadId: original.threadId,
      commandId: original.commandId,
    });
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(options.reserve).toHaveBeenCalledTimes(1);
    expect(read()).toBeNull();
  });

  it("rejects a saved operation appearing after a guarded empty-pointer preflight", async () => {
    const { run, options, read } = harness();
    await run(threadRef, target);
    const original = read()!;
    options.clear();
    options.stop.mockClear();
    options.observe.mockClear();
    const pending = run(threadRef, target, null);
    options.reserve(threadRef, original);
    expect(await pending).toMatchObject({ status: "unknown", commandId: null, target: null });
    expect(options.stop).not.toHaveBeenCalled();
    expect(options.observe).not.toHaveBeenCalled();
    expect(read()).toEqual(original);
  });

  it("rejects a replacement command between guarded preflight and scheduled pointer read", async () => {
    const { run, options, read } = harness();
    await run(threadRef, target);
    const original = read()!;
    options.stop.mockClear();
    const pending = run(threadRef, target, original);
    const replacement = { ...original, commandId: CommandId.make("command:replacement") };
    options.reserve(threadRef, replacement);
    expect(await pending).toMatchObject({ status: "unknown", commandId: null, target: null });
    expect(options.stop).not.toHaveBeenCalled();
    expect(options.observe).not.toHaveBeenCalled();
    expect(read()).toEqual(replacement);
  });

  it("does not give a replacement target the completion of an in-flight original stop", async () => {
    const { run, options, read } = harness();
    let complete!: (value: OrchestrationV2StopCurrentThreadRuntimeResult) => void;
    options.stop.mockImplementation(
      () =>
        new Promise((resolve) => {
          complete = resolve;
        }),
    );
    const pending = run(threadRef, target, null);
    await Promise.resolve();
    const original = read()!;
    const replacement = {
      ...original,
      commandId: CommandId.make("command:replacement"),
      target: { ...target, evidenceRevision: 8 },
    };
    expect(await run(threadRef, replacement.target, replacement)).toMatchObject({
      status: "unknown",
      commandId: null,
      target: null,
    });
    expect(await run.observe(threadRef, replacement)).toMatchObject({
      status: "unknown",
      commandId: null,
      target: null,
    });
    expect(run(threadRef, original.target, original)).toBe(pending);
    expect(run.observe(threadRef, original)).toBe(pending);
    complete(result(original, { runtimeStop: { status: "stopped" } }));
    expect(await pending).toMatchObject({
      status: "stopped",
      commandId: original.commandId,
      target: original.target,
    });
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(options.observe).not.toHaveBeenCalled();
  });

  it("never retargets a stale status callback to a replacement saved pointer", async () => {
    const { run, options, read } = harness();
    await run(threadRef, target);
    const original = read()!;
    const replacement = {
      ...original,
      commandId: CommandId.make("command:replacement"),
      target: { ...target, evidenceRevision: 8 },
    };
    options.reserve(threadRef, replacement);
    expect(await run.observe(threadRef, original)).toMatchObject({
      status: "unknown",
      commandId: null,
      target: null,
    });
    expect(options.observe).not.toHaveBeenCalled();
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(read()).toEqual(replacement);
    expect(await run.observe(threadRef, replacement)).toMatchObject({
      status: "pending",
      commandId: replacement.commandId,
      target: replacement.target,
    });
    expect(options.observe).toHaveBeenCalledWith(threadRef, {
      threadId: replacement.threadId,
      commandId: replacement.commandId,
    });
  });

  it("retains uncertainty when the original status callback has lost its pointer", async () => {
    const { run, options, read } = harness();
    await run(threadRef, target);
    const original = read()!;
    options.clear();
    expect(await run.observe(threadRef, original)).toMatchObject({
      status: "unknown",
      commandId: null,
      target: null,
    });
    expect(options.observe).not.toHaveBeenCalled();
    expect(options.stop).toHaveBeenCalledTimes(1);
  });

  it("rejects a changed native generation even when the saved command ID is unchanged", async () => {
    const { run, options, read } = harness();
    await run(threadRef, target);
    const original = read()!;
    const replacement = {
      ...original,
      target: {
        ...original.target,
        binding: { ...original.target.binding, runtimeGeneration: "generation:replacement" },
      },
    };
    options.reserve(threadRef, replacement);
    expect(await run.observe(threadRef, original)).toMatchObject({
      status: "unknown",
      commandId: null,
      target: null,
    });
    expect(await run(threadRef, original.target, original)).toMatchObject({
      status: "unknown",
      commandId: null,
      target: null,
    });
    expect(options.observe).not.toHaveBeenCalled();
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(read()).toEqual(replacement);
  });

  it("returns the actual original carrier after a pointer replacement during the RPC", async () => {
    const { run, options, read } = harness();
    let complete!: (value: OrchestrationV2StopCurrentThreadRuntimeResult) => void;
    options.stop.mockImplementation(
      () =>
        new Promise((resolve) => {
          complete = resolve;
        }),
    );
    const pending = run(threadRef, target, null);
    await Promise.resolve();
    const original = read()!;
    const replacement = {
      ...original,
      commandId: CommandId.make("command:replacement"),
      target: { ...target, evidenceRevision: 8 },
    };
    options.reserve(threadRef, replacement);
    complete(result(original));
    expect(await pending).toMatchObject({
      status: "pending",
      commandId: original.commandId,
      target: original.target,
    });
    expect(read()).toEqual(replacement);
    expect(options.clear).not.toHaveBeenCalled();
  });

  it("never claims runtime stop from acceptance and a queue fence alone", async () => {
    const { run, options, read } = harness();
    expect((await run(threadRef, target)).status).toBe("pending");
    options.observe.mockImplementation(async () =>
      result(read()!, { runtimeStop: { status: "stopped" } }),
    );
    expect((await run(threadRef, target)).status).toBe("stopped");
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(options.clear).toHaveBeenCalledTimes(1);
  });

  it("retains an unknown pointer on a missing or mismatched result target", async () => {
    const { run, options, read } = harness();
    options.stop.mockImplementation(async (_ref, input) =>
      result(input, {
        target: null,
        commandStatus: "unknown",
        receipt: null,
        queueFence: { status: "unknown", affectedRunIds: [] },
        runtimeStop: { status: "unknown" },
      }),
    );
    expect((await run(threadRef, target)).status).toBe("unknown");
    const original = read()!;
    options.observe.mockImplementation(async () =>
      result(original, {
        target: { ...target, evidenceRevision: 99 },
        runtimeStop: { status: "stopped" },
      }),
    );
    expect((await run(threadRef, target)).status).toBe("unknown");
    expect(read()).toEqual(original);
    expect(options.clear).not.toHaveBeenCalled();
  });

  it("sends no stop RPC when correlation storage fails", async () => {
    const { run, options } = harness();
    options.reserve.mockImplementation(() => {
      throw new Error("Storage is full");
    });
    await expect(run(threadRef, target)).rejects.toThrow("Storage is full");
    expect(options.stop).not.toHaveBeenCalled();
    expect(options.observe).not.toHaveBeenCalled();
  });

  it("permits an explicit save retry only for the same live pre-RPC operation", async () => {
    const { run, options, read } = harness();
    options.reserve.mockImplementationOnce(() => {
      throw new Error("Storage is full");
    });
    await expect(run(threadRef, target)).rejects.toThrow("Storage is full");
    const originalId = options.reserve.mock.calls[0]![1].commandId;
    expect((await run(threadRef, target)).status).toBe("pending");
    expect(read()!.commandId).toBe(originalId);
    expect(options.stop).toHaveBeenCalledTimes(1);
    expect(options.reserve).toHaveBeenCalledTimes(2);
  });

  it("keeps a confirmed result when terminal correlation cleanup fails after RPC", async () => {
    const { run, options } = harness();
    options.stop.mockImplementation(async (_ref, input) =>
      result(input, { runtimeStop: { status: "stopped" } }),
    );
    options.clear.mockImplementation(() => {
      throw new Error("Storage is full");
    });
    const outcome = await run(threadRef, target);
    expect(outcome.status).toBe("stopped");
    expect(outcome.reason).toContain("saved correlation could not be cleared");
    expect(options.stop).toHaveBeenCalledTimes(1);
  });
});
