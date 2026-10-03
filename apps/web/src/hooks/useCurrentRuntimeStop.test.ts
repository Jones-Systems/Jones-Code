import {
  CommandId,
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2CurrentThreadRuntimeTarget,
  type OrchestrationV2StopCurrentThreadRuntimeInput,
  type OrchestrationV2StopCurrentThreadRuntimeResult,
  type OrchestrationV2ThreadRuntimeAttachmentResult,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

import type { CurrentRuntimeStopPointer } from "../composerDraftStore";
import { createCurrentRuntimeStopPort, useCurrentRuntimeStop } from "./useCurrentRuntimeStop";

const hook = vi.hoisted(() => ({
  pointer: null as CurrentRuntimeStopPointer | null,
  readAttachment: vi.fn(),
  reserve: vi.fn(),
  clear: vi.fn(),
  stop: vi.fn(),
  observe: vi.fn(),
}));

vi.mock("react", async (original) => ({
  ...(await original<typeof import("react")>()),
  useMemo: (create: () => unknown) => create(),
}));
vi.mock("../composerDraftStore", () => ({
  useComposerDraftStore: { getState: () => ({ getComposerDraft: () => ({ currentRuntimeStop: hook.pointer }) }) },
  reserveCurrentRuntimeStopPointer: (pointer: CurrentRuntimeStopPointer) => hook.reserve(pointer),
  clearCurrentRuntimeStopPointer: (pointer: CurrentRuntimeStopPointer) => hook.clear(pointer),
}));
vi.mock("../state/threads", () => ({
  threadContinuation: { runtimeAttachment: "read-current-attachment" },
  threadEnvironment: {
    stopCurrentThreadRuntime: "stop-current-runtime",
    observeCurrentThreadRuntimeStop: "observe-current-runtime-stop",
  },
}));
vi.mock("../state/use-atom-query-runner", () => ({
  useAtomQueryRunner: () => hook.readAttachment,
}));
vi.mock("../state/use-atom-command", () => ({
  useAtomCommand: (command: string) => {
    if (command === "stop-current-runtime") return hook.stop;
    if (command === "observe-current-runtime-stop") return hook.observe;
    throw new Error(`Unexpected command: ${command}`);
  },
}));

const ref: ScopedThreadRef = {
  environmentId: EnvironmentId.make("environment:current-stop"),
  threadId: ThreadId.make("thread:current-stop"),
};
const target: OrchestrationV2CurrentThreadRuntimeTarget = {
  binding: {
    threadId: ref.threadId,
    providerThreadId: ProviderThreadId.make("provider-thread:active-owner"),
    providerSessionId: ProviderSessionId.make("session:active-owner"),
    instanceId: ProviderInstanceId.make("active-owner"),
    runtimeGeneration: "generation:original",
    nativeThreadId: "native:original",
  },
  driver: ProviderDriverKind.make("codex"),
  evidenceRevision: 7,
};
const saved = (patch: Partial<CurrentRuntimeStopPointer> = {}): CurrentRuntimeStopPointer => ({
  environmentId: ref.environmentId,
  threadId: ref.threadId,
  commandId: CommandId.make("command:original-stop"),
  target,
  ...patch,
});
const attached = (): OrchestrationV2ThreadRuntimeAttachmentResult => ({
  threadId: ref.threadId,
  stopCapability: { version: 2 },
  attachment: {
    status: "attached", binding: target.binding, driver: target.driver,
    evidenceRevision: target.evidenceRevision, runtimeStatus: "idle",
    observedAt: "2026-10-03T07:12:27Z",
  },
});
const receipt = (
  input: OrchestrationV2StopCurrentThreadRuntimeInput,
  patch: Partial<OrchestrationV2StopCurrentThreadRuntimeResult> = {},
): OrchestrationV2StopCurrentThreadRuntimeResult => ({
  version: 2,
  commandId: input.commandId,
  threadId: input.threadId,
  target: input.target,
  commandStatus: "accepted",
  receipt: {
    commandId: input.commandId,
    threadId: input.threadId,
    commandType: "provider-session.detach",
    acceptedAt: DateTime.makeUnsafe("2026-10-03T07:12:27Z"),
    resultSequence: 1,
    status: "accepted",
    error: null,
  },
  queueFence: { status: "installed", affectedRunIds: [] },
  runtimeStop: { status: "pending" },
  reason: null,
  ...patch,
});

function harness(initial: CurrentRuntimeStopPointer | null = null) {
  let pointer = initial;
  const order: string[] = [];
  const options = {
    readPointer: vi.fn(() => { order.push("pointer"); return pointer; }),
    readAttachment: vi.fn(async () => { order.push("attachment"); return attached(); }),
    reserve: vi.fn((threadRef: ScopedThreadRef, input: OrchestrationV2StopCurrentThreadRuntimeInput) => {
      order.push("reserve");
      pointer = { environmentId: threadRef.environmentId, ...input };
    }),
    clear: vi.fn((_threadRef: ScopedThreadRef, input: OrchestrationV2StopCurrentThreadRuntimeInput) => {
      if (pointer?.commandId === input.commandId) pointer = null;
    }),
    stop: vi.fn(async (_threadRef: ScopedThreadRef, input: OrchestrationV2StopCurrentThreadRuntimeInput) => {
      order.push("stop");
      expect(pointer?.commandId).toBe(input.commandId);
      return receipt(input);
    }),
    observe: vi.fn(async (_threadRef: ScopedThreadRef, input: Pick<OrchestrationV2StopCurrentThreadRuntimeInput, "threadId" | "commandId">) => {
      if (pointer === null || pointer.commandId !== input.commandId) throw new Error("Missing original operation");
      return receipt(pointer);
    }),
  };
  return {
    options,
    order,
    port: createCurrentRuntimeStopPort(options),
    pointer: () => pointer,
    replace: (next: CurrentRuntimeStopPointer | null) => { pointer = next; },
  };
}

describe("current runtime stop shared port", () => {
  it("reads the saved pointer before a fresh capability-gated owner capture", async () => {
    const h = harness(saved());
    await expect(h.port.capture(ref)).resolves.toEqual({ status: "current", target });
    expect(h.order).toEqual(["pointer", "attachment", "pointer"]);
    expect(h.options.stop).not.toHaveBeenCalled();
    expect(h.options.observe).not.toHaveBeenCalled();
  });

  it.each([
    { threadId: ref.threadId, attachment: attached().attachment },
    { ...attached(), stopCapability: null },
    { ...attached(), attachment: { status: "stopped", reason: "runtime_not_resident", observedAt: "2026-10-03T07:12:27Z" } },
    { ...attached(), attachment: { status: "unknown", reason: "Current owner read failed", observedAt: "2026-10-03T07:12:27Z" } },
    { ...attached(), threadId: ThreadId.make("thread:other") },
  ] satisfies ReadonlyArray<OrchestrationV2ThreadRuntimeAttachmentResult>)("keeps absent capability, nonresidency, unknown and wrong-thread reads unavailable: %j", async (result) => {
    const h = harness();
    h.options.readAttachment.mockResolvedValue(result);
    expect((await h.port.capture(ref)).status).toBe("unavailable");
    expect(h.options.stop).not.toHaveBeenCalled();
    expect(h.options.reserve).not.toHaveBeenCalled();
  });

  it("keeps failed attachment transport unavailable without manufacturing a stopped timestamp", async () => {
    const h = harness();
    h.options.readAttachment.mockRejectedValue(new Error("Disconnected"));
    await expect(h.port.capture(ref)).resolves.toEqual({ status: "unavailable", reason: "Disconnected" });
  });

  it.each([
    { ...target, driver: ProviderDriverKind.make("claude") },
    { ...target, evidenceRevision: 8 },
    { ...target, binding: { ...target.binding, providerThreadId: ProviderThreadId.make("provider-thread:replacement") } },
    { ...target, binding: { ...target.binding, providerSessionId: ProviderSessionId.make("session:replacement") } },
    { ...target, binding: { ...target.binding, instanceId: ProviderInstanceId.make("replacement") } },
    { ...target, binding: { ...target.binding, runtimeGeneration: "generation:replacement" } },
    { ...target, binding: { ...target.binding, nativeThreadId: "native:replacement" } },
  ] satisfies ReadonlyArray<OrchestrationV2CurrentThreadRuntimeTarget>)("does not capture or retarget a saved operation to a changed full tuple: %j", async (replacement) => {
    const h = harness(saved({ target: replacement }));
    expect((await h.port.capture(ref)).status).toBe("unavailable");
    expect((await h.port.request(ref, target)).status).toBe("unknown");
    expect(h.pointer()?.target).toEqual(replacement);
    expect(h.options.reserve).not.toHaveBeenCalled();
    expect(h.options.stop).not.toHaveBeenCalled();
    expect(h.options.observe).not.toHaveBeenCalled();
  });

  it.each([
    saved({ environmentId: EnvironmentId.make("environment:other") }),
    saved({ threadId: ThreadId.make("thread:other") }),
    saved({ target: { ...target, binding: { ...target.binding, threadId: ThreadId.make("thread:other") } } }),
  ])("blocks pointer scope mismatch without treating it as an absent operation: %j", async (pointer) => {
    const h = harness(pointer);
    expect((await h.port.capture(ref)).status).toBe("unavailable");
    expect((await h.port.request(ref, target)).status).toBe("unknown");
    expect((await h.port.observe(ref))?.status).toBe("unknown");
    expect(h.options.readAttachment).not.toHaveBeenCalled();
    expect(h.options.stop).not.toHaveBeenCalled();
    expect(h.options.observe).not.toHaveBeenCalled();
  });

  it("holds capture when its saved command changes during the attachment read", async () => {
    const h = harness(saved());
    h.options.readAttachment.mockImplementation(async () => {
      h.replace(saved({ commandId: CommandId.make("command:new-stop") }));
      return attached();
    });
    expect((await h.port.capture(ref)).status).toBe("unavailable");
    expect(h.options.reserve).not.toHaveBeenCalled();
  });

  it("reserves before STOP and keeps accepted fence installation pending", async () => {
    const h = harness();
    const outcome = await h.port.request(ref, target);
    expect(outcome).toMatchObject({
      status: "pending", commandAccepted: true, queueFenceInstalled: true,
      commandId: h.pointer()?.commandId, target,
    });
    expect(Object.isFrozen(outcome.target)).toBe(true);
    expect(Object.isFrozen(outcome.target?.binding)).toBe(true);
    expect(h.order.indexOf("reserve")).toBeLessThan(h.order.indexOf("stop"));
    expect(h.pointer()?.target).toEqual(target);
    expect(h.options.clear).not.toHaveBeenCalled();
  });

  it("reconciles the original command after a lost response and remount without reading or redispatching an attachment", async () => {
    const h = harness();
    h.options.stop.mockRejectedValueOnce(new Error("Response lost"));
    const lost = await h.port.request(ref, target);
    expect(lost.status).toBe("unknown");
    const original = h.pointer();
    expect(original).not.toBeNull();
    expect(lost).toMatchObject({ commandId: original!.commandId, target: original!.target });
    h.options.readAttachment.mockRejectedValue(new Error("Current attachment unavailable"));
    h.options.observe.mockImplementationOnce(async () => receipt(original!, { runtimeStop: { status: "stopped" } }));
    const remounted = createCurrentRuntimeStopPort(h.options);
    await expect(remounted.observe(ref)).resolves.toMatchObject({
      status: "stopped", commandId: original!.commandId, target: original!.target,
    });
    expect(h.options.observe).toHaveBeenCalledWith(ref, { threadId: ref.threadId, commandId: original!.commandId });
    expect(h.options.stop).toHaveBeenCalledOnce();
    expect(h.options.reserve).toHaveBeenCalledOnce();
    expect(h.options.readAttachment).not.toHaveBeenCalled();
    expect(h.pointer()).toBeNull();
  });

  it("preserves the original pointer on absent receipt or wrong-target observations", async () => {
    const original = saved();
    const h = harness(original);
    h.options.observe.mockResolvedValueOnce(receipt(original, {
      commandStatus: "not_found", target: null, receipt: null,
      queueFence: { status: "unknown", affectedRunIds: [] }, runtimeStop: { status: "unknown" },
    }));
    expect((await h.port.observe(ref))?.status).toBe("unknown");
    h.options.observe.mockResolvedValueOnce(receipt(original, {
      target: { ...target, evidenceRevision: 8 }, runtimeStop: { status: "stopped" },
    }));
    expect((await h.port.observe(ref))?.status).toBe("unknown");
    expect(h.pointer()).toEqual(original);
    expect(h.options.clear).not.toHaveBeenCalled();
    expect(h.options.stop).not.toHaveBeenCalled();
  });

  it("does not clear a replacement pointer while completing an original observation", async () => {
    const original = saved();
    const replacement = saved({ commandId: CommandId.make("command:replacement-stop") });
    const h = harness(original);
    h.options.observe.mockImplementationOnce(async () => {
      h.replace(replacement);
      return receipt(original, { runtimeStop: { status: "stopped" } });
    });
    await expect(h.port.observe(ref)).resolves.toMatchObject({
      status: "stopped", commandId: original.commandId, target: original.target,
    });
    expect(h.pointer()).toEqual(replacement);
    expect(h.options.clear).toHaveBeenCalledWith(ref, {
      threadId: original.threadId, commandId: original.commandId, target: original.target,
    });
    expect(h.options.stop).not.toHaveBeenCalled();
  });

  it("returns null only with no saved command and never admits one through observe", async () => {
    const h = harness();
    await expect(h.port.observe(ref)).resolves.toBeNull();
    expect(h.options.readAttachment).not.toHaveBeenCalled();
    expect(h.options.reserve).not.toHaveBeenCalled();
    expect(h.options.stop).not.toHaveBeenCalled();
    expect(h.options.observe).not.toHaveBeenCalled();
  });

  it("coalesces request and status check around the same in-flight STOP", async () => {
    const h = harness();
    let reserved!: () => void;
    const reservation = new Promise<void>((resolve) => { reserved = resolve; });
    h.options.reserve.mockImplementationOnce((threadRef, input) => {
      h.replace({ environmentId: threadRef.environmentId, ...input });
      reserved();
    });
    let complete!: (result: OrchestrationV2StopCurrentThreadRuntimeResult) => void;
    const response = new Promise<OrchestrationV2StopCurrentThreadRuntimeResult>((resolve) => { complete = resolve; });
    h.options.stop.mockImplementationOnce(async () => response);
    const request = h.port.request(ref, target);
    await reservation;
    const observation = h.port.observe(ref);
    complete(receipt(h.pointer()!));
    expect(await observation).toEqual(await request);
    expect(h.options.stop).toHaveBeenCalledOnce();
    expect(h.options.reserve).toHaveBeenCalledOnce();
    expect(h.options.observe).not.toHaveBeenCalled();
  });

  it("returns a typed save failure and never retries the unsent operation through observe", async () => {
    const h = harness();
    h.options.reserve.mockImplementationOnce(() => { throw new Error("Durable pointer unavailable; request was not sent"); });
    await expect(h.port.request(ref, target)).resolves.toMatchObject({
      status: "unknown", commandAccepted: false, queueFenceInstalled: false,
      reason: "Durable pointer unavailable; request was not sent",
      commandId: null, target: null,
    });
    await expect(h.port.observe(ref)).resolves.toBeNull();
    expect(h.options.reserve).toHaveBeenCalledOnce();
    expect(h.options.stop).not.toHaveBeenCalled();
    expect(h.options.observe).not.toHaveBeenCalled();
  });

  it("does not let an old status callback observe a newer saved operation", async () => {
    const original = saved();
    const replacement = saved({ commandId: CommandId.make("command:newer-stop") });
    const h = harness(replacement);
    await expect(h.port.observe(ref, original)).resolves.toMatchObject({
      status: "unknown", commandId: null, target: null,
    });
    expect(h.pointer()).toEqual(replacement);
    expect(h.options.observe).not.toHaveBeenCalled();
    expect(h.options.stop).not.toHaveBeenCalled();
    expect(h.options.reserve).not.toHaveBeenCalled();
  });

  it("keeps an expected missing operation unknown instead of admitting or refreshing a replacement", async () => {
    const h = harness();
    await expect(h.port.observe(ref, saved())).resolves.toMatchObject({
      status: "unknown", commandId: null, target: null,
    });
    expect(h.options.observe).not.toHaveBeenCalled();
    expect(h.options.stop).not.toHaveBeenCalled();
  });

  it("does not follow a pointer replacement between the hook's pin and controller observation", async () => {
    const original = saved();
    const replacement = saved({ commandId: CommandId.make("command:changed-before-observe") });
    const h = harness(replacement);
    h.options.readPointer.mockReturnValueOnce(original);
    await expect(h.port.observe(ref, original)).resolves.toMatchObject({
      status: "unknown", commandId: null, target: null,
    });
    expect(h.pointer()).toEqual(replacement);
    expect(h.options.observe).not.toHaveBeenCalled();
    expect(h.options.stop).not.toHaveBeenCalled();
  });

  it("does not complete a changed saved operation during the request's scheduled read", async () => {
    const original = saved();
    const replacement = saved({ commandId: CommandId.make("command:changed-before-request") });
    const h = harness(replacement);
    h.options.readPointer.mockReturnValueOnce(original);
    await expect(h.port.request(ref, target)).resolves.toMatchObject({
      status: "unknown", commandId: null, target: null,
    });
    expect(h.pointer()).toEqual(replacement);
    expect(h.options.observe).not.toHaveBeenCalled();
    expect(h.options.stop).not.toHaveBeenCalled();
    expect(h.options.reserve).not.toHaveBeenCalled();
  });

  it("does not claim correlation when draft storage cannot be read", async () => {
    const h = harness(saved());
    h.options.readPointer.mockImplementation(() => { throw new Error("Draft storage unavailable"); });
    await expect(h.port.observe(ref, saved())).resolves.toMatchObject({
      status: "unknown", commandId: null, target: null,
    });
    await expect(h.port.request(ref, target)).resolves.toMatchObject({
      status: "unknown", commandId: null, target: null,
    });
    expect(h.options.observe).not.toHaveBeenCalled();
    expect(h.options.stop).not.toHaveBeenCalled();
  });

  it("does not coalesce a replacement target with an in-flight original stop", async () => {
    const h = harness();
    let reserved!: () => void;
    const reservation = new Promise<void>((resolve) => { reserved = resolve; });
    h.options.reserve.mockImplementationOnce((threadRef, input) => {
      h.replace({ environmentId: threadRef.environmentId, ...input });
      reserved();
    });
    let complete!: (result: OrchestrationV2StopCurrentThreadRuntimeResult) => void;
    const response = new Promise<OrchestrationV2StopCurrentThreadRuntimeResult>((resolve) => { complete = resolve; });
    h.options.stop.mockImplementationOnce(async () => response);
    const request = h.port.request(ref, target);
    await reservation;
    const original = h.pointer()!;
    const replacement = saved({
      commandId: CommandId.make("command:replacement-inflight"),
      target: { ...target, evidenceRevision: 8 },
    });
    h.replace(replacement);
    await expect(h.port.request(ref, replacement.target)).resolves.toMatchObject({
      status: "unknown", commandId: null, target: null,
    });
    await expect(h.port.observe(ref, replacement)).resolves.toMatchObject({
      status: "unknown", commandId: null, target: null,
    });
    complete(receipt(original));
    await expect(request).resolves.toMatchObject({
      status: "pending", commandId: original.commandId, target: original.target,
    });
    expect(h.options.stop).toHaveBeenCalledOnce();
    expect(h.options.observe).not.toHaveBeenCalled();
    expect(h.pointer()).toEqual(replacement);
  });
});

describe("current runtime stop hook adapters", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    hook.pointer = null;
    hook.readAttachment.mockImplementation(async () => ({ _tag: "Success", value: attached() }));
    hook.reserve.mockImplementation((pointer: CurrentRuntimeStopPointer) => { hook.pointer = pointer; });
    hook.clear.mockImplementation((pointer: CurrentRuntimeStopPointer) => {
      if (hook.pointer?.commandId === pointer.commandId) hook.pointer = null;
    });
    hook.stop.mockImplementation(async ({ input }: { input: OrchestrationV2StopCurrentThreadRuntimeInput }) => ({ _tag: "Success", value: receipt(input) }));
    hook.observe.mockImplementation(async () => ({ _tag: "Success", value: receipt(hook.pointer!) }));
  });

  it("carries the environment and original full tuple through the actual dedicated adapters", async () => {
    const port = useCurrentRuntimeStop();
    const capture = await port.capture(ref);
    expect(capture).toEqual({ status: "current", target });
    if (capture.status !== "current") throw new Error("Expected a qualified synthetic attachment");
    const outcome = await port.request(ref, capture.target);
    expect(outcome.status).toBe("pending");
    const original = hook.pointer!;
    expect(hook.readAttachment).toHaveBeenCalledWith({ environmentId: ref.environmentId, input: { threadId: ref.threadId } });
    expect(hook.reserve).toHaveBeenCalledWith(original);
    expect(hook.stop).toHaveBeenCalledWith({
      environmentId: ref.environmentId,
      input: { commandId: original.commandId, threadId: ref.threadId, target },
    });
    const remounted = useCurrentRuntimeStop();
    expect((await remounted.observe(ref))?.status).toBe("pending");
    expect(hook.observe).toHaveBeenCalledWith({
      environmentId: ref.environmentId, input: { threadId: ref.threadId, commandId: original.commandId },
    });
    expect(hook.stop).toHaveBeenCalledOnce();
    expect(hook.reserve).toHaveBeenCalledOnce();
  });
});
