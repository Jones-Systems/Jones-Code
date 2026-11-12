import { describe, expect, it, vi } from "@effect/vitest";
import {
  CommandId,
  ThreadId,
  ProviderThreadId,
  ProviderSessionId,
  ProviderInstanceId,
  ProviderDriverKind,
  type StopCurrentThreadRuntimeInput,
  type ExecutionEnvironmentCapabilities,
  type StopCurrentThreadRuntimeResult,
} from "@t3tools/contracts";
import {
  restartCapturedCurrentRuntime,
  type CurrentRuntimeRestartPort,
} from "./currentRuntimeStop.ts";
const threadId = ThreadId.make("thread:stop");
const target = {
  binding: {
    threadId,
    providerThreadId: ProviderThreadId.make("provider-thread:stop"),
    providerSessionId: ProviderSessionId.make("session:stop"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    driver: ProviderDriverKind.make("codex"),
    nativeThreadId: "native:stop",
    runtimeGeneration: "physical:stop",
  },
  evidenceRevision: 1,
};
const capability: NonNullable<ExecutionEnvironmentCapabilities["currentRuntimeStop"]> = {
  targetRequired: true,
  supportedDrivers: ["codex", "claudeAgent"],
  backgroundCoverage: "partial",
};
const fixture = () => {
  let saved: StopCurrentThreadRuntimeInput | null = null;
  const result = (
    input: StopCurrentThreadRuntimeInput,
    status: StopCurrentThreadRuntimeResult["status"],
  ): StopCurrentThreadRuntimeResult => ({
    commandId: input.commandId,
    threadId: input.threadId,
    status,
    affectedRunIds: [],
    backgroundCoverage: "partial",
  });
  const reserve = vi.fn((input: StopCurrentThreadRuntimeInput) => {
    saved = input;
  });
  const clear = vi.fn(() => {
    saved = null;
  });
  const capture = vi.fn(async () => ({
    status: "available" as const,
    target,
    backgroundCoverage: "partial" as const,
  }));
  const submit = vi.fn(async (input: StopCurrentThreadRuntimeInput) => result(input, "accepted"));
  const observe = vi.fn(
    async (input: StopCurrentThreadRuntimeInput): Promise<StopCurrentThreadRuntimeResult | null> =>
      result(input, "stopped"),
  );
  const refresh = vi.fn(async () => {});
  const port: CurrentRuntimeRestartPort = {
    readPointer: () => saved,
    reserve,
    clear,
    capture,
    submit,
    observe,
    refresh,
    commandId: () => CommandId.make("command:stop"),
  };
  return { port, capture, submit, observe, refresh, reserve, clear, saved: () => saved, result };
};
describe("captured runtime restart consumer", () => {
  it("old server capability absence sends no RPC and never falls back to interrupt", async () => {
    const f = fixture();
    expect((await restartCapturedCurrentRuntime(threadId, undefined, f.port)).status).toBe(
      "unavailable",
    );
    expect(f.capture).not.toHaveBeenCalled();
    expect(f.submit).not.toHaveBeenCalled();
    expect(f.observe).not.toHaveBeenCalled();
    expect(f.refresh).not.toHaveBeenCalled();
  });
  it("persists identity before submission and refreshes only after read-only receipt observes stopped", async () => {
    const f = fixture();
    f.submit.mockImplementation(async (input) => {
      expect(f.saved()).toEqual(input);
      return f.result(input, "accepted");
    });
    expect((await restartCapturedCurrentRuntime(threadId, capability, f.port)).status).toBe(
      "accepted",
    );
    expect(f.refresh).not.toHaveBeenCalled();
    expect(f.saved()).not.toBeNull();
    expect((await restartCapturedCurrentRuntime(threadId, capability, f.port)).status).toBe(
      "stopped",
    );
    expect(f.capture).toHaveBeenCalledTimes(1);
    expect(f.submit).toHaveBeenCalledTimes(1);
    expect(f.observe).toHaveBeenCalledWith({ commandId: "command:stop", threadId, target });
    expect(f.refresh).toHaveBeenCalledWith(target);
    expect(f.saved()).toBeNull();
  });
  it("lost acceptance response survives remount and observes original identity without replay", async () => {
    const f = fixture();
    f.submit.mockRejectedValue(new Error("response lost"));
    expect((await restartCapturedCurrentRuntime(threadId, capability, f.port)).status).toBe(
      "unknown",
    );
    const remounted = { ...f.port };
    expect((await restartCapturedCurrentRuntime(threadId, capability, remounted)).status).toBe(
      "stopped",
    );
    expect(f.submit).toHaveBeenCalledTimes(1);
    expect(f.capture).toHaveBeenCalledTimes(1);
  });
  it("missing or unknown receipt holds original pointer and does not stop the replacement", async () => {
    const f = fixture();
    await restartCapturedCurrentRuntime(threadId, capability, f.port);
    f.observe.mockResolvedValueOnce(null);
    expect((await restartCapturedCurrentRuntime(threadId, capability, f.port)).status).toBe(
      "unknown",
    );
    f.observe.mockImplementation(async (input) => f.result(input, "unknown"));
    expect((await restartCapturedCurrentRuntime(threadId, capability, f.port)).status).toBe(
      "unknown",
    );
    expect(f.saved()?.target).toEqual(target);
    expect(f.submit).toHaveBeenCalledTimes(1);
    expect(f.capture).toHaveBeenCalledTimes(1);
    expect(f.refresh).not.toHaveBeenCalled();
  });
  it("failed durable reservation has zero physical-stop submissions", async () => {
    const f = fixture();
    f.reserve.mockImplementation(() => {
      throw new Error("quota");
    });
    expect((await restartCapturedCurrentRuntime(threadId, capability, f.port)).status).toBe(
      "unknown",
    );
    expect(f.submit).not.toHaveBeenCalled();
    expect(f.refresh).not.toHaveBeenCalled();
  });
  it("correlation mismatch and changed capture never refresh or clear another stop", async () => {
    const f = fixture();
    await restartCapturedCurrentRuntime(threadId, capability, f.port);
    f.observe.mockImplementation(async (input) => ({
      ...f.result(input, "stopped"),
      commandId: CommandId.make("command:other"),
    }));
    expect((await restartCapturedCurrentRuntime(threadId, capability, f.port)).status).toBe(
      "unknown",
    );
    expect(f.refresh).not.toHaveBeenCalled();
    expect(f.clear).not.toHaveBeenCalled();
    const g = fixture();
    g.capture.mockResolvedValue({
      status: "available",
      target: {
        ...target,
        binding: { ...target.binding, threadId: ThreadId.make("thread:replacement") },
      },
      backgroundCoverage: "partial",
    });
    expect((await restartCapturedCurrentRuntime(threadId, capability, g.port)).status).toBe(
      "unavailable",
    );
    expect(g.submit).not.toHaveBeenCalled();
  });
  it("failed refresh retains stopped correlation for observation instead of stop replay", async () => {
    const f = fixture();
    f.submit.mockImplementation(async (input) => f.result(input, "stopped"));
    f.refresh.mockRejectedValueOnce(new Error("refresh lost"));
    expect((await restartCapturedCurrentRuntime(threadId, capability, f.port)).status).toBe(
      "unknown",
    );
    expect(f.saved()).not.toBeNull();
    expect((await restartCapturedCurrentRuntime(threadId, capability, f.port)).status).toBe(
      "stopped",
    );
    expect(f.submit).toHaveBeenCalledTimes(1);
    expect(f.observe).toHaveBeenCalledTimes(1);
  });
});
