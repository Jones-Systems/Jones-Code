import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { ExecutionEnvironmentDescriptor } from "../environment.ts";
import { OrchestrationV2Command } from "../orchestrationV2.ts";
import { THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY } from "./guardedThreadSettlement.ts";

describe("optional guarded settlement wire", () => {
  it("preserves old unguarded commands and accepts the exact optional guard", () => {
    const decode = Schema.decodeUnknownSync(OrchestrationV2Command);
    const legacy = { type: "thread.settle", threadId: "t", commandId: "c" };
    expect(decode(legacy)).toEqual(legacy);
    const guard = {
      policy: "preserve_pending_work",
      expectedUpdatedAt: "2026-10-26T07:00:00.000Z",
    };
    expect(decode({ ...legacy, guard })).toEqual({ ...legacy, guard });
    for (const bad of [
      { ...guard, policy: "cancel_work" },
      { ...guard, expectedUpdatedAt: "yesterday" },
      { policy: "preserve_pending_work" },
    ]) {
      expect(() => decode({ ...legacy, guard: bad })).toThrow();
    }
  });
  it("does not require the capability from older independently upgraded servers", () => {
    const descriptor = {
      environmentId: "e",
      label: "Test",
      platform: { os: "linux", arch: "x64" },
      serverVersion: "0.1",
      capabilities: { repositoryIdentity: true },
    };
    const decode = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);
    expect(decode(descriptor).capabilities.threadMonitorGuardedSettle).toBeUndefined();
    expect(
      decode({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          threadMonitorGuardedSettle: THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY,
        },
      }).capabilities.threadMonitorGuardedSettle,
    ).toBe(THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY);
  });
});
