import { describe, expect, it } from "vite-plus/test";
import { ProviderInstanceId } from "@t3tools/contracts";
import { makeThreadFixture } from "../../test-fixtures";
import { resolveNavigationAttentionPill } from "./attention";

const runtime = {
  status: "running" as const,
  activeRunId: null,
  providerInstanceId: ProviderInstanceId.make("codex"),
  providerName: "Codex",
  lastError: null,
  updatedAt: "2026-11-16T12:00:00.000Z",
};

describe("composed navigation attention", () => {
  it("keeps a startup distinct from provider work and a blocked run distinct from both", () => {
    const thread = makeThreadFixture({ runtime });
    expect(resolveNavigationAttentionPill({ thread })?.label).toBe("Working");
    expect(resolveNavigationAttentionPill({
      thread: { ...thread, runtime: { ...runtime, status: "starting" } },
    })?.label).toBe("Connecting");
    expect(resolveNavigationAttentionPill({
      thread: { ...thread, blockedOnThreadWait: true },
    })).toMatchObject({ label: "Waiting", pulse: false });
  });

  it("retains upstream guest attention below Jones child input and approval", () => {
    const thread = makeThreadFixture({ runtime, pendingGuestMessageCount: 1 });
    expect(resolveNavigationAttentionPill({ thread })?.label).toBe("Guest Request");
    expect(resolveNavigationAttentionPill({
      thread: { ...thread, pendingSubagentRequest: "user_input" },
    })?.label).toBe("Awaiting Input");
    expect(resolveNavigationAttentionPill({
      thread: { ...thread, pendingSubagentRequest: "approval" },
    })?.label).toBe("Pending Approval");
  });
});
