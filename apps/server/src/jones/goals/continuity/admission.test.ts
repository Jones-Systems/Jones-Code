import { expect, it } from "vite-plus/test";
import type { ContinuityOwner } from "@t3tools/contracts";
import type { ContinuityRequirement } from "@t3tools/provider-core/server/jones/GoalContinuityContext";
import { continuityAdmissionRefusal, continuityRecoveryCapability } from "./admission.ts";

const owner = {
  environmentId: "synthetic-environment", threadId: "synthetic-thread",
  providerInstanceId: "codex", providerThreadId: "synthetic-provider-thread",
  driver: "codex", nativeThreadId: "synthetic-native-thread",
} as ContinuityOwner;
const required: ContinuityRequirement = {
  outcome: "required", owner,
  target: { goalId: "goal", revision: "1", generation: "1", snapshotDigest: "a".repeat(64) },
  gate: { outcome: "clear", fenceRevision: "0" },
};

it("preserves exact conversation identity across every binding dimension", () => {
  expect(continuityAdmissionRefusal(required, owner)).toBeUndefined();
  for (const field of Object.keys(owner) as (keyof ContinuityOwner)[]) {
    expect(continuityAdmissionRefusal(required, { ...owner, [field]: "changed" } as ContinuityOwner))
      .toBe("binding_changed");
  }
  expect(continuityAdmissionRefusal(required, null)).toBe("binding_changed");
  expect(continuityAdmissionRefusal(required, owner, true)).toBe("binding_changed");
});

it("retains an unknown delivery fence after the current record is gone", () => {
  expect(continuityAdmissionRefusal({
    outcome: "hold", reason: "delivery_unknown", current: null,
    gate: { outcome: "hold", reason: "delivery_unknown", fenceRevision: "3", deliveryIds: ["delivery"] },
  }, null)).toBe("delivery_unknown");
  expect(continuityAdmissionRefusal({
    outcome: "hold", reason: "unavailable", current: null, gate: null,
  }, owner)).toBe("unavailable");
  expect(continuityAdmissionRefusal({
    outcome: "absent", gate: { outcome: "clear", fenceRevision: "0" },
  }, null)).toBeUndefined();
});

it("advertises only supported recovery and never a strict execution barrier", () => {
  expect(continuityRecoveryCapability("codex", true).compaction).toBe("fresh_context_injection");
  for (const driver of ["claudeAgent", "cursor", "grok", "opencode", "antigravity", "muse", "pi"]) {
    expect(continuityRecoveryCapability(driver, true).compaction).toBe("unsupported");
  }
  expect(continuityRecoveryCapability("codex", false).turnStart).toBe("unsupported");
  expect(continuityRecoveryCapability("codex", true).strictPreResumptionBarrier).toBe("unsupported");
});
