import type { ContinuityOwner, ContinuityRecoveryCapability } from "@t3tools/contracts";
import type { ContinuityRequirement } from "@t3tools/provider-core/server/jones/GoalContinuityContext";

/** Delivery history is checked even when the current record was retired. */
export function continuityAdmissionRefusal(
  requirement: ContinuityRequirement,
  owner: ContinuityOwner | null,
  transfersConversation = false,
): string | undefined {
  if (requirement.outcome === "hold") return requirement.reason;
  if (requirement.outcome === "absent") return undefined;
  if (transfersConversation || owner === null) return "binding_changed";
  const expected = requirement.owner;
  return expected.environmentId === owner.environmentId &&
    expected.threadId === owner.threadId &&
    expected.providerInstanceId === owner.providerInstanceId &&
    expected.providerThreadId === owner.providerThreadId &&
    expected.driver === owner.driver &&
    expected.nativeThreadId === owner.nativeThreadId
    ? undefined
    : "binding_changed";
}

export function continuityRecoveryCapability(
  driver: string,
  strongIdentity: boolean,
): ContinuityRecoveryCapability {
  return {
    strongIdentity,
    turnStart: driver === "codex" && strongIdentity ? "fresh_context" : "unsupported",
    compaction: driver === "codex" && strongIdentity ? "fresh_context_injection" : "unsupported",
    strictPreResumptionBarrier: "unsupported",
  };
}
