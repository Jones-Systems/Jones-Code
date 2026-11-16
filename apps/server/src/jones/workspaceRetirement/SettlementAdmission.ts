const SETTLE_SCRIPT_TIMEOUT_MS = 10 * 60 * 1000;
export type SettlementScriptState = "pending" | "running" | "completed" | "failed" | "cancelled";
export function settlementAdmission(input: {
  script: SettlementScriptState;
  startedAt: number;
  now: number;
  checkpoint: "ready" | "pending" | "unavailable";
  enabled: boolean;
}) {
  if (input.script !== "completed")
    return {
      decision: "hold",
      reason:
        input.now - input.startedAt >= SETTLE_SCRIPT_TIMEOUT_MS
          ? "settle_script_unfinished"
          : "settle_script_pending",
    } as const;
  if (input.checkpoint !== "ready")
    return { decision: "hold", reason: "final_checkpoint_unfinished" } as const;
  return input.enabled
    ? ({ decision: "evaluate", reason: null } as const)
    : ({ decision: "propose", reason: "settlement_retirement_disabled" } as const);
}

// Missing historical metadata cannot issue a script or grant automatic removal.
export function legacySettlementAdmission(input: {
  settledAt: number;
  now: number;
  execute: boolean;
  ownerConfirmed: boolean;
}): string | null {
  if (!Number.isFinite(input.settledAt) || input.now - input.settledAt < SETTLE_SCRIPT_TIMEOUT_MS)
    return "settlement_intent_missing";
  return input.execute && !input.ownerConfirmed ? "legacy_settlement_confirmation_required" : null;
}
