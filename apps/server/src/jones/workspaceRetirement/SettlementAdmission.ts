const SETTLE_SCRIPT_TIMEOUT_MS = 10 * 60 * 1000;
// Checkpoint statuses are ready | missing | error | stale: none means "in progress". Capture is only
// attempted for runs that finish as completed, interrupted or cancelled (CheckpointCaptureService), so a
// failed or rolled_back run never gains a ready checkpoint and stays held until that evidence exists.
export function finalCheckpointSettled(
  last: { readonly id: string; readonly status: string } | undefined,
  checkpoints: readonly { readonly runId: string | null; readonly status: string }[],
): boolean {
  return (
    last === undefined ||
    (["completed", "cancelled", "interrupted"].includes(last.status) &&
      checkpoints.some((c) => c.runId === last.id && c.status === "ready"))
  );
}
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
