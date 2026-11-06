export const SETTLE_SCRIPT_TIMEOUT_MS = 10 * 60 * 1000;
export type SettlementScriptState = "pending" | "running" | "completed" | "failed" | "cancelled";
export function settlementAdmission(input: {
  script: SettlementScriptState; startedAt: number; now: number;
  checkpoint: "ready" | "pending" | "unavailable"; enabled: boolean;
}) {
  if (input.script !== "completed") return { decision: "hold", reason: input.now - input.startedAt >= SETTLE_SCRIPT_TIMEOUT_MS ? "settle_script_unfinished" : "settle_script_pending" } as const;
  if (input.checkpoint !== "ready") return { decision: "hold", reason: "final_checkpoint_unfinished" } as const;
  return input.enabled ? { decision: "evaluate", reason: null } as const : { decision: "propose", reason: "settlement_retirement_disabled" } as const;
}
