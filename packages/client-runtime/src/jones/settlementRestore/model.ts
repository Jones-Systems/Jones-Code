import {
  SETTLEMENT_RESTORE_CAPABILITY,
  type CommandId,
  type ExecutionEnvironmentCapabilities,
  type SettlementRestoreInspection,
  type SettlementRestoreRejectionCode,
} from "@t3tools/contracts";
import * as Data from "effect/Data";

export function supportsSettlementRestore(
  capabilities: Pick<ExecutionEnvironmentCapabilities, "settlementRestore"> | undefined,
): boolean {
  return (
    capabilities?.settlementRestore?.schemaVersion === SETTLEMENT_RESTORE_CAPABILITY.schemaVersion
  );
}

/** How one undo attempt ended, as far as this client can prove. */
export type SettlementRestoreOutcome =
  | { readonly type: "restored"; readonly restoreCommandId: CommandId }
  | { readonly type: "rejected"; readonly code: SettlementRestoreRejectionCode }
  /** The server may or may not have applied it; a retry reuses the same command ID. */
  | { readonly type: "unknown"; readonly restoreCommandId: CommandId }
  | { readonly type: "unsupported" }
  | { readonly type: "unavailable" };

// The server words a domain rejection as `Settlement restore rejected: <code>: ...`
// (apps/server/src/jones/settlementRestore/plan.ts). It can be nested in a
// previously-rejected receipt message, so match anywhere in the text.
const REJECTION = /Settlement restore rejected: ([a-z_]+):/;
const REJECTION_CODES = new Set<string>([
  "none",
  "superseded",
  "already_restored",
  "thread_missing",
  "stale_settle_command",
  "invalid_projected_state",
  "command_id_reused",
] satisfies SettlementRestoreRejectionCode[]);

function failureTexts(failure: unknown): string[] {
  if (typeof failure !== "object" || failure === null) return [];
  const texts: string[] = [];
  for (const field of ["detail", "message"] as const) {
    const value = (failure as Record<string, unknown>)[field];
    if (typeof value === "string") texts.push(value);
  }
  return texts;
}

/** A rejection the server reported for this restore. Any other failure leaves the effect unknown. */
export function readSettlementRestoreRejection(
  failure: unknown,
): SettlementRestoreRejectionCode | null {
  for (const text of failureTexts(failure)) {
    const code = REJECTION.exec(text)?.[1];
    if (code !== undefined && REJECTION_CODES.has(code)) {
      return code as SettlementRestoreRejectionCode;
    }
  }
  return null;
}

/** The server keeps a rejected command's receipt, so that command ID can never succeed. */
export function isPreviouslyRejectedCommand(failure: unknown): boolean {
  return failureTexts(failure).some((text) => / was previously rejected/.test(text));
}

/** The rejection an inspection implies for a restore that did not apply. */
export function inspectionRejection(
  inspection: SettlementRestoreInspection,
): SettlementRestoreRejectionCode | null {
  switch (inspection.status) {
    case "restorable":
      return null;
    case "restored":
      return "already_restored";
    case "none":
      return "none";
    case "superseded":
      switch (inspection.blocker) {
        case "stale_settle_command":
        case "thread_missing":
        case "invalid_projected_state":
          return inspection.blocker;
        default:
          return "superseded";
      }
  }
}

const rejectionMessages: Record<SettlementRestoreRejectionCode, string> = {
  none: "This thread has no settle to undo. Use Un-settle instead.",
  superseded: "The thread changed after it was settled, so Undo would overwrite newer changes.",
  already_restored: "This settle was already undone.",
  thread_missing: "The thread no longer exists.",
  stale_settle_command: "The thread was settled again since, so this Undo no longer applies.",
  invalid_projected_state:
    "The thread's earlier position is no longer valid. Use Un-settle instead.",
  command_id_reused: "The undo request conflicted with another request. Try again.",
};

export function settlementRestoreMessage(
  outcome: Exclude<SettlementRestoreOutcome, { readonly type: "restored" }>,
): string {
  switch (outcome.type) {
    case "rejected":
      return rejectionMessages[outcome.code];
    case "unknown":
      return "Couldn't confirm the undo. Choose Undo settle from the thread's menu to check and retry.";
    case "unsupported":
      return "This environment's server cannot undo settles. Update the server to use it.";
    case "unavailable":
      return "Couldn't check whether this thread's settle can be undone. Try again.";
  }
}

export class SettlementRestoreNotAppliedError extends Data.TaggedError(
  "SettlementRestoreNotAppliedError",
)<{
  readonly outcome: Exclude<SettlementRestoreOutcome, { readonly type: "restored" }>;
  readonly message: string;
}> {}

export function settlementRestoreNotApplied(
  outcome: Exclude<SettlementRestoreOutcome, { readonly type: "restored" }>,
) {
  return new SettlementRestoreNotAppliedError({
    outcome,
    message: settlementRestoreMessage(outcome),
  });
}
