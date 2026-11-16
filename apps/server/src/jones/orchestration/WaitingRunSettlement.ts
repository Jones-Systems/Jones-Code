import type {
  OrchestrationV2ProviderTurn,
  OrchestrationV2Run,
  OrchestrationV2RunAttempt,
} from "@t3tools/contracts";
import type { OrchestrationEffectV2 } from "../../orchestration-v2/EffectOutbox.ts";

export type WaitingRunStopEvidence = "persisted_terminal" | "absent_session" | "adapter_confirmed";

// Waiting is checkpoint finalization, so Stop needs both an ended attempt and no live capture.
export const canSettleWaitingRun = (input: {
  readonly run: Pick<OrchestrationV2Run, "id" | "status" | "activeAttemptId">;
  readonly attempt: Pick<OrchestrationV2RunAttempt, "id" | "runId" | "status">;
  readonly providerTurn: Pick<OrchestrationV2ProviderTurn, "runAttemptId" | "status">;
  readonly checkpointEffects: ReadonlyArray<Pick<OrchestrationEffectV2, "request" | "status">>;
  readonly evidence: WaitingRunStopEvidence | undefined;
}) =>
  input.run.status === "waiting" &&
  input.attempt.runId === input.run.id &&
  input.run.activeAttemptId === input.attempt.id &&
  input.providerTurn.runAttemptId === input.attempt.id &&
  ["completed", "interrupted", "failed", "cancelled", "superseded"].includes(
    input.attempt.status,
  ) &&
  (input.evidence === "absent_session" ||
    input.evidence === "adapter_confirmed" ||
    (input.evidence === "persisted_terminal" &&
      ["completed", "interrupted", "failed", "cancelled"].includes(input.providerTurn.status))) &&
  !input.checkpointEffects.some(
    (effect) =>
      effect.request.type === "checkpoint.capture" &&
      effect.request.runId === input.run.id &&
      (effect.status === "pending" || effect.status === "running"),
  );
