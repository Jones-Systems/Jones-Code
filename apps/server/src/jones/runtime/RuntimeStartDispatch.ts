import type { OrchestrationEffectV2 } from "../../orchestration-v2/EffectOutbox.ts";

interface StartLease {
  readonly effect_id: string;
  readonly thread_id: string;
  readonly command_id: string;
  readonly effect_type: string;
  readonly attempt_count: number;
  readonly lease_owner: string | null;
  readonly updated_at: string;
  readonly payload_json: string;
}

/**
 * The claiming outbox records this witness under its writer lock, before returning the lease.
 * Its worker consumes it under the same writer lock before entering the executor. A Stop that
 * holds the lock can prove this exact first claim has not crossed that boundary. After process
 * loss a claimed row alone provides no such proof.
 */
export function makeRuntimeStartDispatch() {
  const pending = new Map<string, StartLease>();
  const hasWitness = (claim: OrchestrationEffectV2) => {
    const row = pending.get(claim.id);
    return (
      row !== undefined &&
      row.thread_id === claim.threadId &&
      row.command_id === claim.commandId &&
      row.effect_type === claim.request.type &&
      row.attempt_count === claim.attemptCount &&
      row.lease_owner === claim.leaseOwner &&
      row.updated_at === claim.updatedAt &&
      row.payload_json === claim.payloadIdentity
    );
  };
  return {
    claim(row: StartLease, tracked: boolean) {
      if (tracked && row.effect_type === "provider-turn.start" && row.attempt_count === 1) {
        pending.set(row.effect_id, row);
      } else {
        pending.delete(row.effect_id);
      }
    },
    hasWitness,
    enter(claim: OrchestrationEffectV2) {
      if (hasWitness(claim)) pending.delete(claim.id);
    },
    forget(effectId: string) {
      pending.delete(effectId);
    },
  };
}
