import type * as SqlClient from "effect/sql/SqlClient";

// Predicate for an outbox UPDATE: a native effect completes only for the
// worker, lease and attempt its confirmation names, with no unknown-effect
// hold. It reads Jones tables, so only native completion may use it.
export const confirmedNativeCompletion = (
  sql: SqlClient.SqlClient,
  input: { readonly workerId: string; readonly now: string },
) => sql`EXISTS (
  SELECT 1 FROM jones_native_creation_execution_confirmations confirmation
  JOIN jones_native_creation_execution_starts start ON start.effect_id = confirmation.effect_id
  WHERE confirmation.effect_id = orchestration_v2_effect_outbox.effect_id
    AND orchestration_v2_effect_outbox.lease_expires_at > ${input.now}
    AND confirmation.worker_id = ${input.workerId}
    AND confirmation.expected_attempt = orchestration_v2_effect_outbox.attempt_count
    AND start.lease_expires_at = orchestration_v2_effect_outbox.lease_expires_at
    AND NOT EXISTS (
      SELECT 1 FROM jones_native_creation_execution_holds hold
      WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id
    )
)`;
