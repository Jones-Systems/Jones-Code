import type * as SqlClient from "effect/sql/SqlClient";

// Predicate for an outbox UPDATE: a native effect completes only for the
// worker, process claim and attempt its confirmation names, with no unknown-effect
// hold. It reads Jones tables, so only native completion may use it.
export const confirmedNativeCompletion = (
  sql: SqlClient.SqlClient,
  input: { readonly workerId: string; readonly now: string },
) => sql`EXISTS (
  SELECT 1 FROM jones_native_creation_execution_process_confirmations confirmation
  JOIN jones_native_creation_execution_process_claims start ON start.effect_id = confirmation.effect_id
  WHERE confirmation.effect_id = orchestration_v2_effect_outbox.effect_id
    AND orchestration_v2_effect_outbox.status = 'running'
    AND orchestration_v2_effect_outbox.effect_type = 'provider-turn.start'
    AND start.command_id = orchestration_v2_effect_outbox.command_id
    AND json_extract(confirmation.evidence_json, '$.effectId') = orchestration_v2_effect_outbox.effect_id
    AND json_extract(confirmation.evidence_json, '$.commandId') = orchestration_v2_effect_outbox.command_id
    AND json_extract(confirmation.evidence_json, '$.threadId') = orchestration_v2_effect_outbox.thread_id
    AND json_extract(confirmation.evidence_json, '$.runId') = json_extract(orchestration_v2_effect_outbox.payload_json, '$.request.runId')
    AND json_extract(orchestration_v2_effect_outbox.payload_json, '$.request.type') = 'provider-turn.start'
    AND json_extract(orchestration_v2_effect_outbox.payload_json, '$.nativeCreationExecutionReference.version') = json_extract(start.reference_json, '$.version')
    AND json_extract(orchestration_v2_effect_outbox.payload_json, '$.nativeCreationExecutionReference.claimId') = start.claim_id
    AND json_extract(orchestration_v2_effect_outbox.payload_json, '$.nativeCreationExecutionReference.stageCommandId') = start.command_id
    AND json_extract(orchestration_v2_effect_outbox.payload_json, '$.nativeCreationExecutionReference.effectId') = start.effect_id
    AND json_extract(orchestration_v2_effect_outbox.payload_json, '$.nativeCreationExecutionReference.stage') = json_extract(start.reference_json, '$.stage')
    AND orchestration_v2_effect_outbox.lease_owner = ${input.workerId}
    AND confirmation.worker_id = ${input.workerId}
    AND confirmation.expected_attempt = orchestration_v2_effect_outbox.attempt_count
    AND start.worker_id = ${input.workerId}
    AND start.expected_attempt = orchestration_v2_effect_outbox.attempt_count
    AND start.claimed_at = orchestration_v2_effect_outbox.updated_at
    AND NOT EXISTS (SELECT 1 FROM jones_native_creation_execution_process_holds hold WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
    AND start.command_id = orchestration_v2_effect_outbox.command_id
    AND start.claim_id = json_extract(orchestration_v2_effect_outbox.payload_json, '$.nativeCreationExecutionReference.claimId')
    AND json_extract(start.reference_json, '$.effectId') = orchestration_v2_effect_outbox.effect_id
    AND json_extract(start.reference_json, '$.stageCommandId') = orchestration_v2_effect_outbox.command_id
    AND NOT EXISTS (
      SELECT 1 FROM jones_native_creation_execution_holds hold
      WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id
    )
)`;
