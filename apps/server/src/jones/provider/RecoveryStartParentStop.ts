import type { OrchestrationV2Run } from "@t3tools/contracts";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as Statement from "effect/sql/Statement";
import type { OrchestrationEffectV2 } from "../../orchestration-v2/EffectOutbox.ts";
import { EVENT_LOG_PRUNED_THROUGH_PROJECTION } from "../persistence/EventLogPruneBoundary.ts";
import { preparedStartBirth } from "./PreparedStartBirth.ts";

const recoveryStartCommandPrefix = "command:restart-resend:";

export function isRecoveryStartReplacement(effect: OrchestrationEffectV2): boolean {
  return (
    effect.request.type === "provider-turn.start" &&
    effect.commandId.startsWith(recoveryStartCommandPrefix)
  );
}

export function admitsRecoveryStartClaim(sql: SqlClient.SqlClient) {
  const runId = sql`json_extract(candidate.payload_json, '$.runId')`;
  return sql`CASE
    WHEN candidate.effect_type = 'provider-turn.start'
      AND candidate.command_id GLOB ${`${recoveryStartCommandPrefix}*`}
    THEN CASE WHEN candidate.attempt_count = 0 AND candidate.lease_owner IS NULL
      AND candidate.completed_at IS NULL AND json_valid(candidate.payload_json)
      THEN candidate.command_id = ${recoveryStartCommandPrefix} || ${runId}
        AND ${recoveryStartParentAllowed(sql, sql`candidate.thread_id`, runId)}
      ELSE 0 END
    ELSE 1
  END`;
}

// The outbox's row order is also its insertion order for retained rows. A Stop
// with no events records the parent's previous sequence. Queued and prepared sends
// need an actual Stop event ordered before birth because their effect is written later.
function originalStartAfterStop(sql: SqlClient.SqlClient) {
  const retainedStopEvent = sql`
    stop_event.sequence = stop_receipt.result_sequence
    AND stop_event.command_id = stop.command_id AND stop_event.stream_id = target.parent_id
    AND stop_event.aggregate_kind = 'thread' AND stop_event.application_event_version = 2
    AND stop_event.sequence > COALESCE((
      SELECT last_sequence FROM orchestration_v2_projection_metadata
      WHERE projection_name = ${EVENT_LOG_PRUNED_THROUGH_PROJECTION}
    ), 0)
  `;
  return sql`EXISTS (
    SELECT 1 FROM orchestration_v2_effect_outbox AS original
    JOIN orchestration_command_receipts AS accepted ON accepted.command_id = original.command_id
    WHERE original.thread_id = target.child_id
      AND original.effect_type = 'provider-turn.start'
      AND original.effect_id = 'effect:' || original.command_id || ':provider-turn.start:' || target.run_id
      AND json_extract(original.payload_json, '$.runId') = target.run_id
      AND original.status = 'cancelled' AND original.attempt_count = 0
      AND original.lease_owner IS NULL AND original.completed_at IS NOT NULL
      AND accepted.aggregate_kind = 'thread' AND accepted.aggregate_id = target.child_id
      AND accepted.status = 'accepted' AND accepted.error IS NULL
      AND original.rowid > stop.rowid
      AND (
        accepted.command_type IN ('message.dispatch', 'thread.create')
        OR (accepted.command_type = 'queue.start' AND EXISTS (
          SELECT 1 FROM orchestration_events AS stop_event
          JOIN orchestration_events AS birth
            ON birth.stream_id = target.child_id
              AND birth.aggregate_kind = 'thread' AND birth.application_event_version = 2
              AND birth.event_type = 'run.created' AND birth.sequence > stop_event.sequence
              AND birth.sequence < accepted.result_sequence
              AND json_extract(birth.payload_json, '$.id') = target.run_id
              AND json_extract(birth.payload_json, '$.userMessageId') = target.message_id
              AND json_extract(birth.payload_json, '$.status') = 'queued'
          JOIN orchestration_command_receipts AS sent ON sent.command_id = birth.command_id
          WHERE ${retainedStopEvent}
            AND sent.aggregate_kind = 'thread' AND sent.aggregate_id = target.child_id
            AND sent.status = 'accepted' AND sent.error IS NULL
            AND sent.command_type IN ('message.dispatch', 'thread.create')
            AND sent.result_sequence >= birth.sequence
        ))
        OR (accepted.command_type = 'prepared-run.release'
          AND original.payload_json = json_object('type', 'provider-turn.start', 'runId', target.run_id)
          AND EXISTS (
            SELECT 1 FROM orchestration_events AS stop_event
            WHERE ${retainedStopEvent}
              AND 1 = (
                SELECT count(*) FROM (${preparedStartBirth(sql, {
                  threadId: sql`target.child_id`,
                  runId: sql`target.run_id`,
                  releaseCommandId: sql`original.command_id`,
                })}) AS birth
                HAVING min(birth.sequence) > stop_event.sequence
                  AND min(birth.parent_id IS NULL OR birth.parent_id = target.parent_id)
              )
          )
        )
      )
  )`;
}

// The parent's durable fan-out fences older delegated starts before the child
// Stop arrives. A missing task ID leaves old starts ambiguous unless the stored
// message identifies an independent sender.
export function recoveryStartParentAllowed(
  sql: SqlClient.SqlClient,
  threadId: Statement.Fragment,
  runId: Statement.Fragment,
  expectedRun?: Pick<OrchestrationV2Run, "activeAttemptId" | "delegatedTaskId">,
) {
  const binding =
    expectedRun === undefined
      ? sql`1 = 1`
      : sql`json_extract(run.payload_json, '$.activeAttemptId') IS ${expectedRun.activeAttemptId}
      AND json_extract(run.payload_json, '$.delegatedTaskId') IS ${expectedRun.delegatedTaskId ?? null}`;
  return sql`EXISTS (
    WITH target AS (
      SELECT child.thread_id AS child_id, run.run_id,
        json_extract(child.payload_json, '$.lineage.parentThreadId') AS parent_id,
        json_extract(child.payload_json, '$.lineage.relationshipToParent') AS relationship,
        json_extract(run.payload_json, '$.delegatedTaskId') AS task_id,
        json_extract(run.payload_json, '$.userMessageId') AS message_id,
        json_extract(message.payload_json, '$.senderThreadId') AS sender_id,
        json_extract(message.payload_json, '$.createdBy') AS sender_kind,
        json_extract(message.payload_json, '$.scheduledTaskId') AS scheduled_task_id
      FROM orchestration_v2_projection_runs AS run
      JOIN orchestration_v2_projection_threads AS child ON child.thread_id = run.thread_id
      LEFT JOIN orchestration_v2_projection_messages AS message
        ON message.message_id = json_extract(run.payload_json, '$.userMessageId')
          AND message.thread_id = child.thread_id AND message.run_id = run.run_id AND message.role = 'user'
      WHERE run.run_id = ${runId} AND run.thread_id = ${threadId} AND ${binding}
    )
    SELECT 1 FROM target
    WHERE (target.task_id IS NULL OR (
      target.relationship IS 'subagent' AND EXISTS (
        SELECT 1 FROM orchestration_v2_projection_subagents AS task
        JOIN orchestration_v2_projection_runs AS owner ON owner.run_id = task.run_id
          AND owner.thread_id = target.parent_id
        WHERE task.subagent_id = target.task_id AND task.origin = 'app_owned'
          AND task.thread_id = target.parent_id AND task.child_thread_id = target.child_id
      )
    ))
      AND CASE
        WHEN target.relationship IS NOT 'subagent' OR target.parent_id IS NULL THEN 1
        WHEN target.task_id IS NULL AND (
          (target.sender_id IS NULL AND (target.sender_kind IS 'user' OR target.scheduled_task_id IS NOT NULL))
          OR (typeof(target.sender_id) = 'text' AND target.sender_id NOT IN (target.parent_id, target.child_id))
        ) THEN 1
        ELSE NOT EXISTS (
          SELECT 1 FROM orchestration_v2_effect_outbox AS stop
          LEFT JOIN orchestration_command_receipts AS stop_receipt ON stop_receipt.command_id = stop.command_id
            AND stop_receipt.aggregate_kind = 'thread' AND stop_receipt.aggregate_id = target.parent_id
            AND stop_receipt.command_type IN ('thread.stop', 'run.interrupt')
            AND stop_receipt.status = 'accepted' AND stop_receipt.error IS NULL
          WHERE stop.thread_id = target.parent_id AND stop.effect_type = 'delegated-tasks.stop'
            AND stop.effect_id = 'effect:' || stop.command_id || ':delegated-tasks.stop'
            AND NOT (${originalStartAfterStop(sql)})
        )
      END
  )`;
}
