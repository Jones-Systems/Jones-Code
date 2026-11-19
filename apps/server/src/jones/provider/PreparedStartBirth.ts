import type * as SqlClient from "effect/sql/SqlClient";
import type * as Statement from "effect/sql/Statement";
import { EVENT_LOG_PRUNED_THROUGH_PROJECTION } from "../persistence/EventLogPruneBoundary.ts";

export interface PreparedStartBirth {
  readonly command_id: string;
  readonly sequence: number;
  readonly parent_id: string | null;
  readonly release_sequence: number;
  readonly release_event_id: string;
}

// ThreadLaunch releases under the launch key plus ":release". Use that key to
// find the original command directly, including the parent's multi-thread
// delegation receipt. The release's sequence cannot date the child's input.
export function preparedStartBirth(
  sql: SqlClient.SqlClient,
  input: {
    readonly threadId: Statement.Fragment;
    readonly runId: Statement.Fragment;
    readonly releaseCommandId: Statement.Fragment;
  },
) {
  return sql<PreparedStartBirth>`
    WITH released AS MATERIALIZED (
      SELECT receipt.result_sequence, finish.event_id, finish.payload_json AS run,
        substr(receipt.command_id, 1, length(receipt.command_id) - length(':release')) AS launch_id,
        child.payload_json AS child, message.payload_json AS message,
        json_extract(child.payload_json, '$.lineage.parentThreadId') AS parent_id,
        json_extract(child.payload_json, '$.forkedFrom.nodeId') AS task_id,
        COALESCE((
          SELECT last_sequence FROM orchestration_v2_projection_metadata
          WHERE projection_name = ${EVENT_LOG_PRUNED_THROUGH_PROJECTION}
        ), 0) AS pruned
      FROM orchestration_command_receipts AS receipt
      JOIN orchestration_events AS finish ON finish.sequence = receipt.result_sequence
        AND finish.command_id = receipt.command_id AND finish.stream_id = receipt.aggregate_id
      JOIN orchestration_v2_projection_runs AS run
        ON run.run_id = ${input.runId} AND run.thread_id = receipt.aggregate_id
      JOIN orchestration_v2_projection_threads AS child ON child.thread_id = run.thread_id
      JOIN orchestration_v2_projection_messages AS message
        ON message.message_id = json_extract(run.payload_json, '$.userMessageId')
          AND message.thread_id = child.thread_id AND message.run_id = run.run_id AND message.role = 'user'
      WHERE receipt.command_id = ${input.releaseCommandId} AND substr(receipt.command_id, -8) = ':release'
        AND receipt.aggregate_kind = 'thread' AND receipt.aggregate_id = ${input.threadId}
        AND receipt.command_type = 'prepared-run.release' AND receipt.status = 'accepted' AND receipt.error IS NULL
        AND finish.aggregate_kind = 'thread' AND finish.application_event_version = 2
        AND finish.event_type = 'run.updated'
        AND 3 = (
          SELECT count(*) FROM orchestration_events AS part
          WHERE part.sequence BETWEEN finish.sequence - 3 AND finish.sequence - 1
            AND part.command_id = receipt.command_id AND part.stream_id = receipt.aggregate_id
            AND part.aggregate_kind = 'thread' AND part.application_event_version = 2
            AND part.event_type = CASE part.sequence
              WHEN finish.sequence - 3 THEN 'checkpoint-scope.created'
              WHEN finish.sequence - 2 THEN 'node.updated'
              ELSE 'turn-item.updated'
            END
        )
        AND json_extract(finish.payload_json, '$.status') = 'starting'
        AND json_extract(finish.payload_json, '$.id') = run.run_id
        AND json_extract(finish.payload_json, '$.threadId') = child.thread_id
        AND json_extract(finish.payload_json, '$.activeAttemptId') = json_extract(run.payload_json, '$.activeAttemptId')
        AND json_extract(finish.payload_json, '$.userMessageId') = message.message_id
        AND json_extract(finish.payload_json, '$.rootNodeId') = json_extract(run.payload_json, '$.rootNodeId')
        AND json_extract(finish.payload_json, '$.providerThreadId') = json_extract(run.payload_json, '$.providerThreadId')
        AND json_extract(finish.payload_json, '$.workspacePreparation') = json_extract(run.payload_json, '$.workspacePreparation')
        AND json_extract(finish.payload_json, '$.workspacePreparation.type') IN ('root', 'worktree', 'existing_worktree')
    )
    SELECT birth.command_id, birth.sequence, CASE WHEN accepted.command_type = 'delegated_task.request'
      THEN released.parent_id ELSE NULL END AS parent_id,
      released.result_sequence AS release_sequence, released.event_id AS release_event_id
    FROM released
    JOIN orchestration_command_receipts AS accepted
      ON accepted.command_id IN (released.launch_id, released.launch_id || ':initial-message')
    JOIN orchestration_events AS birth INDEXED BY idx_orch_events_command_id
      ON birth.command_id = accepted.command_id
    WHERE accepted.aggregate_kind = 'thread' AND accepted.status = 'accepted' AND accepted.error IS NULL
      AND accepted.result_sequence >= birth.sequence AND accepted.result_sequence < released.result_sequence
      AND birth.aggregate_kind = 'thread' AND birth.application_event_version = 2
      AND birth.stream_id = ${input.threadId} AND birth.event_type = 'run.created'
      AND json_extract(birth.payload_json, '$.id') = ${input.runId}
      AND json_extract(birth.payload_json, '$.threadId') = ${input.threadId}
      AND json_extract(birth.payload_json, '$.status') = 'preparing'
      AND json_type(birth.payload_json, '$.startedAt') = 'null'
      AND json_type(birth.payload_json, '$.completedAt') = 'null'
      AND json_extract(birth.payload_json, '$.activeAttemptId') = json_extract(released.run, '$.activeAttemptId')
      AND json_extract(birth.payload_json, '$.userMessageId') = json_extract(released.run, '$.userMessageId')
      AND json_extract(birth.payload_json, '$.rootNodeId') = json_extract(released.run, '$.rootNodeId')
      AND json_extract(birth.payload_json, '$.providerThreadId') = json_extract(released.run, '$.providerThreadId')
      AND json_extract(birth.payload_json, '$.workspacePreparation') = json_extract(released.run, '$.workspacePreparation')
      AND (
        SELECT count(*) = max(sequence) - min(sequence) + 1
          AND max(sequence) = accepted.result_sequence AND min(sequence) > released.pruned
          AND min(application_event_version IS 2 AND aggregate_kind = 'thread'
            AND (stream_id = ${input.threadId} OR stream_id = accepted.aggregate_id))
        FROM orchestration_events INDEXED BY idx_orch_events_command_id
        WHERE command_id = accepted.command_id
      )
      AND EXISTS (
        SELECT 1 FROM orchestration_events AS attempt INDEXED BY idx_orch_events_command_id
        WHERE attempt.command_id = accepted.command_id AND attempt.stream_id = ${input.threadId}
          AND attempt.event_type = 'run-attempt.created'
          AND json_extract(attempt.payload_json, '$.id') = json_extract(released.run, '$.activeAttemptId')
          AND json_extract(attempt.payload_json, '$.runId') = ${input.runId}
          AND json_extract(attempt.payload_json, '$.rootNodeId') = json_extract(released.run, '$.rootNodeId')
          AND json_extract(attempt.payload_json, '$.providerThreadId') = json_extract(released.run, '$.providerThreadId')
          AND json_extract(attempt.payload_json, '$.providerInstanceId') = json_extract(released.run, '$.providerInstanceId')
          AND json_extract(attempt.payload_json, '$.attemptOrdinal') = 1
          AND json_extract(attempt.payload_json, '$.reason') = 'initial'
          AND json_extract(attempt.payload_json, '$.status') = 'pending'
          AND json_type(attempt.payload_json, '$.startedAt') = 'null'
      )
      AND EXISTS (
        SELECT 1 FROM orchestration_events AS input INDEXED BY idx_orch_events_command_id
        WHERE input.command_id = accepted.command_id AND input.stream_id = ${input.threadId}
          AND input.event_type = 'message.updated' AND input.payload_json = released.message
      )
      AND (
        (accepted.command_type IN ('message.dispatch', 'thread.create') AND accepted.aggregate_id = ${input.threadId})
        OR (
          accepted.command_type = 'delegated_task.request' AND accepted.command_id = released.launch_id
          AND accepted.aggregate_id = released.parent_id AND released.parent_id <> ${input.threadId}
          AND json_extract(released.child, '$.lineage.relationshipToParent') = 'subagent'
          AND json_extract(released.child, '$.forkedFrom.type') = 'node'
          AND json_extract(released.message, '$.senderThreadId') = released.parent_id
          AND json_extract(released.message, '$.creationSource') = 'mcp'
          AND json_extract(released.message, '$.createdBy') = 'agent'
          AND json_extract(birth.payload_json, '$.ordinal') = 1
          AND json_extract(birth.payload_json, '$.delegatedTaskId') IS NULL
          AND json_extract(birth.payload_json, '$.workspacePreparation.type') IN ('worktree', 'existing_worktree')
          AND EXISTS (
            SELECT 1 FROM orchestration_events AS created INDEXED BY idx_orch_events_command_id
            WHERE created.command_id = accepted.command_id AND created.stream_id = ${input.threadId}
              AND created.event_type = 'thread.created' AND created.sequence < birth.sequence
              AND json_extract(created.payload_json, '$.id') = ${input.threadId}
              AND json_extract(created.payload_json, '$.lineage') = json_extract(released.child, '$.lineage')
              AND json_extract(created.payload_json, '$.forkedFrom') = json_extract(released.child, '$.forkedFrom')
          )
          AND EXISTS (
            SELECT 1 FROM orchestration_v2_projection_subagents AS task
            JOIN orchestration_v2_projection_runs AS owner
              ON owner.run_id = task.run_id AND owner.thread_id = released.parent_id
            JOIN orchestration_events AS requested INDEXED BY idx_orch_events_command_id
              ON requested.command_id = accepted.command_id AND requested.stream_id = released.parent_id
            WHERE task.subagent_id = released.task_id AND task.origin = 'app_owned'
              AND task.thread_id = released.parent_id AND task.child_thread_id = ${input.threadId}
              AND json_extract(owner.payload_json, '$.rootNodeId') = task.parent_node_id
              AND task.status IN ('pending', 'running', 'waiting')
              AND json_type(task.payload_json, '$.result') = 'null'
              AND json_extract(task.payload_json, '$.cancelledRunId') IS NULL
              AND requested.event_type = 'subagent.updated' AND requested.sequence < birth.sequence
              AND json_extract(requested.payload_json, '$.origin') = 'app_owned'
              AND json_extract(requested.payload_json, '$.id') = task.subagent_id
              AND json_extract(requested.payload_json, '$.threadId') = task.thread_id
              AND json_extract(requested.payload_json, '$.childThreadId') = task.child_thread_id
              AND json_extract(requested.payload_json, '$.runId') = owner.run_id
              AND json_extract(requested.payload_json, '$.parentNodeId') = task.parent_node_id
              AND json_extract(requested.payload_json, '$.prompt') = json_extract(released.message, '$.text')
          )
        )
      )
    LIMIT 2
  `;
}
