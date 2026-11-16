import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { RemoteAdmissionLaunchResult } from "@t3tools/contracts/jones/remoteAdmissionReview";

/** Only an unclaimed denial/cancellation proves refusal. A closed handler with
 * no exact creation evidence remains unknown, regardless of its generic error. */
export const terminalLaunchResult = (input: {
  readonly status: string;
  readonly commandId: string | null;
  readonly recordedAt: string;
  readonly initialMessageRequested: boolean;
}): RemoteAdmissionLaunchResult => {
  const refused =
    input.commandId === null && (input.status === "denied" || input.status === "cancelled");
  return {
    outcome: refused ? "refused" : "unknown",
    stages: {
      creation: refused ? "refused" : "unknown",
      availability: "unknown",
      initialMessage: input.initialMessageRequested
        ? refused
          ? "refused"
          : "unconfirmed"
        : "none",
      preparation: "unknown",
      providerProgress: refused ? "not_started" : "unknown",
    },
    threadId: null,
    recordedAt: input.recordedAt,
    reasonCode: refused ? `remote_admission_${input.status}` : "launch_outcome_unknown",
  };
};

/** Pure projection shared by owner review, peer observation and inspection.
 * Legacy terminal rows are projected without rewriting their immutable receipt. */
export const recordedLaunchResult = Effect.fn("AdmissionRecovery.recordedResult")(function* (
  sql: SqlClient.SqlClient,
  id: string,
) {
  const row = (yield* sql<{
    operation: string;
    status: string;
    command_id: string | null;
    updated_at: string;
    initial_message_requested: number;
    result_json: string | null;
    launch_result: string | null;
  }>`SELECT json_extract(a.envelope_json,'$.operation') operation,a.status,a.command_id,a.updated_at,
    CASE WHEN json_type(a.envelope_json,'$.payload.params.message')='text'
      OR (json_type(a.envelope_json,'$.payload.params.attachments')='array'
        AND json_array_length(a.envelope_json,'$.payload.params.attachments')>0)
      THEN 1 ELSE 0 END initial_message_requested,
    r.result_json,json_extract(r.result_json,'$.launchResult') launch_result
    FROM jones_remote_admissions a LEFT JOIN jones_remote_admission_results r ON r.admission_id=a.id
    WHERE a.id=${id}`)[0];
  if (!row || row.operation !== "t3_thread_launch") return null;
  if (row.launch_result !== null)
    return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RemoteAdmissionLaunchResult))(
      row.launch_result,
    );
  if (row.result_json === null && !["denied", "cancelled", "dispatched"].includes(row.status))
    return null;
  return terminalLaunchResult({
    status: row.status,
    commandId: row.command_id,
    recordedAt: row.updated_at,
    initialMessageRequested: row.initial_message_requested === 1,
  });
});
