import { EventId, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ImportedApplicationAttachmentBirthV1 } from "./ImportedApplicationAttachmentInventory.ts";

const threadIdentity = Schema.fromJsonString(
  Schema.Struct({ id: ThreadId, projectId: ProjectId, createdAt: Schema.String }),
);

export const readApplicationBirthRecord = Effect.fnUntraced(function* (threadId: ThreadId) {
  const sql = yield* SqlClient.SqlClient;
  const births = yield* sql<{
    readonly event_id: string;
    readonly sequence: number;
    readonly payload_json: string;
  }>`SELECT event_id, sequence, payload_json FROM orchestration_events
    WHERE application_event_version = 2 AND aggregate_kind = 'thread'
      AND stream_id = ${threadId} AND event_type = 'thread.created'
    ORDER BY sequence DESC LIMIT 1`;
  if (births.length === 0) return null;
  const current = yield* sql<{ readonly payload_json: string }>`SELECT payload_json
    FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}
      AND json_extract(payload_json, '$.deletedAt') IS NULL`;
  if (current.length !== 1) return null;
  const birth = Schema.decodeUnknownOption(threadIdentity)(births[0]!.payload_json);
  const projection = Schema.decodeUnknownOption(threadIdentity)(current[0]!.payload_json);
  if (
    Option.isNone(birth) ||
    Option.isNone(projection) ||
    birth.value.id !== threadId ||
    projection.value.id !== threadId ||
    birth.value.projectId !== projection.value.projectId ||
    birth.value.createdAt !== projection.value.createdAt
  )
    return null;
  const result = {
    kind: "application_v2_thread_birth" as const,
    threadId,
    eventId: EventId.make(births[0]!.event_id),
    sequence: births[0]!.sequence,
  };
  return Schema.is(ImportedApplicationAttachmentBirthV1)(result) ? result : null;
});
