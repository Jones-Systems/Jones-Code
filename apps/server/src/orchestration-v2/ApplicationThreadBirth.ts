import { EventId, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import type { OrdinaryApplicationBirthV1 } from "./OrdinaryCheckoutOwnership.ts";

const decodeIdentity = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({ id: ThreadId, projectId: ProjectId, createdAt: Schema.String }),
  ),
);

/** Reads the canonical V2 birth only while its application projection still agrees. */
export const readApplicationThreadBirth = Effect.fn("readApplicationThreadBirth")(function* (
  threadId: ThreadId,
) {
  const sql = yield* SqlClient.SqlClient;
  const births = yield* sql<{
    readonly event_id: string;
    readonly sequence: number;
    readonly payload_json: string;
  }>`SELECT event_id, sequence, payload_json FROM orchestration_events
    WHERE application_event_version = 2 AND aggregate_kind = 'thread'
      AND stream_id = ${threadId} AND event_type = 'thread.created'
    ORDER BY sequence DESC LIMIT 1`;
  const latest = births[0];
  if (latest === undefined) return null;
  const current = yield* sql<{ readonly payload_json: string }>`
    SELECT payload_json FROM orchestration_v2_projection_threads
    WHERE thread_id = ${threadId} AND json_extract(payload_json, '$.deletedAt') IS NULL`;
  if (current.length !== 1) return null;
  const birth = yield* decodeIdentity(latest.payload_json);
  const projection = yield* decodeIdentity(current[0]!.payload_json);
  if (
    birth.id !== threadId ||
    projection.id !== threadId ||
    birth.projectId !== projection.projectId ||
    birth.createdAt !== projection.createdAt
  )
    return null;
  return {
    kind: "application_v2_thread_birth",
    threadId,
    eventId: EventId.make(latest.event_id),
    sequence: latest.sequence,
  } satisfies OrdinaryApplicationBirthV1;
});
