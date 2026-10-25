import type { EnvironmentId } from "@t3tools/contracts";
import {
  LinkedThreadHandoff,
  LinkedThreadLink,
  LinkedThreadsError,
} from "@t3tools/contracts/jones/linkedThreads";
import type { ThreadEndpointRef } from "@t3tools/contracts/jones/linkedThreads";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export interface LinkRow {
  readonly link_id: string;
  readonly kind: "launch" | "continuation" | "peer";
  readonly from_environment_id: string;
  readonly from_thread_id: string;
  readonly to_environment_id: string;
  readonly to_thread_id: string;
  readonly basis: "launch" | "handoff" | "manual" | "recovered";
  readonly asserted_by: string;
  readonly launch_command_id: string | null;
  readonly from_title: string | null;
  readonly to_title: string | null;
  readonly remote_environment_label: string | null;
  readonly reverse_state: "local" | "recorded" | "unavailable" | "unknown";
  readonly acceptance: "offered" | "accepted" | null;
  readonly handoff_json: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly accepted_at: string | null;
  readonly removed_at: string | null;
}
export const storageFailure = () =>
  new LinkedThreadsError({ reason: "not_found", message: "Linked thread storage is unavailable." });
export const read = Effect.fn("LinkedThreadsRepository.read")(function* (linkId: string) {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<LinkRow>`SELECT * FROM jones_thread_links WHERE link_id = ${linkId}`)[0];
}, Effect.mapError(storageFailure));
// Each existence probe uses the upstream thread primary key. Claims have no visible row until creation.
export const touching = Effect.fn("LinkedThreadsRepository.touching")(function* (
  here: EnvironmentId,
  thread: ThreadEndpointRef,
  limit: number,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<LinkRow>`SELECT l.* FROM jones_thread_links l
    WHERE l.removed_at IS NULL
      AND ((l.from_environment_id = ${thread.environmentId} AND l.from_thread_id = ${thread.threadId})
        OR (l.to_environment_id = ${thread.environmentId} AND l.to_thread_id = ${thread.threadId}))
      AND (l.from_environment_id != ${here} OR EXISTS (SELECT 1 FROM orchestration_v2_projection_threads t WHERE t.thread_id = l.from_thread_id AND t.deleted_at IS NULL))
      AND (l.to_environment_id != ${here} OR EXISTS (SELECT 1 FROM orchestration_v2_projection_threads t WHERE t.thread_id = l.to_thread_id AND t.deleted_at IS NULL))
    ORDER BY l.created_at DESC, l.link_id LIMIT ${limit}`;
}, Effect.mapError(storageFailure));
export const outgoing = Effect.fn("LinkedThreadsRepository.outgoing")(function* (
  here: EnvironmentId,
  thread: ThreadEndpointRef,
  kind: "launch" | "continuation",
  limit: number,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<LinkRow>`SELECT l.* FROM jones_thread_links l
    WHERE l.removed_at IS NULL AND l.kind = ${kind}
      AND l.from_environment_id = ${thread.environmentId} AND l.from_thread_id = ${thread.threadId}
      AND (l.from_environment_id != ${here} OR EXISTS (SELECT 1 FROM orchestration_v2_projection_threads t WHERE t.thread_id = l.from_thread_id AND t.deleted_at IS NULL))
      AND (l.to_environment_id != ${here} OR EXISTS (SELECT 1 FROM orchestration_v2_projection_threads t WHERE t.thread_id = l.to_thread_id AND t.deleted_at IS NULL))
    ORDER BY l.created_at DESC, l.link_id LIMIT ${limit}`;
}, Effect.mapError(storageFailure));
export const insert = Effect.fn("LinkedThreadsRepository.insert")(function* (r: LinkRow) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT OR IGNORE INTO jones_thread_links ${sql.insert({ ...r })}`;
}, Effect.mapError(storageFailure));
export const project = (r: LinkRow, here: EnvironmentId): LinkedThreadLink =>
  Schema.decodeSync(LinkedThreadLink)({
    linkId: r.link_id,
    kind: r.kind,
    from: { environmentId: r.from_environment_id, threadId: r.from_thread_id },
    to: { environmentId: r.to_environment_id, threadId: r.to_thread_id },
    basis: r.basis,
    assertedBy:
      r.asserted_by === "user"
        ? "user"
        : r.asserted_by === "system"
          ? "system"
          : r.asserted_by.startsWith("thread:")
            ? "agent"
            : r.asserted_by.startsWith("link:")
              ? "linked_environment"
              : "client",
    reverse: r.reverse_state,
    acceptance: r.acceptance,
    handoff:
      r.handoff_json === null
        ? null
        : Schema.decodeSync(Schema.fromJsonString(LinkedThreadHandoff))(r.handoff_json),
    remote:
      r.from_environment_id === r.to_environment_id
        ? null
        : {
            environmentLabel: r.remote_environment_label,
            title: r.from_environment_id === here ? r.to_title : r.from_title,
          },
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    acceptedAt: r.accepted_at,
  });
