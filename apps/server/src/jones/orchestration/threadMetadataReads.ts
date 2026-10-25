import type { OrchestrationV2ThreadShell, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { RecordedAppThreadJson } from "../../orchestration-v2/RecordedTypes.ts";

export class ThreadMetadataReadError extends Schema.TaggedError<ThreadMetadataReadError>()(
  "ThreadMetadataReadError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Failed to read thread metadata.";
  }
}

export interface ThreadMetadataQuery {
  readonly location?: "active" | "archive";
  readonly unsettledOnly?: boolean;
  readonly projectId?: ProjectId;
  readonly threadIds?: ReadonlyArray<ThreadId>;
  readonly worktree?: { readonly path: string; readonly branch: string };
}

export type ThreadMetadata = Pick<OrchestrationV2ThreadShell, "id" | "lineage">;

export class ThreadMetadataReads extends Context.Service<
  ThreadMetadataReads,
  {
    readonly listIds: (
      query?: ThreadMetadataQuery,
    ) => Effect.Effect<ReadonlyArray<ThreadId>, ThreadMetadataReadError>;
    readonly list: (
      query?: ThreadMetadataQuery,
    ) => Effect.Effect<
      { readonly snapshotSequence: number; readonly threads: ReadonlyArray<ThreadMetadata> },
      ThreadMetadataReadError
    >;
    readonly count: Effect.Effect<number, ThreadMetadataReadError>;
    readonly watermark: Effect.Effect<number, ThreadMetadataReadError>;
  }
>()("t3/jones/orchestration/threadMetadataReads") {}

const decodeThread = Schema.decodeUnknownEffect(Schema.fromJsonString(RecordedAppThreadJson));

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const predicates = (query: ThreadMetadataQuery) => sql`
    t.deleted_at IS NULL${
      query.projectId === undefined ? sql`` : sql` AND t.project_id = ${query.projectId}`
    }${
      query.location === "active"
        ? sql` AND json_extract(t.payload_json, '$.archivedAt') IS NULL`
        : query.location === "archive"
          ? sql` AND json_extract(t.payload_json, '$.archivedAt') IS NOT NULL`
          : sql``
    }${
      query.unsettledOnly
        ? sql` AND json_extract(t.payload_json, '$.settledAt') IS NULL AND json_extract(t.payload_json, '$.settledOverride') IS NOT 'settled'`
        : sql``
    }${
      query.threadIds === undefined
        ? sql``
        : query.threadIds.length === 0
          ? sql` AND 0`
          : sql` AND t.thread_id IN ${sql.in(query.threadIds)}`
    }${
      query.worktree === undefined
        ? sql``
        : sql` AND json_extract(t.payload_json, '$.worktreePath') = ${query.worktree.path} AND json_extract(t.payload_json, '$.branch') = ${query.worktree.branch}`
    }
  `;
  const ordering = (query: ThreadMetadataQuery) => sql`
    ${
      query.location === undefined
        ? sql`json_extract(t.payload_json, '$.archivedAt') IS NOT NULL ASC,`
        : sql``
    } t.updated_at ASC, t.thread_id ASC
  `;
  const readWatermark = sql<{ readonly snapshot_sequence: number | null }>`
    SELECT MAX(sequence) AS snapshot_sequence
    FROM orchestration_events
    WHERE application_event_version = 2
      AND aggregate_kind = 'thread'
  `.pipe(Effect.map((rows) => rows[0]?.snapshot_sequence ?? 0));
  const annotate = (query: ThreadMetadataQuery, rowCount: number) =>
    Effect.annotateCurrentSpan({
      "threadMetadata.location": query.location ?? "all",
      "threadMetadata.unsettledOnly": query.unsettledOnly === true,
      "threadMetadata.projectFiltered": query.projectId !== undefined,
      "threadMetadata.idsFiltered": query.threadIds !== undefined,
      "threadMetadata.worktreeFiltered": query.worktree !== undefined,
      "threadMetadata.rowCount": rowCount,
    });
  const listIds = Effect.fn("jones.orchestration.threadMetadataReads.listIds")(
    function* (query: ThreadMetadataQuery = {}) {
      const rows = yield* sql.withTransaction(sql<{ readonly thread_id: ThreadId }>`
        SELECT t.thread_id FROM orchestration_v2_projection_threads t
        WHERE ${predicates(query)} ORDER BY ${ordering(query)}
      `);
      yield* annotate(query, rows.length);
      return rows.map((row) => row.thread_id);
    },
    Effect.mapError((cause) => new ThreadMetadataReadError({ cause })),
  );
  const list = Effect.fn("jones.orchestration.threadMetadataReads.list")(
    function* (query: ThreadMetadataQuery = {}) {
      const snapshot = yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql<{ readonly payload_json: string }>`
            SELECT t.payload_json FROM orchestration_v2_projection_threads t
            WHERE ${predicates(query)} ORDER BY ${ordering(query)}
          `;
          const snapshotSequence = yield* readWatermark;
          return { rows, snapshotSequence };
        }),
      );
      // Release the standalone transaction before schema decoding. An enclosing
      // writer transaction still retains its connection and read-your-writes.
      const threads = yield* Effect.forEach(snapshot.rows, (row) =>
        decodeThread(row.payload_json).pipe(
          Effect.map((thread): ThreadMetadata => ({ id: thread.id, lineage: thread.lineage })),
        ),
      );
      yield* annotate(query, threads.length);
      return { snapshotSequence: snapshot.snapshotSequence, threads };
    },
    Effect.mapError((cause) => new ThreadMetadataReadError({ cause })),
  );
  const count = Effect.gen(function* () {
    const rows = yield* sql.withTransaction(sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM orchestration_v2_projection_threads t
        WHERE t.deleted_at IS NULL
      `);
    const count = rows[0]?.count ?? 0;
    yield* annotate({}, count);
    return count;
  }).pipe(
    Effect.mapError((cause) => new ThreadMetadataReadError({ cause })),
    Effect.withSpan("jones.orchestration.threadMetadataReads.count"),
  );
  const watermark = sql.withTransaction(readWatermark).pipe(
    Effect.mapError((cause) => new ThreadMetadataReadError({ cause })),
    Effect.withSpan("jones.orchestration.threadMetadataReads.watermark"),
  );
  return ThreadMetadataReads.of({ listIds, list, count, watermark });
});

export const layer = Layer.effect(ThreadMetadataReads, make);
