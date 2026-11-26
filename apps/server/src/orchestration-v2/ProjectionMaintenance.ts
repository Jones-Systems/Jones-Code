import type { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";

import {
  EVENT_LOG_PRUNE_MARK,
  EVENT_LOG_PRUNED_THROUGH,
} from "../persistence/OrchestrationEventStore.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { isPublicStoredOrchestrationEvent } from "./WireProjection.ts";

export interface ProjectionVerificationV2 {
  readonly valid: boolean;
  readonly schemaVersion: number;
  readonly expectedSequence: number;
  readonly projectionSequence: number;
  readonly unreadableThreadIds: ReadonlyArray<ThreadId>;
  readonly missingThreadIds: ReadonlyArray<ThreadId>;
  readonly unexpectedThreadIds: ReadonlyArray<ThreadId>;
}

export class ProjectionMaintenanceError extends Schema.TaggedError<ProjectionMaintenanceError>()(
  "ProjectionMaintenanceError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface ProjectionMaintenanceV2Shape {
  /**
   * Deletes events the projections already cover once they are older than the retained history,
   * then purges any deleted thread that still holds conversation content.
   */
  readonly pruneEventStore: (betweenPages?: Effect.Effect<void>) => Effect.Effect<
    {
      readonly deletedEventCount: number;
      readonly deletedReceiptCount: number;
      readonly purgedRowCount: number;
      readonly reclaimableBytes: number;
    },
    ProjectionMaintenanceError
  >;
  /**
   * Removes a deleted thread's conversation, and that of deleted run-fork sources it alone kept.
   * Returns the number of rows removed.
   */
  readonly purgeDeletedThread: (
    threadId: ThreadId,
  ) => Effect.Effect<number, ProjectionMaintenanceError>;
}

export class ProjectionMaintenanceV2 extends Context.Service<
  ProjectionMaintenanceV2,
  ProjectionMaintenanceV2Shape
>()("t3/orchestration-v2/ProjectionMaintenance/ProjectionMaintenanceV2") {}

// Reconnect replay and command-id retries read this tail; older cursors resync from a snapshot.
const RETAINED_EVENT_HISTORY = Duration.days(1);
// Small pages bound work on the reader worker and synchronous fallback.
const PAGE_SIZE = 50;
const REFERENCE_SCAN_WINDOW = 1000;
// Bounds how long one purge statement holds the writer: on a real database a 500-row batch of
// turn items took about 400 ms, index and count-trigger work included.
const PURGE_BATCH_SIZE = 50;
// Titles default to the first message, so a tombstone cannot keep the real one.
export const DELETED_THREAD_TITLE = "Deleted thread";

// Every thread_id table that holds conversation text. The thread row stays as the tombstone, and
// the metadata that storage cleanup and usage history still read (checkpoints, provider sessions,
// provider turns, run attempts, context transfers) stays with it.
export const DELETED_THREAD_CONTENT_TABLES = [
  "orchestration_v2_turn_item_positions",
  "orchestration_v2_projection_turn_items",
  "orchestration_v2_projection_messages",
  "orchestration_v2_projection_nodes",
  "orchestration_v2_projection_runs",
  "orchestration_v2_projection_subagents",
  "orchestration_v2_projection_context_handoffs",
  "orchestration_v2_projection_runtime_requests",
  "orchestration_v2_projection_plans",
  "mcp_app_model_context",
  // A legacy thread's transcript source; deleting it means the importer has nothing to restore.
  "projection_thread_messages",
] as const;

type MetadataRow = {
  readonly projection_name: string;
  readonly last_sequence: number;
  readonly updated_at: string;
};

type ReadClassification = typeof backgroundRead;

type ProjectionMetadataRow = {
  readonly schema_version: number;
  readonly last_sequence: number;
};

const encodeEntityKey = Schema.encodeSync(
  Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String, Schema.NullOr(Schema.String)])),
);

export const layer: Layer.Layer<
  ProjectionMaintenanceV2,
  never,
  EventStore.EventStoreV2 | ProjectionStore.ProjectionStoreV2 | SqlClient.SqlClient
> = Layer.effect(
  ProjectionMaintenanceV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const legacyStorage = LegacyStorage.make(sql);
    const eventStore = yield* EventStore.EventStoreV2;
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;

    const verify = Effect.gen(function* () {
      const expectedThreadRows = yield* sql<{ readonly thread_id: string }>`
        SELECT DISTINCT stream_id AS thread_id
        FROM orchestration_events INDEXED BY orchestration_events_v2_created_threads_idx
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND event_type = 'thread.created'
        ORDER BY stream_id ASC
      `;
      const projectionRows = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id
        FROM orchestration_v2_projection_threads
        ORDER BY thread_id ASC
      `;
      const actualIds = projectionRows.map((row) => ThreadId.make(row.thread_id));
      const expectedIds = expectedThreadRows.map((row) => ThreadId.make(row.thread_id));
      const actualSet = new Set(actualIds);
      const expectedSet = new Set(expectedIds);
      const missingThreadIds = expectedIds.filter((threadId) => !actualSet.has(threadId));
      const unexpectedThreadIds = actualIds.filter((threadId) => !expectedSet.has(threadId));
      const unreadableThreadIds = yield* projectionStore.getUnreadableThreadIds();
      const metadata = yield* sql<ProjectionMetadataRow>`
        SELECT schema_version, last_sequence
        FROM orchestration_v2_projection_metadata
        WHERE projection_name = 'thread-projections'
        LIMIT 1
      `;
      const expectedSequence = yield* eventStore.latestSequence();
      const schemaVersion = metadata[0]?.schema_version ?? 0;
      const projectionSequence = metadata[0]?.last_sequence ?? 0;
      return {
        valid:
          schemaVersion === ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION &&
          projectionSequence === expectedSequence &&
          missingThreadIds.length === 0 &&
          unexpectedThreadIds.length === 0 &&
          unreadableThreadIds.length === 0,
        schemaVersion,
        expectedSequence,
        projectionSequence,
        unreadableThreadIds,
        missingThreadIds,
        unexpectedThreadIds,
      } satisfies ProjectionVerificationV2;
    }).pipe(sql.withTransaction);

    const rebuild = Effect.gen(function* () {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const throughSequence = yield* eventStore.latestSequence();
          // Jones hook: replay writes public rows only, so retain historical inline values first.
          yield* legacyStorage
            .preserveInlineProjection()
            .pipe(Effect.provideService(SqlClient.SqlClient, sql));
          yield* sql`DELETE FROM orchestration_v2_projection_context_transfers`;
          yield* sql`DELETE FROM orchestration_v2_projection_context_handoffs`;
          yield* sql`DELETE FROM orchestration_v2_projection_checkpoints`;
          yield* sql`DELETE FROM orchestration_v2_projection_checkpoint_scopes`;
          yield* sql`DELETE FROM orchestration_v2_projection_turn_items`;
          yield* sql`DELETE FROM orchestration_v2_projection_plans`;
          yield* sql`DELETE FROM orchestration_v2_projection_messages`;
          yield* sql`DELETE FROM orchestration_v2_projection_runtime_requests`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_turns`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_threads`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_sessions`;
          yield* sql`DELETE FROM orchestration_v2_projection_subagents`;
          yield* sql`DELETE FROM orchestration_v2_projection_nodes`;
          yield* sql`DELETE FROM orchestration_v2_projection_run_attempts`;
          yield* sql`DELETE FROM orchestration_v2_projection_runs`;
          yield* sql`DELETE FROM orchestration_v2_projection_threads`;
          yield* sql`DELETE FROM orchestration_v2_turn_item_positions`;

          const pageSize = 500;
          let lastSequence = 0;
          while (true) {
            const page = yield* eventStore
              .read({ afterSequence: lastSequence, throughSequence, limit: pageSize })
              .pipe(Stream.runCollect);
            for (const stored of page) {
              // Retained private preflight rows keep their sequence but never reach projections.
              if (isPublicStoredOrchestrationEvent(stored)) {
                yield* projectionStore.apply(stored.event);
                if (stored.event.type === "turn-item.updated") {
                  yield* sql`
                  INSERT INTO orchestration_v2_turn_item_positions (
                    thread_id,
                    turn_item_id,
                    ordinal
                  )
                  VALUES (
                    ${stored.event.threadId},
                    ${stored.event.payload.id},
                    ${stored.event.payload.ordinal}
                  )
                  ON CONFLICT(thread_id, turn_item_id) DO UPDATE SET
                    ordinal = excluded.ordinal
                  `;
                }
              }
              lastSequence = stored.sequence;
            }
            if (page.length < pageSize) break;
            yield* Effect.yieldNow;
          }
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`
            INSERT INTO orchestration_v2_projection_metadata (
              projection_name,
              schema_version,
              last_sequence,
              updated_at
            )
            VALUES (
              'thread-projections',
              ${ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
              ${lastSequence},
              ${now}
            )
            ON CONFLICT(projection_name) DO UPDATE SET
              schema_version = excluded.schema_version,
              last_sequence = excluded.last_sequence,
              updated_at = excluded.updated_at
          `;
        }),
      );
      return yield* verify;
    });

    const mapError =
      (operation: string) =>
      <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(
          Effect.mapError((cause) => new ProjectionMaintenanceError({ operation, cause })),
        );

    // Keep thread.created as the verification and replay anchor; clients recognize deletion by thread.deleted event type.
    const SUPERSEDABLE_THREAD_EVENT_TYPES = [
      "thread.archived",
      "thread.unarchived",
      "thread.settled",
      "thread.unsettled",
      "thread.snoozed",
      "thread.unsnoozed",
      "thread.pinned",
      "thread.auto-settle-set",
      "thread.unpinned",
      "thread.pin-reordered",
      "thread.active-reordered",
      "thread.metadata-updated",
      "thread.pull-request-synced",
      "thread.runtime-mode-updated",
      "thread.interaction-mode-updated",
      "thread.model-selection-updated",
      "thread.provider-switched",
      "thread.guest-share-updated",
    ];
    // Read-state events preserve updatedAt and supersede only each other. Activity events supersede both groups.
    const READ_STATE_THREAD_EVENT_TYPES = ["thread.visited", "thread.marked-unread"];
    // Older clients skip tag events, so these supersede only each other.
    const TAG_THREAD_EVENT_TYPE = "thread.tags-updated";

    const supersedableThreadEventTypes = new Set(SUPERSEDABLE_THREAD_EVENT_TYPES);
    const readStateThreadEventTypes = new Set(READ_STATE_THREAD_EVENT_TYPES);
    // Subagent snapshots stay intact because later rows can reference an earlier prompt.
    // Commanded turn items stay for command-result replay.
    // Small pages bound work on the reader worker and synchronous fallback.
    const COMPACTION_PAGE_SIZE = 50;

    // Keep first/latest keyed events for replay order; page every event to bound sparse scans.
    const compactEventStore = Effect.fn("ProjectionMaintenance.compactEventStore")(function* (
      betweenPages?: Effect.Effect<void>,
    ) {
      // DatabaseStorage's automatic compactor supplies its duty-cycle hook.
      // The offline/manual DatabaseMaintenance caller supplies none. Keep its
      // reads interactive; classify only the automatic scan bodies below.
      const classifyRead = betweenPages === undefined ? interactiveRead : backgroundRead;
      const pauseBetweenPages = betweenPages ?? Effect.void;
      const bounds = yield* sql<{
        readonly event_sequence: number;
        readonly receipt_row_id: number;
      }>`
        SELECT
          COALESCE((SELECT MAX(sequence) FROM orchestration_events), 0) AS event_sequence,
          COALESCE((SELECT MAX(rowid) FROM orchestration_command_receipts), 0) AS receipt_row_id
      `.pipe(NodeSqliteClient.readOnly, classifyRead);
      let throughSequence = bounds[0]?.event_sequence ?? 0;
      let throughReceiptRowId = bounds[0]?.receipt_row_id ?? 0;
      const retainedThreadIds = new Set<string>();
      const retainedReadStateThreadIds = new Set<string>();
      const retainedTagThreadIds = new Set<string>();
      // Retain each key's oldest candidate, deleting it only when an older event appears.
      const oldestCandidateByEntityKey = new Map<string, number | null>();
      let deletedEventCount = 0;
      let deletedReceiptCount = 0;

      while (throughSequence > 0) {
        const rows = yield* sql<{
          readonly sequence: number;
          readonly application_event_version: number;
          readonly aggregate_kind: string;
          readonly stream_id: string;
          readonly event_type: string;
          readonly entity_id: string | null;
          readonly imported_legacy_thread: number;
        }>`
          SELECT
            event.sequence,
            event.application_event_version,
            event.aggregate_kind,
            event.stream_id,
            event.event_type,
            CASE
              WHEN event.application_event_version = 2
                AND event.event_type IN ('message.updated', 'node.updated')
              THEN json_extract(event.payload_json, '$.id')
              WHEN event.application_event_version = 2
                AND event.event_type = 'turn-item.updated'
                AND event.command_id IS NULL
                AND json_extract(event.payload_json, '$.type') <> 'subagent'
              THEN json_extract(event.payload_json, '$.id')
              WHEN event.application_event_version = 2
                AND event.event_type = 'thread.pull-request-link-updated'
              THEN json_array(json_extract(event.payload_json, '$.host'),
                json_extract(event.payload_json, '$.repository'),
                json_extract(event.payload_json, '$.number'))
              ELSE NULL
            END AS entity_id,
            CASE
              WHEN event.application_event_version = 1 AND event.aggregate_kind = 'thread'
              THEN EXISTS (
                SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import
                WHERE legacy_import.thread_id = event.stream_id
                  AND legacy_import.transcript_imported_at IS NOT NULL
              )
              ELSE 0
            END AS imported_legacy_thread
          FROM orchestration_events AS event
          WHERE event.sequence <= ${throughSequence}
          ORDER BY event.sequence DESC
          LIMIT ${COMPACTION_PAGE_SIZE + 1}
        `.pipe(NodeSqliteClient.readOnly, classifyRead);
        const page = rows.slice(0, COMPACTION_PAGE_SIZE);
        const obsolete: number[] = [];
        for (const row of page) {
          if (row.imported_legacy_thread === 1) {
            obsolete.push(row.sequence);
          } else if (row.application_event_version === 2) {
            if (
              row.aggregate_kind === "thread" &&
              supersedableThreadEventTypes.has(row.event_type)
            ) {
              if (retainedThreadIds.has(row.stream_id)) obsolete.push(row.sequence);
              else retainedThreadIds.add(row.stream_id);
            } else if (
              row.aggregate_kind === "thread" &&
              readStateThreadEventTypes.has(row.event_type)
            ) {
              if (
                retainedThreadIds.has(row.stream_id) ||
                retainedReadStateThreadIds.has(row.stream_id)
              ) {
                obsolete.push(row.sequence);
              } else retainedReadStateThreadIds.add(row.stream_id);
            } else if (
              row.aggregate_kind === "thread" &&
              row.event_type === TAG_THREAD_EVENT_TYPE
            ) {
              if (retainedThreadIds.has(row.stream_id) || retainedTagThreadIds.has(row.stream_id)) {
                obsolete.push(row.sequence);
              } else retainedTagThreadIds.add(row.stream_id);
            } else if (
              row.event_type === "thread.pull-request-link-updated" &&
              retainedThreadIds.has(row.stream_id)
            ) {
              // A newer whole-thread payload already carries this link's state.
              obsolete.push(row.sequence);
            } else if (
              row.event_type === "thread.pull-request-link-updated" ||
              row.event_type === "message.updated" ||
              row.event_type === "node.updated" ||
              (row.event_type === "turn-item.updated" && row.entity_id !== null)
            ) {
              const key = encodeEntityKey([row.event_type, row.stream_id, row.entity_id]);
              const candidate = oldestCandidateByEntityKey.get(key);
              if (candidate === undefined) oldestCandidateByEntityKey.set(key, null);
              else {
                if (candidate !== null) obsolete.push(candidate);
                oldestCandidateByEntityKey.set(key, row.sequence);
              }
            }
          }
        }
        // Never hold a transaction or the reader lock across a yield or a rest.
        yield* Effect.yieldNow;
        if (obsolete.length > 0) {
          yield* sql`DELETE FROM orchestration_events WHERE sequence IN ${sql.in(obsolete)}`;
          deletedEventCount += obsolete.length;
          yield* Effect.yieldNow;
        }
        throughSequence =
          rows.length <= COMPACTION_PAGE_SIZE ? 0 : (page.at(-1)?.sequence ?? 1) - 1;
        if (throughSequence > 0 || throughReceiptRowId > 0) yield* pauseBetweenPages;
      }

      while (throughReceiptRowId > 0) {
        const rows = yield* sql<{
          readonly row_id: number;
          readonly command_id: string;
          readonly imported_legacy_thread: number;
        }>`
          SELECT
            receipt.rowid AS row_id,
            receipt.command_id,
            CASE
              WHEN receipt.command_type = 'legacy' AND receipt.aggregate_kind = 'thread'
              THEN EXISTS (
                SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import
                WHERE legacy_import.thread_id = receipt.aggregate_id
                  AND legacy_import.transcript_imported_at IS NOT NULL
              )
              ELSE 0
            END AS imported_legacy_thread
          FROM orchestration_command_receipts AS receipt
          WHERE receipt.rowid <= ${throughReceiptRowId}
          ORDER BY receipt.rowid DESC
          LIMIT ${COMPACTION_PAGE_SIZE + 1}
        `.pipe(NodeSqliteClient.readOnly, classifyRead);
        const page = rows.slice(0, COMPACTION_PAGE_SIZE);
        const obsolete = page
          .filter((row) => row.imported_legacy_thread === 1)
          .map((row) => row.command_id);
        yield* Effect.yieldNow;
        if (obsolete.length > 0) {
          yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id IN ${sql.in(obsolete)}`;
          deletedReceiptCount += obsolete.length;
          yield* Effect.yieldNow;
        }
        throughReceiptRowId = (page.at(-1)?.row_id ?? 1) - 1;
        if (rows.length <= COMPACTION_PAGE_SIZE) break;
        yield* pauseBetweenPages;
      }

      const freelistRows = yield* sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`;
      const pageSizeRows = yield* sql<{ readonly page_size: number }>`PRAGMA page_size`;
      const reclaimableBytes =
        (freelistRows[0]?.freelist_count ?? 0) * (pageSizeRows[0]?.page_size ?? 0);

      return { deletedEventCount, deletedReceiptCount, reclaimableBytes };
    }, mapError("compact event store"));

    const writeMetadata = (name: string, sequence: number, at: DateTime.Utc) => sql`
      INSERT INTO orchestration_v2_projection_metadata (
        projection_name, schema_version, last_sequence, updated_at
      )
      VALUES (
        ${name},
        ${ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
        ${sequence},
        ${DateTime.formatIso(at)}
      )
      ON CONFLICT(projection_name) DO UPDATE SET
        last_sequence = excluded.last_sequence,
        updated_at = excluded.updated_at
    `;

    // Collects anchors whose prompt later rows borrow through promptSequence.
    const readPromptReferences = Effect.fnUntraced(function* (
      afterSequence: number,
      throughSequence: number,
      betweenPages: Effect.Effect<void>,
      classifyRead: ReadClassification,
    ) {
      const referenced = new Set<number>();
      for (let cursor = afterSequence; cursor < throughSequence;) {
        const windowEnd = Math.min(cursor + REFERENCE_SCAN_WINDOW, throughSequence);
        const rows = yield* sql<{ readonly prompt_sequence: number }>`
          SELECT json_extract(payload_json, '$.promptSequence') AS prompt_sequence
          FROM orchestration_events
          WHERE sequence > ${cursor}
            AND sequence <= ${windowEnd}
            AND event_type IN ('subagent.updated', 'turn-item.updated')
            AND json_type(payload_json, '$.promptSequence') = 'integer'
        `.pipe(NodeSqliteClient.readOnly, classifyRead);
        for (const row of rows) referenced.add(row.prompt_sequence);
        cursor = windowEnd;
        yield* betweenPages;
      }
      return referenced;
    });

    const pruneEvents = Effect.fnUntraced(function* (
      throughSequence: number,
      head: number,
      betweenPages: Effect.Effect<void>,
      classifyRead: ReadClassification,
    ) {
      const referenced = yield* readPromptReferences(
        throughSequence,
        head,
        betweenPages,
        classifyRead,
      );
      // Stream heads at or below the boundary are kept, so the anchors they borrow must stay too.
      const keptHeadReferences = yield* sql<{ readonly prompt_sequence: number }>`
        SELECT json_extract(event.payload_json, '$.promptSequence') AS prompt_sequence
        FROM (
          SELECT MAX(sequence) AS sequence FROM orchestration_events
          GROUP BY aggregate_kind, stream_id
        ) AS heads
        JOIN orchestration_events AS event ON event.sequence = heads.sequence
        WHERE heads.sequence <= ${throughSequence}
          AND event.event_type IN ('subagent.updated', 'turn-item.updated')
          AND json_type(event.payload_json, '$.promptSequence') = 'integer'
      `.pipe(NodeSqliteClient.readOnly, classifyRead);
      for (const row of keptHeadReferences) referenced.add(row.prompt_sequence);
      yield* betweenPages;
      let referencesThrough = head;
      let deleted = 0;
      let cursor = 0;
      while (cursor < throughSequence) {
        const rows = yield* sql<{ readonly sequence: number; readonly prunable: number }>`
          SELECT
            event.sequence,
            CASE
              WHEN event.application_event_version = 1 THEN 0
              -- The legacy importer treats a thread without thread.created as not yet imported.
              WHEN event.event_type = 'thread.created' THEN 0
              -- Codex usage history reads the selection a run attempt started with.
              WHEN event.event_type = 'run.updated'
                AND json_extract(event.payload_json, '$.status') = 'running' THEN 0
              -- The newest row keeps each stream's high water, stream_version, and snapshot sequence.
              WHEN event.sequence = (
                SELECT MAX(newer.sequence) FROM orchestration_events AS newer
                WHERE newer.aggregate_kind = event.aggregate_kind
                  AND newer.stream_id = event.stream_id
              ) THEN 0
              ELSE 1
            END AS prunable
          FROM orchestration_events AS event
          WHERE event.sequence > ${cursor}
            AND event.sequence <= ${throughSequence}
          ORDER BY event.sequence ASC
          LIMIT ${PAGE_SIZE}
        `.pipe(NodeSqliteClient.readOnly, classifyRead);
        const candidates = rows.filter((row) => row.prunable === 1).map((row) => row.sequence);
        // Never hold a transaction or the reader lock across a yield or a rest.
        yield* Effect.yieldNow;
        if (candidates.length > 0) {
          deleted += yield* sql.withTransaction(
            Effect.gen(function* () {
              // Rows appended since the reference scan may borrow a prompt from a candidate.
              const appended = yield* sql<{
                readonly sequence: number;
                readonly prompt_sequence: number | null;
              }>`
                SELECT
                  sequence,
                  CASE
                    WHEN event_type IN ('subagent.updated', 'turn-item.updated')
                      AND json_type(payload_json, '$.promptSequence') = 'integer'
                    THEN json_extract(payload_json, '$.promptSequence')
                  END AS prompt_sequence
                FROM orchestration_events
                WHERE sequence > ${referencesThrough}
              `;
              for (const row of appended) {
                if (row.prompt_sequence !== null) referenced.add(row.prompt_sequence);
                referencesThrough = Math.max(referencesThrough, row.sequence);
              }
              const obsolete = candidates.filter((sequence) => !referenced.has(sequence));
              if (obsolete.length === 0) return 0;
              yield* sql`DELETE FROM orchestration_events WHERE sequence IN ${sql.in(obsolete)}`;
              return obsolete.length;
            }),
          );
          yield* Effect.yieldNow;
        }
        // Rest after every page, including the last, so the next loop starts a fresh slice.
        yield* betweenPages;
        if (rows.length < PAGE_SIZE) break;
        cursor = rows.at(-1)?.sequence ?? throughSequence;
      }
      return deleted;
    });

    // Imported legacy threads are served from v2 rows; their v1 events and receipts have no reader.
    const pruneImportedLegacyHistory = Effect.fnUntraced(function* (
      throughSequence: number,
      throughReceiptRowId: number,
      betweenPages: Effect.Effect<void>,
      classifyRead: ReadClassification,
    ) {
      let deletedEventCount = 0;
      let deletedReceiptCount = 0;
      for (let cursor = 0; ;) {
        const rows = yield* sql<{ readonly sequence: number; readonly imported: number }>`
          SELECT
            event.sequence,
            event.aggregate_kind = 'thread' AND EXISTS (
              SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import
              WHERE legacy_import.thread_id = event.stream_id
                AND legacy_import.transcript_imported_at IS NOT NULL
            ) AS imported
          FROM orchestration_events AS event
            INDEXED BY idx_orchestration_events_application_sequence
          WHERE event.application_event_version = 1
            AND event.sequence > ${cursor}
            AND event.sequence <= ${throughSequence}
          ORDER BY event.sequence ASC
          LIMIT ${PAGE_SIZE}
        `.pipe(NodeSqliteClient.readOnly, classifyRead);
        const obsolete = rows.filter((row) => row.imported === 1).map((row) => row.sequence);
        yield* Effect.yieldNow;
        if (obsolete.length > 0) {
          yield* sql`DELETE FROM orchestration_events WHERE sequence IN ${sql.in(obsolete)}`;
          deletedEventCount += obsolete.length;
          yield* Effect.yieldNow;
        }
        yield* betweenPages;
        if (rows.length < PAGE_SIZE) break;
        cursor = rows.at(-1)?.sequence ?? cursor;
      }
      for (let cursor = 0; ;) {
        const rows = yield* sql<{
          readonly row_id: number;
          readonly command_id: string;
          readonly imported: number;
        }>`
          SELECT
            receipt.rowid AS row_id,
            receipt.command_id,
            receipt.command_type = 'legacy' AND receipt.aggregate_kind = 'thread' AND EXISTS (
              SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import
              WHERE legacy_import.thread_id = receipt.aggregate_id
                AND legacy_import.transcript_imported_at IS NOT NULL
            ) AS imported
          FROM orchestration_command_receipts AS receipt
          WHERE receipt.rowid > ${cursor} AND receipt.rowid <= ${throughReceiptRowId}
          ORDER BY receipt.rowid ASC
          LIMIT ${PAGE_SIZE}
        `.pipe(NodeSqliteClient.readOnly, classifyRead);
        const obsolete = rows.filter((row) => row.imported === 1).map((row) => row.command_id);
        yield* Effect.yieldNow;
        if (obsolete.length > 0) {
          yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id IN ${sql.in(obsolete)}`;
          deletedReceiptCount += obsolete.length;
          yield* Effect.yieldNow;
        }
        yield* betweenPages;
        if (rows.length < PAGE_SIZE) break;
        cursor = rows.at(-1)?.row_id ?? cursor;
      }
      return { deletedEventCount, deletedReceiptCount };
    });

    // A run fork shows its source's history without copying it, through any chain of forks, and a
    // pending merge-back still reads its source. Such a deleted thread keeps its content until
    // every live thread that shows it is deleted too.
    const purgeableThreads = (deletedThreadId: ThreadId | undefined) =>
      sql<{
        readonly thread_id: string;
      }>`
      WITH RECURSIVE shown(thread_id) AS (
        SELECT thread_id FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL
        UNION
        SELECT transfer.source_thread_id
        FROM orchestration_v2_projection_context_transfers AS transfer
        JOIN orchestration_v2_projection_threads AS target
          ON target.thread_id = transfer.target_thread_id
        WHERE transfer.type = 'merge_back' AND transfer.status = 'pending'
          AND target.deleted_at IS NULL
        UNION
        SELECT json_extract(fork.payload_json, '$.forkedFrom.threadId')
        FROM shown
        JOIN orchestration_v2_projection_threads AS fork ON fork.thread_id = shown.thread_id
        WHERE json_extract(fork.payload_json, '$.forkedFrom.type') = 'run'
      ),
      candidate(thread_id) AS (${
        deletedThreadId === undefined
          ? sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE deleted_at IS NOT NULL`
          : // Deleting a fork can release the deleted sources it was the last to show.
            sql`
              SELECT ${deletedThreadId}
              UNION
              SELECT json_extract(fork.payload_json, '$.forkedFrom.threadId')
              FROM candidate
              JOIN orchestration_v2_projection_threads AS fork
                ON fork.thread_id = candidate.thread_id
              WHERE json_extract(fork.payload_json, '$.forkedFrom.type') = 'run'
            `
      })
      SELECT thread.thread_id
      FROM candidate
      JOIN orchestration_v2_projection_threads AS thread ON thread.thread_id = candidate.thread_id
      WHERE thread.deleted_at IS NOT NULL
        AND thread.thread_id NOT IN (SELECT thread_id FROM shown WHERE thread_id IS NOT NULL)
        -- Earlier effects still read the thread; a later purge or sweep takes it.
        AND NOT EXISTS (
          SELECT 1 FROM orchestration_v2_effect_outbox AS effect
          WHERE effect.thread_id = thread.thread_id
            AND effect.status IN ('pending', 'running')
            -- Title generation has its own lane and needs nothing the purge removes.
            AND effect.effect_type NOT IN ('thread-content.purge', 'thread-title.generate')
        )
        -- A live parent settles its delegated task from this child's runs.
        AND NOT EXISTS (
          SELECT 1 FROM orchestration_v2_projection_subagents AS task
          JOIN orchestration_v2_projection_threads AS parent ON parent.thread_id = task.thread_id
          WHERE task.child_thread_id = thread.thread_id
            AND task.status IN ('idle', 'pending', 'running', 'waiting')
            AND parent.deleted_at IS NULL
        )
        -- The delete's own purge always runs; the sweep only takes threads with something left.
        AND ${
          deletedThreadId !== undefined
            ? sql`1 = 1`
            : sql.or([
                sql`thread.title <> ${DELETED_THREAD_TITLE}`,
                // A thread renamed to the placeholder still has its first title in thread.created.
                sql`EXISTS (
            SELECT 1 FROM orchestration_events
              INDEXED BY idx_orchestration_events_agent_stream_sequence
            WHERE application_event_version = 2
              AND aggregate_kind = 'thread'
              AND stream_id = thread.thread_id
              AND event_type IN ('thread.created', 'thread.deleted')
              AND json_extract(payload_json, '$.title') IS NOT ${DELETED_THREAD_TITLE}
          )`,
                sql`json_type(thread.payload_json, '$.guestShare') IS NOT NULL`,
                // An interrupted pass leaves events the purge does not keep.
                sql`EXISTS (
                  SELECT 1 FROM orchestration_events
                    INDEXED BY idx_orchestration_events_agent_stream_sequence
                  WHERE application_event_version = 2
                    AND aggregate_kind = 'thread'
                    AND stream_id = thread.thread_id
                    AND event_type NOT IN ('thread.created', 'thread.deleted')
                    AND NOT (
                      event_type = 'run.updated'
                      AND json_extract(payload_json, '$.status') = 'running'
                    )
                )`,
                // The purge deletes settled requests and clears a failed one's error.
                sql`EXISTS (
                  SELECT 1 FROM orchestration_v2_effect_outbox AS effect
                  WHERE effect.thread_id = thread.thread_id
                    AND effect.effect_type <> 'thread-content.purge'
                    AND (
                      effect.status IN ('succeeded', 'cancelled')
                      OR (effect.status = 'failed' AND effect.last_error IS NOT NULL)
                    )
                )`,
                ...DELETED_THREAD_CONTENT_TABLES.map(
                  (table) =>
                    sql`EXISTS (SELECT 1 FROM ${sql(table)} WHERE thread_id = thread.thread_id)`,
                ),
              ])
        }
    `.pipe(NodeSqliteClient.readOnly);

    const purgeThreads = Effect.fnUntraced(function* (
      deletedThreadId: ThreadId | undefined,
      betweenPages: Effect.Effect<void>,
    ) {
      const threads = yield* purgeableThreads(deletedThreadId);
      const now = DateTime.formatIso(yield* DateTime.now);
      let purged = 0;
      // Each statement commits on its own, so a huge thread never holds the writer for long.
      const drain = Effect.fnUntraced(function* (
        deleteBatch: Effect.Effect<ReadonlyArray<unknown>, SqlError.SqlError>,
      ) {
        for (;;) {
          const rows = yield* deleteBatch;
          purged += rows.length;
          yield* Effect.yieldNow;
          yield* betweenPages;
          if (rows.length < PURGE_BATCH_SIZE) return;
        }
      });
      for (const { thread_id: threadId } of threads) {
        const tombstone = sql`(
          SELECT json_remove(json_set(payload_json, '$.title', ${DELETED_THREAD_TITLE}), '$.guestShare')
          FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}
        )`;
        yield* sql`
          UPDATE orchestration_events
          SET payload_json = json_remove(
            json_set(payload_json, '$.title', ${DELETED_THREAD_TITLE}),
            '$.guestShare'
          )
          WHERE application_event_version = 2
            AND aggregate_kind = 'thread'
            AND stream_id = ${threadId}
            AND event_type IN ('thread.created', 'thread.deleted')
        `;
        // The head row only holds the stream's position, so any other head becomes the tombstone.
        // First, so the head never borrows a prompt from an anchor already deleted below.
        yield* sql`
          UPDATE orchestration_events
          SET event_type = 'thread.deleted', payload_json = ${tombstone}, metadata_json = '{}'
          WHERE sequence = (
              SELECT MAX(sequence) FROM orchestration_events
              WHERE application_event_version = 2
                AND aggregate_kind = 'thread'
                AND stream_id = ${threadId}
            )
            AND event_type NOT IN ('thread.created', 'thread.deleted')
            AND NOT (
              event_type = 'run.updated' AND json_extract(payload_json, '$.status') = 'running'
            )
        `;
        for (const table of DELETED_THREAD_CONTENT_TABLES) {
          yield* drain(sql`
            DELETE FROM ${sql(table)}
            WHERE rowid IN (
              SELECT rowid FROM ${sql(table)} WHERE thread_id = ${threadId} LIMIT ${PURGE_BATCH_SIZE}
            )
            RETURNING 1 AS purged
          `);
        }
        // Keep only what the pruner keeps for any stream: replay still delivers the deletion.
        yield* drain(sql`
          DELETE FROM orchestration_events
          WHERE sequence IN (
            SELECT sequence FROM orchestration_events
              INDEXED BY idx_orchestration_events_agent_stream_sequence
            WHERE application_event_version = 2
              AND aggregate_kind = 'thread'
              AND stream_id = ${threadId}
              AND event_type NOT IN ('thread.created', 'thread.deleted')
              AND NOT (
                event_type = 'run.updated' AND json_extract(payload_json, '$.status') = 'running'
              )
              AND sequence < (
                SELECT MAX(sequence) FROM orchestration_events
                WHERE application_event_version = 2
                  AND aggregate_kind = 'thread'
                  AND stream_id = ${threadId}
              )
            -- Newest first: a row borrowing its prompt goes before the anchor it borrows from.
            ORDER BY sequence DESC
            LIMIT ${PURGE_BATCH_SIZE}
          )
          RETURNING 1 AS purged
        `);
        // Settled requests can hold answers, feedback, and titles. A failed one stays as the
        // marker that keeps a deleted thread's worktree, so only its text goes.
        yield* sql`
          DELETE FROM orchestration_v2_effect_outbox
          WHERE thread_id = ${threadId} AND status IN ('succeeded', 'cancelled')
        `;
        yield* sql`
          UPDATE orchestration_v2_effect_outbox
          SET payload_json = CASE
            WHEN json_type(payload_json, '$.title') = 'text'
              THEN json_set(payload_json, '$.title', ${DELETED_THREAD_TITLE})
            ELSE json_remove(payload_json, '$.feedback', '$.answers')
          END,
            last_error = NULL
          WHERE thread_id = ${threadId} AND status = 'failed'
        `;
        // Its v1 events then go with the imported-history prune, and no import refills it.
        yield* sql`
          UPDATE orchestration_v2_legacy_imports
          SET transcript_imported_at = ${now}
          WHERE thread_id = ${threadId} AND transcript_imported_at IS NULL
        `;
        // Last, because the new title is what marks the purge as done.
        yield* sql`
          UPDATE projection_threads SET title = ${DELETED_THREAD_TITLE} WHERE thread_id = ${threadId}
        `;
        yield* sql`
          UPDATE orchestration_v2_projection_threads
          SET title = ${DELETED_THREAD_TITLE}, payload_json = ${tombstone}
          WHERE thread_id = ${threadId}
        `;
      }
      return purged;
    });

    const purgeDeletedThread: ProjectionMaintenanceV2Shape["purgeDeletedThread"] = (threadId) =>
      purgeThreads(threadId, Effect.void).pipe(
        Effect.withSpan("ProjectionMaintenance.purgeDeletedThread"),
        Effect.mapError(
          (cause) => new ProjectionMaintenanceError({ operation: "purge deleted thread", cause }),
        ),
      );

    // A pass prunes through the mark the previous pass left once that mark is a day old, so
    // at least RETAINED_EVENT_HISTORY of events always remains. Receipts stay for idempotency.
    const pruneEventStore = Effect.fn("ProjectionMaintenance.pruneEventStore")(
      function* (betweenPagesHook?: Effect.Effect<void>) {
        // DatabaseStorage's automatic pass supplies its duty-cycle hook; the offline/manual
        // DatabaseMaintenance caller supplies none. Keep its reads interactive.
        const classifyRead = betweenPagesHook === undefined ? interactiveRead : backgroundRead;
        const betweenPages = betweenPagesHook ?? Effect.void;
        const now = yield* DateTime.now;
        const metadata = yield* sql<MetadataRow>`
          SELECT projection_name, last_sequence, updated_at
          FROM orchestration_v2_projection_metadata
          WHERE projection_name IN (
            'thread-projections', ${EVENT_LOG_PRUNE_MARK}, ${EVENT_LOG_PRUNED_THROUGH}
          )
        `.pipe(NodeSqliteClient.readOnly, classifyRead);
        const read = (name: string) => metadata.find((row) => row.projection_name === name);
        const projected = read("thread-projections")?.last_sequence ?? 0;
        const mark = read(EVENT_LOG_PRUNE_MARK);
        const prunedThrough = read(EVENT_LOG_PRUNED_THROUGH)?.last_sequence ?? 0;
        // Fixed bounds: foreground commits during the pass wait for the next one.
        const bounds = yield* sql<{ readonly sequence: number; readonly receipt_row_id: number }>`
          SELECT
            COALESCE((SELECT MAX(sequence) FROM orchestration_events), 0) AS sequence,
            COALESCE((SELECT MAX(rowid) FROM orchestration_command_receipts), 0) AS receipt_row_id
        `.pipe(NodeSqliteClient.readOnly, classifyRead);
        const head = bounds[0]?.sequence ?? 0;

        let deletedEventCount = 0;
        const markIsDue =
          mark !== undefined &&
          DateTime.toEpochMillis(now) - Date.parse(mark.updated_at) >=
            Duration.toMillis(RETAINED_EVENT_HISTORY);
        if (mark !== undefined && markIsDue) {
          const throughSequence = Math.max(Math.min(mark.last_sequence, projected), prunedThrough);
          if (throughSequence > prunedThrough) {
            // Publish the boundary first so resuming clients take a snapshot, never a partial replay.
            yield* writeMetadata(EVENT_LOG_PRUNED_THROUGH, throughSequence, now);
          }
          deletedEventCount += yield* pruneEvents(
            throughSequence,
            head,
            betweenPages,
            classifyRead,
          );
        }
        if (mark === undefined || markIsDue) yield* writeMetadata(EVENT_LOG_PRUNE_MARK, head, now);

        // Catches threads deleted before purging existed, and any purge a crash interrupted.
        // It runs first so the legacy prune below takes the v1 events of threads it marks.
        const purgedRowCount = yield* purgeThreads(undefined, betweenPages);

        const legacy = yield* pruneImportedLegacyHistory(
          head,
          bounds[0]?.receipt_row_id ?? 0,
          betweenPages,
          classifyRead,
        );
        deletedEventCount += legacy.deletedEventCount;

        const freelistRows = yield* sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`;
        const pageSizeRows = yield* sql<{ readonly page_size: number }>`PRAGMA page_size`;
        const reclaimableBytes =
          (freelistRows[0]?.freelist_count ?? 0) * (pageSizeRows[0]?.page_size ?? 0);

        return {
          deletedEventCount,
          deletedReceiptCount: legacy.deletedReceiptCount,
          purgedRowCount,
          reclaimableBytes,
        };
      },
      Effect.mapError(
        (cause) => new ProjectionMaintenanceError({ operation: "prune event store", cause }),
      ),
    );

    return ProjectionMaintenanceV2.of({ pruneEventStore, purgeDeletedThread });
  }),
);
