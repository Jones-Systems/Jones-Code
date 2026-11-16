// @effect-diagnostics nodeBuiltinImport:off -- Opaque, receiver-local review cursors.
import {
  REMOTE_ADMISSION_DEFAULT_PAGE_SIZE,
  REMOTE_ADMISSION_MAX_PAGE_SIZE,
} from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as NodeBuffer from "node:buffer";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type {
  RemoteAdmissionReviewCounts,
  RemoteAdmissionReviewItem,
  RemoteAdmissionReviewView,
  RemoteAdmissionGrant,
} from "@t3tools/contracts/jones/remoteAdmissionReview";
import type { Grant } from "./ReceiverAccess.ts";
import type { SqlError } from "effect/sql/SqlError";
import type * as SqlClient from "effect/sql/SqlClient";

export class ReviewHistoryUnavailable extends Schema.TaggedError<ReviewHistoryUnavailable>()(
  "ReviewHistoryUnavailable",
  {},
) {}
export type ReviewView = Exclude<RemoteAdmissionReviewView, "access">;
export type ReviewCounts = RemoteAdmissionReviewCounts;
interface ProjectionRow {
  readonly id: string;
  readonly created_at: string;
}
interface Cursor {
  readonly view: RemoteAdmissionReviewView;
  readonly authorizationScope: string;
  readonly revision: number;
  readonly createdAt: string;
  readonly id: string;
}
const CursorSchema = Schema.Struct({
  view: Schema.Literals(["current", "blocked", "inflight", "unresolved", "done", "access"]),
  authorizationScope: Schema.String,
  revision: Schema.Number,
  createdAt: Schema.String,
  id: Schema.String,
});

export const makeReviewProjection = (sql: SqlClient.SqlClient) => {
  const revision = Effect.fn("ReviewProjection.revision")(function* () {
    return (yield* sql<{ revision: number; prune_revision: number }>`SELECT revision,prune_revision
      FROM jones_remote_admission_review_state WHERE singleton=1`)[0]!;
  });
  const page = Effect.fn("ReviewProjection.page")(function* (input: {
    view: ReviewView;
    authorizationScope: string;
    cursor?: string;
    limit?: number;
    now: string;
    validLeaseIds: (grants: ReadonlyArray<Grant>) => Effect.Effect<ReadonlyArray<string>, SqlError>;
    projectItem: (id: string) => Effect.Effect<RemoteAdmissionReviewItem, ReviewHistoryUnavailable>;
    // Reads receiver-owned shell projections in this same transaction; never assumes liveness.
    liveDestinations: (
      ids: ReadonlyArray<string>,
    ) => Effect.Effect<ReadonlyArray<string>, SqlError>;
  }) {
    const snapshot = yield* revision();
    let cursor: Cursor | undefined;
    if (input.cursor) {
      cursor = yield* Effect.try({
        try: () =>
          Schema.decodeUnknownSync(CursorSchema)(
            JSON.parse(NodeBuffer.Buffer.from(input.cursor!, "base64url").toString("utf8")),
          ),
        catch: () => new ReviewHistoryUnavailable({}),
      });
      if (
        cursor.view !== input.view ||
        cursor.authorizationScope !== input.authorizationScope ||
        cursor.revision !== snapshot.revision ||
        cursor.revision < snapshot.prune_revision
      )
        return yield* new ReviewHistoryUnavailable({});
      const anchor =
        yield* sql`SELECT id FROM jones_remote_admissions WHERE id=${cursor.id} AND created_at=${cursor.createdAt}`;
      if (!anchor[0]) return yield* new ReviewHistoryUnavailable({});
    }
    const limit = Math.max(1, Math.min(REMOTE_ADMISSION_MAX_PAGE_SIZE, Math.floor(input.limit ?? REMOTE_ADMISSION_DEFAULT_PAGE_SIZE)));
    if (!Number.isFinite(limit)) return yield* new ReviewHistoryUnavailable({});
    const view = input.view;
    const rows = yield* sql<ProjectionRow>`SELECT a.id,a.created_at FROM jones_remote_admissions a
      LEFT JOIN jones_remote_admission_blocked_marks b ON b.admission_id=a.id
      LEFT JOIN jones_remote_admission_extensions e ON e.admission_id=a.id
      WHERE ((${view}='current' AND ((a.status='held' AND b.admission_id IS NULL) OR e.outcome='failure'))
      OR (${view}='blocked' AND a.status='held' AND b.admission_id IS NOT NULL)
      OR (${view}='inflight' AND a.status IN ('passed','approved','dispatching'))
      OR (${view}='unresolved' AND e.outcome='unknown' AND NOT EXISTS
        (SELECT 1 FROM jones_remote_admission_recovered_results r WHERE r.admission_id=a.id AND json_extract(r.result_json,'$.outcome') IN ('created','refused','not_created')))
      OR (${view}='done' AND (a.status IN ('denied','cancelled') OR (a.status='dispatched' AND
        (e.outcome IN ('success','failure') OR EXISTS (SELECT 1 FROM jones_remote_admission_recovered_results r WHERE r.admission_id=a.id AND json_extract(r.result_json,'$.outcome') IN ('created','refused','not_created')))))))
      AND (${cursor?.id ?? null} IS NULL OR a.created_at < ${cursor?.createdAt ?? ""}
        OR (a.created_at=${cursor?.createdAt ?? ""} AND a.id < ${cursor?.id ?? ""}))
      ORDER BY a.created_at DESC,a.id DESC LIMIT ${limit + 1}`;
    const totals = (yield* sql<{
      currentHeldAdmissions: number;
      blockedHeldAdmissions: number;
      inFlightAdmissions: number;
      failedAdmissions: number;
      unresolvedAdmissions: number;
    }>`SELECT
      COALESCE(SUM(a.status='held' AND b.admission_id IS NULL),0) AS currentHeldAdmissions,
      COALESCE(SUM(a.status='held' AND b.admission_id IS NOT NULL),0) AS blockedHeldAdmissions,
      COALESCE(SUM(a.status IN ('passed','approved','dispatching')),0) AS inFlightAdmissions,
      COALESCE(SUM(e.outcome='failure'),0) AS failedAdmissions,
      COALESCE(SUM(e.outcome='unknown' AND NOT EXISTS (SELECT 1 FROM jones_remote_admission_recovered_results r WHERE r.admission_id=a.id AND json_extract(r.result_json,'$.outcome') IN ('created','refused','not_created'))),0) AS unresolvedAdmissions
      FROM jones_remote_admissions a LEFT JOIN jones_remote_admission_blocked_marks b ON b.admission_id=a.id
      LEFT JOIN jones_remote_admission_extensions e ON e.admission_id=a.id`)[0]!;
    const candidates = yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants
      WHERE state='active' AND expires_at>${input.now} AND managed_scope_id IS NOT NULL`;
    const valid = new Set(yield* input.validLeaseIds(candidates));
    const access = candidates.filter((g) => valid.has(g.grant_id));
    const live = new Set(
      yield* input.liveDestinations(
        access.flatMap((g) => (g.destination_thread_id ? [g.destination_thread_id] : [])),
      ),
    );
    const counts: ReviewCounts = {
      ...totals,
      heldAdmissions: totals.currentHeldAdmissions + totals.blockedHeldAdmissions,
      activeAccessScopes: new Set(access.map((g) => g.managed_scope_id)).size,
      activeThreads: new Set(
        access
          .filter((g) => g.destination_thread_id !== null && live.has(g.destination_thread_id))
          .map((g) => g.destination_thread_id),
      ).size,
    };
    const items = rows.slice(0, limit);
    const last = items.at(-1);
    const nextCursor =
      rows.length > limit && last
        ? NodeBuffer.Buffer.from(
            JSON.stringify({
              view,
              authorizationScope: input.authorizationScope,
              revision: snapshot.revision,
              createdAt: last.created_at,
              id: last.id,
            } satisfies Cursor),
          ).toString("base64url")
        : null;
    const doneCount = (yield* sql<{ n: number }>`SELECT COUNT(*) n FROM jones_remote_admissions a
      LEFT JOIN jones_remote_admission_extensions e ON e.admission_id=a.id
      WHERE a.status IN ('denied','cancelled') OR (a.status='dispatched' AND (e.outcome IN ('success','failure')
      OR EXISTS (SELECT 1 FROM jones_remote_admission_recovered_results r WHERE r.admission_id=a.id
        AND json_extract(r.result_json,'$.outcome') IN ('created','refused','not_created'))))`)[0]!
      .n;
    const retainedRecordCount =
      view === "done"
        ? doneCount
        : view === "current"
          ? counts.currentHeldAdmissions + counts.failedAdmissions
          : view === "blocked"
            ? counts.blockedHeldAdmissions
            : view === "inflight"
              ? counts.inFlightAdmissions
              : counts.unresolvedAdmissions;
    return {
      view,
      snapshotRevision: String(snapshot.revision),
      retainedRecordCount,
      counts,
      items: yield* Effect.forEach(items, (row) => input.projectItem(row.id)),
      nextCursor,
    };
  });
  const changes = Effect.fn("ReviewProjection.changes")(function* (after: number) {
    const snapshot = yield* revision();
    if (after < snapshot.prune_revision || after > snapshot.revision)
      return yield* new ReviewHistoryUnavailable({});
    const items = yield* sql<{
      revision: number;
      kind: string;
      entity_id: string;
      created_at: string;
    }>`
      SELECT * FROM jones_remote_admission_changes WHERE revision>${after} ORDER BY revision LIMIT 100`;
    return {
      revision: snapshot.revision,
      items,
      next: items.at(-1)?.revision ?? snapshot.revision,
    };
  });
  const blockedDiagnostics = Effect.fn("ReviewProjection.blockedDiagnostics")(function* () {
    const events = yield* sql<{
      event_id: string;
      admission_id: string;
      managed_scope_id: string;
      reason: string;
      first_at: string;
      last_at: string;
      repeat_count: number;
    }>`SELECT * FROM jones_remote_admission_blocked_events ORDER BY last_at DESC,event_id DESC`;
    const summaries = yield* sql<{
      managed_scope_id: string;
      reason: string;
      logical_events: number;
      repeat_arrivals: number;
      first_at: string;
      last_at: string;
    }>`SELECT * FROM jones_remote_admission_blocked_summaries ORDER BY last_at DESC,managed_scope_id DESC`;
    return {
      events,
      summaries,
      counts: {
        logicalEvents: events.length + summaries.reduce((n, s) => n + s.logical_events, 0),
        repeatArrivals:
          events.reduce((n, e) => n + e.repeat_count, 0) +
          summaries.reduce((n, s) => n + s.repeat_arrivals, 0),
        summaryRows: summaries.length,
      },
    };
  });
  const accessPage = Effect.fn("ReviewProjection.accessPage")(function* (input: {
    authorizationScope: string;
    cursor?: string;
    limit?: number;
    now: string;
    projectGrant: (row: Grant, now: string) => Effect.Effect<RemoteAdmissionGrant, SqlError>;
  }) {
    const snapshot = yield* revision();
    const cursor = input.cursor
      ? yield* Effect.try({
          try: () =>
            Schema.decodeUnknownSync(CursorSchema)(
              JSON.parse(NodeBuffer.Buffer.from(input.cursor!, "base64url").toString("utf8")),
            ),
          catch: () => new ReviewHistoryUnavailable({}),
        })
      : undefined;
    if (
      cursor &&
      (cursor.view !== "access" ||
        cursor.authorizationScope !== input.authorizationScope ||
        cursor.revision !== snapshot.revision ||
        cursor.revision < snapshot.prune_revision)
    )
      return yield* new ReviewHistoryUnavailable({});
    if (
      cursor &&
      !(yield* sql`SELECT grant_id FROM jones_remote_admission_grants WHERE grant_id=${cursor.id} AND created_at=${cursor.createdAt}`)[0]
    )
      return yield* new ReviewHistoryUnavailable({});
    const limit = Math.max(1, Math.min(REMOTE_ADMISSION_MAX_PAGE_SIZE, Math.floor(input.limit ?? REMOTE_ADMISSION_DEFAULT_PAGE_SIZE)));
    if (!Number.isFinite(limit)) return yield* new ReviewHistoryUnavailable({});
    const rows = yield* sql<Grant>`SELECT * FROM jones_remote_admission_grants
      WHERE (${cursor?.id ?? null} IS NULL OR created_at<${cursor?.createdAt ?? ""}
        OR (created_at=${cursor?.createdAt ?? ""} AND grant_id<${cursor?.id ?? ""})) ORDER BY created_at DESC,grant_id DESC LIMIT ${limit + 1}`;
    const items = rows.slice(0, limit);
    const last = items.at(-1);
    const nextCursor =
      rows.length > limit && last
        ? NodeBuffer.Buffer.from(
            JSON.stringify({
              view: "access",
              authorizationScope: input.authorizationScope,
              revision: snapshot.revision,
              createdAt: last.created_at,
              id: last.grant_id,
            } satisfies Cursor),
          ).toString("base64url")
        : null;
    return {
      snapshotRevision: String(snapshot.revision),
      grants: yield* Effect.forEach(items, (g) => input.projectGrant(g, input.now)),
      nextCursor,
      serverNow: input.now,
    };
  });
  return {
    accessPage: (input: Parameters<typeof accessPage>[0]) => sql.withTransaction(accessPage(input)),
    blockedDiagnostics: () => sql.withTransaction(blockedDiagnostics()),
    page: (input: Parameters<typeof page>[0]) => sql.withTransaction(page(input)),
    changes: (after: number) => sql.withTransaction(changes(after)),
    revision,
  };
};
