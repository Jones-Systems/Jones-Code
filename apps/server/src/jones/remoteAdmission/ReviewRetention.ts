import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";

// Only Jones detail is pruned here. Upstream receipts, durable restrictions,
// predecessor fences, unresolved results and unacknowledged notices are untouched.
export const makeReviewRetention = (
  sql: SqlClient.SqlClient,
  invalidate: Effect.Effect<void> = Effect.void,
) => {
  const prune = Effect.fn("ReviewRetention.prune")(function* (now: string) {
    const cutoff = new Date(Date.parse(now) - 30 * 86400000).toISOString();
    // A restored grant still needs its predecessor's verified invocation and
    // command fingerprint. Retain the entire ancestry while any descendant is
    // pending, live, or inside its own terminal retention window.
    const grants = yield* sql<{ grant_id: string }>`WITH RECURSIVE retained(grant_id) AS (
      SELECT grant_id FROM jones_remote_admission_grants
        WHERE state='pending' OR (state='active' AND expires_at >= ${cutoff})
          OR (state='revoked' AND (revoked_at IS NULL OR revoked_at >= ${cutoff}))
      UNION
      SELECT r.predecessor_grant_id FROM jones_remote_admission_grant_restores r
        JOIN retained ON retained.grant_id=r.successor_grant_id
    ) SELECT grant_id FROM jones_remote_admission_grants
      WHERE ((state='revoked' AND revoked_at < ${cutoff}) OR (state='active' AND expires_at < ${cutoff}))
        AND grant_id NOT IN (SELECT grant_id FROM retained)`;
    for (const grant of grants) {
      yield* sql`DELETE FROM jones_remote_admission_grant_restores WHERE predecessor_grant_id=${grant.grant_id}`;
      yield* sql`DELETE FROM jones_remote_admission_grants WHERE grant_id=${grant.grant_id}`;
    }
    yield* sql`DELETE FROM jones_remote_admission_blocked_events WHERE last_at < ${cutoff}`;
    yield* sql`DELETE FROM jones_remote_admission_blocked_summaries WHERE last_at < ${cutoff}`;
    yield* sql`DELETE FROM jones_remote_admission_blocked_event_keys WHERE last_at < ${cutoff}`;
    yield* sql`DELETE FROM jones_remote_admission_attempts WHERE terminal_at < ${cutoff} AND state IN ('completed','refused','expired')`;
    yield* sql`INSERT OR IGNORE INTO jones_remote_admission_notice_tombstones
      (receiver_environment_id,admission_id,recovered_revision,session_id,evidence_digest,disposition)
      SELECT receiver_environment_id,admission_id,recovered_revision,session_id,evidence_digest,state
      FROM jones_remote_admission_recovered_notices WHERE acknowledged_at < ${cutoff}
      AND state IN ('delivered','suppressed','closed_unknown')`;
    yield* sql`DELETE FROM jones_remote_admission_recovered_notices WHERE acknowledged_at < ${cutoff}
      AND state IN ('delivered','suppressed','closed_unknown') AND EXISTS (
        SELECT 1 FROM jones_remote_admission_notice_tombstones t
        WHERE t.receiver_environment_id=jones_remote_admission_recovered_notices.receiver_environment_id
          AND t.admission_id=jones_remote_admission_recovered_notices.admission_id
          AND t.recovered_revision=jones_remote_admission_recovered_notices.recovered_revision
          AND t.session_id=jones_remote_admission_recovered_notices.session_id
          AND t.evidence_digest=jones_remote_admission_recovered_notices.evidence_digest
          AND t.disposition=jones_remote_admission_recovered_notices.state)`;
    // Invalidates retained cursors even when an anchor happened to survive pruning.
    yield* sql`UPDATE jones_remote_admission_review_state SET revision=revision+1,prune_revision=revision+1 WHERE singleton=1`;
    yield* sql`DELETE FROM jones_remote_admission_changes WHERE created_at < ${cutoff}`;
    yield* invalidate;
  });
  return { prune: (now: string) => sql.withTransaction(prune(now)) };
};
