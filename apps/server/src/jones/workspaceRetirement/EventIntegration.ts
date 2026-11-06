import * as Schema from "effect/Schema";
import { DutyIdentityJson, QueuedHolderJson } from "./DurableCodecs.ts";
import type { RecordedEvent } from "../../orchestration-v2/RecordedTypes.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import { RetirementError } from "./Receipts.ts";
import { dutyIdentity } from "./DutyIdentity.ts";

// Called within EventSink's transaction, before projections change. Read-only native
// binding performs no optional enrichment and acquires no common-store writer lock.
export const events = Effect.fn("WorkspaceRetirement.admitRecordedEvents")(function* (
  sql: SqlClient.SqlClient,
  values: readonly RecordedEvent[],
) {
  const batchPaths = new Map<string, string | null>();
  for (const event of values) {
    if (
      !event.type.startsWith("thread.") &&
      event.type !== "run.created" &&
      event.type !== "run.updated"
    )
      continue;
    const payload = event.payload;
    const nextPath = "worktreePath" in payload ? payload.worktreePath : undefined;
    let prior: { worktreePath: string | null } | null = null;
    if (event.type !== "thread.created") {
      if (batchPaths.has(event.threadId)) prior = { worktreePath: batchPaths.get(event.threadId)! };
      else {
        // Read only the binding inside this same transaction. Do not enter a
        // projection lookup/lineage barrier before the event's actual owner does.
        const rows = yield* sql<{
          worktreePath: string | null;
        }>`SELECT json_extract(payload_json,'$.worktreePath') AS worktreePath FROM orchestration_v2_projection_threads WHERE thread_id=${event.threadId}`;
        prior = rows[0] ?? null;
      }
    }
    const paths = [
      ...new Set([prior?.worktreePath, nextPath].filter((p): p is string => typeof p === "string")),
    ];
    // Binding changes, queue delivery, restore and owner metadata writes all conflict in this same transaction.
    for (const worktree of paths) {
      const retiring =
        yield* sql`SELECT 1 FROM jones_workspace_retirement_state WHERE worktree=${worktree} AND state='retiring'`;
      if (retiring.length)
        return yield* new RetirementError({ reason: "retiring", target: worktree });
    }
    if (event.type === "thread.created" || Object.hasOwn(payload, "worktreePath"))
      batchPaths.set(event.threadId, nextPath ?? null);
    if (event.type === "thread.settled" && prior?.worktreePath) {
      if (event.payload.settledAt === null)
        return yield* new RetirementError({
          reason: "settlement_time_missing",
          target: prior.worktreePath,
        });
      yield* sql`INSERT INTO jones_workspace_retirement_intents (event_id,thread_id,worktree,settled_at) VALUES (${event.id},${event.threadId},${prior.worktreePath},${DateTime.formatIso(event.payload.settledAt)}) ON CONFLICT(event_id) DO NOTHING`;
    }
    if (event.type === "run.created" && prior?.worktreePath) {
      const worktree = prior.worktreePath;
      const identity = yield* Effect.promise(() => dutyIdentity(worktree));
      const claim = `queued-turn:${event.payload.id}`;
      yield* sql`INSERT INTO jones_workspace_duty_claims VALUES (${claim},${prior.worktreePath},${identity.status === "bound" ? identity.incarnation : null},'queued-turn',${event.threadId},${yield* Schema.encodeEffect(QueuedHolderJson)({ runId: event.payload.id, threadId: event.threadId })},${DateTime.formatIso(event.occurredAt)},NULL) ON CONFLICT(claim_id) DO NOTHING`;
      yield* sql`INSERT INTO jones_workspace_duty_identity VALUES (${claim},${yield* Schema.encodeEffect(DutyIdentityJson)(identity)},NULL) ON CONFLICT(claim_id) DO NOTHING`;
    }
    if (
      event.type === "run.updated" &&
      ["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(
        event.payload.status,
      )
    )
      yield* sql`UPDATE jones_workspace_duty_claims SET released_at=${DateTime.formatIso(event.occurredAt)} WHERE claim_id=${`queued-turn:${event.payload.id}`} AND released_at IS NULL`;
  }
});
