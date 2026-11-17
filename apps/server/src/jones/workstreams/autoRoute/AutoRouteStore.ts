import type { ThreadId } from "@t3tools/contracts";
import type * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { makeJonesTablePresence } from "../../persistence/JonesTablePresence.ts";
import type {
  AutoRouteOutcome,
  AutoRouteState,
  CandidateStats,
  ClassifierReply,
  Correction,
} from "./policy.ts";
import { AUTO_ROUTE_BOUNDS } from "./policy.ts";

const AUTO_ROUTE_TABLE = "jones_workstream_auto_route_decisions";

export interface DecisionKey {
  readonly threadId: ThreadId;
  readonly commandId: string;
}

export interface AutoRouteDecisionView {
  readonly state: AutoRouteState;
  readonly code: string | null;
  readonly workstreamId: string | null;
  readonly decidedAt: string | null;
}

export type BeginResult = "inserted" | "duplicate" | "thread_missing";

// Main conversations only: no subagents, side chats or threads delegated from another environment.
const MAIN_CONVERSATION = `json_extract(t.payload_json, '$.lineage.relationshipToParent') IS NOT 'subagent'
  AND json_extract(t.payload_json, '$.lineage.sideChat') IS NOT 1
  AND json_extract(t.payload_json, '$.delegatedFrom') IS NULL`;

/**
 * The decision store, built on the caller's SQL client like `threadTitle/store.ts`.
 * It is not a service, so the Workstreams service and the worker share it without a layer cycle.
 * Rows hold codes, IDs and timestamps only: never prompt text, titles or model output.
 */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const present = makeJonesTablePresence(sql);
  const available = present(AUTO_ROUTE_TABLE);

  const begin = Effect.fn("AutoRouteStore.begin")(function* (input: {
    readonly key: DecisionKey;
    readonly messageId: string;
    readonly origin: string;
    readonly now: string;
  }) {
    const { threadId, commandId } = input.key;
    const existing =
      yield* sql`SELECT 1 FROM jones_workstream_auto_route_decisions WHERE thread_id=${threadId} AND command_id=${commandId}`;
    if (existing.length > 0) return "duplicate" as BeginResult;
    // No foreign key: the upstream thread row is verified in the same statement.
    yield* sql`INSERT OR IGNORE INTO jones_workstream_auto_route_decisions
      (thread_id, command_id, message_id, origin, state, created_at)
      SELECT ${threadId}, ${commandId}, ${input.messageId}, ${input.origin}, 'pending', ${input.now}
      WHERE EXISTS (SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id=${threadId})`;
    const written =
      yield* sql`SELECT 1 FROM jones_workstream_auto_route_decisions WHERE thread_id=${threadId} AND command_id=${commandId}`;
    return (written.length > 0 ? "inserted" : "thread_missing") as BeginResult;
  });

  const markModelRequested = Effect.fn("AutoRouteStore.markModelRequested")(function* (
    key: DecisionKey,
    input: {
      readonly instanceId: string;
      readonly model: string;
      readonly candidateCount: number;
    },
  ) {
    yield* sql`UPDATE jones_workstream_auto_route_decisions
      SET requested_instance_id=${input.instanceId}, requested_model=${input.model},
        candidate_count=${input.candidateCount}
      WHERE thread_id=${key.threadId} AND command_id=${key.commandId}`;
  });

  const markAssigning = Effect.fn("AutoRouteStore.markAssigning")(function* (
    key: DecisionKey,
    input: {
      readonly workstreamId: string;
      readonly revision: number;
      readonly confidence?: ClassifierReply["confidence"] | undefined;
      readonly basis?: ClassifierReply["basis"] | undefined;
    },
  ) {
    yield* sql`UPDATE jones_workstream_auto_route_decisions
      SET state='assigning', code=NULL, workstream_id=${input.workstreamId},
        assignment_revision=${input.revision}, confidence=${input.confidence ?? null},
        basis=${input.basis ?? null}
      WHERE thread_id=${key.threadId} AND command_id=${key.commandId}`;
  });

  const finalize = Effect.fn("AutoRouteStore.finalize")(function* (
    key: DecisionKey,
    outcome: AutoRouteOutcome & {
      readonly confidence?: ClassifierReply["confidence"] | undefined;
      readonly basis?: ClassifierReply["basis"] | undefined;
      readonly candidateCount?: number | undefined;
    },
    now: string,
  ) {
    const code = outcome.state === "assigned" ? null : outcome.code;
    yield* sql`UPDATE jones_workstream_auto_route_decisions
      SET state=${outcome.state}, code=${code}, decided_at=${now},
        confidence=COALESCE(${outcome.confidence ?? null}, confidence),
        basis=COALESCE(${outcome.basis ?? null}, basis),
        candidate_count=COALESCE(${outcome.candidateCount ?? null}, candidate_count)
      WHERE thread_id=${key.threadId} AND command_id=${key.commandId}`;
  });

  const modelAttempts = Effect.fn("AutoRouteStore.modelAttempts")(function* (key: DecisionKey) {
    const rows = yield* sql<{ count: number }>`SELECT COUNT(*) AS count
      FROM jones_workstream_auto_route_decisions
      WHERE thread_id=${key.threadId} AND command_id<>${key.commandId}
        AND requested_instance_id IS NOT NULL`;
    return Number(rows[0]?.count ?? 0);
  });

  const latest = Effect.fn("AutoRouteStore.latest")(function* (threadId: ThreadId) {
    if (!(yield* available)) return null;
    const rows = yield* sql<AutoRouteDecisionView>`SELECT state, code,
        workstream_id AS workstreamId, decided_at AS decidedAt
      FROM jones_workstream_auto_route_decisions WHERE thread_id=${threadId}
      ORDER BY created_at DESC, rowid DESC LIMIT 1`;
    return rows[0] ?? null;
  });

  /** Automatic assignments still in force: the thread holds the exact workstream and revision written. */
  const currentAutoAssignments = Effect.fn("AutoRouteStore.currentAutoAssignments")(function* () {
    if (!(yield* available)) return undefined;
    return yield* sql<P.PersonalWorkstreamAutoAssignment>`SELECT d.thread_id AS threadId,
        d.workstream_id AS workstreamId, d.assignment_revision AS revision,
        COALESCE(d.decided_at, d.created_at) AS decidedAt
      FROM jones_workstream_auto_route_decisions d
      JOIN jones_personal_workstream_assignments a ON a.thread_id=d.thread_id
      JOIN orchestration_v2_projection_threads t ON t.thread_id=d.thread_id
      WHERE d.state IN ('assigning','assigned') AND t.deleted_at IS NULL
        AND a.workstream_id=d.workstream_id AND a.revision=d.assignment_revision
      ORDER BY d.thread_id`;
  });

  /** Automatic assignments the owner later changed, newest first. Deletion of the group is not a correction. */
  const corrections = Effect.fn("AutoRouteStore.corrections")(function* (
    excludeThreadId: ThreadId,
    limit: number,
  ) {
    const rows = yield* sql<{
      threadTitle: string;
      automaticName: string;
      ownerName: string | null;
    }>`SELECT t.title AS threadTitle, auto.name AS automaticName, owner.name AS ownerName
      FROM jones_workstream_auto_route_decisions d
      JOIN jones_personal_workstream_assignments a ON a.thread_id=d.thread_id
      JOIN orchestration_v2_projection_threads t ON t.thread_id=d.thread_id
      JOIN jones_personal_workstreams auto ON auto.id=d.workstream_id
      LEFT JOIN jones_personal_workstreams owner ON owner.id=a.workstream_id
      WHERE d.state='assigned' AND d.thread_id<>${excludeThreadId} AND t.deleted_at IS NULL
        AND a.revision>d.assignment_revision
        AND (a.workstream_id IS NULL OR a.workstream_id<>d.workstream_id)
        AND NOT (a.workstream_id IS NULL AND auto.deleted_at IS NOT NULL)
      ORDER BY a.updated_at DESC LIMIT ${limit}`;
    return rows as readonly Correction[];
  });

  /** Member counts and recent main-conversation titles per Workstream, excluding the target thread. */
  const candidateStats = Effect.fn("AutoRouteStore.candidateStats")(function* (input: {
    readonly threadId: ThreadId;
    readonly projectId: string | null;
  }) {
    const rows = yield* sql.unsafe<{
      workstreamId: string;
      title: string;
      projectId: string;
    }>(
      `SELECT a.workstream_id AS workstreamId, t.title AS title, t.project_id AS projectId
      FROM jones_personal_workstream_assignments a
      JOIN orchestration_v2_projection_threads t ON t.thread_id=a.thread_id
      WHERE a.workstream_id IS NOT NULL AND t.deleted_at IS NULL AND a.thread_id<>?
        AND ${MAIN_CONVERSATION}
      ORDER BY t.updated_at DESC, t.thread_id`,
      [input.threadId],
    );
    const stats = new Map<
      string,
      { memberCount: number; projectMemberCount: number; memberTitles: string[] }
    >();
    for (const row of rows) {
      const entry = stats.get(row.workstreamId) ?? {
        memberCount: 0,
        projectMemberCount: 0,
        memberTitles: [],
      };
      entry.memberCount += 1;
      if (row.projectId === input.projectId) entry.projectMemberCount += 1;
      if (entry.memberTitles.length < AUTO_ROUTE_BOUNDS.memberTitles)
        entry.memberTitles.push(row.title);
      stats.set(row.workstreamId, entry);
    }
    return stats as ReadonlyMap<string, CandidateStats>;
  });

  const projectName = Effect.fn("AutoRouteStore.projectName")(function* (projectId: string) {
    const rows = yield* sql<{
      title: string;
    }>`SELECT title FROM projection_projects WHERE project_id=${projectId} AND deleted_at IS NULL`;
    return rows[0]?.title ?? null;
  });

  /**
   * Startup recovery. Pending rows are not replayed. An assigning row becomes assigned only when
   * the thread holds exactly the assignment it wrote.
   */
  const recoverInterrupted = Effect.fn("AutoRouteStore.recoverInterrupted")(function* (
    now: string,
  ) {
    if (!(yield* available)) return { interrupted: 0, reconciled: 0 };
    const reconciled = yield* sql<{
      thread_id: string;
    }>`UPDATE jones_workstream_auto_route_decisions
      SET state='assigned', code=NULL, decided_at=${now}
      WHERE state='assigning' AND EXISTS (
        SELECT 1 FROM jones_personal_workstream_assignments a
        WHERE a.thread_id=jones_workstream_auto_route_decisions.thread_id
          AND a.workstream_id=jones_workstream_auto_route_decisions.workstream_id
          AND a.revision=jones_workstream_auto_route_decisions.assignment_revision)
      RETURNING thread_id`;
    const interrupted = yield* sql<{
      thread_id: string;
    }>`UPDATE jones_workstream_auto_route_decisions
      SET state='failed', code='interrupted', decided_at=${now}
      WHERE state IN ('pending','assigning')
      RETURNING thread_id`;
    return { interrupted: interrupted.length, reconciled: reconciled.length };
  });

  return {
    available,
    begin,
    markModelRequested,
    markAssigning,
    finalize,
    modelAttempts,
    latest,
    currentAutoAssignments,
    corrections,
    candidateStats,
    projectName,
    recoverInterrupted,
  };
});

export type AutoRouteStore = Effect.Success<typeof make>;
