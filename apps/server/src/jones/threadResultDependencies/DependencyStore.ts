import { changed } from "./ChangeTracking.ts";
import {
  MessageId, ThreadId, RunId, OrchestratorMcpFailure,
  type ThreadResultDependency, type ThreadResultRequest, type ThreadResultOutcome, type ThreadResultObservation,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type { JonesTablePresence } from "../persistence/JonesTablePresence.ts";
import type { McpThreadInvocationScope } from "../../mcp/McpInvocationContext.ts";
import type { OpenRequest, WakeCommit } from "./DependencyCommit.ts";
import { forThread } from "./DependencyDuties.ts";

export type IntentScope = Pick<McpThreadInvocationScope, "environmentId" | "requestNamespace" | "thread">;
export type Dependency = ThreadResultDependency & {
  readonly waiterThreadId: ThreadId;
  readonly waiterRunId: RunId | null;
  readonly generation: number;
  readonly scope: IntentScope | null;
  readonly forwardedRequestKey: string | null;
  readonly wakeMessageId: MessageId | null;
  readonly notified: boolean;
};
interface DependencyRow {
  dependency_id: string; waiter_thread_id: string; waiter_run_id: string | null;
  target_environment_id: string | null; target_thread_id: string; target_label: string;
  operation_id: string; request_digest: string | null; forwarded_request_key: string | null; origin_scope_json: string | null;
  request_id: string | null; message_id: string | null; state: Dependency["state"];
  generation: number; outcome: ThreadResultOutcome | null; result_text: string | null;
  observation_json: string; wake_message_id: string | null; notified: number;
}
interface RequestRow {
  request_id: string; target_thread_id: string; message_id: string; command_id: string;
  origin_kind: "local" | "linked"; origin_environment_id: string; origin_thread_id: string; origin_label: string;
  operation_id: string; state: ThreadResultRequest["state"]; result_text: string | null;
  resolved_by_run_id: string | null; requester_released_at: string | null; revision: number;
}
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const dependency = (row: DependencyRow): Dependency => ({
  dependencyId: row.dependency_id, waiterThreadId: ThreadId.make(row.waiter_thread_id),
  waiterRunId: row.waiter_run_id === null ? null : RunId.make(row.waiter_run_id),
  operationId: row.operation_id, targetThreadId: ThreadId.make(row.target_thread_id),
  targetEnvironmentId: row.target_environment_id, targetLabel: row.target_label,
  requestId: row.request_id, messageId: row.message_id === null ? null : MessageId.make(row.message_id),
  state: row.state, generation: row.generation, outcome: row.outcome, result: row.result_text,
  observation: decodeJson(row.observation_json) as ThreadResultObservation,
  scope: row.origin_scope_json === null ? null : decodeJson(row.origin_scope_json) as IntentScope,
  forwardedRequestKey: row.forwarded_request_key,
  wakeMessageId: row.wake_message_id === null ? null : MessageId.make(row.wake_message_id), notified: row.notified === 1,
});
const request = (row: RequestRow): ThreadResultRequest => ({
  requestId: row.request_id, operationId: row.operation_id, targetThreadId: ThreadId.make(row.target_thread_id),
  messageId: MessageId.make(row.message_id), originEnvironmentId: row.origin_environment_id,
  originThreadId: ThreadId.make(row.origin_thread_id), originLabel: row.origin_label, state: row.state,
  result: row.result_text, resolvedByRunId: row.resolved_by_run_id === null ? null : RunId.make(row.resolved_by_run_id),
  requesterReleased: row.requester_released_at !== null,
});
const conflict = () => new OrchestratorMcpFailure({ code: "request_conflict", message: "The result request is fenced, released, or conflicts with an accepted answer." });
const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));

/** SQL facts only. Callers own transactions and publish changes strictly after commit. */
export function makeDependencyStore(sql: SqlClient.SqlClient, tables: JonesTablePresence) {
  const available = tables("jones_thread_result_dependencies");
  // Shell cursors include project events. The legacy V2 ledger is not the
  // application high-water mark and cannot qualify quiet reconnect refreshes.
  const sequence = sql<{ sequence: number }>`SELECT COALESCE(MAX(sequence), 0) AS sequence
    FROM orchestration_events INDEXED BY idx_orchestration_events_application_high_water
    WHERE aggregate_kind = 'project' OR (application_event_version = 2 AND aggregate_kind = 'thread')`
    .pipe(Effect.map((rows) => rows[0]!.sequence));
  const waiterGeneration = (threadId: ThreadId) => sql<{ generation: number }>`SELECT generation FROM jones_thread_result_fences WHERE origin_environment_id = 'local-waiter' AND operation_id = ${threadId}`.pipe(Effect.map((rows) => rows[0]?.generation ?? 0));
  const dependencies = Effect.fn("DependencyStore.dependencies")(function* (threadId?: ThreadId) {
    if (!(yield* available)) return [];
    return (yield* (threadId === undefined
      ? sql<DependencyRow>`SELECT d.* FROM jones_thread_result_dependencies d WHERE state != 'released' AND EXISTS (SELECT 1 FROM orchestration_v2_projection_threads t WHERE t.thread_id = d.waiter_thread_id)`
      : sql<DependencyRow>`SELECT d.* FROM jones_thread_result_dependencies d WHERE waiter_thread_id = ${threadId} AND EXISTS (SELECT 1 FROM orchestration_v2_projection_threads t WHERE t.thread_id = d.waiter_thread_id)`)).map(dependency);
  });
  const requests = Effect.fn("DependencyStore.requests")(function* (threadId: ThreadId) {
    if (!(yield* available)) return [];
    return (yield* sql<RequestRow>`SELECT r.* FROM jones_thread_result_requests r WHERE target_thread_id = ${threadId} AND EXISTS (SELECT 1 FROM orchestration_v2_projection_threads t WHERE t.thread_id = r.target_thread_id)`).map(request);
  });
  const get = (id: string) => sql<DependencyRow>`SELECT * FROM jones_thread_result_dependencies WHERE dependency_id = ${id}`.pipe(Effect.map((rows) => rows[0] ? dependency(rows[0]) : undefined));
  const requestByOperation = (origin: string, operation: string) => sql<RequestRow>`SELECT * FROM jones_thread_result_requests WHERE origin_environment_id = ${origin} AND operation_id = ${operation}`.pipe(Effect.map((rows) => rows[0] ? request(rows[0]) : undefined));
  const duties = Effect.fn("DependencyStore.duties")(function* (threadId: ThreadId) {
    const outgoing = yield* dependencies(threadId);
    const owed = yield* requests(threadId);
    const wakeRuns = new Map<string, { status: string }>();
    for (const item of outgoing) {
      if (item.state !== "delivered" || item.wakeMessageId === null) continue;
      const rows = yield* sql<{ status: string }>`SELECT r.status FROM orchestration_v2_projection_messages m JOIN orchestration_v2_projection_runs r ON r.run_id = m.run_id WHERE m.message_id = ${item.wakeMessageId}`;
      if (rows[0]) wakeRuns.set(item.wakeMessageId, rows[0]);
    }
    return forThread(outgoing, owed, wakeRuns);
  });
  const changedThreads = Effect.fn("DependencyStore.changedThreads")(function* (afterSequence: number) {
    if (!(yield* available)) return [];
    const rows = yield* sql<{ thread_id: string }>`SELECT DISTINCT waiter_thread_id AS thread_id FROM jones_thread_result_dependencies WHERE changed_sequence >= ${afterSequence}
      UNION SELECT DISTINCT target_thread_id AS thread_id FROM jones_thread_result_requests WHERE changed_sequence >= ${afterSequence}`;
    return rows.map((row) => ThreadId.make(row.thread_id));
  });
  const fenced = (origin: string, operation: string) => sql`SELECT 1 FROM jones_thread_result_fences WHERE origin_environment_id = ${origin} AND operation_id = ${operation}`.pipe(Effect.map((rows) => rows.length > 0));
  const fence = Effect.fn("DependencyStore.fence")(function* (origin: string, operation: string) {
    yield* sql`INSERT OR IGNORE INTO jones_thread_result_fences(origin_environment_id, operation_id, changed_sequence, created_at) VALUES (${origin}, ${operation}, ${yield* sequence}, ${yield* now})`;
  });
  const open = Effect.fn("DependencyStore.open")(function* (input: OpenRequest) {
    const existing = yield* requestByOperation(input.originEnvironmentId, input.operationId);
    if (existing !== undefined) {
      if (existing.messageId !== input.messageId || existing.targetThreadId !== input.targetThreadId) return yield* conflict();
      return;
    }
    if (yield* fenced(input.originEnvironmentId, input.operationId)) return yield* conflict();
    if (input.localDependency) {
      if ((yield* waiterGeneration(input.originThreadId)) !== input.localDependency.waiterGeneration) return yield* conflict();
      const live = yield* sql`SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${input.originThreadId} AND deleted_at IS NULL AND archived_at IS NULL`;
      if (live.length !== 1) return yield* conflict();
    }
    const message = yield* sql`SELECT 1 FROM orchestration_v2_projection_messages WHERE message_id = ${input.messageId} AND thread_id = ${input.targetThreadId}`;
    if (message.length === 0) return yield* conflict();
    const stamp = yield* sequence;
    const time = yield* now;
    yield* changed(input.targetThreadId);
    yield* sql`INSERT INTO jones_thread_result_requests
      (request_id, target_thread_id, message_id, command_id, origin_kind, origin_environment_id, origin_thread_id, origin_label, operation_id, state, changed_sequence, created_at, updated_at)
      VALUES (${input.requestId}, ${input.targetThreadId}, ${input.messageId}, ${input.commandId}, ${input.originKind}, ${input.originEnvironmentId}, ${input.originThreadId}, ${input.originLabel}, ${input.operationId}, 'pending', ${stamp}, ${time}, ${time})`;
    if (input.localDependency) {
      const local = input.localDependency;
      yield* changed(input.originThreadId);
      // Local open and the receiver's dispatch are one transaction. No invisible accepted send.
      yield* sql`INSERT INTO jones_thread_result_dependencies
        (dependency_id, waiter_thread_id, waiter_run_id, target_thread_id, target_label, operation_id, request_id, message_id, state, observation_json, changed_sequence, created_at, updated_at)
        VALUES (${local.dependencyId}, ${input.originThreadId}, ${local.waiterRunId}, ${input.targetThreadId}, ${local.targetLabel}, ${input.operationId}, ${input.requestId}, ${input.messageId}, 'awaiting', ${encodeJson("receiver_working")}, ${stamp}, ${time}, ${time})`;
    }
  });
  const prepare = Effect.fn("DependencyStore.prepare")(function* (input: {
    dependencyId: string; operationId: string; waiterThreadId: ThreadId; waiterRunId: RunId | null;
    targetEnvironmentId: string; targetThreadId: ThreadId; targetLabel: string; scope: IntentScope; forwardedRequestKey: string; requestDigest: string; waiterGeneration: number;
  }) {
    const previous = yield* get(input.dependencyId);
    if (previous) {
      const digest = (yield* sql<{ request_digest: string | null }>`SELECT request_digest FROM jones_thread_result_dependencies WHERE dependency_id = ${input.dependencyId}`)[0]?.request_digest;
      if (previous.targetThreadId !== input.targetThreadId || previous.targetEnvironmentId !== input.targetEnvironmentId || digest !== input.requestDigest) return yield* conflict();
      return previous;
    }
    if ((yield* waiterGeneration(input.waiterThreadId)) !== input.waiterGeneration) return yield* conflict();
    const live = yield* sql`SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${input.waiterThreadId} AND deleted_at IS NULL AND archived_at IS NULL`;
    if (live.length === 0) return yield* conflict();
    const time = yield* now;
    yield* sql`INSERT INTO jones_thread_result_dependencies
      (dependency_id, waiter_thread_id, waiter_run_id, target_environment_id, target_thread_id, target_label, operation_id, forwarded_request_key, request_digest, origin_scope_json, state, observation_json, changed_sequence, created_at, updated_at)
      VALUES (${input.dependencyId}, ${input.waiterThreadId}, ${input.waiterRunId}, ${input.targetEnvironmentId}, ${input.targetThreadId}, ${input.targetLabel}, ${input.operationId}, ${input.forwardedRequestKey}, ${input.requestDigest}, ${encodeJson(input.scope)}, 'sending', ${encodeJson("delivery_unknown")}, ${yield* sequence}, ${time}, ${time})`;
    yield* changed(input.waiterThreadId);
    return (yield* get(input.dependencyId))!;
  });
  const observe = Effect.fn("DependencyStore.observe")(function* (id: string, generation: number, observation: ThreadResultObservation, binding?: ThreadResultRequest) {
    if (binding !== undefined) {
      const current = yield* get(id);
      if (!current || current.generation !== generation || !["sending", "awaiting"].includes(current.state)) return;
      if (binding.operationId !== current.operationId || binding.targetThreadId !== current.targetThreadId || binding.originThreadId !== current.waiterThreadId ||
        (current.scope !== null && binding.originEnvironmentId !== current.scope.environmentId) ||
        (current.requestId !== null && current.requestId !== binding.requestId) || (current.messageId !== null && current.messageId !== binding.messageId))
        return yield* conflict();
    }
    const rows = yield* sql<{ waiter_thread_id: string }>`UPDATE jones_thread_result_dependencies SET
      state = CASE WHEN ${binding?.requestId ?? null} IS NOT NULL OR ${observation} IN ('admission_held','admission_dispatching') THEN 'awaiting' ELSE state END,
      request_id = COALESCE(${binding?.requestId ?? null}, request_id), message_id = COALESCE(${binding?.messageId ?? null}, message_id),
      observation_json = ${encodeJson(observation)}, changed_sequence = ${yield* sequence}, updated_at = ${yield* now}
      WHERE dependency_id = ${id} AND generation = ${generation} AND state IN ('sending','awaiting')
      AND (observation_json != ${encodeJson(observation)} OR (${binding?.requestId ?? null} IS NOT NULL AND request_id IS NOT ${binding?.requestId ?? null})) RETURNING waiter_thread_id`;
    for (const row of rows) yield* changed(ThreadId.make(row.waiter_thread_id));
  });
  const resolve = Effect.fn("DependencyStore.resolve")(function* (id: string, generation: number, outcome: ThreadResultOutcome, result: string, silent = false) {
    const rows = yield* sql<{ waiter_thread_id: string }>`UPDATE jones_thread_result_dependencies SET state = ${silent ? "released" : "resolved"}, outcome = ${outcome}, result_text = ${result.slice(0, 16000)}, changed_sequence = ${yield* sequence}, updated_at = ${yield* now}
      WHERE dependency_id = ${id} AND generation = ${generation} AND state IN ('sending','awaiting') RETURNING waiter_thread_id`;
    for (const row of rows) yield* changed(ThreadId.make(row.waiter_thread_id));
  });
  const release = Effect.fn("DependencyStore.release")(function* (threadId: ThreadId, id?: string) {
    if (!(yield* available)) return false;
    const stamp = yield* sequence;
    const time = yield* now;
    if (id === undefined) yield* sql`INSERT INTO jones_thread_result_fences(origin_environment_id, operation_id, changed_sequence, created_at, generation)
      VALUES ('local-waiter', ${threadId}, ${stamp}, ${time}, 1) ON CONFLICT(origin_environment_id, operation_id) DO UPDATE SET generation = generation + 1, changed_sequence = excluded.changed_sequence`;
    const rows = yield* sql<{ request_id: string | null }>`UPDATE jones_thread_result_dependencies SET state = 'released', generation = generation + 1, outcome = 'released', changed_sequence = ${stamp}, updated_at = ${time}
      WHERE waiter_thread_id = ${threadId} AND (${id ?? null} IS NULL OR dependency_id = ${id ?? null}) AND state IN ('sending','awaiting','resolved') RETURNING request_id`;
    if (rows.length > 0) yield* changed(threadId);
    for (const row of rows) if (row.request_id !== null) {
      const requests = yield* sql<{ target_thread_id: string }>`UPDATE jones_thread_result_requests SET requester_released_at = ${time}, revision = revision + 1, changed_sequence = ${stamp}, updated_at = ${time} WHERE request_id = ${row.request_id} AND origin_kind = 'local' AND requester_released_at IS NULL RETURNING target_thread_id`;
      for (const request of requests) yield* changed(ThreadId.make(request.target_thread_id));
    }
    return rows.length > 0;
  });
  const answer = Effect.fn("DependencyStore.answer")(function* (threadId: ThreadId, requestId: string, state: "fulfilled" | "declined", text: string, runId: RunId | null) {
    const rows = yield* sql<RequestRow>`SELECT * FROM jones_thread_result_requests WHERE request_id = ${requestId} AND target_thread_id = ${threadId}`;
    const row = rows[0];
    if (!row) return yield* new OrchestratorMcpFailure({ code: "capability_denied", message: "Only the receiving thread may answer this result request." });
    if (row.state !== "pending") {
      if (row.state !== state || row.result_text !== text) return yield* conflict();
      return { request: request(row), requester: row.origin_kind === "linked" ? "linked" as const : row.requester_released_at ? "released" as const : "waiting" as const };
    }
    yield* sql`UPDATE jones_thread_result_requests SET state = ${state}, result_text = ${text}, resolved_by_run_id = ${runId}, revision = revision + 1, changed_sequence = ${yield* sequence}, updated_at = ${yield* now} WHERE request_id = ${requestId} AND state = 'pending'`;
    yield* changed(threadId);
    const local = yield* sql<DependencyRow>`SELECT * FROM jones_thread_result_dependencies WHERE request_id = ${requestId} AND state = 'awaiting'`;
    for (const item of local) yield* resolve(item.dependency_id, item.generation, state, text);
    return { request: (yield* requestByOperation(row.origin_environment_id, row.operation_id))!, requester: row.origin_kind === "linked" ? "linked" as const : row.requester_released_at ? "released" as const : "waiting" as const };
  });
  const disposeRequests = Effect.fn("DependencyStore.disposeRequests")(function* (threadId: ThreadId, outcome: "dropped" | "withdrawn", messageId?: string) {
    if (!(yield* available)) return;
    const rows = yield* sql<RequestRow>`UPDATE jones_thread_result_requests SET state = ${outcome}, result_text = ${outcome === "withdrawn" ? "The queued request was withdrawn before delivery." : "The receiver disposed of the request without an explicit answer."}, revision = revision + 1, changed_sequence = ${yield* sequence}, updated_at = ${yield* now}
      WHERE target_thread_id = ${threadId} AND state = 'pending' AND (${messageId ?? null} IS NULL OR message_id = ${messageId ?? null}) RETURNING *`;
    if (rows.length > 0) yield* changed(threadId);
    for (const row of rows) {
      const locals = yield* sql<DependencyRow>`SELECT * FROM jones_thread_result_dependencies WHERE request_id = ${row.request_id} AND state = 'awaiting'`;
      for (const local of locals) yield* resolve(local.dependency_id, local.generation, outcome, row.result_text!);
    }
  });
  const disposeRunRequests = Effect.fn("DependencyStore.disposeRunRequests")(function* (threadId: ThreadId, runId: RunId, outcome: "dropped" | "withdrawn") {
    if (!(yield* available)) return;
    const messages = yield* sql<{ message_id: string }>`SELECT request.message_id FROM jones_thread_result_requests request
      JOIN orchestration_v2_projection_messages message ON message.message_id = request.message_id
      WHERE request.target_thread_id = ${threadId} AND request.state = 'pending' AND message.run_id = ${runId}`;
    for (const message of messages) yield* disposeRequests(threadId, outcome, message.message_id);
  });
  const admitWake = Effect.fn("DependencyStore.admitWake")(function* (input: WakeCommit) {
    const live = yield* sql`SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${input.waiterThreadId} AND archived_at IS NULL AND deleted_at IS NULL`;
    if (live.length === 0) return yield* conflict();
    const accepted = yield* sql`UPDATE jones_thread_result_dependencies SET state = 'delivered', wake_message_id = ${input.messageId}, notified = 1, changed_sequence = ${yield* sequence}, updated_at = ${yield* now}
      WHERE dependency_id = ${input.dependencyId} AND waiter_thread_id = ${input.waiterThreadId} AND generation = ${input.generation} AND state = 'resolved' RETURNING dependency_id`;
    if (accepted.length !== 1) return yield* conflict();
    yield* changed(input.waiterThreadId);
  });
  const prune = Effect.gen(function* () {
    if (!(yield* available)) return;
    const cutoff = (yield* sequence) - 1000;
    // Remote answers/fences have no delivery acknowledgment and must survive offline senders.
    // Only fully consumed local facts can be retired beyond the shell replay window.
    yield* sql`DELETE FROM jones_thread_result_requests WHERE origin_kind = 'local' AND state != 'pending' AND changed_sequence < ${cutoff}
      AND NOT EXISTS (SELECT 1 FROM jones_thread_result_dependencies d WHERE d.request_id = jones_thread_result_requests.request_id AND d.state IN ('sending','awaiting','resolved'))`;
    yield* sql`DELETE FROM jones_thread_result_dependencies WHERE changed_sequence < ${cutoff} AND (state = 'released' OR (state = 'delivered' AND EXISTS (
      SELECT 1 FROM orchestration_v2_projection_messages m JOIN orchestration_v2_projection_runs r ON r.run_id = m.run_id
      WHERE m.message_id = jones_thread_result_dependencies.wake_message_id AND r.status IN ('completed','failed','cancelled','interrupted','rolled_back')
    )))`;
  });
  return { available, sequence, waiterGeneration, prune, dependencies, requests, get, duties, changedThreads, fenced, fence, open, prepare, observe, resolve, release, answer, disposeRequests, disposeRunRequests, admitWake, requestByOperation };
}
