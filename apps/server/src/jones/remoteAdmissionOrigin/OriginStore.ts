import { OrchestratorMcpFailure, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { RemoteAdmissionLaunchResult } from "@t3tools/contracts/jones/remoteAdmissionReview";
import type { OriginWakeCommit } from "../threadResultDependencies/DependencyCommit.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

export interface ReturnBinding {
  readonly rootObservationKey: string;
  readonly originThreadId: string;
  readonly returnRouteKey: string;
  readonly waiterGeneration: number;
  readonly waiterRunId: string | null;
}
export interface OriginIntent {
  readonly phase: "prepared" | "held" | "sent_unknown";
  readonly fingerprint: string;
  readonly registered: boolean;
  readonly acknowledged: boolean;
  readonly limits?: import("../../peer/PeerMcpClient.ts").PeerCallLimits;
}
export interface ObservationRow {
  observation_key: string;
  peer_environment_id: string;
  link_generation: string;
  session_id: string;
  client_request_id: string;
  receiver_namespace: string;
  kind: "launch" | "retry" | "recovered_notice";
  root_observation_key: string;
  predecessor_observation_key: string | null;
  admission_id: string | null;
  attempt_id: string | null;
  recovered_revision: number | null;
  return_binding_json: string;
  intent_json: string;
  result_json: string | null;
  state: "intent" | "pending" | "resolved" | "delivered" | "released" | "suppressed" | "closed_unknown";
  created_at: string;
  updated_at: string;
  terminal_at: string | null;
}
export const originConflict = () => new OrchestratorMcpFailure({
  code: "request_conflict", message: "The launch observation binding is unavailable or fenced.",
});
export const returnBinding = (row: ObservationRow): ReturnBinding => JSON.parse(row.return_binding_json);
export const intent = (row: ObservationRow): OriginIntent => JSON.parse(row.intent_json);

/** Same-client SQL operations. Transactional operations are named explicitly. */
export function makeOriginStore(sql: SqlClient.SqlClient) {
  const tables = makeJonesTablePresence(sql);
  const available = tables("jones_remote_origin_observations");
  const get = (key: string) => sql<ObservationRow>`SELECT * FROM jones_remote_origin_observations WHERE observation_key = ${key}`.pipe(Effect.map((rows) => rows[0]));
  const list = () => sql<ObservationRow>`SELECT * FROM jones_remote_origin_observations ORDER BY created_at, observation_key`;
  const prepareInTransaction = Effect.fn("OriginStore.prepareInTransaction")(function* (row: ObservationRow) {
    const old = yield* get(row.observation_key);
    if (old) {
      if (old.peer_environment_id !== row.peer_environment_id || old.session_id !== row.session_id ||
          old.link_generation !== row.link_generation || old.client_request_id !== row.client_request_id ||
          old.return_binding_json !== row.return_binding_json || intent(old).fingerprint !== intent(row).fingerprint)
        return yield* originConflict();
      return old;
    }
    const binding = returnBinding(row);
    const fence = yield* sql<{ generation: number }>`SELECT generation FROM jones_thread_result_fences WHERE origin_environment_id = 'local-waiter' AND operation_id = ${binding.originThreadId}`;
    if (row.kind !== "recovered_notice" && (fence[0]?.generation ?? 0) !== binding.waiterGeneration) return yield* originConflict();
    if (row.kind !== "launch") {
      const roots = yield* sql<{ return_binding_json: string; session_id: string; peer_environment_id: string }>`SELECT * FROM jones_remote_origin_return_anchors WHERE root_observation_key = ${row.root_observation_key}`;
      const root = roots[0];
      if (!root || root.session_id !== row.session_id || root.peer_environment_id !== row.peer_environment_id || root.return_binding_json !== row.return_binding_json) return yield* originConflict();
      if (row.kind === "retry") {
        const predecessor = row.predecessor_observation_key === null ? undefined : yield* get(row.predecessor_observation_key);
        if (!predecessor || predecessor.root_observation_key !== row.root_observation_key || predecessor.session_id !== row.session_id || predecessor.peer_environment_id !== row.peer_environment_id) return yield* originConflict();
      }
    }
    yield* sql`INSERT INTO jones_remote_origin_observations ${sql.insert(row)}`;
    if (row.kind === "launch") yield* sql`INSERT INTO jones_remote_origin_return_anchors
      (root_observation_key,peer_environment_id,session_id,return_binding_json)
      VALUES (${row.root_observation_key},${row.peer_environment_id},${row.session_id},${row.return_binding_json}) ON CONFLICT DO NOTHING`;
    return row;
  });
  const updateIntent = Effect.fn("OriginStore.updateIntent")(function* (key: string, patch: Partial<OriginIntent>, now: string) {
    const row = yield* get(key);
    if (!row) return yield* originConflict();
    yield* sql`UPDATE jones_remote_origin_observations SET intent_json = ${JSON.stringify({ ...intent(row), ...patch })}, updated_at = ${now} WHERE observation_key = ${key}`;
  });
  const pending = Effect.fn("OriginStore.pending")(function* (key: string, phase: "held" | "sent_unknown", now: string) {
    yield* updateIntent(key, { phase }, now);
    yield* sql`UPDATE jones_remote_origin_observations SET state = 'pending' WHERE observation_key = ${key} AND state = 'intent'`;
  });
  const resolve = (key: string, result: RemoteAdmissionLaunchResult, admissionId: string | null, now: string) =>
    sql`UPDATE jones_remote_origin_observations SET state = 'resolved', result_json = ${JSON.stringify(result)}, admission_id = COALESCE(admission_id,${admissionId}), updated_at = ${now}
      WHERE observation_key = ${key} AND state IN ('intent','pending')`;
  const release = (key: string, now: string) => sql`UPDATE jones_remote_origin_observations SET state = 'released', updated_at = ${now}, terminal_at = ${now} WHERE observation_key = ${key} AND state IN ('intent','pending')`;
  const anchor = (key: string) => sql<{ return_binding_json: string; stopped_at: string | null; peer_environment_id: string; session_id: string }>`SELECT * FROM jones_remote_origin_return_anchors WHERE root_observation_key = ${key}`.pipe(Effect.map((rows) => rows[0]));
  const suppressIfStoppedInTransaction = Effect.fn("OriginStore.suppressIfStoppedInTransaction")(function* (row: ObservationRow, now: string) {
    const binding = returnBinding(row);
    const root = yield* anchor(row.root_observation_key);
    const fence = yield* sql<{ generation: number }>`SELECT generation FROM jones_thread_result_fences WHERE origin_environment_id = 'local-waiter' AND operation_id = ${binding.originThreadId}`;
    const live = yield* sql`SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${binding.originThreadId} AND archived_at IS NULL AND deleted_at IS NULL`;
    if (root && root.stopped_at === null && (fence[0]?.generation ?? 0) === binding.waiterGeneration && live.length === 1) return false;
    yield* sql`UPDATE jones_remote_origin_return_anchors SET stopped_at = COALESCE(stopped_at,${now}) WHERE root_observation_key = ${row.root_observation_key}`;
    yield* sql`UPDATE jones_remote_origin_observations SET state = 'suppressed', updated_at = ${now}, terminal_at = ${now} WHERE observation_key = ${row.observation_key} AND state IN ('intent','pending','resolved')`;
    return true;
  });
  return { available, get, list, prepareInTransaction, updateIntent, pending, resolve, release, anchor, suppressIfStoppedInTransaction };
}

/** Called only by DependencyStore.admitWake inside EventSink's message/receipt transaction. */
export const admitOriginWake = Effect.fn("OriginStore.admitWake")(function* (sql: SqlClient.SqlClient, input: OriginWakeCommit) {
  const store = makeOriginStore(sql);
  const row = yield* store.get(input.observationKey);
  if (!row || row.state !== "resolved") return yield* originConflict();
  const binding = returnBinding(row);
  if (binding.originThreadId !== input.waiterThreadId || binding.waiterGeneration !== input.generation ||
      row.link_generation !== input.linkGeneration || row.session_id !== input.sessionId ||
      (yield* store.suppressIfStoppedInTransaction(row, input.committedAt))) return yield* originConflict();
  const accepted = yield* sql`UPDATE jones_remote_origin_observations SET state = 'delivered', updated_at = ${input.committedAt}, terminal_at = ${input.committedAt}
    WHERE observation_key = ${input.observationKey} AND state = 'resolved' RETURNING observation_key`;
  if (accepted.length !== 1) return yield* originConflict();
  // The durable command receipt owns message identity; the Jones row owns delivery identity.
  yield* sql`UPDATE jones_remote_origin_return_anchors SET resolved_at = ${input.committedAt} WHERE root_observation_key = ${row.root_observation_key}`;
  return ThreadId.make(binding.originThreadId);
});
