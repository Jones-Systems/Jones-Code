import { RunAttemptId, RunId, ThreadId, type OrchestrationV2StoredEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { JonesTablePresence } from "../persistence/JonesTablePresence.ts";
import type { CertificationObserver, StartRefusals } from "../startRefusal/Store.ts";
import { acceptedStartWitness } from "../startRefusal/model.ts";
import { changed } from "./ChangeTracking.ts";
import type { WakeCommit } from "./DependencyCommit.ts";

export type WakeHoldState = "held" | "armed" | "consumed" | "void";
export const wakeEligible = (dependency: {
  readonly state: string;
  readonly wakeHold?: WakeHoldState;
}) =>
  dependency.state === "resolved" &&
  (dependency.wakeHold === undefined || dependency.wakeHold === "consumed");

export function makeWakeObligations(sql: SqlClient.SqlClient, tables: JonesTablePresence) {
  const available = tables("jones_peer_wake_holds");
  const holdState = Effect.fn("WakeObligations.holdState")(function* (
    id: string,
    generation: number,
  ) {
    if (!(yield* available)) return undefined;
    return (yield* sql<{ state: WakeHoldState }>`SELECT state FROM jones_peer_wake_holds
      WHERE dependency_id = ${id} AND generation = ${generation}`)[0]?.state;
  });
  const bind = Effect.fn("WakeObligations.bind")(function* (
    input: WakeCommit,
    epoch: number,
    sequence: number,
    establish?: StartRefusals["Service"]["establish"],
  ) {
    if (!(yield* available) || establish === undefined) return;
    const rows = yield* sql<{ run_id: string; attempt_id: string }>`SELECT r.run_id, a.attempt_id
      FROM orchestration_v2_projection_messages m
      JOIN orchestration_v2_projection_runs r ON r.run_id = m.run_id AND r.thread_id = m.thread_id
      JOIN orchestration_v2_projection_run_attempts a ON a.run_id = r.run_id AND a.thread_id = r.thread_id
      WHERE m.message_id = ${input.messageId} AND m.thread_id = ${input.waiterThreadId}
        AND json_type(m.payload_json, '$.notification') = 'object'
        AND a.attempt_id = json_extract(r.payload_json, '$.activeAttemptId')
        AND json_extract(a.payload_json, '$.attemptOrdinal') = 1
        AND (SELECT COUNT(*) FROM orchestration_v2_projection_run_attempts other WHERE other.run_id = r.run_id) = 1`;
    const row = rows[0];
    if (row === undefined) return;
    yield* sql`INSERT INTO jones_peer_wake_bindings
      (dependency_id,generation,thread_id,message_id,run_id,attempt_id,waiter_epoch,admitted_sequence)
      VALUES (${input.dependencyId},${input.generation},${input.waiterThreadId},${input.messageId},${row.run_id},${row.attempt_id},${epoch},${sequence})
      ON CONFLICT DO NOTHING`;
    yield* establish(
      {
        threadId: input.waiterThreadId,
        runId: RunId.make(row.run_id),
        attemptId: RunAttemptId.make(row.attempt_id),
      },
      "dependency_wake",
      sequence,
    );
  });
  const certified: CertificationObserver["Service"]["certified"] = Effect.fn(
    "WakeObligations.certified",
  )(function* (input) {
    if (input.scope !== "dependency_wake" || !(yield* available)) return;
    const rows = yield* sql<{ dependency_id: string; generation: number; waiter_epoch: number }>`
      SELECT b.dependency_id,b.generation,b.waiter_epoch FROM jones_peer_wake_bindings b
      JOIN jones_thread_result_dependencies d ON d.dependency_id = b.dependency_id AND d.generation = b.generation
      JOIN orchestration_v2_projection_threads t ON t.thread_id = b.thread_id
      JOIN orchestration_v2_projection_messages m ON m.message_id = b.message_id AND m.thread_id = b.thread_id AND m.run_id = b.run_id
      JOIN orchestration_v2_projection_runs r ON r.run_id = b.run_id AND r.thread_id = b.thread_id
      JOIN orchestration_v2_projection_run_attempts a ON a.attempt_id = b.attempt_id AND a.run_id = b.run_id AND a.thread_id = b.thread_id
      WHERE b.thread_id = ${input.threadId} AND b.run_id = ${input.runId} AND b.attempt_id = ${input.attemptId}
        AND d.waiter_thread_id = b.thread_id AND d.state = 'delivered' AND d.wake_message_id = b.message_id
        AND t.archived_at IS NULL AND t.deleted_at IS NULL
        AND b.waiter_epoch = COALESCE((SELECT generation FROM jones_thread_result_fences WHERE origin_environment_id = 'local-waiter' AND operation_id = b.thread_id),0)
        AND r.status = 'failed' AND json_type(r.payload_json,'$.startedAt') = 'null'
        AND json_extract(r.payload_json,'$.activeAttemptId') = b.attempt_id
        AND json_extract(r.payload_json,'$.userMessageId') = b.message_id
        AND json_type(m.payload_json,'$.notification') = 'object'
        AND a.status = 'failed' AND json_type(a.payload_json,'$.startedAt') = 'null'
        AND json_type(a.payload_json,'$.providerTurnId') = 'null' AND json_type(a.payload_json,'$.providerSettlement') = 'null'
        AND (SELECT COUNT(*) FROM orchestration_v2_projection_run_attempts other WHERE other.run_id = b.run_id) = 1
        AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_provider_turns p WHERE p.thread_id = b.thread_id AND (p.run_attempt_id = b.attempt_id OR p.node_id = a.root_node_id))`;
    for (const row of rows) {
      yield* sql`UPDATE jones_thread_result_dependencies SET state = 'resolved', generation = generation + 1,
        changed_sequence = ${input.settlement.sequence} WHERE dependency_id = ${row.dependency_id} AND generation = ${row.generation} AND state = 'delivered'`;
      yield* sql`INSERT INTO jones_peer_wake_holds (dependency_id,generation,refused_run_id,waiter_epoch,state,created_sequence)
        VALUES (${row.dependency_id},${row.generation + 1},${input.runId},${row.waiter_epoch},'held',${input.settlement.sequence})`;
      yield* changed(input.threadId);
    }
  });
  const current = (id: string, generation: number) => sql<{ thread_id: string }>`
    SELECT d.waiter_thread_id AS thread_id FROM jones_thread_result_dependencies d
    JOIN jones_peer_wake_holds h ON h.dependency_id = d.dependency_id AND h.generation = d.generation
    JOIN orchestration_v2_projection_threads t ON t.thread_id = d.waiter_thread_id
    WHERE d.dependency_id = ${id} AND d.generation = ${generation} AND d.state = 'resolved'
      AND t.archived_at IS NULL AND t.deleted_at IS NULL
      AND h.waiter_epoch = COALESCE((SELECT generation FROM jones_thread_result_fences WHERE origin_environment_id = 'local-waiter' AND operation_id = d.waiter_thread_id),0)`;
  const events = Effect.fn("WakeObligations.events")(function* (
    events: ReadonlyArray<OrchestrationV2StoredEvent>,
  ) {
    if (!(yield* available)) return;
    const childHolds = yield* tables("jones_start_refusal_holds");
    for (const stored of events) {
      const turn = acceptedStartWitness(stored);
      if (turn === undefined) continue;
      const used =
        yield* sql`SELECT 1 FROM jones_peer_wake_holds WHERE release_event_id = ${stored.event.id}
        ${childHolds ? sql`UNION ALL SELECT 1 FROM jones_start_refusal_holds WHERE release_event_id = ${stored.event.id}` : sql``}`;
      if (used.length > 0) continue;
      const births = yield* sql<{ run_id: string; sequence: number }>`SELECT a.run_id,e.sequence
        FROM orchestration_v2_projection_run_attempts a JOIN orchestration_events e ON e.stream_id = a.thread_id
        AND e.event_type = 'run-attempt.created' AND json_extract(e.payload_json,'$.id') = a.attempt_id
        AND json_extract(e.payload_json,'$.runId') = a.run_id
        WHERE a.attempt_id = ${turn.runAttemptId} AND a.thread_id = ${stored.event.threadId}
          AND a.provider_thread_id = ${turn.providerThreadId} AND a.root_node_id = ${turn.nodeId}
          AND a.run_id = ${stored.event.runId ?? null} AND e.sequence < ${stored.sequence}
          AND NOT EXISTS (SELECT 1 FROM jones_peer_wake_holds WHERE refused_run_id = a.run_id)
          ${childHolds ? sql`AND NOT EXISTS (SELECT 1 FROM jones_start_refusal_holds WHERE refused_run_id = a.run_id)` : sql``}`;
      const birth = births[0];
      if (birth === undefined) continue;
      const alreadyWitnessed = yield* sql`SELECT 1 FROM jones_peer_wake_holds h
        JOIN jones_thread_result_dependencies d ON d.dependency_id = h.dependency_id
        WHERE d.waiter_thread_id = ${stored.event.threadId} AND h.release_sequence >= ${birth.sequence}
        ${childHolds ? sql`UNION ALL SELECT 1 FROM jones_start_refusal_holds WHERE thread_id = ${stored.event.threadId} AND release_sequence >= ${birth.sequence}` : sql``}`;
      if (alreadyWitnessed.length > 0) continue;
      const holds = yield* sql<{
        dependency_id: string;
        generation: number;
      }>`SELECT h.dependency_id,h.generation FROM jones_peer_wake_holds h
        JOIN jones_thread_result_dependencies d ON d.dependency_id = h.dependency_id AND d.generation = h.generation
        WHERE d.waiter_thread_id = ${stored.event.threadId} AND h.state IN ('held','armed') AND h.created_sequence < ${birth.sequence}
          AND h.refused_run_id != ${birth.run_id} ORDER BY h.created_sequence,h.dependency_id`;
      for (const hold of holds) {
        if ((yield* current(hold.dependency_id, hold.generation)).length !== 1) continue;
        yield* sql`UPDATE jones_peer_wake_holds SET state = 'armed',release_event_id = ${stored.event.id},release_sequence = ${stored.sequence}
          WHERE dependency_id = ${hold.dependency_id} AND generation = ${hold.generation} AND state IN ('held','armed')`;
        yield* changed(stored.event.threadId);
        break;
      }
    }
  });
  const armedBy = Effect.fn("WakeObligations.armedBy")(function* (eventId: string) {
    if (!(yield* available)) return [];
    return yield* sql<{
      dependency_id: string;
      generation: number;
      release_sequence: number;
    }>`SELECT dependency_id,generation,release_sequence
      FROM jones_peer_wake_holds WHERE release_event_id = ${eventId} AND state = 'armed'`;
  });
  const finishArming = Effect.fn("WakeObligations.finishArming")(function* (
    hold: { dependency_id: string; generation: number; release_sequence: number },
    open: boolean,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const valid = yield* current(hold.dependency_id, hold.generation);
        const rows =
          yield* sql`UPDATE jones_peer_wake_holds SET state = ${valid.length === 1 ? (open ? "consumed" : "held") : "void"},
        created_sequence = CASE WHEN ${open ? 1 : 0} = 0 THEN ${hold.release_sequence} ELSE created_sequence END
        WHERE dependency_id = ${hold.dependency_id} AND generation = ${hold.generation}
          AND state = 'armed' AND release_sequence = ${hold.release_sequence} RETURNING state`;
        if (valid[0]) yield* changed(ThreadId.make(valid[0].thread_id));
        return rows[0]?.state === "consumed";
      }),
    );
  });
  const voidReleased = Effect.fn("WakeObligations.voidReleased")(function* (threadId: ThreadId) {
    if (!(yield* available)) return;
    yield* sql`UPDATE jones_peer_wake_holds SET state = 'void' WHERE dependency_id IN
      (SELECT dependency_id FROM jones_thread_result_dependencies WHERE waiter_thread_id = ${threadId} AND state = 'released')`;
  });
  return { available, holdState, bind, certified, events, armedBy, finishArming, voidReleased };
}
