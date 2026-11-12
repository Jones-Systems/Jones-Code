import { MessageId, RunId, ThreadId, type OrchestrationV2StoredEvent } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import {
  acceptedStartWitness,
  type ArmedHold,
  type AttemptKey,
  type AttemptScope,
  type HoldKey,
  type HoldReservation,
  type RefusalReason,
} from "./model.ts";

export class HeldDelivery extends Schema.TaggedError<HeldDelivery>()("HeldDelivery", {
  threadId: ThreadId,
}) {}

// Dependency wakes can join certification in the terminal writer's transaction.
// The callback is invoked only after eligible -> refused actually changes a row.
export class CertificationObserver extends Context.Service<
  CertificationObserver,
  {
    readonly certified: (
      input: AttemptKey & {
        readonly scope: AttemptScope;
        readonly reason: RefusalReason;
        readonly settlement: OrchestrationV2StoredEvent;
      },
    ) => Effect.Effect<void, SqlError>;
  }
>()("t3/jones/startRefusal/Store/CertificationObserver") {}

interface HoldRow {
  readonly thread_id: string;
  readonly parent_run_id: string;
  readonly generation: number;
  readonly message_id: string;
  readonly refused_run_id: string;
  readonly source_generation: number;
  readonly state: "held" | "armed" | "consumed" | "void";
  readonly created_sequence: number;
  readonly release_event_id: string | null;
  readonly release_sequence: number | null;
}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const present = makeJonesTablePresence(sql);
  const observer = yield* Effect.serviceOption(CertificationObserver);
  const attemptsPresent = present("jones_start_refusal_attempts");
  const holdsPresent = present("jones_start_refusal_holds");

  const establish = Effect.fn("StartRefusal.establish")(function* (
    key: AttemptKey,
    scope: AttemptScope,
    createdSequence: number,
  ) {
    if (!(yield* attemptsPresent)) return;
    yield* sql`INSERT INTO jones_start_refusal_attempts
      (thread_id, run_id, attempt_id, scope, state, created_sequence)
      VALUES (${key.threadId}, ${key.runId}, ${key.attemptId}, ${scope}, 'eligible', ${createdSequence})
      ON CONFLICT DO NOTHING`;
  });

  // This commit precedes all post-prefix native work, including a later outbox
  // execution of this same attempt. Missing rows deliberately stay unqualified.
  const cross = Effect.fn("StartRefusal.cross")(function* (key: AttemptKey) {
    if (!(yield* attemptsPresent)) return;
    yield* sql`UPDATE jones_start_refusal_attempts SET state = 'crossed'
      WHERE thread_id = ${key.threadId} AND run_id = ${key.runId}
        AND attempt_id = ${key.attemptId} AND state = 'eligible'`;
  });

  const certify = Effect.fn("StartRefusal.certify")(function* (
    key: AttemptKey,
    reason: RefusalReason,
    events: ReadonlyArray<OrchestrationV2StoredEvent>,
    consumer?: CertificationObserver["Service"],
  ) {
    if (!(yield* attemptsPresent)) return;
    const settlement = events.find(
      ({ event }) =>
        event.type === "run.updated" &&
        event.threadId === key.threadId &&
        event.payload.id === key.runId &&
        event.payload.activeAttemptId === key.attemptId &&
        event.payload.status === "failed" &&
        event.payload.startedAt === null,
    );
    if (
      settlement === undefined ||
      !events.some(
        ({ event }) =>
          event.type === "run-attempt.updated" &&
          event.threadId === key.threadId &&
          event.payload.id === key.attemptId &&
          event.payload.runId === key.runId &&
          event.payload.status === "failed" &&
          event.payload.startedAt === null,
      )
    )
      return;
    const changed = yield* sql<{ readonly scope: AttemptScope }>`
      UPDATE jones_start_refusal_attempts
      SET state = 'refused', reason = ${reason}, settlement_event_id = ${settlement.event.id}
      WHERE thread_id = ${key.threadId} AND run_id = ${key.runId}
        AND attempt_id = ${key.attemptId} AND state = 'eligible'
      RETURNING scope`;
    if (changed[0] !== undefined) {
      const input = { ...key, scope: changed[0].scope, reason, settlement };
      if (Option.isSome(observer)) yield* observer.value.certified(input);
      if (consumer !== undefined) yield* consumer.certified(input);
    }
  });

  const classify = Effect.fn("StartRefusal.classify")(function* (threadId: ThreadId, runId: RunId) {
    if (!(yield* attemptsPresent) || !(yield* holdsPresent)) return "ordinary" as const;
    const runs = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
      WHERE run_id = ${runId} AND thread_id = ${threadId} AND status = 'failed'
        AND json_type(payload_json, '$.startedAt') = 'null'`;
    if (runs.length !== 1) return "ordinary" as const;
    const attempts = yield* sql<{
      readonly attempt_id: string;
      readonly root_node_id: string;
      readonly clean: number;
    }>`
      SELECT attempt_id, root_node_id, (status = 'failed'
        AND json_type(payload_json, '$.startedAt') = 'null'
        AND json_type(payload_json, '$.providerTurnId') = 'null'
        AND json_type(payload_json, '$.providerSettlement') = 'null') AS clean FROM orchestration_v2_projection_run_attempts
      WHERE thread_id = ${threadId} AND run_id = ${runId}`;
    if (attempts.length === 0) return "ordinary" as const;
    let unavailable = false;
    for (const attempt of attempts) {
      const proof = yield* sql<{ readonly available: number }>`
        SELECT EXISTS (SELECT 1 FROM orchestration_events AS event
            WHERE event.event_id = proof.settlement_event_id AND event.event_type = 'run.updated'
              AND event.stream_id = ${threadId}
              AND json_extract(event.payload_json, '$.id') = ${runId}
              AND json_extract(event.payload_json, '$.activeAttemptId') = ${attempt.attempt_id}
              AND json_extract(event.payload_json, '$.status') = 'failed'
              AND json_type(event.payload_json, '$.startedAt') = 'null') AS available
        FROM jones_start_refusal_attempts AS proof
        WHERE proof.thread_id = ${threadId} AND proof.run_id = ${runId}
          AND proof.attempt_id = ${attempt.attempt_id} AND proof.state = 'refused'
          AND proof.scope = 'delegated_completion'`;
      if (proof.length !== 1 || attempt.clean !== 1) return "ordinary" as const;
      if (proof[0]!.available !== 1) unavailable = true;
      const turns = yield* sql`SELECT 1 FROM orchestration_v2_projection_provider_turns
        WHERE thread_id = ${threadId}
          AND (run_attempt_id = ${attempt.attempt_id} OR node_id = ${attempt.root_node_id}) LIMIT 1`;
      if (turns.length > 0) return "ordinary" as const;
    }
    // A persistent certificate outlives its settlement event. Missing proof
    // permits neither a successor nor ordinary finalization of unread results.
    return unavailable ? ("unknown" as const) : ("qualified" as const);
  });

  const qualifies = Effect.fn("StartRefusal.qualifies")(function* (
    threadId: ThreadId,
    runId: RunId,
  ) {
    return (yield* classify(threadId, runId)) === "qualified";
  });

  const currentDelivery = Effect.fn("StartRefusal.currentDelivery")(function* (
    key: HoldKey,
    refusedRunId?: RunId,
    replacing?: HoldRow,
  ) {
    const current = yield* sql<{ readonly task_ids: string }>`
      SELECT json_extract(run.payload_json, '$.delegatedCompletion.delivery.taskIds') AS task_ids
      FROM orchestration_v2_projection_runs AS run
      WHERE run.run_id = ${key.parentRunId} AND run.thread_id = ${key.threadId}
        AND json_extract(run.payload_json, '$.delegatedCompletion.disposition') = 'open'
        AND json_extract(run.payload_json, '$.delegatedCompletion.delivery.generation') = ${key.generation}
        AND json_extract(run.payload_json, '$.delegatedCompletion.delivery.messageId') = ${key.messageId}
        AND EXISTS (SELECT 1 FROM orchestration_v2_projection_threads AS thread
          WHERE thread.thread_id = ${key.threadId} AND thread.archived_at IS NULL
            AND thread.deleted_at IS NULL AND json_extract(thread.payload_json, '$.settledAt') IS NULL)`;
    if (current[0] === undefined) return undefined;
    const ids = (yield* sql<{
      readonly id: string;
    }>`SELECT value AS id FROM json_each(${current[0].task_ids})`).map((row) => row.id);
    if (ids.length === 0 || new Set(ids).size !== ids.length) return undefined;
    if (refusedRunId !== undefined) {
      const source = yield* sql<{
        readonly id: string;
        readonly generation: number;
        readonly state: string | null;
      }>`
        SELECT task.value AS id,
          json_extract(message.payload_json, '$.delegatedCompletion.generation') AS generation,
          child.completion_delivery_state AS state
        FROM orchestration_v2_projection_runs AS run
        JOIN orchestration_v2_projection_messages AS message
          ON message.message_id = json_extract(run.payload_json, '$.userMessageId')
        JOIN json_each(json_extract(message.payload_json, '$.delegatedCompletion.taskIds')) AS task
        LEFT JOIN orchestration_v2_projection_subagents AS child
          ON child.subagent_id = task.value AND child.thread_id = ${key.threadId}
            AND child.run_id = ${key.parentRunId} AND child.origin = 'app_owned'
        WHERE run.run_id = ${refusedRunId} AND run.thread_id = ${key.threadId}
          AND message.run_id = ${refusedRunId} AND message.thread_id = ${key.threadId}
          AND json_extract(message.payload_json, '$.delegatedCompletion.parentRunId') = ${key.parentRunId}`;
      // A remainder gets a new key linked to the preceding reservation. Its proof
      // still describes the immutable refused message; only explicit dispositions
      // may subtract obligations. Membership under the old key must stay exact.
      const anchor =
        replacing === undefined
          ? key
          : {
              threadId: ThreadId.make(replacing.thread_id),
              parentRunId: RunId.make(replacing.parent_run_id),
              generation: replacing.generation,
              messageId: MessageId.make(replacing.message_id),
            };
      const chain = yield* sql<{
        readonly size: number;
        readonly source_generation: number | null;
      }>`
        WITH RECURSIVE lineage AS (
          SELECT generation, source_generation FROM jones_start_refusal_holds
          WHERE thread_id = ${anchor.threadId} AND parent_run_id = ${anchor.parentRunId}
            AND generation = ${anchor.generation} AND message_id = ${anchor.messageId}
            AND refused_run_id = ${refusedRunId} AND generation > source_generation
          UNION ALL
          SELECT previous.generation, previous.source_generation FROM jones_start_refusal_holds AS previous
          JOIN lineage ON previous.generation = lineage.source_generation
          WHERE previous.thread_id = ${key.threadId} AND previous.parent_run_id = ${key.parentRunId}
            AND previous.refused_run_id = ${refusedRunId} AND previous.generation > previous.source_generation
        ) SELECT count(*) AS size, min(source_generation) AS source_generation FROM lineage`;
      const removed = Math.max(0, chain[0]!.size - (replacing === undefined ? 1 : 0));
      const inherited = removed > 0;
      // Each link records one explicit task disposition. Counting the immutable
      // links prevents further subtraction under an already reserved key.
      if (
        inherited &&
        (chain[0]!.source_generation !== source[0]?.generation ||
          source.length - ids.length !== removed)
      )
        return undefined;
      if (
        inherited &&
        source.some(
          (row) =>
            row.state !== "claimed" && row.state !== "acknowledged" && row.state !== "disposed",
        )
      )
        return undefined;
      const expected = inherited ? source.filter((row) => row.state === "claimed") : source;
      if (expected.length !== ids.length || expected.some((row, i) => row.id !== ids[i]))
        return undefined;
    }
    for (const id of ids) {
      const task = yield* sql`SELECT 1 FROM orchestration_v2_projection_subagents
        WHERE subagent_id = ${id} AND thread_id = ${key.threadId} AND run_id = ${key.parentRunId}
          AND origin = 'app_owned' AND completion_delivery_state = 'claimed'`;
      if (task.length !== 1) return undefined;
    }
    return ids;
  });

  const validateReservation = Effect.fn("StartRefusal.validateReservation")(function* (
    r: HoldReservation,
  ) {
    if (!(yield* holdsPresent) || !(yield* qualifies(r.threadId, r.refusedRunId))) return false;
    const ids = yield* currentDelivery(
      { ...r, generation: r.source.generation, messageId: r.source.messageId },
      r.refusedRunId,
    );
    if (
      ids === undefined ||
      ids.length !== r.source.taskIds.length ||
      ids.some((id, index) => id !== r.source.taskIds[index]) ||
      ids.length !== r.successor.taskIds.length ||
      ids.some((id, index) => id !== r.successor.taskIds[index])
    )
      return false;
    const parents = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
      WHERE run_id = ${r.parentRunId} AND thread_id = ${r.threadId}
        AND json_extract(payload_json, '$.delegatedCompletion.nextGeneration') = ${r.successor.generation}`;
    const messages = yield* sql`SELECT 1 FROM orchestration_v2_projection_messages
      WHERE message_id = ${r.source.messageId} AND thread_id = ${r.threadId}
        AND run_id = ${r.refusedRunId}
        AND json_extract(payload_json, '$.delegatedCompletion.parentRunId') = ${r.parentRunId}
        AND json_extract(payload_json, '$.delegatedCompletion.generation') = ${r.source.generation}`;
    return parents.length === 1 && messages.length === 1;
  });

  const reserve = Effect.fn("StartRefusal.reserve")(function* (
    r: HoldReservation,
    sequence: number,
  ) {
    yield* sql`INSERT INTO jones_start_refusal_holds
      (thread_id, parent_run_id, generation, message_id, source_generation, refused_run_id, state, created_sequence)
      VALUES (${r.threadId}, ${r.parentRunId}, ${r.successor.generation}, ${r.successor.messageId},
        ${r.source.generation}, ${r.refusedRunId}, 'held', ${sequence})`;
  });

  const blocked = Effect.fn("StartRefusal.blocked")(function* (key: HoldKey) {
    if (!(yield* holdsPresent)) return false;
    const rows = yield* sql`SELECT 1 FROM jones_start_refusal_holds
      WHERE thread_id = ${key.threadId} AND parent_run_id = ${key.parentRunId}
        AND generation = ${key.generation} AND message_id = ${key.messageId}
        AND state IN ('held', 'armed', 'void')`;
    return rows.length > 0;
  });

  const admit = Effect.fn("StartRefusal.admit")(function* (
    events: ReadonlyArray<OrchestrationV2StoredEvent["event"]>,
  ) {
    for (const event of events) {
      if (event.type !== "message.updated" || event.payload.delegatedCompletion === undefined)
        continue;
      if (
        yield* blocked({
          threadId: event.threadId,
          ...event.payload.delegatedCompletion,
          messageId: event.payload.id,
        })
      )
        return yield* new HeldDelivery({ threadId: event.threadId });
    }
  });

  const keyOf = (row: HoldRow): HoldKey => ({
    threadId: ThreadId.make(row.thread_id),
    parentRunId: RunId.make(row.parent_run_id),
    generation: row.generation,
    messageId: MessageId.make(row.message_id),
  });
  const owns = Effect.fn("StartRefusal.owns")(function* (key: HoldKey) {
    if (!(yield* holdsPresent)) return false;
    const rows = yield* sql`SELECT 1 FROM jones_start_refusal_holds
      WHERE thread_id = ${key.threadId} AND parent_run_id = ${key.parentRunId}
        AND generation = ${key.generation} AND message_id = ${key.messageId}`;
    return rows.length === 1;
  });

  const canRekey = Effect.fn("StartRefusal.canRekey")(function* (key: HoldKey) {
    if (!(yield* holdsPresent)) return false;
    const rows = yield* sql<HoldRow>`SELECT * FROM jones_start_refusal_holds
      WHERE thread_id = ${key.threadId} AND parent_run_id = ${key.parentRunId}
        AND generation = ${key.generation} AND message_id = ${key.messageId} AND state <> 'void'`;
    return (
      rows[0] !== undefined &&
      (yield* currentDelivery(key, RunId.make(rows[0].refused_run_id))) !== undefined
    );
  });

  const reserveRemainders = Effect.fn("StartRefusal.reserveRemainders")(function* (
    storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>,
  ) {
    if (!(yield* holdsPresent)) return;
    for (const stored of storedEvents) {
      const event = stored.event;
      if (event.type !== "run.updated") continue;
      const cohort = event.payload.delegatedCompletion;
      const delivery = cohort?.delivery;
      if (
        cohort?.disposition !== "open" ||
        delivery == null ||
        cohort.nextGeneration !== delivery.generation + 1
      )
        continue;
      const previous = yield* sql<HoldRow>`SELECT * FROM jones_start_refusal_holds
        WHERE thread_id = ${event.threadId} AND parent_run_id = ${event.payload.id}
          AND generation = ${delivery.generation - 1} AND message_id <> ${delivery.messageId}
          AND state <> 'void'`;
      const row = previous[0];
      if (previous.length !== 1 || row === undefined) continue;
      const key = {
        threadId: event.threadId,
        parentRunId: event.payload.id,
        generation: delivery.generation,
        messageId: delivery.messageId,
      };
      const source = yield* sql<{ readonly id: string }>`SELECT task.value AS id
        FROM orchestration_v2_projection_runs AS run
        JOIN orchestration_v2_projection_messages AS message
          ON message.message_id = json_extract(run.payload_json, '$.userMessageId')
        JOIN json_each(json_extract(message.payload_json, '$.delegatedCompletion.taskIds')) AS task
        WHERE run.run_id = ${row.refused_run_id} AND run.thread_id = ${event.threadId}`;
      const disposition = storedEvents.some(
        ({ event: changed }) =>
          changed.type === "subagent.updated" &&
          changed.threadId === key.threadId &&
          changed.payload.runId === key.parentRunId &&
          source.some((task) => task.id === changed.payload.id) &&
          !delivery.taskIds.includes(changed.payload.id) &&
          (changed.payload.completionDelivery?.state === "acknowledged" ||
            changed.payload.completionDelivery?.state === "disposed"),
      );
      if (
        !disposition ||
        (yield* currentDelivery(key, RunId.make(row.refused_run_id), row)) === undefined
      )
        continue;
      yield* sql`INSERT INTO jones_start_refusal_holds
        (thread_id, parent_run_id, generation, message_id, source_generation, refused_run_id, state, created_sequence)
        VALUES (${key.threadId}, ${key.parentRunId}, ${key.generation}, ${key.messageId},
          ${row.generation}, ${row.refused_run_id}, 'held', ${storedEvents.at(-1)!.sequence})`;
    }
  });

  const voidInvalid = Effect.fn("StartRefusal.voidInvalid")(function* (threadId: ThreadId) {
    if (!(yield* holdsPresent)) return;
    const rows = yield* sql<HoldRow>`SELECT * FROM jones_start_refusal_holds
      WHERE thread_id = ${threadId} AND state <> 'void'`;
    for (const row of rows) {
      if ((yield* currentDelivery(keyOf(row), RunId.make(row.refused_run_id))) !== undefined)
        continue;
      yield* sql`UPDATE jones_start_refusal_holds SET state = 'void'
        WHERE thread_id = ${threadId} AND parent_run_id = ${row.parent_run_id}
          AND generation = ${row.generation} AND message_id = ${row.message_id}`;
    }
  });

  const arm = Effect.fn("StartRefusal.arm")(function* (stored: OrchestrationV2StoredEvent) {
    const turn = acceptedStartWitness(stored);
    if (turn === undefined || !(yield* holdsPresent)) return;
    const rows = yield* sql<HoldRow>`SELECT * FROM jones_start_refusal_holds
      WHERE thread_id = ${stored.event.threadId} AND state IN ('held', 'armed')
        AND created_sequence < ${stored.sequence}
      ORDER BY created_sequence, parent_run_id`;
    if (rows.length === 0) return;
    const used = yield* sql`SELECT 1 FROM jones_start_refusal_holds
      WHERE thread_id = ${stored.event.threadId} AND
        (release_event_id = ${stored.event.id} OR release_sequence >= ${stored.sequence}) LIMIT 1`;
    if (used.length > 0) return;
    const attempts = yield* sql<{ readonly run_id: string }>`
      SELECT run_id FROM orchestration_v2_projection_run_attempts
      WHERE attempt_id = ${turn.runAttemptId} AND thread_id = ${stored.event.threadId}
        AND provider_thread_id = ${turn.providerThreadId} AND root_node_id = ${turn.nodeId}`;
    const runId = attempts[0]?.run_id;
    if (runId === undefined || stored.event.runId !== runId) return;
    const heldRun = yield* sql`SELECT 1 FROM jones_start_refusal_holds
      WHERE thread_id = ${stored.event.threadId} AND refused_run_id = ${runId} LIMIT 1`;
    if (heldRun.length > 0) return;
    // Indexed by stream/type/sequence. Pruned or missing creation evidence fails closed.
    const births = yield* sql<{
      readonly sequence: number;
    }>`SELECT sequence FROM orchestration_events
      WHERE stream_id = ${stored.event.threadId} AND event_type = 'run-attempt.created'
        AND sequence > ${rows[0]!.created_sequence} AND sequence < ${stored.sequence}
        AND json_extract(payload_json, '$.id') = ${turn.runAttemptId}
        AND json_extract(payload_json, '$.runId') = ${runId} ORDER BY sequence LIMIT 1`;
    const birth = births[0]?.sequence;
    if (birth === undefined) return;
    for (const row of rows) {
      if (birth <= row.created_sequence || row.refused_run_id === runId) continue;
      // The successor is not admitted yet, but exclude its run as well if a
      // stale/external message managed to acquire one before the hold existed.
      const own = yield* sql`SELECT 1 FROM orchestration_v2_projection_messages
        WHERE message_id = ${row.message_id} AND run_id = ${runId}`;
      if (
        own.length > 0 ||
        (yield* currentDelivery(keyOf(row), RunId.make(row.refused_run_id))) === undefined
      )
        continue;
      yield* sql`UPDATE jones_start_refusal_holds
        SET state = 'armed', release_event_id = ${stored.event.id}, release_sequence = ${stored.sequence}
        WHERE thread_id = ${row.thread_id} AND parent_run_id = ${row.parent_run_id}
          AND generation = ${row.generation} AND message_id = ${row.message_id}
          AND state IN ('held', 'armed')`;
      return;
    }
  });

  const events = Effect.fn("StartRefusal.events")(function* (
    storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>,
  ) {
    for (const stored of storedEvents) {
      const event = stored.event;
      if (event.type === "run-attempt.created" && (yield* attemptsPresent)) {
        const messages = yield* sql`SELECT 1 FROM orchestration_v2_projection_messages
          WHERE message_id = (SELECT json_extract(payload_json, '$.userMessageId') FROM orchestration_v2_projection_runs
            WHERE run_id = ${event.payload.runId} AND thread_id = ${event.threadId})
            AND json_type(payload_json, '$.delegatedCompletion') = 'object'`;
        if (messages.length > 0)
          yield* establish(
            { threadId: event.threadId, runId: event.payload.runId, attemptId: event.payload.id },
            "delegated_completion",
            stored.sequence,
          );
      }
      if (event.type === "provider-turn.updated") yield* arm(stored);
    }
    const invalidators = new Set(
      storedEvents.flatMap(({ event }) =>
        (event.type === "run.updated" && event.payload.delegatedCompletion !== undefined) ||
        (event.type === "subagent.updated" && event.payload.completionDelivery !== undefined) ||
        event.type === "thread.archived" ||
        event.type === "thread.deleted" ||
        event.type === "thread.settled"
          ? [event.threadId]
          : [],
      ),
    );
    yield* reserveRemainders(storedEvents);
    for (const threadId of invalidators) yield* voidInvalid(threadId);
  });

  const armedBy = Effect.fn("StartRefusal.armedBy")(function* (stored: OrchestrationV2StoredEvent) {
    if (acceptedStartWitness(stored) === undefined || !(yield* holdsPresent)) return [];
    const rows = yield* sql<HoldRow>`SELECT * FROM jones_start_refusal_holds
      WHERE release_event_id = ${stored.event.id} AND release_sequence = ${stored.sequence} AND state = 'armed'`;
    return rows.map((row): ArmedHold => ({
      ...keyOf(row),
      refusedRunId: RunId.make(row.refused_run_id),
      releaseEventId: row.release_event_id!,
      releaseSequence: row.release_sequence!,
    }));
  });

  const finishArming = Effect.fn("StartRefusal.finishArming")(function* (
    hold: ArmedHold,
    open: boolean,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const valid = (yield* currentDelivery(hold, hold.refusedRunId)) !== undefined;
        const rows = yield* sql`UPDATE jones_start_refusal_holds
        SET state = ${valid ? (open ? "consumed" : "held") : "void"},
          created_sequence = CASE WHEN ${open ? 1 : 0} = 0 THEN ${hold.releaseSequence} ELSE created_sequence END
        WHERE thread_id = ${hold.threadId} AND parent_run_id = ${hold.parentRunId}
          AND generation = ${hold.generation} AND message_id = ${hold.messageId}
          AND state = 'armed' AND release_event_id = ${hold.releaseEventId}
          AND release_sequence = ${hold.releaseSequence}
        RETURNING state`;
        return rows[0]?.state === "consumed";
      }),
    );
  });

  return {
    establish,
    cross,
    certify,
    classify,
    qualifies,
    validateReservation,
    reserve,
    blocked,
    owns,
    canRekey,
    admit,
    events,
    armedBy,
    finishArming,
  };
});

export class StartRefusals extends Context.Service<StartRefusals, Effect.Success<typeof make>>()(
  "t3/jones/startRefusal/Store/StartRefusals",
) {}
export const layer = Layer.effect(StartRefusals, make);
