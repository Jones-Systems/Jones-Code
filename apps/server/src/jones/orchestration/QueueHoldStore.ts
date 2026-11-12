import type {
  OrchestrationV2DomainEvent,
  OrchestrationV2Run,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as SqlClient from "effect/sql/SqlClient";
import type { JonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { isQueueHoldReason, type QueueHold, type QueueHoldReason } from "./QueueHold.ts";

export interface QueueHoldRecord extends QueueHold {
  readonly runId: RunId;
  readonly threadId: ThreadId;
}

/** A hold its writer names explicitly, for a system write that carries no command type. */
export interface QueueHoldIntent {
  readonly reason: QueueHoldReason;
  readonly runIds: ReadonlyArray<RunId>;
}

/** A provider failure holds the thread's queue; a run already held keeps its reason and time. */
export const providerFailureQueueHold = (
  runs: ReadonlyArray<OrchestrationV2Run>,
): QueueHoldIntent => ({
  reason: "provider_failure",
  runIds: runs
    .filter((run) => run.status === "queued" && run.queueHeld !== true)
    .map((run) => run.id),
});

/** What the accepting write knows about its origin. */
export interface QueueHoldOrigin {
  readonly commandType?: string;
  readonly queueHold?: QueueHoldIntent | undefined;
}

interface Row {
  readonly run_id: string;
  readonly reason: string;
  readonly held_at: string;
}

/**
 * Sparse hold metadata in `jones_queue_holds`, keyed by upstream run ID. Readers start from the
 * upstream run and treat a missing table or row as a hold recorded before reasons existed.
 */
export const makeQueueHoldStore = (sql: SqlClient.SqlClient, presence: JonesTablePresence) => {
  const available = presence("jones_queue_holds");
  const record = Effect.fn("QueueHoldStore.record")(function* (
    holds: ReadonlyArray<QueueHoldRecord>,
  ) {
    if (holds.length === 0 || !(yield* available)) return;
    for (const hold of holds)
      yield* sql`INSERT INTO jones_queue_holds (run_id, thread_id, reason, held_at)
        VALUES (${hold.runId}, ${hold.threadId}, ${hold.reason}, ${DateTime.formatIso(hold.heldAt)})
        ON CONFLICT(run_id) DO UPDATE SET
          thread_id = excluded.thread_id,
          reason = excluded.reason,
          held_at = excluded.held_at`;
  });
  const get = Effect.fn("QueueHoldStore.get")(function* (runIds: ReadonlyArray<RunId>) {
    const holds = new Map<RunId, QueueHold>();
    if (runIds.length === 0 || !(yield* available)) return holds;
    const rows = yield* sql<Row>`SELECT run_id, reason, held_at FROM jones_queue_holds
      WHERE run_id IN ${sql.in(runIds)}`;
    for (const row of rows) {
      const heldAt = DateTime.make(row.held_at);
      // A reason this build does not know, or an unreadable time, reads as an unrecorded hold.
      if (!isQueueHoldReason(row.reason) || Option.isNone(heldAt)) continue;
      holds.set(row.run_id as RunId, { reason: row.reason, heldAt: DateTime.toUtc(heldAt.value) });
    }
    return holds;
  });
  const forget = Effect.fn("QueueHoldStore.forget")(function* (runIds: ReadonlyArray<RunId>) {
    if (runIds.length === 0 || !(yield* available)) return;
    yield* sql`DELETE FROM jones_queue_holds WHERE run_id IN ${sql.in(runIds)}`;
  });
  const forgetThreads = Effect.fn("QueueHoldStore.forgetThreads")(function* (
    threadIds: ReadonlyArray<ThreadId>,
  ) {
    if (threadIds.length === 0 || !(yield* available)) return;
    yield* sql`DELETE FROM jones_queue_holds WHERE thread_id IN ${sql.in(threadIds)}`;
  });
  return { available, record, get, forget, forgetThreads };
};
export type QueueHoldStore = ReturnType<typeof makeQueueHoldStore>;

const RESTART_HOLD_COMMAND = "provider-runtime.reconcile";
const STOP_HOLD_COMMANDS: ReadonlySet<string> = new Set(["thread.stop", "run.interrupt"]);

/**
 * Pure. Only hold sites emit a held queued run under these origins: `run.created` behind held
 * work, startup recovery, Stop, or an explicit intent. Other writers re-emit holds unchanged.
 * A run that leaves the queue or is released loses its row.
 */
export function classifyQueueHoldEvents(
  events: ReadonlyArray<OrchestrationV2DomainEvent>,
  origin: QueueHoldOrigin | undefined,
): {
  readonly holds: ReadonlyArray<QueueHoldRecord>;
  readonly released: ReadonlyArray<RunId>;
  readonly deletedThreads: ReadonlyArray<ThreadId>;
} {
  const holds: Array<QueueHoldRecord> = [];
  const released: Array<RunId> = [];
  const deletedThreads: Array<ThreadId> = [];
  const commandReason: QueueHoldReason | undefined =
    origin?.commandType === RESTART_HOLD_COMMAND
      ? "restart"
      : origin?.commandType !== undefined && STOP_HOLD_COMMANDS.has(origin.commandType)
        ? "stop"
        : undefined;
  const explicit = origin?.queueHold;
  for (const event of events) {
    if (event.type === "thread.deleted") {
      deletedThreads.push(event.threadId);
      continue;
    }
    if (event.type !== "run.created" && event.type !== "run.updated") continue;
    const run = event.payload;
    if (run.status !== "queued") {
      if (run.queueHeld === true) released.push(run.id);
      continue;
    }
    if (run.queueHeld === false) {
      released.push(run.id);
      continue;
    }
    if (run.queueHeld !== true) continue;
    const reason: QueueHoldReason | undefined =
      event.type === "run.created"
        ? "inherited"
        : explicit?.runIds.includes(run.id) === true
          ? explicit.reason
          : commandReason;
    if (reason === undefined) continue;
    holds.push({ runId: run.id, threadId: event.threadId, reason, heldAt: event.occurredAt });
  }
  return { holds, released, deletedThreads };
}

/** Runs inside the accepting upstream transaction, after projections. */
export const acceptQueueHoldEvents = Effect.fn("QueueHoldStore.acceptEvents")(function* (
  store: QueueHoldStore,
  events: ReadonlyArray<OrchestrationV2DomainEvent>,
  origin?: QueueHoldOrigin,
) {
  const { holds, released, deletedThreads } = classifyQueueHoldEvents(events, origin);
  yield* store.forget(released);
  yield* store.forgetThreads(deletedThreads);
  yield* store.record(holds);
});
