import { CommandId, RunId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Scheduler from "../../scheduling/Scheduler.ts";
import { RESTART_CONTINUATION_MESSAGE_PREFIX } from "./QueueHold.ts";

/** Hours a held automatic message waits before the sweep cancels it. */
const HELD_QUEUE_EXPIRY_HOURS_ENV = "T3CODE_HELD_QUEUE_EXPIRY_HOURS";
export const DEFAULT_HELD_QUEUE_EXPIRY_HOURS = 24;
const HELD_QUEUE_SWEEP_INTERVAL_MS = 60 * 60 * 1000;

const HOUR_MS = 60 * 60 * 1000;

export function heldQueueExpiryMs(configured: string | undefined): number {
  const hours = configured === undefined || configured.trim() === "" ? NaN : Number(configured);
  return (Number.isFinite(hours) && hours > 0 ? hours : DEFAULT_HELD_QUEUE_EXPIRY_HOURS) * HOUR_MS;
}

/** One held queued run and the markers of the message it would deliver. */
export interface HeldQueuedRunRow {
  readonly run_id: string;
  readonly thread_id: string;
  readonly requested_at: string;
  readonly held_at: string | null;
  readonly held_reason: string | null;
  readonly message_id: string | null;
  readonly message_role: string | null;
  readonly created_by: string | null;
  readonly creation_source: string | null;
  readonly has_notification: number | null;
  readonly has_delegated_completion: number | null;
  readonly has_sender_thread: number | null;
  readonly has_scheduled_task: number | null;
  readonly has_idle_compaction: number | null;
}

/**
 * A person typed this message into a client. Anything without that evidence, including a
 * missing message row, counts as human so the sweep never discards words it cannot classify;
 * only positive machine markers make a message automatic.
 */
export function isHumanTypedHeldMessage(row: HeldQueuedRunRow): boolean {
  if (row.message_id === null) return true;
  const automatic =
    row.message_id.startsWith(RESTART_CONTINUATION_MESSAGE_PREFIX) ||
    row.has_notification === 1 ||
    row.has_delegated_completion === 1 ||
    row.has_sender_thread === 1 ||
    row.has_scheduled_task === 1 ||
    row.has_idle_compaction === 1 ||
    row.message_role !== "user" ||
    row.created_by !== "user" ||
    (row.creation_source !== "web" && row.creation_source !== "mobile");
  return !automatic;
}

/** Held age starts at the recorded hold, or at the request for holds recorded without a time. */
function heldSinceMs(row: HeldQueuedRunRow): number {
  return Date.parse(row.held_at ?? row.requested_at);
}

function expiredHeldRuns(
  rows: ReadonlyArray<HeldQueuedRunRow>,
  nowMs: number,
  expiryMs: number,
): ReadonlyArray<HeldQueuedRunRow> {
  return rows.filter((row) => {
    const since = heldSinceMs(row);
    return Number.isFinite(since) && nowMs - since >= expiryMs && !isHumanTypedHeldMessage(row);
  });
}

const heldQueueExpiryCommandId = (runId: string) =>
  CommandId.make(`command:held-queue-expiry:${runId}`);

// The partial recovery index on (status, thread_id) bounds this to queued runs; each message is
// read by its primary key.
const readHeldQueuedRuns = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<HeldQueuedRunRow>`
    SELECT
      run.run_id AS run_id,
      run.thread_id AS thread_id,
      run.requested_at AS requested_at,
      json_extract(run.payload_json, '$.queueHeldAt') AS held_at,
      json_extract(run.payload_json, '$.queueHeldReason') AS held_reason,
      message.message_id AS message_id,
      message.role AS message_role,
      json_extract(message.payload_json, '$.createdBy') AS created_by,
      json_extract(message.payload_json, '$.creationSource') AS creation_source,
      json_type(message.payload_json, '$.notification') IS NOT NULL AS has_notification,
      json_type(message.payload_json, '$.delegatedCompletion') IS NOT NULL
        AS has_delegated_completion,
      json_type(message.payload_json, '$.senderThreadId') IS NOT NULL AS has_sender_thread,
      json_type(message.payload_json, '$.scheduledTaskId') IS NOT NULL AS has_scheduled_task,
      json_type(message.payload_json, '$.idleCompaction') IS NOT NULL AS has_idle_compaction
    FROM orchestration_v2_projection_runs AS run
    LEFT JOIN orchestration_v2_projection_messages AS message
      ON message.message_id = json_extract(run.payload_json, '$.userMessageId')
    WHERE run.status = 'queued'
      AND json_extract(run.payload_json, '$.queueHeld') = 1
  `;
});

export interface HeldQueueExpiryResult {
  readonly cancelled: ReadonlyArray<{
    readonly runId: string;
    readonly threadId: string;
    readonly reason: string | null;
    readonly heldHours: number;
  }>;
  readonly failed: number;
}

/**
 * Cancels held automatic messages older than `expiryMs` through `queued-run.cancel`, the same
 * path a person's cancel takes, so delegated-completion cohorts are disposed as usual. A second
 * sweep finds nothing: cancelled runs are no longer queued.
 */
export const sweepHeldQueue = Effect.fn("HeldQueueExpiry.sweep")(function* (options: {
  readonly expiryMs: number;
}) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
  const expired = expiredHeldRuns(yield* readHeldQueuedRuns, nowMs, options.expiryMs);
  const cancelled: Array<HeldQueueExpiryResult["cancelled"][number]> = [];
  let failed = 0;
  for (const row of expired) {
    const outcome = yield* orchestrator
      .dispatch({
        type: "queued-run.cancel",
        commandId: heldQueueExpiryCommandId(row.run_id),
        threadId: ThreadId.make(row.thread_id),
        runId: RunId.make(row.run_id),
      })
      .pipe(
        Effect.as(true),
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.interrupt
            : Effect.logWarning("jones.held-queue-expiry.cancel-failed", {
                runId: row.run_id,
                threadId: row.thread_id,
                cause: Cause.pretty(cause),
              }).pipe(Effect.as(false)),
        ),
      );
    if (!outcome) {
      failed += 1;
      continue;
    }
    cancelled.push({
      runId: row.run_id,
      threadId: row.thread_id,
      reason: row.held_reason,
      heldHours: Math.floor((nowMs - heldSinceMs(row)) / HOUR_MS),
    });
  }
  if (cancelled.length > 0 || failed > 0) {
    yield* Effect.logInfo("jones.held-queue-expiry.cancelled", {
      count: cancelled.length,
      failed,
      expiryHours: options.expiryMs / HOUR_MS,
      runs: cancelled,
    });
  }
  return { cancelled, failed } satisfies HeldQueueExpiryResult;
});

/**
 * Runs the sweep once the scheduler admits work after startup recovery, then at most hourly.
 */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const scheduler = yield* Scheduler.Scheduler;
    const expiryMs = heldQueueExpiryMs(process.env[HELD_QUEUE_EXPIRY_HOURS_ENV]);
    let lastSweepMs: number | undefined;
    const due = Effect.gen(function* () {
      const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
      if (lastSweepMs !== undefined && nowMs - lastSweepMs < HELD_QUEUE_SWEEP_INTERVAL_MS) return;
      lastSweepMs = nowMs;
      yield* sweepHeldQueue({ expiryMs });
    });
    yield* scheduler.register("jones-held-queue-expiry", due);
  }),
);
