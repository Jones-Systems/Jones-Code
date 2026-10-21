import { ScheduledTaskId, type ScheduledTask } from "@t3tools/contracts";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

const TABLE = "jones_scheduled_webhook_pending_runs";

const restartedPastMaxAgeReason =
  "The server restarted before this delivery ran, and it is now older than the task's maximum age.";

/**
 * A webhook delivery is acknowledged before its run starts, and the runs of one task wait in
 * memory behind each other. Neither the sender nor the relay, whose delivery is claimed, sends an
 * acknowledged delivery again. So a delivery is recorded here in the transaction that claims and
 * logs it, before it is acknowledged, and stays recorded until its run settles. A restart, such
 * as an update install, then runs the ones it cut off instead of losing them.
 */
export interface PendingWebhookRun {
  readonly deliveryId: string;
  readonly taskId: ScheduledTaskId;
  /** Identifies the task as it was, so a task replaced under the same id skips the run. */
  readonly taskCreatedAt: string;
  readonly receivedAt: string;
  readonly prompt: string;
}

interface PendingWebhookRunRow {
  readonly delivery_id: string;
  readonly task_id: string;
  readonly task_created_at: string;
  readonly received_at: string;
}

/** Without the table a delivery cannot survive a restart, so it must not be acknowledged. */
class PendingWebhookRunsMissing extends Data.TaggedError("PendingWebhookRunsMissing") {}

function pastMaxDeliveryAge(
  task: Pick<ScheduledTask, "schedule">,
  receivedAt: string,
  now: DateTime.DateTime,
): boolean {
  const maxMinutes = task.schedule.type === "webhook" ? task.schedule.maxDeliveryAgeMinutes : null;
  if (maxMinutes == null) return false;
  return Option.match(DateTime.make(receivedAt), {
    onNone: () => false,
    onSome: (at) => DateTime.toEpochMillis(now) - DateTime.toEpochMillis(at) > maxMinutes * 60_000,
  });
}

export const makePendingWebhookRuns = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const present = makeJonesTablePresence(sql)(TABLE);
  // Deliveries this server accepted or took from an earlier one. Each runs at most once per
  // server, so a recovery pass skips them; one a pass could not start is released to the next.
  const owned = new Set<string>();

  /**
   * Must run in the transaction that claims and logs the delivery, so they commit together.
   * Fails when the delivery cannot be recorded; the caller rolls back and asks for a retry
   * instead of acknowledging a delivery a restart would lose.
   */
  const record = Effect.fn("PendingWebhookRuns.record")(function* (run: PendingWebhookRun) {
    if (!(yield* present)) return yield* new PendingWebhookRunsMissing();
    // Marked before the insert, so no recovery pass sees the committed row as unowned.
    owned.add(run.deliveryId);
    yield* sql`
        INSERT INTO jones_scheduled_webhook_pending_runs
          (delivery_id, task_id, task_created_at, received_at, prompt)
        VALUES (
          ${run.deliveryId}, ${run.taskId}, ${run.taskCreatedAt}, ${run.receivedAt}, ${run.prompt}
        )
        ON CONFLICT (delivery_id) DO NOTHING
      `;
  });

  /**
   * Called once a run reaches an outcome. A run the server stopped stays recorded. A record
   * that cannot be removed runs again after the next start under the same command id, which
   * does not repeat a dispatch that landed.
   */
  const settle = (deliveryId: string) =>
    present.pipe(
      Effect.flatMap((ok) =>
        ok
          ? sql`DELETE FROM jones_scheduled_webhook_pending_runs WHERE delivery_id = ${deliveryId}`
          : Effect.void,
      ),
      Effect.andThen(Effect.sync(() => owned.delete(deliveryId))),
      Effect.catchCause((cause) =>
        Effect.logWarning("Could not settle a pending webhook run", { cause }),
      ),
      Effect.asVoid,
    );

  /**
   * A scheduler source: runs the deliveries an earlier server recorded but did not settle, in
   * arrival order. `run` settles each one when it reaches an outcome, and skips it when the task
   * was paused, replaced or retargeted. The command id of a run is derived from its delivery, so
   * a run whose dispatch landed before the stop is not dispatched again. A pass that cannot read
   * the records fails, and the next tick tries again.
   */
  const recover = Effect.fn("PendingWebhookRuns.recover")(function* <FindError, RunError>(input: {
    readonly findTask: (id: ScheduledTaskId) => Effect.Effect<ScheduledTask | null, FindError>;
    readonly skip: (deliveryId: string, reason: string) => Effect.Effect<void>;
    readonly run: (
      task: ScheduledTask,
      deliveryId: string,
      prompt: string,
    ) => Effect.Effect<void, RunError>;
  }) {
    if (!(yield* present)) return;
    // Prompts stay unread until a run needs one; most records belong to this server.
    const rows = (yield* sql<PendingWebhookRunRow>`
        SELECT delivery_id, task_id, task_created_at, received_at
        FROM jones_scheduled_webhook_pending_runs
        ORDER BY received_at, rowid
      `).filter((row) => !owned.has(row.delivery_id));
    if (rows.length > 0) {
      yield* Effect.logInfo("Running webhook deliveries a restart cut off", {
        deliveries: rows.length,
      });
    }
    for (const row of rows) {
      owned.add(row.delivery_id);
      yield* Effect.gen(function* () {
        const task = yield* input.findTask(ScheduledTaskId.make(row.task_id));
        // Deleting a task removes its delivery log, so there is nothing to run or mark.
        if (task === null) {
          yield* settle(row.delivery_id);
        } else if (pastMaxDeliveryAge(task, row.received_at, yield* DateTime.now)) {
          yield* input.skip(row.delivery_id, restartedPastMaxAgeReason);
          yield* settle(row.delivery_id);
        } else {
          const prompts = yield* sql<{ readonly prompt: string }>`
              SELECT prompt FROM jones_scheduled_webhook_pending_runs
              WHERE delivery_id = ${row.delivery_id}
            `;
          if (prompts[0] === undefined) return;
          yield* input.run(
            { ...task, createdAt: row.task_created_at },
            row.delivery_id,
            prompts[0].prompt,
          );
        }
      }).pipe(
        // A delivery whose task cannot be read yet is left for the next pass rather than
        // stopping the others.
        Effect.catch((cause) =>
          Effect.sync(() => owned.delete(row.delivery_id)).pipe(
            Effect.andThen(
              Effect.logWarning("Could not run a webhook delivery a restart cut off", {
                taskId: row.task_id,
                cause,
              }),
            ),
          ),
        ),
      );
    }
  });

  return { record, settle, recover };
});
