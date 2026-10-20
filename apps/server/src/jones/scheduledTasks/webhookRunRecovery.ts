import { ScheduledTaskId, type ScheduledTask } from "@t3tools/contracts";
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
 * memory behind each other. A delivery is recorded here from acceptance until its run settles,
 * so a restart, such as an update install, runs the ones it cut off instead of losing them:
 * neither the sender nor the relay, whose delivery is already claimed, sends them again.
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
  readonly prompt: string;
}

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
  const present = makeJonesTablePresence(sql);
  const available = present(TABLE).pipe(Effect.orElseSucceed(() => false));
  // Deliveries this server accepted are already running; recovery takes only earlier ones.
  const acceptedHere = new Set<string>();
  let recovered = false;

  // Best effort: a delivery that cannot be recorded still runs; it only loses restart recovery.
  const record = (run: PendingWebhookRun) =>
    Effect.sync(() => acceptedHere.add(run.deliveryId)).pipe(
      Effect.andThen(available),
      Effect.flatMap((ok) =>
        ok
          ? sql`
              INSERT INTO jones_scheduled_webhook_pending_runs
                (delivery_id, task_id, task_created_at, received_at, prompt)
              VALUES (
                ${run.deliveryId}, ${run.taskId}, ${run.taskCreatedAt}, ${run.receivedAt},
                ${run.prompt}
              )
              ON CONFLICT (delivery_id) DO NOTHING
            `
          : Effect.void,
      ),
      Effect.catchCause((cause) =>
        Effect.logWarning("Could not record a pending webhook run", {
          taskId: run.taskId,
          cause,
        }),
      ),
      Effect.asVoid,
    );

  /** Called once a run reaches an outcome. A run the server stopped stays recorded. */
  const settle = (deliveryId: string) =>
    available.pipe(
      Effect.flatMap((ok) =>
        ok
          ? sql`DELETE FROM jones_scheduled_webhook_pending_runs WHERE delivery_id = ${deliveryId}`
          : Effect.void,
      ),
      Effect.andThen(Effect.sync(() => acceptedHere.delete(deliveryId))),
      Effect.catchCause((cause) =>
        Effect.logWarning("Could not settle a pending webhook run", { cause }),
      ),
      Effect.asVoid,
    );

  /**
   * Runs the deliveries an earlier server accepted but did not settle, once, after activation.
   * `run` settles each one when it reaches an outcome, and skips it when the task was paused,
   * replaced or retargeted. The command id of a run is derived from its delivery, so a run whose
   * dispatch landed before the stop is not dispatched again.
   */
  const recover = <E>(input: {
    readonly findTask: (id: ScheduledTaskId) => Effect.Effect<ScheduledTask | null, E>;
    readonly skip: (deliveryId: string, reason: string) => Effect.Effect<void>;
    readonly run: (task: ScheduledTask, deliveryId: string, prompt: string) => Effect.Effect<void>;
  }) =>
    Effect.gen(function* () {
      if (recovered) return;
      if (yield* available) {
        const rows = (yield* sql<PendingWebhookRunRow>`
          SELECT delivery_id, task_id, task_created_at, received_at, prompt
          FROM jones_scheduled_webhook_pending_runs
          ORDER BY received_at, rowid
        `).filter((row) => !acceptedHere.has(row.delivery_id));
        if (rows.length > 0) {
          yield* Effect.logInfo("Running webhook deliveries a restart cut off", {
            deliveries: rows.length,
          });
        }
        for (const row of rows) {
          yield* Effect.gen(function* () {
            const task = yield* input.findTask(ScheduledTaskId.make(row.task_id));
            // Deleting a task removes its delivery log, so there is nothing to run or mark.
            if (task === null) {
              yield* settle(row.delivery_id);
            } else if (pastMaxDeliveryAge(task, row.received_at, yield* DateTime.now)) {
              yield* input.skip(row.delivery_id, restartedPastMaxAgeReason);
              yield* settle(row.delivery_id);
            } else {
              yield* input.run(
                { ...task, createdAt: row.task_created_at },
                row.delivery_id,
                row.prompt,
              );
            }
          }).pipe(
            // An unreadable task keeps its delivery for the next start rather than stopping
            // the others or retrying on every scheduler tick.
            Effect.catch((cause) =>
              Effect.logWarning("Could not run a webhook delivery a restart cut off", {
                taskId: row.task_id,
                cause,
              }),
            ),
          );
        }
      }
      recovered = true;
    });

  return { record, settle, recover };
});
