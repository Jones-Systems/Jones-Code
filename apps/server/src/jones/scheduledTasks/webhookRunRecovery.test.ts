import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import {
  ScheduledTaskUpsertInput,
  ScheduledTaskWebhookDeliveryId,
  type ScheduledTask,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedProjectFolders from "../../project/ManagedProjectFolders.ts";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as ThreadLaunchService from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ScheduledTaskService from "../../scheduledTasks/ScheduledTaskService.ts";
import * as Scheduler from "../../scheduling/Scheduler.ts";
import * as SecretRequests from "../../secrets/SecretRequests.ts";

const decodeUpsertInput = Schema.decodeUnknownEffect(ScheduledTaskUpsertInput);

type LaunchInput = ThreadLaunchService.ThreadLaunchInput;

const hookTask = (id: string, schedule: Record<string, unknown> = {}) =>
  decodeUpsertInput({
    id,
    title: "Review PRs",
    prompt: "Review {{body.url}}",
    enabled: true,
    schedule: { type: "webhook", ...schedule },
    projectId: "project-webhook",
    workspaceStrategy: { type: "root" },
    modelSelection: { instanceId: "codex", model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
  });

const requestFor = (
  task: ScheduledTask,
  url: string,
  relayDeliveryId?: string,
): ScheduledTaskService.WebhookTriggerRequest => {
  const bodyText = JSON.stringify({ url });
  return {
    hookId: task.id,
    token: task.webhook?.path.split("/").at(-1) ?? "",
    method: "POST",
    path: `/api/hooks/${task.id}`,
    query: "",
    headers: { "content-type": "application/json" },
    body: new TextEncoder().encode(bodyText),
    bodyText,
    ...(relayDeliveryId === undefined ? {} : { relayDeliveryId }),
  };
};

const deliver = (
  service: ScheduledTaskService.ScheduledTaskService["Service"],
  task: ScheduledTask,
  url: string,
  relayDeliveryId?: string,
) =>
  service
    .triggerWebhook(requestFor(task, url, relayDeliveryId))
    .pipe(
      Effect.map((result) =>
        result._tag === "accepted" ? result.deliveryId : assert.fail(result._tag),
      ),
    );

const commandIdOf = (task: ScheduledTask, deliveryId: string) =>
  `scheduled-task:${task.id}:webhook:${deliveryId}`;

const countRows = (table: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<{ readonly count: number }>(
      `SELECT COUNT(*) AS count FROM ${table}`,
    );
    return rows[0]?.count ?? 0;
  });

/** Makes every write to `table` fail as a full disk would, until the returned effect runs. */
const failWrites = (table: string, event: "INSERT" | "DELETE", when = "1") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(`CREATE TRIGGER jones_test_fail_write BEFORE ${event} ON ${table}
      WHEN ${when} BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END`);
    return sql.unsafe(`DROP TRIGGER jones_test_fail_write`).pipe(Effect.orDie);
  });

/**
 * One server lifetime over the shared database. Launches either hang, like a run an update
 * stops, or complete. Scheduler sources run only when the test ticks them, as they would once
 * the server activates.
 */
const lifetime = <A, E>(
  launch: "hang" | "complete" | ((input: LaunchInput) => "hang" | "complete"),
  body: (input: {
    readonly service: ScheduledTaskService.ScheduledTaskService["Service"];
    readonly launches: Queue.Queue<LaunchInput>;
    readonly tick: Effect.Effect<void>;
  }) => Effect.Effect<A, E, SqlClient.SqlClient>,
) =>
  Effect.gen(function* () {
    const launches = yield* Queue.unbounded<LaunchInput>();
    const sources: Array<Effect.Effect<void>> = [];
    const dependencies = Layer.mergeAll(
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        isScratchProject: () => Effect.succeed(false),
      }),
      NodeCrypto.layer,
      Layer.mock(Scheduler.Scheduler)({
        register: <E, R>(_name: string, work: Effect.Effect<void, E, R>) =>
          Effect.gen(function* () {
            const context = yield* Effect.context<R>();
            sources.push(work.pipe(Effect.provideContext(context), Effect.orDie));
          }),
      }),
      Layer.mock(ThreadLaunchService.ThreadLaunchService)({
        launch: (input) =>
          Queue.offer(launches, input).pipe(
            Effect.andThen(
              (typeof launch === "function" ? launch(input) : launch) === "hang"
                ? Effect.never
                : Effect.succeed({ threadId: "thread-1", resumed: false } as never),
            ),
          ),
      }),
      Layer.mock(ThreadManagementService.ThreadManagementService)({}),
      Layer.mock(SecretRequests.SecretRequests)({}),
      Layer.succeed(
        ScheduledTaskService.ScheduledTaskWebhookOrigin,
        Effect.succeed({ relayHookBaseUrl: null }),
      ),
    );
    const tick = Effect.suspend(() => Effect.forEach(sources, (source) => source));
    return yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      return yield* body({ service, launches, tick: Effect.asVoid(tick) });
    }).pipe(Effect.provide(ScheduledTaskService.layer.pipe(Layer.provide(dependencies))));
  });

const nothingLeft = lifetime("complete", ({ launches, tick }) =>
  Effect.gen(function* () {
    yield* tick;
    assert.equal(yield* Queue.size(launches), 0);
    assert.equal(yield* countRows("jones_scheduled_webhook_pending_runs"), 0);
  }),
);

it.effect("a restart runs the webhook deliveries it cut off, once each and in order", () =>
  Effect.gen(function* () {
    const before = yield* lifetime("hang", ({ service, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(yield* hookTask("scheduled-task:restart"));
        const deliveryIds = yield* Effect.forEach(["one", "two", "three"], (url) =>
          deliver(service, task, url),
        );
        // The first run is starting when the server stops; the others wait behind it.
        const started = yield* Queue.take(launches);
        return { task, deliveryIds, started };
      }),
    );

    yield* lifetime("complete", ({ service, launches, tick }) =>
      Effect.gen(function* () {
        yield* tick;
        assert.equal(yield* Queue.size(launches), 3);
        const launched = yield* Effect.forEach(before.deliveryIds, () => Queue.take(launches));
        // The cut-off run retries under its own command id, so a dispatch that landed is not
        // repeated.
        assert.equal(launched[0]?.commandId, before.started.commandId);
        for (const [index, deliveryId] of before.deliveryIds.entries()) {
          const { delivery } = yield* service.getWebhookDelivery({
            id: before.task.id,
            deliveryId,
          });
          assert.equal(launched[index]?.commandId, commandIdOf(before.task, deliveryId));
          assert.equal(launched[index]?.initialMessage?.text, delivery.renderedPrompt);
          assert.equal(delivery.outcome, "accepted");
        }
        const [task] = (yield* service.list()).tasks;
        assert.equal(task?.lastRunStatus, "succeeded");
        // Later passes leave settled deliveries alone.
        yield* tick;
        assert.equal(yield* Queue.size(launches), 0);
      }),
    );

    yield* nothingLeft;
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "a restart drops cut-off deliveries whose task was paused, deleted or replaced, or that aged out",
  () =>
    Effect.gen(function* () {
      const before = yield* lifetime("hang", ({ service, launches }) =>
        Effect.gen(function* () {
          const names = ["paused", "deleted", "replaced", "aged", "kept"] as const;
          const tasks = yield* Effect.forEach(names, (name) =>
            Effect.flatMap(
              hookTask(
                `scheduled-task:${name}`,
                name === "aged" ? { maxDeliveryAgeMinutes: 5 } : {},
              ),
              service.upsert,
            ),
          );
          const deliveryIds = yield* Effect.forEach(tasks, ({ task }) =>
            deliver(service, task, task.id),
          );
          yield* Effect.forEach(names, () => Queue.take(launches));
          yield* service.setEnabled({ id: tasks[0]!.task.id, enabled: false });
          yield* service.delete({ id: tasks[1]!.task.id });
          yield* service.delete({ id: tasks[2]!.task.id });
          yield* TestClock.adjust("1 second");
          yield* Effect.flatMap(hookTask("scheduled-task:replaced"), service.upsert);
          return { tasks: tasks.map(({ task }) => task), deliveryIds };
        }),
      );
      yield* TestClock.adjust("10 minutes");

      yield* lifetime("complete", ({ service, launches, tick }) =>
        Effect.gen(function* () {
          yield* tick;
          assert.equal(yield* Queue.size(launches), 1);
          const kept = yield* Queue.take(launches);
          assert.equal(kept.commandId, commandIdOf(before.tasks[4]!, before.deliveryIds[4]!));
          const paused = yield* service.getWebhookDelivery({
            id: before.tasks[0]!.id,
            deliveryId: before.deliveryIds[0]!,
          });
          assert.equal(paused.delivery.outcome, "dispatch_failed");
          assert.equal(paused.delivery.error, "The task was paused before this delivery ran.");
          const aged = yield* service.getWebhookDelivery({
            id: before.tasks[3]!.id,
            deliveryId: before.deliveryIds[3]!,
          });
          assert.equal(aged.delivery.outcome, "dispatch_failed");
          assert.include(aged.delivery.error ?? "", "older than the task's maximum age");
          const replaced = yield* service.listWebhookDeliveries({ id: before.tasks[2]!.id });
          assert.deepEqual(replaced.deliveries, []);
        }),
      );

      yield* nothingLeft;
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect.each(["jones_scheduled_webhook_pending_runs", "scheduled_task_webhook_deliveries"])(
  "a delivery whose write to %s fails is refused with nothing claimed, and its retry runs",
  (table) =>
    lifetime("complete", ({ service, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(yield* hookTask("scheduled-task:full-disk"));
        const restore = yield* failWrites(table, "INSERT");
        // More attempts than a task may queue, so a queue slot a failed attempt kept would show.
        for (let attempt = 0; attempt < 25; attempt++) {
          const refused = yield* service.triggerWebhook(requestFor(task, `direct-${attempt}`));
          assert.deepEqual(refused, { _tag: "unavailable" });
        }
        const held = requestFor(task, "held", "held-1");
        assert.deepEqual(yield* service.triggerWebhook(held), { _tag: "unavailable" });
        yield* restore;
        assert.equal(yield* Queue.size(launches), 0);
        assert.equal(yield* countRows("scheduled_task_webhook_relay_deliveries"), 0);
        assert.equal(yield* countRows("scheduled_task_webhook_deliveries"), 0);
        assert.equal(yield* countRows("jones_scheduled_webhook_pending_runs"), 0);

        // The relay's retry is a new delivery, not a duplicate of one that never ran.
        assert.deepEqual(yield* service.triggerWebhook(held), {
          _tag: "accepted",
          deliveryId: ScheduledTaskWebhookDeliveryId.make("delivery:relay:held-1"),
          outcome: "accepted",
        });
        const launched = yield* Queue.take(launches);
        assert.equal(launched.commandId, commandIdOf(task, "delivery:relay:held-1"));
        yield* deliver(service, task, "direct");
        yield* Queue.take(launches);
        assert.equal(yield* Queue.size(launches), 0);
      }),
    ).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "a retried delivery the server recorded is a duplicate, and the restart runs it once",
  () =>
    Effect.gen(function* () {
      // The server recorded and acknowledged the delivery, but the relay never saw the answer.
      const before = yield* lifetime("hang", ({ service, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(yield* hookTask("scheduled-task:lost-ack"));
          const deliveryId = yield* deliver(service, task, "held", "held-1");
          const started = yield* Queue.take(launches);
          return { task, deliveryId, started };
        }),
      );

      yield* lifetime("complete", ({ service, launches, tick }) =>
        Effect.gen(function* () {
          const retry = service.triggerWebhook(requestFor(before.task, "held", "held-1"));
          const duplicate = {
            _tag: "accepted",
            deliveryId: before.deliveryId,
            outcome: "duplicate",
          } as const;
          // Before activation the retry is answered from the record the restart will run.
          assert.deepEqual(yield* retry, duplicate);
          assert.equal(yield* Queue.size(launches), 0);
          yield* tick;
          assert.equal((yield* Queue.take(launches)).commandId, before.started.commandId);
          assert.deepEqual(yield* retry, duplicate);
          yield* tick;
          assert.equal(yield* Queue.size(launches), 0);
        }),
      );

      yield* nothingLeft;
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a run whose record cannot be removed runs again after the next start", () =>
  Effect.gen(function* () {
    const ran = yield* lifetime(
      (input) => (input.commandId.endsWith("delivery:relay:c") ? "hang" : "complete"),
      ({ service, launches, tick }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(yield* hookTask("scheduled-task:settle"));
          const restore = yield* failWrites(
            "jones_scheduled_webhook_pending_runs",
            "DELETE",
            "OLD.delivery_id = 'delivery:relay:a'",
          );
          for (const id of ["a", "b", "c"]) yield* deliver(service, task, id, id);
          // Runs of one task go in turn, so c starting means a and b have settled or tried to.
          const launched = yield* Effect.forEach(["a", "b", "c"], () => Queue.take(launches));
          // This server already ran a, and c is still running, so no pass starts either again.
          yield* tick;
          assert.equal(yield* Queue.size(launches), 0);
          yield* restore;
          return { task, launched };
        }),
    );

    yield* lifetime("complete", ({ launches, tick }) =>
      Effect.gen(function* () {
        yield* tick;
        // a's dispatch landed, so its retry reuses the command id and cannot dispatch twice.
        const again = yield* Effect.forEach(["a", "c"], () => Queue.take(launches));
        assert.deepEqual(
          again.map((input) => input.commandId),
          [ran.launched[0]!.commandId, ran.launched[2]!.commandId],
        );
        assert.equal(yield* Queue.size(launches), 0);
      }),
    );

    yield* nothingLeft;
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a delivery whose task cannot be read yet is left for a later pass", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const before = yield* lifetime("hang", ({ service, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(yield* hookTask("scheduled-task:unreadable"));
        yield* deliver(service, task, "one");
        return { task, started: yield* Queue.take(launches) };
      }),
    );
    const [stored] = yield* sql<{ readonly schedule_json: string }>`
      SELECT schedule_json FROM scheduled_tasks WHERE task_id = ${before.task.id}
    `;
    yield* sql`UPDATE scheduled_tasks SET schedule_json = '{' WHERE task_id = ${before.task.id}`;

    yield* lifetime("complete", ({ launches, tick }) =>
      Effect.gen(function* () {
        yield* tick;
        assert.equal(yield* Queue.size(launches), 0);
        yield* sql`UPDATE scheduled_tasks SET schedule_json = ${stored!.schedule_json}
          WHERE task_id = ${before.task.id}`;
        yield* tick;
        assert.equal((yield* Queue.take(launches)).commandId, before.started.commandId);
      }),
    );

    yield* nothingLeft;
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

// Upstream-only harnesses build the service without Jones tables. The missing table is no
// reason to acknowledge a delivery a restart would lose.
const upstreamOnly = Layer.effectDiscard(
  runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] }),
).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));

it.effect("without the pending-run table a delivery is refused rather than acknowledged", () =>
  lifetime("complete", ({ service, launches, tick }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* hookTask("scheduled-task:upstream-only"));
      assert.deepEqual(yield* service.triggerWebhook(requestFor(task, "direct")), {
        _tag: "unavailable",
      });
      assert.deepEqual(yield* service.triggerWebhook(requestFor(task, "held", "held-1")), {
        _tag: "unavailable",
      });
      yield* tick;
      assert.equal(yield* Queue.size(launches), 0);
      assert.equal(yield* countRows("scheduled_task_webhook_relay_deliveries"), 0);
      assert.equal(yield* countRows("scheduled_task_webhook_deliveries"), 0);
    }),
  ).pipe(Effect.provide(upstreamOnly)),
);
