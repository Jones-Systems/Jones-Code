import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import { ScheduledTaskUpsertInput, type ScheduledTask } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as ThreadLaunchService from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ScheduledTaskService from "../../scheduledTasks/ScheduledTaskService.ts";
import * as Scheduler from "../../scheduling/Scheduler.ts";
import * as SecretRequests from "../../secrets/SecretRequests.ts";

const decodeUpsertInput = Schema.decodeUnknownEffect(ScheduledTaskUpsertInput);

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

const deliver = (
  service: ScheduledTaskService.ScheduledTaskService["Service"],
  task: ScheduledTask,
  url: string,
) => {
  const bodyText = JSON.stringify({ url });
  return service
    .triggerWebhook({
      hookId: task.id,
      token: task.webhook?.path.split("/").at(-1) ?? "",
      method: "POST",
      path: `/api/hooks/${task.id}`,
      query: "",
      headers: { "content-type": "application/json" },
      body: new TextEncoder().encode(bodyText),
      bodyText,
    })
    .pipe(
      Effect.map((result) =>
        result._tag === "accepted" ? result.deliveryId : assert.fail(result._tag),
      ),
    );
};

/**
 * One server lifetime over the shared database. Launches either hang, like a run an update
 * stops, or complete. Scheduler sources run only when the test ticks them, as they would once
 * the server activates.
 */
const lifetime = <A, E>(
  launch: "hang" | "complete",
  body: (input: {
    readonly service: ScheduledTaskService.ScheduledTaskService["Service"];
    readonly launches: Queue.Queue<ThreadLaunchService.ThreadLaunchInput>;
    readonly tick: Effect.Effect<void>;
  }) => Effect.Effect<A, E>,
) =>
  Effect.gen(function* () {
    const launches = yield* Queue.unbounded<ThreadLaunchService.ThreadLaunchInput>();
    const sources: Array<Effect.Effect<void>> = [];
    const dependencies = Layer.mergeAll(
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
              launch === "hang"
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
          assert.equal(
            launched[index]?.commandId,
            `scheduled-task:${before.task.id}:webhook:${deliveryId}`,
          );
          assert.equal(launched[index]?.initialMessage?.text, delivery.renderedPrompt);
          assert.equal(delivery.outcome, "accepted");
        }
        const [task] = (yield* service.list()).tasks;
        assert.equal(task?.lastRunStatus, "succeeded");
      }),
    );

    // A later restart has nothing left to run.
    yield* lifetime("complete", ({ launches, tick }) =>
      Effect.gen(function* () {
        yield* tick;
        assert.equal(yield* Queue.size(launches), 0);
      }),
    );
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
          assert.equal(
            kept.commandId,
            `scheduled-task:${before.tasks[4]!.id}:webhook:${before.deliveryIds[4]}`,
          );
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

      yield* lifetime("complete", ({ launches, tick }) =>
        Effect.gen(function* () {
          yield* tick;
          assert.equal(yield* Queue.size(launches), 0);
        }),
      );
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
