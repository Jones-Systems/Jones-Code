import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import { ScheduledTaskUpsertInput, type ScheduledTask } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as ThreadLaunchService from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ManagedProjectFolders from "../../project/ManagedProjectFolders.ts";
import * as ScheduledTaskService from "../../scheduledTasks/ScheduledTaskService.ts";
import * as Scheduler from "../../scheduling/Scheduler.ts";
import * as SecretRequests from "../../secrets/SecretRequests.ts";
import * as Fence from "./UpdateAdmissionFence.ts";

type LaunchInput = ThreadLaunchService.ThreadLaunchInput;

const decodeUpsertInput = Schema.decodeUnknownEffect(ScheduledTaskUpsertInput);
const hookTask = (id: string) =>
  decodeUpsertInput({
    id,
    title: "Review PRs",
    prompt: "Review {{body.url}}",
    enabled: true,
    schedule: { type: "webhook" },
    projectId: "project-webhook",
    workspaceStrategy: { type: "root" },
    modelSelection: { instanceId: "codex", model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
  });

const deliver = (
  service: ScheduledTaskService.ScheduledTaskService["Service"],
  task: ScheduledTask,
) =>
  Effect.gen(function* () {
    const bodyText = JSON.stringify({ url: "https://example.test/pull/1" });
    const result = yield* service.triggerWebhook({
      hookId: task.id,
      token: task.webhook?.path.split("/").at(-1) ?? "",
      method: "POST",
      path: `/api/hooks/${task.id}`,
      query: "",
      headers: { "content-type": "application/json" },
      body: new TextEncoder().encode(bodyText),
      bodyText,
    });
    return result._tag === "accepted" ? result.deliveryId : assert.fail(result._tag);
  });

const pendingRuns = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly count: number }>`
    SELECT COUNT(*) AS count FROM jones_scheduled_webhook_pending_runs`;
  return rows[0]?.count ?? 0;
});

/** Lets forked runs reach their next wait. */
const settle = Effect.forEach(Array.from({ length: 200 }), () => Effect.yieldNow, {
  discard: true,
});

/**
 * One server lifetime over the shared database. A launch dispatches its first message as the
 * launch service does: the host's fence admits it, and a scheduled message the fence refuses is
 * held. Scheduler sources run only when the test ticks them, as they would once the server
 * activates; the lifetime's end interrupts its runs as an update's restart does.
 */
const lifetime = <A, E>(
  fence: Fence.UpdateAdmissionFence["Service"],
  body: (input: {
    readonly service: ScheduledTaskService.ScheduledTaskService["Service"];
    readonly attempts: Queue.Queue<LaunchInput>;
    readonly tick: Effect.Effect<void>;
  }) => Effect.Effect<A, E, SqlClient.SqlClient>,
) =>
  Effect.gen(function* () {
    const attempts = yield* Queue.unbounded<LaunchInput>();
    const sources: Array<Effect.Effect<void>> = [];
    const firstMessage = (input: LaunchInput) =>
      Queue.offer(attempts, input).pipe(
        Effect.andThen(fence.admit([{ request: { type: "provider-turn.start" } }])),
        Effect.mapError(
          (cause) =>
            new ThreadLaunchService.ThreadLaunchError({
              operation: "dispatch-message",
              commandId: input.commandId,
              projectId: input.projectId,
              cause,
            }),
        ),
      );
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
          Fence.holdScheduledDispatch(Option.some(fence), firstMessage(input)).pipe(
            Effect.as({ threadId: "thread-1", resumed: false } as never),
          ),
      }),
      Layer.mock(ThreadManagementService.ThreadManagementService)({}),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        isScratchProject: () => Effect.succeed(false),
      }),
      Layer.mock(SecretRequests.SecretRequests)({}),
      Layer.succeed(
        ScheduledTaskService.ScheduledTaskWebhookOrigin,
        Effect.succeed({ relayHookBaseUrl: null }),
      ),
    );
    const tick = Effect.suspend(() => Effect.forEach(sources, (source) => source));
    return yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      return yield* body({ service, attempts, tick: Effect.asVoid(tick) });
    }).pipe(Effect.provide(ScheduledTaskService.layer.pipe(Layer.provide(dependencies))));
  });

it.effect(
  "an accepted delivery held for an update reruns under its command id after the restart",
  () =>
    Effect.gen(function* () {
      // The launcher accepted this host's update, so it is about to restart.
      const updating = yield* Fence.make;
      const ticket = yield* updating.raise("manual");
      assert.isTrue(yield* ticket.handOff);
      yield* ticket.accept;
      const before = yield* lifetime(updating, ({ service, attempts }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(yield* hookTask("scheduled-task:fenced"));
          const deliveryId = yield* deliver(service, task);
          const held = yield* Queue.take(attempts);
          yield* settle;
          // Held, not failed: the delivery is still accepted and its record is kept.
          const { delivery } = yield* service.getWebhookDelivery({ id: task.id, deliveryId });
          assert.equal(delivery.outcome, "accepted");
          assert.equal((yield* service.list()).tasks[0]?.lastRunStatus, "running");
          assert.equal(yield* pendingRuns, 1);
          return { task, deliveryId, held };
        }),
      );
      assert.equal(yield* pendingRuns, 1);

      yield* lifetime(yield* Fence.make, ({ service, attempts, tick }) =>
        Effect.gen(function* () {
          yield* tick;
          const rerun = yield* Queue.take(attempts);
          assert.equal(rerun.commandId, before.held.commandId);
          assert.equal(
            rerun.commandId,
            `scheduled-task:${before.task.id}:webhook:${before.deliveryId}`,
          );
          assert.equal(yield* Queue.size(attempts), 0);
          assert.equal((yield* service.list()).tasks[0]?.lastRunStatus, "succeeded");
          assert.equal(yield* pendingRuns, 0);
        }),
      );
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("an accepted delivery held for an update runs when the update is called off", () =>
  Effect.gen(function* () {
    const fence = yield* Fence.make;
    const ticket = yield* fence.raise("automatic");
    assert.isTrue(yield* ticket.handOff);
    yield* lifetime(fence, ({ service, attempts }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(yield* hookTask("scheduled-task:called-off"));
        const deliveryId = yield* deliver(service, task);
        const held = yield* Queue.take(attempts);
        yield* settle;
        assert.equal(yield* pendingRuns, 1);
        assert.equal((yield* service.list()).tasks[0]?.lastRunStatus, "running");
        // The launcher refused or the install failed: admission reopens in this lifetime.
        assert.isTrue(yield* ticket.release);
        const retried = yield* Queue.take(attempts);
        assert.equal(retried.commandId, held.commandId);
        assert.equal(retried.commandId, `scheduled-task:${task.id}:webhook:${deliveryId}`);
        yield* settle;
        assert.equal((yield* service.list()).tasks[0]?.lastRunStatus, "succeeded");
        const { delivery } = yield* service.getWebhookDelivery({ id: task.id, deliveryId });
        assert.equal(delivery.outcome, "accepted");
        assert.equal(yield* pendingRuns, 0);
      }),
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

/**
 * Starts an installation whose launcher never answers, so its handoff outcome is unknown until
 * `proof` settles it; interrupting the returned fiber cancels the installation.
 */
const unansweredHandoff = (
  fence: Fence.UpdateAdmissionFence["Service"],
  proof: Deferred.Deferred<"accepted" | "rejected">,
) =>
  Effect.gen(function* () {
    const handingOff = yield* Deferred.make<void>();
    const install = yield* Fence.fencedInstall({
      fence,
      mode: "manual",
      handoffOutcome: () => Deferred.await(proof),
      install: ({ onPrepared }) =>
        onPrepared(Effect.void).pipe(
          Effect.andThen(Deferred.succeed(handingOff, undefined)),
          Effect.andThen(Effect.never),
        ),
    }).pipe(Effect.forkChild);
    yield* Deferred.await(handingOff);
    return install;
  });

it.effect(
  "an accepted delivery stays held through a cancelled update until its rejection is proven",
  () =>
    Effect.gen(function* () {
      const fence = yield* Fence.make;
      const proof = yield* Deferred.make<"accepted" | "rejected">();
      const install = yield* unansweredHandoff(fence, proof);
      yield* lifetime(fence, ({ service, attempts }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(yield* hookTask("scheduled-task:unknown"));
          const deliveryId = yield* deliver(service, task);
          const held = yield* Queue.take(attempts);
          // Cancelling the update leaves its launcher outcome unknown; time proves nothing.
          yield* Fiber.interrupt(install);
          yield* TestClock.adjust("1 hour");
          yield* settle;
          assert.equal(yield* Queue.size(attempts), 0);
          assert.equal(yield* pendingRuns, 1);
          assert.equal((yield* service.list()).tasks[0]?.lastRunStatus, "running");
          // The launcher proves it refused the handoff: the delivery runs once, under its id.
          yield* Deferred.succeed(proof, "rejected");
          const retried = yield* Queue.take(attempts);
          assert.equal(retried.commandId, held.commandId);
          assert.equal(retried.commandId, `scheduled-task:${task.id}:webhook:${deliveryId}`);
          yield* settle;
          assert.equal(yield* Queue.size(attempts), 0);
          assert.equal((yield* service.list()).tasks[0]?.lastRunStatus, "succeeded");
          assert.equal(yield* pendingRuns, 0);
        }),
      );
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a delivery held while an acceptance is proven late reruns only after the restart", () =>
  Effect.gen(function* () {
    const fence = yield* Fence.make;
    const proof = yield* Deferred.make<"accepted" | "rejected">();
    const install = yield* unansweredHandoff(fence, proof);
    const before = yield* lifetime(fence, ({ service, attempts }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(yield* hookTask("scheduled-task:late-accept"));
        const deliveryId = yield* deliver(service, task);
        const held = yield* Queue.take(attempts);
        yield* Fiber.interrupt(install);
        yield* Deferred.succeed(proof, "accepted");
        yield* TestClock.adjust("1 hour");
        yield* settle;
        assert.equal(yield* Queue.size(attempts), 0);
        assert.equal(yield* pendingRuns, 1);
        assert.equal((yield* service.list()).tasks[0]?.lastRunStatus, "running");
        return { task, deliveryId, held };
      }),
    );
    assert.equal(yield* pendingRuns, 1);
    yield* lifetime(yield* Fence.make, ({ service, attempts, tick }) =>
      Effect.gen(function* () {
        yield* tick;
        const rerun = yield* Queue.take(attempts);
        assert.equal(rerun.commandId, before.held.commandId);
        assert.equal(yield* Queue.size(attempts), 0);
        assert.equal((yield* service.list()).tasks[0]?.lastRunStatus, "succeeded");
        assert.equal(yield* pendingRuns, 0);
      }),
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
