import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import { ScheduledTaskUpsertInput, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedProjectFolders from "../../project/ManagedProjectFolders.ts";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as Scheduler from "../../scheduling/Scheduler.ts";
import * as ScheduledTaskService from "../../scheduledTasks/ScheduledTaskService.ts";
import * as ThreadLaunchService from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as SecretRequests from "../../secrets/SecretRequests.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";

const decodeUpsertInput = Schema.decodeUnknownEffect(ScheduledTaskUpsertInput);

it.effect("coalesces timer wakes by task and thread until the queued run clears", () =>
  Effect.gen(function* () {
    let tick = Effect.void;
    const deliveries: Array<{ threadId: string; scheduledTaskId?: string; messageId: string }> = [];
    const runs: Array<{ id: string; threadId: string; userMessageId: string; status: string }> = [];
    const dependencies = Layer.mergeAll(
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/unused",
        isScratchProject: () => Effect.succeed(false),
      }),
      NodeCrypto.layer,
      Layer.mock(Scheduler.Scheduler)({
        register: <E, R>(name: string, work: Effect.Effect<void, E, R>) =>
          Effect.gen(function* () {
            if (name !== "scheduled-tasks") return;
            const context = yield* Effect.context<R>();
            tick = work.pipe(Effect.provideContext(context), Effect.orDie);
          }),
      }),
      Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
      Layer.mock(SecretRequests.SecretRequests)({}),
      Layer.mock(ThreadManagementService.ThreadManagementService)({
        getThreadRecords: (threadId, _fields, filter) =>
          Effect.sync(
            () =>
              ({
                runs: runs.filter((run) => run.threadId === threadId),
                messages: deliveries
                  .filter(
                    (message) =>
                      message.threadId === threadId &&
                      (filter?.messageIds === undefined ||
                        filter.messageIds.some((id) => id === message.messageId)),
                  )
                  .map((message) => ({
                    id: message.messageId,
                    scheduledTaskId: message.scheduledTaskId,
                  })),
              }) as never,
          ),
        sendToThread: (input) =>
          Effect.sync(() => {
            deliveries.push(input);
            runs.push({
              id: input.messageId,
              threadId: input.threadId,
              userMessageId: input.messageId,
              status: "queued",
            });
            return {} as never;
          }),
      }),
    );
    yield* TestClock.setTime(0);
    yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      const create = (id: string, threadId = "thread:a") =>
        decodeUpsertInput({
          id,
          threadId,
          title: "Wake",
          prompt: "Review progress",
          enabled: true,
          schedule: { type: "interval", everyMs: 60_000 },
          projectId: "project:test",
          workspaceStrategy: { type: "root" },
          modelSelection: { instanceId: "codex", model: "gpt-5" },
          runtimeMode: "full-access",
          interactionMode: "default",
        }).pipe(Effect.flatMap(service.upsert));
      yield* create("task:a");
      const fire = (time: number) =>
        TestClock.setTime(time).pipe(Effect.andThen(Effect.suspend(() => tick)));
      yield* fire(60_000);
      yield* fire(120_000);
      expect(deliveries).toHaveLength(1);
      const skipped = (yield* service.list()).tasks[0]!;
      expect(skipped.nextRunAt).toBe("1970-01-01T00:03:00.000Z");
      expect(skipped.lastRunError).toBe(
        "Skipped scheduled wake: this task already has a queued run in the target thread.",
      );
      expect(skipped.runCount).toBe(1);
      yield* create("task:b");
      yield* fire(180_000);
      expect(deliveries.map((message) => message.scheduledTaskId)).toEqual(["task:a", "task:b"]);
      for (const status of ["running", "waiting", "completed", "failed", "cancelled"]) {
        for (const run of runs) run.status = status;
        yield* fire(
          240_000 +
            ["running", "waiting", "completed", "failed", "cancelled"].indexOf(status) * 60_000,
        );
      }
      expect(deliveries).toHaveLength(12);
      const taskA = (yield* service.list()).tasks.find((task) => task.id === "task:a")!;
      expect(taskA.lastRunError).toBeNull();
      yield* service.upsert({ ...taskA, threadId: ThreadId.make("thread:b") });
      yield* fire(540_000);
      expect(deliveries.filter((message) => message.threadId === "thread:b")).toHaveLength(1);
    }).pipe(Effect.provide(ScheduledTaskService.layer.pipe(Layer.provide(dependencies))));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
