import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import {
  EnvironmentId,
  NodeId,
  type OrchestrationV2Run,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
  ProviderDriverKind,
  ProviderInstanceId,
  type RemoteTaskObservation,
  RunId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { idleThreadProjection, liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import migration from "../persistence/Migrations/121_JonesRemoteTaskObservations.ts";
import * as ObservationHooks from "./ObservationHooks.ts";

const parentThreadId = ThreadId.make("thread:observation-parent");
const taskId = NodeId.make("task:observation-child");
const observation: RemoteTaskObservation = {
  schemaVersion: "t3.remote-task-observation/v1",
  environmentId: EnvironmentId.make("environment:remote"),
  threadId: ThreadId.make("thread:remote"),
  originalRunId: RunId.make("run:remote"),
  blockingQuestions: {
    resumeToken: "question-1",
    requests: [
      { requestId: RuntimeRequestId.make("request:remote"), runId: RunId.make("run:remote") },
    ],
  },
};
const input = { parentThreadId, taskId, observation };
const task: OrchestrationV2Subagent = {
  id: taskId,
  threadId: parentThreadId,
  runId: RunId.make("run:parent"),
  parentNodeId: NodeId.make("node:parent"),
  origin: "app_owned",
  createdBy: "agent",
  driver: ProviderDriverKind.make("remote"),
  providerInstanceId: ProviderInstanceId.make("codex"),
  providerThreadId: null,
  childThreadId: null,
  remoteChild: {
    environmentId: observation.environmentId,
    threadId: observation.threadId,
    runId: observation.originalRunId,
    label: "Remote",
  },
  nativeTaskRef: null,
  prompt: "Inspect the fixture",
  title: null,
  model: "fixture",
  status: "running",
  result: null,
  startedAt: DateTime.makeUnsafe(0),
  completedAt: null,
  updatedAt: DateTime.makeUnsafe(0),
};

const fixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const parent = yield* Ref.make<OrchestrationV2ThreadProjection>({
    ...idleThreadProjection(liveThreadShell(parentThreadId)),
    subagents: [task],
  });
  const missingParent = yield* Ref.make(false);
  const failSignal = yield* Ref.make(false);
  const signals = yield* Ref.make(0);
  const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2ServerCommand>>([]);
  const dispatch = (command: OrchestrationV2ServerCommand) =>
    Ref.update(commands, (seen) => [...seen, command]);
  const dependencies = Layer.mergeAll(
    IdAllocator.layer,
    ThreadCommandExecutor.layer,
    Layer.mock(ProjectionStore.ProjectionStoreV2)({
      getThreadRecords: () =>
        Ref.get(missingParent).pipe(
          Effect.flatMap((missing) =>
            missing
              ? Effect.fail(
                  new ProjectionStore.ProjectionStoreThreadNotFoundError({
                    threadId: parentThreadId,
                  }),
                )
              : Ref.get(parent),
          ),
        ),
    }),
    Layer.mock(EventSink.EventSinkV2)({
      write: ({ events }) =>
        Effect.gen(function* () {
          const rows = yield* sql<{
            signal_pending: number;
          }>`SELECT signal_pending FROM jones_remote_task_observations`;
          expect(rows[0]?.signal_pending).toBe(1);
          expect(events[0]).toMatchObject({
            type: "subagent.updated",
            threadId: parentThreadId,
            payload: task,
          });
          if (yield* Ref.get(failSignal))
            return yield* new EventSink.EventSinkWriteError({ eventCount: events.length });
          yield* Ref.update(signals, (count) => count + 1);
          return [];
        }).pipe(
          Effect.mapError((cause) =>
            Schema.is(EventSink.EventSinkWriteError)(cause)
              ? cause
              : new EventSink.EventSinkWriteError({ eventCount: events.length, cause }),
          ),
        ),
    }),
  );
  const context = yield* Layer.build(dependencies);
  const build = ObservationHooks.make(dispatch).pipe(Effect.provide(context));
  return { sql, parent, missingParent, failSignal, signals, commands, build, hooks: yield* build };
});
const database = NodeSqliteClient.layer({ filename: ":memory:" });

it.effect(
  "recovers a committed observation after signaling fails and reuses the notice identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* migration;
        const f = yield* fixture;
        yield* Ref.set(f.failSignal, true);
        expect((yield* f.hooks.observeRemoteTask(input).pipe(Effect.result))._tag).toBe("Failure");
        expect(yield* f.hooks.readRemoteTaskObservation(parentThreadId, taskId)).toEqual(
          observation,
        );
        yield* Ref.set(f.failSignal, false);
        const restarted = yield* f.build;
        yield* restarted.recoverRemoteTaskQuestions;
        yield* restarted.recoverRemoteTaskQuestions;
        const commands = yield* Ref.get(f.commands);
        expect(commands).toHaveLength(2);
        expect(commands[0]?.commandId).toBe(commands[1]?.commandId);
        expect(commands[0]).toMatchObject({
          type: "message.dispatch",
          threadId: parentThreadId,
          notification: { source: { kind: "delegated_task", taskIds: [taskId] } },
        });
        if (commands[0]?.type !== "message.dispatch")
          throw new Error("Expected a parent question notice");
        expect(commands[0].text).toContain(
          `environmentId ${observation.environmentId}, threadId ${observation.threadId}, requestId request:remote`,
        );
        expect(commands[0].text).toContain("t3_pending_request_respond");
        expect(commands[0].senderThreadId).toBeUndefined();
        expect(yield* Ref.get(f.signals)).toBe(1);
        expect(
          (yield* f.sql<{
            signal_pending: number;
          }>`SELECT signal_pending FROM jones_remote_task_observations`)[0]?.signal_pending,
        ).toBe(0);
        yield* restarted.observeRemoteTask(input);
        expect(yield* Ref.get(f.signals)).toBe(1);
      }),
    ).pipe(Effect.provide(database)),
);

it.effect.each(["resolved", "cancelled", "interrupted"] as const)(
  "cancels a queued remote question notice after %s and does not reopen a stopped task",
  (outcome) =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* migration;
        const f = yield* fixture;
        yield* f.hooks.observeRemoteTask(input);
        yield* f.hooks.reconcileRemoteTaskQuestions(parentThreadId, taskId);
        const notice = (yield* Ref.get(f.commands))[0];
        if (notice?.type !== "message.dispatch")
          throw new Error("Expected a parent question notice");
        const queuedRunId = RunId.make("run:queued-notice");
        yield* Ref.update(f.parent, (parent) => ({
          ...parent,
          runs: [
            {
              id: queuedRunId,
              status: "queued",
              userMessageId: notice.messageId,
            } as OrchestrationV2Run,
          ],
          subagents:
            outcome === "resolved"
              ? parent.subagents
              : [{ ...task, status: outcome, result: "Stopped." }],
        }));
        if (outcome === "resolved") {
          yield* f.hooks.observeRemoteTask({
            ...input,
            observation: {
              ...observation,
              blockingQuestions: { resumeToken: "cleared", requests: [] },
            },
          });
        } else {
          yield* f.hooks.observeRemoteTask(input);
          expect(yield* f.hooks.readRemoteTaskObservation(parentThreadId, taskId)).toBeUndefined();
          expect(yield* Ref.get(f.signals)).toBe(1);
        }
        const restarted = yield* f.build;
        yield* restarted.recoverRemoteTaskQuestions;
        const commands = yield* Ref.get(f.commands);
        expect(commands.filter((command) => command.type === "message.dispatch")).toHaveLength(1);
        expect(commands[1]).toMatchObject({
          type: "queued-run.cancel",
          threadId: parentThreadId,
          runId: queuedRunId,
        });
        expect(
          (yield* f.sql<{
            notices_json: string;
          }>`SELECT notices_json FROM jones_remote_task_observations`)[0]?.notices_json,
        ).toBe("[]");
      }),
    ).pipe(Effect.provide(database)),
);

it.effect(
  "requires storage for a new observation and sees a table installed after construction",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        expect(yield* f.hooks.readRemoteTaskObservation(parentThreadId, taskId)).toBeUndefined();
        const missing = yield* f.hooks.observeRemoteTask(input).pipe(Effect.flip);
        expect(missing.code).toBe("orchestration_error");
        expect(yield* Ref.get(f.signals)).toBe(0);
        yield* migration;
        yield* f.hooks.observeRemoteTask(input);
        expect(yield* f.hooks.readRemoteTaskObservation(parentThreadId, taskId)).toEqual(
          observation,
        );
      }),
    ).pipe(Effect.provide(database)),
);

it.effect("reports storage and decode failures without publishing a successful observation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* migration;
      const f = yield* fixture;
      yield* f.sql`CREATE TRIGGER jones_test_observation_refusal BEFORE INSERT ON jones_remote_task_observations BEGIN SELECT RAISE(ABORT, 'synthetic write failure'); END`;
      expect((yield* f.hooks.observeRemoteTask(input).pipe(Effect.flip)).code).toBe(
        "orchestration_error",
      );
      expect(yield* Ref.get(f.signals)).toBe(0);
      yield* f.sql`DROP TRIGGER jones_test_observation_refusal`;
      yield* f.hooks.observeRemoteTask(input);
      yield* f.sql`UPDATE jones_remote_task_observations SET observation_json = '{invalid'`;
      expect(
        (yield* f.hooks.readRemoteTaskObservation(parentThreadId, taskId).pipe(Effect.flip)).code,
      ).toBe("orchestration_error");
      expect((yield* f.hooks.recoverRemoteTaskQuestions.pipe(Effect.flip)).code).toBe(
        "orchestration_error",
      );
      expect(yield* Ref.get(f.commands)).toEqual([]);
    }),
  ).pipe(Effect.provide(database)),
);

it.effect("ignores orphaned sidecar rows and mismatched remote bindings", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* migration;
      const f = yield* fixture;
      yield* f.hooks.observeRemoteTask({
        ...input,
        observation: { ...observation, originalRunId: RunId.make("run:other") },
      });
      expect(yield* Ref.get(f.signals)).toBe(0);
      yield* f.hooks.observeRemoteTask(input);
      yield* Ref.set(f.missingParent, true);
      expect(yield* f.hooks.readRemoteTaskObservation(parentThreadId, taskId)).toBeUndefined();
      yield* f.hooks.recoverRemoteTaskQuestions;
      expect(yield* Ref.get(f.commands)).toEqual([]);
    }),
  ).pipe(Effect.provide(database)),
);
