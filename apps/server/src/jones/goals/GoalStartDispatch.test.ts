import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { makeCommandObservationQuery } from "../../orchestration-v2/CommandObservation.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as ServerConfig from "../../config.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as GoalStart from "./GoalStartDispatch.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("The durable-dispatch fixture never starts a provider"),
} as ProviderAdapterV2Shape;
const database = Sqlite.layerMemory;
const runtime = Replay.layerWithRegistry(
  { name: "goal-start-dispatch" },
  Registry.layerFromAdapters([adapter]),
  {
    databaseLayer: database,
    runEffectWorker: false,
    serverConfigLayer: ServerConfig.layerTest(process.cwd(), {
      prefix: "goal-resume-replay-",
    }).pipe(Layer.provide(NodeServices.layer), Layer.orDie),
  },
);
const threadId = ThreadId.make("goal-thread");
function layer(lostAck = false) {
  const threads = Layer.effect(
    Threads.ThreadManagementService,
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      return Threads.ThreadManagementService.of({
        getThreadRecords: orchestrator.getThreadRecords,
        dispatch: (command) =>
          orchestrator.dispatch(command).pipe(
            Effect.flatMap((accepted) =>
              lostAck
                ? Effect.fail(
                    new Orchestrator.OrchestratorDispatchError({
                      commandId: command.commandId,
                      commandType: command.type,
                      cause: "synthetic lost acknowledgement",
                    }),
                  )
                : Effect.succeed(accepted),
            ),
          ),
      } as Threads.ThreadManagementService["Service"]);
    }),
  ).pipe(Layer.provide(runtime));
  return GoalStart.layer.pipe(
    Layer.provide(threads),
    Layer.provideMerge(runtime),
    Layer.provideMerge(database),
  );
}
const createThread = (id = threadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${id}`),
      threadId: id,
      projectId: ProjectId.make("project"),
      title: "Goal",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    return (yield* orchestrator.getThreadProjection(id)).thread;
  });
const create = createThread();

const completedChild = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const parentThreadId = ThreadId.make("goal-parent");
  const taskId = NodeId.make("original-task");
  yield* createThread(parentThreadId);
  const child = yield* create;
  for (const id of [parentThreadId, threadId]) {
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`setup:${id}`),
      messageId: MessageId.make(`setup:${id}`),
      threadId: id,
      text: "Setup",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "start_immediately" },
    });
    const run = (yield* orchestrator.getThreadProjection(id)).runs[0]!;
    yield* sink.write({
      events: [
        {
          id: EventId.make(`completed:${id}`),
          type: "run.updated",
          threadId: id,
          runId: run.id,
          occurredAt: now,
          payload: { ...run, status: "completed", startedAt: now, completedAt: now },
        },
      ],
    });
  }
  const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs[0]!;
  yield* sink.write({
    events: [
      {
        id: EventId.make("child-lineage"),
        type: "thread.metadata-updated",
        threadId,
        occurredAt: now,
        payload: {
          ...child,
          lineage: {
            parentThreadId,
            relationshipToParent: "subagent",
            rootThreadId: parentThreadId,
          },
          forkedFrom: { type: "node", nodeId: taskId },
        },
      },
      {
        id: EventId.make("original-task"),
        type: "subagent.updated",
        threadId: parentThreadId,
        runId: parentRun.id,
        nodeId: taskId,
        occurredAt: now,
        payload: {
          id: taskId,
          threadId: parentThreadId,
          runId: parentRun.id,
          parentNodeId: parentRun.rootNodeId!,
          origin: "app_owned",
          createdBy: "agent",
          driver: adapter.driver,
          providerInstanceId: instanceId,
          providerThreadId: null,
          childThreadId: threadId,
          nativeTaskRef: null,
          prompt: "Setup",
          title: null,
          model: null,
          completionWake: "always",
          completionDelivery: { state: "acknowledged", observedByRunId: parentRun.id },
          status: "completed",
          result: "Setup finished",
          startedAt: now,
          completedAt: now,
          updatedAt: now,
        },
      },
    ],
  });
  return { thread: (yield* orchestrator.getThreadProjection(threadId)).thread, parentThreadId };
});

it.effect("parent resumes a completed child and replays the owned run across completion", () =>
  Effect.gen(function* () {
    const { thread, parentThreadId } = yield* completedChild;
    const service = yield* GoalStart.GoalStartDispatch;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const guard = yield* service.capture(threadId);
    expect(guard).not.toBeNull();
    const input = {
      thread,
      guard: guard!,
      requestId: "parent-resume",
      senderThreadId: parentThreadId,
      text: "/goal resume",
    };
    const first = yield* service.start(input);
    const sql = yield* SqlClient.SqlClient;
    const [tail] = yield* sql<{
      stream_id: string;
    }>`SELECT e.stream_id FROM orchestration_command_receipts r
      JOIN orchestration_events e ON e.sequence = r.result_sequence WHERE r.command_id = 'goal:parent-resume'`;
    expect(tail?.stream_id).toBe(parentThreadId);
    const projection = yield* orchestrator.getThreadProjection(threadId);
    const run = projection.runs.find(
      (r) => r.userMessageId === MessageId.make("goal:parent-resume"),
    )!;
    expect(run.delegatedTaskId).toBeDefined();
    expect(first).toMatchObject({ outcome: "accepted", runId: run.id });
    expect(yield* service.start(input)).toEqual(first);
    const now = yield* DateTime.now;
    yield* (yield* EventSink.EventSinkV2).write({
      events: [
        {
          id: EventId.make("resume-completed"),
          type: "run.updated",
          threadId,
          runId: run.id,
          occurredAt: now,
          payload: { ...run, status: "completed", startedAt: now, completedAt: now },
        },
      ],
    });
    expect(yield* service.observe(threadId, input.requestId)).toEqual(first);
    expect(yield* service.start(input)).toEqual(first);
    expect((yield* orchestrator.getThreadProjection(threadId)).runs).toHaveLength(2);
    expect((yield* orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(2);
  }).pipe(Effect.scoped, Effect.provide(layer())),
);

it.effect.each([
  "foreign-stream",
  "foreign-kind",
  "missing-companion",
  "duplicate-companion",
  "task-child",
  "task-id",
  "parent-run",
  "target-run",
  "sender",
  "message",
  "birth",
  "receipt-target",
  "receipt-tail",
  "duplicate-run",
] as const)("resume correlation rejects %s evidence", (change) =>
  Effect.gen(function* () {
    const { thread, parentThreadId } = yield* completedChild;
    const service = yield* GoalStart.GoalStartDispatch;
    const guard = yield* service.capture(threadId);
    const requestId = "guarded-resume";
    const first = yield* service.start({
      thread,
      guard: guard!,
      requestId,
      senderThreadId: parentThreadId,
      text: "/goal resume",
    });
    expect(first.outcome).toBe("accepted");
    const commandId = CommandId.make(`goal:${requestId}`);
    const sql = yield* SqlClient.SqlClient;
    if (change === "foreign-stream")
      yield* sql`UPDATE orchestration_events SET stream_id = 'unrelated' WHERE command_id = ${commandId} AND stream_id = ${parentThreadId}`;
    else if (change === "foreign-kind")
      yield* sql`UPDATE orchestration_events SET event_type = 'thread.metadata-updated' WHERE command_id = ${commandId} AND event_type = 'subagent.updated'`;
    else if (change === "missing-companion")
      yield* sql`DELETE FROM orchestration_events WHERE command_id = ${commandId} AND event_type = 'node.updated' AND stream_id = ${parentThreadId}`;
    else if (change === "duplicate-companion")
      yield* sql`UPDATE orchestration_events SET event_type = 'node.updated' WHERE command_id = ${commandId} AND event_type = 'subagent.updated'`;
    else if (change === "receipt-target")
      yield* sql`UPDATE orchestration_command_receipts SET aggregate_id = 'unrelated' WHERE command_id = ${commandId}`;
    else if (change === "receipt-tail")
      yield* sql`UPDATE orchestration_command_receipts SET result_sequence = 999999 WHERE command_id = ${commandId}`;
    else if (change === "duplicate-run")
      yield* sql`INSERT INTO orchestration_v2_projection_runs (run_id,thread_id,ordinal,provider,provider_instance_id,provider_thread_id,status,requested_at,completed_at,payload_json)
      SELECT 'duplicate',thread_id,ordinal+1,provider,provider_instance_id,provider_thread_id,status,requested_at,completed_at,json_set(payload_json,'$.id','duplicate','$.ordinal',ordinal+1)
      FROM orchestration_v2_projection_runs WHERE run_id=${first.runId!}`;
    else {
      const [eventType, field] = {
        "task-child": ["subagent.updated", "$.childThreadId"],
        "task-id": ["subagent.updated", "$.id"],
        "parent-run": ["subagent.updated", "$.runId"],
        "target-run": ["run.updated", "$.delegatedTaskId"],
        sender: ["message.updated", "$.senderThreadId"],
        message: ["message.updated", "$.id"],
        birth: ["run.created", "$.userMessageId"],
      }[change]!;
      yield* sql`UPDATE orchestration_events SET payload_json = json_set(payload_json, ${field}, 'unrelated') WHERE command_id = ${commandId} AND event_type = ${eventType}`;
    }
    expect(yield* service.observe(threadId, requestId)).toMatchObject({ outcome: "unknown" });
    const query = yield* makeCommandObservationQuery().pipe(Effect.provide(ProjectionStore.layer));
    const observation = yield* query.observe({
      threadId,
      commandId,
      messageId: MessageId.make(commandId),
    });
    expect(observation.correlation).toBe(
      change === "duplicate-run"
        ? "ambiguous"
        : change === "receipt-tail"
          ? "missing"
          : "mismatched",
    );
    expect(observation.turn).toBeNull();
  }).pipe(Effect.scoped, Effect.provide(layer())),
);
it.effect.each([false, true])("idle resume uses one durable run with lostAck=%s", (lostAck) =>
  Effect.gen(function* () {
    const thread = yield* create;
    const service = yield* GoalStart.GoalStartDispatch;
    const guard = yield* service.capture(threadId);
    expect(guard).not.toBeNull();
    const input = { thread, guard: guard!, requestId: "stable-key", text: "/goal resume" };
    const first = yield* service.start(input);
    expect(first.outcome).toBe("accepted");
    expect(yield* service.start(input)).toEqual(first);
    const projection = yield* (yield* Orchestrator.OrchestratorV2).getThreadProjection(threadId);
    expect(projection.runs).toHaveLength(1);
    expect(projection.messages).toHaveLength(1);
    expect(projection.messages[0]?.text).toBe("/goal resume");
  }).pipe(Effect.scoped, Effect.provide(layer(lostAck))),
);

it.effect("an intervening run rejects resume instead of queuing it", () =>
  Effect.gen(function* () {
    const thread = yield* create;
    const service = yield* GoalStart.GoalStartDispatch;
    const guard = yield* service.capture(threadId);
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("other"),
      messageId: MessageId.make("other"),
      threadId,
      text: "Owner work",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "start_immediately" },
    });
    expect(
      yield* service.start({ thread, guard: guard!, requestId: "resume", text: "/goal resume" }),
    ).toMatchObject({ reason: "dispatch_rejected" });
    expect((yield* orchestrator.getThreadProjection(threadId)).runs).toHaveLength(1);
  }).pipe(Effect.scoped, Effect.provide(layer())),
);
