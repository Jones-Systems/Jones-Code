import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import * as Continuations from "@t3tools/provider-core/server/ProviderContinuationRequests";
import type * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Context from "effect/Context";
import * as Queue from "effect/Queue";
import * as SqlClient from "effect/sql/SqlClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Config from "../../config.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as Start from "../../orchestration-v2/ProviderTurnStartService.ts";
import * as Admission from "../orchestration/ControlAdmission.ts";
import * as Store from "./Store.ts";
import * as Probe from "./Probe.ts";

export const driver = ProviderDriverKind.make("codex");
const instance = ProviderInstanceId.make("codex");
export const selection = { instanceId: instance, model: "test" };
const adapter = {
  instanceId: instance,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Refusal fixtures never open a provider"),
} as Adapter.ProviderAdapterV2["Service"];
export const database = Sqlite.layerMemory;
const config = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-start-refusal-" });
    return Config.layerTest(process.cwd(), directory);
  }).pipe(Effect.orDie),
).pipe(Layer.provide(NodeServices.layer), Layer.orDie);
export class Gate extends Context.Service<
  Gate,
  {
    allow: boolean;
    calls: number;
    readonly entered: Queue.Queue<void>;
  }
>()("t3/jones/startRefusal/integration.testkit/Gate") {}
const gateLayer = Layer.effect(
  Gate,
  Effect.gen(function* () {
    return { allow: true, calls: 0, entered: yield* Queue.unbounded<void>() };
  }),
);
const probeLayer = Layer.effect(
  Probe.Probe,
  Effect.gen(function* () {
    const gate = yield* Gate;
    return Probe.Probe.of({
      open: () =>
        Effect.gen(function* () {
          gate.calls++;
          yield* Queue.offer(gate.entered, undefined);
          return gate.allow;
        }),
    });
  }),
).pipe(Layer.provideMerge(gateLayer));
export class Offers extends Context.Service<
  Offers,
  {
    readonly requests: Array<Continuations.ProviderContinuationRequest>;
  }
>()("t3/jones/startRefusal/integration.testkit/Offers") {}
const offersLayer = Layer.sync(Offers, () => ({ requests: [] }));
const continuationsLayer = Layer.effect(
  Continuations.ProviderContinuationRequests,
  Effect.gen(function* () {
    const offers = yield* Offers;
    return {
      offer: (request: Continuations.ProviderContinuationRequest) =>
        Effect.sync(() => {
          offers.requests.push(request);
        }),
      take: Effect.never,
    };
  }),
).pipe(Layer.provide(offersLayer));
const makeRuntime = <Output>(
  extra: Layer.Layer<Output>,
  missing: false | "both" | "attempts" | "holds" = false,
  suppliedDatabase = database,
) => {
  const db = missing
    ? Layer.effect(
        SqlClient.SqlClient,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          if (missing === "both" || missing === "attempts")
            yield* sql`DROP TABLE jones_start_refusal_attempts`;
          if (missing === "both" || missing === "holds")
            yield* sql`DROP TABLE jones_start_refusal_holds`;
          return sql;
        }),
      ).pipe(Layer.provide(suppliedDatabase))
    : suppliedDatabase;
  return Replay.layerWithRegistryAndServices(
    { name: "start-refusal" },
    Registry.layerFromAdapters([adapter]),
    {
      databaseLayer: db,
      serverConfigLayer: config,
      runEffectWorker: false,
      runContinuationWorker: false,
      continuationRequestsLayer: continuationsLayer,
      startAdmissionLayer: Layer.mock(Admission.ControlAdmission)({
        assertOpen: ({ instanceId }) =>
          Effect.fail(new Admission.ControlAdmissionHeld({ instanceId })),
      }),
    },
  ).pipe(
    Layer.provideMerge(Store.layer.pipe(Layer.provideMerge(db))),
    Layer.provideMerge(extra),
    Layer.provideMerge(offersLayer),
  );
};
export const runtime = makeRuntime(probeLayer);
export const runtimeWithoutProbe = makeRuntime(Layer.empty);
export const legacyRuntime = makeRuntime(probeLayer, "both");
export const missingAttemptsRuntime = makeRuntime(probeLayer, "attempts");
export const missingHoldsRuntime = makeRuntime(probeLayer, "holds");
export const runtimeOn = (sql: SqlClient.SqlClient, withProbe: boolean) =>
  makeRuntime(withProbe ? probeLayer : Layer.empty, false, Layer.succeed(SqlClient.SqlClient, sql));

export const seedTasks = Effect.fnUntraced(function* (taskCount = 1) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* Sink.EventSinkV2;
  const start = yield* Start.ProviderTurnStartServiceV2;
  const threadId = ThreadId.make("thread:refusal-parent");
  let sequence = 0;
  const event = (value: Omit<OrchestrationV2DomainEvent, "id" | "occurredAt">) =>
    Effect.map(
      DateTime.now,
      (now) =>
        ({
          ...value,
          id: EventId.make(`fixture:refusal:${++sequence}`),
          occurredAt: now,
        }) as OrchestrationV2DomainEvent,
    );
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create:refusal"),
    threadId,
    projectId: ProjectId.make("project:refusal"),
    title: "Synthetic refusal",
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("send:parent"),
    threadId,
    messageId: MessageId.make("message:parent"),
    text: "Synthetic parent",
    attachments: [],
    modelSelection: selection,
    dispatchMode: { type: "queue_after_active" },
    createdBy: "user",
    creationSource: "web",
  });
  const initial = yield* orchestrator.getThreadProjection(threadId);
  const parent = initial.runs[0]!;
  const taskId = NodeId.make("task:refusal-child");
  const now = yield* DateTime.now;
  const task: OrchestrationV2Subagent = {
    id: taskId,
    threadId,
    runId: parent.id,
    parentNodeId: parent.rootNodeId!,
    origin: "app_owned",
    createdBy: "agent",
    driver,
    providerInstanceId: instance,
    providerThreadId: null,
    childThreadId: null,
    remoteChild: {
      environmentId: EnvironmentId.make("environment:fixture"),
      threadId: ThreadId.make("thread:child"),
      label: "fixture",
    },
    nativeTaskRef: null,
    prompt: "Synthetic child",
    title: null,
    model: null,
    completionWake: "always",
    completionDelivery: { state: "pending", observedByRunId: null },
    status: "running",
    result: null,
    startedAt: now,
    completedAt: null,
    updatedAt: now,
  };
  yield* sink.write({
    events: [
      yield* event({
        type: "run.updated",
        threadId,
        runId: parent.id,
        payload: { ...parent, status: "completed", completedAt: now },
      }),
      yield* event({
        type: "subagent.updated",
        threadId,
        runId: parent.id,
        nodeId: taskId,
        payload: task,
      }),
    ],
  });
  yield* orchestrator.dispatch({
    type: "delegated_task.remote.complete",
    commandId: CommandId.make("complete:child"),
    parentThreadId: threadId,
    taskId,
    status: "completed",
    result: "Retain this result",
    stoppedThere: "ran-out",
  });
  for (let index = 1; index < taskCount; index++) {
    const siblingId = NodeId.make(`task:refusal-sibling:${index}`);
    yield* sink.write({
      events: [
        yield* event({
          type: "subagent.updated",
          threadId,
          runId: parent.id,
          nodeId: siblingId,
          payload: { ...task, id: siblingId },
        }),
      ],
    });
    yield* orchestrator.dispatch({
      type: "delegated_task.remote.complete",
      commandId: CommandId.make(`complete:sibling:${index}`),
      parentThreadId: threadId,
      taskId: siblingId,
      status: "completed",
      result: `Sibling result ${index}`,
      stoppedThere: "ran-out",
    });
  }
  const claimed = yield* orchestrator.getThreadProjection(threadId);
  const delivery = claimed.runs.find((run) => run.id === parent.id)!.delegatedCompletion!.delivery!;
  const dispatchDelivery = (key = "initial") =>
    orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`delivery:${key}`),
      threadId,
      messageId: delivery.messageId,
      text: "Delegated result",
      attachments: [],
      modelSelection: selection,
      dispatchMode: { type: "queue_after_active" },
      delegatedCompletion: {
        parentRunId: parent.id,
        generation: delivery.generation,
        taskIds: delivery.taskIds,
      },
      createdBy: "agent",
      creationSource: "server",
    });
  yield* dispatchDelivery();
  const admitted = yield* orchestrator.getThreadProjection(threadId);
  const run = admitted.runs.find((run) => run.userMessageId === delivery.messageId)!;
  const refuse = Effect.gen(function* () {
    const finalized = yield* sink.stream({ threadId, eventType: "run.updated" }).pipe(
      Stream.filter(
        ({ event: e }) =>
          e.type === "run.updated" &&
          e.payload.id === parent.id &&
          e.payload.delegatedCompletion?.delivery?.messageId !== delivery.messageId,
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkChild,
    );
    yield* Effect.yieldNow;
    yield* start.start({ threadId, runId: run.id }).pipe(Effect.result);
    yield* Fiber.join(finalized);
  });
  const witness = (key: string, native = true, createAttempt = true, witnessDriver = driver) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const current = yield* orchestrator.getThreadProjection(threadId);
      const witnessRun = {
        ...run,
        ordinal: Math.max(...current.runs.map((r) => r.ordinal)) + 1,
        id: RunId.make(`run:witness:${key}`),
        userMessageId: MessageId.make(`message:witness:${key}`),
        activeAttemptId: RunAttemptId.make(`attempt:witness:${key}`),
        status: "running" as const,
        startedAt: now,
        completedAt: null,
      };
      const original = admitted.attempts.find((a) => a.id === run.activeAttemptId)!;
      const attempt = {
        ...original,
        id: witnessRun.activeAttemptId,
        runId: witnessRun.id,
        status: "running" as const,
        startedAt: now,
        completedAt: null,
      };
      if (createAttempt)
        yield* sink.write({
          events: [
            yield* event({
              type: "run.created",
              threadId,
              runId: witnessRun.id,
              payload: witnessRun,
            }),
            yield* event({
              type: "run-attempt.created",
              threadId,
              runId: witnessRun.id,
              payload: attempt,
            }),
          ],
        });
      const stored = yield* sink.write({
        events: [
          yield* event({
            type: "provider-turn.updated",
            threadId,
            runId: witnessRun.id,
            driver: witnessDriver,
            payload: {
              id: ProviderTurnId.make(`turn:witness:${key}`),
              providerThreadId: attempt.providerThreadId,
              nodeId: attempt.rootNodeId,
              runAttemptId: attempt.id,
              ordinal: Math.max(0, ...current.providerTurns.map((t) => t.ordinal)) + 1,
              status: "running",
              nativeTurnRef: native
                ? { driver: witnessDriver, nativeId: `native:${key}`, strength: "strong" }
                : null,
              startedAt: now,
              completedAt: null,
            },
          }),
        ],
      });
      return stored[0]!;
    });
  return {
    orchestrator,
    sink,
    start,
    threadId,
    parent,
    taskId,
    delivery,
    run,
    event,
    refuse,
    dispatchDelivery,
    witness,
  };
});

export const seed = seedTasks();
