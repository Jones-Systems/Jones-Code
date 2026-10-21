import { expect, it } from "@effect/vitest";
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
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ThreadId,
  RuntimeRequestId,
  TurnItemId,
  type OrchestrationV2Run,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import { Tool } from "effect/ai";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ServerConfig from "../../../config.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as Sqlite from "../../../persistence/Sqlite.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../../orchestration-v2/ProjectionStore.ts";
import * as ReceiptStore from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import * as EffectOutbox from "../../../orchestration-v2/EffectOutbox.ts";
import * as Recovery from "../../../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Registry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import * as Service from "./UserResponseRequestMcpService.ts";
import { handlers } from "./handlers.ts";
import { UserResponseRequestToolkit } from "./tools.ts";

const instanceId = ProviderInstanceId.make("codex");
const sessionId = ProviderSessionId.make("owner-session");
const threadId = ThreadId.make("owner-thread");
const modelSelection = { instanceId, model: "gpt-6" };
const scope: Invocation.McpThreadInvocationScope = {
  environmentId: EnvironmentId.make("synthetic"),
  requestNamespace: "owner-session",
  thread: { threadId, providerSessionId: sessionId, providerInstanceId: instanceId },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("This test never starts a provider"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const database = Sqlite.layerMemory;
const config = Layer.effect(
  ServerConfig.ServerConfig,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* Effect.acquireRelease(Harness.makeReplayServerConfig("owner-response"), (value) =>
      fs.remove(value.baseDir, { recursive: true }).pipe(Effect.orDie),
    );
  }).pipe(Effect.orDie),
).pipe(Layer.provide(NodeServices.layer));
const runtime = Harness.layerWithRegistry(
  { name: "owner-response" },
  Registry.layerFromAdapters([adapter]),
  { databaseLayer: database, serverConfigLayer: config, runEffectWorker: false },
);
const stores = Layer.mergeAll(ProjectionStore.layer, ReceiptStore.layer, EffectOutbox.layer).pipe(
  Layer.provide(database),
);
const threadManagement = Layer.unwrap(
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const receipts = yield* ReceiptStore.CommandReceiptStoreV2;
    return Layer.mock(ThreadManagement.ThreadManagementService)({
      dispatch: orchestrator.dispatch,
      getThreadRecords: orchestrator.getThreadRecords,
      getThreadShell: orchestrator.getThreadShell,
      hasReceipt: (id) => receipts.getByCommandId(id).pipe(Effect.map(Option.isSome), Effect.orDie),
    });
  }),
).pipe(Layer.provide(Layer.merge(runtime, stores)));
const services = Layer.mergeAll(
  runtime,
  stores,
  threadManagement,
  Service.layer.pipe(Layer.provide(threadManagement)),
);
const recovery = Recovery.layer.pipe(
  Layer.provide(Layer.mergeAll(runtime, stores, IdAllocator.layer, ServerSettings.layerTest())),
);
const testLayer = Layer.merge(services, recovery);

const seed = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create"),
    threadId,
    projectId: ProjectId.make("project"),
    title: "Review plan",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  const run: OrchestrationV2Run = {
    id: RunId.make("run"),
    threadId,
    ordinal: 1,
    providerInstanceId: instanceId,
    modelSelection,
    providerThreadId: ProviderThreadId.make("provider-thread"),
    userMessageId: MessageId.make("initial"),
    rootNodeId: NodeId.make("root"),
    activeAttemptId: null,
    status: "running",
    requestedAt: now,
    startedAt: now,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const root: OrchestrationV2ExecutionNode = {
    id: run.rootNodeId!,
    threadId,
    runId: run.id,
    parentNodeId: null,
    rootNodeId: run.rootNodeId!,
    kind: "root_turn",
    status: "running",
    countsForRun: true,
    providerThreadId: run.providerThreadId,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: null,
  };
  const providerThread: OrchestrationV2ProviderThread = {
    id: run.providerThreadId!,
    driver: adapter.driver,
    providerInstanceId: instanceId,
    providerSessionId: sessionId,
    appThreadId: threadId,
    ownerNodeId: root.id,
    nativeThreadRef: null,
    nativeConversationHeadRef: null,
    status: "active",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  yield* sink.write({
    commandId: CommandId.make("seed"),
    events: [
      {
        id: EventId.make("seed-run"),
        type: "run.created",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: run,
      },
      {
        id: EventId.make("seed-root"),
        type: "node.updated",
        threadId,
        runId: run.id,
        nodeId: root.id,
        occurredAt: now,
        payload: root,
      },
      {
        id: EventId.make("seed-provider"),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: providerThread,
      },
    ],
  });
  return { orchestrator, sink, now, run };
});

it.effect(
  "creates immediately, deduplicates live retries and concurrent content conflicts, then delivers free text after completion and recovery",
  () =>
    Effect.gen(function* () {
      const { orchestrator, sink, now, run } = yield* seed;
      const service = yield* Service.UserResponseRequestMcpService;
      const input = {
        instruction: "Review the plan and reply with changes.",
        clientRequestId: "review",
      };
      const first = yield* service.request(scope, input);
      expect(first).toMatchObject({ status: "pending", created: true });
      expect(yield* service.request(scope, input)).toEqual({ ...first, created: false });
      expect(
        (yield* service
          .request(scope, { ...input, instruction: "Different action." })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      const concurrent = yield* Effect.all(
        [
          service.request(scope, { ...input, clientRequestId: "concurrent" }),
          service.request(scope, {
            ...input,
            clientRequestId: "concurrent",
            instruction: "Different action.",
          }),
        ].map((effect) => effect.pipe(Effect.result)),
        { concurrency: 2 },
      );
      expect(concurrent.map((result) => result._tag).sort()).toEqual(["Failure", "Success"]);
      let projection = yield* orchestrator.getThreadProjection(threadId);
      expect(projection.runtimeRequests).toHaveLength(2);
      expect(projection.turnItems.find((item) => item.type === "user_input_request")).toMatchObject(
        {
          questions: [
            { question: input.instruction, options: [], required: true, allowCustomAnswer: true },
          ],
          responseMode: "message",
        },
      );
      yield* sink.write({
        commandId: CommandId.make("complete"),
        events: [
          {
            id: EventId.make("complete"),
            type: "run.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: { ...run, status: "completed", completedAt: now },
          },
        ],
      });
      yield* Recovery.ProviderRuntimeRecoveryService.use((value) => value.recover);
      projection = yield* orchestrator.getThreadProjection(threadId);
      expect(
        projection.runtimeRequests.find((request) => request.id === first.requestId)?.status,
      ).toBe("pending");
      expect((yield* service.request(scope, input).pipe(Effect.result))._tag).toBe("Failure");
      expect(
        (yield* orchestrator
          .dispatch({
            type: "runtime-request.respond",
            commandId: CommandId.make("blank"),
            threadId,
            requestId: first.requestId,
            answers: { response: " " },
          })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(
        (yield* orchestrator.getThreadProjection(threadId)).runtimeRequests.find(
          (request) => request.id === first.requestId,
        )?.status,
      ).toBe("pending");
      yield* orchestrator.dispatch({
        type: "runtime-request.respond",
        commandId: CommandId.make("answer"),
        threadId,
        requestId: first.requestId,
        answers: { response: "  Proceed with the first stage.  " },
      });
      projection = yield* orchestrator.getThreadProjection(threadId);
      expect(
        projection.runtimeRequests.find((request) => request.id === first.requestId)?.status,
      ).toBe("resolved");
      expect(projection.messages.map((message) => message.text)).toEqual([
        `${input.instruction}\nProceed with the first stage.`,
      ]);
      const other = projection.runtimeRequests.find((request) => request.id !== first.requestId)!;
      yield* orchestrator.dispatch({
        type: "thread.user-input.dismiss",
        commandId: CommandId.make("dismiss"),
        threadId,
        requestId: other.id,
      });
      projection = yield* orchestrator.getThreadProjection(threadId);
      expect(projection.runtimeRequests.find((request) => request.id === other.id)).toMatchObject({
        status: "resolved",
        decision: "cancel",
      });
      expect(projection.nodes.find((node) => node.id === other.nodeId)?.status).toBe("cancelled");
      expect(projection.messages).toHaveLength(1);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("keeps a newly created request pending when recovery terminalizes its active run", () =>
  Effect.gen(function* () {
    const { orchestrator } = yield* seed;
    const service = yield* Service.UserResponseRequestMcpService;
    const request = yield* service.request(scope, {
      instruction: "Review the plan.",
      clientRequestId: "restart",
    });
    yield* Recovery.ProviderRuntimeRecoveryService.use((value) => value.recover);
    const projection = yield* orchestrator.getThreadProjection(threadId);
    expect(projection.runs[0]?.status).not.toBe("running");
    expect(projection.runtimeRequests.find((item) => item.id === request.requestId)?.status).toBe(
      "pending",
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("enforces self-only live caller, capability, client and session ownership", () =>
  Effect.gen(function* () {
    const { orchestrator, sink, now, run } = yield* seed;
    const toolkit = yield* UserResponseRequestToolkit.pipe(
      Effect.provide(McpToolAccess.HandlersLayer.layer(handlers)),
    );
    const invoke = (caller: Invocation.McpInvocationScope) =>
      toolkit
        .handle("t3_request_user_response", {
          instruction: "Review the plan.",
          clientRequestId: "access",
        })
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.map((values) => values.at(-1)!),
          Effect.provideService(Invocation.McpInvocationContext, caller),
        );
    for (const caller of [
      { ...scope, capabilities: new Set<Invocation.McpCapability>() },
      {
        ...scope,
        thread: undefined,
        client: { sessionId: "external", label: "External", access: "full-access" as const },
      },
      {
        ...scope,
        client: { sessionId: "external", label: "External", access: "read-only" as const },
      },
      { ...scope, thread: { ...scope.thread, providerSessionId: "stale-session" } },
      {
        ...scope,
        thread: { ...scope.thread, providerInstanceId: ProviderInstanceId.make("foreign") },
      },
    ])
      expect((yield* invoke(caller)).isFailure).toBe(true);
    expect((yield* orchestrator.getThreadProjection(threadId)).runtimeRequests).toHaveLength(0);
    expect((yield* invoke(scope)).isFailure).toBe(false);
    yield* sink.write({
      commandId: CommandId.make("end-access"),
      events: [
        {
          id: EventId.make("end-access"),
          type: "run.updated",
          threadId,
          runId: run.id,
          occurredAt: now,
          payload: { ...run, status: "completed", completedAt: now },
        },
      ],
    });
    expect((yield* invoke(scope)).isFailure).toBe(true);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("serial dispatch refuses a fresh record whose bound run ended before acceptance", () =>
  Effect.gen(function* () {
    const { orchestrator, sink, now, run } = yield* seed;
    yield* sink.write({
      commandId: CommandId.make("race-complete"),
      events: [
        {
          id: EventId.make("race-complete"),
          type: "run.updated",
          threadId,
          runId: run.id,
          occurredAt: now,
          payload: { ...run, status: "completed", completedAt: now },
        },
      ],
    });
    const result = yield* orchestrator
      .dispatch({
        type: "user_input_request.record",
        commandId: CommandId.make("late-record"),
        threadId,
        runId: run.id,
        rootNodeId: run.rootNodeId!,
        providerInstanceId: instanceId,
        requestId: RuntimeRequestId.make("late"),
        requestNodeId: NodeId.make("late"),
        turnItemId: TurnItemId.make("late"),
        question: {
          id: "response",
          header: "Action needed",
          question: "Review the plan.",
          options: [],
          required: true,
          allowCustomAnswer: true,
        },
      })
      .pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    const projection = yield* orchestrator.getThreadProjection(threadId);
    expect(projection.runtimeRequests).toHaveLength(0);
    expect(projection.turnItems).toHaveLength(0);
  }).pipe(Effect.provide(testLayer)),
);

it("publishes a self-only object input schema", () => {
  const schema = Tool.getJsonSchema(UserResponseRequestToolkit.tools.t3_request_user_response);
  expect(schema).toMatchObject({
    type: "object",
    properties: { instruction: { type: "string" }, clientRequestId: { type: "string" } },
  });
  expect(schema.properties).not.toHaveProperty("threadId");
  expect(schema.properties).not.toHaveProperty("runId");
});
