import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  CommandId,
  EventId,
  MessageId,
  ThreadId,
  ProjectId,
  EnvironmentId,
  ProviderInstanceId,
  ProviderDriverKind,
  type ModelSelection,
  type ServerProvider,
  ThreadConfigurationEvidence,
  ThreadSelfConfigurationResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as Ref from "effect/Ref";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import { McpServer, McpSchema } from "effect/ai";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Catalog from "../../provider/ProviderRegistry.ts";
import * as ThreadSearch from "../../orchestration-v2/ThreadSearch.ts";
import * as Scheduled from "../../scheduledTasks/ScheduledTaskService.ts";
import * as Peer from "../../peer/PeerForwarding.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Mcp from "../../mcp/McpHttpServer.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Service from "./ThreadSelfConfiguration.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import {
  driver,
  providerInstanceId,
  initialSelection,
  replacementSelection,
  makeRestartAdapter,
  finishActive,
  type RestartAdapterState,
} from "./testkit.ts";

const replacement: ModelSelection = {
  ...replacementSelection,
  options: [
    { id: "reasoningEffort", value: "high" },
    { id: "serviceTier", value: "priority" },
  ],
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "self-configure-test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "self-configure-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const catalog: ServerProvider = {
  instanceId: providerInstanceId,
  driver,
  enabled: true,
  installed: true,
  version: "synthetic",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-10-22T00:00:00.000Z",
  models: [initialSelection, replacementSelection].map(({ model }) => ({
    slug: model,
    name: model,
    isCustom: true,
    capabilities: {
      optionDescriptors: [
        {
          id: "reasoningEffort",
          label: "Effort",
          type: "select",
          options: [
            { id: "high", label: "High" },
            { id: "default", label: "Default" },
          ],
        },
        {
          id: "serviceTier",
          label: "Tier",
          type: "select",
          options: [
            { id: "priority", label: "Priority" },
            { id: "default", label: "Default" },
          ],
        },
      ],
    },
  })),
  slashCommands: [],
  skills: [],
};
const decodeEvidence = Schema.decodeUnknownEffect(ThreadConfigurationEvidence);
const decodeReceipt = Schema.decodeUnknownEffect(ThreadSelfConfigurationResult);
const failure = (result: McpSchema.CallToolResult) =>
  result.content[0]?.type === "text" ? JSON.parse(result.content[0].text) : null;

function scenario<E>(
  name: string,
  restartRequired: boolean,
  body: (ctx: {
    threadId: ThreadId;
    scope: Invocation.McpInvocationScope;
    state: Ref.Ref<RestartAdapterState>;
    call: (
      name: string,
      args?: Record<string, unknown>,
      caller?: Invocation.McpInvocationScope,
    ) => Effect.Effect<McpSchema.CallToolResult>;
  }) => Effect.Effect<
    void,
    E,
    | Scope.Scope
    | Sessions.ProviderSessionManagerV2
    | Orchestrator.OrchestratorV2
    | Worker.OrchestrationEffectWorkerV2
    | Sink.EventSinkV2
  >,
  target?: {
    readonly driver: "codex" | "claude-code";
    readonly compatible: boolean;
    readonly enabled?: boolean;
  },
  afterRestart?: typeof body,
  pooled = false,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(name);
      const database = yield* Layer.build(Sqlite.layerMemory);
      const sql = Context.get(database, SqlClient.SqlClient);
      const threadId = ThreadId.make("thread:" + name);
      const state = yield* Ref.make<RestartAdapterState>({
        activeTurn: null,
        opened: [],
        started: [],
        closedSessionCount: 0,
        failedReplacementOpen: true,
      });
      const scope: Invocation.McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-test"),
        requestNamespace: name,
        thread: { threadId, providerSessionId: "mcp-test:" + threadId, providerInstanceId },
        client: undefined,
        issuedAt: 0,
        capabilities: new Set(["orchestration"]),
      };
      const targetId = ProviderInstanceId.make("target-provider");
      const adapters = [
        makeRestartAdapter(
          state,
          pooled ? CodexProviderCapabilitiesV2 : undefined,
          providerInstanceId,
          restartRequired,
        ),
        ...(target === undefined
          ? []
          : [
              makeRestartAdapter(
                state,
                undefined,
                targetId,
                false,
                ProviderDriverKind.make(target.driver),
              ),
            ]),
      ];
      const registry = Layer.succeed(Registry.ProviderAdapterRegistryV2, {
        get: (id) => {
          const adapter = adapters.find((a) => a.instanceId === id);
          return adapter
            ? Effect.succeed(adapter)
            : Effect.fail(new Registry.ProviderAdapterRegistryLookupError({ instanceId: id }));
        },
        list: () => Effect.succeed(adapters.map((a) => a.instanceId)),
        getMetadata: (id) =>
          Effect.gen(function* () {
            const adapter = adapters.find((a) => a.instanceId === id)!;
            return {
              driver: adapter.driver,
              continuationKey: target?.compatible === false ? "home:" + id : "home:shared",
              enabled: true,
              capabilities: yield* adapter.getCapabilities(),
            };
          }).pipe(Effect.orDie),
      });
      const runtime = Harness.layerWithRegistryAndServices(
        { name, runtimePolicyOverride: { cwd } },
        registry,
        {
          configureMcp: true,
          databaseLayer: Layer.succeed(SqlClient.SqlClient, sql),
          recoverOnStartup: afterRestart !== undefined,
        },
      );
      const service = Service.layer.pipe(
        Layer.provide(runtime),
        Layer.provide(
          Layer.mock(Catalog.ProviderRegistry)({
            getProviders: Effect.succeed([
              catalog,
              ...(target === undefined
                ? []
                : [
                    {
                      ...catalog,
                      instanceId: targetId,
                      driver: ProviderDriverKind.make(target.driver),
                      enabled: target.enabled ?? true,
                    },
                  ]),
            ]),
          }),
        ),
      );
      const tools = Layer.mergeAll(
        Mcp.layerThreadToolkit,
        Mcp.layerThreadSelfConfigurationToolkit,
      ).pipe(
        Layer.provide(service),
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(runtime),
        Layer.provide(Layer.mock(ThreadSearch.ThreadSearch)({})),
        Layer.provide(Layer.mock(Scheduled.ScheduledTaskService)({})),
        Layer.provide(Layer.mock(Peer.PeerForwarding)({})),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* Worker.OrchestrationEffectWorkerV2;
        const sink = yield* Sink.EventSinkV2;
        const server = yield* McpServer.McpServer;
        const call = (name: string, args: Record<string, unknown> = {}, caller = scope) =>
          server
            .callTool({ name, arguments: args })
            .pipe(
              Effect.provideService(Invocation.McpInvocationContext, caller),
              Effect.provideService(McpSchema.McpServerClient, client),
              Effect.orDie,
            );
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(name + ":create"),
          threadId,
          projectId: ProjectId.make("self-configure-project"),
          title: name,
          modelSelection: initialSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const running = yield* sink.stream({ threadId }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "provider-turn.updated" && event.payload.status === "running",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(name + ":start"),
          threadId,
          messageId: MessageId.make(name + ":start"),
          text: "hold at barrier",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          dispatchMode: { type: "start_immediately" },
        });
        yield* Fiber.join(running);
        yield* worker.drain();
        yield* body({ threadId, scope, state, call });
      }).pipe(Effect.provide(Layer.fresh(Layer.merge(runtime, tools))));
      if (afterRestart !== undefined)
        yield* Effect.gen(function* () {
          const server = yield* McpServer.McpServer;
          const call = (name: string, args: Record<string, unknown> = {}, caller = scope) =>
            server
              .callTool({ name, arguments: args })
              .pipe(
                Effect.provideService(Invocation.McpInvocationContext, caller),
                Effect.provideService(McpSchema.McpServerClient, client),
                Effect.orDie,
              );
          yield* afterRestart({ threadId, scope, state, call });
        }).pipe(Effect.provide(Layer.fresh(Layer.merge(runtime, tools))));
    }),
  );
}

it.live.each([false, true])(
  "registered tool protects same run and applies next admission; restart=%s",
  (restartRequired) =>
    scenario(
      "self-configure-" + restartRequired,
      restartRequired,
      ({ threadId, state, call, scope }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          const sink = yield* Sink.EventSinkV2;
          const before = yield* decodeEvidence(
            (yield* call("t3_thread_configuration")).structuredContent,
          );
          assert.isNotNull(before.executionPrecondition);
          const request = {
            clientRequestId: "save",
            modelSelection: replacement,
            expectedConfigurationRevision: before.configurationRevision,
            executionPrecondition: before.executionPrecondition,
          };
          const saved = yield* decodeReceipt(
            (yield* call("t3_thread_self_configure", request)).structuredContent,
          );
          assert.deepEqual(saved.acceptance.committedSelection, replacement);
          assert.deepEqual(
            saved.freshObservation.currentExecution?.requestedSelection,
            initialSelection,
          );
          assert.equal(
            saved.freshObservation.currentExecution?.providerObservation?.observed.model.status,
            "unknown",
          );
          assert.equal(
            saved.freshObservation.currentExecution?.reasoningEffortObservation,
            "unavailable",
          );
          assert.equal((yield* Ref.get(state)).closedSessionCount, 0);
          assert.lengthOf((yield* Ref.get(state)).opened, 1);
          assert.notEqual((yield* call("t3_thread_organize", { action: "pin" })).isError, true);
          const legacy = yield* call("t3_thread_configure", { modelSelection: initialSelection });
          assert.include(failure(legacy).message, "t3_thread_self_configure");
          const duplicate = yield* decodeReceipt(
            (yield* call("t3_thread_self_configure", request)).structuredContent,
          );
          assert.deepEqual(duplicate.acceptance, saved.acceptance);
          assert.equal(duplicate.freshObservation.configurationRevision, saved.acceptance.sequence);
          const conflict = yield* call("t3_thread_self_configure", {
            ...request,
            modelSelection: initialSelection,
          });
          assert.include(failure(conflict).message, "request_conflict");
          const complete = yield* sink.stream({ threadId }).pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "run.updated" &&
                event.payload.id === before.executionPrecondition!.runId &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* finishActive(state);
          yield* Fiber.join(complete);
          yield* worker.drain();
          const sameRun = yield* orchestrator.getThreadProjection(threadId);
          assert.lengthOf(sameRun.runs, 1);
          assert.lengthOf(sameRun.attempts, 1);
          assert.equal(sameRun.runs[0]?.id, before.executionPrecondition!.runId);
          assert.equal(sameRun.attempts[0]?.id, before.executionPrecondition!.attemptId);
          assert.equal(sameRun.attempts[0]?.status, "completed");
          assert.isTrue(
            sameRun.turnItems.some(
              (item) =>
                item.type === "assistant_message" &&
                item.text === "continued-after-self-configuration",
            ),
          );
          const afterCompletion = yield* decodeReceipt(
            (yield* call("t3_thread_self_configure", request)).structuredContent,
          );
          assert.deepEqual(afterCompletion.acceptance, saved.acceptance);
          const freshCredential = yield* decodeReceipt(
            (yield* call("t3_thread_self_configure", request, {
              ...scope,
              thread: { ...scope.thread!, providerSessionId: "replacement-credential" },
            })).structuredContent,
          );
          assert.deepEqual(freshCredential.acceptance, saved.acceptance);
          assert.isNull(freshCredential.freshObservation.executionPrecondition);
          const nextDone = yield* sink.stream({ threadId }).pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "run.updated" &&
                event.payload.ordinal === 2 &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("next:" + threadId),
            threadId,
            messageId: MessageId.make("next:" + threadId),
            text: "inherit saved choice",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          });
          yield* Fiber.join(nextDone);
          yield* worker.drain();
          const captured = yield* Ref.get(state);
          assert.deepEqual(captured.started[1]?.selection, replacement);
          assert.lengthOf(captured.opened, restartRequired ? 2 : 1);
          assert.equal(captured.closedSessionCount, restartRequired ? 1 : 0);
          if (restartRequired) assert.equal(captured.opened[1]?.closedBeforeOpen, 1);
        }),
    ),
);

it.live(
  "rejects stale execution/revision, invalid catalog values and non-thread callers without effects",
  () =>
    scenario("self-configure-refusals", false, ({ call, scope, state }) =>
      Effect.gen(function* () {
        const evidence = yield* decodeEvidence(
          (yield* call("t3_thread_configuration")).structuredContent,
        );
        const request = {
          clientRequestId: "invalid",
          modelSelection: replacement,
          expectedConfigurationRevision: evidence.configurationRevision,
          executionPrecondition: evidence.executionPrecondition,
        };
        for (const modelSelection of [
          { ...replacement, instanceId: "unknown-provider" },
          { ...replacement, model: "unknown-model" },
          { ...replacement, options: [{ id: "unknown", value: true }] },
          { ...replacement, options: [{ id: "serviceTier", value: "nope" }] },
          {
            ...replacement,
            options: [
              { id: "serviceTier", value: "priority" },
              { id: "serviceTier", value: "default" },
            ],
          },
        ])
          assert.equal(
            (yield* call("t3_thread_self_configure", { ...request, modelSelection })).isError,
            true,
          );
        assert.include(
          failure(
            yield* call("t3_thread_self_configure", {
              ...request,
              expectedConfigurationRevision: 0,
            }),
          ).message,
          "configuration_changed",
        );
        assert.include(
          failure(
            yield* call("t3_thread_self_configure", {
              ...request,
              executionPrecondition: {
                ...evidence.executionPrecondition,
                runtimeGeneration: "stale",
              },
            }),
          ).message,
          "execution_changed",
        );
        assert.include(
          failure(
            yield* call("t3_thread_self_configure", {
              ...request,
              executionPrecondition: {
                ...evidence.executionPrecondition,
                attemptId: "stale-attempt",
              },
            }),
          ).message,
          "execution_changed",
        );
        assert.equal(
          (yield* call("t3_thread_self_configure", request, {
            ...scope,
            thread: undefined,
            client: { sessionId: "client", label: "test", access: "full-access" },
          })).isError,
          true,
        );
        assert.notEqual((yield* call("t3_thread_organize", { action: "pin" })).isError, true);
        assert.equal(
          (yield* decodeEvidence((yield* call("t3_thread_configuration")).structuredContent))
            .configurationRevision,
          evidence.configurationRevision,
        );
        const explicitDefaults = {
          ...replacement,
          options: [
            { id: "reasoningEffort", value: "default" },
            { id: "serviceTier", value: "default" },
          ],
        };
        const accepted = yield* decodeReceipt(
          (yield* call("t3_thread_self_configure", {
            ...request,
            clientRequestId: "defaults",
            modelSelection: explicitDefaults,
          })).structuredContent,
        );
        const cleared = yield* decodeReceipt(
          (yield* call("t3_thread_self_configure", {
            ...request,
            clientRequestId: "clear",
            modelSelection: replacementSelection,
            expectedConfigurationRevision: accepted.freshObservation.configurationRevision,
          })).structuredContent,
        );
        assert.isUndefined(cleared.acceptance.committedSelection.options);
        assert.equal((yield* Ref.get(state)).closedSessionCount, 0);
      }),
    ),
);

it.live("old captured queue delivery and implicit steering preserve newer saved defaults", () =>
  scenario("self-configure-queue", false, ({ threadId, call, state }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* Worker.OrchestrationEffectWorkerV2;
      const sink = yield* Sink.EventSinkV2;
      const dispatch = (
        id: string,
        dispatchMode:
          | { type: "queue_after_active" }
          | { type: "steer_active"; targetRunId: import("@t3tools/contracts").RunId },
      ) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(id),
          threadId,
          messageId: MessageId.make(id),
          text: id,
          attachments: [],
          createdBy: "system",
          creationSource: "server",
          dispatchMode,
        });
      yield* dispatch("old-system-completion", { type: "queue_after_active" });
      const before = yield* decodeEvidence(
        (yield* call("t3_thread_configuration")).structuredContent,
      );
      yield* call("t3_thread_self_configure", {
        clientRequestId: "queue-save",
        modelSelection: replacement,
        expectedConfigurationRevision: before.configurationRevision,
        executionPrecondition: before.executionPrecondition,
      });
      yield* dispatch("implicit-steer", {
        type: "steer_active",
        targetRunId: before.executionPrecondition!.runId,
      });
      yield* worker.drain();
      let projection = yield* orchestrator.getThreadProjection(threadId);
      assert.lengthOf(projection.runs, 2);
      assert.deepEqual(projection.runs[0]?.modelSelection, initialSelection);
      assert.deepEqual(projection.runs[1]?.modelSelection, initialSelection);
      assert.deepEqual(projection.thread.modelSelection, replacement);
      yield* dispatch("new-system-completion", { type: "queue_after_active" });
      const oldStarted = yield* sink.stream({ threadId }).pipe(
        Stream.filter(
          ({ event }) =>
            event.type === "provider-turn.updated" &&
            event.payload.runAttemptId === projection.runs[1]?.activeAttemptId &&
            event.payload.status === "running",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* finishActive(state);
      yield* Fiber.join(oldStarted);
      yield* worker.drain();
      projection = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(projection.thread.modelSelection, replacement);
      assert.deepEqual((yield* Ref.get(state)).started[1]?.selection, initialSelection);
      const finalDone = yield* sink.stream({ threadId }).pipe(
        Stream.filter(
          ({ event }) =>
            event.type === "run.updated" &&
            event.payload.ordinal === 3 &&
            event.payload.status === "completed",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* finishActive(state);
      yield* Fiber.join(finalDone);
      yield* worker.drain();
      projection = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(projection.thread.modelSelection, replacement);
      assert.deepEqual((yield* Ref.get(state)).started[2]?.selection, replacement);
      assert.isTrue(projection.runs.every((run) => run.status === "completed"));
    }),
  ),
);

it.live("owner edit wins over retry; detached runtime cannot authorize a new save", () =>
  scenario("self-configure-owner", false, ({ threadId, call, state }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sessions = yield* Sessions.ProviderSessionManagerV2;
      const before = yield* decodeEvidence(
        (yield* call("t3_thread_configuration")).structuredContent,
      );
      const request = {
        clientRequestId: "owner-save",
        modelSelection: replacement,
        expectedConfigurationRevision: before.configurationRevision,
        executionPrecondition: before.executionPrecondition,
      };
      const accepted = yield* decodeReceipt(
        (yield* call("t3_thread_self_configure", request)).structuredContent,
      );
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("owner-choice"),
        threadId,
        modelSelection: initialSelection,
      });
      const retry = yield* decodeReceipt(
        (yield* call("t3_thread_self_configure", request)).structuredContent,
      );
      assert.deepEqual(retry.acceptance, accepted.acceptance);
      assert.deepEqual(retry.freshObservation.savedSelection, initialSelection);
      const opened = (yield* Ref.get(state)).opened.length;
      const sink = yield* Sink.EventSinkV2;
      const worker = yield* Worker.OrchestrationEffectWorkerV2;
      yield* sessions.detach({
        providerSessionId: before.executionPrecondition!.providerSessionId,
        threadId,
        revokeMcpCredential: true,
      });
      yield* worker.drain();
      const stranded = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
      yield* sink.write({
        events: [
          {
            id: EventId.make("synthetic-runtime-not-resident"),
            type: "run.updated",
            threadId,
            runId: stranded.id,
            occurredAt: yield* DateTime.now,
            payload: { ...stranded, status: "starting" },
          },
        ],
      });
      const unavailable = yield* decodeEvidence(
        (yield* call("t3_thread_configuration")).structuredContent,
      );
      assert.isNull(unavailable.executionPrecondition);
      const stale = yield* call("t3_thread_self_configure", {
        ...request,
        clientRequestId: "detached-save",
        expectedConfigurationRevision: retry.freshObservation.configurationRevision,
      });
      assert.include(failure(stale).message, "execution_changed");
      assert.equal((yield* Ref.get(state)).opened.length, opened);
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
        initialSelection,
      );
    }),
  ),
);

it.live.each([
  { driver: "codex", compatible: true },
  { driver: "codex", compatible: false },
  { driver: "claude-code", compatible: false },
  { driver: "claude-code", compatible: false, enabled: false },
] as const)(
  "safe save respects account/handoff eligibility %j",
  (target) =>
    scenario(
      "self-configure-target-" +
        target.driver +
        "-" +
        target.compatible +
        "-" +
        ("enabled" in target ? target.enabled : true),
      false,
      ({ threadId, call, state }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const sink = yield* Sink.EventSinkV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          const before = yield* decodeEvidence(
            (yield* call("t3_thread_configuration")).structuredContent,
          );
          const selection = {
            ...replacement,
            instanceId: ProviderInstanceId.make("target-provider"),
          };
          const response = yield* call("t3_thread_self_configure", {
            clientRequestId: "target-save",
            modelSelection: selection,
            expectedConfigurationRevision: before.configurationRevision,
            executionPrecondition: before.executionPrecondition,
          });
          if (
            (target.driver === "codex" && !target.compatible) ||
            ("enabled" in target && !target.enabled)
          ) {
            assert.equal(response.isError, true);
            assert.deepEqual(
              (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
              initialSelection,
            );
            assert.lengthOf((yield* Ref.get(state)).opened, 1);
            return;
          }
          const receipt = yield* decodeReceipt(response.structuredContent);
          assert.deepEqual(receipt.acceptance.committedSelection, selection);
          assert.notEqual((yield* call("t3_thread_organize", { action: "pin" })).isError, true);
          assert.equal((yield* Ref.get(state)).closedSessionCount, 0);
          const firstDone = yield* sink.stream({ threadId }).pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "run.updated" &&
                event.payload.ordinal === 1 &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* finishActive(state);
          yield* Fiber.join(firstDone);
          yield* worker.drain();
          const nextDone = yield* sink.stream({ threadId }).pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "run.updated" &&
                event.payload.ordinal === 2 &&
                ["completed", "failed"].includes(event.payload.status),
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("target-next:" + threadId),
            threadId,
            messageId: MessageId.make("target-next:" + threadId),
            text: "inherit target",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          });
          yield* Fiber.join(nextDone);
          yield* worker.drain();
          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(projection.runs[1]?.status, "completed");
          assert.deepEqual((yield* Ref.get(state)).started[1]?.selection, selection);
          assert.equal((yield* Ref.get(state)).closedSessionCount, 1);
          assert.deepEqual(projection.thread.modelSelection, selection);
          assert.equal(projection.contextHandoffs.length > 0, target.driver !== "codex");
        }),
      target,
    ),
  30_000,
);

it.live(
  "Stop and server reconstruction preserve saved defaults and receipts without an apply job",
  () => {
    let request: Record<string, unknown>;
    let acceptance: ThreadSelfConfigurationResult["acceptance"];
    return scenario(
      "self-configure-restart",
      false,
      ({ threadId, call, state }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const sink = yield* Sink.EventSinkV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("recovery-old-queue"),
            threadId,
            messageId: MessageId.make("recovery-old-queue"),
            text: "old captured recovery work",
            attachments: [],
            createdBy: "system",
            creationSource: "server",
            dispatchMode: { type: "queue_after_active" },
          });
          const before = yield* decodeEvidence(
            (yield* call("t3_thread_configuration")).structuredContent,
          );
          request = {
            clientRequestId: "restart-save",
            modelSelection: replacement,
            expectedConfigurationRevision: before.configurationRevision,
            executionPrecondition: before.executionPrecondition,
          };
          acceptance = (yield* decodeReceipt(
            (yield* call("t3_thread_self_configure", request)).structuredContent,
          )).acceptance;
          const stopped = yield* sink.stream({ threadId }).pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "run.updated" &&
                event.payload.ordinal === 1 &&
                event.payload.status === "interrupted",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("owner-stop"),
            threadId,
            runId: before.executionPrecondition!.runId,
            holdQueue: true,
          });
          yield* Fiber.join(stopped);
          yield* worker.drain();
          assert.lengthOf((yield* Ref.get(state)).started, 1);
          assert.deepEqual(
            (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
            replacement,
          );
        }),
      undefined,
      ({ threadId, call, state }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          yield* worker.drain();
          const retry = yield* decodeReceipt(
            (yield* call("t3_thread_self_configure", request)).structuredContent,
          );
          assert.deepEqual(retry.acceptance, acceptance);
          assert.deepEqual(retry.freshObservation.savedSelection, replacement);
          assert.isNull(retry.freshObservation.executionPrecondition);
          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.lengthOf(projection.runs, 2);
          assert.equal(projection.runs[0]?.status, "interrupted");
          assert.deepEqual(projection.runs[1]?.modelSelection, initialSelection);
          assert.lengthOf((yield* Ref.get(state)).started, 1);
          const stale = yield* call("t3_thread_self_configure", {
            ...request,
            clientRequestId: "after-restart",
          });
          assert.include(failure(stale).message, "execution_changed");
        }),
    );
  },
);

it.live("restart-required selection retires only the caller attachment in a pooled runtime", () =>
  scenario(
    "self-configure-pooled",
    true,
    ({ threadId, call, state }) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* Worker.OrchestrationEffectWorkerV2;
        const sink = yield* Sink.EventSinkV2;
        const sessions = yield* Sessions.ProviderSessionManagerV2;
        const before = yield* decodeEvidence(
          (yield* call("t3_thread_configuration")).structuredContent,
        );
        const source = yield* orchestrator.getThreadProjection(threadId);
        const sibling = ThreadId.make("self-configure-pooled-sibling");
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("pooled-sibling-create"),
          threadId: sibling,
          projectId: source.thread.projectId,
          title: "Sibling",
          modelSelection: replacementSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: source.thread.worktreePath,
          createdBy: "user",
          creationSource: "web",
        });
        const siblingDone = yield* sink.stream({ threadId: sibling }).pipe(
          Stream.filter(
            ({ event }) => event.type === "run.updated" && event.payload.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("pooled-sibling-start"),
          threadId: sibling,
          messageId: MessageId.make("pooled-sibling-start"),
          text: "sibling",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          dispatchMode: { type: "start_immediately" },
        });
        yield* Fiber.join(siblingDone);
        yield* worker.drain();
        const siblingProjection = yield* orchestrator.getThreadProjection(sibling);
        const siblingSession = siblingProjection.providerThreads[0]!.providerSessionId!;
        assert.equal(siblingSession, before.executionPrecondition!.providerSessionId);
        assert.lengthOf((yield* Ref.get(state)).opened, 1);
        yield* decodeReceipt(
          (yield* call("t3_thread_self_configure", {
            clientRequestId: "pooled-save",
            modelSelection: replacement,
            expectedConfigurationRevision: before.configurationRevision,
            executionPrecondition: before.executionPrecondition,
          })).structuredContent,
        );
        const firstDone = yield* sink.stream({ threadId }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.ordinal === 1 &&
              event.payload.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* finishActive(state);
        yield* Fiber.join(firstDone);
        yield* worker.drain();
        const nextDone = yield* sink.stream({ threadId }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.ordinal === 2 &&
              event.payload.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("pooled-next"),
          threadId,
          messageId: MessageId.make("pooled-next"),
          text: "next",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          dispatchMode: { type: "start_immediately" },
        });
        yield* Fiber.join(nextDone);
        yield* worker.drain();
        assert.lengthOf((yield* Ref.get(state)).opened, 2);
        assert.equal((yield* Ref.get(state)).closedSessionCount, 0);
        assert.isTrue(
          yield* sessions.isMcpCallerAttached({
            threadId: sibling,
            providerSessionId: siblingSession,
            providerInstanceId,
            mcpCredentialId: "mcp-test:" + sibling,
          }),
        );
        assert.notEqual(
          (yield* orchestrator.getThreadProjection(threadId)).providerThreads[0]?.providerSessionId,
          siblingSession,
        );
      }),
    undefined,
    undefined,
    true,
  ),
);

it.live("explicit owner queue choices save at acceptance; automatic captured choices do not", () =>
  scenario("self-configure-explicit-queue", false, ({ threadId, call }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const before = yield* decodeEvidence(
        (yield* call("t3_thread_configuration")).structuredContent,
      );
      yield* decodeReceipt(
        (yield* call("t3_thread_self_configure", {
          clientRequestId: "explicit-save",
          modelSelection: replacement,
          expectedConfigurationRevision: before.configurationRevision,
          executionPrecondition: before.executionPrecondition,
        })).structuredContent,
      );
      const dispatch = (
        id: string,
        modelSelection: ModelSelection,
        creationSource: "server" | "web",
      ) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(id),
          threadId,
          messageId: MessageId.make(id),
          text: id,
          attachments: [],
          modelSelection,
          createdBy: creationSource === "server" ? "system" : "user",
          creationSource,
          dispatchMode: { type: "queue_after_active" },
        });
      yield* dispatch("automatic-old-capture", initialSelection, "server");
      let projection = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(projection.thread.modelSelection, replacement);
      assert.deepEqual(projection.runs[1]?.modelSelection, initialSelection);
      const ownerChoice = {
        ...replacementSelection,
        options: [{ id: "serviceTier", value: "default" }],
      };
      yield* dispatch("explicit-owner-choice", ownerChoice, "web");
      projection = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(projection.thread.modelSelection, ownerChoice);
      assert.deepEqual(projection.runs[2]?.modelSelection, ownerChoice);
      assert.deepEqual(projection.runs[0]?.modelSelection, initialSelection);
    }),
  ),
);
