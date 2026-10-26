import { withOwnerSelectionIngress } from "./ownerIngress.ts";
import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
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
  type OrchestrationV2ProviderCapabilities,
  type ServerProvider,
  ThreadConfigurationEvidence,
  ThreadSelfConfigurationResult,
  OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Ref from "effect/Ref";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import { McpServer, McpSchema } from "effect/ai";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { ClaudeProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Catalog from "../../provider/ProviderRegistry.ts";
import * as ThreadSearch from "../../orchestration-v2/ThreadSearch.ts";
import * as Scheduled from "../../scheduledTasks/ScheduledTaskService.ts";
import * as Peer from "../../peer/PeerForwarding.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Mcp from "../../mcp/McpHttpServer.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import { continueRestartedRun } from "../../orchestration-v2/RestartContinuation.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Settings from "../../serverSettings.ts";
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
const decodeCommand = Schema.decodeUnknownEffect(OrchestrationV2Command);

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
    sql: SqlClient.SqlClient;
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
    | Outbox.EffectOutboxV2
    | Threads.ThreadManagementService
    | Settings.ServerSettingsService
  >,
  target?: {
    readonly driver: "codex" | "claude-code";
    readonly compatible: boolean;
    readonly enabled?: boolean;
  },
  afterRestart?: typeof body,
  pooled = false,
  source?: {
    readonly driver: "codex" | "claude-code";
    readonly capabilities: OrchestrationV2ProviderCapabilities;
  },
  preparing = false,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(name);
      const database = yield* Layer.build(Sqlite.layerMemory);
      const sql = Context.get(database, SqlClient.SqlClient);
      const scratch =
        afterRestart === undefined
          ? undefined
          : yield* Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem;
              const path = yield* Path.Path;
              return path.join(
                yield* fs.makeTempDirectoryScoped({ prefix: "owner-crash-" }),
                "pre-crash.sqlite",
              );
            }).pipe(Effect.provide(NodeServices.layer));
      let recoverFromSnapshot = false;
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
          source?.capabilities ?? (pooled ? CodexProviderCapabilitiesV2 : undefined),
          providerInstanceId,
          restartRequired,
          source === undefined ? driver : ProviderDriverKind.make(source.driver),
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
        getMetadata: Effect.fn("ThreadConfigurationTest.getMetadata")(function* (id) {
          const adapter = adapters.find((a) => a.instanceId === id)!;
          return {
            driver: adapter.driver,
            continuationKey: target?.compatible === false ? "home:" + id : "home:shared",
            enabled: true,
            capabilities: yield* adapter.getCapabilities(),
          };
        }, Effect.orDie),
      });
      const runtime = Harness.layerWithRegistryAndServices(
        { name, runtimePolicyOverride: { cwd } },
        registry,
        {
          configureMcp: true,
          databaseLayer: Layer.unwrap(
            Effect.sync(() =>
              recoverFromSnapshot && scratch !== undefined
                ? NodeSqliteClient.layer({ filename: scratch })
                : Layer.succeed(SqlClient.SqlClient, sql),
            ),
          ),
          recoverOnStartup: afterRestart !== undefined,
          onSessionDetached: (input) =>
            Ref.update(state, (current) => ({
              ...current,
              detached: [...(current.detached ?? []), input],
            })),
        },
      );
      const service = Service.layer.pipe(
        Layer.provide(runtime),
        Layer.provide(
          Layer.mock(Catalog.ProviderRegistry)({
            getProviders: Effect.succeed([
              {
                ...catalog,
                driver: source === undefined ? driver : ProviderDriverKind.make(source.driver),
              },
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
        const running = preparing
          ? undefined
          : yield* sink.stream({ threadId }).pipe(
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
          dispatchMode: preparing ? { type: "defer_start" } : { type: "start_immediately" },
        });
        if (running !== undefined) yield* Fiber.join(running);
        yield* worker.drain();
        yield* body({ threadId, scope, state, call, sql });
        if (scratch !== undefined) {
          // Capture committed crash state before scoped shutdown can persist stopped sessions.
          yield* sql`VACUUM INTO ${scratch}`;
        }
      }).pipe(Effect.provide(Layer.fresh(Layer.merge(runtime, tools))));
      if (afterRestart !== undefined) {
        recoverFromSnapshot = true;
        yield* Effect.gen(function* () {
          const recoveredSql = yield* SqlClient.SqlClient;
          const server = yield* McpServer.McpServer;
          const call = (name: string, args: Record<string, unknown> = {}, caller = scope) =>
            server
              .callTool({ name, arguments: args })
              .pipe(
                Effect.provideService(Invocation.McpInvocationContext, caller),
                Effect.provideService(McpSchema.McpServerClient, client),
                Effect.orDie,
              );
          yield* afterRestart({ threadId, scope, state, call, sql: recoveredSql });
        }).pipe(Effect.provide(Layer.fresh(Layer.merge(runtime, tools))));
      }
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

it.live.each(["implicit", "notification"] as const)(
  "old captured queue delivery and %s steering preserve newer saved defaults",
  (steeringSource) =>
    scenario("self-configure-queue-" + steeringSource, false, ({ threadId, call, state }) =>
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
            ...(dispatchMode.type === "steer_active" && steeringSource === "notification"
              ? {
                  modelSelection: replacement,
                  createdBy: "agent" as const,
                  modelSelectionExplicit: true,
                  notification: {
                    source: { kind: "monitor" as const },
                    outcome: "updated" as const,
                    summary: "Synthetic notification after saved selection changed",
                  },
                }
              : {}),
            dispatchMode:
              dispatchMode.type === "steer_active" && steeringSource === "notification"
                ? { type: "queue_after_active" }
                : dispatchMode,
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
        if (steeringSource === "notification") {
          const active = yield* orchestrator.getThreadProjection(threadId);
          const providerThread = active.providerThreads[0]!;
          yield* sink.write({
            events: [
              {
                id: EventId.make("self-configure-active-goal"),
                type: "provider-thread.updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: {
                  ...providerThread,
                  goal: { objective: "Synthetic goal", status: "active", tokensUsed: 0 },
                },
              },
            ],
          });
        }
        yield* dispatch("implicit-steer", {
          type: "steer_active",
          targetRunId: before.executionPrecondition!.runId,
        });
        yield* worker.drain();
        let projection = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(projection.runs, 2);
        assert.equal(projection.runs[0]?.activeAttemptId, before.executionPrecondition!.attemptId);
        assert.lengthOf(
          projection.attempts.filter(
            (attempt) => attempt.runId === before.executionPrecondition!.runId,
          ),
          1,
        );
        assert.equal((yield* Ref.get(state)).closedSessionCount, 0);
        assert.equal(
          projection.messages.find((message) => message.id === "implicit-steer")?.notification
            ?.summary,
          steeringSource === "notification"
            ? "Synthetic notification after saved selection changed"
            : undefined,
        );
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

it.live("owner provider selection can leave an active Claude run on the next send", () =>
  scenario(
    "self-configure-owner-leaves-claude",
    false,
    ({ threadId, state }) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* Worker.OrchestrationEffectWorkerV2;
        const sink = yield* Sink.EventSinkV2;
        const active = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        const ownerSelection: ModelSelection = {
          ...replacement,
          instanceId: ProviderInstanceId.make("target-provider"),
        };
        const replacementDone = yield* sink.stream({ threadId }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === active.id &&
              event.providerInstanceId === ownerSelection.instanceId &&
              event.payload.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* orchestrator
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("owner-leaves-active-claude"),
            threadId,
            messageId: MessageId.make("owner-leaves-active-claude"),
            text: "Continue with the provider I selected",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            modelSelection: ownerSelection,
            modelSelectionExplicit: true,
            dispatchMode: { type: "steer_active", targetRunId: active.id },
          })
          .pipe(withOwnerSelectionIngress);
        yield* Fiber.join(replacementDone);
        yield* worker.drain();
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(after.thread.modelSelection, ownerSelection);
        assert.deepEqual((yield* Ref.get(state)).started[1]?.selection, ownerSelection);
        assert.equal(after.messages.filter((m) => m.id === "owner-leaves-active-claude").length, 1);
        assert.equal(after.attempts.filter((attempt) => attempt.status === "running").length, 0);
        assert.equal(after.runs[0]?.status, "completed");
        assert.equal(
          after.attempts.find((attempt) => attempt.id === after.runs[0]?.activeAttemptId)?.status,
          "completed",
        );
      }),
    { driver: "codex", compatible: false },
    undefined,
    false,
    { driver: "claude-code", capabilities: ClaudeProviderCapabilitiesV2 },
  ),
);

it.live("owner selection supersedes an older agent queue selection at execution", () =>
  scenario("self-configure-owner-queue-override", false, ({ threadId, call, state }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* Sink.EventSinkV2;
      const worker = yield* Worker.OrchestrationEffectWorkerV2;
      const before = yield* decodeEvidence(
        (yield* call("t3_thread_configuration")).structuredContent,
      );
      yield* call("t3_thread_self_configure", {
        clientRequestId: "agent-save-before-owner",
        modelSelection: replacement,
        expectedConfigurationRevision: before.configurationRevision,
        executionPrecondition: before.executionPrecondition,
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("agent-queue-before-owner"),
        threadId,
        messageId: MessageId.make("agent-queue-before-owner"),
        text: "Older agent work must use the subsequent owner selection",
        attachments: [],
        createdBy: "agent",
        creationSource: "mcp",
        modelSelection: replacement,
        modelSelectionExplicit: true,
        dispatchMode: { type: "queue_after_active" },
      });
      const ownerSelection: ModelSelection = {
        ...initialSelection,
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "serviceTier", value: "default" },
        ],
      };
      yield* orchestrator
        .dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make("owner-overrides-agent-queue"),
          threadId,
          modelSelection: ownerSelection,
        })
        .pipe(withOwnerSelectionIngress);
      const queued = (yield* orchestrator.getThreadProjection(threadId)).runs[1]!;
      const started = yield* sink.stream({ threadId }).pipe(
        Stream.filter(
          ({ event }) =>
            event.type === "provider-turn.updated" &&
            event.payload.runAttemptId === queued.activeAttemptId &&
            (event.payload.status === "running" || event.payload.status === "completed"),
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* finishActive(state);
      yield* Fiber.join(started);
      yield* worker.drain();
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(after.thread.modelSelection, ownerSelection);
      assert.deepEqual((yield* Ref.get(state)).started[1]?.selection, ownerSelection);
      assert.deepEqual(after.runs[1]?.modelSelection, ownerSelection);
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
  { driver: "codex", compatible: true, explicit: true },
  { driver: "codex", compatible: true, explicit: undefined },
  { driver: "codex", compatible: false },
  { driver: "claude-code", compatible: false },
  { driver: "claude-code", compatible: false, enabled: false },
] as const)("safe save respects account/handoff eligibility %j", (target) =>
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
        if (target.driver === "codex" && target.compatible) {
          // The web composer includes its inherited selection even without a picker change.
          const explicit = "explicit" in target ? target.explicit : false;
          yield* orchestrator.dispatch(
            yield* decodeCommand({
              type: "message.dispatch",
              commandId: CommandId.make("ordinary-follow-up:" + threadId),
              threadId,
              messageId: MessageId.make("ordinary-follow-up:" + threadId),
              text: "ordinary web follow-up",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
              modelSelection: selection,
              ...(explicit === undefined ? {} : { modelSelectionExplicit: explicit }),
              deliveryIntent: "auto",
              dispatchMode: { type: "start_immediately" },
            }),
          );
          yield* worker.drain();
          const afterFollowUp = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(afterFollowUp.runs[0]?.id, before.executionPrecondition!.runId);
          if (explicit !== false) {
            assert.notEqual(
              afterFollowUp.runs[0]?.activeAttemptId,
              before.executionPrecondition!.attemptId,
            );
            assert.lengthOf(afterFollowUp.attempts, 2);
            assert.deepEqual(afterFollowUp.runs[0]?.modelSelection, selection);
            assert.deepEqual((yield* Ref.get(state)).started[1]?.selection, selection);
            return;
          }
          assert.equal(
            afterFollowUp.runs[0]?.activeAttemptId,
            before.executionPrecondition!.attemptId,
          );
          assert.lengthOf(afterFollowUp.attempts, 1);
          assert.equal(afterFollowUp.runs[0]?.status, "running");
          assert.equal((yield* Ref.get(state)).closedSessionCount, 0);
          assert.lengthOf((yield* Ref.get(state)).opened, 1);
          assert.notEqual((yield* call("t3_thread_organize", { action: "pin" })).isError, true);
        }
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
        const completed = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(completed.runs[0]?.id, before.executionPrecondition!.runId);
        assert.equal(completed.runs[0]?.activeAttemptId, before.executionPrecondition!.attemptId);
        assert.equal(completed.attempts[0]?.status, "completed");
        assert.isTrue(
          completed.turnItems.some(
            (item) =>
              item.type === "assistant_message" &&
              item.text === "continued-after-self-configuration",
          ),
        );
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
          modelSelection: initialSelection,
          modelSelectionExplicit: false,
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
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.notEqual(after.providerThreads[0]?.providerSessionId, siblingSession);
        assert.isFalse(after.providerSessions.some((session) => session.id === siblingSession));
        assert.isTrue(
          (yield* orchestrator.getThreadProjection(sibling)).providerSessions.some(
            (session) => session.id === siblingSession,
          ),
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

// These barriers observe durable lifecycle facts; no test depends on a sleep.
const observeRun = (
  threadId: ThreadId,
  ordinal: number,
  status: "running" | "completed" | "failed",
) =>
  Effect.gen(function* () {
    const sink = yield* Sink.EventSinkV2;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    return yield* sink.stream({ threadId }).pipe(
      Stream.filterEffect(({ event }) => {
        if (status !== "running")
          return Effect.succeed(
            event.type === "run.updated" &&
              event.payload.ordinal === ordinal &&
              event.payload.status === status,
          );
        if (event.type !== "provider-turn.updated" || event.payload.status !== "running")
          return Effect.succeed(false);
        const attemptId = event.payload.runAttemptId;
        return orchestrator
          .getThreadProjection(threadId)
          .pipe(
            Effect.map((projection) =>
              projection.runs.some(
                (run) => run.ordinal === ordinal && run.activeAttemptId === attemptId,
              ),
            ),
          );
      }),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  });
const ownerRows = (sql: SqlClient.SqlClient, threadId: ThreadId) =>
  sql<{ command_id: string; result_sequence: number; model_selection_json: string }>`
    SELECT command_id, result_sequence, model_selection_json FROM jones_owner_selections WHERE thread_id = ${threadId}`;
const countEffects = (sql: SqlClient.SqlClient, threadId: ThreadId) =>
  sql<{
    count: number;
  }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE thread_id = ${threadId}`.pipe(
    Effect.map((rows) => rows[0]!.count),
  );
const messageCommand = (threadId: ThreadId, id: string) => ({
  type: "message.dispatch" as const,
  commandId: CommandId.make(id),
  threadId,
  messageId: MessageId.make(id),
  text: id,
  attachments: [],
  createdBy: "user" as const,
  creationSource: "web" as const,
  dispatchMode: { type: "queue_after_active" as const },
});

it.live.each(["codex", "claude-code"] as const)(
  "A2 owner setter keeps the admitted %s session and the implicit send transitions the full tuple",
  (sourceDriver) =>
    scenario(
      "owner-implicit-" + sourceDriver,
      true,
      ({ threadId, state, sql }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          const initial = yield* orchestrator.getThreadProjection(threadId);
          yield* orchestrator
            .dispatch({
              type: "thread.model-selection.set",
              commandId: CommandId.make("owner:C"),
              threadId,
              modelSelection: replacement,
            })
            .pipe(withOwnerSelectionIngress);
          yield* worker.drain();
          assert.deepEqual((yield* orchestrator.getThreadProjection(threadId)).runs, initial.runs);
          assert.equal((yield* Ref.get(state)).closedSessionCount, 0);
          const done = yield* observeRun(threadId, 1, "completed");
          yield* orchestrator
            .dispatch({
              ...messageCommand(threadId, "implicit-owner:C"),
              deliveryIntent: "auto",
              modelSelection: initialSelection,
              modelSelectionExplicit: false,
            })
            .pipe(withOwnerSelectionIngress);
          yield* Fiber.join(done);
          yield* worker.drain();
          const invoked = yield* Ref.get(state);
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(invoked.started[0]?.selection, initialSelection);
          assert.deepEqual(invoked.started[1]?.selection, replacement);
          assert.lengthOf(invoked.started, 2);
          assert.lengthOf(invoked.interrupted ?? [], 1);
          assert.equal(
            invoked.interrupted?.[0]?.providerThread.id,
            initial.runs[0]!.providerThreadId,
          );
          assert.notEqual(invoked.started[1]?.sessionId, invoked.started[0]?.sessionId);
          assert.lengthOf(invoked.detached ?? [], 1);
          assert.equal(invoked.detached?.[0]?.threadId, threadId);
          assert.equal(invoked.detached?.[0]?.providerSessionId, invoked.started[0]?.sessionId);
          assert.equal(invoked.closedSessionCount, sourceDriver === "codex" ? 0 : 1);
          assert.lengthOf(after.runs, 1);
          assert.lengthOf(after.attempts, 2);
          assert.equal((yield* ownerRows(sql, threadId))[0]?.command_id, "owner:C");
        }),
      undefined,
      undefined,
      false,
      {
        driver: sourceDriver,
        capabilities:
          sourceDriver === "codex" ? CodexProviderCapabilitiesV2 : ClaudeProviderCapabilitiesV2,
      },
    ),
);

it.live("A3 agent-only save remains prospective for an implicit owner send", () =>
  scenario("agent-save-owner-implicit", true, ({ threadId, call, state, sql }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* Worker.OrchestrationEffectWorkerV2;
      const before = yield* decodeEvidence(
        (yield* call("t3_thread_configuration")).structuredContent,
      );
      yield* decodeReceipt(
        (yield* call("t3_thread_self_configure", {
          clientRequestId: "agent:B",
          modelSelection: replacement,
          expectedConfigurationRevision: before.configurationRevision,
          executionPrecondition: before.executionPrecondition,
        })).structuredContent,
      );
      yield* orchestrator
        .dispatch({
          ...messageCommand(threadId, "owner:implicit-no-pending"),
          deliveryIntent: "auto",
          modelSelection: replacement,
          modelSelectionExplicit: false,
        })
        .pipe(withOwnerSelectionIngress);
      yield* worker.drain();
      const after = yield* orchestrator.getThreadProjection(threadId);
      const invoked = yield* Ref.get(state);
      assert.lengthOf(after.attempts, 1);
      assert.lengthOf(invoked.started, 1);
      assert.lengthOf(invoked.steered ?? [], 1);
      assert.equal(
        invoked.steered?.[0]?.providerThread.id,
        before.executionPrecondition!.providerThreadId,
      );
      assert.equal(invoked.steered?.[0]?.runId, before.executionPrecondition!.runId);
      assert.lengthOf(invoked.interrupted ?? [], 0);
      assert.equal(invoked.closedSessionCount, 0);
      assert.deepEqual(after.thread.modelSelection, replacement);
      assert.lengthOf(yield* ownerRows(sql, threadId), 0);
    }),
  ),
);

it.live.each(["internal", "peer"] as const)(
  "A1 an unattributed %s setter cannot supersede agent-captured queued work",
  (setter) =>
    scenario("non-owner-setter-" + setter, false, ({ threadId, call, state, sql, scope }) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* Worker.OrchestrationEffectWorkerV2;
        const before = yield* decodeEvidence(
          (yield* call("t3_thread_configuration")).structuredContent,
        );
        yield* decodeReceipt(
          (yield* call("t3_thread_self_configure", {
            clientRequestId: "agent:B",
            modelSelection: replacement,
            expectedConfigurationRevision: before.configurationRevision,
            executionPrecondition: before.executionPrecondition,
          })).structuredContent,
        );
        yield* orchestrator.dispatch({
          ...messageCommand(threadId, "agent:queued:B"),
          createdBy: "agent",
          creationSource: "mcp",
          modelSelection: replacement,
        });
        if (setter === "peer") {
          const mainActive = (yield* Ref.get(state)).activeTurn;
          const sibling = ThreadId.make(threadId + ":peer");
          const projection = yield* orchestrator.getThreadProjection(threadId);
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("peer:create"),
            threadId: sibling,
            projectId: projection.thread.projectId,
            title: "Peer",
            modelSelection: initialSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: projection.thread.worktreePath,
            createdBy: "agent",
            creationSource: "mcp",
          });
          const running = yield* observeRun(sibling, 1, "running");
          yield* orchestrator.dispatch({
            ...messageCommand(sibling, "peer:start"),
            dispatchMode: { type: "start_immediately" },
          });
          yield* Fiber.join(running);
          yield* worker.drain();
          yield* Ref.update(state, (s) => ({ ...s, activeTurn: mainActive }));
          const result = yield* call(
            "t3_thread_configure",
            { threadId, modelSelection: initialSelection },
            {
              ...scope,
              thread: {
                ...scope.thread!,
                threadId: sibling,
                providerSessionId: "mcp-test:" + sibling,
              },
            },
          );
          assert.notEqual(result.isError, true);
        } else
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("internal:A"),
            threadId,
            modelSelection: initialSelection,
          });
        const queued = (yield* orchestrator.getThreadProjection(threadId)).runs[1]!;
        assert.deepEqual(queued.modelSelection, replacement);
        assert.lengthOf(yield* ownerRows(sql, threadId), 0);
        const done = yield* observeRun(threadId, 2, "completed");
        yield* finishActive(state);
        yield* Fiber.join(done);
        yield* worker.drain();
        assert.deepEqual((yield* Ref.get(state)).started.at(-1)?.selection, replacement);
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
          initialSelection,
        );
      }),
    ),
);

it.live.each(["setter", "message"] as const)(
  "A4/A10/A12 equal owner %s fences stale CAS, preserves rebound work after fresh D, and monitors follow D",
  (selectionKind) =>
    scenario("equal-owner-" + selectionKind, false, ({ threadId, call, state, sql }) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* Worker.OrchestrationEffectWorkerV2;
        const read = () =>
          call("t3_thread_configuration").pipe(
            Effect.flatMap((r) => decodeEvidence(r.structuredContent)),
          );
        const save = (
          id: string,
          selection: ModelSelection,
          evidence: ThreadConfigurationEvidence,
        ) =>
          call("t3_thread_self_configure", {
            clientRequestId: id,
            modelSelection: selection,
            expectedConfigurationRevision: evidence.configurationRevision,
            executionPrecondition: evidence.executionPrecondition,
          });
        yield* decodeReceipt(
          (yield* save("agent:B", replacement, yield* read())).structuredContent,
        );
        yield* orchestrator.dispatch({
          ...messageCommand(threadId, "queued:B"),
          modelSelection: replacement,
          createdBy: "agent",
          creationSource: "mcp",
        });
        yield* decodeReceipt(
          (yield* save("agent:restore:A", initialSelection, yield* read())).structuredContent,
        );
        const stale = yield* read();
        const command =
          selectionKind === "setter"
            ? {
                type: "thread.model-selection.set" as const,
                commandId: CommandId.make("equal-owner:A"),
                threadId,
                modelSelection: initialSelection,
              }
            : {
                ...messageCommand(threadId, "equal-owner:A"),
                modelSelection: initialSelection,
                modelSelectionExplicit: true,
              };
        const accepted = yield* orchestrator.dispatch(command).pipe(withOwnerSelectionIngress);
        const owner = yield* ownerRows(sql, threadId);
        assert.lengthOf(owner, 1);
        assert.equal(owner[0]!.result_sequence, accepted.sequence);
        assert.isAbove((yield* read()).configurationRevision, stale.configurationRevision);
        const rebound = (yield* orchestrator.getThreadProjection(threadId)).runs[1]!;
        assert.deepEqual(rebound.modelSelection, initialSelection);
        const replay = yield* orchestrator.dispatch(command).pipe(withOwnerSelectionIngress);
        assert.isTrue(replay.replayed);
        assert.equal(replay.sequence, accepted.sequence);
        assert.deepEqual(yield* ownerRows(sql, threadId), owner);
        const sink = yield* Sink.EventSinkV2;
        const duplicateCommit = yield* sink.commitCommand({
          commandId: command.commandId,
          commandType: command.type,
          threadId,
          acceptedAt: yield* DateTime.now,
          events: accepted.storedEvents.map(({ event }) => event),
          effects: [],
          ownerSelection: replacement,
        });
        assert.isFalse(duplicateCommit.committed);
        assert.deepEqual(yield* ownerRows(sql, threadId), owner);
        assert.include(
          failure(yield* save("stale:D", replacementSelection, stale)).message,
          "configuration_changed",
        );
        yield* orchestrator.dispatch({
          ...messageCommand(threadId, "monitor:before-D"),
          createdBy: "agent",
          creationSource: "server",
          notification: {
            source: { kind: "monitor" },
            outcome: "updated",
            summary: "Monitor follows current selection",
          },
        });
        yield* decodeReceipt(
          (yield* save("fresh:D", replacementSelection, yield* read())).structuredContent,
        );
        assert.lengthOf(yield* ownerRows(sql, threadId), 0);
        let projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(projection.runs[1]?.modelSelection, initialSelection);
        yield* orchestrator
          .dispatch({
            ...messageCommand(threadId, "owner:new-inherited-D"),
            modelSelectionExplicit: false,
            modelSelection: initialSelection,
          })
          .pipe(withOwnerSelectionIngress);
        const finalOrdinal = projection.runs.length + 1;
        // The old source and each rebound A run deliberately remain active until this test finishes them.
        const aRunCount = selectionKind === "message" ? 3 : 2;
        for (let ordinal = 1; ordinal < aRunCount; ordinal++) {
          const running = yield* observeRun(threadId, ordinal + 1, "running");
          yield* finishActive(state);
          yield* Fiber.join(running);
          yield* worker.drain();
          assert.deepEqual((yield* Ref.get(state)).started[ordinal]?.selection, initialSelection);
        }
        const done = yield* observeRun(threadId, finalOrdinal, "completed");
        yield* finishActive(state);
        yield* Fiber.join(done);
        yield* worker.drain();
        projection = yield* orchestrator.getThreadProjection(threadId);
        const invoked = yield* Ref.get(state);
        const monitor = projection.messages.find((m) => m.id === "monitor:before-D")!;
        assert.deepEqual(
          invoked.started.find((s) => s.input.runId === monitor.runId)?.selection,
          replacementSelection,
        );
        assert.deepEqual(invoked.started.at(-1)?.selection, replacementSelection);
        assert.deepEqual(projection.thread.modelSelection, replacementSelection);
      }),
    ),
);

it.live.each([false, true])("A5 single promote respects owner pending=%s", (ownerPending) =>
  scenario("single-promote-" + ownerPending, true, ({ threadId, call, state }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* Worker.OrchestrationEffectWorkerV2;
      const before = yield* decodeEvidence(
        (yield* call("t3_thread_configuration")).structuredContent,
      );
      yield* decodeReceipt(
        (yield* call("t3_thread_self_configure", {
          clientRequestId: "agent:B",
          modelSelection: replacement,
          expectedConfigurationRevision: before.configurationRevision,
          executionPrecondition: before.executionPrecondition,
        })).structuredContent,
      );
      yield* orchestrator.dispatch(messageCommand(threadId, "queue:promote"));
      if (ownerPending)
        yield* orchestrator
          .dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("owner:promote:C"),
            threadId,
            modelSelection: replacement,
          })
          .pipe(withOwnerSelectionIngress);
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const done = ownerPending ? yield* observeRun(threadId, 1, "completed") : undefined;
      yield* orchestrator
        .dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("promote:one"),
          threadId,
          queuedRunId: projection.runs[1]!.id,
          targetRunId: projection.runs[0]!.id,
        })
        .pipe(withOwnerSelectionIngress);
      if (done !== undefined) yield* Fiber.join(done);
      yield* worker.drain();
      const invoked = yield* Ref.get(state);
      assert.equal(
        (yield* orchestrator.getThreadProjection(threadId)).runs[1]?.status,
        "cancelled",
      );
      if (ownerPending) {
        assert.deepEqual(invoked.started[1]?.selection, replacement);
        assert.lengthOf(invoked.interrupted ?? [], 1);
      } else {
        assert.lengthOf(invoked.started, 1);
        assert.lengthOf(invoked.steered ?? [], 1);
        assert.equal(
          invoked.steered?.[0]?.providerThread.id,
          before.executionPrecondition!.providerThreadId,
        );
        assert.lengthOf(invoked.interrupted ?? [], 0);
      }
    }),
  ),
);

it.live(
  "A7 cross-account queue rebinding reconciles run, attempt, node and checkpoint identity",
  () =>
    scenario(
      "owner-queue-cross-account",
      true,
      ({ threadId, state }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          yield* orchestrator.dispatch({
            ...messageCommand(threadId, "queued:older-account"),
            modelSelection: replacement,
          });
          const queuedBefore = (yield* orchestrator.getThreadProjection(threadId)).runs[1]!;
          const selection = {
            ...replacement,
            instanceId: ProviderInstanceId.make("target-provider"),
          };
          yield* orchestrator
            .dispatch({
              type: "provider.switch",
              commandId: CommandId.make("owner:new-account"),
              threadId,
              modelSelection: selection,
            })
            .pipe(withOwnerSelectionIngress);
          const rebound = (yield* orchestrator.getThreadProjection(threadId)).runs[1]!;
          assert.equal(rebound.id, queuedBefore.id);
          assert.equal(rebound.activeAttemptId, queuedBefore.activeAttemptId);
          assert.equal(rebound.queuePosition, queuedBefore.queuePosition);
          assert.equal(rebound.userMessageId, queuedBefore.userMessageId);
          const done = yield* observeRun(threadId, 2, "completed");
          yield* finishActive(state);
          yield* Fiber.join(done);
          yield* worker.drain();
          const after = yield* orchestrator.getThreadProjection(threadId);
          const run = after.runs[1]!;
          const attempt = after.attempts.find((a) => a.id === run.activeAttemptId)!;
          const node = after.nodes.find((n) => n.id === run.rootNodeId)!;
          const checkpoint = after.checkpointScopes.find((c) => c.id === node.checkpointScopeId)!;
          const invoked = (yield* Ref.get(state)).started[1]!;
          assert.deepEqual(invoked.selection, selection);
          assert.equal(invoked.instanceId, selection.instanceId);
          assert.equal(invoked.input.providerThread.providerInstanceId, selection.instanceId);
          assert.equal(run.providerInstanceId, selection.instanceId);
          assert.equal(attempt.providerInstanceId, selection.instanceId);
          assert.equal(run.providerThreadId, invoked.input.providerThread.id);
          assert.equal(attempt.providerThreadId, invoked.input.providerThread.id);
          assert.equal(node.providerThreadId, invoked.input.providerThread.id);
          assert.equal(checkpoint.providerThreadId, invoked.input.providerThread.id);
        }),
      { driver: "codex", compatible: true },
    ),
);

it.live.each(["interrupt", "plan"] as const)(
  "F1/F2 implicit pending owner transition refuses at %s without committing or invoking old B",
  (refusal) =>
    scenario(
      "implicit-refusal-" + refusal,
      true,
      ({ threadId, state, sql }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          const sink = yield* Sink.EventSinkV2;
          yield* orchestrator
            .dispatch({
              type: "thread.model-selection.set",
              commandId: CommandId.make("owner:pending:C"),
              threadId,
              modelSelection: replacement,
            })
            .pipe(withOwnerSelectionIngress);
          yield* worker.drain();
          if (refusal === "plan")
            yield* Ref.update(state, (s) => ({ ...s, rejectSelection: true }));
          const before = yield* sink.latestSequence({ threadId });
          const effects = yield* countEffects(sql, threadId);
          const owner = yield* ownerRows(sql, threadId);
          const error = yield* orchestrator
            .dispatch({ ...messageCommand(threadId, "implicit:refused"), deliveryIntent: "auto" })
            .pipe(withOwnerSelectionIngress, Effect.flip);
          assert.instanceOf(error, Orchestrator.OrchestratorDispatchError);
          if (error._tag === "OrchestratorDispatchError") {
            if (refusal === "plan") assert.equal(error.cause, "Synthetic selection refusal");
            else assert.nestedPropertyVal(error, "cause.capability", "interrupt_restart_steering");
          }
          yield* worker.drain();
          assert.equal(yield* sink.latestSequence({ threadId }), before);
          assert.equal(yield* countEffects(sql, threadId), effects);
          assert.deepEqual(yield* ownerRows(sql, threadId), owner);
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(after.thread.modelSelection, replacement);
          assert.lengthOf(after.runs, 1);
          assert.lengthOf(after.attempts, 1);
          const invoked = yield* Ref.get(state);
          assert.lengthOf(invoked.started, 1);
          assert.deepEqual(invoked.started[0]?.selection, initialSelection);
          assert.lengthOf(invoked.steered ?? [], 0);
          assert.lengthOf(invoked.interrupted ?? [], 0);
        }),
      undefined,
      undefined,
      false,
      {
        driver: "codex",
        capabilities: {
          ...CodexProviderCapabilitiesV2,
          turns: {
            ...CodexProviderCapabilitiesV2.turns,
            supportsInterrupt: refusal !== "interrupt",
          },
        },
      },
    ),
);

it.live(
  "A8 failed owner interrupt records a failed successor and prevents a second execution",
  () =>
    scenario("owner-interrupt-failure", true, ({ threadId, state, sql }) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* Worker.OrchestrationEffectWorkerV2;
        const effects = yield* Outbox.EffectOutboxV2;
        yield* Ref.update(state, (s) => ({ ...s, failInterrupt: true }));
        const failed = yield* observeRun(threadId, 1, "failed");
        const command = {
          ...messageCommand(threadId, "owner:interrupt-fails"),
          modelSelection: replacement,
          modelSelectionExplicit: true,
          deliveryIntent: "auto" as const,
        };
        yield* orchestrator.dispatch(command).pipe(withOwnerSelectionIngress);
        yield* Fiber.join(failed);
        yield* worker.drain();
        const restarts = (yield* effects.listByCommandId(command.commandId)).filter(
          (effect) => effect.request.type === "provider-turn.restart",
        );
        assert.lengthOf(restarts, 1);
        yield* effects.awaitSettled(restarts[0]!.id);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(after.runs, 1);
        assert.lengthOf(after.attempts, 2);
        assert.equal(after.attempts.find((a) => a.reason === "initial")?.status, "superseded");
        assert.equal(after.attempts.find((a) => a.reason === "steering_restart")?.status, "failed");
        assert.isTrue(
          after.turnItems.some(
            (item) =>
              item.type === "error" && item.failure.message.includes("replacement was not started"),
          ),
        );
        const outbox = yield* sql<{
          status: string;
          attempt_count: number;
        }>`SELECT status, attempt_count FROM orchestration_v2_effect_outbox WHERE command_id = ${command.commandId}`;
        assert.isTrue(
          outbox.some((effect) => effect.status === "failed" && effect.attempt_count === 1),
        );
        yield* orchestrator.dispatch(command).pipe(withOwnerSelectionIngress);
        yield* worker.drain();
        assert.lengthOf((yield* Ref.get(state)).started, 1);
        assert.lengthOf((yield* Ref.get(state)).interrupted ?? [], 1);
        assert.equal((yield* Ref.get(state)).closedSessionCount, 0);
        const second = yield* observeRun(threadId, 2, "failed");
        yield* orchestrator
          .dispatch({
            ...messageCommand(threadId, "owner:held-after-failure"),
            dispatchMode: { type: "start_immediately" },
          })
          .pipe(withOwnerSelectionIngress);
        yield* Fiber.join(second);
        yield* worker.drain();
        assert.lengthOf((yield* Ref.get(state)).started, 1);
        const held = yield* orchestrator.getThreadProjection(threadId);
        assert.isTrue(
          held.turnItems.some(
            (item) => item.type === "error" && item.failure.message.includes("unresolved outcome"),
          ),
        );
      }),
    ),
);

it.live("P1 preparing same-instance owner selection reaches the first adapter invocation", () =>
  scenario(
    "owner-preparing-model",
    true,
    ({ threadId, state }) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* Worker.OrchestrationEffectWorkerV2;
        const before = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(before.runs[0]?.status, "preparing");
        yield* orchestrator
          .dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("preparing:C"),
            threadId,
            modelSelection: replacement,
          })
          .pipe(withOwnerSelectionIngress);
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(threadId)).runs[0]?.modelSelection,
          replacement,
        );
        const done = yield* observeRun(threadId, 1, "completed");
        yield* orchestrator.dispatch({
          type: "prepared-run.release",
          commandId: CommandId.make("preparing:release"),
          threadId,
          runId: before.runs[0]!.id,
        });
        yield* Fiber.join(done);
        yield* worker.drain();
        assert.lengthOf((yield* Ref.get(state)).started, 1);
        assert.deepEqual((yield* Ref.get(state)).started[0]?.selection, replacement);
      }),
    undefined,
    undefined,
    false,
    undefined,
    true,
  ),
);

it.live.each(["codex", "claude-code"] as const)(
  "P2 pristine preparing %s account rebind preserves the provider-thread ID and queued identity",
  (preparingDriver) =>
    scenario(
      "owner-preparing-account-" + preparingDriver,
      true,
      ({ threadId, state }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          yield* orchestrator.dispatch(messageCommand(threadId, "preparing:queued-behind"));
          const before = yield* orchestrator.getThreadProjection(threadId);
          const selection = {
            ...replacement,
            instanceId: ProviderInstanceId.make("target-provider"),
          };
          yield* orchestrator
            .dispatch({
              type: "provider.switch",
              commandId: CommandId.make("preparing:new-account"),
              threadId,
              modelSelection: selection,
            })
            .pipe(withOwnerSelectionIngress);
          const rebound = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(rebound.thread.activeProviderThreadId, before.thread.activeProviderThreadId);
          assert.equal(rebound.providerThreads[0]?.id, before.providerThreads[0]?.id);
          assert.equal(rebound.providerThreads[0]?.providerInstanceId, selection.instanceId);
          for (const [i, run] of rebound.runs.entries()) {
            assert.deepEqual(run.modelSelection, selection);
            assert.equal(run.providerInstanceId, selection.instanceId);
            assert.equal(run.id, before.runs[i]!.id);
            assert.equal(run.userMessageId, before.runs[i]!.userMessageId);
            assert.equal(run.activeAttemptId, before.runs[i]!.activeAttemptId);
            assert.equal(run.queuePosition, before.runs[i]!.queuePosition);
            assert.equal(
              rebound.attempts.find((a) => a.id === run.activeAttemptId)?.providerInstanceId,
              selection.instanceId,
            );
          }
          const done = yield* observeRun(threadId, 2, "completed");
          yield* orchestrator.dispatch({
            type: "prepared-run.release",
            commandId: CommandId.make("preparing:release-account"),
            threadId,
            runId: before.runs[0]!.id,
          });
          yield* Fiber.join(done);
          yield* worker.drain();
          const after = yield* orchestrator.getThreadProjection(threadId);
          const invoked = (yield* Ref.get(state)).started;
          assert.lengthOf(invoked, 2);
          for (const start of invoked) {
            assert.deepEqual(start.selection, selection);
            assert.equal(start.instanceId, selection.instanceId);
            const run = after.runs.find((r) => r.id === start.input.runId)!;
            const attempt = after.attempts.find((a) => a.id === run.activeAttemptId)!;
            const node = after.nodes.find((n) => n.id === run.rootNodeId)!;
            const checkpoint = after.checkpointScopes.find((c) => c.id === node.checkpointScopeId)!;
            assert.equal(run.providerThreadId, before.thread.activeProviderThreadId);
            assert.equal(run.providerInstanceId, selection.instanceId);
            assert.equal(attempt.providerInstanceId, selection.instanceId);
            assert.equal(attempt.providerThreadId, start.input.providerThread.id);
            assert.equal(node.providerThreadId, start.input.providerThread.id);
            assert.equal(checkpoint.providerThreadId, start.input.providerThread.id);
          }
        }),
      { driver: preparingDriver, compatible: false },
      undefined,
      false,
      {
        driver: preparingDriver,
        capabilities:
          preparingDriver === "codex" ? CodexProviderCapabilitiesV2 : ClaudeProviderCapabilitiesV2,
      },
      true,
    ),
);

it.live(
  "P3 preparing cross-driver owner selection refuses before events; unattributed setter keeps legacy behavior",
  () =>
    scenario(
      "owner-preparing-driver-refusal",
      false,
      ({ threadId, state, sql }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const sink = yield* Sink.EventSinkV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          const before = yield* orchestrator.getThreadProjection(threadId);
          const sequence = yield* sink.latestSequence({ threadId });
          const selection = {
            ...replacement,
            instanceId: ProviderInstanceId.make("target-provider"),
          };
          const command = {
            type: "provider.switch" as const,
            commandId: CommandId.make("preparing:driver-refusal"),
            threadId,
            modelSelection: selection,
          };
          const error = yield* orchestrator
            .dispatch(command)
            .pipe(withOwnerSelectionIngress, Effect.flip);
          assert.instanceOf(error, Orchestrator.OrchestratorDispatchError);
          if (error._tag === "OrchestratorDispatchError")
            assert.include(String(error.cause), "Workspace preparation is still running");
          assert.equal(yield* sink.latestSequence({ threadId }), sequence);
          assert.deepEqual(
            (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
            initialSelection,
          );
          assert.lengthOf(yield* ownerRows(sql, threadId), 0);
          assert.lengthOf((yield* Ref.get(state)).started, 0);
          const done = yield* observeRun(threadId, 1, "running");
          yield* orchestrator.dispatch({
            ...command,
            commandId: CommandId.make("preparing:legacy-switch"),
          });
          assert.deepEqual(
            (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
            selection,
          );
          const legacySequence = yield* sink.latestSequence({ threadId });
          const equalOwnerError = yield* orchestrator
            .dispatch({
              ...command,
              commandId: CommandId.make("preparing:equal-owner-after-legacy"),
            })
            .pipe(withOwnerSelectionIngress, Effect.flip);
          assert.instanceOf(equalOwnerError, Orchestrator.OrchestratorDispatchError);
          if (equalOwnerError._tag === "OrchestratorDispatchError")
            assert.include(String(equalOwnerError.cause), "Workspace preparation is still running");
          assert.equal(yield* sink.latestSequence({ threadId }), legacySequence);
          assert.lengthOf(yield* ownerRows(sql, threadId), 0);
          assert.deepEqual((yield* orchestrator.getThreadProjection(threadId)).runs, before.runs);
          yield* orchestrator.dispatch({
            type: "prepared-run.release",
            commandId: CommandId.make("preparing:release-legacy"),
            threadId,
            runId: before.runs[0]!.id,
          });
          yield* Fiber.join(done);
          yield* worker.drain();
          assert.lengthOf((yield* Ref.get(state)).started, 1);
          assert.deepEqual((yield* Ref.get(state)).started[0]?.selection, initialSelection);
          assert.equal((yield* Ref.get(state)).started[0]?.instanceId, initialSelection.instanceId);
        }),
      { driver: "claude-code", compatible: false },
      undefined,
      false,
      undefined,
      true,
    ),
);

it.live.each(["starting", "running"] as const)(
  "P4 committed B is never rebound; owner sends during %s transition once",
  (sendAt) =>
    scenario(
      "owner-starting-admission-" + sendAt,
      false,
      ({ threadId, state }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          const before = yield* orchestrator.getThreadProjection(threadId);
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          yield* Ref.update(state, (s) => ({ ...s, openGate: { entered, release } }));
          yield* orchestrator.dispatch({
            type: "prepared-run.release",
            commandId: CommandId.make("starting:release-first"),
            threadId,
            runId: before.runs[0]!.id,
          });
          yield* Deferred.await(entered);
          const admitted = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
          assert.equal(admitted.status, "starting");
          const selection = {
            ...replacement,
            instanceId: ProviderInstanceId.make("target-provider"),
          };
          yield* orchestrator
            .dispatch({
              type: "provider.switch",
              commandId: CommandId.make("starting:owner:C"),
              threadId,
              modelSelection: selection,
            })
            .pipe(withOwnerSelectionIngress);
          assert.deepEqual((yield* orchestrator.getThreadProjection(threadId)).runs[0], admitted);
          assert.equal((yield* Ref.get(state)).closedSessionCount, 0);
          // Owner sends during the committed start capture C without changing B's admitted binding.
          if (sendAt === "starting") {
            yield* orchestrator
              .dispatch({
                ...messageCommand(threadId, "starting:owner-queued-C"),
                deliveryIntent: "auto",
              })
              .pipe(withOwnerSelectionIngress);
            assert.deepEqual(
              (yield* orchestrator.getThreadProjection(threadId)).runs[1]?.modelSelection,
              selection,
            );
          }
          const running = yield* observeRun(threadId, 1, "running");
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(running);
          yield* worker.drain();
          assert.deepEqual((yield* Ref.get(state)).started[0]?.selection, initialSelection);
          assert.equal((yield* Ref.get(state)).closedSessionCount, 0);
          const projection = yield* orchestrator.getThreadProjection(threadId);
          // A single promotion consumes the already queued message instead of sending it twice.
          const done = yield* observeRun(threadId, 1, "completed");
          yield* orchestrator
            .dispatch(
              sendAt === "starting"
                ? {
                    type: "queued-message.promote-to-steer",
                    commandId: CommandId.make("starting:owner-promote-C"),
                    threadId,
                    queuedRunId: projection.runs[1]!.id,
                    targetRunId: projection.runs[0]!.id,
                  }
                : {
                    ...messageCommand(threadId, "running:implicit-owner-C"),
                    deliveryIntent: "auto",
                  },
            )
            .pipe(withOwnerSelectionIngress);
          yield* Fiber.join(done);
          yield* worker.drain();
          const invoked = yield* Ref.get(state);
          assert.lengthOf(invoked.started, 2);
          assert.deepEqual(invoked.started[1]?.selection, selection);
          assert.lengthOf(
            invoked.started.filter((s) => s.instanceId === initialSelection.instanceId),
            1,
          );
          assert.lengthOf(invoked.interrupted ?? [], 1);
          assert.equal(
            invoked.interrupted?.[0]?.providerThread.providerInstanceId,
            initialSelection.instanceId,
          );
          assert.equal(invoked.closedSessionCount, 1);
          const after = yield* orchestrator.getThreadProjection(threadId);
          if (sendAt === "starting") assert.equal(after.runs[1]?.status, "cancelled");
          else assert.lengthOf(after.runs, 1);
          assert.lengthOf(
            after.attempts.filter((a) => a.reason === "steering_restart"),
            1,
          );
        }),
      { driver: "codex", compatible: false },
      undefined,
      false,
      { driver: "claude-code", capabilities: ClaudeProviderCapabilitiesV2 },
      true,
    ),
);

it.live.each(["restart", "next_turn", "handoff", "reject"] as const)(
  "A9/A9b crash continuation with pending C validates a fresh session: %s",
  (transition) =>
    scenario(
      "owner-crash-continuation-" + transition,
      transition !== "next_turn",
      ({ threadId, state }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          yield* orchestrator
            .dispatch({
              type: "thread.model-selection.set",
              commandId: CommandId.make("crash:owner:C"),
              threadId,
              modelSelection: replacement,
            })
            .pipe(withOwnerSelectionIngress);
          yield* worker.drain();
          assert.lengthOf((yield* Ref.get(state)).started, 1);
          yield* Ref.update(state, (s) => ({
            ...s,
            rejectSelection: transition === "reject",
            handoffSelection: transition === "handoff",
          }));
        }),
      undefined,
      ({ threadId, state, sql }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          const sink = yield* Sink.EventSinkV2;
          const sourceId = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!.id;
          const outbox = yield* Outbox.EffectOutboxV2;
          const scheduled = (yield* outbox.listByThreadId(threadId)).filter(
            (effect) =>
              effect.request.type === "provider-runtime.continue" &&
              effect.request.sourceRunId === sourceId,
          );
          assert.lengthOf(
            scheduled,
            1,
            "Crash recovery must persist continuation intent before its outcome is observed",
          );
          const barrier =
            transition === "reject"
              ? yield* sink.stream({ threadId }).pipe(
                  Stream.filter(
                    (stored) =>
                      stored.commandId === "command:restart-continuation:" + sourceId &&
                      stored.event.type === "thread.metadata-updated",
                  ),
                  Stream.take(1),
                  Stream.runDrain,
                  Effect.forkScoped,
                )
              : yield* observeRun(threadId, 2, "completed");
          yield* worker.drain();
          yield* outbox.awaitSettled(scheduled[0]!.id);
          const continuation = yield* orchestrator.getThreadProjection(threadId);
          assert.lengthOf(
            continuation.runs,
            transition === "reject" ? 1 : 2,
            "Settled recovery intent must admit C or record its typed decline",
          );
          assert.notEqual(
            continuation.runs[1]?.status,
            "failed",
            "Recovery must reach the provider without a pre-admission failure",
          );
          if (transition !== "reject")
            assert.deepEqual((yield* Ref.get(state)).started[1]?.selection, replacement);
          yield* Fiber.join(barrier);
          yield* worker.drain();
          const after = yield* orchestrator.getThreadProjection(threadId);
          const invoked = yield* Ref.get(state);
          assert.deepEqual(after.thread.modelSelection, replacement);
          assert.equal(after.runs[0]?.status, "cancelled");
          assert.equal((yield* ownerRows(sql, threadId))[0]?.command_id, "crash:owner:C");
          if (transition === "reject") {
            assert.lengthOf(after.runs, 1);
            assert.lengthOf(invoked.started, 1);
            assert.lengthOf(invoked.resumed ?? [], 0);
            const declined = yield* sink
              .readByCommandId({
                commandId: CommandId.make("command:restart-continuation:" + after.runs[0]!.id),
              })
              .pipe(Stream.runCollect);
            assert.isTrue(declined.some(({ event }) => event.type === "thread.metadata-updated"));
            yield* outbox.awaitSettled(scheduled[0]!.id);
            const sequence = yield* sink.latestSequence({ threadId });
            yield* continueRestartedRun({ threadId, sourceRunId: sourceId });
            assert.equal(yield* sink.latestSequence({ threadId }), sequence);
            assert.lengthOf((yield* Ref.get(state)).started, 1);
            assert.lengthOf((yield* Ref.get(state)).resumed ?? [], 0);
          } else {
            assert.lengthOf(after.runs, 2);
            assert.equal(after.runs[1]?.restartContinuationOfRunId, after.runs[0]?.id);
            assert.lengthOf(invoked.started, 2);
            assert.deepEqual(invoked.started[1]?.selection, replacement);
            assert.notEqual(invoked.started[1]?.sessionId, invoked.started[0]?.sessionId);
            if (transition === "handoff") {
              assert.notEqual(
                invoked.started[1]?.input.providerThread.id,
                invoked.started[0]?.input.providerThread.id,
              );
              assert.notEqual(
                invoked.started[1]?.input.providerThread.nativeThreadRef?.nativeId,
                invoked.started[0]?.input.providerThread.nativeThreadRef?.nativeId,
              );
              assert.lengthOf(after.contextHandoffs, 1);
            } else {
              assert.deepEqual(invoked.resumed?.at(-1)?.selection, replacement);
              assert.equal(invoked.resumed?.at(-1)?.sessionId, invoked.started[1]?.sessionId);
            }
            assert.isTrue(
              invoked.started
                .slice(1)
                .every((start) => start.selection.model === replacement.model),
            );
          }
        }),
    ),
);

it.live("owner next-turn selection steers without interrupting the admitted run", () =>
  scenario("owner-next-turn-steer", false, ({ threadId, state }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* Worker.OrchestrationEffectWorkerV2;
      const before = yield* orchestrator.getThreadProjection(threadId);
      yield* orchestrator
        .dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make("next-turn:owner:C"),
          threadId,
          modelSelection: replacement,
        })
        .pipe(withOwnerSelectionIngress);
      yield* orchestrator
        .dispatch({
          ...messageCommand(threadId, "next-turn:implicit-owner"),
          deliveryIntent: "auto",
        })
        .pipe(withOwnerSelectionIngress);
      yield* worker.drain();
      const invoked = yield* Ref.get(state);
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.lengthOf(invoked.started, 1);
      assert.lengthOf(invoked.steered ?? [], 1);
      assert.equal(invoked.steered?.[0]?.providerThread.id, before.runs[0]?.providerThreadId);
      assert.lengthOf(invoked.interrupted ?? [], 0);
      assert.equal(invoked.closedSessionCount, 0);
      assert.deepEqual(after.runs[0], before.runs[0]);
      assert.deepEqual(after.thread.modelSelection, replacement);
      const done = yield* observeRun(threadId, 2, "completed");
      yield* orchestrator
        .dispatch({ ...messageCommand(threadId, "next-turn:queued-owner") })
        .pipe(withOwnerSelectionIngress);
      yield* finishActive(state);
      yield* Fiber.join(done);
      yield* worker.drain();
      assert.deepEqual((yield* Ref.get(state)).started[1]?.selection, replacement);
    }),
  ),
);

it.live(
  "A9 an owner-rebound held continuation applies the queued handoff plan in a fresh session",
  () =>
    scenario(
      "owner-queued-continuation-handoff",
      true,
      ({ threadId, state }) =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          const sink = yield* Sink.EventSinkV2;
          const sessions = yield* Sessions.ProviderSessionManagerV2;
          yield* orchestrator.dispatch(messageCommand(threadId, "queued:old-continuation"));
          const before = yield* orchestrator.getThreadProjection(threadId);
          const source = before.runs[0]!;
          const queued = before.runs[1]!;
          const provider = before.providerThreads.find(
            (row) => row.id === source.providerThreadId,
          )!;
          const now = yield* DateTime.now;
          // Model the persisted recovery boundary: its native session ended and queued work is held.
          yield* sink.write({
            events: [
              {
                id: EventId.make("queued-recovery:source"),
                type: "run.updated",
                threadId,
                runId: source.id,
                occurredAt: now,
                payload: { ...source, status: "cancelled", completedAt: now },
              },
              {
                id: EventId.make("queued-recovery:queue"),
                type: "run.updated",
                threadId,
                runId: queued.id,
                occurredAt: now,
                payload: { ...queued, queueHeld: true, restartContinuationOfRunId: source.id },
              },
              ...before.attempts
                .filter((attempt) => attempt.runId === source.id)
                .map((attempt) => ({
                  id: EventId.make("queued-recovery:" + attempt.id),
                  type: "run-attempt.updated" as const,
                  threadId,
                  runId: source.id,
                  occurredAt: now,
                  payload: { ...attempt, status: "cancelled" as const, completedAt: now },
                })),
              ...before.nodes
                .filter((node) => node.runId === source.id)
                .map((node) => ({
                  id: EventId.make("queued-recovery:" + node.id),
                  type: "node.updated" as const,
                  threadId,
                  runId: source.id,
                  occurredAt: now,
                  payload: { ...node, status: "cancelled" as const, completedAt: now },
                })),
              ...before.providerTurns.map((turn) => ({
                id: EventId.make("queued-recovery:" + turn.id),
                type: "provider-turn.updated" as const,
                threadId,
                occurredAt: now,
                payload: { ...turn, status: "cancelled" as const, completedAt: now },
              })),
              {
                id: EventId.make("queued-recovery:session"),
                type: "provider-session.detached",
                threadId,
                occurredAt: now,
                payload: {
                  providerSessionId: provider.providerSessionId!,
                  detachedAt: now,
                  reason: "Synthetic process restart",
                },
              },
            ],
          });
          // Retire the live fixture only after its crash boundary holds queued work and terminalizes B.
          yield* sessions.detach({ providerSessionId: provider.providerSessionId!, threadId });
          yield* Ref.update(state, (s) => ({ ...s, activeTurn: null }));
          yield* worker.drain();
          assert.lengthOf((yield* Ref.get(state)).started, 1);
          yield* Ref.update(state, (s) => ({ ...s, handoffSelection: true }));
          yield* orchestrator
            .dispatch({
              type: "thread.model-selection.set",
              commandId: CommandId.make("queued-recovery:owner:C"),
              threadId,
              modelSelection: replacement,
            })
            .pipe(withOwnerSelectionIngress);
          const rebound = (yield* orchestrator.getThreadProjection(threadId)).runs[1]!;
          assert.equal(rebound.id, queued.id);
          assert.equal(rebound.activeAttemptId, queued.activeAttemptId);
          assert.equal(rebound.queuePosition, queued.queuePosition);
          assert.isTrue(rebound.queueHeld);
          assert.equal(rebound.restartContinuationOfRunId, source.id);
          assert.deepEqual(rebound.modelSelection, replacement);
          const done = yield* observeRun(threadId, 2, "completed");
          yield* orchestrator.dispatch({
            type: "queued-run.start",
            commandId: CommandId.make("queued-recovery:release"),
            threadId,
            runId: queued.id,
          });
          yield* worker.drain();
          const effects = yield* Outbox.EffectOutboxV2;
          const starts = (yield* effects.listByThreadId(threadId)).filter(
            (effect) =>
              effect.request.type === "provider-turn.start" && effect.request.runId === queued.id,
          );
          assert.lengthOf(starts, 1);
          yield* effects.awaitSettled(starts[0]!.id);
          const admitted = yield* orchestrator.getThreadProjection(threadId);
          assert.notEqual(
            admitted.runs.find((run) => run.id === queued.id)?.status,
            "failed",
            "Held continuation must pass its provider-admission guards",
          );
          assert.deepEqual((yield* Ref.get(state)).started[1]?.selection, replacement);
          yield* Fiber.join(done);
          yield* worker.drain();
          const after = yield* orchestrator.getThreadProjection(threadId);
          const invoked = yield* Ref.get(state);
          assert.lengthOf(invoked.started, 2);
          assert.deepEqual(invoked.started[1]?.selection, replacement);
          assert.notEqual(invoked.started[1]?.sessionId, invoked.started[0]?.sessionId);
          assert.notEqual(invoked.started[1]?.input.providerThread.id, provider.id);
          assert.notEqual(
            invoked.started[1]?.input.providerThread.nativeThreadRef?.nativeId,
            provider.nativeThreadRef?.nativeId,
          );
          assert.lengthOf(after.contextHandoffs, 1);
          const run = after.runs[1]!;
          const attempt = after.attempts.find((row) => row.id === run.activeAttemptId)!;
          const node = after.nodes.find((row) => row.id === run.rootNodeId)!;
          const checkpoint = after.checkpointScopes.find(
            (row) => row.id === node.checkpointScopeId,
          )!;
          assert.equal(attempt.providerThreadId, run.providerThreadId);
          assert.equal(node.providerThreadId, run.providerThreadId);
          assert.equal(checkpoint.providerThreadId, run.providerThreadId);
          assert.equal(run.providerThreadId, invoked.started[1]?.input.providerThread.id);
        }),
      undefined,
      undefined,
      true,
    ),
);

it.live("owner unchanged Claude restart intent retains the generic typed refusal", () =>
  scenario(
    "owner-claude-unchanged-restart",
    false,
    ({ threadId, state, sql }) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* Sink.EventSinkV2;
        yield* orchestrator
          .dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("claude:equal-owner"),
            threadId,
            modelSelection: initialSelection,
          })
          .pipe(withOwnerSelectionIngress);
        const sequence = yield* sink.latestSequence({ threadId });
        const effects = yield* countEffects(sql, threadId);
        const error = yield* orchestrator
          .dispatch({
            ...messageCommand(threadId, "claude:unchanged-restart"),
            deliveryIntent: "restart",
          })
          .pipe(withOwnerSelectionIngress, Effect.flip);
        assert.instanceOf(error, Orchestrator.OrchestratorDispatchError);
        assert.nestedPropertyVal(error, "cause.capability", "interrupt_restart_steering");
        assert.equal(yield* sink.latestSequence({ threadId }), sequence);
        assert.equal(yield* countEffects(sql, threadId), effects);
        assert.lengthOf((yield* Ref.get(state)).started, 1);
        assert.lengthOf((yield* Ref.get(state)).interrupted ?? [], 0);
        assert.lengthOf((yield* Ref.get(state)).steered ?? [], 0);
      }),
    undefined,
    undefined,
    false,
    { driver: "claude-code", capabilities: ClaudeProviderCapabilitiesV2 },
  ),
);
