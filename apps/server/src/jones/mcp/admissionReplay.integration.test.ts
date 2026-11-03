import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  ProjectId,
  ScheduledTaskId,
  ThreadId,
  type ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as ServerConfig from "../../config.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as Mcp from "../../mcp/OrchestratorMcpService.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Launch from "../../orchestration-v2/ThreadLaunchService.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Database from "../../persistence/Sqlite.ts";
import * as Providers from "../../provider/ProviderRegistry.ts";
import * as Projects from "../../project/ProjectService.ts";
import * as Schedules from "../../scheduledTasks/ScheduledTaskService.ts";
import * as Secrets from "../../secrets/SecretRequests.ts";
import * as Remote from "../../peer/RemoteDelegation.ts";
import * as Settings from "../../serverSettings.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import { threadSendIdentity } from "../threadResultDependencies/RemoteRequestProtocol.ts";

const projectId = ProjectId.make("project:c4a");
const parentId = ThreadId.make("thread:c4a-parent");
const instanceId = ProviderInstanceId.make("codex");
const selection = { instanceId, model: "fixture", options: [{ id: "effort", value: "high" }] };
const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("environment:c4a"),
  requestNamespace: "provider-session:c4a",
  thread: { threadId: parentId, providerSessionId: "session:c4a", providerInstanceId: instanceId },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};
const external: McpInvocationScope = {
  ...scope,
  thread: undefined,
  client: { sessionId: "client:c4a", label: "Synthetic", access: "full-access" },
};
const catalog = (values = ["low", "high"]): ReadonlyArray<ServerProvider> => [
  {
    instanceId,
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: "fixture",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-11-02T00:00:00.000Z",
    models: [
      {
        slug: "fixture",
        name: "Fixture",
        isCustom: false,
        capabilities: {
          optionDescriptors: [
            {
              id: "effort",
              label: "Effort",
              type: "select",
              options: values.map((id) => ({ id, label: id })),
            },
          ],
        },
      },
    ],
    slashCommands: [],
    skills: [],
  },
];
const commandId = (operation: string, key: string, index?: number) =>
  CommandId.make(
    [
      "command",
      "mcp",
      encodeURIComponent(scope.requestNamespace),
      operation,
      key,
      ...(index === undefined ? [] : [String(index)]),
    ].join(":"),
  );

const fixture = (queueOnly = false) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-mcp-c4a-" });
    // Allocate and register cleanup before the existing config builder writes children.
    const config = yield* Harness.makeReplayServerConfig("c4a").pipe(
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        makeTempDirectory: () => Effect.succeed(root),
      }),
    );
    const providers = yield* Ref.make(catalog());
    const catalogReads = yield* Ref.make(0);
    const refreshes = yield* Ref.make(0);
    const failReceiptRead = yield* Ref.make<CommandId | undefined>(undefined);
    const holdDispatch = yield* Ref.make<CommandId | undefined>(undefined);
    const dispatchReached = yield* Deferred.make<void>();
    const releaseDispatch = yield* Deferred.make<void>();
    const capabilities = {
      ...CodexProviderCapabilitiesV2,
      turns: {
        ...CodexProviderCapabilitiesV2.turns,
        supportsActiveSteering: !queueOnly,
        supportsQueuedMessages: true,
        supportsSteeringByInterruptRestart: true,
      },
    };
    const registry = Registry.layerFromAdapters([
      {
        instanceId,
        driver: ProviderDriverKind.make("codex"),
        getCapabilities: () => Effect.succeed(capabilities),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
        openSession: (input) =>
          Effect.gen(function* () {
            const now = yield* DateTime.now;
            return {
              instanceId,
              driver: ProviderDriverKind.make("codex"),
              providerSessionId: input.providerSessionId,
              providerSession: {
                id: input.providerSessionId,
                driver: ProviderDriverKind.make("codex"),
                providerInstanceId: instanceId,
                status: "ready" as const,
                cwd: root,
                model: "fixture",
                capabilities,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
              events: Stream.never,
              ensureThread: () => Effect.die("No native execution in admission tests"),
              resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
              startTurn: () => Effect.die("No native execution in admission tests"),
              steerTurn: () => Effect.die("The committed outbox is the test boundary"),
              interruptTurn: () => Effect.die("The committed outbox is the test boundary"),
              respondToRuntimeRequest: () => Effect.void,
              readThreadSnapshot: () => Effect.die("unused"),
              rollbackThread: () => Effect.die("unused"),
              forkThread: () => Effect.die("unused"),
            };
          }),
      },
    ]);
    const database = Database.layerMemory;
    const receipts = Receipts.layer.pipe(Layer.provide(database));
    const settings = Settings.layerTest({
      providerInstances: { [instanceId]: { driver: "codex", enabled: true } },
    });
    const orchestration = Harness.layerWithRegistry(
      { name: "c4a", runtimePolicyOverride: { cwd: root } },
      registry,
      {
        databaseLayer: database,
        runEffectWorker: false,
        serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, { ...config, cwd: root }),
      },
    );
    const gatedOrchestrator = Layer.effect(
      Orchestrator.OrchestratorV2,
      Effect.map(Orchestrator.OrchestratorV2, (service) =>
        Orchestrator.OrchestratorV2.of({
          ...service,
          dispatch: (command) =>
            Ref.get(holdDispatch).pipe(
              Effect.flatMap((held) =>
                command.commandId === held
                  ? Deferred.succeed(dispatchReached, undefined).pipe(
                      Effect.andThen(Deferred.await(releaseDispatch)),
                      Effect.andThen(service.dispatch(command)),
                    )
                  : service.dispatch(command),
              ),
            ),
        }),
      ),
    ).pipe(Layer.provide(orchestration));
    const originalThreads = Threads.layer.pipe(
      Layer.provide(gatedOrchestrator),
      Layer.provideMerge(orchestration),
      Layer.provideMerge(receipts),
      Layer.provideMerge(settings),
    );
    const threads = Layer.effect(
      Threads.ThreadManagementService,
      Effect.map(Threads.ThreadManagementService, (service) =>
        Threads.ThreadManagementService.of({
          ...service,
          getCommandReceipt: (id) =>
            Ref.get(failReceiptRead).pipe(
              Effect.flatMap((fail) =>
                fail === id
                  ? Effect.fail(new Receipts.CommandReceiptStoreReadError({ commandId: id }))
                  : service.getCommandReceipt(id),
              ),
            ),
        }),
      ),
    ).pipe(Layer.provide(originalThreads));
    const layer = Mcp.layer.pipe(
      Layer.provide(threads),
      Layer.provideMerge(originalThreads),
      Layer.provide(
        Layer.mergeAll(
          NodeServices.layer,
          registry,
          Layer.mock(Launch.ThreadLaunchService)({}),
          Layer.mock(Git.GitVcsDriver)({}),
          Layer.mock(Projects.ProjectService)({}),
          Layer.mock(Schedules.ScheduledTaskService)({}),
          Layer.mock(Secrets.SecretRequests)({}),
          Layer.mock(Remote.RemoteDelegation)({}),
          Layer.mock(Providers.ProviderRegistry)({
            getProviders: Ref.update(catalogReads, (n) => n + 1).pipe(
              Effect.andThen(Ref.get(providers)),
            ),
            refreshInstance: () =>
              Ref.update(refreshes, (n) => n + 1).pipe(Effect.andThen(Ref.get(providers))),
          }),
        ),
      ),
    );
    return {
      layer,
      root,
      providers,
      catalogReads,
      refreshes,
      failReceiptRead,
      holdDispatch,
      dispatchReached,
      releaseDispatch,
    };
  });

const startParent = (cwd: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("parent:create"),
      threadId: parentId,
      projectId,
      title: "Parent",
      modelSelection: selection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
      createdBy: "user",
      creationSource: "web",
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("parent:start"),
      threadId: parentId,
      messageId: MessageId.make("parent:message"),
      text: "Parent work",
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
  });

it.effect.each(["delegate-task", "create-thread"] as const)(
  "receipt states control public %s admission before the catalog is read",
  (operation) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* Effect.gen(function* () {
          yield* startParent(f.root);
          const service = yield* Mcp.OrchestratorMcpService;
          const threads = yield* Threads.ThreadManagementService;
          const receipts = yield* Receipts.CommandReceiptStoreV2;
          const call = (key: string) =>
            operation === "delegate-task"
              ? service
                  .delegateTask(scope, { task: "Task", mode: "async", clientRequestId: key })
                  .pipe(Effect.asVoid)
              : service
                  .createThreads(scope, { threads: [{ title: "Child" }], clientRequestId: key })
                  .pipe(Effect.asVoid);
          const rejectedId = commandId(
            operation,
            "rejected",
            operation === "create-thread" ? 0 : undefined,
          );
          yield* receipts.insertIfAbsent({
            commandId: rejectedId,
            threadId:
              operation === "delegate-task"
                ? parentId
                : ThreadId.make(
                    `thread:mcp:${encodeURIComponent(scope.requestNamespace)}:rejected:0`,
                  ),
            commandType: operation === "delegate-task" ? "delegated_task.request" : "thread.create",
            acceptedAt: yield* DateTime.now,
            resultSequence: 0,
            status: "rejected",
            error: "Synthetic terminal rejection",
          });
          assert.include(
            (yield* call("rejected").pipe(Effect.flip)).message,
            "authoritative rejection",
          );
          yield* Ref.set(
            f.failReceiptRead,
            commandId(operation, "unreadable", operation === "create-thread" ? 0 : undefined),
          );
          assert.equal((yield* call("unreadable").pipe(Effect.flip)).code, "orchestration_error");
          yield* Ref.set(f.failReceiptRead, undefined);
          for (const mismatch of ["type", "thread"] as const) {
            const id = commandId(
              operation,
              mismatch,
              operation === "create-thread" ? 0 : undefined,
            );
            yield* receipts.insertIfAbsent({
              commandId: id,
              threadId:
                mismatch === "thread"
                  ? ThreadId.make("another-thread")
                  : operation === "delegate-task"
                    ? parentId
                    : ThreadId.make(
                        `thread:mcp:${encodeURIComponent(scope.requestNamespace)}:${mismatch}:0`,
                      ),
              commandType:
                mismatch === "type"
                  ? "message.dispatch"
                  : operation === "delegate-task"
                    ? "delegated_task.request"
                    : "thread.create",
              acceptedAt: yield* DateTime.now,
              resultSequence: 0,
              status: "accepted",
              error: null,
            });
            assert.equal((yield* call(mismatch).pipe(Effect.flip)).code, "orchestration_error");
          }
          assert.equal(yield* Ref.get(f.catalogReads), 0);
          assert.equal(yield* Ref.get(f.refreshes), 0);
          assert.equal((yield* threads.getShellSnapshot()).threads.length, 1);
          yield* call("absent");
          assert.equal((yield* threads.getShellSnapshot()).threads.length, 2);
          assert.isAbove(yield* Ref.get(f.catalogReads), 0);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["provider", "model", "options"] as const)(
  "a lost delegate response replays its original task after %s removal and changed defaults",
  (removed) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* Effect.gen(function* () {
          yield* startParent(f.root);
          const service = yield* Mcp.OrchestratorMcpService;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threads = yield* Threads.ThreadManagementService;
          const outbox = yield* Outbox.EffectOutboxV2;
          const request = {
            task: "Original task",
            mode: "async" as const,
            target: { model: "fixture", options: selection.options },
            clientRequestId: "accepted",
          };
          const lost = yield* service.delegateTask(scope, request).pipe(
            Effect.flatMap((accepted) =>
              Effect.fail({ accepted, transport: "lost_response" as const }),
            ),
            Effect.flip,
          );
          if (!("accepted" in lost)) return yield* Effect.fail(lost);
          const first = lost.accepted;
          const effects = yield* outbox.listByThreadId(first.childThreadId!);
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("parent:defaults"),
            threadId: parentId,
            modelSelection: { instanceId, model: "replacement", options: [] },
          });
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("child:defaults"),
            threadId: first.childThreadId!,
            modelSelection: { instanceId, model: "replacement", options: [] },
          });
          yield* Ref.set(
            f.providers,
            removed === "provider"
              ? []
              : removed === "model"
                ? catalog().map((p) => ({
                    ...p,
                    models: [{ ...p.models[0]!, slug: "replacement" }],
                  }))
                : catalog(["low"]),
          );
          const reads = yield* Ref.get(f.catalogReads);
          const retry = yield* service.delegateTask(scope, {
            ...request,
            task: "Revised text cannot revise an accepted key",
          });
          assert.equal(retry.taskId, first.taskId);
          assert.equal(retry.childThreadId, first.childThreadId);
          assert.equal(retry.childRunId, first.childRunId);
          assert.equal(retry.model, selection.model);
          assert.deepEqual(retry.options, selection.options);
          assert.equal(yield* Ref.get(f.catalogReads), reads);
          assert.equal(yield* Ref.get(f.refreshes), 0);
          assert.deepEqual(yield* outbox.listByThreadId(first.childThreadId!), effects);
          assert.equal((yield* threads.getThreadProjection(parentId)).subagents.length, 1);
          assert.equal(
            (yield* threads.getThreadProjection(first.childThreadId!)).messages[0]?.text,
            request.task,
          );
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "partial batch replay preserves accepted creation/message and admits only its new first prompt",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* Effect.gen(function* () {
          yield* startParent(f.root);
          const service = yield* Mcp.OrchestratorMcpService;
          const threads = yield* Threads.ThreadManagementService;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const request = {
            clientRequestId: "batch",
            threads: [{ prompt: "Accepted" }, { target: { model: "missing" } }],
          };
          assert.equal(
            (yield* service.createThreads(scope, request).pipe(Effect.flip)).code,
            "model_unavailable",
          );
          const firstId = ThreadId.make(
            `thread:mcp:${encodeURIComponent(scope.requestNamespace)}:batch:0`,
          );
          const before = yield* threads.getThreadProjection(firstId);
          assert.lengthOf(before.messages, 1);
          const repaired = yield* service.createThreads(scope, {
            ...request,
            threads: [{ prompt: "Accepted" }, {}],
          });
          assert.equal(repaired.threads[0]?.threadId, firstId);
          assert.deepEqual((yield* threads.getThreadProjection(firstId)).runs, before.runs);
          const secondId = repaired.threads[1]!.threadId;
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("batch:defaults"),
            threadId: firstId,
            modelSelection: { instanceId, model: "replacement", options: [] },
          });
          yield* Ref.set(f.providers, []);
          const acceptedReads = yield* Ref.get(f.catalogReads);
          assert.equal(
            (yield* service.createThreads(scope, {
              clientRequestId: "batch",
              threads: [{ prompt: "Accepted", target: { model: "changed" } }],
            })).threads[0]?.model,
            "fixture",
          );
          assert.equal(yield* Ref.get(f.catalogReads), acceptedReads);
          assert.equal(
            (yield* service
              .createThreads(scope, {
                clientRequestId: "batch",
                threads: [{ prompt: "Accepted" }, { prompt: "New work" }],
              })
              .pipe(Effect.flip)).code,
            "provider_unavailable",
          );
          assert.lengthOf((yield* threads.getThreadProjection(secondId)).runs, 0);
          yield* Ref.set(f.providers, catalog());
          yield* orchestrator.dispatch({
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("parent:narrow"),
            threadId: parentId,
            runtimeMode: "approval-required",
          });
          assert.equal(
            (yield* service
              .createThreads(scope, {
                clientRequestId: "batch",
                threads: [{ prompt: "Accepted" }, { prompt: "New work" }],
              })
              .pipe(Effect.flip)).code,
            "runtime_mode_escalation_denied",
          );
          assert.lengthOf((yield* threads.getThreadProjection(secondId)).messages, 0);
          const accepted = yield* service.createThreads(scope, {
            clientRequestId: "batch",
            threads: [{ prompt: "Accepted" }],
          });
          assert.equal(accepted.threads[0]?.runId, before.runs[0]?.id);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

const attachRunningTurn = (cwd: string, queueOnly: boolean) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* Sink.EventSinkV2;
    const projection = yield* orchestrator.getThreadProjection(parentId);
    const run = projection.runs[0]!;
    const providerThread = projection.providerThreads.find((p) => p.id === run.providerThreadId)!;
    const now = yield* DateTime.now;
    const sessionId = ProviderSessionId.make("session:synthetic");
    const sessions = yield* Sessions.ProviderSessionManagerV2;
    yield* sessions.open({
      threadId: parentId,
      providerSessionId: sessionId,
      modelSelection: selection,
      runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd },
    });
    yield* sink.write({
      events: [
        {
          id: EventId.make("c4a:provider-session:attached"),
          type: "provider-session.attached",
          threadId: parentId,
          occurredAt: now,
          payload: {
            id: sessionId,
            driver: ProviderDriverKind.make("codex"),
            providerInstanceId: instanceId,
            status: "running",
            cwd,
            model: "fixture",
            capabilities: {
              ...CodexProviderCapabilitiesV2,
              turns: {
                ...CodexProviderCapabilitiesV2.turns,
                supportsActiveSteering: !queueOnly,
                supportsQueuedMessages: true,
                supportsSteeringByInterruptRestart: true,
              },
            },
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
        },
        {
          id: EventId.make("c4a:provider-thread:running"),
          type: "provider-thread.updated",
          threadId: parentId,
          occurredAt: now,
          payload: {
            ...providerThread,
            providerSessionId: sessionId,
            status: "active",
            updatedAt: now,
          },
        },
        {
          id: EventId.make("c4a:run:running"),
          type: "run.updated",
          threadId: parentId,
          runId: run.id,
          occurredAt: now,
          payload: { ...run, status: "running", updatedAt: now },
        },
        {
          id: EventId.make("c4a:provider-turn:running"),
          type: "provider-turn.updated",
          threadId: parentId,
          runId: run.id,
          occurredAt: now,
          payload: {
            id: ProviderTurnId.make("turn:synthetic"),
            providerThreadId: providerThread.id,
            nodeId: run.rootNodeId!,
            runAttemptId: run.activeAttemptId,
            nativeTurnRef: null,
            ordinal: 1,
            status: "running",
            startedAt: now,
            completedAt: null,
          },
        },
      ],
    });
    return run;
  });

it.effect.each([true, false])(
  "public MCP auto uses queue-only=%s capabilities once without unnecessary restart",
  (queueOnly) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture(queueOnly);
        yield* Effect.gen(function* () {
          yield* startParent(f.root);
          const active = yield* attachRunningTurn(f.root, queueOnly);
          const service = yield* Mcp.OrchestratorMcpService;
          const outbox = yield* Outbox.EffectOutboxV2;
          const threads = yield* Threads.ThreadManagementService;
          const request = { threadId: parentId, message: "Follow up", clientRequestId: "auto" };
          const first = yield* service.sendToThread(external, request);
          assert.equal(first.delivery, queueOnly ? "queued" : "steered");
          assert.equal((yield* service.sendToThread(external, request)).runId, first.runId);
          const effects = yield* outbox.listByCommandId(
            threadSendIdentity(external.requestNamespace, "auto").commandId,
          );
          assert.deepEqual(
            effects.map((e) => e.request.type),
            queueOnly ? [] : ["provider-turn.steer"],
          );
          const projection = yield* threads.getThreadProjection(parentId);
          assert.equal(projection.runs.find((r) => r.id === active.id)?.status, "running");
          assert.equal(projection.messages.length, 2);
          assert.equal(projection.runs.length, queueOnly ? 2 : 1);
          assert.deepEqual(
            (yield* outbox.listByThreadId(parentId)).filter(
              (e) =>
                e.request.type === "provider-turn.restart" ||
                e.request.type === "provider-turn.interrupt",
            ),
            [],
          );
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "Stop between MCP send preflight and locked auto admission durably queues the follow-up",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* Effect.gen(function* () {
          yield* startParent(f.root);
          const run = yield* attachRunningTurn(f.root, false);
          const original = yield* Threads.ThreadManagementService;
          const service = yield* Mcp.OrchestratorMcpService;
          const outbox = yield* Outbox.EffectOutboxV2;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          // sendToThread has read a steerable turn when it reaches dispatch.
          yield* Ref.set(
            f.holdDispatch,
            threadSendIdentity(external.requestNamespace, "stop-race").commandId,
          );
          const sending = service
            .sendToThread(external, {
              threadId: parentId,
              message: "After Stop",
              clientRequestId: "stop-race",
            })
            .pipe(Effect.forkChild);
          const fiber = yield* sending;
          yield* Deferred.await(f.dispatchReached);
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop:race"),
            threadId: parentId,
            runId: run.id,
            holdQueue: true,
          });
          yield* Deferred.succeed(f.releaseDispatch, undefined);
          const result = yield* Fiber.join(fiber);
          assert.equal(result.delivery, "queued");
          assert.deepEqual(
            yield* outbox.listByCommandId(
              threadSendIdentity(external.requestNamespace, "stop-race").commandId,
            ),
            [],
          );
          assert.lengthOf((yield* original.getThreadProjection(parentId)).messages, 2);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["queue", "steer", "restart"] as const)(
  "public MCP explicit %s retains its requested delivery",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* Effect.gen(function* () {
          yield* startParent(f.root);
          yield* attachRunningTurn(f.root, false);
          const service = yield* Mcp.OrchestratorMcpService;
          const threads = yield* Threads.ThreadManagementService;
          const outbox = yield* Outbox.EffectOutboxV2;
          const delivered = yield* service.sendToThread(external, {
            threadId: parentId,
            message: "Explicit delivery",
            clientRequestId: "explicit",
            mode,
          });
          assert.equal(
            delivered.delivery,
            mode === "queue" ? "queued" : mode === "steer" ? "steered" : "restarted",
          );
          const effects = yield* outbox.listByCommandId(
            threadSendIdentity(external.requestNamespace, "explicit").commandId,
          );
          assert.deepEqual(
            effects.map((e) => e.request.type),
            mode === "queue"
              ? []
              : [mode === "steer" ? "provider-turn.steer" : "provider-turn.restart"],
          );
          if (mode === "queue") {
            const scheduledId = CommandId.make("scheduled:dispatch");
            const scheduled = yield* threads.sendToThread({
              projectId,
              threadId: parentId,
              commandId: scheduledId,
              messageId: MessageId.make("scheduled:message"),
              scheduledTaskId: ScheduledTaskId.make("scheduled:task"),
              text: "Scheduled delivery",
              attachments: [],
              mode: "queue",
              createdBy: "agent",
              creationSource: "server",
            });
            assert.equal(scheduled.delivery, "queued");
            assert.deepEqual(yield* outbox.listByCommandId(scheduledId), []);
          }
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "accepted batch binding survives catalog changes while each new first prompt needs current admission",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* Effect.gen(function* () {
          yield* startParent(f.root);
          const service = yield* Mcp.OrchestratorMcpService;
          const threads = yield* Threads.ThreadManagementService;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const request = {
            clientRequestId: "creation",
            threads: [{ target: { model: "fixture", options: selection.options } }],
          };
          const first = yield* service.createThreads(scope, request);
          const childId = first.threads[0]!.threadId;
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("creation:defaults"),
            threadId: childId,
            modelSelection: { instanceId, model: "changed-saved-default", options: [] },
          });
          for (const [providers, expected] of [
            [catalog(["low"]), "invalid_request"],
            [
              catalog().map((p) => ({ ...p, models: [{ ...p.models[0]!, slug: "replacement" }] })),
              "model_unavailable",
            ],
            [[], "provider_unavailable"],
          ] as const) {
            yield* Ref.set(f.providers, providers);
            const reads = yield* Ref.get(f.catalogReads);
            const refreshes = yield* Ref.get(f.refreshes);
            const replay = yield* service.createThreads(scope, {
              ...request,
              threads: [{ target: { model: "revised-input" } }],
            });
            assert.equal(replay.threads[0]?.threadId, childId);
            assert.equal(replay.threads[0]?.model, "fixture");
            assert.equal(yield* Ref.get(f.catalogReads), reads);
            assert.equal(yield* Ref.get(f.refreshes), refreshes);
            const refused = yield* service
              .createThreads(scope, {
                ...request,
                threads: [{ ...request.threads[0]!, prompt: "New first prompt" }],
              })
              .pipe(Effect.flip);
            assert.equal(refused.code, expected);
            assert.lengthOf((yield* threads.getThreadProjection(childId)).runs, 0);
          }
          yield* Ref.set(f.providers, catalog());
          const messageId = commandId("dispatch-thread", "creation", 0);
          yield* Ref.set(f.failReceiptRead, messageId);
          const reads = yield* Ref.get(f.catalogReads);
          assert.equal(
            (yield* service
              .createThreads(scope, { ...request, threads: [{ prompt: "New first prompt" }] })
              .pipe(Effect.flip)).code,
            "orchestration_error",
          );
          assert.equal(yield* Ref.get(f.catalogReads), reads);
          yield* Ref.set(f.failReceiptRead, undefined);
          const receipts = yield* Receipts.CommandReceiptStoreV2;
          yield* receipts.insertIfAbsent({
            commandId: messageId,
            threadId: childId,
            commandType: "message.dispatch",
            acceptedAt: yield* DateTime.now,
            resultSequence: 0,
            status: "rejected",
            error: "Synthetic first-message refusal",
          });
          assert.include(
            (yield* service
              .createThreads(scope, { ...request, threads: [{ prompt: "New first prompt" }] })
              .pipe(Effect.flip)).message,
            "authoritative rejection",
          );
          assert.equal(yield* Ref.get(f.catalogReads), reads);
          assert.lengthOf((yield* threads.getThreadProjection(childId)).messages, 0);
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "accepted replay retains caller ownership, namespace, capability and deleted-target fences",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* Effect.gen(function* () {
          yield* startParent(f.root);
          const service = yield* Mcp.OrchestratorMcpService;
          const threads = yield* Threads.ThreadManagementService;
          const sink = yield* Sink.EventSinkV2;
          const request = { task: "Task", mode: "async" as const, clientRequestId: "fenced" };
          const accepted = yield* service.delegateTask(scope, request);
          const batch = yield* service.createThreads(scope, {
            threads: [{}],
            clientRequestId: "fenced",
          });
          yield* Ref.set(f.providers, []);
          const reads = yield* Ref.get(f.catalogReads);
          const otherOwner = {
            ...scope,
            thread: {
              ...scope.thread!,
              providerInstanceId: ProviderInstanceId.make("another-account"),
            },
          };
          assert.equal(
            (yield* service.delegateTask(otherOwner, request).pipe(Effect.flip)).code,
            "parent_not_active",
          );
          assert.equal(
            (yield* service
              .delegateTask({ ...scope, capabilities: new Set() }, request)
              .pipe(Effect.flip)).code,
            "capability_denied",
          );
          assert.equal(yield* Ref.get(f.catalogReads), reads);
          assert.equal(
            (yield* service
              .delegateTask({ ...scope, requestNamespace: "another-credential" }, request)
              .pipe(Effect.flip)).code,
            "provider_unavailable",
          );
          const now = yield* DateTime.now;
          for (const childId of [accepted.childThreadId!, batch.threads[0]!.threadId]) {
            const child = (yield* threads.getThreadProjection(childId)).thread;
            yield* sink.write({
              events: [
                {
                  id: EventId.make(`c4a:delete:${childId}`),
                  type: "thread.deleted",
                  threadId: childId,
                  occurredAt: now,
                  payload: { ...child, deletedAt: now, updatedAt: now },
                },
              ],
            });
          }
          assert.equal(
            (yield* service.delegateTask(scope, request).pipe(Effect.flip)).code,
            "thread_not_found",
          );
          assert.equal(
            (yield* service
              .createThreads(scope, { threads: [{}], clientRequestId: "fenced" })
              .pipe(Effect.flip)).code,
            "thread_not_found",
          );
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
