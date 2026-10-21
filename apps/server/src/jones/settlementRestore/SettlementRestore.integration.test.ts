import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2Command,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import { vi } from "vite-plus/test";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as McpSessionRegistryTestkit from "../../mcp/McpSessionRegistry.testkit.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionMaintenance from "../../orchestration-v2/ProjectionMaintenance.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProviderSessionManager from "../../orchestration-v2/ProviderSessionManager.ts";
import * as RuntimeLayer from "../../orchestration-v2/runtimeLayer.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as SourceControlProviderRegistry from "../../sourceControl/SourceControlProviderRegistry.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import * as SettlementRestore from "./SettlementRestore.ts";

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const driver = ProviderDriverKind.make("codex");
const providerInstance = {
  instanceId: modelSelection.instanceId,
  driverKind: driver,
  continuationIdentity: { driverKind: driver, continuationKey: "codex:test" },
  displayName: "Codex test",
  enabled: true,
  snapshot: { getSnapshot: Effect.succeed({}) } as unknown as ProviderInstance["snapshot"],
  orchestrationAdapter: {
    instanceId: modelSelection.instanceId,
    driver,
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: () => Effect.die("sessions are not used by settlement restore tests"),
  } as ProviderAdapterV2Shape,
  textGeneration: {} as ProviderInstance["textGeneration"],
} satisfies ProviderInstance;

const Platform = Layer.merge(
  NodeServices.layer,
  Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
    resolveLink: () => Effect.die("unused title link"),
  }),
);

// Scratch lives in the worktree's ignored .t3 directory; the scope removes it on any exit.
const scratchParent = (path: Path.Path) =>
  path.resolve(import.meta.dirname, "../../../../..", ".t3");
const makeScratch = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const parent = scratchParent(yield* Path.Path);
  yield* fs.makeDirectory(parent, { recursive: true });
  return yield* fs.makeTempDirectoryScoped({ directory: parent, prefix: "settlement-restore-" });
});

const runtimeFor = <EC, ED, RD>(
  config: Layer.Layer<ServerConfig.ServerConfig, EC>,
  database: Layer.Layer<SqlClient.SqlClient, ED, RD>,
) => {
  const vcs = VcsDriverRegistry.layer.pipe(
    Layer.provide(VcsProcess.layer),
    Layer.provide(config),
    Layer.provide(Platform),
  );
  const runtime = Layer.mergeAll(
    RuntimeLayer.layer,
    RuntimeLayer.layerEventSink,
    ProjectStore.layer,
    ProjectionStore.layer,
    EffectOutbox.layer,
    ThreadCommandExecutor.layer,
    AuthSessions.layer,
  ).pipe(
    Layer.provide(McpSessionRegistryTestkit.layer),
    Layer.provide(McpProviderSessions.layer),
    Layer.provideMerge(database),
    Layer.provide(CheckpointStore.layer.pipe(Layer.provide(vcs))),
    Layer.provide(config),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(
      Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
        getInstance: (instanceId) =>
          Effect.succeed(instanceId === providerInstance.instanceId ? providerInstance : undefined),
        listInstances: Effect.succeed([providerInstance]),
        listUnavailable: Effect.succeed([]),
        streamChanges: Stream.empty,
        subscribeChanges: Effect.never,
      }),
    ),
    Layer.provide(
      Layer.mock(GitWorkflow.GitWorkflowService)({
        pruneWorktrees: () => Effect.void,
        createWorktree: () => Effect.succeed({} as never),
      }),
    ),
    Layer.provide(Layer.mock(ProjectService.ProjectService)({ getById: () => Effect.succeedNone })),
    Layer.provide(Platform),
  );
  return SettlementRestore.layer.pipe(
    Layer.provideMerge(runtime),
    Layer.provide(
      Layer.mock(ServerEnvironment.ServerEnvironment)({
        getEnvironmentId: Effect.succeed(EnvironmentId.make("environment:restore-test")),
      }),
    ),
  );
};

const scratchConfig = Layer.unwrap(
  Effect.map(makeScratch, (baseDir) => ServerConfig.layerTest(process.cwd(), baseDir)),
).pipe(Layer.provide(NodeServices.layer));
const TestLayer = runtimeFor(scratchConfig, SqlitePersistence.layerMemory);

const createThread = Effect.fnUntraced(function* (prefix: string) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const threadId = ThreadId.make(`${prefix}-thread`);
  const projectId = ProjectId.make(`${prefix}-project`);
  const createdAt = DateTime.formatIso(yield* DateTime.now);
  yield* projects.apply({
    sequence: 0,
    eventId: EventId.make(`seed:${projectId}`),
    aggregateKind: "project",
    aggregateId: projectId,
    occurredAt: createdAt,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    type: "project.created",
    payload: {
      projectId,
      title: "Settlement restore",
      workspaceRoot: process.cwd(),
      defaultModelSelection: modelSelection,
      scripts: [],
      createdAt,
      updatedAt: createdAt,
    },
  });
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`${prefix}-create`),
    threadId,
    createdBy: "user",
    creationSource: "web",
    projectId,
    title: "Settlement restore",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
  });
  return { orchestrator, threadId, projectId };
});

type ThreadCommand = Extract<OrchestrationV2Command, { readonly threadId: ThreadId }>;
type CommandBody<C> = C extends unknown ? Omit<C, "commandId" | "threadId"> : never;
const send = (threadId: ThreadId, id: string, body: CommandBody<ThreadCommand>) =>
  Effect.flatMap(Orchestrator.OrchestratorV2, (orchestrator) =>
    orchestrator.dispatch({
      ...body,
      commandId: CommandId.make(id),
      threadId,
    } as OrchestrationV2Command),
  );

const placement = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const thread = (yield* (yield* Orchestrator.OrchestratorV2).getThreadProjection(threadId))
      .thread;
    const iso = (value: DateTime.Utc | null | undefined) =>
      value == null ? null : DateTime.formatIso(value);
    return {
      settledOverride: thread.settledOverride,
      settledAt: iso(thread.settledAt),
      unsettledAt: iso(thread.unsettledAt),
      pinnedAt: iso(thread.pinnedAt),
      pinOrderKey: thread.pinOrderKey ?? null,
      activeOrderKey: thread.activeOrderKey ?? null,
      snoozedUntil: iso(thread.snoozedUntil),
    };
  });

const rowFor = (threadId: ThreadId) =>
  Effect.flatMap(SqlClient.SqlClient, (sql) =>
    sql<{
      readonly settle_command_id: string;
      readonly restore_command_id: string | null;
      readonly restored_sequence: number | null;
    }>`SELECT settle_command_id, restore_command_id, restored_sequence
      FROM jones_settlement_restores WHERE thread_id = ${threadId}`.pipe(
      Effect.map((rows) => rows[0] ?? null),
    ),
  );

const eventsFor = (commandId: string) =>
  Effect.flatMap(EventSink.EventSinkV2, (sink) =>
    sink.readByCommandId({ commandId: CommandId.make(commandId) }).pipe(
      Stream.runCollect,
      Effect.map((events) => Array.from(events, (stored) => stored.event.type)),
    ),
  );

const restore = (threadId: ThreadId, settleCommandId: string, commandId: string) =>
  Effect.flatMap(SettlementRestore.SettlementRestore, (service) =>
    service.restore({
      threadId,
      settleCommandId: CommandId.make(settleCommandId),
      commandId: CommandId.make(commandId),
    }),
  );

const inspect = (threadId: ThreadId, settleCommandId?: string) =>
  Effect.flatMap(SettlementRestore.SettlementRestore, (service) =>
    service
      .inspect({
        items: [
          {
            threadId,
            ...(settleCommandId === undefined
              ? {}
              : { settleCommandId: CommandId.make(settleCommandId) }),
          },
        ],
      })
      .pipe(Effect.map((result) => result.items[0]!)),
  );

/** Unrelated pins the capture and restore must never read. */
const seedUnrelatedPins = Effect.fnUntraced(function* (prefix: string, count: number) {
  for (let index = 0; index < count; index += 1) {
    const { threadId } = yield* createThread(`${prefix}-pin-${index}`);
    yield* send(threadId, `${prefix}-pin-${index}-pin`, {
      type: "thread.pin",
      orderKey: `n${String.fromCharCode(98 + (index % 24))}`,
    });
  }
});

it.layer(TestLayer)("settlement restore", (it) => {
  it.effect(
    "restores the keyed pin, active key and wake time a settle removed, with one event and no effects",
    () =>
      Effect.gen(function* () {
        yield* seedUnrelatedPins("keyed", 3);
        const { threadId } = yield* createThread("keyed");
        yield* send(threadId, "keyed-wake", { type: "thread.unsettle", reason: "user" });
        yield* send(threadId, "keyed-active", { type: "thread.active.reorder", orderKey: "m" });
        yield* send(threadId, "keyed-pin", { type: "thread.pin", orderKey: "h" });
        const before = yield* placement(threadId);
        assert.equal(before.pinOrderKey, "h");
        assert.equal(before.activeOrderKey, "m");
        assert.isNotNull(before.unsettledAt);

        // MCP other-thread settlement goes through the thread management service.
        const threads = yield* ThreadManagement.ThreadManagementService;
        yield* threads.settleThread({
          threadId,
          commandId: CommandId.make("keyed-settle"),
          byOwnAgent: false,
        });
        const settled = yield* placement(threadId);
        assert.equal(settled.settledOverride, "settled");
        assert.isNull(settled.pinnedAt);
        assert.equal((yield* inspect(threadId, "keyed-settle")).status, "restorable");

        const result = yield* restore(threadId, "keyed-settle", "keyed-restore");
        assert.equal(result.outcome, "restored");
        const after = yield* placement(threadId);
        assert.deepEqual(after, {
          ...before,
          settledOverride: "active",
          settledAt: null,
        });
        assert.deepEqual(yield* eventsFor("keyed-restore"), ["thread.unsettled"]);
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        assert.deepEqual(yield* outbox.listByCommandId(CommandId.make("keyed-restore")), []);
        const inspected = yield* inspect(threadId, "keyed-settle");
        assert.equal(inspected.status, "restored");
        assert.equal(inspected.record?.restore?.sequence, result.sequence);
      }),
  );

  it.effect("keeps a keyless pin keyless and restores its original pin time", () =>
    Effect.gen(function* () {
      const { threadId } = yield* createThread("keyless");
      yield* send(threadId, "keyless-pin", { type: "thread.pin" });
      const before = yield* placement(threadId);
      assert.isNull(before.pinOrderKey);
      yield* send(threadId, "keyless-settle", { type: "thread.settle" });
      assert.equal(
        (yield* restore(threadId, "keyless-settle", "keyless-restore")).outcome,
        "restored",
      );
      assert.deepEqual(yield* placement(threadId), {
        ...before,
        settledOverride: "active",
        settledAt: null,
      });
    }),
  );

  it.effect("captures automatic settlement and restores an unpinned active position", () =>
    Effect.gen(function* () {
      const { threadId } = yield* createThread("auto");
      yield* send(threadId, "auto-active", { type: "thread.active.reorder", orderKey: "q" });
      const before = yield* placement(threadId);
      assert.isNull(before.settledOverride);
      yield* send(threadId, "auto-settle", {
        type: "thread.auto-settle",
        snapshotAt: yield* DateTime.now,
      });
      assert.equal((yield* placement(threadId)).settledOverride, "settled");
      assert.equal((yield* rowFor(threadId))?.settle_command_id, "auto-settle");
      assert.equal((yield* restore(threadId, "auto-settle", "auto-restore")).outcome, "restored");
      const after = yield* placement(threadId);
      // Explicit active intent keeps the sweep from settling it straight back.
      assert.equal(after.settledOverride, "active");
      assert.equal(after.activeOrderKey, "q");
      assert.isNull(after.pinnedAt);
    }),
  );

  it.effect("captures deferred self-settlement and settle-after-run when their runs complete", () =>
    Effect.gen(function* () {
      for (const mode of ["self", "after-run"] as const) {
        const prefix = `run-${mode}`;
        const { orchestrator, threadId, projectId } = yield* createThread(prefix);
        yield* send(threadId, `${prefix}-pin`, { type: "thread.pin", orderKey: "t" });
        yield* send(threadId, `${prefix}-start`, {
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          messageId: MessageId.make(`${prefix}-message`),
          text: "Finish this task",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
        });
        const initial = yield* orchestrator.getThreadProjection(threadId);
        const run = initial.runs[0]!;
        const provider = initial.providerThreads[0]!;
        const sink = yield* EventSink.EventSinkV2;
        const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const spy = vi
          .spyOn(sessions, "isMcpCallerAttached")
          .mockImplementation((input) =>
            Effect.succeed(
              input.threadId === threadId &&
                input.mcpCredentialId === "self-credential" &&
                input.providerSessionId === provider.providerSessionId,
            ),
          );
        yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
        yield* sink.write({
          events: [
            {
              id: EventId.make(`${prefix}-attached`),
              type: "provider-session.attached",
              threadId,
              occurredAt: now,
              payload: {
                id: provider.providerSessionId!,
                driver,
                providerInstanceId: modelSelection.instanceId,
                status: "running",
                cwd: process.cwd(),
                model: modelSelection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
            },
          ],
        });
        const before = yield* placement(threadId);
        if (mode === "self")
          yield* orchestrator.requestSelfSettlement({
            threadId,
            commandId: CommandId.make(`${prefix}-request`),
            mcpCredentialId: "self-credential",
            providerInstanceId: modelSelection.instanceId,
          });
        const settledEvents = yield* Queue.unbounded<void>();
        yield* sink
          .stream({ threadId, afterSequence: yield* orchestrator.getThreadEventSequence(threadId) })
          .pipe(
            Stream.runForEach((stored) =>
              stored.event.type === "thread.settled"
                ? Queue.offer(settledEvents, undefined)
                : Effect.void,
            ),
            Effect.forkScoped,
          );
        yield* sink.write({
          events: [
            {
              id: EventId.make(`${prefix}-completed`),
              type: "run.updated",
              threadId,
              runId: run.id,
              occurredAt: now,
              payload: { ...run, status: "completed", completedAt: now },
            },
          ],
        });
        if (mode === "after-run")
          yield* (yield* ThreadManagement.ThreadManagementService).settleAfterRun({
            projectId,
            threadId,
            runId: run.id,
          });
        yield* Queue.take(settledEvents);
        const settleCommandId =
          mode === "self" ? `${prefix}-request:settle` : `server:settle-after-run:${run.id}`;
        assert.equal((yield* rowFor(threadId))?.settle_command_id, settleCommandId);
        assert.equal(
          (yield* restore(threadId, settleCommandId, `${prefix}-restore`)).outcome,
          "restored",
        );
        const after = yield* placement(threadId);
        assert.equal(after.pinnedAt, before.pinnedAt);
        assert.equal(after.pinOrderKey, "t");
        // The detached provider session stays detached.
        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.isFalse(
          projection.providerSessions.some(
            (session) => session.id === provider.providerSessionId && session.status === "running",
          ),
        );
      }
    }),
  );

  it.effect("a no-op re-settle keeps the original record", () =>
    Effect.gen(function* () {
      const { threadId } = yield* createThread("noop");
      yield* send(threadId, "noop-pin", { type: "thread.pin", orderKey: "k" });
      yield* send(threadId, "noop-settle", { type: "thread.settle" });
      yield* send(threadId, "noop-again", { type: "thread.settle" });
      assert.deepEqual(yield* eventsFor("noop-again"), ["thread.settled"]);
      assert.equal((yield* rowFor(threadId))?.settle_command_id, "noop-settle");
      assert.equal((yield* restore(threadId, "noop-settle", "noop-restore")).outcome, "restored");
      assert.equal((yield* placement(threadId)).pinOrderKey, "k");
    }),
  );

  it.effect("newer owner intent wins over a stale undo", () =>
    Effect.gen(function* () {
      const cases: ReadonlyArray<readonly [string, CommandBody<ThreadCommand> | null]> = [
        ["unpin-later", { type: "thread.pin" }],
        ["archive-later", { type: "thread.archive" }],
        ["unsettle-later", { type: "thread.unsettle", reason: "user" }],
        ["snooze-later", { type: "thread.snooze", snoozedUntil: "2999-01-01T00:00:00.000Z" }],
      ];
      for (const [prefix, later] of cases) {
        const { threadId } = yield* createThread(prefix);
        yield* send(threadId, `${prefix}-pin`, { type: "thread.pin", orderKey: "p" });
        yield* send(threadId, `${prefix}-settle`, { type: "thread.settle" });
        yield* send(threadId, `${prefix}-later`, later!);
        const current = yield* placement(threadId);
        const result = yield* restore(threadId, `${prefix}-settle`, `${prefix}-restore`);
        assert.equal(result.outcome, "superseded", prefix);
        assert.deepEqual(yield* placement(threadId), current, prefix);
        assert.isNull((yield* rowFor(threadId))?.restore_command_id ?? null);
        assert.equal((yield* inspect(threadId)).status, "superseded");
      }

      // Un-settle then re-settle replaces the record; the old settle ID is stale.
      const { threadId } = yield* createThread("resettle");
      yield* send(threadId, "resettle-pin", { type: "thread.pin", orderKey: "r" });
      yield* send(threadId, "resettle-first", { type: "thread.settle" });
      yield* send(threadId, "resettle-unsettle", { type: "thread.unsettle", reason: "user" });
      yield* send(threadId, "resettle-second", { type: "thread.settle" });
      const stale = yield* restore(threadId, "resettle-first", "resettle-restore-old");
      assert.equal(stale.outcome, "stale_settle_command");
      assert.equal(stale.historicalOutcome, "unavailable");
      assert.equal((yield* rowFor(threadId))?.settle_command_id, "resettle-second");
      assert.isNull((yield* rowFor(threadId))?.restore_command_id ?? null);
      const inspected = yield* inspect(threadId, "resettle-first");
      assert.equal(inspected.status, "superseded");
      assert.equal(inspected.blocker, "stale_settle_command");
      assert.equal(inspected.record?.settleCommandId, "resettle-second");
      // The newer settle restores only its own preimage: the explicit un-settle, unpinned.
      assert.equal(
        (yield* restore(threadId, "resettle-second", "resettle-restore")).outcome,
        "restored",
      );
      const after = yield* placement(threadId);
      assert.isNull(after.pinnedAt);
      assert.equal(after.settledOverride, "active");
    }),
  );

  it.effect("retries keep one success after receipt compaction and reject reuse elsewhere", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const { threadId } = yield* createThread("retry");
      const other = (yield* createThread("retry-other")).threadId;
      yield* send(threadId, "retry-pin", { type: "thread.pin", orderKey: "s" });
      yield* send(threadId, "retry-settle", { type: "thread.settle" });
      yield* send(other, "retry-other-settle", { type: "thread.settle" });
      const first = yield* restore(threadId, "retry-settle", "retry-restore");
      assert.equal(first.outcome, "restored");
      // Receipt hit: generic replay, no second event.
      assert.equal((yield* restore(threadId, "retry-settle", "retry-restore")).outcome, "restored");
      assert.deepEqual(yield* eventsFor("retry-restore"), ["thread.unsettled"]);

      // Later placement events supersede the restore event, and compaction drops it. Receipt
      // compaction keeps v2 receipts today, so the receipt loss a restored or legacy store
      // can show is simulated directly.
      yield* send(threadId, "retry-later", { type: "thread.unpin" });
      yield* send(threadId, "retry-repin", { type: "thread.pin", orderKey: "s" });
      yield* (yield* ProjectionMaintenance.ProjectionMaintenanceV2).compactEventStore;
      yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = 'retry-restore'`;
      const sequence = yield* (yield* Orchestrator.OrchestratorV2).getThreadEventSequence(threadId);
      const retried = yield* restore(threadId, "retry-settle", "retry-restore");
      assert.equal(retried.outcome, "restored");
      assert.equal(retried.sequence, first.sequence);
      assert.equal(retried.restoredAt, first.restoredAt);
      assert.equal(
        yield* (yield* Orchestrator.OrchestratorV2).getThreadEventSequence(threadId),
        sequence,
      );
      assert.equal(
        (yield* restore(threadId, "retry-settle", "retry-restore-second")).outcome,
        "already_restored",
      );
      assert.equal(
        (yield* restore(other, "retry-other-settle", "retry-restore")).outcome,
        "command_id_reused",
      );
      // A restore ID cannot replay a different command's receipt.
      assert.equal(
        (yield* restore(other, "retry-other-settle", "retry-other-settle")).outcome,
        "command_id_reused",
      );
      assert.equal((yield* placement(other)).settledOverride, "settled");
    }),
  );

  it.effect(
    "a failed capture rolls back the settle and a failed marker rolls back the restore",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const { threadId } = yield* createThread("inject");
        yield* send(threadId, "inject-pin", { type: "thread.pin", orderKey: "u" });
        const before = yield* placement(threadId);
        yield* sql`CREATE TRIGGER jones_test_capture_fails BEFORE INSERT ON jones_settlement_restores
        BEGIN SELECT RAISE(ABORT, 'injected capture failure'); END`;
        const failed = yield* Effect.exit(
          send(threadId, "inject-settle", { type: "thread.settle" }),
        );
        yield* sql`DROP TRIGGER jones_test_capture_fails`;
        assert.isTrue(Exit.isFailure(failed));
        assert.deepEqual(yield* eventsFor("inject-settle"), []);
        assert.notInclude(
          (yield* sql<{ readonly status: string }>`SELECT status FROM orchestration_command_receipts
          WHERE command_id = 'inject-settle'`).map((row) => row.status),
          "accepted",
        );
        assert.deepEqual(yield* placement(threadId), before);
        assert.isNull(yield* rowFor(threadId));

        yield* send(threadId, "inject-settle-again", { type: "thread.settle" });
        const settled = yield* placement(threadId);
        yield* sql`CREATE TRIGGER jones_test_marker_fails BEFORE UPDATE ON jones_settlement_restores
        BEGIN SELECT RAISE(ABORT, 'injected marker failure'); END`;
        const restoreFailed = yield* Effect.exit(
          restore(threadId, "inject-settle-again", "inject-restore"),
        );
        yield* sql`DROP TRIGGER jones_test_marker_fails`;
        assert.isTrue(Exit.isFailure(restoreFailed));
        assert.deepEqual(yield* eventsFor("inject-restore"), []);
        assert.deepEqual(yield* placement(threadId), settled);
        assert.isNull((yield* rowFor(threadId))?.restore_command_id ?? null);
      }),
  );

  it.effect("rejects a projected state the orchestrator never produces", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const { threadId } = yield* createThread("invalid");
      yield* send(threadId, "invalid-settle", { type: "thread.settle" });
      // A pin order key without a pin is corrupt: only pinned threads are ordered.
      yield* sql`UPDATE jones_settlement_restores SET prior_pin_order_key = 'x'
        WHERE thread_id = ${threadId}`;
      const current = yield* placement(threadId);
      assert.equal(
        (yield* restore(threadId, "invalid-settle", "invalid-restore")).outcome,
        "invalid_projected_state",
      );
      assert.deepEqual(yield* placement(threadId), current);
      assert.equal((yield* inspect(threadId)).blocker, "invalid_projected_state");
    }),
  );

  it.effect(
    "restore cancels a pending self-settlement; a duplicate retry still emits nothing",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const { threadId } = yield* createThread("cancel-self");
        yield* send(threadId, "cancel-self-pin", { type: "thread.pin", orderKey: "v" });
        yield* send(threadId, "cancel-self-settle", { type: "thread.settle" });
        // A pending intent is marked directly; requesting one needs a live provider run.
        const markIntent = sql`UPDATE orchestration_v2_projection_threads
        SET payload_json = json_set(payload_json, '$.selfSettlement', json_object(
          'mcpCredentialId', 'credential', 'commandId', 'cancel-self-intent', 'runId', 'run-x',
          'providerSessionId', 'session-x', 'providerInstanceId', ${modelSelection.instanceId}))
        WHERE thread_id = ${threadId}`;
        yield* markIntent;
        assert.isTrue(
          (yield* orchestrator.getThreadProjection(threadId)).thread.selfSettlement != null,
        );
        yield* restore(threadId, "cancel-self-settle", "cancel-self-restore");
        assert.deepEqual(yield* eventsFor("cancel-self-restore"), [
          "thread.unsettled",
          "thread.metadata-updated",
        ]);
        const thread = (yield* orchestrator.getThreadProjection(threadId)).thread;
        assert.isNull(thread.selfSettlement ?? null);
        assert.equal(thread.pinOrderKey, "v");

        yield* markIntent;
        yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = 'cancel-self-restore'`;
        const sequence = yield* orchestrator.getThreadEventSequence(threadId);
        assert.equal(
          (yield* restore(threadId, "cancel-self-settle", "cancel-self-restore")).outcome,
          "restored",
        );
        assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
      }),
  );
});

it.effect("the record survives restart and event compaction on the same database", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const baseDir = yield* makeScratch;
      const runtime = () =>
        runtimeFor(
          ServerConfig.layerTest(process.cwd(), baseDir).pipe(Layer.provide(NodeServices.layer)),
          SqlitePersistence.layerConfig,
        );
      const threadId = ThreadId.make("restart-thread");
      const before = yield* Effect.gen(function* () {
        yield* createThread("restart");
        yield* send(threadId, "restart-active", { type: "thread.active.reorder", orderKey: "w" });
        yield* send(threadId, "restart-pin", { type: "thread.pin" });
        const before = yield* placement(threadId);
        yield* send(threadId, "restart-settle", { type: "thread.settle" });
        yield* (yield* ProjectionMaintenance.ProjectionMaintenanceV2).compactEventStore;
        return before;
      }).pipe(Effect.scoped, Effect.provide(runtime()));
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        // Pre-settle placement events are gone; only the Jones record still holds the preimage.
        assert.lengthOf(
          yield* sql`SELECT 1 FROM orchestration_events WHERE command_id IN ('restart-active', 'restart-pin')`,
          0,
        );
        assert.equal((yield* inspect(threadId, "restart-settle")).status, "restorable");
        assert.equal(
          (yield* restore(threadId, "restart-settle", "restart-restore")).outcome,
          "restored",
        );
        assert.deepEqual(yield* placement(threadId), {
          ...before,
          settledOverride: "active",
          settledAt: null,
        });
      }).pipe(Effect.scoped, Effect.provide(runtime()));
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
