import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import {
  DEFAULT_HELD_QUEUE_EXPIRY_HOURS,
  heldQueueExpiryMs,
  isHumanTypedHeldMessage,
  sweepHeldQueue,
  type HeldQueuedRunRow,
} from "./HeldQueueExpiry.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { makeQueueHoldStore } from "./QueueHoldStore.ts";

const HOUR_MS = 60 * 60 * 1000;
const expiryMs = 24 * HOUR_MS;
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Queue tests inspect durable start intent without a provider"),
} as ProviderAdapterV2Shape;
const database = SqlitePersistence.layerMemory;
const testLayer = Layer.mergeAll(
  database,
  EffectOutbox.layer.pipe(Layer.provide(database)),
  ProjectionStore.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "held-queue-expiry" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ),
);

type Source = "user" | "peer" | "mcp" | "server" | "notification";

const fixture = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const holds = makeQueueHoldStore(sql, makeJonesTablePresence(sql));
  const threadId = ThreadId.make("thread:held-queue-expiry");
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create"),
    threadId,
    projectId: ProjectId.make("project:held-queue-expiry"),
    title: "Held queue expiry",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  const send = (
    text: string,
    type: "start_immediately" | "queue_after_active",
    source: Source = "user",
  ) =>
    orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`send:${text}`),
      threadId,
      messageId: MessageId.make(`message:${text}`),
      text,
      attachments: [],
      dispatchMode: { type },
      createdBy: source === "user" || source === "server" ? "user" : "agent",
      creationSource: source === "user" ? "web" : source === "server" ? "server" : "mcp",
      ...(source === "peer" ? { senderThreadId: ThreadId.make("thread:peer") } : {}),
    });
  const projection = () => store.getThreadProjection(threadId);
  const run = (text: string) =>
    projection().pipe(
      Effect.map((current) =>
        current.runs.find((candidate) => candidate.userMessageId === `message:${text}`)!,
      ),
    );
  const message = (text: string) =>
    projection().pipe(
      Effect.map((current) =>
        current.messages.find((candidate) => candidate.id === `message:${text}`)!,
      ),
    );
  const put = (next: OrchestrationV2Run) =>
    Effect.gen(function* () {
      yield* store.apply({
        id: EventId.make(`fixture:${next.id}:${String(next.queueHeld)}`),
        type: "run.updated",
        threadId,
        runId: next.id,
        occurredAt: yield* DateTime.now,
        payload: next,
      });
    });
  /** Holds a run and records a restart hold at the current time, as startup recovery would. */
  const putRestartHold = (held: OrchestrationV2Run) =>
    Effect.gen(function* () {
      yield* put({ ...held, queueHeld: true });
      yield* holds.record([
        { runId: held.id, threadId, reason: "restart", heldAt: yield* DateTime.now },
      ]);
    });
  /** Queues a message behind the active run, then records the hold a restart would. */
  const holdNew = (text: string, source: Source) =>
    Effect.gen(function* () {
      yield* send(text, "queue_after_active", source);
      if (source === "notification") {
        const current = yield* message(text);
        yield* store.apply({
          id: EventId.make(`notification:${current.id}`),
          type: "message.updated",
          threadId,
          occurredAt: yield* DateTime.now,
          payload: {
            ...current,
            notification: {
              source: { kind: "monitor" },
              outcome: "updated",
              summary: "Monitor event",
            },
          },
        });
      }
      yield* putRestartHold(yield* run(text));
    });
  yield* send("active", "start_immediately");
  return {
    orchestrator,
    store,
    holds,
    threadId,
    send,
    projection,
    run,
    message,
    put,
    putRestartHold,
    holdNew,
  };
});

const status = (runs: ReadonlyArray<OrchestrationV2Run>, text: string) =>
  runs.find((run) => run.userMessageId === `message:${text}`)?.status;

describe("held queue expiry", () => {
  it.effect(
    "cancels automatic held work older than the expiry, keeps younger and human-typed work",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.holdNew("human-old", "user");
        yield* f.holdNew("peer-old", "peer");
        yield* f.holdNew("mcp-old", "mcp");
        yield* f.holdNew("server-old", "server");
        yield* f.holdNew("monitor-old", "notification");
        // A hold recorded before reasons existed: its age runs from the request.
        yield* f.send("legacy", "queue_after_active", "peer");
        const legacyRun = yield* f.run("legacy");
        yield* f.put({ ...legacyRun, queueHeld: true });
        yield* f.holds.forget([legacyRun.id]);
        // Requested long ago but held recently: its age runs from the recorded hold.
        yield* f.send("peer-late-hold", "queue_after_active", "peer");
        yield* TestClock.adjust("20 hours");
        yield* f.holdNew("peer-young", "peer");
        yield* f.putRestartHold(yield* f.run("peer-late-hold"));
        yield* TestClock.adjust("5 hours");

        const result = yield* sweepHeldQueue({ expiryMs });
        assert.sameMembers(
          result.cancelled.map((entry) => entry.runId),
          yield* Effect.forEach(
            ["peer-old", "mcp-old", "server-old", "monitor-old", "legacy"],
            (text) => f.run(text).pipe(Effect.map((run) => run.id)),
          ),
        );
        assert.equal(result.failed, 0);
        const legacyId = (yield* f.run("legacy")).id;
        const legacy = result.cancelled.find((entry) => entry.runId === legacyId);
        assert.isNull(legacy?.reason);
        assert.equal(legacy?.heldHours, 25);

        const runs = (yield* f.projection()).runs;
        for (const text of ["peer-old", "mcp-old", "server-old", "monitor-old", "legacy"]) {
          assert.equal(status(runs, text), "cancelled", text);
        }
        for (const text of ["human-old", "peer-young", "peer-late-hold"]) {
          assert.equal(status(runs, text), "queued", text);
          assert.isTrue(runs.find((run) => run.userMessageId === `message:${text}`)?.queueHeld);
        }
        // Cancelling a held run forgets its recorded hold; waiting holds keep theirs.
        const recorded = yield* f.holds.get(runs.map((run) => RunId.make(run.id)));
        assert.sameMembers(
          [...recorded.keys()],
          yield* Effect.forEach(["human-old", "peer-young", "peer-late-hold"], (text) =>
            f.run(text).pipe(Effect.map((run) => run.id)),
          ),
        );
        assert.equal(status(runs, "active"), "starting");

        const effects = yield* EffectOutbox.EffectOutboxV2.pipe(
          Effect.flatMap((outbox) => outbox.listByThreadId(f.threadId)),
        );
        const startedRunIds = effects.flatMap((effect) =>
          effect.request.type === "provider-turn.start" ? [effect.request.runId] : [],
        );
        assert.isFalse(
          result.cancelled.some((entry) => startedRunIds.some((id) => id === entry.runId)),
        );

        const again = yield* sweepHeldQueue({ expiryMs });
        assert.deepEqual(again, { cancelled: [], failed: 0 });

        // Human words wait however long they are held.
        yield* TestClock.adjust("1000 hours");
        const later = yield* sweepHeldQueue({ expiryMs });
        assert.sameMembers(
          later.cancelled.map((entry) => entry.runId),
          [(yield* f.run("peer-young")).id, (yield* f.run("peer-late-hold")).id],
        );
        assert.equal(status((yield* f.projection()).runs, "human-old"), "queued");
      }).pipe(Effect.provide(testLayer)),
  );

  it.effect("an expired delegated-completion delivery disposes its cohort", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const parent = yield* f.run("active");
      yield* f.send("completion", "queue_after_active", "mcp");
      const completion = yield* f.message("completion");
      yield* f.store.apply({
        id: EventId.make("completion:message"),
        type: "message.updated",
        threadId: f.threadId,
        occurredAt: yield* DateTime.now,
        payload: {
          ...completion,
          delegatedCompletion: { parentRunId: parent.id, generation: 1, taskIds: [] },
        },
      });
      yield* f.put({
        ...parent,
        delegatedCompletion: {
          disposition: "open",
          nextGeneration: 2,
          delivery: { generation: 1, messageId: completion.id, taskIds: [] },
        },
      });
      yield* f.putRestartHold(yield* f.run("completion"));
      yield* TestClock.adjust("25 hours");

      const result = yield* sweepHeldQueue({ expiryMs });
      const delivery = yield* f.run("completion");
      assert.deepEqual(
        result.cancelled.map((entry) => entry.runId),
        [delivery.id],
      );
      assert.equal(delivery.status, "cancelled");
      const cohort = (yield* f.run("active")).delegatedCompletion;
      assert.equal(cohort?.disposition, "disposed");
      assert.isNull(cohort?.delivery);
    }).pipe(Effect.provide(testLayer)),
  );
});

describe("held message classification", () => {
  const row = (overrides: Partial<HeldQueuedRunRow> = {}): HeldQueuedRunRow => ({
    run_id: "run",
    thread_id: "thread",
    requested_at: "2026-11-10T00:00:00.000Z",
    message_id: "message:typed",
    message_role: "user",
    created_by: "user",
    creation_source: "web",
    has_notification: 0,
    has_delegated_completion: 0,
    has_sender_thread: 0,
    has_scheduled_task: 0,
    has_idle_compaction: 0,
    ...overrides,
  });

  it("treats only client-typed user messages, or unknown messages, as human", () => {
    assert.isTrue(isHumanTypedHeldMessage(row()));
    assert.isTrue(isHumanTypedHeldMessage(row({ creation_source: "mobile" })));
    assert.isTrue(isHumanTypedHeldMessage(row({ message_id: null })));
    for (const automatic of [
      row({ message_id: "message:restart-continuation:run" }),
      row({ has_notification: 1 }),
      row({ has_delegated_completion: 1 }),
      row({ has_sender_thread: 1 }),
      row({ has_scheduled_task: 1 }),
      row({ has_idle_compaction: 1 }),
      row({ created_by: "agent" }),
      row({ creation_source: "server" }),
      row({ creation_source: "mcp" }),
      row({ message_role: "system" }),
    ]) {
      assert.isFalse(isHumanTypedHeldMessage(automatic));
    }
  });

  it("reads the expiry from configuration with a 24-hour default", () => {
    assert.equal(DEFAULT_HELD_QUEUE_EXPIRY_HOURS, 24);
    assert.equal(heldQueueExpiryMs(undefined), 24 * HOUR_MS);
    assert.equal(heldQueueExpiryMs(""), 24 * HOUR_MS);
    assert.equal(heldQueueExpiryMs("0"), 24 * HOUR_MS);
    assert.equal(heldQueueExpiryMs("soon"), 24 * HOUR_MS);
    assert.equal(heldQueueExpiryMs("6"), 6 * HOUR_MS);
  });
});
