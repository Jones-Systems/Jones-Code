import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Statement from "effect/unstable/sql/Statement";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import * as ProjectionMaintenance from "../../orchestration-v2/ProjectionMaintenance.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";

const databaseLayer = SqlitePersistenceMemory;
const eventStoreProvided = EventStore.layer.pipe(Layer.provideMerge(databaseLayer));
const projectionStoreProvided = ProjectionStore.layer.pipe(Layer.provideMerge(databaseLayer));
const storesProvided = Layer.mergeAll(databaseLayer, eventStoreProvided, projectionStoreProvided);
const eventSinkProvided = EventSink.layer.pipe(Layer.provide(storesProvided));
const effectOutboxProvided = EffectOutbox.layer.pipe(Layer.provide(databaseLayer));
const commandReceiptStoreProvided = CommandReceiptStore.layer.pipe(Layer.provide(databaseLayer));
const projectionMaintenanceProvided = ProjectionMaintenance.layer.pipe(
  Layer.provide(storesProvided),
);
const TestLayer = Layer.mergeAll(
  storesProvided,
  eventSinkProvided,
  effectOutboxProvided,
  commandReceiptStoreProvided,
  IdAllocator.layer,
  projectionMaintenanceProvided,
);

const boundarySteerFixture = Effect.fnUntraced(function* (prefix: string) {
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const now = yield* DateTime.now;
  const threadId = ThreadId.make(`thread:${prefix}`);
  const request = {
    type: "provider-turn.steer" as const,
    providerSessionId: ProviderSessionId.make(`session:${prefix}`),
    providerThreadId: ProviderThreadId.make(`provider-thread:${prefix}`),
    providerTurnId: ProviderTurnId.make(`provider-turn:${prefix}`),
    messageId: MessageId.make(`message:${prefix}`),
  };
  const first = {
    id: `effect:${prefix}:9`,
    commandId: CommandId.make(`command:queue-tool-boundary:${prefix}:9`),
    threadId,
    request,
  };
  const second = {
    id: `effect:${prefix}:10`,
    commandId: CommandId.make(`command:queue-tool-boundary:${prefix}:10`),
    threadId,
    request: { ...request, messageId: MessageId.make(`message:${prefix}:second`) },
  };
  yield* Effect.addFinalizer(() =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`DELETE FROM orchestration_v2_effect_outbox WHERE thread_id = ${threadId}`;
          yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id IN (${first.commandId}, ${second.commandId}) OR (aggregate_kind = 'thread' AND aggregate_id = ${threadId})`;
        }),
      )
      .pipe(Effect.orDie),
  );
  yield* sql.withTransaction(
    Effect.gen(function* () {
      for (const [effect, resultSequence] of [
        [first, 9],
        [second, 10],
      ] as const) {
        yield* receipts.upsert({
          commandId: effect.commandId,
          threadId,
          commandType: "queued-message.promote-to-steer",
          acceptedAt: now,
          resultSequence,
          status: "accepted",
          error: null,
        });
      }
      yield* outbox.enqueue([first, second]);
    }),
  );
  const workerId = `worker:${prefix}`;
  const claim = outbox.claimNext({ workerId, leaseDurationMs: 30_000 });
  const succeed = (effectId: string) => outbox.succeed({ effectId, workerId });
  return { outbox, receipts, sql, now, threadId, first, second, workerId, claim, succeed };
});

const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.4",
} satisfies ModelSelection;

function makeThread(threadId: ThreadId, now: DateTime.Utc): OrchestrationV2AppThread {
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make(`project:${threadId}`),
    title: `Thread ${threadId}`,
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: threadId,
    },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

function threadCreatedEvent(input: {
  readonly id: string;
  readonly thread: OrchestrationV2AppThread;
  readonly now: DateTime.Utc;
}): OrchestrationV2DomainEvent {
  return {
    id: EventId.make(input.id),
    type: "thread.created",
    threadId: input.thread.id,
    providerInstanceId,
    occurredAt: input.now,
    payload: input.thread,
  };
}

it.layer(TestLayer)("orchestration V2 foundation persistence", (it) => {
  it.effect("claims automatic boundary steers in receipt order instead of lexical run IDs", () =>
    Effect.gen(function* () {
      const f = yield* boundarySteerFixture("boundary-fifo-lexical");
      assert.equal(Option.getOrThrow(yield* f.claim).id, f.first.id);
      assert.isTrue(yield* f.succeed(f.first.id));
      assert.equal(Option.getOrThrow(yield* f.claim).id, f.second.id);
      assert.isTrue(yield* f.succeed(f.second.id));
    }),
  );

  it.effect("holds automatic boundary steer successors behind a delayed predecessor retry", () =>
    Effect.gen(function* () {
      const f = yield* boundarySteerFixture("boundary-fifo-retry");
      assert.equal(Option.getOrThrow(yield* f.claim).id, f.first.id);
      assert.isTrue(
        yield* f.outbox.retry({
          effectId: f.first.id,
          workerId: f.workerId,
          error: "retry",
          delayMs: 1000,
        }),
      );
      assert.isTrue(Option.isNone(yield* f.claim));
      assert.equal(
        DateTime.toEpochMillis(Option.getOrThrow(yield* f.outbox.nextClaimableAt)),
        DateTime.toEpochMillis(f.now) + 1000,
      );
      yield* TestClock.adjust("999 millis");
      assert.isTrue(Option.isNone(yield* f.claim));
      yield* TestClock.adjust("1 millis");
      const retried = Option.getOrThrow(yield* f.claim);
      assert.equal(retried.id, f.first.id);
      assert.equal(retried.attemptCount, 2);
      assert.isTrue(yield* f.succeed(f.first.id));
      assert.equal(Option.getOrThrow(yield* f.claim).id, f.second.id);
      assert.isTrue(yield* f.succeed(f.second.id));
    }),
  );

  it.effect.each(["failed", "cancelled", "unknown-held"] as const)(
    "keeps successors of a %s automatic boundary steer pending without a wake deadline",
    (status) =>
      Effect.gen(function* () {
        const f = yield* boundarySteerFixture(`boundary-fifo-${status}`);
        const first = Option.getOrThrow(yield* f.claim);
        assert.equal(first.id, f.first.id);
        if (status === "failed") {
          assert.isTrue(
            yield* f.outbox.fail({ effectId: first.id, workerId: f.workerId, error: "failed" }),
          );
        } else if (status === "cancelled") {
          yield* f.sql`UPDATE orchestration_v2_effect_outbox SET status = 'cancelled', lease_owner = NULL, lease_expires_at = NULL WHERE effect_id = ${first.id}`;
        } else {
          // V2 cancels process-bound steering on lost execution evidence; it cannot replay it.
          assert.isTrue(Option.isNone(yield* f.claim));
          yield* f.outbox.reconcileAfterProcessLoss;
          yield* f.sql`UPDATE orchestration_v2_effect_outbox SET status = 'pending' WHERE effect_id = ${f.second.id}`;
          assert.isFalse(yield* f.succeed(first.id));
        }
        assert.isTrue(Option.isNone(yield* f.claim));
        assert.isTrue(Option.isNone(yield* f.outbox.nextClaimableAt));
        assert.equal(Option.getOrThrow(yield* f.outbox.get(f.second.id)).status, "pending");
      }).pipe(Effect.scoped, Effect.provide(Layer.fresh(TestLayer))),
  );

  it.effect("orders automatic steers across distinct boundaries for the same provider turn", () =>
    Effect.gen(function* () {
      const f = yield* boundarySteerFixture("boundary-fifo-distinct-boundaries");
      const nextCommand = CommandId.make(
        "command:queue-tool-boundary:later-run:same-turn:later-boundary",
      );
      yield* f.receipts.upsert({
        commandId: nextCommand,
        threadId: f.threadId,
        commandType: "queued-message.promote-to-steer",
        acceptedAt: f.now,
        resultSequence: 20,
        status: "accepted",
        error: null,
      });
      yield* f.sql`UPDATE orchestration_v2_effect_outbox SET command_id = ${nextCommand} WHERE effect_id = ${f.second.id}`;
      assert.equal(Option.getOrThrow(yield* f.claim).id, f.first.id);
      assert.isTrue(
        yield* f.outbox.retry({
          effectId: f.first.id,
          workerId: f.workerId,
          error: "earlier boundary retry",
          delayMs: 1000,
        }),
      );
      assert.isTrue(Option.isNone(yield* f.claim));
      yield* TestClock.adjust("1 second");
      assert.equal(Option.getOrThrow(yield* f.claim).id, f.first.id);
      assert.isTrue(yield* f.succeed(f.first.id));
      assert.equal(Option.getOrThrow(yield* f.claim).id, f.second.id);
      assert.isTrue(yield* f.succeed(f.second.id));
    }),
  );

  it.effect.each([
    "candidate-missing",
    "candidate-rejected",
    "candidate-aggregate",
    "candidate-kind",
    "candidate-type",
    "candidate-sequence",
    "predecessor-missing",
    "predecessor-rejected",
    "predecessor-aggregate",
    "predecessor-kind",
    "predecessor-type",
    "predecessor-sequence",
    "equal-sequence",
  ] as const)("fails closed for %s automatic steering receipt evidence", (condition) =>
    Effect.gen(function* () {
      const f = yield* boundarySteerFixture(`boundary-fifo-${condition}`);
      const candidate = condition.startsWith("candidate-");
      const commandId = candidate ? f.second.commandId : f.first.commandId;
      if (candidate)
        yield* f.sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded' WHERE effect_id = ${f.first.id}`;
      else if (condition !== "equal-sequence")
        yield* f.sql`UPDATE orchestration_v2_effect_outbox SET status = 'failed' WHERE effect_id = ${f.first.id}`;
      if (condition.endsWith("missing"))
        yield* f.sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${commandId}`;
      else if (condition.endsWith("rejected"))
        yield* f.sql`UPDATE orchestration_command_receipts SET status = 'rejected' WHERE command_id = ${commandId}`;
      else if (condition.endsWith("aggregate"))
        yield* f.sql`UPDATE orchestration_command_receipts SET aggregate_id = 'different-thread' WHERE command_id = ${commandId}`;
      else if (condition.endsWith("kind"))
        yield* f.sql`UPDATE orchestration_command_receipts SET aggregate_kind = 'project' WHERE command_id = ${commandId}`;
      else if (condition.endsWith("type"))
        yield* f.sql`UPDATE orchestration_command_receipts SET command_type = 'message.dispatch' WHERE command_id = ${commandId}`;
      else if (condition === "equal-sequence")
        yield* f.sql`UPDATE orchestration_command_receipts SET result_sequence = 9 WHERE command_id = ${f.second.commandId}`;
      else
        yield* f.sql`UPDATE orchestration_command_receipts SET result_sequence = 0 WHERE command_id = ${commandId}`;
      assert.isTrue(Option.isNone(yield* f.claim));
      assert.isTrue(Option.isNone(yield* f.outbox.nextClaimableAt));
      assert.equal(Option.getOrThrow(yield* f.outbox.get(f.second.id)).status, "pending");
    }),
  );

  it.effect(
    "lets other threads, provider targets and control lanes progress behind failed automatic steering",
    () =>
      Effect.gen(function* () {
        const f = yield* boundarySteerFixture("boundary-fifo-independent");
        assert.equal(Option.getOrThrow(yield* f.claim).id, f.first.id);
        assert.isTrue(
          yield* f.outbox.fail({ effectId: f.first.id, workerId: f.workerId, error: "failed" }),
        );
        const otherThreadId = ThreadId.make(`${f.threadId}:other`);
        const otherCommand = CommandId.make("command:queue-tool-boundary:independent-thread");
        yield* Effect.addFinalizer(() =>
          f.sql
            .withTransaction(
              Effect.gen(function* () {
                yield* f.sql`DELETE FROM orchestration_v2_effect_outbox WHERE thread_id = ${otherThreadId}`;
                yield* f.sql`DELETE FROM orchestration_command_receipts WHERE aggregate_kind = 'thread' AND aggregate_id = ${otherThreadId}`;
              }),
            )
            .pipe(Effect.orDie),
        );
        yield* f.receipts.upsert({
          commandId: otherCommand,
          threadId: otherThreadId,
          commandType: "queued-message.promote-to-steer",
          acceptedAt: f.now,
          resultSequence: 30,
          status: "accepted",
          error: null,
        });
        const independent = [
          {
            id: "effect:boundary-fifo-independent:a-other-thread",
            commandId: otherCommand,
            threadId: otherThreadId,
            request: f.second.request,
          },
          {
            id: "effect:boundary-fifo-independent:b-detach",
            commandId: f.first.commandId,
            threadId: f.threadId,
            request: {
              type: "provider-session.detach" as const,
              providerSessionId: f.second.request.providerSessionId,
            },
          },
          {
            id: "effect:boundary-fifo-independent:c-cleanup",
            commandId: f.first.commandId,
            threadId: f.threadId,
            request: { type: "terminal.cleanup" as const },
          },
          {
            id: "effect:boundary-fifo-independent:d-manual",
            commandId: CommandId.make("command:manual-steer"),
            threadId: f.threadId,
            request: f.second.request,
          },
          {
            id: "effect:boundary-fifo-independent:e-title",
            commandId: f.first.commandId,
            threadId: f.threadId,
            request: {
              type: "thread-title.generate" as const,
              kind: { type: "regenerate" as const },
            },
          },
        ];
        yield* f.outbox.enqueue(independent);
        for (const expected of independent) {
          assert.equal(Option.getOrThrow(yield* f.claim).id, expected.id);
          assert.isTrue(yield* f.succeed(expected.id));
        }
        const changedCommand = CommandId.make("command:queue-tool-boundary:new-provider-target");
        yield* f.receipts.upsert({
          commandId: changedCommand,
          threadId: f.threadId,
          commandType: "queued-message.promote-to-steer",
          acceptedAt: f.now,
          resultSequence: 40,
          status: "accepted",
          error: null,
        });
        const newTarget = {
          ...f.second,
          id: "effect:boundary-fifo-independent:new-target",
          commandId: changedCommand,
          request: {
            ...f.second.request,
            providerTurnId: ProviderTurnId.make("new-provider-turn"),
          },
        };
        yield* f.outbox.enqueue([newTarget]);
        assert.equal(Option.getOrThrow(yield* f.claim).id, newTarget.id);
        assert.isTrue(yield* f.succeed(newTarget.id));
        assert.isTrue(Option.isNone(yield* f.claim));
      }),
  );

  it.effect(
    "uses the existing outbox thread status index for automatic steering predecessors in claims and deadlines",
    () =>
      Effect.gen(function* () {
        const f = yield* boundarySteerFixture("boundary-fifo-query-plan");
        const queries: Array<readonly [string, ReadonlyArray<unknown>]> = [];
        const capture: Statement.Transformer = (statement) => {
          const compiled = statement.compile();
          if (compiled[0].includes("AS predecessor")) queries.push(compiled);
          return Effect.succeed(statement);
        };
        yield* f.outbox.nextClaimableAt.pipe(
          Effect.provideService(Statement.CurrentTransformer, capture),
        );
        assert.equal(
          Option.getOrThrow(
            yield* f.claim.pipe(Effect.provideService(Statement.CurrentTransformer, capture)),
          ).id,
          f.first.id,
        );
        assert.equal(queries.length, 2);
        for (const [query, params] of queries) {
          const plan = yield* f.sql.unsafe<{ detail: string }>(
            `EXPLAIN QUERY PLAN ${query}`,
            params,
          );
          const details = plan.map((row) => row.detail).join("\n");
          assert.match(
            details,
            /SEARCH predecessor USING INDEX orchestration_v2_effect_outbox_thread_status_idx/,
          );
          assert.notMatch(details, /SCAN predecessor/);
          yield* Effect.logInfo("Automatic steering predecessor query plan", {
            statements: query.includes("UPDATE orchestration_v2_effect_outbox")
              ? "claim"
              : "deadline",
            predecessor: plan
              .filter((row) => row.detail.includes("predecessor"))
              .map((row) => row.detail),
          });
        }
        assert.isTrue(yield* f.succeed(f.first.id));
      }),
  );
});

it.effect.each([undefined, false, true] as const)(
  "replays committed queue birth eligibility %s as a Boolean",
  (eligibility) =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const store = yield* EventStore.EventStoreV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const now = yield* DateTime.now;
      const thread = makeThread(ThreadId.make(`thread:queue-boolean-${eligibility}`), now);
      const run: OrchestrationV2Run = {
        id: RunId.make(`run:queue-boolean-${eligibility}`),
        threadId: thread.id,
        ordinal: 1,
        providerInstanceId,
        modelSelection,
        providerThreadId: null,
        userMessageId: MessageId.make("queue-boolean-message"),
        rootNodeId: null,
        activeAttemptId: null,
        status: "queued",
        requestedAt: now,
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      yield* sink.write({
        events: [
          threadCreatedEvent({ id: `queue-boolean-thread-${eligibility}`, thread, now }),
          {
            id: EventId.make(`queue-boolean-birth-${eligibility}`),
            type: "run.created",
            threadId: thread.id,
            runId: run.id,
            occurredAt: now,
            payload: {
              ...run,
              ...(eligibility === undefined ? {} : { queuedToolBoundaryEligible: eligibility }),
            },
          },
        ],
      });
      yield* sink.write({
        events: [
          {
            id: EventId.make(`queue-boolean-stale-${eligibility}`),
            type: "run.updated",
            threadId: thread.id,
            runId: run.id,
            occurredAt: now,
            payload: run,
          },
        ],
      });
      assert.strictEqual(
        (yield* projections.getThreadProjection(thread.id)).runs[0]!.queuedToolBoundaryEligible,
        eligibility,
      );
      const rows = yield* sql<{
        kind: string | null;
      }>`SELECT json_type(payload_json,'$.queuedToolBoundaryEligible') AS kind FROM orchestration_v2_projection_runs WHERE run_id=${run.id}`;
      assert.strictEqual(
        rows[0]!.kind,
        eligibility === undefined ? null : eligibility ? "true" : "false",
      );
      const events = yield* store.read({ threadId: thread.id }).pipe(Stream.runCollect);
      yield* Effect.gen(function* () {
        const memory = yield* ProjectionStore.ProjectionStoreV2;
        for (const event of events) yield* memory.apply(event.event);
        assert.strictEqual(
          (yield* memory.getThreadProjection(thread.id)).runs[0]!.queuedToolBoundaryEligible,
          eligibility,
        );
      }).pipe(Effect.provide(ProjectionStore.layerMemory));
    }).pipe(Effect.provide(TestLayer)),
);
