import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as CommandReceipts from "./CommandReceiptStore.ts";
import { ThreadSelfConfigurationError } from "../jones/threadConfiguration/ThreadSelfConfiguration.ts";
import { TaskContinuationError } from "../jones/taskContinuation/sourceBinding.ts";
import { ThreadTitleStateError } from "../jones/threadTitle/store.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as Sqlite from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const isThreadSelfConfigurationError = Schema.is(ThreadSelfConfigurationError);
const isTaskContinuationError = Schema.is(TaskContinuationError);

const layer = EventSink.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(EventStore.layer, ProjectionStore.layer, CommandReceipts.layer),
  ),
  Layer.provideMerge(Sqlite.layerMemory),
);

const makeThread = (
  threadId: ThreadId,
  providerInstanceId: ProviderInstanceId,
  now: DateTime.Utc,
): OrchestrationV2AppThread => {
  return {
    id: threadId,
    projectId: ProjectId.make("project"),
    title: "Source",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    deletedAt: null,
    settledOverride: null,
    settledAt: null,
    snoozedUntil: null,
    snoozedAt: null,
    lastVisitedAt: null,
  };
};

it.effect("rejects a planned fork whose source was deleted before it committed", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const now = yield* DateTime.now;
    const sourceId = ThreadId.make("fork-source");
    const targetId = ThreadId.make("fork-target");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const source = makeThread(sourceId, providerInstanceId, now);
    yield* sink.write({
      events: [
        {
          id: EventId.make("source-created"),
          type: "thread.created",
          threadId: sourceId,
          occurredAt: now,
          payload: source,
        },
      ],
    });
    const plannedFork = {
      commandId: CommandId.make("fork"),
      commandType: "thread.fork",
      threadId: targetId,
      acceptedAt: now,
      effects: [],
      events: [
        {
          id: EventId.make("fork-created"),
          type: "thread.created" as const,
          threadId: targetId,
          occurredAt: now,
          payload: {
            ...source,
            id: targetId,
            forkedFrom: { type: "run" as const, threadId: sourceId, runId: RunId.make("run") },
          },
        },
      ],
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make("source-deleted"),
          type: "thread.deleted",
          threadId: sourceId,
          occurredAt: now,
          payload: { ...source, deletedAt: now },
        },
      ],
    });
    const rejected = yield* sink.commitCommand(plannedFork);
    assert.equal(rejected.receipt.status, "rejected");
    assert.isFalse(rejected.committed);
    const replayed = yield* sink.commitCommand(plannedFork);
    assert.deepEqual(replayed.receipt, rejected.receipt);
    assert.isFalse(replayed.committed);
    const missing = yield* projections.getThread(targetId).pipe(Effect.flip);
    assert.instanceOf(missing, ProjectionStore.ProjectionStoreThreadNotFoundError);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["configuration_changed", "execution_changed"] as const)(
  "self-configuration %s rolls back the original transaction and accepted retries do not revalidate",
  (reason) =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("self-configuration-transaction:" + reason);
      const commandId = CommandId.make("self-configure:" + reason);
      const thread = makeThread(threadId, ProviderInstanceId.make("codex"), now);
      yield* sink.write({
        events: [
          {
            id: EventId.make("birth:" + reason),
            type: "thread.created",
            threadId,
            occurredAt: now,
            payload: thread,
          },
        ],
      });
      yield* sql`CREATE TABLE self_configuration_transaction_probe (value INTEGER NOT NULL)`;
      yield* sql`INSERT INTO self_configuration_transaction_probe (value) VALUES (0)`;
      const sequence = yield* sink.latestSequence({ threadId });
      const replacement = { ...thread.modelSelection, model: "replacement" };
      const event = {
        id: EventId.make("save:" + reason),
        type: "thread.model-selection-updated" as const,
        threadId,
        occurredAt: now,
        payload: { ...thread, modelSelection: replacement },
      };
      const input = {
        commandId,
        commandType: "jones.thread.self-configure",
        threadId,
        acceptedAt: now,
        events: [event],
        effects: [],
      };
      let validations = 0;
      const rejected = yield* sink
        .commitCommand({
          ...input,
          threadSelfConfiguration: {
            revalidate: () =>
              Effect.gen(function* () {
                validations++;
                // Reservation and callback SQL share the original transaction. A separate validation
                // transaction would either miss this receipt or leave this probe committed on failure.
                assert.isTrue(Option.isSome(yield* receipts.getByCommandId(commandId)));
                yield* sql`UPDATE self_configuration_transaction_probe SET value = 1`;
                return yield* new ThreadSelfConfigurationError({ reason });
              }).pipe(
                Effect.mapError((cause) =>
                  isThreadSelfConfigurationError(cause)
                    ? cause
                    : new ThreadSelfConfigurationError({ reason: "state_unavailable" }),
                ),
              ),
          },
        })
        .pipe(Effect.result);
      assert.equal(rejected._tag, "Failure");
      assert.equal(validations, 1);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(commandId)));
      assert.equal(
        (yield* sql<{ value: number }>`SELECT value FROM self_configuration_transaction_probe`)[0]
          ?.value,
        0,
      );
      assert.equal(yield* sink.latestSequence({ threadId }), sequence);
      assert.deepEqual(
        (yield* projections.getThread(threadId)).modelSelection,
        thread.modelSelection,
      );
      assert.lengthOf(yield* sink.readByCommandId({ commandId }).pipe(Stream.runCollect), 0);
      const effects =
        yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE command_id = ${commandId}`;
      assert.lengthOf(effects, 0);

      const revalidated = { ...event, payload: { ...event.payload, title: "Current metadata" } };
      const accepted = yield* sink.commitCommand({
        ...input,
        threadSelfConfiguration: {
          revalidate: () =>
            Effect.sync(() => {
              validations++;
              return [revalidated];
            }),
        },
      });
      assert.isTrue(accepted.committed);
      assert.equal(validations, 2);
      assert.equal((yield* projections.getThread(threadId)).title, "Current metadata");
      assert.deepEqual((yield* projections.getThread(threadId)).modelSelection, replacement);
      const replay = yield* sink.commitCommand({
        ...input,
        threadSelfConfiguration: {
          revalidate: () => Effect.die("accepted receipt must replay before live revalidation"),
        },
      });
      assert.isFalse(replay.committed);
      assert.deepEqual(replay.receipt, accepted.receipt);
      assert.equal(validations, 2);
      assert.equal(yield* sink.latestSequence({ threadId }), accepted.receipt.resultSequence);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["binding_lost", "later_write_failed"] as const)(
  "continuation %s rolls back its sidecar, receipt and projection; replay never accepts again",
  (failure) =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("continuation-tx:" + failure);
      const thread = makeThread(threadId, ProviderInstanceId.make("claude"), now);
      yield* sink.write({
        events: [
          {
            id: EventId.make("birth:" + failure),
            type: "thread.created",
            threadId,
            occurredAt: now,
            payload: thread,
          },
        ],
      });
      yield* sql`CREATE TABLE jones_continuation_transaction_probe (command_id TEXT PRIMARY KEY)`;
      const commandId = CommandId.make("continuation:" + failure);
      const before = yield* sink.latestSequence({ threadId });
      const input = {
        commandId,
        threadId,
        commandType: "message.dispatch",
        acceptedAt: now,
        effects: [],
        events: [
          {
            id: EventId.make("continued:" + failure),
            type: "thread.metadata-updated" as const,
            threadId,
            occurredAt: now,
            payload: { ...thread, title: "Continuation committed" },
          },
        ],
      };
      let accepts = 0;
      const accept = (refuse: boolean) =>
        Effect.gen(function* () {
          accepts++;
          assert.isTrue(Option.isSome(yield* receipts.getByCommandId(commandId)));
          yield* sql`INSERT INTO jones_continuation_transaction_probe VALUES (${commandId})`;
          if (refuse) return yield* new TaskContinuationError({ reason: "native_reference_moved" });
        }).pipe(
          Effect.mapError((cause) =>
            isTaskContinuationError(cause)
              ? cause
              : new TaskContinuationError({ reason: "storage_unavailable", cause }),
          ),
        );
      const failed = yield* sink
        .commitCommand({
          ...input,
          taskContinuation: { accept: () => accept(failure === "binding_lost") },
          ...(failure === "later_write_failed"
            ? {
                threadTitle: {
                  accept: Effect.fail(
                    new ThreadTitleStateError({ cause: "synthetic failure after projection" }),
                  ),
                },
              }
            : {}),
        })
        .pipe(Effect.result);
      assert.equal(failed._tag, "Failure");
      assert.equal(accepts, 1);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(commandId)));
      assert.deepEqual(yield* sql`SELECT * FROM jones_continuation_transaction_probe`, []);
      assert.equal(yield* sink.latestSequence({ threadId }), before);
      assert.equal((yield* projections.getThread(threadId)).title, thread.title);
      assert.isEmpty(yield* sink.readByCommandId({ commandId }).pipe(Stream.runCollect));
      const committed = yield* sink.commitCommand({
        ...input,
        taskContinuation: { accept: () => accept(false) },
      });
      assert.isTrue(committed.committed);
      assert.equal(accepts, 2);
      const replay = yield* sink.commitCommand({
        ...input,
        taskContinuation: {
          accept: () => Effect.die("replay must retain the original continuation binding"),
        },
      });
      assert.isFalse(replay.committed);
      assert.deepEqual(replay.receipt, committed.receipt);
      assert.equal(accepts, 2);
      assert.deepEqual(yield* sql`SELECT * FROM jones_continuation_transaction_probe`, [
        { command_id: commandId },
      ]);
      assert.equal((yield* projections.getThread(threadId)).title, "Continuation committed");
    }).pipe(Effect.provide(layer)),
);

it.effect("refuses an unowned outer transaction before invoking commit hooks or publishing", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const sink = yield* EventSink.EventSinkV2;
    const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make("unowned-outer-transaction");
    const commandId = CommandId.make("unowned-outer-command");
    let accepted = false;
    const result = yield* sql.withTransaction(
      sink
        .commitCommand({
          commandId,
          threadId,
          commandType: "thread.create",
          acceptedAt: now,
          effects: [],
          events: [
            {
              id: EventId.make("unowned-outer-birth"),
              type: "thread.created",
              threadId,
              occurredAt: now,
              payload: makeThread(threadId, ProviderInstanceId.make("codex"), now),
            },
          ],
          taskContinuation: {
            accept: () =>
              Effect.sync(() => {
                accepted = true;
              }),
          },
        })
        .pipe(Effect.result),
    );
    assert.equal(result._tag, "Failure");
    assert.isFalse(accepted);
    assert.isTrue(Option.isNone(yield* receipts.getByCommandId(commandId)));
    assert.isEmpty(yield* sink.readByCommandId({ commandId }).pipe(Stream.runCollect));
    const nestedBatch = yield* sql.withTransaction(
      sink.withProviderBatch(Effect.void).pipe(Effect.result),
    );
    assert.equal(nestedBatch._tag, "Failure");
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "binds a recreated checkout only while the projection still records the expected checkout",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("checkout-recreation");
      const runId = RunId.make("checkout-recreation:run");
      const activeAttemptId = RunAttemptId.make("checkout-recreation:attempt");
      const providerInstanceId = ProviderInstanceId.make("codex");
      const expected = { worktreePath: "/managed/checkout", branch: "feature/restore" };
      // The snapshot each recreation read before it rebuilt the checkout.
      const stale = { ...makeThread(threadId, providerInstanceId, now), ...expected };
      yield* sink.write({
        events: [
          {
            id: EventId.make("checkout-recreation:birth"),
            type: "thread.created",
            threadId,
            occurredAt: now,
            payload: stale,
          },
          {
            id: EventId.make("checkout-recreation:run"),
            type: "run.created",
            threadId,
            runId,
            occurredAt: now,
            payload: {
              id: runId,
              threadId,
              ordinal: 1,
              providerInstanceId,
              modelSelection: stale.modelSelection,
              providerThreadId: null,
              userMessageId: MessageId.make("checkout-recreation:message"),
              rootNodeId: null,
              activeAttemptId,
              status: "starting",
              requestedAt: now,
              startedAt: null,
              completedAt: null,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
        ],
      });
      // An owner edits the thread after the recreations read their snapshot.
      yield* sink.write({
        events: [
          {
            id: EventId.make("checkout-recreation:owner-edit"),
            type: "thread.metadata-updated",
            threadId,
            occurredAt: now,
            payload: { ...stale, title: "Owner title" },
          },
        ],
      });
      const bind = (key: string, worktreePath: string, checkout: typeof expected) =>
        sink.writeIfRunCurrent({
          commandId: CommandId.make(`checkout-recreation:${key}`),
          threadId,
          runId,
          activeAttemptId,
          expectedStatus: "starting",
          checkoutRecreation: checkout,
          events: [
            {
              id: EventId.make(`checkout-recreation:${key}`),
              type: "thread.metadata-updated",
              threadId,
              occurredAt: now,
              payload: { ...stale, worktreePath },
            },
          ],
        });

      // Two recreations race for one expected checkout on the same database.
      const [first, second] = yield* Effect.all(
        [bind("first", "/managed/first", expected), bind("second", "/managed/second", expected)],
        { concurrency: "unbounded" },
      );
      assert.deepEqual([first.committed, second.committed].toSorted(), [false, true]);
      const winner = first.committed ? "/managed/first" : "/managed/second";
      const loser = first.committed ? "second" : "first";
      const bound = yield* projections.getThread(threadId);
      assert.equal(bound.worktreePath, winner);
      assert.equal(bound.branch, expected.branch);
      // The bind replaces only the checkout fields; the owner's later edit survives.
      assert.equal(bound.title, "Owner title");
      assert.isEmpty(
        yield* sink
          .readByCommandId({ commandId: CommandId.make(`checkout-recreation:${loser}`) })
          .pipe(Stream.runCollect),
      );

      // A recreation still expecting the replaced checkout commits nothing.
      const before = yield* sink.latestSequence({ threadId });
      const staleBind = yield* bind("stale", "/managed/stale", expected);
      assert.isFalse(staleBind.committed);
      assert.isEmpty(staleBind.storedEvents);
      assert.equal(yield* sink.latestSequence({ threadId }), before);
      assert.equal((yield* projections.getThread(threadId)).worktreePath, winner);
      assert.isEmpty(
        yield* sink
          .readByCommandId({ commandId: CommandId.make("checkout-recreation:stale") })
          .pipe(Stream.runCollect),
      );
    }).pipe(Effect.provide(layer)),
);
