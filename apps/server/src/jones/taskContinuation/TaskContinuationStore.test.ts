import { assert, it } from "@effect/vitest";
import { RunAttemptId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as Store from "./TaskContinuationStore.ts";
import { fixture } from "./testkit.ts";

const database = Sqlite.layerMemory;
const persistence = Layer.mergeAll(Store.layer, ProjectionStore.layer, EventStore.layer).pipe(
  Layer.provideMerge(database),
);
const stores = EventSink.layer.pipe(Layer.provideMerge(persistence));

it.effect(
  "persists only explicit, current per-attempt native confirmation and rejects changed witness bindings",
  () =>
    Effect.gen(function* () {
      const f = fixture();
      const store = yield* Store.TaskContinuationStore;
      const sink = yield* EventSink.EventSinkV2;
      const run = {
        ...f.run,
        id: f.row.runId,
        userMessageId: f.row.messageId,
        delegatedTaskId: f.row.taskId,
        activeAttemptId: RunAttemptId.make("continued-attempt"),
        status: "running" as const,
        completedAt: null,
      };
      const attempt = {
        ...f.attempt,
        id: run.activeAttemptId,
        runId: run.id,
        status: "running" as const,
        completedAt: null,
      };
      yield* sink.write({
        events: [
          f.event("thread.created", f.child.thread.id, f.child.thread),
          f.event("provider-thread.updated", f.child.thread.id, f.provider),
          f.event("run.created", f.child.thread.id, run),
          f.event("run-attempt.created", f.child.thread.id, attempt),
        ],
      });
      yield* store.insert(f.row);
      assert.deepStrictEqual(yield* store.witnesses(f.row), []);
      const input = {
        runId: run.id,
        attemptId: attempt.id,
        nativeSessionId: "original-native",
        runtimeGeneration: "query-generation",
      };
      yield* store.confirm(f.row, input);
      yield* store.confirm(f.row, input);
      assert.deepStrictEqual(yield* store.witnesses(f.row), [input]);
      assert.equal(
        (yield* Effect.result(
          store.confirm(f.row, { ...input, runtimeGeneration: "changed-generation" }),
        ))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.result(
          store.confirm(f.row, { ...input, attemptId: RunAttemptId.make("missing") }),
        ))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.result(
          store.confirm(f.row, { ...input, nativeSessionId: "another-native" }),
        ))._tag,
        "Failure",
      );
      yield* sink.write({
        events: [
          f.event("run.updated", f.child.thread.id, {
            ...run,
            activeAttemptId: RunAttemptId.make("new-attempt"),
          }),
        ],
      });
      assert.equal((yield* Effect.result(store.confirm(f.row, input)))._tag, "Failure");
    }).pipe(Effect.provide(stores)),
);

it.effect("rolls the sparse acceptance row back with the owning SQL transaction", () =>
  Effect.gen(function* () {
    const f = fixture();
    const store = yield* Store.TaskContinuationStore;
    const sql = yield* SqlClient.SqlClient;
    const result = yield* Effect.result(
      sql.withTransaction(
        Effect.gen(function* () {
          yield* store.insert(f.row);
          return yield* Effect.fail("rollback-probe");
        }),
      ),
    );
    assert.equal(result._tag, "Failure");
    assert.isNull(yield* store.get(f.row.messageId));
    yield* store.insert(f.row);
    assert.deepStrictEqual(yield* store.get(f.row.messageId), f.row);
  }).pipe(Effect.provide(stores)),
);
