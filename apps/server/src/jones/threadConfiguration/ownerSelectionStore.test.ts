import { assert, it } from "@effect/vitest";
import { CommandId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import migration from "../persistence/Migrations/132_JonesOwnerSelections.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { makeOwnerSelectionStore } from "./ownerSelectionStore.ts";

const threadId = ThreadId.make("owner-store-thread");
const selection = { instanceId: ProviderInstanceId.make("a"), model: "model" };
const event = {
  type: "thread.model-selection-updated",
  threadId,
  payload: { modelSelection: selection },
};
const input = {
  threadId,
  commandId: CommandId.make("owner"),
  sequence: 10,
  events: [event],
  record: selection,
  recordedAt: DateTime.makeUnsafe("2026-10-26T00:00:00Z"),
};

it.effect("missing Jones tables remain compatible and become visible after migration", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = makeOwnerSelectionStore(sql, makeJonesTablePresence(sql));
    assert.isNull(yield* store.read(threadId));
    yield* store.commit(input);
    yield* migration;
    yield* store.commit(input);
    assert.deepEqual(yield* store.pending(threadId, selection), selection);
    assert.isUndefined(
      yield* store.pending(threadId, { ...selection, options: [{ id: "fast", value: true }] }),
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "owner rows are monotonic, cleared only by a newer selection commit, and transactional",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migration;
      const store = makeOwnerSelectionStore(sql, makeJonesTablePresence(sql));
      yield* store.commit(input);
      const original = yield* store.read(threadId);
      yield* store.commit({ ...input, record: { ...selection, model: "older" }, sequence: 9 });
      yield* store.commit({
        ...input,
        commandId: CommandId.make("duplicate"),
        record: { ...selection, model: "duplicate" },
      });
      assert.deepEqual(yield* store.read(threadId), original);
      yield* store.commit({ ...input, sequence: 11, record: undefined, events: [] });
      assert.deepEqual(yield* store.read(threadId), original);
      yield* store.commit({
        ...input,
        sequence: 11,
        record: undefined,
        events: [{ ...event, threadId: ThreadId.make("other-thread") }],
      });
      assert.deepEqual(yield* store.read(threadId), original);
      yield* store.commit({ ...input, sequence: 9, record: undefined });
      assert.deepEqual(yield* store.read(threadId), original);
      const rolledBack = yield* store
        .commit({ ...input, sequence: 11, record: undefined })
        .pipe(Effect.andThen(Effect.fail("abort command")), sql.withTransaction, Effect.exit);
      assert.equal(rolledBack._tag, "Failure");
      assert.deepEqual(yield* store.read(threadId), original);
      yield* store.commit({ ...input, sequence: 11, record: undefined });
      assert.isNull(yield* store.read(threadId));
      yield* sql`INSERT INTO jones_owner_selections VALUES (${threadId}, 'malformed', 12, '{}', 'synthetic')`;
      assert.isNull(yield* store.read(threadId));
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
