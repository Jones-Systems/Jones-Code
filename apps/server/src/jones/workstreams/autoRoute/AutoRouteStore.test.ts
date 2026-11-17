import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../../persistence/Migrations/158_JonesWorkstreamAutoRoute.ts";
import * as Store from "./AutoRouteStore.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const threadId = ThreadId.make("synthetic-thread");
const key = { threadId, commandId: "cmd" };

it.effect("reads as absent when the Jones table is missing", () =>
  Effect.gen(function* () {
    const store = yield* Store.make;
    assert.isFalse(yield* store.available);
    assert.isNull(yield* store.latest(threadId));
    assert.isUndefined(yield* store.currentAutoAssignments());
    assert.deepStrictEqual(yield* store.recoverInterrupted("2026-01-01T00:00:00.000Z"), {
      interrupted: 0,
      reconciled: 0,
    });
  }).pipe(Effect.provide(memory)),
);

it.effect("writes a row only for an existing upstream thread and holds no text columns", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY)`;
    yield* migration;
    const store = yield* Store.make;
    const begin = (id: ThreadId) =>
      store.begin({
        key: { ...key, threadId: id },
        messageId: "m",
        origin: "queue_http",
        now: "2026-01-01T00:00:00.000Z",
      });
    assert.strictEqual(yield* begin(threadId), "thread_missing");
    yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES(${threadId})`;
    assert.strictEqual(yield* begin(threadId), "inserted");
    assert.strictEqual(yield* begin(threadId), "duplicate");
    yield* store.finalize(
      key,
      { state: "abstained", code: "unclear", confidence: "low", basis: "none" },
      "2026-01-01T00:00:01.000Z",
    );
    assert.deepStrictEqual(yield* store.latest(threadId), {
      state: "abstained",
      code: "unclear",
      workstreamId: null,
      decidedAt: "2026-01-01T00:00:01.000Z",
    });
    const columns = yield* sql<{
      name: string;
    }>`SELECT name FROM pragma_table_info('jones_workstream_auto_route_decisions')`;
    assert.deepStrictEqual(
      columns.map((c) => c.name).filter((name) => /text|prompt|title|reason|reply/.test(name)),
      [],
    );
    const foreignKeys =
      yield* sql`SELECT * FROM pragma_foreign_key_list('jones_workstream_auto_route_decisions')`;
    assert.lengthOf(foreignKeys, 0);
  }).pipe(Effect.provide(memory)),
);
