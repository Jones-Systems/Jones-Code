import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { makeCommitTransaction } from "./CommitTransaction.ts";

describe("owning commit transaction", () => {
  it.effect(
    "publishes captured handles only after the outer commit and releases its lane last",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const tx = yield* makeCommitTransaction();
        const order: Array<string> = [];
        const lane = {};
        yield* sql`CREATE TABLE fixture_commit (value TEXT NOT NULL)`;
        yield* tx.withTransaction(
          Effect.gen(function* () {
            yield* tx.retainUntilSettlement(
              lane,
              Effect.sync(() => {
                order.push("acquire");
              }),
              Effect.sync(() => {
                order.push("release");
              }),
            );
            yield* sql`INSERT INTO fixture_commit VALUES ('outer')`;
            yield* tx.withTransaction(
              Effect.gen(function* () {
                yield* tx.retainUntilSettlement(
                  lane,
                  Effect.sync(() => {
                    order.push("duplicate acquire");
                  }),
                  Effect.sync(() => {
                    order.push("duplicate release");
                  }),
                );
                yield* sql`INSERT INTO fixture_commit VALUES ('inner')`;
                yield* tx.afterCommit(
                  Effect.gen(function* () {
                    assert.equal(
                      (yield* sql`SELECT * FROM fixture_commit`.pipe(Effect.orDie)).length,
                      2,
                    );
                    order.push("publish inner");
                  }),
                );
              }),
            );
            yield* tx.afterCommit(
              Effect.sync(() => {
                order.push("publish outer");
              }),
            );
            assert.deepEqual(order, ["acquire"]);
          }),
        );
        assert.deepEqual(order, ["acquire", "publish inner", "publish outer", "release"]);
      }).pipe(Effect.provide(SqlitePersistenceMemory)),
  );

  it.effect("discards rolled-back publications and still releases a captured lane", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const tx = yield* makeCommitTransaction();
      const order: Array<string> = [];
      yield* sql`CREATE TABLE fixture_rollback (value TEXT NOT NULL)`;
      const result = yield* Effect.exit(
        tx.withTransaction(
          Effect.gen(function* () {
            yield* tx.retainUntilSettlement(
              {},
              Effect.sync(() => {
                order.push("acquire");
              }),
              Effect.sync(() => {
                order.push("release");
              }),
            );
            yield* sql`INSERT INTO fixture_rollback VALUES ('uncommitted')`;
            yield* tx.afterCommit(
              Effect.sync(() => {
                order.push("escaped handle");
              }),
            );
            return yield* Effect.fail("retained fixture failure");
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(result));
      assert.equal((yield* sql`SELECT * FROM fixture_rollback`).length, 0);
      assert.deepEqual(order, ["acquire", "release"]);
    }).pipe(Effect.provide(SqlitePersistenceMemory)),
  );

  it.effect("refuses an unknown enclosing SQL transaction", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const tx = yield* makeCommitTransaction();
      let escaped = false;
      const result = yield* Effect.exit(
        sql.withTransaction(
          tx.withTransaction(
            tx.afterCommit(
              Effect.sync(() => {
                escaped = true;
              }),
            ),
          ),
        ),
      );
      assert.isTrue(Exit.isFailure(result));
      assert.isFalse(escaped);
    }).pipe(Effect.provide(SqlitePersistenceMemory)),
  );
});
