import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import effectCancellation from "../../persistence/Migrations/OrchestrationV2/EffectCancellation.ts";
import foreignHistory from "./fixtures/foreignJonesHistory.json" with { type: "json" };
import { runJonesMigrations } from "./JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "./JonesMigrations.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
// The foreign chain was written against upstream through 56, the last upstream
// migration its released databases ran before 7-13 created their tables.
const foreignUpstream = 56;
const latestJones = jonesMigrationEntries.at(-1)!;

// Released Jones 1-6 as those databases hold them. 002 then added this column;
// the current 002 is a no-op, so the released ALTER is replayed here.
const releasedJones = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* runMigrations({ toMigrationInclusive: foreignUpstream });
  yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 7));
  yield* sql`ALTER TABLE projection_thread_sessions ADD COLUMN runtime_identity_json TEXT`;
});

const snapshotForeign = (names: ReadonlySet<string>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const objects = (yield* sql<{
      readonly type: string;
      readonly name: string;
      readonly tbl_name: string;
      readonly sql: string | null;
    }>`SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY name`).filter((object) =>
      names.has(object.name),
    );
    const rows: Record<string, ReadonlyArray<unknown>> = {};
    for (const object of objects)
      if (object.type === "table")
        rows[object.name] = yield* sql.unsafe(
          `SELECT rowid, * FROM "${object.name}" ORDER BY rowid`,
        );
    const ledger =
      yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id < 100 ORDER BY migration_id`;
    return { objects, rows, ledger };
  });

it.effect.each([11, 13])(
  "receiving startup preserves populated foreign history 7-%i without adopting it",
  (prefix) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* releasedJones;
      const baseline = new Set(
        (yield* sql<{ readonly name: string }>`SELECT name FROM sqlite_master`).map(
          (object) => object.name,
        ),
      );
      for (const migration of foreignHistory.migrations.filter(({ id }) => id <= prefix)) {
        for (const statement of migration.statements) yield* sql.unsafe(statement);
        yield* sql`INSERT INTO jones_sql_migrations (migration_id, name) VALUES (${migration.id}, ${migration.name})`;
      }
      for (const statement of foreignHistory.syntheticSeeds) yield* sql.unsafe(statement);
      const foreign = new Set(
        (yield* sql<{ readonly name: string }>`SELECT name FROM sqlite_master`)
          .map((object) => object.name)
          .filter((name) => !baseline.has(name)),
      );
      const before = yield* snapshotForeign(foreign);
      assert.isAbove(Object.keys(before.rows).length, 0);
      assert.deepEqual(
        before.ledger.map((row) => (row as { migration_id: number }).migration_id),
        [1, 2, 3, 4, 5, 6, ...Array.from({ length: prefix - 6 }, (_, index) => index + 7)],
      );

      assert.deepEqual(
        yield* runMigrations(),
        migrationManifest
          .filter(([id]) => id > foreignUpstream)
          .map(([id, name]) => [id, name] as const),
      );
      assert.deepEqual(yield* snapshotForeign(foreign), before);
      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = ${latestJones[0]}`,
        [{ migration_id: latestJones[0], name: latestJones[1] }],
      );
      assert.deepEqual(yield* runMigrations(), []);
      assert.deepEqual(yield* snapshotForeign(foreign), before);

      // The foreign tables still reference upstream rows, so those rows cannot
      // be deleted. This is recorded behavior, not a goal.
      for (const [table, key, id] of [
        ["orchestration_command_receipts", "command_id", "fixture-settle"],
        ["orchestration_events", "event_id", "fixture-event"],
        ["orchestration_v2_effect_outbox", "effect_id", "fixture-effect"],
      ] as const)
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(sql.unsafe(`DELETE FROM "${table}" WHERE "${key}" = ?`, [id])),
          ),
          table,
        );

      // Future-rebuild characterization only. Upstream 55 already ran this
      // rebuild before the foreign tables existed, so no current pending
      // migration runs it. A later upstream rebuild of the outbox would fail
      // the same way, and must roll back without changing foreign state.
      assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.withTransaction(effectCancellation))));
      assert.deepEqual(yield* snapshotForeign(foreign), before);
      assert.deepEqual(yield* sql`PRAGMA foreign_key_check`, []);
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name = 'orchestration_v2_effect_outbox_next'`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect("receiving startup preserves the single foreign 007 lookup index", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* releasedJones;
    yield* sql`CREATE INDEX IF NOT EXISTS idx_orch_events_thread_creation_lookup
      ON orchestration_events (stream_id, sequence DESC, event_id)
      WHERE aggregate_kind = 'thread' AND event_type = 'thread.created'`;
    yield* sql`INSERT INTO jones_sql_migrations (migration_id, name) VALUES (7, 'ThreadCreationLookupIndex')`;
    const before = yield* snapshotForeign(new Set(["idx_orch_events_thread_creation_lookup"]));
    yield* runMigrations();
    assert.deepEqual(
      yield* snapshotForeign(new Set(["idx_orch_events_thread_creation_lookup"])),
      before,
    );
    assert.deepEqual(
      yield* sql`SELECT name FROM jones_sql_migrations WHERE migration_id = ${latestJones[0]}`,
      [{ name: latestJones[1] }],
    );
  }).pipe(Effect.provide(memory)),
);
