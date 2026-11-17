import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const through158 = jonesMigrationEntries.filter(([id]) => id <= 158);
const objects = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{ readonly name: string }>`SELECT type,name,tbl_name,sql FROM sqlite_master
    WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`;
});

it.effect("158 adds only the decision table and its index, idempotently", () =>
  Effect.gen(function* () {
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    yield* runJonesMigrations(through158.filter(([id]) => id < 158));
    const before = yield* objects;
    assert.deepStrictEqual(yield* runJonesMigrations(through158), [[158, "WorkstreamAutoRoute"]]);
    const after = yield* objects;
    assert.deepStrictEqual(
      after.filter(({ name }) => !name.startsWith("jones_workstream_auto_route")),
      before,
    );
    assert.deepStrictEqual(
      after.filter(({ name }) => name.startsWith("jones_workstream_auto_route")).map((o) => o.name),
      ["jones_workstream_auto_route_by_thread", "jones_workstream_auto_route_decisions"],
    );
    assert.deepStrictEqual(yield* runJonesMigrations(through158), []);
  }).pipe(Effect.provide(memory)),
);
