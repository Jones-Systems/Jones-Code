import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";

const throughUpgrade = jonesMigrationEntries.filter(([id]) => id <= 115);
const memory = NodeSqliteClient.layer({ filename: ":memory:" });

// Builds released before this migration may already have recorded 114. The guard rejects a
// pending id below a recorded one, so this table must take an id above every released one.
it.effect("115 adds the pending webhook run table to a ledger already recorded through 114", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 115));
    const upstream = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;

    const ledgerBefore = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;

    assert.deepStrictEqual(yield* runJonesMigrations(throughUpgrade), [
      [115, "ScheduledWebhookPendingRuns"],
    ]);
    const ledger =
      yield* sql`SELECT migration_id, name FROM jones_sql_migrations ORDER BY migration_id`;
    assert.deepStrictEqual(ledger, [
      ...ledgerBefore.map(({ migration_id, name }) => ({ migration_id, name })),
      { migration_id: 115, name: "ScheduledWebhookPendingRuns" },
    ]);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id < 115 ORDER BY migration_id`,
      ledgerBefore,
    );
    assert.deepStrictEqual(yield* runJonesMigrations(throughUpgrade), []);
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id, name FROM jones_sql_migrations ORDER BY migration_id`,
      ledger,
    );
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM pragma_table_info('jones_scheduled_webhook_pending_runs')
        ORDER BY cid`,
      [
        { name: "delivery_id" },
        { name: "task_id" },
        { name: "task_created_at" },
        { name: "received_at" },
        { name: "prompt" },
      ],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
      upstream,
    );
  }).pipe(Effect.provide(memory)),
);
