import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });

// Builds released before this migration may already have recorded 114. The guard rejects a
// pending id below a recorded one, so this table must take an id above every released one.
it.effect("115 adds the pending webhook run table to a ledger already recorded through 114", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 115));
    const upstream = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;

    assert.deepStrictEqual(yield* runJonesMigrations(jonesMigrationEntries), [
      [115, "ScheduledWebhookPendingRuns"],
      [116, "RemoteAdmission"],
      [117, "RemoteDelegationIntents"],
      [118, "RemoteAdmissionIntegration"],
      [119, "PeerEnvironmentLinks"],
    ]);
    assert.deepStrictEqual(yield* runJonesMigrations(jonesMigrationEntries), []);
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
