import { NodeHttpServer } from "@effect/platform-node";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import * as PeerLinks from "../../peer/PeerLinks.ts";
import {
  descriptorOf,
  layerLinkingEnvironment,
  linkTo,
  servePeer,
} from "../../peer/PeerLinks.testkit.ts";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import { peerEnvironmentLinksTable } from "./table.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });

const assertJonesPeerSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  assert.deepStrictEqual(
    yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = 119`,
    [{ migration_id: 119, name: "PeerEnvironmentLinks" }],
  );
  assert.deepStrictEqual(
    yield* sql`SELECT name FROM sqlite_master WHERE name = 'peer_environment_links'`,
    [],
  );
  assert.deepStrictEqual(
    yield* sql`SELECT * FROM pragma_foreign_key_list(${peerEnvironmentLinksTable})`,
    [],
  );
  const columns = yield* sql<{ readonly name: string }>`
    SELECT name FROM pragma_table_info(${peerEnvironmentLinksTable}) ORDER BY cid
  `;
  assert.deepStrictEqual(
    columns.map(({ name }) => name),
    [
      "environment_id",
      "secret_name",
      "label",
      "urls_json",
      "access",
      "linked_at",
      "expires_at",
      "last_reached_at",
      "last_error",
      "fenced",
    ],
  );
});

it.effect("fresh startup keeps experimental peer links entirely in the Jones ledger", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    const upstream = yield* sql<{ readonly migration_id: number; readonly name: string }>`
      SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
    `;
    assert.deepStrictEqual(
      upstream.map(({ migration_id, name }) => [migration_id, name] as const),
      migrationManifest,
    );
    assert.strictEqual(upstream.at(-1)?.migration_id, 63);
    yield* assertJonesPeerSchema;
    assert.deepStrictEqual(yield* sql`SELECT * FROM ${sql(peerEnvironmentLinksTable)}`, []);
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(memory)),
);

it.effect("released schema 63 keeps its rows and leaves IDs 64–67 available to upstream", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* runMigrations({ toMigrationInclusive: 63 });
    yield* sql`
      INSERT INTO orchestration_command_receipts
        (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
      VALUES ('synthetic-peer-command', 'thread', 'synthetic-thread', '2026-10-21', 1, 'accepted')
    `;
    const objects = yield* sql<{
      readonly type: string;
      readonly name: string;
      readonly sql: string | null;
    }>`
      SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name
    `;
    const tables = objects.filter(({ type }) => type === "table");
    const rowsBefore = yield* Effect.forEach(tables, ({ name }) => sql`SELECT * FROM ${sql(name)}`);

    assert.deepStrictEqual(yield* runMigrations(), []);
    yield* assertJonesPeerSchema;
    const objectsAfter = yield* sql`
      SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name
    `;
    for (const object of objects) assert.deepInclude(objectsAfter, object);
    assert.deepStrictEqual(
      yield* Effect.forEach(tables, ({ name }) => sql`SELECT * FROM ${sql(name)}`),
      rowsBefore,
    );

    // Synthetic future migrations prove these numbers remain free without
    // adopting or predicting the contents of any future upstream migration.
    const future = [64, 65, 66, 67].map((id) => [id, `SyntheticFuture${id}`] as const);
    const executed = yield* Migrator.make({})({
      loader: Migrator.fromRecord(
        Object.fromEntries(
          future.map(([id, name]) => [
            `${id}_${name}`,
            sql`CREATE TABLE ${sql(`synthetic_upstream_${id}`)} (id TEXT PRIMARY KEY)`.pipe(
              Effect.asVoid,
            ),
          ]),
        ),
      ),
    });
    assert.deepStrictEqual(executed, future);
    const futureHistory = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
    assert.deepStrictEqual(yield* runMigrations(), []);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
      futureHistory,
    );
    yield* assertJonesPeerSchema;
  }).pipe(Effect.provide(memory)),
);

it.effect("pairing creates a sparse Jones row and unlink removes only the local link", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const peer = yield* servePeer(descriptorOf("environment-box", "Box"), Layer.empty);
      const context = yield* Layer.build(
        layerLinkingEnvironment(descriptorOf("environment-laptop", "Laptop")),
      );
      const links = Context.get(context, PeerLinks.PeerLinks);
      const sql = Context.get(context, SqlClient.SqlClient);
      const rows = sql<{
        readonly environment_id: string;
        readonly secret_name: string;
        readonly fenced: number;
      }>`SELECT environment_id, secret_name, fenced FROM ${sql(peerEnvironmentLinksTable)}`;
      assert.deepStrictEqual(yield* rows, []);
      const linked = yield* linkTo(links, peer);
      const stored = yield* rows;
      assert.strictEqual(stored.length, 1);
      assert.strictEqual(stored[0]?.environment_id, linked.environmentId);
      assert.strictEqual(stored[0]?.fenced, 1);
      assert.match(stored[0]!.secret_name, /^peer-link-[a-f0-9]+$/);
      assert.strictEqual(
        (yield* links.resolve(linked.environmentId)).link.environmentId,
        linked.environmentId,
      );
      assert.strictEqual((yield* peer.linkedSessions).length, 1);

      assert.isTrue(yield* links.unlink(linked.environmentId));
      assert.deepStrictEqual(yield* rows, []);
      assert.isTrue(Option.isNone(yield* links.get(linked.environmentId)));
      assert.strictEqual(
        (yield* links.resolve(linked.environmentId).pipe(Effect.flip)).reason,
        "unknown_link",
      );
      assert.isFalse(yield* links.unlink(linked.environmentId));
      assert.strictEqual((yield* peer.linkedSessions).length, 1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
