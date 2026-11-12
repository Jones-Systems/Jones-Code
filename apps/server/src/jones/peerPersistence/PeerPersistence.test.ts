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
import { runJonesMigrations } from "../persistence/JonesMigrationGuard.ts";
import * as JonesMigrations from "../persistence/JonesMigrations.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });

// Jones kept links in this table (migration 119) before upstream gained its own.
// Migration 152 copies them across and leaves this table for rollback.
const legacyTable = "jones_peer_environment_links";
const adoptionId = 152;
const lastUpstreamId = Math.max(...migrationManifest.map(([id]) => id));

const jonesEntriesBeforeAdoption = JonesMigrations.jonesMigrationEntries.filter(
  ([id]) => id < adoptionId,
);

const assertUpstreamPeerSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql`PRAGMA table_info(peer_environment_links)`;
  assert.deepStrictEqual(columns, [
    { cid: 0, name: "environment_id", type: "TEXT", notnull: 0, dflt_value: null, pk: 1 },
    { cid: 1, name: "secret_name", type: "TEXT", notnull: 1, dflt_value: null, pk: 0 },
    { cid: 2, name: "label", type: "TEXT", notnull: 1, dflt_value: null, pk: 0 },
    { cid: 3, name: "urls_json", type: "TEXT", notnull: 1, dflt_value: null, pk: 0 },
    { cid: 4, name: "access", type: "TEXT", notnull: 1, dflt_value: null, pk: 0 },
    { cid: 5, name: "linked_at", type: "TEXT", notnull: 1, dflt_value: null, pk: 0 },
    { cid: 6, name: "expires_at", type: "TEXT", notnull: 1, dflt_value: null, pk: 0 },
    { cid: 7, name: "last_reached_at", type: "TEXT", notnull: 0, dflt_value: null, pk: 0 },
    { cid: 8, name: "last_error", type: "TEXT", notnull: 0, dflt_value: null, pk: 0 },
    { cid: 9, name: "fenced", type: "INTEGER", notnull: 1, dflt_value: "0", pk: 0 },
  ]);
  assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_list(peer_environment_links)`, []);
});

const linkRow = (environmentId: string, overrides: Record<string, unknown> = {}) => ({
  environment_id: environmentId,
  secret_name: `peer-link-${environmentId}`,
  label: `Label ${environmentId}`,
  urls_json: `["https://${environmentId}.invalid"]`,
  access: "auto",
  linked_at: "2026-10-01T00:00:00.000Z",
  expires_at: "2027-10-01T00:00:00.000Z",
  last_reached_at: "2026-10-02T00:00:00.000Z",
  last_error: "synthetic preserved error",
  fenced: 1,
  ...overrides,
});

const assertAdoptionRecorded = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  assert.deepStrictEqual(
    yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = ${adoptionId}`,
    [{ migration_id: adoptionId, name: "PeerLinksAdoption" }],
  );
});

it.effect(
  "fresh startup records the adoption, creates the upstream table and keeps the Jones one",
  () =>
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
      yield* assertAdoptionRecorded;
      yield* assertUpstreamPeerSchema;
      assert.deepStrictEqual(yield* sql`SELECT * FROM peer_environment_links`, []);
      // Left in place so a rollback to the build that still reads it finds its rows.
      assert.deepStrictEqual(yield* sql`SELECT * FROM ${sql(legacyTable)}`, []);
      assert.deepStrictEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "adoption copies Jones links into the upstream table and keeps a newer upstream row",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: lastUpstreamId });
      yield* runJonesMigrations(jonesEntriesBeforeAdoption);
      yield* assertUpstreamPeerSchema;

      const rowA = linkRow("synthetic-env-a");
      const rowB = linkRow("synthetic-env-b", {
        fenced: 0,
        last_reached_at: null,
        last_error: null,
      });
      const legacy = [rowA, rowB, linkRow("synthetic-env-shared", { label: "Jones copy" })];
      for (const row of legacy) {
        yield* sql`INSERT INTO ${sql(legacyTable)} ${sql.insert(row)}`;
      }
      const upstreamRow = linkRow("synthetic-env-shared", { label: "Upstream copy" });
      yield* sql`INSERT INTO peer_environment_links ${sql.insert(upstreamRow)}`;

      assert.deepStrictEqual(
        (yield* runJonesMigrations(JonesMigrations.jonesMigrationEntries)).map(([id]) => id),
        [adoptionId],
      );
      yield* assertAdoptionRecorded;

      const adopted = yield* sql`SELECT * FROM peer_environment_links ORDER BY environment_id`;
      assert.deepStrictEqual(adopted, [rowA, rowB, upstreamRow]);
      // The Jones rows stay as the pre-adoption record.
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM ${sql(legacyTable)} ORDER BY environment_id`,
        legacy,
      );

      // A second run has nothing to do and does not copy again.
      assert.deepStrictEqual(yield* runJonesMigrations(JonesMigrations.jonesMigrationEntries), []);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM peer_environment_links ORDER BY environment_id`,
        adopted,
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "released schema 63 keeps its Jones links through upstream catch-up and then adopts them",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations({ toMigrationInclusive: 63 });
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name = 'peer_environment_links'`,
        [],
      );
      yield* runJonesMigrations(jonesEntriesBeforeAdoption);
      const legacy = linkRow("synthetic-released-env");
      yield* sql`INSERT INTO ${sql(legacyTable)} ${sql.insert(legacy)}`;
      yield* sql`
        INSERT INTO orchestration_command_receipts
          (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
        VALUES ('synthetic-peer-command', 'thread', 'synthetic-thread', '2026-10-21', 1, 'accepted')
      `;

      assert.deepStrictEqual(
        yield* runMigrations(),
        migrationManifest.filter(([id]) => id > 63),
      );
      yield* assertAdoptionRecorded;
      yield* assertUpstreamPeerSchema;
      assert.deepStrictEqual(yield* sql`SELECT * FROM peer_environment_links`, [legacy]);
      assert.deepStrictEqual(yield* sql`SELECT * FROM ${sql(legacyTable)}`, [legacy]);
      assert.deepStrictEqual(yield* sql`SELECT command_id FROM orchestration_command_receipts`, [
        { command_id: "synthetic-peer-command" },
      ]);

      // Synthetic later upstream migrations prove Jones still leaves future upstream
      // IDs available without predicting their contents.
      const future = [lastUpstreamId + 1, lastUpstreamId + 2].map(
        (id) => [id, `SyntheticFuture${id}`] as const,
      );
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
      assert.deepStrictEqual(yield* sql`SELECT * FROM peer_environment_links`, [legacy]);
    }).pipe(Effect.provide(memory)),
);

it.effect("pairing writes the upstream table and unlink removes only the local link", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const peer = yield* servePeer(descriptorOf("environment-box", "Box"), Layer.empty);
      const context = yield* Layer.build(
        layerLinkingEnvironment(descriptorOf("environment-laptop", "Laptop")),
      );
      const links = Context.get(context, PeerLinks.PeerLinks);
      const sql = Context.get(context, SqlClient.SqlClient);
      yield* assertUpstreamPeerSchema.pipe(Effect.provideService(SqlClient.SqlClient, sql));
      yield* JonesMigrations.runMigrations().pipe(Effect.provideService(SqlClient.SqlClient, sql));
      const rows = sql<{
        readonly environment_id: string;
        readonly secret_name: string;
        readonly fenced: number;
      }>`SELECT environment_id, secret_name, fenced FROM peer_environment_links`;
      const legacyRows = sql`SELECT * FROM ${sql(legacyTable)}`;
      assert.deepStrictEqual(yield* rows, []);
      const linked = yield* linkTo(links, peer);
      const stored = yield* rows;
      assert.strictEqual(stored.length, 1);
      assert.strictEqual(stored[0]?.environment_id, linked.environmentId);
      assert.strictEqual(stored[0]?.fenced, 1);
      assert.match(stored[0]!.secret_name, /^peer-link-[a-f0-9]+$/);
      // Nothing is written to the retained Jones table any more.
      assert.deepStrictEqual(yield* legacyRows, []);
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
      assert.deepStrictEqual(yield* legacyRows, []);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
