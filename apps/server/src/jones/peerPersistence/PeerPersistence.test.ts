import * as NodeURL from "node:url";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import { assert, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
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
import { peerEnvironmentLinksTable } from "./table.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });

// Upstream now ships its own peer_environment_links (070, 071). Jones keeps its links in
// the Jones table and never reads or writes upstream's, which stays empty.
const upstreamTable = "peer_environment_links";
const lastUpstreamId = Math.max(...migrationManifest.map(([id]) => id));
// The last upstream and Jones migrations a main build ran before this sync.
const mainUpstreamId = 69;
const mainJonesId = 147;

const columns = [
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
];

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

const jonesLinks = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT * FROM ${sql(peerEnvironmentLinksTable)} ORDER BY environment_id`;
});

const upstreamLinks = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT * FROM ${sql(upstreamTable)}`;
});

const assertJonesPeerSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  assert.deepStrictEqual(
    yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = 119`,
    [{ migration_id: 119, name: "PeerEnvironmentLinks" }],
  );
  assert.deepStrictEqual(
    yield* sql`SELECT * FROM pragma_foreign_key_list(${peerEnvironmentLinksTable})`,
    [],
  );
  const names = yield* sql<{ readonly name: string }>`
    SELECT name FROM pragma_table_info(${peerEnvironmentLinksTable}) ORDER BY cid
  `;
  assert.deepStrictEqual(
    names.map(({ name }) => name),
    columns,
  );
});

const jonesLedger = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM jones_sql_migrations ORDER BY migration_id
  `;
  return rows.map(({ migration_id, name }) => [migration_id, name] as const);
});

it.effect("fresh startup keeps peer links in the Jones table and leaves upstream's empty", () =>
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
    yield* assertJonesPeerSchema;
    assert.deepStrictEqual(yield* jonesLinks, []);
    assert.deepStrictEqual(yield* upstreamLinks, []);
    // Identity 152 is recorded with no effect: upstream's table stays empty.
    assert.deepInclude(yield* jonesLedger, [152, "PeerLinksAdoption"]);
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(memory)),
);

it.effect("upgrading a main database keeps every Jones link and writes no upstream row", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: mainUpstreamId });
    yield* runJonesMigrations(
      JonesMigrations.jonesMigrationEntries.filter(([id]) => id <= mainJonesId),
    );
    const legacy = [
      linkRow("synthetic-env-a"),
      linkRow("synthetic-env-b", { fenced: 0, last_reached_at: null, last_error: null }),
    ];
    for (const row of legacy) {
      yield* sql`INSERT INTO ${sql(peerEnvironmentLinksTable)} ${sql.insert(row)}`;
    }
    const before = yield* jonesLedger;

    yield* runMigrations();

    assert.deepStrictEqual(yield* jonesLinks, legacy);
    assert.deepStrictEqual(yield* upstreamLinks, []);
    const added = (yield* jonesLedger).filter(
      ([id]) => !before.some(([existing]) => existing === id),
    );
    assert.deepStrictEqual(
      added.map(([id]) => id),
      JonesMigrations.jonesMigrationEntries.map(([id]) => id).filter((id) => id > mainJonesId),
    );
    assert.isTrue(added.every(([id]) => id > mainJonesId));
  }).pipe(Effect.provide(memory)),
);

// Replica of the unreleased adoption body that 152 ran before it became a no-op. Synthetic
// databases use it to reproduce a ledger and upstream residue such a build left behind.
const unreleasedAdoption = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT OR IGNORE INTO peer_environment_links (
      environment_id, secret_name, label, urls_json, access, linked_at, expires_at,
      last_reached_at, last_error, fenced
    )
    SELECT
      environment_id, secret_name, label, urls_json, access, linked_at, expires_at,
      last_reached_at, last_error, fenced
    FROM jones_peer_environment_links
  `;
});

it.effect(
  "a database that recorded the unreleased adoption starts and its upstream copies stay untouched",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: lastUpstreamId });
      yield* runJonesMigrations(JonesMigrations.jonesMigrationEntries.filter(([id]) => id <= 151));
      const legacy = [linkRow("synthetic-env-a"), linkRow("synthetic-env-b", { fenced: 0 })];
      for (const row of legacy) {
        yield* sql`INSERT INTO ${sql(peerEnvironmentLinksTable)} ${sql.insert(row)}`;
      }
      yield* runJonesMigrations([
        ...JonesMigrations.jonesMigrationEntries.filter(([id]) => id <= 151),
        [152, "PeerLinksAdoption", unreleasedAdoption],
      ]);
      // That build then wrote new links only to upstream's table.
      yield* sql`INSERT INTO ${sql(upstreamTable)} ${sql.insert(linkRow("synthetic-env-after-adoption"))}`;
      const upstreamBefore = yield* upstreamLinks;
      assert.strictEqual(upstreamBefore.length, 3);
      const ledgerBefore = yield* jonesLedger;
      assert.deepInclude(ledgerBefore, [152, "PeerLinksAdoption"]);

      assert.deepStrictEqual(yield* runMigrations(), []);

      const expectedLedger = [
        ...ledgerBefore,
        [153, "PersonalWorkstreams"],
        ...JonesMigrations.jonesMigrationEntries
          .filter(([id]) => id > 153)
          .map(([id, name]) => [id, name]),
      ];
      assert.deepStrictEqual(yield* jonesLedger, expectedLedger);
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* jonesLedger, expectedLedger);
      assert.deepStrictEqual(yield* jonesLinks, legacy);
      // No cleanup or repair: residue is neither read, rewritten nor removed.
      assert.deepStrictEqual(yield* upstreamLinks, upstreamBefore);
    }).pipe(Effect.provide(memory)),
);

it.effect("a database from the earlier unreleased 147-151 numbering is refused unchanged", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: lastUpstreamId });
    const effectOf = (name: string) =>
      JonesMigrations.jonesMigrationEntries.find(([, entry]) => entry === name)![2];
    // Builds before d35c6b7d75 ran these identities before main's 147 QueueHolds existed.
    const earlier = [
      ...JonesMigrations.jonesMigrationEntries.filter(([id]) => id <= 146),
      [147, "NativeExecutionProcessCustody", effectOf("NativeExecutionProcessCustody")],
      [148, "OrdinaryCheckoutProcessCustody", effectOf("OrdinaryCheckoutProcessCustody")],
      [149, "AuthSessionClientMetadata", effectOf("AuthSessionClientMetadata")],
      [150, "WorkspaceRuntimeUses", effectOf("WorkspaceRuntimeUses")],
      [151, "PeerLinksAdoption", unreleasedAdoption],
    ] as const;
    yield* runJonesMigrations(earlier.filter(([id]) => id <= 146));
    yield* sql`INSERT INTO ${sql(peerEnvironmentLinksTable)} ${sql.insert(linkRow("synthetic-env-a"))}`;
    yield* runJonesMigrations(earlier);
    assert.strictEqual((yield* upstreamLinks).length, 1);
    const upstreamBefore = yield* upstreamLinks;
    const ledgerBefore = yield* jonesLedger;

    const refused = yield* Effect.flip(JonesMigrations.runMigrations());
    assert.include(String(refused), "unrecognized 147_NativeExecutionProcessCustody");
    assert.deepStrictEqual(yield* jonesLedger, ledgerBefore);
    assert.deepStrictEqual(yield* upstreamLinks, upstreamBefore);
  }).pipe(Effect.provide(memory)),
);

it.effect("the Jones table matches upstream's columns so upstream link code runs unchanged", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    // A failure here means upstream changed its table: add a matching Jones migration.
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM pragma_table_info(${peerEnvironmentLinksTable})`,
      yield* sql`SELECT * FROM pragma_table_info(${upstreamTable})`,
    );
    assert.deepStrictEqual(
      yield* sql`
        SELECT type, name FROM sqlite_master
        WHERE tbl_name = ${upstreamTable} AND type IN ('index', 'trigger') AND sql IS NOT NULL
      `,
      [],
    );
  }).pipe(Effect.provide(memory)),
);

const serverRoot = NodeURL.fileURLToPath(new URL("../../../", import.meta.url));
const unprefixedTable = /(?<!jones_)peer_environment_links/;

it.effect("only upstream's own migrations and dev-copy script name upstream's links table", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const found: Array<string> = [];
    for (const root of ["src", "scripts"]) {
      for (const relative of yield* fs.readDirectory(`${serverRoot}${root}`, {
        recursive: true,
      })) {
        const file = `${root}/${relative.split("\\").join("/")}`;
        if (!file.endsWith(".ts") || /\.(test|testkit)\.ts$/.test(file)) continue;
        if (unprefixedTable.test(yield* fs.readFileString(`${serverRoot}${file}`)))
          found.push(file);
      }
    }
    assert.deepStrictEqual(found.toSorted(), [
      "scripts/migrate-dev-db.ts",
      "src/persistence/Migrations/070_PeerLinks.ts",
      "src/persistence/Migrations/071_PeerLinkFence.ts",
    ]);
  }).pipe(Effect.provide(NodeFileSystem.layer)),
);

it.effect("released schema 63 keeps its rows and Jones links through upstream catch-up", () =>
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

    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 63 }), []);
    yield* JonesMigrations.runMigrations();
    yield* assertJonesPeerSchema;
    const objectsAfter = yield* sql`
      SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name
    `;
    for (const object of objects) assert.deepInclude(objectsAfter, object);
    assert.deepStrictEqual(
      yield* Effect.forEach(tables, ({ name }) => sql`SELECT * FROM ${sql(name)}`),
      rowsBefore,
    );
    const legacy = linkRow("synthetic-released-env");
    yield* sql`INSERT INTO ${sql(peerEnvironmentLinksTable)} ${sql.insert(legacy)}`;

    assert.deepStrictEqual(
      yield* runMigrations(),
      migrationManifest.filter(([id]) => id > 63),
    );
    assert.deepStrictEqual(yield* sql`SELECT command_id FROM orchestration_command_receipts`, [
      { command_id: "synthetic-peer-command" },
    ]);
    yield* assertJonesPeerSchema;
    assert.deepStrictEqual(yield* jonesLinks, [legacy]);
    assert.deepStrictEqual(yield* upstreamLinks, []);

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
    yield* assertJonesPeerSchema;
    assert.deepStrictEqual(yield* jonesLinks, [legacy]);
  }).pipe(Effect.provide(memory)),
);

it.effect("links left in upstream's table by an unreleased build are never read", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const context = yield* Layer.build(
        layerLinkingEnvironment(descriptorOf("environment-laptop", "Laptop")),
      );
      const links = Context.get(context, PeerLinks.PeerLinks);
      const sql = Context.get(context, SqlClient.SqlClient);
      const residue = linkRow("environment-residue", { fenced: 0 });
      const residueId = EnvironmentId.make(residue.environment_id);
      yield* sql`INSERT INTO ${sql(upstreamTable)} ${sql.insert(residue)}`;
      const upstreamBefore = yield* upstreamLinks.pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );

      assert.deepStrictEqual(yield* links.list, []);
      assert.isTrue(Option.isNone(yield* links.get(residueId)));
      assert.strictEqual(
        (yield* links.resolve(residueId).pipe(Effect.flip)).reason,
        "unknown_link",
      );
      assert.isFalse(yield* links.unlink(residueId));
      assert.deepStrictEqual(
        yield* upstreamLinks.pipe(Effect.provideService(SqlClient.SqlClient, sql)),
        upstreamBefore,
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("every link lifecycle write lands in the Jones table and none in upstream's", () =>
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
        readonly last_error: string | null;
        readonly last_reached_at: string | null;
        readonly fenced: number;
      }>`SELECT environment_id, secret_name, last_error, last_reached_at, fenced FROM ${sql(
        peerEnvironmentLinksTable,
      )}`;
      // The fixture database runs every migration, so upstream's table exists and must stay empty.
      const upstreamRows = upstreamLinks.pipe(Effect.provideService(SqlClient.SqlClient, sql));
      assert.deepStrictEqual(yield* rows, []);

      const linked = yield* linkTo(links, peer);
      const stored = yield* rows;
      assert.strictEqual(stored.length, 1);
      assert.strictEqual(stored[0]?.environment_id, linked.environmentId);
      assert.strictEqual(stored[0]?.fenced, 1);
      assert.match(stored[0]!.secret_name, /^peer-link-[a-f0-9]+$/);
      assert.deepStrictEqual(yield* upstreamRows, []);

      const resolved = yield* links.resolve(linked.environmentId);
      assert.strictEqual(resolved.link.environmentId, linked.environmentId);
      assert.strictEqual((yield* peer.linkedSessions).length, 1);
      assert.deepStrictEqual(yield* upstreamRows, []);

      yield* links.recordOutcome(linked.environmentId, resolved.generation, { error: null });
      assert.isNull((yield* rows)[0]?.last_error);
      assert.isNotNull((yield* rows)[0]?.last_reached_at);
      yield* links.recordOutcome(linked.environmentId, resolved.generation, {
        error: "synthetic outage",
      });
      assert.strictEqual((yield* rows)[0]?.last_error, "synthetic outage");
      assert.deepStrictEqual(yield* upstreamRows, []);

      const relinked = yield* linkTo(links, peer);
      assert.strictEqual(relinked.environmentId, linked.environmentId);
      const restored = yield* rows;
      assert.strictEqual(restored.length, 1);
      assert.notStrictEqual(restored[0]?.secret_name, stored[0]?.secret_name);
      assert.deepStrictEqual(yield* upstreamRows, []);

      assert.isTrue(yield* links.unlink(linked.environmentId));
      assert.deepStrictEqual(yield* rows, []);
      assert.isTrue(Option.isNone(yield* links.get(linked.environmentId)));
      assert.strictEqual(
        (yield* links.resolve(linked.environmentId).pipe(Effect.flip)).reason,
        "unknown_link",
      );
      assert.isFalse(yield* links.unlink(linked.environmentId));
      assert.deepStrictEqual(yield* upstreamRows, []);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
