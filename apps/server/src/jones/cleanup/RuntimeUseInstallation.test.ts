import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import { runJonesMigrations } from "../persistence/JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../persistence/JonesMigrations.ts";
import { canonical, digest } from "./RuntimeUseProtocol.ts";
import { createRuntimeUseSchema } from "./RuntimeUseSchema.ts";
import {
  createRuntimeUseInstallation,
  readRuntimeUseInstallation,
  runtimeUseCoverageSource,
} from "./RuntimeUseInstallation.ts";

const prior = jonesMigrationEntries.filter(([id]) => id < 142);
const fixture = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
  Effect.gen(function* () {
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    yield* runJonesMigrations(prior);
    return yield* effect;
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));
const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });
const appendSyntheticEvent = Effect.fn("appendSyntheticInstallationEvent")(function* (
  sequence: number,
  applicationVersion: number,
  kind: string,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_events
    (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,
      command_id,actor_kind,payload_json,metadata_json,application_event_version)
    VALUES (${sequence},${`synthetic:${sequence}`},${kind},${`stream:${sequence}`},1,
      'synthetic.installation-boundary','2026-11-04T00:00:00.000Z',
      ${`command:${sequence}`},'system','{}','{}',${applicationVersion})`;
});

it.effect(
  "the original installation and global event boundary serialize with a real second SQLite writer",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "runtime-installation-order-" });
        const filename = root + "/statev2.sqlite";
        const a = Context.get(
          yield* Layer.build(NodeSqliteClient.layer({ filename })),
          SqlClient.SqlClient,
        );
        const b = Context.get(
          yield* Layer.build(NodeSqliteClient.layer({ filename })),
          SqlClient.SqlClient,
        );
        assert.notStrictEqual(a, b);
        yield* Effect.gen(function* () {
          yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
          yield* runJonesMigrations(prior);
          yield* appendSyntheticEvent(41, 2, "thread");
        }).pipe(Effect.provideService(SqlClient.SqlClient, a));
        yield* b`PRAGMA busy_timeout=0`;
        const captured = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const installing = yield* a
          .withTransaction(
            Effect.gen(function* () {
              yield* runJonesMigrations(jonesMigrationEntries);
              assert.deepStrictEqual((yield* readRuntimeUseInstallation())?.globalEventWatermark, {
                kind: "sequence",
                sequence: 41,
              });
              yield* Deferred.succeed(captured, undefined);
              yield* Deferred.await(release);
            }),
          )
          .pipe(Effect.provideService(SqlClient.SqlClient, a), Effect.forkChild);
        const attempted = yield* Effect.exit(
          Effect.gen(function* () {
            yield* Deferred.await(captured);
            // The same valid write succeeds after commit below. It cannot cross the
            // original installation transaction and change that installation's basis.
            yield* refused(
              appendSyntheticEvent(900, 1, "project").pipe(
                Effect.provideService(SqlClient.SqlClient, b),
              ),
            );
            assert.deepStrictEqual(
              yield* b`SELECT sequence FROM orchestration_events ORDER BY sequence`,
              [{ sequence: 41 }],
            );
            assert.deepStrictEqual(
              yield* b`SELECT name FROM sqlite_master WHERE name='jones_workspace_runtime_installation'`,
              [],
            );
          }),
        ).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(installing);
        if (Exit.isFailure(attempted)) return yield* Effect.failCause(attempted.cause);
        yield* appendSyntheticEvent(900, 1, "project").pipe(
          Effect.provideService(SqlClient.SqlClient, b),
        );
        assert.deepStrictEqual(
          yield* b`SELECT sequence FROM orchestration_events ORDER BY sequence`,
          [{ sequence: 41 }, { sequence: 900 }],
        );
        assert.deepStrictEqual(
          (yield* readRuntimeUseInstallation().pipe(Effect.provideService(SqlClient.SqlClient, b)))
            ?.globalEventWatermark,
          { kind: "sequence", sequence: 41 },
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "a fresh database captures its basis before the migrator creates its first Jones ledger",
  () =>
    Effect.gen(function* () {
      yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
      yield* runJonesMigrations(jonesMigrationEntries);
      assert.deepStrictEqual((yield* readRuntimeUseInstallation())?.globalEventWatermark, {
        kind: "empty",
      });
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "installation distinguishes empty global history and binds the exact schema/source contracts",
  () =>
    fixture(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.isNull(yield* readRuntimeUseInstallation());
        yield* runJonesMigrations(jonesMigrationEntries);
        const basis = yield* readRuntimeUseInstallation();
        assert.isNotNull(basis);
        assert.deepStrictEqual(basis?.globalEventWatermark, { kind: "empty" });
        assert.deepStrictEqual(basis?.migration, { id: 142, name: "WorkspaceRuntimeUses" });
        assert.strictEqual(basis?.coverageSourceSha256, digest(runtimeUseCoverageSource));
        const rows =
          yield* sql`SELECT id,basis_json,basis_sha256 FROM jones_workspace_runtime_installation`;
        assert.deepStrictEqual(rows, [
          { id: 1, basis_json: canonical(basis), basis_sha256: digest(basis) },
        ]);
        assert.deepStrictEqual(
          yield* sql`PRAGMA foreign_key_list(jones_workspace_runtime_installation)`,
          [],
        );
      }),
    ),
);

it.effect(
  "installation records the maximum GLOBAL sequence across streams, aggregate kinds and event versions",
  () =>
    fixture(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* appendSyntheticEvent(41, 2, "thread");
        yield* appendSyntheticEvent(900, 1, "project");
        yield* runJonesMigrations(jonesMigrationEntries);
        const before = yield* sql`SELECT * FROM jones_workspace_runtime_installation`;
        assert.deepStrictEqual((yield* readRuntimeUseInstallation())?.globalEventWatermark, {
          kind: "sequence",
          sequence: 900,
        });
        yield* appendSyntheticEvent(1200, 2, "thread");
        assert.deepStrictEqual(yield* runJonesMigrations(jonesMigrationEntries), []);
        assert.deepStrictEqual((yield* readRuntimeUseInstallation())?.globalEventWatermark, {
          kind: "sequence",
          sequence: 900,
        });
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM jones_workspace_runtime_installation`,
          before,
        );
      }),
    ),
);

it.effect(
  "installation cannot be updated, deleted, replaced or recaptured using a newer maximum",
  () =>
    fixture(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runJonesMigrations(jonesMigrationEntries);
        const before = yield* sql`SELECT * FROM jones_workspace_runtime_installation`;
        yield* appendSyntheticEvent(12, 2, "thread");
        yield* refused(sql`UPDATE jones_workspace_runtime_installation SET basis_json='{}'`);
        yield* refused(sql`DELETE FROM jones_workspace_runtime_installation`);
        yield* refused(sql`INSERT OR REPLACE INTO jones_workspace_runtime_installation
      SELECT * FROM jones_workspace_runtime_installation`);
        yield* refused(sql.withTransaction(createRuntimeUseInstallation));
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM jones_workspace_runtime_installation`,
          before,
        );
      }),
    ),
);

it.effect(
  "an already-applied142 without its atomic basis stays ineligible without schema or history repair",
  () =>
    fixture(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runJonesMigrations([
          ...prior,
          [142, "WorkspaceRuntimeUses", createRuntimeUseSchema],
        ]);
        const before = yield* sql`SELECT type,name,sql FROM sqlite_master ORDER BY name`;
        yield* appendSyntheticEvent(25, 2, "thread");
        assert.isNull(yield* readRuntimeUseInstallation());
        assert.deepStrictEqual(yield* runJonesMigrations(jonesMigrationEntries), []);
        yield* refused(sql.withTransaction(createRuntimeUseInstallation));
        assert.deepStrictEqual(
          yield* sql`SELECT type,name,sql FROM sqlite_master ORDER BY name`,
          before,
        );
        assert.isNull(yield* readRuntimeUseInstallation());
      }),
    ),
);

it.effect(
  "changed journal definitions invalidate the retained basis rather than refreshing it",
  () =>
    fixture(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runJonesMigrations(jonesMigrationEntries);
        const before = yield* sql`SELECT * FROM jones_workspace_runtime_installation`;
        yield* sql`DROP TRIGGER jones_workspace_runtime_producer_identity`;
        yield* refused(readRuntimeUseInstallation());
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM jones_workspace_runtime_installation`,
          before,
        );
      }),
    ),
);

it.effect("unexpected journal-prefix objects abort the entire original migration", () =>
  fixture(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE jones_workspace_runtime_unrecognized (value TEXT)`;
      const before = yield* sql`SELECT type,name,sql FROM sqlite_master ORDER BY name`;
      yield* refused(runJonesMigrations(jonesMigrationEntries));
      assert.deepStrictEqual(
        yield* sql`SELECT type,name,sql FROM sqlite_master ORDER BY name`,
        before,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id=142`,
        [],
      );
    }),
  ),
);
