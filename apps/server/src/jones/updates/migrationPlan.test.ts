// @effect-diagnostics-next-line nodeBuiltinImport:off -- Verifies actual database and WAL bytes, timestamps, and fixture cleanup around synchronous node:sqlite.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Builds isolated native database fixture paths for the synchronous candidate CLI adapter.
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import { runJonesMigrations } from "../persistence/JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../persistence/JonesMigrations.ts";
import { readMigrationPlan } from "./migrationPlan.ts";

const manifest = {
  upstream: [
    [1, "Initial"],
    [2, "UpstreamNext"],
  ] as const,
  jones: [
    [1, "JonesInitial"],
    [105, "UpstreamReferenceIsolation"],
  ] as const,
};
function withDatabase(run: (databasePath: string, root: string) => void) {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-migration-plan-"));
  try {
    const databasePath = NodePath.join(root, "state.sqlite");
    const database = new NodeSqlite.DatabaseSync(databasePath);
    try {
      database.exec(
        "CREATE TABLE effect_sql_migrations (migration_id INTEGER, name TEXT); CREATE TABLE jones_sql_migrations (migration_id INTEGER, name TEXT); INSERT INTO effect_sql_migrations VALUES (1, 'Initial'); INSERT INTO jones_sql_migrations VALUES (1, 'JonesInitial')",
      );
    } finally {
      database.close();
    }
    run(databasePath, root);
  } finally {
    NodeFS.rmSync(root, { recursive: true, force: true });
  }
}

it("reads the candidate's pending IDs without changing database bytes or timestamps", () => {
  withDatabase((databasePath, root) => {
    const before = NodeFS.readFileSync(databasePath);
    const modified = NodeFS.statSync(databasePath).mtimeMs;
    expect(readMigrationPlan({ databasePath, ...manifest })).toEqual({
      status: "ready",
      migrationPlan: { pendingUpstream: [2], pendingJones: [105] },
    });
    expect(NodeFS.readFileSync(databasePath)).toEqual(before);
    expect(NodeFS.statSync(databasePath).mtimeMs).toBe(modified);
    expect(NodeFS.readdirSync(root)).toEqual(["state.sqlite"]);
  });
});

it("blocks a candidate older than the recorded Jones database", () => {
  withDatabase((databasePath) => {
    const database = new NodeSqlite.DatabaseSync(databasePath);
    try {
      database.exec("INSERT INTO jones_sql_migrations VALUES (106, 'Future')");
    } finally {
      database.close();
    }
    expect(readMigrationPlan({ databasePath, ...manifest })).toEqual({
      status: "blocked",
      reason: "Candidate older than this database (Jones migration 106).",
    });
  });
});

it("reads committed ledgers still in WAL without checkpointing the database", () => {
  withDatabase((databasePath, root) => {
    const writer = new NodeSqlite.DatabaseSync(databasePath);
    try {
      writer.exec(
        "PRAGMA journal_mode = WAL; INSERT INTO effect_sql_migrations VALUES (2, 'UpstreamNext')",
      );
      const before = NodeFS.readFileSync(databasePath);
      const wal = NodeFS.readFileSync(`${databasePath}-wal`);
      const modified = NodeFS.statSync(databasePath).mtimeMs;
      const files = NodeFS.readdirSync(root);
      expect(readMigrationPlan({ databasePath, ...manifest })).toEqual({
        status: "ready",
        migrationPlan: { pendingUpstream: [], pendingJones: [105] },
      });
      expect(NodeFS.readFileSync(databasePath)).toEqual(before);
      expect(NodeFS.readFileSync(`${databasePath}-wal`)).toEqual(wal);
      expect(NodeFS.statSync(databasePath).mtimeMs).toBe(modified);
      expect(NodeFS.readdirSync(root)).toEqual(files);
    } finally {
      writer.close();
    }
  });
});

it("blocks unknown Jones history without writing a ledger", () => {
  withDatabase((databasePath) => {
    const database = new NodeSqlite.DatabaseSync(databasePath);
    try {
      database.exec("UPDATE jones_sql_migrations SET name = 'Unknown'");
    } finally {
      database.close();
    }
    expect(readMigrationPlan({ databasePath, ...manifest }).status).toBe("blocked");
  });
});

it("allows first startup without creating the missing database", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-migration-plan-"));
  try {
    expect(
      readMigrationPlan({ databasePath: NodePath.join(root, "missing.sqlite"), ...manifest }),
    ).toEqual({
      status: "ready",
      migrationPlan: { pendingUpstream: [1, 2], pendingJones: [1, 105] },
    });
    expect(NodeFS.readdirSync(root)).toEqual([]);
  } finally {
    NodeFS.rmSync(root, { recursive: true, force: true });
  }
});

it.effect(
  "preflights a tiny real installation, upgrades it, and preserves rows on migration failure",
  () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-install-migrations-"));
      const databasePath = NodePath.join(root, "statev2.sqlite");
      const current = {
        upstream: migrationManifest,
        jones: jonesMigrationEntries.map(([id, name]) => [id, name] as const),
      };
      const latestJones = jonesMigrationEntries.at(-1)!;
      const run = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
        effect.pipe(
          Effect.provide(
            NodeSqliteClient.layer({
              filename: databasePath,
              readerWorker: false,
              checkpointWorker: false,
            }),
          ),
          Effect.scoped,
        );
      try {
        expect(readMigrationPlan({ databasePath, ...current })).toEqual({
          status: "ready",
          migrationPlan: {
            pendingUpstream: current.upstream.map(([id]) => id),
            pendingJones: current.jones.map(([id]) => id),
          },
        });
        yield* run(
          Effect.gen(function* () {
            yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
            yield* runJonesMigrations(jonesMigrationEntries.slice(0, -1));
            const sql = yield* SqlClient.SqlClient;
            yield* sql`CREATE TABLE jones_install_fixture (value TEXT NOT NULL)`;
            yield* sql`INSERT INTO jones_install_fixture VALUES ('keep-one'), ('keep-two')`;
          }),
        );
        expect(readMigrationPlan({ databasePath, ...current })).toEqual({
          status: "ready",
          migrationPlan: { pendingUpstream: [], pendingJones: [latestJones[0]] },
        });
        yield* run(runMigrations());
        expect(readMigrationPlan({ databasePath, ...current })).toEqual({
          status: "ready",
          migrationPlan: { pendingUpstream: [], pendingJones: [] },
        });
        expect(
          readMigrationPlan({ databasePath, ...current, jones: current.jones.slice(0, -1) }),
        ).toEqual({
          status: "blocked",
          reason: `Candidate older than this database (Jones migration ${latestJones[0]}).`,
        });
        const failingId = latestJones[0] + 1;
        const failed = yield* run(
          Effect.exit(
            runJonesMigrations([
              ...jonesMigrationEntries,
              [
                failingId,
                "SyntheticInstallFailure",
                Effect.gen(function* () {
                  const sql = yield* SqlClient.SqlClient;
                  yield* sql`CREATE TABLE jones_failed_install_fixture (value TEXT)`;
                  yield* sql`DELETE FROM jones_install_fixture`;
                  yield* sql`SELECT * FROM missing_install_fixture`;
                }),
              ],
            ]),
          ),
        );
        expect(Exit.isFailure(failed)).toBe(true);
        const database = new NodeSqlite.DatabaseSync(databasePath, { readOnly: true });
        try {
          expect(
            database.prepare("SELECT value FROM jones_install_fixture ORDER BY value").all(),
          ).toEqual([{ value: "keep-one" }, { value: "keep-two" }]);
          expect(
            database
              .prepare("SELECT name FROM sqlite_master WHERE name = 'jones_failed_install_fixture'")
              .all(),
          ).toEqual([]);
          expect(
            database
              .prepare("SELECT migration_id FROM jones_sql_migrations WHERE migration_id = ?")
              .all(failingId),
          ).toEqual([]);
        } finally {
          database.close();
        }
        expect(readMigrationPlan({ databasePath, ...current }).status).toBe("ready");
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }),
);
