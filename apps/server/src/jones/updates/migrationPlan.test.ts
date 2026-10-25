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
  "upgrades a retained upstream-63 installation through 64–67 and preserves rows on failure",
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
            yield* runMigrations({ toMigrationInclusive: 63 });
            yield* runJonesMigrations(jonesMigrationEntries.slice(0, -1));
            const sql = yield* SqlClient.SqlClient;
            const retainedAt = "2026-09-01T00:00:00.000Z";
            const retainedPayload = JSON.stringify({
              id: "retained-thread",
              title: "Retained upstream thread",
              updatedAt: retainedAt,
              linkedPullRequest: {
                projectId: "retained-project",
                repository: "Acme/Web",
                number: 42,
                url: "https://github.com/acme/web/pull/42",
              },
            });
            yield* sql`INSERT INTO orchestration_v2_projection_threads (
              thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
              created_at, updated_at, payload_json
            ) VALUES ('retained-thread', 'retained-project', 'Retained upstream thread',
              'codex', 'full-access', 'default', ${retainedAt}, ${retainedAt}, ${retainedPayload})`;
            yield* sql`INSERT INTO orchestration_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
              actor_kind, payload_json, metadata_json, application_event_version
            ) VALUES ('retained-event', 'thread', 'retained-thread', 0, 'thread.created',
              ${retainedAt}, 'user', ${retainedPayload}, '{}', 2)`;
            yield* sql`INSERT INTO jones_planned_update_continuity
              (operation_id, binding_json, activated) VALUES ('retained-update', '{}', 0)`;
            yield* sql`INSERT INTO jones_planned_update_threads
              (operation_id, thread_id, snapshot_json, queue_done, work_done)
              VALUES ('retained-update', 'retained-thread', '{}', 0, 0)`;
            yield* sql`CREATE TABLE jones_install_fixture (value TEXT NOT NULL)`;
            yield* sql`INSERT INTO jones_install_fixture VALUES ('keep-one'), ('keep-two')`;
          }),
        );
        expect(readMigrationPlan({ databasePath, ...current })).toEqual({
          status: "ready",
          migrationPlan: { pendingUpstream: [64, 65, 66, 67], pendingJones: [latestJones[0]] },
        });
        const beforePreflight = NodeFS.readFileSync(databasePath);
        const preflightMtime = NodeFS.statSync(databasePath).mtimeMs;
        const preflightFiles = NodeFS.readdirSync(root);
        expect(readMigrationPlan({ databasePath, ...current })).toEqual({
          status: "ready",
          migrationPlan: { pendingUpstream: [64, 65, 66, 67], pendingJones: [latestJones[0]] },
        });
        expect(NodeFS.readFileSync(databasePath)).toEqual(beforePreflight);
        expect(NodeFS.statSync(databasePath).mtimeMs).toBe(preflightMtime);
        expect(NodeFS.readdirSync(root)).toEqual(preflightFiles);
        expect(yield* run(runMigrations({ toMigrationInclusive: 67 }))).toEqual(
          migrationManifest.filter(([id]) => id >= 64 && id <= 67),
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
        yield* run(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            const projection = yield* sql<{ readonly payload_json: string }>`
            SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = 'retained-thread'`;
            const events = yield* sql<{ readonly payload_json: string }>`
            SELECT payload_json FROM orchestration_events WHERE event_id = 'retained-event'`;
            expect(projection).toHaveLength(1);
            expect(events).toHaveLength(1);
            expect(JSON.parse(projection[0]!.payload_json)).toMatchObject({
              id: "retained-thread",
              title: "Retained upstream thread",
              linkedPullRequest: { number: 42 },
              pullRequests: [
                { host: "github.com", repository: "acme/web", number: 42, source: "manual" },
              ],
            });
            expect(JSON.parse(events[0]!.payload_json)).toEqual(
              JSON.parse(projection[0]!.payload_json),
            );
            expect(yield* sql`SELECT * FROM jones_planned_update_continuity`).toEqual([
              { operation_id: "retained-update", binding_json: "{}", activated: 0 },
            ]);
            expect(yield* sql`SELECT * FROM jones_planned_update_threads`).toEqual([
              {
                operation_id: "retained-update",
                thread_id: "retained-thread",
                snapshot_json: "{}",
                queue_done: 0,
                work_done: 0,
              },
            ]);
            expect(
              (yield* sql<{ readonly migration_id: number; readonly name: string }>`
            SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`).map(
                ({ migration_id, name }) => [migration_id, name],
              ),
            ).toEqual(current.upstream);
            expect(
              (yield* sql<{ readonly migration_id: number; readonly name: string }>`
            SELECT migration_id, name FROM jones_sql_migrations ORDER BY migration_id`).map(
                ({ migration_id, name }) => [migration_id, name],
              ),
            ).toEqual(current.jones);
            expect(
              yield* sql`SELECT name FROM sqlite_master WHERE name = 'orchestration_v2_provider_turns_codex_native_id_idx'`,
            ).toHaveLength(1);
            expect(
              yield* sql`SELECT name FROM sqlite_master WHERE name IN ('scheduled_task_webhook_replay_keys', 'scheduled_task_webhook_pending')`,
            ).toHaveLength(2);
            expect(
              (yield* sql<{ readonly name: string }>`PRAGMA table_info(scheduled_tasks)`).some(
                ({ name }) => name === "last_run_webhook_delivery_id",
              ),
            ).toBe(true);
          }),
        );
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
