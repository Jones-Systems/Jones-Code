import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { expect, it } from "@effect/vitest";
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
