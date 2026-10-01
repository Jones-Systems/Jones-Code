import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import { describe, expect, vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { migrationManifest } from "../../apps/server/src/persistence/Migrations.ts";
import { makePopulatedFixture, readFixture, withDatabase } from "./fixture.mjs";
import { withRunScratch } from "./support.mjs";

vi.mock("effect/unstable/sql/Migrator", async (importOriginal) => ({
  ...(await importOriginal()),
}));

function assertHistoricalRowsPreserved(before, after) {
  for (const [table, rows] of Object.entries(before.tables)) {
    assert.equal(after.tables[table].length, rows.length);
    for (const [index, row] of rows.entries()) {
      const originalColumns = Object.fromEntries(
        Object.keys(row).map((key) => [key, after.tables[table][index][key]]),
      );
      assert.deepEqual(originalColumns, row);
    }
  }
  assert.deepEqual(after.files, before.files);
  assert.deepEqual(after.references, before.references);
  assert.isTrue(after.references.valid);
}

async function missing(filename) {
  try {
    await NodeFSP.stat(filename);
    return false;
  } catch (error) {
    if (error.code === "ENOENT") return true;
    throw error;
  }
}

describe("populated historical database adoption", () => {
  for (const throughMigration of [53, 54]) {
    it(`adopts upstream ${throughMigration} through real startup, preserves rows/files and is idempotent`, async () => {
      await withRunScratch({ label: `adoption-${throughMigration}` }, async ({ root, record }) => {
        const fixture = await makePopulatedFixture({ baseDir: root, throughMigration });
        const before = await readFixture(fixture);
        assert.isTrue(before.references.valid);
        assert.deepEqual(before.ledgers.fork, []);
        assert.equal(before.tables.projection_threads.length, 2);
        assert.equal(before.tables.projection_thread_messages.length, 2);
        assert.equal(before.tables.provider_session_runtime.length, 2);
        await withDatabase(fixture, { startup: true }, Effect.void);
        const after = await readFixture(fixture);
        assertHistoricalRowsPreserved(before, after);
        assert.deepEqual(
          after.ledgers.upstream,
          migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
        );
        assert.deepEqual(after.ledgers.fork, [
          { migration_id: 1, name: "WorktreeOwnershipLeases" },
          { migration_id: 2, name: "ProjectionThreadRuntimeIdentity" },
        ]);
        for (const thread of after.tables.projection_threads)
          assert.isNull(thread.auto_settle_disabled_at);
        for (const session of after.tables.projection_thread_sessions)
          assert.isNull(session.runtime_identity_json);
        await withDatabase(fixture, { startup: true }, Effect.void);
        assert.deepEqual(await readFixture(fixture), after);
        record({
          checkId: `startup-upstream-${throughMigration}`,
          proofKind: "synthetic-populated-production-migrations",
          result: "passed",
          upstream: after.ledgers.upstream.at(-1).migration_id,
          fork: after.ledgers.fork,
          nativeProviderResume: "unproved",
          installedPackageAdoption: "unproved",
        });
      });
    });
  }

  it("aborts migration 54 before effects, keeps the record/fork absent, then recovers", async () => {
    await withRunScratch({ label: "migration-transaction" }, async ({ root, record }) => {
      const fixture = await makePopulatedFixture({ baseDir: root, throughMigration: 53 });
      const before = await readFixture(fixture);
      await withDatabase(
        fixture,
        {},
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TRIGGER qualification_abort_54 BEFORE INSERT ON effect_sql_migrations
          WHEN NEW.migration_id = 54 BEGIN SELECT RAISE(ABORT, 'qualification abort migration 54'); END`;
        }),
      );
      await expect(withDatabase(fixture, { startup: true }, Effect.void)).rejects.toThrow();
      const failed = await readFixture(fixture);
      assert.deepEqual(failed, before);
      await withDatabase(
        fixture,
        {},
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const columns = yield* sql`PRAGMA table_info(projection_threads)`;
          assert.isFalse(columns.some((column) => column.name === "auto_settle_disabled_at"));
          const fork =
            yield* sql`SELECT name FROM sqlite_master WHERE name IN ('jones_sql_migrations', 'worktree_ownership_leases')`;
          assert.deepEqual(fork, []);
          yield* sql`DROP TRIGGER qualification_abort_54`;
        }),
      );
      await withDatabase(fixture, { startup: true }, Effect.void);
      const recovered = await readFixture(fixture);
      assertHistoricalRowsPreserved(before, recovered);
      assert.equal(recovered.ledgers.upstream.at(-1).migration_id, 54);
      assert.equal(recovered.ledgers.fork.length, 2);
      record({
        checkId: "migration-54-pre-effect-recovery",
        proofKind: "synthetic-trigger-real-startup",
        result: "passed",
        limit:
          "Migrator inserts ledger records before executing effects; this trigger aborts before ALTER",
      });
    });
  });

  it("rolls back actual migration 54 and later writes after a post-write fault, leaving the fork untouched", async () => {
    await withRunScratch({ label: "post-write-transaction" }, async ({ root, record }) => {
      const fixture = await makePopulatedFixture({ baseDir: root, throughMigration: 53 });
      const before = await readFixture(fixture);
      const originalFromRecord = Migrator.fromRecord;
      let reachedPostWriteFault = false;
      const abortAfterWrite = Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`SELECT auto_settle_disabled_at FROM projection_threads`;
        yield* sql`CREATE TABLE qualification_post_write_probe (proof TEXT NOT NULL)`;
        yield* sql`INSERT INTO qualification_post_write_probe VALUES ('written-before-failure')`;
        reachedPostWriteFault = true;
        return yield* Effect.fail(new Error("qualification post-write migration failure"));
      });
      const injected = vi.spyOn(Migrator, "fromRecord").mockImplementation((entries) =>
        originalFromRecord(
          Object.hasOwn(entries, "1_WorktreeOwnershipLeases")
            ? entries
            : {
                ...entries,
                "55_QualificationPostWriteAbort": abortAfterWrite,
              },
        ),
      );
      try {
        await expect(withDatabase(fixture, { startup: true }, Effect.void)).rejects.toThrow();
        assert.isTrue(reachedPostWriteFault);
        assert.deepEqual(await readFixture(fixture), before);
        await withDatabase(
          fixture,
          {},
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            const columns = yield* sql`PRAGMA table_info(projection_threads)`;
            assert.isFalse(columns.some((column) => column.name === "auto_settle_disabled_at"));
            assert.deepEqual(
              yield* sql`SELECT name FROM sqlite_master WHERE name IN (
            'qualification_post_write_probe', 'jones_sql_migrations', 'worktree_ownership_leases')`,
              [],
            );
          }),
        );
      } finally {
        injected.mockRestore();
      }
      await withDatabase(fixture, { startup: true }, Effect.void);
      const recovered = await readFixture(fixture);
      assertHistoricalRowsPreserved(before, recovered);
      assert.equal(recovered.ledgers.upstream.at(-1).migration_id, 54);
      assert.equal(recovered.ledgers.fork.length, 2);
      record({
        checkId: "migration-post-write-transaction-recovery",
        proofKind: "real-migrations-with-appended-fault",
        result: "passed",
        limit:
          "Synthetic migration 55 injects the fault; production migration 54 and runner remain unchanged",
      });
    });
  });

  it("reports a missing attachment and broken project reference", async () => {
    await withRunScratch({ label: "broken-references" }, async ({ root }) => {
      const fixture = await makePopulatedFixture({ baseDir: root });
      const identity = fixture.expected.identities[0];
      await NodeFSP.rm(NodePath.join(fixture.paths.attachmentsDir, `${identity.attachmentId}.png`));
      await withDatabase(
        fixture,
        {},
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`UPDATE projection_threads SET project_id = 'missing-synthetic-project' WHERE thread_id = ${identity.threadId}`;
          yield* sql`UPDATE projection_projects SET workspace_root = ${NodePath.join(root, "missing-project-directory")} WHERE project_id = ${identity.projectId}`;
        }),
      );
      const result = await readFixture(fixture);
      assert.isFalse(result.references.valid);
      assert.isTrue(
        result.references.errors.some((error) => error.startsWith("Missing attachment:")),
      );
      assert.isTrue(
        result.references.errors.some((error) => error.startsWith("Broken thread project:")),
      );
      assert.isTrue(
        result.references.errors.some((error) => error.startsWith("Broken project path:")),
      );
    });
  });
});

describe("qualification scratch ownership", () => {
  it("awaits callback resource closure before cleanup during cooperative cancellation", async () => {
    const controller = new AbortController();
    let activeRoot;
    let resourcesClosed = false;
    let announceStarted;
    const started = new Promise((resolve) => {
      announceStarted = resolve;
    });
    const run = withRunScratch({ label: "cooperative-cancellation" }, async ({ root }) => {
      activeRoot = root;
      const aborted = new Promise((resolve) =>
        controller.signal.addEventListener("abort", resolve, { once: true }),
      );
      announceStarted();
      try {
        await aborted;
        throw new Error("cooperative cancellation");
      } finally {
        assert.isFalse(await missing(root));
        await NodeFSP.writeFile(
          NodePath.join(root, "resource-closed.txt"),
          "closed before root cleanup",
        );
        resourcesClosed = true;
      }
    });
    await started;
    controller.abort();
    await expect(run).rejects.toThrow("cooperative cancellation");
    assert.isTrue(resourcesClosed);
    assert.isTrue(await missing(activeRoot));
  });

  it("cleans exact roots on success and failure without removing concurrent siblings", async () => {
    let successRoot;
    let failureRoot;
    let release;
    let rootReady;
    const started = new Promise((resolve) => {
      rootReady = resolve;
    });
    const waiting = new Promise((resolve) => {
      release = resolve;
    });
    const success = withRunScratch({ label: "concurrent-success" }, async ({ root }) => {
      successRoot = root;
      await NodeFSP.writeFile(NodePath.join(root, "owned.txt"), "success");
      rootReady();
      await waiting;
      return "returned-value";
    });
    const failure = withRunScratch({ label: "concurrent-failure" }, async ({ root }) => {
      failureRoot = root;
      await NodeFSP.writeFile(NodePath.join(root, "owned.txt"), "failure");
      throw new Error("expected fixture failure");
    });
    try {
      await expect(failure).rejects.toThrow("expected fixture failure");
      assert.isTrue(await missing(failureRoot));
      await started;
      assert.isFalse(await missing(successRoot));
    } finally {
      release();
    }
    assert.equal(await success, "returned-value");
    assert.isTrue(await missing(successRoot));
  });
});
