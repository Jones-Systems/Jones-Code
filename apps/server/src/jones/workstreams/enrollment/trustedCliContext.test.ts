// @effect-diagnostics nodeBuiltinImport:off -- All persisted qualification fixtures are synthetic and cleanup-owned.
import * as NodeFS from "node:fs";
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_RESTART_PENDING_FILE,
} from "../../../cloud/serviceProtocol.ts";
import { snapshotTree, syntheticT3Home, syntheticVersion } from "./testHome.ts";
import {
  makeTrustedNativeEnrollmentCliContext,
  makeTrustedNativeEnrollmentDesktopContext,
} from "./trustedCliContext.ts";
import type { DesktopNativeEnrollmentOwner } from "./desktopOwner.ts";

vi.mock("../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
const version = syntheticVersion;
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJsonRecord = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const fixture = syntheticT3Home("workstreams-cli-context-synthetic-").pipe(
  Effect.tap((home) =>
    Effect.sync(() => {
      const db = new NodeSqlite.DatabaseSync(home.dbPath);
      try {
        db.exec("CREATE TABLE qualification_fixture (id TEXT)");
      } finally {
        db.close();
      }
      NodeFS.chmodSync(home.dbPath, 0o600);
    }),
  ),
);
const snapshot = snapshotTree;

it.effect(
  "persisted CLI preflight qualifies exact installed source without creating or changing any file",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const before = snapshot(f.baseDir);
      const trusted = yield* makeTrustedNativeEnrollmentCliContext(f);
      assert.deepEqual(yield* trusted.authority.readCurrent, {
        environmentId: "synthetic-environment",
        authorityNamespace: "t3-native:12345678-1234-4234-8234-123456789abc",
        storeGeneration: 7,
      });
      yield* trusted.verifyUnchanged;
      assert.deepEqual(snapshot(f.baseDir), before);
      assert.strictEqual(NodeFS.existsSync(NodePath.join(f.baseDir, "userdata", "secrets")), false);
    }).pipe(Effect.scoped),
);

it.effect(
  "persisted CLI preflight rejects changed environment, authority and launcher metadata",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const trusted = yield* makeTrustedNativeEnrollmentCliContext(f);
      const authority = NodeFS.readFileSync(f.authorityPath, "utf8");
      NodeFS.writeFileSync(
        f.authorityPath,
        encodeJson({ ...decodeJsonRecord(authority), store_generation: 8 }),
      );
      assert.strictEqual((yield* Effect.flip(trusted.verifyUnchanged)).code, "context_changed");
      NodeFS.writeFileSync(f.authorityPath, authority);
      NodeFS.writeFileSync(f.environmentPath, "changed-environment\n");
      assert.strictEqual((yield* trusted.verifyUnchanged.pipe(Effect.result))._tag, "Failure");
      NodeFS.writeFileSync(f.environmentPath, "synthetic-environment\n");
      NodeFS.writeFileSync(
        NodePath.join(f.baseDir, "runtime", SERVICE_RESTART_PENDING_FILE),
        version,
        {
          mode: 0o600,
        },
      );
      assert.strictEqual(
        (yield* Effect.flip(makeTrustedNativeEnrollmentCliContext(f))).code,
        "launcher_unavailable",
      );
    }).pipe(Effect.scoped),
);

it.effect(
  "persisted CLI preflight rejects missing authority, pending update and mismatched qualified build",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const receipt = NodeFS.readFileSync(f.receiptPath, "utf8");
      NodeFS.writeFileSync(
        f.receiptPath,
        encodeJson({ ...decodeJsonRecord(receipt), sourceTree: "f".repeat(40) }),
      );
      assert.strictEqual(
        (yield* Effect.flip(makeTrustedNativeEnrollmentCliContext(f))).code,
        "source_mismatch",
      );
      NodeFS.writeFileSync(f.receiptPath, receipt);
      NodeFS.writeFileSync(
        f.statePath,
        encodeJson({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: version,
          update: {
            id: "synthetic-update",
            fromVersion: version,
            targetVersion: "0.0.0-preview.20261002.101",
            status: "pending",
            phase: "accepted",
            dbPath: f.dbPath,
          },
        }),
      );
      assert.strictEqual(
        (yield* Effect.flip(makeTrustedNativeEnrollmentCliContext(f))).code,
        "launcher_unavailable",
      );
      NodeFS.writeFileSync(
        f.statePath,
        encodeJson({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: version }),
      );
      NodeFS.rmSync(f.authorityDir, { recursive: true });
      assert.strictEqual(
        (yield* makeTrustedNativeEnrollmentCliContext(f).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(NodeFS.existsSync(f.authorityDir), false);
    }).pipe(Effect.scoped),
);

it.effect(
  "standalone WAL qualification fails closed without opening SQLite or modifying its files",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const db = new NodeSqlite.DatabaseSync(f.dbPath);
      try {
        db.exec("PRAGMA journal_mode=WAL");
      } finally {
        db.close();
      }
      const before = snapshot(f.baseDir);
      assert.strictEqual(
        (yield* Effect.flip(makeTrustedNativeEnrollmentCliContext(f))).code,
        "database_wal_unqualified",
      );
      assert.deepEqual(snapshot(f.baseDir), before);
    }).pipe(Effect.scoped),
);

it.effect(
  "operational inspection and apply qualify WAL metadata without opening a database or reading signing material",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const db = new NodeSqlite.DatabaseSync(f.dbPath);
      try {
        db.exec("PRAGMA journal_mode=WAL");
      } finally {
        db.close();
      }
      const before = snapshot(f.baseDir);
      const inspection = yield* makeTrustedNativeEnrollmentCliContext(f, "operational_inspection");
      assert.strictEqual(
        new URL(inspection.sqlite.filename).search,
        "?mode=ro&cache=private&vfs=unix",
      );
      assert.strictEqual(inspection.sqlite.readonly, true);
      assert.strictEqual(inspection.sqlite.allowExtension, false);
      yield* inspection.verifyUnchanged;
      const apply = yield* makeTrustedNativeEnrollmentCliContext(f, "apply");
      assert.strictEqual(new URL(apply.sqlite.filename).search, "?mode=rw&cache=private&vfs=unix");
      assert.strictEqual(apply.sqlite.readonly, false);
      assert.strictEqual(apply.sqlite.allowExtension, false);
      assert.deepEqual(snapshot(f.baseDir), before);
      assert.strictEqual(NodeFS.existsSync(NodePath.join(f.baseDir, "userdata", "secrets")), false);
    }).pipe(Effect.scoped),
);

it.effect("apply authority accepts only its bound connection's active rollback transaction", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const trusted = yield* makeTrustedNativeEnrollmentCliContext(f, "apply");
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const authority = yield* trusted.bindAuthorityToSqlClient(sql);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO qualification_fixture (id) VALUES ('owned-transaction')`;
          assert.strictEqual(NodeFS.existsSync(`${f.dbPath}-journal`), true);
          assert.deepEqual(yield* authority.readCurrent, {
            environmentId: "synthetic-environment",
            authorityNamespace: "t3-native:12345678-1234-4234-8234-123456789abc",
            storeGeneration: 7,
          });
          assert.strictEqual(
            (yield* trusted.authority.readCurrent.pipe(Effect.result))._tag,
            "Failure",
          );
          assert.strictEqual(
            (yield* Effect.flip(trusted.verifyUnchanged)).code,
            "database_unavailable",
          );
        }),
      );
      yield* trusted.verifyUnchanged;
      assert.strictEqual(NodeFS.existsSync(`${f.dbPath}-journal`), false);
      NodeFS.writeFileSync(`${f.dbPath}-journal`, "synthetic unrelated journal", {
        flag: "wx",
        mode: 0o600,
      });
      assert.strictEqual((yield* authority.readCurrent.pipe(Effect.result))._tag, "Failure");
      assert.strictEqual(
        (yield* Effect.flip(trusted.verifyUnchanged)).code,
        "database_unavailable",
      );
      assert.strictEqual(
        NodeFS.readFileSync(`${f.dbPath}-journal`, "utf8"),
        "synthetic unrelated journal",
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
  }).pipe(Effect.scoped),
);

it.effect("a foreign client's rollback transaction cannot qualify the bound apply authority", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const trusted = yield* makeTrustedNativeEnrollmentCliContext(f, "apply");
    yield* Effect.gen(function* () {
      const ownedSql = yield* SqlClient.SqlClient;
      const authority = yield* trusted.bindAuthorityToSqlClient(ownedSql);
      const refused = yield* Effect.gen(function* () {
        const foreignSql = yield* SqlClient.SqlClient;
        assert.notStrictEqual(foreignSql, ownedSql);
        assert.notStrictEqual(foreignSql.transactionService, ownedSql.transactionService);
        return yield* foreignSql
          .withTransaction(
            Effect.gen(function* () {
              yield* foreignSql`INSERT INTO qualification_fixture (id) VALUES ('foreign-transaction')`;
              assert.strictEqual(NodeFS.existsSync(`${f.dbPath}-journal`), true);
              return yield* authority.readCurrent;
            }),
          )
          .pipe(Effect.result);
      }).pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
      assert.strictEqual(refused._tag, "Failure");
      yield* trusted.verifyUnchanged;
      assert.deepEqual(yield* ownedSql`SELECT id FROM qualification_fixture`, []);
    }).pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
  }).pipe(Effect.scoped),
);

it.effect(
  "bound apply authority still rejects changed native metadata and rolls back its own transaction",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const trusted = yield* makeTrustedNativeEnrollmentCliContext(f, "apply");
      const original = NodeFS.readFileSync(f.authorityPath, "utf8");
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const authority = yield* trusted.bindAuthorityToSqlClient(sql);
        const refused = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* sql`INSERT INTO qualification_fixture (id) VALUES ('changed-authority')`;
              assert.strictEqual(NodeFS.existsSync(`${f.dbPath}-journal`), true);
              NodeFS.writeFileSync(
                f.authorityPath,
                encodeJson({ ...decodeJsonRecord(original), store_generation: 8 }),
              );
              return yield* authority.readCurrent;
            }),
          )
          .pipe(
            Effect.result,
            Effect.ensuring(Effect.sync(() => NodeFS.writeFileSync(f.authorityPath, original))),
          );
        assert.strictEqual(refused._tag, "Failure");
        yield* trusted.verifyUnchanged;
        assert.deepEqual(yield* sql`SELECT id FROM qualification_fixture`, []);
      }).pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect("apply authority binding rejects a connection to another main database", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const other = yield* fixture;
    const trusted = yield* makeTrustedNativeEnrollmentCliContext(f, "apply");
    const otherTrusted = yield* makeTrustedNativeEnrollmentCliContext(other, "apply");
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.strictEqual(
        (yield* Effect.flip(trusted.bindAuthorityToSqlClient(sql))).code,
        "database_unavailable",
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer(otherTrusted.sqlite)), Effect.scoped);
  }).pipe(Effect.scoped),
);

const decodeCompetingWrite = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      pid: Schema.Number,
      acquired: Schema.Boolean,
      errcode: Schema.optional(Schema.Number),
    }),
  ),
);
const competingWrite = (filename: string, stage: string) => {
  const child = NodeChildProcess.spawnSync(
    process.execPath,
    [
      "--input-type=commonjs",
      "-e",
      `
    const { DatabaseSync } = require("node:sqlite");
    const db = new DatabaseSync(process.argv[1]);
    let result;
    try {
      db.exec("PRAGMA busy_timeout=0; BEGIN IMMEDIATE; INSERT INTO qualification_fixture (id) VALUES ('competing-writer'); ROLLBACK");
      result = { acquired: true };
    } catch (error) {
      if (db.isTransaction) db.exec("ROLLBACK");
      if (error.errcode !== 5) throw error;
      result = { acquired: false, errcode: error.errcode };
    } finally { db.close(); }
    process.stdout.write(JSON.stringify({ pid: process.pid, ...result }));
  `,
      filename,
    ],
    { encoding: "utf8", timeout: 10000 },
  );
  assert.strictEqual(child.error, undefined);
  assert.strictEqual(child.signal, null);
  assert.strictEqual(child.status, 0, child.stderr);
  const result = decodeCompetingWrite(child.stdout);
  assert.strictEqual(child.pid, result.pid);
  let absent = false;
  try {
    process.kill(result.pid, 0);
  } catch (error) {
    absent =
      typeof error === "object" && error !== null && "code" in error && error.code === "ESRCH";
  }
  assert.strictEqual(absent, true);
  if (process.env.NW_SOURCE_LOCK_EVIDENCE)
    NodeFS.appendFileSync(
      process.env.NW_SOURCE_LOCK_EVIDENCE,
      JSON.stringify({ stage, ...result, exit: child.status, absent: true }) + "\n",
    );
  return result.acquired;
};

it.effect.each([
  { mode: "DELETE", finish: "commit" },
  { mode: "DELETE", finish: "rollback" },
  { mode: "WAL", finish: "commit" },
  { mode: "WAL", finish: "rollback" },
] as const)(
  "standalone $mode $finish keeps competing processes excluded across owner revalidation",
  ({ mode, finish }) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const setup = new NodeSqlite.DatabaseSync(f.dbPath);
      try {
        setup.exec(`PRAGMA journal_mode=${mode}`);
      } finally {
        setup.close();
      }
      const trusted = yield* makeTrustedNativeEnrollmentCliContext(f, "apply");
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const authority = yield* trusted.bindAuthorityToSqlClient(sql);
        const result = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* sql`INSERT INTO qualification_fixture (id) VALUES ('owner')`;
              assert.strictEqual(
                competingWrite(trusted.sqlite.filename, `${mode}/${finish}/before`),
                false,
              );
              const revalidation = yield* Effect.forkChild(authority.readCurrent);
              assert.strictEqual(
                competingWrite(trusted.sqlite.filename, `${mode}/${finish}/during`),
                false,
              );
              yield* Fiber.join(revalidation);
              assert.strictEqual(
                competingWrite(trusted.sqlite.filename, `${mode}/${finish}/after-first`),
                false,
              );
              yield* authority.readCurrent;
              assert.strictEqual(
                competingWrite(trusted.sqlite.filename, `${mode}/${finish}/after-last`),
                false,
              );
              if (finish === "rollback") return yield* Effect.fail("synthetic-owner-rollback");
            }),
          )
          .pipe(Effect.result);
        assert.strictEqual(result._tag, finish === "commit" ? "Success" : "Failure");
        assert.deepEqual(
          yield* sql`SELECT id FROM qualification_fixture`,
          finish === "commit" ? [{ id: "owner" }] : [],
        );
        assert.strictEqual(
          competingWrite(trusted.sqlite.filename, `${mode}/${finish}/released`),
          true,
        );
        yield* trusted.verifyUnchanged;
      }).pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect.each(["DELETE", "WAL"] as const)(
  "foreign %s client refusal preserves its write exclusion",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const setup = new NodeSqlite.DatabaseSync(f.dbPath);
      try {
        setup.exec(`PRAGMA journal_mode=${mode}`);
      } finally {
        setup.close();
      }
      const trusted = yield* makeTrustedNativeEnrollmentCliContext(f, "apply");
      yield* Effect.gen(function* () {
        const owner = yield* SqlClient.SqlClient;
        const authority = yield* trusted.bindAuthorityToSqlClient(owner);
        yield* Effect.gen(function* () {
          const foreign = yield* SqlClient.SqlClient;
          const result = yield* foreign
            .withTransaction(
              Effect.gen(function* () {
                yield* foreign`INSERT INTO qualification_fixture (id) VALUES ('foreign')`;
                assert.strictEqual(
                  (yield* authority.readCurrent.pipe(Effect.result))._tag,
                  "Failure",
                );
                assert.strictEqual(
                  competingWrite(trusted.sqlite.filename, `${mode}/foreign/refused`),
                  false,
                );
                return yield* Effect.fail("synthetic-foreign-rollback");
              }),
            )
            .pipe(Effect.result);
          assert.strictEqual(result._tag, "Failure");
        }).pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
        assert.deepEqual(yield* owner`SELECT id FROM qualification_fixture`, []);
        assert.strictEqual(
          competingWrite(trusted.sqlite.filename, `${mode}/foreign/released`),
          true,
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect.each([
  "permission",
  "authority",
  "build",
  "environment",
  "launcher",
  "inode",
  "symlink",
] as const)("owned transaction refuses changed %s and rolls back", (change) =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const trusted = yield* makeTrustedNativeEnrollmentCliContext(f, "apply");
    const replacement = `${f.dbPath}.replacement`;
    const displaced = `${f.dbPath}.displaced`;
    NodeFS.copyFileSync(f.dbPath, replacement);
    NodeFS.chmodSync(replacement, 0o600);
    const target =
      change === "authority"
        ? f.authorityPath
        : change === "build"
          ? f.receiptPath
          : change === "environment"
            ? f.environmentPath
            : f.statePath;
    const original = NodeFS.readFileSync(target, "utf8");
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const authority = yield* trusted.bindAuthorityToSqlClient(sql);
      const result = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`INSERT INTO qualification_fixture (id) VALUES ('refused-change')`;
            if (change === "permission") NodeFS.chmodSync(f.dbPath, 0o640);
            else if (change === "inode" || change === "symlink") {
              NodeFS.renameSync(f.dbPath, displaced);
              if (change === "inode") NodeFS.renameSync(replacement, f.dbPath);
              else NodeFS.symlinkSync(displaced, f.dbPath);
            } else if (change === "authority")
              NodeFS.writeFileSync(
                target,
                encodeJson({ ...decodeJsonRecord(original), store_generation: 8 }),
              );
            else if (change === "build")
              NodeFS.writeFileSync(
                target,
                encodeJson({ ...decodeJsonRecord(original), sourceTree: "f".repeat(40) }),
              );
            else if (change === "environment")
              NodeFS.writeFileSync(target, "another-environment\n");
            else
              NodeFS.writeFileSync(
                target,
                encodeJson({
                  protocol: SERVICE_LAUNCHER_PROTOCOL,
                  activeVersion: version,
                  update: { status: "pending" },
                }),
              );
            const refusal = yield* authority.readCurrent.pipe(
              Effect.result,
              Effect.ensuring(
                Effect.sync(() => {
                  if (change === "permission") NodeFS.chmodSync(f.dbPath, 0o600);
                  else if (change === "inode" || change === "symlink") {
                    if (change === "inode") NodeFS.renameSync(f.dbPath, replacement);
                    else NodeFS.unlinkSync(f.dbPath);
                    NodeFS.renameSync(displaced, f.dbPath);
                  } else NodeFS.writeFileSync(target, original);
                }),
              ),
            );
            assert.strictEqual(refusal._tag, "Failure");
            assert.strictEqual(competingWrite(trusted.sqlite.filename, `${change}/refused`), false);
            return yield* Effect.fail("synthetic-changed-owner-rollback");
          }),
        )
        .pipe(Effect.result);
      assert.strictEqual(result._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT id FROM qualification_fixture`, []);
      yield* trusted.verifyUnchanged;
      assert.strictEqual(competingWrite(trusted.sqlite.filename, `${change}/released`), true);
    }).pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
  }).pipe(Effect.scoped),
);

it.effect("desktop authority retains the live owner boundary and no-open transaction checks", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    // A desktop owner is independent of the standalone launcher/receipt path.
    NodeFS.unlinkSync(f.statePath);
    NodeFS.unlinkSync(f.receiptPath);
    let current = true;
    let refreshes = 0;
    const owner: DesktopNativeEnrollmentOwner = {
      binding: {
        home: f.baseDir,
        databasePath: f.dbPath,
        profile: NodePath.join(f.baseDir, "profile"),
        environmentId: "synthetic-environment",
        version,
        sourceSha: "a".repeat(40),
        sourceTree: "b".repeat(40),
        manifestSha256: "c".repeat(64),
      },
      assertCurrent: () => {
        if (!current) throw new Error("synthetic owner expired");
      },
      refresh: Effect.sync(() => {
        refreshes++;
      }),
    };
    const trusted = yield* makeTrustedNativeEnrollmentDesktopContext(f, "apply", owner);
    assert.strictEqual(
      (yield* makeTrustedNativeEnrollmentCliContext(f, "apply").pipe(Effect.result))._tag,
      "Failure",
    );
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const authority = yield* trusted.bindAuthorityToSqlClient(sql);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO qualification_fixture (id) VALUES ('desktop-owner')`;
          yield* authority.readCurrent;
          assert.strictEqual(competingWrite(trusted.sqlite.filename, "desktop/qualified"), false);
          current = false;
          assert.strictEqual((yield* authority.readCurrent.pipe(Effect.result))._tag, "Failure");
          assert.strictEqual(competingWrite(trusted.sqlite.filename, "desktop/refused"), false);
          current = true;
        }),
      );
      assert.strictEqual(competingWrite(trusted.sqlite.filename, "desktop/released"), true);
    }).pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
    assert.isAbove(refreshes, 2);
  }).pipe(Effect.scoped),
);

it.effect.each(["DELETE", "WAL"] as const)(
  "same-client %s binding refuses an already active transaction without losing locks",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const setup = new NodeSqlite.DatabaseSync(f.dbPath);
      try {
        setup.exec(`PRAGMA journal_mode=${mode}`);
      } finally {
        setup.close();
      }
      const trusted = yield* makeTrustedNativeEnrollmentCliContext(f, "apply");
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql.withTransaction(
          Effect.gen(function* () {
            assert.strictEqual(
              competingWrite(trusted.sqlite.filename, `${mode}/active-binding/before`),
              false,
            );
            assert.strictEqual(
              (yield* Effect.flip(trusted.bindAuthorityToSqlClient(sql))).code,
              "database_unavailable",
            );
            assert.strictEqual(
              competingWrite(trusted.sqlite.filename, `${mode}/active-binding/refused`),
              false,
            );
          }),
        );
        assert.strictEqual(
          competingWrite(trusted.sqlite.filename, `${mode}/active-binding/released`),
          true,
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
    }).pipe(Effect.scoped),
);
