// @effect-diagnostics nodeBuiltinImport:off -- All persisted qualification fixtures are synthetic and cleanup-owned.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { HostProcessPlatform, HostProcessArchitecture } from "@t3tools/shared/hostProcess";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { qualifiedPayloadDigest, QUALIFIED_RUNTIME_RECEIPT } from "../../cloud/qualifiedRuntime.ts";
import {
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_RESTART_PENDING_FILE,
} from "../../cloud/serviceProtocol.ts";
import { makeTrustedNativeEnrollmentCliContext } from "./trustedCliContext.ts";

vi.mock("../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
const version = "0.0.0-preview.20261002.100";
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJsonRecord = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const fixture = Effect.acquireRelease(
  Effect.gen(function* () {
    const platform = yield* HostProcessPlatform;
    const architecture = yield* HostProcessArchitecture;
    return yield* Effect.tryPromise(async () => {
      const baseDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "workstreams-cli-context-synthetic-"),
      );
      try {
        const authorityDir = NodePath.join(baseDir, "native-store-authority");
        const release = NodePath.join(baseDir, "runtime", "versions", version);
        for (const dir of [NodePath.join(baseDir, "userdata"), authorityDir, release])
          NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
        const statePath = NodePath.join(baseDir, "runtime", "service-state.json");
        NodeFS.writeFileSync(
          statePath,
          encodeJson({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: version }),
          { mode: 0o600 },
        );
        const environmentPath = NodePath.join(baseDir, "userdata", "environment-id");
        NodeFS.writeFileSync(environmentPath, "synthetic-environment\n", { mode: 0o600 });
        const dbPath = NodePath.join(baseDir, "userdata", "state.sqlite");
        const db = new NodeSqlite.DatabaseSync(dbPath);
        try {
          db.exec("CREATE TABLE qualification_fixture (id TEXT)");
        } finally {
          db.close();
        }
        NodeFS.chmodSync(dbPath, 0o600);
        const authorityPath = NodePath.join(authorityDir, "native-store-authority-v1.json");
        NodeFS.writeFileSync(
          authorityPath,
          encodeJson({
            record_version: "t3-native-store-authority/1.0.0",
            environment_id: "synthetic-environment",
            authority_namespace: "t3-native:12345678-1234-4234-8234-123456789abc",
            store_generation: 7,
            state: "active",
            transition_id: null,
          }),
          { mode: 0o600 },
        );
        NodeFS.writeFileSync(NodePath.join(release, "t3"), "synthetic executable bytes", {
          mode: 0o700,
        });
        const receiptPath = NodePath.join(release, QUALIFIED_RUNTIME_RECEIPT);
        NodeFS.writeFileSync(
          receiptPath,
          encodeJson({
            protocol: 1,
            repository: "Jones-Systems/Jones-Code",
            channel: "jones-main",
            version,
            sourceSha: "a".repeat(40),
            sourceTree: "b".repeat(40),
            installedSourceSha: "c".repeat(40),
            runId: 101,
            runAttempt: 1,
            artifactId: 102,
            workflow:
              platform === "darwin"
                ? ".github/workflows/artifact-desktop-mac.yml"
                : ".github/workflows/artifact-cli-linux.yml",
            artifactDigest: `sha256:${"d".repeat(64)}`,
            archiveSha256: "e".repeat(64),
            platform,
            architecture,
            payloadSha256: await qualifiedPayloadDigest(release),
          }),
          { mode: 0o600 },
        );
        return {
          baseDir,
          authorityDir,
          authorityPath,
          statePath,
          environmentPath,
          dbPath,
          receiptPath,
        };
      } catch (error) {
        NodeFS.rmSync(baseDir, { recursive: true, force: true });
        throw error;
      }
    });
  }).pipe(
    Effect.provideService(HostProcessPlatform, HostProcessPlatform.defaultValue()),
    Effect.provideService(HostProcessArchitecture, HostProcessArchitecture.defaultValue()),
  ),
  (f) => Effect.sync(() => NodeFS.rmSync(f.baseDir, { recursive: true, force: true })),
);
const snapshot = (directory: string): Record<string, unknown> =>
  Object.fromEntries(
    NodeFS.readdirSync(directory)
      .sort()
      .map((name) => {
        const path = NodePath.join(directory, name);
        const stat = NodeFS.lstatSync(path);
        return [
          name,
          stat.isDirectory()
            ? snapshot(path)
            : {
                bytes: NodeFS.readFileSync(path).toString("hex"),
                mode: stat.mode,
                mtime: stat.mtimeMs,
              },
        ];
      }),
  );

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

it.effect(
  "native enrollment rejects selected V2 and copied stores before opening SQLite or changing authority",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      for (const name of ["statev2.sqlite", "copied-state.sqlite"]) {
        const dbPath = NodePath.join(f.baseDir, "userdata", name);
        NodeFS.copyFileSync(f.dbPath, dbPath);
        const before = snapshot(f.baseDir);
        for (const access of ["strict_inspection", "operational_inspection", "apply"] as const) {
          const result = yield* makeTrustedNativeEnrollmentCliContext(
            { ...f, dbPath },
            access,
          ).pipe(Effect.result);
          assert.strictEqual(result._tag, "Failure");
          if (result._tag === "Failure")
            assert.strictEqual(result.failure.code, "runtime_unqualified");
          assert.deepEqual(snapshot(f.baseDir), before);
          assert.strictEqual(NodeFS.existsSync(`${dbPath}-wal`), false);
          assert.strictEqual(NodeFS.existsSync(`${dbPath}-shm`), false);
        }
      }
    }).pipe(Effect.scoped),
);
