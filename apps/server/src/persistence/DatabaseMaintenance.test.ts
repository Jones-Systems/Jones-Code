import * as Schema from "effect/Schema";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "./Migrations.ts";
import { runJonesMigrations } from "../jones/persistence/JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../jones/persistence/JonesMigrations.ts";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import * as Sqlite from "./Sqlite.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";

const encodeNativeProviderEnrollment = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

// Synthetic released-history rows are opaque to ordinary maintenance. No legacy runtime is loaded.
const request = {
  owner_id: "owner-synthetic",
  principal_id: "principal-synthetic",
  command_id: "command-synthetic-0001",
  native_reference_id: "reference-synthetic",
  native_action: "settle",
  request_sha256: "2222222222222222222222222222222222222222222222222222222222222222",
  reservation_sha256: "3333333333333333333333333333333333333333333333333333333333333333",
  identity: {
    provider: "t3",
    source_instance_id: "source-synthetic",
    resource_kind: "thread",
    id_kind: "internal",
    native_id: "thread-synthetic",
    account_provenance: {
      kind: "not_account_scoped",
    },
  },
  expected_authority_namespace: "authority-synthetic",
  expected_store_generation: 7,
} as const;
const binding = {
  owner_id: "owner-synthetic",
  principal_id: "principal-synthetic",
  source_instance_id: "source-synthetic",
  authority_namespace: "authority-synthetic",
  store_generation: 7,
  enrollment_id: "enrollment-synthetic",
  protocol: "workstreams-t3-provider/1.0.0",
  build: {
    repository: "Jones-Systems/Jones-Code",
    sha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    tree: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  },
  session_id: "session-synthetic",
  registry_origin: "https://registry.invalid",
  scopes: [
    "workstreams:native:context",
    "workstreams:native:settlement",
    "workstreams:native:reconciliation",
  ],
} as const;
const requestText =
  '{"owner_id":"owner-synthetic","principal_id":"principal-synthetic","command_id":"command-synthetic-0001","native_reference_id":"reference-synthetic","native_action":"settle","request_sha256":"2222222222222222222222222222222222222222222222222222222222222222","reservation_sha256":"3333333333333333333333333333333333333333333333333333333333333333","identity":{"provider":"t3","source_instance_id":"source-synthetic","resource_kind":"thread","id_kind":"internal","native_id":"thread-synthetic","account_provenance":{"kind":"not_account_scoped"}},"expected_authority_namespace":"authority-synthetic","expected_store_generation":7}';
const requestBytesSha256 = "c829d669cdb5482b6bc4d6594d83eb326fb24d794050353f463744659c32e297";
const now = "2026-01-01T00:00:00.000Z";

it.layer(DatabaseMaintenance.layer.pipe(Layer.provideMerge(NodeServices.layer)))(
  "DatabaseMaintenance",
  (it) => {
    it.effect("reports an unreadable database as an inspection failure", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const maintenance = yield* DatabaseMaintenance.DatabaseMaintenance;
        const dbPath = path.join(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-inspect-" }),
          "statev2.sqlite",
        );
        yield* fs.writeFileString(dbPath, "not a database");
        const failure = yield* Effect.flip(maintenance.inspect(dbPath));
        assert.equal(failure._tag, "DatabaseInspectError");
      }).pipe(Effect.scoped),
    );

    it.effect("reports and deletes only the database from before 0.1.0", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const maintenance = yield* DatabaseMaintenance.DatabaseMaintenance;
        const userdata = yield* fs.makeTempDirectoryScoped({ prefix: "t3-legacy-" });
        const dbPath = path.join(userdata, "statev2.sqlite");
        const legacy = path.join(userdata, "state.sqlite");
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TABLE retained_data (value TEXT)`;
          yield* sql`INSERT INTO retained_data VALUES ('keep')`;
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
        assert.isNull((yield* maintenance.inspect(dbPath)).legacyDatabaseBytes);

        const openLegacy = NodeSqliteClient.layer({ filename: legacy });
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA journal_mode = WAL`;
          yield* sql`CREATE TABLE v1_data (value TEXT)`;
          yield* sql`INSERT INTO v1_data VALUES ('rollback')`;
        }).pipe(Effect.provide(openLegacy));
        assert.isAbove((yield* maintenance.inspect(dbPath)).legacyDatabaseBytes ?? 0, 0);

        const refused = yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`SELECT value FROM v1_data`;
          const failure = yield* Effect.flip(maintenance.removeLegacyDatabase(dbPath));
          assert.deepEqual(yield* sql`SELECT value FROM v1_data`, [{ value: "rollback" }]);
          return failure;
        }).pipe(Effect.provide(openLegacy));
        assert.equal(refused._tag, "DatabaseInUseError");
        assert.isTrue(yield* fs.exists(legacy));

        yield* Effect.flatMap(DatabaseMaintenance.DatabaseMaintenance, (service) =>
          service.removeLegacyDatabase(dbPath),
        ).pipe(
          Effect.provide(Layer.fresh(DatabaseMaintenance.layer)),
          Effect.provideService(HostProcess.Platform, "win32"),
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            remove: (target, options) =>
              Effect.gen(function* () {
                if (target === legacy) {
                  yield* Effect.gen(function* () {
                    const sql = yield* SqlClient.SqlClient;
                    yield* sql`PRAGMA busy_timeout = 0`;
                    yield* sql`BEGIN EXCLUSIVE`;
                    yield* sql`COMMIT`;
                  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: legacy })));
                  // Create sidecars after the probe closes so only Doctor can remove them.
                  yield* fs.writeFileString(`${legacy}-wal`, "leftover WAL");
                  yield* fs.writeFileString(`${legacy}-shm`, "leftover SHM");
                }
                yield* fs.remove(target, options);
              }),
          }),
        );
        assert.isFalse(yield* fs.exists(legacy));
        assert.isFalse(yield* fs.exists(`${legacy}-wal`));
        assert.isFalse(yield* fs.exists(`${legacy}-shm`));
        assert.isTrue(yield* fs.exists(dbPath));
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(yield* sql`SELECT value FROM retained_data`, [{ value: "keep" }]);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath, readonly: true })));
        assert.isNull((yield* maintenance.inspect(dbPath)).legacyDatabaseBytes);
      }).pipe(Effect.scoped),
    );

    it.effect("runs a repair requested from Settings once and keeps its failure for Settings", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-requested-reclaim-" });
        const dbPath = path.join(baseDir, "userdata", "statev2.sqlite");
        const files = DatabaseMaintenance.reclaimFiles(path, dbPath);
        yield* Effect.void.pipe(Effect.provide(Sqlite.layerFromPath(dbPath)));
        const autoVacuum = Effect.flatMap(
          SqlClient.SqlClient,
          (sql) => sql<{ readonly auto_vacuum: number }>`PRAGMA auto_vacuum`,
        ).pipe(
          Effect.map((rows) => rows[0]?.auto_vacuum),
          Effect.provide(NodeSqliteClient.layer({ filename: dbPath, readonly: true })),
        );
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA auto_vacuum = NONE`;
          yield* sql`VACUUM`;
          yield* sql`CREATE TABLE retained_data (value TEXT)`;
          yield* sql`INSERT INTO retained_data VALUES ('keep')`;
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));

        // Every server exit runs this; without a request it must leave the database alone.
        yield* DatabaseMaintenance.runRequestedReclaim(dbPath);
        assert.equal(yield* autoVacuum, 0);

        yield* fs.writeFileString(files.request, "");
        yield* fs.writeFileString(files.failure, "an earlier failure");
        yield* DatabaseMaintenance.runRequestedReclaim(dbPath);
        assert.isFalse(yield* fs.exists(files.request));
        assert.isFalse(yield* fs.exists(files.failure));
        assert.equal(yield* autoVacuum, 2);
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(yield* sql`SELECT value FROM retained_data`, [{ value: "keep" }]);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath, readonly: true })));

        const brokenPath = path.join(baseDir, "broken", "statev2.sqlite");
        const broken = DatabaseMaintenance.reclaimFiles(path, brokenPath);
        yield* fs.makeDirectory(path.dirname(brokenPath));
        yield* fs.writeFileString(brokenPath, "not a database");
        yield* fs.writeFileString(broken.request, "");
        yield* DatabaseMaintenance.runRequestedReclaim(brokenPath);
        assert.isFalse(
          yield* fs.exists(broken.request),
          "a failed repair must not rerun on every exit",
        );
        assert.equal(
          yield* fs.readFileString(broken.failure),
          new DatabaseMaintenance.DatabaseCompactError({ path: brokenPath, cause: null }).message,
        );
      }).pipe(Effect.scoped),
    );

    it.effect("refuses a repair whose copies would not fit on disk", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const dbPath = path.join(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-reclaim-space-" }),
          "statev2.sqlite",
        );
        assert.isNull(yield* DatabaseMaintenance.reclaimSpaceShortfall(dbPath, 1024));
        const huge = 2 ** 60;
        const shortfall = yield* DatabaseMaintenance.reclaimSpaceShortfall(dbPath, huge);
        assert.isAtLeast(shortfall?.neededBytes ?? 0, huge);
        assert.isBelow(shortfall?.freeBytes ?? Infinity, huge);
      }).pipe(Effect.scoped),
    );

    it.effect(
      "refuses pre-107 offline compaction before superseded settlement evidence can be pruned",
      () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const maintenance = yield* DatabaseMaintenance.DatabaseMaintenance;
          const dbPath = path.join(
            yield* fs.makeTempDirectoryScoped({ prefix: "jones-pre107-compact-" }),
            "statev2.sqlite",
          );
          const connection = NodeSqliteClient.layer({ filename: dbPath });
          const offline = EventStore.layer.pipe(Layer.provideMerge(connection));
          const readEvidence = Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            return {
              events: yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
              receipts:
                yield* sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
              attempts: yield* sql`SELECT * FROM workstreams_native_attempts`,
              upstreamLedger: yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
              jonesLedger: yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
            };
          });
          const before = yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
            yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id <= 106));
            const events = yield* EventStore.EventStoreV2;
            const at = DateTime.makeUnsafe(now);
            const threadId = ThreadId.make(request.identity.native_id);
            const instanceId = ProviderInstanceId.make("codex");
            const thread: OrchestrationV2AppThread = {
              id: threadId,
              projectId: ProjectId.make("offline-project"),
              title: "Offline fixture",
              createdBy: "user",
              creationSource: "web",
              providerInstanceId: instanceId,
              modelSelection: { instanceId, model: "synthetic-model" },
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              activeProviderThreadId: null,
              lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
              forkedFrom: null,
              createdAt: at,
              updatedAt: at,
              archivedAt: null,
              deletedAt: null,
              settledAt: null,
              settledOverride: null,
              lastVisitedAt: null,
            };
            yield* events.append({
              events: [
                {
                  id: EventId.make("offline-created"),
                  type: "thread.created",
                  threadId,
                  occurredAt: at,
                  payload: thread,
                },
              ],
            });
            const commandId = CommandId.make("offline-native-settle");
            const settled = { ...thread, settledAt: at, settledOverride: "settled" as const };
            const batch = yield* events.append({
              commandId,
              events: [
                {
                  id: EventId.make("offline-settled"),
                  type: "thread.settled",
                  threadId,
                  occurredAt: at,
                  payload: settled,
                },
                {
                  id: EventId.make("offline-unpinned"),
                  type: "thread.unpinned",
                  threadId,
                  occurredAt: at,
                  payload: settled,
                },
              ],
            });
            yield* sql`INSERT INTO orchestration_command_receipts
            (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
            VALUES (${commandId}, 'thread', ${threadId}, 'thread.settle', ${now}, ${batch.at(-1)!.sequence}, 'accepted', NULL)`;
            const enrollment = yield* encodeNativeProviderEnrollment(binding);
            yield* sql`INSERT INTO workstreams_native_attempts VALUES
            (${request.owner_id}, ${request.principal_id}, ${request.command_id}, ${requestText}, ${requestBytesSha256}, ${"e".repeat(64)}, ${enrollment}, ${commandId}, ${now}, ${now})`;
            yield* events.append({
              events: [
                {
                  id: EventId.make("offline-newer-metadata"),
                  type: "thread.metadata-updated",
                  threadId,
                  occurredAt: at,
                  payload: { ...settled, title: "Superseding metadata" },
                },
              ],
            });
            assert.deepEqual(
              yield* sql`SELECT name FROM sqlite_master WHERE name = 'jones_workstreams_settlement_proofs'`,
              [],
            );
            return yield* readEvidence;
          }).pipe(Effect.provide(offline));
          const result = yield* maintenance.compact(dbPath).pipe(Effect.result);
          assert.equal(result._tag, "Failure");
          if (result._tag !== "Failure")
            throw new Error("Expected pre-upgrade compaction refusal.");
          assert.equal(result.failure._tag, "JonesSettlementProofMigrationRequiredError");
          assert.include(result.failure.message, "Start the updated Jones Code server once");
          assert.deepEqual(
            yield* readEvidence.pipe(
              Effect.provide(NodeSqliteClient.layer({ filename: dbPath, readonly: true })),
            ),
            before,
          );
        }).pipe(Effect.scoped),
    );

    it.effect("compacts an upstream-only database without requiring absent Jones tables", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const maintenance = yield* DatabaseMaintenance.DatabaseMaintenance;
        const dbPath = path.join(
          yield* fs.makeTempDirectoryScoped({ prefix: "upstream-only-compact-" }),
          "statev2.sqlite",
        );
        const ledger = yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
          return yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
        const result = yield* maintenance.compact(dbPath);
        assert.equal(result.deletedEventCount, 0);
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(
            yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
            ledger,
          );
          assert.deepEqual(
            yield* sql`SELECT name FROM sqlite_master WHERE name IN ('jones_sql_migrations', 'workstreams_native_attempts', 'jones_workstreams_settlement_proofs')`,
            [],
          );
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath, readonly: true })));
      }).pipe(Effect.scoped),
    );

    it.effect("compacts an existing database offline only once its server has stopped", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const maintenance = yield* DatabaseMaintenance.DatabaseMaintenance;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-compact-" });
        const dbPath = path.join(baseDir, "userdata", "statev2.sqlite");
        // Opening the database migrates it.
        yield* Effect.void.pipe(Effect.provide(Sqlite.layerFromPath(dbPath)));
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA auto_vacuum = NONE`;
          yield* sql`VACUUM`;
          yield* sql`CREATE TABLE retained_data (value TEXT)`;
          yield* sql`INSERT INTO retained_data VALUES ('keep')`;
          yield* sql`CREATE TABLE discarded_data (value BLOB)`;
          yield* sql`INSERT INTO discarded_data VALUES (zeroblob(4194304))`;
          yield* sql`DROP TABLE discarded_data`;
          yield* sql`
            INSERT INTO orchestration_v2_projection_messages (
              message_id, thread_id, run_id, node_id, role, streaming, created_at, updated_at, payload_json
            )
            VALUES (
              'message-1', 'thread-1', NULL, NULL, 'user', 0,
              '2026-09-01T00:00:00.000Z', '2026-09-01T00:00:00.000Z', '{"text":"needle"}'
            )
          `;
          // An index that drifted from its rows, as a renumbering VACUUM would leave it.
          yield* sql`
            INSERT INTO orchestration_v2_message_search (orchestration_v2_message_search)
            VALUES ('delete-all')
          `;
          yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
          yield* sql`INSERT INTO orchestration_v2_legacy_imports (
            thread_id, source_updated_at, shell_imported_at, transcript_imported_at, imported_message_count
          ) VALUES ('imported', '2026-10-17', '2026-10-17', '2026-10-17', 0)`;
          yield* sql`WITH RECURSIVE history(n) AS (
            SELECT 1 UNION ALL SELECT n + 1 FROM history WHERE n < 51
          ) INSERT INTO orchestration_command_receipts (
            command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status, command_type
          ) SELECT 'legacy-' || n, 'thread', 'imported', '2026-10-17', 0, 'accepted', 'legacy' FROM history`;
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));

        const sizeBefore = (yield* fs.stat(dbPath)).size;
        const refused = yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`SELECT value FROM retained_data`;
          return yield* maintenance.compact(dbPath).pipe(Effect.flip);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
        assert.equal(refused._tag, "DatabaseInUseError");
        assert.equal((yield* fs.stat(dbPath)).size, sizeBefore);

        const clock = yield* Clock.Clock;
        const pauses: number[] = [];
        let activeNanos = 0n;
        const result = yield* maintenance.compact(dbPath).pipe(
          Effect.provideService(Clock.Clock, {
            ...clock,
            // Exceed the background budget whenever the compactor checks elapsed time.
            monotonicTimeNanos: Effect.sync(() => (activeNanos += 30_000_000n)),
            sleep: (duration) =>
              Effect.sync(() => {
                pauses.push(Duration.toMillis(duration));
              }),
          }),
        );
        assert.deepEqual(pauses, [], "offline doctor must not throttle compaction");
        assert.equal(result.deletedReceiptCount, 51);
        assert.isBelow(result.afterBytes, result.beforeBytes - 3 * 1024 * 1024);
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(yield* sql`PRAGMA auto_vacuum`, [{ auto_vacuum: 2 }]);
          assert.deepEqual(yield* sql`SELECT value FROM retained_data`, [{ value: "keep" }]);
          assert.deepEqual(
            yield* sql`
              SELECT messages.message_id AS "messageId"
              FROM orchestration_v2_message_search AS search
              INNER JOIN orchestration_v2_projection_messages AS messages
                ON messages.rowid = search.rowid
              WHERE orchestration_v2_message_search MATCH '"needle"'
            `,
            [{ messageId: "message-1" }],
          );
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath, readonly: true })));
      }).pipe(Effect.scoped),
    );
  },
);
