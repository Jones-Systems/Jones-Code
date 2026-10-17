import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
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
import { NativeProviderEnrollmentBinding } from "../jones/workstreams/nativeProvider/enrollment.ts";
import {
  binding,
  request,
  requestText,
  requestBytesSha256,
  now,
} from "../jones/workstreams/nativeProvider/testFixtures.ts";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import * as Sqlite from "./Sqlite.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";

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
          const offline = ProjectionMaintenance.layer.pipe(
            Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
            Layer.provideMerge(connection),
          );
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
            const projections = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
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
            const enrollment = yield* Schema.encodeEffect(
              Schema.fromJsonString(NativeProviderEnrollmentBinding),
            )(binding);
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
            assert.isTrue((yield* projections.rebuild).valid);
            assert.isTrue((yield* projections.verify).valid);
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
          const projections = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
          assert.isTrue((yield* projections.rebuild).valid);
          return yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
        }).pipe(
          Effect.provide(
            ProjectionMaintenance.layer.pipe(
              Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer)),
              Layer.provideMerge(NodeSqliteClient.layer({ filename: dbPath })),
            ),
          ),
        );
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
        // A migrated database created before incremental auto_vacuum, with free pages.
        yield* Effect.flatMap(
          ProjectionMaintenance.ProjectionMaintenanceV2,
          (projections) => projections.rebuild,
        ).pipe(
          Effect.provide(
            ProjectionMaintenance.layer.pipe(
              Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer)),
              Layer.provideMerge(Sqlite.layerFromPath(dbPath)),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA auto_vacuum = NONE`;
          yield* sql`VACUUM`;
          yield* sql`CREATE TABLE retained_data (value TEXT)`;
          yield* sql`INSERT INTO retained_data VALUES ('keep')`;
          yield* sql`CREATE TABLE discarded_data (value BLOB)`;
          yield* sql`INSERT INTO discarded_data VALUES (zeroblob(4194304))`;
          yield* sql`DROP TABLE discarded_data`;
          yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));

        // A server that merely has the database open, like an idle one, is enough to refuse.
        const sizeBefore = (yield* fs.stat(dbPath)).size;
        const refused = yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`SELECT value FROM retained_data`;
          return yield* maintenance.compact(dbPath).pipe(Effect.flip);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
        assert.equal(refused._tag, "DatabaseInUseError");
        assert.equal((yield* fs.stat(dbPath)).size, sizeBefore);

        const result = yield* maintenance.compact(dbPath);
        // The 4 MiB of free pages are returned to the filesystem.
        assert.isBelow(result.afterBytes, result.beforeBytes - 3 * 1024 * 1024);
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(yield* sql`PRAGMA auto_vacuum`, [{ auto_vacuum: 2 }]);
          assert.deepEqual(yield* sql`SELECT value FROM retained_data`, [{ value: "keep" }]);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath, readonly: true })));
      }).pipe(Effect.scoped),
    );
  },
);
