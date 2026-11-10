import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import * as ServerConfig from "../config.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import * as DatabaseStorage from "./DatabaseStorage.ts";
import * as Sqlite from "./Sqlite.ts";

const freePages = Effect.map(
  Effect.flatMap(
    SqlClient.SqlClient,
    (sql) => sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`,
  ),
  (rows) => rows[0]?.freelist_count ?? 0,
);

// ProjectionMaintenance reads and prunes the event and projection stores, so tests that use the real
// maintenance service also provide the real stores over the same database.
const projectionMaintenanceLayer = ProjectionMaintenance.layer.pipe(
  Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer)),
);

const storageLayer = <E>(
  config: Layer.Layer<ServerConfig.ServerConfig, E, FileSystem.FileSystem | Path.Path>,
) =>
  DatabaseStorage.layer.pipe(
    Layer.provide(projectionMaintenanceLayer),
    Layer.provideMerge(Sqlite.layerMemory),
    Layer.provideMerge(config),
    Layer.provide(NodeServices.layer),
  );

const testConfig = ServerConfig.layerTest(process.cwd(), { prefix: "database-storage-" });

it.effect("a new database returns freed pages online without losing data", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const storage = yield* DatabaseStorage.DatabaseStorage;
    assert.deepEqual(yield* sql`PRAGMA auto_vacuum`, [{ auto_vacuum: 2 }]);
    yield* sql`CREATE TABLE retained (value TEXT)`;
    yield* sql`INSERT INTO retained VALUES ('keep')`;
    yield* sql`CREATE TABLE discarded (value BLOB)`;
    yield* sql`INSERT INTO discarded VALUES (zeroblob(1048576))`;
    yield* sql`DROP TABLE discarded`;
    assert.isAbove(yield* freePages, 0);
    yield* storage.reclaim;
    assert.equal(yield* freePages, 0);
    assert.deepEqual(yield* sql`SELECT value FROM retained`, [{ value: "keep" }]);
  }).pipe(Effect.provide(storageLayer(testConfig))),
);

const compactionLayer = (
  pruneEventStore: ProjectionMaintenance.ProjectionMaintenanceV2["Service"]["pruneEventStore"],
) =>
  DatabaseStorage.layer.pipe(
    Layer.provide(
      Layer.mock(ProjectionMaintenance.ProjectionMaintenanceV2)({
        pruneEventStore,
      }),
    ),
    Layer.provide(Sqlite.layerMemory),
  );

const minute = 60_000;
const startupTime = Date.UTC(2026, 9, 19);
const emptyCompaction = { deletedEventCount: 0, deletedReceiptCount: 0, reclaimableBytes: 0 };

it.effect("remembers completion only after the paced pass finishes", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(startupTime);
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* ServerConfig.ServerConfig;
    const timestampPath = path.join(path.dirname(config.dbPath), "compacted-at");
    const betweenPagesReached = yield* Deferred.make<void>();
    const completed = yield* Deferred.make<void>();
    const timestampWritten = yield* Deferred.make<void>();
    const compact = (betweenPages: Effect.Effect<void> = Effect.void) =>
      Effect.gen(function* () {
        yield* Effect.sleep(30);
        yield* Deferred.succeed(betweenPagesReached, undefined);
        yield* betweenPages;
        yield* Deferred.succeed(completed, undefined);
        return emptyCompaction;
      });
    yield* Effect.gen(function* () {
      yield* TestClock.adjust(minute);
      yield* TestClock.adjust(30);
      yield* Deferred.await(betweenPagesReached);
      assert.isFalse(yield* Deferred.isDone(completed));
      assert.isFalse(yield* fs.exists(timestampPath));
      yield* TestClock.adjust(269);
      assert.isFalse(yield* Deferred.isDone(completed));
      assert.isFalse(yield* fs.exists(timestampPath));
      yield* TestClock.adjust(1);
      yield* Deferred.await(timestampWritten);
      assert.isTrue(yield* Deferred.isDone(completed));
      assert.equal(Number(yield* fs.readFileString(timestampPath)), startupTime + minute + 300);
    }).pipe(
      Effect.provide(compactionLayer(compact)),
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        writeFileString: (file, content, options) =>
          fs
            .writeFileString(file, content, options)
            .pipe(Effect.ensuring(Deferred.succeed(timestampWritten, undefined))),
      }),
    );
  }).pipe(Effect.provide(testConfig.pipe(Layer.provideMerge(NodeServices.layer)))),
);

// Sqlite.layerMemory applies every migration, so the preflight reads real state; `rewind` then puts
// the database back where an install from before the settlement proofs would be.
const provenCompactionLayer = (
  pruneEventStore: ProjectionMaintenance.ProjectionMaintenanceV2["Service"]["pruneEventStore"],
  rewind: Effect.Effect<void, SqlError, SqlClient.SqlClient>,
) =>
  DatabaseStorage.layer.pipe(
    Layer.provide(Layer.mock(ProjectionMaintenance.ProjectionMaintenanceV2)({ pruneEventStore })),
    Layer.provide(Layer.effectDiscard(rewind).pipe(Layer.provideMerge(Sqlite.layerMemory))),
  );

const beforeSettlementProofs = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DROP TABLE jones_workstreams_settlement_proofs`;
  yield* sql`DELETE FROM jones_sql_migrations WHERE migration_id = 107`;
});

const onlineCompactionWith = (rewind: Effect.Effect<void, SqlError, SqlClient.SqlClient>) =>
  Effect.gen(function* () {
    // The clock is set before the layer starts the passes, so its wake-ups stay near the epoch.
    yield* TestClock.setTime(startupTime);
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* ServerConfig.ServerConfig;
    const timestampPath = path.join(path.dirname(config.dbPath), "compacted-at");
    const settled = yield* Deferred.make<void>();
    const warnings: Array<string> = [];
    let prunes = 0;
    const pruneEventStore = () =>
      Effect.sync(() => {
        prunes += 1;
        return emptyCompaction;
      });
    const logger = Logger.make(({ message, logLevel }) => {
      if (logLevel === "Warn") warnings.push(String(message));
      if (String(message).includes("compaction skipped before pruning")) {
        Deferred.doneUnsafe(settled, Effect.void);
      }
    });
    yield* Effect.gen(function* () {
      yield* TestClock.adjust(minute);
      yield* Deferred.await(settled);
    }).pipe(
      Effect.provide(provenCompactionLayer(pruneEventStore, rewind)),
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        writeFileString: (file, content, options) =>
          fs
            .writeFileString(file, content, options)
            .pipe(Effect.ensuring(Deferred.succeed(settled, undefined))),
      }),
      Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
    );
    return { prunes, warnings, timestampWritten: yield* fs.exists(timestampPath) };
  }).pipe(Effect.provide(testConfig.pipe(Layer.provideMerge(NodeServices.layer))));

it.effect("online compaction refuses to prune until settlement proofs are preserved", () =>
  Effect.gen(function* () {
    // A database from before settlement proofs (Jones migration 107), as an old install leaves it.
    const result = yield* onlineCompactionWith(beforeSettlementProofs);
    assert.equal(result.prunes, 0);
    assert.isFalse(result.timestampWritten, "a skipped pass is not remembered as completed");
    assert.isTrue(
      result.warnings.some((warning) => warning.includes("compaction skipped before pruning")),
    );
  }),
);

it.effect("online compaction prunes once settlement proofs are preserved", () =>
  Effect.gen(function* () {
    const result = yield* onlineCompactionWith(Effect.void);
    assert.equal(result.prunes, 1);
    assert.isTrue(result.timestampWritten);
    assert.deepEqual(result.warnings, []);
  }),
);

// NONE is a database from before automatic shrinking, which the repair converts.
const reclaimLayer = (
  restart: Effect.Effect<void> | null,
  autoVacuum: "NONE" | "FULL" | "INCREMENTAL" = "NONE",
) =>
  DatabaseStorage.layer.pipe(
    Layer.provide(projectionMaintenanceLayer),
    Layer.provide(Layer.succeed(DatabaseStorage.ServerRestart, restart)),
    Layer.provide(
      Layer.effectDiscard(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql.unsafe(`PRAGMA auto_vacuum = ${autoVacuum}`);
          yield* sql`VACUUM`;
        }),
      ).pipe(Layer.provideMerge(Sqlite.layerMemory)),
    ),
  );

it.effect(
  "a reclaim from Settings saves its request, then restarts the server after replying",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const config = yield* ServerConfig.ServerConfig;
      const files = DatabaseMaintenance.reclaimFiles(path, config.dbPath);
      const request = Effect.flatMap(
        DatabaseStorage.DatabaseStorage,
        (storage) => storage.requestReclaim,
      );
      const restarted = yield* Deferred.make<void>();
      const restart = Deferred.succeed(restarted, undefined).pipe(Effect.asVoid);

      // Nothing starts a server run by hand again, so it keeps pointing at the command.
      const unsupervised = yield* request.pipe(Effect.flip, Effect.provide(reclaimLayer(null)));
      assert.equal(unsupervised._tag, "DatabaseReclaimError");
      // A database that already shrinks on its own (or a stale dialog after another client's
      // repair) has nothing to reclaim, so it must not restart.
      for (const autoVacuum of ["FULL", "INCREMENTAL"] as const) {
        const refused = yield* request.pipe(
          Effect.flip,
          Effect.provide(reclaimLayer(restart, autoVacuum)),
        );
        assert.equal(refused._tag, "DatabaseReclaimError", autoVacuum);
      }
      assert.isFalse(yield* fs.exists(files.request));

      yield* fs.writeFileString(files.failure, "an earlier failure");
      yield* Effect.gen(function* () {
        const storage = yield* DatabaseStorage.DatabaseStorage;
        const shownFailure = storage.status.pipe(
          Stream.filter((status) => status !== null),
          Stream.runHead,
          Effect.map((status) => Option.getOrThrow(status)?.reclaimFailure),
        );
        assert.equal(yield* shownFailure, "an earlier failure");
        yield* storage.requestReclaim;
        // The retry clears it at once, so Settings shows the restart, not the old failure.
        assert.isUndefined(yield* shownFailure);
      }).pipe(Effect.provide(reclaimLayer(restart)));
      assert.isTrue(yield* fs.exists(files.request));
      assert.isFalse(yield* fs.exists(files.failure));
      assert.isFalse(yield* Deferred.isDone(restarted), "the reply must go out before the restart");
      yield* TestClock.adjust(500);
      yield* Deferred.await(restarted);
    }).pipe(Effect.provide(testConfig.pipe(Layer.provideMerge(NodeServices.layer)))),
);
