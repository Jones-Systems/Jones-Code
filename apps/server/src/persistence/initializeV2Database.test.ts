// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { afterEach, vi } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import * as SqlitePersistence from "./Layers/Sqlite.ts";
import { runMigrations } from "./Migrations.ts";
import { initializeV2Database } from "./initializeV2Database.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as LegacyV1ThreadImporter from "../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";

vi.mock("node:sqlite", async (importOriginal) => ({
  ...(await importOriginal<typeof NodeSqlite>()),
}));

afterEach(() => vi.restoreAllMocks());

it.effect(
  "snapshots V1, imports transcripts lazily, and preserves both databases across switches",
  () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-v1-v2-"));
    const sourcePath = NodePath.join(directory, "state.sqlite");
    const destinationPath = NodePath.join(directory, "statev2.sqlite");
    const threadId = ThreadId.make("legacy-thread");
    const seed = Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 52 });
      yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
      VALUES ('project', 'Project', '/tmp/project', '[]', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
      yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, created_at, updated_at)
      VALUES (${threadId}, 'project', 'V1 thread', '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', 'default', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
      for (let index = 0; index < 6; index++) {
        yield* sql`INSERT INTO projection_thread_messages (message_id, thread_id, role, text, is_streaming, created_at, updated_at)
        VALUES (${`message-${index}`}, ${threadId}, ${index % 2 ? "assistant" : "user"}, ${`Text ${index}`}, 0, ${`2026-01-0${index + 1}T00:00:00.000Z`}, ${`2026-01-0${index + 1}T00:00:00.000Z`})`;
      }
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: sourcePath })));

    return Effect.gen(function* () {
      yield* seed;
      const original = NodeFS.readFileSync(sourcePath);
      const config = yield* ServerConfig.ServerConfig;
      const databaseLayer = SqlitePersistence.layerConfig.pipe(
        Layer.provide(ServerConfig.layer({ ...config, dbPath: destinationPath })),
      );
      const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
        Layer.provideMerge(databaseLayer),
      );
      const sink = EventSink.layer.pipe(Layer.provide(stores));
      const importer = LegacyV1ThreadImporter.layer.pipe(
        Layer.provideMerge(Layer.mergeAll(stores, sink)),
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const legacy = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        yield* legacy.reconcileShells;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const shell = yield* projections.getThreadProjection(threadId);
        assert.equal(shell.thread.id, threadId);
        assert.deepEqual(
          shell.messages.map((message) => message.text),
          ["Text 4", "Text 5"],
        );
        const pending =
          yield* sql`SELECT transcript_imported_at FROM orchestration_v2_legacy_imports`;
        assert.equal(pending[0]?.transcript_imported_at, null);
        yield* legacy.ensureTranscript(threadId);
        const transcript = yield* projections.getThreadProjection(threadId);
        assert.deepEqual(
          transcript.messages.map((message) => message.text),
          ["Text 0", "Text 1", "Text 2", "Text 3", "Text 4", "Text 5"],
        );
        const imported =
          yield* sql`SELECT imported_message_count, transcript_imported_at FROM orchestration_v2_legacy_imports`;
        assert.equal(imported[0]?.imported_message_count, 6);
        assert.isNotNull(imported[0]?.transcript_imported_at);
        yield* sql`CREATE TABLE v2_work (text TEXT)`;
        yield* sql`INSERT INTO v2_work VALUES ('Keep V2 work')`;
      }).pipe(Effect.provide(importer));
      assert.deepEqual(NodeFS.readFileSync(sourcePath), original);
      const v1 = new NodeSqlite.DatabaseSync(sourcePath);
      try {
        assert.equal(
          v1.prepare("SELECT MAX(migration_id) AS id FROM effect_sql_migrations").get()?.id,
          52,
        );
        assert.equal(
          v1
            .prepare(
              "SELECT count(*) AS count FROM sqlite_master WHERE name = 'orchestration_v2_legacy_imports'",
            )
            .get()?.count,
          0,
        );
        v1.exec("UPDATE projection_threads SET title = 'Continued in V1'");
      } finally {
        v1.close();
      }
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.equal((yield* sql`SELECT text FROM v2_work`)[0]?.text, "Keep V2 work");
        assert.equal((yield* sql`SELECT title FROM projection_threads`)[0]?.title, "V1 thread");
      }).pipe(Effect.provide(databaseLayer));
    }).pipe(
      Effect.provide(
        ServerConfig.layerTest(directory, directory).pipe(Layer.provideMerge(NodeServices.layer)),
      ),
      Effect.ensuring(
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      ),
    );
  },
);

it.effect("includes committed WAL data and does not publish a failed snapshot", () => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-v2-snapshot-"));
  const sourcePath = NodePath.join(directory, "state.sqlite");
  const destinationPath = NodePath.join(directory, "statev2.sqlite");
  return Effect.gen(function* () {
    NodeFS.writeFileSync(sourcePath, "invalid SQLite");
    assert.isTrue((yield* Effect.result(initializeV2Database(destinationPath)))._tag === "Failure");
    assert.isFalse(NodeFS.existsSync(destinationPath));
    assert.deepEqual(NodeFS.readdirSync(directory), ["state.sqlite"]);
    NodeFS.unlinkSync(sourcePath);
    const source = new NodeSqlite.DatabaseSync(sourcePath);
    try {
      source.exec(
        "PRAGMA journal_mode=WAL; CREATE TABLE messages(text TEXT); INSERT INTO messages VALUES ('committed'); BEGIN; INSERT INTO messages VALUES ('uncommitted');",
      );
      yield* initializeV2Database(destinationPath);
      assert.isFalse(NodeFS.readdirSync(directory).some((name) => name.startsWith(".v2-import-")));
      const copy = new NodeSqlite.DatabaseSync(destinationPath, { readOnly: true });
      try {
        assert.deepEqual(
          copy
            .prepare("SELECT text FROM messages")
            .all()
            .map((row) => row.text),
          ["committed"],
        );
      } finally {
        copy.close();
      }
      source.exec("ROLLBACK");
    } finally {
      source.close();
    }
  }).pipe(
    Effect.provide(NodeServices.layer),
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true }))),
  );
});

it.effect("uses statev2.sqlite for default and explicit development paths", () =>
  Effect.gen(function* () {
    for (const devUrl of [undefined, new URL("http://localhost:5173")]) {
      for (const baseDirIsExplicit of [false, true]) {
        const paths = yield* ServerConfig.deriveServerPaths("/tmp/t3", devUrl, {
          baseDirIsExplicit,
        });
        assert.equal(NodePath.basename(paths.dbPath), "statev2.sqlite");
        assert.equal(paths.settingsPath, NodePath.join(paths.stateDir, "settings.json"));
      }
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("starts fresh without V1 and never imports over existing V2 state", () => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-v2-fresh-"));
  const destinationPath = NodePath.join(directory, "userdata", "statev2.sqlite");
  return Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const database = SqlitePersistence.layerConfig.pipe(
      Layer.provide(ServerConfig.layer({ ...config, dbPath: destinationPath })),
    );
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE v2_work (text TEXT)`;
      yield* sql`INSERT INTO v2_work VALUES ('fresh V2 work')`;
    }).pipe(Effect.provide(database));
    const sourcePath = NodePath.join(NodePath.dirname(destinationPath), "state.sqlite");
    assert.isFalse(NodeFS.existsSync(sourcePath));
    NodeFS.writeFileSync(sourcePath, "This source must never be opened once V2 exists");
    yield* initializeV2Database(destinationPath);
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.equal((yield* sql`SELECT text FROM v2_work`)[0]?.text, "fresh V2 work");
    }).pipe(Effect.provide(database));
  }).pipe(
    Effect.provide(
      ServerConfig.layerTest(directory, directory).pipe(Layer.provideMerge(NodeServices.layer)),
    ),
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true }))),
  );
});

const withSnapshotFixture = <A, E, R>(
  use: (fixture: {
    readonly directory: string;
    readonly sourcePath: string;
    readonly destinationPath: string;
  }) => Effect.Effect<A, E, R>,
) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-v2-snapshot-lifecycle-"));
  return Effect.suspend(() =>
    use({
      directory,
      sourcePath: NodePath.join(directory, "state.sqlite"),
      destinationPath: NodePath.join(directory, "statev2.sqlite"),
    }),
  ).pipe(
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true }))),
  );
};

const seedSnapshotSource = (sourcePath: string) => {
  const source = new NodeSqlite.DatabaseSync(sourcePath);
  try {
    source.exec(
      "CREATE TABLE snapshot_probe(value TEXT); INSERT INTO snapshot_probe VALUES ('source');",
    );
  } finally {
    source.close();
  }
};

it.effect("does not create a source, destination or temporary directory when V1 is absent", () =>
  withSnapshotFixture(({ directory, sourcePath, destinationPath }) =>
    Effect.gen(function* () {
      yield* initializeV2Database(destinationPath);
      assert.isFalse(NodeFS.existsSync(sourcePath));
      assert.isFalse(NodeFS.existsSync(destinationPath));
      assert.deepEqual(NodeFS.readdirSync(directory), []);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("keeps the winner of a publication race and cleans the losing complete snapshot", () =>
  withSnapshotFixture(({ directory, sourcePath, destinationPath }) =>
    Effect.gen(function* () {
      seedSnapshotSource(sourcePath);
      const original = NodeFS.readFileSync(sourcePath);
      const fs = yield* FileSystem.FileSystem;
      const racingFileSystem = FileSystem.FileSystem.of({
        ...fs,
        link: Effect.fn(function* (snapshotPath, requestedDestination) {
          const copy = new NodeSqlite.DatabaseSync(snapshotPath, { readOnly: true });
          try {
            assert.equal(copy.prepare("SELECT value FROM snapshot_probe").get()?.value, "source");
          } finally {
            copy.close();
          }
          NodeFS.writeFileSync(requestedDestination, "existing winner");
          yield* fs.link(snapshotPath, requestedDestination);
        }),
      });
      yield* initializeV2Database(destinationPath).pipe(
        Effect.provideService(FileSystem.FileSystem, racingFileSystem),
      );
      assert.equal(NodeFS.readFileSync(destinationPath, "utf8"), "existing winner");
      assert.deepEqual(NodeFS.readFileSync(sourcePath), original);
      assert.deepEqual(NodeFS.readdirSync(directory).sort(), ["state.sqlite", "statev2.sqlite"]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("publication failure preserves V1 and removes every task-owned snapshot", () =>
  withSnapshotFixture(({ directory, sourcePath, destinationPath }) =>
    Effect.gen(function* () {
      seedSnapshotSource(sourcePath);
      const original = NodeFS.readFileSync(sourcePath);
      const fs = yield* FileSystem.FileSystem;
      const failingFileSystem = FileSystem.FileSystem.of({
        ...fs,
        link: (snapshotPath) =>
          fs.link(snapshotPath, NodePath.join(directory, "absent", "statev2.sqlite")),
      });
      const result = yield* Effect.result(
        initializeV2Database(destinationPath).pipe(
          Effect.provideService(FileSystem.FileSystem, failingFileSystem),
        ),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure._tag, "V2DatabaseImportError");
        assert.equal(result.failure.sourcePath, sourcePath);
        assert.equal(result.failure.destinationPath, destinationPath);
      }
      assert.isFalse(NodeFS.existsSync(destinationPath));
      assert.deepEqual(NodeFS.readFileSync(sourcePath), original);
      assert.deepEqual(NodeFS.readdirSync(directory), ["state.sqlite"]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("cancellation before publication closes the snapshot scope without publishing", () =>
  withSnapshotFixture(({ directory, sourcePath, destinationPath }) =>
    Effect.gen(function* () {
      seedSnapshotSource(sourcePath);
      const original = NodeFS.readFileSync(sourcePath);
      const fs = yield* FileSystem.FileSystem;
      const publicationReached = yield* Deferred.make<string>();
      const blockedFileSystem = FileSystem.FileSystem.of({
        ...fs,
        link: Effect.fn(function* (requestedSnapshotPath) {
          yield* Deferred.succeed(publicationReached, requestedSnapshotPath);
          return yield* Effect.never;
        }),
      });
      const fiber = yield* initializeV2Database(destinationPath).pipe(
        Effect.provideService(FileSystem.FileSystem, blockedFileSystem),
        Effect.forkChild,
      );
      const snapshotPath = yield* Deferred.await(publicationReached);
      assert.isTrue(NodeFS.existsSync(snapshotPath));
      yield* Fiber.interrupt(fiber);
      assert.isFalse(NodeFS.existsSync(snapshotPath));
      assert.isFalse(NodeFS.existsSync(destinationPath));
      assert.deepEqual(NodeFS.readFileSync(sourcePath), original);
      assert.deepEqual(NodeFS.readdirSync(directory), ["state.sqlite"]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "concurrent initializers publish one complete snapshot and clean their separate roots",
  () =>
    withSnapshotFixture(({ directory, sourcePath, destinationPath }) =>
      Effect.gen(function* () {
        seedSnapshotSource(sourcePath);
        const original = NodeFS.readFileSync(sourcePath);
        yield* Effect.all(
          [initializeV2Database(destinationPath), initializeV2Database(destinationPath)],
          { concurrency: 2 },
        );
        const copy = new NodeSqlite.DatabaseSync(destinationPath, { readOnly: true });
        try {
          assert.deepEqual(copy.prepare("SELECT value FROM snapshot_probe").all(), [
            { value: "source" },
          ]);
        } finally {
          copy.close();
        }
        assert.deepEqual(NodeFS.readFileSync(sourcePath), original);
        assert.deepEqual(NodeFS.readdirSync(directory).sort(), ["state.sqlite", "statev2.sqlite"]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "cancellation during backup waits for SQLite to finish before closing and cleaning its scope",
  () =>
    withSnapshotFixture(({ directory, sourcePath, destinationPath }) =>
      Effect.gen(function* () {
        seedSnapshotSource(sourcePath);
        const original = NodeFS.readFileSync(sourcePath);
        const backupReached = yield* Deferred.make<{
          readonly snapshotPath: string;
          readonly database: NodeSqlite.DatabaseSync;
        }>();
        let releaseBackup: () => void = () => {};
        const backupPermission = new Promise<void>((resolve) => {
          releaseBackup = resolve;
        });
        const actualBackup = NodeSqlite.backup;
        vi.spyOn(NodeSqlite, "backup").mockImplementationOnce(
          async (database, snapshotPath, options) => {
            Deferred.doneUnsafe(
              backupReached,
              Effect.succeed({ snapshotPath: String(snapshotPath), database }),
            );
            await backupPermission;
            return actualBackup(database, snapshotPath, options);
          },
        );
        const fiber = yield* initializeV2Database(destinationPath).pipe(Effect.forkChild);
        const backup = yield* Deferred.await(backupReached);
        try {
          fiber.interruptUnsafe();
          yield* Effect.yieldNow;
          assert.isTrue(backup.database.isOpen);
          assert.isTrue(NodeFS.existsSync(NodePath.dirname(backup.snapshotPath)));
        } finally {
          releaseBackup();
        }
        assert.isTrue(Exit.isFailure(yield* Fiber.await(fiber)));
        assert.isFalse(backup.database.isOpen);
        assert.isFalse(NodeFS.existsSync(destinationPath));
        assert.deepEqual(NodeFS.readFileSync(sourcePath), original);
        assert.deepEqual(NodeFS.readdirSync(directory), ["state.sqlite"]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
