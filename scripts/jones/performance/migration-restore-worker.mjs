import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";

import {
  seedQualificationFixture,
  withQualificationFixture,
} from "./fixtures-historical-worker.mjs";
import {
  assertOwnedDatabase,
  createOwnedRoot,
  disposeOwnedRoot,
  observeSyntheticClose,
} from "./guard.mjs";
import {
  assertCurrentDatabaseSource,
  assertQualificationDatabaseSource,
  qualificationDatabaseSource,
} from "./sources.mjs";
import {
  foreignV2Names,
  historicalQualificationRequirements,
  migrationRestoreSchema,
  nativeBackupSchema,
  originalJonesNames,
  validateMigrationRestoreRequest,
} from "./migration-restore.mjs";
import { readRuntimeBinding } from "./runtime-binding.mjs";

const receivingRoot = NodePath.resolve(import.meta.dirname, "../../..");
const lookupIndex = "orchestration_events_v2_created_threads_idx";
const hash = (value) => NodeCrypto.createHash("sha256").update(JSON.stringify(value)).digest("hex");
const quote = (name) => `"${name.replaceAll('"', '""')}"`;
const canonical = (value) => {
  if (typeof value === "bigint") return ["bigint", String(value)];
  if (value instanceof Uint8Array) return ["bytes", Buffer.from(value).toString("hex")];
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonical(value[key])]),
    );
  return value;
};
const failure = (error) => ({
  code: error.code ?? "qualification_failed",
  message: String(error.message ?? error).slice(0, 512),
});

function retainedQualificationCleanup(owner, reason = "unknown_resource_or_child_close") {
  return {
    schema: "jones-performance-cleanup/v1",
    creationReceipt: owner.creationReceipt,
    outcome: "retained",
    absent: false,
    childReceipts: [],
    reason,
  };
}

function checkSource(source, current) {
  return current ? assertCurrentDatabaseSource(source) : assertQualificationDatabaseSource(source);
}

async function loadModules(source, current) {
  checkSource(source, current);
  const require = NodeModule.createRequire(
    NodePath.join(source.worktreePath, "apps/server/package.json"),
  );
  const dependencies = {
    Effect: "effect/Effect",
    Schema: "effect/Schema",
    Layer: "effect/Layer",
    ManagedRuntime: "effect/ManagedRuntime",
    Logger: "effect/Logger",
    SqlClient: "effect/unstable/sql/SqlClient",
    NodeServices: "@effect/platform-node/NodeServices",
  };
  const files = {
    Sqlite: "apps/server/src/persistence/Layers/Sqlite.ts",
    Client: "packages/shared/src/nodeSqliteClient.ts",
  };
  if (current)
    Object.assign(files, {
      Init: "apps/server/src/persistence/initializeV2Database.ts",
      EventSink: "apps/server/src/orchestration-v2/EventSink.ts",
      EventStore: "apps/server/src/orchestration-v2/EventStore.ts",
      ProjectionStore: "apps/server/src/orchestration-v2/ProjectionStore.ts",
      Importer: "apps/server/src/orchestration-v2/legacy/LegacyV1ThreadImporter.ts",
      Contracts: "packages/contracts/src/index.ts",
      Guard: "apps/server/src/jones/persistence/JonesMigrationGuard.ts",
      Birth: "apps/server/src/jones/importedHistory/ApplicationBirth.ts",
    });
  return Object.fromEntries(
    await Promise.all([
      ...Object.entries(dependencies).map(async ([key, specifier]) => [
        key,
        await import(NodeURL.pathToFileURL(require.resolve(specifier)).href),
      ]),
      ...Object.entries(files).map(async ([key, relative]) => [
        key,
        await import(NodeURL.pathToFileURL(NodePath.join(source.worktreePath, relative)).href),
      ]),
    ]),
  );
}

async function capture(query) {
  const definitions = (
    await query(
      "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name",
    )
  ).map(canonical);
  const tables = {};
  let bytes = 0;
  for (const { name } of definitions.filter(({ type }) => type === "table")) {
    const count = Number((await query(`SELECT count(*) AS count FROM ${quote(name)}`))[0].count);
    NodeAssert.ok(count <= 4096, "qualification capture exceeds bounded row count");
    const rows = (await query(`SELECT * FROM ${quote(name)}`))
      .map((row) => JSON.stringify(canonical(row)))
      .sort();
    bytes += rows.reduce((size, row) => size + Buffer.byteLength(row), 0);
    NodeAssert.ok(bytes <= 16 * 1024 * 1024, "qualification capture exceeds bounded content bytes");
    tables[name] = { count, sha256: hash(rows) };
  }
  const ledgers = {};
  for (const table of ["effect_sql_migrations", "jones_sql_migrations"])
    ledgers[table] = tables[table]
      ? (await query(`SELECT migration_id AS id,name FROM ${table} ORDER BY migration_id`)).map(
          ({ id, name }) => ({ id: Number(id), name: String(name) }),
        )
      : [];
  const integrity = (await query("PRAGMA integrity_check")).map((row) =>
    String(Object.values(row)[0]),
  );
  const foreignKeys = await query("PRAGMA foreign_key_check");
  NodeAssert.deepEqual(integrity, ["ok"]);
  NodeAssert.deepEqual(foreignKeys, []);
  const runtime = (
    await query("SELECT sqlite_version() AS sqliteVersion,sqlite_source_id() AS sqliteSourceId")
  )[0];
  const pragmas = {};
  for (const name of [
    "journal_mode",
    "synchronous",
    "foreign_keys",
    "busy_timeout",
    "journal_size_limit",
  ])
    pragmas[name] = Object.values((await query(`PRAGMA ${name}`))[0])[0];
  return {
    content: { tables, ledgers, definitions },
    integrity,
    foreignKeys,
    runtime: {
      nodeVersion: process.versions.node,
      executable: process.execPath,
      ...runtime,
      pragmas,
    },
  };
}

function closedLayout(path) {
  const fd = NodeFS.openSync(path, "r");
  const header = Buffer.alloc(100);
  let bytesRead;
  try {
    bytesRead = NodeFS.readSync(fd, header, 0, header.length, 0);
  } finally {
    NodeFS.closeSync(fd);
  }
  NodeAssert.equal(bytesRead, 100);
  NodeAssert.equal(header.toString("ascii", 0, 16), "SQLite format 3\0");
  return {
    header: { bytesRead, writeVersion: header[18], readVersion: header[19] },
    sidecars: Object.fromEntries(
      ["-wal", "-shm", "-journal"].map((suffix) => [suffix, NodeFS.existsSync(`${path}${suffix}`)]),
    ),
    healthQualification: "unqualified-readonly-health",
  };
}

async function nativePhase(record, binding, use) {
  const permit = assertOwnedDatabase(record.owner, {
    databaseRelativePath: record.relativePath,
    access: "readwrite",
  });
  let database;
  try {
    database = new NodeSqlite.DatabaseSync(permit.canonicalPath, {
      readOnly: true,
      allowExtension: false,
    });
    record.closeKnown = false;
    return await use(database);
  } finally {
    if (database) {
      await observeSyntheticClose(record.owner, {
        permit,
        producerStep: binding.taskRef,
        resource: database,
        close: (resource) => resource.close(),
      });
      record.closeKnown = true;
    }
  }
}

async function runtimePhase(record, source, current, binding, signal, mode, use) {
  checkSource(source, current);
  const modules = await loadModules(source, current);
  const permit = assertOwnedDatabase(record.owner, {
    databaseRelativePath: record.relativePath,
    access: "readwrite",
  });
  let runtime;
  const pending = new Set();
  const cancellation = new AbortController();
  const combined = signal ? AbortSignal.any([signal, cancellation.signal]) : cancellation.signal;
  let open = true;
  try {
    const { Effect, Layer } = modules;
    let database =
      mode === "client"
        ? modules.Client.layer({ filename: permit.canonicalPath })
        : modules.Sqlite.makeSqlitePersistenceLive(permit.canonicalPath);
    database = database.pipe(Layer.provide(modules.NodeServices.layer));
    let layer = database;
    if (mode === "import") {
      const stores = Layer.merge(modules.EventStore.layer, modules.ProjectionStore.layer).pipe(
        Layer.provideMerge(database),
      );
      const sink = modules.EventSink.layer.pipe(Layer.provideMerge(stores));
      layer = modules.Importer.layer.pipe(Layer.provideMerge(sink));
    }
    runtime = modules.ManagedRuntime.make(layer.pipe(Layer.provide(modules.Logger.layer([]))));
    record.closeKnown = false;
    const run = (effect) => {
      if (!open) throw new Error("qualification runtime is closed");
      const promise = runtime.runPromise(effect, { signal: combined });
      pending.add(promise);
      void promise.then(
        () => pending.delete(promise),
        () => pending.delete(promise),
      );
      return promise;
    };
    const query = (text, values = []) =>
      run(
        Effect.gen(function* () {
          const sql = yield* modules.SqlClient.SqlClient;
          return yield* sql.unsafe(text, values);
        }),
      );
    if (current && mode !== "client")
      await run(
        modules.Init.initializeV2Database(permit.canonicalPath).pipe(
          Effect.provide(modules.NodeServices.layer),
        ),
      );
    await run(Effect.service(modules.SqlClient.SqlClient));
    const value = await use({ modules, run, query });
    return { value, capture: await capture(query) };
  } finally {
    open = false;
    cancellation.abort();
    await Promise.allSettled([...pending]);
    if (runtime) {
      await observeSyntheticClose(record.owner, {
        permit,
        producerStep: binding.taskRef,
        resource: runtime,
        close: (resource) => resource.dispose(),
      });
      record.closeKnown = true;
    }
  }
}

async function nativeBackup(source, destination, binding, signal) {
  const permit = assertOwnedDatabase(destination.owner, {
    databaseRelativePath: destination.relativePath,
    access: "create",
  });
  destination.path = permit.canonicalPath;
  const result = { schema: nativeBackupSchema, backupCompleted: false, sourceClosed: false };
  let backupStarted = false;
  destination.closeKnown = false;
  try {
    await nativePhase(source, binding, async (database) => {
      result.capture = await capture((text, values = []) => database.prepare(text).all(...values));
      signal?.throwIfAborted();
      // Native backup has no cancellation API. Await its settlement before releasing either owned root.
      backupStarted = true;
      await NodeSqlite.backup(database, destination.path);
      result.backupCompleted = true;
    });
    result.sourceClosed = source.closeKnown;
    const operation = { result };
    await observeSyntheticClose(destination.owner, {
      permit,
      producerStep: binding.taskRef,
      resource: operation,
      close: () => {
        NodeAssert.equal(result.backupCompleted, true);
        NodeAssert.equal(result.sourceClosed, true);
      },
    });
    destination.closeKnown = true;
    result.originalClosedOutput = closedLayout(destination.path);
    result.runtime = result.capture.runtime;
    signal?.throwIfAborted();
    const restored = await nativePhase(destination, binding, (database) =>
      capture((text, values = []) => database.prepare(text).all(...values)),
    );
    NodeAssert.deepEqual(
      restored.content,
      result.capture.content,
      "native backup restored canonical content differs",
    );
    result.sourceCaptureSha256 = hash(result.capture.content);
    result.restoredContentSha256 = hash(restored.content);
    result.closureBasis = "awaited-native-backup-completion-and-observed-source-close";
    return result;
  } catch (error) {
    if (!backupStarted) {
      const operation = { backupStarted };
      await observeSyntheticClose(destination.owner, {
        permit,
        producerStep: binding.taskRef,
        resource: operation,
        close: () => NodeAssert.equal(operation.backupStarted, false),
      });
      destination.closeKnown = true;
    }
    error.backupEvidence = { ...result, failure: failure(error) };
    throw error;
  }
}

export function receivingCreationLookupProgram({ modules, queryEffect }) {
  const { Effect } = modules;
  return Effect.gen(function* () {
    const threadId = modules.Contracts.ThreadId.make("qualification-native-birth");
    const payload = JSON.stringify({
      id: threadId,
      projectId: "qualification-project",
      createdAt: "2026-10-07T00:00:00.000Z",
      deletedAt: null,
    });
    const lookup =
      "SELECT event_id, sequence, payload_json FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ? AND event_type = 'thread.created' ORDER BY sequence DESC LIMIT 1";
    const indexedLookup = lookup.replace(
      "FROM orchestration_events",
      `FROM orchestration_events INDEXED BY ${lookupIndex}`,
    );
    const definitions = yield* queryEffect(
      "SELECT sql FROM sqlite_schema WHERE type='index' AND name=?",
      [lookupIndex],
    );
    NodeAssert.equal(definitions.length, 1, "receiving migration055 lookup index missing");
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* queryEffect("BEGIN");
        return yield* restore(
          Effect.gen(function* () {
            yield* queryEffect(
              "INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES(?,?,'Synthetic','codex','full-access','default',?,?,?)",
              [
                threadId,
                "qualification-project",
                "2026-10-07T00:00:00.000Z",
                "2026-10-07T00:00:00.000Z",
                payload,
              ],
            );
            for (const [eventId, version, streamVersion] of [
              ["qualification-current-birth", 2, 1],
              ["qualification-foreign-birth", 1, 2],
            ])
              yield* queryEffect(
                "INSERT INTO orchestration_events(event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,actor_kind,payload_json,metadata_json,application_event_version) VALUES(?,'thread',?,?,'thread.created',?,'system',?,'{}',?)",
                [eventId, threadId, streamVersion, "2026-10-07T00:00:00.000Z", payload, version],
              );
            const birth = yield* modules.Birth.readApplicationBirthRecord(threadId);
            NodeAssert.equal(birth?.eventId, "qualification-current-birth");
            const plan = yield* queryEffect(`EXPLAIN QUERY PLAN ${indexedLookup}`, [threadId]);
            NodeAssert.ok(plan.some((row) => String(row.detail).includes(lookupIndex)));
            NodeAssert.deepEqual(
              yield* queryEffect(indexedLookup, [threadId]),
              yield* queryEffect(lookup, [threadId]),
            );
            yield* queryEffect(`DROP INDEX ${lookupIndex}`);
            yield* queryEffect(indexedLookup, [threadId]).pipe(
              Effect.match({
                onFailure: (error) => {
                  NodeAssert.equal(error?._tag, "SqlError");
                  NodeAssert.equal(error.reason?._tag, "UnknownError");
                  NodeAssert.equal(error.reason?.operation, "execute");
                  NodeAssert.equal(error.reason?.cause?.code, "ERR_SQLITE_ERROR");
                  NodeAssert.equal(error.reason?.cause?.message, `no such index: ${lookupIndex}`);
                },
                onSuccess: () => NodeAssert.fail("lookup succeeded after its index was removed"),
              }),
            );
            yield* queryEffect(definitions[0].sql);
            yield* queryEffect(
              "UPDATE orchestration_v2_projection_threads SET payload_json=? WHERE thread_id=?",
              [
                JSON.stringify({
                  id: threadId,
                  projectId: "replacement-project",
                  createdAt: "2026-10-07T00:00:00.000Z",
                }),
                threadId,
              ],
            );
            NodeAssert.equal(yield* modules.Birth.readApplicationBirthRecord(threadId), null);
            return {
              owner: "055_OrchestrationV2/RecoveryIndexes",
              index: lookupIndex,
              plan,
              actualLookupExecuted: true,
              foreignBirthExcluded: true,
              changedProjectionRejected: true,
              missingIndexRejected: true,
            };
          }),
        ).pipe(Effect.ensuring(queryEffect("ROLLBACK").pipe(Effect.orDie)));
      }),
    );
  });
}

export async function probeReceivingCreationLookup({ modules, run, query }) {
  return run(
    receivingCreationLookupProgram({
      modules,
      queryEffect: (text, values = []) =>
        modules.Effect.tryPromise({
          try: () => query(text, values),
          catch: (cause) => cause,
        }),
    }),
  );
}

function receivingJonesManifest(candidate) {
  const path = NodePath.join(
    candidate.worktreePath,
    "apps/server/src/jones/persistence/JonesMigrations.ts",
  );
  const source = NodeFS.readFileSync(path, "utf8");
  const imports = new Map(
    [...source.matchAll(/import\s+(JonesMigration\d+)\s+from\s+"(\.\/Migrations\/[^"\n]+)"/g)].map(
      (match) => [match[1], match[2]],
    ),
  );
  const entries = [
    ...source.matchAll(/\[\s*(\d+)\s*,\s*"([A-Za-z0-9]+)"\s*,\s*(JonesMigration\d+)\s*\]/g),
  ].map((match) => ({ id: Number(match[1]), name: match[2], relative: imports.get(match[3]) }));
  NodeAssert.deepEqual(
    entries.filter(({ id }) => id <= 6).map(({ id, name }) => ({ id, name })),
    originalJonesNames.map((name, index) => ({ id: index + 1, name })),
  );
  NodeAssert.ok(
    entries.every(({ relative }) => typeof relative === "string"),
    "receiving Jones manifest has an unresolved import",
  );
  return entries.map((entry) => ({
    ...entry,
    path: NodePath.resolve(NodePath.dirname(path), entry.relative),
  }));
}

async function importLegacy(scope) {
  const { modules, run, query } = scope;
  const before = await query("SELECT thread_id FROM projection_threads ORDER BY thread_id");
  const legacyMessages = await query(
    "SELECT thread_id,message_id,role,text FROM projection_thread_messages ORDER BY thread_id,message_id",
  );
  const ledgerEvents = await query(
    "SELECT event_id,sequence FROM orchestration_events ORDER BY sequence",
  );
  const receipts = await query(
    "SELECT command_id FROM orchestration_command_receipts ORDER BY command_id",
  );
  const importer = await run(modules.Effect.service(modules.Importer.LegacyV1ThreadImporter));
  await run(importer.reconcileShells);
  for (const { thread_id } of before)
    await run(importer.ensureTranscript(modules.Contracts.ThreadId.make(thread_id)));
  const projections = await run(modules.Effect.service(modules.ProjectionStore.ProjectionStoreV2));
  for (const { thread_id } of before) {
    const projection = await run(projections.getThread(modules.Contracts.ThreadId.make(thread_id)));
    NodeAssert.equal(projection.id, thread_id);
  }
  const importedMessages = await query(
    "SELECT thread_id,message_id,role,json_extract(payload_json,'$.text') AS text FROM orchestration_v2_projection_messages ORDER BY thread_id,message_id",
  );
  for (const message of legacyMessages)
    NodeAssert.ok(
      importedMessages.some(
        (row) =>
          row.thread_id === message.thread_id &&
          row.message_id === message.message_id &&
          row.role === message.role &&
          row.text === message.text,
      ),
      "V1 message was not preserved through import",
    );
  NodeAssert.deepEqual(
    await query("SELECT event_id,sequence FROM orchestration_events ORDER BY sequence"),
    ledgerEvents,
    "import redispatched V1 events",
  );
  NodeAssert.deepEqual(
    await query("SELECT command_id FROM orchestration_command_receipts ORDER BY command_id"),
    receipts,
    "import redispatched V1 receipts",
  );
  return {
    legacyThreadCount: before.length,
    legacyMessageCount: legacyMessages.length,
    redispatchedV1: false,
  };
}

function assertJonesLedger(captured, requirement) {
  const originals = originalJonesNames.map((name, index) => ({ id: index + 1, name }));
  const foreign =
    requirement?.foreign === "lookup007"
      ? [{ id: 7, name: "ThreadCreationLookupIndex" }]
      : foreignV2Names
          .slice(0, requirement?.foreignPrefix ?? 0)
          .map((name, index) => ({ id: index + 7, name }));
  const ledger = captured.content.ledgers.jones_sql_migrations;
  NodeAssert.deepEqual(
    ledger.filter(({ id }) => id < 100),
    [...originals, ...foreign],
  );
  return foreign;
}

async function rollbackProbe(scope, candidate) {
  const { modules, run, query } = scope;
  const originals = await Promise.all(
    receivingJonesManifest(candidate).map(async ({ id, name, path }) => {
      NodeAssert.ok(id < 10000, "rollback probe IDs collide with the receiving manifest");
      const migration = await import(NodeURL.pathToFileURL(path).href);
      return [id, name, migration.default];
    }),
  );
  const before = await capture(query);
  const facts = { ddlObserved: false, ledgerObserved: false, failureInjected: false };
  let inject = true;
  const { Effect, SqlClient } = modules;
  const entries = [
    ...originals,
    [
      10000,
      "QualificationRollbackProbe",
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`CREATE TABLE jones_qualification_rollback_probe(value TEXT NOT NULL)`;
      }),
    ],
    [
      10001,
      "QualificationInjectedFailure",
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        if (inject) {
          facts.ddlObserved =
            (yield* sql`SELECT name FROM sqlite_schema WHERE name='jones_qualification_rollback_probe'`)
              .length === 1;
          facts.ledgerObserved =
            (yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id=10000`)
              .length === 1;
          facts.failureInjected = true;
          return yield* Effect.fail(new Error("declared Jones qualification rollback fault"));
        }
      }),
    ],
  ];
  await NodeAssert.rejects(run(modules.Guard.runJonesMigrations(entries)));
  NodeAssert.deepEqual(facts, { ddlObserved: true, ledgerObserved: true, failureInjected: true });
  NodeAssert.deepEqual(
    (await capture(query)).content,
    before.content,
    "injected Jones failure did not roll back DDL and ledger",
  );
  inject = false;
  await run(modules.Guard.runJonesMigrations(entries));
  NodeAssert.equal(
    (
      await query(
        "SELECT migration_id FROM jones_sql_migrations WHERE migration_id IN (10000,10001)",
      )
    ).length,
    2,
  );
  await run(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`DROP TABLE jones_qualification_rollback_probe`;
          yield* sql`DELETE FROM jones_sql_migrations WHERE migration_id IN (10000,10001)`;
        }),
      );
    }),
  );
  NodeAssert.deepEqual((await capture(query)).content, before.content);
  return {
    ...facts,
    retrySucceeded: true,
    probeRemoved: true,
    injectionSurface: "receiving-runJonesMigrations-transaction",
  };
}

export async function runBoundMigrationRestoreCase(request, input) {
  const { specification, candidate, historical } = validateMigrationRestoreRequest(
    request?.specification?.id,
    input,
  );
  const timeoutMs = input.timeoutMs ?? 120000;
  const deadline = AbortSignal.timeout(timeoutMs);
  input = { ...input, signal: input.signal ? AbortSignal.any([input.signal, deadline]) : deadline };
  NodeAssert.equal(
    candidate.worktreePath,
    NodeFS.realpathSync(receivingRoot),
    "worker must be in the bound receiving candidate",
  );
  checkSource(candidate, true);
  const runtime = readRuntimeBinding();
  NodeAssert.equal(process.execPath, runtime.executablePath);
  NodeAssert.equal(process.versions.node, runtime.nodeVersion);
  input.signal?.throwIfAborted();
  const filesystem = NodeFS.statfsSync(input.parentPath);
  NodeAssert.ok(
    ![0x01021994, 0x858458f6].includes(filesystem.type),
    "qualification scratch must be disk-backed",
  );
  const evidence = {
    schema: migrationRestoreSchema,
    caseId: specification.id,
    candidate,
    runtime,
    deadline: { timeoutMs, nativeBackupDrain: "await-native-settlement-before-cleanup" },
    sourceIdentity: [],
    phases: [],
    backups: [],
    cleanups: [],
    runnerClosed: false,
  };
  const caseOwner = createOwnedRoot(input);
  evidence.creationReceipt = caseOwner.creationReceipt;
  const caseRoot = caseOwner.creationReceipt.canonicalRootPath;
  const records = [];
  let unresolvedSeed = false;
  let retainReason;
  let primary;
  const allocate = (childName, relativePath) => {
    const record = {
      owner: createOwnedRoot({ ...input, parentPath: caseRoot, childName }),
      relativePath,
      closeKnown: true,
    };
    records.push(record);
    return record;
  };
  const phase = async (name, record, source, current, mode, use = async () => undefined) => {
    const facts = { name, mode, databaseSource: source, closeKnown: false };
    evidence.phases.push(facts);
    try {
      const result = await runtimePhase(
        record,
        source,
        current,
        input.binding,
        input.signal,
        mode,
        use,
      );
      Object.assign(facts, result, { closeKnown: record.closeKnown });
      return result;
    } catch (error) {
      Object.assign(facts, { failure: failure(error), closeKnown: record.closeKnown });
      throw error;
    }
  };
  try {
    if (specification.kind === "rollback") {
      const { produceCurrentFixture } =
        await import("../../../apps/server/scripts/jones/currentFixtures.ts");
      unresolvedSeed = true;
      const produced = await produceCurrentFixture(
        {
          ...input,
          parentPath: caseRoot,
          childName: "current",
          producer: "current-v2",
          databaseSource: candidate,
        },
        (context) => context.paths,
      );
      unresolvedSeed = false;
      retainReason = produced.retainReason;
      const record = {
        owner: produced.owner,
        relativePath: "statev2.sqlite",
        path: produced.value?.dbPath,
        closeKnown: produced.closeKnown,
      };
      records.push(record);
      evidence.phases.push({
        name: "current-v2-seed",
        capture: produced.capture,
        closeKnown: produced.closeKnown,
      });
      if (produced.error) throw produced.error;
      evidence.rollback = (
        await phase(
          "injected-Jones-transaction-and-retry",
          record,
          candidate,
          true,
          "client",
          (scope) => rollbackProbe(scope, candidate),
        )
      ).value;
      await phase("candidate-reopen", record, candidate, true, "migration");
    } else {
      const requirement = historicalQualificationRequirements.find(
        ({ directory }) => directory === specification.seed,
      );
      const base =
        specification.kind === "foreign" || specification.kind === "successor"
          ? qualificationDatabaseSource("c4c68bb0b33eafb72545e6e23b0b7258e49bd613")
          : historical;
      checkSource(base, false);
      unresolvedSeed = true;
      const seeded = await seedQualificationFixture({
        ...input,
        parentPath: caseRoot,
        childName: "seed",
        databaseSource: base,
        recipe: { kind: "coherent-v1", historyTurns: 3, payloadBytes: 256 },
      });
      unresolvedSeed = false;
      retainReason = seeded.retainReason;
      const root = seeded.owner.creationReceipt.canonicalRootPath;
      const seed = {
        owner: seeded.owner,
        relativePath: NodePath.relative(
          root,
          seeded.value?.paths.dbPath ?? NodePath.join(root, "state.sqlite"),
        ),
        path: seeded.value?.paths.dbPath,
        closeKnown: seeded.closeKnown,
      };
      records.push(seed);
      evidence.sourceIdentity.push(base);
      evidence.phases.push({
        name: "historical-seed",
        databaseSource: base,
        capture: seeded.capture,
        closeKnown: seeded.closeKnown,
      });
      if (seeded.error) throw seeded.error;
      if (
        specification.id === "open-414" ||
        specification.id === "restore-pre" ||
        specification.id === "restore-post"
      ) {
        const old = await withQualificationFixture(
          seeded.value,
          { databaseSource: base, mode: "engine", signal: input.signal },
          async ({ context, modules }) => {
            const command = modules.Schema.decodeUnknownSync(
              modules.Contracts.OrchestrationCommand,
            )({
              type: "thread.activity.append",
              commandId: "qualification-old-write",
              threadId: "fixture-leased",
              activity: {
                id: "qualification-old-note",
                kind: "fixture.note",
                summary: "Synthetic earlier 414 write",
                tone: "info",
                turnId: null,
                payload: { synthetic: true },
                createdAt: "2026-10-02T12:30:00.000Z",
              },
              createdAt: "2026-10-02T12:30:00.000Z",
            });
            await context.run(context.engine.dispatch(command));
            return { commandId: command.commandId };
          },
        );
        seed.closeKnown = old.closeKnown;
        evidence.phases.push({
          name: "earlier-414-supported-write",
          value: old.value,
          closeKnown: old.closeKnown,
          ...(old.error ? { failure: failure(old.error) } : {}),
        });
        if (old.error) throw old.error;
      }
      if (specification.kind === "foreign" || specification.kind === "successor") {
        evidence.sourceIdentity.push(historical);
        const applied = await phase(
          "exact-historical-foreign-loader",
          seed,
          historical,
          false,
          "migration",
        );
        assertJonesLedger(applied.capture, requirement);
      }
      const receiving = allocate("receiving", "statev2.sqlite");
      evidence.backups.push(await nativeBackup(seed, receiving, input.binding, input.signal));
      evidence.initializationCopyBasis =
        "native-backup-to-registered-destination; initializer absent-destination copy covered separately by initializeV2Database.test.ts";
      if (specification.kind === "foreign") {
        evidence.foreignGuard = (
          await phase(
            "receiving-guard-preserves-foreign-content",
            receiving,
            candidate,
            true,
            "client",
            async ({ modules, run, query }) => {
              const before = await capture(query);
              const entries = await Promise.all(
                receivingJonesManifest(candidate).map(async ({ id, name, path }) => [
                  id,
                  name,
                  (await import(NodeURL.pathToFileURL(path).href)).default,
                ]),
              );
              await run(modules.Guard.runJonesMigrations(entries));
              const after = await capture(query);
              for (const [table, summary] of Object.entries(before.content.tables)) {
                if (table !== "jones_sql_migrations")
                  NodeAssert.deepEqual(
                    after.content.tables[table],
                    summary,
                    `foreign guard changed ${table}`,
                  );
              }
              for (const definition of before.content.definitions)
                NodeAssert.ok(
                  after.content.definitions.some(
                    (item) => JSON.stringify(item) === JSON.stringify(definition),
                  ),
                  "foreign guard changed a recorded schema definition",
                );
              NodeAssert.deepEqual(
                after.content.ledgers.jones_sql_migrations.filter(({ id }) => id < 100),
                before.content.ledgers.jones_sql_migrations.filter(({ id }) => id < 100),
              );
              return { unchangedForeignContent: true, adoptedForeignFeatures: false };
            },
          )
        ).value;
      }
      let destination = receiving;
      if (specification.id === "restore-pre") {
        destination = allocate("restored-pre", "statev2.sqlite");
        evidence.backups.push(
          await nativeBackup(receiving, destination, input.binding, input.signal),
        );
      }
      const opened = await phase(
        "receiving-loader-and-V1-import",
        destination,
        candidate,
        true,
        "import",
        importLegacy,
      );
      const foreign = assertJonesLedger(opened.capture, requirement);
      if (foreign.length) evidence.foreignHistory = { recognized: foreign, featureAdopted: false };
      const repeated = await phase(
        "receiving-rerun-idempotent",
        destination,
        candidate,
        true,
        "import",
        importLegacy,
      );
      NodeAssert.deepEqual(
        repeated.capture.content,
        opened.capture.content,
        "receiving reopen was not idempotent",
      );
      if (specification.id === "restore-post") {
        const restored = allocate("restored-post", "statev2.sqlite");
        evidence.backups.push(
          await nativeBackup(destination, restored, input.binding, input.signal),
        );
        const reopened = await phase(
          "post-restore-receiving-open",
          restored,
          candidate,
          true,
          "import",
          importLegacy,
        );
        NodeAssert.deepEqual(reopened.capture.content, repeated.capture.content);
      }
      if (specification.kind === "foreign" || specification.kind === "successor") {
        evidence.foreignWire = (
          await phase(
            "receiving-V2-codec-refuses-foreign-command-wire",
            destination,
            candidate,
            true,
            "client",
            async ({ modules, query }) => {
              const before = await capture(query);
              NodeAssert.throws(() =>
                modules.Schema.decodeUnknownSync(modules.Contracts.OrchestrationV2Command)({
                  type: "thread.activity.append",
                  commandId: "qualification-foreign-wire",
                  threadId: "fixture-leased",
                  activity: {
                    id: "qualification-foreign-activity",
                    kind: "fixture.note",
                    summary: "Synthetic foreign wire",
                    tone: "info",
                    turnId: null,
                    payload: { synthetic: true },
                    createdAt: "2026-10-02T12:30:00.000Z",
                  },
                  createdAt: "2026-10-02T12:30:00.000Z",
                }),
              );
              NodeAssert.deepEqual((await capture(query)).content, before.content);
              return {
                receivingCodec: "OrchestrationV2Command",
                rejected: true,
                unchanged: true,
                dispatched: false,
              };
            },
          )
        ).value;
      }
      if (specification.kind === "foreign") {
        const variants =
          requirement.foreign === "lookup007"
            ? ["unknown-ID7-name", "missing-original-prefix"]
            : ["missing-foreign-prefix"];
        for (const variant of variants) {
          await phase(
            `foreign-negative-history-${variant}`,
            destination,
            candidate,
            true,
            "client",
            async ({ query }) => {
              if (variant === "unknown-ID7-name")
                await query(
                  "UPDATE jones_sql_migrations SET name='UnknownForeign' WHERE migration_id=7",
                );
              else if (variant === "missing-original-prefix") {
                await query(
                  "UPDATE jones_sql_migrations SET name='ThreadCreationLookupIndex' WHERE migration_id=7",
                );
                await query("DELETE FROM jones_sql_migrations WHERE migration_id=3");
              } else await query("DELETE FROM jones_sql_migrations WHERE migration_id=7");
            },
          );
          const before = await nativePhase(destination, input.binding, (database) =>
            capture((text, values = []) => database.prepare(text).all(...values)),
          );
          let rejected;
          try {
            await phase(
              `receiving-badHistory-refusal-${variant}`,
              destination,
              candidate,
              true,
              "migration",
            );
          } catch (error) {
            rejected = error;
          }
          NodeAssert.ok(rejected, "invalid foreign history was accepted");
          NodeAssert.match(String(rejected.message), /Jones migration history/);
          const after = await nativePhase(destination, input.binding, (database) =>
            capture((text, values = []) => database.prepare(text).all(...values)),
          );
          NodeAssert.deepEqual(
            after.content,
            before.content,
            "badHistory refusal mutated schema or ledger",
          );
        }
        evidence.badHistory = { rejected: true, unchanged: true, variants };
      }
      if (specification.kind === "successor") {
        evidence.successor = (
          await phase(
            "receiving-055-application-birth-lookup",
            destination,
            candidate,
            true,
            "client",
            probeReceivingCreationLookup,
          )
        ).value;
      }
    }
    checkSource(candidate, true);
  } catch (error) {
    if (error.backupEvidence) evidence.backups.push(error.backupEvidence);
    primary = error;
  } finally {
    evidence.runnerClosed = !unresolvedSeed && records.every(({ closeKnown }) => closeKnown);
    if (!evidence.runnerClosed || retainReason)
      evidence.cleanup = retainedQualificationCleanup(caseOwner, retainReason);
    else {
      for (const record of [...records].reverse()) {
        const cleanup = disposeOwnedRoot(record.owner);
        evidence.cleanups.push(cleanup);
        if (cleanup.outcome !== "complete") break;
      }
      evidence.cleanup =
        evidence.cleanups.length === records.length &&
        evidence.cleanups.every(({ outcome }) => outcome === "complete")
          ? disposeOwnedRoot(caseOwner)
          : retainedQualificationCleanup(caseOwner, "nested_cleanup_unproved");
    }
    if (evidence.cleanup.outcome !== "complete")
      primary ??= new Error("qualification root retained because closure or cleanup is unproved");
  }
  if (primary) {
    primary.qualificationEvidence = evidence;
    throw primary;
  }
  return { ...evidence, status: "complete" };
}
