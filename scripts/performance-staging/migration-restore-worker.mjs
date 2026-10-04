import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";

import { seedQualificationFixture, withQualificationFixture } from "./fixtures-worker.mjs";
import {
  assertOwnedDatabase,
  createOwnedRoot,
  disposeOwnedRoot,
  observeSyntheticClose,
} from "./guard.mjs";
import { runOwnedChild } from "./lifecycle.mjs";
import { assertQualificationDatabaseSource, qualificationDatabaseSource } from "./sources.mjs";

const directory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const repository = "Jones-Systems/Jones-Code";
const source = (sourceRevision, forkCount) =>
  Object.freeze({ ...qualificationDatabaseSource(sourceRevision), forkCount });
const sources = Object.freeze({
  e5: source("e5a31aceec91484b64315c63dcce80f6e7581604", 4),
  live: source("414bb8da204c3275cd0b76b2ec4d74dfb09a97e4", 2),
  six: source("c4c68bb0b33eafb72545e6e23b0b7258e49bd613", 6),
  candidate: source("da5f4aee0035beec471b38598eaa2857d1e5155c", 7),
});
const forkNames = [
  "WorktreeOwnershipLeases",
  "ProjectionThreadRuntimeIdentity",
  "NativeCreationIntents",
  "NativeCreationCommandIdentities",
  "WorkstreamsNativeAttempts",
  "WorkstreamsProviderEnrollments",
  "ThreadCreationLookupIndex",
];
const historyDifferences = [
  "apps/mobile/src/lib/threadActivity.bench.ts",
  "apps/mobile/src/lib/threadActivity.test.ts",
  "apps/server/src/orchestration/Layers/ProjectionSnapshotQuery.history.bench.ts",
  "packages/client-runtime/src/remotePerformance.bench.ts",
  "packages/client-runtime/src/state/threadReducer.test.ts",
  "tests/check-groups.json",
];
const indexName = "idx_orch_events_thread_creation_lookup";
const indexDDL = `CREATE INDEX IF NOT EXISTS ${indexName}
  ON orchestration_events (stream_id, sequence DESC, event_id)
  WHERE aggregate_kind = 'thread' AND event_type = 'thread.created'`;
const normalizeSQL = (text) => text.trim().replace(/;$/, "").replace(/\s+/g, " ");
const sha256 = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const digest = (value) => sha256(JSON.stringify(value));
const contentOf = (capture) => ({ content: capture.content, definitions: capture.definitions });
const taskRef = "spec.jones-performance-portfolio#task.e-migrate.001";

export const qualificationCases = Object.freeze(
  [
    {
      id: "upgrade-e5",
      title: "e5 fork1–4 upgrades through real A007 and reruns idempotently",
      seed: "e5",
    },
    {
      id: "upgrade-414",
      title: "actual414 fork1–2 upgrades through real A007 and reruns idempotently",
      seed: "live",
    },
    {
      id: "upgrade-six",
      title: "c4 fork1–6 migration-only adds only ledger007 and index007",
      seed: "six",
    },
    {
      id: "rollback-seven",
      title: "fresh-six post-DDL007 failure rolls back and real retry succeeds",
      seed: "six",
    },
    {
      id: "old-write-rollforward",
      title: "actual414 writes after A007 preserve replay and fencing on roll-forward",
      seed: "live",
    },
    {
      id: "restore-414",
      title: "pre-upgrade414 native backup restores files and supports414 writes then A007",
      seed: "live",
    },
    {
      id: "restore-seven",
      title: "A007 native backup restores files and supports414 writes then A007",
      seed: "live",
    },
  ].map(Object.freeze),
);

export function pinnedHead(root) {
  NodeAssert.equal(NodeFS.realpathSync(root), root);
  const metadataPath = NodePath.join(root, ".git");
  const metadata = NodeFS.lstatSync(metadataPath);
  NodeAssert.equal(metadata.isSymbolicLink(), false);
  let gitDirectory = metadataPath;
  let common = metadataPath;
  if (!metadata.isDirectory()) {
    NodeAssert.ok(metadata.isFile());
    const gitFile = NodeFS.readFileSync(metadataPath, "utf8").trim();
    NodeAssert.match(gitFile, /^gitdir: /);
    gitDirectory = NodePath.resolve(root, gitFile.slice(8));
    common = NodePath.resolve(
      gitDirectory,
      NodeFS.readFileSync(NodePath.join(gitDirectory, "commondir"), "utf8").trim(),
    );
  }
  const checkedHead = (value) => {
    NodeAssert.match(value, /^[a-f0-9]{40}$/);
    return value;
  };
  const head = NodeFS.readFileSync(NodePath.join(gitDirectory, "HEAD"), "utf8").trim();
  if (/^[a-f0-9]{40}$/.test(head)) return checkedHead(head);
  NodeAssert.match(head, /^ref: refs\/heads\/[a-zA-Z0-9_./-]+$/);
  const ref = head.slice(5);
  for (const base of [gitDirectory, common]) {
    try {
      return checkedHead(NodeFS.readFileSync(NodePath.join(base, ref), "utf8").trim());
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
  const packed = NodeFS.readFileSync(NodePath.join(common, "packed-refs"), "utf8");
  NodeAssert.ok(Buffer.byteLength(packed) <= 2 * 1024 * 1024);
  const match = packed.split("\n").find((line) => line.endsWith(` ${ref}`));
  NodeAssert.ok(match, "pinned source ref is missing");
  return checkedHead(match.split(" ")[0]);
}

function sourceTreeHash(root, relatives) {
  const entries = [];
  const visit = (relative) => {
    const path = NodePath.join(root, relative);
    const info = NodeFS.lstatSync(path);
    NodeAssert.equal(info.isSymbolicLink(), false, "source capture refuses symlinks");
    if (info.isDirectory()) {
      for (const name of NodeFS.readdirSync(path).sort()) visit(NodePath.join(relative, name));
    } else if (info.isFile() && !/\.(?:test|bench)\.[cm]?[jt]sx?$/.test(relative)) {
      entries.push([relative, sha256(NodeFS.readFileSync(path))]);
    }
  };
  relatives.forEach(visit);
  return { fileCount: entries.length, sha256: digest(entries) };
}

function packageIdentity(entry, snapshots) {
  let parent = NodePath.dirname(entry);
  while (true) {
    const path = NodePath.join(parent, "package.json");
    if (NodeFS.existsSync(path)) {
      const bytes = NodeFS.readFileSync(path);
      const json = JSON.parse(bytes);
      if (typeof json.name === "string" && typeof json.version === "string") {
        if (!snapshots.has(path))
          snapshots.set(path, {
            path,
            name: json.name,
            version: json.version,
            sha256: sha256(bytes),
            implementation: sourceTreeHash(parent, ["dist"]),
          });
        return snapshots.get(path);
      }
    }
    const next = NodePath.dirname(parent);
    NodeAssert.notEqual(next, parent, "resolved dependency has no package identity");
    parent = next;
  }
}

function bindSource(binding) {
  assertQualificationDatabaseSource({
    repository: binding.repository,
    sourceRevision: binding.sourceRevision,
    worktreePath: binding.worktreePath,
  });
  NodeAssert.equal(pinnedHead(binding.worktreePath), binding.sourceRevision, "source HEAD drifted");
  const migrationsPath = NodePath.join(
    binding.worktreePath,
    "apps/server/src/persistence/Migrations.ts",
  );
  const migrations = NodeFS.readFileSync(migrationsPath, "utf8");
  const upstream = [
    ...migrations.matchAll(/\[\s*(\d+)\s*,\s*"([a-zA-Z0-9]+)"\s*,\s*Migration\d+/g),
  ].map((match) => ({ id: Number(match[1]), name: match[2] }));
  const fork = [...migrations.matchAll(/"(\d+)_([a-zA-Z0-9]+)"\s*:\s*JonesMigration/g)].map(
    (match) => ({ id: Number(match[1]), name: match[2] }),
  );
  NodeAssert.equal(upstream.length, 54);
  NodeAssert.deepEqual(
    fork,
    forkNames.slice(0, binding.forkCount).map((name, index) => ({ id: index + 1, name })),
  );
  const require = NodeModule.createRequire(
    NodePath.join(binding.worktreePath, "apps/server/package.json"),
  );
  const packageSnapshots = new Map();
  const dependencies = [
    "effect/Effect",
    "effect/Layer",
    "effect/ManagedRuntime",
    "effect/Schema",
    "effect/unstable/sql/SqlClient",
    "@effect/platform-node/NodeServices",
  ].map((specifier) => {
    const path = NodeFS.realpathSync(require.resolve(specifier));
    return {
      specifier,
      path,
      sha256: sha256(NodeFS.readFileSync(path)),
      package: packageIdentity(path, packageSnapshots),
    };
  });
  const configuration = [
    "package.json",
    "pnpm-workspace.yaml",
    "pnpm-lock.yaml",
    "apps/server/package.json",
    "apps/server/tsconfig.json",
    "packages/contracts/package.json",
    "packages/shared/package.json",
  ].map((relativePath) => ({
    relativePath,
    sha256: sha256(NodeFS.readFileSync(NodePath.join(binding.worktreePath, relativePath))),
  }));
  const production = sourceTreeHash(binding.worktreePath, [
    "apps/server/src",
    "packages/contracts/src",
    "packages/shared/src",
    "patches",
  ]);
  const conformance =
    "packages/contracts/contracts/workstreams-t3-provider/v1/fixtures/conformance.json";
  configuration.push({
    relativePath: conformance,
    ...(NodeFS.existsSync(NodePath.join(binding.worktreePath, conformance))
      ? { sha256: sha256(NodeFS.readFileSync(NodePath.join(binding.worktreePath, conformance))) }
      : { status: "absent" }),
  });
  NodeAssert.equal(pinnedHead(binding.worktreePath), binding.sourceRevision);
  return {
    ...binding,
    owningPackage: NodePath.join(binding.worktreePath, "apps/server"),
    migrationManifest: { upstream, fork, sha256: sha256(migrations) },
    production,
    configuration,
    dependencies,
    ...(binding === sources.six
      ? {
          productionEquivalentAnchor: "8de693b103e6fb69e7a8b07775a9dbbab8d0f149",
          equivalenceBasis:
            "root-verified production graph, manifests, workspace, patches and lock; execution is c4",
          excludedHistoryDifferences: historyDifferences,
        }
      : {}),
  };
}

function assertCapture(capture) {
  NodeAssert.deepEqual(capture.integrity, ["ok"]);
  NodeAssert.deepEqual(capture.foreignKeys, []);
  NodeAssert.match(capture.runtime.sqliteVersion, /^\d+\.\d+\.\d+/);
  NodeAssert.ok(capture.runtime.sqliteSourceId.length > 40);
  NodeAssert.equal(capture.runtime.executable, process.execPath);
  if (capture.application) {
    NodeAssert.equal(capture.application.readModel.equivalent, true);
    NodeAssert.equal(capture.application.pages.overlap, 0);
    NodeAssert.equal(capture.application.integrity.ok, true);
    NodeAssert.equal(capture.application.foreignKeys.violations, 0);
    NodeAssert.equal(capture.application.files.length >= 6, true);
  }
}

function assertManifest(capture, identity) {
  NodeAssert.deepEqual(
    capture.content.ledgers.effect_sql_migrations,
    identity.migrationManifest.upstream,
  );
  NodeAssert.deepEqual(
    capture.content.ledgers.jones_sql_migrations,
    identity.migrationManifest.fork,
  );
}

function assertUpgrade(before, migrated, identity) {
  assertManifest(migrated, identity);
  for (const [name, summary] of Object.entries(before.content.tables)) {
    if (name === "jones_sql_migrations" || summary.status === "absent") continue;
    NodeAssert.deepEqual(
      migrated.content.tables[name],
      summary,
      `${name} changed during migration-only open`,
    );
  }
  const definition = migrated.definitions.find((item) => item.name === indexName);
  NodeAssert.ok(definition, "actual007 index is absent");
  NodeAssert.equal(definition.type, "index");
  NodeAssert.equal(definition.table, "orchestration_events");
  NodeAssert.equal(
    normalizeSQL(definition.sql).replace("CREATE INDEX IF NOT EXISTS ", "CREATE INDEX "),
    normalizeSQL(indexDDL).replace("CREATE INDEX IF NOT EXISTS ", "CREATE INDEX "),
  );
  for (const table of [
    "workstreams_native_attempts",
    "workstreams_native_enrollments",
    "auth_sessions",
  ])
    NodeAssert.equal(migrated.content.tables[table].status, "present");
}

async function importSourceFile(binding, relative) {
  NodeAssert.equal(pinnedHead(binding.worktreePath), binding.sourceRevision);
  return import(NodeURL.pathToFileURL(NodePath.join(binding.worktreePath, relative)).href);
}

async function exerciseWorkstreams(scope, binding, prior) {
  const { context, modules } = scope;
  const [Attempts, Enrollments, AuthSessions, FixtureData, EnrollmentData] = await Promise.all([
    importSourceFile(binding, "apps/server/src/workstreams/nativeProvider/attemptRepository.ts"),
    importSourceFile(binding, "apps/server/src/workstreams/enrollment/service.ts"),
    importSourceFile(binding, "apps/server/src/persistence/AuthSessions.ts"),
    importSourceFile(binding, "apps/server/src/workstreams/nativeProvider/testFixtures.ts"),
    importSourceFile(binding, "apps/server/src/workstreams/enrollment/testFixtures.ts"),
  ]);
  const { Effect, Option } = modules;
  const attempts = await context.run(Attempts.makeNativeProviderAttempts);
  const at = "2026-10-02T12:20:00.000Z";
  const attempt = {
    request: FixtureData.request,
    requestBytesSha256: FixtureData.requestBytesSha256,
    enrollmentSha256: "e".repeat(64),
    enrollment: FixtureData.binding,
    nativeCommandId: "workstreams:qualification-command",
    createdAt: at,
    dispatchStartedAt: null,
  };
  const before = await context.run(attempts.get(attempt.request));
  NodeAssert.equal(Option.isSome(before), Boolean(prior));
  const first = await context.run(attempts.reserve(attempt));
  const duplicate = await context.run(
    attempts.reserve({ ...attempt, requestBytesSha256: "f".repeat(64) }),
  );
  NodeAssert.deepEqual(duplicate, first);
  const started = await context.run(attempts.startDispatch(attempt.request, at));
  NodeAssert.equal(started, !prior);
  NodeAssert.equal(await context.run(attempts.startDispatch(attempt.request, at)), false);
  const persisted = await context.run(attempts.get(attempt.request));
  NodeAssert.equal(Option.isSome(persisted), true);
  NodeAssert.equal(persisted.value.dispatchStartedAt, at);
  const enrollmentRequest = prior?.enrollmentRequest ?? {
    ...EnrollmentData.enrollmentRequest,
    session: {
      ...EnrollmentData.enrollmentRequest.session,
      issued_at: new Date(Date.now() - 60_000).toISOString(),
      expires_at: new Date(Date.now() + 24 * 60 * 60_000).toISOString(),
    },
  };
  const sessions = await context.run(AuthSessions.make);
  const enrollment = await context.run(
    Enrollments.makeNativeEnrollments({
      authority: {
        readCurrent: Effect.succeed({
          environmentId: enrollmentRequest.context.source_instance_id,
          authorityNamespace: enrollmentRequest.context.authority_namespace,
          storeGeneration: enrollmentRequest.context.store_generation,
        }),
      },
      build: Effect.succeed(Option.some(enrollmentRequest.context.build)),
    }).pipe(Effect.provideService(AuthSessions.AuthSessionRepository, sessions)),
  );
  NodeAssert.deepEqual(await context.run(enrollment.inspect(enrollmentRequest)), {
    state: prior ? "reserved" : "absent",
  });
  NodeAssert.deepEqual(await context.run(enrollment.reserve(enrollmentRequest)), {
    state: "reserved",
  });
  NodeAssert.deepEqual(await context.run(enrollment.reserve(enrollmentRequest)), {
    state: "reserved",
  });
  NodeAssert.equal(
    Option.isSome(
      await context.run(enrollment.getBySessionId(enrollmentRequest.session.session_id)),
    ),
    true,
  );
  NodeAssert.equal(
    Option.isSome(
      await context.run(sessions.getById({ sessionId: enrollmentRequest.session.session_id })),
    ),
    true,
  );
  const evidence = { attemptSha256: digest(persisted.value), enrollmentRequest };
  if (prior) NodeAssert.equal(evidence.attemptSha256, prior.attemptSha256);
  return evidence;
}

async function oldSupportedWrites(scope) {
  const { context, modules, query } = scope;
  const decode = modules.Schema.decodeUnknownSync(modules.Contracts.OrchestrationCommand);
  const at = "2026-10-02T12:30:00.000Z";
  const creation = decode({
    type: "thread.create",
    commandId: "qualification-old-create",
    threadId: "qualification-old-created",
    projectId: "fixture-project",
    title: "Synthetic old-source thread",
    modelSelection: { instanceId: "codex", model: "synthetic-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt: at,
  });
  const creationReceipt = await context.run(context.engine.dispatch(creation));
  const afterCreation = await query("SELECT count(*) AS n FROM orchestration_events");
  NodeAssert.deepEqual(await context.run(context.engine.dispatch(creation)), creationReceipt);
  NodeAssert.deepEqual(
    await query("SELECT count(*) AS n FROM orchestration_events"),
    afterCreation,
  );
  const command = decode({
    type: "thread.activity.append",
    commandId: "qualification-old-write",
    threadId: creation.threadId,
    activity: {
      id: "qualification-old-note",
      kind: "fixture.note",
      summary: "Synthetic old-source write",
      tone: "info",
      turnId: null,
      payload: { synthetic: true },
      createdAt: at,
    },
    createdAt: at,
  });
  const accepted = await context.run(context.engine.dispatch(command));
  const events = await query("SELECT count(*) AS n FROM orchestration_events");
  NodeAssert.deepEqual(await context.run(context.engine.dispatch(command)), accepted);
  NodeAssert.deepEqual(await query("SELECT count(*) AS n FROM orchestration_events"), events);
  const leases = await context.run(modules.Leases.makeWorktreeOwnershipLeaseStore());
  const createdIncarnation = await context.run(leases.getThreadIncarnation(creation.threadId));
  NodeAssert.equal(modules.Option.isSome(createdIncarnation), true);
  NodeAssert.equal(
    createdIncarnation.value,
    (
      await query(
        "SELECT event_id FROM orchestration_events WHERE stream_id=? AND event_type='thread.created' ORDER BY sequence DESC LIMIT 1",
        [creation.threadId],
      )
    )[0].event_id,
  );
  const incarnation = await context.run(leases.getThreadIncarnation("fixture-leased"));
  NodeAssert.equal(modules.Option.isSome(incarnation), true);
  const event = (
    await query(
      "SELECT event_id FROM orchestration_events WHERE stream_id=? AND event_type='thread.created' ORDER BY sequence DESC LIMIT 1",
      ["fixture-leased"],
    )
  )[0];
  NodeAssert.equal(incarnation.value, event.event_id);
  const currentLease = (await context.run(leases.listAll())).find(
    (lease) => lease.leaseId === "fixture-lease-current",
  );
  NodeAssert.ok(currentLease);
  const nowMs = Date.parse(at);
  const first = await context.run(
    leases.acquire({
      ...currentLease,
      leaseId: "qualification-old-generation",
      nowMs,
      expiresAtMs: nowMs + 300_000,
    }),
  );
  NodeAssert.equal(modules.Option.isSome(first), true);
  const rotated = await context.run(
    leases.acquire({ ...currentLease, nowMs: nowMs + 1, expiresAtMs: nowMs + 300_001 }),
  );
  NodeAssert.equal(modules.Option.isSome(rotated), true);
  NodeAssert.equal(
    await context.run(
      leases.renew({ ...first.value, nowMs: nowMs + 2, expiresAtMs: nowMs + 300_002 }),
    ),
    false,
  );
  await context.run(leases.release(first.value));
  NodeAssert.deepEqual(
    (await context.run(leases.listAll())).find(
      (lease) => lease.resourcePath === currentLease.resourcePath,
    ),
    rotated.value,
  );
  NodeAssert.equal(
    await context.run(
      leases.renew({
        ...rotated.value,
        ownerIncarnation: "synthetic-stale-incarnation",
        nowMs: nowMs + 3,
        expiresAtMs: nowMs + 300_003,
      }),
    ),
    false,
  );
  NodeAssert.equal(
    modules.Option.isNone(
      await context.run(
        leases.acquire({
          ...rotated.value,
          ownerThreadId: creation.threadId,
          leaseId: "qualification-foreign",
          nowMs,
          expiresAtMs: nowMs + 300_000,
        }),
      ),
    ),
    true,
  );
  return {
    commandId: command.commandId,
    creationCommandId: creation.commandId,
    createdThreadId: creation.threadId,
    replayAddedEvents: 0,
    incarnation: incarnation.value,
    createdIncarnation: createdIncarnation.value,
    generationFenced: true,
    staleIncarnationRenewed: false,
    foreignAcquired: false,
  };
}

function installSevenFault() {
  const prepare = NodeSqlite.DatabaseSync.prototype.prepare;
  const statements = [];
  const facts = {
    declared: "post-real007-DDL-before-transaction-commit",
    ddlExecuted: false,
    indexObservedInTransaction: false,
    ledgerSevenObservedInTransaction: false,
    calls: 0,
  };
  const injected = new Error("declared synthetic007 fault after actual index DDL");
  injected.code = "SYNTHETIC_007_AFTER_DDL";
  function intercepted(sql) {
    const statement = prepare.call(this, sql);
    if (normalizeSQL(sql) !== normalizeSQL(indexDDL)) return statement;
    const database = this;
    const descriptor = Object.getOwnPropertyDescriptor(statement, "run");
    const run = statement.run;
    statements.push({ statement, descriptor });
    statement.run = function (...args) {
      run.apply(this, args);
      facts.calls += 1;
      facts.ddlExecuted = true;
      facts.indexObservedInTransaction =
        prepare
          .call(database, "SELECT name FROM sqlite_schema WHERE type='index' AND name=?")
          .get(indexName)?.name === indexName;
      facts.ledgerSevenObservedInTransaction =
        prepare.call(database, "SELECT name FROM jones_sql_migrations WHERE migration_id=7").get()
          ?.name === forkNames[6];
      NodeAssert.equal(facts.indexObservedInTransaction, true);
      NodeAssert.equal(facts.ledgerSevenObservedInTransaction, true);
      throw injected;
    };
    return statement;
  }
  NodeSqlite.DatabaseSync.prototype.prepare = intercepted;
  return {
    facts,
    injected,
    remove: () => {
      NodeAssert.strictEqual(
        NodeSqlite.DatabaseSync.prototype.prepare,
        intercepted,
        "SQL interceptor ownership changed",
      );
      NodeSqlite.DatabaseSync.prototype.prepare = prepare;
      for (const { statement, descriptor } of statements) {
        if (descriptor) Object.defineProperty(statement, "run", descriptor);
        else delete statement.run;
      }
    },
  };
}

export function qualificationProducerIdentity() {
  const worktreePath = NodePath.resolve(directory, "../..");
  return {
    repository,
    worktreePath,
    sourceRevision: pinnedHead(worktreePath),
    files: [
      "fixtures-worker.mjs",
      "sources.mjs",
      "migration-restore-worker.mjs",
      "migration-restore.test.mjs",
      "guard.mjs",
      "lifecycle.mjs",
    ].map((name) => ({
      relativePath: `scripts/performance-staging/${name}`,
      sha256: sha256(NodeFS.readFileSync(NodePath.join(directory, name))),
    })),
  };
}

async function closedLayout(path) {
  const handle = await NodeFSP.open(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  let header;
  try {
    const bytes = Buffer.alloc(20);
    NodeAssert.equal((await handle.read(bytes, 0, 20, 0)).bytesRead, 20);
    NodeAssert.equal(bytes.subarray(0, 16).toString("binary"), "SQLite format 3\0");
    header = { writeVersion: bytes[18], readVersion: bytes[19], sha256: sha256(bytes) };
  } finally {
    await handle.close();
  }
  const layout = {};
  for (const suffix of ["", "-wal", "-shm", "-journal"]) {
    try {
      const info = await NodeFSP.lstat(`${path}${suffix}`, { bigint: true });
      NodeAssert.equal(info.isFile(), true);
      NodeAssert.equal(info.isSymbolicLink(), false);
      NodeAssert.equal(info.nlink, 1n);
      layout[suffix || "main"] = {
        present: true,
        size: Number(info.size),
        device: String(info.dev),
        inode: String(info.ino),
      };
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      layout[suffix] = { present: false };
    }
  }
  return { header, layout, healthEligibility: "unqualified-backup-output" };
}

async function copyClosedDatabase(owner, permit, sourcePath, producerStep) {
  let sourceHandle;
  let destinationHandle;
  let failure;
  try {
    sourceHandle = await NodeFSP.open(
      sourcePath,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
    );
    destinationHandle = await NodeFSP.open(
      permit.canonicalPath,
      NodeFS.constants.O_WRONLY | NodeFS.constants.O_NOFOLLOW,
    );
    const buffer = Buffer.alloc(64 * 1024);
    let position = 0;
    while (true) {
      const { bytesRead } = await sourceHandle.read(buffer, 0, buffer.length, position);
      if (!bytesRead) break;
      let written = 0;
      while (written < bytesRead) {
        const result = await destinationHandle.write(
          buffer,
          written,
          bytesRead - written,
          position + written,
        );
        NodeAssert.ok(result.bytesWritten > 0);
        written += result.bytesWritten;
      }
      position += bytesRead;
    }
    await destinationHandle.sync();
  } catch (error) {
    failure = error;
  }
  if (!destinationHandle) {
    try {
      await sourceHandle?.close();
    } catch (error) {
      failure ??= error;
    }
    return {
      closeKnown: false,
      error: failure ?? new Error("copy destination resource was never opened"),
    };
  }
  const resource = { sourceHandle, destinationHandle };
  try {
    const proof = await observeSyntheticClose(owner, {
      permit,
      producerStep,
      resource,
      close: async () => {
        const results = await Promise.allSettled([
          sourceHandle?.close(),
          destinationHandle?.close(),
        ]);
        const failed = results.find((result) => result.status === "rejected");
        if (failed) throw failed.reason;
      },
    });
    return { closeKnown: true, proof, error: failure };
  } catch (error) {
    return { closeKnown: false, error: failure ?? error };
  }
}

// These private cases keep the original owners, permits and complete captures in process.
export async function runMigrationRestoreCase(caseId, input) {
  NodeAssert.equal(
    process.versions.node.split(".")[0],
    "24",
    "this private seam qualifies actual Node24 only",
  );
  const specification = qualificationCases.find((item) => item.id === caseId);
  NodeAssert.ok(specification, "unknown qualification case");
  const seedSource = sources[specification.seed];
  const identities = [seedSource, sources.candidate];
  if (caseId.startsWith("restore-") || caseId === "old-write-rollforward")
    identities.push(sources.live);
  const bound = new Map([...new Set(identities)].map((binding) => [binding, bindSource(binding)]));
  const producer = qualificationProducerIdentity();
  input = {
    ...input,
    binding: { repository, sourceRevision: producer.sourceRevision, taskRef, runId: input.runId },
  };
  const caseOwner = createOwnedRoot(input);
  const caseRoot = caseOwner.creationReceipt.canonicalRootPath;
  const evidence = {
    schema: "jones-performance-migration-restore/v1",
    caseId,
    producer,
    sourceIdentity: [...bound.values()],
    phases: [],
    api: {},
    backups: [],
    cleanups: [],
    creationReceipt: caseOwner.creationReceipt,
  };
  const owned = new Map();
  const children = new Map();
  const binding = { ...input.binding, taskRef };
  let seedResolved = false;
  let unknownExternalResource = false;
  let retainReason;
  let data;
  let workstreams;
  let primary;

  const phase = async (name, databaseSource, mode, use, expectedFailure = false) => {
    input.signal?.throwIfAborted();
    NodeAssert.equal(pinnedHead(databaseSource.worktreePath), databaseSource.sourceRevision);
    owned.set(data.owner, false);
    const result = await withQualificationFixture(
      data.context,
      {
        databaseSource,
        mode,
        owner: data.owner,
        paths: data.paths,
        referenceRoot: caseRoot,
        files: data.files,
        signal: input.signal,
      },
      async (scope) => {
        const value = use ? await use(scope) : undefined;
        const capture = await scope.capture();
        assertCapture(capture);
        return { value, capture };
      },
    );
    owned.set(data.owner, result.closeKnown);
    if (result.context) data.context = result.context;
    evidence.phases.push({
      name,
      mode,
      databaseSource,
      closeKnown: result.closeKnown,
      ...(result.value ?? {}),
      ...(result.error
        ? { failure: { code: result.error.code ?? "phase_failed", name: result.error.name } }
        : {}),
    });
    if (!result.closeKnown) throw result.error ?? new Error(`${name} resource closure is unknown`);
    if (result.error && !expectedFailure) throw result.error;
    if (expectedFailure)
      NodeAssert.ok(result.error, "declared fault did not fail the actual migrator");
    return result;
  };

  const upgrade = async (before) => {
    const migrated = (
      await phase("candidate-migration-only", sources.candidate, "migration", async (scope) => {
        const columns = await scope.query(`PRAGMA index_xinfo(${indexName})`);
        NodeAssert.deepEqual(
          columns
            .filter((column) => Number(column.key) === 1)
            .map((column) => ({
              name: column.name,
              descending: Number(column.desc),
            })),
          [
            { name: "stream_id", descending: 0 },
            { name: "sequence", descending: 1 },
            { name: "event_id", descending: 0 },
          ],
        );
        const index = (await scope.query("PRAGMA index_list(orchestration_events)")).find(
          (row) => row.name === indexName,
        );
        NodeAssert.equal(Number(index.partial), 1);
        return {
          indexColumns: columns.map((column) => ({
            name: column.name,
            descending: Number(column.desc),
            key: Number(column.key),
          })),
        };
      })
    ).value.capture;
    assertUpgrade(before, migrated, bound.get(sources.candidate));
    if (seedSource === sources.six) {
      NodeAssert.deepEqual(
        migrated.definitions.filter((item) => item.name !== indexName),
        before.definitions,
      );
      for (const [name, summary] of Object.entries(before.content.tables)) {
        if (name !== "jones_sql_migrations")
          NodeAssert.deepEqual(migrated.content.tables[name], summary);
      }
    }
    const repeated = (await phase("candidate-migrator-rerun", sources.candidate, "migration")).value
      .capture;
    NodeAssert.deepEqual(
      contentOf(repeated),
      contentOf(migrated),
      "actual unrestricted migrator was not idempotent",
    );
    const started = (await phase("candidate-engine-bootstrap", sources.candidate, "engine")).value
      .capture;
    assertManifest(started, bound.get(sources.candidate));
    const bootstrap = started.application.coupling.projectionCursors.find(
      (cursor) => cursor.projector === "projection.attachment-cleanup",
    );
    NodeAssert.equal(bootstrap.sequence, started.application.coupling.maxSequence);
    NodeAssert.equal(
      migrated.content.tables.projection_state.count,
      started.content.tables.projection_state.count,
    );
    const native = (
      await phase(
        "candidate-native-and-workstreams",
        sources.candidate,
        "engine",
        async (scope) => {
          const history = await scope.exerciseNative();
          NodeAssert.deepEqual(history.phases, ["started", "completed"]);
          workstreams = await exerciseWorkstreams(scope, sources.candidate);
          return {
            native: history,
            workstreams: {
              attemptSha256: workstreams.attemptSha256,
              enrollmentRequestSha256: digest(workstreams.enrollmentRequest),
              duplicateReserve: "same",
              duplicateDispatch: false,
            },
          };
        },
      )
    ).value;
    evidence.api.native = native.value.native;
    evidence.api.workstreams = native.value.workstreams;
    const persisted = (
      await phase("candidate-repository-reopen", sources.candidate, "engine", async (scope) => {
        const history = await scope.exerciseNative();
        NodeAssert.deepEqual(history, evidence.api.native);
        workstreams = await exerciseWorkstreams(scope, sources.candidate, workstreams);
        return { nativeHistorySha256: history.sha256, attemptSha256: workstreams.attemptSha256 };
      })
    ).value.capture;
    for (const table of ["workstreams_native_attempts", "workstreams_native_enrollments"])
      NodeAssert.equal(persisted.content.tables[table].count, 1);
    NodeAssert.ok(persisted.content.tables.auth_sessions.count >= 1);
    return persisted;
  };

  const writeOldAndRollForward = async () => {
    const beforeOld = (await phase("before-old-source-client", sources.candidate, "client")).value
      .capture;
    const old = (
      await phase("actual414-supported-writes", sources.live, "engine", oldSupportedWrites)
    ).value;
    NodeAssert.deepEqual(
      old.capture.content.ledgers,
      beforeOld.content.ledgers,
      "actual old source changed the candidate ledgers",
    );
    NodeAssert.equal(old.capture.application.native.status, "absent");
    evidence.api.oldSource = old.value;
    const forward = (
      await phase("candidate-roll-forward-migration-only", sources.candidate, "migration")
    ).value.capture;
    assertUpgrade(old.capture, forward, bound.get(sources.candidate));
    if (old.capture.content.ledgers.jones_sql_migrations.length === 7)
      NodeAssert.deepEqual(
        contentOf(forward),
        contentOf(old.capture),
        "roll-forward migrator changed supported old writes",
      );
    const rerun = (
      await phase("candidate-roll-forward-migrator-rerun", sources.candidate, "migration")
    ).value.capture;
    NodeAssert.deepEqual(contentOf(rerun), contentOf(forward));
    const final = (
      await phase("candidate-roll-forward-engine", sources.candidate, "engine", async (scope) => {
        const metadata = await scope.query(
          "SELECT command_id FROM orchestration_command_receipts WHERE command_id=?",
          [old.value.commandId],
        );
        NodeAssert.equal(metadata.length, 1);
        NodeAssert.equal(
          (
            await scope.query("SELECT thread_id FROM projection_threads WHERE thread_id=?", [
              old.value.createdThreadId,
            ])
          ).length,
          1,
        );
        NodeAssert.equal(
          (
            await scope.query(
              "SELECT command_id FROM orchestration_command_receipts WHERE command_id=?",
              [old.value.creationCommandId],
            )
          ).length,
          1,
        );
        const history = await scope.exerciseNative();
        if (evidence.api.native) NodeAssert.deepEqual(history, evidence.api.native);
        if (workstreams)
          workstreams = await exerciseWorkstreams(scope, sources.candidate, workstreams);
        else workstreams = await exerciseWorkstreams(scope, sources.candidate);
        return { historySha256: history.sha256, attemptSha256: workstreams.attemptSha256 };
      })
    ).value.capture;
    assertManifest(final, bound.get(sources.candidate));
    NodeAssert.equal(final.application.leaseFencing.staleRenewed, false);
    NodeAssert.equal(final.application.pages.overlap, 0);
    return final;
  };

  const restore = async (snapshotSource) => {
    retainReason = "backup_or_restore_unqualified";
    const backupOwner = createOwnedRoot({
      ...input,
      parentPath: caseRoot,
      childName: "native-backup",
      binding,
    });
    owned.set(backupOwner, true);
    children.set(backupOwner, []);
    const destinationOwner = createOwnedRoot({
      ...input,
      parentPath: caseRoot,
      childName: "restored",
      binding,
    });
    owned.set(destinationOwner, true);
    const destinationRoot = destinationOwner.creationReceipt.canonicalRootPath;
    const prior = await phase(
      "closed-snapshot-source-capture",
      snapshotSource,
      "client",
      async ({ context, modules }) =>
        context.run(
          modules.Config.deriveServerPaths(destinationRoot, undefined).pipe(
            modules.Effect.provide(modules.NodeServices.layer),
            modules.Effect.provide(modules.Logger.layer([])),
          ),
        ),
    );
    const snapshotCapture = prior.value.capture;
    const restoredPaths = prior.value.value;
    const sourcePath = data.paths.dbPath;
    const originalData = data;
    const permit = assertOwnedDatabase(backupOwner, {
      databaseRelativePath: "snapshot.sqlite",
      access: "create",
    });
    owned.set(backupOwner, false);
    const request = {
      schema: "jones-performance-native-backup/v1",
      caseRoot,
      sourcePath,
      destinationPath: permit.canonicalPath,
    };
    const operation = {
      completion: runOwnedChild({
        owner: backupOwner,
        executable: process.execPath,
        args: [
          NodePath.join(directory, "migration-restore-worker.mjs"),
          "--backup",
          JSON.stringify(request),
        ],
        env: { NODE_NO_WARNINGS: "1", LANG: "C.UTF-8", TZ: "UTC" },
        timeoutMs: 30_000,
        terminateGraceMs: 1000,
        reapTimeoutMs: 2000,
        maxOutputBytes: 64 * 1024,
        signal: input.signal,
      }),
    };
    let child;
    let proof;
    try {
      proof = await observeSyntheticClose(backupOwner, {
        permit,
        producerStep: taskRef,
        resource: operation,
        close: async () => {
          child = await operation.completion;
          children.get(backupOwner).push(child);
          NodeAssert.equal(child.closed, true, "native backup child close is unknown");
          NodeAssert.equal(child.reaped, true, "native backup child reap is unknown");
          NodeAssert.notEqual(child.outcome, "unknown");
        },
      });
      owned.set(backupOwner, true);
      NodeAssert.equal(child.outcome, "success", "native backup leaf failed");
      NodeAssert.equal(child.truncated, false);
      NodeAssert.ok(Buffer.byteLength(child.stdout) <= 49 * 1024);
      NodeAssert.equal(child.stderr, "");
      const completion = JSON.parse(child.stdout);
      NodeAssert.equal(completion.schema, request.schema);
      NodeAssert.equal(completion.backupCompleted, true);
      NodeAssert.equal(completion.sourceClosed, true);
      NodeAssert.equal(completion.runtime.executable, process.execPath);
      NodeAssert.equal(completion.runtime.sqliteVersion, snapshotCapture.runtime.sqliteVersion);
      NodeAssert.equal(completion.runtime.sqliteSourceId, snapshotCapture.runtime.sqliteSourceId);
      unknownExternalResource = true;
      const layout = await closedLayout(permit.canonicalPath);
      unknownExternalResource = false;
      const backupEvidence = {
        snapshotSource,
        sourceCaptureSha256: digest(contentOf(snapshotCapture)),
        completion,
        childReceipt: child,
        originalClosedOutput: layout,
        closureBasis:
          "actual-backup-completion-and-source-close-then-captured-fixed-child-close-reap",
      };
      evidence.backups.push(backupEvidence);
      NodeAssert.ok(proof);
      for (const suffix of ["-wal", "-shm", "-journal"])
        NodeAssert.equal(
          layout.layout[suffix].present,
          false,
          "minimum closed-copy restore requires a standalone closed snapshot",
        );
      await NodeFSP.mkdir(restoredPaths.stateDir, { recursive: true, mode: 0o700 });
      const destinationPermit = assertOwnedDatabase(destinationOwner, {
        databaseRelativePath: NodePath.relative(destinationRoot, restoredPaths.dbPath),
        access: "create",
      });
      owned.set(destinationOwner, false);
      const copied = await copyClosedDatabase(
        destinationOwner,
        destinationPermit,
        permit.canonicalPath,
        taskRef,
      );
      owned.set(destinationOwner, copied.closeKnown);
      if (copied.error) throw copied.error;
      NodeAssert.deepEqual(
        await NodeFSP.readFile(restoredPaths.dbPath),
        await NodeFSP.readFile(permit.canonicalPath),
      );
      const restoredFiles = [];
      for (const file of originalData.files) {
        const relative = NodePath.relative(
          originalData.owner.creationReceipt.canonicalRootPath,
          file,
        );
        NodeAssert.ok(relative && !relative.startsWith("..") && !NodePath.isAbsolute(relative));
        const destination = NodePath.join(destinationRoot, relative);
        await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true, mode: 0o700 });
        await NodeFSP.copyFile(file, destination, NodeFS.constants.COPYFILE_EXCL);
        NodeAssert.equal(
          sha256(await NodeFSP.readFile(destination)),
          sha256(await NodeFSP.readFile(file)),
        );
        restoredFiles.push(destination);
      }
      NodeAssert.equal(restoredFiles.length, 6);
      data = {
        owner: destinationOwner,
        context: originalData.context,
        paths: restoredPaths,
        files: [...originalData.files, ...restoredFiles],
      };
      const restored = (
        await phase("restored-client-content-before-any-migration", snapshotSource, "client")
      ).value.capture;
      NodeAssert.deepEqual(
        contentOf(restored),
        contentOf(snapshotCapture),
        "restored canonical state differs from the closed snapshot",
      );
      backupEvidence.restoredContentSha256 = digest(contentOf(restored));
      backupEvidence.companionFiles = originalData.files.map((file, index) => ({
        originalRelativePath: NodePath.relative(caseRoot, file),
        restoredRelativePath: NodePath.relative(caseRoot, restoredFiles[index]),
        sha256: sha256(NodeFS.readFileSync(file)),
      }));
      backupEvidence.pathPolicy =
        "original synthetic workspace/worktree remain alive; database paths are never rewritten";
      retainReason = undefined;
    } catch (error) {
      if (child)
        evidence.backups.push({
          snapshotSource,
          childReceipt: child,
          failure: { code: error.code ?? "backup_or_restore_failed" },
        });
      throw error;
    }
  };

  try {
    input.signal?.throwIfAborted();
    const seeded = await seedQualificationFixture({
      ...input,
      parentPath: caseRoot,
      childName: "seed",
      binding,
      databaseSource: seedSource,
      recipe: { kind: "coherent-v1", historyTurns: 3, payloadBytes: 256 },
    });
    seedResolved = true;
    owned.set(seeded.owner, seeded.closeKnown);
    evidence.phases.push({
      name: "actual-source-seed-and-production-close",
      mode: "seed",
      databaseSource: seedSource,
      capture: seeded.capture,
      closeKnown: seeded.closeKnown,
      ...(seeded.error ? { failure: { code: seeded.error.code ?? "seed_failed" } } : {}),
    });
    if (seeded.error) throw seeded.error;
    NodeAssert.equal(seeded.closeKnown, true);
    const seedRoot = seeded.owner.creationReceipt.canonicalRootPath;
    data = {
      owner: seeded.owner,
      context: seeded.value,
      paths: seeded.value.paths,
      files: seeded.capture.files.map((file) => NodePath.join(seedRoot, file.relativePath)),
    };
    const before = (await phase("closed-old-source-canonical-state", seedSource, "client")).value
      .capture;
    assertManifest(before, bound.get(seedSource));
    if (caseId === "rollback-seven") {
      const interceptor = installSevenFault();
      let failed;
      try {
        failed = await phase(
          "declared-post-DDL007-fault",
          sources.candidate,
          "migration",
          undefined,
          true,
        );
      } finally {
        interceptor.remove();
      }
      NodeAssert.equal(interceptor.facts.calls, 1);
      NodeAssert.equal(interceptor.facts.ddlExecuted, true);
      NodeAssert.equal(interceptor.facts.indexObservedInTransaction, true);
      NodeAssert.equal(interceptor.facts.ledgerSevenObservedInTransaction, true);
      NodeAssert.ok(failed.error);
      NodeAssert.equal(failed.error.kind, "Failed");
      NodeAssert.equal(failed.error.cause.reason._tag, "UnknownError");
      NodeAssert.strictEqual(failed.error.cause.reason.cause, interceptor.injected);
      evidence.api.fault = {
        ...interceptor.facts,
        failureKind: failed.error.kind,
        failureReason: failed.error.cause.reason._tag,
        removedBeforeRetry: true,
      };
      const rolledBack = (await phase("rollback-client-canonical-state", seedSource, "client"))
        .value.capture;
      NodeAssert.deepEqual(
        contentOf(rolledBack),
        contentOf(before),
        "post-DDL transaction did not roll back schema and full canonical state",
      );
      NodeAssert.equal(
        rolledBack.definitions.some((item) => item.name === indexName),
        false,
      );
      NodeAssert.equal(rolledBack.content.ledgers.jones_sql_migrations.length, 6);
      await upgrade(rolledBack);
    } else if (caseId === "restore-414") {
      await restore(seedSource);
      await writeOldAndRollForward();
    } else {
      await upgrade(before);
      if (caseId === "restore-seven") {
        await restore(sources.candidate);
        await writeOldAndRollForward();
      } else if (caseId === "old-write-rollforward") await writeOldAndRollForward();
    }
  } catch (error) {
    primary = error;
  } finally {
    if (
      retainReason ||
      !seedResolved ||
      unknownExternalResource ||
      [...owned.values()].some((known) => !known) ||
      [...children.values()]
        .flat()
        .some((child) => !child.closed || !child.reaped || child.outcome === "unknown")
    ) {
      evidence.cleanup = {
        outcome: "retained",
        absent: false,
        reason: retainReason ?? "unknown_resource_or_child_close",
        creationReceipt: caseOwner.creationReceipt,
      };
    } else {
      for (const owner of [...owned.keys()].toReversed()) {
        const receipt = disposeOwnedRoot(owner, { childReceipts: children.get(owner) ?? [] });
        evidence.cleanups.push(receipt);
        if (receipt.outcome !== "complete") break;
      }
      if (
        evidence.cleanups.every((receipt) => receipt.outcome === "complete") &&
        evidence.cleanups.length === owned.size
      )
        evidence.cleanup = disposeOwnedRoot(caseOwner);
      else
        evidence.cleanup = {
          outcome: "retained",
          absent: false,
          reason: "nested_cleanup_unproved",
          creationReceipt: caseOwner.creationReceipt,
        };
    }
    if (evidence.cleanup.outcome !== "complete")
      primary ??= new Error(
        "qualification scratch retained because closure or cleanup is unproved",
      );
  }
  if (primary) {
    primary.qualificationEvidence = evidence;
    throw primary;
  }
  return evidence;
}

async function backupLeafMain() {
  NodeAssert.equal(process.argv.length, 4);
  NodeAssert.ok(Buffer.byteLength(process.argv[3]) <= 49 * 1024);
  const request = JSON.parse(process.argv[3]);
  NodeAssert.equal(request.schema, "jones-performance-native-backup/v1");
  NodeAssert.equal(NodeFS.realpathSync(request.caseRoot), request.caseRoot);
  for (const path of [request.sourcePath, request.destinationPath]) {
    const relative = NodePath.relative(request.caseRoot, path);
    NodeAssert.ok(relative && !relative.startsWith("..") && !NodePath.isAbsolute(relative));
    const info = NodeFS.lstatSync(path);
    NodeAssert.equal(info.isSymbolicLink(), false);
    NodeAssert.equal(info.isFile(), true);
    NodeAssert.equal(info.nlink, 1);
  }
  NodeAssert.equal(process.cwd(), NodePath.dirname(request.destinationPath));
  const db = new NodeSqlite.DatabaseSync(request.sourcePath, {
    readOnly: true,
    allowExtension: false,
  });
  const result = {
    schema: request.schema,
    backupCompleted: false,
    sourceClosed: false,
    runtime: {
      nodeVersion: process.versions.node,
      executable: process.execPath,
      ...db
        .prepare("SELECT sqlite_version() AS sqliteVersion,sqlite_source_id() AS sqliteSourceId")
        .get(),
    },
  };
  let failure;
  try {
    await NodeSqlite.backup(db, request.destinationPath);
    result.backupCompleted = true;
  } catch (error) {
    failure = error;
  } finally {
    try {
      db.close();
      result.sourceClosed = true;
    } catch (error) {
      failure ??= error;
    }
  }
  if (failure) {
    result.failure = { code: failure.code ?? "native_backup_failed" };
    process.exitCode = 1;
  }
  const bytes = `${JSON.stringify(result)}\n`;
  NodeAssert.ok(Buffer.byteLength(bytes) <= 49 * 1024);
  process.stdout.write(bytes);
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  NodeAssert.equal(process.argv[2], "--backup");
  await backupLeafMain().catch((error) => {
    process.stderr.write(`${error.code ?? "backup_worker_failed"}\n`);
    process.exitCode = 1;
  });
}
