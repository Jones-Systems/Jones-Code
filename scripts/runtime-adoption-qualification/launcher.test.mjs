import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  Launcher,
  readServiceState,
  writeServiceState,
} from "../../apps/server/src/serviceLauncher.ts";
import {
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_STATE_FILE,
  SERVICE_LAUNCHER_CONTEXT_ENV,
} from "../../apps/server/src/cloud/serviceProtocol.ts";
import {
  initializeNativeStoreAuthority,
  readNativeStoreAuthorityState,
} from "../../apps/server/src/environment/nativeStoreAuthorityPersistence.ts";
import { migrationManifest } from "../../apps/server/src/persistence/Migrations.ts";
import { makePopulatedFixture, readFixture, withDatabase } from "./fixture.mjs";
import { sha256File, withRunScratch } from "./support.mjs";

async function observeChildren() {
  const token = NodeCrypto.randomUUID();
  const receipts = [];
  const pending = new Set();
  const sockets = new Set();
  const closed = new Set();
  let childError;
  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    let buffer = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      buffer += chunk;
      let end;
      while ((end = buffer.indexOf("\n")) !== -1) {
        const value = JSON.parse(buffer.slice(0, end));
        buffer = buffer.slice(end + 1);
        NodeAssert.equal(value.token, token);
        receipts.push(value);
        if (value.type === "child-error") {
          childError = new Error(value.error);
          for (const waiter of pending) waiter.reject(childError);
          pending.clear();
        }
        for (const waiter of pending) {
          if (waiter.type === value.type) {
            pending.delete(waiter);
            waiter.resolve(value);
          }
        }
      }
    });
    const closure = new Promise((resolve) => socket.once("close", resolve));
    closed.add(closure);
    socket.on("error", (error) => {
      for (const waiter of pending) waiter.reject(error);
      pending.clear();
    });
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return {
    port: server.address().port,
    token,
    receipts,
    waitFor(type) {
      if (childError) return Promise.reject(childError);
      const received = receipts.find((value) => value.type === type);
      if (received) return Promise.resolve(received);
      return new Promise((resolve, reject) => pending.add({ type, resolve, reject }));
    },
    async close() {
      for (const socket of sockets) socket.destroy();
      await Promise.all(closed);
      await new Promise((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

// These Node shebang executables qualify launcher/state behavior, not a packaged T3 release.
async function writeSyntheticRuntime(fixture, version, mode, observer) {
  const versionDir = NodePath.join(fixture.baseDir, "runtime", "versions", version);
  await NodeFSP.mkdir(versionDir, { recursive: true });
  const childPath = NodeURL.fileURLToPath(new URL("./runtime-child.mjs", import.meta.url));
  const metadata = { fixture, mode, receiptPort: observer.port, receiptToken: observer.token };
  const environment = {
    HOME: NodePath.join(fixture.baseDir, "child-home"),
    TMPDIR: NodePath.join(fixture.baseDir, "child-temp"),
    PATH: NodePath.dirname(process.execPath),
    T3CODE_HOME: fixture.baseDir,
    T3CODE_NATIVE_AUTHORITY_STATE_DIR: fixture.paths.authorityStateDir,
  };
  await NodeFSP.mkdir(environment.HOME, { recursive: true });
  await NodeFSP.mkdir(environment.TMPDIR, { recursive: true });
  const source = `#!${process.execPath}\nconst context = process.env[${JSON.stringify(SERVICE_LAUNCHER_CONTEXT_ENV)}];\nfor (const key of Object.keys(process.env)) delete process.env[key];\nObject.assign(process.env, ${JSON.stringify(environment)}, { [${JSON.stringify(SERVICE_LAUNCHER_CONTEXT_ENV)}]: context });\nimport(${JSON.stringify(childPath)}).then(({ runRuntimeChild }) => runRuntimeChild(${JSON.stringify(metadata)})).catch(error => { console.error(error); process.exitCode = 1; if (process.connected) process.disconnect(); });\n`;
  await NodeFSP.writeFile(NodePath.join(versionDir, "t3"), source, { mode: 0o755 });
  await NodeFSP.writeFile(NodePath.join(versionDir, ".install-complete"), `${version}\n`);
}

function preserveExistingColumns(before, after) {
  for (const [table, rows] of Object.entries(before.tables)) {
    NodeAssert.equal(after.tables[table].length, rows.length, table);
    NodeAssert.deepEqual(
      after.tables[table].map((row, index) =>
        Object.fromEntries(Object.keys(rows[index]).map((column) => [column, row[column]])),
      ),
      rows,
      table,
    );
  }
  NodeAssert.deepEqual(after.files, before.files);
  NodeAssert.deepEqual(after.references, before.references);
  NodeAssert.equal(after.references.valid, true);
}

function assertCurrentSchema(snapshot) {
  NodeAssert.deepEqual(
    snapshot.ledgers.upstream,
    migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
  );
  NodeAssert.deepEqual(snapshot.ledgers.fork, [
    { migration_id: 1, name: "WorktreeOwnershipLeases" },
    { migration_id: 2, name: "ProjectionThreadRuntimeIdentity" },
    { migration_id: 3, name: "NativeCreationIntents" },
    { migration_id: 4, name: "NativeCreationCommandIdentities" },
    { migration_id: 5, name: "WorkstreamsNativeAttempts" },
    { migration_id: 6, name: "WorkstreamsProviderEnrollments" },
    { migration_id: 7, name: "ThreadCreationLookupIndex" },
  ]);
  for (const row of snapshot.tables.projection_threads)
    NodeAssert.ok(Object.hasOwn(row, "auto_settle_disabled_at"));
}

async function assertNoBackupOrSidecars(fixture, id) {
  for (const target of [
    `${fixture.paths.dbPath}-wal`,
    `${fixture.paths.dbPath}-shm`,
    NodePath.join(fixture.baseDir, "runtime", "db-backup", id),
    NodePath.join(fixture.baseDir, "runtime", "db-backup", `${id}.staging`),
  ])
    await NodeAssert.rejects(NodeFSP.lstat(target), { code: "ENOENT" });
}

async function launcherScenario(mode, effect, setup = async () => {}) {
  return withRunScratch({ label: `launcher-${mode}` }, async ({ root, record }) => {
    const fixture = await makePopulatedFixture({
      baseDir: NodePath.join(root, "home"),
      throughMigration: 53,
      environmentId: `synthetic-launcher-${NodeCrypto.randomUUID()}`,
    });
    const observer = await observeChildren();
    let launcher;
    let completion;
    const ownedEnvironment = {
      T3CODE_NATIVE_AUTHORITY_STATE_DIR: fixture.paths.authorityStateDir,
      NODE_OPTIONS: undefined,
    };
    const originalEnvironment = Object.fromEntries(
      Object.keys(ownedEnvironment).map((key) => [key, process.env[key]]),
    );
    try {
      for (const [key, value] of Object.entries(ownedEnvironment)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
      await writeSyntheticRuntime(fixture, "1.0.0", mode, observer);
      await writeSyntheticRuntime(fixture, "1.1.0", mode, observer);
      const id = NodeCrypto.randomUUID();
      await setup(fixture, id);
      const before = await readFixture(fixture);
      const beforeHash = await sha256File(fixture.paths.dbPath);
      const authority = initializeNativeStoreAuthority(
        fixture.paths.authorityStateDir,
        fixture.expected.environmentId,
      );
      const statePath = NodePath.join(fixture.baseDir, "runtime", SERVICE_STATE_FILE);
      await writeServiceState(statePath, {
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "1.0.0",
        update: {
          id,
          fromVersion: "1.0.0",
          targetVersion: "1.1.0",
          dbPath: fixture.paths.dbPath,
          status: "pending",
          phase: "accepted",
        },
      });
      launcher = new Launcher(fixture.baseDir, await readServiceState(statePath));
      completion = launcher.run().then(
        () => ({ stopped: true }),
        (error) => ({ error }),
      );
      const receipt = (type) =>
        Promise.race([
          observer.waitFor(type),
          completion.then((result) => {
            throw result.error ?? new Error(`launcher stopped before ${type}`);
          }),
        ]);
      await effect({
        fixture,
        observer,
        launcher,
        completion,
        receipt,
        before,
        beforeHash,
        authority,
        id,
        statePath,
      });
      record({
        case: mode,
        packagedRelease: false,
        receipts: observer.receipts.map((value) =>
          Object.fromEntries(Object.entries(value).filter(([key]) => key !== "token")),
        ),
      });
    } finally {
      // Stop owns child exit and launcher completion before the scratch owner's cleanup.
      try {
        if (launcher) await launcher.stop("SIGTERM");
        if (completion) {
          const result = await completion;
          if (result.error) throw result.error;
        }
      } finally {
        await observer.close();
        for (const [key, value] of Object.entries(originalEnvironment)) {
          if (value === undefined) delete process.env[key];
          else process.env[key] = value;
        }
      }
    }
  });
}

async function assertRestored(
  { fixture, receipt, before, beforeHash, authority, id, statePath },
  reason,
) {
  const prior = await receipt("prior-readback");
  NodeAssert.equal(prior.version, "1.0.0");
  NodeAssert.deepEqual(prior.snapshot, before);
  NodeAssert.equal(
    prior.schema.some((column) => column.name === "auto_settle_disabled_at"),
    false,
  );
  NodeAssert.equal(
    await sha256File(fixture.paths.dbPath),
    beforeHash,
    "restore must match the closed pretrial database bytes",
  );
  const state = await readServiceState(statePath);
  NodeAssert.equal(state.activeVersion, "1.0.0");
  NodeAssert.equal(state.update.id, id);
  NodeAssert.equal(state.update.status, "rolled-back");
  NodeAssert.equal(state.update.reason, reason);
  const restoredAuthority = readNativeStoreAuthorityState(fixture.paths.authorityStateDir);
  NodeAssert.equal(restoredAuthority.state, "active");
  NodeAssert.equal(restoredAuthority.transition_id, null);
  NodeAssert.equal(restoredAuthority.store_generation, authority.store_generation + 1);
  NodeAssert.equal(restoredAuthority.environment_id, authority.environment_id);
  NodeAssert.equal(restoredAuthority.authority_namespace, authority.authority_namespace);
  await assertNoBackupOrSidecars(fixture, id);
  preserveExistingColumns(before, prior.snapshot);
}

it("production launcher commits after real SQLite migrations and matching prepared identity", async () => {
  await launcherScenario(
    "commit",
    async ({ fixture, receipt, before, authority, id, statePath }) => {
      const committed = await receipt("committed");
      NodeAssert.equal(committed.updateId, id);
      assertCurrentSchema(committed.snapshot);
      preserveExistingColumns(before, committed.snapshot);
      const state = await readServiceState(statePath);
      NodeAssert.equal(state.activeVersion, "1.1.0");
      NodeAssert.deepEqual(state.update, {
        id,
        fromVersion: "1.0.0",
        targetVersion: "1.1.0",
        status: "committed",
      });
      NodeAssert.deepEqual(
        readNativeStoreAuthorityState(fixture.paths.authorityStateDir),
        authority,
      );
      await assertNoBackupOrSidecars(fixture, id);
    },
  );
});

it("production launcher restores closed pretrial bytes after a migrated candidate exits", async () => {
  await launcherScenario("exit-after-migration", async (scenario) => {
    const migrated = await scenario.receipt("migrated");
    assertCurrentSchema(migrated.snapshot);
    preserveExistingColumns(scenario.before, migrated.snapshot);
    await assertRestored(scenario, "candidate-exited:23");
  });
});

it("production launcher restores historical state after a real migration trigger aborts", async () => {
  await launcherScenario(
    "migration-trigger-failure",
    async (scenario) => {
      const failure = await scenario.receipt("migration-failed");
      NodeAssert.equal(failure.sqlFailure?.tag, "SqlError");
      NodeAssert.equal(failure.sqlFailure.reason.operation, "execute");
      NodeAssert.equal(failure.sqlFailure.reason.cause.message, "qualification migration abort");
      await assertRestored(scenario, "candidate-exited:23");
    },
    async (fixture) => {
      await withDatabase(
        fixture,
        {},
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql.unsafe(
            `CREATE TRIGGER qualification_migration_abort BEFORE INSERT ON effect_sql_migrations WHEN NEW.migration_id = 54 BEGIN SELECT RAISE(ABORT, 'qualification migration abort'); END`,
          );
        }),
      );
    },
  );
});

it("production launcher rejects wrong prepared identity and restores migrated state", async () => {
  await launcherScenario("wrong-prepared", async (scenario) => {
    assertCurrentSchema((await scenario.receipt("migrated")).snapshot);
    const prepared = await scenario.receipt("prepared");
    NodeAssert.equal(prepared.updateId, `${scenario.id}-wrong`);
    await assertRestored(scenario, "invalid-prepared");
    NodeAssert.equal(
      scenario.observer.receipts.some((value) => value.type === "committed"),
      false,
    );
  });
});

it("production launcher backup failure keeps the prior database and never boots the candidate", async () => {
  await launcherScenario(
    "backup-failure",
    async ({ fixture, receipt, observer, before, beforeHash, authority, id, statePath }) => {
      const prior = await receipt("prior-readback");
      NodeAssert.deepEqual(prior.snapshot, before);
      NodeAssert.equal(await sha256File(fixture.paths.dbPath), beforeHash);
      NodeAssert.deepEqual(
        readNativeStoreAuthorityState(fixture.paths.authorityStateDir),
        authority,
      );
      const state = await readServiceState(statePath);
      NodeAssert.equal(state.activeVersion, "1.0.0");
      NodeAssert.deepEqual(state.update, {
        id,
        fromVersion: "1.0.0",
        targetVersion: "1.1.0",
        status: "failed",
        reason: "db-backup-failed",
      });
      NodeAssert.equal(
        observer.receipts.some((value) => value.version === "1.1.0"),
        false,
      );
      await assertNoBackupOrSidecars(fixture, id);
    },
    async (fixture, id) => {
      await NodeFSP.mkdir(NodePath.join(fixture.baseDir, "runtime", "db-backup", id), {
        recursive: true,
      });
    },
  );
});

it("cancellation awaits the owned trial child and launcher before scratch cleanup", async () => {
  await launcherScenario(
    "hold-before-prepared",
    async ({ launcher, completion, receipt, observer, statePath }) => {
      const migrated = await receipt("migrated");
      assertCurrentSchema(migrated.snapshot);
      const closingReceipt = observer.waitFor("child-closing");
      await launcher.stop("SIGTERM");
      NodeAssert.deepEqual(await completion, { stopped: true });
      const closing = await closingReceipt;
      NodeAssert.equal(closing.pid, migrated.pid);
      NodeAssert.equal(closing.code, 0);
      NodeAssert.equal(
        observer.receipts.some((value) => value.type === "committed"),
        false,
      );
      const state = await readServiceState(statePath);
      NodeAssert.equal(state.activeVersion, "1.0.0");
      NodeAssert.equal(state.update.status, "pending");
      NodeAssert.equal(state.update.phase, "trial-ready");
    },
  );
});
