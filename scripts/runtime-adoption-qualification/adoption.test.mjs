import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeChildProcess from "node:child_process";
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { assert, it } from "@effect/vitest";
import { describe, expect, vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Schema from "effect/Schema";
import { KeybindingsConfig } from "../../packages/contracts/src/keybindings.ts";
import { ServerSettings } from "../../packages/contracts/src/settings.ts";
import {
  HostProcessArchitecture,
  HostProcessPlatform,
} from "../../packages/shared/src/hostProcess.ts";
import { DEFAULT_KEYBINDINGS } from "../../packages/shared/src/keybindings.ts";
import { migrationManifest } from "../../apps/server/src/persistence/Migrations.ts";
import { makePopulatedFixture, readFixture, withDatabase } from "./fixture.mjs";
import { bindArtifactInputs, sha256File, withRunScratch } from "./support.mjs";

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

const acceptedPackageSource = "f7301caf3fff1ebff013e94570e324579deaa6ca";
const acceptedT2Source = "50935bb50f2dae620e93686b7a6e10c9c3067285";

async function compiledCandidate(context) {
  const mode = process.env.T4_QUAL_MODE ?? "dev";
  assert.isTrue(mode === "dev" || mode === "final", "Invalid qualification mode");
  const descriptorPath = process.env.T4_QUAL_DESCRIPTOR_PATH;
  if (!descriptorPath) {
    if (mode === "dev") {
      context.skip("UNAVAILABLE: actual compiled candidate descriptor missing in development mode");
      return;
    }
    throw new Error("Final compiled adoption requires an artifact descriptor");
  }
  assert.isTrue(NodePath.isAbsolute(descriptorPath), "Descriptor path must be absolute");
  let descriptor;
  try {
    descriptor = JSON.parse(await NodeFSP.readFile(descriptorPath, "utf8"));
  } catch {
    throw new Error("Compiled adoption descriptor cannot be read or decoded");
  }
  assert.equal(descriptor.acceptedCumulativeSource, acceptedPackageSource);
  assert.equal(descriptor.acceptedT2Source, acceptedT2Source);
  assert.equal(descriptor.acceptedCumulativeTree, "704a5cf13f6fcb5d279dd5e3191b5febece0e76e");
  assert.equal(descriptor.candidate?.sourceCommit, acceptedPackageSource);
  assert.equal(descriptor.candidate?.sourceTree, descriptor.acceptedCumulativeTree);
  assert.equal(
    descriptor.candidate?.sha256,
    "db2ced245b32d3f651864e5427be4ef8c0c74d4d1a6525d5bba868d93c55bcc9",
  );
  assert.equal(
    descriptor.candidate?.runnerSha256,
    "f5246360095283f89c5f49e936f9c75dcd89e9f3212c58b87baf8fda10910b8a",
  );
  assert.equal(descriptor.candidate?.platform, "linux");
  assert.equal(descriptor.candidate?.architecture, "x64");
  assert.equal(descriptor.candidate?.version, "0.0.44-preview.20261001.1004");
  assert.equal(descriptor.candidate?.channel, "preview");
  assert.equal(HostProcessPlatform.defaultValue(), "linux");
  assert.equal(HostProcessArchitecture.defaultValue(), "x64");
  // run.mjs proves clean production-source equivalence; these cases recheck the accepted bytes.
  const binding = await bindArtifactInputs(descriptor, {
    repository: "Jones-Systems/Jones-Code",
    commit: acceptedPackageSource,
    clean: true,
  });
  assert.equal(binding.status, "bound", "Compiled candidate inputs must bind");
  assert.isString(binding.candidate.runnerPath);
  return {
    ...binding.candidate,
    acceptedT2Source,
    descriptorSha256: await sha256File(descriptorPath),
  };
}

async function isolateCompiledFixture(fixture) {
  const settings = Schema.decodeUnknownSync(ServerSettings)(
    JSON.parse(await NodeFSP.readFile(fixture.paths.settingsPath, "utf8")),
  );
  const disabledConfig = (driver, config) => ({
    ...config,
    enabled: false,
    binaryPath: NodePath.join(fixture.baseDir, "missing-binaries", driver),
    ...(["codex", "claudeAgent"].includes(driver)
      ? { homePath: NodePath.join(fixture.baseDir, "isolated-home", driver) }
      : {}),
  });
  settings.enableProviderUpdateChecks = false;
  settings.providers = Object.fromEntries(
    Object.entries(settings.providers).map(([driver, config]) => [
      driver,
      disabledConfig(driver, config),
    ]),
  );
  settings.providerInstances = Object.fromEntries(
    Object.entries(settings.providerInstances).map(([id, entry]) => [
      id,
      { ...entry, enabled: false, config: disabledConfig(entry.driver, entry.config) },
    ]),
  );
  await NodeFSP.writeFile(
    fixture.paths.settingsPath,
    JSON.stringify(Schema.encodeSync(ServerSettings)(settings)),
  );
  const keybindings = Schema.decodeUnknownSync(KeybindingsConfig)([
    ...DEFAULT_KEYBINDINGS.filter((rule) => rule.command !== "thread.stop"),
    { key: "mod+alt+shift+k", command: "thread.stop" },
  ]);
  await NodeFSP.writeFile(fixture.paths.keybindingsConfigPath, JSON.stringify(keybindings));
}

async function decodedFixtureSettings(fixture) {
  return Schema.decodeUnknownSync(ServerSettings)(
    JSON.parse(await NodeFSP.readFile(fixture.paths.settingsPath, "utf8")),
  );
}

function changedSettingsPaths(before, after) {
  const paths = [];
  let truncated = false;
  const visit = (left, right, path) => {
    if (JSON.stringify(left) === JSON.stringify(right)) return;
    if (paths.length === 64) {
      truncated = true;
      return;
    }
    if (left !== null && right !== null && typeof left === "object" && typeof right === "object") {
      for (const key of [...new Set([...Object.keys(left), ...Object.keys(right)])].sort()) {
        visit(left[key], right[key], [...path, key]);
      }
      return;
    }
    paths.push(path.join(".").slice(0, 160));
  };
  visit(before, after, []);
  return { paths, truncated };
}

function expectedCompiledStartupSettings(before, fixtureReadback) {
  assert.isFalse(before.projectSettingsFolded);
  for (const key of [
    "projectSettingsOverrides",
    "projectAgentBrowserAccessOverrides",
    "projectAutoPullOverrides",
    "projectScriptOverrides",
  ]) {
    assert.deepEqual(before[key], {});
  }
  for (const project of fixtureReadback.tables.projection_projects) {
    assert.isNull(project.default_model_selection_json);
    assert.isNull(project.default_thread_env_mode);
    assert.equal(project.auto_pull, 0);
    assert.deepEqual(JSON.parse(project.scripts_json), []);
  }
  // Populated projects cause the one-time marker fold; these rows add no project overrides.
  const providerInstances = Object.fromEntries(
    Object.entries(before.providerInstances).map(([id, entry]) => {
      assert.isFalse(entry.enabled);
      const { enabled, ...config } = entry.config;
      assert.isFalse(enabled);
      return [id, { ...entry, config }];
    }),
  );
  return Schema.decodeUnknownSync(ServerSettings)(
    Schema.encodeSync(ServerSettings)({
      ...before,
      providerInstances,
      projectSettingsFolded: true,
    }),
  );
}

async function allocateLoopbackPort() {
  const listener = NodeNet.createServer();
  try {
    await new Promise((resolve, reject) => {
      listener.once("error", reject);
      listener.listen(0, "127.0.0.1", resolve);
    });
    return listener.address().port;
  } finally {
    if (listener.listening)
      await new Promise((resolve, reject) =>
        listener.close((error) => (error ? reject(error) : resolve())),
      );
  }
}

function rootHttpStatus(port) {
  return new Promise((resolve, reject) => {
    const request = NodeHttp.get(
      { host: "127.0.0.1", port, path: "/", agent: false },
      (response) => {
        const status = response.statusCode;
        response.destroy();
        resolve(status);
      },
    );
    request.on("error", () => reject(new Error("Compiled candidate loopback request failed")));
    request.setTimeout(5_000, () => request.destroy(new Error("Loopback request timed out")));
  });
}

async function startCompiledCandidate(scratch, fixture, candidate) {
  const home = NodePath.join(scratch.root, "isolated-home");
  const privateBin = NodePath.join(home, "empty-bin");
  const shell = NodePath.join(home, "qualification-shell");
  const env = {
    HOME: home,
    XDG_CONFIG_HOME: NodePath.join(home, "config"),
    XDG_CACHE_HOME: NodePath.join(home, "cache"),
    XDG_DATA_HOME: NodePath.join(home, "data"),
    XDG_STATE_HOME: NodePath.join(home, "state"),
    XDG_RUNTIME_DIR: NodePath.join(home, "runtime"),
    TMPDIR: NodePath.join(home, "tmp"),
    TMP: NodePath.join(home, "tmp"),
    TEMP: NodePath.join(home, "tmp"),
    NODE_COMPILE_CACHE: NodePath.join(home, "node-cache"),
    T3CODE_NATIVE_AUTHORITY_STATE_DIR: fixture.paths.authorityStateDir,
    SHELL: shell,
    PATH: "",
  };
  for (const directory of [...Object.values(env).filter(NodePath.isAbsolute), privateBin]) {
    if (directory !== shell) await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  }
  // fixPath tries SHELL first. Its capture needs printenv; both executables avoid real profiles.
  await NodeFSP.writeFile(
    shell,
    '#!/bin/sh\nPATH="$HOME/empty-bin"\nexport PATH\nexec /bin/bash --noprofile --norc -c "$2"\n',
    { mode: 0o700 },
  );
  await NodeFSP.writeFile(
    NodePath.join(privateBin, "printenv"),
    '#!/bin/sh\n[ "$1" = PATH ] || exit 1\nprintf "%s\\n" "$PATH"\n',
    { mode: 0o700 },
  );
  assert.equal(await sha256File(candidate.runnerPath), candidate.runnerSha256);
  const port = await allocateLoopbackPort();
  let settleStartup;
  const startup = new Promise((resolve) => {
    settleStartup = resolve;
  });
  let startupSettled = false;
  const settle = (value) => {
    if (startupSettled) return;
    startupSettled = true;
    settleStartup(value);
  };
  let reading = false;
  const observeReceipt = async () => {
    if (reading || startupSettled || !scratch.child?.pid) return;
    reading = true;
    try {
      const state = JSON.parse(
        await NodeFSP.readFile(fixture.paths.serverRuntimeStatePath, "utf8"),
      );
      if (
        state.pid !== scratch.child.pid ||
        state.port !== port ||
        state.host !== "127.0.0.1" ||
        state.origin !== `http://127.0.0.1:${port}`
      ) {
        settle({ kind: "invalid-runtime-receipt" });
        return;
      }
      const httpStatus = await rootHttpStatus(port);
      settle({ kind: "ready", pid: state.pid, host: state.host, port: state.port, httpStatus });
    } catch (error) {
      if (error.code !== "ENOENT") settle({ kind: "runtime-receipt-or-http-failure" });
    } finally {
      reading = false;
    }
  };
  const watcher = NodeFS.watch(fixture.paths.stateDir, (_event, filename) => {
    if (filename === null || String(filename) === "server-runtime.json") void observeReceipt();
  });
  watcher.on("error", () => settle({ kind: "runtime-receipt-watch-failure" }));
  const watchdog = setTimeout(() => settle({ kind: "startup-timeout" }), 20_000);
  scratch.closed = false;
  try {
    scratch.child = NodeChildProcess.spawn(
      candidate.runnerPath,
      [
        "serve",
        "--base-dir",
        fixture.baseDir,
        "--host",
        "127.0.0.1",
        "--port",
        String(port),
        "--no-browser",
      ],
      { cwd: home, env, detached: false, stdio: ["ignore", "pipe", "pipe"] },
    );
  } catch {
    scratch.closed = true;
    watcher.close();
    clearTimeout(watchdog);
    throw new Error("Compiled candidate spawn failed before PID allocation");
  }
  const diagnostics = {
    pid: scratch.child.pid ?? null,
    stdoutBytes: 0,
    stderrBytes: 0,
    outputLimitExceeded: false,
    migration54AbortObserved: false,
  };
  const abortMarker = "qualification abort migration 54";
  const diagnosticSuffix = { stdoutBytes: "", stderrBytes: "" };
  const count = (key) => (chunk) => {
    diagnostics[key] += chunk.length;
    const text = diagnosticSuffix[key] + chunk.toString("utf8");
    if (text.includes(abortMarker)) diagnostics.migration54AbortObserved = true;
    diagnosticSuffix[key] = text.slice(-(abortMarker.length - 1));
    if (
      diagnostics.stdoutBytes + diagnostics.stderrBytes > 262_144 &&
      !diagnostics.outputLimitExceeded
    ) {
      diagnostics.outputLimitExceeded = true;
      settle({ kind: "output-limit" });
      scratch.child.kill("SIGTERM");
    }
  };
  scratch.child.stdout.on("data", count("stdoutBytes"));
  scratch.child.stderr.on("data", count("stderrBytes"));
  scratch.child.on("error", () => settle({ kind: "spawn-error" }));
  scratch.exit = new Promise((resolve) => {
    scratch.child.once("close", (code, signal) => {
      diagnosticSuffix.stdoutBytes = "";
      diagnosticSuffix.stderrBytes = "";
      scratch.closed = true;
      scratch.exitInfo = { code, signal };
      settle({ kind: "exit", code, signal });
      resolve(scratch.exitInfo);
    });
  });
  scratch.diagnostics = diagnostics;
  scratch.stop = async () => {
    if (!scratch.closed) scratch.child.kill("SIGTERM");
    let timer;
    try {
      await Promise.race([
        scratch.exit,
        new Promise((_, reject) => {
          timer = setTimeout(
            () => reject(new Error("Compiled candidate closure unknown; scratch retained")),
            10_000,
          );
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  };
  try {
    scratch.record({ phase: "spawn", ...diagnostics, processGroup: "inherited-test-group", port });
    return await startup;
  } finally {
    clearTimeout(watchdog);
    watcher.close();
  }
}

async function withCompiledScratch(label, callback) {
  const fallback = NodeURL.fileURLToPath(
    new URL("../../.t3/runtime-adoption-qualification", import.meta.url),
  );
  const parent = NodePath.resolve(process.env.T4_QUAL_SCRATCH_ROOT ?? fallback);
  await NodeFSP.mkdir(parent, { recursive: true });
  const canonicalParent = await NodeFSP.realpath(parent);
  assert.isFalse(canonicalParent === "/tmp" || canonicalParent.startsWith("/tmp/"));
  const root = await NodeFSP.mkdtemp(NodePath.join(canonicalParent, `${label}-`));
  const evidenceDir = process.env.T4_QUAL_EVIDENCE_DIR
    ? NodePath.resolve(process.env.T4_QUAL_EVIDENCE_DIR)
    : undefined;
  const records = [];
  const scratch = { root, closed: true };
  const persist = (status) => {
    if (!evidenceDir) return;
    NodeFS.mkdirSync(evidenceDir, { recursive: true });
    NodeFS.writeFileSync(
      NodePath.join(evidenceDir, `${NodePath.basename(root)}.json`),
      `${JSON.stringify({ schema: "jones-runtime-qualification-run/v1", root, status, records }, null, 2)}\n`,
    );
  };
  scratch.record = (record) => {
    records.push(record);
    persist("running");
  };
  let status = "failure";
  let failure;
  try {
    assert.isFalse(evidenceDir === root || evidenceDir?.startsWith(`${root}${NodePath.sep}`));
    await callback(scratch);
    status = "success";
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    try {
      try {
        if (!scratch.closed && scratch.stop) await scratch.stop();
      } finally {
        records.push({
          phase: "closure",
          ...scratch.diagnostics,
          exit: scratch.exitInfo ?? null,
          pidClosure: scratch.closed ? "reaped-or-not-started" : "unknown",
          scratchRetained: !scratch.closed,
        });
        try {
          persist(scratch.closed ? status : "closure-unknown");
        } finally {
          if (scratch.closed) {
            await NodeFSP.rm(root, { recursive: true, force: true });
            assert.isTrue(await missing(root));
            records.push({ phase: "cleanup", exactRootAbsent: true });
            persist(status);
          } else {
            process.stderr.write(
              `${JSON.stringify({ kind: "compiled-candidate-closure-unknown", root, pid: scratch.child?.pid ?? null })}\n`,
            );
          }
        }
      }
    } catch (error) {
      if (!failure) throw error;
      records.push({ phase: "cleanup", result: "failed", primaryFailurePreserved: true });
      try {
        persist(scratch.closed ? "failure" : "closure-unknown");
      } catch {
        /* Keep the original scenario failure. */
      }
      process.stderr.write(
        `${JSON.stringify({ kind: "compiled-candidate-cleanup-failure", root, pid: scratch.child?.pid ?? null, pidClosure: scratch.closed ? "reaped-or-not-started" : "unknown" })}\n`,
      );
    }
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
          { migration_id: 3, name: "NativeCreationIntents" },
          { migration_id: 4, name: "NativeCreationCommandIdentities" },
          { migration_id: 5, name: "WorkstreamsNativeAttempts" },
          { migration_id: 6, name: "WorkstreamsProviderEnrollments" },
          { migration_id: 7, name: "ThreadCreationLookupIndex" },
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
      assert.equal(recovered.ledgers.fork.length, 7);
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
      assert.equal(recovered.ledgers.fork.length, 7);
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

describe("actual compiled candidate populated-home adoption", () => {
  it("boots upstream 53 with the hash-bound Linux runner and preserves populated state after shutdown", async (context) => {
    const candidate = await compiledCandidate(context);
    if (!candidate) return;
    await withCompiledScratch("compiled-adoption-53", async (scratch) => {
      const fixture = await makePopulatedFixture({ baseDir: scratch.root, throughMigration: 53 });
      await isolateCompiledFixture(fixture);
      const before = await readFixture(fixture);
      const beforeSettings = await decodedFixtureSettings(fixture);
      const expectedSettings = expectedCompiledStartupSettings(beforeSettings, before);
      const intentionalSettingsChanges = changedSettingsPaths(beforeSettings, expectedSettings);
      assert.isTrue(before.references.valid);
      assert.equal(before.ledgers.upstream.at(-1).migration_id, 53);
      assert.deepEqual(before.ledgers.fork, []);
      const readiness = await startCompiledCandidate(scratch, fixture, candidate);
      assert.equal(readiness.kind, "ready");
      assert.equal(readiness.httpStatus, 200);
      scratch.record({ phase: "readiness", ...readiness });
      await scratch.stop();
      assert.isTrue(scratch.closed);
      assert.isFalse(scratch.diagnostics.outputLimitExceeded);
      assert.isTrue(await missing(fixture.paths.serverRuntimeStatePath));
      const after = await readFixture(fixture);
      const afterSettings = await decodedFixtureSettings(fixture);
      const changes = changedSettingsPaths(beforeSettings, afterSettings);
      const unexpectedChanges = changedSettingsPaths(expectedSettings, afterSettings);
      const beforeSettingsHash = before.files.find(
        (file) => file.path === fixture.paths.settingsPath,
      ).sha256;
      const afterSettingsHash = after.files.find(
        (file) => file.path === fixture.paths.settingsPath,
      ).sha256;
      scratch.record({
        phase: "settings-preservation",
        proofKind: "intentional-production-settings-migration",
        beforeSha256: beforeSettingsHash,
        afterSha256: afterSettingsHash,
        settingsBytesChanged: beforeSettingsHash !== afterSettingsHash,
        decodedSettingsEqual: changes.paths.length === 0 && !changes.truncated,
        settingsSemanticsPreserved:
          unexpectedChanges.paths.length === 0 && !unexpectedChanges.truncated,
        intentionalChangedPaths: intentionalSettingsChanges.paths,
        changedPaths: changes.paths,
        changedPathsTruncated: changes.truncated,
        unexpectedChangedPaths: unexpectedChanges.paths,
        unexpectedChangedPathsTruncated: unexpectedChanges.truncated,
      });
      assert.deepEqual(afterSettings, expectedSettings);
      assert.isFalse(afterSettings.enableProviderUpdateChecks);
      for (const settings of [beforeSettings, afterSettings]) {
        for (const provider of Object.values(settings.providers)) assert.isFalse(provider.enabled);
        for (const instance of Object.values(settings.providerInstances))
          assert.isFalse(instance.enabled);
      }
      assert.deepEqual(changes, intentionalSettingsChanges);
      const strictFiles = (files) =>
        files.filter((file) => file.path !== fixture.paths.settingsPath);
      assertHistoricalRowsPreserved(
        { ...before, files: strictFiles(before.files) },
        { ...after, files: strictFiles(after.files) },
      );
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
      await withDatabase(
        fixture,
        { startup: false },
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
          assert.deepEqual(yield* sql`PRAGMA foreign_key_check`, []);
        }),
      );
      scratch.record({
        checkId: "compiled-startup-upstream-53",
        proofKind: "actual-compiled-candidate/synthetic-populated-home",
        result: "passed",
        sourceCommit: candidate.sourceCommit,
        sourceTree: candidate.sourceTree,
        acceptedT2Source: candidate.acceptedT2Source,
        version: candidate.version,
        archiveSha256: candidate.sha256,
        runnerSha256: candidate.runnerSha256,
        descriptorSha256: candidate.descriptorSha256,
        isolation: {
          providersEnabled: false,
          enableProviderUpdateChecks: false,
          inheritedEnvironment: false,
          realLoginProfiles: false,
        },
        before: {
          upstream: before.ledgers.upstream.at(-1).migration_id,
          fork: before.ledgers.fork,
          files: before.files,
        },
        after: {
          upstream: after.ledgers.upstream.at(-1).migration_id,
          fork: after.ledgers.fork,
          files: after.files,
        },
        historicalRowsAndNativeIdentityPreserved: true,
        settingsBytesChanged: beforeSettingsHash !== afterSettingsHash,
        settingsSemanticsPreserved: true,
        intentionalSettingsChangedPaths: intentionalSettingsChanges.paths,
        shutdownRuntimeReceiptCleared: true,
        projectWorktreeAttachmentReferencesValid: after.references.valid,
        environmentId: fixture.expected.environmentId,
        limitations: [
          "Providers disabled; native provider continuation unproved",
          "Actual prior-package paired rollback unproved",
          "Synthetic home only; installed service, browser and Connect interaction unproved",
        ],
      });
    });
  }, 60_000);

  it("fails actual runner startup at the migration 54 pre-effect trigger and leaves historical state intact", async (context) => {
    const candidate = await compiledCandidate(context);
    if (!candidate) return;
    await withCompiledScratch("compiled-abort-54", async (scratch) => {
      const fixture = await makePopulatedFixture({ baseDir: scratch.root, throughMigration: 53 });
      await isolateCompiledFixture(fixture);
      await withDatabase(
        fixture,
        { startup: false },
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TRIGGER qualification_abort_54 BEFORE INSERT ON effect_sql_migrations
          WHEN NEW.migration_id = 54 BEGIN SELECT RAISE(ABORT, 'qualification abort migration 54'); END`;
        }),
      );
      const before = await readFixture(fixture);
      const startup = await startCompiledCandidate(scratch, fixture, candidate);
      assert.equal(startup.kind, "exit");
      assert.isNumber(startup.code);
      assert.notEqual(startup.code, 0);
      assert.isNull(startup.signal);
      assert.isTrue(scratch.closed);
      assert.isFalse(scratch.diagnostics.outputLimitExceeded);
      assert.isTrue(
        scratch.diagnostics.migration54AbortObserved,
        "Startup must report the synthetic migration 54 abort",
      );
      const after = await readFixture(fixture);
      assert.deepEqual(after, before);
      await withDatabase(
        fixture,
        { startup: false },
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const columns = yield* sql`PRAGMA table_info(projection_threads)`;
          assert.isFalse(columns.some((column) => column.name === "auto_settle_disabled_at"));
          assert.deepEqual(
            yield* sql`SELECT name FROM sqlite_master WHERE name IN ('jones_sql_migrations', 'worktree_ownership_leases')`,
            [],
          );
          assert.deepEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
        }),
      );
      scratch.record({
        checkId: "compiled-migration-54-pre-effect-failure",
        proofKind: "actual-compiled-candidate/synthetic-populated-home",
        result: "passed",
        sourceCommit: candidate.sourceCommit,
        sourceTree: candidate.sourceTree,
        acceptedT2Source: candidate.acceptedT2Source,
        version: candidate.version,
        archiveSha256: candidate.sha256,
        runnerSha256: candidate.runnerSha256,
        descriptorSha256: candidate.descriptorSha256,
        startup,
        isolation: {
          providersEnabled: false,
          enableProviderUpdateChecks: false,
          inheritedEnvironment: false,
          realLoginProfiles: false,
        },
        before: {
          upstream: before.ledgers.upstream.at(-1).migration_id,
          fork: before.ledgers.fork,
          files: before.files,
        },
        after: {
          upstream: after.ledgers.upstream.at(-1).migration_id,
          fork: after.ledgers.fork,
          files: after.files,
        },
        historicalStatePreserved: true,
        migration54AbortObserved: scratch.diagnostics.migration54AbortObserved,
        migration54ColumnAndForkAbsent: true,
        environmentId: fixture.expected.environmentId,
        limitations: [
          "Trigger rejects the migration ledger insert before effects; post-write rollback remains covered by separate source tests",
          "Actual prior-package paired rollback unproved",
        ],
      });
    });
  }, 60_000);
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
