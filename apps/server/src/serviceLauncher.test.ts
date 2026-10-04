// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  configuredDatabasePathForBaseDir,
  Launcher,
  readServiceState,
  validateDatabasePathForBaseDir,
  writeServiceState,
} from "./serviceLauncher.ts";
import {
  compareExactServiceVersions,
  decodeServiceLauncherContext,
  LEGACY_SERVICE_LAUNCHER_PROTOCOL,
  decodeServiceState,
  isExactServiceVersion,
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_RESTART_PENDING_FILE,
  SERVICE_STOP_MARKER_FILE,
} from "./cloud/serviceProtocol.ts";
import {
  initializeNativeStoreAuthority,
  readNativeStoreAuthorityState,
} from "./environment/nativeStoreAuthorityPersistence.ts";

it("accepts only exact semantic versions", () => {
  for (const version of ["0.0.0", "1.2.3", "1.2.3-alpha.1", "1.2.3-0", "1.2.3+001"]) {
    assert.isTrue(isExactServiceVersion(version), version);
  }
  for (const version of ["latest", "01.2.3", "1.2.3-01", "1.2.3-alpha..1", "1.2.3+."]) {
    assert.isFalse(isExactServiceVersion(version), version);
  }
});

it("orders exact semantic versions without treating build metadata as precedence", () => {
  assert.equal(compareExactServiceVersions("1.2.3", "1.2.3"), 0);
  assert.equal(compareExactServiceVersions("1.2.4", "1.2.3"), 1);
  assert.equal(compareExactServiceVersions("2.0.0-alpha.1", "2.0.0-alpha.2"), -1);
  assert.equal(compareExactServiceVersions("2.0.0-alpha.2", "2.0.0-alpha.beta"), -1);
  assert.equal(compareExactServiceVersions("2.0.0-alpha-beta", "2.0.0-alpha-alpha"), 1);
  assert.equal(compareExactServiceVersions("2.0.0", "2.0.0-rc.1"), 1);
  assert.equal(compareExactServiceVersions("2.0.0+one", "2.0.0+two"), 0);
});

it("rejects contradictory service state", () => {
  assert.isUndefined(
    decodeServiceState({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: "0.0.31",
      update: {
        id: "update-1",
        fromVersion: "0.0.30",
        targetVersion: "0.0.32",
        dbPath: "/tmp/state.sqlite",
        status: "pending",
        phase: "trial-ready",
      },
    }),
  );

  assert.isUndefined(
    decodeServiceState({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: "1.0.0",
      update: {
        id: "update-3",
        fromVersion: "1.0.0",
        targetVersion: "1.1.0",
        status: "pending",
        phase: "trial-ready",
      },
    }),
  );

  assert.isUndefined(
    decodeServiceState({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: "1.0.0",
      update: {
        id: "update-2",
        fromVersion: "1.0.0",
        targetVersion: "0.9.0",
        dbPath: "/tmp/state.sqlite",
        status: "pending",
        phase: "trial-ready",
      },
    }),
  );
});

it("requires durable phases and never upgrades legacy pending state", () => {
  const update = {
    id: "phase-test",
    fromVersion: "1.0.0",
    targetVersion: "1.1.0",
    dbPath: "/fixture/userdata/state.sqlite",
    status: "pending",
  };
  for (const phase of [undefined, "unknown"]) {
    assert.isUndefined(
      decodeServiceState({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "1.0.0",
        update: { ...update, phase },
      }),
    );
  }
  for (const phase of ["accepted", "trial-ready"]) {
    assert.isDefined(
      decodeServiceState({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "1.0.0",
        update: { ...update, phase },
      }),
    );
  }
  assert.isUndefined(
    decodeServiceState({
      protocol: LEGACY_SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: "1.0.0",
      update,
    }),
  );
  const legacy = decodeServiceLauncherContext(
    JSON.stringify({ protocol: LEGACY_SERVICE_LAUNCHER_PROTOCOL, childVersion: "1.1.0", update }),
  );
  assert.equal(legacy?.protocol, LEGACY_SERVICE_LAUNCHER_PROTOCOL);
  assert.isFalse(legacy?.update !== undefined && "phase" in legacy.update);
  assert.isUndefined(
    decodeServiceLauncherContext(
      JSON.stringify({
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        childVersion: "1.1.0",
        update: { ...update, phase: "accepted" },
      }),
    ),
  );
});

it("binds service updates to the configured database path", () => {
  const baseDir = "/fixture/t3-service-path-test";
  const configuredPath = configuredDatabasePathForBaseDir(baseDir);
  assert.equal(validateDatabasePathForBaseDir(baseDir, configuredPath), configuredPath);
  assert.throws(
    () => validateDatabasePathForBaseDir(baseDir, "/fixture/alternate.sqlite"),
    /selected userdata database/,
  );
});

// A pinned runtime is an executable at <versionDir>/t3. The tests stand one up
// as a Node shebang script so the launcher spawns it the way it spawns the
// real single-executable, IPC channel included.
const writeFakeRuntime = (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  versionDir: string,
  childSource: string,
) =>
  Effect.gen(function* () {
    const entryPath = path.join(versionDir, "t3");
    yield* fs.makeDirectory(versionDir, { recursive: true });
    yield* fs.writeFileString(entryPath, `#!${process.execPath}\n${childSource}`);
    yield* fs.chmod(entryPath, 0o755);
    yield* fs.writeFileString(
      path.join(versionDir, ".install-complete"),
      `${path.basename(versionDir)}\n`,
    );
    return entryPath;
  });

it.layer(NodeServices.layer)("service state persistence", (it) => {
  it.effect("durably replaces and strictly reads one state document", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-test-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const state = {
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "0.0.31",
      } as const;

      yield* Effect.promise(() => writeServiceState(statePath, state));
      assert.deepEqual(yield* Effect.promise(() => readServiceState(statePath)), state);
    }),
  );

  it.effect("a fresh launcher clears a restart deferred by t3 update", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-restart-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const restartPending = path.join(root, "runtime", SERVICE_RESTART_PENDING_FILE);
      yield* writeFakeRuntime(
        fs,
        path,
        path.join(root, "runtime", "versions", "1.0.0"),
        "setInterval(() => {}, 1_000);\n",
      );
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );
      const run = () =>
        Effect.gen(function* () {
          const launcher = new Launcher(
            root,
            yield* Effect.promise(() => readServiceState(statePath)),
          );
          const running = launcher.run();
          yield* Effect.promise(() => launcher.stop("SIGTERM"));
          yield* Effect.promise(() => running);
        });

      // A launcher that is still the old version leaves a marker that waits
      // for a newer one.
      yield* fs.writeFileString(restartPending, "1.0.1\n");
      yield* run();
      assert.isTrue(yield* fs.exists(restartPending));

      // Whoever restarted the service, the launcher now runs what the unit
      // names, so the deferred-restart marker is gone.
      yield* fs.writeFileString(restartPending, "1.0.0\n");
      yield* run();
      assert.isFalse(yield* fs.exists(restartPending));
    }),
  );

  it.effect("serializes shutdown with launcher recovery", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-stop-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      yield* writeFakeRuntime(
        fs,
        path,
        path.join(root, "runtime", "versions", "1.0.0"),
        "setInterval(() => {}, 1_000);\n",
      );
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );

      const launcher = new Launcher(
        root,
        yield* Effect.promise(() => readServiceState(statePath)),
        { databasePath: path.join(root, "userdata", "state.sqlite") },
      );
      const running = launcher.run();
      const stopping = launcher.stop("SIGTERM");
      // An explicit stop leaves the marker that tells a child shutting down
      // mid-update that no replacement server is coming. It is present as
      // soon as stop() returns its promise, before queued transitions run.
      assert.isTrue(yield* fs.exists(path.join(root, "runtime", SERVICE_STOP_MARKER_FILE)));
      yield* Effect.promise(() => stopping);
      yield* Effect.promise(() => running);
    }),
  );

  it.effect("commits only after the trial reports prepared", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-flow-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const databasePath = path.join(root, "userdata", "state.sqlite");
      yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
      yield* fs.writeFileString(databasePath, "before trial");
      // @effect-diagnostics-next-line preferSchemaOverJson:off - embeds a path in fake child source.
      const encodedDatabasePath = JSON.stringify(databasePath);
      const childSource = `
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  process.send({ type: "prepared", updateId: context.update.id });
  process.on("message", (message) => {
    if (message.type === "committed") process.exit(0);
  });
} else if (context.update === undefined) {
  process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${encodedDatabasePath} });
  setInterval(() => {}, 1_000);
} else {
  process.exit(0);
}
`;
      for (const version of ["1.0.0", "1.1.0"]) {
        yield* writeFakeRuntime(
          fs,
          path,
          path.join(root, "runtime", "versions", version),
          childSource,
        );
      }
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );

      const launcher = new Launcher(
        root,
        yield* Effect.promise(() => readServiceState(statePath)),
        { databasePath: path.join(root, "userdata", "state.sqlite") },
      );
      yield* Effect.promise(() =>
        launcher.run().then(
          () => Promise.reject(new Error("launcher unexpectedly completed")),
          () => Promise.resolve(),
        ),
      );

      const state = yield* Effect.promise(() => readServiceState(statePath));
      assert.equal(state.activeVersion, "1.1.0");
      assert.equal(state.update?.status, "committed");
    }),
  );

  it.effect("rolls back a trial that reports the wrong update ID", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-rollback-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const databasePath = path.join(root, "userdata", "state.sqlite");
      yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
      yield* fs.writeFileString(databasePath, "before trial");
      // @effect-diagnostics-next-line preferSchemaOverJson:off - embeds a path in fake child source.
      const encodedDatabasePath = JSON.stringify(databasePath);
      const childSource = `
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  process.send({ type: "prepared", updateId: "wrong-update" });
} else if (context.update === undefined) {
  process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${encodedDatabasePath} });
  setInterval(() => {}, 1_000);
} else {
  process.exit(0);
}
`;
      for (const version of ["1.0.0", "1.1.0"]) {
        yield* writeFakeRuntime(
          fs,
          path,
          path.join(root, "runtime", "versions", version),
          childSource,
        );
      }
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );

      const launcher = new Launcher(
        root,
        yield* Effect.promise(() => readServiceState(statePath)),
        { databasePath: path.join(root, "userdata", "state.sqlite") },
      );
      yield* Effect.promise(() =>
        launcher.run().then(
          () => Promise.reject(new Error("launcher unexpectedly completed")),
          () => Promise.resolve(),
        ),
      );

      const state = yield* Effect.promise(() => readServiceState(statePath));
      assert.equal(state.activeVersion, "1.0.0");
      assert.equal(state.update?.status, "rolled-back");
      assert.equal(
        state.update?.status === "rolled-back" ? state.update.reason : undefined,
        "invalid-prepared",
      );
    }),
  );

  it.effect.each(["state.sqlite", "statev2.sqlite"])(
    "restores the selected %s database when a migrating trial exits",
    (databaseName) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-db-" });
        const statePath = path.join(root, "runtime", "service-state.json");
        const databasePath = path.join(root, "userdata", databaseName);
        const authorityStateDir = path.join(root, "native-store-authority");
        const original = "SQLite format 3\0database before migration";
        yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
        yield* fs.writeFileString(databasePath, original);
        yield* fs.writeFileString(
          path.join(root, "userdata", "environment-id"),
          "environment-launcher\n",
        );
        initializeNativeStoreAuthority(authorityStateDir, "environment-launcher");
        // @effect-diagnostics-next-line preferSchemaOverJson:off - embeds a path in fake child source.
        const encodedDatabasePath = JSON.stringify(databasePath);
        const childSource = `
import { writeFileSync } from "node:fs";
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  writeFileSync(context.update.dbPath, "database after migration");
  writeFileSync(context.update.dbPath + "-wal", "trial wal");
  writeFileSync(context.update.dbPath + "-shm", "trial shm");
  process.exit(1);
} else if (context.update === undefined) {
  process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${encodedDatabasePath} });
  setInterval(() => {}, 1_000);
} else {
  process.exit(0);
}
`;
        for (const version of ["1.0.0", "1.1.0"]) {
          yield* writeFakeRuntime(
            fs,
            path,
            path.join(root, "runtime", "versions", version),
            childSource,
          );
        }
        yield* Effect.promise(() =>
          writeServiceState(statePath, {
            protocol: SERVICE_LAUNCHER_PROTOCOL,
            activeVersion: "1.0.0",
          }),
        );

        const launcher = new Launcher(
          root,
          yield* Effect.promise(() => readServiceState(statePath)),
          { databasePath: path.join(root, "userdata", databaseName) },
        );
        yield* Effect.promise(() =>
          launcher.run().then(
            () => Promise.reject(new Error("launcher unexpectedly completed")),
            () => Promise.resolve(),
          ),
        );

        const state = yield* Effect.promise(() => readServiceState(statePath));
        assert.equal(state.activeVersion, "1.0.0");
        assert.equal(state.update?.status, "rolled-back");
        assert.equal(yield* fs.readFileString(databasePath), original);
        const authority = readNativeStoreAuthorityState(authorityStateDir);
        assert.equal(authority.state, "active");
        assert.equal(authority.store_generation, 2);
        assert.isFalse(yield* fs.exists(`${databasePath}-wal`));
        assert.isFalse(yield* fs.exists(`${databasePath}-shm`));
        const updateId = state.update?.id;
        assert.isDefined(updateId);
        assert.isFalse(yield* fs.exists(path.join(root, "runtime", "db-backup", updateId)));
      }),
  );

  it.effect("returns to the previous executable when the pretrial backup fails", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-backup-failed-" });
      for (const version of ["1.0.0", "1.1.0"]) {
        yield* writeFakeRuntime(
          fs,
          path,
          path.join(root, "runtime", "versions", version),
          "process.exit(0);\n",
        );
      }
      const databasePath = path.join(root, "userdata", "state.sqlite");
      const statePath = path.join(root, "runtime", "service-state.json");
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
          update: {
            id: "backup-failure",
            fromVersion: "1.0.0",
            targetVersion: "1.1.0",
            dbPath: databasePath,
            status: "pending",
            phase: "accepted",
          },
        }),
      );
      const launcher = new Launcher(
        root,
        yield* Effect.promise(() => readServiceState(statePath)),
        { databasePath: path.join(root, "userdata", "state.sqlite") },
      );
      yield* Effect.promise(() =>
        launcher.run().then(
          () => Promise.reject(new Error("launcher unexpectedly completed")),
          () => Promise.resolve(),
        ),
      );
      const state = yield* Effect.promise(() => readServiceState(statePath));
      assert.equal(state.activeVersion, "1.0.0");
      assert.equal(state.update?.status, "failed");
      assert.equal(
        state.update?.status === "failed" ? state.update.reason : undefined,
        "db-backup-failed",
      );
      assert.isFalse(yield* fs.exists(path.join(root, "runtime", "db-backup", "backup-failure")));
    }),
  );

  it.effect("fences authority without replacing a missing rollback baseline", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-service-launcher-missing-backup-",
      });
      const statePath = path.join(root, "runtime", "service-state.json");
      const databasePath = path.join(root, "userdata", "state.sqlite");
      yield* fs.makeDirectory(path.join(root, "userdata"), { recursive: true });
      yield* fs.writeFileString(databasePath, "SQLite format 3\0trial-modified database");
      yield* fs.writeFileString(
        path.join(root, "userdata", "environment-id"),
        "environment-missing-backup\n",
      );
      initializeNativeStoreAuthority(
        path.join(root, "native-store-authority"),
        "environment-missing-backup",
      );
      yield* writeFakeRuntime(
        fs,
        path,
        path.join(root, "runtime", "versions", "1.1.0"),
        "process.exit(0);\n",
      );
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
          update: {
            id: "missing-backup-update",
            fromVersion: "1.0.0",
            targetVersion: "1.1.0",
            dbPath: databasePath,
            status: "pending",
            phase: "trial-ready",
          },
        }),
      );

      const launcher = new Launcher(
        root,
        yield* Effect.promise(() => readServiceState(statePath)),
        { databasePath: path.join(root, "userdata", "state.sqlite") },
      );
      yield* Effect.promise(() =>
        launcher.run().then(
          () => Promise.reject(new Error("launcher unexpectedly completed")),
          () => Promise.resolve(),
        ),
      );

      const authority = readNativeStoreAuthorityState(path.join(root, "native-store-authority"));
      assert.equal(authority.state, "fenced");
      assert.equal(
        yield* fs.readFileString(databasePath),
        "SQLite format 3\0trial-modified database",
      );
      assert.equal(
        (yield* Effect.promise(() => readServiceState(statePath))).update?.status,
        "pending",
      );
      assert.isFalse(
        yield* fs.exists(path.join(root, "runtime", "db-backup", "missing-backup-update")),
      );
    }),
  );

  it.effect("resumes a marked restore after abrupt authority writer death", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-service-launcher-restore-resume-",
      });
      const updateId = "restore-resume-update";
      const statePath = path.join(root, "runtime", "service-state.json");
      const databasePath = path.join(root, "userdata", "state.sqlite");
      const backupDir = path.join(root, "runtime", "db-backup", updateId);
      yield* fs.makeDirectory(path.join(root, "userdata"), { recursive: true });
      yield* fs.makeDirectory(backupDir, { recursive: true });
      yield* fs.writeFileString(databasePath, "SQLite format 3\0trial-modified database");
      yield* fs.writeFileString(
        path.join(root, "userdata", "environment-id"),
        "environment-restore-resume\n",
      );
      yield* fs.writeFileString(path.join(backupDir, "database"), "SQLite format 3\0original");
      yield* fs.writeFileString(path.join(backupDir, ".restore-pending"), "");
      initializeNativeStoreAuthority(
        path.join(root, "native-store-authority"),
        "environment-restore-resume",
      );

      const authorityStateDir = path.join(root, "native-store-authority");
      const crash = NodeChildProcess.spawnSync(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `
          import fs from "node:fs";
          import { syncBuiltinESMExports } from "node:module";
          const authority = await import(process.argv[1]);
          const statePath = authority.nativeStoreAuthorityPaths(process.argv[2]).statePath;
          const rename = fs.renameSync;
          fs.renameSync = (...args) => {
            rename(...args);
            if (args[1] === statePath) process.kill(process.pid, "SIGKILL");
          };
          syncBuiltinESMExports();
          authority.fenceNativeStoreAuthority(process.argv[2], "environment-restore-resume");
          throw new Error("writer unexpectedly survived");
        `,
          new URL("./environment/nativeStoreAuthorityPersistence.ts", import.meta.url).href,
          authorityStateDir,
        ],
        { timeout: 10_000, encoding: "utf8" },
      );
      assert.isUndefined(crash.error);
      assert.equal(crash.signal, "SIGKILL");
      assert.equal(readNativeStoreAuthorityState(authorityStateDir).state, "fenced");
      assert.equal(readNativeStoreAuthorityState(authorityStateDir).store_generation, 1);
      assert.equal(
        yield* fs.readFileString(databasePath),
        "SQLite format 3\0trial-modified database",
      );

      const versionDir = path.join(root, "runtime", "versions", "1.0.0");
      const previousStarted = path.join(root, "previous-runtime-started");
      // @effect-diagnostics-next-line preferSchemaOverJson:off - path embedded in a fixture executable.
      const previousStartedLiteral = JSON.stringify(previousStarted);
      yield* writeFakeRuntime(
        fs,
        path,
        versionDir,
        `require("node:fs").writeFileSync(${previousStartedLiteral}, "started"); process.exit(0);\n`,
      );

      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
          update: {
            id: updateId,
            fromVersion: "1.0.0",
            targetVersion: "1.1.0",
            dbPath: databasePath,
            status: "pending",
            phase: "trial-ready",
          },
        }),
      );

      const launcher = new Launcher(
        root,
        yield* Effect.promise(() => readServiceState(statePath)),
        { databasePath: path.join(root, "userdata", "state.sqlite") },
      );
      yield* Effect.promise(() =>
        launcher.run().then(
          () => Promise.reject(new Error("launcher unexpectedly completed")),
          () => Promise.resolve(),
        ),
      );

      const state = yield* Effect.promise(() => readServiceState(statePath));
      assert.equal(state.update?.status, "failed");
      assert.equal(
        state.update?.status === "failed" ? state.update.reason : undefined,
        "rollback-interrupted",
      );
      assert.equal(yield* fs.readFileString(databasePath), "SQLite format 3\0original");
      assert.equal(
        readNativeStoreAuthorityState(path.join(root, "native-store-authority")).store_generation,
        2,
      );
      assert.isFalse(yield* fs.exists(backupDir));
      assert.equal(yield* fs.readFileString(previousStarted), "started");
    }),
  );
});
