// @effect-diagnostics nodeBuiltinImport:off -- SQLite subprocesses and files belong only to each scoped synthetic fixture.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import {
  isQualifiedNativeEnrollmentSqliteRuntime,
  makeNativeEnrollmentSqliteConfig,
  NativeEnrollmentSqliteQualificationError,
} from "./sqliteConnection.ts";

const helperUrl = new URL("./sqliteConnection.ts", import.meta.url).href;
const childSource = `
  import * as NodeFS from 'node:fs';
  import * as NodeSqlite from 'node:sqlite';
  const [helperUrl, dbPath, action] = process.argv.slice(1);
  if (action === 'failure') throw new Error('synthetic fixture failure');
  if (action === 'hold') { setInterval(() => {}, 1000); }
  else {
    const { makeNativeEnrollmentSqliteConfig } = await import(helperUrl);
    const config = makeNativeEnrollmentSqliteConfig(dbPath, action === 'wal' || action === 'missing' ? 'apply' : 'operational_inspection');
    if (action === 'missing') {
      try { const db = new NodeSqlite.DatabaseSync(config.filename, { readOnly: config.readonly, allowExtension: config.allowExtension }); db.close();
        process.stdout.write(JSON.stringify({ failed: false, exists: NodeFS.existsSync(dbPath) })); }
      catch { process.stdout.write(JSON.stringify({ failed: true, exists: NodeFS.existsSync(dbPath) })); }
    } else {
      const db = new NodeSqlite.DatabaseSync(config.filename, { readOnly: config.readonly, allowExtension: config.allowExtension });
      if (action === 'wal') {
        db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; INSERT INTO workstreams_native_enrollments VALUES ('wal-only', 'committed-in-wal')");
        // Exit without closing this fixture writer so committed WAL frames cannot be checkpointed on close.
        process.stdout.write(JSON.stringify({ committed: true }), () => process.exit(0));
      } else {
        try { process.stdout.write(JSON.stringify(db.prepare('SELECT * FROM workstreams_native_enrollments ORDER BY enrollment_id').all())); }
        finally { db.close(); }
      }
    }
  }
`;

const fixtureProcess = (dbPath: string, action: "wal" | "read" | "missing" | "failure" | "hold") =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const child = NodeChildProcess.spawn(
        process.execPath,
        ["--no-warnings", "--input-type=module", "-e", childSource, helperUrl, dbPath, action],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let stdout = "";
      let stderr = "";
      let failure: Error | undefined;
      const completed = new Promise<{
        stdout: string;
        code: number | null;
        failure: Error | undefined;
      }>((resolve) => {
        const collect = (chunk: Buffer, stream: "stdout" | "stderr") => {
          if (stream === "stdout") stdout += chunk.toString("utf8");
          else stderr += chunk.toString("utf8");
          if (Buffer.byteLength(stdout) + Buffer.byteLength(stderr) > 8192) {
            failure = new Error("Synthetic SQLite child exceeded its output bound.");
            child.kill("SIGTERM");
          }
        };
        child.stdout?.on("data", (chunk: Buffer) => collect(chunk, "stdout"));
        child.stderr?.on("data", (chunk: Buffer) => collect(chunk, "stderr"));
        child.once("error", (error) => {
          failure = error;
        });
        child.once("close", (code) => {
          resolve({ stdout, code, failure });
        });
      });
      return { child, completed };
    }),
    (owned) =>
      Effect.promise(async () => {
        if (owned.child.exitCode === null && owned.child.signalCode === null)
          owned.child.kill("SIGTERM");
        // Wait for this captured child to close before the enclosing fixture removes its exact directory.
        await owned.completed;
      }),
  );
const runFixtureProcess = (dbPath: string, action: "wal" | "read" | "missing" | "failure") =>
  Effect.gen(function* () {
    const owned = yield* fixtureProcess(dbPath, action);
    const stdout = yield* Effect.tryPromise(async () => {
      const result = await owned.completed;
      if (result.failure) throw result.failure;
      if (result.code !== 0) throw new Error("Synthetic SQLite child failed.");
      return result.stdout;
    }).pipe(Effect.timeout("10 seconds"), TestClock.withLive);
    return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(stdout);
  });
const fixture = Effect.acquireRelease(
  Effect.sync(() => {
    const root = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "workstreams-sqlite-synthetic-"),
    );
    try {
      const dbPath = NodePath.join(root, "state # ?.sqlite");
      const db = new NodeSqlite.DatabaseSync(dbPath);
      try {
        db.exec(
          "CREATE TABLE workstreams_native_enrollments (enrollment_id TEXT PRIMARY KEY, request_sha256 TEXT); INSERT INTO workstreams_native_enrollments VALUES ('baseline', 'checkpointed')",
        );
      } finally {
        db.close();
      }
      NodeFS.chmodSync(dbPath, 0o600);
      return { root, dbPath };
    } catch (error) {
      NodeFS.rmSync(root, { recursive: true, force: true });
      throw error;
    }
  }),
  (f) => Effect.sync(() => NodeFS.rmSync(f.root, { recursive: true, force: true })),
);

it("accepts only the three source-qualified exact Node/SQLite pairs", () => {
  for (const [node, sqlite] of [
    ["24.13.1", "3.51.2"],
    ["24.19.0", "3.53.3"],
    ["24.21.0", "3.53.4"],
  ])
    assert.strictEqual(isQualifiedNativeEnrollmentSqliteRuntime(node, sqlite), true);
  for (const [node, sqlite] of [
    ["24.13.1", "3.53.3"],
    ["24.19.0", "3.51.2"],
    ["24.19.1", "3.53.3"],
    ["24.13.1", "3.53.4"],
    ["24.19.0", "3.53.4"],
    ["24.21.0", "3.51.2"],
    ["24.21.0", "3.53.3"],
    ["24.21.1", "3.53.4"],
    ["24.21.0", "3.53.5"],
    ["24.19.0", undefined],
    [undefined, "3.53.3"],
  ])
    assert.strictEqual(isQualifiedNativeEnrollmentSqliteRuntime(node, sqlite), false);
  assert.throws(
    () =>
      makeNativeEnrollmentSqliteConfig(
        "file:///arbitrary.sqlite?immutable=1",
        "operational_inspection",
      ),
    NativeEnrollmentSqliteQualificationError,
  );
  assert.throws(
    () =>
      Reflect.apply(makeNativeEnrollmentSqliteConfig, undefined, [
        "/synthetic/state.sqlite",
        "invalid",
      ]),
    NativeEnrollmentSqliteQualificationError,
  );
});

it.effect(
  "uses fixed escaped ro/rw file URIs with private unix cache and disabled extensions",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const readonly = makeNativeEnrollmentSqliteConfig(f.dbPath, "operational_inspection");
      const mutable = makeNativeEnrollmentSqliteConfig(f.dbPath, "apply");
      assert.deepEqual(
        { readonly: readonly.readonly, allowExtension: readonly.allowExtension },
        { readonly: true, allowExtension: false },
      );
      assert.deepEqual(
        { readonly: mutable.readonly, allowExtension: mutable.allowExtension },
        { readonly: false, allowExtension: false },
      );
      assert.strictEqual(new URL(readonly.filename).search, "?mode=ro&cache=private&vfs=unix");
      assert.strictEqual(new URL(mutable.filename).search, "?mode=rw&cache=private&vfs=unix");
      assert.strictEqual(new URL(readonly.filename).hash, "");
      assert.strictEqual(readonly.filename.includes("%23%20%3F.sqlite"), true);
    }).pipe(Effect.scoped),
);

it.effect(
  "separate operational readers see committed WAL-only enrollment rows without changing main bytes or records",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      assert.deepEqual(yield* runFixtureProcess(f.dbPath, "wal"), { committed: true });
      assert.strictEqual(NodeFS.existsSync(`${f.dbPath}-wal`), true);
      assert.strictEqual(
        NodeFS.readFileSync(f.dbPath).includes(Buffer.from("committed-in-wal")),
        false,
      );
      const before = NodeFS.readFileSync(f.dbPath);
      const expected = [
        { enrollment_id: "baseline", request_sha256: "checkpointed" },
        { enrollment_id: "wal-only", request_sha256: "committed-in-wal" },
      ];
      assert.deepEqual(yield* runFixtureProcess(f.dbPath, "read"), expected);
      assert.deepEqual(NodeFS.readFileSync(f.dbPath), before);
      assert.deepEqual(yield* runFixtureProcess(f.dbPath, "read"), expected);
      assert.deepEqual(NodeFS.readFileSync(f.dbPath), before);
      assert.strictEqual(NodeFS.existsSync(NodePath.join(f.root, "server-signing-key.bin")), false);
    }).pipe(Effect.scoped),
);

it.effect(
  "a checkpointed WAL-header store with missing sidecars remains usable without main-file changes",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const writer = new NodeSqlite.DatabaseSync(f.dbPath);
      try {
        writer.exec(
          "PRAGMA journal_mode=WAL; INSERT INTO workstreams_native_enrollments VALUES ('checkpointed-wal', 'durable'); PRAGMA wal_checkpoint(TRUNCATE)",
        );
      } finally {
        writer.close();
      }
      assert.strictEqual(NodeFS.readFileSync(f.dbPath)[18], 2);
      assert.strictEqual(NodeFS.existsSync(`${f.dbPath}-wal`), false);
      assert.strictEqual(NodeFS.existsSync(`${f.dbPath}-shm`), false);
      const before = NodeFS.readFileSync(f.dbPath);
      assert.deepEqual(yield* runFixtureProcess(f.dbPath, "read"), [
        { enrollment_id: "baseline", request_sha256: "checkpointed" },
        { enrollment_id: "checkpointed-wal", request_sha256: "durable" },
      ]);
      assert.deepEqual(NodeFS.readFileSync(f.dbPath), before);
    }).pipe(Effect.scoped),
);

it.effect("mode=rw does not create a main file that disappeared after qualification", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    NodeFS.unlinkSync(f.dbPath);
    assert.deepEqual(yield* runFixtureProcess(f.dbPath, "missing"), {
      failed: true,
      exists: false,
    });
    assert.strictEqual(NodeFS.existsSync(f.dbPath), false);
  }).pipe(Effect.scoped),
);

it.effect(
  "synthetic process fixtures close their child and remove only their own root on success, failure and cancellation",
  () =>
    Effect.gen(function* () {
      const sibling = yield* fixture;
      const siblingBefore = NodeFS.readFileSync(sibling.dbPath);
      let successfulRoot = "";
      yield* Effect.gen(function* () {
        const f = yield* fixture;
        successfulRoot = f.root;
        yield* runFixtureProcess(f.dbPath, "read");
      }).pipe(Effect.scoped);
      assert.strictEqual(NodeFS.existsSync(successfulRoot), false);
      let failedRoot = "";
      const failed = yield* Effect.gen(function* () {
        const f = yield* fixture;
        failedRoot = f.root;
        yield* runFixtureProcess(f.dbPath, "failure");
      }).pipe(Effect.scoped, Effect.result);
      assert.strictEqual(failed._tag, "Failure");
      assert.strictEqual(NodeFS.existsSync(failedRoot), false);
      const started = yield* Deferred.make<void>();
      let cancelledRoot = "";
      let ownedChild: NodeChildProcess.ChildProcess | undefined;
      const pending = yield* Effect.gen(function* () {
        const f = yield* fixture;
        cancelledRoot = f.root;
        ownedChild = (yield* fixtureProcess(f.dbPath, "hold")).child;
        yield* Deferred.succeed(started, undefined);
        return yield* Effect.never;
      }).pipe(Effect.scoped, Effect.forkChild);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(pending);
      assert.strictEqual(NodeFS.existsSync(cancelledRoot), false);
      assert.strictEqual(ownedChild?.signalCode, "SIGTERM");
      assert.deepEqual(NodeFS.readFileSync(sibling.dbPath), siblingBefore);
    }).pipe(Effect.scoped),
);
