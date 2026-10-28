import * as NodeSqlite from "node:sqlite";
import * as NodeWorkerThreads from "node:worker_threads";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import type * as Tracer from "effect/Tracer";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";

import * as SqliteClient from "./nodeSqliteClient.ts";
import { withReadPriority } from "./jones/sqliteReadPriority.ts";
import type { SqliteReaderPoolOptions } from "./jones/sqliteReaderContracts.ts";

const startedWorkers = vi.hoisted((): Array<NodeWorkerThreads.Worker> => []);
const silentWorkers = vi.hoisted(() => ({
  on: false,
  readyOnTerminate: false,
  terminationGate: undefined as Promise<void> | undefined,
  rejectTermination: false,
}));
const readerDiagnostic = vi.hoisted(() => ({
  port: undefined as NodeWorkerThreads.MessagePort | undefined,
  gate: undefined as SharedArrayBuffer | undefined,
}));
const workerTerminals = vi.hoisted(
  () =>
    new WeakMap<
      NodeWorkerThreads.Worker,
      {
        error?: Error;
        exitCode?: number;
        terminationRequested: boolean;
        terminationRequests: number;
      }
    >(),
);
const checkpointDiagnostic = vi.hoisted(() => ({
  port: undefined as NodeWorkerThreads.MessagePort | undefined,
  gate: undefined as SharedArrayBuffer | undefined,
}));
const failingWorkers = vi.hoisted(() => ({ message: undefined as string | undefined }));

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  class RecordedWorker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      if (checkpointDiagnostic.port) {
        const [source, options] = args;
        // Observe the real first prepare on a separate port: parentPort messages acknowledge checks.
        const prelude = `
const __diagnostic = require("node:worker_threads").workerData.checkpointDiagnostic;
const __prototype = require("node:sqlite").DatabaseSync.prototype;
const __prepare = __prototype.prepare;
__prototype.prepare = function (...args) {
  __prototype.prepare = __prepare;
  __diagnostic.port.postMessage("first-prepare");
  __diagnostic.port.close();
  if (__diagnostic.gate) Atomics.wait(new Int32Array(__diagnostic.gate), 0, 0);
  return Reflect.apply(__prepare, this, args);
};
`;
        args = [
          prelude + String(source),
          {
            ...options,
            workerData: {
              ...options?.workerData,
              checkpointDiagnostic: {
                port: checkpointDiagnostic.port,
                gate: checkpointDiagnostic.gate,
              },
            },
            transferList: [...(options?.transferList ?? []), checkpointDiagnostic.port],
          },
        ];
      }
      if (readerDiagnostic.port) {
        const [source, options] = args;
        // Block only the marked statement in the real reader, after its startup handshake.
        const prelude = `
const __readerDiagnostic = require("node:worker_threads").workerData.readerDiagnostic;
const __readerPrototype = require("node:sqlite").DatabaseSync.prototype;
const __readerPrepare = __readerPrototype.prepare;
__readerPrototype.prepare = function (sql, ...args) {
  if (sql.includes("__reader_progress__")) {
    __readerDiagnostic.port.postMessage("query-start");
    __readerDiagnostic.port.close();
    Atomics.wait(new Int32Array(__readerDiagnostic.gate), 0, 0);
  }
  return Reflect.apply(__readerPrepare, this, [sql, ...args]);
};
`;
        args = [
          prelude + String(source),
          {
            ...options,
            workerData: {
              ...options?.workerData,
              readerDiagnostic: { port: readerDiagnostic.port, gate: readerDiagnostic.gate },
            },
            transferList: [...(options?.transferList ?? []), readerDiagnostic.port],
          },
        ];
      }
      super(
        ...((silentWorkers.on
          ? ["setInterval(() => {}, 1 << 30);", { eval: true }]
          : failingWorkers.message !== undefined
            ? [`throw new Error(${JSON.stringify(failingWorkers.message)});`, { eval: true }]
            : args) as ConstructorParameters<typeof actual.Worker>),
      );
      const terminal = { terminationRequested: false, terminationRequests: 0 } as {
        error?: Error;
        exitCode?: number;
        terminationRequested: boolean;
        terminationRequests: number;
      };
      workerTerminals.set(this, terminal);
      this.on("error", (error) => {
        terminal.error = error;
      });
      this.on("exit", (code) => {
        terminal.exitCode = code;
      });
      startedWorkers.push(this);
    }
    override terminate() {
      const terminal = workerTerminals.get(this)!;
      terminal.terminationRequested = true;
      terminal.terminationRequests++;
      // A "ready" already queued when termination starts reaches the parent before "exit".
      if (silentWorkers.readyOnTerminate) this.emit("message", { type: "ready" });
      const gate = silentWorkers.terminationGate;
      const termination =
        gate === undefined ? super.terminate() : gate.then(() => super.terminate());
      // The real worker still exits; only the parent's cleanup proof is rejected.
      return silentWorkers.rejectTermination
        ? termination.then(() => {
            throw new Error("synthetic termination rejection");
          })
        : termination;
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

const withReaderClient = <A, E>(
  use: (sql: SqlClient.SqlClient) => Effect.Effect<A, E, SqlClient.SqlClient>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-reader-" });
    const filename = path.join(directory, "state.sqlite");
    return yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA journal_mode = WAL`;
      yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
      yield* sql`INSERT INTO entries VALUES ('first')`;
      return yield* use(sql);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, readerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

const countEntries = (sql: SqlClient.SqlClient) =>
  Effect.map(
    sql<{ count: number }>`SELECT COUNT(*) AS count FROM entries`,
    (rows) => rows[0]?.count,
  );

const layer = it.layer(SqliteClient.layer({ filename: ":memory:" }));

layer("NodeSqliteClient", (it) => {
  it.effect("records the writer connection owner on a waiting query", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const holder = yield* sql
        .withTransaction(
          sql`SELECT 1`.pipe(
            Effect.andThen(Deferred.succeed(held, undefined)),
            Effect.andThen(Deferred.await(release)),
          ),
        )
        .pipe(Effect.withSpan("test.writerHolder"), Effect.forkChild);
      yield* Deferred.await(held);

      const waitingSpan = yield* Deferred.make<Tracer.Span>();
      const observeWaiter: Statement.Transformer = (statement, _sql, _fiber, span) =>
        Deferred.succeed(waitingSpan, span).pipe(Effect.as(statement));
      const waiter = yield* sql`SELECT 1`.pipe(
        Effect.provideService(Statement.CurrentTransformer, observeWaiter),
        Effect.provideService(Statement.SpanPropagationEnabled, true),
        (effect) => Effect.forkChild(effect, { startImmediately: true }),
      );
      const span = yield* Deferred.await(waitingSpan);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(holder);
      yield* Fiber.join(waiter);

      assert.equal(span.attributes.get("db.lock_owner_at_start.name"), "test.writerHolder");
      assert.typeOf(span.attributes.get("db.lock_owner_at_start.fiber_id"), "number");
      assert.typeOf(span.attributes.get("db.lock_owner_at_start.trace_id"), "string");
      assert.typeOf(span.attributes.get("db.lock_owner_at_start.span_id"), "string");
    }),
  );

  it.effect("retries preparing a query after the missing schema becomes available", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const select = sql<{ name: string }>`SELECT name FROM created_after_prepare_failure`;
      const error = yield* select.pipe(Effect.flip);
      assert.equal(error._tag, "SqlError");
      assert.equal(error.reason.operation, "prepare");

      yield* sql`CREATE TABLE created_after_prepare_failure(name TEXT NOT NULL)`;
      yield* sql`INSERT INTO created_after_prepare_failure VALUES ('recovered')`;
      assert.deepEqual(yield* select, [{ name: "recovered" }]);
      assert.deepEqual(yield* select.values, [["recovered"]]);
    }),
  );

  it.effect("classifies constraint failures by their SQLite result code", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE constrained(name TEXT NOT NULL UNIQUE)`;
      yield* sql`INSERT INTO constrained VALUES ('taken')`;

      const duplicate = yield* sql`INSERT INTO constrained VALUES ('taken')`.pipe(Effect.flip);
      assert(duplicate.reason._tag === "UniqueViolation");
      assert.equal(duplicate.reason.constraint, "constrained.name");

      const missing = yield* sql`INSERT INTO constrained VALUES (NULL)`.pipe(Effect.flip);
      assert.equal(missing.reason._tag, "ConstraintError");
    }),
  );
});

const largeText = "x".repeat(64 * 1024 + 1);

it.effect("reads integers on the values path as the caller asked, cached or not", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const value of ["small", largeText]) {
      const values = sql`SELECT 1 AS id, length(${value}) AS size`.values;
      assert.deepEqual(yield* values.pipe(Effect.provideService(SqlClient.SafeIntegers, true)), [
        [1n, BigInt(value.length)],
      ]);
      assert.deepEqual(yield* values, [[1, value.length]]);
    }
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

const makeTempDatabase = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-transaction-" });
  const filename = path.join(directory, "state.sqlite");
  const other = yield* Effect.acquireRelease(
    Effect.sync(() => new NodeSqlite.DatabaseSync(filename)),
    (database) => Effect.sync(() => database.close()),
  );
  yield* Effect.sync(() => {
    other.exec(`
      PRAGMA journal_mode = WAL;
      CREATE TABLE counters(id INTEGER PRIMARY KEY, value INTEGER NOT NULL);
      INSERT INTO counters VALUES (1, 0);
    `);
  });
  return { filename, other };
});

it.effect("keeps another connection from committing between a transaction's read and write", () =>
  Effect.gen(function* () {
    const { filename, other } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const [row] = yield* sql<{ readonly value: number }>`
            SELECT value FROM counters WHERE id = 1
          `;
          // Deferred BEGIN permits a competing commit, causing SQLITE_BUSY_SNAPSHOT that no timeout can resolve.
          yield* Effect.sync(() =>
            assert.throws(
              () => other.exec("UPDATE counters SET value = value + 1 WHERE id = 1"),
              /database is locked/,
            ),
          );
          yield* sql`UPDATE counters SET value = ${(row?.value ?? 0) + 10} WHERE id = 1`;
        }),
      );
      yield* Effect.sync(() => other.exec("UPDATE counters SET value = value + 1 WHERE id = 1"));
      assert.deepEqual(yield* sql`SELECT value FROM counters WHERE id = 1`.values, [[11]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename })));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("reports a transaction blocked by another writer as a lock timeout", () =>
  Effect.gen(function* () {
    const { filename, other } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA busy_timeout = 0`;
      const read = sql.withTransaction(sql`SELECT value FROM counters WHERE id = 1`.values);

      yield* Effect.sync(() => other.exec("BEGIN IMMEDIATE"));
      const error = yield* read.pipe(Effect.flip);
      assert.equal(error.reason._tag, "LockTimeoutError");

      yield* Effect.sync(() => other.exec("COMMIT"));
      assert.deepEqual(yield* read, [[0]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename })));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("lets the writer commit while a read-only transaction is open", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const snapshotTaken = yield* Deferred.make<void>();
      const writer = yield* Effect.forkChild(
        Deferred.await(snapshotTaken).pipe(
          Effect.andThen(sql`INSERT INTO entries VALUES ('second')`),
        ),
      );
      const counts = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const before = yield* countEntries(sql);
            yield* Deferred.succeed(snapshotTaken, undefined);
            // Holding the writer lock here would deadlock this join.
            yield* Fiber.join(writer);
            const after = yield* countEntries(sql);
            return { before, after };
          }),
        )
        .pipe(SqliteClient.readOnly);

      assert.deepEqual(counts, { before: 1, after: 1 });
      assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 2);
    }),
  ),
);

// regression: #19226 read-only transactions run on the separate reader connection
it.effect("rejects a write in a read-only transaction", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const error = yield* sql
        .withTransaction(sql`INSERT INTO entries VALUES ('blocked')`)
        .pipe(SqliteClient.readOnly, Effect.flip);

      assert.equal(error._tag, "SqlError");
      assert.include(String(error.reason.cause), "readonly database");
      assert.equal(yield* countEntries(sql), 1);
    }),
  ),
);

it.effect("keeps the reader read-only even if a statement turns query_only off", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const error = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`PRAGMA query_only = OFF`;
            yield* sql`INSERT INTO entries VALUES ('blocked')`;
          }),
        )
        .pipe(SqliteClient.readOnly, Effect.flip);

      assert.equal(error._tag, "SqlError");
      assert.include(String(error.reason.cause), "readonly database");
      assert.equal(yield* countEntries(sql), 1);
    }),
  ),
);

it.effect("keeps read-only work inside a write transaction on the writer", () =>
  withReaderClient((sql) =>
    sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO entries VALUES ('uncommitted')`;
        const count = yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly);
        assert.equal(count, 2);
      }),
    ),
  ),
);

it.effect("fails a read-only transaction whose reader stops, then starts a new reader", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const startedBefore = startedWorkers.length;
      const error = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* countEntries(sql);
            yield* Effect.promise(() => startedWorkers.at(-1)!.terminate());
            return yield* countEntries(sql);
          }),
        )
        .pipe(SqliteClient.readOnly, Effect.flip);

      assert.equal(error._tag, "SqlError");
      assert.include(String(error.reason.cause), "Database reader exited");
      assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
      assert.equal(startedWorkers.length, startedBefore + 2);
    }),
  ),
);

const withFileClient = <A, E>(
  setup: (
    sql: SqlClient.SqlClient,
    filename: string,
  ) => Effect.Effect<A, E, SqlClient.SqlClient | FileSystem.FileSystem>,
  options: Pick<
    SqliteClient.SqliteClientConfig,
    "readerStartupTimeout" | "readerFailureMode" | "readerWorker" | "readerPool" | "checkpointWorker"
  > = {},
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-reader-off-" });
    const filename = path.join(directory, "state.sqlite");
    return yield* Effect.gen(function* () {
      return yield* setup(yield* SqlClient.SqlClient, filename);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, readerWorker: true, ...options })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

it.effect.each([{ readerFailureMode: "fallback" }, { readerFailureMode: "fail" }] as const)(
  "recovers a timed-out reader after cleanup and coalesces concurrent retries ($readerFailureMode)",
  ({ readerFailureMode }) =>
    withFileClient(
      (sql) =>
        Effect.gen(function* () {
          yield* sql`PRAGMA journal_mode = WAL`;
          yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
          yield* sql`INSERT INTO entries VALUES ('kept')`;
          const startedBefore = startedWorkers.length;
          const writerPrepare = vi.spyOn(NodeSqlite.DatabaseSync.prototype, "prepare");
          const read = sql<{ count: number }>`
            SELECT COUNT(*) AS count FROM entries /* strict-reader-probe */
          `;
          const unavailableRead = read.pipe(
            SqliteClient.readOnly,
            Effect.match({
              onFailure: (error) => {
                assert.equal(readerFailureMode, "fail");
                assert.equal(error._tag, "SqlError");
                assert.equal(error.reason.operation, "open");
                return undefined;
              },
              onSuccess: (rows) => {
                assert.equal(readerFailureMode, "fallback");
                return rows[0]?.count;
              },
            }),
          );
          const assertUnavailableRead = Effect.gen(function* () {
            assert.equal(yield* unavailableRead, readerFailureMode === "fail" ? undefined : 1);
            if (readerFailureMode === "fail") {
              assert.isFalse(
                writerPrepare.mock.calls.some(([sql]) => sql.includes("strict-reader-probe")),
              );
            }
          });
          let releaseTermination = () => {};
          silentWorkers.on = true;
          silentWorkers.readyOnTerminate = true;
          silentWorkers.terminationGate = new Promise<void>((resolve) => {
            releaseTermination = resolve;
          });
          yield* Effect.gen(function* () {
            const reads = yield* Effect.forkChild(
              Effect.all(
                Array.from({ length: 8 }, () => unavailableRead),
                { concurrency: "unbounded" },
              ),
              { startImmediately: true },
            );
            yield* TestClock.adjust("100 millis");
            assert.deepEqual(
              yield* Fiber.join(reads),
              Array(8).fill(readerFailureMode === "fail" ? undefined : 1),
            );
            yield* assertUnavailableRead;
            yield* sql`INSERT INTO entries VALUES ('writer-progress')`;
            assert.equal(yield* countEntries(sql), 2);
            yield* sql`DELETE FROM entries WHERE name = 'writer-progress'`;
            const retired = startedWorkers.at(-1)!;
            assert.equal(startedWorkers.length, startedBefore + 1);
            assert.equal(workerTerminals.get(retired)!.terminationRequests, 1);
            assert.isUndefined(workerTerminals.get(retired)!.exitCode);
            silentWorkers.on = false;
            silentWorkers.readyOnTerminate = false;
            // Neither a late ready nor demand during termination can start or revive a reader.
            retired.emit("message", { type: "ready" });
            yield* assertUnavailableRead;
            assert.equal(startedWorkers.length, startedBefore + 1);
            const cancelled = yield* Effect.forkChild(
              unavailableRead.pipe(Effect.andThen(Effect.never)),
              {
                startImmediately: true,
              },
            );
            yield* Fiber.interrupt(cancelled);
            assert.equal(startedWorkers.length, startedBefore + 1);
            releaseTermination();
            yield* Effect.promise(
              () =>
                new Promise<void>((resolve) => {
                  if (workerTerminals.get(retired)!.exitCode !== undefined) resolve();
                  else retired.once("exit", () => resolve());
                }),
            );
            yield* eventLoopTurn;
            yield* assertUnavailableRead;
            assert.equal(startedWorkers.length, startedBefore + 1);
            yield* TestClock.adjust("1 second");
            const recovered = yield* Effect.all(
              Array.from({ length: 8 }, () => countEntries(sql).pipe(SqliteClient.readOnly)),
              { concurrency: "unbounded" },
            );
            assert.deepEqual(recovered, Array(8).fill(1));
            assert.equal(startedWorkers.length, startedBefore + 2);
            const error = yield* sql
              .withTransaction(sql`INSERT INTO entries VALUES ('blocked')`)
              .pipe(SqliteClient.readOnly, Effect.flip);
            assert.include(String(error.reason.cause), "readonly database");
            assert.equal(workerTerminals.get(retired)!.terminationRequests, 1);
            assert.equal(yield* countEntries(sql), 1);
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                silentWorkers.on = false;
                silentWorkers.readyOnTerminate = false;
                silentWorkers.terminationGate = undefined;
                releaseTermination();
                writerPrepare.mockRestore();
              }),
            ),
          );
        }),
      { readerStartupTimeout: "100 millis", readerFailureMode },
    ),
);

it.effect("waits for a retiring reader during scope close even when close is interrupted", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const retired = yield* Deferred.make<void>();
    const startedBefore = startedWorkers.length;
    let releaseTermination = () => {};
    silentWorkers.on = true;
    silentWorkers.terminationGate = new Promise<void>((resolve) => {
      releaseTermination = resolve;
    });
    yield* Effect.gen(function* () {
      const work = yield* Effect.forkChild(
        withFileClient(
          (sql) =>
            Effect.gen(function* () {
              yield* sql`PRAGMA journal_mode = WAL`;
              yield* Deferred.succeed(started, undefined);
              const error = yield* sql`SELECT 1`.pipe(SqliteClient.readOnly, Effect.flip);
              assert.equal(error._tag, "SqlError");
              yield* Deferred.succeed(retired, undefined);
            }),
          { readerStartupTimeout: "100 millis", readerFailureMode: "fail" },
        ),
        { startImmediately: true },
      );
      yield* Deferred.await(started);
      yield* TestClock.adjust("100 millis");
      yield* Deferred.await(retired);
      yield* eventLoopTurn;
      const reader = startedWorkers.at(-1)!;
      assert.isUndefined(work.pollUnsafe());
      assert.equal(workerTerminals.get(reader)!.terminationRequests, 1);
      const interrupted = yield* Effect.forkChild(Fiber.interrupt(work), {
        startImmediately: true,
      });
      assert.isUndefined(interrupted.pollUnsafe());
      reader.emit("message", { type: "ready" });
      assert.equal(startedWorkers.length, startedBefore + 1);
      releaseTermination();
      yield* Fiber.join(interrupted);
      assert.equal(workerTerminals.get(reader)!.terminationRequests, 1);
      assert.isDefined(workerTerminals.get(reader)!.exitCode);
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          silentWorkers.on = false;
          silentWorkers.terminationGate = undefined;
          releaseTermination();
        }),
      ),
    );
  }),
);

it.effect("does not replace a reader when termination rejects", () =>
  withFileClient(
    (sql) =>
      Effect.gen(function* () {
        yield* sql`PRAGMA journal_mode = WAL`;
        const startedBefore = startedWorkers.length;
        silentWorkers.on = true;
        silentWorkers.rejectTermination = true;
        yield* Effect.gen(function* () {
          const read = yield* Effect.forkChild(
            sql`SELECT 1`.pipe(SqliteClient.readOnly, Effect.flip),
            {
              startImmediately: true,
            },
          );
          yield* TestClock.adjust("100 millis");
          assert.equal((yield* Fiber.join(read))._tag, "SqlError");
          const reader = startedWorkers.at(-1)!;
          yield* Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                if (workerTerminals.get(reader)!.exitCode !== undefined) resolve();
                else reader.once("exit", () => resolve());
              }),
          );
          yield* eventLoopTurn;
          silentWorkers.on = false;
          yield* TestClock.adjust("1 minute");
          assert.equal(
            (yield* sql`SELECT 1`.pipe(SqliteClient.readOnly, Effect.flip))._tag,
            "SqlError",
          );
          assert.equal(startedWorkers.length, startedBefore + 1);
          assert.equal(workerTerminals.get(reader)!.terminationRequests, 1);
          assert.deepEqual(yield* sql`SELECT 1 AS value`, [{ value: 1 }]);
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              silentWorkers.on = false;
              silentWorkers.rejectTermination = false;
            }),
          ),
        );
      }),
    { readerStartupTimeout: "100 millis", readerFailureMode: "fail" },
  ),
);

it.effect.each([
  { filename: ":memory:", readerWorker: true },
  { filename: "", readerWorker: true },
])("keeps strict reader policy inactive for non-worker databases ($filename)", (options) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const startedBefore = startedWorkers.length;
    yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
    yield* sql`INSERT INTO entries VALUES ('kept')`;
    assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
    assert.equal(startedWorkers.length, startedBefore);
  }).pipe(Effect.provide(SqliteClient.layer({ ...options, readerFailureMode: "fail" }))),
);

it.effect("keeps strict reader policy inactive when the file reader is disabled", () =>
  withFileClient(
    (sql) =>
      Effect.gen(function* () {
        const startedBefore = startedWorkers.length;
        yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
        yield* sql`INSERT INTO entries VALUES ('kept')`;
        assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
        assert.equal(startedWorkers.length, startedBefore);
      }),
    { readerWorker: false, readerFailureMode: "fail" },
  ),
);

it.effect("fails strict reads permanently when the database does not use WAL", () =>
  withFileClient(
    (sql) =>
      Effect.gen(function* () {
        yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
        yield* sql`INSERT INTO entries VALUES ('kept')`;
        const startedBefore = startedWorkers.length;
        const read = countEntries(sql).pipe(SqliteClient.readOnly, Effect.flip);
        assert.equal((yield* read)._tag, "SqlError");
        yield* TestClock.adjust("1 minute");
        assert.equal((yield* read)._tag, "SqlError");
        assert.equal(startedWorkers.length, startedBefore + 1);
        assert.equal(yield* countEntries(sql), 1);
      }),
    { readerFailureMode: "fail" },
  ),
);

it.effect("lets the event loop and writer advance while a reader statement is busy", () =>
  Effect.gen(function* () {
    const { port1, port2 } = new NodeWorkerThreads.MessageChannel();
    const gate = new Int32Array(new SharedArrayBuffer(4));
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        Atomics.store(gate, 0, 1);
        Atomics.notify(gate, 0);
        port1.close();
        port2.close();
        readerDiagnostic.port = undefined;
        readerDiagnostic.gate = undefined;
      }),
    );
    readerDiagnostic.port = port2;
    readerDiagnostic.gate = gate.buffer;
    const queryStarted = new Promise<void>((resolve) => port1.once("message", () => resolve()));
    yield* withReaderClient((sql) =>
      Effect.gen(function* () {
        const read = yield* Effect.forkChild(
          sql<{ count: number }>`
            SELECT COUNT(*) AS count FROM entries /* __reader_progress__ */
          `.pipe(SqliteClient.readOnly),
          { startImmediately: true },
        );
        yield* Effect.promise(() => queryStarted);
        yield* eventLoopTurn;
        yield* sql`INSERT INTO entries VALUES ('writer-progress')`;
        assert.equal(yield* countEntries(sql), 2);
        assert.isUndefined(read.pollUnsafe());
        yield* Effect.sync(() => {
          Atomics.store(gate, 0, 1);
          Atomics.notify(gate, 0);
        });
        assert.deepEqual(yield* Fiber.join(read), [{ count: 2 }]);
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            Atomics.store(gate, 0, 1);
            Atomics.notify(gate, 0);
          }),
        ),
      ),
    );
  }).pipe(Effect.scoped),
);

it.effect("uses the writer when the reader cannot open the database", () =>
  withFileClient((sql, filename) =>
    Effect.gen(function* () {
      yield* sql`PRAGMA journal_mode = WAL`;
      yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
      yield* sql`INSERT INTO entries VALUES ('kept')`;
      yield* (yield* FileSystem.FileSystem).remove(filename);
      const startedBefore = startedWorkers.length;

      assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
      assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
      assert.equal(startedWorkers.length, startedBefore + 1);
    }),
  ),
);

it.effect("finishes masked database cleanup after its caller is interrupted", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE cleanup_writes(value INTEGER PRIMARY KEY)`;
    const entered = yield* Deferred.make<void>();
    const work = yield* Effect.forkChild(
      Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.onInterrupt(() =>
          Effect.gen(function* () {
            yield* sql`INSERT INTO cleanup_writes VALUES (1)`;
            yield* sql.withTransaction(sql`INSERT INTO cleanup_writes VALUES (2)`);
          }),
        ),
      ),
      { startImmediately: true },
    );
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(work);
    assert.deepEqual(yield* sql`SELECT value FROM cleanup_writes ORDER BY value`.values, [
      [1],
      [2],
    ]);
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

// oxlint-disable-next-line t3code/no-real-time-wait-in-tests -- a zero-delay macrotask boundary that lets the worker's queued messages land, not a wait for time
const eventLoopTurn = Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)));

it.effect.each([
  { reservation: false, masked: false },
  { reservation: false, masked: true },
  { reservation: true, masked: false },
  { reservation: true, masked: true },
])(
  "preserves caller cancellation while waiting (reservation=$reservation, masked=$masked)",
  ({ reservation, masked }) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE entries(value INTEGER)`;
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const holder = yield* Effect.forkChild(
        sql.withTransaction(
          Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
        ),
        { startImmediately: true },
      );
      yield* Deferred.await(held);
      const entered = yield* Deferred.make<void>();
      const body = sql`INSERT INTO entries VALUES (1)`.pipe(
        Effect.andThen(Deferred.succeed(entered, undefined)),
      );
      const acquire = reservation
        ? Effect.scoped(
            Effect.gen(function* () {
              const connection = yield* sql.reserve;
              yield* connection.executeUnprepared("INSERT INTO entries VALUES (1)", [], undefined);
              yield* Deferred.succeed(entered, undefined);
            }),
          )
        : sql.withTransaction(body);
      const waiter = yield* Effect.forkChild(masked ? Effect.uninterruptible(acquire) : acquire, {
        startImmediately: true,
      });
      const cancelled = yield* Effect.forkChild(Fiber.interrupt(waiter), {
        startImmediately: true,
      });
      yield* Effect.gen(function* () {
        yield* eventLoopTurn;
        assert.equal(cancelled.pollUnsafe() !== undefined, !masked);
        assert.isFalse(yield* Deferred.isDone(entered));
      }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
      yield* Fiber.join(holder);
      yield* Fiber.join(cancelled);
      assert.equal(yield* Deferred.isDone(entered), masked);
      yield* sql.withTransaction(sql`INSERT INTO entries VALUES (2)`);
      assert.deepEqual(
        yield* sql`SELECT value FROM entries ORDER BY value`.values,
        masked ? [[1], [2]] : [[2]],
      );
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);


const readerPoolOptions = (size: 2 | 3): SqliteReaderPoolOptions => ({
  size, interactiveQueueCapacity: 64, backgroundQueueCapacity: 64,
  interactiveQueueTimeoutMs: 10_000, backgroundQueueTimeoutMs: 30_000, leaseWatchdogMs: 60_000,
});

it.effect("annotates the real statement span for an ordinary pooled read", () =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`PRAGMA journal_mode = WAL`;
    const observed = yield* Deferred.make<Tracer.Span>();
    const observeStatement: Statement.Transformer = (statement, _sql, _fiber, span) =>
      Deferred.succeed(observed, span).pipe(Effect.as(statement));
    assert.deepEqual(yield* sql`SELECT 1 AS value`.pipe(
      SqliteClient.readOnly,
      Effect.provideService(Statement.CurrentTransformer, observeStatement),
      Effect.provideService(Statement.SpanPropagationEnabled, true),
    ), [{ value: 1 }]);
    const span = yield* Deferred.await(observed);
    assert.equal(span.attributes.get("db.connection"), "reader");
    const wait = span.attributes.get("db.lock_wait_ms");
    assert.typeOf(wait, "number");
    assert.isTrue(Number.isFinite(wait));
    assert.isAtLeast(wait as number, 0);
  }), { readerPool: readerPoolOptions(2), readerFailureMode: "fail" }),
);

it.effect.each([2, 3] as const)("opens exactly %s independent SQLite reader connections and keeps reservations pinned", (size) =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
    yield* sql`INSERT INTO entries VALUES ('first')`;
    const before = startedWorkers.length;
    const release = yield* Deferred.make<void>();
    const ready = yield* Effect.forEach(Array.from({ length: size }), () => Deferred.make<void>());
    const holders = [];
    // Occupied shared connections force the last reservation onto the interactive reader.
    for (let index = 0; index < size; index++) {
      const indexNow = index;
      holders.push(yield* Effect.forkChild(Effect.scoped(Effect.gen(function* () {
        const connection = yield* sql.reserve;
        yield* connection.executeUnprepared("PRAGMA cache_size = " + (100 + indexNow), [], undefined);
        yield* Deferred.succeed(ready[indexNow]!, undefined);
        yield* Deferred.await(release);
        assert.deepEqual(yield* connection.executeUnprepared("PRAGMA cache_size", [], undefined), [{ cache_size: 100 + indexNow }]);
      })).pipe(SqliteClient.readOnly, withReadPriority(indexNow === size - 1 ? "interactive" : "background")), { startImmediately: true }));
      yield* Deferred.await(ready[index]!);
    }
    assert.equal(startedWorkers.length - before, size);
    yield* sql`INSERT INTO entries VALUES ('committed')`;
    yield* Deferred.succeed(release, undefined);
    for (const holder of holders) yield* Fiber.join(holder);
    const released = yield* Effect.scoped(sql.reserve.pipe(SqliteClient.readOnly));
    const retired = yield* released.executeUnprepared("SELECT 1", [], undefined).pipe(Effect.flip);
    assert.include(String(retired.reason.cause), "generation-retired");
    assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 2);
    yield* sql.withTransaction(Effect.gen(function* () {
      yield* sql`INSERT INTO entries VALUES ('writer-affinity')`;
      yield* Effect.scoped(Effect.gen(function* () {
        const connection = yield* sql.reserve;
        assert.deepEqual(
          yield* connection.executeUnprepared("SELECT COUNT(*) AS count FROM entries", [], undefined),
          [{ count: 3 }],
        );
      })).pipe(SqliteClient.readOnly);
    }));
    assert.equal(startedWorkers.length - before, size);
  }), { readerPool: readerPoolOptions(size), readerFailureMode: "fail" }),
);

it.effect.each([2, 3] as const)("keeps one reader generation and snapshot through nested savepoint rollback (size=%s)", (size) =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
    yield* sql`INSERT INTO entries VALUES ('first')`;
    const snapshot = yield* Deferred.make<void>();
    const committed = yield* Deferred.make<void>();
    const reader = yield* Effect.forkChild(sql.withTransaction(Effect.gen(function* () {
      assert.equal(yield* countEntries(sql), 1);
      yield* sql`PRAGMA cache_size = 321`;
      yield* Deferred.succeed(snapshot, undefined);
      yield* Deferred.await(committed);
      yield* sql.withTransaction(Effect.gen(function* () {
        assert.equal(yield* countEntries(sql), 1);
        assert.deepEqual(yield* sql`PRAGMA cache_size`, [{ cache_size: 321 }]);
        return yield* Effect.fail("rollback-savepoint");
      })).pipe(Effect.flip);
      assert.equal(yield* countEntries(sql), 1);
    })).pipe(SqliteClient.readOnly), { startImmediately: true });
    yield* Deferred.await(snapshot);
    yield* sql`INSERT INTO entries VALUES ('later')`;
    yield* Deferred.succeed(committed, undefined);
    yield* Fiber.join(reader);
    assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 2);
  }), { readerPool: readerPoolOptions(size), readerFailureMode: "fail" }),
);

it.effect("keeps interrupted native SQL on its occupied shared slot until its reply", () =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
    yield* sql`INSERT INTO entries VALUES ('first')`;
    const { port1, port2 } = new NodeWorkerThreads.MessageChannel();
    const gate = new Int32Array(new SharedArrayBuffer(4));
    const unblock = () => { Atomics.store(gate, 0, 1); Atomics.notify(gate, 0); };
    const started = new Promise<void>((resolve) => port1.once("message", () => resolve()));
    readerDiagnostic.port = port2;
    readerDiagnostic.gate = gate.buffer;
    yield* Effect.gen(function* () {
      const before = startedWorkers.length;
      const running = yield* Effect.forkChild(sql`SELECT COUNT(*) AS count FROM entries /* __reader_progress__ */`.pipe(SqliteClient.readOnly, withReadPriority("background")), { startImmediately: true });
      yield* Effect.promise(() => started);
      readerDiagnostic.port = undefined;
      readerDiagnostic.gate = undefined;
      const cancelled = yield* Effect.forkChild(Fiber.interrupt(running), { startImmediately: true });
      const queued = yield* Effect.forkChild(countEntries(sql).pipe(SqliteClient.readOnly, withReadPriority("background")), { startImmediately: true });
      assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
      assert.isUndefined(cancelled.pollUnsafe());
      assert.isUndefined(queued.pollUnsafe());
      assert.equal(startedWorkers.length - before, 2);
      yield* sql`INSERT INTO entries VALUES ('writer-progress')`;
      unblock();
      yield* Fiber.join(cancelled);
      assert.equal(yield* Fiber.join(queued), 2);
    }).pipe(Effect.ensuring(Effect.sync(() => {
      unblock(); port1.close(); port2.close(); readerDiagnostic.port = undefined; readerDiagnostic.gate = undefined;
    })));
  }), { readerPool: readerPoolOptions(2), readerFailureMode: "fail" }),
);

it.effect("fails a crashed pooled transaction as a unit and opens only a fresh later generation", () =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
    yield* sql`INSERT INTO entries VALUES ('first')`;
    const before = startedWorkers.length;
    const error = yield* sql.withTransaction(Effect.gen(function* () {
      assert.equal(yield* countEntries(sql), 1);
      yield* Effect.promise(() => startedWorkers.at(-1)!.terminate());
      return yield* countEntries(sql);
    })).pipe(SqliteClient.readOnly, withReadPriority("background"), Effect.flip);
    assert.equal(error._tag, "SqlError");
    assert.equal(startedWorkers.length - before, 1);
    const cooling = yield* countEntries(sql).pipe(SqliteClient.readOnly, withReadPriority("background"), Effect.flip);
    assert.include(String(cooling.reason.cause), "reader-unavailable");
    yield* TestClock.adjust("1 second");
    assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly, withReadPriority("background")), 1);
    assert.equal(startedWorkers.length - before, 2);
  }), { readerPool: readerPoolOptions(2), readerFailureMode: "fail" }),
);

it.effect("never falls back to the synchronous writer for an enabled pool", () =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
    yield* sql`INSERT INTO entries VALUES ('first')`;
    const prepare = vi.spyOn(NodeSqlite.DatabaseSync.prototype, "prepare");
    yield* Effect.gen(function* () {
      const error = yield* sql`SELECT COUNT(*) AS count FROM entries /* pool-fail-closed */`.pipe(SqliteClient.readOnly, Effect.flip);
      assert.equal(error._tag, "SqlError");
      assert.isFalse(prepare.mock.calls.some(([statement]) => statement.includes("pool-fail-closed")));
      assert.equal(yield* countEntries(sql), 1);
    }).pipe(Effect.ensuring(Effect.sync(() => prepare.mockRestore())));
  }), { readerPool: readerPoolOptions(2), readerFailureMode: "fallback" }),
);

it.effect("drains a pinned pool snapshot under real WAL pressure before admitting a new reader", () =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
    yield* sql`INSERT INTO entries VALUES ('first')`;
    const checkpoint = startedWorkers.at(-1)!;
    assert.isAbove(checkpoint.threadId, 0, "unavailable coverage: real checkpoint-worker APIs are required");
    const release = yield* Deferred.make<void>();
    const pinned = yield* Deferred.make<void>();
    const reader = yield* Effect.forkChild(sql.withTransaction(Effect.gen(function* () {
      assert.equal(yield* countEntries(sql), 1);
      yield* Deferred.succeed(pinned, undefined);
      yield* Deferred.await(release);
      // Statement continuation and rollback/commit never re-enter the new-lease gate.
      assert.equal(yield* countEntries(sql), 1);
    })).pipe(SqliteClient.readOnly, withReadPriority("background")), { startImmediately: true });
    yield* Deferred.await(pinned);
    let resolveBlocked: () => void = () => {};
    const blocked = new Promise<void>((resolve) => { resolveBlocked = resolve; });
    const observe = (message: unknown) => {
      if (typeof message === "object" && message !== null && "type" in message && message.type === "checkpoint" && "blocked" in message && message.blocked === true) resolveBlocked();
    };
    checkpoint.on("message", observe);
    yield* Effect.gen(function* () {
      yield* sql`PRAGMA journal_size_limit = 1024`;
      yield* sql`INSERT INTO entries VALUES (${"x".repeat(8192)})`;
      yield* Effect.promise(() => blocked);
      const before = startedWorkers.length;
      const next = yield* Effect.forkChild(countEntries(sql).pipe(SqliteClient.readOnly), { startImmediately: true });
      assert.isUndefined(next.pollUnsafe());
      assert.equal(startedWorkers.length, before, "pressure must gate before the reserved reader starts");
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(reader);
      assert.equal(yield* Fiber.join(next), 2);
      yield* sql`INSERT INTO entries VALUES ('writer-recovered')`;
      assert.equal(yield* countEntries(sql), 3);
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)), Effect.ensuring(Effect.sync(() => checkpoint.off("message", observe))));
  }), { readerPool: readerPoolOptions(2), readerFailureMode: "fail", checkpointWorker: true }),
);

it.effect("retires a timed-out pooled startup, fences late ready, and retries only on later demand", () =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
    yield* sql`INSERT INTO entries VALUES ('first')`;
    const before = startedWorkers.length;
    silentWorkers.on = true;
    silentWorkers.readyOnTerminate = true;
    yield* Effect.gen(function* () {
      const reservationScope = yield* Scope.make();
      yield* Effect.addFinalizer((exit) => Scope.close(reservationScope, exit));
      const reserve = Scope.provide(sql.reserve.pipe(SqliteClient.readOnly, withReadPriority("background")), reservationScope);
      const first = yield* Effect.forkChild(reserve.pipe(Effect.flip), { startImmediately: true });
      yield* TestClock.adjust("100 millis");
      assert.equal((yield* Fiber.join(first))._tag, "SqlError");
      const old = startedWorkers.at(-1)!;
      assert.equal(workerTerminals.get(old)!.terminationRequests, 1);
      assert.isDefined(workerTerminals.get(old)!.exitCode);
      old.emit("message", { type: "ready" });
      const cooling = yield* countEntries(sql).pipe(SqliteClient.readOnly, withReadPriority("background"), Effect.flip);
      assert.include(String(cooling.reason.cause), "reader-unavailable");
      assert.equal(startedWorkers.length - before, 1);
      silentWorkers.on = false;
      silentWorkers.readyOnTerminate = false;
      yield* TestClock.adjust("1 second");
      const recovered = yield* reserve;
      assert.deepEqual(yield* recovered.executeUnprepared("SELECT COUNT(*) AS count FROM entries", [], undefined), [{ count: 1 }]);
      assert.equal(startedWorkers.length - before, 2);
    }).pipe(Effect.scoped, Effect.ensuring(Effect.sync(() => { silentWorkers.on = false; silentWorkers.readyOnTerminate = false; })));
  }), { readerPool: readerPoolOptions(2), readerStartupTimeout: "100 millis", readerFailureMode: "fail" }),
);

it.effect("serves a warm reserved reader while a shared slot waits for cold startup", () =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
    yield* sql`INSERT INTO entries VALUES ('first')`;
    assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
    silentWorkers.on = true;
    yield* Effect.gen(function* () {
      const starting = yield* Effect.forkChild(
        countEntries(sql).pipe(SqliteClient.readOnly, withReadPriority("background"), Effect.flip),
        { startImmediately: true },
      );
      assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
      assert.isUndefined(starting.pollUnsafe());
      yield* TestClock.adjust("100 millis");
      assert.equal((yield* Fiber.join(starting))._tag, "SqlError");
    }).pipe(Effect.ensuring(Effect.sync(() => { silentWorkers.on = false; })));
  }), { readerPool: readerPoolOptions(2), readerStartupTimeout: "100 millis", readerFailureMode: "fail" }),
);
