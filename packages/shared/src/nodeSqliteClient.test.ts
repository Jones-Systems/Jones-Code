import * as NodeSqlite from "node:sqlite";
import * as NodeWorkerThreads from "node:worker_threads";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as Queue from "effect/Queue";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import * as Statement from "effect/sql/Statement";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import type * as Tracer from "effect/Tracer";
import { vi } from "vite-plus/test";

import * as SqliteClient from "./nodeSqliteClient.ts";
import { withReadPriority } from "./jones/sqliteReadPriority.ts";
import type { SqliteReaderPoolOptions } from "./jones/sqliteReaderContracts.ts";

const startedWorkers = vi.hoisted((): Array<NodeWorkerThreads.Worker> => []);
const workerExits = vi.hoisted(() => new WeakMap<NodeWorkerThreads.Worker, Promise<void>>());
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
const checkpointWorkers = vi.hoisted((): Array<NodeWorkerThreads.Worker> => []);

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
      workerExits.set(this, new Promise((resolve) => this.once("exit", () => resolve())));
      if (args[1]?.workerData?.frames !== undefined) checkpointWorkers.push(this);
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

it.effect.each([false, true])("batches worker results without losing rows (reader=%s)", (reader) =>
  Effect.gen(function* () {
    const { filename } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const route = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        reader ? effect.pipe(SqliteClient.readOnly) : effect;
      yield* route(sql`SELECT 1`);
      const worker = startedWorkers
        .filter((candidate) => !checkpointWorkers.includes(candidate))
        .at(-1)!;
      const batches: Array<ReadonlyArray<unknown>> = [];
      const record = (reply: { rows?: ReadonlyArray<unknown> }) => {
        if (Array.isArray(reply.rows) && reply.rows.length > 0) batches.push(reply.rows);
      };
      worker.prependListener("message", record);
      yield* Effect.gen(function* () {
        const text = "x".repeat(16 * 1024);
        const query = sql<{ n: number; text: string }>`
          WITH RECURSIVE sequence(n) AS (
            SELECT 1 UNION ALL SELECT n + 1 FROM sequence WHERE n < 300
          ) SELECT n, ${text} AS text FROM sequence
        `;
        const rows = yield* reader ? route(query.values) : query;
        assert.equal(rows.length, 300);
        for (const [index, row] of rows.entries()) {
          assert.deepEqual(row, reader ? [index + 1, text] : { n: index + 1, text });
        }
        assert.isAbove(batches.length, 1);
        for (const batch of batches) {
          assert.isAtMost(batch.length, 16_384);
          assert.isAtMost(batch.length * text.length * 2, 8 * 1024 * 1024);
        }
        batches.length = 0;
        const narrow = sql<{ n: number }>`
          WITH RECURSIVE sequence(n) AS (
            SELECT 1 UNION ALL SELECT n + 1 FROM sequence WHERE n < 10000
          ) SELECT n FROM sequence
        `;
        const narrowRows = yield* reader ? route(narrow.values) : narrow;
        assert.equal(narrowRows.length, 10_000);
        assert.deepEqual(narrowRows.at(-1), reader ? [10_000] : { n: 10_000 });
        assert.equal(batches.length, 1);
      }).pipe(Effect.ensuring(Effect.sync(() => worker.off("message", record))));
    }).pipe(
      Effect.provide(SqliteClient.layer({ filename, writerWorker: true, readerWorker: true })),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("reuses a worker query after an iterator constraint failure", () =>
  Effect.gen(function* () {
    const { filename } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE unique_entries(name TEXT UNIQUE)`;
      const insert = (name: string) =>
        sql<{ name: string }>`INSERT INTO unique_entries VALUES (${name}) RETURNING name`;
      yield* insert("first");
      const error = yield* insert("first").pipe(Effect.flip);
      assert.equal(error.reason._tag, "UniqueViolation");
      assert.deepEqual(yield* insert("second"), [{ name: "second" }]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("commits on the worker writer after a returning write timed out on a lock", () =>
  Effect.gen(function* () {
    const { filename, other } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA busy_timeout = 0`;
      yield* Effect.sync(() => other.exec("BEGIN IMMEDIATE"));
      const error = yield* sql`UPDATE counters SET value = 1 WHERE id = 1 RETURNING value`.pipe(
        Effect.flip,
      );
      assert.equal(error.reason._tag, "LockTimeoutError");
      yield* Effect.sync(() => other.exec("COMMIT"));

      yield* sql.withTransaction(sql`UPDATE counters SET value = 2 WHERE id = 1`);
      assert.deepEqual(yield* sql`SELECT value FROM counters WHERE id = 1`.values, [[2]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each([false, true])("keeps another connection from committing between a transaction's read and write (worker=%s)", (writerWorker) =>
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
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker })));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("waits for another process's lock while the database opens", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-open-" });
    const filename = path.join(directory, "state.sqlite");
    const seed = new NodeSqlite.DatabaseSync(filename);
    seed.exec("PRAGMA journal_mode = WAL; CREATE TABLE entries(name TEXT NOT NULL)");
    seed.close();
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const holder = yield* spawner.spawn(
      ChildProcess.make(process.execPath, [
        "-e",
        `const db = new (require("node:sqlite").DatabaseSync)(process.argv[1]);
        db.exec("PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE");
        console.log("held");
        setTimeout(() => db.close(), 300);`,
        filename,
      ]),
    );
    yield* Stream.runHead(holder.stdout);
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* sql`SELECT COUNT(*) FROM entries`.values, [[0]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each([false, true])("reports a transaction blocked by another writer as a lock timeout (worker=%s)", (writerWorker) =>
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
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker })));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("lets the event loop run while a write waits for another connection's lock", () =>
  Effect.gen(function* () {
    const { filename, other } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA busy_timeout = 5000`;
      yield* Effect.sync(() => other.exec("BEGIN IMMEDIATE"));
      const write = yield* Effect.forkChild(
        sql`UPDATE counters SET value = value + 1 WHERE id = 1`,
        { startImmediately: true },
      );
      // A writer on the event loop sleeps in SQLite's busy handler here, so this commit
      // cannot run until that write has already timed out.
      yield* eventLoopTurn;
      yield* Effect.sync(() => other.exec("UPDATE counters SET value = 10 WHERE id = 1; COMMIT"));
      yield* Fiber.join(write);
      assert.deepEqual(yield* sql`SELECT value FROM counters WHERE id = 1`.values, [[11]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("finishes a write already sent to the writer before its caller is interrupted", () =>
  Effect.gen(function* () {
    const { filename, other } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA busy_timeout = 5000`;
      yield* Effect.sync(() => other.exec("BEGIN IMMEDIATE"));
      const write = yield* Effect.forkChild(
        sql`UPDATE counters SET value = value + 1 WHERE id = 1`,
        { startImmediately: true },
      );
      yield* eventLoopTurn;
      const interrupted = yield* Effect.forkChild(Fiber.interrupt(write), {
        startImmediately: true,
      });
      yield* eventLoopTurn;
      // The write is waiting in the worker for the lock, so the interrupt has to wait too.
      assert.isUndefined(interrupted.pollUnsafe());
      yield* Effect.sync(() => other.exec("COMMIT"));
      yield* Fiber.join(interrupted);
      assert.deepEqual(yield* sql`SELECT value FROM counters WHERE id = 1`.values, [[1]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
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

it.effect("serves another read while a read-only transaction is open", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const opened = yield* Deferred.make<void>();
      const other = yield* Effect.forkChild(
        Deferred.await(opened).pipe(Effect.andThen(countEntries(sql).pipe(SqliteClient.readOnly))),
      );
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* countEntries(sql);
            yield* Deferred.succeed(opened, undefined);
            // One shared reader would make this join wait on the transaction forever.
            assert.equal(yield* Fiber.join(other), 1);
          }),
        )
        .pipe(SqliteClient.readOnly);
    }),
  ),
);

it.effect("gives a queued read the first reader that frees up", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const holders = yield* Effect.forEach([0, 1, 2, 3], () =>
        Effect.gen(function* () {
          const held = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const fiber = yield* sql
            .withTransaction(
              countEntries(sql).pipe(
                Effect.andThen(Deferred.succeed(held, undefined)),
                Effect.andThen(Deferred.await(release)),
              ),
            )
            .pipe(SqliteClient.readOnly, Effect.forkChild);
          yield* Deferred.await(held);
          return { fiber, release };
        }),
      );
      const cancelled = yield* Effect.uninterruptibleMask((restore) =>
        SqliteClient.readOnly(restore(sql.withTransaction(countEntries(sql)))),
      ).pipe((effect) => Effect.forkChild(effect, { startImmediately: true }));
      yield* Fiber.interrupt(cancelled);
      const cancelledRead = yield* Effect.uninterruptibleMask((restore) =>
        SqliteClient.readOnly(restore(countEntries(sql))),
      ).pipe((effect) => Effect.forkChild(effect, { startImmediately: true }));
      yield* Fiber.interrupt(cancelledRead);
      const queuedSpan = yield* Deferred.make<Tracer.Span>();
      const observeQueued: Statement.Transformer = (statement, _sql, _fiber, span) =>
        Deferred.succeed(queuedSpan, span).pipe(Effect.as(statement));
      const queued = yield* countEntries(sql).pipe(
        Effect.provideService(Statement.CurrentTransformer, observeQueued),
        Effect.provideService(Statement.SpanPropagationEnabled, true),
        SqliteClient.readOnly,
        (effect) => Effect.forkChild(effect, { startImmediately: true }),
      );
      // Masked read-only work keeps its cancellation pending and still runs once a reader frees up.
      let maskedRan = 0;
      const maskedRuns = yield* Effect.forEach(
        [
          Effect.scoped(
            Effect.flatMap(sql.reserve, (connection) =>
              connection.executeUnprepared("SELECT 1", [], undefined),
            ),
          ).pipe(Effect.asVoid),
          sql.withTransaction(countEntries(sql)).pipe(Effect.asVoid),
        ],
        (effect) =>
          effect.pipe(
            Effect.andThen(Effect.sync(() => maskedRan++)),
            Effect.uninterruptible,
            SqliteClient.readOnly,
            (masked) => Effect.forkChild(masked, { startImmediately: true }),
          ),
      );
      const interrupting = yield* Effect.forEach(maskedRuns, (fiber) =>
        Effect.forkChild(Fiber.interrupt(fiber), { startImmediately: true }),
      );
      yield* TestClock.adjust("1 second");
      yield* Deferred.succeed(holders[1]!.release, undefined);
      yield* Fiber.join(holders[1]!.fiber);
      // Pinned to the first reader it saw, this read would wait for that reader's transaction.
      assert.equal(yield* Fiber.join(queued), 1);
      assert.equal((yield* Deferred.await(queuedSpan)).attributes.get("db.lock_wait_ms"), 1000);
      for (const { fiber, release } of [holders[0]!, holders[2]!, holders[3]!]) {
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(fiber);
      }
      yield* Effect.forEach(interrupting, Fiber.join);
      assert.equal(maskedRan, 2);
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
    | "readerStartupTimeout"
    | "readerFailureMode"
    | "readerWorker"
    | "readerPool"
    | "checkpointWorker"
    | "writerWorker"
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

it.effect.each([
  { writerWorker: false, configured: false }, { writerWorker: true, configured: false },
  { writerWorker: false, configured: true }, { writerWorker: true, configured: true },
])("waits for legacy reader retirement through interrupted close (worker=$writerWorker, configured=$configured)", ({ writerWorker, configured }) =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const retired = yield* Deferred.make<void>();
    let expectedStarts = startedWorkers.length + 1;
    let writer: NodeWorkerThreads.Worker | undefined;
    let releaseTermination = () => {};
    silentWorkers.on = false;
    silentWorkers.terminationGate = new Promise<void>((resolve) => {
      releaseTermination = resolve;
    });
    yield* Effect.gen(function* () {
      const work = yield* Effect.forkChild(
        withFileClient(
          (sql) =>
            Effect.gen(function* () {
              yield* sql`PRAGMA journal_mode = WAL`;
              expectedStarts = startedWorkers.length + 1;
              writer = writerWorker ? startedWorkers.filter((worker) => !checkpointWorkers.includes(worker)).at(-1) : undefined;
              silentWorkers.on = true;
              yield* Deferred.succeed(started, undefined);
              const result = yield* sql`SELECT 1`.pipe(SqliteClient.readOnly, Effect.result);
              assert.equal(result._tag, configured ? "Failure" : "Success");
              yield* Deferred.succeed(retired, undefined);
            }),
          { writerWorker, ...(configured ? { readerStartupTimeout: "100 millis", readerFailureMode: "fail" as const } : {}) },
        ),
        { startImmediately: true },
      );
      yield* Deferred.await(started);
      yield* TestClock.adjust(configured ? "100 millis" : "10 seconds");
      yield* Deferred.await(retired);
      yield* eventLoopTurn;
      const reader = startedWorkers.at(-1)!;
      assert.isUndefined(work.pollUnsafe());
      assert.equal(workerTerminals.get(reader)!.terminationRequests, 1);
      const interrupted = yield* Effect.forkChild(Fiber.interrupt(work), {
        startImmediately: true,
      });
      assert.isUndefined(interrupted.pollUnsafe());
      if (writer !== undefined) assert.isAbove(writer.threadId, 0);
      reader.emit("message", { type: "ready" });
      assert.equal(startedWorkers.length, expectedStarts);
      releaseTermination();
      yield* Fiber.join(interrupted);
      if (writer !== undefined) assert.equal(writer.threadId, -1);
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

it.effect.each([
  { writerWorker: false, configured: false }, { writerWorker: true, configured: false },
  { writerWorker: false, configured: true }, { writerWorker: true, configured: true },
])("does not replace an unproved legacy reader or close its writer (worker=$writerWorker, configured=$configured)", ({ writerWorker, configured }) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "s1-legacy-retirement-" });
    const scope = yield* Scope.make();
    const prepare = vi.spyOn(NodeSqlite.DatabaseSync.prototype, "prepare");
    let database: NodeSqlite.DatabaseSync | undefined;
    let writer: NodeWorkerThreads.Worker | undefined;
    let reader: NodeWorkerThreads.Worker | undefined;
    let restoreClose = () => {};
    let assertCloseRefused = () => {};
    yield* Effect.gen(function* () {
      const context = yield* Layer.build(SqliteClient.layer({
        filename: path.join(directory, "state.sqlite"), readerWorker: true, writerWorker,
        ...(configured ? { readerStartupTimeout: "100 millis", readerFailureMode: "fail" as const } : {}),
      })).pipe(Effect.provideService(Scope.Scope, scope));
      const sql = Context.get(context, SqlClient.SqlClient);
      yield* sql`PRAGMA journal_mode = WAL`;
      database = prepare.mock.contexts.at(-1);
      prepare.mockRestore();
      writer = writerWorker ? startedWorkers.filter((worker) => !checkpointWorkers.includes(worker)).at(-1) : undefined;
      if (writerWorker) {
        const close = vi.spyOn(writer!, "postMessage");
        restoreClose = () => close.mockRestore();
        assertCloseRefused = () => {
          assert.isFalse(close.mock.calls.some(([message]) =>
            typeof message === "object" && message !== null && "type" in message && message.type === "close"));
          assert.isAbove(writer!.threadId, 0);
        };
      } else {
        const close = vi.spyOn(database!, "close");
        restoreClose = () => close.mockRestore();
        assertCloseRefused = () => assert.equal(close.mock.calls.length, 0);
      }
      const startedBefore = startedWorkers.length;
      silentWorkers.on = true;
      silentWorkers.rejectTermination = true;
      const read = yield* Effect.forkChild(sql`SELECT 1`.pipe(SqliteClient.readOnly, Effect.result), { startImmediately: true });
      yield* TestClock.adjust(configured ? "100 millis" : "10 seconds");
      assert.equal((yield* Fiber.join(read))._tag, configured ? "Failure" : "Success");
      reader = startedWorkers.at(-1)!;
      yield* Effect.promise(() => workerExits.get(reader!)!);
      yield* eventLoopTurn;
      silentWorkers.on = false;
      silentWorkers.rejectTermination = false;
      yield* TestClock.adjust("1 minute");
      assert.equal((yield* sql`SELECT 1`.pipe(SqliteClient.readOnly, Effect.result))._tag,
        configured ? "Failure" : "Success");
      assert.equal(startedWorkers.length, startedBefore + 1);
      assert.equal(workerTerminals.get(reader)!.terminationRequests, 1);
      assert.deepEqual(yield* sql`SELECT 1 AS value`, [{ value: 1 }]);
      const closed = yield* Effect.exit(Scope.close(scope, Exit.void));
      assert.isTrue(Exit.isFailure(closed));
      if (Exit.isFailure(closed)) assert.include(Cause.pretty(closed.cause), "SQLite writer retained because reader termination is unproved");
      assertCloseRefused();
    }).pipe(Effect.ensuring(Effect.gen(function* () {
      silentWorkers.on = false;
      silentWorkers.rejectTermination = false;
      yield* Effect.exit(Scope.close(scope, Exit.void));
      // Test-owned recovery only: prove reader exit directly before releasing the retained writer.
      yield* Effect.promise(async () => {
        if (reader !== undefined) await reader.terminate();
        if (writer !== undefined && writer.threadId !== -1) {
          const exited = workerExits.get(writer)!;
          writer.ref();
          // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node worker close after fixture-owned reader exit proof.
          writer.postMessage({ type: "close" });
          await exited;
        } else if (!writerWorker && database?.isOpen) database.close();
      });
      restoreClose();
      prepare.mockRestore();
    })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
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

      // A read beside an open read transaction must not start a worker on the next slot.
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const holder = yield* sql
        .withTransaction(
          countEntries(sql).pipe(
            Effect.andThen(Deferred.succeed(held, undefined)),
            Effect.andThen(Deferred.await(release)),
          ),
        )
        .pipe(SqliteClient.readOnly, Effect.forkChild);
      yield* Deferred.await(held);
      const other = yield* countEntries(sql).pipe(SqliteClient.readOnly, (effect) =>
        Effect.forkChild(effect, { startImmediately: true }),
      );
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(holder);
      assert.equal(yield* Fiber.join(other), 1);
      assert.equal(startedWorkers.length, startedBefore + 1);
    }),
  ),
);

const withLogLines = <A, E, R>(
  use: (lines: Queue.Queue<ReadonlyArray<unknown>>) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const lines = yield* Queue.unbounded<ReadonlyArray<unknown>>();
    const logger = Logger.make(({ logLevel, message }) => {
      Queue.offerUnsafe(lines, [logLevel, ...(message as ReadonlyArray<unknown>)]);
    });
    return yield* use(lines).pipe(
      Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
    );
  });

const readConnection = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const spans: Array<Tracer.Span> = [];
    const observe: Statement.Transformer = (statement, _sql, _fiber, span) =>
      Effect.sync(() => {
        spans.push(span);
        return statement;
      });
    const count = yield* countEntries(sql).pipe(
      SqliteClient.readOnly,
      Effect.provideService(Statement.CurrentTransformer, observe),
      Effect.provideService(Statement.SpanPropagationEnabled, true),
    );
    return { count, connection: spans.map((span) => span.attributes.get("db.connection")) };
  });

const withWorkers = <A, E, R>(
  mode: { readonly failWith?: string; readonly silent?: boolean },
  effect: Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      failingWorkers.message = mode.failWith;
      silentWorkers.on = mode.silent ?? false;
    }),
    () => effect,
    () =>
      Effect.sync(() => {
        failingWorkers.message = undefined;
        silentWorkers.on = false;
      }),
  );

const unavailableWarning = "SQLite reader worker unavailable; reads use the writer";

const notWal = "database uses journal_mode delete, not wal";

it.effect.each([
  {
    failure: "a crash",
    workers: { failWith: "reader start failed" },
    startup: Duration.zero,
    reason: "Database reader failed: reader start failed",
  },
  {
    failure: "a startup timeout",
    workers: { silent: true },
    startup: Duration.seconds(10),
    reason: "Database reader did not open the database within 10000 ms",
  },
])(
  "reads on the writer after $failure until a retry starts the reader again",
  ({ workers, startup, reason }) =>
    withLogLines((lines) =>
      withReaderClient((sql) =>
        Effect.gen(function* () {
          const failed = yield* withWorkers(
            workers,
            Effect.gen(function* () {
              const read = yield* Effect.forkChild(readConnection(sql), {
                startImmediately: true,
              });
              yield* TestClock.adjust(startup);
              return yield* Fiber.join(read);
            }),
          );
          assert.deepEqual(failed, { count: 1, connection: ["writer"] });
          const startedBefore = startedWorkers.length;
          assert.deepEqual(yield* readConnection(sql), { count: 1, connection: ["writer"] });
          assert.equal(startedWorkers.length, startedBefore);
          assert.deepEqual(yield* Queue.take(lines), ["Warn", unavailableWarning, { reason }]);

          yield* TestClock.adjust("1 second");
          assert.equal(startedWorkers.length, startedBefore + 1);
          assert.deepEqual(yield* Queue.take(lines), [
            "Info",
            "SQLite reader worker available again; reads use the reader workers",
          ]);
          assert.deepEqual(yield* readConnection(sql), { count: 1, connection: ["reader"] });
          assert.equal(Queue.sizeUnsafe(lines), 0);
        }),
      ),
    ),
);

it.effect("backs off a reader that never starts without logging each retry", () =>
  withLogLines((lines) =>
    withReaderClient((sql) =>
      withWorkers(
        { silent: true },
        Effect.gen(function* () {
          const read = yield* Effect.forkChild(readConnection(sql), { startImmediately: true });
          yield* TestClock.adjust("10 seconds");
          assert.deepEqual(yield* Fiber.join(read), { count: 1, connection: ["writer"] });

          const startedBefore = startedWorkers.length;
          // Each retry waits 10 s for startup; the gaps between them double from 1 s up to a minute.
          yield* TestClock.adjust("10 minutes");
          assert.equal(startedWorkers.length - startedBefore, 12);
          assert.equal(Queue.sizeUnsafe(lines), 1);
        }),
      ),
    ),
  ),
);

it.effect.each([
  { phase: "the retry waits", beforeClose: Duration.zero, retries: 0 },
  { phase: "a retried reader starts", beforeClose: Duration.seconds(1), retries: 1 },
])("stops retrying the reader when the client closes while $phase", ({ beforeClose, retries }) =>
  withWorkers(
    { silent: true },
    Effect.gen(function* () {
      const startedAtClose = yield* withReaderClient((sql) =>
        Effect.gen(function* () {
          const read = yield* Effect.forkChild(readConnection(sql), { startImmediately: true });
          yield* TestClock.adjust("10 seconds");
          assert.deepEqual(yield* Fiber.join(read), { count: 1, connection: ["writer"] });

          const startedBefore = startedWorkers.length;
          yield* TestClock.adjust(beforeClose);
          assert.equal(startedWorkers.length - startedBefore, retries);
          return startedWorkers.length;
        }),
      );
      yield* TestClock.adjust("10 minutes");
      assert.equal(startedWorkers.length, startedAtClose);
    }),
  ),
);

it.effect.each([
  {
    when: "the database is not in WAL mode",
    journalMode: "DELETE",
    workers: {},
    starts: 1,
    logged: [["Warn", unavailableWarning, { reason: notWal }]],
  },
  {
    when: "a retry finds the database out of WAL mode",
    journalMode: "WAL",
    workers: { failWith: "reader start failed" },
    starts: 2,
    logged: [
      ["Warn", unavailableWarning, { reason: "Database reader failed: reader start failed" }],
      [
        "Info",
        "SQLite reader worker retries stopped permanently; reads use the writer",
        { reason: notWal },
      ],
    ],
  },
])(
  "keeps readers off without further retries when $when",
  ({ journalMode, workers, starts, logged }) =>
    withLogLines((lines) =>
      withFileClient((sql) =>
        Effect.gen(function* () {
          yield* sql.unsafe(`PRAGMA journal_mode = ${journalMode}`);
          yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
          yield* sql`INSERT INTO entries VALUES ('kept')`;
          const startedBefore = startedWorkers.length;

          const failed = yield* withWorkers(workers, readConnection(sql));
          assert.deepEqual(failed, { count: 1, connection: ["writer"] });
          yield* sql`PRAGMA journal_mode = DELETE`;
          yield* TestClock.adjust("1 second");
          yield* Effect.promise(() => workerExits.get(startedWorkers.at(-1)!)!);
          yield* TestClock.adjust("1 hour");
          assert.deepEqual(yield* readConnection(sql), { count: 1, connection: ["writer"] });
          assert.equal(startedWorkers.length, startedBefore + starts);
          assert.deepEqual(yield* Queue.clear(lines), logged);
        }),
      ),
    ),
);

it.effect("fails with a typed SqlError when the database file is not SQLite", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-notadb-" });
    const filename = path.join(directory, "state.sqlite");
    yield* fs.writeFileString(filename, "not a db\n");

    const exit = yield* Layer.build(SqliteClient.layer({ filename, writerWorker: true })).pipe(
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    if (Exit.isFailure(exit)) assert.isTrue(SqlError.isSqlError(Cause.squash(exit.cause)));
    if (Exit.isFailure(exit)) assert.isFalse(Cause.hasDies(exit.cause));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
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
 ].flatMap((entry) => [{ ...entry, worker: false }, { ...entry, worker: true }]))(
  "preserves caller cancellation while waiting (reservation=$reservation, masked=$masked, worker=$worker)",
  ({ reservation, masked, worker }) => {
    const program = Effect.gen(function* () {
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
    });
    return worker ? withFileClient(() => program, { writerWorker: true })
      : program.pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);

const readerPoolOptions = (size: 2 | 3): SqliteReaderPoolOptions => ({
  size,
  interactiveQueueCapacity: 64,
  backgroundQueueCapacity: 64,
  interactiveQueueTimeoutMs: 10_000,
  backgroundQueueTimeoutMs: 30_000,
  leaseWatchdogMs: 60_000,
});

it.effect("annotates the real statement span for an ordinary pooled read", () =>
  withFileClient(
    (sql) =>
      Effect.gen(function* () {
        yield* sql`PRAGMA journal_mode = WAL`;
        const observed = yield* Deferred.make<Tracer.Span>();
        const observeStatement: Statement.Transformer = (statement, _sql, _fiber, span) =>
          Deferred.succeed(observed, span).pipe(Effect.as(statement));
        assert.deepEqual(
          yield* sql`SELECT 1 AS value`.pipe(
            SqliteClient.readOnly,
            Effect.provideService(Statement.CurrentTransformer, observeStatement),
            Effect.provideService(Statement.SpanPropagationEnabled, true),
          ),
          [{ value: 1 }],
        );
        const span = yield* Deferred.await(observed);
        assert.equal(span.attributes.get("db.connection"), "reader");
        const wait = span.attributes.get("db.lock_wait_ms");
        assert.typeOf(wait, "number");
        assert.isTrue(Number.isFinite(wait));
        assert.isAtLeast(wait as number, 0);
      }),
    { readerPool: readerPoolOptions(2), readerFailureMode: "fail" },
  ),
);

it.effect.each([2, 3] as const)(
  "opens exactly %s independent SQLite reader connections and keeps reservations pinned",
  (size) =>
    withFileClient(
      (sql) =>
        Effect.gen(function* () {
          yield* sql`PRAGMA journal_mode = WAL`;
          yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
          yield* sql`INSERT INTO entries VALUES ('first')`;
          const before = startedWorkers.length;
          const release = yield* Deferred.make<void>();
          const ready = yield* Effect.forEach(Array.from({ length: size }), () =>
            Deferred.make<void>(),
          );
          const holders = [];
          // Occupied shared connections force the last reservation onto the interactive reader.
          for (let index = 0; index < size; index++) {
            const indexNow = index;
            holders.push(
              yield* Effect.forkChild(
                Effect.scoped(
                  Effect.gen(function* () {
                    const connection = yield* sql.reserve;
                    yield* connection.executeUnprepared(
                      "PRAGMA cache_size = " + (100 + indexNow),
                      [],
                      undefined,
                    );
                    yield* Deferred.succeed(ready[indexNow]!, undefined);
                    yield* Deferred.await(release);
                    assert.deepEqual(
                      yield* connection.executeUnprepared("PRAGMA cache_size", [], undefined),
                      [{ cache_size: 100 + indexNow }],
                    );
                  }),
                ).pipe(
                  SqliteClient.readOnly,
                  withReadPriority(indexNow === size - 1 ? "interactive" : "background"),
                ),
                { startImmediately: true },
              ),
            );
            yield* Deferred.await(ready[index]!);
          }
          assert.equal(startedWorkers.length - before, size);
          yield* sql`INSERT INTO entries VALUES ('committed')`;
          yield* Deferred.succeed(release, undefined);
          for (const holder of holders) yield* Fiber.join(holder);
          const released = yield* Effect.scoped(sql.reserve.pipe(SqliteClient.readOnly));
          const retired = yield* released
            .executeUnprepared("SELECT 1", [], undefined)
            .pipe(Effect.flip);
          assert.include(String(retired.reason.cause), "generation-retired");
          assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 2);
          yield* sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`INSERT INTO entries VALUES ('writer-affinity')`;
              yield* Effect.scoped(
                Effect.gen(function* () {
                  const connection = yield* sql.reserve;
                  assert.deepEqual(
                    yield* connection.executeUnprepared(
                      "SELECT COUNT(*) AS count FROM entries",
                      [],
                      undefined,
                    ),
                    [{ count: 3 }],
                  );
                }),
              ).pipe(SqliteClient.readOnly);
            }),
          );
          assert.equal(startedWorkers.length - before, size);
        }),
      { readerPool: readerPoolOptions(size), readerFailureMode: "fail" },
    ),
);

it.effect.each([2, 3] as const)(
  "keeps one reader generation and snapshot through nested savepoint rollback (size=%s)",
  (size) =>
    withFileClient(
      (sql) =>
        Effect.gen(function* () {
          yield* sql`PRAGMA journal_mode = WAL`;
          yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
          yield* sql`INSERT INTO entries VALUES ('first')`;
          const snapshot = yield* Deferred.make<void>();
          const committed = yield* Deferred.make<void>();
          const reader = yield* Effect.forkChild(
            sql
              .withTransaction(
                Effect.gen(function* () {
                  assert.equal(yield* countEntries(sql), 1);
                  yield* sql`PRAGMA cache_size = 321`;
                  yield* Deferred.succeed(snapshot, undefined);
                  yield* Deferred.await(committed);
                  yield* sql
                    .withTransaction(
                      Effect.gen(function* () {
                        assert.equal(yield* countEntries(sql), 1);
                        assert.deepEqual(yield* sql`PRAGMA cache_size`, [{ cache_size: 321 }]);
                        return yield* Effect.fail("rollback-savepoint");
                      }),
                    )
                    .pipe(Effect.flip);
                  assert.equal(yield* countEntries(sql), 1);
                }),
              )
              .pipe(SqliteClient.readOnly),
            { startImmediately: true },
          );
          yield* Deferred.await(snapshot);
          yield* sql`INSERT INTO entries VALUES ('later')`;
          yield* Deferred.succeed(committed, undefined);
          yield* Fiber.join(reader);
          assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 2);
        }),
      { readerPool: readerPoolOptions(size), readerFailureMode: "fail" },
    ),
);

it.effect("keeps interrupted native SQL on its occupied shared slot until its reply", () =>
  withFileClient(
    (sql) =>
      Effect.gen(function* () {
        yield* sql`PRAGMA journal_mode = WAL`;
        yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
        yield* sql`INSERT INTO entries VALUES ('first')`;
        const { port1, port2 } = new NodeWorkerThreads.MessageChannel();
        const gate = new Int32Array(new SharedArrayBuffer(4));
        const unblock = () => {
          Atomics.store(gate, 0, 1);
          Atomics.notify(gate, 0);
        };
        const started = new Promise<void>((resolve) => port1.once("message", () => resolve()));
        readerDiagnostic.port = port2;
        readerDiagnostic.gate = gate.buffer;
        yield* Effect.gen(function* () {
          const before = startedWorkers.length;
          const running = yield* Effect.forkChild(
            sql`SELECT COUNT(*) AS count FROM entries /* __reader_progress__ */`.pipe(
              SqliteClient.readOnly,
              withReadPriority("background"),
            ),
            { startImmediately: true },
          );
          yield* Effect.promise(() => started);
          readerDiagnostic.port = undefined;
          readerDiagnostic.gate = undefined;
          const cancelled = yield* Effect.forkChild(Fiber.interrupt(running), {
            startImmediately: true,
          });
          const queued = yield* Effect.forkChild(
            countEntries(sql).pipe(SqliteClient.readOnly, withReadPriority("background")),
            { startImmediately: true },
          );
          assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
          assert.isUndefined(cancelled.pollUnsafe());
          assert.isUndefined(queued.pollUnsafe());
          assert.equal(startedWorkers.length - before, 2);
          yield* sql`INSERT INTO entries VALUES ('writer-progress')`;
          unblock();
          yield* Fiber.join(cancelled);
          assert.equal(yield* Fiber.join(queued), 2);
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              unblock();
              port1.close();
              port2.close();
              readerDiagnostic.port = undefined;
              readerDiagnostic.gate = undefined;
            }),
          ),
        );
      }),
    { readerPool: readerPoolOptions(2), readerFailureMode: "fail" },
  ),
);

it.effect(
  "fails a crashed pooled transaction as a unit and opens only a fresh later generation",
  () =>
    withFileClient(
      (sql) =>
        Effect.gen(function* () {
          yield* sql`PRAGMA journal_mode = WAL`;
          yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
          yield* sql`INSERT INTO entries VALUES ('first')`;
          const before = startedWorkers.length;
          const error = yield* sql
            .withTransaction(
              Effect.gen(function* () {
                assert.equal(yield* countEntries(sql), 1);
                yield* Effect.promise(() => startedWorkers.at(-1)!.terminate());
                return yield* countEntries(sql);
              }),
            )
            .pipe(SqliteClient.readOnly, withReadPriority("background"), Effect.flip);
          assert.equal(error._tag, "SqlError");
          assert.equal(startedWorkers.length - before, 1);
          const cooling = yield* countEntries(sql).pipe(
            SqliteClient.readOnly,
            withReadPriority("background"),
            Effect.flip,
          );
          assert.include(String(cooling.reason.cause), "reader-unavailable");
          yield* TestClock.adjust("1 second");
          assert.equal(
            yield* countEntries(sql).pipe(SqliteClient.readOnly, withReadPriority("background")),
            1,
          );
          assert.equal(startedWorkers.length - before, 2);
        }),
      { readerPool: readerPoolOptions(2), readerFailureMode: "fail" },
    ),
);

it.effect("never falls back to the synchronous writer for an enabled pool", () =>
  withFileClient(
    (sql) =>
      Effect.gen(function* () {
        yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
        yield* sql`INSERT INTO entries VALUES ('first')`;
        const prepare = vi.spyOn(NodeSqlite.DatabaseSync.prototype, "prepare");
        yield* Effect.gen(function* () {
          const error =
            yield* sql`SELECT COUNT(*) AS count FROM entries /* pool-fail-closed */`.pipe(
              SqliteClient.readOnly,
              Effect.flip,
            );
          assert.equal(error._tag, "SqlError");
          assert.isFalse(
            prepare.mock.calls.some(([statement]) => statement.includes("pool-fail-closed")),
          );
          assert.equal(yield* countEntries(sql), 1);
        }).pipe(Effect.ensuring(Effect.sync(() => prepare.mockRestore())));
      }),
    { readerPool: readerPoolOptions(2), readerFailureMode: "fallback" },
  ),
);

it.effect.each([false, true])(
  "drains a pinned pool snapshot under real WAL pressure before admitting a new reader (worker=%s)",
  (writerWorker) =>
    withFileClient(
      (sql) =>
        Effect.gen(function* () {
          yield* sql`PRAGMA journal_mode = WAL`;
          yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
          yield* sql`INSERT INTO entries VALUES ('first')`;
          const checkpoint = startedWorkers.at(-1)!;
          assert.isAbove(
            checkpoint.threadId,
            0,
            "unavailable coverage: real checkpoint-worker APIs are required",
          );
          const release = yield* Deferred.make<void>();
          const pinned = yield* Deferred.make<void>();
          const reader = yield* Effect.forkChild(
            sql
              .withTransaction(
                Effect.gen(function* () {
                  assert.equal(yield* countEntries(sql), 1);
                  yield* Deferred.succeed(pinned, undefined);
                  yield* Deferred.await(release);
                  // Statement continuation and rollback/commit never re-enter the new-lease gate.
                  assert.equal(yield* countEntries(sql), 1);
                }),
              )
              .pipe(SqliteClient.readOnly, withReadPriority("background")),
            { startImmediately: true },
          );
          yield* Deferred.await(pinned);
          let resolveBlocked: () => void = () => {};
          const blocked = new Promise<void>((resolve) => {
            resolveBlocked = resolve;
          });
          const observe = (message: unknown) => {
            if (
              typeof message === "object" &&
              message !== null &&
              "type" in message &&
              message.type === "checkpoint" &&
              "blocked" in message &&
              message.blocked === true
            )
              resolveBlocked();
          };
          checkpoint.on("message", observe);
          yield* Effect.gen(function* () {
            yield* sql`PRAGMA journal_size_limit = 1024`;
            yield* sql`INSERT INTO entries VALUES (${"x".repeat(8192)})`;
            yield* Effect.promise(() => blocked);
            if (writerWorker) {
              const waitingWrite = yield* Effect.forkChild(sql`INSERT INTO entries VALUES ('cancelled-waiter')`, { startImmediately: true });
              yield* eventLoopTurn;
              assert.isUndefined(waitingWrite.pollUnsafe());
              yield* Fiber.interrupt(waitingWrite);
            }
            const before = startedWorkers.length;
            const next = yield* Effect.forkChild(countEntries(sql).pipe(SqliteClient.readOnly), {
              startImmediately: true,
            });
            assert.isUndefined(next.pollUnsafe());
            assert.equal(
              startedWorkers.length,
              before,
              "pressure must gate before the reserved reader starts",
            );
            yield* Deferred.succeed(release, undefined);
            yield* Fiber.join(reader);
            assert.equal(yield* Fiber.join(next), 2);
            yield* sql`INSERT INTO entries VALUES ('writer-recovered')`;
            assert.equal(yield* countEntries(sql), 3);
          }).pipe(
            Effect.ensuring(Deferred.succeed(release, undefined)),
            Effect.ensuring(Effect.sync(() => checkpoint.off("message", observe))),
          );
        }),
      { readerPool: readerPoolOptions(2), readerFailureMode: "fail", checkpointWorker: true, writerWorker },
    ),
);

it.effect(
  "retires a timed-out pooled startup, fences late ready, and retries only on later demand",
  () =>
    withFileClient(
      (sql) =>
        Effect.gen(function* () {
          yield* sql`PRAGMA journal_mode = WAL`;
          yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
          yield* sql`INSERT INTO entries VALUES ('first')`;
          const before = startedWorkers.length;
          silentWorkers.on = true;
          silentWorkers.readyOnTerminate = true;
          yield* Effect.gen(function* () {
            const reservationScope = yield* Scope.make();
            yield* Effect.addFinalizer((exit) => Scope.close(reservationScope, exit));
            const reserve = Scope.provide(
              sql.reserve.pipe(SqliteClient.readOnly, withReadPriority("background")),
              reservationScope,
            );
            const first = yield* Effect.forkChild(reserve.pipe(Effect.flip), {
              startImmediately: true,
            });
            yield* TestClock.adjust("100 millis");
            assert.equal((yield* Fiber.join(first))._tag, "SqlError");
            const old = startedWorkers.at(-1)!;
            assert.equal(workerTerminals.get(old)!.terminationRequests, 1);
            assert.isDefined(workerTerminals.get(old)!.exitCode);
            old.emit("message", { type: "ready" });
            const cooling = yield* countEntries(sql).pipe(
              SqliteClient.readOnly,
              withReadPriority("background"),
              Effect.flip,
            );
            assert.include(String(cooling.reason.cause), "reader-unavailable");
            assert.equal(startedWorkers.length - before, 1);
            silentWorkers.on = false;
            silentWorkers.readyOnTerminate = false;
            yield* TestClock.adjust("1 second");
            const recovered = yield* reserve;
            assert.deepEqual(
              yield* recovered.executeUnprepared(
                "SELECT COUNT(*) AS count FROM entries",
                [],
                undefined,
              ),
              [{ count: 1 }],
            );
            assert.equal(startedWorkers.length - before, 2);
          }).pipe(
            Effect.scoped,
            Effect.ensuring(
              Effect.sync(() => {
                silentWorkers.on = false;
                silentWorkers.readyOnTerminate = false;
              }),
            ),
          );
        }),
      {
        readerPool: readerPoolOptions(2),
        readerStartupTimeout: "100 millis",
        readerFailureMode: "fail",
      },
    ),
);

it.effect("serves a warm reserved reader while a shared slot waits for cold startup", () =>
  withFileClient(
    (sql) =>
      Effect.gen(function* () {
        yield* sql`PRAGMA journal_mode = WAL`;
        yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
        yield* sql`INSERT INTO entries VALUES ('first')`;
        assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
        silentWorkers.on = true;
        yield* Effect.gen(function* () {
          const starting = yield* Effect.forkChild(
            countEntries(sql).pipe(
              SqliteClient.readOnly,
              withReadPriority("background"),
              Effect.flip,
            ),
            { startImmediately: true },
          );
          assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
          assert.isUndefined(starting.pollUnsafe());
          yield* TestClock.adjust("100 millis");
          assert.equal((yield* Fiber.join(starting))._tag, "SqlError");
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              silentWorkers.on = false;
            }),
          ),
        );
      }),
    {
      readerPool: readerPoolOptions(2),
      readerStartupTimeout: "100 millis",
      readerFailureMode: "fail",
    },
  ),
);

it.effect("keeps writes moving while an outside reader pins the WAL, then trims the WAL", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-pinned-wal-" });
    const filename = path.join(directory, "state.sqlite");
    // Stands in for `sqlite3 statev2.sqlite` or a backup script reading the database.
    const outside = yield* Effect.acquireRelease(
      Effect.sync(() => new NodeSqlite.DatabaseSync(filename)),
      (db) =>
        Effect.sync(() => {
          if (db.isTransaction) db.exec("COMMIT");
          db.close();
        }),
    );
    outside.exec("PRAGMA journal_mode = WAL; CREATE TABLE blobs(data BLOB NOT NULL)");
    const walSize = fs.stat(`${filename}-wal`).pipe(
      Effect.map((info) => Number(info.size)),
      Effect.orDie,
    );
    const limit = 8 * 1024 * 1024;
    const payload = new Uint8Array(1024 * 1024).fill(7);
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe(`PRAGMA journal_size_limit = ${limit}`);
      yield* sql`INSERT INTO blobs VALUES (${payload})`;
      outside.exec("BEGIN");
      outside.prepare("SELECT COUNT(*) FROM blobs").get();
      for (let write = 0; write < 24; write++) yield* sql`INSERT INTO blobs VALUES (${payload})`;
      assert.isAbove(yield* walSize, limit);
      outside.exec("COMMIT");
      const checkpointer = checkpointWorkers.at(-1)!;
      const caughtUp = Effect.callback<void>((resume) => {
        const onMessage = (message: { type?: unknown; log?: unknown; checkpointed?: unknown }) => {
          if (
            message?.type === "checked" &&
            typeof message.log === "number" &&
            message.log > 0 &&
            message.log === message.checkpointed
          ) {
            resume(Effect.void);
          }
        };
        checkpointer.on("message", onMessage);
        checkpointer.ref();
        return Effect.sync(() => {
          checkpointer.off("message", onMessage);
          checkpointer.unref();
        });
      });
      // The worker checkpoints the released frames; the next write restarts the WAL at the limit.
      yield* Effect.all([caughtUp, sql`DELETE FROM blobs WHERE rowid = 1`], { concurrency: 2 });
      yield* sql`DELETE FROM blobs WHERE rowid = 2`;
      assert.isAtMost(yield* walSize, limit);
      assert.deepEqual(yield* sql`SELECT COUNT(*) FROM blobs`.values, [[23]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);


it.effect("Jones pooled reads retain every ordered bigint across upstream chunk receipts", () =>
  withFileClient(
    (sql) => Effect.gen(function* () {
      yield* sql`PRAGMA journal_mode = WAL`;
      const rows = yield* sql<{ ordinal: bigint; payload: string }>`
        WITH RECURSIVE sequence(value) AS (
          SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 33000
        ) SELECT value AS ordinal, printf('%08d', value) AS payload FROM sequence ORDER BY value
      `.pipe(Effect.provideService(SqlClient.SafeIntegers, true), SqliteClient.readOnly);
      assert.equal(rows.length, 33000);
      for (let index = 0; index < rows.length; index++) {
        assert.equal(rows[index]!.ordinal, BigInt(index + 1));
        assert.equal(rows[index]!.payload, String(index + 1).padStart(8, "0"));
      }
      assert.deepEqual(yield* sql`SELECT 7 AS value`.pipe(SqliteClient.readOnly), [{ value: 7 }]);
    }),
    { readerPool: readerPoolOptions(2), readerFailureMode: "fail" },
  ),
);

it.effect("worker failed COMMIT preserves the original SQLite error and rolls back before reuse", () =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`CREATE TABLE commit_parent(id INTEGER PRIMARY KEY)`;
    yield* sql`CREATE TABLE commit_child(id INTEGER REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`;
    const error = yield* sql.withTransaction(sql`INSERT INTO commit_child VALUES (19)`).pipe(Effect.flip);
    assert.equal(error._tag, "SqlError");
    assert.equal((error.reason.cause as { errcode?: number }).errcode, 787);
    assert.deepEqual(yield* sql`SELECT * FROM commit_child`, []);
    yield* sql`INSERT INTO commit_parent VALUES (19)`;
    yield* sql.withTransaction(sql`INSERT INTO commit_child VALUES (19)`);
    assert.deepEqual(yield* sql`SELECT * FROM commit_child`, [{ id: 19 }]);
  }), { writerWorker: true }),
);

it.effect("worker loss fails the whole writer transaction and never recreates its connection", () =>
  withFileClient((sql) => Effect.gen(function* () {
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`CREATE TABLE writer_loss(value INTEGER)`;
    const writer = startedWorkers.filter((worker) => !checkpointWorkers.includes(worker)).at(-1)!;
    const before = startedWorkers.length;
    const failed = yield* sql.withTransaction(Effect.gen(function* () {
      yield* sql`INSERT INTO writer_loss VALUES (1)`;
      yield* Effect.promise(() => writer.terminate());
      yield* sql`INSERT INTO writer_loss VALUES (2)`;
    })).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(failed));
    assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`SELECT * FROM writer_loss`)));
    assert.equal(startedWorkers.length, before);
  }), { writerWorker: true }),
);
