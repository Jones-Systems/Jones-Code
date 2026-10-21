import * as NodeSqlite from "node:sqlite";
import * as NodeWorkerThreads from "node:worker_threads";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import { vi } from "vite-plus/test";
import * as SqliteClient from "../nodeSqliteClient.ts";

const startedWorkers = vi.hoisted((): Array<NodeWorkerThreads.Worker> => []);
const silentWorkers = vi.hoisted(() => ({ on: false, readyOnTerminate: false }));
const workerTerminals = vi.hoisted(
  () =>
    new WeakMap<
      NodeWorkerThreads.Worker,
      {
        error?: Error;
        exitCode?: number;
        terminationRequested: boolean;
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
      super(
        ...((silentWorkers.on
          ? ["setInterval(() => {}, 1 << 30);", { eval: true }]
          : failingWorkers.message !== undefined
            ? [`throw new Error(${JSON.stringify(failingWorkers.message)});`, { eval: true }]
            : args) as ConstructorParameters<typeof actual.Worker>),
      );
      const terminal = { terminationRequested: false } as {
        error?: Error;
        exitCode?: number;
        terminationRequested: boolean;
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
      workerTerminals.get(this)!.terminationRequested = true;
      // A "ready" already queued when termination starts reaches the parent before "exit".
      if (silentWorkers.readyOnTerminate) this.emit("message", { type: "ready" });
      return super.terminate();
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

const layer = it.layer(SqliteClient.layer({ filename: ":memory:" }));

const captureWarnings = () => {
  const warnings: Array<unknown> = [];
  const logger = Logger.make(({ logLevel, message }) => {
    if (logLevel === "Warn") warnings.push(message);
  });
  return { warnings, layer: Logger.layer([logger], { mergeWithExisting: false }) };
};

class CheckpointWorkerFailure extends Error {
  readonly _tag = "CheckpointWorkerFailure";
  override readonly cause: Error;
  readonly exitCode: number | undefined;
  constructor(cause: Error, exitCode?: number) {
    super(cause.message, { cause });
    this.cause = cause;
    this.exitCode = exitCode;
  }
}

const checkpointWorkerReply = (worker: NodeWorkerThreads.Worker) =>
  Effect.callback<void, CheckpointWorkerFailure>((resume) => {
    const cleanup = () => {
      worker.off("message", onReply);
      worker.off("error", onError);
      worker.off("exit", onExit);
    };
    const onError = (error: Error) => {
      cleanup();
      resume(Effect.fail(new CheckpointWorkerFailure(error)));
    };
    const onExit = (code: number) => {
      cleanup();
      resume(
        Effect.fail(
          new CheckpointWorkerFailure(
            new Error(`Checkpoint worker exited with code ${code}`),
            code,
          ),
        ),
      );
    };
    const onReply = (reply: unknown) => {
      if (reply !== "barrier") return;
      cleanup();
      resume(Effect.void);
    };
    const terminal = workerTerminals.get(worker);
    if (terminal?.error) {
      onError(terminal.error);
      return;
    }
    if (terminal?.exitCode !== undefined) {
      onExit(terminal.exitCode);
      return;
    }
    worker.on("message", onReply);
    worker.on("error", onError);
    worker.on("exit", onExit);
    try {
      // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
      worker.postMessage("barrier");
    } catch (cause) {
      onError(cause instanceof Error ? cause : new Error(String(cause)));
    }
    return Effect.sync(cleanup);
  });

const workerExit = (worker: NodeWorkerThreads.Worker) =>
  Effect.callback<number>((resume) => {
    const code = workerTerminals.get(worker)?.exitCode;
    if (code !== undefined) return resume(Effect.succeed(code));
    // Node clears terminal listeners after emitting exit; observe the completed event.
    const onExit = (code: number) => queueMicrotask(() => resume(Effect.succeed(code)));
    worker.once("exit", onExit);
    return Effect.sync(() => {
      worker.off("exit", onExit);
    });
  });

const checkpointListeners = (worker: NodeWorkerThreads.Worker) =>
  ["message", "error", "exit"].map((event) => worker.listenerCount(event));

const withContendedCheckpointClient = <A, E>(
  use: (fixture: {
    sql: SqlClient.SqlClient;
    worker: NodeWorkerThreads.Worker;
    firstPrepare: Promise<void>;
    release: () => void;
  }) => Effect.Effect<A, E>,
  gate?: SharedArrayBuffer,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-contention-" });
    const filename = path.join(directory, "state.sqlite");
    let holderClosed = false;
    const holder = yield* Effect.acquireRelease(
      Effect.sync(() => new NodeSqlite.DatabaseSync(filename)),
      (db) =>
        Effect.sync(() => {
          if (holderClosed) return;
          db.close();
          holderClosed = true;
        }),
    );
    holder.exec("PRAGMA journal_mode = DELETE; CREATE TABLE entries(value TEXT);");
    let locked = false;
    const release = () => {
      if (!locked) return;
      // EXCLUSIVE locking mode retains the file lock after rollback. Install WAL
      // before closing the holder, so the worker cannot observe an unlocked DELETE database.
      holder.exec("ROLLBACK");
      locked = false;
      try {
        assert.equal(holder.prepare("PRAGMA journal_mode = WAL").get()?.journal_mode, "wal");
      } finally {
        holder.close();
        holderClosed = true;
      }
    };
    yield* Effect.addFinalizer(() => Effect.sync(release));
    const channel = yield* Effect.acquireRelease(
      Effect.sync(() => new NodeWorkerThreads.MessageChannel()),
      ({ port1, port2 }) =>
        Effect.sync(() => {
          port1.close();
          port2.close();
          checkpointDiagnostic.port = undefined;
          checkpointDiagnostic.gate = undefined;
        }),
    );
    const firstPrepare = new Promise<void>((resolve) => {
      channel.port1.once("message", (message) => {
        assert.equal(message, "first-prepare");
        resolve();
      });
    });
    checkpointDiagnostic.port = channel.port2;
    checkpointDiagnostic.gate = gate;
    const startedBefore = startedWorkers.length;
    return yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.equal(startedWorkers.length, startedBefore + 1);
      const worker = startedWorkers.at(-1)!;
      // Client construction must finish the real writer capability/admission probes first.
      // No worker check has been sent yet; its lazy connection alone meets this lock.
      holder.exec("PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE;");
      locked = true;
      assert.equal(holder.isTransaction, true);
      // Upstream initializes the checkpoint connection on its first request.
      // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
      worker.postMessage({ type: "check" });
      return yield* use({ sql, worker, firstPrepare, release });
    }).pipe(Effect.provide(SqliteClient.layer({ filename, checkpointWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

it.effect("R1 tolerates bounded checkpoint startup contention", () => {
  const { warnings, layer: logs } = captureWarnings();
  return withContendedCheckpointClient(({ sql, worker, firstPrepare, release }) =>
    Effect.gen(function* () {
      yield* Effect.promise(() => firstPrepare);
      release();
      const listeners = checkpointListeners(worker);
      yield* checkpointWorkerReply(worker);
      assert.deepEqual(checkpointListeners(worker), listeners);
      assert.equal(workerTerminals.get(worker)?.error, undefined);
      assert.equal(workerTerminals.get(worker)?.exitCode, undefined);
      const rows = yield* sql<{ wal_autocheckpoint: number }>`PRAGMA wal_autocheckpoint`;
      // The upstream worker owns backpressure; the writer must not checkpoint concurrently.
      assert.equal(rows[0]?.wal_autocheckpoint, 0);
      assert.lengthOf(warnings, 0);
    }),
  ).pipe(Effect.provide(logs));
});

it.effect(
  "R2 fails barriers promptly and falls back once under sustained startup contention",
  () => {
    const { warnings, layer: logs } = captureWarnings();
    return withContendedCheckpointClient(({ sql, worker, release }) =>
      Effect.gen(function* () {
        const listeners = checkpointListeners(worker);
        const error = yield* checkpointWorkerReply(worker).pipe(Effect.flip);
        assert.equal(error.message, "database is locked");
        assert.propertyVal(error.cause, "errcode", 5);
        assert.strictEqual(error.cause, workerTerminals.get(worker)?.error);
        assert.deepEqual(checkpointListeners(worker), listeners);
        const alreadyFailed = yield* checkpointWorkerReply(worker).pipe(Effect.flip);
        assert.strictEqual(alreadyFailed.cause, error.cause);
        assert.deepEqual(checkpointListeners(worker), listeners);
        assert.equal(yield* workerExit(worker), 1);
        release();
        const rows = yield* sql<{ wal_autocheckpoint: number }>`PRAGMA wal_autocheckpoint`;
        assert.equal(rows[0]?.wal_autocheckpoint, 1000);
        assert.deepEqual(warnings, [
          [
            "SQLite checkpoint worker unavailable; checkpoints run on the main connection",
            { reason: "database is locked" },
          ],
        ]);
        assert.deepEqual(checkpointListeners(worker), [0, 0, 0]);
      }),
    ).pipe(Effect.provide(logs));
  },
);

it.effect("R3 terminates the contended worker before closing the writer without warning", () => {
  const { warnings, layer: logs } = captureWarnings();
  return Effect.gen(function* () {
    const closed: Array<{ workerExited: boolean; intentional: boolean }> = [];
    const original = NodeSqlite.DatabaseSync.prototype.close;
    const close = vi.spyOn(NodeSqlite.DatabaseSync.prototype, "close").mockImplementation(function (
      this: NodeSqlite.DatabaseSync,
    ) {
      const worker = startedWorkers.at(-1)!;
      closed.push({
        workerExited: worker.threadId === -1,
        intentional: workerTerminals.get(worker)!.terminationRequested,
      });
      return Reflect.apply(original, this, []);
    });
    yield* Effect.addFinalizer(() => Effect.sync(() => close.mockRestore()));
    yield* withContendedCheckpointClient(({ firstPrepare }) => Effect.promise(() => firstPrepare));
    assert.deepEqual(closed, [
      { workerExited: true, intentional: true },
      { workerExited: true, intentional: true },
    ]);
    assert.lengthOf(warnings, 0);
    assert.deepEqual(checkpointListeners(startedWorkers.at(-1)!), [0, 0, 0]);
  }).pipe(Effect.scoped, Effect.provide(logs));
});

it.effect(
  "checkpoint barrier removes interrupted listeners and rejects current and prior exits",
  () => {
    const { layer: logs } = captureWarnings();
    return withContendedCheckpointClient(
      ({ worker, firstPrepare }) =>
        Effect.gen(function* () {
          yield* Effect.promise(() => firstPrepare);
          const listeners = checkpointListeners(worker);
          const pending = yield* Effect.forkChild(checkpointWorkerReply(worker));
          yield* Effect.yieldNow;
          assert.deepEqual(
            checkpointListeners(worker),
            listeners.map((count) => count + 1),
          );
          yield* Fiber.interrupt(pending);
          assert.deepEqual(checkpointListeners(worker), listeners);
          const waiting = yield* Effect.forkChild(checkpointWorkerReply(worker).pipe(Effect.flip));
          yield* Effect.yieldNow;
          assert.deepEqual(
            checkpointListeners(worker),
            listeners.map((count) => count + 1),
          );
          const code = yield* Effect.promise(() => worker.terminate());
          const error = yield* Fiber.join(waiting);
          assert.equal(error.message, `Checkpoint worker exited with code ${code}`);
          const alreadyExited = yield* checkpointWorkerReply(worker).pipe(Effect.flip);
          assert.equal(alreadyExited.message, error.message);
          assert.equal(error.exitCode, code);
          assert.equal(alreadyExited.exitCode, code);
          assert.equal(workerTerminals.get(worker)?.error, undefined);
          assert.equal(workerTerminals.get(worker)?.exitCode, code);
          assert.deepEqual(checkpointListeners(worker), [0, 0, 0]);
        }),
      new SharedArrayBuffer(4),
    ).pipe(Effect.provide(logs));
  },
);
