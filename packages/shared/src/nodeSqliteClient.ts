// @effect-diagnostics-next-line nodeBuiltinImport:off -- Checkpoint admission observes WAL size synchronously with the writer WAL snapshot.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";
import * as NodeUtil from "node:util";
import * as NodeWorkerThreads from "node:worker_threads";

import * as Cache from "effect/Cache";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import { identity } from "effect/Function";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Scheduler from "effect/Scheduler";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Context from "effect/Context";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as Reactivity from "effect/reactivity/Reactivity";
import * as Client from "effect/sql/SqlClient";
import type { Borrower, Connection } from "effect/sql/SqlConnection";
import { SqlError, classifySqliteError } from "effect/sql/SqlError";
import * as Statement from "effect/sql/Statement";

import { retireReaderWorker } from "./jones/sqliteReaders/lifecycle.ts";
import { ReadPriority } from "./jones/sqliteReadPriority.ts";
import type { SqliteReaderPoolOptions } from "./jones/sqliteReaderContracts.ts";
import {
  makeCheckpointReaderAdmission,
  makeReaderPool,
  type CheckpointReaderAdmission,
} from "./jones/sqliteReaders/admission.ts";

const ATTR_DB_SYSTEM_NAME = "db.system.name";

export interface SqliteClientConfig {
  readonly filename: string;
  readonly readonly?: boolean | undefined;
  readonly spanAttributes?: Record<string, unknown> | undefined;
  /** Ignored for in-memory databases, which cannot share a second connection. */
  readonly readerWorker?: boolean | undefined;
  readonly readerPool?: SqliteReaderPoolOptions | undefined;
  readonly readerStartupTimeout?: Duration.Input | undefined;
  /**
   * Fail marked reads when an enabled reader is unavailable instead of running
   * them on the synchronous writer. Ignored when readerWorker is disabled or
   * the database is in memory; existing writer transactions retain affinity.
   */
  readonly readerFailureMode?: "fallback" | "fail" | undefined;
  /**
   * Ignored for in-memory databases.
   */
  readonly checkpointWorker?: boolean | undefined;
  readonly writerWorker?: boolean | undefined;
}

// SqlClient masks transaction acquisition, so readOnly carries the caller's interruption policy.
const ReadOnly = Context.Reference<typeof Effect.interruptible | undefined>(
  "@t3tools/shared/nodeSqliteClient/ReadOnly",
  { defaultValue: () => undefined },
);

/** Wrap the whole transaction; existing write transactions retain the writer, and writes on the reader fail as readonly. */
export const readOnly = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.uninterruptibleMask((restore) =>
    restore(Effect.provideService(effect, ReadOnly, restore)),
  );

interface Acquired {
  readonly connection: "writer" | "reader";
  readonly lockWaitMs: number;
}

interface ConnectionOwner {
  readonly fiberId: number;
  readonly name: string;
  readonly traceId?: string;
  readonly spanId?: string;
}

const connectionOwner = (fiber: Fiber.Fiber<unknown, unknown>): ConnectionOwner => {
  const current = Context.getOrUndefined(fiber.context, Tracer.ParentSpan);
  if (current?._tag !== "Span") return { fiberId: fiber.id, name: "untraced" };
  let span: Tracer.Span = current;
  while (span.name === "sqlite" || span.name === "sql.transaction") {
    const parent: Tracer.AnySpan | undefined = Option.getOrUndefined(span.parent);
    if (parent?._tag !== "Span") break;
    span = parent;
  }
  return { fiberId: fiber.id, name: span.name, traceId: span.traceId, spanId: span.spanId };
};

const annotateAcquired = (
  fiber: Fiber.Fiber<unknown, unknown>,
  acquired: Acquired | undefined,
  ownerAtWaitStart?: ConnectionOwner,
) => {
  const span = Context.getOrUndefined(fiber.context, Tracer.ParentSpan);
  if (span?._tag !== "Span" || acquired === undefined) return;
  span.attribute("db.connection", acquired.connection);
  span.attribute("db.lock_wait_ms", acquired.lockWaitMs);
  if (ownerAtWaitStart !== undefined) {
    span.attribute("db.lock_owner_at_start.fiber_id", ownerAtWaitStart.fiberId);
    span.attribute("db.lock_owner_at_start.name", ownerAtWaitStart.name);
    if (ownerAtWaitStart.traceId !== undefined) {
      span.attribute("db.lock_owner_at_start.trace_id", ownerAtWaitStart.traceId);
    }
    if (ownerAtWaitStart.spanId !== undefined) {
      span.attribute("db.lock_owner_at_start.span_id", ownerAtWaitStart.spanId);
    }
  }
};

const TransactionAcquire = Context.Reference<
  | {
      acquired?: Acquired;
      ownerAtWaitStart: ConnectionOwner | undefined;
      restore: typeof Effect.interruptible | undefined;
    }
  | undefined
>("@t3tools/shared/nodeSqliteClient/TransactionAcquire", { defaultValue: () => undefined });

export class UnsupportedNodeSqliteVersionError extends Schema.TaggedError<UnsupportedNodeSqliteVersionError>()(
  "UnsupportedNodeSqliteVersionError",
  {
    nodeVersion: Schema.String,
    requirement: Schema.String,
  },
) {
  override get message(): string {
    return `Node.js ${this.nodeVersion} is missing required node:sqlite APIs. Upgrade to ${this.requirement}.`;
  }
}

export class UnsupportedNodeSqliteOperationError extends Schema.TaggedError<UnsupportedNodeSqliteOperationError>()(
  "UnsupportedNodeSqliteOperationError",
  {},
) {
  override get message(): string {
    return "Node SQLite does not support executeStream.";
  }
}

const checkNodeSqliteCompat = Effect.fn("checkNodeSqliteCompat")(function () {
  const parts = process.versions.node.split(".").map(Number);
  const major = parts[0] ?? 0;
  const minor = parts[1] ?? 0;
  const supported = (major === 22 && minor >= 16) || (major === 23 && minor >= 11) || major >= 24;

  if (!supported) {
    return Effect.die(
      new UnsupportedNodeSqliteVersionError({
        nodeVersion: process.versions.node,
        requirement: "Node.js >=22.16, >=23.11, or >=24",
      }),
    );
  }
  return Effect.void;
});

// node:sqlite supplies errcode; classifySqliteError expects errno.
const classifyError = (cause: unknown, message: string, operation: string) => {
  if (
    Predicate.hasProperty(cause, "errcode") &&
    typeof cause.errcode === "number" &&
    !Predicate.hasProperty(cause, "errno")
  ) {
    Object.assign(cause, { errno: cause.errcode });
  }
  return classifySqliteError(cause, { message, operation });
};

// SQLite retains text/blob bindings until rebind or finalization; node:sqlite cannot clear them.
const LARGE_BINDING_SIZE = 64 * 1024;

const isBlob = (value: unknown): value is ArrayBufferView | ArrayBufferLike =>
  ArrayBuffer.isView(value) || NodeUtil.types.isAnyArrayBuffer(value);

const bindingSize = (value: unknown): number => {
  if (typeof value === "string") {
    return value.length;
  }
  return isBlob(value) ? value.byteLength : 0;
};

const hasLargeBinding = (params: ReadonlyArray<unknown>): boolean => {
  try {
    let size = 0;
    for (const param of params) {
      if (
        (typeof param === "object" || typeof param === "function") &&
        param !== null &&
        !isBlob(param)
      ) {
        // Named parameters may be callable objects; read data properties only to avoid invoking getters twice.
        for (const property of Object.values(Object.getOwnPropertyDescriptors(param))) {
          if (!property.enumerable) continue;
          if (!("value" in property)) return true;
          size += bindingSize(property.value);
        }
      } else {
        size += bindingSize(param);
      }
    }
    return size > LARGE_BINDING_SIZE;
  } catch {
    return true;
  }
};

// A reader refuses a database outside WAL mode with this code; retrying cannot change that.
const NOT_WAL = "ERR_SQLITE_NOT_WAL";

// Inline worker source also runs in bundles and single-executable builds without module files.
const workerSource = `
const { parentPort, workerData } = require("node:worker_threads");
const { DatabaseSync } = require("node:sqlite");
const describe = (error) => ({
  message: String(error && error.message ? error.message : error),
  code: error && error.code,
  errcode: error && error.errcode,
  errstr: error && error.errstr,
});
let db;
try {
  // A read-only open cannot be undone by SQL, unlike PRAGMA query_only.
  const next = new DatabaseSync(workerData.filename, {
    readOnly: workerData.readOnly,
    allowExtension: workerData.allowExtension,
  });
  next.exec("PRAGMA busy_timeout = 5000;");
  // Reads the header, so a file that is not a database fails here rather than on first use.
  const mode = String(next.prepare("PRAGMA journal_mode").get().journal_mode).toLowerCase();
  // Without WAL a reader's shared lock blocks the writer's commits, so refuse.
  if (workerData.requireWal && mode !== "wal") {
    throw Object.assign(new Error("database uses journal_mode " + mode + ", not wal"), { code: ${JSON.stringify(NOT_WAL)} });
  }
  db = next;
  parentPort.postMessage({ type: "ready", workerTimeMs: Number(process.hrtime.bigint()) / 1e6 });
} catch (error) {
  parentPort.postMessage({ type: "unavailable", ...describe(error) });
}
const statements = new Map();
const prepare = (sql, cached) => {
  if (!cached) return db.prepare(sql);
  let statement = statements.get(sql);
  if (statement === undefined) {
    statement = db.prepare(sql);
    if (statements.size >= workerData.prepareCacheSize) statements.delete(statements.keys().next().value);
  } else {
    statements.delete(sql);
  }
  statements.set(sql, statement);
  return statement;
};
parentPort.on("message", (request) => {
  if (request.type === "close") {
    // Closing lets SQLite checkpoint and remove the WAL, which terminating would skip.
    try {
      if (db !== undefined) db.close();
    } catch {}
    parentPort.close();
    return;
  }
  const { id, sql, params, cached, values, raw, safeIntegers, received } = request;
  let operation = "open";
  const started = process.hrtime.bigint();
  const timing = () => ({ durationMs: Number(process.hrtime.bigint() - started) / 1e6, sentAtMs: Number(process.hrtime.bigint()) / 1e6 });
  let iterator;
  try {
    if (db === undefined) throw new Error("database connection is unavailable");
    operation = "prepare";
    const statement = prepare(sql, cached);
    operation = "execute";
    statement.setReadBigInts(safeIntegers);
    if (statement.columns().length > 0) {
      statement.setReturnArrays(values);
      let rows = [];
      let bytes = 0;
      let pendingBatch = false;
      iterator = statement.iterate(...params);
      for (const row of iterator) {
        let rowBytes = 0;
        for (const key in row) {
          const value = row[key];
          rowBytes += typeof value === "string" ? value.length * 2
            : ArrayBuffer.isView(value) ? value.byteLength : 8;
        }
        if (rows.length > 0 && (rows.length >= 16384 || bytes + rowBytes > 8 * 1024 * 1024)) {
          // Build the next batch during receipt, but allow only one unacknowledged message.
          if (pendingBatch) Atomics.wait(received, 0, 0);
          Atomics.store(received, 0, 0);
          parentPort.postMessage({ id, rows, more: true });
          rows = [];
          bytes = 0;
          pendingBatch = true;
        }
        rows.push(row);
        bytes += rowBytes;
      }
      if (pendingBatch) Atomics.wait(received, 0, 0);
      parentPort.postMessage({ id, rows, isTransaction: db.isTransaction, ...timing() });
    } else {
      const result = statement.run(...params);
      parentPort.postMessage({ id, rows: raw ? result : [], isTransaction: db.isTransaction, ...timing() });
    }
  } catch (error) {
    // A step that throws leaves its statement active; an active write blocks every later COMMIT.
    try {
      iterator?.return();
    } catch {}
    // A failed iterator can report its previous SQLite error when the statement is reused.
    statements.delete(sql);
    parentPort.postMessage({ id, error: { operation, ...describe(error) }, isTransaction: db.isTransaction, ...timing() });
  }
});
`;

interface WorkerRequest {
  readonly sql: string;
  readonly params: ReadonlyArray<unknown>;
  readonly cached: boolean;
  readonly values: boolean;
  readonly raw: boolean;
}

interface WorkerFailure {
  readonly message: string;
  readonly code?: unknown;
  readonly errcode?: unknown;
  readonly errstr?: unknown;
}

type WorkerStartup =
  | { readonly type: "ready"; readonly workerTimeMs?: number }
  | ({ readonly type: "unavailable" } & WorkerFailure);

type WorkerReply =
  | { readonly id: number; readonly isTransaction?: boolean; readonly rows: ReadonlyArray<any>; readonly more?: boolean; readonly durationMs?: number; readonly sentAtMs?: number }
  | {
      readonly id: number;
      readonly isTransaction?: boolean;
      readonly durationMs?: number;
      readonly sentAtMs?: number;
      readonly error: WorkerFailure & { readonly operation: "open" | "prepare" | "execute" };
    };

const workerFailureMessages = {
  open: "Failed to open database",
  prepare: "Failed to prepare statement",
  execute: "Failed to execute statement",
} as const;

const failed = (cause: unknown, message: string, operation: string) =>
  new SqlError({ reason: classifyError(cause, message, operation) });

const failedWith = (
  { message, code, errcode, errstr }: WorkerFailure,
  operation: keyof typeof workerFailureMessages,
) =>
  failed(
    Object.assign(new Error(message), { code, errcode, errstr }),
    workerFailureMessages[operation],
    operation,
  );

// Read-transaction cleanup succeeds after worker exit because it can no longer change anything.
const endsReadTransaction = (sql: string) => /^\s*ROLLBACK\b|^\s*COMMIT\b/i.test(sql);

// WAL readers run concurrently; one worker serialized every read behind the slowest transaction.
const READER_POOL_SIZE = 4;
const READER_STARTUP_TIMEOUT = Duration.seconds(10);
const READER_RETRY_DELAY = Duration.seconds(1);
const READER_RETRY_MAX_DELAY = Duration.minutes(1);

// Bind transactions to one worker; fail after its exit instead of resuming outside the transaction on a replacement.
const makeWorkerConnection = Effect.fnUntraced(function* (options: {
  readonly filename: string;
  readonly role: "reader" | "writer";
  readonly readOnly: boolean;
  readonly onReply?: (inTransaction: boolean | undefined) => void;
  readonly canClose?: () => boolean;
}) {
  const label = options.role === "reader" ? "Database reader" : "Database writer";
  const readOnly = options.readOnly;
  let current:
    | {
        readonly worker: NodeWorkerThreads.Worker;
        readonly connection: Connection;
        readonly started: Promise<WorkerStartup>;
      }
    | undefined;
  let nextRequestId = 0;
  // A replacement writer would lose the pragmas callers set on it, such as foreign_keys.
  let writerStopped: string | undefined;

  const start = () => {
    const worker = new NodeWorkerThreads.Worker(workerSource, {
      eval: true,
      workerData: {
        filename: options.filename,
        readOnly,
        requireWal: options.role === "reader",
        allowExtension: false,
        prepareCacheSize: 200,
      },
    });
    let announce: (startup: WorkerStartup) => void = () => {};
    const started = new Promise<WorkerStartup>((resolve) => {
      announce = resolve;
    });
    let stoppedMessage: string | undefined;
    const pending = new Map<
      number,
      {
        readonly endsTransaction: boolean;
        readonly received: Int32Array;
        readonly rows: Array<unknown>;
        readonly resume: (reply: WorkerReply) => void;
      }
    >();
    const stop = (message: string) => {
      stoppedMessage = message;
      if (options.role === "writer") writerStopped = message;
      announce({ type: "unavailable", message });
      if (current?.worker === worker) current = undefined;
      for (const [id, request] of pending) {
        request.resume(
          request.endsTransaction
            ? { id, rows: [] }
            : { id, error: { operation: "execute", message } },
        );
      }
      pending.clear();
    };
    worker.on("message", (reply: WorkerReply | WorkerStartup) => {
      if ("type" in reply) {
        announce(reply);
        if (pending.size === 0) worker.unref();
        return;
      }
      const request = pending.get(reply.id);
      if (request === undefined) return;
      if ("rows" in reply && reply.more) {
        for (const row of reply.rows) request.rows.push(row);
        Atomics.store(request.received, 0, 1);
        Atomics.notify(request.received, 0);
        return;
      }
      pending.delete(reply.id);
      if (pending.size === 0) worker.unref();
      options.onReply?.(typeof reply.isTransaction === "boolean" ? reply.isTransaction : undefined);
      if ("rows" in reply && request.rows.length > 0) {
        for (const row of reply.rows) request.rows.push(row);
        request.resume({ id: reply.id, rows: request.rows });
      } else {
        request.resume(reply);
      }
    });
    worker.on("error", (cause) => stop(`${label} failed: ${cause.message}`));
    worker.on("exit", (code) => stop(`${label} exited with code ${code}`));

    const request = (input: WorkerRequest) =>
      Effect.withFiber<ReadonlyArray<any>, SqlError>((fiber) => {
        // A COMMIT that never reached a writer must fail; only its ROLLBACK is moot.
        const endsTransaction = readOnly
          ? endsReadTransaction(input.sql)
          : /^\s*ROLLBACK\b/i.test(input.sql);
        if (stoppedMessage !== undefined) {
          return endsTransaction
            ? Effect.succeed([])
            : Effect.fail(failedWith({ message: stoppedMessage }, "execute"));
        }
        const safeIntegers = Boolean(Context.get(fiber.context, Client.SafeIntegers));
        const reply = Effect.callback<ReadonlyArray<any>, SqlError>((resume) => {
          const id = nextRequestId++;
          const received = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
          pending.set(id, {
            endsTransaction,
            received,
            rows: [],
            resume: (reply) =>
              resume(
                "rows" in reply
                  ? Effect.succeed(reply.rows)
                  : Effect.fail(failedWith(reply.error, reply.error.operation)),
              ),
          });
          if (pending.size === 1) worker.ref();
          const cached = input.cached && !hasLargeBinding(input.params);
          // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
          worker.postMessage({ id, safeIntegers, ...input, cached, received });
        });
        // A posted write runs to the end, as it did on the event loop; the caller and its lock
        // must not move on while the worker is still applying it. Reads stay cancellable.
        return options.role === "writer" ? Effect.uninterruptible(reply) : reply;
      });

    const connection = identity<Connection>({
      execute(sql, params, rowTransform) {
        const effect = request({ sql, params, cached: true, values: false, raw: false });
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeRaw(sql, params) {
        return request({ sql, params, cached: true, values: false, raw: true });
      },
      executeValues(sql, params) {
        return request({ sql, params, cached: true, values: true, raw: false });
      },
      executeValuesUnprepared(sql, params) {
        return request({ sql, params: params ?? [], cached: false, values: true, raw: false });
      },
      executeUnprepared(sql, params, rowTransform) {
        // Read snapshots need deferred BEGIN; readonly connections reject IMMEDIATE and EXCLUSIVE.
        const statement = readOnly && /^\s*BEGIN\b/i.test(sql) ? "BEGIN" : sql;
        const effect = request({
          sql: statement,
          params: params ?? [],
          cached: false,
          values: false,
          raw: false,
        });
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeStream(_sql, _params) {
        return Stream.die(new UnsupportedNodeSqliteOperationError());
      },
    });
    current = { worker, connection, started };
    return current;
  };

  yield* Effect.addFinalizer(() =>
    Effect.promise(async () => {
      const open = current;
      if (open === undefined) return;
      if (options.role === "writer") {
        if (options.canClose?.() === false) throw new Error("SQLite writer retained because reader termination is unproved");
        await new Promise<void>((resolve) => {
          open.worker.once("exit", () => resolve());
          open.worker.ref();
          // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
          open.worker.postMessage({ type: "close" });
        });
      }
      await open.worker.terminate();
    }),
  );

  const retire = (worker: NodeWorkerThreads.Worker) => {
    if (current?.worker === worker) current = undefined;
    void worker.terminate();
  };

  return Effect.suspend((): Effect.Effect<Connection, SqlError> => {
    if (current === undefined && writerStopped !== undefined) {
      return Effect.fail(failedWith({ message: writerStopped }, "execute"));
    }
    let entry: NonNullable<typeof current>;
    try {
      entry = current ?? start();
    } catch (cause) {
      return Effect.fail(failed(cause, workerFailureMessages.open, "open"));
    }
    const started = Effect.promise(() => entry.started);
    return Effect.flatMap(
      Effect.withFiber((fiber) => {
        const restore = Context.get(fiber.context, ReadOnly) ?? identity;
        // Readers fall back to the writer; the writer has no fallback, so a slow disk only delays it.
        return restore(
          options.role === "reader"
            ? Effect.timeoutOption(started, READER_STARTUP_TIMEOUT)
            : Effect.asSome(started),
        );
      }),
      (startup): Effect.Effect<Connection, SqlError> => {
        const outcome = Option.getOrUndefined(startup);
        if (outcome?.type === "ready") {
          // A late ready event must not hand out a worker retired by another acquire timeout.
          return current === entry
            ? Effect.succeed(entry.connection)
            : Effect.fail(failedWith({ message: `${label} stopped while starting` }, "open"));
        }
        retire(entry.worker);
        return Effect.fail(
          failedWith(
            outcome ?? {
              message: `${label} did not open the database within ${Duration.toMillis(READER_STARTUP_TIMEOUT)} ms`,
            },
            "open",
          ),
        );
      },
    );
  });
});

const startFailure = (error: SqlError) => {
  const cause = error.reason.cause;
  return {
    reason: cause instanceof Error ? cause.message : error.message,
    permanent: Predicate.hasProperty(cause, "code") && cause.code === NOT_WAL,
  };
};

// A reader that cannot start turns off the whole pool, so routing never picks a dead slot, until
// one background retry starts it again.
const makeReader = Effect.fnUntraced(function* (filename: string, pool: { unavailable: boolean }) {
  const acquire = yield* makeWorkerConnection({ filename, role: "reader", readOnly: true });
  const scope = yield* Effect.scope;
  const context = yield* Effect.context<never>();

  const recover = (delay: Duration.Duration): Effect.Effect<void> =>
    Effect.sleep(delay).pipe(
      Effect.andThen(acquire),
      Effect.matchEffect({
        onSuccess: () =>
          Effect.suspend(() => {
            pool.unavailable = false;
            return Effect.logInfo(
              "SQLite reader worker available again; reads use the reader workers",
            );
          }),
        onFailure: (error) => {
          const { reason, permanent } = startFailure(error);
          return permanent
            ? Effect.logInfo(
                "SQLite reader worker retries stopped permanently; reads use the writer",
                { reason },
              )
            : recover(Duration.min(Duration.times(delay, 2), READER_RETRY_MAX_DELAY));
        },
      }),
    );

  return Effect.suspend((): Effect.Effect<Connection | undefined> =>
    pool.unavailable
      ? Effect.undefined
      : acquire.pipe(
          Effect.map((connection) => (pool.unavailable ? undefined : connection)),
          Effect.catch((error) => {
            if (pool.unavailable) return Effect.undefined;
            pool.unavailable = true;
            const { reason, permanent } = startFailure(error);
            // Pool state belongs to the client: never log or retry with the failed read's services.
            return Effect.logWarning("SQLite reader worker unavailable; reads use the writer", {
              reason,
            }).pipe(
              Effect.andThen(
                permanent ? Effect.void : Effect.forkIn(recover(READER_RETRY_DELAY), scope),
              ),
              Effect.updateContext((_: Context.Context<never>) => context),
              Effect.as(undefined),
            );
          }),
        ),
  );
});

// A checkpoint copies WAL pages into the database and syncs it. On the writer's thread every
// write would wait for that sync, so a second connection checkpoints beside the writes.
const makeJonesReader = Effect.fnUntraced(function* (
  options: SqliteClientConfig,
  slotId = 0,
  management?: Semaphore.Semaphore,
  onUnprovedRetirement: () => void = () => {},
  onUnavailable: () => void = () => {},
) {
  let current:
    | {
        readonly worker: NodeWorkerThreads.Worker;
        readonly connection: Connection;
        readonly started: Promise<WorkerStartup>;
        readonly stop: (message: string) => void;
        readonly settled: () => Promise<void> | undefined;
        readonly ready: () => boolean;
        readonly stopped: () => boolean;
        readonly exited: () => boolean;
      }
    | undefined;
  const clock = yield* Clock.Clock;
  let unavailable = false;
  let closed = false;
  let retiring: Promise<void> | undefined;
  let retryAt = 0n;
  let retryDelayMs = 1000;
  let nextRequestId = 0;
  let generation = 0;
  let retirementProved = true;

  const start = () => {
    const startupSentAt = Number(clock.monotonicTimeNanosUnsafe()) / 1e6;
    let offsetMs: number | undefined;
    let uncertaintyMs: number | undefined;
    const worker = new NodeWorkerThreads.Worker(workerSource, {
      eval: true,
      workerData: {
        filename: options.filename,
        readOnly: true,
        requireWal: true,
        allowExtension: false,
        prepareCacheSize: 200,
      },
    });
    generation++;
    let exited = false;
    const settledWaiters = new Set<() => void>();
    const notifySettled = () => {
      if (pending.size !== 0) return;
      for (const resume of settledWaiters) resume();
      settledWaiters.clear();
    };
    let announce: (startup: WorkerStartup) => void = () => {};
    const started = new Promise<WorkerStartup>((resolve) => {
      announce = resolve;
    });
    let ready = false;
    let stoppedMessage: string | undefined;
    const pending = new Map<
      number,
      { readonly endsTransaction: boolean; readonly received: Int32Array; readonly rows: Array<unknown>; readonly resume: (reply: WorkerReply) => void }
    >();
    const stop = (message: string) => {
      if (stoppedMessage !== undefined) return;
      stoppedMessage = message;
      ready = false;
      announce({ type: "unavailable", message });
      for (const [id, request] of pending) {
        request.resume(
          request.endsTransaction
            ? { id, rows: [] }
            : { id, error: { operation: "execute", message } },
        );
      }
      pending.clear();
      notifySettled();
    };
    worker.on("message", (reply: WorkerReply | WorkerStartup) => {
      if (stoppedMessage !== undefined) return;
      if ("type" in reply) {
        ready = reply.type === "ready";
        if (reply.type === "ready" && reply.workerTimeMs !== undefined) {
          const receivedAt = Number(clock.monotonicTimeNanosUnsafe()) / 1e6;
          offsetMs = (startupSentAt + receivedAt) / 2 - reply.workerTimeMs;
          uncertaintyMs = (receivedAt - startupSentAt) / 2;
        }
        announce(reply);
        if (pending.size === 0) worker.unref();
        return;
      }
      const request = pending.get(reply.id);
      if (request === undefined) return;
      if ("rows" in reply && reply.more) {
        for (const row of reply.rows) request.rows.push(row);
        Atomics.store(request.received, 0, 1);
        Atomics.notify(request.received, 0);
        return;
      }
      pending.delete(reply.id);
      notifySettled();
      if (pending.size === 0) worker.unref();
      if ("rows" in reply && request.rows.length > 0) {
        for (const row of reply.rows) request.rows.push(row);
        request.resume({ ...reply, rows: request.rows });
      } else request.resume(reply);
    });
    worker.on("error", (cause) => stop(`Database reader failed: ${cause.message}`));
    worker.on("exit", (code) => {
      exited = true;
      stop(`Database reader exited with code ${code}`);
      if (current?.worker === worker) {
        current = undefined;
        if (options.readerPool && !closed) {
          retryAt = clock.monotonicTimeNanosUnsafe() + BigInt(retryDelayMs) * 1000000n;
          retryDelayMs = Math.min(retryDelayMs * 2, 30000);
        }
      }
    });

    const request = (input: WorkerRequest) =>
      Effect.withFiber<ReadonlyArray<any>, SqlError>((fiber) => {
        const endsTransaction = endsReadTransaction(input.sql);
        if (stoppedMessage !== undefined) {
          return endsTransaction
            ? Effect.succeed([])
            : Effect.fail(
                failed(new Error(stoppedMessage), workerFailureMessages.execute, "execute"),
              );
        }
        const safeIntegers = Boolean(Context.get(fiber.context, Client.SafeIntegers));
        const requestedAt = clock.monotonicTimeNanosUnsafe();
        const priority = fiber.getRef(ReadPriority);
        const observe = (
          phase: "execute" | "reply",
          durationMs: number,
          outcome: "ok" | "failed",
          uncertainty?: number,
        ) => {
          try {
            options.readerPool?.observe?.({
              phase,
              priority,
              slotId,
              generation,
              durationMs,
              outcome,
              queueDepth: 0,
              ...(phase === "reply" ? { estimated: true } : {}),
              ...(uncertainty === undefined ? {} : { uncertaintyMs: uncertainty }),
            });
          } catch {}
        };
        return Effect.uninterruptible(
          Effect.callback<ReadonlyArray<any>, SqlError>((resume) => {
            const id = nextRequestId++;
            const received = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
            pending.set(id, {
              received,
              rows: [],
              endsTransaction,
              resume: (reply) => {
                const elapsed = Number(clock.monotonicTimeNanosUnsafe() - requestedAt) / 1e6;
                const execution = reply.durationMs;
                if (execution !== undefined) {
                  observe("execute", execution, "rows" in reply ? "ok" : "failed");
                  // Startup bounds the clock offset; transfer estimates retain that uncertainty.
                  const transfer =
                    reply.sentAtMs !== undefined && offsetMs !== undefined
                      ? Number(clock.monotonicTimeNanosUnsafe()) / 1e6 - (reply.sentAtMs + offsetMs)
                      : elapsed - execution;
                  observe(
                    "reply",
                    Math.max(0, transfer),
                    "rows" in reply ? "ok" : "failed",
                    uncertaintyMs ?? elapsed,
                  );
                }
                if ("rows" in reply) {
                  resume(Effect.succeed(reply.rows));
                  return;
                }
                const { operation, message, ...codes } = reply.error;
                resume(
                  Effect.fail(
                    failed(
                      Object.assign(new Error(message), codes),
                      workerFailureMessages[operation],
                      operation,
                    ),
                  ),
                );
              },
            });
            if (pending.size === 1) worker.ref();
            const cached = input.cached && !hasLargeBinding(input.params);
            try {
              // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
              worker.postMessage({ id, safeIntegers, ...input, cached, received });
            } catch (cause) {
              pending.delete(id);
              notifySettled();
              resume(Effect.fail(failed(cause, workerFailureMessages.execute, "execute")));
            }
          }),
        );
      });

    const connection = identity<Connection>({
      execute(sql, params, rowTransform) {
        const effect = request({ sql, params, cached: true, values: false, raw: false });
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeRaw(sql, params) {
        return request({ sql, params, cached: true, values: false, raw: true });
      },
      executeValues(sql, params) {
        return request({ sql, params, cached: true, values: true, raw: false });
      },
      executeValuesUnprepared(sql, params) {
        return request({ sql, params: params ?? [], cached: false, values: true, raw: false });
      },
      executeUnprepared(sql, params, rowTransform) {
        // Read snapshots need deferred BEGIN; readonly connections reject IMMEDIATE and EXCLUSIVE.
        const statement = /^\s*BEGIN\b/i.test(sql) ? "BEGIN" : sql;
        const effect = request({
          sql: statement,
          params: params ?? [],
          cached: false,
          values: false,
          raw: false,
        });
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeStream(_sql, _params) {
        return Stream.die(new UnsupportedNodeSqliteOperationError());
      },
    });
    current = {
      worker,
      connection,
      started,
      stop,
      ready: () => ready,
      stopped: () => stoppedMessage !== undefined,
      exited: () => exited,
      // Already received replies need no asynchronous barrier. A live request still
      // registers a waiter before its slot can be reused or retired.
      settled: () =>
        pending.size === 0
          ? undefined
          : new Promise<void>((resolve) => settledWaiters.add(resolve)),
    };
    return current;
  };

  yield* Effect.addFinalizer(() =>
    Effect.promise(async () => {
      closed = true;
      const reader = current;
      current = undefined;
      reader?.stop("Database reader scope closed");
      if (reader !== undefined) {
        if (options.readerPool) retirementProved = await retireReaderWorker(reader.worker);
        else await reader.worker.terminate();
      }
      await retiring;
      if (options.readerPool && !retirementProved) {
        onUnprovedRetirement();
        throw new Error("SQLite reader termination remains unproved");
      }
    }),
  );

  const startupTimeout = Duration.fromInputUnsafe(
    options.readerStartupTimeout ?? Duration.seconds(10),
  );

  const turnOff = (reader: typeof current, reason: string, permanent = true) =>
    Effect.suspend(() => {
      if (reader !== undefined) {
        if (current !== reader) return Effect.sync(() => undefined);
        current = undefined;
        retirementProved = false;
        // Retire before terminating: a queued ready/reply cannot revive this connection.
        reader.stop(reason);
        const cleanup = (
          options.readerPool
            ? retireReaderWorker(reader.worker)
            : Promise.resolve()
                .then(() => reader.worker.terminate())
                .then(
                  () => true,
                  () => false,
                )
        ).then((proved) => {
          retirementProved = proved;
          // Failed or timed-out cleanup consumes the slot for the rest of this scope.
          if (!proved) {
            unavailable = true;
            onUnprovedRetirement();
          }
        });
        retiring = cleanup;
        void cleanup.then(() => {
          if (retiring === cleanup) retiring = undefined;
        });
      }
      if (unavailable) return Effect.undefined;
      unavailable = permanent;
      if (permanent) onUnavailable();
      if (!permanent) {
        // Retry on later demand only, after cleanup and a capped exponential cooldown.
        retryAt = clock.monotonicTimeNanosUnsafe() + BigInt(retryDelayMs) * 1000000n;
        retryDelayMs = Math.min(retryDelayMs * 2, 30000);
      }
      return Effect.logWarning("SQLite reader worker unavailable", {
        reason,
        readerFailureMode: options.readerFailureMode ?? "fallback",
      }).pipe(Effect.as(undefined));
    });

  const acquire = Effect.suspend((): Effect.Effect<Connection | undefined> => {
    if (closed || unavailable || retiring !== undefined) return Effect.undefined;
    if (clock.monotonicTimeNanosUnsafe() < retryAt) return Effect.undefined;
    let reader: NonNullable<typeof current>;
    try {
      reader = current ?? start();
    } catch (cause) {
      return turnOff(undefined, String(cause), options.readerPool === undefined);
    }
    return Effect.flatMap(
      Effect.withFiber((fiber) => {
        const restore = Context.get(fiber.context, ReadOnly) ?? identity;
        return restore(
          Effect.timeoutOption(
            Effect.promise(() => reader.started),
            startupTimeout,
          ),
        );
      }),
      (startup): Effect.Effect<Connection | undefined> =>
        // A late ready event must not hand out a worker retired by another acquire timeout.
        closed || unavailable || current !== reader
          ? Effect.undefined
          : Option.isSome(startup) && startup.value.type === "ready"
            ? Effect.sync(() => {
                retryDelayMs = 1000;
                return reader.connection;
              })
            : turnOff(
                reader,
                Option.isSome(startup) && startup.value.type === "unavailable"
                  ? startup.value.message
                  : `worker did not open the database within ${Duration.toMillis(startupTimeout)} ms`,
                options.readerPool === undefined && Option.isSome(startup),
              ),
    );
  });
  const awaitRetirement = Effect.suspend(() => {
    const pending = retiring;
    return pending === undefined ? Effect.void : Effect.promise(() => pending);
  });
  const managed = <A>(effect: Effect.Effect<A>) =>
    management === undefined ? effect : management.withPermits(1)(effect);
  const coldAcquire = managed(
    options.readerPool
      ? acquire.pipe(
          Effect.onInterrupt(() => turnOff(current, "SQLite reader startup interrupted", false)),
          Effect.ensuring(awaitRetirement),
        )
      : acquire,
  );
  const readyConnection = () =>
    current?.ready() && !closed && !unavailable && retiring === undefined
      ? current.connection
      : undefined;
  return {
    readyConnection,
    acquire: Effect.suspend(() => {
      const connection = readyConnection();
      return connection === undefined ? coldAcquire : Effect.succeed(connection);
    }),
    generation: () => generation,
    available: () =>
      !closed &&
      !unavailable &&
      retiring === undefined &&
      clock.monotonicTimeNanosUnsafe() >= retryAt,
    retire: (reason: string) =>
      managed(turnOff(current, reason, false).pipe(Effect.andThen(awaitRetirement))),
    settled: Effect.gen(function* () {
      const reader = current;
      const pending = reader?.settled();
      if (pending !== undefined) yield* Effect.promise(() => pending);
      if (reader !== undefined && reader.stopped() && !reader.exited()) {
        yield* managed(
          turnOff(reader, "SQLite reader stopped without exit proof", false).pipe(
            Effect.andThen(awaitRetirement),
          ),
        );
      }
      yield* awaitRetirement;
      return retirementProved;
    }),
  };
});

const CHECKPOINT_FRAMES = 1000;
const CHECKPOINT_IDLE_MS = 100;
const CHECKPOINT_BUSY_TIMEOUT_MS = 1000;
const CHECKPOINT_BACKPRESSURE_FRAMES = 32_768;

const readSafeSqliteInteger = (value: unknown) => {
  const number = typeof value === "bigint" ? Number(value) : value;
  return typeof number === "number" && Number.isSafeInteger(number) ? number : undefined;
};

// Evaluated source also works in bundles and single-executable builds.
const jonesCheckpointWorkerSource = `
const { parentPort, workerData } = require("node:worker_threads");
const { DatabaseSync } = require("node:sqlite");
const { statSync } = require("node:fs");
let db;
let frames;
let checkpoint;
let truncate;
let journalSizeLimit = -1;
let generation = 0;
let nextTruncateId = 0;
let pendingTruncate;
let pendingBarriers = 0;
let idle;
const initialize = () => {
  if (db !== undefined) return;
  // The writer switches to WAL after constructing the client.
  db = new DatabaseSync(workerData.filename);
  // Bound startup contention before the first lock-taking prepare.
  db.exec("PRAGMA busy_timeout = " + workerData.busyTimeoutMs + ";");
  const mode = String(db.prepare("PRAGMA journal_mode").get().journal_mode).toLowerCase();
  if (mode !== "wal") throw new Error("database uses journal_mode " + mode + ", not wal");
  // macOS requires F_FULLFSYNC to flush the drive cache.
  db.exec("PRAGMA checkpoint_fullfsync = ON;");
  frames = db.prepare("PRAGMA wal_checkpoint(NOOP)");
  checkpoint = db.prepare("PRAGMA wal_checkpoint(PASSIVE)");
  truncate = db.prepare("PRAGMA wal_checkpoint(TRUNCATE)");
  db.exec("PRAGMA busy_timeout = 5000;");
};
const atCeiling = (state) => state.log >= workerData.backpressureFrames;
const exceedsSizeLimit = () =>
  journalSizeLimit >= 0 && statSync(workerData.filename + "-wal").size > journalSizeLimit;
const schedule = (state, blocked) => {
  if (blocked || state.log - state.checkpointed >= workerData.frames) {
    idle = setTimeout(() => check(undefined, undefined, true), workerData.idleMs);
  }
};
const check = (id, nextJournalSizeLimit, force, nextGeneration = generation) => {
  initialize();
  if (Number.isSafeInteger(nextGeneration)) generation = Math.max(generation, nextGeneration);
  if (Number.isSafeInteger(nextJournalSizeLimit)) journalSizeLimit = nextJournalSizeLimit;
  let state = frames.get();
  const oversized = exceedsSizeLimit();
  if (force || state.log - state.checkpointed >= workerData.frames || oversized) {
    checkpoint.get();
    state = frames.get();
  }
  if (state.checkpointed === state.log && (atCeiling(state) || oversized)) {
    pendingTruncate = { truncateId: ++nextTruncateId, checkId: id, generation, state };
    parentPort.postMessage({
      type: "truncate-request",
      truncateId: pendingTruncate.truncateId,
      checkId: id,
      generation,
      ...state,
    });
    return;
  }
  const blocked = (atCeiling(state) || oversized) && state.checkpointed < state.log;
  parentPort.postMessage({ type: "checkpoint", id, generation, ...state, blocked });
  schedule(state, blocked);
};
const finishTruncate = (truncateId) => {
  if (pendingTruncate === undefined || pendingTruncate.truncateId !== truncateId) return;
  const pending = pendingTruncate;
  parentPort.postMessage({ type: "truncate-start", truncateId });
  let busy;
  // Admission is paused here; a pinned reader must not make truncation wait.
  db.exec("PRAGMA busy_timeout = 0;");
  try {
    busy = truncate.get().busy !== 0;
  } finally {
    db.exec("PRAGMA busy_timeout = 5000;");
  }
  const state = frames.get();
  const blocked = (atCeiling(state) && (busy || state.checkpointed < state.log)) || exceedsSizeLimit();
  pendingTruncate = undefined;
  parentPort.postMessage({
    type: "checkpoint",
    id: pending.checkId,
    generation: pending.generation,
    truncateId,
    ...state,
    blocked,
  });
  while (pendingBarriers > 0) {
    pendingBarriers--;
    parentPort.postMessage("barrier");
  }
  schedule(state, blocked);
};
parentPort.on("message", (request) => {
  clearTimeout(idle);
  if (request && request.type === "truncate") {
    finishTruncate(request.truncateId);
    return;
  }
  const isBarrier = request === "barrier";
  if (pendingTruncate !== undefined) {
    if (isBarrier) pendingBarriers++;
    else if (request && request.type === "check") {
      // Retire the request without labeling the pending snapshot with a newer generation.
      parentPort.postMessage({
        type: "checkpoint",
        id: request.id,
        generation: pendingTruncate.generation,
        ...pendingTruncate.state,
        blocked: true,
      });
    }
    return;
  }
  const input = request && typeof request === "object" ? request : {};
  check(input.id, input.journalSizeLimit, input.force, input.generation);
  if (isBarrier) {
    if (pendingTruncate !== undefined) pendingBarriers++;
    else parentPort.postMessage("barrier");
  }
});
`;

const startJonesCheckpointWorker = Effect.fnUntraced(function* (
  filename: string,
  writer: NodeSqlite.DatabaseSync,
  readers: CheckpointReaderAdmission,
) {
  const warn = (reason: string) =>
    Effect.logWarning(
      "SQLite checkpoint worker unavailable; checkpoints run on the main connection",
      { reason },
    );
  if (typeof writer.isTransaction !== "boolean" || typeof writer.setAuthorizer !== "function") {
    yield* warn("Node SQLite lacks transaction-state or statement-authorizer support");
    return undefined;
  }
  // These are the writer's first statements, before callers set their own wait.
  writer.exec("PRAGMA busy_timeout = 5000;");
  const version = String(writer.prepare("SELECT sqlite_version() AS version").get()?.version);
  const [major = 0, minor = 0] = version.split(".").map(Number);
  // Older SQLite treats NOOP as PASSIVE, which would checkpoint on the writer.
  if (major < 3 || (major === 3 && minor < 51)) {
    yield* warn(`SQLite ${version} has no NOOP checkpoint`);
    return undefined;
  }
  const frames = writer.prepare("PRAGMA wal_checkpoint(NOOP)");
  const sizeLimit = writer.prepare("PRAGMA journal_size_limit");
  const started = Result.try(
    () =>
      new NodeWorkerThreads.Worker(jonesCheckpointWorkerSource, {
        eval: true,
        workerData: {
          filename,
          frames: CHECKPOINT_FRAMES,
          backpressureFrames: CHECKPOINT_BACKPRESSURE_FRAMES,
          busyTimeoutMs: CHECKPOINT_BUSY_TIMEOUT_MS,
          idleMs: CHECKPOINT_IDLE_MS,
        },
      }),
  );
  if (Result.isFailure(started)) {
    yield* warn(String(started.failure));
    return undefined;
  }
  const worker = started.success;
  const runFork = Effect.runForkWith(yield* Effect.context<never>());
  let stopped = false;
  let generation = 0;
  let nextId = 0;
  let inFlight: number | undefined;
  let checkAgain = false;
  let blocked = false;
  let log = 0;
  let checkpointed = 0;
  let unknown = false;
  let truncate: { id: number; acknowledged: boolean } | undefined;
  type Waiter = { resume: () => void; cancelled: boolean };
  const waiters: Array<Waiter> = [];
  let elected: Waiter | undefined;
  const publishReaders = () => {
    if (stopped) return;
    readers.updateUnsafe({
      generation,
      state: unknown
        ? "uncertain"
        : truncate !== undefined
          ? "truncate"
          : blocked || log >= CHECKPOINT_BACKPRESSURE_FRAMES
            ? "pressure"
            : "open",
    });
  };
  const gated = () =>
    !stopped &&
    !writer.isTransaction &&
    (unknown || blocked || truncate !== undefined || log >= CHECKPOINT_BACKPRESSURE_FRAMES);
  const wake = () => {
    if (!gated() && elected === undefined) {
      elected = waiters.shift();
      elected?.resume();
    }
    if (waiters.length === 0 && elected === undefined) worker.unref();
  };
  const release = (waiter: Waiter) => {
    waiter.cancelled = true;
    const index = waiters.indexOf(waiter);
    if (index !== -1) waiters.splice(index, 1);
    if (elected === waiter) elected = undefined;
    wake();
  };
  const handBack = (reason: string) => {
    if (stopped) return;
    stopped = true;
    truncate = undefined;
    generation++;
    readers.updateUnsafe({ generation, state: "uncertain" });
    try {
      writer.exec(`PRAGMA wal_autocheckpoint = ${CHECKPOINT_FRAMES};`);
      const restored = readSafeSqliteInteger(
        writer.prepare("PRAGMA wal_autocheckpoint").get()?.wal_autocheckpoint,
      );
      if (restored === CHECKPOINT_FRAMES) readers.updateUnsafe({ generation, state: "restored" });
    } catch {}
    wake();
    runFork(warn(reason));
  };
  const send = (message: unknown) => {
    try {
      // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
      worker.postMessage(message);
    } catch (cause) {
      handBack(String(cause));
    }
  };
  const acknowledge = () => {
    if (truncate === undefined || truncate.acknowledged || stopped || writer.isTransaction) return;
    truncate.acknowledged = true;
    send({ type: "truncate", truncateId: truncate.id });
  };
  const observe = () => {
    try {
      const state = frames.get();
      const nextLog = readSafeSqliteInteger(state?.log);
      const nextCheckpointed = readSafeSqliteInteger(state?.checkpointed);
      if (nextLog === undefined || nextCheckpointed === undefined)
        throw new Error("Invalid WAL frame counts");
      log = nextLog;
      checkpointed = nextCheckpointed;
      unknown = false;
      const limit = readSafeSqliteInteger(sizeLimit.get()?.journal_size_limit);
      if (limit !== undefined && limit >= 0) {
        try {
          if (NodeFS.statSync(filename + "-wal").size > limit) blocked = true;
        } catch (cause) {
          if (!Predicate.hasProperty(cause, "code") || cause.code !== "ENOENT") throw cause;
        }
      }
    } catch {
      unknown = true;
    }
  };
  const request = (force = false) => {
    if (stopped || writer.isTransaction) return;
    if (truncate !== undefined) {
      acknowledge();
      return;
    }
    if (inFlight !== undefined) {
      checkAgain = true;
      return;
    }
    if (!force && !gated() && log - checkpointed < CHECKPOINT_FRAMES) return;
    inFlight = ++nextId;
    try {
      send({
        type: "check",
        id: inFlight,
        generation,
        journalSizeLimit: readSafeSqliteInteger(sizeLimit.get()?.journal_size_limit),
        force: force || gated(),
      });
    } catch (cause) {
      handBack(String(cause));
    }
  };
  const check = () => {
    if (stopped || writer.isTransaction) return;
    generation++;
    observe();
    request();
    publishReaders();
    wake();
  };
  worker.on("error", (cause) => handBack(cause.message));
  worker.on("exit", (code) => handBack(`exited with code ${code}`));
  worker.on("message", (message: unknown) => {
    if (stopped || typeof message !== "object" || message === null || !("type" in message)) return;
    if (
      message.type === "truncate-request" &&
      "truncateId" in message &&
      typeof message.truncateId === "number"
    ) {
      truncate = { id: message.truncateId, acknowledged: false };
      blocked = true;
      publishReaders();
      acknowledge();
      return;
    }
    if (message.type !== "checkpoint") return;
    const completedTruncate =
      truncate !== undefined && "truncateId" in message && message.truncateId === truncate.id;
    if (completedTruncate) truncate = undefined;
    // An acknowledged truncate excludes new writer mutations, so its result is current.
    if (
      (completedTruncate || ("generation" in message && message.generation === generation)) &&
      "log" in message &&
      typeof message.log === "number" &&
      "checkpointed" in message &&
      typeof message.checkpointed === "number" &&
      "blocked" in message &&
      typeof message.blocked === "boolean"
    ) {
      log = message.log;
      checkpointed = message.checkpointed;
      blocked = message.blocked;
      unknown = false;
    }
    // Even a stale snapshot must retire its request so a newer check can run.
    if ("id" in message && message.id === inFlight) inFlight = undefined;
    if (checkAgain && inFlight === undefined) {
      checkAgain = false;
      request(true);
    }
    publishReaders();
    wake();
  });
  yield* Effect.addFinalizer(() =>
    Effect.promise(() => {
      stopped = true;
      truncate = undefined;
      wake();
      return worker.terminate();
    }),
  );
  writer.exec("PRAGMA wal_autocheckpoint = 0;");
  observe();
  publishReaders();
  let releaseCheck: ReturnType<typeof setImmediate> | undefined;
  readers.onReaderReleased(() => {
    if (releaseCheck !== undefined || stopped) return;
    releaseCheck = setImmediate(() => {
      releaseCheck = undefined;
      if (!stopped) check();
    });
  });
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      if (releaseCheck !== undefined) clearImmediate(releaseCheck);
      readers.onReaderReleased(() => {});
    }),
  );
  worker.unref();

  const runWithAdmission = <A, E>(
    execute: () => Exit.Exit<A, E>,
    admitted?: Waiter,
  ): Effect.Effect<A, E> =>
    Effect.suspend(() => {
      if (!gated()) {
        // Release the election after executing; its successor must see the new WAL state.
        try {
          return execute();
        } finally {
          if (admitted !== undefined) release(admitted);
        }
      }
      if (admitted !== undefined) release(admitted);
      const waiter: Waiter = { resume: () => {}, cancelled: false };
      const wait = Effect.uninterruptibleMask((restore) =>
        Effect.withFiber((fiber) =>
          (fiber.getRef(TransactionAcquire)?.restore ?? restore)(
            Effect.callback<void>(function (this: Scheduler.Scheduler, resume) {
              const dispatcher = this.makeDispatcher();
              waiter.resume = () => {
                dispatcher.scheduleTask(() => {
                  if (!waiter.cancelled) resume(Effect.void);
                }, 0);
              };
              waiters.push(waiter);
              worker.ref();
              request(true);
              wake();
              return Effect.sync(() => release(waiter));
            }),
          ),
        ),
      );
      return wait.pipe(
        Effect.flatMap(() => runWithAdmission(execute, waiter)),
        Effect.onInterrupt(() => Effect.sync(() => release(waiter))),
      );
    });
  return { check, runWithAdmission };
});

// Worker replies carry transaction state from the actual writer connection.
const startJonesAsyncCheckpointWorker = Effect.fnUntraced(function* (
  filename: string,
  writer: Connection,
  readers: CheckpointReaderAdmission,
  inTransaction: () => boolean,
  transactionStateAvailable: () => boolean,
) {
  const query = (sql: string) => writer.executeUnprepared(sql, [], undefined);
  const [versionRow] = yield* query("SELECT sqlite_version() AS version");
  const [major = 0, minor = 0] = String(versionRow?.version).split(".").map(Number);
  // NOOP on older SQLite is PASSIVE: keep SQLite's own bounded fallback there.
  if (!transactionStateAvailable() || major < 3 || (major === 3 && minor < 51)) {
    yield* query(`PRAGMA wal_autocheckpoint = ${CHECKPOINT_FRAMES}`);
    return undefined;
  }
  const worker = yield* Effect.try({
    try: () => new NodeWorkerThreads.Worker(jonesCheckpointWorkerSource, {
      eval: true,
      workerData: { filename, frames: CHECKPOINT_FRAMES,
        backpressureFrames: CHECKPOINT_BACKPRESSURE_FRAMES,
        busyTimeoutMs: CHECKPOINT_BUSY_TIMEOUT_MS, idleMs: CHECKPOINT_IDLE_MS },
    }),
    catch: (cause) => failed(cause, "Failed to start checkpoint worker", "open"),
  }).pipe(Effect.catch(() => Effect.succeed(undefined)));
  if (worker === undefined) {
    yield* query(`PRAGMA wal_autocheckpoint = ${CHECKPOINT_FRAMES}`);
    return undefined;
  }
  const runFork = Effect.runForkWith(yield* Effect.context<never>());
  let generation = 0;
  let nextId = 0;
  let inFlight: number | undefined;
  let truncate: number | undefined;
  let acknowledged = false;
  let active = false;
  let stopped = false;
  let fallbackReady = false;
  let fallbackFailure: SqlError | undefined;
  let pressure = false;
  let uncertain = false;
  let log = 0;
  let checkpointed = 0;
  let sizeLimit: number | undefined;
  const listeners = new Set<() => void>();
  const publish = () => {
    readers.updateUnsafe({ generation, state: stopped
      ? fallbackReady ? "restored" : "uncertain"
      : uncertain ? "uncertain" : truncate !== undefined ? "truncate" : pressure ? "pressure" : "open" });
    for (const listener of listeners) listener();
  };
  const handBack = () => {
    if (stopped) return;
    stopped = true;
    generation++;
    publish();
    // Retirement proof precedes reopening admission: an errored worker may still be truncating.
    runFork(Effect.tryPromise({
      try: () => worker.terminate(),
      catch: (cause) => failed(cause, "Checkpoint retirement is unproved", "close"),
    }).pipe(
      Effect.andThen(query(`PRAGMA wal_autocheckpoint = ${CHECKPOINT_FRAMES}`)),
      Effect.andThen(query("PRAGMA wal_autocheckpoint")),
      Effect.tap((rows) => Effect.sync(() => {
        fallbackReady = Number(rows[0]?.wal_autocheckpoint) === CHECKPOINT_FRAMES;
        if (!fallbackReady) fallbackFailure = failed(new Error("Checkpoint fallback was not restored"), "Checkpoint fallback was not restored", "execute");
        publish();
      })),
      Effect.catch((cause) => Effect.sync(() => { fallbackFailure = cause; publish(); })),
    ));
  };
  const send = (message: unknown) => {
    try {
      // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers have no target origin.
      worker.postMessage(message);
    } catch { handBack(); }
  };
  const acknowledge = () => {
    if (stopped || active || inTransaction() || truncate === undefined || acknowledged) return;
    acknowledged = true;
    send({ type: "truncate", truncateId: truncate });
  };
  const request = (force = false) => {
    if (stopped || active || inTransaction()) return;
    if (truncate !== undefined) { acknowledge(); return; }
    if (inFlight !== undefined) return;
    if (!force && !uncertain && !pressure && log - checkpointed < CHECKPOINT_FRAMES) return;
    inFlight = ++nextId;
    send({ type: "check", id: inFlight, generation, journalSizeLimit: sizeLimit, force });
  };
  const observe = Effect.gen(function* () {
    if (stopped || inTransaction()) return;
    const [state] = yield* query("PRAGMA wal_checkpoint(NOOP)");
    const [limit] = yield* query("PRAGMA journal_size_limit");
    const nextLog = readSafeSqliteInteger(state?.log);
    const nextCheckpointed = readSafeSqliteInteger(state?.checkpointed);
    if (nextLog === undefined || nextCheckpointed === undefined) {
      uncertain = true;
    } else {
      log = Math.max(0, nextLog);
      checkpointed = Math.max(0, nextCheckpointed);
      sizeLimit = readSafeSqliteInteger(limit?.journal_size_limit);
      let oversized = false;
      if (sizeLimit !== undefined && sizeLimit >= 0) {
        try { oversized = NodeFS.statSync(filename + "-wal").size > sizeLimit; }
        catch (cause) {
          if (!Predicate.hasProperty(cause, "code") || cause.code !== "ENOENT") throw cause;
        }
      }
      pressure = log >= CHECKPOINT_BACKPRESSURE_FRAMES || oversized;
      uncertain = false;
    }
  }).pipe(Effect.catchCause(() => Effect.sync(() => { uncertain = true; })));
  worker.on("error", handBack);
  worker.on("exit", handBack);
  worker.on("message", (message: unknown) => {
    if (stopped || message === null || typeof message !== "object" || !("type" in message)) return;
    const state = message as Record<string, unknown>;
    if (message.type === "truncate-request") {
      if (typeof state.truncateId !== "number" || !Number.isSafeInteger(state.truncateId)) return;
      truncate = state.truncateId;
      acknowledged = false;
      publish();
      acknowledge();
    } else if (message.type === "checkpoint") {
      const completed = truncate !== undefined && state.truncateId === truncate;
      if (completed) { truncate = undefined; acknowledged = false; }
      if ((completed || state.generation === generation) && !active && !inTransaction()) {
        const nextLog = readSafeSqliteInteger(state.log);
        const nextCheckpointed = readSafeSqliteInteger(state.checkpointed);
        if (nextLog !== undefined && nextCheckpointed !== undefined && typeof state.blocked === "boolean") {
          log = Math.max(0, nextLog); checkpointed = Math.max(0, nextCheckpointed);
          pressure = state.blocked; uncertain = false;
        }
      }
      if (state.id === inFlight) inFlight = undefined;
      publish();
      if (!completed && state.generation !== generation) request(true);
    }
  });
  yield* Effect.addFinalizer(() => Effect.promise(async () => {
    stopped = true;
    fallbackReady = false;
    readers.onReaderReleased(() => {});
    await worker.terminate();
  }));
  yield* query("PRAGMA wal_autocheckpoint = 0");
  worker.unref();
  readers.onReaderReleased(() => request(true));
  const wait = Effect.callback<void, SqlError>((resume) => {
    const ready = () => {
      if (fallbackFailure !== undefined) {
        listeners.delete(ready); worker.unref(); resume(Effect.fail(fallbackFailure)); return;
      }
      if (inTransaction() || (stopped ? fallbackReady : !uncertain && !pressure && truncate === undefined)) {
        listeners.delete(ready); worker.unref(); resume(Effect.void);
      }
    };
    listeners.add(ready);
    worker.ref();
    if (pressure || uncertain || truncate !== undefined) request(true);
    ready();
    return Effect.sync(() => { listeners.delete(ready); if (listeners.size === 0) worker.unref(); });
  });
  const run = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E | SqlError> =>
    Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
      // Cancellation is restored only while waiting; a posted write and its observation drain.
      yield* Effect.withFiber((fiber) => (fiber.getRef(TransactionAcquire)?.restore ?? restore)(wait));
      active = true;
      generation++;
      return yield* effect.pipe(Effect.ensuring(
        observe.pipe(Effect.ensuring(Effect.sync(() => {
          active = false;
          publish();
          request();
          acknowledge();
        }))),
      ));
    }));
  return { run };
});

const make = Effect.fn("makeWithDatabase")(function* (
  options: SqliteClientConfig,
): Effect.fn.Return<Client.SqlClient, SqlError, Scope.Scope | Reactivity.Reactivity> {
  yield* checkNodeSqliteCompat();

  const compiler = Statement.makeCompilerSqlite();
  const isFileDatabase = options.filename !== ":memory:" && options.filename !== "";

  const readerAdmission = makeCheckpointReaderAdmission();
  let readerRetirementUncertain = false;
  const makeConnection = Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const db = yield* Effect.try({
      try: () =>
        new NodeSqlite.DatabaseSync(options.filename, {
          readOnly: options.readonly ?? false,
          allowExtension: false,
        }),
      catch: (cause) =>
        new SqlError({
          reason: classifyError(cause, "Failed to open database", "open"),
        }),
    });
    yield* Scope.addFinalizer(
      scope,
      Effect.try({
        try: () => {
          if (readerRetirementUncertain)
            throw new Error("SQLite writer retained because reader termination is unproved");
          db.close();
        },
        catch: (cause) =>
          new SqlError({
            reason: classifyError(cause, "Failed to close database", "close"),
          }),
      }).pipe(Effect.orDie),
    );
    // Started after the close finalizer, so the worker stops before the writer closes.
    const checkpoint =
      options.checkpointWorker && isFileDatabase
        ? yield* startJonesCheckpointWorker(options.filename, db, readerAdmission)
        : undefined;

    const statementReaderCache = new WeakMap<NodeSqlite.StatementSync, boolean>();
    interface CheckpointClassification {
      backpressure: boolean;
      check: boolean;
    }
    const checkpointClassifications = new WeakMap<
      NodeSqlite.StatementSync,
      CheckpointClassification
    >();
    const mutatingPragmasWithValues = new Set([
      "application_id",
      "auto_vacuum",
      "journal_mode",
      "schema_version",
      "user_version",
    ]);
    const mutatesPragma = (name: string | null, value: string | null) => {
      const pragma = name?.toLowerCase();
      if (pragma === undefined) return false;
      if (pragma === "incremental_vacuum" || pragma === "optimize") return true;
      if (pragma === "wal_checkpoint") return value?.toLowerCase() !== "noop";
      return value !== null && mutatingPragmasWithValues.has(pragma);
    };
    let activeClassification: CheckpointClassification | undefined;
    const withClassification = <A>(
      classification: CheckpointClassification | undefined,
      f: () => A,
    ): A => {
      if (classification === undefined) return f();
      const previous = activeClassification;
      activeClassification = classification;
      try {
        return f();
      } finally {
        activeClassification = previous;
      }
    };
    if (checkpoint !== undefined) {
      // SQLite may reprepare an expired statement while it executes. Keep this
      // callback installed and collect only around that statement's prepare/run.
      db.setAuthorizer((actionCode, action, argument) => {
        const classification = activeClassification;
        if (classification === undefined) return NodeSqlite.constants.SQLITE_OK;
        const constants = NodeSqlite.constants;
        if (actionCode === constants.SQLITE_TRANSACTION) {
          classification.backpressure ||= action === "BEGIN";
          classification.check ||= action === "COMMIT" || action === "ROLLBACK";
        } else if (actionCode === constants.SQLITE_SAVEPOINT) {
          classification.backpressure ||= action === "BEGIN";
          classification.check = true;
        } else if (actionCode === constants.SQLITE_PRAGMA && mutatesPragma(action, argument)) {
          classification.backpressure = true;
          classification.check = true;
        } else if (
          actionCode !== constants.SQLITE_READ &&
          actionCode !== constants.SQLITE_SELECT &&
          actionCode !== constants.SQLITE_FUNCTION &&
          actionCode !== constants.SQLITE_RECURSIVE &&
          actionCode !== constants.SQLITE_PRAGMA
        ) {
          classification.backpressure = true;
          classification.check = true;
        }
        return constants.SQLITE_OK;
      });
    }
    const hasRows = (statement: NodeSqlite.StatementSync): boolean => {
      const cached = statementReaderCache.get(statement);
      if (cached !== undefined) {
        return cached;
      }
      const value = statement.columns().length > 0;
      statementReaderCache.set(statement, value);
      return value;
    };

    const prepare = (sql: string) =>
      Effect.try({
        try: () => {
          const classification = { backpressure: false, check: false };
          const statement = withClassification(classification, () => db.prepare(sql));
          if (checkpoint !== undefined) checkpointClassifications.set(statement, classification);
          return statement;
        },
        catch: (cause) =>
          new SqlError({
            reason: classifyError(cause, "Failed to prepare statement", "prepare"),
          }),
      });

    const prepareCache = yield* Cache.makeWith(prepare, {
      capacity: 200,
      // A transient prepare failure must not outlive the lock or missing schema.
      timeToLive: (exit) => (Exit.isSuccess(exit) ? Duration.minutes(10) : Duration.zero),
    });

    const statementFor = (sql: string, params: ReadonlyArray<unknown>) =>
      hasLargeBinding(params) ? prepare(sql) : Cache.get(prepareCache, sql);

    const runStatement = (
      statement: NodeSqlite.StatementSync,
      params: ReadonlyArray<unknown>,
      mode: "rows" | "raw" | "values" = "rows",
    ) =>
      Effect.withFiber<ReadonlyArray<any>, SqlError>((fiber) => {
        const classification = checkpointClassifications.get(statement);
        const execute = () => {
          let arrays = false;
          let result: Exit.Exit<ReadonlyArray<unknown>, SqlError>;
          try {
            const value = withClassification(classification, () => {
              statement.setReadBigInts(Boolean(Context.get(fiber.context, Client.SafeIntegers)));
              if (hasRows(statement)) {
                arrays = mode === "values";
                if (arrays) statement.setReturnArrays(true);
                return statement.all(...(params as any));
              }
              const result = statement.run(...(params as any));
              return mode === "raw" ? (result as unknown as ReadonlyArray<unknown>) : [];
            });
            result = Exit.succeed(value);
          } catch (cause) {
            result = Exit.fail(
              new SqlError({
                reason: classifyError(cause, "Failed to execute statement", "execute"),
              }),
            );
          } finally {
            // OR FAIL may have committed earlier rows even when execution throws.
            if (classification?.check) checkpoint?.check();
          }
          if (arrays) {
            try {
              statement.setReturnArrays(false);
            } catch (cause) {
              return Exit.die(
                new SqlError({
                  reason: classifyError(
                    cause,
                    "Failed to reset statement result mode",
                    "resetResultMode",
                  ),
                }),
              );
            }
          }
          return result;
        };
        return checkpoint !== undefined && classification?.backpressure
          ? checkpoint.runWithAdmission(execute)
          : execute();
      });

    const run = (sql: string, params: ReadonlyArray<unknown>, raw = false) =>
      Effect.flatMap(statementFor(sql, params), (statement) =>
        runStatement(statement, params, raw ? "raw" : "rows"),
      );
    const runValues = (sql: string, params: ReadonlyArray<unknown>) =>
      Effect.flatMap(statementFor(sql, params), (statement) =>
        runStatement(statement, params, "values"),
      );

    return identity<Connection>({
      execute(sql, params, rowTransform) {
        return rowTransform ? Effect.map(run(sql, params), rowTransform) : run(sql, params);
      },
      executeRaw(sql, params) {
        return run(sql, params, true);
      },
      executeValues(sql, params) {
        return runValues(sql, params);
      },
      executeValuesUnprepared(sql, params) {
        return Effect.flatMap(prepare(sql), (statement) =>
          runStatement(statement, params ?? [], "values"),
        );
      },
      executeUnprepared(sql, params, rowTransform) {
        const effect = prepare(sql).pipe(
          Effect.flatMap((statement) => runStatement(statement, params ?? [])),
        );
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeStream(_sql, _params) {
        return Stream.die(new UnsupportedNodeSqliteOperationError());
      },
    });
  });

  const semaphore = yield* Semaphore.make(1);
  const writerWorker = options.writerWorker === true && isFileDatabase;
  let writerInTransaction = false;
  let writerTransactionStateAvailable = false;
  const writerConnection: Effect.Effect<Connection, SqlError> = writerWorker
    ? yield* makeWorkerConnection({
        filename: options.filename,
        role: "writer",
        readOnly: options.readonly ?? false,
        onReply: (transaction) => {
          writerTransactionStateAvailable = transaction !== undefined;
          writerInTransaction = transaction === true;
        },
        canClose: () => !readerRetirementUncertain,
      })
    : Effect.succeed(yield* makeConnection);
  // Opening now fails the layer for a file that is not a database, not its first query.
  const opened = yield* writerConnection;
  // One checkpoint authority per writer. Wrap every operation through completion, including COMMIT.
  const asyncCheckpoint = writerWorker && options.readonly !== true
    ? yield* startJonesAsyncCheckpointWorker(options.filename, opened, readerAdmission, () => writerInTransaction, () => writerTransactionStateAvailable)
    : undefined;
  const admittedWriter: Effect.Effect<Connection, SqlError> = asyncCheckpoint === undefined
    ? writerConnection
    : Effect.map(writerConnection, (connection) => ({
        execute: (sql, params, transform) => asyncCheckpoint.run(connection.execute(sql, params, transform)),
        executeRaw: (sql, params) => asyncCheckpoint.run(connection.executeRaw(sql, params)),
        executeValues: (sql, params) => asyncCheckpoint.run(connection.executeValues(sql, params)),
        executeUnprepared: (sql, params, transform) => asyncCheckpoint.run(connection.executeUnprepared(sql, params, transform)),
        executeValuesUnprepared: (sql, params) => asyncCheckpoint.run(connection.executeValuesUnprepared(sql, params)),
        executeStream: (sql, params) => connection.executeStream(sql, params),
      } satisfies Connection));
  const readersEnabled =
    options.readerWorker !== false &&
    isFileDatabase &&
    (options.readerWorker === true || options.readerPool !== undefined);
  const pool =
    readersEnabled && options.readerPool !== undefined
      ? yield* Effect.gen(function* () {
          const management = yield* Semaphore.make(1);
          const slots = [];
          for (let slotId = 0; slotId < options.readerPool!.size; slotId++) {
            slots.push(
              yield* makeJonesReader(options, slotId, management, () => {
                readerRetirementUncertain = true;
              }),
            );
          }
          return yield* makeReaderPool(options.readerPool!, slots, readerAdmission);
        })
      : undefined;
  const readerPool = { unavailable: false };
  const readers =
    options.readerWorker && isFileDatabase && pool === undefined
      ? yield* Effect.forEach(Array.from({ length: READER_POOL_SIZE }), () =>
          Effect.map(
            Effect.all([
              options.readerStartupTimeout !== undefined || options.readerFailureMode !== undefined
                ? Effect.map(makeJonesReader(options), (slot) => slot.acquire)
                : makeReader(options.filename, readerPool),
              Semaphore.make(1),
            ]),
            ([acquire, lock]) => ({
              acquire,
              lock,
              claimed: false,
            }),
          ),
        )
      : [];
  // One permit per slot, so a queued read waits for any reader, not one it picked while all were busy.
  const legacyReaderAdmission = yield* Semaphore.make(readers.length);

  interface Target {
    readonly name: Acquired["connection"];
    readonly lock: Semaphore.Semaphore;
    readonly connection: Connection;
  }
  const writer = Effect.map(admittedWriter, (connection): Target => ({
    name: "writer",
    lock: semaphore,
    connection,
  }));
  let activeWriter: ConnectionOwner | undefined;
  // Callers must run route uninterruptibly and pass `claimed` a finalizer that runs `release` once.
  // Waits use the caller's `restore`, not readOnly's, which may have been captured outside a mask.
  const route = (
    claimed: (release: Effect.Effect<void>) => Effect.Effect<void>,
    restore: typeof Effect.interruptible,
  ) =>
    Effect.withFiber((fiber): Effect.Effect<Target, SqlError> => {
      if (readers.length === 0 || (readerPool.unavailable && options.readerFailureMode !== "fail") || !Context.get(fiber.context, ReadOnly)) {
        return writer;
      }
      const claim = Effect.andThen(
        restore(legacyReaderAdmission.take(1)),
        Effect.suspend(() => {
          // Holding a permit leaves a slot idle; the lowest wins, so idle slots never start a worker.
          const reader = readers.find((slot) => !slot.claimed)!;
          reader.claimed = true;
          const release = Effect.andThen(
            Effect.sync(() => {
              reader.claimed = false;
            }),
            legacyReaderAdmission.release(1),
          );
          return Effect.as(claimed(release), reader);
        }),
      );
      return Effect.flatMap(
        // A scheduler yield after taking the permit must not precede its release finalizer.
        Effect.provideService(claim, Scheduler.PreventSchedulerYield, true),
        (reader) =>
          Effect.flatMap(
            Effect.provideService(reader.acquire, ReadOnly, restore),
            (readerConnection) =>
              readerConnection === undefined
                ? options.readerFailureMode === "fail"
                  ? Effect.fail(failed(new Error("Database reader is unavailable"), workerFailureMessages.open, "open"))
                  : writer
                : Effect.succeed<Target>({
                    name: "reader",
                    lock: reader.lock,
                    connection: readerConnection,
                  }),
          ),
      );
    });

  const acquired = (
    target: { readonly name: Acquired["connection"] },
    clock: Clock.Clock,
    start: bigint,
  ): Acquired => ({
    connection: target.name,
    lockWaitMs: Number((clock.monotonicTimeNanosUnsafe() - start) / 1000n) / 1000,
  });

  const legacyBorrower: Borrower = (use) =>
    Effect.uninterruptibleMask((restore) => {
      let release: Effect.Effect<void> = Effect.void;
      const claimed = (next: Effect.Effect<void>) =>
        Effect.sync(() => {
          release = next;
        });
      // Started before routing, so lock wait includes waiting for reader admission.
      return Effect.withFiber((fiber) => {
        const start = fiber.getRef(Clock.Clock).monotonicTimeNanosUnsafe();
        return Effect.flatMap(route(claimed, restore), (target) =>
          restore(borrowTarget(target, use, start)),
        );
      }).pipe(Effect.ensuring(Effect.suspend(() => release)));
    });
  const borrowTarget = <A, E, R>(
    target: Target,
    use: (connection: Connection) => Effect.Effect<A, E, R>,
    start: bigint,
  ): Effect.Effect<A, E, R> =>
    Effect.withFiber((fiber) => {
      const clock = fiber.getRef(Clock.Clock);
      const ownerAtWaitStart = target.name === "writer" ? activeWriter : undefined;
      const owner = target.name === "writer" ? connectionOwner(fiber) : undefined;
      const useConnection = Effect.suspend(() => {
        if (target.name === "writer") activeWriter = owner;
        if (
          fiber.getRef(References.TracerEnabled) &&
          fiber.getRef(Statement.SpanPropagationEnabled)
        ) {
          annotateAcquired(fiber, acquired(target, clock, start), ownerAtWaitStart);
        }
        return use(target.connection);
      });
      const useOwnedConnection =
        target.name === "writer"
          ? Effect.onExit(useConnection, () =>
              Effect.sync(() => {
                if (activeWriter === owner) activeWriter = undefined;
              }),
            )
          : useConnection;
      return target.lock.withPermits(1)(useOwnedConnection);
    });
  const claimedUntilScopeCloses = (release: Effect.Effect<void>) =>
    Effect.withFiber((fiber) =>
      Scope.addFinalizer(Context.getUnsafe(fiber.context, Scope.Scope), release),
    );
  // Started before routing, so lock wait includes waiting for reader admission.
  const legacyTransactionAcquirer = Effect.uninterruptibleMask((maskRestore) =>
    Effect.withFiber((fiber) => {
      const start = fiber.getRef(Clock.Clock).monotonicTimeNanosUnsafe();
      const pending = fiber.getRef(TransactionAcquire);
      const restore = pending?.restore ?? maskRestore;
      return Effect.flatMap(route(claimedUntilScopeCloses, restore), (target) => {
        const scope = Context.getUnsafe(fiber.context, Scope.Scope);
        const clock = fiber.getRef(Clock.Clock);
        const ownerAtWaitStart = target.name === "writer" ? activeWriter : undefined;
        const owner = target.name === "writer" ? connectionOwner(fiber) : undefined;
        const acquire = Effect.tap(restore(target.lock.take(1)), () => {
          if (target.name === "writer") activeWriter = owner;
          if (pending !== undefined) {
            pending.acquired = acquired(target, clock, start);
            pending.ownerAtWaitStart = ownerAtWaitStart;
          }
          return Scope.addFinalizer(
            scope,
            Effect.sync(() => {
              if (target.name === "writer" && activeWriter === owner) activeWriter = undefined;
            }).pipe(Effect.andThen(target.lock.release(1))),
          );
        });
        return Effect.as(
          // A scheduler yield after taking the permit must not precede its release finalizer.
          Effect.provideService(acquire, Scheduler.PreventSchedulerYield, true),
          target.connection,
        );
      });
    }),
  );

  const wantsPool = Effect.withFiber((fiber) =>
    Effect.succeed(pool !== undefined && fiber.getRef(ReadOnly) !== undefined),
  );
  const poolAcquire = Effect.uninterruptibleMask((restore) =>
    Effect.withFiber((fiber) => {
      const pending = fiber.getRef(TransactionAcquire);
      const clock = fiber.getRef(Clock.Clock);
      const start = clock.monotonicTimeNanosUnsafe();
      const acquire = pool!.acquire(fiber.getRef(ReadPriority), fiber);
      return (pending?.restore ?? fiber.getRef(ReadOnly) ?? restore)(acquire).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            const acquisition: Acquired = {
              connection: "reader",
              lockWaitMs: Number(clock.monotonicTimeNanosUnsafe() - start) / 1e6,
            };
            if (pending !== undefined) pending.acquired = acquisition;
            else if (
              fiber.getRef(References.TracerEnabled) &&
              fiber.getRef(Statement.SpanPropagationEnabled)
            ) {
              annotateAcquired(fiber, acquisition);
            }
          }),
        ),
        Effect.map((lease) => lease.connection),
      );
    }),
  );
  const borrower: Borrower = (use) =>
    Effect.flatMap(wantsPool, (enabled) =>
      enabled ? Effect.scoped(Effect.flatMap(poolAcquire, use)) : legacyBorrower(use),
    );
  const transactionAcquirer: Effect.Effect<Connection, SqlError, Scope.Scope> = Effect.suspend(() =>
    Effect.flatMap(Effect.serviceOption(client.transactionService), (transaction) =>
      Option.isSome(transaction)
        ? Effect.succeed(transaction.value[0])
        : Effect.flatMap(wantsPool, (enabled) =>
            enabled ? poolAcquire : legacyTransactionAcquirer,
          ),
    ),
  );

  const client: Client.SqlClient = yield* Client.make({
    driver: "node:sqlite",
    acquirer: transactionAcquirer,
    borrower,
    compiler,
    transactionAcquirer,
    // BEGIN IMMEDIATE avoids read-then-write SQLITE_BUSY_SNAPSHOT, which busy_timeout cannot resolve.
    beginTransaction: options.readonly === true ? "BEGIN" : "BEGIN IMMEDIATE",
    spanAttributes: [
      ...(options.spanAttributes ? Object.entries(options.spanAttributes) : []),
      [ATTR_DB_SYSTEM_NAME, "sqlite"],
    ],
  });
  const withTransaction = client.withTransaction;
  return Object.assign(client, {
    withTransaction: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      Effect.uninterruptibleMask((restore) => {
        // SqlClient masks setup; acquisition must preserve the caller's interruptibility.
        const pending: {
          acquired?: Acquired;
          ownerAtWaitStart: ConnectionOwner | undefined;
          restore: typeof Effect.interruptible | undefined;
        } = {
          ownerAtWaitStart: undefined,
          restore,
        };
        const body = Effect.withFiber<A, E, R>((fiber) => {
          annotateAcquired(fiber, pending.acquired, pending.ownerAtWaitStart);
          pending.restore = undefined;
          return effect;
        });
        return restore(
          withTransaction(body).pipe(Effect.provideService(TransactionAcquire, pending)),
        );
      }),
  });
});

export const layer = (config: SqliteClientConfig): Layer.Layer<Client.SqlClient, SqlError> =>
  Layer.effect(Client.SqlClient, make(config)).pipe(Layer.provide(Reactivity.layer));
