// @effect-diagnostics-next-line nodeBuiltinImport:off -- Fixture placement needs native statfs to reject memory-backed storage before constructing DatabaseSync.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous fixture placement guards share native paths with DatabaseSync and statfs.
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import type * as NodeWorkerThreads from "node:worker_threads";
import { assert } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Scheduler from "effect/Scheduler";
import type * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import { vi } from "vite-plus/test";
import * as SqliteClient from "../nodeSqliteClient.ts";

// Restores the small real-WAL fixture and scheduler seam from upstream 616819ac17.
// Holds affect transport/scheduling only; admission and checkpoint SQL stay real.
export const receipt = <A = void>() => {
  let resolve!: (value: A) => void;
  const promise = new Promise<A>((done) => {
    resolve = done;
  });
  return { promise, complete: resolve, wait: Effect.promise(() => promise) };
};

export type Message = Record<string, unknown>;
export const isMessage = (value: unknown): value is Message =>
  typeof value === "object" && value !== null;

export const admissionScheduler = (afterResume = false) => {
  const registered = receipt();
  const paused = receipt();
  const continuation = receipt();
  let registrations = 0;
  let dispatches = 0;
  let resuming = false;
  let yielded = false;
  let holdNext = false;
  let released = false;
  let heldTask: (() => void) | undefined;
  const immediates = new Set<ReturnType<typeof setImmediate>>();
  const enqueue = (task: () => void) => {
    const immediate = setImmediate(() => {
      immediates.delete(immediate);
      task();
    });
    immediates.add(immediate);
    return () => {
      clearImmediate(immediate);
      immediates.delete(immediate);
    };
  };
  const mixed = new Scheduler.MixedScheduler("async", (task) => {
    if (holdNext && !released) {
      holdNext = false;
      heldTask = task;
      if (resuming) continuation.complete();
      else paused.complete();
      return () => {
        if (heldTask === task) heldTask = undefined;
      };
    }
    return enqueue(task);
  });
  const scheduler: Scheduler.Scheduler = {
    executionMode: mixed.executionMode,
    makeDispatcher: () => {
      // startImmediately + no forced yield: the first dispatcher is created by
      // runWithAdmission's callback registration, before any election dispatch.
      registrations++;
      registered.complete();
      const dispatcher = mixed.makeDispatcher();
      return {
        flush: () => dispatcher.flush(),
        scheduleTask(task, priority) {
          dispatches++;
          dispatcher.scheduleTask(task, priority);
        },
      };
    },
    shouldYield: () => {
      if (resuming && afterResume && !yielded && !released) {
        yielded = true;
        holdNext = true;
        return true;
      }
      return false;
    },
  };
  const resume = () => {
    const task = heldTask;
    heldTask = undefined;
    if (task)
      enqueue(() => {
        resuming = true;
        try {
          task();
        } finally {
          resuming = false;
        }
      });
  };
  return {
    scheduler,
    registered: registered.wait,
    paused: paused.wait,
    continuation: continuation.wait,
    get registrations() {
      return registrations;
    },
    get dispatches() {
      return dispatches;
    },
    arm() {
      holdNext = true;
    },
    resume,
    release() {
      released = true;
      holdNext = false;
      // Drain rather than discard: interruption may need the held continuation.
      resume();
    },
    assertDrained() {
      assert.isUndefined(heldTask);
      assert.equal(immediates.size, 0);
    },
  };
};

const scratchDirectory = () => {
  // GitHub's job-owned root is already provisioned; local qualification supplies
  // an explicit task-owned root. Both still pass the same placement checks.
  const configured =
    process.env.JONES_SQLITE_WRITER_ADMISSION_SCRATCH_ROOT ??
    (process.env.GITHUB_ACTIONS === "true" ? process.env.RUNNER_TEMP : undefined);
  assert(
    configured && NodePath.isAbsolute(configured),
    "unavailable coverage: set JONES_SQLITE_WRITER_ADMISSION_SCRATCH_ROOT to an existing task-owned disk directory",
  );
  const root = NodeFS.realpathSync(configured);
  assert.isTrue(NodeFS.statSync(root).isDirectory());
  const filesystem = NodeFS.statfsSync(root);
  assert.notInclude(
    [0x01021994, 0x858458f6],
    filesystem.type,
    "scratch must be disk-backed, not tmpfs or ramfs",
  );
  const repository = NodeFS.realpathSync(NodePath.join(import.meta.dirname, "../../../.."));
  const inside = (parent: string) => {
    const path = NodePath.relative(parent, root);
    return (
      path === "" ||
      (path !== ".." && !path.startsWith(`..${NodePath.sep}`) && !NodePath.isAbsolute(path))
    );
  };
  assert.isFalse(inside(repository), "scratch must be outside the repository");
  if (NodeFS.existsSync("/tmp")) {
    assert.isFalse(inside(NodeFS.realpathSync("/tmp")), "scratch must be outside /tmp");
  }
  return NodeFS.mkdtempSync(NodePath.join(root, "sqlite-writer-admission-"));
};

const requireCapabilities = () => {
  const db = new NodeSqlite.DatabaseSync(":memory:");
  try {
    const version = String(db.prepare("SELECT sqlite_version() AS version").get()?.version);
    const [major = 0, minor = 0] = version.split(".").map(Number);
    assert.isTrue(
      major > 3 || (major === 3 && minor >= 51),
      `unavailable coverage: SQLite ${version} has no genuine NOOP checkpoint`,
    );
    assert.equal(typeof db.setAuthorizer, "function", "unavailable coverage: authorizer required");
    assert.equal(typeof db.isTransaction, "boolean");
    let authorized = false;
    db.setAuthorizer(() => {
      authorized = true;
      return NodeSqlite.constants.SQLITE_OK;
    });
    db.prepare("SELECT 1").get();
    assert.isTrue(authorized);
    assert.isNumber(db.prepare("PRAGMA wal_checkpoint(NOOP)").get()?.log);
  } finally {
    db.close();
  }
};

const observeWorker = (worker: NodeWorkerThreads.Worker) => {
  const messages: unknown[] = [];
  const waiters = new Set<() => void>();
  let terminal: Error | undefined;
  let exitCode: number | undefined;
  const exited = receipt<number>();
  const onMessage = (message: unknown) => {
    messages.push(message);
    for (const wake of [...waiters]) wake();
  };
  const onError = (error: Error) => {
    terminal = error;
    for (const wake of [...waiters]) wake();
  };
  const onExit = (code: number) => {
    exitCode = code;
    exited.complete(code);
    onError(new Error(`checkpoint worker exited: ${code}`));
  };
  worker.on("message", onMessage);
  worker.on("error", onError);
  worker.on("exit", onExit);
  return {
    messages,
    exited: exited.wait,
    get exitCode() {
      return exitCode;
    },
    wait(predicate: (value: unknown) => boolean, after = 0) {
      return Effect.callback<unknown>((resume) => {
        const wake = () => {
          const found = messages.slice(after).find(predicate);
          if (found !== undefined) {
            waiters.delete(wake);
            resume(Effect.succeed(found));
          } else if (terminal) {
            waiters.delete(wake);
            resume(Effect.die(terminal));
          }
        };
        waiters.add(wake);
        wake();
        return Effect.sync(() => {
          waiters.delete(wake);
        });
      });
    },
    close() {
      worker.off("message", onMessage);
      worker.off("error", onError);
      worker.off("exit", onExit);
      assert.equal(waiters.size, 0, "all message waiters must leave with their scope");
    },
  };
};

const checkpointTraffic = (worker: NodeWorkerThreads.Worker) => {
  const send = worker.postMessage.bind(worker);
  const queued: Array<Parameters<typeof worker.postMessage>> = [];
  const requests: Message[] = [];
  const sent: Message[] = [];
  let restored = false;
  const forward = (args: Parameters<typeof worker.postMessage>) => {
    if (isMessage(args[0])) sent.push(args[0]);
    send(...args);
  };
  const spy = vi.spyOn(worker, "postMessage").mockImplementation((...args) => {
    if (isMessage(args[0])) requests.push(args[0]);
    queued.push(args);
  });
  return {
    queued,
    requests,
    sent,
    next() {
      const message = queued[0]?.[0];
      assert(isMessage(message), "expected a real parent-generated request");
      return message;
    },
    sendNext() {
      const args = queued.shift();
      assert.isDefined(args, "expected a real parent-generated request");
      forward(args!);
      return args![0] as Message;
    },
    barrier() {
      // No invented check ID: the real worker acknowledges this after its work.
      send("barrier");
    },
    release() {
      if (restored) return;
      restored = true;
      spy.mockRestore();
      for (const args of queued.splice(0)) forward(args);
    },
  };
};

export const holdMessage = (
  worker: NodeWorkerThreads.Worker,
  predicate: (message: Message) => boolean,
) => {
  const ready = receipt<Message>();
  const emit = worker.emit.bind(worker);
  let held: Message | undefined;
  let restored = false;
  const spy = vi.spyOn(worker, "emit").mockImplementation((event, ...args) => {
    if (event === "message" && held === undefined && isMessage(args[0]) && predicate(args[0])) {
      held = args[0];
      ready.complete(held);
      return true;
    }
    return emit(event, ...args);
  });
  return {
    ready: ready.wait,
    release() {
      if (restored) return;
      restored = true;
      spy.mockRestore();
      if (held) emit("message", held);
    },
  };
};

export type Fixture = {
  sql: SqlClient.SqlClient;
  filename: string;
  worker: NodeWorkerThreads.Worker;
  journal: ReturnType<typeof observeWorker>;
  traffic: ReturnType<typeof checkpointTraffic>;
  own: (release: () => void) => void;
  release: () => void;
};

// oxlint-disable-next-line t3code/prefer-effect-fn -- Generic test scope wrapper preserves the caller's result and error types without adding a span.
export const withCheckpointClient = <A, E>(
  checkpointFor: (filename: string) => NodeWorkerThreads.Worker | undefined,
  use: (fixture: Fixture) => Effect.Effect<A, E, Scope.Scope | SqlClient.SqlClient>,
) =>
  Effect.gen(function* () {
    requireCapabilities();
    const directory = yield* Effect.acquireRelease(Effect.sync(scratchDirectory), (path) =>
      Effect.sync(() => {
        NodeFS.rmSync(path, { recursive: true, force: true });
        assert.isFalse(
          NodeFS.existsSync(path),
          "the creating fixture must remove its exact scratch root",
        );
      }),
    );
    const filename = NodePath.join(directory, "synthetic.sqlite");
    return yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const worker = checkpointFor(filename);
      assert.isDefined(worker, "unavailable coverage: real checkpoint worker did not activate");
      assert.deepEqual(yield* sql`PRAGMA wal_autocheckpoint`, [{ wal_autocheckpoint: 0 }]);
      assert.deepEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
      const journal = observeWorker(worker!);
      yield* Effect.addFinalizer(() => Effect.sync(journal.close));
      const traffic = checkpointTraffic(worker!);
      const releases: Array<() => void> = [];
      const release = () => {
        for (const cleanup of releases.splice(0)) cleanup();
        traffic.release();
      };
      const fixture: Fixture = {
        sql,
        filename,
        worker: worker!,
        journal,
        traffic,
        own: (release) => {
          releases.push(release);
        },
        release,
      };
      // The inner body releases holds BEFORE child/scope finalizers can await
      // masked cleanup. The layer then stops workers before closing its database.
      return yield* Effect.scoped(use(fixture).pipe(Effect.ensuring(Effect.sync(release))));
    }).pipe(
      Effect.provide(
        SqliteClient.layer({
          filename,
          checkpointWorker: true,
          readerWorker: true,
          readerFailureMode: "fail",
        }),
      ),
    );
  }).pipe(Effect.scoped);

export const pinSmallWal = Effect.fn("sqliteWriterAdmission.pinSmallWal")(function* (
  fixture: Fixture,
) {
  const { sql, filename } = fixture;
  yield* sql.unsafe("PRAGMA journal_size_limit = 1").unprepared;
  yield* sql.unsafe("CREATE TABLE entries(value INTEGER PRIMARY KEY)").unprepared;
  yield* sql.unsafe("CREATE TABLE pages(payload BLOB)").unprepared;
  const reader = yield* Effect.acquireRelease(
    Effect.sync(() => new NodeSqlite.DatabaseSync(filename)),
    (db) => Effect.sync(() => db.close()),
  );
  let pinned = false;
  const releasePin = () => {
    if (pinned) {
      pinned = false;
      reader.exec("ROLLBACK");
    }
  };
  fixture.own(releasePin);
  reader.exec("BEGIN");
  pinned = true;
  reader.prepare("SELECT COUNT(*) FROM entries").get();
  yield* sql.unsafe(`
    WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1200)
    INSERT INTO pages(payload) SELECT randomblob(4000) FROM n
  `).unprepared;
  return { releasePin };
});

export const checkpointReply = Effect.fn("sqliteWriterAdmission.checkpointReply")(function* (
  fixture: Fixture,
  request: Message,
  after = 0,
) {
  assert.equal(request.type, "check");
  assert.isNumber(request.id);
  const reply = yield* fixture.journal.wait(
    (message) => isMessage(message) && message.type === "checkpoint" && message.id === request.id,
    after,
  );
  assert(isMessage(reply));
  return reply;
});

export const blockBehindReader = Effect.fn("sqliteWriterAdmission.blockBehindReader")(function* (
  fixture: Fixture,
) {
  const { traffic } = fixture;
  const { releasePin } = yield* pinSmallWal(fixture);
  const request = traffic.sendNext();
  const blocked = yield* checkpointReply(fixture, request);
  assert.equal(blocked.generation, request.generation);
  assert.equal(blocked.blocked, true);
  assert.isAbove(Number(blocked.log), 1000);
  assert.isBelow(Number(blocked.checkpointed), Number(blocked.log));
  return { releasePin, blocked };
});

export const recover = Effect.fn("sqliteWriterAdmission.recover")(function* (
  fixture: Fixture,
  releasePin: () => void,
) {
  const { journal, traffic } = fixture;
  const after = journal.messages.length;
  releasePin();
  // A real barrier forces a check even if the next parent check is coalesced.
  // Keep subsequent parent checks held until handoff has been verified.
  traffic.barrier();
  const request = yield* journal.wait(
    (message) => isMessage(message) && message.type === "truncate-request",
    after,
  );
  assert(isMessage(request));
  while (traffic.next().type === "check") {
    const check = traffic.sendNext();
    yield* checkpointReply(fixture, check, after);
  }
  const acknowledgement = traffic.sendNext();
  assert.deepEqual(acknowledgement, { type: "truncate", truncateId: request.truncateId });
  const completed = yield* journal.wait(
    (message) =>
      isMessage(message) &&
      message.type === "checkpoint" &&
      message.truncateId === request.truncateId &&
      message.blocked === false,
    after,
  );
  assert(isMessage(completed));
  assert.equal(completed.generation, request.generation);
  assert.equal(completed.id, request.checkId);
  return completed;
});

export const readerProgress = Effect.fn("sqliteWriterAdmission.readerProgress")(function* (
  sql: SqlClient.SqlClient,
) {
  assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM entries`.pipe(SqliteClient.readOnly), [
    { count: 0 },
  ]);
});

// A cancellable macrotask drain for scheduler cleanup, never an elapsed-time barrier.
export const eventLoopTurn = Effect.callback<void>((resume) => {
  const immediate = setImmediate(() => resume(Effect.void));
  return Effect.sync(() => clearImmediate(immediate));
});

export const verifyFreshWriter = Effect.fn("sqliteWriterAdmission.verifyFreshWriter")(function* (
  sql: SqlClient.SqlClient,
) {
  yield* sql.withTransaction(sql`INSERT INTO entries VALUES (3)`);
  assert.deepEqual(yield* sql`SELECT value FROM entries ORDER BY value`.values, [[2], [3]]);
});
