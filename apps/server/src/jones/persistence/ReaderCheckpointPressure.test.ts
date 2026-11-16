// @effect-diagnostics nodeBuiltinImport:off -- Synthetic native SQLite fixtures own disk placement and worker lifetime.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import type * as NodeWorkerThreads from "node:worker_threads";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqliteClient from "@t3tools/shared/nodeSqliteClient";
import { afterEach, vi } from "vite-plus/test";
import {
  blockBehindReader,
  checkpointReply,
  checkpointTraffic,
  isMessage,
  observeWorker,
  recover,
  scratchDirectory,
  type Fixture,
} from "../../../../../packages/shared/src/jones/sqliteWriterAdmission.fixture.ts";
import { findSqliteReaderAdmissionFailure } from "../../../../../packages/shared/src/jones/sqliteReaders/admissionFailure.ts";
import { readerPoolOptions } from "../serverParallelism/persistenceOptions.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";

const workers = vi.hoisted(
  (): Array<{ worker: NodeWorkerThreads.Worker; checkpoint: boolean; filename: string }> => [],
);

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  class RecordedWorker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      super(...args);
      const data = args[1]?.workerData;
      workers.push({
        worker: this,
        filename: data?.filename,
        checkpoint: typeof data?.backpressureFrames === "number",
      });
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

afterEach(() => {
  for (const { worker } of workers) {
    assert.equal(worker.threadId, -1, "the client must join every owned native worker");
  }
  workers.length = 0;
});

const withClient = <A, E>(use: (fixture: Fixture) => Effect.Effect<A, E, Scope.Scope>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(Effect.sync(scratchDirectory), (path) =>
        Effect.sync(() => {
          try {
            assert.isFalse(NodeFS.existsSync(NodePath.join(path, "state.sqlite-wal")));
            assert.isFalse(NodeFS.existsSync(NodePath.join(path, "state.sqlite-shm")));
          } finally {
            NodeFS.rmSync(path, { recursive: true, force: true });
          }
          assert.isFalse(NodeFS.existsSync(path));
        }),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      return yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const worker = workers.find(
          (entry) => entry.filename === filename && entry.checkpoint,
        )?.worker;
        assert.isDefined(worker, "a real checkpoint worker is required");
        const journal = observeWorker(worker!);
        yield* Effect.addFinalizer(() => Effect.sync(journal.close));
        yield* sql`CREATE TABLE burst_probe(id INTEGER PRIMARY KEY, payload BLOB)`;
        yield* sql`INSERT INTO burst_probe VALUES (1, NULL)`;
        yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
        const after = journal.messages.length;
        // Flush startup checks before holding only this scenario's parent traffic.
        // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers accept no target origin.
        worker!.postMessage("barrier");
        yield* journal.wait((message) => message === "barrier", after);
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
          own: (cleanup) => releases.push(cleanup),
          release,
        };
        // Release pins and held messages before inner child/scope finalizers can join.
        return yield* Effect.scoped(use(fixture).pipe(Effect.ensuring(Effect.sync(release))));
      }).pipe(
        Effect.provide(
          SqlitePersistence.layerFromPath(filename, {
            ...readerPoolOptions(2),
            interactiveQueueTimeoutMs: 100,
            backgroundQueueTimeoutMs: 100,
          }).pipe(Layer.provide(NodeServices.layer)),
        ),
      );
    }),
  );

it.effect.each([128, 32] as const)(
  "discriminates pooled reader admission behind a retained 48 MiB snapshot (limit=%s MiB)",
  (limitMiB) =>
    withClient((fixture) =>
      Effect.gen(function* () {
        const { sql, filename, traffic, journal } = fixture;
        assert.deepEqual(yield* sql`PRAGMA journal_size_limit`, [
          { journal_size_limit: 128 * 1024 * 1024 },
        ]);
        if (limitMiB === 32) yield* sql.unsafe(`PRAGMA journal_size_limit = ${32 * 1024 * 1024}`);

        const count = sql<{ count: number }>`SELECT COUNT(*) AS count FROM burst_probe`.pipe(
          SqliteClient.readOnly,
        );
        assert.deepEqual(yield* count, [{ count: 1 }]);
        const readers = () =>
          workers.filter((entry) => entry.filename === filename && !entry.checkpoint);
        assert.lengthOf(readers(), 1, "the baseline must use a pooled worker");
        const reader = yield* Effect.acquireRelease(
          Effect.sync(() => new NodeSqlite.DatabaseSync(filename, { readOnly: true })),
          (database) => Effect.sync(() => database.close()),
        );
        let pinned = false;
        const releasePin = () => {
          if (pinned) {
            reader.exec("ROLLBACK");
            pinned = false;
          }
        };
        fixture.own(releasePin);
        reader.exec("BEGIN");
        pinned = true;
        assert.equal(reader.prepare("SELECT COUNT(*) AS count FROM burst_probe").get()?.count, 1);
        yield* sql.withTransaction(
          sql`INSERT INTO burst_probe VALUES (2, zeroblob(${48 * 1024 * 1024}))`,
        );
        const request = traffic.sendNext();
        assert.equal(request.journalSizeLimit, limitMiB * 1024 * 1024);
        const state = yield* checkpointReply(fixture, request);
        assert.isAbove(Number(state.log), 10_000);
        assert.isBelow(
          Number(state.log),
          32_768,
          "the frame ceiling must not confound the size limit",
        );
        assert.isBelow(Number(state.checkpointed), Number(state.log));
        assert.equal(state.blocked, limitMiB === 32);
        const walBytes = NodeFS.statSync(`${filename}-wal`).size;
        assert.isAbove(walBytes, 48 * 1024 * 1024);
        assert.isBelow(walBytes, 128 * 1024 * 1024);

        if (limitMiB === 128) {
          assert.deepEqual(yield* count, [{ count: 2 }]);
          yield* sql.withTransaction(sql`INSERT INTO burst_probe VALUES (3, NULL)`);
          assert.equal(reader.prepare("SELECT COUNT(*) AS count FROM burst_probe").get()?.count, 1);
          releasePin();
          const after = journal.messages.length;
          traffic.barrier();
          yield* journal.wait(
            (message) =>
              isMessage(message) &&
              message.type === "checkpoint" &&
              message.blocked === false &&
              message.log === message.checkpointed,
            after,
          );
        } else {
          const failure = yield* count.pipe(Effect.flip);
          assert.deepEqual(findSqliteReaderAdmissionFailure(failure), {
            _tag: "SqliteReaderAdmissionError",
            reason: "checkpoint-held",
          });
          assert.lengthOf(readers(), 1, "checkpoint admission must precede a free slot's startup");
          const recovered = yield* recover(fixture, releasePin);
          assert.equal(recovered.log, recovered.checkpointed);
        }
        traffic.release();
        yield* sql.withTransaction(sql`INSERT INTO burst_probe VALUES (4, NULL)`);
        assert.deepEqual(yield* count, [{ count: limitMiB === 128 ? 4 : 3 }]);
      }),
    ),
);

it.effect.each(["failure", "interruption"] as const)(
  "releases snapshot and transport holds before joining a blocked writer on %s",
  (outcome) =>
    Effect.gen(function* () {
      const result = yield* withClient((fixture) =>
        Effect.gen(function* () {
          yield* blockBehindReader(fixture);
          const writer = yield* Effect.forkChild(
            fixture.sql.withTransaction(fixture.sql`INSERT INTO entries VALUES (2)`),
            { startImmediately: true },
          );
          assert.isUndefined(writer.pollUnsafe(), "the writer must await checkpoint admission");
          return yield* outcome === "failure" ? Effect.fail("expected failure") : Effect.interrupt;
        }),
      ).pipe(Effect.exit);
      if (outcome === "failure") assert.deepEqual(result, Exit.fail("expected failure"));
      else {
        assert(Exit.isFailure(result));
        assert.isTrue(Cause.hasInterruptsOnly(result.cause));
      }
    }),
);
