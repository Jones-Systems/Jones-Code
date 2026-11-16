import type * as NodeWorkerThreads from "node:worker_threads";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scheduler from "effect/Scheduler";
import type * as Scope from "effect/Scope";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as SqlConnection from "effect/sql/SqlConnection";
import { afterEach, vi } from "vite-plus/test";
import {
  admissionScheduler,
  blockBehindReader,
  checkpointReply,
  eventLoopTurn,
  holdMessage,
  isMessage,
  pinSmallWal,
  readerProgress,
  receipt,
  recover,
  verifyFreshWriter,
  withCheckpointClient,
  type Fixture,
} from "./sqliteWriterAdmission.fixture.ts";

const workers = vi.hoisted(
  (): Array<{
    worker: NodeWorkerThreads.Worker;
    filename: string;
    checkpoint: boolean;
  }> => [],
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
    assert.equal(worker.threadId, -1, "the creating client must await every owned worker's exit");
  }
  workers.length = 0;
});

const withClient = <A, E>(
  use: (fixture: Fixture) => Effect.Effect<A, E, Scope.Scope | SqlClient.SqlClient>,
) =>
  withCheckpointClient(
    (filename) =>
      workers.find((record) => record.checkpoint && record.filename === filename)?.worker,
    use,
  );

const assertInterrupted = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(fiber)));
  });

const queuedAtAdmission = (scheduled: ReturnType<typeof admissionScheduler>) =>
  Effect.gen(function* () {
    yield* scheduled.registered;
    assert.equal(scheduled.registrations, 1);
    assert.equal(scheduled.dispatches, 0, "blocked admission has registered but elected nobody");
  });

it.effect.each([
  { reserved: false, stage: "queued" },
  { reserved: true, stage: "queued" },
  { reserved: false, stage: "elected" },
  { reserved: true, stage: "elected" },
  { reserved: false, stage: "resumed" },
  { reserved: true, stage: "resumed" },
] as const)(
  "hands checkpoint admission to its successor (reserved=$reserved, stage=$stage)",
  ({ reserved, stage }) =>
    withClient((fixture) =>
      Effect.gen(function* () {
        const { sql, traffic } = fixture;
        const { releasePin } = yield* blockBehindReader(fixture);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const connection = reserved ? yield* sql.reserve : undefined;
            const insert = (value: number) =>
              connection
                ? connection.executeUnprepared("INSERT INTO entries VALUES (?)", [value], undefined)
                : sql`INSERT INTO entries VALUES (${value})`;
            const firstSchedule = admissionScheduler(stage === "resumed");
            const secondSchedule = admissionScheduler();
            fixture.own(firstSchedule.release);
            fixture.own(secondSchedule.release);
            const first = yield* Effect.forkScoped(
              insert(1).pipe(Effect.provideService(Scheduler.Scheduler, firstSchedule.scheduler)),
              { startImmediately: true },
            );
            yield* queuedAtAdmission(firstSchedule);
            const secondStarted = receipt();
            const second = yield* Effect.forkScoped(
              Effect.gen(function* () {
                secondStarted.complete();
                return yield* insert(2);
              }).pipe(Effect.provideService(Scheduler.Scheduler, secondSchedule.scheduler)),
              { startImmediately: true },
            );
            yield* secondStarted.wait;
            if (reserved) {
              yield* queuedAtAdmission(secondSchedule);
            } else {
              // Immediate start, no forced yield, and the first borrow owns the only
              // writer permit: this successor cannot yet reach the admission callback.
              assert.equal(secondSchedule.registrations, 0);
            }
            assert.isUndefined(second.pollUnsafe());
            yield* readerProgress(sql);
            if (stage !== "queued") {
              firstSchedule.arm();
              yield* recover(fixture, releasePin);
              yield* firstSchedule.paused;
              assert.equal(firstSchedule.dispatches, 1, "the elected admission callback is held");
              if (stage === "resumed") {
                firstSchedule.resume();
                yield* firstSchedule.continuation;
                assert.equal(firstSchedule.dispatches, 2, "the resumed fiber yielded before SQL");
              }
            }
            yield* Fiber.interrupt(first);
            yield* assertInterrupted(first);
            firstSchedule.release();
            if (stage === "queued") {
              yield* queuedAtAdmission(secondSchedule);
              yield* recover(fixture, releasePin);
            }
            const sentAfterTruncate = traffic.sent.length;
            yield* Fiber.join(second);
            assert.equal(
              traffic.sent.length,
              sentAfterTruncate,
              "handoff must finish without forwarding another checkpoint request",
            );
            if (connection) {
              assert.deepEqual(
                yield* connection.executeValues("SELECT value FROM entries ORDER BY value", []),
                [[2]],
              );
            }
            yield* eventLoopTurn;
            firstSchedule.assertDrained();
            secondSchedule.assertDrained();
          }).pipe(Effect.ensuring(Effect.sync(fixture.release))),
        );
        yield* verifyFreshWriter(sql);
      }),
    ),
);

it.effect("recovers expected backpressure after a real pin and truncate acknowledgement", () =>
  withClient((fixture) =>
    Effect.gen(function* () {
      const { sql, traffic } = fixture;
      const { releasePin } = yield* blockBehindReader(fixture);
      const scheduled = admissionScheduler();
      fixture.own(scheduled.release);
      const mutation = yield* Effect.forkScoped(
        sql`INSERT INTO entries VALUES (2)`.pipe(
          Effect.provideService(Scheduler.Scheduler, scheduled.scheduler),
        ),
        { startImmediately: true },
      );
      yield* queuedAtAdmission(scheduled);
      const lookupStarted = receipt();
      const lookup = yield* Effect.forkScoped(
        Effect.gen(function* () {
          lookupStarted.complete();
          return yield* sql`SELECT value FROM entries`;
        }),
        { startImmediately: true },
      );
      yield* lookupStarted.wait;
      yield* readerProgress(sql);
      assert.isUndefined(mutation.pollUnsafe());
      assert.isUndefined(
        lookup.pollUnsafe(),
        "an ordinary SELECT waits for the held writer permit",
      );
      yield* recover(fixture, releasePin);
      const sentAfterTruncate = traffic.sent.length;
      yield* Fiber.join(mutation);
      assert.deepEqual(yield* Fiber.join(lookup), [{ value: 2 }]);
      assert.equal(traffic.sent.length, sentAfterTruncate);
      traffic.release();
      yield* verifyFreshWriter(sql);
    }),
  ),
);

it.effect("cancels the actual reservation owner while admission is pinned", () =>
  withClient((fixture) =>
    Effect.gen(function* () {
      const { sql } = fixture;
      const { releasePin } = yield* blockBehindReader(fixture);
      const scheduled = admissionScheduler();
      fixture.own(scheduled.release);
      const closed = receipt();
      const owner = yield* Effect.forkScoped(
        Effect.scoped(
          Effect.gen(function* () {
            const connection = yield* sql.reserve;
            yield* Effect.addFinalizer(() => Effect.sync(() => closed.complete()));
            yield* connection.executeUnprepared("INSERT INTO entries VALUES (1)", [], undefined);
          }),
        ).pipe(Effect.provideService(Scheduler.Scheduler, scheduled.scheduler)),
        { startImmediately: true },
      );
      yield* queuedAtAdmission(scheduled);
      yield* readerProgress(sql);
      yield* Fiber.interrupt(owner);
      yield* closed.wait;
      yield* assertInterrupted(owner);
      assert.deepEqual(
        yield* sql`SELECT value FROM entries`,
        [],
        "the completed owner scope returned the permit while the real reader is still pinned",
      );
      yield* recover(fixture, releasePin);
      fixture.traffic.release();
      yield* sql`INSERT INTO entries VALUES (2)`;
      yield* verifyFreshWriter(sql);
    }),
  ),
);

it.effect("cancels a withTransaction caller at BEGIN without entering its body", () =>
  withClient((fixture) =>
    Effect.gen(function* () {
      const { sql } = fixture;
      const { releasePin } = yield* blockBehindReader(fixture);
      const scheduled = admissionScheduler();
      fixture.own(scheduled.release);
      let bodyEntries = 0;
      const completed = receipt();
      const transaction = yield* Effect.forkScoped(
        sql
          .withTransaction(
            Effect.gen(function* () {
              bodyEntries++;
              yield* sql`INSERT INTO entries VALUES (1)`;
            }),
          )
          .pipe(
            Effect.provideService(Scheduler.Scheduler, scheduled.scheduler),
            Effect.ensuring(Effect.sync(() => completed.complete())),
          ),
        { startImmediately: true },
      );
      yield* queuedAtAdmission(scheduled);
      yield* readerProgress(sql);
      yield* Fiber.interrupt(transaction);
      yield* completed.wait;
      yield* assertInterrupted(transaction);
      assert.equal(bodyEntries, 0);
      assert.deepEqual(yield* sql`SELECT value FROM entries`, []);
      yield* recover(fixture, releasePin);
      fixture.traffic.release();
      yield* sql`INSERT INTO entries VALUES (2)`;
      yield* verifyFreshWriter(sql);
    }),
  ),
);

it.effect(
  "a live reservation owner retains its permit after its admission child is cancelled",
  () =>
    withClient((fixture) =>
      Effect.gen(function* () {
        const { sql } = fixture;
        const { releasePin } = yield* blockBehindReader(fixture);
        const acquired = receipt<SqlConnection.Connection>();
        const releaseOwner = receipt();
        fixture.own(() => releaseOwner.complete());
        const owner = yield* Effect.forkScoped(
          Effect.scoped(
            Effect.gen(function* () {
              const connection = yield* sql.reserve;
              acquired.complete(connection);
              yield* releaseOwner.wait;
            }),
          ),
          { startImmediately: true },
        );
        const connection = yield* acquired.wait;
        const scheduled = admissionScheduler();
        fixture.own(scheduled.release);
        const child = yield* Effect.forkScoped(
          connection
            .executeUnprepared("INSERT INTO entries VALUES (1)", [], undefined)
            .pipe(Effect.provideService(Scheduler.Scheduler, scheduled.scheduler)),
          { startImmediately: true },
        );
        yield* queuedAtAdmission(scheduled);
        yield* Fiber.interrupt(child);
        yield* assertInterrupted(child);
        const lookupStarted = receipt();
        const lookup = yield* Effect.forkScoped(
          Effect.gen(function* () {
            lookupStarted.complete();
            return yield* sql`SELECT value FROM entries`;
          }),
          { startImmediately: true },
        );
        yield* lookupStarted.wait;
        yield* readerProgress(sql);
        yield* recover(fixture, releasePin);
        assert.isUndefined(owner.pollUnsafe());
        assert.isUndefined(
          lookup.pollUnsafe(),
          "restored checkpoint progress cannot release a live owner",
        );
        assert.deepEqual(yield* connection.executeValues("SELECT value FROM entries", []), []);
        releaseOwner.complete();
        yield* Fiber.join(owner);
        assert.deepEqual(yield* Fiber.join(lookup), []);
        fixture.traffic.release();
        yield* sql`INSERT INTO entries VALUES (2)`;
        yield* verifyFreshWriter(sql);
      }),
    ),
);

it.effect.each([false, true])(
  "finishes masked cleanup only after checkpoint progress (transaction=%s)",
  (transaction) =>
    withClient((fixture) =>
      Effect.gen(function* () {
        const { sql } = fixture;
        const { releasePin } = yield* blockBehindReader(fixture);
        const entered = receipt();
        const cleanupEntered = receipt();
        const committed = receipt();
        const scheduled = admissionScheduler();
        fixture.own(scheduled.release);
        const work = yield* Effect.forkScoped(
          Effect.gen(function* () {
            entered.complete();
            return yield* Effect.never;
          }).pipe(
            Effect.onInterrupt(() =>
              Effect.gen(function* () {
                cleanupEntered.complete();
                const insert = sql`INSERT INTO entries VALUES (1)`;
                yield* transaction ? sql.withTransaction(insert) : insert;
                committed.complete();
              }),
            ),
            Effect.provideService(Scheduler.Scheduler, scheduled.scheduler),
          ),
          { startImmediately: true },
        );
        yield* entered.wait;
        const interrupt = yield* Effect.forkScoped(Fiber.interrupt(work), {
          startImmediately: true,
        });
        yield* cleanupEntered.wait;
        yield* queuedAtAdmission(scheduled);
        yield* readerProgress(sql);
        assert.isUndefined(work.pollUnsafe());
        assert.isUndefined(
          interrupt.pollUnsafe(),
          "masked cleanup must retain the writer until admitted",
        );
        yield* recover(fixture, releasePin);
        yield* committed.wait;
        yield* Fiber.join(interrupt);
        yield* assertInterrupted(work);
        fixture.traffic.release();
        yield* sql.withTransaction(sql`INSERT INTO entries VALUES (2)`);
        assert.deepEqual(yield* sql`SELECT value FROM entries ORDER BY value`.values, [[1], [2]]);
      }),
    ),
);

it.effect("a held real worker response recovers by delivery while the worker stays alive", () =>
  withClient((fixture) =>
    Effect.gen(function* () {
      const { sql, worker, journal } = fixture;
      const { releasePin } = yield* blockBehindReader(fixture);
      const scheduled = admissionScheduler();
      fixture.own(scheduled.release);
      const writer = yield* Effect.forkScoped(
        sql`INSERT INTO entries VALUES (2)`.pipe(
          Effect.provideService(Scheduler.Scheduler, scheduled.scheduler),
        ),
        { startImmediately: true },
      );
      yield* queuedAtAdmission(scheduled);
      const response = holdMessage(
        worker,
        (message) =>
          message.type === "checkpoint" &&
          typeof message.truncateId === "number" &&
          message.blocked === false,
      );
      fixture.own(response.release);
      const recovery = yield* Effect.forkScoped(recover(fixture, releasePin), {
        startImmediately: true,
      });
      const actual = yield* response.ready;
      assert.equal(actual.blocked, false);
      assert.isAbove(worker.threadId, 0);
      assert.isUndefined(journal.exitCode);
      yield* readerProgress(sql);
      assert.equal(scheduled.dispatches, 0);
      assert.isUndefined(
        writer.pollUnsafe(),
        "SQL has completed on the worker; delivery remains held",
      );
      response.release();
      const delivered = yield* Fiber.join(recovery);
      assert.equal(delivered, actual);
      yield* Fiber.join(writer);
      assert.isUndefined(journal.exitCode);
      fixture.traffic.release();
      yield* verifyFreshWriter(sql);
    }),
  ),
);

it.effect("actual checkpoint-worker exit hands admission and autocheckpointing back", () =>
  withClient((fixture) =>
    Effect.gen(function* () {
      const { sql, worker, journal } = fixture;
      const { releasePin } = yield* blockBehindReader(fixture);
      const scheduled = admissionScheduler();
      fixture.own(scheduled.release);
      const writer = yield* Effect.forkScoped(
        sql`INSERT INTO entries VALUES (2)`.pipe(
          Effect.provideService(Scheduler.Scheduler, scheduled.scheduler),
        ),
        { startImmediately: true },
      );
      yield* queuedAtAdmission(scheduled);
      yield* readerProgress(sql);
      const code = yield* Effect.promise(() => worker.terminate());
      assert.equal(yield* journal.exited, code);
      assert.equal(worker.threadId, -1);
      yield* Fiber.join(writer);
      assert.deepEqual(yield* sql`PRAGMA wal_autocheckpoint`, [{ wal_autocheckpoint: 1000 }]);
      assert.deepEqual(yield* sql`SELECT value FROM entries`, [{ value: 2 }]);
      releasePin();
      fixture.traffic.release();
      yield* verifyFreshWriter(sql);
    }),
  ),
);

it.effect("retires real stale requests overlapping a pending truncate acknowledgement", () =>
  withClient((fixture) =>
    Effect.gen(function* () {
      const { sql, worker, traffic, journal } = fixture;
      const { releasePin } = yield* pinSmallWal(fixture);
      const initial = traffic.next();
      // A failed rollback still checks the WAL and advances the generation.
      // A new mutation would correctly wait behind the held checkpoint traffic.
      assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.unsafe("ROLLBACK").unprepared)));
      assert.equal(traffic.sendNext(), initial);
      const initialReply = yield* checkpointReply(fixture, initial);
      assert.equal(initialReply.blocked, true);
      assert.equal(initialReply.generation, initial.generation);
      const overlapping = traffic.next();
      assert.equal(overlapping.type, "check");
      assert.isAbove(Number(overlapping.id), Number(initial.id));
      assert.isAbove(Number(overlapping.generation), Number(initial.generation));
      assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.unsafe("ROLLBACK").unprepared)));
      const truncate = holdMessage(worker, (message) => message.type === "truncate-request");
      fixture.own(truncate.release);
      releasePin();
      traffic.barrier();
      const pending = yield* truncate.ready;
      assert.equal(pending.generation, initial.generation);
      assert.equal(pending.log, pending.checkpointed);
      assert.isAbove(Number(overlapping.generation), Number(pending.generation));
      assert.equal(traffic.sendNext(), overlapping);
      const stale = yield* checkpointReply(fixture, overlapping);
      assert.equal(stale.generation, pending.generation);
      assert.equal(stale.blocked, true);
      // Only retiring the matching inFlight request can generate this coalesced
      // follow-up. Every ID and generation below came from the actual parent.
      const followup = traffic.next();
      assert.equal(followup.type, "check");
      assert.isAbove(Number(followup.id), Number(overlapping.id));
      assert.isAbove(Number(followup.generation), Number(overlapping.generation));
      truncate.release();
      const scheduled = admissionScheduler();
      fixture.own(scheduled.release);
      const writer = yield* Effect.forkScoped(
        sql`INSERT INTO entries VALUES (2)`.pipe(
          Effect.provideService(Scheduler.Scheduler, scheduled.scheduler),
        ),
        { startImmediately: true },
      );
      yield* queuedAtAdmission(scheduled);
      assert.equal(traffic.sendNext(), followup);
      const staleFollowup = yield* checkpointReply(fixture, followup);
      assert.equal(staleFollowup.generation, pending.generation);
      assert.equal(staleFollowup.blocked, true);
      assert.equal(
        scheduled.dispatches,
        0,
        "a stale reply must leave pending truncate admission shut",
      );
      const acknowledgement = traffic.sendNext();
      assert.deepEqual(acknowledgement, { type: "truncate", truncateId: pending.truncateId });
      const completed = yield* journal.wait(
        (message) =>
          isMessage(message) &&
          message.type === "checkpoint" &&
          message.truncateId === pending.truncateId,
      );
      assert(isMessage(completed));
      assert.equal(completed.id, pending.checkId);
      assert.equal(completed.generation, pending.generation);
      assert.equal(completed.blocked, false);
      const sentAfterTruncate = traffic.sent.length;
      yield* Fiber.join(writer);
      assert.equal(traffic.sent.length, sentAfterTruncate);
      traffic.release();
      yield* verifyFreshWriter(sql);
    }),
  ),
);
