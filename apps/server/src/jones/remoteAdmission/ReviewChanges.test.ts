import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import migrate from "../persistence/Migrations/156_JonesRemoteAdmissionReview.ts";
import * as Changes from "./ReviewChanges.ts";
import { makeReceiverAccess } from "./ReceiverAccess.ts";

const memory = Layer.effectDiscard(migrate).pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const layer = Changes.layer.pipe(Layer.provideMerge(memory));
const readRevision = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<{
    revision: number;
  }>`SELECT revision FROM jones_remote_admission_review_state WHERE singleton=1`)[0]!.revision;
});

it.effect(
  "subscribes before read so a commit while the first read returns stale cannot be missed",
  () =>
    Effect.gen(function* () {
      const changes = yield* Changes.ReviewChanges;
      const sql = yield* SqlClient.SqlClient;
      const readStarted = yield* Deferred.make<void>();
      const releaseRead = yield* Deferred.make<void>();
      let reads = 0;
      const read = Effect.gen(function* () {
        const revision = yield* readRevision;
        reads++;
        if (reads === 1) {
          yield* Deferred.succeed(readStarted, undefined);
          yield* Deferred.await(releaseRead);
        }
        return revision;
      });
      const waiter = yield* changes
        .waitFor({ read, isReady: (r) => r > 0, timeoutMs: 50000 })
        .pipe(Effect.forkChild);
      yield* Deferred.await(readStarted);
      const access = makeReceiverAccess(sql, changes.invalidate);
      yield* sql.withTransaction(
        access.bumpInTransaction("admission", "a", "2026-11-16T00:00:00.000Z"),
      );
      yield* Deferred.succeed(releaseRead, undefined);
      expect(yield* Fiber.join(waiter)).toBe(1);
      expect(reads).toBeGreaterThanOrEqual(2);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "rollback invalidation re-reads committed revision and waits for a later real commit",
  () =>
    Effect.gen(function* () {
      const changes = yield* Changes.ReviewChanges;
      const sql = yield* SqlClient.SqlClient;
      const firstRead = yield* Deferred.make<void>();
      const rollbackRead = yield* Deferred.make<void>();
      let reads = 0;
      const waiter = yield* changes
        .waitFor({
          read: Effect.gen(function* () {
            const revision = yield* readRevision;
            reads++;
            if (reads === 1) yield* Deferred.succeed(firstRead, undefined);
            if (reads === 2) {
              expect(revision).toBe(0);
              yield* Deferred.succeed(rollbackRead, undefined);
            }
            return revision;
          }),
          isReady: (r) => r > 0,
          timeoutMs: 50000,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(firstRead);
      const access = makeReceiverAccess(sql, changes.invalidate);
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* access.bumpInTransaction("admission", "rolled-back", "2026-11-16T00:00:00.000Z");
            return yield* Effect.fail("synthetic rollback");
          }),
        )
        .pipe(Effect.exit);
      yield* Deferred.await(rollbackRead);
      yield* sql.withTransaction(
        access.bumpInTransaction("admission", "committed", "2026-11-16T00:00:00.000Z"),
      );
      expect(yield* Fiber.join(waiter)).toBe(1);
    }).pipe(Effect.provide(layer)),
);

it.effect("coalesces unread invalidations to one token per subscription", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const changes = yield* Changes.ReviewChanges;
      const subscription = yield* changes.subscribe;
      // A separate observer confirms each batch has passed the commit barrier before
      // sending the next, while the first subscriber deliberately remains unread.
      const observer = yield* changes.subscribe;
      for (let i = 0; i < 3; i++) {
        yield* changes.invalidate;
        yield* observer.wait;
      }
      yield* subscription.wait;
      const empty = yield* subscription.wait.pipe(Effect.forkChild);
      yield* TestClock.adjust("1 millis");
      expect(empty.pollUnsafe()).toBeUndefined();
      yield* changes.invalidate;
      yield* Fiber.join(empty);
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("bounded timeout performs one final fresh read without periodic SQL scanning", () =>
  Effect.gen(function* () {
    const changes = yield* Changes.ReviewChanges;
    const began = yield* Deferred.make<void>();
    let reads = 0;
    const waiter = yield* changes
      .waitFor({
        read: Effect.sync(() => {
          reads++;
          return 0;
        }).pipe(Effect.tap(() => Deferred.succeed(began, undefined))),
        isReady: (r) => r > 0,
        timeoutMs: 999999,
      })
      .pipe(Effect.forkChild);
    yield* Deferred.await(began);
    yield* TestClock.adjust("50 seconds");
    expect(yield* Fiber.join(waiter)).toBe(0);
    expect(reads).toBe(2);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "publisher waits for the outer writer transaction to finish before waking subscribers",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const changes = yield* Changes.ReviewChanges;
        const sql = yield* SqlClient.SqlClient;
        const subscription = yield* changes.subscribe;
        const writerReady = yield* Deferred.make<void>();
        const releaseWriter = yield* Deferred.make<void>();
        const writer = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* sql.withTransaction(
                makeReceiverAccess(sql, changes.invalidate).bumpInTransaction(
                  "admission",
                  "nested",
                  "2026-11-16T00:00:00.000Z",
                ),
              );
              yield* Deferred.succeed(writerReady, undefined);
              yield* Deferred.await(releaseWriter);
            }),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(writerReady);
        const waiting = yield* subscription.wait.pipe(Effect.forkChild);
        yield* TestClock.adjust("1 millis");
        expect(waiting.pollUnsafe()).toBeUndefined();
        yield* Deferred.succeed(releaseWriter, undefined);
        yield* Fiber.join(writer);
        yield* Fiber.join(waiting);
        expect(yield* readRevision).toBe(1);
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect("every invalidation re-runs the caller's authorization read and propagates refusal", () =>
  Effect.gen(function* () {
    const changes = yield* Changes.ReviewChanges;
    const began = yield* Deferred.make<void>();
    let authorized = true;
    const waiter = yield* changes
      .waitFor({
        read: Effect.gen(function* () {
          if (!authorized) return yield* Effect.fail("session-revoked");
          yield* Deferred.succeed(began, undefined);
          return 0;
        }),
        isReady: (r) => r > 0,
        timeoutMs: 50000,
      })
      .pipe(Effect.forkChild);
    yield* Deferred.await(began);
    authorized = false;
    yield* changes.invalidate;
    expect(yield* Fiber.join(waiter).pipe(Effect.flip)).toBe("session-revoked");
  }).pipe(Effect.provide(layer)),
);
