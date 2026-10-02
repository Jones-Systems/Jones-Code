import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import migration from "../../persistence/Migrations/005_JonesWorkstreamsNativeAttempts.ts";
import { makeNativeProviderAttempts, type NativeProviderAttempt } from "./attemptRepository.ts";
import { binding, request, requestBytesSha256, now } from "./testFixtures.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const attempt: NativeProviderAttempt = {
  request,
  requestBytesSha256,
  enrollmentSha256: "e".repeat(64),
  enrollment: binding,
  nativeCommandId: "workstreams:synthetic-command",
  createdAt: now,
  dispatchStartedAt: null,
};

it.effect(
  "native attempts preserve complete immutable association and allow dispatch-start once",
  () =>
    Effect.gen(function* () {
      yield* migration;
      yield* migration;
      const repo = yield* makeNativeProviderAttempts;
      assert.deepEqual(yield* repo.reserve(attempt), attempt);
      assert.deepEqual(
        yield* repo.reserve({ ...attempt, requestBytesSha256: "f".repeat(64) }),
        attempt,
      );
      assert.strictEqual(yield* repo.startDispatch(request, now), true);
      assert.strictEqual(yield* repo.startDispatch(request, now), false);
      const read = yield* repo.get(request);
      assert.strictEqual(Option.isSome(read), true);
      if (Option.isSome(read)) assert.deepEqual(read.value, { ...attempt, dispatchStartedAt: now });
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "native migration rejects changing, deleting or replacing immutable request ownership",
  () =>
    Effect.gen(function* () {
      yield* migration;
      const repo = yield* makeNativeProviderAttempts;
      const sql = yield* SqlClient.SqlClient;
      yield* repo.reserve(attempt);
      for (const mutation of [
        sql`UPDATE workstreams_native_attempts SET request_json = '{}'`,
        sql`UPDATE workstreams_native_attempts SET request_bytes_sha256 = ${"f".repeat(64)}`,
        sql`UPDATE workstreams_native_attempts SET enrollment_json = '{}'`,
        sql`UPDATE workstreams_native_attempts SET native_command_id = 'other-command'`,
        sql`UPDATE workstreams_native_attempts SET owner_id = 'other-owner'`,
        sql`UPDATE workstreams_native_attempts SET principal_id = 'other-principal'`,
        sql`DELETE FROM workstreams_native_attempts`,
        sql`INSERT OR REPLACE INTO workstreams_native_attempts SELECT * FROM workstreams_native_attempts`,
      ])
        assert.strictEqual((yield* mutation.pipe(Effect.result))._tag, "Failure");
      assert.deepEqual(Option.getOrNull(yield* repo.get(request)), attempt);
      yield* repo.startDispatch(request, now);
      assert.strictEqual(
        (yield* sql`UPDATE workstreams_native_attempts SET dispatch_started_at = NULL`.pipe(
          Effect.result,
        ))._tag,
        "Failure",
      );
      assert.strictEqual(
        (yield* sql`UPDATE workstreams_native_attempts SET dispatch_started_at = 'later'`.pipe(
          Effect.result,
        ))._tag,
        "Failure",
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "dispatch start and association survive repository reconstruction after a lost reply",
  () =>
    Effect.gen(function* () {
      yield* migration;
      const first = yield* makeNativeProviderAttempts;
      yield* first.reserve(attempt);
      yield* first.startDispatch(request, now);
      const reopened = yield* makeNativeProviderAttempts;
      assert.deepEqual(Option.getOrNull(yield* reopened.get(request)), {
        ...attempt,
        dispatchStartedAt: now,
      });
      assert.strictEqual(yield* reopened.startDispatch(request, now), false);
      assert.strictEqual(
        Option.isNone(yield* reopened.get({ ...request, principal_id: "other-principal" })),
        true,
      );
    }).pipe(Effect.provide(memory)),
);
