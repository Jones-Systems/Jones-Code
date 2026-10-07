import { assert, it } from "@effect/vitest";
import { AuthSessionId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import migration from "../../persistence/Migrations/006_JonesWorkstreamsProviderEnrollments.ts";
import { NativeStoreAuthorityPersistenceError } from "../../environment/nativeStoreAuthorityPersistence.ts";
import { deriveNativeEnrollmentSession } from "./request.ts";
import { makeNativeEnrollments, type NativeEnrollmentPorts } from "./service.ts";
import { enrollmentRequest as request } from "./testFixtures.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const ports: NativeEnrollmentPorts = {
  authority: {
    readCurrent: Effect.succeed({
      environmentId: request.context.source_instance_id,
      authorityNamespace: request.context.authority_namespace,
      storeGeneration: request.context.store_generation,
    }),
  },
  build: Effect.succeed(Option.some(request.context.build)),
};
const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(request.session.issued_at));
  yield* runMigrations({ toMigrationInclusive: 54 });
  yield* migration;
  const sessions = yield* AuthSessions.make;
  const make = (override: NativeEnrollmentPorts = ports) =>
    makeNativeEnrollments(override).pipe(
      Effect.provideService(AuthSessions.AuthSessionRepository, sessions),
    );
  return { sessions, make, enrollments: yield* make(), sql: yield* SqlClient.SqlClient };
});

it.effect(
  "native reservation survives a lost response and reconstructed service with identical records",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      assert.deepEqual(yield* f.enrollments.inspect(request), { state: "absent" });
      assert.deepEqual(yield* f.enrollments.reserve(request), { state: "reserved" });
      const reopened = yield* f.make();
      assert.deepEqual(yield* reopened.reserve(request), { state: "reserved" });
      const binding = yield* reopened.getBySessionId(request.session.session_id);
      assert.strictEqual(Option.isSome(binding), true);
      if (Option.isSome(binding)) assert.deepEqual(binding.value.build, request.context.build);
      assert.strictEqual((yield* f.sql`SELECT * FROM auth_sessions`).length, 1);
      assert.strictEqual((yield* f.sql`SELECT * FROM workstreams_native_enrollments`).length, 1);
    }).pipe(Effect.provide(memory)),
);

it.effect("same enrollment ID with different bytes rolls back its newly reserved session", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.enrollments.reserve(request);
    const conflict = {
      ...request,
      registry_origin: "https://different.invalid",
      session: { ...request.session, session_id: AuthSessionId.make("different-reserved-session") },
    };
    assert.strictEqual(
      (yield* Effect.flip(f.enrollments.reserve(conflict))).code,
      "enrollment_conflict",
    );
    assert.strictEqual((yield* f.sql`SELECT * FROM auth_sessions`).length, 1);
    assert.strictEqual((yield* f.sql`SELECT * FROM workstreams_native_enrollments`).length, 1);
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "a conflicting reserved auth record is never widened or accepted after ignored insert",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const expected = deriveNativeEnrollmentSession(request);
      yield* f.sessions.create({ ...expected, subject: "another-client" });
      assert.strictEqual(
        (yield* Effect.flip(f.enrollments.reserve(request))).code,
        "session_conflict",
      );
      assert.strictEqual((yield* f.sql`SELECT * FROM workstreams_native_enrollments`).length, 0);
      assert.strictEqual(
        Option.getOrNull(yield* f.sessions.getById({ sessionId: expected.sessionId }))?.subject,
        "another-client",
      );
    }).pipe(Effect.provide(memory)),
);

it.effect("revoked, expired and wrong-scope session records never produce enrollment lookup", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.enrollments.reserve(request);
    yield* f.sql`UPDATE auth_sessions SET scopes = '["orchestration:read"]' WHERE session_id = ${request.session.session_id}`;
    assert.strictEqual(
      Option.isNone(yield* f.enrollments.getBySessionId(request.session.session_id)),
      true,
    );
    yield* f.sql`UPDATE auth_sessions SET scopes = ${encodeJson(deriveNativeEnrollmentSession(request).scopes)} WHERE session_id = ${request.session.session_id}`;
    yield* TestClock.setTime(Date.parse(request.session.expires_at));
    assert.strictEqual(
      Option.isNone(yield* f.enrollments.getBySessionId(request.session.session_id)),
      true,
    );
    yield* TestClock.setTime(Date.parse(request.session.issued_at));
    yield* f.sessions.revoke({
      sessionId: request.session.session_id,
      revokedAt: DateTime.makeUnsafe(request.session.issued_at),
    });
    assert.strictEqual(
      Option.isNone(yield* f.enrollments.getBySessionId(request.session.session_id)),
      true,
    );
    assert.strictEqual(
      (yield* Effect.flip(f.enrollments.reserve(request))).code,
      "session_conflict",
    );
  }).pipe(Effect.provide(memory)),
);

it.effect("authority absence and wrong source, generation or build fail before reservation", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const absent = yield* f.make({
      ...ports,
      authority: {
        readCurrent: Effect.fail(
          new NativeStoreAuthorityPersistenceError(
            "source_unavailable",
            "Synthetic authority missing.",
          ),
        ),
      },
    });
    assert.strictEqual((yield* Effect.flip(absent.reserve(request))).code, "authority_unavailable");
    const wrongSha = request.context.build.sha === "a".repeat(40) ? "f".repeat(40) : "a".repeat(40);
    assert.notStrictEqual(wrongSha, request.context.build.sha);
    const wrongBuild = yield* f.make({
      ...ports,
      build: Effect.succeed(Option.some({ ...request.context.build, sha: wrongSha })),
    });
    assert.strictEqual((yield* Effect.flip(wrongBuild.reserve(request))).code, "build_mismatch");
    for (const context of [
      { ...request.context, source_instance_id: "other-source" },
      { ...request.context, store_generation: request.context.store_generation + 1 },
      { ...request.context, authority_namespace: "other-namespace" },
    ])
      assert.strictEqual(
        (yield* Effect.flip(f.enrollments.reserve({ ...request, context }))).code,
        "authority_mismatch",
      );
    assert.strictEqual((yield* f.sql`SELECT * FROM auth_sessions`).length, 0);
  }).pipe(Effect.provide(memory)),
);

it.effect("authority changes during reservation roll back both records", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    let reads = 0;
    const changing = yield* f.make({
      ...ports,
      authority: {
        readCurrent: Effect.sync(() => ({
          environmentId: request.context.source_instance_id,
          authorityNamespace: request.context.authority_namespace,
          storeGeneration:
            ++reads >= 3 ? request.context.store_generation + 1 : request.context.store_generation,
        })),
      },
    });
    assert.strictEqual((yield* Effect.flip(changing.reserve(request))).code, "authority_mismatch");
    assert.strictEqual((yield* f.sql`SELECT * FROM auth_sessions`).length, 0);
    assert.strictEqual((yield* f.sql`SELECT * FROM workstreams_native_enrollments`).length, 0);
  }).pipe(Effect.provide(memory)),
);

it.effect("immutable enrollment rows reject updates, deletion and replacement", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.enrollments.reserve(request);
    for (const mutation of [
      f.sql`UPDATE workstreams_native_enrollments SET request_sha256 = ${"f".repeat(64)}`,
      f.sql`DELETE FROM workstreams_native_enrollments`,
      f.sql`INSERT OR REPLACE INTO workstreams_native_enrollments SELECT * FROM workstreams_native_enrollments`,
    ])
      assert.strictEqual((yield* mutation.pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* f.enrollments.inspect(request), { state: "reserved" });
  }).pipe(Effect.provide(memory)),
);

it.effect("a missing enrollment table fails closed without creating it", () =>
  Effect.gen(function* () {
    yield* runMigrations({ toMigrationInclusive: 54 });
    const sessions = yield* AuthSessions.make;
    const enrollments = yield* makeNativeEnrollments(ports).pipe(
      Effect.provideService(AuthSessions.AuthSessionRepository, sessions),
    );
    assert.strictEqual(
      Option.isNone(yield* enrollments.getBySessionId(request.session.session_id)),
      true,
    );
    const sql = yield* SqlClient.SqlClient;
    assert.strictEqual(
      (yield* sql`SELECT name FROM sqlite_master WHERE name = 'workstreams_native_enrollments'`)
        .length,
      0,
    );
  }).pipe(Effect.provide(memory)),
);
