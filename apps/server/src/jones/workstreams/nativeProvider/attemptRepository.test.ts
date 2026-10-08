import { assert, it } from "@effect/vitest";
import { WorkstreamsNativeSettlementRequest } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import conformance from "../../../../../../packages/contracts/contracts/workstreams-t3-provider/v1/fixtures/conformance.json" with { type: "json" };
import migration from "../../persistence/Migrations/005_JonesWorkstreamsNativeAttempts.ts";
import * as Attempts from "./attemptRepository.ts";
import { NativeProviderEnrollmentBinding } from "./enrollment.ts";

const request = Schema.decodeUnknownSync(WorkstreamsNativeSettlementRequest)(
  conformance.cases.find((entry) => entry.name === "SettlementRequest")?.value,
);
const context = conformance.cases.find((entry) => entry.name === "Context")?.value;
const now = "2026-01-01T00:00:00.000Z";
const encodeRequest = Schema.encodeEffect(
  Schema.fromJsonString(WorkstreamsNativeSettlementRequest),
);
const encodeEnrollment = Schema.encodeEffect(
  Schema.fromJsonString(NativeProviderEnrollmentBinding),
);
const attempt: Attempts.NativeProviderAttempt = {
  request,
  requestBytesSha256: "a".repeat(64),
  enrollmentSha256: "e".repeat(64),
  enrollment: Schema.decodeUnknownSync(NativeProviderEnrollmentBinding)({
    ...context,
    session_id: "session-synthetic",
    registry_origin: "https://registry.invalid",
    scopes: [
      "workstreams:native:context",
      "workstreams:native:settlement",
      "workstreams:native:reconciliation",
    ],
  }),
  nativeCommandId: "workstreams:synthetic-command",
  createdAt: now,
  dispatchStartedAt: null,
};
const memory = Attempts.NativeProviderAttemptsLive.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);

it.effect(
  "native attempts preserve complete immutable association and allow dispatch-start once",
  () =>
    Effect.gen(function* () {
      yield* migration;
      yield* migration;
      const repo = yield* Attempts.NativeProviderAttempts;
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
      const repo = yield* Attempts.NativeProviderAttempts;
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
      const first = yield* Attempts.NativeProviderAttempts;
      yield* first.reserve(attempt);
      yield* first.startDispatch(request, now);
      const sql = yield* SqlClient.SqlClient;
      const reopened = yield* Attempts.NativeProviderAttempts.pipe(
        Effect.provide(Attempts.NativeProviderAttemptsLive),
      );
      assert.deepEqual(Option.getOrNull(yield* reopened.get(request)), {
        ...attempt,
        dispatchStartedAt: now,
      });
      assert.strictEqual(yield* reopened.startDispatch(request, now), false);
      assert.strictEqual(
        Option.isNone(yield* reopened.get({ ...request, principal_id: "other-principal" })),
        true,
      );
      assert.strictEqual((yield* sql`SELECT * FROM workstreams_native_attempts`).length, 1);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "reservation keys isolate owners, principals and commands; native command IDs remain unique",
  () =>
    Effect.gen(function* () {
      yield* migration;
      const repo = yield* Attempts.NativeProviderAttempts;
      yield* repo.reserve(attempt);
      for (const field of ["owner_id", "principal_id", "command_id"] as const) {
        const other = {
          ...attempt,
          request: { ...request, [field]: "other-synthetic-command" },
          nativeCommandId: `native-${field}`,
        };
        assert.strictEqual(Option.isNone(yield* repo.get(other.request)), true);
        assert.strictEqual(yield* repo.startDispatch(other.request, now), false);
        assert.deepEqual(yield* repo.reserve(other), other);
        assert.strictEqual(yield* repo.startDispatch(other.request, now), true);
        assert.strictEqual(yield* repo.startDispatch(other.request, "later"), false);
      }
      const conflict = yield* repo
        .reserve({ ...attempt, request: { ...request, command_id: "duplicate-native-id" } })
        .pipe(Effect.result);
      assert.strictEqual(conflict._tag, "Failure");
      if (conflict._tag === "Failure") assert.strictEqual(conflict.failure.operation, "reserve");
      assert.deepEqual(Option.getOrNull(yield* repo.get(request)), attempt);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "reads legacy 005 rows with typed enrollment and rejects malformed persisted bindings",
  () =>
    Effect.gen(function* () {
      yield* migration;
      const sql = yield* SqlClient.SqlClient;
      const requestJson = yield* encodeRequest(request);
      const enrollmentJson = yield* encodeEnrollment(attempt.enrollment);
      yield* sql`INSERT INTO workstreams_native_attempts
      (owner_id, principal_id, command_id, request_json, request_bytes_sha256, enrollment_sha256,
        enrollment_json, native_command_id, created_at, dispatch_started_at)
      VALUES (${request.owner_id}, ${request.principal_id}, ${request.command_id}, ${requestJson},
        ${attempt.requestBytesSha256}, ${attempt.enrollmentSha256}, ${enrollmentJson},
        ${attempt.nativeCommandId}, ${attempt.createdAt}, ${now})`;
      const repo = yield* Attempts.NativeProviderAttempts;
      assert.deepEqual(Option.getOrNull(yield* repo.get(request)), {
        ...attempt,
        dispatchStartedAt: now,
      });
      assert.deepEqual(yield* repo.reserve(attempt), { ...attempt, dispatchStartedAt: now });
      assert.strictEqual(yield* repo.startDispatch(request, "later"), false);
      const invalidBindings = [
        { future_binding_field: [1, true, null] },
        { ...attempt.enrollment, store_generation: 0 },
        { ...attempt.enrollment, session_id: "" },
        { ...attempt.enrollment, registry_origin: "https://registry.invalid/private-path" },
        { ...attempt.enrollment, scopes: ["unsupported-scope"] },
        { ...attempt.enrollment, protocol: "unsupported-protocol" },
        { ...attempt.enrollment, build: {} },
      ];
      for (const [index, binding] of invalidBindings.entries()) {
        const commandId = `invalid-binding-command-${index}`;
        const invalidRequest = { ...request, command_id: commandId };
        const invalidRequestJson = yield* encodeRequest(invalidRequest);
        const invalidBindingJson = yield* Schema.encodeEffect(
          Schema.fromJsonString(Schema.Unknown),
        )(binding);
        yield* sql`INSERT INTO workstreams_native_attempts
        (owner_id, principal_id, command_id, request_json, request_bytes_sha256, enrollment_sha256,
          enrollment_json, native_command_id, created_at, dispatch_started_at)
        VALUES (${request.owner_id}, ${request.principal_id}, ${commandId}, ${invalidRequestJson},
          ${attempt.requestBytesSha256}, ${attempt.enrollmentSha256}, ${invalidBindingJson},
          ${commandId}, ${attempt.createdAt}, NULL)`;
        const decoded = yield* repo.get(invalidRequest).pipe(Effect.result);
        assert.strictEqual(decoded._tag, "Failure");
        if (decoded._tag === "Failure") {
          assert.strictEqual(decoded.failure.operation, "get");
          assert.strictEqual(
            decoded.failure.message,
            "Native provider attempt persistence failed.",
          );
          assert.strictEqual(Object.hasOwn(decoded.failure, "cause"), false);
        }
      }
      assert.deepEqual(Option.getOrNull(yield* repo.get(request)), {
        ...attempt,
        dispatchStartedAt: now,
      });
    }).pipe(Effect.provide(memory)),
);

it.effect("concurrent reservations converge and only one dispatch CAS wins", () =>
  Effect.gen(function* () {
    yield* migration;
    const repo = yield* Attempts.NativeProviderAttempts;
    const reserved = yield* Effect.all(
      [repo.reserve(attempt), repo.reserve({ ...attempt, enrollmentSha256: "f".repeat(64) })],
      { concurrency: 2 },
    );
    assert.deepEqual(reserved[0], reserved[1]);
    const started = yield* Effect.all(
      [repo.startDispatch(request, now), repo.startDispatch(request, "later")],
      { concurrency: 2 },
    );
    assert.strictEqual(started.filter(Boolean).length, 1);
    assert.strictEqual(yield* repo.startDispatch(request, now), false);
  }).pipe(Effect.provide(memory)),
);

it.effect("storage failures return typed operations without database text", () =>
  Effect.gen(function* () {
    const repo = yield* Attempts.NativeProviderAttempts;
    const missing = yield* repo.get(request).pipe(Effect.result);
    assert.strictEqual(missing._tag, "Failure");
    if (missing._tag === "Failure") {
      assert.strictEqual(missing.failure._tag, "NativeProviderAttemptError");
      assert.strictEqual(missing.failure.operation, "get");
      assert.strictEqual(missing.failure.message, "Native provider attempt persistence failed.");
    }
    yield* migration;
    const invalid = yield* repo
      .reserve({ ...attempt, requestBytesSha256: "invalid" })
      .pipe(Effect.result);
    assert.strictEqual(invalid._tag, "Failure");
    assert.strictEqual(Option.isNone(yield* repo.get(request)), true);
    const invalidEnrollment = yield* repo
      .reserve({
        ...attempt,
        enrollment: { ...attempt.enrollment, session_id: "" },
      })
      .pipe(Effect.result);
    assert.strictEqual(invalidEnrollment._tag, "Failure");
    if (invalidEnrollment._tag === "Failure")
      assert.strictEqual(invalidEnrollment.failure.operation, "reserve");
    assert.strictEqual(Option.isNone(yield* repo.get(request)), true);
  }).pipe(Effect.provide(memory)),
);
