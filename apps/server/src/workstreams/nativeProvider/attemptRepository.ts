import { WorkstreamsNativeSettlementRequest } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import { NativeProviderEnrollmentBinding } from "./enrollment.ts";

export const NativeProviderAttempt = Schema.Struct({
  request: WorkstreamsNativeSettlementRequest,
  requestBytesSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  enrollmentSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  enrollment: NativeProviderEnrollmentBinding,
  nativeCommandId: Schema.String,
  createdAt: Schema.String,
  dispatchStartedAt: Schema.NullOr(Schema.String),
});
export type NativeProviderAttempt = typeof NativeProviderAttempt.Type;
export type NativeProviderAttemptKey = Pick<
  WorkstreamsNativeSettlementRequest,
  "owner_id" | "principal_id" | "command_id"
>;

export class NativeProviderAttemptError extends Schema.TaggedError<NativeProviderAttemptError>()(
  "NativeProviderAttemptError",
  { operation: Schema.Literals(["get", "reserve", "startDispatch"]) },
) {
  override get message(): string {
    return "Native provider attempt persistence failed.";
  }
}

export class NativeProviderAttempts extends Context.Service<
  NativeProviderAttempts,
  {
    readonly get: (
      key: NativeProviderAttemptKey,
    ) => Effect.Effect<Option.Option<NativeProviderAttempt>, NativeProviderAttemptError>;
    readonly reserve: (
      attempt: NativeProviderAttempt,
    ) => Effect.Effect<NativeProviderAttempt, NativeProviderAttemptError>;
    readonly startDispatch: (
      key: NativeProviderAttemptKey,
      startedAt: string,
    ) => Effect.Effect<boolean, NativeProviderAttemptError>;
  }
>()("t3/workstreams/nativeProvider/attemptRepository/NativeProviderAttempts") {}

const Key = Schema.Struct({
  owner_id: Schema.String,
  principal_id: Schema.String,
  command_id: Schema.String,
});
const EnrollmentJson = Schema.fromJsonString(NativeProviderEnrollmentBinding);
const RequestJson = Schema.fromJsonString(WorkstreamsNativeSettlementRequest);
const encodeEnrollment = Schema.encodeEffect(EnrollmentJson);
const encodeRequest = Schema.encodeEffect(RequestJson);
const validateAttempt = Schema.decodeUnknownEffect(NativeProviderAttempt);
const Row = Schema.Struct({
  ...NativeProviderAttempt.fields,
  request: RequestJson,
  enrollment: EnrollmentJson,
});

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const read = SqlSchema.findOneOption({
    Request: Key,
    Result: Row,
    execute: (key) => sql`
      SELECT request_json AS "request", request_bytes_sha256 AS "requestBytesSha256",
        enrollment_sha256 AS "enrollmentSha256", enrollment_json AS "enrollment",
        native_command_id AS "nativeCommandId", created_at AS "createdAt",
        dispatch_started_at AS "dispatchStartedAt"
      FROM workstreams_native_attempts WHERE owner_id = ${key.owner_id}
        AND principal_id = ${key.principal_id} AND command_id = ${key.command_id}
    `,
  });
  const get: NativeProviderAttempts["Service"]["get"] = (key) =>
    read(key).pipe(Effect.mapError(() => new NativeProviderAttemptError({ operation: "get" })));
  const reserve: NativeProviderAttempts["Service"]["reserve"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const attempt = yield* validateAttempt(input);
          const requestJson = yield* encodeRequest(attempt.request);
          const enrollmentJson = yield* encodeEnrollment(attempt.enrollment);
          yield* sql`
        INSERT INTO workstreams_native_attempts
          (owner_id, principal_id, command_id, request_json, request_bytes_sha256,
            enrollment_sha256, enrollment_json, native_command_id, created_at)
        SELECT ${attempt.request.owner_id}, ${attempt.request.principal_id},
          ${attempt.request.command_id}, ${requestJson}, ${attempt.requestBytesSha256},
          ${attempt.enrollmentSha256}, ${enrollmentJson}, ${attempt.nativeCommandId},
          ${attempt.createdAt}
        WHERE NOT EXISTS (SELECT 1 FROM workstreams_native_attempts
          WHERE owner_id = ${attempt.request.owner_id}
            AND principal_id = ${attempt.request.principal_id}
            AND command_id = ${attempt.request.command_id})
      `;
          const persisted = yield* read(attempt.request);
          if (Option.isNone(persisted))
            return yield* new NativeProviderAttemptError({ operation: "reserve" });
          return persisted.value;
        }),
      )
      .pipe(Effect.mapError(() => new NativeProviderAttemptError({ operation: "reserve" })));
  const startDispatch: NativeProviderAttempts["Service"]["startDispatch"] = (key, startedAt) =>
    sql`
      UPDATE workstreams_native_attempts SET dispatch_started_at = ${startedAt}
      WHERE owner_id = ${key.owner_id} AND principal_id = ${key.principal_id}
        AND command_id = ${key.command_id} AND dispatch_started_at IS NULL RETURNING command_id
    `.pipe(
      Effect.map((rows) => rows.length === 1),
      Effect.mapError(() => new NativeProviderAttemptError({ operation: "startDispatch" })),
    );
  return NativeProviderAttempts.of({ get, reserve, startDispatch });
});

export const NativeProviderAttemptsLive = Layer.effect(NativeProviderAttempts, make);
