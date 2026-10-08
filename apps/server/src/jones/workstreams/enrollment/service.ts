import { AuthSessionId, WorkstreamsNativeBuild } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { matchesReservedAuthSession } from "../../../auth/SessionStore.ts";
import { NativeStoreAuthority } from "../../../environment/NativeStoreAuthority.ts";
import * as AuthSessions from "../../../persistence/AuthSessions.ts";
import {
  NativeProviderEnrollment,
  NativeProviderEnrollmentBinding,
  NativeProviderEnrollmentError,
} from "../nativeProvider/enrollment.ts";
import { readNativeProviderBuild } from "../nativeProvider/service.ts";
import {
  NativeEnrollmentRequest,
  canonicalNativeEnrollmentRequest,
  deriveNativeEnrollmentBinding,
  deriveNativeEnrollmentSession,
  nativeEnrollmentRequestSha256,
} from "./request.ts";

export class NativeEnrollmentError extends Schema.TaggedError<NativeEnrollmentError>()(
  "NativeEnrollmentError",
  {
    code: Schema.Literals([
      "invalid_request",
      "enrollment_conflict",
      "session_conflict",
      "authority_unavailable",
      "authority_mismatch",
      "build_unavailable",
      "build_mismatch",
      "record_unavailable",
      "unknown_commit",
    ]),
  },
) {}
export type NativeEnrollmentInspection = { readonly state: "absent" | "reserved" };

export class NativeEnrollments extends Context.Service<
  NativeEnrollments,
  {
    readonly inspect: (
      request: NativeEnrollmentRequest,
    ) => Effect.Effect<NativeEnrollmentInspection, NativeEnrollmentError>;
    readonly reserve: (
      request: NativeEnrollmentRequest,
    ) => Effect.Effect<NativeEnrollmentInspection, NativeEnrollmentError>;
    readonly getBySessionId: (
      sessionId: string,
    ) => Effect.Effect<Option.Option<NativeProviderEnrollmentBinding>, NativeEnrollmentError>;
  }
>()("t3/jones/workstreams/enrollment/service/NativeEnrollments") {}

export interface NativeEnrollmentPorts {
  readonly authority: Pick<NativeStoreAuthority["Service"], "readCurrent">;
  readonly build: Effect.Effect<Option.Option<WorkstreamsNativeBuild>>;
}
const Row = Schema.Struct({
  enrollmentId: Schema.String,
  sessionId: AuthSessionId,
  requestSha256: Schema.String,
  requestJson: Schema.String,
  bindingJson: Schema.String,
});
type Row = typeof Row.Type;
const decodeRequest = Schema.decodeUnknownEffect(NativeEnrollmentRequest);
const decodePersistedRequest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(NativeEnrollmentRequest),
);
const decodeBinding = Schema.decodeUnknownEffect(
  Schema.fromJsonString(NativeProviderEnrollmentBinding),
);
const jsonBinding = Schema.encodeSync(Schema.fromJsonString(NativeProviderEnrollmentBinding));

export const makeNativeEnrollments = (ports: NativeEnrollmentPorts) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const sessions = yield* AuthSessions.AuthSessionRepository;
    const read = (enrollmentId: string, sessionId: string) =>
      sql`
    SELECT enrollment_id AS "enrollmentId", session_id AS "sessionId", request_sha256 AS "requestSha256",
      request_json AS "requestJson", binding_json AS "bindingJson"
    FROM workstreams_native_enrollments WHERE enrollment_id = ${enrollmentId} OR session_id = ${sessionId}`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Row))),
        Effect.mapError(() => new NativeEnrollmentError({ code: "record_unavailable" })),
      );
    const validate = (input: NativeEnrollmentRequest) =>
      decodeRequest(input).pipe(
        Effect.mapError(() => new NativeEnrollmentError({ code: "invalid_request" })),
      );
    const qualify = (request: NativeEnrollmentRequest) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        if (
          Date.parse(request.session.issued_at) > now.epochMilliseconds ||
          Date.parse(request.session.expires_at) <= now.epochMilliseconds
        )
          return yield* new NativeEnrollmentError({ code: "session_conflict" });
        const authority = yield* ports.authority.readCurrent.pipe(
          Effect.mapError(() => new NativeEnrollmentError({ code: "authority_unavailable" })),
        );
        if (
          authority.environmentId !== request.context.source_instance_id ||
          authority.authorityNamespace !== request.context.authority_namespace ||
          authority.storeGeneration !== request.context.store_generation
        )
          return yield* new NativeEnrollmentError({ code: "authority_mismatch" });
        const build = yield* ports.build;
        if (Option.isNone(build))
          return yield* new NativeEnrollmentError({ code: "build_unavailable" });
        if (
          build.value.repository !== request.context.build.repository ||
          build.value.sha !== request.context.build.sha ||
          build.value.tree !== request.context.build.tree
        )
          return yield* new NativeEnrollmentError({ code: "build_mismatch" });
      });
    const checkedRowRequest = (row: Row) =>
      Effect.gen(function* () {
        const request = yield* decodePersistedRequest(row.requestJson);
        const binding = yield* decodeBinding(row.bindingJson);
        if (
          row.enrollmentId !== request.enrollment_id ||
          row.sessionId !== request.session.session_id ||
          row.requestJson !== canonicalNativeEnrollmentRequest(request) ||
          row.requestSha256 !== nativeEnrollmentRequestSha256(request) ||
          row.bindingJson !== jsonBinding(deriveNativeEnrollmentBinding(request)) ||
          jsonBinding(binding) !== row.bindingJson
        )
          return yield* new NativeEnrollmentError({ code: "enrollment_conflict" });
        return request;
      }).pipe(Effect.mapError(() => new NativeEnrollmentError({ code: "enrollment_conflict" })));
    const activeSession = (request: NativeEnrollmentRequest) =>
      Effect.gen(function* () {
        const session = yield* sessions
          .getById({ sessionId: request.session.session_id })
          .pipe(Effect.mapError(() => new NativeEnrollmentError({ code: "record_unavailable" })));
        const now = yield* DateTime.now;
        if (Option.isNone(session)) return false;
        if (
          session.value.revokedAt !== null ||
          session.value.expiresAt.epochMilliseconds <= now.epochMilliseconds ||
          session.value.issuedAt.epochMilliseconds > now.epochMilliseconds ||
          !matchesReservedAuthSession(session.value, deriveNativeEnrollmentSession(request))
        )
          return yield* new NativeEnrollmentError({ code: "session_conflict" });
        return true;
      });
    const inspectRecords = (request: NativeEnrollmentRequest) =>
      Effect.gen(function* () {
        const rows = yield* read(request.enrollment_id, request.session.session_id);
        const active = yield* activeSession(request);
        if (rows.length === 0) return { state: "absent" as const };
        if (rows.length !== 1)
          return yield* new NativeEnrollmentError({ code: "enrollment_conflict" });
        const stored = yield* checkedRowRequest(rows[0]!);
        if (nativeEnrollmentRequestSha256(stored) !== nativeEnrollmentRequestSha256(request))
          return yield* new NativeEnrollmentError({ code: "enrollment_conflict" });
        if (!active) return yield* new NativeEnrollmentError({ code: "session_conflict" });
        return { state: "reserved" as const };
      });
    const inspect: NativeEnrollments["Service"]["inspect"] = (input) =>
      Effect.gen(function* () {
        const request = yield* validate(input);
        yield* qualify(request);
        const result = yield* inspectRecords(request);
        yield* qualify(request);
        return result;
      });
    const reserve: NativeEnrollments["Service"]["reserve"] = (input) =>
      Effect.gen(function* () {
        const request = yield* validate(input);
        yield* qualify(request);
        // A failed COMMIT is unresolved; callers inspect exact records before any later reserve.
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* qualify(request);
              yield* sessions.createIfAbsent(deriveNativeEnrollmentSession(request));
              if (!(yield* activeSession(request)))
                return yield* new NativeEnrollmentError({ code: "session_conflict" });
              const before = yield* inspectRecords(request);
              if (before.state === "absent") {
                yield* sql`INSERT INTO workstreams_native_enrollments
          (enrollment_id, session_id, request_sha256, request_json, binding_json)
          VALUES (${request.enrollment_id}, ${request.session.session_id}, ${nativeEnrollmentRequestSha256(request)},
            ${canonicalNativeEnrollmentRequest(request)}, ${jsonBinding(deriveNativeEnrollmentBinding(request))})`;
              }
              const result = yield* inspectRecords(request);
              if (result.state !== "reserved")
                return yield* new NativeEnrollmentError({ code: "enrollment_conflict" });
              yield* qualify(request);
              return result;
            }),
          )
          .pipe(
            Effect.mapError((error) =>
              Schema.is(NativeEnrollmentError)(error)
                ? error
                : new NativeEnrollmentError({ code: "unknown_commit" }),
            ),
          );
        const result = yield* inspectRecords(request);
        yield* qualify(request);
        return result;
      });
    const getBySessionId: NativeEnrollments["Service"]["getBySessionId"] = (sessionId) =>
      Effect.gen(function* () {
        const rows = yield* read("", sessionId);
        if (rows.length === 0) return Option.none<NativeProviderEnrollmentBinding>();
        if (rows.length !== 1)
          return yield* new NativeEnrollmentError({ code: "enrollment_conflict" });
        const request = yield* checkedRowRequest(rows[0]!);
        if (request.session.session_id !== sessionId || !(yield* activeSession(request)))
          return Option.none<NativeProviderEnrollmentBinding>();
        return Option.some(deriveNativeEnrollmentBinding(request));
      }).pipe(Effect.catch(() => Effect.succeed(Option.none<NativeProviderEnrollmentBinding>())));
    return NativeEnrollments.of({ inspect, reserve, getBySessionId });
  });

export const NativeEnrollmentsLive = Layer.effect(
  NativeEnrollments,
  Effect.gen(function* () {
    const authority = yield* NativeStoreAuthority;
    return yield* makeNativeEnrollments({ authority, build: readNativeProviderBuild });
  }),
);
export const NativeProviderEnrollmentLive = Layer.effect(
  NativeProviderEnrollment,
  Effect.gen(function* () {
    const enrollments = yield* NativeEnrollments;
    return NativeProviderEnrollment.of({
      getBySessionId: (sessionId) =>
        enrollments
          .getBySessionId(sessionId)
          .pipe(Effect.mapError(() => new NativeProviderEnrollmentError())),
    });
  }),
);
