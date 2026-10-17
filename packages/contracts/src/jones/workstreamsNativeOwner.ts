import * as Schema from "effect/Schema";
import { WorkstreamsNativeContext } from "./workstreamsNativeProvider.ts";

const closed = <Fields extends Schema.Struct.Fields>(schema: Schema.Struct<Fields>) =>
  Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter(
        (value) =>
          typeof value === "object" &&
          value !== null &&
          !Array.isArray(value) &&
          Reflect.ownKeys(value).every((key) => Object.hasOwn(schema.fields, key)),
      ),
    ),
  );
const Id = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/));
const Sha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const WorkstreamsNativeOwnerRequest = Schema.Union([
  closed(Schema.Struct({ operation: Schema.Literal("authority-enroll") })),
  closed(
    Schema.Struct({
      operation: Schema.Literal("provider-prepare"),
      enrollmentId: Id,
      ownerId: Id,
      principalId: Id,
      registryOrigin: Schema.String.check(Schema.isMaxLength(2048)),
      lifetimeDays: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 30 })),
    }),
  ),
  closed(Schema.Struct({ operation: Schema.Literal("provider-plan"), enrollmentId: Id })),
  closed(
    Schema.Struct({
      operation: Schema.Literal("provider-apply"),
      enrollmentId: Id,
      expectedPriorSha256: Schema.NullOr(Sha256),
    }),
  ),
  closed(
    Schema.Struct({
      operation: Schema.Literal("provider-readback"),
      enrollmentId: Id,
      expectedCredentialSha256: Sha256,
    }),
  ),
]);
export type WorkstreamsNativeOwnerRequest = typeof WorkstreamsNativeOwnerRequest.Type;
export const WorkstreamsNativeOwnerResponse = Schema.Union([
  closed(
    Schema.Struct({
      schema: Schema.Literal("jones-code.workstreams-native-authority-enrolled/v1"),
      environment_id: Id,
      authority_namespace: Id,
      store_generation: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
    }),
  ),
  closed(
    Schema.Struct({
      schema: Schema.Literal("jones-code.workstreams-native-enrollment-prepared/v1"),
      enrollment_id: Id,
      request_sha256: Sha256,
      session_id: Id,
      expires_at: Schema.String,
      native_context: WorkstreamsNativeContext,
    }),
  ),
  closed(
    Schema.Struct({
      schema: Schema.Literal("jones-code.workstreams-native-enrollment-receipt/v1"),
      enrollment_id: Id,
      request_sha256: Sha256,
      session_id: Id,
      native_context: WorkstreamsNativeContext,
      state: Schema.Literals(["planned", "applied", "unchanged", "conflict", "unknown"]),
      completed_phases: Schema.Array(
        Schema.Literals(["native_records_reserved", "native_credential_published"]),
      ),
      file_pre_sha256: Schema.NullOr(Sha256),
      file_post_sha256: Schema.NullOr(Sha256),
      reason: Schema.Literals([
        "ready",
        "complete",
        "records_absent",
        "credential_absent",
        "enrollment_conflict",
        "session_conflict",
        "qualification_failed",
        "records_unavailable",
        "commit_unresolved",
        "credential_conflict",
        "credential_unavailable",
        "credential_issue_failed",
        "publication_unresolved",
      ]),
    }),
  ),
]);
export type WorkstreamsNativeOwnerResponse = typeof WorkstreamsNativeOwnerResponse.Type;
