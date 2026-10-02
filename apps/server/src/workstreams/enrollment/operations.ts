import type { WorkstreamsNativeContext } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { IssuedSession, SessionCredentialIssueError } from "../../auth/SessionStore.ts";
import type { CreateAuthSessionInput } from "../../persistence/AuthSessions.ts";
import type { NativeCredentialWriter } from "./credentialFile.ts";
import {
  NativeEnrollmentRequest,
  deriveNativeEnrollmentSession,
  nativeEnrollmentRequestSha256,
} from "./request.ts";
import type { NativeEnrollments } from "./service.ts";

export type NativeEnrollmentPhase = "native_records_reserved" | "native_credential_published";
export interface NativeEnrollmentReceipt {
  readonly schema: "jones-code.workstreams-native-enrollment-receipt/v1";
  readonly enrollment_id: string;
  readonly request_sha256: string;
  readonly state: "planned" | "applied" | "unchanged" | "conflict" | "unknown";
  readonly completed_phases: ReadonlyArray<NativeEnrollmentPhase>;
  readonly session_id: string;
  readonly native_context: WorkstreamsNativeContext;
  readonly file_pre_sha256: string | null;
  readonly file_post_sha256: string | null;
  readonly reason:
    | "ready"
    | "complete"
    | "records_absent"
    | "credential_absent"
    | "enrollment_conflict"
    | "session_conflict"
    | "qualification_failed"
    | "records_unavailable"
    | "commit_unresolved"
    | "credential_conflict"
    | "credential_unavailable"
    | "credential_issue_failed"
    | "publication_unresolved";
}
export interface NativeEnrollmentOperationPorts {
  readonly enrollments: Pick<NativeEnrollments["Service"], "inspect" | "reserve">;
  readonly materialize: (
    expected: CreateAuthSessionInput,
  ) => Effect.Effect<IssuedSession, SessionCredentialIssueError>;
  readonly credential: NativeCredentialWriter;
}

export const makeNativeEnrollmentOperations = (ports: NativeEnrollmentOperationPorts) => {
  const receipt = (
    request: NativeEnrollmentRequest,
    data: Pick<NativeEnrollmentReceipt, "state" | "reason"> &
      Partial<
        Pick<NativeEnrollmentReceipt, "completed_phases" | "file_pre_sha256" | "file_post_sha256">
      >,
  ): NativeEnrollmentReceipt => ({
    schema: "jones-code.workstreams-native-enrollment-receipt/v1",
    enrollment_id: request.enrollment_id,
    request_sha256: nativeEnrollmentRequestSha256(request),
    session_id: request.session.session_id,
    native_context: request.context,
    completed_phases: [],
    file_pre_sha256: null,
    file_post_sha256: null,
    ...data,
  });
  const recordFailure = (code: string): Pick<NativeEnrollmentReceipt, "state" | "reason"> => {
    if (code === "enrollment_conflict" || code === "session_conflict")
      return { state: "conflict", reason: code };
    if (code === "unknown_commit") return { state: "unknown", reason: "commit_unresolved" };
    if (code === "record_unavailable") return { state: "unknown", reason: "records_unavailable" };
    return { state: "conflict", reason: "qualification_failed" };
  };
  const plan = (request: NativeEnrollmentRequest) =>
    Effect.gen(function* () {
      const records = yield* ports.enrollments.inspect(request).pipe(Effect.result);
      if (records._tag === "Failure") return receipt(request, recordFailure(records.failure.code));
      const file = yield* ports.credential.observe().pipe(Effect.result);
      if (file._tag === "Failure")
        return receipt(request, { state: "unknown", reason: "credential_unavailable" });
      return receipt(request, {
        state: "planned",
        reason: "ready",
        completed_phases: records.success.state === "reserved" ? ["native_records_reserved"] : [],
        file_pre_sha256: file.success.sha256,
        file_post_sha256: file.success.sha256,
      });
    });
  const readback = (request: NativeEnrollmentRequest, expectedCredentialSha256: string) =>
    Effect.gen(function* () {
      const records = yield* ports.enrollments.inspect(request).pipe(Effect.result);
      if (records._tag === "Failure") return receipt(request, recordFailure(records.failure.code));
      if (records.success.state !== "reserved")
        return receipt(request, { state: "unknown", reason: "records_absent" });
      const file = yield* ports.credential.observe().pipe(Effect.result);
      if (file._tag === "Failure")
        return receipt(request, {
          state: "unknown",
          reason: "credential_unavailable",
          completed_phases: ["native_records_reserved"],
        });
      const matches =
        /^[a-f0-9]{64}$/.test(expectedCredentialSha256) &&
        file.success.sha256 === expectedCredentialSha256;
      return receipt(request, {
        state: matches ? "unchanged" : file.success.sha256 === null ? "unknown" : "conflict",
        reason: matches
          ? "complete"
          : file.success.sha256 === null
            ? "credential_absent"
            : "credential_conflict",
        completed_phases: matches
          ? ["native_records_reserved", "native_credential_published"]
          : ["native_records_reserved"],
        file_pre_sha256: file.success.sha256,
        file_post_sha256: file.success.sha256,
      });
    });
  const apply = (request: NativeEnrollmentRequest, expectedBeforeSha256: string | null) =>
    Effect.gen(function* () {
      const observed = yield* ports.enrollments.inspect(request).pipe(Effect.result);
      if (observed._tag === "Failure")
        return receipt(request, recordFailure(observed.failure.code));
      const reserved =
        observed.success.state === "reserved"
          ? observed
          : yield* ports.enrollments.reserve(request).pipe(Effect.result);
      if (reserved._tag === "Failure")
        return receipt(request, recordFailure(reserved.failure.code));
      const issued = yield* ports
        .materialize(deriveNativeEnrollmentSession(request))
        .pipe(Effect.result);
      if (issued._tag === "Failure")
        return receipt(request, {
          state: "unknown",
          reason: "credential_issue_failed",
          completed_phases: ["native_records_reserved"],
        });
      // The bearer crosses only this private writer port and never enters a public receipt.
      const published = yield* ports.credential
        .publish(issued.success.token, expectedBeforeSha256)
        .pipe(Effect.result);
      if (published._tag === "Failure")
        return receipt(request, {
          state: "unknown",
          reason: "credential_unavailable",
          completed_phases: ["native_records_reserved"],
        });
      const result = published.success;
      return receipt(request, {
        state: result.state === "published" ? "applied" : result.state,
        reason:
          result.state === "published" || result.state === "unchanged"
            ? "complete"
            : result.state === "conflict"
              ? "credential_conflict"
              : "publication_unresolved",
        completed_phases:
          result.state === "published" || result.state === "unchanged"
            ? ["native_records_reserved", "native_credential_published"]
            : ["native_records_reserved"],
        file_pre_sha256: result.beforeSha256,
        file_post_sha256: result.afterSha256,
      });
    });
  return { plan, apply, readback };
};
