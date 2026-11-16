import { recordedLaunchResult } from "./recordedResult.ts";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as SqlClient from "effect/sql/SqlClient";
import * as DTO from "@t3tools/contracts/jones/remoteAdmissionReview";
import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import { readCaller, assertLiveCaller } from "../../mcp/threadAccess.ts";
import * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import { ReceiverRecoveryEvidence, makeRecoveryStore, type AttemptRow } from "./RecoveryStore.ts";

export class ReceiverInspection extends Context.Service<
  ReceiverInspection,
  {
    readonly status: (
      input: DTO.RemoteAdmissionRecoveryStatusInput,
    ) => Effect.Effect<
      DTO.RemoteAdmissionRecoveryStatus,
      OrchestratorMcpFailure,
      Invocation.McpInvocationContext
    >;
  }
>()("jones/remoteAdmissionIntegration/ReceiverInspection") {}
const unavailable = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: "Remote request status is unavailable.",
  });
export const inspectionStatus = Effect.fn("AdmissionInspection.status")(function* (
  input: DTO.RemoteAdmissionRecoveryStatusInput,
) {
  const scope = yield* Invocation.McpInvocationContext;
  // A locator is not an authority token. Validate live caller/capability before selecting.
  let projectId: string | undefined;
  let localSession: string | undefined;
  if (scope.thread) {
    yield* Invocation.requireThreadScope(scope, "t3_remote_admission_recovery_status");
    const caller = yield* readCaller();
    yield* assertLiveCaller(caller);
    projectId = caller.caller?.projectId;
    localSession = caller.caller?.linkOrigin?.sessionId;
  } else if (
    !scope.client?.linked ||
    !scope.capabilities.has("orchestration") ||
    !scope.client.remoteOrigin?.thread ||
    scope.client.remoteOrigin.linkSessionId !== scope.client.sessionId
  )
    return yield* unavailable();
  const sql = yield* SqlClient.SqlClient,
    admissions = yield* Admission.RemoteAdmission;
  if (scope.client) {
    const active =
      yield* sql`SELECT 1 FROM auth_sessions WHERE session_id=${scope.client.sessionId} AND subject='mcp-client'
      AND method='bearer-access-token' AND revoked_at IS NULL AND expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now') AND EXISTS(SELECT 1 FROM json_each(scopes) WHERE value='orchestration:operate')`;
    if (!active.length) return yield* unavailable();
  }
  let id: string | undefined;
  if ("locator" in input) {
    const rows =
      yield* sql<AttemptRow>`SELECT * FROM jones_remote_admission_attempts WHERE locator=${input.locator}`;
    id = rows[0]?.child_admission_id;
  } else {
    const client = scope.client;
    if (!client || client.remoteOrigin?.thread?.threadId !== input.originThreadId)
      return yield* unavailable();
    const rows = yield* sql<{
      id: string;
    }>`SELECT id FROM jones_remote_admissions WHERE client_id=${client.sessionId}
      AND json_extract(envelope_json,'$.payload.params.clientRequestId')=${input.clientRequestId}`;
    id = rows[0]?.id;
  }
  const record = id ? yield* admissions.get(id) : null;
  if (!record) return yield* unavailable();
  const invocation = (
    record.envelope.payload as {
      invocation: { client?: { remoteOrigin?: { thread?: { threadId: string } } } };
    }
  ).invocation;
  if (scope.thread) {
    if (
      !projectId ||
      record.envelope.target.projectId !== projectId ||
      (localSession !== undefined && localSession !== record.envelope.identity.clientId)
    )
      return yield* unavailable();
  } else if (
    scope.client?.sessionId !== record.envelope.identity.clientId ||
    scope.client.remoteOrigin?.thread?.threadId !==
      invocation.client?.remoteOrigin?.thread?.threadId
  )
    return yield* unavailable();
  const reader = yield* ReceiverRecoveryEvidence;
  const recovered = yield* makeRecoveryStore(sql, admissions, reader).recovered(record.id);
  const fences =
    yield* sql`SELECT 1 FROM jones_remote_admission_fences WHERE predecessor_admission_id=${record.id}`;
  const result = recovered?.result ?? (yield* recordedLaunchResult(sql, record.id));
  return {
    receiverEnvironmentId: scope.environmentId,
    state: fences.length
      ? "superseded"
      : (result?.outcome ?? (record.status === "dispatched" ? "unknown" : "awaiting_confirmation")),
    reasonCode: fences.length
      ? "superseded"
      : (result?.reasonCode ??
        (record.status === "dispatched" ? "recorded_outcome_unknown" : "awaiting_confirmation")),
    stages: result?.stages ?? {
      creation: "pending",
      availability: "unknown",
      initialMessage: "unconfirmed",
      preparation: "unknown",
      providerProgress: "unknown",
    },
    confirmedThreadId:
      result?.outcome === "created" && result.stages.availability === "available"
        ? result.threadId
        : null,
    requestsRoute: `/requests?receiver=${encodeURIComponent(scope.environmentId)}&request=${encodeURIComponent(record.id)}`,
  } satisfies DTO.RemoteAdmissionRecoveryStatus;
}, Effect.mapError(unavailable));
