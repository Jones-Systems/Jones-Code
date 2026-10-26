import { OrchestratorMcpFailure, RemoteAdmissionEnvelope, type ThreadResultOutcome } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { admissionOperation, threadSendIdentity } from "./RemoteRequestProtocol.ts";

const decodeEnvelope = Schema.decodeUnknownOption(Schema.fromJsonString(RemoteAdmissionEnvelope));
const decodeResult = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Struct({ ok: Schema.Boolean, value: Schema.Unknown })));
const noEffectFailures = new Set<OrchestratorMcpFailure["code"]>([
  "capability_denied", "parent_not_active", "provider_unavailable", "model_unavailable",
  "runtime_mode_escalation_denied", "interaction_mode_escalation_denied", "thread_not_found",
  "run_not_found", "thread_not_sendable", "invalid_request", "thread_credential_required", "target_required",
]);

// Called in the same read transaction as requestByOperation. Admission's command_id
// identifies its claim, not the MCP send; only the immutable envelope identifies that receipt.
export const readAdmissionObservation = Effect.fn("ResultDependencies.readAdmissionObservation")(function* (
  sql: SqlClient.SqlClient,
  input: { readonly originEnvironmentId: string; readonly operationId: string },
) {
  const rows = yield* sql<{ id: string; status: string; envelope_json: string; result_json: string | null }>`
    SELECT admission.id, admission.status, admission.envelope_json, result.result_json
    FROM jones_remote_admissions AS admission
    LEFT JOIN jones_remote_admission_results AS result ON result.admission_id = admission.id
    WHERE admission.origin_environment_id = ${input.originEnvironmentId} AND admission.operation_id = ${input.operationId}`;
  const admission = rows[0];
  if (!admission) return undefined;
  let outcome: ThreadResultOutcome | null = null;
  let result: string | null = null;
  let receiptStatus: string | null = null;
  let exactIdentity = false;
  const envelope = decodeEnvelope(admission.envelope_json);
  if (Option.isSome(envelope)) {
    const operation = admissionOperation(envelope.value);
    const payload = envelope.value.payload as { params?: { clientRequestId?: unknown; threadId?: unknown }; invocation?: { requestNamespace?: unknown } };
    if (operation?.origin === input.originEnvironmentId && operation.operation === input.operationId &&
      typeof payload.params?.clientRequestId === "string" && typeof payload.params.threadId === "string" &&
      typeof payload.invocation?.requestNamespace === "string") {
      exactIdentity = true;
      const { commandId } = threadSendIdentity(payload.invocation.requestNamespace, payload.params.clientRequestId);
      const receipts = yield* sql<{ status: string; aggregate_kind: string; aggregate_id: string; command_type: string }>`
        SELECT status, aggregate_kind, aggregate_id, command_type FROM orchestration_command_receipts WHERE command_id = ${commandId}`;
      const receipt = receipts[0];
      receiptStatus = receipt === undefined ? null : receipt.aggregate_kind === "thread" &&
        receipt.aggregate_id === payload.params.threadId && receipt.command_type === "message.dispatch" ? receipt.status : "mismatch";
    }
  }
  const saved = admission.result_json === null ? Option.none() : decodeResult(admission.result_json);
  const refused = receiptStatus === "rejected" || exactIdentity && receiptStatus === null && Option.isSome(saved) &&
    saved.value.ok === false && Schema.is(OrchestratorMcpFailure)(saved.value.value) && noEffectFailures.has(saved.value.value.code);
  if (refused) {
    outcome = "refused";
    result = "The receiving send was refused before accepting the request.";
  } else if (admission.result_json !== null || receiptStatus !== null || admission.status === "dispatched") {
    outcome = "unknown";
    result = "The receiving send ended without a correlated request binding. Remote work may continue; its outcome is unknown.";
  }
  return { id: admission.id, status: admission.status, outcome, result, receiptStatus };
});
