// @effect-diagnostics nodeBuiltinImport:off -- Synchronous receiver identity hashes and opaque IDs preserve retained command/fence bytes.
import * as Crypto from "node:crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as DateTime from "effect/DateTime";
import { RemoteAdmissionReceiverPolicy, type RemoteAdmissionRecord } from "@t3tools/contracts";
import type * as DTO from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as RemoteAdmission from "../remoteAdmission/RemoteAdmission.ts";
import {
  classifyLaunchEvidence,
  evidenceDigest,
  type LaunchEvidenceInput,
} from "./LaunchRecovery.ts";
import { canonicalJsonStrict } from "../remoteDelegation/shared.ts";

export class RecoveryUnavailable extends Schema.TaggedError<RecoveryUnavailable>()(
  "RecoveryUnavailable",
  {},
) {}
export class RecoveryConflict extends Schema.TaggedError<RecoveryConflict>()("RecoveryConflict", {
  reason: Schema.String,
}) {}
/** Required source adapters. All callbacks are DB-only, use the same transaction, and never take settings locks. */
export class ReceiverRecoveryEvidence extends Context.Service<
  ReceiverRecoveryEvidence,
  {
    readonly read: (
      record: RemoteAdmissionRecord,
    ) => Effect.Effect<LaunchEvidenceInput, RecoveryUnavailable>;
    readonly root: (
      record: RemoteAdmissionRecord,
    ) => Effect.Effect<DTO.RemoteAdmissionReturnBinding | null, RecoveryUnavailable>;
    readonly authorize: (
      record: RemoteAdmissionRecord,
    ) => Effect.Effect<boolean, RecoveryUnavailable>;
  }
>()("t3/jones/remoteAdmissionIntegration/RecoveryStore/ReceiverRecoveryEvidence") {}
export interface AttemptRow {
  attempt_id: string;
  root_admission_id: string;
  predecessor_admission_id: string;
  child_admission_id: string;
  session_id: string;
  command_key: string;
  fingerprint: string;
  locator: string;
  state: "prepared" | "origin_registered" | "claimed" | "completed" | "refused" | "expired";
  root_observation_key: string;
  observation_key: string | null;
  grant_intent_json: string | null;
  proof_digest: string;
  created_at: string;
  expires_at: string;
  terminal_at: string | null;
}
export const attemptView = (
  row: AttemptRow,
  receiverEnvironmentId: string,
): DTO.RemoteAdmissionRetryAttempt => ({
  attemptId: row.attempt_id,
  rootAdmissionId: row.root_admission_id,
  predecessorAdmissionId: row.predecessor_admission_id,
  childAdmissionId: row.child_admission_id,
  receiverEnvironmentId,
  locator: row.locator,
  state: row.state,
  createdAt: row.created_at,
  expiresAt: row.expires_at,
  grantIntent: row.grant_intent_json
    ? Schema.decodeSync(Schema.fromJsonString(DTOGrantIntent))(row.grant_intent_json)
    : null,
});
// Import the runtime schema separately so DTO namespace stays type-only.
import {
  RemoteAdmissionGrantIntent as DTOGrantIntent,
  RemoteAdmissionRecoveredResult as RecoveredSchema,
  RemoteAdmissionLaunchResult,
  RemoteAdmissionRecoveryEvidence,
  RemoteAdmissionAccessBinding,
} from "@t3tools/contracts/jones/remoteAdmissionReview";
export const makeRecoveryStore = (
  sql: SqlClient.SqlClient,
  admissions: Pick<RemoteAdmission.RemoteAdmission["Service"], "access" | "get">,
  reader: ReceiverRecoveryEvidence["Service"],
) => {
  const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const required = Effect.fn("Recovery.required")(function* (input: {
    id: string;
    bindingDigest: string;
  }) {
    const record = yield* admissions.get(input.id);
    if (!record) return yield* new RecoveryUnavailable({});
    if (record.bindingDigest !== input.bindingDigest)
      return yield* new RecoveryConflict({ reason: "binding_changed" });
    return record;
  });
  const recovered = Effect.fn("Recovery.recovered")(function* (id: string) {
    const rows = yield* sql<{
      result_json: string;
      evidence_json: string;
      revision: number;
    }>`SELECT result_json,evidence_json,revision FROM jones_remote_admission_recovered_results WHERE admission_id=${id} ORDER BY revision DESC LIMIT 1`;
    return rows[0]
      ? yield* Schema.decodeEffect(RecoveredSchema)({
          admissionId: id,
          revision: rows[0].revision,
          result: yield* Schema.decodeEffect(Schema.fromJsonString(RemoteAdmissionLaunchResult))(
            rows[0].result_json,
          ),
          evidence: yield* Schema.decodeEffect(
            Schema.fromJsonString(RemoteAdmissionRecoveryEvidence),
          )(rows[0].evidence_json),
        })
      : null;
  });
  // Caller holds the settings snapshot before entering this transaction.
  const recheck = Effect.fn("Recovery.recheck")(function* (input: DTO.RemoteAdmissionRecheckInput) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const record = yield* required(input);
        if (!(yield* reader.authorize(record)))
          return yield* new RecoveryConflict({ reason: "authority_changed" });
        if (record.status !== "dispatched" || record.envelope.operation !== "t3_thread_launch")
          return yield* new RecoveryConflict({ reason: "state_changed" });
        const raw = yield* reader.read(record);
        if (raw.liveClaim || !raw.readsComplete) return yield* new RecoveryUnavailable({});
        const classified = classifyLaunchEvidence(raw);
        if (classified.result.outcome === "unknown") return yield* new RecoveryUnavailable({});
        const prior = yield* recovered(record.id);
        // observedAt changes alone do not create another recovered revision.
        const digest = evidenceDigest({
          result: { ...classified.result, recordedAt: null },
          evidence: { ...classified.evidence, observedAt: null, evidenceDigest: null },
        });
        const existing = yield* sql<{
          result_json: string;
          evidence_json: string;
          revision: number;
        }>`SELECT result_json,evidence_json,revision FROM jones_remote_admission_recovered_results WHERE admission_id=${record.id} AND evidence_digest=${digest}`;
        if (existing[0])
          return yield* Schema.decodeEffect(RecoveredSchema)({
            admissionId: record.id,
            revision: existing[0].revision,
            result: yield* Schema.decodeEffect(Schema.fromJsonString(RemoteAdmissionLaunchResult))(
              existing[0].result_json,
            ),
            evidence: yield* Schema.decodeEffect(
              Schema.fromJsonString(RemoteAdmissionRecoveryEvidence),
            )(existing[0].evidence_json),
          });
        const result: DTO.RemoteAdmissionRecoveredResult = {
          admissionId: record.id,
          revision: (prior?.revision ?? 0) + 1,
          evidence: { ...classified.evidence, evidenceDigest: digest },
          result: classified.result,
        };
        const now = yield* nowIso;
        const root = yield* reader.root(record);
        const lineage = (yield* sql<{
          root_admission_id: string;
        }>`SELECT root_admission_id FROM jones_remote_admission_attempts WHERE child_admission_id=${record.id}`)[0];
        const sessionId = record.envelope.identity.clientId;
        const supported =
          yield* sql`SELECT 1 FROM jones_remote_peer_capabilities WHERE session_id=${sessionId}
        AND json_extract(versions_json,'$.recoveredResultObservation')='jones.remote-admission-recovered-result/v1' LIMIT 1`;
        yield* sql`INSERT INTO jones_remote_admission_recovered_results VALUES(${record.id},${result.revision},${digest},${canonicalJsonStrict(result.result)},${canonicalJsonStrict(result.evidence)},${now})`;
        yield* sql`INSERT INTO jones_remote_admission_recovered_notices(receiver_environment_id,admission_id,recovered_revision,
        session_id,root_admission_id,root_observation_key,return_binding_json,evidence_digest,result_json,state,created_at)
        VALUES(${rawReceiver(record)},${record.id},${result.revision},${sessionId},${lineage?.root_admission_id ?? record.id},${root?.rootObservationKey ?? "unavailable"},
        ${canonicalJsonStrict(root)},${digest},${canonicalJsonStrict(result.result)},${root === null ? "undeliverable" : supported.length ? "unregistered" : "awaiting_support"},${now})`;
        if (
          result.result.outcome === "created" &&
          result.result.threadId !== null &&
          !result.evidence.contradictions.length
        ) {
          const grants = yield* sql<{
            generation: number;
          }>`SELECT generation FROM jones_remote_admission_grants WHERE admission_id=${record.id}`;
          if (grants[0])
            yield* admissions.access.activateInTransaction({
              admissionId: record.id,
              expectedGeneration: grants[0].generation,
              destinationThreadId: result.result.threadId,
              now,
            });
        }
        yield* admissions.access.bumpInTransaction("recovered_result", record.id, now, sessionId);
        return result;
      }),
    );
  });
  const retry = Effect.fn("Recovery.retry")(function* (input: DTO.RemoteAdmissionRetryInput) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const record = yield* required(input);
        const fp = evidenceDigest(input);
        const existing =
          (yield* sql<AttemptRow>`SELECT * FROM jones_remote_admission_attempts WHERE predecessor_admission_id=${record.id}`)[0];
        if (existing) {
          if (existing.command_key !== input.commandKey || existing.fingerprint !== fp)
            return yield* new RecoveryConflict({ reason: "command_changed" });
          return attemptView(existing, rawReceiver(record));
        }
        const payload = record.envelope.payload as { params?: Record<string, unknown> };
        const params = payload.params;
        const saved =
          yield* sql`SELECT 1 FROM jones_remote_admission_results WHERE admission_id=${record.id}`;
        const root = yield* reader.root(record);
        const supported =
          yield* sql`SELECT 1 FROM jones_remote_peer_capabilities WHERE session_id=${record.envelope.identity.clientId}
        AND json_extract(versions_json,'$.retryObservation')='jones.remote-admission-retry-observation/v1' LIMIT 1`;
        if (
          !params ||
          typeof params.clientRequestId !== "string" ||
          (Array.isArray(params.attachments) && params.attachments.length > 0) ||
          record.envelope.operation !== "t3_thread_launch" ||
          record.status !== "dispatched" ||
          !saved.length ||
          !root ||
          !supported.length ||
          !(yield* reader.authorize(record))
        )
          return yield* new RecoveryConflict({ reason: "retry_ineligible" });
        const proof = classifyLaunchEvidence(yield* reader.read(record));
        if (proof.result.outcome !== "not_created")
          return yield* new RecoveryConflict({ reason: "retry_ineligible" });
        const now = yield* nowIso;
        const lineage = (yield* sql<{
          root_admission_id: string;
        }>`SELECT root_admission_id FROM jones_remote_admission_attempts WHERE child_admission_id=${record.id}`)[0];
        const currentPolicy = (yield* sql<{
          policy_json: string;
        }>`SELECT policy_json FROM jones_remote_admission_policy WHERE singleton=1`)[0];
        if (!currentPolicy) return yield* new RecoveryUnavailable({});
        const receiverPolicy = yield* Schema.decodeEffect(
          Schema.fromJsonString(RemoteAdmissionReceiverPolicy),
        )(currentPolicy.policy_json);
        const attemptId = Crypto.randomUUID(),
          childId = Crypto.randomUUID();
        const envelope = {
          ...record.envelope,
          receiverPolicy,
          payload: {
            ...payload,
            params: { ...params, clientRequestId: attemptId },
            invocation: {
              ...(record.envelope.payload as { invocation: Record<string, unknown> }).invocation,
              requestNamespace: `admission-retry:${record.id}`,
            },
          },
        };
        const { receiverPolicy: _policy, ...incoming } = envelope;
        const childKey = evidenceDigest({
          namespace: `admission-retry:${record.id}`,
          operation: envelope.operation,
          key: attemptId,
        });
        const bindingDigest = evidenceDigest({ version: 1, envelope });
        yield* sql`INSERT INTO jones_remote_admission_fences VALUES(${record.envelope.identity.clientId},${record.idempotencyKey},${record.id},${childId},'superseded',${now})`;
        yield* sql`INSERT INTO jones_remote_admissions(id,client_id,idempotency_key,request_digest,binding_digest,envelope_json,status,created_at,updated_at)
        VALUES(${childId},${record.envelope.identity.clientId},${childKey},${evidenceDigest({ version: 1, incoming })},${bindingDigest},${canonicalJsonStrict(envelope)},'approved',${now},${now})`;
        const binding = (yield* sql<{
          binding_json: string | null;
        }>`SELECT binding_json FROM jones_remote_admission_extensions WHERE admission_id=${record.id}`)[0]
          ?.binding_json;
        yield* sql`INSERT INTO jones_remote_admission_extensions(admission_id,binding_json,attempt_id) VALUES(${childId},${binding ?? null},${attemptId})`;
        if (input.grant) {
          if (!binding) return yield* new RecoveryConflict({ reason: "grant_ineligible" });
          yield* admissions.access.createGrantInTransaction({
            admissionId: childId,
            binding: yield* Schema.decodeEffect(
              Schema.fromJsonString(RemoteAdmissionAccessBinding),
            )(binding),
            durationSeconds: input.grant.durationSeconds,
            now,
          });
        }
        yield* sql`INSERT INTO jones_remote_admission_attempts(attempt_id,root_admission_id,predecessor_admission_id,child_admission_id,session_id,command_key,fingerprint,locator,state,
        root_observation_key,grant_intent_json,proof_digest,created_at,expires_at)
        VALUES(${attemptId},${lineage?.root_admission_id ?? record.id},${record.id},${childId},${record.envelope.identity.clientId},${input.commandKey},${fp},${Crypto.randomUUID()},'prepared',${root.rootObservationKey},
        ${input.grant ? canonicalJsonStrict(input.grant) : null},${proof.evidence.evidenceDigest},${now},${DateTime.formatIso(DateTime.add(DateTime.makeUnsafe(now), { hours: 24 }))})`;
        yield* admissions.access.bumpInTransaction(
          "attempt",
          childId,
          now,
          record.envelope.identity.clientId,
        );
        return attemptView(
          (yield* sql<AttemptRow>`SELECT * FROM jones_remote_admission_attempts WHERE attempt_id=${attemptId}`)[0]!,
          rawReceiver(record),
        );
      }),
    );
  });
  const guardChildInTransaction = Effect.fn("Recovery.guardChild")(function* (
    record: RemoteAdmissionRecord,
  ) {
    const attempt =
      (yield* sql<AttemptRow>`SELECT * FROM jones_remote_admission_attempts WHERE child_admission_id=${record.id}`)[0];
    if (!attempt) return true;
    const now = yield* nowIso;
    if (attempt.expires_at <= now) {
      yield* sql`UPDATE jones_remote_admission_attempts SET state='expired',terminal_at=${now} WHERE attempt_id=${attempt.attempt_id} AND state IN ('prepared','origin_registered')`;
      return false;
    }
    if (attempt.state !== "origin_registered") return false;
    const predecessor = yield* admissions.get(attempt.predecessor_admission_id);
    if (!predecessor) return false;
    const proof = classifyLaunchEvidence(yield* reader.read(predecessor));
    if (proof.result.outcome !== "not_created") {
      yield* sql`UPDATE jones_remote_admission_attempts SET state='refused',terminal_at=${now} WHERE attempt_id=${attempt.attempt_id}`;
      return false;
    }
    return true;
  });
  return { recovered, recheck, retry, guardChildInTransaction };
};
function rawReceiver(record: RemoteAdmissionRecord): string {
  return (record.envelope.payload as { invocation: { environmentId: string } }).invocation
    .environmentId;
}
