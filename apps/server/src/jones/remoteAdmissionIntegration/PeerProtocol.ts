import type { ReviewChanges } from "../remoteAdmission/ReviewChanges.ts";
import { recordedLaunchResult } from "./recordedResult.ts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as SqlClient from "effect/sql/SqlClient";
import * as DTO from "@t3tools/contracts/jones/remoteAdmissionReview";
import { OrchestratorMcpFailure, publicRemoteAdmission } from "@t3tools/contracts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import type * as RemoteAdmission from "../remoteAdmission/RemoteAdmission.ts";
import { attemptView, type AttemptRow } from "./RecoveryStore.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { originalReturnBinding } from "./returnBinding.ts";
import { canonicalJsonStrict } from "../remoteDelegation/shared.ts";

const unavailable = () => new OrchestratorMcpFailure({code:"capability_denied",message:"Remote request status is unavailable."});
interface NoticeRow {
  receiver_environment_id:string; admission_id:string; recovered_revision:number; session_id:string;
  root_admission_id:string; root_observation_key:string; return_binding_json:string; evidence_digest:string; result_json:string;
  state:"awaiting_support"|"unregistered"|"registered"|"delivered"|"suppressed"|"undeliverable"|"closed_unknown";
  observation_key:string|null;
}
const noticeView = (row:NoticeRow): DTO.RemoteAdmissionRecoveredNotice => ({
  receiverEnvironmentId:row.receiver_environment_id,admissionId:row.admission_id,recoveredRevision:row.recovered_revision,
  rootAdmissionId:row.root_admission_id,returnBinding:Schema.decodeUnknownSync(Schema.fromJsonString(DTO.RemoteAdmissionReturnBinding))(row.return_binding_json),
  evidenceDigest:row.evidence_digest,result:Schema.decodeUnknownSync(Schema.fromJsonString(DTO.RemoteAdmissionLaunchResult))(row.result_json),
  deliveryState:row.state === "closed_unknown" ? "undeliverable" : row.state,
});
export type ObservationSelector = {readonly clientRequestId:string}|{readonly attemptId:string;readonly observationKey:string};
export const makePeerProtocol = (sql:SqlClient.SqlClient, admissions: Pick<RemoteAdmission.RemoteAdmission["Service"],"access"|"get">,
  readKeyedResult?: (sessionId:string,clientRequestId:string) => Effect.Effect<DTO.RemoteAdmissionLaunchResult,unknown>,
  changes?:ReviewChanges["Service"]) => {
  const presence=makeJonesTablePresence(sql);
  const nowIso=DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const peer = Effect.fn("AdmissionPeer.authorize")(function* (scope:McpInvocationScope) {
    if (scope.thread || !scope.client?.linked || !scope.capabilities.has("orchestration")) return yield* unavailable();
    const rows=yield* sql<{scopes:string}>`SELECT scopes FROM auth_sessions WHERE session_id=${scope.client.sessionId}
      AND subject='mcp-client' AND method='bearer-access-token' AND revoked_at IS NULL AND expires_at>${yield* nowIso}`;
    if (!rows[0] || !Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Array(Schema.String)))(rows[0].scopes).includes("orchestration:operate")) return yield* unavailable();
    return scope.client.sessionId;
  });
  const registerCapabilities = Effect.fn("AdmissionPeer.capabilitiesRegister")(function* (scope:McpInvocationScope,input:DTO.RemoteAdmissionCapabilitiesRegisterInput) {
    return yield* sql.withTransaction(Effect.gen(function* () {
      const session=yield* peer(scope), now=yield* nowIso;
      const versions=canonicalJsonStrict(input.versions);
      const old=yield* sql<{versions_json:string}>`SELECT versions_json FROM jones_remote_peer_capabilities WHERE session_id=${session} AND coordinator_generation=${input.coordinatorGeneration}`;
      if (old[0] && old[0].versions_json !== versions) return yield* unavailable();
      yield* sql`INSERT OR IGNORE INTO jones_remote_peer_capabilities VALUES(${session},${input.coordinatorGeneration},${versions},${now})`;
      if (input.versions.recoveredResultObservation) yield* sql`UPDATE jones_remote_admission_recovered_notices SET state='unregistered' WHERE session_id=${session} AND state='awaiting_support'`;
      if (!old[0]) {
        yield* sql`INSERT INTO jones_remote_admission_peer_revisions(session_id,revision) VALUES(${session},1) ON CONFLICT(session_id) DO UPDATE SET revision=revision+1`;
        yield* admissions.access.invalidate;
      }
      return input;
    }));
  });
  const readStatus = Effect.fn("AdmissionPeer.status")(function* (scope:McpInvocationScope,input:DTO.RemoteAdmissionPeerStatusInput) {
    // Pure reads, including timeout handling. No registration, ack, owner sequence or counts.
    return yield* sql.withTransaction(Effect.gen(function* () {
      const session=yield* peer(scope), now=yield* nowIso;
      const revision=(yield* sql<{revision:number}>`SELECT revision FROM jones_remote_admission_peer_revisions WHERE session_id=${session}`)[0]?.revision ?? 0;
      if (input.after) {
        const prefix=`peer:${session}:`;
        const cursor=input.after.startsWith(prefix) ? Number(input.after.slice(prefix.length)) : NaN;
        if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor > revision) return yield* unavailable();
      }
      const grants=yield* admissions.access.listGrants(session);
      const attempts=yield* sql<AttemptRow>`SELECT * FROM jones_remote_admission_attempts WHERE session_id=${session} AND state IN ('prepared','origin_registered') AND expires_at>${now}`;
      const notices=yield* sql<NoticeRow>`SELECT * FROM jones_remote_admission_recovered_notices WHERE session_id=${session} AND state IN ('awaiting_support','unregistered','registered')`;
      return {after:`peer:${session}:${revision}`,sessionId:session,peerLongPoll:changes !== undefined,receiverEnvironmentId:scope.environmentId,serverNow:now,
        grants:yield* Effect.forEach(grants,g => admissions.access.grantView(g,now)),
        attempts:attempts.map(a => attemptView(a,scope.environmentId)),notices:notices.map(noticeView)} satisfies DTO.RemoteAdmissionPeerStatus;
    }));
  });
  const registerAttempt = Effect.fn("AdmissionPeer.attemptRegister")(function* (scope:McpInvocationScope,input:DTO.RemoteAdmissionAttemptRegisterInput) {
    return yield* sql.withTransaction(Effect.gen(function* () {
      const session=yield* peer(scope),now=yield* nowIso;
      const row=(yield* sql<AttemptRow>`SELECT * FROM jones_remote_admission_attempts WHERE attempt_id=${input.attemptId} AND session_id=${session}`)[0];
      if (!row || (row.observation_key !== null && row.observation_key !== input.observationKey)) return yield* unavailable();
      if (row.observation_key === input.observationKey) return {attempt:attemptView(row,scope.environmentId)};
      if (row.expires_at <= now || row.state !== 'prepared') return yield* unavailable();
      if (row.state === "prepared") {
        yield* sql`UPDATE jones_remote_admission_attempts SET state='origin_registered',observation_key=${input.observationKey} WHERE attempt_id=${row.attempt_id} AND state='prepared'`;
        yield* admissions.access.bumpInTransaction("attempt",row.child_admission_id,now,session);
      }
      return {attempt:attemptView({...row,state:"origin_registered",observation_key:input.observationKey},scope.environmentId)};
    }));
  });
  const registerNotice = Effect.fn("AdmissionPeer.noticeRegister")(function* (scope:McpInvocationScope,input:DTO.RemoteAdmissionNoticeRegisterInput) {
    return yield* sql.withTransaction(Effect.gen(function* () {
      const session=yield* peer(scope),now=yield* nowIso;
      const row=(yield* sql<NoticeRow>`SELECT * FROM jones_remote_admission_recovered_notices WHERE receiver_environment_id=${input.receiverEnvironmentId}
        AND admission_id=${input.admissionId} AND recovered_revision=${input.recoveredRevision} AND session_id=${session}`)[0];
      if (!row || (row.observation_key !== null && row.observation_key !== input.observationKey)) return yield* unavailable();
      if (row.observation_key === input.observationKey) return {notice:noticeView(row)};
      if (row.state !== 'unregistered') return yield* unavailable();
      if (row.state === "unregistered") {
        yield* sql`UPDATE jones_remote_admission_recovered_notices SET state='registered',observation_key=${input.observationKey}
          WHERE receiver_environment_id=${input.receiverEnvironmentId} AND admission_id=${input.admissionId} AND recovered_revision=${input.recoveredRevision} AND state='unregistered'`;
        yield* admissions.access.bumpInTransaction("recovered_result",row.admission_id,now,session);
      }
      return {notice:noticeView({...row,state:"registered",observation_key:input.observationKey})};
    }));
  });
  const ackNotice = Effect.fn("AdmissionPeer.noticeAck")(function* (scope:McpInvocationScope,input:DTO.RemoteAdmissionNoticeAckInput) {
    return yield* sql.withTransaction(Effect.gen(function* () {
      const session=yield* peer(scope),now=yield* nowIso;
      const row=(yield* sql<NoticeRow>`SELECT * FROM jones_remote_admission_recovered_notices WHERE receiver_environment_id=${input.receiverEnvironmentId}
        AND admission_id=${input.admissionId} AND recovered_revision=${input.recoveredRevision} AND session_id=${session}`)[0];
      if (!row) {
        const tombstone=yield* sql`SELECT 1 FROM jones_remote_admission_notice_tombstones WHERE receiver_environment_id=${input.receiverEnvironmentId}
          AND admission_id=${input.admissionId} AND recovered_revision=${input.recoveredRevision} AND session_id=${session} AND disposition=${input.disposition}`;
        if (!tombstone.length) return yield* unavailable();
        return {acknowledged:true as const};
      }
      if (row.state !== "registered" && row.state !== input.disposition) return yield* unavailable();
      if (row.state === "registered") {
        yield* sql`UPDATE jones_remote_admission_recovered_notices SET state=${input.disposition},acknowledged_at=${now}
          WHERE receiver_environment_id=${input.receiverEnvironmentId} AND admission_id=${input.admissionId} AND recovered_revision=${input.recoveredRevision} AND state='registered'`;
        yield* admissions.access.bumpInTransaction("recovered_result",row.admission_id,now,session);
      }
      return {acknowledged:true as const};
    }));
  });
  const selectObservation=Effect.fn('AdmissionPeer.selectObservation')(function* (session:string,input:ObservationSelector) {
    if ('attemptId' in input) {
      const attempts=yield* sql<AttemptRow>`SELECT * FROM jones_remote_admission_attempts WHERE attempt_id=${input.attemptId}
        AND session_id=${session} AND observation_key=${input.observationKey} AND state IN ('origin_registered','claimed','completed','refused','expired')`;
      if (!attempts[0]) return yield* unavailable();
      const record=yield* admissions.get(attempts[0].child_admission_id);
      if (!record || record.envelope.identity.clientId !== session) return yield* unavailable();
      return {record,key:input.observationKey,kind:'retry' as const};
    }
    const rows=yield* sql<{id:string}>`SELECT id FROM jones_remote_admissions WHERE client_id=${session}
      AND json_extract(envelope_json,'$.payload.params.clientRequestId')=${input.clientRequestId} AND json_extract(envelope_json,'$.operation')='t3_thread_launch'
      AND NOT EXISTS(SELECT 1 FROM jones_remote_admission_attempts a WHERE a.child_admission_id=jones_remote_admissions.id)`;
    const record=rows[0] ? yield* admissions.get(rows[0].id) : null;
    const root=record ? originalReturnBinding(record) : null;
    return {record,key:root?.rootObservationKey ?? null,kind:'launch' as const};
  });
  const observe = Effect.fn("AdmissionPeer.observe")(function* (scope:McpInvocationScope,input:ObservationSelector) {
    return yield* sql.withTransaction(Effect.gen(function* () {
      const session=yield* peer(scope), selected=yield* selectObservation(session,input);
      return {admission:selected.record ? publicRemoteAdmission(selected.record) : null,
        result:selected.record ? yield* recordedLaunchResult(sql,selected.record.id)
          : 'clientRequestId' in input && readKeyedResult ? yield* readKeyedResult(session,input.clientRequestId) : null} satisfies DTO.RemoteAdmissionObservation;
    }));
  });
  const ack = Effect.fn('AdmissionPeer.ack')(function* (scope:McpInvocationScope,input:ObservationSelector & {readonly disposition:'delivered'|'suppressed'}) {
    return yield* sql.withTransaction(Effect.gen(function* () {
      const session=yield* peer(scope),now=yield* nowIso;
      if (!(yield* presence('jones_remote_admission_observation_acks'))) return yield* unavailable();
      if ('clientRequestId' in input) {
        const retained=yield* sql<{disposition:string}>`SELECT disposition FROM jones_remote_admission_observation_acks WHERE session_id=${session}
          AND kind='launch' AND observation_key=${`origin:${scope.environmentId}:${input.clientRequestId}`}`;
        if (retained[0]) {
          if (retained[0].disposition !== input.disposition) return yield* unavailable();
          return {acknowledged:true as const};
        }
      }
      const selected=yield* selectObservation(session,input);
      let key=selected.key,admissionId=selected.record?.id ?? null;
      if (!admissionId && 'clientRequestId' in input && readKeyedResult) {
        const result=yield* readKeyedResult(session,input.clientRequestId);
        if (result.outcome === 'unknown') return yield* unavailable();
        key=`origin:${scope.environmentId}:${input.clientRequestId}`;

      }
      if (!key) return yield* unavailable();
      const old=yield* sql<{disposition:string;admission_id:string|null}>`SELECT disposition,admission_id FROM jones_remote_admission_observation_acks
        WHERE session_id=${session} AND observation_key=${key} AND kind=${selected.kind}`;
      if (old[0] && (old[0].disposition !== input.disposition || old[0].admission_id !== admissionId)) return yield* unavailable();
      yield* sql`INSERT OR IGNORE INTO jones_remote_admission_observation_acks(session_id,observation_key,kind,admission_id,disposition,acknowledged_at)
        VALUES(${session},${key},${selected.kind},${admissionId},${input.disposition},${now})`;
      if (admissionId) {
        const previous=yield* sql<{session_id:string;disposition:string}>`SELECT session_id,disposition FROM jones_remote_admission_result_acks WHERE admission_id=${admissionId}`;
        if (previous[0] && (previous[0].session_id !== session || previous[0].disposition !== input.disposition)) return yield* unavailable();
        yield* sql`INSERT OR IGNORE INTO jones_remote_admission_result_acks(admission_id,session_id,disposition,acknowledged_at)
          VALUES(${admissionId},${session},${input.disposition},${now})`;
        if (!old[0]) yield* admissions.access.bumpInTransaction('admission',admissionId,now,session);
      }
      return {acknowledged:true as const};
    }));
  });
  const status = Effect.fn("AdmissionPeer.longPoll")(function* (scope:McpInvocationScope,input:DTO.RemoteAdmissionPeerStatusInput) {
    const read=readStatus(scope,input);
    return yield* (changes ? changes.waitFor({read,isReady:response => !input.after || input.after !== response.after,
      timeoutMs:input.timeoutMs ?? 0}) : read);
  });
  return {peer,registerCapabilities,status,registerAttempt,registerNotice,ackNotice,observe,ack};
};
