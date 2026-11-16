import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { ProviderInstanceId, type RemoteAdmissionEnvelope } from "@t3tools/contracts";
import admissionMigration from "../persistence/Migrations/116_JonesRemoteAdmission.ts";
import reviewMigration from "../persistence/Migrations/156_JonesRemoteAdmissionReview.ts";
import * as Admission from "./RemoteAdmission.ts";
import type { AccessBinding } from "./ReceiverAccess.ts";

const memory = Layer.effectDiscard(Effect.gen(function* () { yield* admissionMigration; yield* reviewMigration; }))
  .pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));
const testLayer = Admission.layerWithEvaluator(() => Effect.succeed({ verdict: "pass", category: "none", reason: "ordinary" }))
  .pipe(Layer.provideMerge(memory));
const binding: AccessBinding = { receiverEnvironmentId: "receiver", originEnvironmentId: "origin", originThreadId: "origin-thread",
  destinationThreadId: "destination", sessionId: "session", operation: "t3_thread_send", runtimeMode: "full-access", interactionMode: "default" };
const policy = { enabled: false, revision: "policy", modelSelection: null };
const envelope: RemoteAdmissionEnvelope = { identity: { clientId: "session", linkId: "link" }, operation: "t3_thread_send", payload: { text: "synthetic" },
  target: { projectId: "project", threadId: "destination", runtimeMode: "full-access", interactionMode: "default" }, receiverPolicy: policy };
const now = "2026-11-16T12:00:00.000Z";
const owner = { kind: "human-owner", subject: "owner" } as const;
const setup = Effect.gen(function* () {
  const service = yield* Admission.RemoteAdmission;
  const sql = yield* SqlClient.SqlClient;
  const grant = yield* sql.withTransaction(service.access.createGrantInTransaction({admissionId:"establish",binding,durationSeconds:86400,now:"2099-01-01T00:00:00.000Z"}));
  return {service,sql,grant};
});
const request = (id: string) => ({ idempotencyKey: id, envelope, accessBinding: binding });

it.effect("policy-off managed pass after revoke is held; once owner approval grants only its permit", () => Effect.gen(function* () {
  const {service,grant} = yield* setup;
  yield* service.access.mutateGrant({ grantId:grant.grant_id,expectedGeneration:1,action:"revoke",now });
  const held = yield* service.request(request("blocked"));
  expect(held.status).toBe("held"); expect(held.category).toBe("out_of_scope");
  const approved = yield* service.resolve({...held,decision:"approve",principal:owner});
  expect((yield* service.claimDispatch({...approved,commandId:"command",currentPolicy:policy})).claimed).toBe(true);
  expect((yield* service.access.listGrants()).length).toBe(1);
  expect((yield* service.request(request("next"))).status).toBe("held");
}).pipe(Effect.provide(testLayer)));

it.effect("generation changes invalidate queued leases, but duplicate claimed rows never demote", () => Effect.gen(function* () {
  const {service,grant} = yield* setup;
  const queued = yield* service.request(request("queued"));
  expect(queued.status).toBe("passed");
  yield* service.access.mutateGrant({grantId:grant.grant_id,expectedGeneration:1,action:"update",durationSeconds:3600,now:"2099-01-01T01:00:00.000Z"});
  const deniedClaim = yield* service.claimDispatch({...queued,commandId:"queued-command",currentPolicy:policy});
  expect(deniedClaim.claimed).toBe(false); expect(deniedClaim.record.id).toBe(queued.id); expect(deniedClaim.record.status).toBe("held");
  const claimed = yield* service.request(request("claimed"));
  expect((yield* service.claimDispatch({...claimed,commandId:"claimed-command",currentPolicy:policy})).claimed).toBe(true);
  yield* service.access.mutateGrant({grantId:grant.grant_id,expectedGeneration:2,action:"revoke",now});
  const duplicate = yield* service.claimDispatch({...claimed,commandId:"claimed-command",currentPolicy:{...policy,revision:"changed"},
    checkCurrentAuthority: () => Effect.die("duplicate must not invoke current-authority callback")});
  expect(duplicate.claimed).toBe(false); expect(duplicate.record.status).toBe("dispatching");
}).pipe(Effect.provide(testLayer)));

it.effect("revoke invalidates an owner permit and never migrates it to a restored lease", () => Effect.gen(function* () {
  const {service,grant} = yield* setup;
  const held = yield* service.request({...request("attachment"),envelope:{...envelope,operation:"t3_thread_send_attachments"}});
  const approved = yield* service.resolve({...held,decision:"approve",principal:owner});
  yield* service.access.mutateGrant({grantId:grant.grant_id,expectedGeneration:1,action:"revoke",now});
  const result = yield* service.claimDispatch({...approved,commandId:"attachment-command",currentPolicy:policy});
  expect(result.claimed).toBe(false); expect(result.record.status).toBe("held");
}).pipe(Effect.provide(testLayer)));

it.effect("restore replay returns successor current state and does not extend or revive it", () => Effect.gen(function* () {
  const {service,grant} = yield* setup;
  yield* service.access.mutateGrant({grantId:grant.grant_id,expectedGeneration:1,action:"revoke",now});
  const held = yield* service.request(request("waiting"));
  const input = {predecessorId:grant.grant_id,expectedGeneration:2,commandKey:"restore",durationSeconds:3600,binding,now,
    authorize: () => Effect.succeed(true)};
  const restored = yield* service.access.restore(input);
  yield* service.access.mutateGrant({grantId:restored.grant_id,expectedGeneration:1,action:"revoke",now});
  const replay = yield* service.access.restore({...input,now:"2099-02-01T00:00:00.000Z"});
  expect(replay.state).toBe("revoked"); expect(replay.expires_at).toBe(restored.expires_at);
  expect((yield* service.get(held.id))?.status).toBe("held");
  expect((yield* service.access.restore({...input,commandKey:"other"}).pipe(Effect.flip()))._tag).toBe("ReceiverAccessConflict");
}).pipe(Effect.provide(testLayer)));

it.effect("retention preserves restored grant ancestry while the successor is live or recently terminal", () => Effect.gen(function* () {
  const {service,sql,grant} = yield* setup;
  yield* service.access.mutateGrant({grantId:grant.grant_id,expectedGeneration:1,action:"revoke",now});
  const restored = yield* service.access.restore({predecessorId:grant.grant_id,expectedGeneration:2,
    commandKey:"retained-restore",durationSeconds:86400,binding,now:"2099-01-01T00:00:00.000Z",
    authorize: () => Effect.succeed(true)});
  yield* service.retention.prune("2099-01-01T12:00:00.000Z");
  expect((yield* service.access.getGrant(grant.grant_id))?.state).toBe("revoked");
  expect((yield* service.access.getGrant(restored.grant_id))?.state).toBe("active");
  expect((yield* sql`SELECT * FROM jones_remote_admission_grant_restores`).length).toBe(1);
  yield* service.access.mutateGrant({grantId:restored.grant_id,expectedGeneration:1,action:"revoke",now:"2099-01-02T00:00:00.000Z"});
  yield* service.retention.prune("2099-01-03T00:00:00.000Z");
  expect((yield* service.access.getGrant(grant.grant_id))?.state).toBe("revoked");
  yield* service.retention.prune("2100-01-01T00:00:00.000Z");
  expect(yield* service.access.getGrant(grant.grant_id)).toBeNull();
  expect(yield* service.access.getGrant(restored.grant_id)).toBeNull();
  expect((yield* sql`SELECT * FROM jones_remote_admission_grant_restores`).length).toBe(0);
}).pipe(Effect.provide(testLayer)));

it.effect("exact creation after pending revoke establishes restriction without activating lease", () => Effect.gen(function* () {
  const service = yield* Admission.RemoteAdmission;
  const sql = yield* SqlClient.SqlClient;
  const pending = yield* sql.withTransaction(service.access.createGrantInTransaction({admissionId:"launch",binding:{...binding,destinationThreadId:null},durationSeconds:3600,now}));
  yield* service.access.mutateGrant({grantId:pending.grant_id,expectedGeneration:1,action:"revoke",now});
  const recovered = yield* sql.withTransaction(service.access.activateInTransaction({admissionId:"launch",expectedGeneration:1,destinationThreadId:"destination",now}));
  expect(recovered?.state).toBe("revoked");
  expect((yield* service.request(request("after-recovery"))).status).toBe("held");
}).pipe(Effect.provide(testLayer)));

it.effect("folding 101 details preserves held links and repeats remain extra arrivals", () => Effect.gen(function* () {
  const {service,sql,grant} = yield* setup;
  yield* service.access.mutateGrant({grantId:grant.grant_id,expectedGeneration:1,action:"revoke",now});
  const first = yield* service.request(request("audit-0"));
  for (let i=1;i<101;i++) yield* service.request(request(`audit-${i}`));
  yield* service.request(request("audit-0"));
  const details = (yield* sql<{n:number}>`SELECT COUNT(*) n FROM jones_remote_admission_blocked_events`)[0]!.n;
  const marks = (yield* sql<{n:number}>`SELECT COUNT(*) n FROM jones_remote_admission_blocked_marks`)[0]!.n;
  const totals = (yield* sql<{n:number}>`SELECT SUM(logical_events) n FROM jones_remote_admission_blocked_summaries`)[0]!.n;
  expect(details).toBe(100); expect(marks).toBe(101); expect(totals).toBe(1);
  expect((yield* service.get(first.id))?.status).toBe("held");
}).pipe(Effect.provide(testLayer)));

it.effect("retention preserves restriction, unresolved notices and predecessor fences", () => Effect.gen(function* () {
  const {service,sql,grant} = yield* setup;
  yield* service.access.mutateGrant({grantId:grant.grant_id,expectedGeneration:1,action:"revoke",now});
  yield* sql`INSERT INTO jones_remote_admission_fences VALUES('session','old-key','old','new','superseded',${now})`;
  yield* sql`INSERT INTO jones_remote_admission_recovered_notices(receiver_environment_id,admission_id,recovered_revision,
    session_id,root_admission_id,root_observation_key,return_binding_json,evidence_digest,result_json,state,created_at)
    VALUES('receiver','unknown',1,'session','root','root-observation','{}','digest','{}','unregistered',${now})`;
  yield* sql`INSERT INTO jones_remote_origin_return_anchors(root_observation_key,peer_environment_id,session_id,return_binding_json)
    VALUES('root-observation','receiver','session','{}')`;
  yield* service.retention.prune("2100-01-01T00:00:00.000Z");
  expect((yield* sql`SELECT * FROM jones_remote_admission_recovered_notices`).length).toBe(1);
  expect((yield* sql`SELECT * FROM jones_remote_origin_return_anchors`).length).toBe(1);
  expect(yield* service.access.getGrant(grant.grant_id)).toBeNull();
  expect((yield* sql`SELECT * FROM jones_remote_managed_scopes`).length).toBe(1);
  expect((yield* sql`SELECT * FROM jones_remote_admission_fences`).length).toBe(1);
  expect((yield* service.request(request("after-prune"))).status).toBe("held");
}).pipe(Effect.provide(testLayer)));

it.effect("duration validation and immutable binding reject widening an existing grant", () => Effect.gen(function* () {
  const {service,sql,grant} = yield* setup;
  expect((yield* service.access.mutateGrant({grantId:grant.grant_id,expectedGeneration:1,action:"update",durationSeconds:42,now}).pipe(Effect.flip()))._tag)
    .toBe("ReceiverAccessUnavailable");
  const changed = yield* sql`UPDATE jones_remote_admission_grants SET session_id='different' WHERE grant_id=${grant.grant_id}`.pipe(Effect.exit);
  expect(changed._tag).toBe("Failure");
  expect((yield* service.access.getGrant(grant.grant_id))?.generation).toBe(1);
}).pipe(Effect.provide(testLayer)));

it.effect("missing origin and re-pair are held while a same-session reconnect keeps its exact lease", () => Effect.gen(function* () {
  const {service} = yield* setup;
  expect((yield* service.request(request("reconnect"))).status).toBe("passed");
  const repaired = yield* service.request({...request("re-pair"),envelope:{...envelope,identity:{clientId:"replacement",linkId:"link"}},accessBinding:{...binding,sessionId:"replacement"}});
  expect(repaired.status).toBe("held"); expect(repaired.reason).toContain("session_not_bound");
  const missing = yield* service.request({idempotencyKey:"missing",envelope});
  expect(missing.status).toBe("held"); expect(missing.reason).toContain("identity_unresolved");
}).pipe(Effect.provide(testLayer)));


it.effect("malicious under an active lease remains ordinary owner-held and is audited without blocked counts", () => Effect.gen(function* () {
  const {service,sql} = yield* setup;
  const held = yield* service.request({...request("malicious"),envelope:{...envelope,receiverPolicy:{...policy,enabled:true,
    modelSelection:{instanceId:ProviderInstanceId.make("synthetic"),model:"synthetic"}}}});
  expect(held.status).toBe("held"); expect(held.category).toBe("malicious");
  expect((yield* sql`SELECT * FROM jones_remote_admission_blocked_marks WHERE admission_id=${held.id}`).length).toBe(0);
  expect((yield* sql`SELECT * FROM jones_remote_admission_blocked_events WHERE admission_id=${held.id}`).length).toBe(1);
  const approved = yield* service.resolve({...held,decision:"approve",principal:owner});
  expect((yield* service.claimDispatch({...approved,commandId:"malicious-owner-command",currentPolicy:held.envelope.receiverPolicy})).claimed).toBe(true);
}).pipe(Effect.provide(Admission.layerWithEvaluator(() => Effect.succeed({verdict:"hold",category:"malicious",reason:"Owner review required."}))
  .pipe(Layer.provideMerge(memory)))));


it.effect("notice detail pruning retains exact-session dedup and ordinary acknowledgement bindings", () => Effect.gen(function* () {
  const service=yield* Admission.RemoteAdmission;
  const sql=yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_remote_admission_recovered_notices(receiver_environment_id,admission_id,recovered_revision,
    session_id,root_admission_id,root_observation_key,return_binding_json,evidence_digest,result_json,state,created_at,acknowledged_at)
    VALUES('receiver','recovered',1,'bound-session','root','root-key','{}','digest','{}','delivered',${now},${now})`;
  yield* sql`INSERT INTO jones_remote_admission_observation_acks VALUES('bound-session','root-key','launch',NULL,'delivered',${now})`;
  yield* service.retention.prune("2100-01-01T00:00:00.000Z");
  expect((yield* sql`SELECT * FROM jones_remote_admission_recovered_notices`).length).toBe(0);
  const tombstones=yield* sql<{session_id:string;disposition:string}>`SELECT * FROM jones_remote_admission_notice_tombstones`;
  expect(tombstones[0]?.session_id).toBe("bound-session");expect(tombstones[0]?.disposition).toBe("delivered");
  expect((yield* sql`SELECT * FROM jones_remote_admission_observation_acks WHERE session_id='bound-session'`).length).toBe(1);
}).pipe(Effect.provide(testLayer)));

it.effect("conflicting notice tombstone preserves detail rather than losing exact-session authority", () => Effect.gen(function* () {
  const service=yield* Admission.RemoteAdmission;
  const sql=yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_remote_admission_recovered_notices(receiver_environment_id,admission_id,recovered_revision,
    session_id,root_admission_id,root_observation_key,return_binding_json,evidence_digest,result_json,state,created_at,acknowledged_at)
    VALUES('receiver','conflicting',1,'original-session','root','root-key','{}','digest','{}','delivered',${now},${now})`;
  yield* sql`INSERT INTO jones_remote_admission_notice_tombstones VALUES('receiver','conflicting',1,'other-session','digest','delivered')`;
  yield* service.retention.prune("2100-01-01T00:00:00.000Z");
  const notices=yield* sql<{session_id:string}>`SELECT * FROM jones_remote_admission_recovered_notices WHERE admission_id='conflicting'`;
  expect(notices[0]?.session_id).toBe("original-session");
}).pipe(Effect.provide(testLayer)));
