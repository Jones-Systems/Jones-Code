import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "@t3tools/shared/nodeSqliteClient";
import { EnvironmentId } from "@t3tools/contracts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import migrate from "../persistence/Migrations/156_JonesRemoteAdmissionReview.ts";
import { makeReceiverAccess } from "../remoteAdmission/ReceiverAccess.ts";
import { makePeerProtocol } from "./PeerProtocol.ts";
const memory=Layer.effectDiscard(Effect.gen(function* () {
  const sql=yield* SqlClient.SqlClient;yield* migrate;
  yield* sql`CREATE TABLE auth_sessions(session_id TEXT,subject TEXT,method TEXT,scopes TEXT,revoked_at TEXT,expires_at TEXT)`;
  for (const id of ['a','b']) yield* sql`INSERT INTO auth_sessions VALUES(${id},'mcp-client','bearer-access-token','["orchestration:operate"]',NULL,'2099-01-01T00:00:00Z')`;
})).pipe(Layer.provideMerge(Sqlite.layer({filename:':memory:'})));
const scope=(id:string):McpInvocationScope => ({environmentId:EnvironmentId.make('receiver'),requestNamespace:`client:${id}`,issuedAt:0,
  capabilities:new Set(['orchestration']),thread:undefined,client:{sessionId:id,label:id,access:'full-access',linked:true}});
const setup=Effect.gen(function* () {const sql=yield* SqlClient.SqlClient;const access=makeReceiverAccess(sql);
  return {sql,protocol:makePeerProtocol(sql,{access,get:() => Effect.succeed(null)})};});
it.effect('pure status does not register capabilities or change owner/peer revisions',() => Effect.gen(function* () {
  const {sql,protocol}=yield* setup;
  const before=yield* sql`SELECT * FROM jones_remote_admission_review_state`;
  const result=yield* protocol.status(scope('a'),{});expect(result.grants).toEqual([]);
  expect(yield* sql`SELECT * FROM jones_remote_peer_capabilities`).toEqual([]);
  expect(yield* sql`SELECT * FROM jones_remote_admission_peer_revisions`).toEqual([]);
  expect(yield* sql`SELECT * FROM jones_remote_admission_review_state`).toEqual(before);
}).pipe(Effect.provide(memory)));
it.effect('registration is exact-session and same-generation changes conflict',() => Effect.gen(function* () {
  const {sql,protocol}=yield* setup;
  const input={coordinatorGeneration:'generation',versions:{}};
  yield* protocol.registerCapabilities(scope('a'),input);yield* protocol.registerCapabilities(scope('a'),input);
  expect((yield* sql`SELECT * FROM jones_remote_peer_capabilities`).length).toBe(1);
  expect((yield* protocol.status(scope('b'),{})).after).toBe('peer:b:0');
  const changed=yield* protocol.registerCapabilities(scope('a'),{...input,versions:{retryObservation:'jones.remote-admission-retry-observation/v1'}}).pipe(Effect.result);
  expect(changed._tag).toBe('Failure');
}).pipe(Effect.provide(memory)));
it.effect('a revoked peer cannot observe or register even with full-access invocation metadata',() => Effect.gen(function* () {
  const {sql,protocol}=yield* setup;yield* sql`UPDATE auth_sessions SET revoked_at='2026-11-16' WHERE session_id='a'`;
  expect((yield* protocol.status(scope('a'),{}).pipe(Effect.result))._tag).toBe('Failure');
  expect((yield* protocol.registerCapabilities(scope('a'),{coordinatorGeneration:'g',versions:{}}).pipe(Effect.result))._tag).toBe('Failure');
  expect(yield* sql`SELECT * FROM jones_remote_peer_capabilities`).toEqual([]);
}).pipe(Effect.provide(memory)));
it.effect('another session cannot register a prepared attempt and no state changes',() => Effect.gen(function* () {
  const {sql,protocol}=yield* setup;
  yield* sql`INSERT INTO jones_remote_admission_attempts(attempt_id,root_admission_id,predecessor_admission_id,child_admission_id,session_id,command_key,fingerprint,locator,state,root_observation_key,proof_digest,created_at,expires_at)
    VALUES('attempt','root','predecessor','child','a','key','digest','locator','prepared','root-observation','proof','2026-11-16','2099-01-01')`;
  expect((yield* protocol.registerAttempt(scope('b'),{attemptId:'attempt',observationKey:'stolen'}).pipe(Effect.result))._tag).toBe('Failure');
  expect((yield* sql<{state:string;observation_key:string|null}>`SELECT state,observation_key FROM jones_remote_admission_attempts`)[0]).toEqual({state:'prepared',observation_key:null});
}).pipe(Effect.provide(memory)));

import * as Changes from "../remoteAdmission/ReviewChanges.ts";
import * as Fiber from "effect/Fiber";
const withSignals=Changes.layer.pipe(Layer.provideMerge(memory));
it.effect('peer long-poll wakes from shared invalidation and re-reads its own durable revision',() => Effect.gen(function* () {
  const sql=yield* SqlClient.SqlClient,changes=yield* Changes.ReviewChanges;
  const protocol=makePeerProtocol(sql,{access:makeReceiverAccess(sql,changes.invalidate),get:() => Effect.succeed(null)},undefined,changes);
  const initial=yield* protocol.status(scope('a'),{});expect(initial.peerLongPoll).toBe(true);
  const waiting=yield* protocol.status(scope('a'),{after:initial.after,timeoutMs:50000}).pipe(Effect.forkChild);
  yield* sql.withTransaction(Effect.gen(function* () {
    yield* sql`INSERT INTO jones_remote_admission_peer_revisions VALUES('a',1)`;
    yield* changes.invalidate;
  }));
  const result=yield* Fiber.join(waiting);expect(result.after).toBe('peer:a:1');expect(result.sessionId).toBe('a');
}).pipe(Effect.provide(withSignals)));
it.effect('peer long-poll rechecks current authentication on wake and synthetic absence never spins',() => Effect.gen(function* () {
  const sql=yield* SqlClient.SqlClient,changes=yield* Changes.ReviewChanges;
  const ordinary=makePeerProtocol(sql,{access:makeReceiverAccess(sql),get:() => Effect.succeed(null)});
  const immediate=yield* ordinary.status(scope('a'),{after:'peer:a:0',timeoutMs:50000});expect(immediate.peerLongPoll).toBe(false);
  const protocol=makePeerProtocol(sql,{access:makeReceiverAccess(sql,changes.invalidate),get:() => Effect.succeed(null)},undefined,changes);
  const waiting=yield* protocol.status(scope('a'),{after:'peer:a:0',timeoutMs:50000}).pipe(Effect.forkChild);
  yield* sql`UPDATE auth_sessions SET revoked_at='2026-11-16' WHERE session_id='a'`;
  yield* changes.invalidate;
  expect((yield* Fiber.join(waiting).pipe(Effect.result))._tag).toBe('Failure');
}).pipe(Effect.provide(withSignals)));
it.effect('pruned notice ack replays only for retained exact session and disposition',() => Effect.gen(function* () {
  const {sql,protocol}=yield* setup;
  yield* sql`INSERT INTO jones_remote_admission_notice_tombstones(receiver_environment_id,admission_id,recovered_revision,session_id,evidence_digest,disposition)
    VALUES('receiver','admission',1,'a','digest','delivered')`;
  const input={receiverEnvironmentId:'receiver',admissionId:'admission',recoveredRevision:1,disposition:'delivered' as const};
  expect(yield* protocol.ackNotice(scope('a'),input)).toEqual({acknowledged:true});
  expect((yield* protocol.ackNotice(scope('b'),input).pipe(Effect.result))._tag).toBe('Failure');
  expect((yield* protocol.ackNotice(scope('a'),{...input,disposition:'suppressed'}).pipe(Effect.result))._tag).toBe('Failure');
}).pipe(Effect.provide(memory)));
