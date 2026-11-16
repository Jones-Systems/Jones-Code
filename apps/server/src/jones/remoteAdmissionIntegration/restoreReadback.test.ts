import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "@t3tools/shared/nodeSqliteClient";
import migrate from "../persistence/Migrations/156_JonesRemoteAdmissionReview.ts";
import { makeReceiverAccess } from "../remoteAdmission/ReceiverAccess.ts";
import { lookupRestoreCommand } from "./restoreReadback.ts";
const memory=Layer.effectDiscard(migrate).pipe(Layer.provideMerge(Sqlite.layer({filename:':memory:'})));
const setup=Effect.gen(function* () {const sql=yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_remote_admission_grants(grant_id,admission_id,binding_json,session_id,state,generation,expires_at,created_at,updated_at,revoked_at)
    VALUES('successor','restore:successor','{}','session','revoked',3,'2026-11-16','2026-11-15','2026-11-17','2026-11-17')`;
  yield* sql`INSERT INTO jones_remote_admission_grant_restores VALUES('predecessor',2,'successor','original-command','fingerprint','2026-11-15')`;
  return {sql,access:makeReceiverAccess(sql)};});
it.effect('lost reply lookup returns updated/revoked successor without changing generation or expiry',() => Effect.gen(function* () {
  const {sql,access}=yield* setup;const before=yield* sql`SELECT * FROM jones_remote_admission_grants`;
  const read=yield* lookupRestoreCommand(sql,access,'predecessor','original-command');
  expect(read).toMatchObject({grant_id:'successor',state:'revoked',generation:3,expires_at:'2026-11-16'});
  expect(yield* sql`SELECT * FROM jones_remote_admission_grants`).toEqual(before);
  expect((yield* sql<{revision:number}>`SELECT revision FROM jones_remote_admission_review_state`)[0]?.revision).toBe(0);
}).pipe(Effect.provide(memory)));
it.effect('a different command cannot discover the successor and missing lineage is unavailable',() => Effect.gen(function* () {
  const {sql,access}=yield* setup;expect(yield* lookupRestoreCommand(sql,access,'predecessor','new-command')).toBeNull();
  yield* sql`DELETE FROM jones_remote_admission_grant_restores WHERE predecessor_grant_id='predecessor'`;
  expect(yield* lookupRestoreCommand(sql,access,'predecessor','original-command')).toBeNull();
  expect((yield* sql`SELECT * FROM jones_remote_admission_grants`).length).toBe(1);
}).pipe(Effect.provide(memory)));
