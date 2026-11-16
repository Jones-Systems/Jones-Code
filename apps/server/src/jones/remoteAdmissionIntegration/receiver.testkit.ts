import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerSettings from "../../serverSettings.ts";
import * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import { ReceiverRecoveryEvidence, RecoveryUnavailable } from "./RecoveryStore.ts";

// Unit intake tests retain synthetic targets. Production authority is covered separately
// through RuntimeAdapters; this fixture supplies no live state or credential material.
export const syntheticReceiverAuth=Effect.gen(function* () {
  const sql=yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE auth_sessions(session_id TEXT PRIMARY KEY,subject TEXT,method TEXT,scopes TEXT,revoked_at TEXT,expires_at TEXT)`;
  yield* sql`INSERT INTO auth_sessions VALUES('remote','mcp-client','bearer-access-token','["orchestration:operate"]',NULL,'2099-01-01T00:00:00.000Z')`;
});
export const syntheticReceiverLayers=Layer.merge(
  ServerSettings.ServerSettingsService.layerTest(),
  Layer.succeed(ReceiverRecoveryEvidence,{authorize:() => Effect.succeed(true),root:() => Effect.succeed(null),
    read:() => Effect.fail(new RecoveryUnavailable({}))}),
);
/** Await the separate durable reactor, preserving the original final-state assertions. */
export const awaitDispatched=Effect.fn('receiver.testkit.awaitDispatched')(function* (id:string) {
  const admissions=yield* Admission.RemoteAdmission;
  for (let turns=0;turns<10000;turns++) {
    const record=yield* admissions.get(id);
    if (record?.status === 'dispatched') return record;
    if (record?.status === 'denied' || record?.status === 'cancelled') return yield* Effect.die(`Admission ${id} ended as ${record.status}`);
    yield* Effect.yieldNow;
  }
  return yield* Effect.die(`Admission ${id} did not dispatch within the bounded scheduler drain`);
});
