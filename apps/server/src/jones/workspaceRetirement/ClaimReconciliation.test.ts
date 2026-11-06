import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import { ClaimAbsence, reconcile } from "./ClaimReconciliation.ts";
import { Receipts, layer } from "./Receipts.ts";

it.effect("startup releases only owner-verified absence with the exact durable holder; unknown and missing queued runs retain duty", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Receipts.pipe(Effect.provide(layer));
    for (const [id,kind] of [["absent","session"],["unknown","terminal"],["changed","setup"],["queued-turn:missing","queued-turn"]] as const)
      yield* sql`INSERT INTO jones_workspace_duty_claims VALUES(${id},'/fixture',NULL,${kind},'owner','"original-holder"','2026-11-06T00:00:00Z',NULL)`;
    const result = yield* reconcile(sql,store).pipe(Effect.provideService(ClaimAbsence, {
      observe: claim => Effect.gen(function* () {
        if (claim.claim_id === "changed")
          yield* sql`UPDATE jones_workspace_duty_claims SET holder_json='"replacement-holder"' WHERE claim_id='changed'`;
        return {holder:claim.holder_json,status:claim.claim_id === "unknown" ? "unknown" as const : "verified_absent" as const};
      }).pipe(Effect.orDie),
    }));
    assert.deepEqual(result,{released:1,retained:3,status:"observed"});
    assert.deepEqual((yield* store.claims("/fixture")).map(claim=>claim.claim_id).sort(),["changed","queued-turn:missing","unknown"]);
    yield* store.releaseClaim("unknown");
    assert.deepEqual((yield* store.claims("/fixture")).map(claim=>claim.claim_id).sort(),["changed","queued-turn:missing"]);
  }).pipe(Effect.provide(Sqlite.layerMemory)),
);
