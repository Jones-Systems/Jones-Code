import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ConstraintError, SqlError } from "effect/sql/SqlError";

import {
  matchesSettlementProof,
  readNativeProviderCommandMetadata,
  NativeProviderMetadataSnapshot,
  SettlementProofJson,
} from "../../workstreams/nativeProvider/settlementProof.ts";

const Association = Schema.fromJsonString(
  Schema.Struct({
    identity: Schema.Struct({ native_id: Schema.String }),
    native_action: Schema.Literals(["settle", "unsettle"]),
  }),
);

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workstreams_settlement_proofs (
    native_command_id TEXT PRIMARY KEY NOT NULL,
    thread_id TEXT NOT NULL,
    native_action TEXT NOT NULL CHECK(native_action IN ('settle', 'unsettle')),
    proof_json TEXT CHECK(proof_json IS NULL OR json_valid(proof_json))
  )`;
  yield* sql`CREATE TRIGGER jones_workstreams_settlement_proofs_immutable
    BEFORE UPDATE ON jones_workstreams_settlement_proofs
    WHEN OLD.native_command_id IS NOT NEW.native_command_id OR OLD.thread_id IS NOT NEW.thread_id
      OR OLD.native_action IS NOT NEW.native_action OR OLD.proof_json IS NOT NULL OR NEW.proof_json IS NULL
    BEGIN SELECT RAISE(ABORT, 'native settlement proof is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_settlement_proofs_no_delete
    BEFORE DELETE ON jones_workstreams_settlement_proofs
    BEGIN SELECT RAISE(ABORT, 'native settlement proofs are permanent'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_settlement_proofs_no_replace
    BEFORE INSERT ON jones_workstreams_settlement_proofs
    WHEN EXISTS (SELECT 1 FROM jones_workstreams_settlement_proofs WHERE native_command_id = NEW.native_command_id)
    BEGIN SELECT RAISE(ABORT, 'native settlement proof ownership is permanent'); END`;

  // Startup runs Jones migrations before compaction. Already-pruned batches cannot be reconstructed.
  const attempts = yield* sql<{
    native_command_id: string;
    request_json: string;
    dispatch_started_at: string | null;
  }>`
    SELECT native_command_id, request_json, dispatch_started_at FROM workstreams_native_attempts`;
  for (const attempt of attempts) {
    const association = Schema.decodeUnknownOption(Association)(attempt.request_json);
    if (Option.isNone(association)) continue;
    const { identity, native_action } = association.value;
    let proofJson: string | null = null;
    if (attempt.dispatch_started_at !== null) {
      const snapshot = Schema.decodeUnknownOption(NativeProviderMetadataSnapshot)(
        yield* readNativeProviderCommandMetadata(attempt.native_command_id),
      );
      if (Option.isSome(snapshot) && snapshot.value.receipts.length === 1) {
        const proof = { receipt: snapshot.value.receipts[0]!, events: snapshot.value.events };
        if (
          matchesSettlementProof(
            proof,
            attempt.native_command_id,
            identity.native_id,
            native_action,
          )
        )
          proofJson = yield* Schema.encodeEffect(SettlementProofJson)(proof).pipe(
            Effect.mapError(
              (cause) =>
                new SqlError({
                  reason: new ConstraintError({
                    cause,
                    operation: "Jones migration 107 proof encoding",
                    message: "Jones migration 107 could not encode verified settlement metadata.",
                  }),
                }),
            ),
          );
      }
    }
    yield* sql`INSERT INTO jones_workstreams_settlement_proofs (native_command_id, thread_id, native_action, proof_json)
      VALUES (${attempt.native_command_id}, ${identity.native_id}, ${native_action}, ${proofJson})`;
  }
});
