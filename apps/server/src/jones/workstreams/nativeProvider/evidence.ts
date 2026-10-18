import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import {
  NativeProviderEvidenceReadError,
  type NativeProviderEvidence as NativeProviderEvidencePort,
} from "./service.ts";

import {
  matchesSettlementProof,
  NativeProviderMetadataSnapshot,
  readNativeProviderCommandMetadata,
  sameNativeProviderEvent,
  sameNativeProviderReceipt,
  SettlementProofJson,
} from "./settlementProof.ts";

export { NativeProviderEvidenceReadError } from "./service.ts";

export class NativeProviderEvidence extends Context.Service<
  NativeProviderEvidence,
  NativeProviderEvidencePort
>()("t3/jones/workstreams/nativeProvider/evidence/NativeProviderEvidence") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const readSnapshotByCommandId: NativeProviderEvidencePort["readSnapshotByCommandId"] = (
    commandId,
    _threadId,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const rows = yield* readNativeProviderCommandMetadata(commandId).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          );
          const snapshot = yield* Schema.decodeUnknownEffect(NativeProviderMetadataSnapshot)(
            rows,
          ).pipe(
            Effect.mapError(
              () => new NativeProviderEvidenceReadError({ reason: "invalid_snapshot" }),
            ),
          );
          if (snapshot.receipts.length > 1)
            return yield* new NativeProviderEvidenceReadError({ reason: "invalid_snapshot" });
          const receipt = snapshot.receipts[0];
          if (receipt === undefined) return { receipt: Option.none(), events: snapshot.events };
          const proofRows = yield* sql<{
            thread_id: string;
            native_action: "settle" | "unsettle";
            proof_json: string | null;
          }>`
            SELECT thread_id, native_action, proof_json FROM jones_workstreams_settlement_proofs
            WHERE native_command_id = ${commandId}`;
          const row = proofRows[0];
          if (row === undefined || row.proof_json === null)
            return { receipt: Option.some(receipt), events: snapshot.events };
          const proof = Schema.decodeUnknownOption(SettlementProofJson)(row.proof_json);
          if (
            Option.isNone(proof) ||
            !matchesSettlementProof(proof.value, commandId, row.thread_id, row.native_action) ||
            !sameNativeProviderReceipt(proof.value.receipt, receipt) ||
            snapshot.events.some(
              (event) =>
                !proof.value.events.some((stored) => sameNativeProviderEvent(event, stored)),
            )
          )
            return yield* new NativeProviderEvidenceReadError({ reason: "invalid_snapshot" });
          return { receipt: Option.some(receipt), events: proof.value.events };
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          Schema.is(NativeProviderEvidenceReadError)(error)
            ? error
            : new NativeProviderEvidenceReadError({ reason: "reader_unavailable" }),
        ),
      );
  return NativeProviderEvidence.of({ readSnapshotByCommandId });
});

export const makeNativeProviderEvidenceLayer = (reader?: NativeProviderEvidencePort) =>
  reader === undefined
    ? Layer.effect(NativeProviderEvidence, make)
    : Layer.succeed(NativeProviderEvidence, reader);
