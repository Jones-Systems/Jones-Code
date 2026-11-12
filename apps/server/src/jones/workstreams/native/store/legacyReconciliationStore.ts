// @effect-diagnostics nodeBuiltinImport:off -- Canonical digests bind the encrypted append-only ledger.
// @effect-diagnostics preferSchemaOverJson:off -- Ledger keys are canonical JSON tuples whose original spelling is checked.
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { LegacyReconciliationDelta } from "../../../../../../../packages/contracts/src/jones/workstreamsLegacyReconciliation.ts";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as NativeSql from "./sql.ts";
export const LEGACY_RECONCILIATION_CLASS = "jones.legacy_reconciliation.v1";
export const reconciliationDigest = (value: unknown): string =>
  NodeCrypto.createHash("sha256").update(canonical(value)).digest("hex");
const decodeDelta = Schema.decodeUnknownEffect(LegacyReconciliationDelta);
export const targetAddress = (t: {
  readonly table: string;
  readonly key: Readonly<Record<string, string | number>>;
}): string => canonical([t.table, t.key]);
export interface LegacyLedger {
  readonly root: string;
  readonly deltas: readonly LegacyReconciliationDelta[];
}
export const readLegacyLedger = Effect.fn("LegacyReconciliationStore.read")(function* (
  scope: NativeSql.OwnerScope,
  runId: string,
) {
  const sql = yield* SqlClient.SqlClient,
    db = yield* NativeSql.NativeSql;
  return yield* db.transaction(
    scope,
    Effect.gen(function* () {
      const runs = yield* sql<{
        snapshot_id: string;
        snapshot_sha256: string;
        registry_instance_id: string;
      }>`SELECT snapshot_id,snapshot_sha256,registry_instance_id FROM jones_workstreams_import_runs WHERE import_run_id=${runId}`;
      const run = runs[0];
      if (!run || run.registry_instance_id !== scope.registry_instance_id)
        return yield* new NativeSql.StoreError({ reason: "binding_mismatch" });
      let root = reconciliationDigest([
        "legacy-reconciliation-v1",
        runId,
        run.snapshot_sha256,
        scope.owner_id,
      ]);
      const deltas: LegacyReconciliationDelta[] = [];
      const rows = yield* sql<{
        record_key: string;
        canonical_sha256: string;
        archive_version: number;
        envelope: string;
      }>`SELECT * FROM jones_workstreams_legacy_archive WHERE import_run_id=${runId} AND class=${LEGACY_RECONCILIATION_CLASS} ORDER BY record_key COLLATE BINARY`;
      const ids = new Set<string>();
      for (const row of rows) {
        const address = yield* Effect.try({
          try: () => JSON.parse(row.record_key) as unknown,
          catch: () => new NativeSql.StoreError({ reason: "invalid_payload" }),
        });
        if (!Array.isArray(address) || address.length !== 3 || typeof address[0] !== "string")
          return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
        if (address[0] !== scope.owner_id) continue;
        const raw = yield* db.openJson(
          {
            ...scope,
            purpose: "legacy_archive",
            record_id: canonical([runId, LEGACY_RECONCILIATION_CLASS, row.record_key]),
            revision: 1,
          },
          row.envelope,
        );
        const delta = yield* decodeDelta(raw).pipe(
          Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
        );
        const b = delta.binding;
        if (
          row.archive_version !== 1 ||
          reconciliationDigest(delta) !== row.canonical_sha256 ||
          b.registry_instance_id !== scope.registry_instance_id ||
          b.owner_id !== scope.owner_id ||
          b.import_run_id !== runId ||
          b.snapshot_id !== run.snapshot_id ||
          b.snapshot_sha256 !== run.snapshot_sha256 ||
          delta.sequence !== deltas.length + 1 ||
          delta.previous_root !== root ||
          canonical(address) !==
            canonical([scope.owner_id, String(delta.sequence).padStart(16, "0"), b.command_id]) ||
          ids.has(b.command_id) ||
          (deltas.length > 0 &&
            delta.before_registry_version !== deltas.at(-1)!.after_registry_version) ||
          delta.after_registry_version !==
            delta.before_registry_version + (delta.receipt.changed ? 1 : 0) ||
          delta.receipt.command_id !== b.command_id ||
          delta.receipt.owner_id !== b.owner_id ||
          delta.receipt.actor.principal_id !== b.principal_id ||
          delta.receipt.request_sha256 !== b.request_sha256 ||
          delta.receipt.server_generation !== b.home_generation ||
          delta.receipt.registry_version !== delta.after_registry_version ||
          delta.provenance.evidence_sha256 !== reconciliationDigest(delta.provenance.evidence)
        )
          return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
        const seen = new Set<string>();
        for (const change of delta.changes) {
          const t = change.after,
            id = targetAddress(t);
          if (
            seen.has(id) ||
            t.data.owner_id !== scope.owner_id ||
            t.data.registry_instance_id !== scope.registry_instance_id ||
            t.key.owner_id !== scope.owner_id ||
            t.key.registry_instance_id !== scope.registry_instance_id ||
            (change.before && targetAddress(change.before) !== id)
          )
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          seen.add(id);
        }
        for (const [className, hash] of [
          ["workstream_command_receipts", b.receipt_row_sha256],
          ["workstream_external_reservations", b.reservation_row_sha256],
        ]) {
          const original = yield* sql<{
            canonical_sha256: string;
          }>`SELECT canonical_sha256 FROM jones_workstreams_legacy_archive WHERE import_run_id=${runId} AND class=${className} AND record_key=${canonical([scope.owner_id, b.command_id])}`;
          if (original[0]?.canonical_sha256 !== hash)
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
        }
        ids.add(b.command_id);
        deltas.push(delta);
        root = row.canonical_sha256;
      }
      return { root, deltas } satisfies LegacyLedger;
    }),
  );
});
export const appendLegacyDelta = Effect.fn("LegacyReconciliationStore.append")(function* (
  scope: NativeSql.OwnerScope,
  input: LegacyReconciliationDelta,
) {
  const sql = yield* SqlClient.SqlClient,
    db = yield* NativeSql.NativeSql;
  const delta = yield* decodeDelta(input).pipe(
    Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
  );
  const ledger = yield* readLegacyLedger(scope, delta.binding.import_run_id);
  if (
    delta.sequence !== ledger.deltas.length + 1 ||
    delta.previous_root !== ledger.root ||
    ledger.deltas.some((d) => d.binding.command_id === delta.binding.command_id)
  )
    return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
  const recordKey = canonical([
    scope.owner_id,
    String(delta.sequence).padStart(16, "0"),
    delta.binding.command_id,
  ]);
  const envelope = yield* db.sealJson(
    {
      ...scope,
      purpose: "legacy_archive",
      record_id: canonical([delta.binding.import_run_id, LEGACY_RECONCILIATION_CLASS, recordKey]),
      revision: 1,
    },
    delta,
  );
  const root = reconciliationDigest(delta);
  yield* sql`INSERT INTO jones_workstreams_legacy_archive ${sql.insert({ import_run_id: delta.binding.import_run_id, class: LEGACY_RECONCILIATION_CLASS, record_key: recordKey, archive_version: 1, canonical_sha256: root, envelope })}`;
  return root;
});
