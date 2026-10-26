// @effect-diagnostics nodeBuiltinImport:off -- SHA-256 verifies retained snapshot evidence.
// @effect-diagnostics preferSchemaOverJson:off -- Compare parsed evidence without changing retained payload spelling.
import * as NodeCrypto from "node:crypto";
import {
  WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
  WorkstreamsLegacyReceiptIdentity,
  WorkstreamsLegacyReceiptProof,
} from "@t3tools/contracts/jones/workstreamsLegacyReceipts";
import { WorkstreamsSnapshotRow } from "@t3tools/contracts/jones/workstreamsSnapshot";
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import { WorkstreamReceipt } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as NativeSql from "./sql.ts";

type Failure = SqlError | NativeSql.StoreError | NativeSql.CryptoError;
export interface ImportManifest {
  readonly registry: { readonly registry_instance_id: string };
  readonly owners: readonly { readonly owner_id: string }[];
  readonly [key: string]: unknown;
}
const Manifest = Schema.Struct({
  registry: Schema.Struct({ registry_instance_id: Schema.String }),
  owners: Schema.Array(Schema.Struct({ owner_id: Schema.String })),
});
const decodeManifest = Schema.decodeUnknownEffect(Manifest);
const decodeEvidence = Schema.decodeUnknownEffect(
  Schema.Struct({ source: WorkstreamsSnapshotRow }),
);
const decodeIdentity = Schema.decodeUnknownEffect(
  Schema.fromJsonString(WorkstreamsLegacyReceiptIdentity),
);
const decodeOpenEvidence = Schema.decodeUnknownEffect(Schema.Struct({ receipt: Schema.Unknown }));
const decodeProof = Schema.decodeUnknownEffect(WorkstreamsLegacyReceiptProof);
const sha256 = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const decodeReceipt = Schema.decodeUnknownEffect(WorkstreamReceipt);
export class ArchiveStore extends Context.Service<
  ArchiveStore,
  {
    readonly contains: (
      scope: NativeSql.OwnerScope,
      commandId: string,
    ) => Effect.Effect<boolean, Failure>;
    readonly evidence: (
      scope: NativeSql.OwnerScope,
      commandId: string,
    ) => Effect.Effect<WorkstreamsLegacyReceiptProof | null, Failure>;
    readonly receipt: (
      scope: NativeSql.OwnerScope,
      commandId: string,
    ) => Effect.Effect<WorkstreamReceipt | null, Failure>;
    readonly coverage: (
      scope: NativeSql.OwnerScope,
    ) => Effect.Effect<"complete" | "import_incomplete" | "archive_class_missing", Failure>;
    readonly startImport: (
      scope: NativeSql.OwnerScope,
      run: {
        import_run_id: string;
        snapshot_id: string;
        snapshot_sha256: string;
        producer: string;
        contract_version: string;
        manifest: ImportManifest;
      },
    ) => Effect.Effect<void, Failure>;
    readonly archive: (
      scope: NativeSql.OwnerScope,
      row: {
        import_run_id: string;
        class: string;
        record_key: string;
        canonical_sha256: string;
        value: unknown;
      },
    ) => Effect.Effect<void, Failure>;
    readonly readArchive: (
      scope: NativeSql.OwnerScope,
      runId: string,
      className: string,
      recordKey: string,
    ) => Effect.Effect<unknown | null, Failure>;
    readonly addReceipt: (
      scope: NativeSql.OwnerScope,
      runId: string,
      receipt: WorkstreamReceipt,
      originalCiphertextSha256: string,
      canonicalSha256: string,
    ) => Effect.Effect<void, Failure>;
  }
>()("t3/jones/workstreams/native/store/archiveStore") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const db = yield* NativeSql.NativeSql;
  const contains = Effect.fn("ArchiveStore.contains")(
    (scope: NativeSql.OwnerScope, commandId: string) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const rows = yield* sql`
        SELECT command_id FROM jones_workstreams_legacy_receipts
        WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND command_id=${commandId}
        UNION ALL SELECT command_id FROM jones_workstreams_receipts
        WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND command_id=${commandId} AND origin='legacy_open'
        UNION ALL SELECT a.record_key AS command_id FROM jones_workstreams_legacy_archive a
        JOIN jones_workstreams_import_runs r ON r.import_run_id=a.import_run_id
        WHERE r.registry_instance_id=${scope.registry_instance_id} AND a.class='workstream_command_receipts'
        AND a.record_key=${canonicalWorkstreamsSnapshotJson([scope.owner_id, commandId])}`;
          return rows.length > 0;
        }),
      ),
  );
  const evidence = Effect.fn("ArchiveStore.evidence")(
    (scope: NativeSql.OwnerScope, commandId: string) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const recordKey = canonicalWorkstreamsSnapshotJson([scope.owner_id, commandId]);
          const rows = yield* sql<{
            import_run_id: string;
            snapshot_id: string;
            canonical_sha256: string;
            archive_version: number;
            envelope: string;
          }>`
        SELECT a.*, r.snapshot_id FROM jones_workstreams_legacy_archive a
        JOIN jones_workstreams_import_runs r ON r.import_run_id=a.import_run_id
        WHERE r.registry_instance_id=${scope.registry_instance_id} AND a.class='workstream_command_receipts' AND a.record_key=${recordKey}`;
          if (!rows.length) return null;
          let proof: WorkstreamsLegacyReceiptProof | null = null;
          for (const row of rows) {
            const raw = yield* db.openJson(
              {
                ...scope,
                purpose: "legacy_archive",
                record_id: canonicalWorkstreamsSnapshotJson([
                  row.import_run_id,
                  "workstream_command_receipts",
                  recordKey,
                ]),
                revision: 1,
              },
              row.envelope,
            );
            const archived = yield* decodeEvidence(raw).pipe(
              Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
            );
            const source = archived.source;
            const value = yield* decodeIdentity(source.row.payload).pipe(
              Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
            );
            if (
              row.archive_version !== 1 ||
              source.class !== "workstream_command_receipts" ||
              canonicalWorkstreamsSnapshotJson(source.key) !== recordKey ||
              sha256(canonicalWorkstreamsSnapshotJson(source)) !== row.canonical_sha256 ||
              source.row.command_id !== commandId ||
              source.row.owner_id !== scope.owner_id ||
              value.command_id !== commandId ||
              value.owner_id !== scope.owner_id ||
              value.actor.principal_id !== source.row.principal_id ||
              value.state !== source.row.state ||
              ["request_sha256", "server_generation", "operation", "registry_version"].some(
                (field) =>
                  source.row[field] !== undefined &&
                  source.row[field] !== value[field as keyof typeof value],
              )
            )
              return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
            const next = yield* decodeProof({
              contract: WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
              command_id: commandId,
              owner_id: scope.owner_id,
              principal_id: value.actor.principal_id,
              operation: value.operation,
              state: value.state,
              request_sha256: value.request_sha256,
              server_generation: value.server_generation,
              registry_version: value.registry_version ?? null,
              payload: source.row.payload,
              request_bytes: "unknown",
              provenance: {
                registry_instance_id: scope.registry_instance_id,
                import_run_id: row.import_run_id,
                snapshot_id: row.snapshot_id,
                archive_version: 1,
                record_key: recordKey,
                canonical_row_sha256: row.canonical_sha256,
                ciphertext_sha256: source.row.ciphertext_sha256,
              },
            }).pipe(Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })));
            if (
              proof &&
              (proof.payload !== next.payload ||
                proof.provenance.canonical_row_sha256 !== next.provenance.canonical_row_sha256)
            )
              return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
            proof = next;
          }
          const stored = yield* sql<{
            envelope: string;
            registry_version: number | null;
            principal_id: string;
            request_sha256: string;
            server_generation: number;
            state: string;
            operation: string;
            canonical_sha256: string;
            original_ciphertext_sha256: string;
            import_run_id: string;
          }>`
        SELECT * FROM jones_workstreams_legacy_receipts WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND command_id=${commandId}`;
          const open = yield* sql<{
            envelope: string;
            registry_version: number | null;
            principal_id: string;
            request_sha256: string;
            server_generation: number;
            state: string;
            operation: string;
            origin: string;
          }>`
        SELECT * FROM jones_workstreams_receipts WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND command_id=${commandId}`;
          const metadata = stored[0] ?? open[0];
          if (
            !proof ||
            !metadata ||
            (stored.length && open.length) ||
            metadata.principal_id !== proof.principal_id ||
            metadata.request_sha256 !== proof.request_sha256 ||
            metadata.server_generation !== proof.server_generation ||
            metadata.state !== proof.state ||
            metadata.operation !== proof.operation ||
            metadata.registry_version !== proof.registry_version ||
            (stored[0] &&
              (stored[0].canonical_sha256 !== proof.provenance.canonical_row_sha256 ||
                stored[0].original_ciphertext_sha256 !== proof.provenance.ciphertext_sha256 ||
                stored[0].import_run_id !== proof.provenance.import_run_id)) ||
            (open[0] && open[0].origin !== "legacy_open")
          )
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          const decrypted = yield* db.openJson(
            {
              ...scope,
              purpose: stored.length ? "legacy_receipt" : "receipt",
              record_id: commandId,
              revision: metadata.registry_version ?? 0,
            },
            metadata.envelope,
          );
          const payload = stored.length
            ? decrypted
            : (yield* decodeOpenEvidence(decrypted).pipe(
                Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
              )).receipt;
          if (
            NativeSql.jsonText(payload) !==
            NativeSql.jsonText(JSON.parse(proof.payload.replace(/^\uFEFF/, "")))
          )
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          return proof;
        }),
      ),
  );
  const receipt = Effect.fn("ArchiveStore.receipt")(
    (scope: NativeSql.OwnerScope, commandId: string) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const rows = yield* sql<{
            envelope: string;
            registry_version: number | null;
          }>`SELECT envelope, registry_version FROM jones_workstreams_legacy_receipts WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id} AND command_id = ${commandId}`;
          if (!rows[0]) return null;
          const value = yield* db.openJson(
            {
              ...scope,
              purpose: "legacy_receipt",
              record_id: commandId,
              revision: rows[0].registry_version ?? 0,
            },
            rows[0].envelope,
          );
          return yield* decodeReceipt(value).pipe(
            Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
          );
        }),
      ),
  );
  const coverage = Effect.fn("ArchiveStore.coverage")((scope: NativeSql.OwnerScope) =>
    db.transaction(
      scope,
      Effect.gen(function* () {
        const runs = yield* sql<{
          import_run_id: string;
          state: string;
          envelope_owner_id: string;
          envelope: string;
        }>`SELECT import_run_id, state, envelope_owner_id, envelope FROM jones_workstreams_import_runs WHERE registry_instance_id = ${scope.registry_instance_id}`;
        let complete = false;
        let missing = false;
        for (const run of runs) {
          const raw = yield* db.openJson(
            {
              ...scope,
              owner_id: run.envelope_owner_id,
              purpose: "import_manifest",
              record_id: run.import_run_id,
              revision: 1,
            },
            run.envelope,
          );
          const manifest = yield* decodeManifest(raw).pipe(
            Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
          );
          if (
            manifest.registry.registry_instance_id !== scope.registry_instance_id ||
            !manifest.owners.some((owner) => owner.owner_id === scope.owner_id)
          )
            continue;
          if (run.state !== "activated") return "import_incomplete" as const;
          const classes = yield* sql<{
            state: string;
          }>`SELECT state FROM jones_workstreams_import_classes WHERE import_run_id = ${run.import_run_id} AND class = 'workstream_command_receipts'`;
          if (classes[0]?.state === "verified") complete = true;
          else missing = true;
        }
        return missing ? "archive_class_missing" : complete ? "complete" : "import_incomplete";
      }),
    ),
  );
  const startImport = Effect.fn("ArchiveStore.startImport")(
    (
      scope: NativeSql.OwnerScope,
      run: {
        import_run_id: string;
        snapshot_id: string;
        snapshot_sha256: string;
        producer: string;
        contract_version: string;
        manifest: ImportManifest;
      },
    ) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const parsed = yield* decodeManifest(run.manifest).pipe(
            Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
          );
          if (
            parsed.registry.registry_instance_id !== scope.registry_instance_id ||
            !parsed.owners.some((owner) => owner.owner_id === scope.owner_id)
          )
            return yield* new NativeSql.StoreError({ reason: "owner_mismatch" });
          const old = yield* sql<{
            snapshot_sha256: string;
          }>`SELECT snapshot_sha256 FROM jones_workstreams_import_runs WHERE import_run_id = ${run.import_run_id}`;
          if (old[0]) {
            if (old[0].snapshot_sha256 !== run.snapshot_sha256)
              return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
            return;
          }
          const envelope = yield* db.sealJson(
            { ...scope, purpose: "import_manifest", record_id: run.import_run_id, revision: 1 },
            run.manifest,
          );
          yield* sql`INSERT INTO jones_workstreams_import_runs ${sql.insert({ import_run_id: run.import_run_id, registry_instance_id: scope.registry_instance_id, envelope_owner_id: scope.owner_id, snapshot_id: run.snapshot_id, snapshot_sha256: run.snapshot_sha256, producer: run.producer, contract_version: run.contract_version, state: "planned", envelope })}`;
        }),
      ),
  );
  const archive = Effect.fn("ArchiveStore.archive")(
    (
      scope: NativeSql.OwnerScope,
      row: {
        import_run_id: string;
        class: string;
        record_key: string;
        canonical_sha256: string;
        value: unknown;
      },
    ) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const old = yield* sql<{
            canonical_sha256: string;
          }>`SELECT canonical_sha256 FROM jones_workstreams_legacy_archive WHERE import_run_id = ${row.import_run_id} AND class = ${row.class} AND record_key = ${row.record_key}`;
          if (old[0]) {
            if (old[0].canonical_sha256 !== row.canonical_sha256)
              return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
            return;
          }
          const envelope = yield* db.sealJson(
            {
              ...scope,
              purpose: "legacy_archive",
              record_id: canonicalWorkstreamsSnapshotJson([
                row.import_run_id,
                row.class,
                row.record_key,
              ]),
              revision: 1,
            },
            row.value,
          );
          yield* sql`INSERT INTO jones_workstreams_legacy_archive ${sql.insert({ import_run_id: row.import_run_id, class: row.class, record_key: row.record_key, archive_version: 1, canonical_sha256: row.canonical_sha256, envelope })}`;
        }),
      ),
  );
  const readArchive = Effect.fn("ArchiveStore.readArchive")(
    (scope: NativeSql.OwnerScope, runId: string, className: string, recordKey: string) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const rows = yield* sql<{
            envelope: string;
          }>`SELECT envelope FROM jones_workstreams_legacy_archive WHERE import_run_id = ${runId} AND class = ${className} AND record_key = ${recordKey}`;
          return rows[0]
            ? yield* db.openJson(
                {
                  ...scope,
                  purpose: "legacy_archive",
                  record_id: canonicalWorkstreamsSnapshotJson([runId, className, recordKey]),
                  revision: 1,
                },
                rows[0].envelope,
              )
            : null;
        }),
      ),
  );
  const addReceipt = Effect.fn("ArchiveStore.addReceipt")(
    (
      scope: NativeSql.OwnerScope,
      runId: string,
      input: WorkstreamReceipt,
      originalCiphertextSha256: string,
      canonicalSha256: string,
    ) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const value = yield* decodeReceipt(input).pipe(
            Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
          );
          if (
            value.owner_id !== scope.owner_id ||
            (value.state !== "committed" && value.state !== "rejected")
          )
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          const old = yield* receipt(scope, value.command_id);
          if (old) {
            if (
              old.request_sha256 !== value.request_sha256 ||
              old.actor.principal_id !== value.actor.principal_id ||
              NativeSql.jsonText(old) !== NativeSql.jsonText(value)
            )
              return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
            return;
          }
          const version = value.state === "committed" ? value.registry_version : null;
          const envelope = yield* db.sealJson(
            {
              ...scope,
              purpose: "legacy_receipt",
              record_id: value.command_id,
              revision: version ?? 0,
            },
            value,
          );
          yield* sql`INSERT INTO jones_workstreams_legacy_receipts ${sql.insert({ ...scope, command_id: value.command_id, archive_version: 1, import_run_id: runId, principal_id: value.actor.principal_id, operation: value.operation, state: value.state, request_sha256: value.request_sha256, server_generation: value.server_generation, registry_version: version, original_ciphertext_sha256: originalCiphertextSha256, canonical_sha256: canonicalSha256, envelope })}`;
        }),
      ),
  );
  return ArchiveStore.of({
    contains,
    evidence,
    receipt,
    coverage,
    startImport,
    archive,
    readArchive,
    addReceipt,
  });
});
/** @public Canonical service layer. */
export const layer = Layer.effect(ArchiveStore, make);
