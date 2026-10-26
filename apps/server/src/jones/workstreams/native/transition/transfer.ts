// @effect-diagnostics nodeBuiltinImport:off -- Transfer permits bind the complete encrypted logical bundle to the fixed destination.
// @effect-diagnostics preferSchemaOverJson:off -- Transfer extension payload strings preserve JSON numbers and original private strings.
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Registry from "../store/registryStore.ts";
import * as NativeSql from "../store/sql.ts";
import * as Archive from "../store/archiveStore.ts";
import * as Keys from "../crypto/keyProvider.ts";
import {
  canonical,
  digest,
  extensionVersion,
  extensions,
  tableKeys,
  TransitionError,
} from "./canonical.ts";
import { archiveScope } from "./archive.ts";
import { loadManifest } from "./apply.ts";
import { prepareReverse, type ReverseInput } from "./reverseExport.ts";
import { encryptSnapshot, decryptSnapshot, parseSnapshot, unbase64 } from "./snapshotReader.ts";
import type { ImportPlan } from "./plan.ts";
export interface TransferExtension {
  readonly class: (typeof extensions)[number];
  readonly key: readonly string[];
  readonly payload: string;
}
export interface TransferBundle {
  readonly contract: typeof extensionVersion;
  readonly transfer_id: string;
  readonly source_home: string;
  readonly destination_home: string;
  readonly next_generations: readonly {
    readonly owner_id: string;
    readonly server_generation: number;
  }[];
  readonly snapshot: string;
  readonly lineage: {
    readonly original_snapshot_id: string;
    readonly original_snapshot_sha256: string;
    readonly import_run_id: string;
  };
  readonly extensions: readonly TransferExtension[];
}
export interface TransferPermit {
  readonly key_id: string;
  readonly bundle_sha256: string;
  readonly signature: string;
}
export const prepareTransfer = Effect.fn("WorkstreamsTransition.prepareTransfer")(function* (
  p: ImportPlan,
  input: ReverseInput & { readonly transfer_id: string; readonly destination_home: string },
) {
  const sql = yield* SqlClient.SqlClient,
    registry = yield* Registry.RegistryStore,
    db = yield* NativeSql.NativeSql,
    archive = yield* Archive.ArchiveStore;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      if (
        !input.transfer_id ||
        !input.destination_home ||
        input.destination_home === p.home_environment_id
      )
        return yield* new TransitionError({ reason: "transfer_destination_invalid" });
      const reverse = yield* prepareReverse(p, input),
        output: TransferExtension[] = [];
      for (const owner of p.snapshot.header.owners) {
        const scope = { ...archiveScope(p), owner_id: owner.owner_id };
        for (const principal of yield* registry.listPlain(scope, "principals"))
          output.push({
            class: "jones.principal_environments",
            key: [owner.owner_id, String(principal.principal_id)],
            payload: JSON.stringify(principal),
          });
        for (const binding of yield* registry.list(scope, "rollup_bindings"))
          output.push({
            class: "jones.rollup_bindings",
            key: [
              owner.owner_id,
              String(binding.metadata.workstream_id),
              String(binding.metadata.native_reference_id),
            ],
            payload: JSON.stringify(binding),
          });
      }
      const runs = yield* sql<{
        import_run_id: string;
        envelope_owner_id: string;
        envelope: string;
      }>`SELECT * FROM jones_workstreams_import_runs WHERE registry_instance_id=${archiveScope(p).registry_instance_id} ORDER BY import_run_id COLLATE BINARY`;
      for (const run of runs) {
        const manifest = yield* db.openJson(
          {
            ...archiveScope(p),
            owner_id: run.envelope_owner_id,
            purpose: "import_manifest",
            record_id: run.import_run_id,
            revision: 1,
          },
          run.envelope,
        );
        const classRows =
          yield* sql`SELECT * FROM jones_workstreams_import_classes WHERE import_run_id=${run.import_run_id}`;
        output.push({
          class: "jones.import_runs",
          key: [run.import_run_id],
          payload: JSON.stringify({
            metadata: { ...run, envelope: undefined },
            manifest,
            classes: classRows,
          }),
        });
      }
      // Preserve every retained original row and prior rollback/activation record, not only the current CP projection.
      const archived = yield* sql<{
        import_run_id: string;
        class: string;
        record_key: string;
        canonical_sha256: string;
      }>`SELECT a.import_run_id,a.class,a.record_key,a.canonical_sha256 FROM jones_workstreams_legacy_archive a JOIN jones_workstreams_import_runs r ON r.import_run_id=a.import_run_id WHERE r.registry_instance_id=${archiveScope(p).registry_instance_id} ORDER BY a.import_run_id,a.class,a.record_key`;
      for (const row of archived) {
        if (
          row.class === "jones.transfer" &&
          row.record_key === "original" &&
          row.import_run_id === p.import_run_id
        )
          continue;
        const source = p.snapshot.rows.find(
          (s) => s.class === row.class && canonical(s.key) === row.record_key,
        );
        const ownerIndex = tableKeys[row.class]?.indexOf("owner_id") ?? -1;
        const keyedOwner =
          ownerIndex >= 0 ? (JSON.parse(row.record_key) as unknown[])[ownerIndex] : undefined;
        const scope = {
          ...archiveScope(p),
          owner_id: String(source?.row.owner_id ?? keyedOwner ?? archiveScope(p).owner_id),
        };
        const value = yield* archive.readArchive(
          scope,
          row.import_run_id,
          row.class,
          row.record_key,
        );
        if (value === null)
          return yield* new TransitionError({ reason: "transfer_archive_missing" });
        output.push({
          class: "jones.legacy_archive",
          key: [row.import_run_id, row.class, row.record_key],
          payload: JSON.stringify({ canonical_sha256: row.canonical_sha256, scope, value }),
        });
      }
      output.sort((a, b) => (canonical([a.class, a.key]) < canonical([b.class, b.key]) ? -1 : 1));
      return {
        contract: extensionVersion,
        transfer_id: input.transfer_id,
        source_home: p.home_environment_id,
        destination_home: input.destination_home,
        next_generations: reverse.next_generations,
        snapshot: reverse.canonical_jsonl,
        lineage: reverse.lineage,
        extensions: output,
      } satisfies TransferBundle;
    }),
  );
});
export const transfer = Effect.fn("WorkstreamsTransition.transfer")(function* (
  p: ImportPlan,
  input: Parameters<typeof prepareTransfer>[1],
  permit: TransferPermit,
  permitKeyId: string,
  publicKey: NodeCrypto.KeyObject,
) {
  const bundle = yield* prepareTransfer(p, input),
    keys = yield* Keys.WorkstreamsKeyProvider,
    archive = yield* Archive.ArchiveStore;
  const hash = digest(canonical(bundle));
  yield* Effect.try({
    try: () => {
      if (
        permit.key_id !== permitKeyId ||
        permit.bundle_sha256 !== hash ||
        !NodeCrypto.verify(
          null,
          Buffer.from(canonical(["jones-workstreams-transfer-permit-v1", hash])),
          publicKey,
          unbase64(permit.signature),
        )
      )
        throw new TransitionError({ reason: "transfer_signature_invalid" });
    },
    catch: () => new TransitionError({ reason: "transfer_signature_invalid" }),
  });
  const prior = yield* archive.readArchive(
    archiveScope(p),
    p.import_run_id,
    "jones.transfer",
    "original",
  );
  const intent = {
    transfer_id: input.transfer_id,
    destination_home: input.destination_home,
    bundle_sha256: hash,
    next_generations: bundle.next_generations,
  };
  if (prior !== null && canonical(prior) !== canonical(intent))
    return yield* new TransitionError({ reason: "transfer_identity_conflict" });
  yield* archive.archive(archiveScope(p), {
    import_run_id: p.import_run_id,
    class: "jones.transfer",
    record_key: "original",
    canonical_sha256: digest(canonical(intent)),
    value: intent,
  });
  const key = yield* keys.get(input.stream_key_id);
  return {
    encrypted: encryptSnapshot(canonical(bundle) + "\n", input.stream_key_id, key),
    bundle_sha256: hash,
    next_generations: bundle.next_generations,
  };
});
export const readTransfer = Effect.fn("WorkstreamsTransition.readTransfer")(function* (input: {
  readonly encrypted: string;
  readonly stream_key_id: string;
  readonly destination_home: string;
  readonly permit: TransferPermit;
  readonly permit_key_id: string;
  readonly public_key: NodeCrypto.KeyObject;
}) {
  const keys = yield* Keys.WorkstreamsKeyProvider,
    key = yield* keys.get(input.stream_key_id);
  return yield* Effect.try({
    try: () => {
      const text = decryptSnapshot(input.encrypted, input.stream_key_id, key),
        bundle = JSON.parse(text) as TransferBundle;
      const hash = digest(canonical(bundle));
      if (
        text !== canonical(bundle) + "\n" ||
        bundle.contract !== extensionVersion ||
        bundle.destination_home !== input.destination_home ||
        input.permit.bundle_sha256 !== hash ||
        input.permit.key_id !== input.permit_key_id ||
        !NodeCrypto.verify(
          null,
          Buffer.from(canonical(["jones-workstreams-transfer-permit-v1", hash])),
          input.public_key,
          unbase64(input.permit.signature),
        )
      )
        throw new TransitionError({ reason: "transfer_invalid" });
      const snapshot = parseSnapshot(bundle.snapshot);
      if (
        canonical(bundle.next_generations) !==
        canonical(
          snapshot.header.owners.map((o) => ({
            owner_id: o.owner_id,
            server_generation: o.server_generation + 1,
          })),
        )
      )
        throw new TransitionError({ reason: "transfer_generation_conflict" });
      if (
        new Set(bundle.extensions.map((x) => canonical([x.class, x.key]))).size !==
          bundle.extensions.length ||
        bundle.extensions.some((x) => !extensions.includes(x.class))
      )
        throw new TransitionError({ reason: "transfer_extension_unqualified" });
      return bundle;
    },
    catch: () => new TransitionError({ reason: "transfer_authentication_or_validation" }),
  });
});

export const applyTransferExtensions = Effect.fn("WorkstreamsTransition.applyTransferExtensions")(
  function* (p: ImportPlan, bundle: TransferBundle) {
    const sql = yield* SqlClient.SqlClient,
      db = yield* NativeSql.NativeSql,
      archive = yield* Archive.ArchiveStore,
      registry = yield* Registry.RegistryStore;
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const run = yield* loadManifest(p);
        if (
          !["applied", "verified", "activated"].includes(run.state) ||
          p.home_environment_id !== bundle.destination_home ||
          p.snapshot.snapshot_sha256 !== parseSnapshot(bundle.snapshot).snapshot_sha256
        )
          return yield* new TransitionError({ reason: "transfer_apply_conflict" });
        const bundleHash = digest(canonical(bundle));
        const old = yield* archive.readArchive(
          archiveScope(p),
          p.import_run_id,
          "jones.transfer.extensions",
          "complete",
        );
        if (old !== null) {
          if (canonical(old) !== canonical({ bundle_sha256: bundleHash }))
            return yield* new TransitionError({ reason: "transfer_apply_conflict" });
          return;
        }
        if (run.state !== "applied")
          return yield* new TransitionError({ reason: "transfer_apply_conflict" });
        const principalRows = bundle.extensions.filter(
          (x) => x.class === "jones.principal_environments",
        );
        // Principal mappings are append-only. The base import must have used the exact captured map.
        for (const row of principalRows) {
          const value = JSON.parse(row.payload) as Record<string, string | number | null>;
          const owner = String(value.owner_id),
            stored = yield* registry.listPlain(
              { ...archiveScope(p), owner_id: owner },
              "principals",
            );
          if (
            !stored.some(
              (p) =>
                p.principal_id === value.principal_id && p.environment_id === value.environment_id,
            )
          )
            return yield* new TransitionError({ reason: "transfer_principal_map_conflict" });
        }
        for (const entry of bundle.extensions) {
          if (entry.class === "jones.rollup_bindings") {
            const v = JSON.parse(entry.payload) as Registry.Entity;
            yield* registry.put(
              { ...archiveScope(p), owner_id: entry.key[0]! },
              "rollup_bindings",
              v,
              null,
            );
          }
          // Retain the original run, archive rows, fixed intents and their full manifests as
          // versioned encrypted extension records. They never create a second membership authority.
          yield* archive.archive(archiveScope(p), {
            import_run_id: p.import_run_id,
            class: entry.class,
            record_key: canonical(entry.key),
            canonical_sha256: digest(entry.payload),
            value: { extension_contract: extensionVersion, payload: entry.payload },
          });
        }
        yield* archive.archive(archiveScope(p), {
          import_run_id: p.import_run_id,
          class: "jones.transfer.extensions",
          record_key: "complete",
          canonical_sha256: digest(canonical({ bundle_sha256: bundleHash })),
          value: { bundle_sha256: bundleHash },
        });
        // Explicitly capture the manifest to bind transfer provenance and prevent activation
        // by a caller that omitted required extension apply/readback.
        const manifest = yield* db.openJson(
          {
            ...archiveScope(p),
            purpose: "import_manifest",
            record_id: p.import_run_id,
            revision: 1,
          },
          run.envelope,
        );
        return {
          bundle_sha256: bundleHash,
          manifest_sha256: digest(canonical(manifest)),
          extension_count: bundle.extensions.length,
        };
      }),
    );
  },
);
export const completeTransfer = Effect.fn("WorkstreamsTransition.completeTransfer")(function* (
  p: ImportPlan,
  receipt: {
    readonly transfer_id: string;
    readonly destination_home: string;
    readonly bundle_sha256: string;
    readonly next_generations: readonly {
      readonly owner_id: string;
      readonly server_generation: number;
    }[];
  },
) {
  const sql = yield* SqlClient.SqlClient,
    archive = yield* Archive.ArchiveStore;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const saved = yield* archive.readArchive(
        archiveScope(p),
        p.import_run_id,
        "jones.transfer",
        "original",
      );
      if (saved === null || canonical(saved) !== canonical(receipt))
        return yield* new TransitionError({ reason: "transfer_receipt_conflict" });
      for (const owner of p.snapshot.header.owners) {
        const generation =
          receipt.next_generations.find((x) => x.owner_id === owner.owner_id)!.server_generation -
          1;
        const rows = yield* sql<{
          state: string;
          server_generation: number;
          home_environment_id: string;
        }>`SELECT state,server_generation,home_environment_id FROM jones_workstreams_registries WHERE registry_instance_id=${archiveScope(p).registry_instance_id} AND owner_id=${owner.owner_id}`;
        if (
          !rows[0] ||
          rows[0].server_generation !== generation ||
          rows[0].home_environment_id !== p.home_environment_id ||
          !["fenced", "transferred_out"].includes(rows[0].state)
        )
          return yield* new TransitionError({ reason: "transfer_source_generation_conflict" });
        if (rows[0].state === "transferred_out") continue;
        const changed =
          yield* sql`UPDATE jones_workstreams_registries SET state='transferred_out' WHERE registry_instance_id=${archiveScope(p).registry_instance_id} AND owner_id=${owner.owner_id} AND state='fenced' AND server_generation=${generation} RETURNING owner_id`;
        if (changed.length !== 1)
          return yield* new TransitionError({ reason: "transfer_source_generation_conflict" });
      }
      return { state: "transferred_out" as const, transfer_id: receipt.transfer_id };
    }),
  );
});

export const readbackTransfer = Effect.fn("WorkstreamsTransition.readbackTransfer")(function* (
  p: ImportPlan,
  bundle: TransferBundle,
) {
  const sql = yield* SqlClient.SqlClient,
    archive = yield* Archive.ArchiveStore,
    registry = yield* Registry.RegistryStore;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* loadManifest(p);
      const hash = digest(canonical(bundle)),
        proof = { bundle_sha256: hash };
      const complete = yield* archive.readArchive(
        archiveScope(p),
        p.import_run_id,
        "jones.transfer.extensions",
        "complete",
      );
      if (complete === null || canonical(complete) !== canonical(proof))
        return yield* new TransitionError({ reason: "transfer_extensions_incomplete" });
      for (const entry of bundle.extensions) {
        const stored = yield* archive.readArchive(
          archiveScope(p),
          p.import_run_id,
          entry.class,
          canonical(entry.key),
        );
        if (
          stored === null ||
          canonical(stored) !==
            canonical({ extension_contract: extensionVersion, payload: entry.payload })
        )
          return yield* new TransitionError({ reason: "transfer_extension_readback" });
        if (entry.class === "jones.rollup_bindings") {
          const expected = JSON.parse(entry.payload) as Registry.Entity;
          const actual = (yield* registry.list(
            { ...archiveScope(p), owner_id: entry.key[0]! },
            "rollup_bindings",
          )).find((x) => canonical(x.metadata) === canonical(expected.metadata));
          if (!actual || JSON.stringify(actual.value) !== JSON.stringify(expected.value))
            return yield* new TransitionError({ reason: "transfer_binding_readback" });
        }
      }
      yield* archive.archive(archiveScope(p), {
        import_run_id: p.import_run_id,
        class: "jones.transfer.extensions",
        record_key: "verified",
        canonical_sha256: digest(canonical(proof)),
        value: proof,
      });
      return {
        state: "verified" as const,
        bundle_sha256: hash,
        extension_count: bundle.extensions.length,
      };
    }),
  );
});
