// @effect-diagnostics preferSchemaOverJson:off -- Target payloads can contain fractional private JSON; compare semantic values without integer-only outer canonicalization.
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Archive from "../store/archiveStore.ts";
import * as Registry from "../store/registryStore.ts";
import {
  readLegacyLedger,
  targetAddress,
  reconciliationDigest,
} from "../store/legacyReconciliationStore.ts";
import type { Target } from "./archive.ts";
import { canonical, compareKeys, digest, TransitionError } from "./canonical.ts";
import { archiveScope, readTarget, type ArchivedRow } from "./archive.ts";
import { loadManifest } from "./apply.ts";
import type { ImportPlan } from "./plan.ts";
import type { SnapshotRow } from "./snapshotReader.ts";
export const readback = Effect.fn("WorkstreamsTransition.readback")(function* (p: ImportPlan) {
  const sql = yield* SqlClient.SqlClient,
    archive = yield* Archive.ArchiveStore,
    registry = yield* Registry.RegistryStore;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const run = yield* loadManifest(p);
      if (!["applied", "verified", "activated"].includes(run.state))
        return yield* new TransitionError({ reason: "import_incomplete" });
      const sources: SnapshotRow[] = [],
        counts = new Map<string, number>();
      const targets = new Map<string, Target>();
      const versions = new Map(
        p.snapshot.header.owners.map((o) => [o.owner_id, o.registry_version]),
      );
      const roots: [string, string][] = [];
      let reconciled = 0;
      for (const owner of p.snapshot.header.owners) {
        for (const table of [
          "records",
          "references",
          "memberships",
          "declarations",
          "lifecycle_declarations",
          "edges",
          "pr_observations",
          "receipts",
          "legacy_receipts",
          "external_reservations",
          "external_results",
          "history",
          "history_scopes",
          "appearance",
          "appearance_receipts",
          "grants",
          "source_bindings",
          "rollup_manifests",
          "rollup_manifest_sources",
          "rollup_queue",
          "rollup_summaries",
        ])
          counts.set(canonical([table, owner.owner_id]), 0);
        const expected = new Set(
          p.snapshot.rows
            .filter((r) => r.row.owner_id === owner.owner_id && r.row.principal_id !== undefined)
            .map((r) => String(r.row.principal_id)),
        );
        const principals = yield* registry.listPlain(
          { ...archiveScope(p), owner_id: owner.owner_id },
          "principals",
        );
        if (
          principals.length !== expected.size ||
          principals.some(
            (r) =>
              !expected.has(String(r.principal_id)) ||
              r.environment_id !==
                (p.mappings.find((m) => m.principal_id === r.principal_id)?.environment_id ?? null),
          )
        )
          return yield* new TransitionError({ reason: "principal_readback_mismatch" });
      }
      for (const c of p.snapshot.classes) {
        const records = yield* sql<{
          record_key: string;
          canonical_sha256: string;
        }>`SELECT record_key,canonical_sha256 FROM jones_workstreams_legacy_archive WHERE import_run_id=${p.import_run_id} AND class=${c.class}`;
        const section: SnapshotRow[] = [];
        if (records.length !== c.count)
          return yield* new TransitionError({ reason: "archive_count_mismatch" });
        for (const record of records) {
          const original = p.snapshot.rows.find(
            (r) => r.class === c.class && canonical(r.key) === record.record_key,
          );
          if (!original) return yield* new TransitionError({ reason: "archive_key_mismatch" });
          const scope = {
            ...archiveScope(p),
            owner_id: String(original.row.owner_id ?? archiveScope(p).owner_id),
          };
          const archived = (yield* archive.readArchive(
            scope,
            p.import_run_id,
            c.class,
            record.record_key,
          )) as ArchivedRow | null;
          if (
            !archived ||
            digest(canonical(archived.source)) !== record.canonical_sha256 ||
            canonical(archived.source) !== canonical(original)
          )
            return yield* new TransitionError({ reason: "archive_digest_mismatch" });
          for (const target of archived.targets) {
            const id = targetAddress(target);
            if (targets.has(id))
              return yield* new TransitionError({ reason: "duplicate_baseline_target" });
            targets.set(id, target);
          }
          section.push(archived.source);
        }
        section.sort((a, b) => compareKeys(a.key, b.key));
        if (digest(section.map((r) => canonical(r) + "\n").join("")) !== c.sha256)
          return yield* new TransitionError({ reason: "class_digest_mismatch" });
        sources.push(...section);
        if (run.state === "applied")
          yield* sql`UPDATE jones_workstreams_import_classes SET state='verified',observed_sha256=${c.sha256} WHERE import_run_id=${p.import_run_id} AND class=${c.class} AND state='applied'`;
      }
      for (const owner of p.snapshot.header.owners) {
        const ledger = yield* readLegacyLedger(
          { ...archiveScope(p), owner_id: owner.owner_id },
          p.import_run_id,
        );
        roots.push([owner.owner_id, ledger.root]);
        for (const delta of ledger.deltas) {
          if (
            delta.before_registry_version !== versions.get(owner.owner_id) ||
            delta.binding.home_environment_id !== p.home_environment_id ||
            delta.binding.home_generation !==
              owner.server_generation +
                (p.snapshot.header.producer.system === "jones-native" ? 1 : 0)
          )
            return yield* new TransitionError({ reason: "reconciliation_context_mismatch" });
          for (const change of delta.changes) {
            const id = targetAddress(change.after),
              before = targets.get(id) ?? null;
            if (canonical(before) !== canonical(change.before))
              return yield* new TransitionError({ reason: "reconciliation_delta_mismatch" });
            targets.set(id, change.after);
          }
          versions.set(owner.owner_id, delta.after_registry_version);
          reconciled++;
        }
      }
      for (const target of targets.values()) {
        const stored = yield* readTarget(target);
        if (
          canonical(stored.data) !== canonical(target.data) ||
          (target.context && JSON.stringify(stored.value) !== JSON.stringify(target.value))
        )
          return yield* new TransitionError({ reason: "target_digest_mismatch" });
        const ownerKey = canonical([target.table, target.data.owner_id]);
        counts.set(ownerKey, (counts.get(ownerKey) ?? 0) + 1);
      }
      for (const [key, count] of counts) {
        const [table, owner] = JSON.parse(key) as [string, string];
        const actual = yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM ${sql(`jones_workstreams_${table}`)} WHERE registry_instance_id=${archiveScope(p).registry_instance_id} AND owner_id=${owner}`;
        if (actual[0]!.count !== count)
          return yield* new TransitionError({ reason: "target_count_mismatch" });
      }
      for (const owner of p.snapshot.header.owners) {
        const context = yield* registry.context({ ...archiveScope(p), owner_id: owner.owner_id });
        if (
          !context ||
          context.canonical_origin !== p.snapshot.header.registry.canonical_origin ||
          context.home_environment_id !== p.home_environment_id ||
          context.server_generation !==
            owner.server_generation +
              (p.snapshot.header.producer.system === "jones-native" ? 1 : 0) ||
          context.registry_version !== versions.get(owner.owner_id)
        )
          return yield* new TransitionError({ reason: "registry_readback_mismatch" });
      }
      if (run.state === "applied")
        yield* sql`UPDATE jones_workstreams_import_runs SET state='verified' WHERE import_run_id=${p.import_run_id}`;
      return {
        reconciliation: {
          delta_root: reconciliationDigest(roots),
          count: reconciled,
          registry_versions: [...versions].map(([owner_id, registry_version]) => ({
            owner_id,
            registry_version,
          })),
        },
        snapshot_sha256: p.snapshot.snapshot_sha256,
        classes: p.snapshot.classes,
        rows: sources,
        state: run.state === "activated" ? "activated" : "verified",
      };
    }),
  );
});
