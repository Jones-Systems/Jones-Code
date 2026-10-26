import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Archive from "../store/archiveStore.ts";
import { readLegacyLedger } from "../store/legacyReconciliationStore.ts";
import { canonical, digest, TransitionError } from "./canonical.ts";
import { archiveScope } from "./archive.ts";
import { loadManifest } from "./apply.ts";
import { readback } from "./readback.ts";
import type { ImportPlan } from "./plan.ts";
export interface ActivationIntent {
  readonly reconciliation?: {
    readonly delta_root: string;
    readonly registry_versions: readonly {
      readonly owner_id: string;
      readonly registry_version: number;
    }[];
  };
  readonly activation_id: string;
  readonly plan_sha256: string;
  readonly home_environment_id: string;
  readonly generations: readonly {
    readonly owner_id: string;
    readonly server_generation: number;
  }[];
}
export const activate = Effect.fn("WorkstreamsTransition.activate")(function* (
  p: ImportPlan,
  intent: ActivationIntent,
) {
  const sql = yield* SqlClient.SqlClient,
    archive = yield* Archive.ArchiveStore;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* loadManifest(p);
      const expected = p.snapshot.header.owners.map((o) => ({
        owner_id: o.owner_id,
        server_generation:
          o.server_generation + (p.snapshot.header.producer.system === "jones-native" ? 1 : 0),
      }));
      if (
        !intent.activation_id ||
        intent.plan_sha256 !== p.plan_sha256 ||
        intent.home_environment_id !== p.home_environment_id ||
        canonical(intent.generations) !== canonical(expected)
      )
        return yield* new TransitionError({ reason: "activation_identity_conflict" });
      // One fixed slot reserves the original activation ID. A lost reply reconciles this slot, never a new intent.
      const saved = yield* archive.readArchive(
        archiveScope(p),
        p.import_run_id,
        "jones.activation",
        "original",
      );
      if (saved !== null && canonical(saved) !== canonical(intent))
        return yield* new TransitionError({ reason: "activation_identity_conflict" });
      const run = yield* loadManifest(p);
      if (run.state === "activated") {
        for (const owner of expected) {
          const contexts =
            yield* sql`SELECT owner_id FROM jones_workstreams_registries WHERE registry_instance_id=${archiveScope(p).registry_instance_id} AND owner_id=${owner.owner_id} AND home_environment_id=${intent.home_environment_id} AND server_generation=${owner.server_generation} AND state IN ('active','fenced')`;
          if (contexts.length !== 1)
            return yield* new TransitionError({ reason: "activation_generation_changed" });
        }
        if (saved === null)
          return yield* new TransitionError({ reason: "activation_receipt_missing" });
        return { state: "activated" as const, activation_id: intent.activation_id, replayed: true };
      }
      const verified = yield* readback(p);
      if (
        verified.reconciliation.count &&
        (!intent.reconciliation ||
          intent.reconciliation.delta_root !== verified.reconciliation.delta_root ||
          canonical(intent.reconciliation.registry_versions) !==
            canonical(verified.reconciliation.registry_versions))
      )
        return yield* new TransitionError({ reason: "activation_reconciliation_identity" });
      const covered = new Set<string>();
      for (const owner of p.snapshot.header.owners) {
        const ledger = yield* readLegacyLedger(
          { ...archiveScope(p), owner_id: owner.owner_id },
          p.import_run_id,
        );
        for (const delta of ledger.deltas)
          covered.add(canonical([owner.owner_id, delta.binding.command_id]));
      }
      const originalOpen = p.snapshot.rows.filter(
        (r) => r.class === "workstream_external_reservations" && r.row.state === "active",
      );
      const originalReceipts = p.snapshot.rows.filter(
        (r) =>
          r.class === "workstream_command_receipts" &&
          ["pending", "unresolved"].includes(String(r.row.state)),
      );
      if (
        originalOpen.length !== p.snapshot.header.open_effects ||
        originalReceipts.length !== originalOpen.length ||
        originalOpen.some((r) => !covered.has(canonical([r.row.owner_id, r.row.command_id]))) ||
        originalReceipts.some((r) => !covered.has(canonical([r.row.owner_id, r.row.command_id]))) ||
        covered.size !== originalOpen.length
      )
        return yield* new TransitionError({ reason: "open_effects" });
      if (p.snapshot.header.producer.system === "jones-native") {
        const extensions = yield* archive.readArchive(
          archiveScope(p),
          p.import_run_id,
          "jones.transfer.extensions",
          "verified",
        );
        if (extensions === null)
          return yield* new TransitionError({ reason: "transfer_extensions_incomplete" });
      }
      if (p.unresolved_sources.length)
        return yield* new TransitionError({ reason: "unresolved_mapping" });
      const open = yield* sql<{
        count: number;
      }>`SELECT (SELECT count(*) FROM jones_workstreams_receipts WHERE registry_instance_id=${archiveScope(p).registry_instance_id} AND state IN ('pending','unresolved')) + (SELECT count(*) FROM jones_workstreams_external_reservations WHERE registry_instance_id=${archiveScope(p).registry_instance_id} AND state='active') + (SELECT count(*) FROM jones_workstreams_operations WHERE dest_registry_instance_id=${archiveScope(p).registry_instance_id} AND state IN ('accepted','dispatching','in_progress','reconciling')) AS count`;
      if (open[0]!.count) return yield* new TransitionError({ reason: "open_effects" });
      yield* archive.archive(archiveScope(p), {
        import_run_id: p.import_run_id,
        class: "jones.activation",
        record_key: "original",
        canonical_sha256: digest(canonical(intent)),
        value: intent,
      });
      for (const owner of expected) {
        const changed =
          yield* sql`UPDATE jones_workstreams_registries SET state='active' WHERE registry_instance_id=${archiveScope(p).registry_instance_id} AND owner_id=${owner.owner_id} AND state='importing' AND server_generation=${owner.server_generation} AND registry_version=${verified.reconciliation.registry_versions.find((o) => o.owner_id === owner.owner_id)!.registry_version} RETURNING owner_id`;
        if (changed.length !== 1) return yield* new TransitionError({ reason: "activation_cas" });
      }
      yield* sql`UPDATE jones_workstreams_import_runs SET state='activated' WHERE import_run_id=${p.import_run_id} AND state='verified'`;
      return { state: "activated" as const, activation_id: intent.activation_id, replayed: false };
    }),
  );
});
