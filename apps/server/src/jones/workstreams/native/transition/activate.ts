import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Archive from "../store/archiveStore.ts";
import { canonical, digest, TransitionError } from "./canonical.ts";
import { archiveScope } from "./archive.ts";
import { loadManifest } from "./apply.ts";
import { readback } from "./readback.ts";
import type { ImportPlan } from "./plan.ts";
export interface ActivationIntent {
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
      yield* readback(p);
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
      if (open[0]!.count || p.snapshot.header.open_effects)
        return yield* new TransitionError({ reason: "open_effects" });
      yield* archive.archive(archiveScope(p), {
        import_run_id: p.import_run_id,
        class: "jones.activation",
        record_key: "original",
        canonical_sha256: digest(canonical(intent)),
        value: intent,
      });
      for (const owner of expected) {
        const changed =
          yield* sql`UPDATE jones_workstreams_registries SET state='active' WHERE registry_instance_id=${archiveScope(p).registry_instance_id} AND owner_id=${owner.owner_id} AND state='importing' AND server_generation=${owner.server_generation} AND registry_version=${p.snapshot.header.owners.find((o) => o.owner_id === owner.owner_id)!.registry_version} RETURNING owner_id`;
        if (changed.length !== 1) return yield* new TransitionError({ reason: "activation_cas" });
      }
      yield* sql`UPDATE jones_workstreams_import_runs SET state='activated' WHERE import_run_id=${p.import_run_id} AND state='verified'`;
      return { state: "activated" as const, activation_id: intent.activation_id, replayed: false };
    }),
  );
});
