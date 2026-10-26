import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NativeSql from "../store/sql.ts";
import * as Registry from "../store/registryStore.ts";
import * as Archive from "../store/archiveStore.ts";
import * as Keys from "../crypto/keyProvider.ts";
import { canonical, digest, TransitionError } from "./canonical.ts";
import { encodeSnapshot, parseSnapshot } from "./snapshotReader.ts";
import { archiveScope, targetsFor, insertTarget } from "./archive.ts";
import { manifestOf, type ImportPlan } from "./plan.ts";
export const loadManifest = Effect.fn("WorkstreamsTransition.loadManifest")(function* (
  p: ImportPlan,
) {
  const sql = yield* SqlClient.SqlClient,
    db = yield* NativeSql.NativeSql;
  const runs = yield* sql<{
    state: string;
    envelope: string;
    registry_instance_id: string;
    envelope_owner_id: string;
  }>`SELECT * FROM jones_workstreams_import_runs WHERE import_run_id=${p.import_run_id}`;
  const run = runs[0];
  if (
    !run ||
    run.registry_instance_id !== p.snapshot.header.registry.registry_instance_id ||
    run.envelope_owner_id !== archiveScope(p).owner_id
  )
    return yield* new TransitionError({ reason: "import_run_missing" });
  const manifest = yield* db.openJson(
    { ...archiveScope(p), purpose: "import_manifest", record_id: p.import_run_id, revision: 1 },
    run.envelope,
  );
  if (
    digest(canonical(manifest)) !== p.plan_sha256 ||
    p.plan_sha256 !== digest(canonical(manifestOf(p)))
  )
    return yield* new TransitionError({ reason: "plan_conflict" });
  return run;
});
export const apply = Effect.fn("WorkstreamsTransition.apply")(function* (
  p: ImportPlan,
  limit = Number.MAX_SAFE_INTEGER,
) {
  const sql = yield* SqlClient.SqlClient,
    db = yield* NativeSql.NativeSql,
    registry = yield* Registry.RegistryStore,
    archive = yield* Archive.ArchiveStore,
    keys = yield* Keys.WorkstreamsKeyProvider;
  yield* Effect.try({
    try: () => {
      const verified = parseSnapshot(encodeSnapshot(p.snapshot.header, p.snapshot.rows));
      if (
        verified.snapshot_sha256 !== p.snapshot.snapshot_sha256 ||
        digest(canonical(manifestOf(p))) !== p.plan_sha256 ||
        !Number.isSafeInteger(limit) ||
        limit < 0
      )
        throw new TransitionError({ reason: "plan_conflict" });
    },
    catch: () => new TransitionError({ reason: "plan_conflict" }),
  });
  yield* sql.withTransaction(
    Effect.gen(function* () {
      const old =
        yield* sql`SELECT import_run_id FROM jones_workstreams_import_runs WHERE import_run_id=${p.import_run_id}`;
      if (old.length) {
        yield* loadManifest(p);
        return;
      }
      const existing =
        yield* sql`SELECT owner_id FROM jones_workstreams_registries WHERE registry_instance_id=${p.snapshot.header.registry.registry_instance_id}`;
      if (existing.length) return yield* new TransitionError({ reason: "target_populated" });
      const keyRows = yield* sql<{
        key_id: string;
      }>`SELECT key_id FROM jones_workstreams_keys WHERE registry_instance_id=${p.snapshot.header.registry.registry_instance_id} AND scope='home' AND state='active'`;
      if (keyRows.length !== 1 || keyRows[0]!.key_id !== p.active_key_id)
        return yield* new TransitionError({ reason: "key_not_provisioned" });
      yield* keys.get(p.active_key_id);
      for (const owner of p.snapshot.header.owners) {
        yield* registry.initialize({
          ...p.snapshot.header.registry,
          owner_id: owner.owner_id,
          home_environment_id: p.home_environment_id,
          server_generation:
            owner.server_generation +
            (p.snapshot.header.producer.system === "jones-native" ? 1 : 0),
          registry_version: owner.registry_version,
          lineage:
            p.snapshot.header.producer.system === "jones-native" ? "transferred" : "imported",
          active_key_id: p.active_key_id,
          state: "importing",
        });
        const ids = new Set(
          p.snapshot.rows
            .filter((r) => r.row.owner_id === owner.owner_id && r.row.principal_id !== undefined)
            .map((r) => String(r.row.principal_id)),
        );
        for (const id of ids) {
          const mapping = p.mappings.find((m) => m.principal_id === id);
          yield* registry.appendPlain(
            { ...archiveScope(p), owner_id: owner.owner_id },
            "principals",
            {
              principal_id: id,
              environment_id: mapping?.environment_id ?? null,
              kind: mapping ? "environment" : "legacy_unmapped",
              provenance: canonical({
                source: "snapshot",
                snapshot_id: p.snapshot.header.snapshot_id,
                evidence_sha256: mapping?.evidence_sha256 ?? null,
              }),
            },
          );
        }
      }
      const envelope = yield* db.sealJson(
        { ...archiveScope(p), purpose: "import_manifest", record_id: p.import_run_id, revision: 1 },
        manifestOf(p),
      );
      yield* sql`INSERT INTO jones_workstreams_import_runs ${sql.insert({ import_run_id: p.import_run_id, registry_instance_id: archiveScope(p).registry_instance_id, envelope_owner_id: archiveScope(p).owner_id, snapshot_id: p.snapshot.header.snapshot_id, snapshot_sha256: p.snapshot.snapshot_sha256, producer: p.snapshot.header.producer.system, contract_version: p.snapshot.header.contract, state: "planned", envelope })}`;
      for (const c of p.snapshot.classes)
        yield* sql`INSERT INTO jones_workstreams_import_classes ${sql.insert({ import_run_id: p.import_run_id, class: c.class, expected_count: c.count, expected_sha256: c.sha256, applied_count: 0, state: "planned" })}`;
      yield* sql`UPDATE jones_workstreams_import_runs SET state='applying' WHERE import_run_id=${p.import_run_id}`;
    }),
  );
  const run = yield* loadManifest(p);
  if (["applied", "verified", "activated"].includes(run.state))
    return { state: run.state, applied: 0 };
  if (run.state !== "applying") return yield* new TransitionError({ reason: "import_fenced" });
  let applied = 0;
  for (const c of p.snapshot.classes) {
    for (const row of p.snapshot.rows.filter((r) => r.class === c.class)) {
      const key = canonical(row.key),
        scope = {
          ...archiveScope(p),
          owner_id: String(row.row.owner_id ?? archiveScope(p).owner_id),
        };
      const prior = yield* archive.readArchive(scope, p.import_run_id, row.class, key);
      if (prior !== null) {
        if (canonical((prior as { source: unknown }).source) !== canonical(row))
          return yield* new TransitionError({ reason: "source_conflict" });
        continue;
      }
      if (applied >= limit) return { state: "applying", applied };
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const targets = yield* targetsFor(p, row);
          for (const t of targets) yield* insertTarget(t);
          yield* archive.archive(scope, {
            import_run_id: p.import_run_id,
            class: row.class,
            record_key: key,
            canonical_sha256: digest(canonical(row)),
            value: { source: row, targets },
          });
          yield* sql`UPDATE jones_workstreams_import_classes SET state='applying',applied_count=applied_count+1,last_key=${key} WHERE import_run_id=${p.import_run_id} AND class=${row.class}`;
        }),
      );
      applied++;
    }
    const states = yield* sql<{
      state: string;
      applied_count: number;
    }>`SELECT state,applied_count FROM jones_workstreams_import_classes WHERE import_run_id=${p.import_run_id} AND class=${c.class}`;
    if (states[0]!.applied_count !== c.count)
      return yield* new TransitionError({ reason: "incomplete_class" });
    if (states[0]!.state !== "applied" && states[0]!.state !== "verified")
      yield* sql`UPDATE jones_workstreams_import_classes SET state='applied' WHERE import_run_id=${p.import_run_id} AND class=${c.class}`;
  }
  yield* sql`UPDATE jones_workstreams_import_runs SET state='applied' WHERE import_run_id=${p.import_run_id} AND state='applying'`;
  return { state: "applied", applied };
});
