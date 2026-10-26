import { WORKSTREAMS_SNAPSHOT_CLASSES } from "@t3tools/contracts/jones/workstreamsSnapshot";
import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import migration, { nativeTableNames } from "./125_JonesWorkstreamsNative.ts";

const scope = { registry_instance_id: "registry", owner_id: "owner" };
const hash = "a".repeat(64);
const envelope = JSON.stringify({
  v: 1,
  alg: "A256GCM",
  key_id: "synthetic",
  nonce: "a".repeat(16),
  ct: "",
  tag: "a".repeat(24),
});
const rows = {
  registries: {
    ...scope,
    canonical_origin: "https://registry.invalid",
    home_environment_id: "home",
    server_generation: 1,
    registry_version: 0,
    lineage: "native",
    active_key_id: "key",
    state: "active",
  },
  keys: {
    key_id: "key",
    registry_instance_id: "registry",
    scope: "home",
    state: "active",
    created_at: "now",
  },
  principals: {
    ...scope,
    principal_id: "principal",
    environment_id: "source",
    kind: "environment",
    provenance: "{}",
  },
  grants: {
    ...scope,
    principal_id: "principal",
    grant_version: 1,
    grant_id: "grant",
    permissions: "[]",
    state: "active",
  },
  source_bindings: {
    ...scope,
    provider: "t3",
    source_instance_id: "source",
    binding_version: 1,
    authority_namespace: "authority",
    store_generation: 1,
    state: "active",
  },
  records: { ...scope, workstream_id: "ws", version: 1, sort_order: 0, envelope },
  references: {
    ...scope,
    native_reference_id: "ref",
    provider: "t3",
    resource_kind: "thread",
    id_kind: "internal",
    identity_index: hash,
    legacy_identity_sha256: null,
    attestation_version: 0,
    registration_state: "quarantined",
    created_by_principal: "principal",
    envelope,
  },
  memberships: {
    ...scope,
    membership_id: "membership",
    workstream_id: "ws",
    native_reference_id: "ref",
    kind: "primary",
    opened_registry_version: 1,
    closed_registry_version: null,
    closed_at: null,
    envelope,
  },
  declarations: {
    ...scope,
    declaration_id: "declaration",
    revision: 1,
    workstream_id: "ws",
    registry_version: 1,
    state: "active",
    envelope,
  },
  lifecycle_declarations: {
    ...scope,
    lifecycle_declaration_id: "lifecycle",
    workstream_id: "ws",
    revision: 1,
    registry_version: 1,
    recorded_at: "now",
    value: "{}",
  },
  edges: {
    ...scope,
    edge_id: "edge",
    from_workstream_id: "ws",
    to_workstream_id: "other",
    relation: "continues_as",
    opened_registry_version: 1,
    closed_registry_version: null,
    closed_at: null,
    value: "{}",
  },
  pr_observations: {
    ...scope,
    native_reference_id: "ref",
    observation_version: 1,
    attempted_at: "now",
    outcome: "observed",
    envelope,
  },
  receipts: {
    ...scope,
    command_id: "command",
    principal_id: "principal",
    operation: "verify_reference",
    state: "pending",
    request_sha256: hash,
    server_generation: 1,
    registry_version: null,
    accepted_at: "now",
    completed_at: null,
    origin: "native",
    envelope,
  },
  external_reservations: {
    ...scope,
    command_id: "command",
    principal_id: "principal",
    native_reference_id: "ref",
    effect: "verify_reference",
    server_generation: 1,
    admitted_registry_version: 2,
    state: "active",
    envelope,
  },
  external_results: { ...scope, command_id: "command", envelope },
  history: {
    ...scope,
    event_id: "event",
    event_sequence: 1,
    legacy_event_sequence: null,
    command_id: "command",
    registry_version: 1,
    occurred_at: "now",
    workstream_ids: '["ws"]',
    envelope,
  },
  history_scopes: {
    ...scope,
    scope_key: "global",
    registry_version: 1,
    event_id: "event",
    event_sequence: 1,
  },
  appearance: { ...scope, workstream_id: "ws", version: 1, envelope },
  appearance_receipts: {
    ...scope,
    command_id: "appearance",
    principal_id: "principal",
    request_sha256: hash,
    origin: "native",
    envelope,
  },
  legacy_receipts: {
    ...scope,
    command_id: "legacy",
    archive_version: 1,
    import_run_id: "run",
    principal_id: "principal",
    operation: "create_workstream",
    state: "committed",
    request_sha256: hash,
    server_generation: 1,
    registry_version: 1,
    original_ciphertext_sha256: hash,
    canonical_sha256: hash,
    envelope,
  },
  legacy_archive: {
    import_run_id: "run",
    class: "workstreams",
    record_key: "ws",
    archive_version: 1,
    canonical_sha256: hash,
    envelope,
  },
  import_runs: {
    import_run_id: "run",
    registry_instance_id: "registry",
    envelope_owner_id: "owner",
    snapshot_id: "snapshot",
    snapshot_sha256: hash,
    producer: "control-plane",
    contract_version: "1.0.0",
    state: "planned",
    envelope,
  },
  import_classes: {
    import_run_id: "run",
    class: "workstream_command_receipts",
    expected_count: 0,
    expected_sha256: hash,
    applied_count: 0,
    last_key: null,
    observed_sha256: null,
    state: "planned",
    conflict_envelope: null,
  },
  rollup_bindings: {
    ...scope,
    workstream_id: "ws",
    native_reference_id: "ref",
    binding_version: 1,
    provider_id: "cp-conversation-intelligence",
    state: "active",
    envelope,
  },
  rollup_manifests: {
    ...scope,
    workstream_id: "ws",
    manifest_sha256: hash,
    workstream_version: 1,
    registry_version: 1,
    envelope,
  },
  rollup_manifest_sources: {
    ...scope,
    workstream_id: "ws",
    manifest_sha256: hash,
    provider_id: "cp-conversation-intelligence",
    account_id: "opaque",
    authorization_policy_sha256: hash,
    authorization_principal_id: "principal",
    authorization_grant_id: "grant",
    authorization_decision_id: "decision",
    authorization_account_generation: 1,
    authorization_revision: 1,
  },
  rollup_queue: {
    ...scope,
    workstream_id: "ws",
    manifest_sha256: hash,
    state: "pending",
    attempt_id: null,
    fence: 0,
    lease_expires_at: null,
    failure_code: null,
    failed_at: null,
    updated_at: "now",
  },
  rollup_discovery_cursors: { cursor_id: "cursor", last_group_key: null, updated_at: "now" },
  rollup_summaries: {
    ...scope,
    workstream_id: "ws",
    rollup_version: 1,
    manifest_sha256: hash,
    output_sha256: hash,
    coverage: "{}",
    freshness: "current",
    generated_at: "now",
    envelope,
  },
  binding_state: {
    singleton: 1,
    ...scope,
    home_environment_id: "home",
    home_generation: 1,
    principal_id: "principal",
    binding_version: 1,
    state: "active",
  },
  operations: {
    operation_id: "operation",
    kind: "set_primary",
    thread_environment_id: "source",
    thread_id: "thread",
    origin: '{"kind":"app"}',
    request_sha256: hash,
    dest_registry_instance_id: "registry",
    dest_owner_id: "owner",
    dest_home_environment_id: "home",
    dest_home_generation: 1,
    binding_version: 1,
    state: "accepted",
    child_command_ids: "[]",
    accepted_at: "now",
    updated_at: "now",
    next_poll_at: null,
    request_envelope: envelope,
    result_envelope: null,
  },
};
const seed = Effect.gen(function* () {
  yield* migration;
  const sql = yield* SqlClient.SqlClient;
  for (const [suffix, row] of Object.entries(rows))
    yield* sql`INSERT INTO ${sql(`jones_workstreams_${suffix}`)} ${sql.insert(row)}`;
});
const memory = NodeSqliteClient.layer({ filename: ":memory:" });

it.effect(
  "all 31 tables preserve history against delete, replacement and identity mutation without upstream foreign keys",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const sql = yield* SqlClient.SqlClient;
      expect(nativeTableNames).toHaveLength(31);
      for (const [suffix, row] of Object.entries(rows)) {
        const table = `jones_workstreams_${suffix}`;
        expect(yield* sql.unsafe(`PRAGMA foreign_key_list(${table})`)).toEqual([]);
        const before = yield* sql`SELECT * FROM ${sql(table)}`;
        expect(Exit.isFailure(yield* Effect.exit(sql`DELETE FROM ${sql(table)}`))).toBe(true);
        yield* sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert(row)}`;
        expect(yield* sql`SELECT * FROM ${sql(table)}`).toEqual(before);
        const key = (yield* sql.unsafe<{ name: string; pk: number }>(
          `PRAGMA table_info(${table})`,
        )).find((column) => column.pk === 1)!.name;
        expect(
          Exit.isFailure(yield* Effect.exit(sql.unsafe(`UPDATE ${table} SET ${key} = 'changed'`))),
        ).toBe(true);
      }
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "registry and append-only versions advance exactly once, while generations retain registry CAS",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const sql = yield* SqlClient.SqlClient;
      expect((yield* sql`SELECT registry_version FROM jones_workstreams_registries`)[0]).toEqual({
        registry_version: 2,
      });
      for (const bad of [
        sql`UPDATE jones_workstreams_registries SET registry_version = 4`,
        sql`UPDATE jones_workstreams_registries SET server_generation = 2, registry_version = 3`,
        sql`UPDATE jones_workstreams_records SET version = 3`,
        sql`UPDATE jones_workstreams_appearance SET version = 1`,
        sql`UPDATE jones_workstreams_references SET attestation_version = 2`,
        sql`UPDATE jones_workstreams_rollup_bindings SET binding_version = 3`,
        sql`UPDATE jones_workstreams_keys SET state = 'active'`,
        sql`UPDATE jones_workstreams_binding_state SET home_generation = 2`,
      ])
        expect(Exit.isFailure(yield* Effect.exit(bad))).toBe(true);
      yield* sql`UPDATE jones_workstreams_registries SET server_generation = 2`;
      yield* sql`UPDATE jones_workstreams_records SET version = 2`;
      yield* sql`UPDATE jones_workstreams_appearance SET version = 2`;
      yield* sql`UPDATE jones_workstreams_keys SET state = 'decrypt_only'`;
      for (const [table, column, changed] of [
        ["grants", "grant_version", { grant_version: 3 }],
        ["source_bindings", "binding_version", { binding_version: 3 }],
        ["declarations", "revision", { revision: 3 }],
        ["lifecycle_declarations", "revision", { lifecycle_declaration_id: "new", revision: 3 }],
        ["pr_observations", "observation_version", { observation_version: 3 }],
        [
          "rollup_summaries",
          "rollup_version",
          { rollup_version: 3, manifest_sha256: "b".repeat(64) },
        ],
      ] as const) {
        const row = { ...rows[table], ...changed };
        expect(
          Exit.isFailure(
            yield* Effect.exit(
              sql`INSERT INTO ${sql(`jones_workstreams_${table}`)} ${sql.insert(row)}`,
            ),
          ),
        ).toBe(true);
        yield* sql`INSERT INTO ${sql(`jones_workstreams_${table}`)} ${sql.insert({ ...row, [column]: 2 })}`;
      }
      expect(
        (yield* sql`SELECT registry_version,server_generation FROM jones_workstreams_registries`)[0],
      ).toEqual({ registry_version: 4, server_generation: 2 });
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "closure, membership uniqueness, attested routing and cross-relation cycles are guarded",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const sql = yield* SqlClient.SqlClient;
      for (const bad of [
        sql`INSERT INTO jones_workstreams_memberships ${sql.insert({ ...rows.memberships, membership_id: "second", workstream_id: "other" })}`,
        sql`INSERT INTO jones_workstreams_memberships ${sql.insert({ ...rows.memberships, membership_id: "association", kind: "secondary" })}`,
        sql`INSERT INTO jones_workstreams_edges ${sql.insert({ ...rows.edges, edge_id: "cycle", from_workstream_id: "other", to_workstream_id: "ws", relation: "superseded_by" })}`,
        sql`UPDATE jones_workstreams_memberships SET envelope = ${envelope}`,
        sql`UPDATE jones_workstreams_edges SET value = '{}'`,
        sql`UPDATE jones_workstreams_references SET attestation_version = 1, routing_source_instance_id = 'source'`,
        sql`INSERT INTO jones_workstreams_records ${sql.insert({ ...rows.records, workstream_id: "plaintext", envelope: "{}" })}`,
        sql`INSERT INTO jones_workstreams_history_scopes ${sql.insert({ ...rows.history_scopes, scope_key: "workstream:unrelated" })}`,
      ])
        expect(Exit.isFailure(yield* Effect.exit(bad))).toBe(true);
      yield* sql`UPDATE jones_workstreams_memberships SET closed_registry_version = 2, closed_at = 'later'`;
      yield* sql`UPDATE jones_workstreams_edges SET closed_registry_version = 2, closed_at = 'later'`;
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE jones_workstreams_memberships SET closed_registry_version = NULL, closed_at = NULL`,
          ),
        ),
      ).toBe(true);
      expect(
        Exit.isFailure(
          yield* Effect.exit(sql`UPDATE jones_workstreams_edges SET closed_registry_version = 3`),
        ),
      ).toBe(true);
      yield* sql`INSERT INTO jones_workstreams_memberships ${sql.insert({ ...rows.memberships, membership_id: "reattached" })}`;
      yield* sql`INSERT INTO jones_workstreams_edges ${sql.insert({ ...rows.edges, edge_id: "reverse", from_workstream_id: "other", to_workstream_id: "ws" })}`;
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "receipt completion, reservation release, import activation, queue fencing and operation transitions cannot skip their guards",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const sql = yield* SqlClient.SqlClient;
      for (const bad of [
        sql`UPDATE jones_workstreams_external_reservations SET state = 'released'`,
        sql`INSERT INTO jones_workstreams_external_reservations ${sql.insert({ ...rows.external_reservations, command_id: "missing" })}`,
        sql`INSERT INTO jones_workstreams_external_results ${sql.insert({ ...rows.external_results, command_id: "missing" })}`,
        sql`UPDATE jones_workstreams_import_runs SET state = 'activated'`,
        sql`UPDATE jones_workstreams_import_classes SET state = 'verified'`,
        sql`UPDATE jones_workstreams_rollup_queue SET state = 'running', attempt_id = 'attempt', lease_expires_at = 'later'`,
        sql`UPDATE jones_workstreams_operations SET state = 'committed'`,
        sql`INSERT INTO jones_workstreams_operations ${sql.insert({ ...rows.operations, operation_id: "overlap" })}`,
      ])
        expect(Exit.isFailure(yield* Effect.exit(bad))).toBe(true);
      yield* sql`UPDATE jones_workstreams_receipts SET state = 'committed', completed_at = 'later', registry_version = 2`;
      yield* sql`UPDATE jones_workstreams_external_reservations SET state = 'released'`;
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE jones_workstreams_receipts SET state = 'pending', completed_at = NULL`,
          ),
        ),
      ).toBe(true);
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE jones_workstreams_external_reservations SET state = 'active'`,
          ),
        ),
      ).toBe(true);
      yield* sql`UPDATE jones_workstreams_import_runs SET state = 'applying'`;
      yield* sql`UPDATE jones_workstreams_import_runs SET state = 'applied'`;
      yield* sql`UPDATE jones_workstreams_import_runs SET state = 'verified'`;
      expect(
        Exit.isFailure(
          yield* Effect.exit(sql`UPDATE jones_workstreams_import_runs SET state = 'activated'`),
        ),
      ).toBe(true);
      for (const className of WORKSTREAMS_SNAPSHOT_CLASSES.filter(
        (name) => name !== "workstream_command_receipts",
      ))
        yield* sql`INSERT INTO jones_workstreams_import_classes ${sql.insert({ ...rows.import_classes, class: className })}`;
      yield* sql`UPDATE jones_workstreams_import_classes SET state = 'applied'`;
      yield* sql`UPDATE jones_workstreams_import_classes SET state = 'verified', observed_sha256 = ${hash}`;
      yield* sql`UPDATE jones_workstreams_import_runs SET state = 'activated'`;
      yield* sql`UPDATE jones_workstreams_rollup_queue SET state = 'running', attempt_id = 'attempt', fence = 1, lease_expires_at = 'later'`;
      expect(
        Exit.isFailure(
          yield* Effect.exit(sql`UPDATE jones_workstreams_rollup_queue SET attempt_id = 'new'`),
        ),
      ).toBe(true);
      yield* sql`UPDATE jones_workstreams_operations SET state = 'dispatching'`;
      expect(
        Exit.isFailure(
          yield* Effect.exit(sql`UPDATE jones_workstreams_operations SET state = 'unavailable'`),
        ),
      ).toBe(true);
      yield* sql`UPDATE jones_workstreams_operations SET state = 'reconciling'`;
      yield* sql`UPDATE jones_workstreams_operations SET state = 'committed'`;
      expect(
        Exit.isFailure(
          yield* Effect.exit(sql`UPDATE jones_workstreams_operations SET state = 'dispatching'`),
        ),
      ).toBe(true);
    }).pipe(Effect.provide(memory)),
);

it.effect("the real Jones loader applies 125 without changing upstream schema or its ledger", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    const before = yield* sql<{
      type: string;
      name: string;
      tbl_name: string;
      sql: string | null;
    }>`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`;
    const ledger = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
    yield* runJonesMigrations(jonesMigrationEntries);
    for (const object of before)
      expect(
        yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type = ${object.type} AND name = ${object.name}`,
      ).toEqual([object]);
    expect(yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`).toEqual(ledger);
    expect(
      yield* sql`SELECT migration_id,name FROM jones_sql_migrations WHERE migration_id = 125`,
    ).toEqual([{ migration_id: 125, name: "WorkstreamsNative" }]);
    for (const table of nativeTableNames)
      expect(yield* sql.unsafe(`PRAGMA foreign_key_list(${table})`)).toEqual([]);
  }).pipe(Effect.provide(memory)),
);
