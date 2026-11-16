import { WORKSTREAMS_SNAPSHOT_CLASSES } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

const scope = "registry_instance_id TEXT NOT NULL, owner_id TEXT NOT NULL";
const owner = "registry_instance_id, owner_id";
const env = (name = "envelope", nullable = false) =>
  `${name} TEXT ${nullable ? "" : "NOT NULL"} CHECK (${nullable ? `${name} IS NULL OR ` : ""}coalesce((json_valid(${name}) AND json_extract(${name}, '$.v') = 1 AND json_extract(${name}, '$.alg') = 'A256GCM' AND json_type(${name}, '$.key_id') = 'text' AND json_type(${name}, '$.nonce') = 'text' AND length(json_extract(${name}, '$.nonce')) = 16 AND json_type(${name}, '$.ct') = 'text' AND json_type(${name}, '$.tag') = 'text' AND length(json_extract(${name}, '$.tag')) = 24), 0))`;
interface Table {
  readonly columns: string;
  readonly key: string;
  readonly mutable?: readonly string[];
  readonly guard?: string;
}
const tables = {
  registries: {
    columns: `${scope}, canonical_origin TEXT NOT NULL, home_environment_id TEXT NOT NULL,
    server_generation INTEGER NOT NULL CHECK(server_generation >= 1), registry_version INTEGER NOT NULL CHECK(registry_version >= 0),
    lineage TEXT NOT NULL CHECK(lineage IN ('native','imported','transferred')), active_key_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active','importing','fenced','transferred_out'))`,
    key: owner,
    mutable: [
      "home_environment_id",
      "server_generation",
      "registry_version",
      "active_key_id",
      "state",
    ],
    guard:
      "NOT ((NEW.server_generation = OLD.server_generation AND NEW.registry_version = OLD.registry_version + 1 AND NEW.home_environment_id = OLD.home_environment_id) OR (NEW.server_generation = OLD.server_generation + 1 AND NEW.registry_version = OLD.registry_version) OR (NEW.server_generation = OLD.server_generation AND NEW.registry_version = OLD.registry_version AND NEW.home_environment_id = OLD.home_environment_id AND NEW.active_key_id = OLD.active_key_id AND ((OLD.state = 'importing' AND NEW.state = 'active') OR (OLD.state = 'active' AND NEW.state = 'fenced') OR (OLD.state = 'fenced' AND NEW.state = 'transferred_out'))))",
  },
  keys: {
    columns:
      "key_id TEXT NOT NULL, registry_instance_id TEXT NOT NULL, scope TEXT NOT NULL CHECK(scope IN ('home','local')), state TEXT NOT NULL CHECK(state IN ('active','decrypt_only','retired')), created_at TEXT NOT NULL",
    key: "key_id",
    mutable: ["state"],
    guard:
      "NOT ((OLD.state = 'active' AND NEW.state IN ('decrypt_only','retired')) OR (OLD.state = 'decrypt_only' AND NEW.state = 'retired'))",
  },
  principals: {
    columns: `${scope}, principal_id TEXT NOT NULL, environment_id TEXT, kind TEXT NOT NULL CHECK(kind IN ('environment','legacy_browser','legacy_unmapped')), provenance TEXT NOT NULL CHECK(json_valid(provenance)), CHECK((kind = 'environment') = (environment_id IS NOT NULL))`,
    key: `${owner},principal_id`,
  },
  grants: {
    columns: `${scope}, principal_id TEXT NOT NULL, grant_version INTEGER NOT NULL CHECK(grant_version >= 1), grant_id TEXT NOT NULL, permissions TEXT NOT NULL CHECK(json_valid(permissions)), state TEXT NOT NULL CHECK(state IN ('active','revoked'))`,
    key: `${owner},principal_id,grant_version`,
  },
  source_bindings: {
    columns: `${scope}, provider TEXT NOT NULL, source_instance_id TEXT NOT NULL, binding_version INTEGER NOT NULL CHECK(binding_version >= 1), authority_namespace TEXT NOT NULL, store_generation INTEGER NOT NULL CHECK(store_generation >= 1), state TEXT NOT NULL CHECK(state IN ('active','revoked'))`,
    key: `${owner},provider,source_instance_id,binding_version`,
  },
  records: {
    columns: `${scope}, workstream_id TEXT NOT NULL, version INTEGER NOT NULL CHECK(version >= 1), sort_order INTEGER NOT NULL CHECK(sort_order >= 0 AND sort_order <= 2147483647), ${env()}`,
    key: `${owner},workstream_id`,
    mutable: ["version", "sort_order", "envelope"],
    guard: "NEW.version != OLD.version + 1",
  },
  references: {
    columns: `${scope}, native_reference_id TEXT NOT NULL, provider TEXT NOT NULL, resource_kind TEXT NOT NULL, id_kind TEXT NOT NULL CHECK(id_kind IN ('internal','external')), identity_index TEXT NOT NULL CHECK(length(identity_index) = 64), legacy_identity_sha256 TEXT,
    attestation_version INTEGER NOT NULL CHECK(attestation_version >= 0), registration_state TEXT NOT NULL CHECK(registration_state IN ('quarantined','verification-pending','attested','verification-failed','expired','re-attestation-required')),
    created_by_principal TEXT NOT NULL, routing_source_instance_id TEXT, routing_native_id TEXT, routing_authority_namespace TEXT,
    routing_store_generation INTEGER CHECK(routing_store_generation >= 1), routing_attested_at TEXT, routing_expires_at TEXT, routing_evidence_sha256 TEXT, ${env()},
    CHECK((routing_source_instance_id IS NULL AND routing_native_id IS NULL AND routing_authority_namespace IS NULL AND routing_store_generation IS NULL AND routing_attested_at IS NULL AND routing_expires_at IS NULL AND routing_evidence_sha256 IS NULL) OR
    (routing_source_instance_id IS NOT NULL AND routing_native_id IS NOT NULL AND routing_authority_namespace IS NOT NULL AND routing_store_generation IS NOT NULL AND routing_attested_at IS NOT NULL AND routing_expires_at IS NOT NULL AND routing_evidence_sha256 IS NOT NULL AND length(routing_evidence_sha256) = 64 AND registration_state = 'attested' AND attestation_version >= 1 AND provider = 't3' AND resource_kind = 'thread' AND id_kind = 'internal'))`,
    key: `${owner},native_reference_id`,
    mutable: [
      "attestation_version",
      "registration_state",
      "routing_source_instance_id",
      "routing_native_id",
      "routing_authority_namespace",
      "routing_store_generation",
      "routing_attested_at",
      "routing_expires_at",
      "routing_evidence_sha256",
      "envelope",
    ],
    guard: "NEW.attestation_version != OLD.attestation_version + 1",
  },
  memberships: {
    columns: `${scope}, membership_id TEXT NOT NULL, workstream_id TEXT NOT NULL, native_reference_id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('primary','secondary')),
    opened_registry_version INTEGER NOT NULL CHECK(opened_registry_version >= 0), closed_registry_version INTEGER, closed_at TEXT, ${env()},
    CHECK((closed_registry_version IS NULL) = (closed_at IS NULL)), CHECK(closed_registry_version IS NULL OR closed_registry_version >= opened_registry_version)`,
    key: `${owner},membership_id`,
    mutable: ["closed_registry_version", "closed_at", "envelope"],
    guard: "OLD.closed_registry_version IS NOT NULL OR NEW.closed_registry_version IS NULL",
  },
  declarations: {
    columns: `${scope}, declaration_id TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision >= 1), workstream_id TEXT NOT NULL, registry_version INTEGER NOT NULL CHECK(registry_version >= 0), state TEXT NOT NULL CHECK(state IN ('active','withdrawn')), ${env()}`,
    key: `${owner},declaration_id,revision`,
  },
  lifecycle_declarations: {
    columns: `${scope}, lifecycle_declaration_id TEXT NOT NULL, workstream_id TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision >= 1), registry_version INTEGER NOT NULL CHECK(registry_version >= 0), recorded_at TEXT NOT NULL, value TEXT NOT NULL CHECK(json_valid(value)), UNIQUE(${owner},workstream_id,revision)`,
    key: `${owner},lifecycle_declaration_id`,
  },
  edges: {
    columns: `${scope}, edge_id TEXT NOT NULL, from_workstream_id TEXT NOT NULL, to_workstream_id TEXT NOT NULL, relation TEXT NOT NULL CHECK(relation IN ('continues_as','superseded_by')), opened_registry_version INTEGER NOT NULL CHECK(opened_registry_version >= 0), closed_registry_version INTEGER, closed_at TEXT, value TEXT NOT NULL CHECK(json_valid(value)), CHECK(from_workstream_id != to_workstream_id), CHECK((closed_registry_version IS NULL) = (closed_at IS NULL)), CHECK(closed_registry_version IS NULL OR closed_registry_version >= opened_registry_version)`,
    key: `${owner},edge_id`,
    mutable: ["closed_registry_version", "closed_at", "value"],
    guard: "OLD.closed_registry_version IS NOT NULL OR NEW.closed_registry_version IS NULL",
  },
  pr_observations: {
    columns: `${scope}, native_reference_id TEXT NOT NULL, observation_version INTEGER NOT NULL CHECK(observation_version >= 1), attempted_at TEXT NOT NULL, outcome TEXT NOT NULL, ${env()}`,
    key: `${owner},native_reference_id,observation_version`,
  },
  receipts: {
    columns: `${scope}, command_id TEXT NOT NULL, principal_id TEXT NOT NULL, operation TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','unresolved','committed','rejected')), request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64), server_generation INTEGER NOT NULL CHECK(server_generation >= 1), registry_version INTEGER CHECK(registry_version >= 0), accepted_at TEXT NOT NULL, completed_at TEXT, origin TEXT NOT NULL CHECK(origin IN ('native','legacy_open')), ${env()}, CHECK((state IN ('committed','rejected')) = (completed_at IS NOT NULL))`,
    key: `${owner},command_id`,
    mutable: ["state", "registry_version", "completed_at", "envelope"],
    guard:
      "OLD.state IN ('committed','rejected') OR NEW.state NOT IN ('pending','unresolved','committed','rejected')",
  },
  external_reservations: {
    columns: `${scope}, command_id TEXT NOT NULL, principal_id TEXT NOT NULL, native_reference_id TEXT NOT NULL, effect TEXT NOT NULL CHECK(effect IN ('verify_reference','request_native_t3_settlement','refresh_linked_pr')), server_generation INTEGER NOT NULL CHECK(server_generation >= 1), admitted_registry_version INTEGER NOT NULL CHECK(admitted_registry_version >= 0), state TEXT NOT NULL CHECK(state IN ('active','released')), ${env()}`,
    key: `${owner},command_id`,
    mutable: ["state"],
    guard:
      "OLD.state != 'active' OR NEW.state != 'released' OR NOT EXISTS(SELECT 1 FROM jones_workstreams_receipts r WHERE r.registry_instance_id = OLD.registry_instance_id AND r.owner_id = OLD.owner_id AND r.command_id = OLD.command_id AND r.state IN ('committed','rejected'))",
  },
  external_results: {
    columns: `${scope}, command_id TEXT NOT NULL, ${env()}`,
    key: `${owner},command_id`,
  },
  history: {
    columns: `${scope}, event_id TEXT NOT NULL, event_sequence INTEGER NOT NULL CHECK(event_sequence >= 1), legacy_event_sequence INTEGER, command_id TEXT NOT NULL, registry_version INTEGER NOT NULL CHECK(registry_version >= 0), occurred_at TEXT NOT NULL, workstream_ids TEXT NOT NULL CHECK(json_valid(workstream_ids) AND json_type(workstream_ids) = 'array'), ${env()}, UNIQUE(${owner},event_sequence)`,
    key: `${owner},event_id`,
  },
  history_scopes: {
    columns: `${scope}, scope_key TEXT NOT NULL CHECK(scope_key = 'global' OR scope_key LIKE 'workstream:%'), registry_version INTEGER NOT NULL CHECK(registry_version >= 0), event_id TEXT NOT NULL, event_sequence INTEGER NOT NULL CHECK(event_sequence >= 1)`,
    key: `${owner},scope_key,registry_version,event_id`,
  },
  appearance: {
    columns: `${scope}, workstream_id TEXT NOT NULL, version INTEGER NOT NULL CHECK(version >= 1), ${env()}`,
    key: `${owner},workstream_id`,
    mutable: ["version", "envelope"],
    guard: "NEW.version != OLD.version + 1",
  },
  appearance_receipts: {
    columns: `${scope}, command_id TEXT NOT NULL, principal_id TEXT NOT NULL, request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64), origin TEXT NOT NULL CHECK(origin IN ('native','legacy')), ${env()}`,
    key: `${owner},command_id`,
  },
  legacy_receipts: {
    columns: `${scope}, command_id TEXT NOT NULL, archive_version INTEGER NOT NULL CHECK(archive_version = 1), import_run_id TEXT NOT NULL, principal_id TEXT NOT NULL, operation TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('committed','rejected')), request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64), server_generation INTEGER NOT NULL CHECK(server_generation >= 1), registry_version INTEGER CHECK(registry_version >= 0), original_ciphertext_sha256 TEXT NOT NULL CHECK(length(original_ciphertext_sha256)=64), canonical_sha256 TEXT NOT NULL CHECK(length(canonical_sha256)=64), ${env()}`,
    key: `${owner},command_id`,
  },
  legacy_archive: {
    columns: `import_run_id TEXT NOT NULL, class TEXT NOT NULL, record_key TEXT NOT NULL, archive_version INTEGER NOT NULL CHECK(archive_version = 1), canonical_sha256 TEXT NOT NULL CHECK(length(canonical_sha256)=64), ${env()}`,
    key: "import_run_id,class,record_key",
  },
  import_runs: {
    columns: `import_run_id TEXT NOT NULL, registry_instance_id TEXT NOT NULL, envelope_owner_id TEXT NOT NULL, snapshot_id TEXT NOT NULL, snapshot_sha256 TEXT NOT NULL CHECK(length(snapshot_sha256)=64), producer TEXT NOT NULL, contract_version TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('planned','applying','applied','verified','activated','conflict','failed','abandoned')), ${env()}`,
    key: "import_run_id",
    mutable: ["state", "envelope"],
    guard:
      "NOT ((OLD.state = 'planned' AND NEW.state IN ('applying','conflict','failed','abandoned')) OR (OLD.state = 'applying' AND NEW.state IN ('applying','applied','conflict','failed','abandoned')) OR (OLD.state = 'applied' AND NEW.state IN ('verified','conflict','failed','abandoned')) OR (OLD.state = 'verified' AND NEW.state IN ('activated','conflict','failed','abandoned')))",
  },
  import_classes: {
    columns: `import_run_id TEXT NOT NULL, class TEXT NOT NULL, expected_count INTEGER NOT NULL CHECK(expected_count >= 0), expected_sha256 TEXT NOT NULL CHECK(length(expected_sha256)=64), applied_count INTEGER NOT NULL CHECK(applied_count >= 0 AND applied_count <= expected_count), last_key TEXT, observed_sha256 TEXT, state TEXT NOT NULL CHECK(state IN ('planned','applying','applied','verified','conflict')), ${env("conflict_envelope", true)}, CHECK(state != 'verified' OR (applied_count = expected_count AND observed_sha256 IS NOT NULL AND observed_sha256 = expected_sha256))`,
    key: "import_run_id,class",
    mutable: ["applied_count", "last_key", "observed_sha256", "state", "conflict_envelope"],
    guard:
      "OLD.state IN ('verified','conflict') OR NEW.applied_count < OLD.applied_count OR NOT ((OLD.state = 'planned' AND NEW.state IN ('applying','applied','conflict')) OR (OLD.state = 'applying' AND NEW.state IN ('applying','applied','conflict')) OR (OLD.state = 'applied' AND NEW.state IN ('verified','conflict')))",
  },
  rollup_bindings: {
    columns: `${scope}, workstream_id TEXT NOT NULL, native_reference_id TEXT NOT NULL, binding_version INTEGER NOT NULL CHECK(binding_version >= 1), provider_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('active','revoked')), ${env()}`,
    key: `${owner},workstream_id,native_reference_id`,
    mutable: ["binding_version", "provider_id", "state", "envelope"],
    guard: "NEW.binding_version != OLD.binding_version + 1",
  },
  rollup_manifests: {
    columns: `${scope}, workstream_id TEXT NOT NULL, manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64), workstream_version INTEGER NOT NULL CHECK(workstream_version >= 1), registry_version INTEGER NOT NULL CHECK(registry_version >= 0), ${env()}`,
    key: `${owner},workstream_id,manifest_sha256`,
  },
  rollup_manifest_sources: {
    columns: `${scope}, workstream_id TEXT NOT NULL, manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64), provider_id TEXT NOT NULL, account_id TEXT NOT NULL, authorization_policy_sha256 TEXT NOT NULL CHECK(length(authorization_policy_sha256)=64), authorization_principal_id TEXT NOT NULL, authorization_grant_id TEXT NOT NULL, authorization_decision_id TEXT NOT NULL, authorization_account_generation INTEGER NOT NULL CHECK(authorization_account_generation >= 1), authorization_revision INTEGER NOT NULL CHECK(authorization_revision >= 1)`,
    key: `${owner},workstream_id,manifest_sha256,provider_id,account_id`,
  },
  rollup_queue: {
    columns: `${scope}, workstream_id TEXT NOT NULL, manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64), state TEXT NOT NULL CHECK(state IN ('pending','running','dead_letter')), attempt_id TEXT, fence INTEGER NOT NULL CHECK(fence >= 0), lease_expires_at TEXT, failure_code TEXT, failed_at TEXT, updated_at TEXT NOT NULL, CHECK((state = 'running') = (attempt_id IS NOT NULL AND lease_expires_at IS NOT NULL)), CHECK(state = 'running' OR (attempt_id IS NULL AND lease_expires_at IS NULL)), CHECK((state = 'dead_letter') = (failure_code IS NOT NULL AND failed_at IS NOT NULL)), CHECK(state = 'dead_letter' OR (failure_code IS NULL AND failed_at IS NULL))`,
    key: `${owner},workstream_id`,
    mutable: [
      "manifest_sha256",
      "state",
      "attempt_id",
      "fence",
      "lease_expires_at",
      "failure_code",
      "failed_at",
      "updated_at",
    ],
    guard:
      "NEW.fence < OLD.fence OR NEW.fence > OLD.fence + 1 OR (NEW.state = 'running' AND OLD.state != 'running' AND NEW.fence != OLD.fence + 1) OR (OLD.state = 'running' AND NEW.state = 'running' AND (NEW.attempt_id != OLD.attempt_id OR NEW.fence != OLD.fence OR NEW.manifest_sha256 != OLD.manifest_sha256))",
  },
  rollup_discovery_cursors: {
    columns: "cursor_id TEXT NOT NULL, last_group_key TEXT, updated_at TEXT NOT NULL",
    key: "cursor_id",
    mutable: ["last_group_key", "updated_at"],
  },
  rollup_summaries: {
    columns: `${scope}, workstream_id TEXT NOT NULL, rollup_version INTEGER NOT NULL CHECK(rollup_version >= 1), manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64), output_sha256 TEXT NOT NULL CHECK(length(output_sha256)=64), coverage TEXT NOT NULL, freshness TEXT NOT NULL, generated_at TEXT NOT NULL, ${env()}, UNIQUE(${owner},workstream_id,manifest_sha256)`,
    key: `${owner},workstream_id,rollup_version`,
  },
  binding_state: {
    columns:
      "singleton INTEGER NOT NULL CHECK(singleton = 1), registry_instance_id TEXT, owner_id TEXT, home_environment_id TEXT, home_generation INTEGER CHECK(home_generation >= 1), principal_id TEXT, binding_version INTEGER NOT NULL CHECK(binding_version >= 1), state TEXT NOT NULL CHECK(state IN ('unbound','active','fenced')), CHECK(state = 'unbound' OR (registry_instance_id IS NOT NULL AND owner_id IS NOT NULL AND home_environment_id IS NOT NULL AND home_generation IS NOT NULL AND principal_id IS NOT NULL))",
    key: "singleton",
    mutable: [
      "registry_instance_id",
      "owner_id",
      "home_environment_id",
      "home_generation",
      "principal_id",
      "binding_version",
      "state",
    ],
    guard:
      "NEW.binding_version != OLD.binding_version + 1 OR (OLD.registry_instance_id IS NOT NULL AND (NEW.registry_instance_id IS NOT OLD.registry_instance_id OR NEW.owner_id IS NOT OLD.owner_id)) OR (OLD.home_generation IS NOT NULL AND NEW.home_generation < OLD.home_generation)",
  },
  operations: {
    columns: `operation_id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('set_primary','create_workstream','command','appearance')), thread_environment_id TEXT, thread_id TEXT, origin TEXT NOT NULL CHECK(json_valid(origin)), request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64), dest_registry_instance_id TEXT NOT NULL, dest_owner_id TEXT NOT NULL, dest_home_environment_id TEXT NOT NULL, dest_home_generation INTEGER NOT NULL CHECK(dest_home_generation >= 1), binding_version INTEGER NOT NULL CHECK(binding_version >= 1), state TEXT NOT NULL CHECK(state IN ('accepted','dispatching','in_progress','reconciling','committed','already_satisfied','conflict','rejected','unavailable')), child_command_ids TEXT NOT NULL CHECK(json_valid(child_command_ids) AND json_type(child_command_ids) = 'array'), accepted_at TEXT NOT NULL, updated_at TEXT NOT NULL, next_poll_at TEXT, ${env("request_envelope")}, ${env("result_envelope", true)}, CHECK((thread_environment_id IS NULL) = (thread_id IS NULL))`,
    key: "operation_id",
    mutable: ["state", "updated_at", "next_poll_at", "result_envelope"],
    guard:
      "NOT ((OLD.state = 'accepted' AND NEW.state IN ('dispatching','unavailable','rejected','conflict')) OR (OLD.state = 'dispatching' AND NEW.state IN ('in_progress','reconciling','committed','already_satisfied','conflict','rejected')) OR (OLD.state IN ('in_progress','reconciling') AND NEW.state IN ('in_progress','reconciling','committed','already_satisfied','conflict','rejected')))",
  },
} satisfies Record<string, Table>;
export const nativeTableNames = Object.keys(tables).map((suffix) => `jones_workstreams_${suffix}`);

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [suffix, definition] of Object.entries<Table>(tables)) {
    const table = `jones_workstreams_${suffix}`;
    yield* sql.unsafe(
      `CREATE TABLE ${table} (${definition.columns}, PRIMARY KEY (${definition.key})) STRICT`,
    );
    const keys = definition.key.split(",").map((key) => key.trim());
    yield* sql.unsafe(`CREATE TRIGGER ${table}_no_replace BEFORE INSERT ON ${table}
      WHEN EXISTS(SELECT 1 FROM ${table} WHERE ${keys.map((key) => `${key} IS NEW.${key}`).join(" AND ")})
      BEGIN SELECT RAISE(IGNORE); END`);
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table} BEGIN SELECT RAISE(ABORT,'Workstreams history cannot be deleted'); END`,
    );
    const columns = yield* sql.unsafe<{ name: string }>(`PRAGMA table_info(${table})`);
    const immutable = columns.filter(({ name }) => !definition.mutable?.includes(name));
    const guard = definition.mutable
      ? [
          ...immutable.map(({ name }) => `NEW.${name} IS NOT OLD.${name}`),
          ...(definition.guard ? [`(${definition.guard})`] : []),
        ].join(" OR ")
      : "1";
    yield* sql.unsafe(`CREATE TRIGGER ${table}_update_guard BEFORE UPDATE ON ${table} WHEN ${guard}
      BEGIN SELECT RAISE(ABORT,'Invalid Workstreams transition'); END`);
  }
  for (const [suffix, version, partition] of [
    ["grants", "grant_version", "principal_id"],
    ["source_bindings", "binding_version", "provider,source_instance_id"],
    ["declarations", "revision", "declaration_id"],
    ["lifecycle_declarations", "revision", "workstream_id"],
    ["pr_observations", "observation_version", "native_reference_id"],
    ["rollup_summaries", "rollup_version", "workstream_id"],
  ]) {
    const table = `jones_workstreams_${suffix}`;
    const same = `registry_instance_id = NEW.registry_instance_id AND owner_id = NEW.owner_id AND ${partition!
      .split(",")
      .map((key) => `${key} = NEW.${key}`)
      .join(" AND ")}`;
    yield* sql.unsafe(`CREATE TRIGGER ${table}_append_version BEFORE INSERT ON ${table}
      WHEN NOT EXISTS(SELECT 1 FROM ${table} WHERE ${same} AND ${version} = NEW.${version}) AND
      NEW.${version} != coalesce((SELECT max(${version}) FROM ${table} WHERE ${same}),0) + 1
      BEGIN SELECT RAISE(ABORT,'Workstreams revision must advance by one'); END`);
    if (suffix === "grants" || suffix === "source_bindings") {
      yield* sql.unsafe(`CREATE TRIGGER ${table}_registry_version AFTER INSERT ON ${table}
        BEGIN UPDATE jones_workstreams_registries SET registry_version = registry_version + 1
        WHERE registry_instance_id = NEW.registry_instance_id AND owner_id = NEW.owner_id AND state = 'active'; END`);
    }
  }
  yield* sql`CREATE UNIQUE INDEX jones_workstreams_keys_one_active ON jones_workstreams_keys(registry_instance_id,scope) WHERE state = 'active'`;
  yield* sql`CREATE UNIQUE INDEX jones_workstreams_principal_environment ON jones_workstreams_principals(registry_instance_id,owner_id,environment_id) WHERE environment_id IS NOT NULL`;
  yield* sql`CREATE UNIQUE INDEX jones_workstreams_identity_index ON jones_workstreams_references(registry_instance_id,owner_id,identity_index)`;
  yield* sql`CREATE UNIQUE INDEX jones_workstreams_attested_thread ON jones_workstreams_references(registry_instance_id,owner_id,routing_source_instance_id,routing_native_id) WHERE routing_source_instance_id IS NOT NULL`;
  yield* sql`CREATE UNIQUE INDEX jones_workstreams_one_open_primary ON jones_workstreams_memberships(registry_instance_id,owner_id,native_reference_id) WHERE closed_registry_version IS NULL AND kind = 'primary'`;
  yield* sql`CREATE UNIQUE INDEX jones_workstreams_one_open_association ON jones_workstreams_memberships(registry_instance_id,owner_id,workstream_id,native_reference_id) WHERE closed_registry_version IS NULL`;
  yield* sql`CREATE UNIQUE INDEX jones_workstreams_active_edge ON jones_workstreams_edges(registry_instance_id,owner_id,from_workstream_id,to_workstream_id,relation) WHERE closed_registry_version IS NULL`;
  yield* sql`CREATE UNIQUE INDEX jones_workstreams_active_reservation ON jones_workstreams_external_reservations(registry_instance_id,owner_id,native_reference_id) WHERE state = 'active'`;
  yield* sql`CREATE INDEX jones_workstreams_history_order ON jones_workstreams_history(registry_instance_id,owner_id,registry_version,event_id COLLATE BINARY)`;
  yield* sql`CREATE INDEX jones_workstreams_history_scope_order ON jones_workstreams_history_scopes(registry_instance_id,owner_id,scope_key,registry_version,event_id COLLATE BINARY,event_sequence)`;
  yield* sql`CREATE UNIQUE INDEX jones_workstreams_open_operation ON jones_workstreams_operations(thread_environment_id,thread_id) WHERE state IN ('accepted','dispatching','in_progress','reconciling') AND thread_id IS NOT NULL`;
  yield* sql`CREATE INDEX jones_workstreams_operation_recovery ON jones_workstreams_operations(state,next_poll_at)`;
  yield* sql`CREATE TRIGGER jones_workstreams_edges_no_cycle BEFORE INSERT ON jones_workstreams_edges
    WHEN NEW.closed_registry_version IS NULL AND EXISTS(
      WITH RECURSIVE reachable(id) AS (SELECT NEW.to_workstream_id UNION
        SELECT e.to_workstream_id FROM jones_workstreams_edges e JOIN reachable r ON e.from_workstream_id = r.id
        WHERE e.registry_instance_id = NEW.registry_instance_id AND e.owner_id = NEW.owner_id AND e.closed_registry_version IS NULL)
      SELECT 1 FROM reachable WHERE id = NEW.from_workstream_id)
    BEGIN SELECT RAISE(ABORT,'Workstreams edge cycle'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_reservation_receipt BEFORE INSERT ON jones_workstreams_external_reservations
    WHEN NOT EXISTS(SELECT 1 FROM jones_workstreams_external_reservations d WHERE d.registry_instance_id = NEW.registry_instance_id AND d.owner_id = NEW.owner_id AND d.command_id = NEW.command_id) AND NOT EXISTS(SELECT 1 FROM jones_workstreams_receipts r WHERE r.registry_instance_id = NEW.registry_instance_id AND r.owner_id = NEW.owner_id AND r.command_id = NEW.command_id AND r.principal_id = NEW.principal_id AND r.server_generation = NEW.server_generation AND r.state IN ('pending','unresolved'))
    BEGIN SELECT RAISE(ABORT,'Reservation requires pending receipt'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_result_reservation BEFORE INSERT ON jones_workstreams_external_results
    WHEN NOT EXISTS(SELECT 1 FROM jones_workstreams_external_results d WHERE d.registry_instance_id = NEW.registry_instance_id AND d.owner_id = NEW.owner_id AND d.command_id = NEW.command_id) AND NOT EXISTS(SELECT 1 FROM jones_workstreams_external_reservations r WHERE r.registry_instance_id = NEW.registry_instance_id AND r.owner_id = NEW.owner_id AND r.command_id = NEW.command_id AND r.state = 'active')
    BEGIN SELECT RAISE(ABORT,'Result requires active reservation'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_history_scope_event BEFORE INSERT ON jones_workstreams_history_scopes
    WHEN NOT EXISTS(SELECT 1 FROM jones_workstreams_history h WHERE h.registry_instance_id = NEW.registry_instance_id AND h.owner_id = NEW.owner_id AND h.event_id = NEW.event_id AND h.registry_version = NEW.registry_version AND h.event_sequence = NEW.event_sequence AND (NEW.scope_key = 'global' OR EXISTS(SELECT 1 FROM json_each(h.workstream_ids) WHERE 'workstream:' || value = NEW.scope_key)))
    BEGIN SELECT RAISE(ABORT,'History scope requires matching event'); END`;
  const classes = WORKSTREAMS_SNAPSHOT_CLASSES.map((name) => `'${name}'`).join(",");
  yield* sql.unsafe(`CREATE TRIGGER jones_workstreams_import_activation BEFORE UPDATE ON jones_workstreams_import_runs
    WHEN NEW.state = 'activated' AND ((SELECT count(*) FROM jones_workstreams_import_classes WHERE import_run_id = NEW.import_run_id AND class IN (${classes})) != ${WORKSTREAMS_SNAPSHOT_CLASSES.length} OR EXISTS(SELECT 1 FROM jones_workstreams_import_classes WHERE import_run_id = NEW.import_run_id AND state != 'verified'))
    BEGIN SELECT RAISE(ABORT,'Import classes are incomplete'); END`);
});
