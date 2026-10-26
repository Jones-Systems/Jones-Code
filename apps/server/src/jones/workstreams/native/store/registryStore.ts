import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as NativeSql from "./sql.ts";

export interface RegistryContext extends NativeSql.OwnerScope {
  readonly canonical_origin: string;
  readonly home_environment_id: string;
  readonly server_generation: number;
  readonly registry_version: number;
  readonly lineage: "native" | "imported" | "transferred";
  readonly active_key_id: string;
  readonly state: "active" | "importing" | "fenced" | "transferred_out";
}
type Cell = string | number | null;
export interface Entity {
  readonly metadata: Readonly<Record<string, Cell>>;
  readonly value: unknown;
}
const collections = {
  records: {
    key: ["workstream_id"],
    columns: ["workstream_id", "version", "sort_order"],
    revision: "version",
    purpose: "workstream_record",
    mutable: true,
  },
  references: {
    key: ["native_reference_id"],
    columns: [
      "native_reference_id",
      "provider",
      "resource_kind",
      "id_kind",
      "identity_index",
      "legacy_identity_sha256",
      "attestation_version",
      "registration_state",
      "created_by_principal",
      "routing_source_instance_id",
      "routing_native_id",
      "routing_authority_namespace",
      "routing_store_generation",
      "routing_attested_at",
      "routing_expires_at",
      "routing_evidence_sha256",
    ],
    revision: "attestation_version",
    purpose: "reference",
    mutable: true,
  },
  memberships: {
    key: ["membership_id"],
    columns: [
      "membership_id",
      "workstream_id",
      "native_reference_id",
      "kind",
      "opened_registry_version",
      "closed_registry_version",
      "closed_at",
    ],
    revision: "opened_registry_version",
    purpose: "membership",
    mutable: true,
  },
  declarations: {
    key: ["declaration_id", "revision"],
    columns: ["declaration_id", "revision", "workstream_id", "registry_version", "state"],
    revision: "revision",
    purpose: "declaration_text",
    mutable: false,
  },
  pr_observations: {
    key: ["native_reference_id", "observation_version"],
    columns: ["native_reference_id", "observation_version", "attempted_at", "outcome"],
    revision: "observation_version",
    purpose: "pr_observation",
    mutable: false,
  },
  appearance: {
    key: ["workstream_id"],
    columns: ["workstream_id", "version"],
    revision: "version",
    purpose: "appearance",
    mutable: true,
  },
  appearance_receipts: {
    key: ["command_id"],
    columns: ["command_id", "principal_id", "request_sha256", "origin"],
    revision: null,
    purpose: "appearance_receipt",
    mutable: false,
  },
  rollup_bindings: {
    key: ["workstream_id", "native_reference_id"],
    columns: ["workstream_id", "native_reference_id", "binding_version", "provider_id", "state"],
    revision: "binding_version",
    purpose: "rollup_binding",
    mutable: true,
  },
  rollup_manifests: {
    key: ["workstream_id", "manifest_sha256"],
    columns: ["workstream_id", "manifest_sha256", "workstream_version", "registry_version"],
    revision: "registry_version",
    purpose: "rollup_manifest",
    mutable: false,
  },
  rollup_summaries: {
    key: ["workstream_id", "rollup_version"],
    columns: [
      "workstream_id",
      "rollup_version",
      "manifest_sha256",
      "output_sha256",
      "coverage",
      "freshness",
      "generated_at",
    ],
    revision: "rollup_version",
    purpose: "rollup_summary",
    mutable: false,
  },
} as const;
export type Collection = keyof typeof collections;
const plainCollections = {
  principals: ["principal_id", "environment_id", "kind", "provenance"],
  grants: ["principal_id", "grant_version", "grant_id", "permissions", "state"],
  source_bindings: [
    "provider",
    "source_instance_id",
    "binding_version",
    "authority_namespace",
    "store_generation",
    "state",
  ],
  lifecycle_declarations: [
    "lifecycle_declaration_id",
    "workstream_id",
    "revision",
    "registry_version",
    "recorded_at",
    "value",
  ],
  edges: [
    "edge_id",
    "from_workstream_id",
    "to_workstream_id",
    "relation",
    "opened_registry_version",
    "closed_registry_version",
    "closed_at",
    "value",
  ],
  rollup_manifest_sources: [
    "workstream_id",
    "manifest_sha256",
    "provider_id",
    "account_id",
    "authorization_policy_sha256",
    "authorization_principal_id",
    "authorization_grant_id",
    "authorization_decision_id",
    "authorization_account_generation",
    "authorization_revision",
  ],
  rollup_queue: [
    "workstream_id",
    "manifest_sha256",
    "state",
    "attempt_id",
    "fence",
    "lease_expires_at",
    "failure_code",
    "failed_at",
    "updated_at",
  ],
} as const;
export type PlainCollection = keyof typeof plainCollections;
type Failure = SqlError | NativeSql.StoreError | NativeSql.CryptoError;
export class RegistryStore extends Context.Service<
  RegistryStore,
  {
    readonly context: (
      scope: NativeSql.OwnerScope,
    ) => Effect.Effect<RegistryContext | null, Failure>;
    readonly initialize: (context: RegistryContext) => Effect.Effect<void, Failure>;
    readonly advance: (
      scope: NativeSql.OwnerScope,
      expected: { server_generation: number; registry_version: number },
      change?: {
        state?: RegistryContext["state"];
        home_environment_id?: string;
        active_key_id?: string;
        generation?: boolean;
      },
    ) => Effect.Effect<RegistryContext, Failure>;
    readonly list: (
      scope: NativeSql.OwnerScope,
      collection: Collection,
    ) => Effect.Effect<readonly Entity[], Failure>;
    readonly put: (
      scope: NativeSql.OwnerScope,
      collection: Collection,
      entity: Entity,
      expectedRevision: number | null,
    ) => Effect.Effect<void, Failure>;
    readonly listPlain: (
      scope: NativeSql.OwnerScope,
      collection: PlainCollection,
    ) => Effect.Effect<readonly Readonly<Record<string, Cell>>[], Failure>;
    readonly appendPlain: (
      scope: NativeSql.OwnerScope,
      collection: PlainCollection,
      row: Readonly<Record<string, Cell>>,
    ) => Effect.Effect<void, Failure>;
    readonly closeEdge: (
      scope: NativeSql.OwnerScope,
      id: string,
      version: number,
      at: string,
      value: unknown,
    ) => Effect.Effect<void, Failure>;
  }
>()("t3/jones/workstreams/native/store/registryStore") {}
const validColumns = (row: Readonly<Record<string, Cell>>, columns: readonly string[]) =>
  Object.keys(row).length === columns.length &&
  columns.every((column) => Object.hasOwn(row, column));
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const db = yield* NativeSql.NativeSql;
  const context = Effect.fn("RegistryStore.context")((scope: NativeSql.OwnerScope) =>
    db.transaction(
      scope,
      sql<RegistryContext>`SELECT * FROM jones_workstreams_registries WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id}`.pipe(
        Effect.map((rows) => rows[0] ?? null),
      ),
    ),
  );
  const initialize = Effect.fn("RegistryStore.initialize")((row: RegistryContext) =>
    db.transaction(
      row,
      Effect.gen(function* () {
        if (yield* context(row)) return yield* new NativeSql.StoreError({ reason: "conflict" });
        yield* sql`INSERT INTO jones_workstreams_registries ${sql.insert({ ...row })}`;
      }),
    ),
  );
  const advance = Effect.fn("RegistryStore.advance")(
    (
      scope: NativeSql.OwnerScope,
      expected: { server_generation: number; registry_version: number },
      change: {
        state?: RegistryContext["state"];
        home_environment_id?: string;
        active_key_id?: string;
        generation?: boolean;
      } = {},
    ) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const old = yield* context(scope);
          if (!old) return yield* new NativeSql.StoreError({ reason: "not_found" });
          if (
            old.server_generation !== expected.server_generation ||
            old.registry_version !== expected.registry_version
          )
            return yield* new NativeSql.StoreError({ reason: "conflict" });
          const rows = yield* sql<RegistryContext>`UPDATE jones_workstreams_registries SET
      server_generation = ${old.server_generation + (change.generation ? 1 : 0)}, registry_version = ${old.registry_version + (change.generation ? 0 : 1)},
      state = ${change.state ?? old.state}, home_environment_id = ${change.home_environment_id ?? old.home_environment_id}, active_key_id = ${change.active_key_id ?? old.active_key_id}
      WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id} AND server_generation = ${expected.server_generation} AND registry_version = ${expected.registry_version} RETURNING *`;
          if (!rows[0]) return yield* new NativeSql.StoreError({ reason: "conflict" });
          return rows[0];
        }),
      ),
  );
  const list = Effect.fn("RegistryStore.list")(
    (scope: NativeSql.OwnerScope, collection: Collection) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const config = collections[collection];
          const rows = yield* sql<
            Record<string, Cell>
          >`SELECT * FROM ${sql(`jones_workstreams_${collection}`)} WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id} ORDER BY ${sql(config.key[0])} COLLATE BINARY`;
          const result: Entity[] = [];
          for (const row of rows) {
            const metadata = Object.fromEntries(
              config.columns.map((column) => [column, row[column] ?? null]),
            );
            const value = yield* db.openJson(
              {
                ...scope,
                purpose: config.purpose,
                record_id: canonicalWorkstreamsSnapshotJson(
                  config.key.map((column) => row[column]),
                ),
                revision: config.revision ? Number(row[config.revision]) : 1,
              },
              String(row.envelope),
            );
            result.push({ metadata, value });
          }
          return result;
        }),
      ),
  );
  const put = Effect.fn("RegistryStore.put")(
    (
      scope: NativeSql.OwnerScope,
      collection: Collection,
      entity: Entity,
      expectedRevision: number | null,
    ) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const config = collections[collection];
          const row = entity.metadata;
          if (!validColumns(row, config.columns))
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          const where = config.key.map((column) => `${column} = ?`).join(" AND ");
          const parameters = [
            scope.registry_instance_id,
            scope.owner_id,
            ...config.key.map((column) => row[column]!),
          ];
          const old = yield* sql.unsafe<Record<string, Cell>>(
            `SELECT * FROM jones_workstreams_${collection} WHERE registry_instance_id = ? AND owner_id = ? AND ${where}`,
            parameters,
          );
          if (
            (expectedRevision === null) !== (old.length === 0) ||
            (old[0] &&
              (!config.mutable ||
                (config.revision && Number(old[0][config.revision]) !== expectedRevision)))
          )
            return yield* new NativeSql.StoreError({ reason: "conflict" });
          const revision = config.revision ? Number(row[config.revision]) : 1;
          const envelope = yield* db.sealJson(
            {
              ...scope,
              purpose: config.purpose,
              record_id: canonicalWorkstreamsSnapshotJson(config.key.map((column) => row[column])),
              revision,
            },
            entity.value,
          );
          if (old.length === 0)
            yield* sql`INSERT INTO ${sql(`jones_workstreams_${collection}`)} ${sql.insert({ ...scope, ...row, envelope })}`;
          else
            yield* sql.unsafe(
              `UPDATE jones_workstreams_${collection} SET ${[...config.columns, "envelope"].map((column) => `${column} = ?`).join(",")} WHERE registry_instance_id = ? AND owner_id = ? AND ${where}`,
              [...config.columns.map((column) => row[column]!), envelope, ...parameters],
            );
        }),
      ),
  );
  const listPlain = Effect.fn("RegistryStore.listPlain")(
    (scope: NativeSql.OwnerScope, collection: PlainCollection) =>
      db.transaction(
        scope,
        sql<
          Record<string, Cell>
        >`SELECT * FROM ${sql(`jones_workstreams_${collection}`)} WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id}`,
      ),
  );
  const appendPlain = Effect.fn("RegistryStore.appendPlain")(
    (
      scope: NativeSql.OwnerScope,
      collection: PlainCollection,
      row: Readonly<Record<string, Cell>>,
    ) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          if (!validColumns(row, plainCollections[collection]))
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          const inserted =
            yield* sql`INSERT INTO ${sql(`jones_workstreams_${collection}`)} ${sql.insert({ ...scope, ...row })} RETURNING *`;
          if (inserted.length !== 1) return yield* new NativeSql.StoreError({ reason: "conflict" });
        }),
      ),
  );
  const closeEdge = Effect.fn("RegistryStore.closeEdge")(
    (scope: NativeSql.OwnerScope, id: string, version: number, at: string, value: unknown) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const rows =
            yield* sql`UPDATE jones_workstreams_edges SET closed_registry_version = ${version}, closed_at = ${at}, value = ${canonicalWorkstreamsSnapshotJson(value)} WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id} AND edge_id = ${id} AND closed_registry_version IS NULL RETURNING edge_id`;
          if (rows.length !== 1) return yield* new NativeSql.StoreError({ reason: "conflict" });
        }),
      ),
  );
  return RegistryStore.of({
    context,
    initialize,
    advance,
    list,
    put,
    listPlain,
    appendPlain,
    closeEdge,
  });
});
/** @public Canonical service layer. */
export const layer = Layer.effect(RegistryStore, make);
