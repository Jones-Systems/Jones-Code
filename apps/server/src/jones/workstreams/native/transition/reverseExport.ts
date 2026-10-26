// @effect-diagnostics nodeBuiltinImport:off -- Ed25519 permit and AES-GCM conformance require Node crypto primitives.
// @effect-diagnostics preferSchemaOverJson:off -- Private JSON numbers and unchanged original UTF-8 payload strings must survive reverse export.
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NativeSql from "../store/sql.ts";
import * as Archive from "../store/archiveStore.ts";
import * as Registry from "../store/registryStore.ts";
import { readLegacyLedger } from "../store/legacyReconciliationStore.ts";
import { readTarget } from "./archive.ts";
import * as Keys from "../crypto/keyProvider.ts";
import {
  canonical,
  digest,
  TransitionError,
  tableKeys,
  rowFields,
  compareKeys,
} from "./canonical.ts";
import { archiveScope, semantic, type ArchivedRow, type Target } from "./archive.ts";
import { loadManifest } from "./apply.ts";
import {
  encodeSnapshot,
  parseSnapshot,
  encryptSnapshot,
  verifyPermit,
  type SnapshotRow,
  type SignedPermit,
} from "./snapshotReader.ts";
import type { ImportPlan } from "./plan.ts";
import type * as NodeCrypto from "node:crypto";
const revisions: Record<string, string> = {
  records: "version",
  references: "attestation_version",
  memberships: "opened_registry_version",
  declarations: "revision",
  pr_observations: "observation_version",
  history: "registry_version",
  appearance: "version",
  rollup_manifests: "registry_version",
  rollup_summaries: "rollup_version",
  receipts: "registry_version",
  legacy_receipts: "registry_version",
  external_reservations: "admitted_registry_version",
  external_results: "admitted_registry_version",
};
const currentTarget = Effect.fn("WorkstreamsTransition.currentTarget")(function* (target: Target) {
  const sql = yield* SqlClient.SqlClient,
    db = yield* NativeSql.NativeSql;
  const rows = yield* sql<
    Record<string, string | number | null>
  >`SELECT * FROM ${sql(`jones_workstreams_${target.table}`)} WHERE ${sql.and(Object.entries(target.key).map(([k, v]) => sql`${sql(k)}=${v}`))}`;
  const row = rows[0];
  if (!row) return yield* new TransitionError({ reason: "reverse_record_missing" });
  if (target.table === "external_results") {
    const reservation = yield* sql<{
      admitted_registry_version: number;
    }>`SELECT admitted_registry_version FROM jones_workstreams_external_reservations WHERE registry_instance_id=${String(row.registry_instance_id)} AND owner_id=${String(row.owner_id)} AND command_id=${String(row.command_id)}`;
    if (!reservation[0])
      return yield* new TransitionError({ reason: "reverse_result_reservation_missing" });
    row.admitted_registry_version = reservation[0].admitted_registry_version;
  }
  const field = revisions[target.table],
    revision = field ? Number(row[field] ?? 0) : (target.context?.revision ?? 1);
  const value = target.context
    ? yield* db.openJson(
        {
          registry_instance_id: String(row.registry_instance_id),
          owner_id: String(row.owner_id),
          ...target.context,
          revision,
        },
        String(row.envelope),
      )
    : null;
  return { row, value };
});
const nativeClasses: Record<
  string,
  { name: string; keys: readonly string[]; purpose?: string; recordId?: "single" }
> = {
  records: { name: "workstreams", keys: ["workstream_id"], purpose: "workstream_record" },
  references: {
    name: "workstream_native_references",
    keys: ["native_reference_id"],
    purpose: "reference",
  },
  memberships: { name: "workstream_memberships", keys: ["membership_id"], purpose: "membership" },
  declarations: {
    name: "workstream_declarations",
    keys: ["declaration_id", "revision"],
    purpose: "declaration_text",
  },
  lifecycle_declarations: {
    name: "workstream_lifecycle_declarations",
    keys: ["lifecycle_declaration_id"],
  },
  edges: { name: "workstream_edges", keys: ["edge_id"] },
  pr_observations: {
    name: "workstream_pr_observations",
    keys: ["native_reference_id", "observation_version"],
    purpose: "pr_observation",
  },
  receipts: {
    name: "workstream_command_receipts",
    keys: ["command_id"],
    purpose: "receipt",
    recordId: "single",
  },
  external_results: {
    name: "workstream_external_results",
    keys: ["command_id"],
    purpose: "external_result",
    recordId: "single",
  },
  history: {
    name: "workstream_audit_events",
    keys: ["event_id"],
    purpose: "history_event",
    recordId: "single",
  },
  appearance: { name: "workstream_appearance", keys: ["workstream_id"], purpose: "appearance" },
  appearance_receipts: {
    name: "workstream_appearance_receipts",
    keys: ["command_id"],
    purpose: "appearance_receipt",
  },
  grants: { name: "workstream_owner_grants", keys: ["principal_id", "grant_version"] },
  source_bindings: {
    name: "workstream_source_bindings",
    keys: ["provider", "source_instance_id", "binding_version"],
  },
  rollup_manifests: {
    name: "workstream_rollup_manifests",
    keys: ["workstream_id", "manifest_sha256"],
    purpose: "rollup_manifest",
  },
  rollup_manifest_sources: {
    name: "workstream_rollup_manifest_accounts",
    keys: ["workstream_id", "manifest_sha256", "provider_id", "account_id"],
  },
  rollup_queue: { name: "workstream_rollup_queue", keys: ["workstream_id"] },
  rollup_summaries: {
    name: "workstream_rollup_summaries",
    keys: ["workstream_id", "rollup_version"],
    purpose: "rollup_summary",
  },
};
const sqlTimestampFields = new Set([
  "closed_at",
  "recorded_at",
  "attempted_at",
  "occurred_at",
  "generated_at",
  "created_at",
  "updated_at",
  "lease_expires_at",
  "failed_at",
  "routing_attested_at",
  "routing_expires_at",
]);
function sqlTimestamps(row: Record<string, unknown>): Record<string, unknown> {
  for (const k of sqlTimestampFields)
    if (typeof row[k] === "string") {
      const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(?:Z|\+00:00)$/.exec(row[k]);
      if (!match) throw new TransitionError({ reason: "reverse_timestamp_unrepresentable" });
      const fraction = (match[2] ?? "").replace(/0+$/, "");
      row[k] = match[1] + (fraction ? "." + fraction : "") + "+00:00";
    }
  return row;
}
function cpReservationEffect(effect: unknown) {
  return effect === "request_native_t3_settlement" ? "native_t3_settlement" : effect;
}
function cpRow(
  name: string,
  data: Readonly<Record<string, string | number | null>>,
  input: unknown,
): SnapshotRow {
  let value = input as Record<string, unknown>;
  if (name === "workstream_audit_events") value = value.value as Record<string, unknown>;
  if (name === "workstream_command_receipts") value = value.receipt as Record<string, unknown>;
  if (!value && data.value) value = JSON.parse(String(data.value));
  const identity = value?.identity as Record<string, unknown> | undefined;
  const revision = Number(
    name === "workstream_rollup_manifests"
      ? data.workstream_version
      : name === "workstream_appearance_receipts" || name === "workstream_command_receipts"
        ? 0
        : (data.version ??
          data.attestation_version ??
          data.revision ??
          data.registry_version ??
          data.rollup_version ??
          0),
  );
  const row: Record<string, unknown> = {};
  for (const k of rowFields(name)) {
    if (k === "payload") row[k] = JSON.stringify(value);
    else if (k === "cipher")
      row[k] = { key_id: JSON.parse(String(data.envelope)).key_id, revision };
    else if (k === "cipher_revision") row[k] = revision;
    else if (k === "ciphertext_sha256") row[k] = digest(String(data.envelope));
    else if (k === "value") row[k] = value;
    else if (k === "permissions" || k === "workstream_ids") row[k] = JSON.parse(String(data[k]));
    else if (k === "effect" && name === "workstream_external_reservations")
      row[k] = cpReservationEffect(data[k]);
    else if (Object.hasOwn(data, k)) row[k] = data[k];
    else if (k === "created_by")
      row[k] = (value.created_by as { principal_id: string }).principal_id;
    else if (k === "identity_sha256")
      row[k] = data.legacy_identity_sha256 ?? digest(canonical(identity));
    else if (k.startsWith("routing_"))
      row[k] = data.routing_source_instance_id
        ? k === "routing_account_kind"
          ? "not_account_scoped"
          : (identity?.[k.slice(8)] ?? null)
        : null;
    else if (value && Object.hasOwn(value, k)) row[k] = value[k];
    else throw new TransitionError({ reason: "reverse_nonrepresentable_native_column" });
  }
  return {
    class: name,
    key: tableKeys[name]!.map((k) => row[k] as string | number | boolean),
    row: sqlTimestamps(row),
  };
}
function derivedRows(rows: readonly SnapshotRow[]): SnapshotRow[] {
  const result: SnapshotRow[] = [],
    history: Record<string, unknown>[] = [];
  const add = (name: string, row: Record<string, unknown>) =>
    result.push({
      class: name,
      key: tableKeys[name]!.map((k) => row[k] as string | number | boolean),
      row,
    });
  const projection = (
    owner: unknown,
    collection: string,
    scope: string,
    id: unknown,
    n = 0,
    n2 = 0,
    seq: unknown = null,
  ) =>
    add("workstream_collection_projections", {
      owner_id: owner,
      collection,
      scope_key: scope,
      record_id: id,
      sort_number_1: n,
      sort_text_1: id,
      sort_number_2: n2,
      event_sequence: seq,
    });
  for (const { class: name, row: r } of rows) {
    const v = semantic(r);
    if (name === "workstreams")
      projection(r.owner_id, "workstreams", "global", r.workstream_id, Number(r.sort_order));
    if (name === "workstream_native_references")
      projection(
        r.owner_id,
        "references",
        r.registration_state === "attested" ? "attested" : `principal:${r.created_by}`,
        r.native_reference_id,
      );
    if (name === "workstream_memberships")
      projection(
        r.owner_id,
        "memberships",
        `workstream:${r.workstream_id}`,
        r.membership_id,
        Number((v.opened as { registry_version: number }).registry_version),
      );
    if (name === "workstream_declarations")
      projection(
        r.owner_id,
        "declarations",
        `workstream:${r.workstream_id}`,
        r.declaration_id,
        Number(r.registry_version),
        Number(r.revision),
      );
    if (name === "workstream_edges")
      for (const id of [r.from_workstream_id, r.to_workstream_id])
        projection(
          r.owner_id,
          "edges",
          `workstream:${id}`,
          r.edge_id,
          Number((v.opened as { registry_version: number }).registry_version),
        );
    if (name === "workstream_audit_events") history.push({ ...r });
  }
  const scopes = new Map<string, Record<string, unknown>[]>();
  for (const r of history.sort((a, b) => Number(a.event_sequence) - Number(b.event_sequence))) {
    for (const scope of [
      "global",
      ...(r.workstream_ids as string[]).map((id) => `workstream:${id}`),
    ]) {
      projection(
        r.owner_id,
        "history",
        scope,
        r.event_id,
        Number(r.registry_version),
        0,
        r.event_sequence,
      );
      const key = canonical([r.owner_id, scope]),
        prior = scopes.get(key) ?? [],
        position = [r.registry_version, r.event_id];
      const ordered = prior.toSorted((a, b) =>
        compareKeys([a.registry_version, a.event_id], [b.registry_version, b.event_id]),
      );
      const predecessor = ordered.findLast(
          (x) => compareKeys([x.registry_version, x.event_id], position) < 0,
        ),
        successor = ordered.find(
          (x) => compareKeys([x.registry_version, x.event_id], position) > 0,
        );
      add("workstream_history_links", {
        owner_id: r.owner_id,
        scope_key: scope,
        predecessor_registry_version: predecessor?.registry_version ?? -1,
        predecessor_event_id: predecessor?.event_id ?? "",
        valid_from_event_sequence: r.event_sequence,
        successor_registry_version: r.registry_version,
        successor_event_id: r.event_id,
      });
      add("workstream_history_links", {
        owner_id: r.owner_id,
        scope_key: scope,
        predecessor_registry_version: r.registry_version,
        predecessor_event_id: r.event_id,
        valid_from_event_sequence: r.event_sequence,
        successor_registry_version: successor?.registry_version ?? null,
        successor_event_id: successor?.event_id ?? null,
      });
      prior.push(r);
      scopes.set(key, prior);
    }
  }
  return result;
}
export interface ReverseInput {
  readonly snapshot_id: string;
  readonly exported_at: string;
  readonly revision: string;
  readonly fence_epoch: number;
  readonly stream_key_id: string;
  readonly representable_principals: readonly string[];
}
export const prepareReverse = Effect.fn("WorkstreamsTransition.prepareReverse")(function* (
  p: ImportPlan,
  input: ReverseInput,
) {
  const sql = yield* SqlClient.SqlClient,
    archive = yield* Archive.ArchiveStore,
    registry = yield* Registry.RegistryStore;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const run = yield* loadManifest(p);
      if (!["verified", "activated"].includes(run.state))
        return yield* new TransitionError({ reason: "reverse_import_incomplete" });
      const contexts = yield* sql<{
        owner_id: string;
      }>`SELECT owner_id FROM jones_workstreams_registries WHERE registry_instance_id=${archiveScope(p).registry_instance_id}`;
      if (
        contexts.length !== p.snapshot.header.owners.length ||
        contexts.some((c) => !p.snapshot.header.owners.some((o) => o.owner_id === c.owner_id))
      )
        return yield* new TransitionError({ reason: "reverse_owner_set_changed" });
      const owners = [];
      for (const owner of p.snapshot.header.owners) {
        const c = yield* registry.context({ ...archiveScope(p), owner_id: owner.owner_id });
        if (!c || c.state !== "fenced" || c.server_generation >= Number.MAX_SAFE_INTEGER)
          return yield* new TransitionError({ reason: "reverse_requires_fence" });
        const high = yield* sql<{
          high: number;
        }>`SELECT coalesce(max(event_sequence),0) AS high FROM jones_workstreams_history WHERE registry_instance_id=${c.registry_instance_id} AND owner_id=${c.owner_id}`;
        owners.push({
          owner_id: c.owner_id,
          server_generation: c.server_generation,
          registry_version: c.registry_version,
          audit_high_water: high[0]!.high,
        });
      }
      for (const owner of p.snapshot.header.owners) {
        const ledger = yield* readLegacyLedger(
          { ...archiveScope(p), owner_id: owner.owner_id },
          p.import_run_id,
        );
        for (const delta of ledger.deltas)
          for (const change of delta.changes.filter((c) => c.after.table !== "references")) {
            const current = yield* readTarget(change.after);
            if (
              canonical(current.data) !== canonical(change.after.data) ||
              (change.after.context && canonical(current.value) !== canonical(change.after.value))
            )
              return yield* new TransitionError({ reason: "reverse_reconciliation_mismatch" });
          }
      }
      const principals = yield* sql<{
        principal_id: string;
      }>`SELECT DISTINCT principal_id FROM jones_workstreams_principals WHERE registry_instance_id=${archiveScope(p).registry_instance_id}`;
      if (principals.some((x) => !input.representable_principals.includes(x.principal_id)))
        return yield* new TransitionError({ reason: "reverse_principal_unrepresentable" });
      const open = yield* sql<{
        count: number;
      }>`SELECT (SELECT count(*) FROM jones_workstreams_receipts WHERE registry_instance_id=${archiveScope(p).registry_instance_id} AND state IN ('pending','unresolved'))+(SELECT count(*) FROM jones_workstreams_external_reservations WHERE registry_instance_id=${archiveScope(p).registry_instance_id} AND state='active')+(SELECT count(*) FROM jones_workstreams_operations WHERE dest_registry_instance_id=${archiveScope(p).registry_instance_id} AND state IN ('accepted','dispatching','in_progress','reconciling')) AS count`;
      if (open[0]!.count) return yield* new TransitionError({ reason: "reverse_open_effects" });
      const rows: SnapshotRow[] = [],
        seen = new Map<string, Set<string>>();
      for (const source of p.snapshot.rows) {
        const scope = {
          ...archiveScope(p),
          owner_id: String(source.row.owner_id ?? archiveScope(p).owner_id),
        };
        const entry = (yield* archive.readArchive(
          scope,
          p.import_run_id,
          source.class,
          canonical(source.key),
        )) as ArchivedRow | null;
        if (!entry || canonical(entry.source) !== canonical(source))
          return yield* new TransitionError({ reason: "reverse_archive_conflict" });
        let row = { ...source.row };
        for (const target of entry.targets) {
          const current = yield* currentTarget(target);
          const ids = seen.get(target.table) ?? new Set<string>();
          ids.add(canonical(target.key));
          seen.set(target.table, ids);
          // Secondary history-scope records have no independent CP row.
          if (target.table === "history_scopes") continue;
          const targetValue = current.value as Record<string, unknown> | null;
          if (target.context) {
            let value: unknown = targetValue;
            if (target.table === "history") value = targetValue?.value;
            if (target.table === "receipts") value = targetValue?.receipt;
            if (
              typeof row.payload === "string" &&
              JSON.stringify(semantic(row)) !== JSON.stringify(value)
            )
              row.payload = JSON.stringify(value);
            else if (Object.hasOwn(row, "value")) row.value = value;
            for (const k of Object.keys(row))
              if (
                k !== "payload" &&
                k !== "cipher" &&
                k !== "ciphertext_sha256" &&
                k !== "value" &&
                Object.hasOwn(current.row, k)
              )
                row[k] =
                  k === "workstream_ids" ? JSON.parse(String(current.row[k])) : current.row[k];
            if (target.table === "references") {
              // Native routing stores seven fields; the four CP discriminators live in identity.
              // Reproject the complete tuple after verification changes an archived null route.
              const projected = cpRow(source.class, current.row, current.value).row;
              for (const k of Object.keys(row).filter((k) => k.startsWith("routing_")))
                row[k] = projected[k];
            }
            if (typeof row.payload === "string" && row.payload !== source.row.payload) {
              const rev = Number(
                current.row[revisions[target.table] ?? ""] ??
                  (row.cipher as { revision: number }).revision,
              );
              row.cipher = {
                key_id: String(JSON.parse(String(current.row.envelope)).key_id),
                revision: rev,
              };
              row.ciphertext_sha256 = digest(String(current.row.envelope));
              if (Object.hasOwn(row, "cipher_revision")) row.cipher_revision = rev;
            }
          } else if (target.table !== "rollup_queue") {
            for (const k of Object.keys(row))
              if (Object.hasOwn(current.row, k))
                row[k] =
                  k === "permissions" || k === "value"
                    ? JSON.parse(String(current.row[k]))
                    : current.row[k];
          } else if (canonical(current.row) !== canonical({ ...target.data })) {
            for (const k of Object.keys(row))
              if (Object.hasOwn(current.row, k)) row[k] = current.row[k];
          }
        }
        if (source.class === "workstream_external_reservations")
          row.effect = cpReservationEffect(row.effect);
        if (source.class === "workstream_registry_context") {
          const o = owners.find((o) => o.owner_id === row.owner_id)!;
          row = {
            owner_id: o.owner_id,
            registry_version: o.registry_version,
            server_generation: o.server_generation,
          };
        }
        rows.push({
          class: source.class,
          key: tableKeys[source.class]!.map((k) => row[k] as string | number | boolean),
          row: sqlTimestamps(row),
        });
      }
      let changed = false;
      for (const [table, config] of Object.entries(nativeClasses)) {
        const native = yield* sql<
          Record<string, string | number | null>
        >`SELECT * FROM ${sql(`jones_workstreams_${table}`)} WHERE registry_instance_id=${archiveScope(p).registry_instance_id}`;
        for (const data of native) {
          const key = Object.fromEntries(
            ["registry_instance_id", "owner_id", ...config.keys].map((k) => [k, data[k]!]),
          ) as Record<string, string | number>;
          if (seen.get(table)?.has(canonical(key))) continue;
          const target: Target = {
            table,
            key,
            data,
            context: config.purpose
              ? {
                  purpose: config.purpose,
                  record_id:
                    config.recordId === "single"
                      ? String(data[config.keys[0]!])
                      : canonical(config.keys.map((k) => data[k])),
                  revision: Number(data[revisions[table] ?? ""] ?? 1),
                }
              : null,
            value: null,
          };
          const current = yield* currentTarget(target);
          const row = yield* Effect.try({
            try: () => cpRow(config.name, current.row, current.value),
            catch: () => new TransitionError({ reason: "reverse_nonrepresentable_native_row" }),
          });
          rows.push(row);
          changed = true;
        }
      }
      if (changed || rows.some((r, i) => canonical(r) !== canonical(p.snapshot.rows[i]))) {
        const retained = rows.filter(
          (r) =>
            r.class !== "workstream_collection_projections" &&
            r.class !== "workstream_history_links",
        );
        rows.splice(0, rows.length, ...retained, ...derivedRows(retained));
      }
      const header = {
        ...p.snapshot.header,
        snapshot_id: input.snapshot_id,
        exported_at: input.exported_at,
        producer: {
          system: "jones-native" as const,
          revision: input.revision,
          schema_version: 124,
        },
        owners,
        fence: { epoch: input.fence_epoch, fenced_at: input.exported_at },
        open_effects: 0,
        encryption: { mode: "stream" as const, key_id: input.stream_key_id },
      };
      const text = encodeSnapshot(header, rows);
      const snapshot = yield* Effect.try({
        try: () => parseSnapshot(text),
        catch: () => new TransitionError({ reason: "reverse_snapshot_validation" }),
      });
      return {
        snapshot,
        canonical_jsonl: text,
        next_generations: owners.map((o) => ({
          owner_id: o.owner_id,
          server_generation: o.server_generation + 1,
        })),
        lineage: {
          original_snapshot_id: p.snapshot.header.snapshot_id,
          original_snapshot_sha256: p.snapshot.snapshot_sha256,
          import_run_id: p.import_run_id,
        },
      };
    }),
  );
});
export const reverseExport = Effect.fn("WorkstreamsTransition.reverseExport")(function* (
  p: ImportPlan,
  input: ReverseInput,
  permit: SignedPermit,
  permitKeyId: string,
  publicKey: NodeCrypto.KeyObject,
) {
  const prepared = yield* prepareReverse(p, input),
    keys = yield* Keys.WorkstreamsKeyProvider;
  yield* Effect.try({
    try: () => {
      if (permit.key_id !== permitKeyId) throw new TransitionError({ reason: "signature_invalid" });
      verifyPermit(prepared.snapshot.header, permit, publicKey);
    },
    catch: () => new TransitionError({ reason: "signature_invalid" }),
  });
  const key = yield* keys.get(input.stream_key_id);
  return {
    encrypted: encryptSnapshot(prepared.canonical_jsonl, input.stream_key_id, key),
    snapshot_sha256: prepared.snapshot.snapshot_sha256,
    next_generations: prepared.next_generations,
    lineage: prepared.lineage,
  };
});
