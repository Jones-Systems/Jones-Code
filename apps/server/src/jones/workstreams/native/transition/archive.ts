// @effect-diagnostics preferSchemaOverJson:off -- CP private payloads retain their original JSON spelling separately from semantic decoding.
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NativeSql from "../store/sql.ts";
import * as Identity from "../crypto/identityIndex.ts";
import type { WorkstreamNativeIdentity } from "@t3tools/contracts";
import { canonical, digest, TransitionError } from "./canonical.ts";
import type { SnapshotRow } from "./snapshotReader.ts";
import type { ImportPlan } from "./plan.ts";
export interface Target {
  readonly table: string;
  readonly key: Readonly<Record<string, string | number>>;
  readonly data: Readonly<Record<string, string | number | null>>;
  readonly context: {
    readonly purpose: string;
    readonly record_id: string;
    readonly revision: number;
  } | null;
  readonly value: unknown;
}
export interface ArchivedRow {
  readonly source: SnapshotRow;
  readonly targets: readonly Target[];
}
export const archiveScope = (p: ImportPlan) => ({
  registry_instance_id: p.snapshot.header.registry.registry_instance_id,
  owner_id: p.snapshot.header.owners[0]?.owner_id ?? "registry:empty",
});
export function semantic(r: Readonly<Record<string, unknown>>): Record<string, unknown> {
  return (
    typeof r.payload === "string" ? JSON.parse(r.payload.replace(/^\uFEFF/, "")) : (r.value ?? r)
  ) as Record<string, unknown>;
}
const pick = (r: Readonly<Record<string, unknown>>, fields: readonly string[]) =>
  Object.fromEntries(fields.map((k) => [k, r[k] as string | number | null]));
export const targetsFor = Effect.fn("WorkstreamsTransition.targetsFor")(function* (
  p: ImportPlan,
  source: SnapshotRow,
) {
  const r = source.row,
    v = semantic(r),
    targets: Target[] = [];
  const scope = {
    registry_instance_id: p.snapshot.header.registry.registry_instance_id,
    owner_id: String(r.owner_id ?? archiveScope(p).owner_id),
  };
  const add = (
    table: string,
    keys: readonly string[],
    data: Record<string, string | number | null>,
    purpose: string | null = null,
    revision = 1,
    value: unknown = v,
    recordId?: string,
  ) => {
    const key = pick(data, keys) as Record<string, string | number>;
    targets.push({
      table,
      key: { ...scope, ...key },
      data: { ...scope, ...data },
      context: purpose
        ? { purpose, record_id: recordId ?? canonical(keys.map((k) => data[k])), revision }
        : null,
      value,
    });
  };
  switch (source.class) {
    case "workstream_registry_context":
      break;
    case "workstream_owner_grants":
      add("grants", ["principal_id", "grant_version"], {
        ...pick(r, ["principal_id", "grant_version", "grant_id", "state"]),
        permissions: canonical(r.permissions),
      });
      break;
    case "workstream_source_bindings":
      add(
        "source_bindings",
        ["provider", "source_instance_id", "binding_version"],
        pick(r, [
          "provider",
          "source_instance_id",
          "binding_version",
          "authority_namespace",
          "store_generation",
          "state",
        ]),
      );
      break;
    case "workstreams":
      add(
        "records",
        ["workstream_id"],
        pick(r, ["workstream_id", "version", "sort_order"]),
        "workstream_record",
        Number(r.version),
      );
      break;
    case "workstream_native_references": {
      const identity = yield* Identity.WorkstreamsIdentityIndex;
      const index = yield* identity.digest(
        scope,
        v.identity as typeof WorkstreamNativeIdentity.Type,
      );
      const i = v.identity as Record<string, unknown>;
      add(
        "references",
        ["native_reference_id"],
        {
          ...pick(r, [
            "native_reference_id",
            "attestation_version",
            "registration_state",
            "routing_source_instance_id",
            "routing_native_id",
            "routing_authority_namespace",
            "routing_store_generation",
            "routing_attested_at",
            "routing_expires_at",
            "routing_evidence_sha256",
          ]),
          provider: String(i.provider),
          resource_kind: String(i.resource_kind),
          id_kind: String(i.id_kind),
          identity_index: index,
          legacy_identity_sha256: String(r.identity_sha256),
          created_by_principal: String(r.created_by),
        },
        "reference",
        Number(r.attestation_version),
      );
      break;
    }
    case "workstream_memberships": {
      const opened = v.opened as Record<string, unknown>,
        closed = v.closed as Record<string, unknown> | null;
      add(
        "memberships",
        ["membership_id"],
        {
          ...pick(r, [
            "membership_id",
            "workstream_id",
            "native_reference_id",
            "kind",
            "closed_at",
          ]),
          opened_registry_version: Number(opened.registry_version),
          closed_registry_version: closed ? Number(closed.registry_version) : null,
        },
        "membership",
        Number(opened.registry_version),
      );
      break;
    }
    case "workstream_declarations":
      add(
        "declarations",
        ["declaration_id", "revision"],
        {
          ...pick(r, ["declaration_id", "revision", "workstream_id", "registry_version"]),
          state: String(v.state),
        },
        "declaration_text",
        Number(r.revision),
      );
      break;
    case "workstream_lifecycle_declarations":
      add("lifecycle_declarations", ["lifecycle_declaration_id"], {
        ...pick(r, ["lifecycle_declaration_id", "workstream_id", "revision", "recorded_at"]),
        registry_version: Number(v.registry_version),
        value: canonical(v),
      });
      break;
    case "workstream_edges": {
      const opened = v.opened as Record<string, unknown>,
        closed = v.closed as Record<string, unknown> | null;
      add("edges", ["edge_id"], {
        ...pick(r, ["edge_id", "from_workstream_id", "to_workstream_id", "relation", "closed_at"]),
        opened_registry_version: Number(opened.registry_version),
        closed_registry_version: closed ? Number(closed.registry_version) : null,
        value: canonical(v),
      });
      break;
    }
    case "workstream_pr_observations":
      add(
        "pr_observations",
        ["native_reference_id", "observation_version"],
        {
          ...pick(r, ["native_reference_id", "observation_version", "attempted_at"]),
          outcome: String(v.outcome),
        },
        "pr_observation",
        Number(r.observation_version),
      );
      break;
    case "workstream_command_receipts": {
      const terminal = r.state === "committed" || r.state === "rejected";
      if (terminal)
        add(
          "legacy_receipts",
          ["command_id"],
          {
            ...pick(r, ["command_id", "principal_id", "state"]),
            archive_version: 1,
            import_run_id: p.import_run_id,
            operation: String(v.operation),
            request_sha256: String(v.request_sha256),
            server_generation: Number(v.server_generation),
            registry_version: v.state === "committed" ? Number(v.registry_version) : null,
            original_ciphertext_sha256: String(r.ciphertext_sha256),
            canonical_sha256: digest(canonical(source)),
          },
          "legacy_receipt",
          v.state === "committed" ? Number(v.registry_version) : 0,
          v,
          String(r.command_id),
        );
      else {
        // CP retains the original command object in the reservation. Never manufacture an executable request from it.
        const reservation = p.snapshot.rows.find(
          (x) =>
            x.class === "workstream_external_reservations" &&
            x.row.owner_id === r.owner_id &&
            x.row.command_id === r.command_id,
        );
        if (!reservation) return yield* new TransitionError({ reason: "open_intent_missing" });
        const request = semantic(reservation.row);
        add(
          "receipts",
          ["command_id"],
          {
            ...pick(r, ["command_id", "principal_id", "state"]),
            operation: String(v.operation),
            request_sha256: String(v.request_sha256),
            server_generation: Number(v.server_generation),
            registry_version: null,
            accepted_at: String(v.accepted_at),
            completed_at: null,
            origin: "legacy_open",
          },
          "receipt",
          0,
          {
            receipt: v,
            request_body: "",
            legacy_request_bytes: "unknown",
            legacy_command: request.command,
          },
          String(r.command_id),
        );
      }
      break;
    }
    case "workstream_external_reservations":
      if (r.state === "active")
        add(
          "external_reservations",
          ["command_id"],
          {
            ...pick(r, [
              "command_id",
              "principal_id",
              "native_reference_id",
              "server_generation",
              "admitted_registry_version",
              "state",
            ]),
            effect:
              r.effect === "native_t3_settlement"
                ? "request_native_t3_settlement"
                : String(r.effect),
          },
          "external_intent",
          Number(r.admitted_registry_version),
          v,
          String(r.command_id),
        );
      break;
    case "workstream_external_results": {
      const open = p.snapshot.rows.some(
        (x) =>
          x.class === "workstream_external_reservations" &&
          x.row.owner_id === r.owner_id &&
          x.row.command_id === r.command_id &&
          x.row.state === "active",
      );
      if (open) {
        const reservation = p.snapshot.rows.find(
          (x) =>
            x.class === "workstream_external_reservations" &&
            x.row.owner_id === r.owner_id &&
            x.row.command_id === r.command_id,
        )!;
        add(
          "external_results",
          ["command_id"],
          pick(r, ["command_id"]),
          "external_result",
          Number(reservation.row.admitted_registry_version),
          v,
          String(r.command_id),
        );
      }
      break;
    }
    case "workstream_audit_events": {
      const ids = r.workstream_ids as string[],
        sequence = Number(r.event_sequence);
      add(
        "history",
        ["event_id"],
        {
          ...pick(r, [
            "event_id",
            "event_sequence",
            "command_id",
            "registry_version",
            "occurred_at",
          ]),
          legacy_event_sequence: sequence,
          workstream_ids: canonical(ids),
        },
        "history_event",
        Number(r.registry_version),
        {
          event_id: r.event_id,
          command_id: r.command_id,
          registry_version: r.registry_version,
          occurred_at: r.occurred_at,
          workstream_ids: ids,
          value: v,
          legacy_event_sequence: sequence,
        },
        String(r.event_id),
      );
      for (const s of ["global", ...ids.map((id) => `workstream:${id}`)])
        add("history_scopes", ["scope_key", "registry_version", "event_id"], {
          scope_key: s,
          registry_version: Number(r.registry_version),
          event_id: String(r.event_id),
          event_sequence: sequence,
        });
      break;
    }
    case "workstream_rollup_manifests":
      add(
        "rollup_manifests",
        ["workstream_id", "manifest_sha256"],
        pick(r, ["workstream_id", "manifest_sha256", "workstream_version", "registry_version"]),
        "rollup_manifest",
        Number(r.registry_version),
      );
      break;
    case "workstream_rollup_manifest_accounts":
      add(
        "rollup_manifest_sources",
        ["workstream_id", "manifest_sha256", "provider_id", "account_id"],
        {
          ...pick(r, [
            "workstream_id",
            "manifest_sha256",
            "account_id",
            "authorization_policy_sha256",
            "authorization_principal_id",
            "authorization_grant_id",
            "authorization_decision_id",
            "authorization_account_generation",
            "authorization_revision",
          ]),
          provider_id: "cp-conversation-intelligence",
        },
      );
      break;
    case "workstream_rollup_queue": {
      const running = r.state === "running";
      add("rollup_queue", ["workstream_id"], {
        ...pick(r, [
          "workstream_id",
          "manifest_sha256",
          "state",
          "fence",
          "failed_at",
          "failure_code",
          "updated_at",
        ]),
        state: running ? "pending" : String(r.state),
        fence: Number(r.fence) + (running ? 1 : 0),
        attempt_id: null,
        lease_expires_at: null,
      });
      break;
    }
    case "workstream_rollup_summaries":
      add(
        "rollup_summaries",
        ["workstream_id", "rollup_version"],
        pick(r, [
          "workstream_id",
          "rollup_version",
          "manifest_sha256",
          "output_sha256",
          "coverage",
          "freshness",
          "generated_at",
        ]),
        "rollup_summary",
        Number(r.rollup_version),
      );
      break;
    case "workstream_appearance":
      add(
        "appearance",
        ["workstream_id"],
        pick(r, ["workstream_id", "version"]),
        "appearance",
        Number(r.version),
      );
      break;
    case "workstream_appearance_receipts":
      add(
        "appearance_receipts",
        ["command_id"],
        { ...pick(r, ["command_id", "principal_id", "request_sha256"]), origin: "legacy" },
        "appearance_receipt",
        1,
      );
      break;
  }
  return targets;
});
export const insertTarget = Effect.fn("WorkstreamsTransition.insertTarget")(function* (t: Target) {
  const sql = yield* SqlClient.SqlClient,
    db = yield* NativeSql.NativeSql;
  const old =
    yield* sql`SELECT * FROM ${sql(`jones_workstreams_${t.table}`)} WHERE ${sql.and(Object.entries(t.key).map(([k, v]) => sql`${sql(k)}=${v}`))}`;
  if (old.length) return yield* new TransitionError({ reason: "target_row_conflict" });
  const scope = {
    registry_instance_id: String(t.data.registry_instance_id),
    owner_id: String(t.data.owner_id),
  };
  const envelope = t.context ? yield* db.sealJson({ ...scope, ...t.context }, t.value) : undefined;
  yield* sql`INSERT INTO ${sql(`jones_workstreams_${t.table}`)} ${sql.insert(envelope ? { ...t.data, envelope } : { ...t.data })}`;
});
export const readTarget = Effect.fn("WorkstreamsTransition.readTarget")(function* (t: Target) {
  const sql = yield* SqlClient.SqlClient,
    db = yield* NativeSql.NativeSql;
  const rows = yield* sql<
    Record<string, string | number | null>
  >`SELECT * FROM ${sql(`jones_workstreams_${t.table}`)} WHERE ${sql.and(Object.entries(t.key).map(([k, v]) => sql`${sql(k)}=${v}`))}`;
  const row = rows[0];
  if (!row) return yield* new TransitionError({ reason: "target_row_missing" });
  const value = t.context
    ? yield* db.openJson(
        {
          registry_instance_id: String(t.data.registry_instance_id),
          owner_id: String(t.data.owner_id),
          ...t.context,
        },
        String(row.envelope),
      )
    : null;
  const data = Object.fromEntries(Object.keys(t.data).map((k) => [k, row[k]]));
  return { data, value };
});
