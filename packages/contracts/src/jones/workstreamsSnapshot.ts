import * as Schema from "effect/Schema";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeId as Id,
  WorkstreamsNativeUuid as Uuid,
  WorkstreamsNativeDigest as Digest,
  WorkstreamsNativeSequence as Sequence,
  WorkstreamsNativeVersion as Version,
  WorkstreamsNativeTimestamp as Timestamp,
  WorkstreamsNativeOrigin,
} from "./workstreamsNative.ts";

const WORKSTREAMS_SNAPSHOT_CONTRACT = "workstreams-snapshot/1.0.0";
export const WORKSTREAMS_SNAPSHOT_CLASSES = [
  "workstream_registry_context",
  "workstream_owner_grants",
  "workstream_source_bindings",
  "workstreams",
  "workstream_native_references",
  "workstream_memberships",
  "workstream_declarations",
  "workstream_lifecycle_declarations",
  "workstream_edges",
  "workstream_pr_observations",
  "workstream_command_receipts",
  "workstream_external_reservations",
  "workstream_external_results",
  "workstream_audit_events",
  "workstream_rollup_manifests",
  "workstream_rollup_manifest_accounts",
  "workstream_rollup_queue",
  "workstream_rollup_discovery_cursors",
  "workstream_rollup_summaries",
  "workstream_collection_projections",
  "workstream_history_links",
  "workstream_appearance",
  "workstream_appearance_receipts",
  "workstream_registry_identity",
  "workstream_registry_binding_principals",
  "workstream_registry_binding_requests",
] as const;
export const WorkstreamsSnapshotClass = Schema.Literals([
  ...WORKSTREAMS_SNAPSHOT_CLASSES,
  "jones.principal_environments",
  "jones.legacy_archive",
  "jones.import_runs",
  "jones.rollup_bindings",
]);
export type WorkstreamsSnapshotClass = typeof WorkstreamsSnapshotClass.Type;

// Construct JSON text directly: JSON.stringify(object) reorders integer-shaped keys.
// Encrypted JSON payloads remain strings, preserving their exact bytes and fractional values.
export function canonicalWorkstreamsSnapshotJson(value: unknown): string {
  const ancestors = new Set<object>();
  const encode = (input: unknown, depth: number): string => {
    if (depth > 32) throw new Error("Snapshot JSON depth exceeded");
    if (input === null || typeof input === "string" || typeof input === "boolean")
      return JSON.stringify(input);
    if (typeof input === "number" && Number.isSafeInteger(input) && !Object.is(input, -0))
      return JSON.stringify(input);
    if (typeof input !== "object" || input === null || ancestors.has(input))
      throw new Error("Invalid snapshot JSON");
    if (
      !Array.isArray(input) &&
      Object.getPrototypeOf(input) !== Object.prototype &&
      Object.getPrototypeOf(input) !== null
    )
      throw new Error("Invalid snapshot JSON object");
    ancestors.add(input);
    try {
      if (Array.isArray(input)) {
        if (
          input.length > 100_000 ||
          Reflect.ownKeys(input).length !== input.length + 1 ||
          Object.keys(input).length !== input.length
        )
          throw new Error("Invalid snapshot JSON array");
        return "[" + input.map((item) => encode(item, depth + 1)).join(",") + "]";
      }
      const record = input as Record<string, unknown>;
      const keys = Reflect.ownKeys(record);
      if (keys.some((key) => typeof key !== "string")) throw new Error("Invalid snapshot JSON key");
      return (
        "{" +
        (keys as string[])
          .sort()
          .map((key) => JSON.stringify(key) + ":" + encode(record[key], depth + 1))
          .join(",") +
        "}"
      );
    } finally {
      ancestors.delete(input);
    }
  };
  return encode(value, 0);
}
export const WorkstreamsSnapshotOwner = closed(
  Schema.Struct({
    owner_id: Id,
    server_generation: Version,
    registry_version: Sequence,
    audit_high_water: Sequence,
  }),
);
export const WorkstreamsSnapshotHeader = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_SNAPSHOT_CONTRACT),
    snapshot_id: Uuid,
    producer: closed(
      Schema.Struct({
        system: Schema.Literals(["control-plane", "jones-native"]),
        revision: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
        schema_version: Version,
      }),
    ),
    registry: closed(
      Schema.Struct({ registry_instance_id: Uuid, canonical_origin: WorkstreamsNativeOrigin }),
    ),
    owners: Schema.Array(WorkstreamsSnapshotOwner).check(
      Schema.isMaxLength(1000),
      Schema.makeFilter(
        (owners) => new Set(owners.map((owner) => owner.owner_id)).size === owners.length,
      ),
    ),
    fence: closed(Schema.Struct({ epoch: Version, fenced_at: Timestamp })),
    exported_at: Timestamp,
    open_effects: Sequence,
    encryption: closed(
      Schema.Struct({ mode: Schema.Literals(["stream", "file"]), key_id: Schema.NullOr(Id) }),
    ),
  }),
);
export type WorkstreamsSnapshotHeader = typeof WorkstreamsSnapshotHeader.Type;
const CanonicalRow = Schema.Record(Schema.String, Schema.Unknown).check(
  Schema.makeFilter((row) => {
    try {
      return new TextEncoder().encode(canonicalWorkstreamsSnapshotJson(row)).length <= 8_388_608;
    } catch {
      return false;
    }
  }),
);
export const WorkstreamsSnapshotRow = closed(
  Schema.Struct({
    class: WorkstreamsSnapshotClass,
    key: Schema.Array(
      // CP singleton identities have boolean keys; history links use a -1 predecessor sentinel.
      Schema.Union([
        Schema.String.check(Schema.isMaxLength(4096)),
        Schema.Boolean,
        Schema.Literal(-1),
        Sequence,
      ]),
    ).check(Schema.isMinLength(1), Schema.isMaxLength(16)),
    row: CanonicalRow,
  }),
);
export type WorkstreamsSnapshotRow = typeof WorkstreamsSnapshotRow.Type;
export const WorkstreamsSnapshotClassFooter = closed(
  Schema.Struct({
    class: WorkstreamsSnapshotClass,
    count: Sequence,
    sha256: Digest,
  }),
);
export const WorkstreamsSnapshotTrailer = closed(
  Schema.Struct({
    snapshot_sha256: Digest,
    complete: Schema.Literal(true),
  }),
);
export const WorkstreamsSnapshotLine = Schema.Union([
  WorkstreamsSnapshotHeader,
  WorkstreamsSnapshotRow,
  WorkstreamsSnapshotClassFooter,
  WorkstreamsSnapshotTrailer,
]);
export type WorkstreamsSnapshotLine = typeof WorkstreamsSnapshotLine.Type;
