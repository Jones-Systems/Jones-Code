import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";

export const WORK_QUEUE_METADATA_MAX_BYTES = 1_048_576;
const closed = <Fields extends Schema.Struct.Fields>(fields: Fields) => {
  const schema = Schema.Struct(fields);
  return Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter((value) =>
        Reflect.ownKeys(value).every((key) => Object.hasOwn(fields, key)),
      ),
    ),
  );
};
const Id = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/));
const NativeId = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(512),
  // oxlint-disable-next-line no-control-regex -- Native identifiers must exclude control characters.
  Schema.isPattern(/^[^\u0000-\u001f\u007f]+$/),
);
const Integer = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);
const Positive = Integer.check(Schema.isGreaterThanOrEqualTo(1));
const Timestamp = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$/),
  Schema.makeFilter(
    (value) =>
      Number.isFinite(Date.parse(value)) &&
      DateTime.formatIso(DateTime.makeUnsafe(value)).slice(0, 19) === value.slice(0, 19),
  ),
);
export const WorkQueueMetadataSource = closed({
  queue_id: Id,
  host_id: Id,
  environment_ref: NativeId,
  exporter_instance_id: Id,
});
export type WorkQueueMetadataSource = typeof WorkQueueMetadataSource.Type;
const CanonicalBinding = closed({
  owner_id: Id,
  server_generation: Positive,
  registry_version: Integer,
  membership_id: Id,
  native_reference_id: Id,
  source_instance_id: Id,
  native_thread_id: NativeId,
  authority_namespace: NativeId,
  store_generation: Positive,
  expires_at: Timestamp,
});
const itemFields = {
  request_id: Id,
  workstream_id: Id,
  canonical_binding: Schema.NullOr(CanonicalBinding),
  entry_kind: Schema.Literals(["ordinary", "flexible", "bootstrap"]),
  request_kind: Schema.Literals(["initial", "owner_followup", "auto_continue", "bootstrap"]),
  lane: Schema.Literals(["high", "normal"]),
  queue_state: Schema.Literals([
    "queued",
    "reserved",
    "waiting_remote",
    "observed_terminal",
    "held",
    "cancelled",
    "reconciliation_required",
    "unknown",
  ]),
  submitted_at_ms: Schema.NullOr(Integer),
  target: Schema.NullOr(closed({ host_id: Id, environment_ref: NativeId, thread_id: NativeId })),
  dispatch_status: Schema.NullOr(Schema.Literals(["accepted", "rejected", "unknown"])),
  native_command_status: Schema.NullOr(Schema.Literals(["accepted", "rejected", "not_found"])),
  finish_line: Schema.Literal("not_tracked"),
};
const ItemV1 = closed(itemFields);
const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const NativeHandoff = closed({
  command_id: NativeId,
  message_id: NativeId,
  command_status: Schema.Literal("accepted"),
  correlation_status: Schema.Literal("exact"),
  receipt_sequence: Integer,
  turn_id: NativeId,
  turn_state: Schema.Literals(["pending", "running", "completed", "interrupted", "error"]),
  observed_at_ms: Integer,
});
const ItemV2 = closed({
  ...itemFields,
  payload_sha256: Schema.NullOr(Digest),
  native_handoff: Schema.NullOr(NativeHandoff),
}).check(
  Schema.makeFilter(
    (item) => item.native_handoff === null || (item.target !== null && item.payload_sha256 !== null),
  ),
);
const snapshotFields = {
  source: WorkQueueMetadataSource,
  observed_at_ms: Integer,
  snapshot_token: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  coverage: Schema.Literals(["complete", "partial"]),
};
const validSnapshot = (value: {
  readonly items: readonly (typeof ItemV1.Type)[];
  readonly observed_at_ms: number;
}) =>
  new Set(value.items.map((item) => item.request_id)).size === value.items.length &&
  value.items.every(
    (item) =>
      (item.submitted_at_ms === null || item.submitted_at_ms <= value.observed_at_ms) &&
      (item.canonical_binding === null ||
        (Date.parse(item.canonical_binding.expires_at) > value.observed_at_ms &&
          item.target !== null &&
          item.target.thread_id === item.canonical_binding.native_thread_id)),
  );
export const WorkQueueMetadataV1 = closed({
  schema: Schema.Literal("codex.t3-work-queue-metadata/v1"),
  ...snapshotFields,
  items: Schema.Array(ItemV1).check(Schema.isMaxLength(1000)),
  authority_effect: Schema.Literal("none"),
}).check(Schema.makeFilter(validSnapshot));
export const WorkQueueMetadataV2 = closed({
  schema: Schema.Literal("codex.t3-work-queue-metadata/v2"),
  ...snapshotFields,
  items: Schema.Array(ItemV2).check(Schema.isMaxLength(1000)),
  authority_effect: Schema.Literal("none"),
}).check(Schema.makeFilter(validSnapshot));
export const WorkQueueMetadata = Schema.Union([WorkQueueMetadataV1, WorkQueueMetadataV2]);
export type WorkQueueMetadata = typeof WorkQueueMetadata.Type;
export const WorkQueueMetadataResult = Schema.Union([
  closed({
    status: Schema.Literals(["ready", "partial", "stale"]),
    snapshot: WorkQueueMetadata,
    expires_at_ms: Integer,
  }),
  closed({ status: Schema.Literal("unconfigured"), reason: Schema.Literal("not_configured") }),
  closed({
    status: Schema.Literal("unavailable"),
    reason: Schema.Literals([
      "invalid_configuration",
      "source_unavailable",
      "invalid_artifact",
      "source_mismatch",
      "future_sample",
    ]),
  }),
]);
export type WorkQueueMetadataResult = typeof WorkQueueMetadataResult.Type;
