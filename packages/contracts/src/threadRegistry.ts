import * as Schema from "effect/Schema";
import { NonNegativeInt, PositiveInt } from "./baseSchemas.ts";
import { T3ThreadPlacement } from "./workstreamPlacements.ts";

const Id = Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(4096));
export const InferredWorkstreamRef = Id.check(Schema.isPattern(/^inferred:.+$/));
export const NativeWorkstreamRef = Id.check(Schema.isPattern(/^native:.+$/));
export const ThreadRegistryWorkstreamRef = Schema.Union([
  NativeWorkstreamRef,
  InferredWorkstreamRef,
]);
const strict = <const Fields extends Schema.Struct.Fields>(fields: Fields) =>
  Schema.Record(Schema.String, Schema.Unknown)
    .check(Schema.isPropertyNames(Schema.Literals(Object.keys(fields))))
    .pipe(Schema.decodeTo(Schema.Struct(fields)));
const Origin = Schema.Literals(["owner", "grouping", "classifier"]);
export const ThreadRegistryLabel = Schema.Struct({
  label_id: InferredWorkstreamRef,
  name: Schema.String,
  description: Schema.String,
  state: Schema.Literals(["active", "retired"]),
  revision: PositiveInt,
  origin: Origin,
});
export type ThreadRegistryLabel = typeof ThreadRegistryLabel.Type;
export const ThreadRegistryAssociation = Schema.Struct({
  subject: Id.check(Schema.isPattern(/^(thread|prompt):.+$/)),
  workstream_ref: InferredWorkstreamRef,
  state: Schema.Literals(["active", "suppressed"]),
  revision: PositiveInt,
  origin: Origin,
  job_id: Schema.NullOr(Schema.String),
  command_id: Schema.optional(Schema.NullOr(Schema.String)),
  receipt_id: Schema.optional(Schema.NullOr(Schema.String)),
});
export type ThreadRegistryAssociation = typeof ThreadRegistryAssociation.Type;
export const ThreadRegistryThread = Schema.Struct({
  thread_key: Id,
  registration: Schema.NullOr(Schema.Record(Schema.String, Schema.Unknown)),
  summary: Schema.NullOr(Schema.Record(Schema.String, Schema.Unknown)),
  activity: Schema.NullOr(Schema.Record(Schema.String, Schema.Unknown)),
  associations: Schema.Array(ThreadRegistryAssociation),
  freshness: Schema.Record(Schema.String, Schema.Unknown),
});
export type ThreadRegistryThread = typeof ThreadRegistryThread.Type;
export const ThreadRegistrySnapshot = Schema.Struct({
  schema: Schema.Literal("voice.registry-read/v1"),
  snapshot_revision: NonNegativeInt,
  next_cursor: Schema.NullOr(Id),
  partial: Schema.Boolean,
  unavailable: Schema.Array(Schema.String),
  threads: Schema.Array(ThreadRegistryThread).check(Schema.isMaxLength(200)),
});
export type ThreadRegistrySnapshot = typeof ThreadRegistrySnapshot.Type;
export const ThreadRegistryComposedThread = Schema.Struct({
  ...ThreadRegistryThread.fields,
  native_memberships: Schema.Array(
    Schema.Struct({
      workstream_ref: NativeWorkstreamRef,
      placement: T3ThreadPlacement,
    }),
  ),
});
export const ThreadRegistryComposedSnapshot = Schema.Struct({
  ...ThreadRegistrySnapshot.fields,
  threads: Schema.Array(ThreadRegistryComposedThread).check(Schema.isMaxLength(200)),
});
export type ThreadRegistryComposedSnapshot = typeof ThreadRegistryComposedSnapshot.Type;
export const ThreadRegistryWorkstreams = Schema.Struct({
  schema: Schema.Literal("voice.registry-read/v1"),
  snapshot_revision: NonNegativeInt,
  workstreams: Schema.Array(ThreadRegistryLabel),
});
export type ThreadRegistryWorkstreams = typeof ThreadRegistryWorkstreams.Type;
export const ThreadRegistryEvents = Schema.Struct({
  schema: Schema.Literal("voice.registry-read/v1"),
  snapshot_revision: NonNegativeInt,
  events: Schema.Array(
    Schema.Struct({
      sequence: PositiveInt,
      kind: Schema.String,
      key: Schema.String,
      record: Schema.Unknown,
      request_id: Schema.NullOr(Schema.String),
    }),
  ),
  next_after: NonNegativeInt,
});
export type ThreadRegistryEvents = typeof ThreadRegistryEvents.Type;
export const ThreadRegistryAssociationPayload = strict({
  schema: Schema.Literal("voice.association-mutation/v1"),
  subject: Id.check(Schema.isPattern(/^(thread|prompt):.+$/)),
  workstream_ref: InferredWorkstreamRef,
  state: Schema.Literals(["active", "suppressed"]),
  expected_revision: NonNegativeInt,
  request_id: Id,
  command_id: Schema.optional(Schema.NullOr(Id)),
  receipt_id: Schema.optional(Schema.NullOr(Id)),
});
export type ThreadRegistryAssociationPayload = typeof ThreadRegistryAssociationPayload.Type;
export const ThreadRegistryLabelPayload = strict({
  schema: Schema.Literal("voice.association-mutation/v1"),
  label_id: InferredWorkstreamRef,
  name: Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(200)),
  description: Schema.String.check(Schema.isMaxLength(2000)),
  state: Schema.Literals(["active", "retired"]),
  expected_revision: NonNegativeInt,
  request_id: Id,
});
export type ThreadRegistryLabelPayload = typeof ThreadRegistryLabelPayload.Type;
export const ThreadRegistryMutationReceipt = Schema.Struct({
  schema: Schema.Literal("voice.registry-receipt/v1"),
  request_id: Id,
  revision: PositiveInt,
  event_sequence: PositiveInt,
  record: Schema.Union([ThreadRegistryAssociation, ThreadRegistryLabel]),
});
export type ThreadRegistryMutationReceipt = typeof ThreadRegistryMutationReceipt.Type;
