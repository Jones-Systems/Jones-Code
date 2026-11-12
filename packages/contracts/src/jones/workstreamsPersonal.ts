import * as Schema from "effect/Schema";
import { EnvironmentId, NonNegativeInt, ThreadId } from "../baseSchemas.ts";

export const PersonalWorkstreamId = Schema.String.check(
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/),
);
export type PersonalWorkstreamId = typeof PersonalWorkstreamId.Type;

export const PersonalWorkstreamName = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(160),
  Schema.makeFilter((value: string) => value === value.trim()),
);
export type PersonalWorkstreamName = typeof PersonalWorkstreamName.Type;

export const PersonalWorkstreamLifecycle = Schema.Literals([
  "planned",
  "active",
  "paused",
  "completed",
  "deferred",
  "abandoned",
]);
export type PersonalWorkstreamLifecycle = typeof PersonalWorkstreamLifecycle.Type;

export const PersonalWorkstreamColor = Schema.NullOr(
  Schema.String.check(Schema.isPattern(/^#[0-9A-F]{6}$/)),
);
export type PersonalWorkstreamColor = typeof PersonalWorkstreamColor.Type;

export const PersonalWorkstreamOrderKey = Schema.String.check(
  Schema.isPattern(/^[a-z]{0,63}[b-z]$/),
);
export type PersonalWorkstreamOrderKey = typeof PersonalWorkstreamOrderKey.Type;

export const PersonalTimestamp = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/),
);
export type PersonalTimestamp = typeof PersonalTimestamp.Type;

export const PersonalWorkstreamRev = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);
export type PersonalWorkstreamRev = typeof PersonalWorkstreamRev.Type;

export const PersonalWorkstreamOrigin = Schema.String.check(
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/),
);
export type PersonalWorkstreamOrigin = typeof PersonalWorkstreamOrigin.Type;

export const PersonalWorkstream = Schema.Struct({
  id: PersonalWorkstreamId,
  name: PersonalWorkstreamName,
  lifecycle: PersonalWorkstreamLifecycle,
  color: PersonalWorkstreamColor,
  orderKey: PersonalWorkstreamOrderKey,
  createdAt: PersonalTimestamp,
  updatedAt: PersonalTimestamp,
  deletedAt: Schema.NullOr(PersonalTimestamp),
  rev: PersonalWorkstreamRev,
  origin: PersonalWorkstreamOrigin,
});
export type PersonalWorkstream = typeof PersonalWorkstream.Type;

export const PersonalWorkstreamAssignment = Schema.Struct({
  threadId: ThreadId,
  workstreamId: Schema.NullOr(PersonalWorkstreamId),
  revision: NonNegativeInt,
  updatedAt: Schema.NullOr(PersonalTimestamp),
});
export type PersonalWorkstreamAssignment = typeof PersonalWorkstreamAssignment.Type;

export const PersonalWorkstreamsImportIssue = Schema.Struct({
  importId: Schema.NullOr(Schema.String),
  reason: Schema.Literals([
    "invalid_file",
    "wrong_environment",
    "content_mismatch",
    "thread_not_found",
    "conflict",
  ]),
});
export type PersonalWorkstreamsImportIssue = typeof PersonalWorkstreamsImportIssue.Type;

export const PersonalWorkstreamsSnapshot = Schema.Struct({
  environmentId: EnvironmentId,
  sequence: NonNegativeInt,
  workstreams: Schema.Array(PersonalWorkstream),
  assignments: Schema.Array(PersonalWorkstreamAssignment),
  importIssue: Schema.NullOr(PersonalWorkstreamsImportIssue),
});
export type PersonalWorkstreamsSnapshot = typeof PersonalWorkstreamsSnapshot.Type;

export class WorkstreamsPersonalError extends Schema.TaggedError<WorkstreamsPersonalError>()(
  "WorkstreamsPersonalError",
  {
    code: Schema.Literals([
      "thread_not_found",
      "workstream_not_found",
      "workstream_deleted",
      "precondition_failed",
      "invalid_request",
      "storage_unavailable",
    ]),
    currentAssignment: Schema.optionalKey(PersonalWorkstreamAssignment),
  },
) {}

export const PutPersonalWorkstreamInput = Schema.Struct({ workstream: PersonalWorkstream });
export type PutPersonalWorkstreamInput = typeof PutPersonalWorkstreamInput.Type;
export const PutPersonalWorkstreamResult = Schema.Struct({
  outcome: Schema.Literals(["applied", "already_satisfied", "superseded"]),
  workstream: PersonalWorkstream,
  sequence: NonNegativeInt,
});
export type PutPersonalWorkstreamResult = typeof PutPersonalWorkstreamResult.Type;

export const AssignPersonalWorkstreamInput = Schema.Struct({
  threadId: ThreadId,
  workstream: Schema.NullOr(PersonalWorkstream),
  expectedRevision: NonNegativeInt,
});
export type AssignPersonalWorkstreamInput = typeof AssignPersonalWorkstreamInput.Type;
export const AssignPersonalWorkstreamResult = Schema.Struct({
  outcome: Schema.Literals(["applied", "already_satisfied"]),
  assignment: PersonalWorkstreamAssignment,
  workstream: Schema.NullOr(PersonalWorkstream),
  sequence: NonNegativeInt,
});
export type AssignPersonalWorkstreamResult = typeof AssignPersonalWorkstreamResult.Type;
export type PutResult = PutPersonalWorkstreamResult;
export type AssignResult = AssignPersonalWorkstreamResult;

export const canonicalPersonalWorkstream = (workstream: PersonalWorkstream): string =>
  JSON.stringify([
    workstream.id,
    workstream.name,
    workstream.lifecycle,
    workstream.color,
    workstream.orderKey,
    workstream.createdAt,
    workstream.updatedAt,
    workstream.deletedAt,
    workstream.rev,
    workstream.origin,
  ]);

const compare = (a: string | number, b: string | number): -1 | 0 | 1 =>
  a < b ? -1 : a > b ? 1 : 0;

export const comparePersonalWorkstreams = (
  a: PersonalWorkstream,
  b: PersonalWorkstream,
): -1 | 0 | 1 =>
  compare(Number(a.deletedAt !== null), Number(b.deletedAt !== null)) ||
  compare(a.rev, b.rev) ||
  compare(a.updatedAt, b.updatedAt) ||
  compare(a.origin, b.origin) ||
  compare(canonicalPersonalWorkstream(a), canonicalPersonalWorkstream(b));

export interface NewPersonalWorkstreamInput {
  readonly id: PersonalWorkstreamId;
  readonly name: string;
  readonly orderKey: PersonalWorkstreamOrderKey;
  readonly now: PersonalTimestamp;
  readonly origin: PersonalWorkstreamOrigin;
  readonly lifecycle?: PersonalWorkstreamLifecycle;
  readonly color?: PersonalWorkstreamColor;
}

const decodeWorkstream = Schema.decodeUnknownSync(PersonalWorkstream);

export const newPersonalWorkstream = ({
  id,
  name,
  orderKey,
  now,
  origin,
  lifecycle = "active",
  color = null,
}: NewPersonalWorkstreamInput): PersonalWorkstream =>
  decodeWorkstream({
    id,
    name: name.trim(),
    lifecycle,
    color,
    orderKey,
    createdAt: now,
    updatedAt: now,
    deletedAt: null,
    rev: 1,
    origin,
  });

export type PersonalWorkstreamPatch = Partial<
  Pick<PersonalWorkstream, "name" | "lifecycle" | "color" | "orderKey">
>;

export const editPersonalWorkstream = (
  base: PersonalWorkstream,
  patch: PersonalWorkstreamPatch,
  now: PersonalTimestamp,
  origin: PersonalWorkstreamOrigin,
): PersonalWorkstream => {
  if (base.deletedAt !== null) throw new Error("Cannot edit a deleted personal Workstream.");
  return decodeWorkstream({
    ...base,
    name: patch.name === undefined ? base.name : patch.name.trim(),
    lifecycle: patch.lifecycle === undefined ? base.lifecycle : patch.lifecycle,
    color: patch.color === undefined ? base.color : patch.color,
    orderKey: patch.orderKey === undefined ? base.orderKey : patch.orderKey,
    updatedAt: now,
    rev: base.rev + 1,
    origin,
  });
};

export const deletePersonalWorkstream = (
  base: PersonalWorkstream,
  now: PersonalTimestamp,
  origin: PersonalWorkstreamOrigin,
): PersonalWorkstream => ({
  ...editPersonalWorkstream(base, {}, now, origin),
  deletedAt: now,
});

export const mergePersonalWorkstreams = (
  copies: readonly PersonalWorkstream[],
): PersonalWorkstream => {
  const first = copies[0];
  if (first === undefined) throw new Error("Cannot merge an empty personal Workstream set.");
  let winner = first;
  for (const copy of copies) {
    if (copy.id !== first.id) throw new Error("Personal Workstream copies must share an id.");
    if (comparePersonalWorkstreams(copy, winner) > 0) winner = copy;
  }
  return winner;
};
