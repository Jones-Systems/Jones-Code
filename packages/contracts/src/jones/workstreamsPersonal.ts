import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { EnvironmentId, NonNegativeInt, ThreadId } from "../baseSchemas.ts";
import { compareWorkstreamNameIcons, preserveWorkstreamNameIcon } from "./workstreamNameIcon.ts";

export { preserveWorkstreamNameIcon } from "./workstreamNameIcon.ts";

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
export const isClosedPersonalWorkstream = (lifecycle: PersonalWorkstreamLifecycle): boolean =>
  lifecycle === "completed" || lifecycle === "abandoned";

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

export const PersonalWorkstreamNameIconProjectKey = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(2048),
);
const PersonalWorkstreamNameIcon = Schema.Struct({
  projectKey: Schema.NullOr(PersonalWorkstreamNameIconProjectKey),
  updatedAt: PersonalTimestamp,
});

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
  nameIcon: Schema.optionalKey(PersonalWorkstreamNameIcon),
  orderKey: PersonalWorkstreamOrderKey,
  createdAt: PersonalTimestamp,
  updatedAt: PersonalTimestamp,
  deletedAt: Schema.NullOr(PersonalTimestamp),
  rev: PersonalWorkstreamRev,
  // Older definitions start at zero. Crossing the open/closed boundary advances this counter.
  lifecycleRevision: Schema.optionalKey(NonNegativeInt),
  snoozedAt: Schema.optionalKey(Schema.NullOr(PersonalTimestamp)),
  snoozedUntil: Schema.optionalKey(Schema.NullOr(PersonalTimestamp)),
  origin: PersonalWorkstreamOrigin,
});
export type PersonalWorkstream = typeof PersonalWorkstream.Type;

export function isPersonalWorkstreamSnoozed(
  workstream: { readonly snoozedUntil?: string | null },
  nowMs: number,
): boolean {
  return workstream.snoozedUntil != null && Date.parse(workstream.snoozedUntil) > nowMs;
}

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

/** A current assignment the server made automatically; absent once anyone moves the thread. */
export const PersonalWorkstreamAutoAssignment = Schema.Struct({
  threadId: ThreadId,
  workstreamId: PersonalWorkstreamId,
  revision: NonNegativeInt,
  decidedAt: PersonalTimestamp,
});
export type PersonalWorkstreamAutoAssignment = typeof PersonalWorkstreamAutoAssignment.Type;

export const PersonalWorkstreamsSnapshot = Schema.Struct({
  environmentId: EnvironmentId,
  sequence: NonNegativeInt,
  workstreams: Schema.Array(PersonalWorkstream),
  assignments: Schema.Array(PersonalWorkstreamAssignment),
  importIssue: Schema.NullOr(PersonalWorkstreamsImportIssue),
  supportsNameIcons: Schema.optionalKey(Schema.Literal(true)),
  supportsSnooze: Schema.optionalKey(Schema.Literal(true)),
  // Optional: older servers omit it and older clients ignore it.
  autoAssignments: Schema.optionalKey(Schema.Array(PersonalWorkstreamAutoAssignment)),
});
export type PersonalWorkstreamsSnapshot = typeof PersonalWorkstreamsSnapshot.Type;

export class WorkstreamsPersonalError extends Schema.TaggedError<WorkstreamsPersonalError>()(
  "WorkstreamsPersonalError",
  {
    code: Schema.Literals([
      "thread_not_found",
      "workstream_not_found",
      "workstream_deleted",
      "workstream_closed",
      "unsettled_threads",
      "closure_unavailable",
      "precondition_failed",
      "invalid_request",
      "storage_unavailable",
    ]),
    currentAssignment: Schema.optionalKey(PersonalWorkstreamAssignment),
  },
) {}

export const PersonalWorkstreamMembersInput = Schema.Struct({ workstreamId: PersonalWorkstreamId });
export const PersonalWorkstreamMembers = Schema.Array(
  Schema.Struct({
    threadId: ThreadId,
    title: Schema.String,
    settled: Schema.Boolean,
  }),
);
export type PersonalWorkstreamMembers = typeof PersonalWorkstreamMembers.Type;

export const PutPersonalWorkstreamInput = Schema.Struct({
  workstream: PersonalWorkstream,
  expected: Schema.optionalKey(PersonalWorkstream),
});
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

const personalWorkstreamBaseFields = (workstream: PersonalWorkstream) => [
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
  workstream.lifecycleRevision ?? 0,
  ...(workstream.snoozedAt == null ? [] : [workstream.snoozedAt, workstream.snoozedUntil ?? null]),
];

export const canonicalPersonalWorkstream = (workstream: PersonalWorkstream): string =>
  JSON.stringify([
    ...personalWorkstreamBaseFields(workstream),
    ...(workstream.nameIcon
      ? [[workstream.nameIcon.projectKey, workstream.nameIcon.updatedAt]]
      : []),
  ]);

/** Keep snooze metadata when a legacy replica submits a definition without the new fields. */
export function preservePersonalWorkstreamSnooze(
  candidate: PersonalWorkstream,
  observed: PersonalWorkstream,
): PersonalWorkstream {
  if (observed.snoozedAt == null || (candidate.snoozedAt ?? "") >= observed.snoozedAt)
    return candidate;
  return {
    ...candidate,
    snoozedAt: observed.snoozedAt,
    snoozedUntil: observed.snoozedUntil ?? null,
  };
}

const compare = (a: string | number, b: string | number): -1 | 0 | 1 =>
  a < b ? -1 : a > b ? 1 : 0;

export const comparePersonalWorkstreams = (
  a: PersonalWorkstream,
  b: PersonalWorkstream,
): -1 | 0 | 1 =>
  compare(Number(a.deletedAt !== null), Number(b.deletedAt !== null)) ||
  compare(a.lifecycleRevision ?? 0, b.lifecycleRevision ?? 0) ||
  compare(a.rev, b.rev) ||
  compare(a.updatedAt, b.updatedAt) ||
  compare(a.origin, b.origin) ||
  compare(
    JSON.stringify(personalWorkstreamBaseFields(a)),
    JSON.stringify(personalWorkstreamBaseFields(b)),
  ) ||
  compareWorkstreamNameIcons(a.nameIcon, b.nameIcon);

export interface NewPersonalWorkstreamInput {
  readonly id: PersonalWorkstreamId;
  readonly name: string;
  readonly orderKey: PersonalWorkstreamOrderKey;
  readonly now: PersonalTimestamp;
  readonly origin: PersonalWorkstreamOrigin;
  readonly lifecycle?: PersonalWorkstreamLifecycle;
  readonly color?: PersonalWorkstreamColor;
  readonly nameIconProjectKey?: string;
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
  nameIconProjectKey,
}: NewPersonalWorkstreamInput): PersonalWorkstream =>
  decodeWorkstream({
    id,
    name: name.trim(),
    lifecycle,
    color,
    ...(nameIconProjectKey === undefined
      ? {}
      : { nameIcon: { projectKey: nameIconProjectKey, updatedAt: now } }),
    orderKey,
    createdAt: now,
    updatedAt: now,
    deletedAt: null,
    rev: 1,
    origin,
  });

export type PersonalWorkstreamPatch = Partial<
  Pick<
    PersonalWorkstream,
    "name" | "lifecycle" | "color" | "orderKey" | "snoozedAt" | "snoozedUntil"
  >
> & { readonly nameIconProjectKey?: string | null };

export const editPersonalWorkstream = (
  base: PersonalWorkstream,
  patch: PersonalWorkstreamPatch,
  now: PersonalTimestamp,
  origin: PersonalWorkstreamOrigin,
): PersonalWorkstream => {
  if (base.deletedAt !== null) throw new Error("Cannot edit a deleted personal Workstream.");
  const lifecycle = patch.lifecycle ?? base.lifecycle;
  const lifecycleChanged =
    isClosedPersonalWorkstream(lifecycle) !== isClosedPersonalWorkstream(base.lifecycle);
  const nameIcon =
    patch.nameIconProjectKey === undefined
      ? base.nameIcon
      : {
          projectKey: patch.nameIconProjectKey,
          updatedAt:
            base.nameIcon && base.nameIcon.updatedAt >= now
              ? DateTime.makeUnsafe(base.nameIcon.updatedAt).pipe(
                  DateTime.add({ milliseconds: 1 }),
                  DateTime.formatIso,
                )
              : now,
        };
  return decodeWorkstream({
    ...base,
    name: patch.name === undefined ? base.name : patch.name.trim(),
    lifecycle,
    ...(lifecycleChanged ? { lifecycleRevision: (base.lifecycleRevision ?? 0) + 1 } : {}),
    color: patch.color === undefined ? base.color : patch.color,
    ...(nameIcon ? { nameIcon } : {}),
    orderKey: patch.orderKey === undefined ? base.orderKey : patch.orderKey,
    ...(patch.snoozedAt === undefined ? {} : { snoozedAt: patch.snoozedAt }),
    ...(patch.snoozedUntil === undefined ? {} : { snoozedUntil: patch.snoozedUntil }),
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
    const [preferred, other] =
      comparePersonalWorkstreams(copy, winner) > 0 ? [copy, winner] : [winner, copy];
    winner = preserveWorkstreamNameIcon(preservePersonalWorkstreamSnooze(preferred, other), other);
  }
  return winner;
};
