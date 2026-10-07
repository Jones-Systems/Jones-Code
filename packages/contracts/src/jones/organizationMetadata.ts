import * as Schema from "effect/Schema";
import {
  EnvironmentId,
  ThreadId,
  ProjectId,
  RunId,
  TrimmedNonEmptyString,
  IsoDateTime,
  NonNegativeInt,
} from "../baseSchemas.ts";

export const NativeInvocationContext = Schema.Struct({
  environmentId: EnvironmentId,
  threadId: ThreadId,
  effectiveBaseDir: TrimmedNonEmptyString,
  loopbackOrigin: Schema.NullOr(TrimmedNonEmptyString),
  serverVersion: TrimmedNonEmptyString,
  serverGeneration: Schema.Null,
});
export type NativeInvocationContext = typeof NativeInvocationContext.Type;

export const OrganizationThreadMetadata = Schema.Struct({
  threadId: ThreadId,
  title: Schema.String.check(Schema.isMaxLength(512)),
  projectId: ProjectId,
  pinnedAt: Schema.NullOr(IsoDateTime),
  pinOrderKey: Schema.NullOr(Schema.String),
  activeOrderKey: Schema.NullOr(Schema.String),
  snoozedUntil: Schema.NullOr(IsoDateTime),
  settledOverride: Schema.NullOr(Schema.Literals(["active", "settled"])),
  settledAt: Schema.NullOr(IsoDateTime),
  archivedAt: Schema.NullOr(IsoDateTime),
  createdAt: IsoDateTime,
  projectionUpdatedAt: IsoDateTime,
  latestUserMessageAt: Schema.NullOr(IsoDateTime),
  latestRunId: Schema.NullOr(RunId),
  activeRunId: Schema.NullOr(RunId),
  status: Schema.Literals([
    "idle",
    "preparing",
    "queued",
    "starting",
    "running",
    "waiting",
    "completed",
    "interrupted",
    "failed",
    "cancelled",
    "rolled_back",
  ]),
  latestRunRequestedAt: Schema.NullOr(IsoDateTime),
  latestRunStartedAt: Schema.NullOr(IsoDateTime),
  latestRunCompletedAt: Schema.NullOr(IsoDateTime),
  hasPendingApprovals: Schema.Boolean,
  hasPendingUserInput: Schema.Boolean,
  hasActionableProposedPlan: Schema.Boolean,
});
export type OrganizationThreadMetadata = typeof OrganizationThreadMetadata.Type;

export const OrganizationThreadMetadataPage = Schema.Struct({
  environmentId: EnvironmentId,
  snapshotSequence: NonNegativeInt,
  observedAt: IsoDateTime,
  threads: Schema.Array(OrganizationThreadMetadata),
  nextOffset: Schema.NullOr(NonNegativeInt),
});
export type OrganizationThreadMetadataPage = typeof OrganizationThreadMetadataPage.Type;
