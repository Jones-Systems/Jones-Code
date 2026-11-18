import * as Schema from "effect/Schema";
import { EnvironmentId, NonNegativeInt, ThreadId } from "../baseSchemas.ts";
import { OrchestratorMcpEnvironmentTarget } from "../orchestratorMcp.ts";
import { StorageCleanupReport } from "../settings.ts";

const cursor = NonNegativeInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER));
const snapshotId = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}(?![\s\S])/));

export const StorageCleanupReportReadInput = Schema.Struct({
  environmentId: OrchestratorMcpEnvironmentTarget,
  cursor: Schema.optional(cursor),
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 50 }))),
  expectedSnapshot: Schema.optional(snapshotId),
  worktreePath: Schema.optional(Schema.NonEmptyString),
  threadId: Schema.optional(ThreadId),
});
export type StorageCleanupReportReadInput = typeof StorageCleanupReportReadInput.Type;

export const StorageCleanupReportPage = Schema.Struct({
  authorityEffect: Schema.Literal("none"),
  environmentId: EnvironmentId,
  retention: Schema.Literal("session-only"),
  snapshotId,
  selectors: Schema.Struct({
    worktreePath: Schema.NullOr(Schema.String),
    threadId: Schema.NullOr(ThreadId),
  }),
  retainedTotal: cursor,
  matchedCount: cursor,
  cursor,
  nextCursor: Schema.NullOr(cursor),
  report: Schema.NullOr(StorageCleanupReport),
});
export type StorageCleanupReportPage = typeof StorageCleanupReportPage.Type;
