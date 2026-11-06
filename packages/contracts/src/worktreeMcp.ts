import { WorkspaceRetirementOutcome } from "./jones/workspaceRetirement.ts";
import * as Schema from "effect/Schema";

import { TrimmedNonEmptyString } from "./baseSchemas.ts";

/** Uses the managed worktree folder; handoff detaches the provider, ending this turn and resuming there next run. */
export const WorktreeMcpHandoffInput = Schema.Struct({
  branch: TrimmedNonEmptyString,
  baseRef: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description: "Default: checked-out branch.",
    }),
  ),
  startFromOrigin: Schema.optional(
    Schema.Boolean.annotate({
      description:
        "True: fetch, start from the remote ref; check `startedFromOrigin`. Default: server setting.",
    }),
  ),
  runSetupScript: Schema.optional(
    Schema.Boolean.annotate({
      description: "Default true.",
    }),
  ),
  continuationPrompt: Schema.optional(
    TrimmedNonEmptyString.check(Schema.isMaxLength(120_000)).annotate({
      description: "Work to resume in the worktree next turn. Omitted: thread waits.",
    }),
  ),
});
export type WorktreeMcpHandoffInput = typeof WorktreeMcpHandoffInput.Type;

export const WorktreeMcpSetupScriptStatus = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("started"),
    scriptName: TrimmedNonEmptyString,
    terminalId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    status: Schema.Literal("no-script"),
  }),
  Schema.Struct({
    status: Schema.Literal("skipped"),
  }),
  Schema.Struct({
    status: Schema.Literal("failed"),
    detail: Schema.String,
  }),
]);
export type WorktreeMcpSetupScriptStatus = typeof WorktreeMcpSetupScriptStatus.Type;

export const WorktreeMcpContinuationStatus = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("scheduled"),
    delivery: Schema.Literals(["started", "queued", "steered", "restarted"]),
  }),
  Schema.Struct({
    status: Schema.Literal("skipped"),
  }),
  Schema.Struct({
    status: Schema.Literal("failed"),
    detail: Schema.String,
  }),
]);
export type WorktreeMcpContinuationStatus = typeof WorktreeMcpContinuationStatus.Type;

export const WorktreeMcpHandoffResult = Schema.Struct({
  worktreePath: TrimmedNonEmptyString,
  branch: TrimmedNonEmptyString,
  baseRef: TrimmedNonEmptyString,
  startedFromOrigin: Schema.Boolean,
  setupScript: WorktreeMcpSetupScriptStatus,
  continuation: WorktreeMcpContinuationStatus,
  note: Schema.String,
});
export type WorktreeMcpHandoffResult = typeof WorktreeMcpHandoffResult.Type;

const ProvenanceDirectoryIdentity = Schema.Struct({
  version: Schema.Literal(1),
  device: Schema.String,
  inode: Schema.String,
  birth: Schema.String,
});
export const WorktreeProvenanceEntry = Schema.Struct({
  schema: Schema.Literal("workspace-provenance/v1"),
  incarnationId: Schema.String,
  environmentId: Schema.String,
  projectId: Schema.String,
  path: Schema.String,
  branch: Schema.NullOr(Schema.String),
  head: Schema.String,
  reason: Schema.Literals(["launch", "delegate", "handoff", "recreate", "reference", "backfill"]),
  createdAt: Schema.String,
  createdBy: Schema.String,
  baseRef: Schema.NullOr(Schema.String),
  baseHead: Schema.NullOr(Schema.String),
  birth: ProvenanceDirectoryIdentity,
  adminId: Schema.NullOr(ProvenanceDirectoryIdentity),
  predecessor: Schema.NullOr(Schema.String),
  threads: Schema.Array(Schema.String),
  tasks: Schema.Array(Schema.String),
  facts: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      kind: Schema.String,
      recordedAt: Schema.String,
      reference: Schema.String,
    }),
  ),
  authorityEffect: Schema.Literal("none"),
});
export type WorktreeProvenanceEntry = typeof WorktreeProvenanceEntry.Type;

export const WorktreeMcpStatusResult = Schema.Struct({
  retirement: Schema.optionalKey(WorkspaceRetirementOutcome),
  retirementObservation: Schema.optionalKey(Schema.Literals(["recorded", "none", "unavailable"])),
  provenance: Schema.optional(Schema.Array(WorktreeProvenanceEntry)),
  attached: Schema.Boolean,
  worktreePath: Schema.NullOr(TrimmedNonEmptyString),
  branch: Schema.NullOr(TrimmedNonEmptyString),
  projectWorkspaceRoot: TrimmedNonEmptyString,
  defaultStartFromOrigin: Schema.Boolean.annotate({
    description: "`startFromOrigin` default for `t3_worktree_handoff`.",
  }),
});
export type WorktreeMcpStatusResult = typeof WorktreeMcpStatusResult.Type;

export class WorktreeMcpFailure extends Schema.TaggedError<WorktreeMcpFailure>()(
  "WorktreeMcpFailure",
  {
    code: Schema.Literals([
      "capability_denied",
      "thread_not_found",
      "project_not_found",
      "already_in_worktree",
      "handoff_in_progress",
      "invalid_request",
      "operation_failed",
      "thread_credential_required",
    ]),
    message: Schema.String,
  },
) {}
