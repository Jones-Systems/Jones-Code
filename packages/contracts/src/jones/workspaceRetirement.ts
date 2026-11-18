import * as Schema from "effect/Schema";
import { ForwardCompatibleOptional, ThreadId } from "../baseSchemas.ts";

export const storageCleanupReportFields = {
  operation: ForwardCompatibleOptional(Schema.Literals(["cleanup", "propose", "retire"])),
};

export const WorkspaceRetirementAction = Schema.Struct({
  action: Schema.Literals(["propose", "retire"]),
  threadId: Schema.optionalKey(ThreadId),
  worktreePath: Schema.optionalKey(Schema.NonEmptyString),
  expectedFingerprint: Schema.optionalKey(Schema.String),
  coveredPaths: Schema.optionalKey(Schema.Array(Schema.String).check(Schema.isMaxLength(10000))),
});
export type WorkspaceRetirementAction = typeof WorkspaceRetirementAction.Type;
/**
 * What removing a checkout would lose, for the owner to read before the Retire click. Optional and
 * forward compatible: a server without preserve branches omits it, and an unknown verdict from a
 * newer server drops it rather than failing the report.
 */
export const WorkspaceRetirementRecoverability = Schema.Struct({
  verdict: Schema.Literals(["recreatable", "retirable_after_preserve", "held", "unknown"]),
  /** Ignored roots deleted with the checkout because their content regenerates. */
  regenerable: Schema.Array(Schema.String),
  /** The preserve plan: absent when the server observed none. */
  preserve: Schema.optionalKey(
    Schema.Struct({
      /** Files a preserve commit would add. */
      files: Schema.Number,
      /** Tracked files absent from the checkout that the preserve commit removes. */
      deletions: Schema.Number,
      bytes: Schema.Number,
      /** Remote a push would target; null when the repository has none, absent when unresolved. */
      remote: Schema.optionalKey(Schema.NullOr(Schema.String)),
      /** Paths no preserve branch may carry, with the server's reason code. */
      excluded: Schema.Array(Schema.Struct({ path: Schema.String, reason: Schema.String })),
      excludedBytes: Schema.Number,
    }),
  ),
});
export type WorkspaceRetirementRecoverability = typeof WorkspaceRetirementRecoverability.Type;

/** The branch that keeps a checkout's uncertain content after the owner's Retire. */
export const WorkspaceRetirementPreserve = Schema.Struct({
  branch: Schema.String,
  sha: Schema.String,
  remote: Schema.NullOr(Schema.String),
  verification: Schema.Literals(["remote_verified", "local_only"]),
  excluded: Schema.Array(Schema.Struct({ path: Schema.String, reason: Schema.String })),
  restore: Schema.String,
  forced: Schema.Boolean,
  readback: Schema.optionalKey(
    Schema.Struct({
      local: Schema.Literals(["verified", "failed"]),
      remote: Schema.Literals(["verified", "failed", "not_applicable"]),
    }),
  ),
});
export type WorkspaceRetirementPreserve = typeof WorkspaceRetirementPreserve.Type;

export const WorkspaceRetirementOutcome = Schema.Struct({
  state: Schema.Literals([
    "proposed",
    "held",
    "intent",
    "retired",
    "not_retired",
    "partial",
    "unknown",
  ]),
  reasonCodes: Schema.Array(Schema.String),
  receiptId: Schema.NullOr(Schema.String),
  fingerprint: Schema.NullOr(Schema.String),
  paths: Schema.Array(Schema.String),
  details: Schema.Array(
    Schema.Struct({
      reason: Schema.String,
      target: Schema.String,
      root: Schema.String,
      fact: Schema.String,
      missingFact: Schema.NullOr(Schema.String),
      lineage: Schema.Array(Schema.Record(Schema.String, Schema.String)),
      nextAction: Schema.String,
    }),
  ),
  outputObligations: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      path: Schema.String,
      owner: Schema.String,
      state: Schema.String,
    }),
  ),
  recoverability: ForwardCompatibleOptional(WorkspaceRetirementRecoverability),
  preserve: ForwardCompatibleOptional(WorkspaceRetirementPreserve),
});
export type WorkspaceRetirementOutcome = typeof WorkspaceRetirementOutcome.Type;
