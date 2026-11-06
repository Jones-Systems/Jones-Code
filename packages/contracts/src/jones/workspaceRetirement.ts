import * as Schema from "effect/Schema";
import { ThreadId } from "../baseSchemas.ts";

export const WorkspaceRetirementAction = Schema.Struct({
  action: Schema.Literals(["propose", "retire"]),
  threadId: Schema.optionalKey(ThreadId),
  worktreePath: Schema.optionalKey(Schema.NonEmptyString),
  expectedFingerprint: Schema.optionalKey(Schema.String),
  coveredPaths: Schema.optionalKey(Schema.Array(Schema.String).check(Schema.isMaxLength(10000))),
});
export type WorkspaceRetirementAction = typeof WorkspaceRetirementAction.Type;
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
});
export type WorkspaceRetirementOutcome = typeof WorkspaceRetirementOutcome.Type;
