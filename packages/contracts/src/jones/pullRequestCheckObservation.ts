import * as Schema from "effect/Schema";

import { TrimmedNonEmptyString } from "../baseSchemas.ts";

/** A same-head observation is distinct from knowledge of the complete required gate set. */
export const PullRequestCheckObservation = Schema.Struct({
  headSha: TrimmedNonEmptyString,
  enumerationComplete: Schema.Boolean,
  workflowState: Schema.Literals(["complete", "pending", "unknown"]),
  requiredSetKnown: Schema.Boolean,
  requiredState: Schema.Literals(["unknown", "missing", "pending", "failure", "success"]),
  requiredChecks: Schema.Array(TrimmedNonEmptyString),
});
export type PullRequestCheckObservation = typeof PullRequestCheckObservation.Type;
