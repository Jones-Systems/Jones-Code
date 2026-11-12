import * as Schema from "effect/Schema";
import { ThreadId, RunId, TrimmedNonEmptyString } from "../baseSchemas.ts";
import { OrchestrationV2ProviderGoal } from "../orchestrationV2.ts";

const key = TrimmedNonEmptyString.check(Schema.isMaxLength(256));
export const ThreadGoalInput = Schema.Struct({
  threadId: Schema.optional(ThreadId),
  action: Schema.Literals(["read", "pause", "resume", "clear", "replace"]),
  objective: Schema.optional(TrimmedNonEmptyString.check(Schema.isMaxLength(120_000))),
  controlToken: Schema.optional(key),
  clientRequestId: Schema.optional(key),
});
export type ThreadGoalInput = typeof ThreadGoalInput.Type;
export const ThreadGoalResult = Schema.Struct({
  threadId: ThreadId,
  action: ThreadGoalInput.fields.action,
  outcome: Schema.Literals([
    "observed",
    "applied",
    "accepted",
    "unsupported",
    "unavailable",
    "stale",
    "busy",
    "unknown",
  ]),
  source: Schema.Literals(["native_rpc", "provider_projection", "claude_transcript", "none"]),
  goal: Schema.NullOr(OrchestrationV2ProviderGoal),
  observedAt: Schema.String,
  reason: Schema.String,
  controlToken: Schema.optional(key),
  runId: Schema.optional(RunId),
  replayed: Schema.optional(Schema.Boolean),
});
export type ThreadGoalResult = typeof ThreadGoalResult.Type;
