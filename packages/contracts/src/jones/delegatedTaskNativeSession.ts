import * as Schema from "effect/Schema";
import { RunAttemptId, RunId } from "../baseSchemas.ts";

export const DelegatedTaskNativeSession = Schema.Struct({
  nativeSessionId: Schema.String,
  sourceRunId: RunId,
  sourceAttemptId: RunAttemptId,
  evidence: Schema.Literal("attempt"),
}).annotate({
  description:
    "Captured destination of the accepted source attempt, not provider-verified continuation. Continuation still checks the current binding.",
});
export type DelegatedTaskNativeSession = typeof DelegatedTaskNativeSession.Type;
