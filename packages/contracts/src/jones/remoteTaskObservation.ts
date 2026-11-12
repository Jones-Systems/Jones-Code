import * as Schema from "effect/Schema";
import { EnvironmentId, RunId, RuntimeRequestId, ThreadId } from "../baseSchemas.ts";

export const RemoteBlockingQuestions = Schema.Struct({
  resumeToken: Schema.String.check(Schema.isMaxLength(16_384)),
  requests: Schema.Array(Schema.Struct({ requestId: RuntimeRequestId, runId: RunId })).check(
    Schema.isMaxLength(64),
  ),
});
export type RemoteBlockingQuestions = typeof RemoteBlockingQuestions.Type;

export const RemoteChildProgress = Schema.Struct({
  hasPendingChildRuns: Schema.Boolean,
  latestTerminalRunId: Schema.NullOr(RunId),
  latestTerminalStatus: Schema.NullOr(
    Schema.Literals(["completed", "failed", "cancelled", "interrupted"]),
  ),
}).check(
  Schema.makeFilter(
    (progress) =>
      (progress.latestTerminalRunId === null) === (progress.latestTerminalStatus === null),
  ),
);
export type RemoteChildProgress = typeof RemoteChildProgress.Type;

export const RemoteTaskObservation = Schema.Struct({
  schemaVersion: Schema.Literal("t3.remote-task-observation/v1"),
  environmentId: EnvironmentId,
  threadId: ThreadId,
  originalRunId: RunId,
  blockingQuestions: RemoteBlockingQuestions,
  childProgress: Schema.optional(RemoteChildProgress),
});
export type RemoteTaskObservation = typeof RemoteTaskObservation.Type;

export const LinkedMessagePolicy = Schema.Struct({
  schemaVersion: Schema.Literal("t3.linked-message-policy/v1"),
  authenticatedOrigin: Schema.Literal(true),
  incomingAcceptance: Schema.Literal(true),
});
