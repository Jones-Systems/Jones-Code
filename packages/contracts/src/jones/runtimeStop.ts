import * as Schema from "effect/Schema";
import { CommandId, ThreadId, RunId, NonNegativeInt } from "../baseSchemas.ts";
import { ProviderRuntimeBinding } from "./providerRuntimeIdentity.ts";
export const CurrentRuntimeStopTarget = Schema.Struct({
  binding: ProviderRuntimeBinding,
  evidenceRevision: NonNegativeInt,
});
export type CurrentRuntimeStopTarget = typeof CurrentRuntimeStopTarget.Type;
export const StopCurrentThreadRuntimeInput = Schema.Struct({
  commandId: CommandId,
  threadId: ThreadId,
  target: CurrentRuntimeStopTarget,
});
export type StopCurrentThreadRuntimeInput = typeof StopCurrentThreadRuntimeInput.Type;
export const StopCurrentThreadRuntimeResult = Schema.Struct({
  commandId: CommandId,
  threadId: ThreadId,
  status: Schema.Literals(["accepted", "stopped", "unknown"]),
  affectedRunIds: Schema.Array(RunId),
  backgroundCoverage: Schema.Literal("partial"),
});
export type StopCurrentThreadRuntimeResult = typeof StopCurrentThreadRuntimeResult.Type;

export const ReadCurrentRuntimeStopTargetInput = Schema.Struct({ threadId: ThreadId });
export const ReadCurrentRuntimeStopTargetResult = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("available"),
    target: CurrentRuntimeStopTarget,
    backgroundCoverage: Schema.Literal("partial"),
  }),
  Schema.Struct({
    status: Schema.Literal("unavailable"),
    reason: Schema.String,
    backgroundCoverage: Schema.Literal("partial"),
  }),
]);
export type ReadCurrentRuntimeStopTargetResult = typeof ReadCurrentRuntimeStopTargetResult.Type;
export class CurrentRuntimeStopRequestError extends Schema.TaggedError<CurrentRuntimeStopRequestError>()(
  "CurrentRuntimeStopRequestError",
  { reason: Schema.String },
) {}
