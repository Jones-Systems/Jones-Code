import * as Schema from "effect/Schema";
import type { ComputeFailureReason } from "../serverParallelism/computeContract.ts";

export class ComputeFailure extends Schema.TaggedError<ComputeFailure>()("ComputeFailure", {
  reason: Schema.Literals([
    "queue-full",
    "queue-timeout",
    "capacity-held",
    "unavailable",
    "deadline",
    "cancelled",
    "worker-exit",
    "protocol",
    "oversize",
    "finder-create",
    "finder-search",
    "finder-refresh",
    "retirement-unknown",
  ]),
  scanTimedOut: Schema.optional(Schema.Boolean),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return "Workspace computation failed.";
  }
}

export const isComputeFailure = Schema.is(ComputeFailure);

export const computeFailure = (reason: ComputeFailureReason, cause?: unknown) =>
  isComputeFailure(cause) && cause.reason === reason
    ? cause
    : new ComputeFailure({ reason, ...(cause === undefined ? {} : { cause }) });
