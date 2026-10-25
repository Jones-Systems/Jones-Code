import { ORCHESTRATION_V2_WS_METHODS, type OrchestrationV2Command } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Data from "effect/Data";
import { getInitialServerConfig, request } from "../../rpc/client.ts";

class QueuedSteerBatchUnavailableError extends Data.TaggedError(
  "QueuedSteerBatchUnavailableError",
)<{ readonly message: string }> {}

export type PromoteQueuedBatchInput = Omit<
  Extract<OrchestrationV2Command, { type: "queued-message.promote-batch-to-steer" }>,
  "type"
>;

export const promoteQueuedBatch = Effect.fn("JonesQueue.promoteQueuedBatch")(function* (
  input: PromoteQueuedBatchInput,
) {
  const config = yield* getInitialServerConfig();
  if (config.environment.capabilities.queuedSteerBatch === undefined) {
    return yield* new QueuedSteerBatchUnavailableError({
      message: "This server does not support batch steering.",
    });
  }
  return yield* request(ORCHESTRATION_V2_WS_METHODS.dispatchCommand, {
    ...input,
    type: "queued-message.promote-batch-to-steer",
  });
});

class QueuedRunStartUnavailableError extends Data.TaggedError("QueuedRunStartUnavailableError")<{
  readonly message: string;
}> {}

export const requireQueuedRunStart = Effect.fn("JonesQueue.requireQueuedRunStart")(function* () {
  const config = yield* getInitialServerConfig();
  if (config.environment.capabilities.queuedRunStart !== true) {
    return yield* new QueuedRunStartUnavailableError({
      message: "This server does not support starting a selected queued message.",
    });
  }
});
