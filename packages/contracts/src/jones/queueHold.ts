import * as Schema from "effect/Schema";

/**
 * Why a queued run was held. `stop` and `inherited` holds wait for the user; a `restart` hold
 * ends when the restart continuation completes. Runs held before reasons were recorded carry
 * neither a reason nor a time.
 */
export const OrchestrationV2QueueHeldReason = Schema.Literals([
  "stop",
  "restart",
  "provider_failure",
  "inherited",
]);
export type OrchestrationV2QueueHeldReason = typeof OrchestrationV2QueueHeldReason.Type;
