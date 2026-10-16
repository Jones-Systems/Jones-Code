import {
  CommandId,
  type OrchestrationV2Run,
  type RunId,
  type ServerSettings,
  type ThreadId,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import type * as DateTime from "effect/DateTime";
import type { PendingOrchestrationEffectV2 } from "./EffectOutbox.ts";
import { restartContinuationRun } from "./RestartContinuationPolicy.ts";

// A null preference snapshot preserves the explicit continue-all preparation path.
export function serverUpdateContinuationRun(
  projection: Parameters<typeof restartContinuationRun>[0],
  preferences: ServerSettings | null,
  now: DateTime.Utc,
): OrchestrationV2Run | undefined {
  if (
    preferences !== null &&
    !resolveProjectSettings(preferences, projection.thread.projectId).settings
      .continueThreadsAfterServerUpdate
  )
    return;
  const run = restartContinuationRun(projection, now);
  if (run === undefined || run.status !== "running") return;
  return run;
}

export function serverUpdateContinuationEffect(input: {
  readonly preparationId: string;
  readonly threadId: ThreadId;
  readonly sourceRunId: RunId;
  readonly optedInOnly: boolean;
}): PendingOrchestrationEffectV2 {
  return {
    id: `effect:server-update-continuation:${input.preparationId}`,
    commandId: CommandId.make(`command:server-update-prepare:${input.preparationId}`),
    threadId: input.threadId,
    request: {
      type: "provider-runtime.continue",
      sourceRunId: input.sourceRunId,
      preparedForRestart: true,
      continueWithoutPreference: !input.optedInOnly,
    },
  };
}
