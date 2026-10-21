import type { OrchestrationV2ProviderThread, OrchestrationV2Run } from "@t3tools/contracts";

/** A native goal can stay active after its last turn settles. Stop must reach that retained owner. */
export const hasRetainedGoalStopTarget = (
  run: OrchestrationV2Run | undefined,
  providerThreads: ReadonlyArray<OrchestrationV2ProviderThread>,
  activeProviderThreadId: OrchestrationV2ProviderThread["id"] | null,
): boolean =>
  run !== undefined &&
  // A retained binding is not evidence that a queued or pre-start run owns a
  // native turn. Let upstream cancel that start and reach later background work.
  run.status !== "queued" &&
  run.status !== "preparing" &&
  run.status !== "starting" &&
  run.providerThreadId === activeProviderThreadId &&
  providerThreads.some(
    (thread) =>
      thread.id === run.providerThreadId &&
      thread.driver === "codex" &&
      thread.providerSessionId !== null &&
      thread.nativeThreadRef !== null,
  );
