import type { OrchestrationV2ProviderThread, OrchestrationV2Run } from "@t3tools/contracts";

/** A native goal can stay active after its last turn settles. Stop must reach that retained owner. */
export const hasRetainedGoalStopTarget = (
  run: OrchestrationV2Run | undefined,
  providerThreads: ReadonlyArray<OrchestrationV2ProviderThread>,
  activeProviderThreadId: OrchestrationV2ProviderThread["id"] | null,
): boolean =>
  run !== undefined &&
  run.providerThreadId === activeProviderThreadId &&
  providerThreads.some(
    (thread) =>
      thread.id === run.providerThreadId &&
      thread.driver === "codex" &&
      thread.providerSessionId !== null &&
      thread.nativeThreadRef !== null,
  );
