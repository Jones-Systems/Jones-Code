import type { ModelSelection, OrchestrationV2ThreadProjection } from "@t3tools/contracts";
type Projection = Pick<OrchestrationV2ThreadProjection, "thread" | "providerThreads"> &
  Partial<Pick<OrchestrationV2ThreadProjection, "runs">>;
/** Captured root execution owns the conversation; saved defaults describe future admission. */
export function executionProjection<P extends Projection>(projection: P): P {
  const captured = projection.runs
    ?.filter(
      (run) => run.startedAt !== null && run.status !== "queued" && run.status !== "rolled_back",
    )
    .toSorted((a, b) => b.ordinal - a.ordinal)[0];
  const provider =
    projection.thread.activeProviderThreadId === null
      ? undefined
      : projection.providerThreads.find(
          (row) =>
            row.id === (captured?.providerThreadId ?? projection.thread.activeProviderThreadId),
        );
  const modelSelection: ModelSelection = captured?.modelSelection ?? {
    ...projection.thread.modelSelection,
    instanceId: provider?.providerInstanceId ?? projection.thread.modelSelection.instanceId,
  };
  return {
    ...projection,
    thread: {
      ...projection.thread,
      modelSelection,
      providerInstanceId: modelSelection.instanceId,
      activeProviderThreadId: provider?.id ?? projection.thread.activeProviderThreadId,
    },
  };
}
