import type {
  OrchestrationV2AppThread,
  OrchestrationV2ProviderThread,
  ThreadPullRequestWatch,
} from "@t3tools/contracts";

export function watchAwaitsChecks(watch: ThreadPullRequestWatch): boolean {
  // An unanswered watch will get a baseline or a fresh read after a push.
  return watch.checksPending !== false;
}

export function threadHasWaitingGoal(records: {
  readonly thread: Pick<OrchestrationV2AppThread, "activeProviderThreadId">;
  readonly providerThreads: ReadonlyArray<Pick<OrchestrationV2ProviderThread, "id" | "goal">>;
}): boolean {
  const goal = records.providerThreads.find(
    (provider) => provider.id === records.thread.activeProviderThreadId,
  )?.goal;
  // The first read can finish before the goal releases its turn or after it has yielded.
  return goal?.status === "active" || goal?.status === "paused";
}

export function pullRequestWatchWillWake(
  thread: Pick<
    OrchestrationV2AppThread,
    "archivedAt" | "deletedAt" | "settledAt" | "settledOverride" | "pullRequests"
  >,
): boolean {
  return (
    thread.archivedAt === null &&
    thread.deletedAt === null &&
    thread.settledAt === null &&
    thread.settledOverride !== "settled" &&
    (thread.pullRequests ?? []).some(
      (link) =>
        link.source !== "stack-dismissed" &&
        link.snapshot?.state !== "merged" &&
        link.watch !== undefined &&
        watchAwaitsChecks(link.watch),
    )
  );
}
