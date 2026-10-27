import type { ThreadResultDependency, ThreadResultRequest } from "@t3tools/contracts";

export const terminalRun = (status: string) =>
  ["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(status);

/** A delivered result remains work until its own processing run ends. Other runs cannot consume it. */
export function forThread(
  dependencies: ReadonlyArray<ThreadResultDependency & { readonly wakeMessageId: string | null }>,
  requests: ReadonlyArray<ThreadResultRequest>,
  wakeRuns: ReadonlyMap<string, { readonly status: string }>,
) {
  return {
    outgoing: dependencies.filter(
      (dependency) =>
        dependency.state === "sending" ||
        dependency.state === "awaiting" ||
        dependency.state === "resolved" ||
        (dependency.state === "delivered" &&
          (dependency.wakeMessageId === null ||
            !terminalRun(wakeRuns.get(dependency.wakeMessageId)?.status ?? "unknown"))),
    ).length,
    owed: requests.filter((request) => request.state === "pending" && !request.requesterReleased)
      .length,
  };
}
