import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { threadPullRequestsOf } from "../../sourceControl/threadPullRequests.ts";

/** Durable Jones intent, with upstream settleAfterRun's later-message and watch guards. */
export const terminalDisposition = (input: {
  readonly projection: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "runs" | "messages" | "runtimeRequests"
  >;
  readonly acceptedAt: DateTime.Utc;
  readonly duties: { readonly outgoing: number; readonly owed: number };
}): "ignore" | "cancel" | "settle" => {
  const { projection, duties } = input;
  const intent = projection.thread.selfSettlement;
  if (intent == null) return "ignore";
  if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null)
    return "cancel";
  const run = projection.runs.find((candidate) => candidate.id === intent.runId);
  if (run === undefined) return "cancel";
  // Waiting includes checkpoint finalization; provider terminal alone is insufficient.
  if (["preparing", "starting", "running", "waiting", "queued"].includes(run.status))
    return "ignore";
  if (run.status !== "completed") return "cancel";
  if (
    threadPullRequestsOf(projection.thread).some(
      (link) =>
        link.watch !== undefined &&
        !DateTime.isLessThan(DateTime.makeUnsafe(link.watch.startedAt), input.acceptedAt),
    )
  )
    return "cancel";
  const cancelledSteered = new Set(
    projection.messages.filter((message) => message.runId === run.id).map((message) => message.id),
  );
  const following = projection.runs.filter(
    (candidate) =>
      candidate.ordinal > run.ordinal &&
      !(
        candidate.status === "cancelled" &&
        (candidate.startedAt === null || cancelledSteered.has(candidate.userMessageId))
      ),
  );
  const automatic = new Set(
    projection.messages
      .filter(
        (message) =>
          message.notification !== undefined || message.delegatedCompletion !== undefined,
      )
      .map((message) => message.id),
  );
  if (following.some((candidate) => !automatic.has(candidate.userMessageId))) return "cancel";
  if (
    following.some((candidate) =>
      ["preparing", "starting", "running", "waiting"].includes(candidate.status),
    )
  )
    return "ignore";
  if (
    following.some((candidate) => candidate.status !== "queued" && candidate.status !== "completed")
  )
    return "cancel";
  if (
    duties.outgoing > 0 ||
    duties.owed > 0 ||
    projection.runtimeRequests.some(
      (request) =>
        request.status === "pending" &&
        (request.kind !== "user_input" || request.responseCapability.type !== "message"),
    )
  )
    return "cancel";
  return "settle";
};
