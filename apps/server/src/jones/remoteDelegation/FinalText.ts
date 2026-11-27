import {
  OrchestratorMcpFailure,
  type OrchestratorMcpThreadReadInput,
  type OrchestratorMcpThreadReadResult,
  type OrchestratorMcpThreadRun,
  type OrchestratorMcpThreadTimelineItem,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import { withExecutedModelNote } from "@t3tools/provider-core/server/subagentProjection";
import * as Effect from "effect/Effect";

/**
 * The last reply `runId` gave on the task's thread there, which is its
 * result, read whole however long it is. A failed run prefers its error
 * from the activity view over an earlier reply, without taking another run's.
 * Led by a note when its provider reported running another model than it asked for.
 */
export const lastReply = Effect.fn("RemoteDelegation.lastReply")(function* (
  readThread: (
    input: Omit<OrchestratorMcpThreadReadInput, "environmentId">,
  ) => Effect.Effect<OrchestratorMcpThreadReadResult, OrchestratorMcpFailure>,
  threadId: ThreadId,
  runId: RunId,
  failed = false,
  probeSuccess = false,
) {
  const incomplete = new OrchestratorMcpFailure({
    code: "orchestration_error",
    message:
      "The linked environment returned incomplete task text; reconciliation remains pending.",
  });
  let reply: OrchestratorMcpThreadTimelineItem | undefined;
  let error: OrchestratorMcpThreadTimelineItem | undefined;
  // Position 0 contains the launch prompt; a cursorless linked read starts at the newest page.
  let afterPosition = 0;
  let run: OrchestratorMcpThreadRun | undefined;
  let remainingItems = 0;
  let tailProbed = false;
  while (true) {
    const read = yield* readThread({
      threadId,
      view: failed ? "activity" : "messages",
      limit: 100,
      runLimit: 50,
      maxCharsPerItem: failed && remainingItems > 100 ? 1 : 20_000,
      afterPosition,
    });
    if (read.thread.threadId !== threadId) return yield* incomplete;
    run ??= read.recentRuns.find((candidate) => candidate.runId === runId);
    reply =
      read.items.findLast((item) => item.type === "assistant_message" && item.runId === runId) ??
      reply;
    if (failed) {
      error = read.items.findLast((item) => item.type === "error" && item.runId === runId) ?? error;
    }
    if (!read.hasMore) break;
    if (read.nextPosition === null || read.nextPosition <= afterPosition) return yield* incomplete;
    if (!failed && probeSuccess && !tailProbed) {
      tailProbed = true;
      const tailPosition = Math.floor((read.thread.itemCount - 1) / 100) * 100 - 1;
      if (tailPosition > read.nextPosition) {
        const tail = yield* readThread({
          threadId,
          view: "messages",
          limit: 100,
          maxCharsPerItem: 20_000,
          afterPosition: tailPosition,
        });
        if (tail.thread.threadId !== threadId) return yield* incomplete;
        run ??= tail.recentRuns.find((candidate) => candidate.runId === runId);
        const tailReply = tail.items.findLast(
          (item) =>
            item.position > tailPosition &&
            item.type === "assistant_message" &&
            item.runId === runId,
        );
        if (!tail.hasMore && tailReply !== undefined) {
          reply = tailReply;
          break;
        }
      }
    }
    afterPosition = read.nextPosition;
    remainingItems = read.thread.itemCount - afterPosition - 1;
  }
  reply = error ?? reply;
  if (reply === undefined) return undefined;
  let text = reply.text ?? "";
  let part = reply;
  let previousOffset = 0;
  while (part.textTruncated) {
    const textOffset = part.nextTextOffset;
    // The MCP text offsets count UTF-16 code units, as String.length does.
    if (
      textOffset === undefined ||
      textOffset === null ||
      textOffset <= previousOffset ||
      textOffset !== text.length
    )
      return yield* incomplete;
    const rest = yield* readThread({
      threadId,
      itemId: reply.itemId,
      textOffset,
      maxCharsPerItem: 20_000,
    });
    const next = rest.items[0];
    if (
      rest.thread.threadId !== threadId ||
      rest.items.length !== 1 ||
      next === undefined ||
      next.itemId !== reply.itemId ||
      next.runId !== runId ||
      next.type !== reply.type ||
      next.sourceThreadId !== reply.sourceThreadId ||
      next.text === null ||
      next.text.length === 0
    )
      return yield* incomplete;
    text += next.text;
    previousOffset = textOffset;
    part = next;
  }
  return run === undefined ? text : withExecutedModelNote(text, run.model, run.executedModel);
});
