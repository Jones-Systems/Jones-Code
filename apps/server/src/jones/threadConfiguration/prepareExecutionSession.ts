import {
  EventId,
  ProviderSessionId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Switch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";

/** Runs only at normal start; retirement completes before a replacement can open. */
export const prepareExecutionSession = Effect.fn("jones.prepareExecutionSession")(function* (
  run: OrchestrationV2Run,
  provider: OrchestrationV2ProviderThread,
  projection: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "runs" | "providerThreads" | "providerSessions"
  >,
) {
  const transitions = yield* Switch.ProviderSwitchServiceV2;
  const sessions = yield* Sessions.ProviderSessionManagerV2;
  const sink = yield* Sink.EventSinkV2;
  const plan = yield* transitions.plan({ projection, targetModelSelection: run.modelSelection });
  for (const providerSessionId of plan.releaseProviderSessionIds) {
    yield* sessions.detach({
      providerSessionId,
      threadId: run.threadId,
      detail: "Selection changed at normal run admission.",
    });
  }
  if (plan.transition.type !== "restart_and_resume") return provider;
  // A new session key prevents a pooled sibling's resident runtime from being reused.
  const replacement = {
    ...provider,
    providerSessionId: ProviderSessionId.make(`self-configure:${run.id}:${run.activeAttemptId}`),
  };
  if (provider.providerSessionId === replacement.providerSessionId) return provider;
  if (run.activeAttemptId === null) return yield* Effect.fail("Run has no active attempt.");
  const result = yield* sink.writeIfRunCurrent({
    threadId: run.threadId,
    runId: run.id,
    activeAttemptId: run.activeAttemptId,
    expectedStatus: "starting",
    events: [
      {
        id: EventId.make(`event:self-configure-session:${run.id}:${run.activeAttemptId}`),
        type: "provider-thread.updated",
        threadId: run.threadId,
        runId: run.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: yield* DateTime.now,
        payload: replacement,
      },
    ],
  });
  if (!result.committed) return yield* Effect.fail("Run changed before replacement.");
  return replacement;
});
