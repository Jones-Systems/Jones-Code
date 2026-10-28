import {
  EventId,
  ProviderSessionId,
  RunId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as Switch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";

export class ExecutionSessionRevalidationError extends Schema.TaggedError<ExecutionSessionRevalidationError>()(
  "ExecutionSessionRevalidationError",
  { runId: RunId, cause: Schema.Defect() },
) {
  override get message(): string {
    return "The run attempt or provider selection changed during retirement.";
  }
}

/** Runs only at normal start; retirement and its durable binding removal precede replacement. */
export const prepareExecutionSession = Effect.fn("jones.prepareExecutionSession")(function* (
  run: OrchestrationV2Run,
  provider: OrchestrationV2ProviderThread,
  projection: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "runs" | "providerThreads" | "providerSessions"
  >,
  revalidateReplacement: Effect.Effect<void, ExecutionSessionRevalidationError> = Effect.void,
) {
  const transitions = yield* Switch.ProviderSwitchServiceV2;
  const sessions = yield* Sessions.ProviderSessionManagerV2;
  const sink = yield* Sink.EventSinkV2;
  const plan = yield* transitions.plan({ projection, targetModelSelection: run.modelSelection });
  if (run.activeAttemptId === null) return yield* Effect.fail("Run has no active attempt.");
  const events: Array<OrchestrationV2DomainEvent> = [];
  for (const providerSessionId of plan.releaseProviderSessionIds) {
    const retired = projection.providerSessions.find((session) => session.id === providerSessionId);
    if (retired === undefined)
      return yield* Effect.fail("Released session has no captured identity.");
    yield* sessions.detach({
      providerSessionId,
      threadId: run.threadId,
      detail: "Selection changed at normal run admission.",
      requireWriterRetirement: true,
    });
    const occurredAt = yield* DateTime.now;
    events.push({
      id: EventId.make(
        `event:self-configure-detach:${run.id}:${run.activeAttemptId}:${providerSessionId}`,
      ),
      type: "provider-session.detached",
      threadId: run.threadId,
      runId: run.id,
      driver: retired.driver,
      providerInstanceId: retired.providerInstanceId,
      occurredAt,
      payload: {
        providerSessionId,
        detachedAt: occurredAt,
        reason: "Selection changed at normal run admission.",
      },
    });
  }
  yield* revalidateReplacement;
  // A new session key prevents a pooled sibling's resident runtime from being reused.
  const replacement =
    plan.transition.type === "restart_and_resume" ||
    (run.restartContinuationOfRunId !== undefined &&
      (plan.transition.type === "switch_model_in_session" ||
        plan.transition.type === "create_with_handoff"))
      ? {
          ...provider,
          providerSessionId: ProviderSessionId.make(
            `self-configure:${run.id}:${run.activeAttemptId}`,
          ),
        }
      : provider;
  if (provider.providerSessionId !== replacement.providerSessionId) {
    events.push({
      id: EventId.make(`event:self-configure-session:${run.id}:${run.activeAttemptId}`),
      type: "provider-thread.updated",
      threadId: run.threadId,
      runId: run.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: yield* DateTime.now,
      payload: replacement,
    });
  }
  if (events.length === 0) return provider;
  const result = yield* sink.writeIfRunCurrent({
    threadId: run.threadId,
    runId: run.id,
    activeAttemptId: run.activeAttemptId,
    expectedStatus: "starting",
    events,
  });
  if (!result.committed) return yield* Effect.fail("Run changed before replacement.");
  return replacement;
});
