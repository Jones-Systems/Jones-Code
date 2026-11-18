import * as Clock from "effect/Clock";
import {
  awaitRetirementForAdmission,
  RETIREMENT_ADMISSION_BUDGET_MS,
} from "../providerRetirement/admissionWait.ts";
import {
  EventId,
  ProviderSessionId,
  RunId,
  ProviderRuntimeBinding,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Switch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import { runtimeBinding } from "@t3tools/provider-core/server/ProviderAdapter";

export class ExecutionSessionRevalidationError extends Schema.TaggedError<ExecutionSessionRevalidationError>()(
  "ExecutionSessionRevalidationError",
  { runId: RunId, cause: Schema.Defect() },
) {
  override get message(): string {
    return "The run attempt or provider selection changed during retirement.";
  }
}

/** Mirrors the orchestrator's probe: a failed probe cannot confirm that the work ended. */
const sessionHasBackgroundWork = Effect.fn("jones.prepareExecutionSession.backgroundWork")(
  function* (
    sessions: Sessions.ProviderSessionManagerV2["Service"],
    detaching: OrchestrationV2ThreadProjection["providerSessions"][number],
    providerThreads: OrchestrationV2ThreadProjection["providerThreads"],
  ) {
    const session = yield* sessions
      .get(detaching.id)
      .pipe(Effect.orElseSucceed(() => Option.none()));
    if (Option.isNone(session)) return false;
    const runtime = session.value;
    const probes =
      !detaching.capabilities.sessions.supportsMultipleProviderThreadsPerSession &&
      runtime.hasPendingBackgroundWork !== undefined
        ? [runtime.hasPendingBackgroundWork]
        : providerThreads.flatMap((providerThread) =>
            providerThread.providerSessionId === detaching.id &&
            runtime.hasPendingBackgroundWorkForThread !== undefined
              ? [runtime.hasPendingBackgroundWorkForThread(providerThread)]
              : [],
          );
    for (const probe of probes) {
      if (yield* probe.pipe(Effect.catchCause(() => Effect.succeed(true)))) return true;
    }
    return false;
  },
);

export class ExecutionSessionRevalidationUnavailableError extends Schema.TaggedError<ExecutionSessionRevalidationUnavailableError>()(
  "ExecutionSessionRevalidationUnavailableError",
  { runId: RunId, cause: Schema.Defect() },
) {
  override get message(): string {
    return "Provider start could not read the current run state after retirement. No provider work was started.";
  }
}
const isExecutionSessionRevalidationUnavailable = Schema.is(
  ExecutionSessionRevalidationUnavailableError,
);

const awaitWriterRetirement = Effect.fn("jones.prepareExecutionSession.awaitWriterRetirement")(
  function* (
    sessions: Sessions.ProviderSessionManagerV2["Service"],
    runId: RunId,
    writer: OrchestrationV2ProviderThread,
  ) {
    const generation = writer.runtimeIdentity?.runtimeGeneration;
    const expected = generation === undefined ? undefined : runtimeBinding(writer, generation);
    if (
      expected === undefined ||
      writer.nativeThreadRef?.driver !== writer.driver ||
      sessions.captureThreadRetirement === undefined
    )
      return yield* new ExecutionSessionRevalidationError({
        runId,
        cause: "The original writer has no exact captured retirement binding.",
      });
    const proof = yield* sessions.captureThreadRetirement(expected);
    if (proof === null || !Schema.toEquivalence(ProviderRuntimeBinding)(proof.binding, expected))
      return yield* new ExecutionSessionRevalidationError({
        runId,
        cause: "Detachment did not retain the original writer retirement.",
      });
    const receipt = yield* proof.awaitRetired;
    if (!Schema.toEquivalence(ProviderRuntimeBinding)(receipt.binding, expected))
      return yield* new ExecutionSessionRevalidationError({
        runId,
        cause: "Retirement receipt belongs to another provider writer.",
      });
  },
);

/** Runs only at normal start; retirement and its durable binding removal precede replacement. */
export const prepareExecutionSession = Effect.fn("jones.prepareExecutionSession")(function* (
  run: OrchestrationV2Run,
  provider: OrchestrationV2ProviderThread,
  projection: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "runs" | "providerThreads" | "providerSessions"
  >,
  revalidateReplacement: Effect.Effect<
    void,
    ExecutionSessionRevalidationError | ExecutionSessionRevalidationUnavailableError
  > = Effect.void,
) {
  const transitions = yield* Switch.ProviderSwitchServiceV2;
  const sessions = yield* Sessions.ProviderSessionManagerV2;
  const sink = yield* Sink.EventSinkV2;
  const plan = yield* transitions.plan({ projection, targetModelSelection: run.modelSelection });
  if (run.activeAttemptId === null)
    return yield* new ExecutionSessionRevalidationError({
      runId: run.id,
      cause: "Run has no active attempt.",
    });
  const deadline = (yield* Clock.currentTimeMillis) + RETIREMENT_ADMISSION_BUDGET_MS;
  const events: Array<OrchestrationV2DomainEvent> = [];
  for (const providerSessionId of plan.releaseProviderSessionIds) {
    const retired = projection.providerSessions.find((session) => session.id === providerSessionId);
    if (retired === undefined)
      return yield* new ExecutionSessionRevalidationError({
        runId: run.id,
        cause: "Released session has no captured identity.",
      });
    // Detaching ends background work in the provider process. A handoff runs in its own session,
    // so a busy one stays up; the queue holds a busy restart before it reaches admission.
    if (
      plan.transition.type !== "restart_and_resume" &&
      (yield* sessionHasBackgroundWork(sessions, retired, projection.providerThreads))
    )
      continue;
    yield* awaitRetirementForAdmission(
      sessions.detach({
        providerSessionId,
        threadId: run.threadId,
        detail: "Selection changed at normal run admission.",
        requireWriterRetirement: true,
      }),
      deadline,
      revalidateReplacement,
    );
    const writers = projection.providerThreads.filter(
      (candidate) =>
        candidate.providerSessionId === providerSessionId && candidate.appThreadId === run.threadId,
    );
    if (writers.length === 0 || sessions.captureThreadRetirement === undefined)
      return yield* new ExecutionSessionRevalidationError({
        runId: run.id,
        cause: "The original provider writer retirement proof is unavailable.",
      });
    for (const writer of writers) yield* awaitWriterRetirement(sessions, run.id, writer);
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
  yield* awaitRetirementForAdmission(
    sessions.awaitThreadRetirement?.({
      threadId: run.threadId,
      instanceId: run.modelSelection.instanceId,
    }) ?? Effect.void,
    deadline,
    revalidateReplacement,
  );
  const retiredWriter =
    provider.providerSessionId !== null &&
    (yield* (
      sessions.hasRetiredWriter?.({
        providerSessionId: provider.providerSessionId,
        threadId: run.threadId,
      }) ?? Effect.succeed(false)
    ));
  const generation = provider.runtimeIdentity?.runtimeGeneration;
  const hasNativeIdentity =
    provider.appThreadId === run.threadId &&
    provider.providerSessionId != null &&
    provider.nativeThreadRef?.nativeId != null &&
    provider.nativeThreadRef.driver === provider.driver;
  const stoppedBinding = !hasNativeIdentity
    ? undefined
    : generation === undefined
      ? ((yield* sink.readStoppedRuntime?.(provider) ?? Effect.succeed(null)) ?? undefined)
      : runtimeBinding(provider, generation);
  const identityBinding =
    stoppedBinding === undefined
      ? undefined
      : runtimeBinding(provider, stoppedBinding.runtimeGeneration);
  const stoppedRuntime =
    stoppedBinding !== undefined &&
    identityBinding !== undefined &&
    Schema.toEquivalence(ProviderRuntimeBinding)(stoppedBinding, identityBinding) &&
    (yield* sink.hasStoppedRuntime?.(stoppedBinding) ?? Effect.succeed(false));
  if (retiredWriter) yield* awaitWriterRetirement(sessions, run.id, provider);
  if (
    plan.transition.type === "restart_and_resume" &&
    events.length === 0 &&
    !stoppedRuntime &&
    !retiredWriter
  )
    return yield* new ExecutionSessionRevalidationError({
      runId: run.id,
      cause: "Restart has no completed original writer retirement.",
    });
  const revalidation = yield* Effect.result(revalidateReplacement);
  if (revalidation._tag === "Failure") {
    // Only a completed detach may be recorded here. The manager's retired-writer fence
    // also covers pending retirement, so it cannot substitute for that completion.
    if (isExecutionSessionRevalidationUnavailable(revalidation.failure) && events.length > 0) {
      const result = yield* sink.writeIfRunCurrent({
        threadId: run.threadId,
        runId: run.id,
        activeAttemptId: run.activeAttemptId,
        expectedStatus: "starting",
        events,
      });
      if (!result.committed)
        return yield* new ExecutionSessionRevalidationError({
          runId: run.id,
          cause: "Run changed before completed retirement could be recorded.",
        });
    }
    return yield* revalidation.failure;
  }
  // A new session key prevents a pooled sibling's resident runtime from being reused.
  const replacement =
    retiredWriter ||
    stoppedRuntime ||
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
    ...((stoppedRuntime && generation === undefined) ||
    (provider.providerSessionId !== replacement.providerSessionId && provider.id !== undefined)
      ? {
          runtimeIdentityBoundary: {
            expectedGeneration: generation ?? null,
            expectedActiveProviderThreadId: projection.thread.activeProviderThreadId ?? provider.id,
            expectedProviderThreadIdentity: {
              id: provider.id,
              appThreadId: provider.appThreadId,
              driver: provider.driver,
              providerInstanceId: provider.providerInstanceId,
              providerSessionId: provider.providerSessionId,
              nativeThreadRef: provider.nativeThreadRef,
            },
          },
        }
      : {}),
    events,
  });
  if (!result.committed)
    return yield* new ExecutionSessionRevalidationError({
      runId: run.id,
      cause: "Run changed before replacement.",
    });
  return replacement;
});
