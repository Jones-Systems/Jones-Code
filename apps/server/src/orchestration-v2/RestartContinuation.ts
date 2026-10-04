import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  MessageId,
  type OrchestrationV2Run,
  type ProviderThreadId,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { ProjectionRuntimeRecoveryState } from "./ProjectionStore.ts";
import type { RestartContinuationDispatchContextV2 } from "./Orchestrator.ts";

import * as ServerSettings from "../serverSettings.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as EventSink from "./EventSink.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import type { RestartContinuationMarkerV2 } from "./EventSink.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";
import {
  isRestartNoteSource,
  restartCancelledBackgroundWorkNote,
} from "./RestartBackgroundNote.ts";

export function capturedRestartContinuationIds(input: {
  readonly effectId: string;
  readonly marker: RestartContinuationMarkerV2;
}): { readonly commandId: CommandId; readonly messageId: MessageId } {
  const digest = nativeCreationSha256(
    nativeCreationCanonicalJson({ version: 1, effectId: input.effectId, marker: input.marker }),
  );
  return {
    commandId: CommandId.make(`command:restart-continuation:captured:${digest}`),
    messageId: MessageId.make(`message:restart-continuation:${input.marker.sourceRunId}`),
  };
}

/**
 * The run a restart continuation resumes, if any: an unfinished root run, or a
 * settled one whose own provider thread lost background work in the restart
 * (`cancelledWorkProviderThreadIds`, which recovery records on that thread).
 */
export function restartContinuationRun(
  projection: Pick<
    ProjectionRuntimeRecoveryState,
    "thread" | "runs" | "providerThreads" | "providerSessions" | "providerTurns"
  >,
  cancelledWorkProviderThreadIds: ReadonlySet<ProviderThreadId> = new Set(),
): OrchestrationV2Run | undefined {
  if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null) return;
  const run = projection.runs.reduce<OrchestrationV2Run | undefined>(
    (latest, candidate) => (!latest || candidate.ordinal > latest.ordinal ? candidate : latest),
    undefined,
  );
  if (!run) return;
  const preparedContinuation =
    run.status === "starting" && run.restartContinuationOfRunId !== undefined;
  // Background work outlived this settled turn; the provider has no live turn.
  const settledWithCancelledWork =
    (run.status === "completed" || run.status === "waiting") &&
    run.providerThreadId !== null &&
    cancelledWorkProviderThreadIds.has(run.providerThreadId);
  if (run.status !== "running" && !preparedContinuation && !settledWithCancelledWork) return;
  const liveTurnRequired = !preparedContinuation && !settledWithCancelledWork;
  if (projection.thread.providerInstanceId !== run.providerInstanceId) return;
  const providerThread = projection.providerThreads.find(
    (thread) => thread.id === run.providerThreadId,
  );
  if (
    !providerThread ||
    providerThread.appThreadId !== projection.thread.id ||
    providerThread.ownerNodeId !== null ||
    providerThread.providerInstanceId !== run.providerInstanceId ||
    providerThread.nativeThreadRef?.nativeId == null ||
    providerThread.nativeThreadRef.strength !== "strong" ||
    providerThread.nativeThreadRef.driver !== providerThread.driver ||
    (liveTurnRequired && providerThread.status !== "active") ||
    providerThread.status === "closed" ||
    providerThread.status === "archived"
  )
    return;
  const session = projection.providerSessions.find(
    (candidate) => candidate.id === providerThread.providerSessionId,
  );
  // A settled thread's session may already be stopped and out of the recovery
  // read; the continuation reopens it from the provider thread's native ref.
  // Most adapters keep a live session "ready" through its turns, so only a
  // stopped or failed session rules out a live turn.
  if (
    session === undefined
      ? !settledWithCancelledWork
      : session.providerInstanceId !== run.providerInstanceId ||
        session.driver !== providerThread.driver ||
        (liveTurnRequired && (session.status === "stopped" || session.status === "error"))
  )
    return;
  if (
    liveTurnRequired &&
    !projection.providerTurns.some(
      (turn) =>
        turn.providerThreadId === providerThread.id &&
        turn.runAttemptId === run.activeAttemptId &&
        turn.status === "running",
    )
  )
    return;
  return run;
}

export interface RestartContinuationInput {
  readonly threadId: ThreadId;
  readonly sourceRunId: RunId;
  readonly capturedContinuation?: RestartContinuationDispatchContextV2;
}

export const continueRestartedRun = Effect.fn("RestartContinuation.continueRestartedRun")(
  function* (input: RestartContinuationInput) {
    const captured = input.capturedContinuation;
    const enabled =
      captured === undefined
        ? yield* ServerSettings.ServerSettingsService.pipe(
            Effect.flatMap((settings) => settings.getSettings),
            Effect.orElseSucceed(() => null),
          )
        : null;
    if (captured === undefined && enabled === null) return;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const messageId = MessageId.make(`message:restart-continuation:${input.sourceRunId}`);
    const projection = yield* threads.getThreadRecords(
      input.threadId,
      ["messages", "runs", "providerTurns", "providerThreads", "attempts"],
      { messageIds: [messageId] },
    );
    if (
      captured === undefined &&
      enabled !== null &&
      !resolveProjectSettings(enabled, projection.thread.projectId).settings
        .continueThreadsAfterServerUpdate
    )
      return;
    if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null) return;

    if (projection.messages.some((message) => message.id === messageId)) return;
    const source = projection.runs.find((run) => run.id === input.sourceRunId);
    // Unmarked settled sources require saved work labels. A captured source
    // can use the generic prompt after its full released marker is rechecked.
    const noteSource =
      source !== undefined && isRestartNoteSource(source, projection.providerTurns);
    const capturedSettledSource =
      captured !== undefined && (source?.status === "completed" || source?.status === "waiting");
    if (!source || (source.status !== "cancelled" && !noteSource && !capturedSettledSource)) return;
    // A user submission after reconciliation takes precedence over an automatic prompt.
    if (projection.runs.some((run) => run.ordinal > source.ordinal)) return;
    if (projection.thread.providerInstanceId !== source.providerInstanceId) return;
    if (captured !== undefined) {
      const sink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const marker = yield* sink.readReleasedRestartContinuation({
        effectId: captured.effectId,
        threadId: input.threadId,
        sourceRunId: input.sourceRunId,
      });
      const expected = captured.marker;
      if (
        marker === null ||
        nativeCreationCanonicalJson(marker) !== nativeCreationCanonicalJson(expected)
      )
        return;
      const registered = yield* sink.readProviderRuntimeEvidence(input.threadId);
      const providerThread = projection.providerThreads.find(
        (candidate) => candidate.id === marker.binding.providerThreadId,
      );
      const attempt = projection.attempts.find(
        (candidate) => candidate.id === marker.sourceRunAttemptId,
      );
      if (
        marker.threadId !== input.threadId ||
        marker.projectId !== projection.thread.projectId ||
        marker.sourceRunId !== source.id ||
        source.activeAttemptId !== marker.sourceRunAttemptId ||
        source.providerThreadId !== marker.binding.providerThreadId ||
        source.providerInstanceId !== marker.binding.instanceId ||
        attempt?.runId !== source.id ||
        attempt.providerThreadId !== marker.binding.providerThreadId ||
        projection.thread.activeProviderThreadId !== marker.binding.providerThreadId ||
        providerThread?.appThreadId !== input.threadId ||
        providerThread.providerSessionId !== marker.binding.providerSessionId ||
        providerThread.providerInstanceId !== marker.binding.instanceId ||
        providerThread.driver !== marker.binding.driver ||
        providerThread.nativeThreadRef?.nativeId !== marker.binding.nativeThreadId ||
        registered === null ||
        registered.evidenceRevision !== marker.evidenceRevision ||
        (
          [
            "threadId",
            "providerThreadId",
            "providerSessionId",
            "instanceId",
            "driver",
            "nativeThreadId",
            "runtimeGeneration",
          ] as const
        ).some((key) => registered.binding[key] !== marker.binding[key]) ||
        (yield* outbox.listHeldByThreadId(input.threadId)).length > 0
      )
        return;
    }
    // A captured command is bound to the full released marker and real effect.
    // Its source-stable message ID also catches an earlier ordinary delivery.
    const command = {
      type: "message.dispatch",
      commandId:
        captured === undefined
          ? CommandId.make(`command:restart-continuation:${input.sourceRunId}`)
          : capturedRestartContinuationIds(captured).commandId,
      threadId: input.threadId,
      messageId,
      text: noteSource
        ? restartCancelledBackgroundWorkNote(source.restartCancelledBackgroundWork ?? [])
        : "Continue where you left off.",
      attachments: [],
      modelSelection: source.modelSelection,
      dispatchMode: { type: "start_immediately" },
      createdBy: "agent",
      creationSource: "server",
      restartContinuationOfRunId: input.sourceRunId,
    } satisfies Parameters<
      ThreadManagementService.ThreadManagementService["Service"]["dispatchRestartContinuation"]
    >[0];
    if (captured === undefined) yield* threads.dispatch(command);
    else yield* threads.dispatchRestartContinuation(command, captured);
  },
);
