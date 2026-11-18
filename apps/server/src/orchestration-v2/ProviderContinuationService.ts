import * as StartRefusals from "../jones/startRefusal/Store.ts";
import {
  CommandId,
  type MessageId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2Run,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import type { CommandReceiptV2 } from "./CommandReceiptStore.ts";
import { EventSinkStreamError } from "./EventSink.ts";
import { LiveStreamBufferError } from "./LiveStreamBudget.ts";
import type * as Orchestrator from "./Orchestrator.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import { isUndeliveredMailboxSteer } from "./NotificationMailbox.ts";

const CONTINUATION_MESSAGE_TEXT = "Background task completed.";

export function isAcceptedQueuedCancellation(
  receipt: Option.Option<CommandReceiptV2>,
  threadId: ThreadId,
  commandId: CommandId,
  eventSequence: number,
): boolean {
  return (
    Option.isSome(receipt) &&
    receipt.value.commandId === commandId &&
    (receipt.value.commandType === "queued-run.cancel" ||
      receipt.value.commandType === "thread.stop" ||
      receipt.value.commandType === "run.interrupt") &&
    receipt.value.status === "accepted" &&
    receipt.value.threadId === threadId &&
    receipt.value.resultSequence >= eventSequence
  );
}

const isSinkStreamError = Schema.is(EventSinkStreamError);
const isBufferOverflow = Schema.is(LiveStreamBufferError);

export const readContinuationTerminal = Effect.fn(
  "ProviderContinuationService.readContinuationTerminal",
)(function* (
  streamEvents: ThreadManagementService.ThreadManagementService["Service"]["streamStoredEventsFrom"],
  created: OrchestrationV2Run,
  creationSequence: number,
) {
  let afterSequence = creationSequence;
  let continuouslyQueued = created.status === "queued" && created.startedAt === null;
  const ended = yield* Effect.suspend(() =>
    streamEvents({ threadId: created.threadId, afterSequence, eventType: "run.updated" }).pipe(
      Stream.dropWhile(({ event, sequence }) => {
        afterSequence = sequence;
        if (
          event.type !== "run.updated" ||
          event.threadId !== created.threadId ||
          event.payload.id !== created.id
        )
          return true;
        if (ThreadManagementService.isTerminalRunStatus(event.payload.status)) return false;
        continuouslyQueued &&=
          event.payload.status === "queued" &&
          event.payload.startedAt === null &&
          event.payload.activeAttemptId === created.activeAttemptId &&
          event.payload.userMessageId === created.userMessageId;
        return true;
      }),
      Stream.runHead,
    ),
  ).pipe(
    Effect.retry({
      schedule: Schedule.spaced("100 millis"),
      while: (error) =>
        error._tag === "OrchestratorDomainEventStreamError" &&
        isSinkStreamError(error.cause) &&
        isBufferOverflow(error.cause.cause),
    }),
  );
  return { ended, continuouslyQueued };
});

function delegatedCompletionText(taskIds: ReadonlyArray<string>): string {
  const taskList = taskIds.join(", ");
  return taskIds.length === 1
    ? `Delegated task ${taskList} reached a terminal state. Use task_status with taskId ${taskList} to read the result.`
    : `Delegated tasks ${taskList} reached terminal states. Use task_status with each taskId to read the results.`;
}

function currentDelegatedCompletionDelivery(
  projection: Pick<OrchestrationV2ThreadProjection, "messages" | "runs" | "providerTurns">,
  completion: NonNullable<
    ProviderContinuationRequests.ProviderContinuationRequest["delegatedCompletion"]
  >,
) {
  const sourceRun = projection.runs.find((candidate) => candidate.id === completion.parentRunId);
  const delivery = sourceRun?.delegatedCompletion?.delivery;
  const alreadyDispatched = projection.messages.some(
    (message) => message.id === completion.messageId,
  );
  if (
    sourceRun?.delegatedCompletion?.disposition !== "open" ||
    delivery === null ||
    delivery === undefined ||
    delivery.generation !== completion.generation ||
    delivery.messageId !== completion.messageId ||
    (alreadyDispatched && !isUndeliveredMailboxSteer(projection, completion.messageId))
  ) {
    return undefined;
  }
  return delivery;
}

function delegatedCompletionRetryKey(
  request: ProviderContinuationRequests.ProviderContinuationRequest,
  completion: NonNullable<
    ProviderContinuationRequests.ProviderContinuationRequest["delegatedCompletion"]
  >,
): string {
  return `${request.threadId}:${completion.parentRunId}:${completion.generation}:${completion.messageId}`;
}

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const ids = yield* IdAllocator.IdAllocatorV2;
    const refusalProof = yield* Effect.serviceOption(StartRefusals.StartRefusals);
    const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const retryAttempts = yield* Ref.make(new Map<string, number>());
    const continuations = yield* KeyedLock.makeRunner<ThreadId>();

    const clearRetryAttempt = (key: string) =>
      Ref.update(retryAttempts, (current) => {
        if (!current.has(key)) return current;
        const updated = new Map(current);
        updated.delete(key);
        return updated;
      });

    const nextRetryDelay = (key: string) =>
      Ref.modify(retryAttempts, (current) => {
        const attempt = current.get(key) ?? 0;
        const updated = new Map(current);
        updated.set(key, attempt + 1);
        return [Math.min(100 * 2 ** Math.min(attempt, 6), 5_000), updated] as const;
      });

    // Cancelling or stopping a queued continuation ends it unstarted; only the request can release the adapter's wake latch.
    const releaseIfNeverStarted = Effect.fn("ProviderContinuationService.releaseIfNeverStarted")(
      function* (
        request: ProviderContinuationRequests.ProviderContinuationRequest,
        messageId: MessageId,
        dispatched: Orchestrator.OrchestratorV2DispatchResult,
      ) {
        const onRunEndedWithoutStarting = request.onRunEndedWithoutStarting;
        if (onRunEndedWithoutStarting === undefined) return;
        const [creation] = dispatched.storedEvents.flatMap(({ event, commandId, sequence }) =>
          event.type === "run.created" &&
          event.threadId === request.threadId &&
          commandId === `provider-continuation:${messageId}` &&
          event.payload.userMessageId === messageId
            ? [{ run: event.payload, sequence }]
            : [],
        );
        if (creation === undefined) return;
        const created = creation.run;
        yield* readContinuationTerminal(
          threads.streamStoredEventsFrom,
          created,
          creation.sequence,
        ).pipe(
          Effect.flatMap(({ ended, continuouslyQueued }) =>
            Effect.gen(function* () {
              if (
                Option.isNone(ended) ||
                ended.value.event.type !== "run.updated" ||
                ended.value.event.payload.activeAttemptId === null
              )
                return;
              const terminal = ended.value.event.payload;
              const { commandId, sequence } = ended.value;
              if (terminal.status === "cancelled") {
                // Stop can also cancel buffered notifications. Prove the original attempt stayed queued through the receipt.
                if (
                  !continuouslyQueued ||
                  terminal.startedAt !== null ||
                  terminal.userMessageId !== messageId ||
                  terminal.activeAttemptId !== created.activeAttemptId ||
                  commandId === null
                )
                  return;
                const receipt = yield* threads.getCommandReceipt(commandId);
                if (isAcceptedQueuedCancellation(receipt, request.threadId, commandId, sequence))
                  yield* onRunEndedWithoutStarting();
                return;
              }
              if (Option.isNone(refusalProof)) return;
              const proof = yield* refusalProof.value.readPrefix({
                threadId: request.threadId,
                runId: terminal.id,
                attemptId: terminal.activeAttemptId!,
              });
              if (proof.state === "refused") yield* onRunEndedWithoutStarting();
            }),
          ),
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestration-v2.provider-continuation.run-watch-failed", {
              threadId: request.threadId,
              providerThreadId: request.providerThreadId,
              cause,
            }),
          ),
          Effect.forkScoped,
        );
      },
    );

    const dispatchContinuation = Effect.fn("ProviderContinuationService.dispatchContinuation")(
      function* (request: ProviderContinuationRequests.ProviderContinuationRequest) {
        const projection = yield* threads.getThreadRecords(
          request.threadId,
          ["messages", "runs", "providerTurns"],
          {
            messageIds:
              request.delegatedCompletion === undefined
                ? []
                : [request.delegatedCompletion.messageId],
          },
        );
        if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null) {
          yield* Effect.logInfo("orchestration-v2.provider-continuation.thread-archived", {
            threadId: request.threadId,
            providerThreadId: request.providerThreadId,
          });
          if (request.delegatedCompletion !== undefined) {
            yield* clearRetryAttempt(
              delegatedCompletionRetryKey(request, request.delegatedCompletion),
            );
          }
          if (request.clearIfCurrent !== undefined) {
            yield* request.clearIfCurrent();
          } else if (request.dispatchIfCurrent !== undefined) {
            yield* request.dispatchIfCurrent(Effect.void);
          }
          return;
        }
        if (request.delegatedCompletion !== undefined) {
          const retryKey = delegatedCompletionRetryKey(request, request.delegatedCompletion);
          const delivery = currentDelegatedCompletionDelivery(
            projection,
            request.delegatedCompletion,
          );
          if (delivery === undefined) {
            yield* clearRetryAttempt(retryKey);
            return;
          }
          const commandId = yield* ids.allocate.command({
            fixtureName: "delegated-completion",
            commandName: "dispatch",
          });
          yield* threads
            .dispatch({
              type: "message.dispatch",
              commandId,
              threadId: request.threadId,
              messageId: delivery.messageId,
              text: delegatedCompletionText(delivery.taskIds),
              attachments: [],
              dispatchMode: { type: "queue_after_active" },
              createdBy: "agent",
              creationSource: "server",
              delegatedCompletion: {
                parentRunId: request.delegatedCompletion.parentRunId,
                generation: delivery.generation,
                taskIds: delivery.taskIds,
              },
            })
            .pipe(
              Effect.catchTags({
                ThreadManagementProviderInstanceDisabledError: (error) =>
                  Effect.logInfo(
                    "orchestration-v2.provider-continuation.delegated-completion-disabled",
                    {
                      threadId: request.threadId,
                      projectId: error.projectId,
                      instanceId: error.instanceId,
                    },
                  ).pipe(
                    Effect.andThen(
                      Effect.forEach(
                        delivery.taskIds,
                        (taskId) =>
                          ids.allocate
                            .command({
                              fixtureName: "delegated-completion",
                              commandName: "dispose-disabled",
                            })
                            .pipe(
                              Effect.flatMap((disposeCommandId) =>
                                threads.dispatch({
                                  type: "delegated_task.completion-delivery.dispose",
                                  commandId: disposeCommandId,
                                  parentThreadId: request.threadId,
                                  taskId,
                                }),
                              ),
                            ),
                        { discard: true },
                      ),
                    ),
                    Effect.andThen(
                      request.clearIfCurrent !== undefined
                        ? request.clearIfCurrent()
                        : request.dispatchIfCurrent !== undefined
                          ? request.dispatchIfCurrent(Effect.void)
                          : Effect.void,
                    ),
                  ),
              }),
            );
          yield* clearRetryAttempt(retryKey);
          return;
        }
        const messageId =
          request.messageId ??
          (yield* ids.allocate.message({
            threadId: request.threadId,
            ordinal: projection.messages.length + 1,
          }));
        const commandId = CommandId.make(`provider-continuation:${messageId}`);
        const dispatch = threads
          .dispatch({
            type: "message.dispatch",
            commandId,
            threadId: request.threadId,
            messageId,
            text: request.detail ?? CONTINUATION_MESSAGE_TEXT,
            notification: request.notification ?? {
              source: { kind: "background_task" },
              outcome: "updated",
              summary: "Background activity updated",
            },
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            // Only buffered wakes use creationSource "provider"; Claude would drain an empty buffer instead of prompting message_text.
            creationSource: request.delivery === "message_text" ? "server" : "provider",
          })
          .pipe(Effect.tap((dispatched) => releaseIfNeverStarted(request, messageId, dispatched)));
        // Catch after dispatchIfCurrent so adapters see raw failures first; clearIfCurrent is idempotent and covers others.
        const logInstanceDisabled = (
          error: ThreadManagementService.ThreadManagementProviderInstanceDisabledError,
        ) =>
          Effect.logInfo("orchestration-v2.provider-continuation.instance-disabled", {
            threadId: request.threadId,
            providerThreadId: request.providerThreadId,
            projectId: error.projectId,
            instanceId: error.instanceId,
          }).pipe(
            Effect.andThen(
              request.clearIfCurrent !== undefined ? request.clearIfCurrent() : Effect.void,
            ),
          );
        if (request.dispatchIfCurrent === undefined) {
          yield* dispatch.pipe(
            Effect.catchTags({
              ThreadManagementProviderInstanceDisabledError: logInstanceDisabled,
            }),
          );
          return;
        }
        yield* request.dispatchIfCurrent(dispatch).pipe(
          Effect.catchTags({
            ThreadManagementProviderInstanceDisabledError: logInstanceDisabled,
          }),
        );
      },
    );

    yield* requests.take.pipe(
      Effect.flatMap((request) =>
        dispatchContinuation(request).pipe(
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              yield* Effect.logWarning("orchestration-v2.provider-continuation.dispatch-failed", {
                threadId: request.threadId,
                providerThreadId: request.providerThreadId,
                cause,
              });
              if (request.delegatedCompletion !== undefined) {
                const completion = request.delegatedCompletion;
                const retryKey = delegatedCompletionRetryKey(request, completion);
                const retryDelay = yield* nextRetryDelay(retryKey);
                yield* Effect.gen(function* () {
                  yield* Effect.sleep(`${retryDelay} millis`);
                  const projection = yield* threads.getThreadRecords(
                    request.threadId,
                    ["messages", "runs", "providerTurns"],
                    {
                      messageIds:
                        request.delegatedCompletion === undefined
                          ? []
                          : [request.delegatedCompletion.messageId],
                    },
                  );
                  if (currentDelegatedCompletionDelivery(projection, completion) !== undefined) {
                    yield* requests.offer(request);
                  } else {
                    yield* clearRetryAttempt(retryKey);
                  }
                }).pipe(
                  Effect.catchCause((retryCause) =>
                    Effect.logWarning("orchestration-v2.provider-continuation.retry-check-failed", {
                      threadId: request.threadId,
                      providerThreadId: request.providerThreadId,
                      cause: retryCause,
                    }).pipe(Effect.andThen(requests.offer(request))),
                  ),
                  Effect.forkScoped,
                );
              } else if (request.clearIfCurrent !== undefined) {
                yield* request.clearIfCurrent();
              }
            }),
          ),
          // A dispatch can wait on its parent's lock, so only its own thread's later requests queue behind it.
          (handled) => continuations.run(request.threadId, handled),
        ),
      ),
      Effect.forever,
      Effect.forkScoped,
    );
  }),
);
