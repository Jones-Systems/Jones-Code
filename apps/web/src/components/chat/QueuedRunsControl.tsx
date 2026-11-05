import { useAtomValue } from "@effect/atom-react";
import { serverEnvironment } from "../../state/server";
import { GripVerticalIcon } from "lucide-react";
import { PendingSendStatus } from "../../jones/pendingSends/PendingSendStatus";
import { QueuedClearAction } from "../../jones/queue/QueuedClearAction";
import { useQueuedRunRemoval } from "../../jones/queue/useQueuedRunRemoval";
import { canDispatchQueuedRun } from "../../jones/queue/queuedRunAdmission";
import { QueuedSteerBatchAction } from "../../jones/queue/QueuedSteerBatchAction";
import { useQueuedSteerBatch } from "../../jones/queue/useQueuedSteerBatch";
import { importedHistoryCommands } from "../../jones/importedHistory/commands";
import type { PreparedImportedHistoryChoice } from "../../jones/importedHistory/ContinuationChoiceBanner";
import {
  importedHistoryContinuationEligible,
  resolveImportedHistoryReview,
} from "@t3tools/client-runtime/jones/imported-history/continuation";
import { importedHistoryCanonicalJson } from "@t3tools/shared/jones/importedHistoryCanonical";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { deriveThreadQueueWorkflowState } from "@t3tools/client-runtime/state/thread-workflows";
import { assistantCitationsToPlainText } from "@t3tools/shared/assistantCitations";
import {
  isAttachmentOnlyBootstrapPrompt,
  replaceComposerContextReferences,
} from "@t3tools/shared/composerContextReferences";
import type {
  ChatAttachment as ContractChatAttachment,
  EnvironmentId,
  MessageId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { resolveUserMessagePresentation } from "@t3tools/client-runtime/user-message";
import { Link } from "@tanstack/react-router";
import {
  ArrowUpIcon,
  CalendarClockIcon,
  ChevronDownIcon,
  MessagesSquareIcon,
  PencilIcon,
  XIcon,
} from "lucide-react";
import * as DateTime from "effect/DateTime";
import {
  useLayoutEffect,
  useId,
  useImperativeHandle,
  useRef,
  useState,
  type ReactNode,
  type Ref,
} from "react";

import { useAssetUrls } from "../../assets/assetUrls";
import { threadEnvironment } from "../../state/threads";
import { useQuietThreadShell, useThreadProjection } from "../../state/entities";
import { useAtomCommand } from "../../state/use-atom-command";
import { isImageAttachment, type ChatMessage } from "../../types";
import { cn } from "~/lib/utils";
import { Button, InlineButton } from "../ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import type { TimestampFormat } from "@t3tools/contracts/settings";
import { GuestMessageName } from "./guestMessageStyle";
import { TimelineRowTimestamp } from "./TimelineRowParts";
import { FoldedMessageHeader, useScheduledTaskTitle } from "./TimelineUserMessageRow";

export interface EditQueuedRunRequest {
  readonly runId: RunId;
  readonly messageId: MessageId;
  readonly text: string;
  readonly attachments: ReadonlyArray<ContractChatAttachment>;
}

interface QueuedRowThumbnail {
  readonly key: string;
  readonly name: string;
  readonly url: string | null;
}

// A longer queue shows its first messages and a button that shows the rest.
const QUEUE_VISIBLE_ROWS = 3;
const QUEUED_RUN_DRAG_TYPE = "application/x-t3code-queued-run";

export interface QueuedRunsControlHandle {
  steerNext: (repeat: boolean) => boolean;
  editLatest: (repeat: boolean) => boolean;
}

type ThreadQueueProjection = Parameters<typeof deriveThreadQueueWorkflowState>[0];

function queuedImageResources(projection: ThreadQueueProjection | undefined) {
  if (!projection) return [];
  return deriveThreadQueueWorkflowState(projection).queuedRuns.flatMap(({ attachments }) =>
    attachments.flatMap((attachment) =>
      attachment.type === "image"
        ? [{ _tag: "attachment" as const, attachmentId: attachment.id }]
        : [],
    ),
  );
}

export function QueuedRunsControl({
  ref,
  ...props
}: {
  readonly ref?: Ref<QueuedRunsControlHandle>;
  readonly steerShortcutLabel?: string | null;
  readonly editShortcutLabel?: string | null;
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly optimisticMessages: ReadonlyArray<
    Pick<ChatMessage, "id" | "inputIntent" | "text" | "attachments">
  >;
  readonly editingRunId: RunId | null;
  /** Set while something else holds the composer, such as an open question. */
  readonly editBlockedReason: string | null;
  readonly onEditQueuedRun: (request: EditQueuedRunRequest) => void;
  readonly onCancelEdit: () => void;
  readonly onImportedHistoryChoice?: (choice: {
    environmentId: EnvironmentId;
    threadId: ThreadId;
    reason: string | null;
    prepared: PreparedImportedHistoryChoice | null;
  }) => void;
  readonly timestampFormat: TimestampFormat;
  readonly onOpenThread: (threadId: ThreadId) => void;
}) {
  const projection = useThreadProjection(
    scopeThreadRef(props.environmentId, props.threadId),
  )?.projection;
  const serverConfig = useAtomValue(serverEnvironment.configValueAtom(props.environmentId));
  const reviewImportedHistory = useAtomCommand(importedHistoryCommands.review, {
    reportFailure: false,
  });
  const importedEligible = importedHistoryContinuationEligible(projection);
  const [dragState, setDragState] = useState<{
    readonly runId: RunId;
    readonly insertIndex: number | null;
  } | null>(null);
  const dragArmedRunIdRef = useRef<RunId | null>(null);
  const reorder = useAtomCommand(threadEnvironment.reorderQueuedRun);
  const promote = useAtomCommand(threadEnvironment.promoteQueuedRun);
  const resume = useAtomCommand(threadEnvironment.resumeThreadQueue);
  const [showAll, setShowAll] = useState(false);
  const queueListId = useId();
  const [busyRunId, setBusyRunId] = useState<RunId | null>(null);
  const steerInFlightRef = useRef(false);
  // Queue messages hand off through their projection; cancellation also retires local rows.
  const acknowledgedMessageIds = new Set([
    ...(projection?.messages ?? []).map((message) => message.id),
    ...(projection?.runs ?? []).flatMap((run) =>
      run.userMessageId && run.status === "cancelled" ? [run.userMessageId] : [],
    ),
  ]);
  const queuedImageAttachmentResources = queuedImageResources(projection);
  const queuedImageAttachmentUrls = useAssetUrls(
    props.environmentId,
    queuedImageAttachmentResources,
  );
  const workflow = projection ? deriveThreadQueueWorkflowState(projection) : null;
  const queued = workflow?.queuedRuns ?? [];
  const removals = useQueuedRunRemoval({
    environmentId: props.environmentId,
    threadId: props.threadId,
    queuedRunIds: queued.map(({ run }) => run.id),
    blocked: () => busyRunId !== null || steerInFlightRef.current,
  });
  const currentQueueRef = useRef({
    environmentId: props.environmentId,
    threadId: props.threadId,
    queued,
    importedEligible,
  });
  // Async review guards always compare against the latest committed route and queue.
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Publish the committed route and queue to asynchronous imported-run review guards.
  useLayoutEffect(() => {
    currentQueueRef.current = {
      environmentId: props.environmentId,
      threadId: props.threadId,
      queued,
      importedEligible,
    };
  }, [props.environmentId, props.threadId, queued, importedEligible, currentQueueRef]);
  const reviewImportedQueued = async (runId: RunId, messageId: MessageId) => {
    const onImportedHistoryChoice = props.onImportedHistoryChoice;
    if (
      !importedEligible ||
      !onImportedHistoryChoice ||
      busyRunId !== null ||
      removals.isBusy() ||
      steerInFlightRef.current
    )
      return;
    const entry = queued.find((item) => item.run.id === runId && item.messageId === messageId);
    if (!entry || entry !== queued[0]) return;
    const snapshot = importedHistoryCanonicalJson(entry);
    const queueIdentity = importedHistoryCanonicalJson(queued);
    const environmentId = props.environmentId;
    const threadId = props.threadId;
    const unchanged = () => {
      const current = currentQueueRef.current;
      const item = current.queued.find(
        (candidate) => candidate.run.id === runId && candidate.messageId === messageId,
      );
      return (
        current.importedEligible &&
        current.environmentId === environmentId &&
        current.threadId === threadId &&
        item !== undefined &&
        importedHistoryCanonicalJson(item) === snapshot &&
        importedHistoryCanonicalJson(current.queued) === queueIdentity
      );
    };
    steerInFlightRef.current = true;
    setBusyRunId(runId);
    await (async () => {
      const delivery = { type: "queued_run" as const, runId, messageId };
      const result = await reviewImportedHistory({ environmentId, input: { threadId, delivery } });
      const review = resolveImportedHistoryReview(result._tag === "Success" ? result.value : null);
      if (unchanged()) {
        onImportedHistoryChoice({
          environmentId,
          threadId,
          reason: review.reason,
          prepared:
            review.status === "available"
              ? {
                  delivery,
                  reviewedBasis: review.reviewedBasis,
                  draftIdentity: snapshot,
                  unchanged,
                }
              : null,
        });
      }
    })().finally(() => {
      steerInFlightRef.current = false;
      setBusyRunId(null);
    });
  };

  const canReorder = workflow?.canReorder === true;
  const activeRun = workflow?.activeRun ?? null;
  const queuedImageUrlById = new Map(
    queuedImageAttachmentUrls.flatMap((url, index) => {
      const resource = queuedImageAttachmentResources[index];
      return url && resource ? [[resource.attachmentId, url] as const] : [];
    }),
  );
  const optimisticQueued = props.optimisticMessages.filter(
    (message) => message.inputIntent === "queued_turn" && !acknowledgedMessageIds.has(message.id),
  );
  const batch = useQueuedSteerBatch({
    environmentId: props.environmentId,
    threadId: props.threadId,
    projection,
    editing: props.editingRunId !== null,
    hasUnsavedMessages: optimisticQueued.length > 0,
    busy: busyRunId !== null || removals.busy || dragState !== null,
    mutationLock: {
      acquire: () => {
        if (steerInFlightRef.current || removals.isBusy()) return false;
        steerInFlightRef.current = true;
        return true;
      },
      release: () => {
        steerInFlightRef.current = false;
      },
    },
  });
  const queueBusy = busyRunId !== null || batch.pending || removals.busy;
  const removalBlocked = busyRunId !== null || batch.pending;
  const canSend = canDispatchQueuedRun({
    busy: queueBusy,
    importedHistory: importedEligible,
    canStart: workflow?.canSendNow === true,
    canSteer: workflow?.canPromoteToSteer === true,
    queuedRunStart: serverConfig?.environment.capabilities.queuedRunStart,
  });
  const items = [
    ...queued.map(({ run, text, attachments, context, message }, serverIndex) => ({
      key: run.id,
      message,
      runId: run.id,
      messageId: run.userMessageId,
      serverIndex,
      text,
      // A guest's message keeps the guest's words; the owner can cancel it, not rewrite it.
      editable: context?.guest === undefined,
      attachments,
      thumbnails: attachments
        .filter((attachment) => attachment.type === "image")
        .map<QueuedRowThumbnail>((attachment) => ({
          key: attachment.id,
          name: attachment.name,
          url: queuedImageUrlById.get(attachment.id) ?? null,
        })),
      pending: false,
    })),
    ...optimisticQueued.map((message) => ({
      key: message.id,
      message: undefined,
      runId: null,
      messageId: message.id,
      serverIndex: null,
      text: message.text,
      editable: true,
      attachments: [] as ReadonlyArray<ContractChatAttachment>,
      thumbnails: (message.attachments ?? [])
        .filter(isImageAttachment)
        .map<QueuedRowThumbnail>((attachment) => ({
          key: attachment.id,
          name: attachment.name,
          url: attachment.previewUrl ?? null,
        })),
      pending: true,
    })),
  ];

  const move = async (runId: RunId, beforeRunId: RunId | null) => {
    if (queueBusy || steerInFlightRef.current || removals.isBusy()) return;
    steerInFlightRef.current = true;
    setBusyRunId(runId);
    await Promise.resolve()
      .then(() =>
        reorder({
          environmentId: props.environmentId,
          input: { threadId: props.threadId, runId, beforeRunId },
        }),
      )
      .finally(() => {
        steerInFlightRef.current = false;
        setBusyRunId(null);
      });
  };

  const completeDrag = (runId: RunId, insertIndex: number | null) => {
    setDragState(null);
    dragArmedRunIdRef.current = null;
    if (insertIndex === null || queueBusy || steerInFlightRef.current || removals.isBusy()) return;
    const draggedIndex = queued.findIndex(({ run }) => run.id === runId);
    if (draggedIndex === -1) return;
    if (insertIndex === draggedIndex || insertIndex === draggedIndex + 1) return;
    void move(runId, queued[insertIndex]?.run.id ?? null);
  };

  const sendQueuedMessage = async (queuedRunId: RunId) => {
    if (!canSend || steerInFlightRef.current || removals.isBusy()) return;
    steerInFlightRef.current = true;
    setBusyRunId(queuedRunId);
    await (async () => {
      if (workflow?.canSendNow) {
        await resume({
          environmentId: props.environmentId,
          input: { threadId: props.threadId, runId: queuedRunId },
        });
      } else if (activeRun !== null) {
        await promote({
          environmentId: props.environmentId,
          input: { threadId: props.threadId, queuedRunId, targetRunId: activeRun.id },
        });
      }
    })().finally(() => {
      steerInFlightRef.current = false;
      setBusyRunId(null);
    });
  };

  useImperativeHandle(ref, () => ({
    steerNext(repeat) {
      const next = queued[0];
      if (!next || !canSend) return false;
      if (!repeat && !queueBusy) void sendQueuedMessage(next.run.id);
      return true;
    },
    // Leave the key to caret navigation while editing.
    editLatest(repeat) {
      const latest = queued.at(-1);
      if (
        !latest ||
        latest.context?.guest !== undefined ||
        props.editingRunId !== null ||
        props.editBlockedReason !== null ||
        queueBusy ||
        steerInFlightRef.current ||
        removals.isBusy()
      ) {
        return false;
      }
      if (!repeat) {
        setShowAll(true);
        props.onEditQueuedRun({
          runId: latest.run.id,
          messageId: latest.run.userMessageId,
          text: latest.text,
          attachments: latest.attachments,
        });
      }
      return true;
    },
  }));

  if (items.length === 0) return null;

  const collapsible = items.length > QUEUE_VISIBLE_ROWS;
  const visibleItems = collapsible && !showAll ? items.slice(0, QUEUE_VISIBLE_ROWS - 1) : items;

  return (
    <section
      aria-label={`${items.length} queued message${items.length === 1 ? "" : "s"}`}
      data-thread-queue="true"
      // Anchors the sr-only live region; unanchored, it escapes the timeline scroller and makes the chat column scroll.
      className="relative mx-auto flex w-full max-w-(--chat-content-max-width) flex-col gap-2 pb-4"
    >
      <span className="sr-only" aria-live="polite">
        {items.length} queued
      </span>
      <div className="flex flex-wrap items-center justify-end gap-2 text-xs">
        <QueuedClearAction
          disabled={
            removalBlocked ||
            props.editingRunId !== null ||
            optimisticQueued.length > 0 ||
            !queued.some(({ run }) => !removals.isRemoving(run.id))
          }
          onClear={() => {
            if (props.editingRunId === null && optimisticQueued.length === 0) {
              setShowAll(true);
              void removals.remove(queued.map(({ run }) => run.id));
            }
          }}
          failedCount={removals.failedCount}
        />
        <QueuedSteerBatchAction batch={batch} onSend={() => setShowAll(true)} />
      </div>
      <ol className="flex flex-col items-end gap-3" id={queueListId}>
        {visibleItems.map((item, position) => {
          const previewText = isAttachmentOnlyBootstrapPrompt(item.text)
            ? ""
            : assistantCitationsToPlainText(
                replaceComposerContextReferences(item.text, (reference) =>
                  reference.kind === "image" && item.thumbnails.length > 0 ? "" : reference.label,
                ),
              ).trim();
          const rowRunId = item.runId;
          const rowServerIndex = item.serverIndex;
          const rowDraggable =
            rowRunId !== null && rowServerIndex !== null && canReorder && !queueBusy;
          const isEditing = item.runId !== null && item.runId === props.editingRunId;
          return (
            <li
              draggable={rowDraggable}
              onDragStart={(event) => {
                if (item.runId === null || dragArmedRunIdRef.current !== item.runId) {
                  event.preventDefault();
                  return;
                }
                event.dataTransfer.setData(QUEUED_RUN_DRAG_TYPE, item.runId);
                event.dataTransfer.effectAllowed = "move";
                setDragState({ runId: item.runId, insertIndex: null });
              }}
              onDragEnd={() => {
                dragArmedRunIdRef.current = null;
                setDragState(null);
              }}
              onDragOver={(event) => {
                if (dragState === null || item.serverIndex === null) return;
                event.preventDefault();
                event.dataTransfer.dropEffect = "move";
                const rect = event.currentTarget.getBoundingClientRect();
                const insertIndex =
                  event.clientY < rect.top + rect.height / 2
                    ? item.serverIndex
                    : item.serverIndex + 1;
                if (dragState.insertIndex !== insertIndex) {
                  setDragState({ runId: dragState.runId, insertIndex });
                }
              }}
              onDrop={(event) => {
                if (dragState === null) return;
                event.preventDefault();
                completeDrag(dragState.runId, dragState.insertIndex);
              }}

              key={item.key}
              aria-label={`Queued message ${position + 1} of ${items.length}, not sent`}
              aria-posinset={position + 1}
              aria-setsize={items.length}
              aria-current={isEditing ? "true" : undefined}
              className="group/queued flex w-full flex-col items-end gap-1 transition-opacity duration-150 ease-out starting:opacity-0 motion-reduce:transition-none"
            >
              {canReorder && rowRunId !== null && rowServerIndex !== null ? (
                <Button
                  size="icon-xs"
                  variant="ghost-muted"
                  aria-label="Reorder queued message (drag, or press the arrow keys)"
                  className="cursor-grab active:cursor-grabbing disabled:cursor-default"
                  disabled={queueBusy}
                  onPointerDown={() => {
                    dragArmedRunIdRef.current = rowRunId;
                  }}
                  onKeyDown={(event) => {
                    if (queueBusy || steerInFlightRef.current || removals.isBusy()) return;
                    if (event.key === "ArrowUp" && rowServerIndex > 0) {
                      event.preventDefault();
                      void move(rowRunId, queued[rowServerIndex - 1]?.run.id ?? null);
                    }
                    if (event.key === "ArrowDown" && rowServerIndex < queued.length - 1) {
                      event.preventDefault();
                      void move(rowRunId, queued[rowServerIndex + 2]?.run.id ?? null);
                    }
                  }}
                >
                  <GripVerticalIcon />
                </Button>
              ) : null}

              <QueuedMessage
                environmentId={props.environmentId}
                message={item.message}
                previewText={previewText}
                thumbnails={item.thumbnails}
                isEditing={isEditing}
                timestampFormat={props.timestampFormat}
                onOpenThread={props.onOpenThread}
              >
                {(workflow?.canSendNow || workflow?.canPromoteToSteer) && !isEditing ? (
                  <>
                    <Tooltip>
                      <TooltipTrigger render={<span className="flex shrink-0" />}>
                        <Button
                          size="xs"
                          variant="ghost-muted"
                          disabled={item.runId === null || !canSend || queueBusy}
                          onClick={() => {
                            if (item.runId !== null) {
                              void sendQueuedMessage(item.runId);
                            }
                          }}
                        >
                          <ArrowUpIcon />
                          {workflow?.canSendNow ? "Send now" : "Steer"}
                        </Button>
                      </TooltipTrigger>
                      <TooltipPopup>
                        {`${workflow?.canSendNow ? "Start a turn with this message" : workflow?.promoteSkipsCompaction ? "Skip compaction and send this message" : "Send into the current turn"}${item.serverIndex === 0 && props.steerShortcutLabel ? ` (${props.steerShortcutLabel})` : ""}`}
                      </TooltipPopup>
                    </Tooltip>
                  </>
                ) : null}
                {importedEligible &&
                !isEditing &&
                item.serverIndex === 0 &&
                item.runId !== null &&
                item.messageId !== null ? (
                  <Button
                    size="xs"
                    variant="ghost-muted"
                    disabled={queueBusy}
                    onClick={() => void reviewImportedQueued(item.runId!, item.messageId!)}
                  >
                    Review imported history
                  </Button>
                ) : null}
                {item.pending ? (
                  <PendingSendStatus
                    environmentId={props.environmentId}
                    threadId={props.threadId}
                    messageId={item.messageId}
                  />
                ) : null}
                {isEditing ? (
                  <>
                    <span className="px-1 text-foreground">Editing in the composer</span>
                    <Button
                      size="xs"
                      variant="ghost"
                      aria-label="Cancel editing queued message"
                      onClick={props.onCancelEdit}
                    >
                      Cancel
                    </Button>
                  </>
                ) : (
                  <>
                    <Tooltip>
                      {/* The span keeps the tooltip, and its reason, on a disabled button. */}
                      <TooltipTrigger render={<span className="flex shrink-0" />}>
                        <Button
                          size="icon-xs"
                          variant="ghost-muted"
                          aria-label="Edit queued message"
                          disabled={
                            item.runId === null ||
                            !item.editable ||
                            queueBusy ||
                            props.editBlockedReason !== null
                          }
                          onClick={() => {
                            if (
                              queueBusy ||
                              steerInFlightRef.current ||
                              removals.isBusy() ||
                              props.editBlockedReason !== null
                            )
                              return;
                            if (item.runId !== null && item.messageId !== null) {
                              props.onEditQueuedRun({
                                runId: item.runId,
                                messageId: item.messageId,
                                text: item.text,
                                attachments: item.attachments,
                              });
                            }
                          }}
                        >
                          <PencilIcon />
                        </Button>
                      </TooltipTrigger>
                      <TooltipPopup>
                        {props.editBlockedReason ??
                          `Edit in the composer${item.serverIndex === queued.length - 1 && props.editShortcutLabel ? ` (${props.editShortcutLabel})` : ""}`}
                      </TooltipPopup>
                    </Tooltip>
                    <Tooltip>
                      <TooltipTrigger
                        render={
                          <Button
                            size="icon-xs"
                            variant="ghost-muted"
                            aria-label="Remove queued message"
                            disabled={
                              item.runId === null ||
                              removalBlocked ||
                              removals.isRemoving(item.runId)
                            }
                            onClick={() => {
                              if (item.runId !== null) void removals.remove([item.runId]);
                            }}
                          />
                        }
                      >
                        <XIcon />
                      </TooltipTrigger>
                      <TooltipPopup>Remove from queue</TooltipPopup>
                    </Tooltip>
                  </>
                )}
              </QueuedMessage>
            </li>
          );
        })}
      </ol>
      {collapsible ? (
        <div className="self-end pe-1 text-xs pointer-coarse:[&_[data-slot=inline-button]]:relative pointer-coarse:[&_[data-slot=inline-button]]:after:absolute pointer-coarse:[&_[data-slot=inline-button]]:after:-inset-x-1.5 pointer-coarse:[&_[data-slot=inline-button]]:after:-inset-y-3.5">
          <InlineButton
            tone="muted"
            aria-label={showAll ? "Collapse queued messages" : "Expand queued messages"}
            aria-expanded={showAll}
            aria-controls={queueListId}
            // Keep the composer focused; keyboard activation still works.
            onPointerDown={(event) => event.preventDefault()}
            onClick={() => setShowAll((value) => !value)}
          >
            {showAll ? "Show fewer" : `Show all ${items.length} queued`}
            <ChevronDownIcon
              aria-hidden
              className={cn(
                "size-3.5 transition-transform duration-150 motion-reduce:transition-none",
                showAll && "rotate-180",
              )}
            />
          </InlineButton>
        </div>
      ) : null}
    </section>
  );
}

type QueuedRunMessage = NonNullable<ThreadQueueProjection["messages"][number]>;

/** A queued turn wears its sent form's attribution; only the dashed edge and controls differ. */
function QueuedMessage(props: {
  readonly environmentId: EnvironmentId;
  readonly message: QueuedRunMessage | undefined;
  readonly previewText: string;
  readonly thumbnails: ReadonlyArray<QueuedRowThumbnail>;
  readonly isEditing: boolean;
  readonly timestampFormat: TimestampFormat;
  readonly onOpenThread: (threadId: ThreadId) => void;
  readonly children: ReactNode;
}) {
  const presentation = props.message ? resolveUserMessagePresentation(props.message) : null;
  const attribution = presentation?.attribution ?? null;
  const scheduledTaskId = presentation?.scheduledTaskId;
  const senderThreadId = props.message?.senderThreadId;
  const [expanded, setExpanded] = useState(false);
  const taskTitle = useScheduledTaskTitle(props.environmentId, scheduledTaskId);
  const senderShell = useQuietThreadShell(
    attribution === "agent" && senderThreadId
      ? { environmentId: props.environmentId, threadId: senderThreadId }
      : null,
  );
  const fold =
    attribution === "automation"
      ? {
          icon: CalendarClockIcon,
          label: "Automation",
          summary: taskTitle ?? props.previewText.split(/\r?\n/, 1)[0] ?? "",
        }
      : attribution === "agent"
        ? {
            icon: MessagesSquareIcon,
            label: senderShell?.title.trim() || "Agent",
            summary: props.previewText.split(/\r?\n/).find((line) => line.trim()) ?? "",
          }
        : null;
  const bodyVisible = fold === null || expanded;

  return (
    <>
      {attribution === "t3code" ? (
        <p className="me-1 text-2xs text-muted-foreground/70">Sent by T3 Code</p>
      ) : null}
      {presentation?.guestName === undefined ? null : (
        <GuestMessageName name={presentation.guestName} />
      )}
      {/* Dashed and unfilled: a turn that has not been sent yet. */}
      <div
        className={cn(
          "flex min-w-0 max-w-[80%] flex-col rounded-2xl border border-dashed text-sm",
          props.isEditing
            ? "border-primary/60 text-foreground"
            : attribution === "guest"
              ? "border-guest/50 text-muted-foreground"
              : "border-muted-foreground/40 text-muted-foreground",
          fold === null && "px-3 py-2",
          fold !== null && expanded && "w-full",
        )}
      >
        {fold ? (
          <FoldedMessageHeader
            icon={fold.icon}
            label={fold.label}
            summary={fold.summary}
            expanded={expanded}
            onToggle={() => setExpanded((value) => !value)}
          />
        ) : null}
        {bodyVisible ? (
          <div
            className={cn(
              "flex flex-col gap-2",
              fold !== null && "max-h-96 overflow-auto px-3 pb-2",
            )}
          >
            {props.thumbnails.length > 0 ? (
              <span className="flex flex-wrap items-center gap-1">
                {props.thumbnails.map((thumbnail) => (
                  <span
                    key={thumbnail.key}
                    className="size-8 overflow-hidden rounded-md border border-border/70 bg-background"
                  >
                    {thumbnail.url ? (
                      <img
                        src={thumbnail.url}
                        alt={thumbnail.name}
                        className="size-full object-cover"
                      />
                    ) : (
                      <span aria-label={thumbnail.name} className="block size-full bg-muted/60" />
                    )}
                  </span>
                ))}
              </span>
            ) : null}
            {/* In full, like a sent turn: a queued message is read before it is sent. */}
            {props.previewText ? (
              <p className="whitespace-pre-wrap wrap-anywhere">{props.previewText}</p>
            ) : null}
          </div>
        ) : null}
      </div>
      <div className="flex items-center gap-0.5 pe-1 text-xs text-muted-foreground">
        {props.message ? (
          <span className="me-1 flex items-center gap-2 px-1 opacity-0 transition-opacity duration-200 pointer-coarse:opacity-100 focus-within:opacity-100 group-hover/queued:opacity-100">
            {attribution === "agent" && senderThreadId ? (
              <InlineButton tone="muted" onClick={() => props.onOpenThread(senderThreadId)}>
                Open thread
              </InlineButton>
            ) : null}
            {scheduledTaskId ? (
              <InlineButton
                tone="muted"
                render={
                  <Link
                    to="/settings/scheduled-tasks"
                    search={{ environmentId: props.environmentId, taskId: scheduledTaskId }}
                  />
                }
              >
                Open automation
              </InlineButton>
            ) : null}
            <TimelineRowTimestamp
              createdAt={DateTime.formatIso(props.message.createdAt)}
              timestampFormat={props.timestampFormat}
              alwaysVisible
            />
          </span>
        ) : null}
        {props.children}
      </div>
    </>
  );
}
