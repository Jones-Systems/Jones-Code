import { PendingSendStatus } from "../../jones/pendingSends/PendingSendStatus";
import { QueuedClearAction } from "../../jones/queue/QueuedClearAction";
import { useQueuedRunRemoval } from "../../jones/queue/useQueuedRunRemoval";
import { canDispatchQueuedRun } from "../../jones/queue/queuedRunAdmission";
import { QueuedSteerBatchAction } from "../../jones/queue/QueuedSteerBatchAction";
import { useQueuedSteerBatch } from "../../jones/queue/useQueuedSteerBatch";
import {
  importedHistoryCommands,
  type PreparedImportedHistoryChoice,
} from "../../jones/importedHistory/ContinuationChoiceBanner";
import {
  importedHistoryContinuationEligible,
  resolveImportedHistoryReview,
} from "@t3tools/client-runtime/jones/imported-history/continuation";
import { importedHistoryCanonicalJson } from "@t3tools/shared/jones/importedHistoryCanonical";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { deriveThreadQueueWorkflowState } from "@t3tools/client-runtime/state/thread-workflows";
import { assistantCitationsToPlainText } from "@t3tools/shared/assistantCitations";
import { replaceComposerContextReferences } from "@t3tools/shared/composerContextReferences";
import type {
  ChatAttachment as ContractChatAttachment,
  EnvironmentId,
  MessageId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { CornerUpRightIcon, GripVerticalIcon, ListOrderedIcon, PencilIcon } from "lucide-react";
import { useId, useImperativeHandle, useLayoutEffect, useRef, useState, type Ref } from "react";

import { useAssetUrls } from "../../assets/assetUrls";
import { threadEnvironment } from "../../state/threads";
import { useThreadProjection, useServerConfigs } from "../../state/entities";
import { useAtomCommand } from "../../state/use-atom-command";
import { isImageAttachment, type ChatMessage } from "../../types";
import { cn } from "~/lib/utils";
import { ComposerBanner } from "./ComposerBanner";
import { Button } from "../ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

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
  readonly onEditQueuedRun: (request: EditQueuedRunRequest) => void;
  readonly onCancelEdit: () => void;
  readonly onImportedHistoryChoice?: (choice: {
    environmentId: EnvironmentId;
    threadId: ThreadId;
    prepared: PreparedImportedHistoryChoice | null;
    reason: string | null;
  }) => void;
}) {
  const projection = useThreadProjection(
    scopeThreadRef(props.environmentId, props.threadId),
  )?.projection;
  const serverConfig = useServerConfigs().get(props.environmentId);
  const reorder = useAtomCommand(threadEnvironment.reorderQueuedRun);
  const promote = useAtomCommand(threadEnvironment.promoteQueuedRun);
  const resume = useAtomCommand(threadEnvironment.resumeThreadQueue);
  const reviewImportedHistory = useAtomCommand(importedHistoryCommands.review, {
    reportFailure: false,
  });
  const importedEligible = importedHistoryContinuationEligible(projection);

  const [expanded, setExpanded] = useState(true);
  const queueListId = useId();
  const [busyRunId, setBusyRunId] = useState<RunId | null>(null);
  const [dragState, setDragState] = useState<{
    readonly runId: RunId;
    readonly insertIndex: number | null;
  } | null>(null);
  // Arm from the grip to prevent accidental text drags from reordering queued runs.
  const dragArmedRunIdRef = useRef<RunId | null>(null);
  const steerInFlightRef = useRef(false);
  // Run-only acknowledgements can resurrect pending rows after cancellation or startup.
  const acknowledgedMessageIds = new Set(
    (projection?.runs ?? []).flatMap((run) =>
      run.userMessageId &&
      (run.status === "cancelled" ||
        projection?.messages.some((message) => message.id === run.userMessageId))
        ? [run.userMessageId]
        : [],
    ),
  );
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
  // oxlint-disable-next-line t3code/react-effect-budget -- Publish the committed route and queue to asynchronous imported-run review guards.
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

  const activeRun = workflow?.activeRun ?? null;
  const canReorder = workflow?.canReorder === true;
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
    ...queued.map(({ run, text, attachments, context }, serverIndex) => ({
      key: run.id,
      runId: run.id,
      messageId: run.userMessageId,
      serverIndex,
      text,
      // A guest's message keeps the guest's words; the owner can cancel it, not rewrite it.
      editable: context?.guest === undefined,
      guestName: context?.guest?.name,
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
      runId: null,
      messageId: message.id,
      serverIndex: null,
      text: message.text,
      editable: true,
      guestName: undefined,
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
        queueBusy ||
        steerInFlightRef.current ||
        removals.isBusy()
      )
        return false;
      if (!repeat) {
        setExpanded(true);
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

  return (
    <>
      <ComposerBanner.Attachment>
        <ComposerBanner.Root
          role="region"
          aria-label={`${items.length} queued message${items.length === 1 ? "" : "s"}`}
          aria-live="polite"
          data-chat-composer-collapsed-controls="true"
          className="relative z-0"
        >
          <ComposerBanner.Row>
            <ComposerBanner.Icon>
              <ListOrderedIcon />
            </ComposerBanner.Icon>
            <ComposerBanner.Content className="text-muted-foreground">
              Queued
              <QueuedClearAction
                disabled={
                  removalBlocked ||
                  props.editingRunId !== null ||
                  optimisticQueued.length > 0 ||
                  !queued.some(({ run }) => !removals.isRemoving(run.id))
                }
                onClear={() => {
                  if (props.editingRunId !== null || optimisticQueued.length > 0) return;
                  void removals.remove(queued.map(({ run }) => run.id));
                }}
                failedCount={removals.failedCount}
              />
            </ComposerBanner.Content>
            <ComposerBanner.Actions>
              <QueuedSteerBatchAction batch={batch} />
              <Button
                size="xs"
                variant="ghost-muted"
                aria-label={expanded ? "Collapse queued messages" : "Expand queued messages"}
                aria-expanded={expanded}
                aria-controls={queueListId}
                onPointerDown={(event) => event.preventDefault()}
                onClick={() => setExpanded((value) => !value)}
              >
                <ComposerBanner.Count>{items.length}</ComposerBanner.Count>
                <ComposerBanner.ToggleIcon expanded={expanded} />
              </Button>
            </ComposerBanner.Actions>
          </ComposerBanner.Row>
          <ComposerBanner.Scroll className={cn("max-h-32", !expanded && "hidden")}>
            <ComposerBanner.Children render={<ol />} id={queueListId}>
              {items.map((item) => {
                const previewText = assistantCitationsToPlainText(
                  replaceComposerContextReferences(item.text, (reference) =>
                    reference.kind === "image" && item.thumbnails.length > 0 ? "" : reference.label,
                  ),
                ).trim();
                const rowRunId = item.runId;
                const rowServerIndex = item.serverIndex;
                const isEditing = rowRunId !== null && rowRunId === props.editingRunId;
                const rowDraggable =
                  rowRunId !== null && rowServerIndex !== null && canReorder && !queueBusy;
                return (
                  <ComposerBanner.Row
                    render={<li />}
                    key={item.key}
                    aria-current={isEditing ? "true" : undefined}
                    className={cn(
                      "relative rounded-sm",
                      isEditing && "bg-accent text-accent-foreground",
                      dragState !== null && dragState.runId === item.runId && "opacity-50",
                    )}
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
                  >
                    {item.serverIndex !== null && dragState?.insertIndex === item.serverIndex ? (
                      <span
                        aria-hidden
                        className="pointer-events-none absolute inset-x-0 top-0 h-0.5 rounded bg-primary/70"
                      />
                    ) : null}
                    {item.serverIndex === queued.length - 1 &&
                    dragState?.insertIndex === queued.length ? (
                      <span
                        aria-hidden
                        className="pointer-events-none absolute inset-x-0 bottom-0 h-0.5 rounded bg-primary/70"
                      />
                    ) : null}
                    <ComposerBanner.Icon aria-hidden={false}>
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
                    </ComposerBanner.Icon>
                    <ComposerBanner.Content className="text-foreground/80">
                      {isEditing ? <span className="sr-only">Editing queued message: </span> : null}
                      {item.pending && item.messageId ? (
                        <PendingSendStatus
                          environmentId={props.environmentId}
                          threadId={props.threadId}
                          messageId={item.messageId}
                        />
                      ) : null}
                      {item.thumbnails.length > 0 ? (
                        <span className="flex shrink-0 items-center gap-0.5">
                          {item.thumbnails.map((thumbnail) => (
                            <span
                              key={thumbnail.key}
                              className="size-4 overflow-hidden rounded border border-border/70 bg-background"
                            >
                              {thumbnail.url ? (
                                <img
                                  src={thumbnail.url}
                                  alt={thumbnail.name}
                                  className="size-full object-cover"
                                />
                              ) : (
                                <span
                                  aria-label={thumbnail.name}
                                  className="block size-full bg-muted/60"
                                />
                              )}
                            </span>
                          ))}
                        </span>
                      ) : null}
                      {item.guestName === undefined ? null : (
                        <span className="shrink-0 text-guest-foreground">{item.guestName}</span>
                      )}
                      <Tooltip>
                        <TooltipTrigger render={<span className="min-w-0 flex-1 truncate" />}>
                          {previewText}
                        </TooltipTrigger>
                        <TooltipPopup side="top" className="max-w-96 break-words">
                          {previewText}
                        </TooltipPopup>
                      </Tooltip>
                    </ComposerBanner.Content>
                    <ComposerBanner.Actions>
                      {importedEligible &&
                      props.onImportedHistoryChoice &&
                      item.serverIndex === 0 &&
                      item.runId !== null &&
                      item.messageId !== null ? (
                        <Button
                          size="xs"
                          variant="ghost-muted"
                          disabled={queueBusy}
                          onClick={() => {
                            void reviewImportedQueued(item.runId!, item.messageId!);
                          }}
                        >
                          Review imported history
                        </Button>
                      ) : null}

                      {isEditing ? (
                        <Button
                          size="xs"
                          variant="ghost"
                          aria-label="Cancel editing queued message"
                          disabled={queueBusy}
                          onClick={() => {
                            if (queueBusy || steerInFlightRef.current || removals.isBusy()) return;
                            props.onCancelEdit();
                          }}
                        >
                          Cancel
                        </Button>
                      ) : (
                        <>
                          <Tooltip>
                            <TooltipTrigger
                              render={
                                <Button
                                  size="icon-xs"
                                  variant="ghost-muted"
                                  aria-label="Edit queued message"
                                  disabled={item.runId === null || !item.editable || queueBusy}
                                  onClick={() => {
                                    if (
                                      !item.editable ||
                                      queueBusy ||
                                      steerInFlightRef.current ||
                                      removals.isBusy()
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
                                />
                              }
                            >
                              <PencilIcon />
                            </TooltipTrigger>
                            <TooltipPopup>
                              {`Edit in the composer${item.serverIndex === queued.length - 1 && props.editShortcutLabel ? ` (${props.editShortcutLabel})` : ""}`}
                            </TooltipPopup>
                          </Tooltip>
                          <Tooltip>
                            <TooltipTrigger render={<span className="flex shrink-0" />}>
                              <Button
                                size="xs"
                                variant="ghost-muted"
                                disabled={item.runId === null || queueBusy || !canSend}
                                onClick={() => {
                                  if (item.runId !== null) {
                                    void sendQueuedMessage(item.runId);
                                  }
                                }}
                              >
                                <CornerUpRightIcon />
                                {workflow?.canSendNow ? "Send now" : "Steer"}
                              </Button>
                            </TooltipTrigger>
                            <TooltipPopup>
                              {`${workflow?.canSendNow ? "Start a turn with this message" : "Send as a steer instead"}${item.serverIndex === 0 && props.steerShortcutLabel ? ` (${props.steerShortcutLabel})` : ""}`}
                            </TooltipPopup>
                          </Tooltip>
                          <Tooltip>
                            <TooltipTrigger
                              render={
                                <ComposerBanner.Dismiss
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
                            />
                            <TooltipPopup>Remove from queue</TooltipPopup>
                          </Tooltip>
                        </>
                      )}
                    </ComposerBanner.Actions>
                  </ComposerBanner.Row>
                );
              })}
            </ComposerBanner.Children>
          </ComposerBanner.Scroll>
        </ComposerBanner.Root>
      </ComposerBanner.Attachment>
    </>
  );
}
