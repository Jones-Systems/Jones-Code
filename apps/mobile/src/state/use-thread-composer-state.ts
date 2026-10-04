import type { ComposerTextPaste } from "../native/T3ComposerEditor.types";
import { useAtomValue } from "@effect/atom-react";
import { threadRuntimeIsActive } from "@t3tools/client-runtime/state/shell";
import {
  deriveProviderSubagentStatus,
  deriveRunlessWorkStartedAt,
  deriveThreadActivityRun,
  deriveThreadRuntime,
  threadRuntimeHasInterruptibleRun,
} from "@t3tools/client-runtime/state/thread-execution";
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { Alert } from "react-native";

import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  MessageId,
  PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  type EnvironmentId,
  type ModelSelection,
  type ProviderInteractionMode,
  type RuntimeMode,
  type ThreadId,
} from "@t3tools/contracts";
import { safeErrorLogAttributes } from "@t3tools/client-runtime/errors";
import {
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import { clampFileAttachmentUploadBytes } from "@t3tools/client-runtime/state/attachments";
import { nextPastedTextFileName, pastedTextDisposition } from "@t3tools/client-runtime/text-paste";
import {
  parseCodexFeedbackCommand,
  submitCodexFeedback,
  type CodexFeedbackSubmission,
} from "@t3tools/client-runtime/state/threads";
import { resolveThreadWorkingStartedAt } from "@t3tools/client-runtime/state/models";
import { upgradeLegacyContextMessage } from "@t3tools/shared/composerContextLegacy";
import {
  composerContextSendBlockReason,
  reidentifyComposerContext,
  uploadedComposerContext,
} from "../lib/composerContext";
import { uuidv4 } from "../lib/uuid";

import { makeQueuedMessageMetadata } from "../lib/commandMetadata";
import { isModelSelectionUnavailable } from "../lib/modelOptions";
import { resolveProviderInteractionMode } from "./legacy-plan-mode";
import {
  convertPastedImagesToAttachments,
  createPastedTextComposerAttachment,
  pasteComposerClipboard,
  pickComposerFiles,
  pickComposerMedia,
  removePersistedComposerAttachmentFile,
} from "../lib/composerImages";
import type { DraftComposerImageAttachment } from "../lib/composerImages";
import { scopedThreadKey } from "../lib/scopedEntities";
import { buildThreadFeed } from "../lib/threadActivity";
import { acknowledgedThreadMessagesAtom } from "./acknowledged-thread-messages";
import { appendPendingThreadMessages } from "../features/threads/pending-thread-feed";
import { threadAllowsProviderSwitch } from "./thread-provider-switching";
import { appAtomRegistry } from "../state/atom-registry";
import { pendingThreadCreationMessage } from "./pending-thread-creation";
import {
  composerAttachmentUploadBlockReason,
  composerAttachmentUploadsAtom,
} from "../state/composer-attachment-uploads";
import {
  appendComposerDraftAttachments,
  captureComposerDraftInsertion,
  countComposerDraftAttachmentsAfterSelection,
  insertComposerDraftText,
  insertComposerDraftContext,
  clearComposerDraftContent,
  composerDraftsAtom,
  composerContextImportsAtom,
  ensureComposerDraftsLoaded,
  getComposerDraftSnapshot,
  mergeComposerDraftContent,
  removeComposerDraftAttachment,
  scheduleUnusedComposerAttachmentCleanup,
  setComposerDraftText,
  updateComposerDraftSettings,
  useComposerDraft,
  replaceComposerDraftAttachments,
  flushComposerDrafts,
  saveComposerImportedContinuationPointer,
  clearComposerImportedContinuationPointer,
  waitForComposerDraftsLoaded,
} from "./use-composer-drafts";
import {
  resolveComposerDispatchMode,
  type ActiveTurnComposerAction,
} from "@t3tools/client-runtime/state/composer-dispatch";
import { Atom } from "effect/unstable/reactivity";
import { AsyncResult } from "effect/unstable/reactivity";
import { prepareTurnAttachments } from "../lib/attachmentUpload";
import { DEFAULT_FOLLOW_UP_BEHAVIOR } from "../lib/followUpBehavior";
import { mobilePreferencesAtom } from "./preferences";
import { environmentThreadDetails } from "./threads";
import {
  endQueuedRunEdit,
  getQueuedRunEdit,
  queuedEditDraftKey,
  removeQueuedRunEditAttachment,
  resolveQueuedEditPayload,
  useQueuedRunEdit,
} from "./queued-run-edit";
import { setPendingConnectionError } from "../state/use-remote-environment-registry";
import {
  useSelectedThreadProjection,
  useSelectedThreadVisibleTurnItems,
} from "../state/use-thread-detail";
import { useThreadSelection } from "../state/use-thread-selection";
import { enqueueThreadOutboxMessage } from "./thread-outbox";
import { dispatchingQueuedMessageIdAtom, useThreadOutboxMessages } from "./use-thread-outbox";
import { threadEnvironment } from "./threads";
import { useAtomCommand } from "./use-atom-command";
import { environmentCatalog } from "../connection/catalog";
import { environmentSession, usePreparedConnection } from "./session";
import * as Option from "effect/Option";
import {
  createMobileImportedContinuationDelivery,
  canUseOrdinaryImportedContinuationDelivery,
  presentMobileImportedContinuation,
  type MobileImportedContinuationPorts,
} from "../features/threads/importedContinuationDelivery";

async function importedCommandValue<A, E>(result: Promise<AtomCommandResult<A, E>>): Promise<A> {
  const value = await result;
  if (value._tag !== "Success") throw squashAtomCommandFailure(value);
  return value.value;
}

const EMPTY_QUEUE_WORKFLOW_ATOM = Atom.make<null>(null).pipe(
  Atom.withLabel("mobile-thread-queue-workflow:empty"),
);

export function appendReviewCommentToDraft(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly text: string;
  readonly attachments?: ReadonlyArray<DraftComposerImageAttachment>;
}): void {
  const threadKey = scopedThreadKey(input.environmentId, input.threadId);
  const upgraded = upgradeLegacyContextMessage(input.text);
  if (
    !insertComposerDraftContext(
      threadKey,
      reidentifyComposerContext(upgraded.text, upgraded.records, uuidv4),
    )
  ) {
    Alert.alert("Too many context items", "Remove some context from the draft and try again.");
    return;
  }
  if (input.attachments && input.attachments.length > 0) {
    // Capped: a review comment is new content, not a send-failure restore, so
    // it must not push the draft over the send limit. Overflow is released.
    const rejectedCount = appendComposerDraftAttachments(threadKey, input.attachments, {
      appendReference: true,
    });
    if (rejectedCount > 0) {
      setPendingConnectionError(
        `${rejectedCount} comment attachment${rejectedCount === 1 ? " was" : "s were"} not added. Messages can contain at most ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} attachments.`,
      );
    }
  }
}

/**
 * Which draft the composer is editing right now. While a queued message is
 * being edited the composer is pointed at that edit's own draft, so typing,
 * attaching, and pasting never touch the user's draft for the thread.
 * Resolved per call rather than captured, so a callback created before the
 * edit began still writes to the right place.
 */
function activeComposerDraftKey(thread: {
  readonly environmentId: EnvironmentId;
  readonly id: ThreadId;
}): string {
  const threadKey = scopedThreadKey(thread.environmentId, thread.id);
  const edit = getQueuedRunEdit(threadKey);
  return edit === null ? threadKey : queuedEditDraftKey(threadKey, edit.runId);
}

export function useThreadDraftForThread(input: {
  readonly environmentId?: EnvironmentId;
  readonly threadId?: ThreadId;
}) {
  const threadKey =
    input.environmentId && input.threadId
      ? scopedThreadKey(input.environmentId, input.threadId)
      : null;
  const draft = useComposerDraft(threadKey);

  return {
    draftMessage: draft.text,
    draftAttachments: draft.attachments,
  };
}

export function useThreadComposerState() {
  const {
    selectedThread: selectedThreadShell,
    selectedThreadCreation,
    selectedEnvironmentRuntime,
  } = useThreadSelection();
  const selectedThreadProjection = useSelectedThreadProjection();
  const selectedThreadVisibleTurnItems = useSelectedThreadVisibleTurnItems();
  const composerDrafts = useAtomValue(composerDraftsAtom);
  const acknowledgedMessages = useAtomValue(acknowledgedThreadMessagesAtom);
  const queuedMessagesByThreadKey = useThreadOutboxMessages();
  const dispatchingQueuedMessageId = useAtomValue(dispatchingQueuedMessageIdAtom);
  const [feedbackSubmissionsByThreadKey, setFeedbackSubmissionsByThreadKey] = useState<
    Record<string, ReadonlyArray<CodexFeedbackSubmission>>
  >({});
  const uploadThreadFeedback = useAtomCommand(threadEnvironment.uploadFeedback, {
    reportFailure: false,
  });
  const editQueuedRun = useAtomCommand(threadEnvironment.editQueuedRun, {
    label: "edit queued message",
    reportFailure: false,
  });
  const prepareImported = useAtomCommand(threadEnvironment.prepareImportedContinuation, {
    reportFailure: false,
  });
  const reviewImported = useAtomCommand(threadEnvironment.reviewImportedHistoryStart, {
    reportFailure: false,
  });
  const deliverImported = useAtomCommand(threadEnvironment.deliverImportedContinuation, {
    reportFailure: false,
  });
  const observeImported = useAtomCommand(threadEnvironment.observeImportedHistoryStart, {
    reportFailure: false,
  });
  const [isSavingQueuedEdit, setIsSavingQueuedEdit] = useState(false);
  const savingQueuedEditRef = useRef(false);
  const pastedTextFileNamesRef = useRef<{ threadKey: string | null; names: Set<string> }>({
    threadKey: null,
    names: new Set(),
  });
  const reservePastedTextFileName = useCallback(
    (threadKey: string, existingNames: ReadonlyArray<string>) => {
      if (pastedTextFileNamesRef.current.threadKey !== threadKey) {
        pastedTextFileNamesRef.current = { threadKey, names: new Set() };
      }
      const names = pastedTextFileNamesRef.current.names;
      for (const name of existingNames) names.add(name);
      const nextName = nextPastedTextFileName([...names]);
      names.add(nextName);
      return nextName;
    },
    [],
  );

  useEffect(() => {
    ensureComposerDraftsLoaded();
  }, []);

  const selectedThreadKey = selectedThreadShell
    ? scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id)
    : null;
  // The creation entry is the thread itself (rendered as the first message),
  // not a follow-up waiting behind it.
  const selectedThreadQueuedMessages = useMemo(
    () =>
      selectedThreadKey
        ? (queuedMessagesByThreadKey[selectedThreadKey] ?? []).filter(
            (message) => message.creation === undefined,
          )
        : [],
    [queuedMessagesByThreadKey, selectedThreadKey],
  );
  const feedbackSubmissions = useMemo(
    () => (selectedThreadKey ? (feedbackSubmissionsByThreadKey[selectedThreadKey] ?? []) : []),
    [feedbackSubmissionsByThreadKey, selectedThreadKey],
  );
  const dismissFeedback = useCallback(
    (id: MessageId) => {
      if (!selectedThreadKey) return;
      setFeedbackSubmissionsByThreadKey((current) => ({
        ...current,
        [selectedThreadKey]: (current[selectedThreadKey] ?? []).filter((entry) => entry.id !== id),
      }));
    },
    [selectedThreadKey],
  );
  const selectedThreadMessages = selectedThreadProjection?.projection.messages;
  const selectedThreadAttempts = selectedThreadProjection?.projection.attempts;
  const selectedThreadNodes = selectedThreadProjection?.projection.nodes;
  // A thread whose creation has not delivered its turn yet: the prompt only
  // exists in the outbox, so it is appended to whatever the server has. The
  // detail is usually present but empty during a worktree checkout, so this
  // cannot be an either/or with the loaded messages.
  const pendingCreationMessage = selectedThreadCreation?.message ?? null;
  const selectedThreadFeed = useMemo(() => {
    const pendingCreation =
      pendingCreationMessage !== null &&
      !selectedThreadMessages?.some((message) => message.id === pendingCreationMessage.messageId)
        ? [pendingThreadCreationMessage(pendingCreationMessage)]
        : [];
    const feed = buildThreadFeed(selectedThreadVisibleTurnItems, {
      anchoredMessages: pendingCreation,
      attempts: selectedThreadAttempts,
      nodes: selectedThreadNodes,
    });
    const pendingAcknowledgments = acknowledgedMessages.filter(
      (message) =>
        scopedThreadKey(message.environmentId, message.threadId) === selectedThreadKey &&
        !selectedThreadQueuedMessages.some((queued) => queued.messageId === message.messageId),
    );
    if (pendingAcknowledgments.length === 0) return feed;
    return appendPendingThreadMessages(feed, feed, pendingAcknowledgments).map((entry) =>
      entry.pendingMessage ? { ...entry, acknowledged: true } : entry,
    );
  }, [
    selectedThreadMessages,
    selectedThreadAttempts,
    selectedThreadNodes,
    selectedThreadVisibleTurnItems,
    pendingCreationMessage,
    selectedThreadKey,
    selectedThreadQueuedMessages,
    acknowledgedMessages,
  ]);
  useEffect(() => {
    const echoedIds = new Set(selectedThreadMessages?.map((message) => message.id));
    if (acknowledgedMessages.some((message) => echoedIds.has(message.messageId))) {
      appAtomRegistry.set(
        acknowledgedThreadMessagesAtom,
        appAtomRegistry
          .get(acknowledgedThreadMessagesAtom)
          .filter((message) => !echoedIds.has(message.messageId)),
      );
    }
  }, [acknowledgedMessages, selectedThreadMessages]);

  const preferencesResult = useAtomValue(mobilePreferencesAtom);
  const followUpBehavior = AsyncResult.isSuccess(preferencesResult)
    ? (preferencesResult.value.followUpBehavior ?? DEFAULT_FOLLOW_UP_BEHAVIOR)
    : DEFAULT_FOLLOW_UP_BEHAVIOR;
  // Steering needs a live provider turn the adapter can interrupt; the queue
  // workflow already derives that from the session's capabilities.
  const queueWorkflow = useAtomValue(
    selectedThreadShell === null
      ? EMPTY_QUEUE_WORKFLOW_ATOM
      : environmentThreadDetails.queueWorkflowAtom({
          environmentId: selectedThreadShell.environmentId,
          threadId: selectedThreadShell.id,
        }),
  );
  const canSteerActiveTurn = queueWorkflow?.canPromoteToSteer === true;
  const queuedRunEdit = useQueuedRunEdit(selectedThreadKey);
  const composerDraftKey =
    selectedThreadKey === null
      ? null
      : queuedRunEdit === null
        ? selectedThreadKey
        : queuedEditDraftKey(selectedThreadKey, queuedRunEdit.runId);
  // Content follows the composer's current draft; the model and mode pickers
  // stay bound to the thread's own draft, which is what they write to.
  const editedDraft = composerDraftKey ? composerDrafts[composerDraftKey] : null;
  const selectedDraft = selectedThreadKey ? composerDrafts[selectedThreadKey] : null;
  const draftMessage = editedDraft?.text ?? "";
  const draftAttachments = editedDraft?.attachments ?? [];
  const selectedThreadQueueCount = selectedThreadQueuedMessages.length;
  const selectedThread = selectedThreadShell;
  const modelSelection = selectedDraft?.modelSelection ?? selectedThread?.modelSelection ?? null;
  const runtimeMode = selectedDraft?.runtimeMode ?? selectedThread?.runtimeMode ?? null;
  const selectedProvider = selectedEnvironmentRuntime?.serverConfig?.providers.find(
    (provider) => provider.instanceId === modelSelection?.instanceId,
  );
  const interactionMode = selectedThread
    ? resolveProviderInteractionMode(
        selectedProvider,
        selectedDraft?.interactionMode ?? selectedThread.interactionMode,
      )
    : null;

  const preparedConnection = usePreparedConnection(selectedThread?.environmentId ?? null);
  const selectedKeyRef = useRef(selectedThreadKey);
  selectedKeyRef.current = selectedThreadKey;
  const mountedRef = useRef(true);
  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);
  const readContinuationConnection = useCallback(() => {
    if (selectedThread === null) return null;
    const environmentId = selectedThread.environmentId;
    const prepared = appAtomRegistry.get(
      environmentSession.preparedConnectionValueAtom(environmentId),
    );
    const connection = appAtomRegistry.get(environmentCatalog.stateAtom(environmentId));
    const session = appAtomRegistry.get(environmentSession.sessionStateAtom(environmentId));
    return Option.isSome(prepared) &&
      AsyncResult.isSuccess(connection) &&
      connection.value.phase === "connected" &&
      AsyncResult.isSuccess(session) &&
      session.value.authenticated
      ? { prepared: prepared.value, generation: connection.value.generation }
      : null;
  }, [selectedThread]);
  const continuationPortsRef = useRef<(() => MobileImportedContinuationPorts) | null>(null);
  continuationPortsRef.current = () => {
    if (selectedThread === null || selectedThreadKey === null)
      throw new Error("No selected thread.");
    const environmentId = selectedThread.environmentId;
    const threadId = selectedThread.id;
    const targetKey = selectedThreadKey;
    const connection = readContinuationConnection();
    return {
      environmentId,
      threadId,
      review: (input) => importedCommandValue(reviewImported({ environmentId, input })),
      deliver: (input) => importedCommandValue(deliverImported({ environmentId, input })),
      observe: (input) => importedCommandValue(observeImported({ environmentId, input })),
      savePointer: (pointer) => saveComposerImportedContinuationPointer(targetKey, pointer),
      isCurrent: () => {
        const current = readContinuationConnection();
        return (
          mountedRef.current &&
          selectedKeyRef.current === targetKey &&
          connection !== null &&
          current?.prepared === connection.prepared &&
          current.generation === connection.generation
        );
      },
    };
  };
  const importedContinuation = useMemo(
    () =>
      selectedThreadKey === null
        ? null
        : createMobileImportedContinuationDelivery(() => continuationPortsRef.current!()),
    [selectedThreadKey],
  );
  const importedState = useSyncExternalStore(
    importedContinuation?.subscribe ?? (() => () => {}),
    importedContinuation?.getSnapshot ?? (() => null),
  );
  const preparingContinuationRef = useRef(false);
  const reviewedDraftRef = useRef<{ threadKey: string; signature: string } | null>(null);
  const reviewedConnectionRef = useRef<ReturnType<typeof readContinuationConnection>>(null);
  const continuationConnectionGeneration = readContinuationConnection()?.generation;
  const continuationDraftSignature = useCallback(() => {
    if (selectedThreadKey === null) return "";
    const draft = getComposerDraftSnapshot(selectedThreadKey);
    const { importedContinuation: _pointer, ...content } = draft;
    const edit = getQueuedRunEdit(selectedThreadKey);
    return JSON.stringify({
      content,
      queuedTarget: edit === null ? null : { runId: edit.runId, messageId: edit.messageId },
      modelSelection: selectedThread?.modelSelection,
      runtimeMode: selectedThread?.runtimeMode,
      interactionMode: selectedThread?.interactionMode,
    });
  }, [selectedThread, selectedThreadKey]);
  useEffect(() => {
    const reviewed = reviewedDraftRef.current;
    if (
      reviewed !== null &&
      (reviewed.threadKey !== selectedThreadKey ||
        reviewed.signature !== continuationDraftSignature())
    ) {
      importedContinuation?.invalidateReview();
      reviewedDraftRef.current = null;
    }
  }, [
    composerDrafts,
    queuedRunEdit,
    selectedThreadKey,
    continuationDraftSignature,
    importedContinuation,
    preparedConnection,
    selectedEnvironmentRuntime?.connectionState,
  ]);
  useEffect(() => {
    importedContinuation?.invalidateReview();
    reviewedDraftRef.current = null;
    reviewedConnectionRef.current = null;
  }, [importedContinuation, preparedConnection, continuationConnectionGeneration]);
  useEffect(() => {
    if (importedContinuation === null || selectedThreadKey === null) return;
    const pointer = selectedDraft?.importedContinuation;
    if (pointer !== undefined && importedContinuation.getSnapshot().pointer === null) {
      importedContinuation.restorePointer(pointer);
      void importedContinuation.observe();
    }
  }, [
    selectedDraft?.importedContinuation,
    selectedThreadKey,
    importedContinuation,
    preparedConnection,
    selectedEnvironmentRuntime?.connectionState,
  ]);
  useEffect(() => {
    if (
      importedContinuation === null ||
      selectedThreadKey === null ||
      importedState?.pointer == null
    )
      return;
    if (
      (importedState.receipt?.status === "pending" ||
        importedState.receipt?.status === "started") &&
      importedState.receipt.intentAccepted &&
      importedState.command?.delivery.type === "message" &&
      selectedKeyRef.current === selectedThreadKey &&
      reviewedDraftRef.current?.threadKey === selectedThreadKey &&
      reviewedDraftRef.current.signature === continuationDraftSignature()
    ) {
      const attachments = getComposerDraftSnapshot(selectedThreadKey).attachments;
      clearComposerDraftContent(selectedThreadKey, { deferAttachmentCleanup: true });
      scheduleUnusedComposerAttachmentCleanup(attachments);
      reviewedDraftRef.current = null;
    }
    if (importedState.receipt?.status !== "started" && importedState.receipt?.status !== "rejected")
      return;
    const pointer = importedState.pointer;
    if (clearComposerImportedContinuationPointer(selectedThreadKey, pointer)) {
      importedContinuation.retirePointer(pointer);
      void flushComposerDrafts().catch(() => {});
    }
  }, [importedContinuation, importedState, selectedThreadKey, continuationDraftSignature]);
  const importedContinuationPresentation =
    importedState === null ? null : presentMobileImportedContinuation(importedState);
  const onStartWithImportedHistory = useCallback(async () => {
    const previous = importedContinuation?.getSnapshot();
    if (previous?.saveFailedBeforeDelivery && previous.command !== null) {
      await importedContinuation?.start(previous.command.commandId);
      return;
    }
    const currentConnection = readContinuationConnection();
    const reviewedConnection = reviewedConnectionRef.current;
    if (
      importedContinuation === null ||
      selectedThreadKey === null ||
      reviewedDraftRef.current?.threadKey !== selectedThreadKey ||
      reviewedDraftRef.current.signature !== continuationDraftSignature() ||
      reviewedConnection === null ||
      currentConnection?.prepared !== reviewedConnection.prepared ||
      currentConnection.generation !== reviewedConnection.generation
    ) {
      importedContinuation?.invalidateReview();
      return;
    }
    const existing = importedContinuation.getSnapshot().command;
    await importedContinuation.start(existing?.commandId ?? CommandId.make(uuidv4()));
  }, [
    importedContinuation,
    selectedThreadKey,
    continuationDraftSignature,
    readContinuationConnection,
  ]);
  const onObserveImportedHistory = useCallback(async () => {
    await importedContinuation?.observe();
  }, [importedContinuation]);
  const reviewImportedDelivery = useCallback(
    async (input: Parameters<typeof prepareImported>[0]["input"]): Promise<boolean> => {
      if (importedContinuation === null || selectedThreadKey === null || selectedThread === null)
        return false;
      if (
        getComposerDraftSnapshot(selectedThreadKey).importedContinuation !== undefined ||
        presentMobileImportedContinuation(importedContinuation.getSnapshot()).blocksOrdinarySend ||
        preparingContinuationRef.current
      )
        return false;
      const targetKey = selectedThreadKey;
      const signature = continuationDraftSignature();
      const connection = readContinuationConnection();
      if (connection === null) return true;
      const stillCurrent = () => {
        const current = readContinuationConnection();
        return (
          mountedRef.current &&
          selectedKeyRef.current === targetKey &&
          continuationDraftSignature() === signature &&
          current?.prepared === connection.prepared &&
          current.generation === connection.generation
        );
      };
      preparingContinuationRef.current = true;
      try {
        const prepared = await importedCommandValue(
          prepareImported({ environmentId: selectedThread.environmentId, input }),
        );
        if (!stillCurrent()) return false;
        reviewedDraftRef.current = { threadKey: targetKey, signature };
        reviewedConnectionRef.current = connection;
        await importedContinuation.review(prepared);
        if (!stillCurrent()) return false;
        return canUseOrdinaryImportedContinuationDelivery(
          importedContinuation.getSnapshot(),
          getComposerDraftSnapshot(targetKey).importedContinuation,
        );
      } catch {
        return (
          stillCurrent() &&
          canUseOrdinaryImportedContinuationDelivery(
            importedContinuation.getSnapshot(),
            getComposerDraftSnapshot(targetKey).importedContinuation,
          )
        );
      } finally {
        preparingContinuationRef.current = false;
      }
    },
    [
      importedContinuation,
      selectedThreadKey,
      selectedThread,
      continuationDraftSignature,
      readContinuationConnection,
      prepareImported,
    ],
  );
  // Whether the model picker may leave this thread's provider. Derived here
  // because the projection already drives this hook; the composer only needs
  // the answer, not a subscription to every projection update.
  const canSwitchThreadProvider = useMemo(
    () =>
      threadAllowsProviderSwitch({
        thread: selectedThreadShell,
        projection: selectedThreadProjection?.projection,
      }),
    [selectedThreadProjection, selectedThreadShell],
  );
  const selectedThreadRuntime = useMemo(
    () =>
      selectedThreadProjection
        ? deriveThreadRuntime(selectedThreadProjection.projection)
        : (selectedThreadShell?.runtime ?? null),
    [selectedThreadProjection, selectedThreadShell?.runtime],
  );
  const selectedThreadActivityRun = useMemo(
    () =>
      selectedThreadProjection
        ? deriveThreadActivityRun(selectedThreadProjection.projection)
        : (selectedThreadShell?.latestRun ?? null),
    [selectedThreadProjection, selectedThreadShell?.latestRun],
  );

  const isCompacting = useMemo(() => {
    const queuedCompact = selectedThreadQueuedMessages.some(
      (message) =>
        message.messageId === dispatchingQueuedMessageId &&
        message.text.trim().toLowerCase() === "/compact" &&
        message.attachments.length === 0,
    );
    if (queuedCompact) return true;
    const activeRunId = selectedThreadRuntime?.activeRunId;
    if (!activeRunId || !threadRuntimeIsActive(selectedThreadRuntime)) return false;
    const compactMessage = selectedThreadVisibleTurnItems.findLast(
      ({ item }) =>
        item.runId === activeRunId &&
        item.type === "user_message" &&
        item.text.trim().toLowerCase() === "/compact" &&
        item.attachments.length === 0,
    );
    if (!compactMessage) return false;
    return !selectedThreadVisibleTurnItems.some(
      ({ item }) =>
        item.runId === activeRunId &&
        item.type === "compaction" &&
        (item.status === "completed" || item.status === "failed"),
    );
  }, [
    dispatchingQueuedMessageId,
    selectedThreadQueuedMessages,
    selectedThreadRuntime,
    selectedThreadVisibleTurnItems,
  ]);

  const runlessWorkStartedAt = useMemo(
    () =>
      selectedThreadProjection
        ? deriveRunlessWorkStartedAt(selectedThreadProjection.projection)
        : null,
    [selectedThreadProjection],
  );
  const activeWorkStartedAt = useMemo(() => {
    if (!selectedThreadShell) {
      return null;
    }
    return (
      resolveThreadWorkingStartedAt({
        latestRun: selectedThreadActivityRun,
        runtime: selectedThreadRuntime,
      }) ?? runlessWorkStartedAt
    );
  }, [selectedThreadActivityRun, runlessWorkStartedAt, selectedThreadRuntime, selectedThreadShell]);
  const runlessWorkActive = runlessWorkStartedAt !== null;

  const providerSubagentStatus = useMemo(
    () =>
      selectedThreadProjection
        ? deriveProviderSubagentStatus(selectedThreadProjection.projection)
        : null,
    [selectedThreadProjection],
  );

  // The run can start, or be cancelled from another client, while its message
  // is open in the composer. Leave edit mode rather than saving into a run the
  // server will refuse, and keep whatever was typed if there is room for it.
  const selectedThreadRuns = selectedThreadProjection?.projection.runs;
  const editedRunId = queuedRunEdit?.runId ?? null;
  useEffect(() => {
    if (selectedThreadKey === null || editedRunId === null || selectedThreadRuns === undefined) {
      return;
    }
    if (savingQueuedEditRef.current) return;
    const stillQueued = selectedThreadRuns.some(
      (run) => run.id === editedRunId && run.status === "queued",
    );
    if (stillQueued) return;
    const editDraftKey = queuedEditDraftKey(selectedThreadKey, editedRunId);
    const editDraft = getComposerDraftSnapshot(editDraftKey);
    const threadDraft = getComposerDraftSnapshot(selectedThreadKey);
    const keepable =
      editDraft.text.trim().length > 0 &&
      threadDraft.text.trim().length === 0 &&
      threadDraft.attachments.length === 0;
    if (keepable) {
      void mergeComposerDraftContent(selectedThreadKey, {
        text: editDraft.text,
        attachments: editDraft.attachments,
        ...(editDraft.context ? { context: editDraft.context } : {}),
      });
    }
    endQueuedRunEdit(selectedThreadKey, { deferAttachmentCleanup: keepable });
    setPendingConnectionError(
      keepable
        ? "That message already started. Your edit is back in the composer."
        : "That message already started, so the edit was discarded.",
    );
  }, [editedRunId, selectedThreadKey, selectedThreadRuns]);

  const activeThreadBusy = threadRuntimeIsActive(selectedThreadRuntime);
  const interruptibleRunId = threadRuntimeHasInterruptibleRun(selectedThreadRuntime)
    ? (selectedThreadRuntime?.activeRunId ?? null)
    : null;

  const cancelQueuedRunEdit = useCallback(() => {
    if (selectedThreadKey === null || savingQueuedEditRef.current) return;
    endQueuedRunEdit(selectedThreadKey);
  }, [selectedThreadKey]);

  const onRemoveQueuedEditAttachment = useCallback(
    (attachmentId: string) => {
      if (selectedThreadKey === null) return;
      removeQueuedRunEditAttachment(selectedThreadKey, attachmentId);
    },
    [selectedThreadKey],
  );

  const saveQueuedRunEdit = useCallback(async () => {
    const thread = selectedThreadShell;
    if (!thread || savingQueuedEditRef.current) return;
    const threadKey = scopedThreadKey(thread.environmentId, thread.id);
    const edit = getQueuedRunEdit(threadKey);
    if (edit === null) return;
    const draft = getComposerDraftSnapshot(queuedEditDraftKey(threadKey, edit.runId));
    const text = draft.text.trim();
    if (text.length === 0) {
      // The server rejects an empty queued message, attachments or not.
      Alert.alert("Add a message", "A queued message cannot be left empty.");
      return;
    }
    if (
      edit.existingAttachments.length + draft.attachments.length >
      PROVIDER_SEND_TURN_MAX_ATTACHMENTS
    ) {
      Alert.alert(
        "Too many attachments",
        `Remove attachments until there are at most ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS}.`,
      );
      return;
    }
    savingQueuedEditRef.current = true;
    setIsSavingQueuedEdit(true);
    try {
      const capabilities = selectedEnvironmentRuntime?.serverConfig?.environment.capabilities;
      const prepared = await prepareTurnAttachments({
        environmentId: thread.environmentId,
        attachments: draft.attachments,
        supportsImageUploads: capabilities?.attachmentUploads === true,
      });
      if (prepared.status !== "ready") return;
      const payload = resolveQueuedEditPayload({
        edit,
        draftContext: draft.context,
        draftAttachments: draft.attachments,
        uploaded: prepared.attachments,
      });
      const result = await editQueuedRun({
        environmentId: thread.environmentId,
        input: {
          threadId: thread.id,
          runId: edit.runId,
          text,
          edit: {
            messageId: edit.messageId,
            attachments: payload.attachments,
            ...(payload.context ? { context: payload.context } : {}),
          },
        },
      });
      if (result._tag !== "Success") {
        Alert.alert(
          "Could not save the queued message",
          "It may have already started. Your edit is still in the composer.",
        );
        return;
      }
      endQueuedRunEdit(threadKey, { deferAttachmentCleanup: true });
      scheduleUnusedComposerAttachmentCleanup(draft.attachments);
    } finally {
      savingQueuedEditRef.current = false;
      setIsSavingQueuedEdit(false);
    }
  }, [editQueuedRun, selectedEnvironmentRuntime?.serverConfig, selectedThreadShell]);

  const onSendMessage = useCallback(
    async (followUpOverride?: ActiveTurnComposerAction) => {
      if (!selectedThreadShell) {
        return null;
      }
      // The server has not created this thread yet. Queuing a follow-up against
      // its id would strand the message: if the creation is rejected the thread
      // never appears and the drain drops the orphan. The composer disables its
      // send button too; this guard also covers the editor's submit key.
      if (selectedThreadCreation !== null) {
        return null;
      }
      const operationKey = scopedThreadKey(
        selectedThreadShell.environmentId,
        selectedThreadShell.id,
      );
      try {
        await waitForComposerDraftsLoaded();
      } catch {
        setPendingConnectionError(
          "Could not load the saved draft. Check the existing request before sending again.",
        );
        return null;
      }
      if (selectedKeyRef.current !== operationKey) return null;
      if (
        getComposerDraftSnapshot(operationKey).importedContinuation !== undefined ||
        (importedContinuation !== null &&
          presentMobileImportedContinuation(importedContinuation.getSnapshot()).blocksOrdinarySend)
      ) {
        return null;
      }

      // An ordinary queued send saves the edit in place. An eligible imported
      // run first waits for the separate choice to start its saved message.
      const editKey = scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id);
      const queuedEdit = getQueuedRunEdit(editKey);
      if (queuedEdit !== null) {
        if (
          !(await reviewImportedDelivery({
            threadId: selectedThreadShell.id,
            delivery: {
              type: "queued_run",
              runId: queuedEdit.runId,
              messageId: queuedEdit.messageId,
            },
          }))
        )
          return null;
        await saveQueuedRunEdit();
        return null;
      }

      const threadKey = scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id);
      const draft = getComposerDraftSnapshot(threadKey);
      if (appAtomRegistry.get(composerContextImportsAtom)[threadKey]) return null;
      const thread = selectedThreadShell;
      const text = draft.text.trim();
      let attachments = draft.attachments;
      if (
        composerAttachmentUploadBlockReason({
          environmentId: selectedThreadShell.environmentId,
          attachments,
          connected: selectedEnvironmentRuntime?.connectionState === "connected",
          serverConfig: selectedEnvironmentRuntime?.serverConfig ?? null,
          states: appAtomRegistry.get(composerAttachmentUploadsAtom),
        }) !== null
      )
        return null;
      if (text.length === 0 && attachments.length === 0) {
        return null;
      }
      // A send-failure restore appends with allowOverflow so it never drops the
      // user's files, which can leave the draft over the cap. Sending it anyway
      // would enqueue a message that outbox recovery rejects forever, so block
      // here until the user removes attachments.
      if (attachments.length > PROVIDER_SEND_TURN_MAX_ATTACHMENTS) {
        Alert.alert(
          "Too many attachments",
          `Remove attachments until there are at most ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS}.`,
        );
        return null;
      }

      const contextBlockReason = composerContextSendBlockReason(draft.context);
      if (contextBlockReason) {
        Alert.alert("Too much context", contextBlockReason);
        return null;
      }

      const modelSelection = draft.modelSelection ?? thread.modelSelection;
      const serverConfig = selectedEnvironmentRuntime?.serverConfig;
      if (
        selectedEnvironmentRuntime?.connectionState === "connected" &&
        isModelSelectionUnavailable(serverConfig, modelSelection)
      ) {
        Alert.alert(
          "Antigravity model unavailable",
          "Set up Antigravity on web or desktop, or choose another model.",
        );
        return null;
      }
      const provider = serverConfig?.providers.find(
        (entry) => entry.instanceId === modelSelection.instanceId,
      );
      const feedbackCommand =
        attachments.length === 0 && provider?.driver === "codex"
          ? parseCodexFeedbackCommand(text)
          : null;
      if (feedbackCommand) {
        if (thread.activeProviderThreadId === null) {
          Alert.alert("Start a Codex thread first", "Send a message before you submit feedback.");
          return null;
        }
        const metadata = makeQueuedMessageMetadata();
        await submitCodexFeedback({
          submission: {
            id: MessageId.make(metadata.messageId),
            command: text,
            createdAt: metadata.createdAt,
          },
          clearDraft: () => clearComposerDraftContent(threadKey),
          onUpdate: (submission) => {
            setFeedbackSubmissionsByThreadKey((current) => {
              const existing = current[threadKey] ?? [];
              const found = existing.some((entry) => entry.id === submission.id);
              return {
                ...current,
                [threadKey]: found
                  ? existing.map((entry) => (entry.id === submission.id ? submission : entry))
                  : [...existing, submission],
              };
            });
          },
          upload: () =>
            uploadThreadFeedback({
              environmentId: thread.environmentId,
              input: { threadId: thread.id, ...feedbackCommand },
            }),
        });
        return null;
      }

      // Resolved here rather than at drain time: the outbox can deliver minutes
      // later, and the choice belongs to the moment the user pressed send.
      // Steering travels as "auto" so a turn that ends in the meantime degrades
      // to a queued run on the server instead of failing the delivery and
      // bouncing the message back into the draft.
      const followUpAction = resolveComposerDispatchMode({
        running: activeThreadBusy && canSteerActiveTurn,
        alternateModifier: followUpOverride !== undefined && followUpOverride !== followUpBehavior,
        activeTurnDefault: followUpBehavior,
      });
      const followUpDispatchMode =
        followUpAction === "auto" ? null : followUpAction === "queue" ? "queue" : "auto";

      const metadata = makeQueuedMessageMetadata();
      const messageId = MessageId.make(metadata.messageId);
      const sendConnection = readContinuationConnection();
      if (sendConnection !== null) {
        const signature = continuationDraftSignature();
        const prepared = await prepareTurnAttachments({
          environmentId: thread.environmentId,
          attachments,
          supportsImageUploads: serverConfig?.environment.capabilities.attachmentUploads === true,
          persistUploadedReferences: async (uploadedDrafts) => {
            if (selectedKeyRef.current !== threadKey || continuationDraftSignature() !== signature)
              return "abandon";
            replaceComposerDraftAttachments(threadKey, uploadedDrafts);
            await flushComposerDrafts();
            return "persisted";
          },
        });
        const currentConnection = readContinuationConnection();
        if (
          prepared.status !== "ready" ||
          selectedKeyRef.current !== threadKey ||
          currentConnection?.prepared !== sendConnection.prepared ||
          currentConnection.generation !== sendConnection.generation
        )
          return null;
        const currentDraft = getComposerDraftSnapshot(threadKey);
        if (
          currentDraft.text !== draft.text ||
          JSON.stringify(currentDraft.context) !== JSON.stringify(draft.context) ||
          currentDraft.modelSelection !== draft.modelSelection ||
          currentDraft.runtimeMode !== draft.runtimeMode ||
          currentDraft.interactionMode !== draft.interactionMode ||
          JSON.stringify(currentDraft.attachments) !== JSON.stringify(prepared.draftAttachments)
        )
          return null;
        if (
          !(await reviewImportedDelivery({
            threadId: thread.id,
            delivery: {
              type: "message",
              messageId,
              text,
              attachments: prepared.attachments,
              ...(draft.context === undefined
                ? {}
                : {
                    context: uploadedComposerContext(
                      draft.context,
                      attachments,
                      prepared.attachments,
                    ),
                  }),
              modelSelection,
              runtimeMode: draft.runtimeMode ?? thread.runtimeMode,
              interactionMode: resolveProviderInteractionMode(
                provider,
                draft.interactionMode ?? thread.interactionMode,
              ),
              dispatchMode: { type: "start_immediately" },
            },
          }))
        )
          return null;
        attachments = prepared.draftAttachments;
      }
      // Enqueue publishes the queued atom synchronously (the durable write
      // happens behind it), so clearing the draft here gives send feedback on
      // the tap frame instead of after file I/O. If the write fails the message
      // is rolled out of the queue and the content is merged back into the
      // draft, preserving anything typed since.
      const enqueuePromise = enqueueThreadOutboxMessage({
        environmentId: selectedThreadShell.environmentId,
        threadId: selectedThreadShell.id,
        messageId,
        commandId: CommandId.make(metadata.commandId),
        text,
        attachments,
        context: draft.context,
        modelSelection,
        runtimeMode: draft.runtimeMode ?? thread.runtimeMode,
        interactionMode: resolveProviderInteractionMode(
          provider,
          draft.interactionMode ?? thread.interactionMode,
        ),
        ...(followUpDispatchMode === null ? {} : { dispatchMode: followUpDispatchMode }),
        createdAt: metadata.createdAt,
      });
      clearComposerDraftContent(threadKey, { deferAttachmentCleanup: true });
      enqueuePromise.then(
        () => scheduleUnusedComposerAttachmentCleanup(attachments),
        (error: unknown) => {
          // Restore text via merge (idempotent) but attachments via the uncapped
          // append: the merge path slots existing attachments first and truncates
          // at the send limit, which would silently drop this message's images if
          // the user attached new ones while the write was in flight.
          void mergeComposerDraftContent(threadKey, {
            text,
            context: draft.context,
            attachments: [],
          });
          appendComposerDraftAttachments(threadKey, attachments, { allowOverflow: true });
          setPendingConnectionError(
            error instanceof Error ? error.message : "Failed to save the queued message.",
          );
        },
      );
      return messageId;
    },
    [
      activeThreadBusy,
      canSteerActiveTurn,
      followUpBehavior,
      saveQueuedRunEdit,
      selectedEnvironmentRuntime?.connectionState,
      selectedEnvironmentRuntime?.serverConfig,
      selectedThreadCreation,
      selectedThreadShell,
      uploadThreadFeedback,
      importedContinuation,
      reviewImportedDelivery,
      readContinuationConnection,
      continuationDraftSignature,
    ],
  );

  const onChangeDraftMessage = useCallback(
    (value: string) => {
      if (!selectedThreadShell) {
        return;
      }

      const threadKey = activeComposerDraftKey(selectedThreadShell);
      setComposerDraftText(threadKey, value);
    },
    [selectedThreadShell],
  );

  const onPickDraftMedia = useCallback(async () => {
    if (!selectedThreadShell) {
      return;
    }

    const threadKey = activeComposerDraftKey(selectedThreadShell);
    const insertion = captureComposerDraftInsertion(threadKey);
    const capabilities = selectedEnvironmentRuntime?.serverConfig?.environment.capabilities;
    const result = await pickComposerMedia({
      existingCount: countComposerDraftAttachmentsAfterSelection(threadKey, insertion),
      maxVideoBytes:
        capabilities?.attachmentUploads === true
          ? capabilities.fileAttachments?.maxUploadBytes
          : undefined,
    });
    const rejectedCount = appendComposerDraftAttachments(threadKey, result.attachments, {
      appendReference: true,
      insertion,
    });
    const problems = [
      ...(result.error ? [result.error] : []),
      ...(rejectedCount > 0
        ? [`You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} attachments per message.`]
        : []),
    ];
    if (problems.length > 0) {
      Alert.alert("Could not attach photo or video", problems.join("\n\n"));
    }
  }, [composerDrafts, selectedEnvironmentRuntime?.serverConfig, selectedThreadShell]);

  const onPickDraftFiles = useCallback(async () => {
    if (!selectedThreadShell) {
      return;
    }
    const maxBytes =
      selectedEnvironmentRuntime?.serverConfig?.environment.capabilities.fileAttachments
        ?.maxUploadBytes;
    if (maxBytes === undefined) {
      Alert.alert("Could not attach file", "This server does not support file attachments.");
      return;
    }

    const threadKey = activeComposerDraftKey(selectedThreadShell);
    const insertion = captureComposerDraftInsertion(threadKey);
    // pickComposerFiles clamps the advertised limit to the contract maximum.
    const result = await pickComposerFiles({
      existingCount: countComposerDraftAttachmentsAfterSelection(threadKey, insertion),
      maxBytes,
    });
    const rejectedCount = appendComposerDraftAttachments(threadKey, result.files, {
      appendReference: true,
      insertion,
    });
    // The picker error and the live-cap rejection can both happen in one
    // pick; report both in a single alert.
    const problems = [
      ...(result.error ? [result.error] : []),
      ...(rejectedCount > 0
        ? [`You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} files per message.`]
        : []),
    ];
    if (problems.length > 0) {
      Alert.alert("Could not attach file", problems.join("\n\n"));
    }
  }, [composerDrafts, selectedEnvironmentRuntime?.serverConfig, selectedThreadShell]);

  const onPasteIntoDraft = useCallback(async () => {
    if (!selectedThreadShell) {
      return;
    }

    const threadKey = activeComposerDraftKey(selectedThreadShell);
    const insertion = captureComposerDraftInsertion(threadKey);
    const result = await pasteComposerClipboard({
      existingCount: countComposerDraftAttachmentsAfterSelection(threadKey, insertion),
    });
    const rejectedPasteCount = appendComposerDraftAttachments(threadKey, result.images, {
      appendReference: true,
      insertion,
    });
    if (result.text) {
      const currentDraft = getComposerDraftSnapshot(threadKey);
      const currentAttachments = currentDraft.attachments;
      const capabilities = selectedEnvironmentRuntime?.serverConfig?.environment.capabilities;
      const advertisedMax =
        capabilities?.attachmentUploads === true
          ? capabilities.fileAttachments?.maxUploadBytes
          : undefined;
      const maxBytes =
        advertisedMax === undefined ? null : clampFileAttachmentUploadBytes(advertisedMax);
      const wouldExceedInputLimit =
        currentDraft.text.length -
          (currentDraft.text === insertion.text
            ? Math.max(0, insertion.end - insertion.start)
            : 0) +
          result.text.length >
        PROVIDER_SEND_TURN_MAX_INPUT_CHARS;
      const shouldFold =
        pastedTextDisposition({
          text: result.text,
          wouldExceedInputLimit,
          canAttach: true,
        }) === "attachment";
      const canAttach =
        maxBytes !== null &&
        countComposerDraftAttachmentsAfterSelection(threadKey, insertion) <
          PROVIDER_SEND_TURN_MAX_ATTACHMENTS &&
        new TextEncoder().encode(result.text).byteLength <= maxBytes;
      if (shouldFold && canAttach && maxBytes !== null) {
        try {
          const attachment = await createPastedTextComposerAttachment({
            text: result.text,
            name: reservePastedTextFileName(
              threadKey,
              currentAttachments.map((item) => item.name),
            ),
            maxBytes,
          });
          // Same reference the pasted images above get: a folded paste is only visible
          // as its chip until the message is sent.
          if (
            appendComposerDraftAttachments(threadKey, [attachment], {
              appendReference: true,
              insertion,
            }) > 0
          ) {
            await removePersistedComposerAttachmentFile(attachment.fileUri);
            setPendingConnectionError(
              `You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} files per message.`,
            );
          }
        } catch (error) {
          setPendingConnectionError(
            error instanceof Error ? error.message : "Could not attach pasted text.",
          );
        }
      } else if (shouldFold && !wouldExceedInputLimit) {
        insertComposerDraftText(threadKey, result.text, insertion);
      } else if (shouldFold) {
        setPendingConnectionError(
          wouldExceedInputLimit
            ? "Pasted text is too large for this message. Remove some text or an attachment, then paste again."
            : "Could not attach pasted text. Remove an attachment or use a smaller paste, then try again.",
        );
      } else {
        insertComposerDraftText(threadKey, result.text, insertion);
      }
    }
    if (result.error) {
      setPendingConnectionError(result.error);
    } else if (rejectedPasteCount > 0) {
      setPendingConnectionError(
        `You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} files per message.`,
      );
    }
  }, [
    composerDrafts,
    reservePastedTextFileName,
    selectedEnvironmentRuntime?.serverConfig,
    selectedThreadShell,
  ]);

  const onNativePasteImages = useCallback(
    async (uris: ReadonlyArray<string>) => {
      if (!selectedThreadShell || uris.length === 0) {
        return;
      }

      const threadKey = activeComposerDraftKey(selectedThreadShell);
      const insertion = captureComposerDraftInsertion(threadKey);
      try {
        const images = await convertPastedImagesToAttachments({
          uris,
          existingCount: countComposerDraftAttachmentsAfterSelection(threadKey, insertion),
        });
        if (images.length > 0) {
          appendComposerDraftAttachments(threadKey, images, { appendReference: true, insertion });
        }
      } catch (error) {
        console.error("[native paste] error converting images", {
          environmentId: selectedThreadShell.environmentId,
          threadId: selectedThreadShell.id,
          uriCount: uris.length,
          ...safeErrorLogAttributes(error),
        });
      }
    },
    [composerDrafts, selectedThreadShell],
  );

  const onNativePasteText = useCallback(
    async (paste: ComposerTextPaste) => {
      if (!selectedThreadShell) return;
      const capabilities = selectedEnvironmentRuntime?.serverConfig?.environment.capabilities;
      const advertisedMax =
        capabilities?.attachmentUploads === true
          ? capabilities.fileAttachments?.maxUploadBytes
          : undefined;
      if (advertisedMax === undefined) return;

      const threadKey = activeComposerDraftKey(selectedThreadShell);
      const insertion = { text: paste.value, ...paste.selection };
      const currentAttachments = getComposerDraftSnapshot(threadKey).attachments;
      try {
        const attachment = await createPastedTextComposerAttachment({
          text: paste.text,
          name: reservePastedTextFileName(
            threadKey,
            currentAttachments.map((item) => item.name),
          ),
          maxBytes: clampFileAttachmentUploadBytes(advertisedMax),
        });
        // The chip is how a folded paste stays visible: without it the attachment is in the
        // draft but nothing in the composer says so until the message is sent. Web folds
        // through its ordinary attach path, which always writes a reference; match that.
        const rejectedCount = appendComposerDraftAttachments(threadKey, [attachment], {
          appendReference: true,
          insertion,
        });
        if (rejectedCount > 0) {
          await removePersistedComposerAttachmentFile(attachment.fileUri);
          setPendingConnectionError(
            `You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} files per message.`,
          );
        }
      } catch (error) {
        setPendingConnectionError(
          error instanceof Error ? error.message : "Could not attach pasted text.",
        );
      }
    },
    [
      composerDrafts,
      reservePastedTextFileName,
      selectedEnvironmentRuntime?.serverConfig,
      selectedThreadShell,
    ],
  );

  const onRemoveDraftImage = useCallback(
    (imageId: string) => {
      if (!selectedThreadShell) {
        return;
      }

      const threadKey = activeComposerDraftKey(selectedThreadShell);
      removeComposerDraftAttachment(threadKey, imageId);
    },
    [selectedThreadShell],
  );

  const onUpdateModelSelection = useCallback(
    (value: ModelSelection) => {
      if (!selectedThreadKey) {
        return;
      }
      const provider = selectedEnvironmentRuntime?.serverConfig?.providers.find(
        (candidate) => candidate.instanceId === value.instanceId,
      );
      updateComposerDraftSettings(selectedThreadKey, {
        modelSelection: value,
        ...(provider?.showInteractionModeToggle === false
          ? { interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE }
          : {}),
      });
    },
    [selectedEnvironmentRuntime?.serverConfig, selectedThreadKey],
  );

  const onUpdateRuntimeMode = useCallback(
    (value: RuntimeMode) => {
      if (!selectedThreadKey) {
        return;
      }
      updateComposerDraftSettings(selectedThreadKey, { runtimeMode: value });
    },
    [selectedThreadKey],
  );

  const onUpdateInteractionMode = useCallback(
    (value: ProviderInteractionMode) => {
      if (!selectedThreadKey) {
        return;
      }
      const modelSelection =
        getComposerDraftSnapshot(selectedThreadKey).modelSelection ??
        selectedThread?.modelSelection;
      const provider = selectedEnvironmentRuntime?.serverConfig?.providers.find(
        (candidate) => candidate.instanceId === modelSelection?.instanceId,
      );
      updateComposerDraftSettings(selectedThreadKey, {
        interactionMode: resolveProviderInteractionMode(provider, value),
      });
    },
    [selectedEnvironmentRuntime?.serverConfig, selectedThread?.modelSelection, selectedThreadKey],
  );

  return {
    feedbackSubmissions,
    dismissFeedback,
    selectedThreadFeed,
    selectedThreadActivityRun,
    selectedThreadQueueCount,
    selectedThreadQueuedMessages,
    dispatchingQueuedMessageId,
    activeWorkStartedAt,
    runlessWorkActive,
    providerSubagentStatus,
    isCompacting,
    draftMessage,
    draftAttachments,
    composerDraftKey,
    followUpBehavior,
    canSteerActiveTurn,
    queuedRunEdit,
    isSavingQueuedEdit,
    cancelQueuedRunEdit,
    onRemoveQueuedEditAttachment,
    modelSelection,
    canSwitchThreadProvider,
    runtimeMode,
    interactionMode,
    activeThreadBusy,
    interruptibleRunId,
    onChangeDraftMessage,
    onPickDraftMedia,
    onPickDraftFiles,
    onPasteIntoDraft,
    onNativePasteImages,
    onNativePasteText,
    onRemoveDraftImage,
    onSendMessage,
    importedContinuationPresentation,
    onStartWithImportedHistory,
    onObserveImportedHistory,
    onUpdateModelSelection,
    onUpdateRuntimeMode,
    onUpdateInteractionMode,
  };
}
