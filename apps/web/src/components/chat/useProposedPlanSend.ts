import { threadEnvironment } from "../../state/threads";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";
import { pendingNewThread } from "../../jones/pendingSends/newThread";
import { pendingSends } from "../../jones/pendingSends/store";
import { usePendingTurn } from "../../jones/pendingSends/usePendingTurn";
import { createPendingPromptRecovery } from "../../jones/pendingSends/composerRecovery";
import { planCreation } from "../../jones/pendingSends/planCreation";
import {
  decodePlanCreationOperation,
  type PlanCreationSlot,
} from "@t3tools/client-runtime/jones/planCreationOperation";
import {
  acknowledgeComposerSelection,
  composerSelectionForDispatch,
} from "../../jones/threadConfiguration/composerSelection";
import {
  AuthOrchestrationOperateScope,
  CommandId,
  type EnvironmentId,
  type ProviderInteractionMode,
  type RuntimeMode,
} from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  isAtomCommandInterrupted,
  settlePromise,
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentProject,
  EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/shell";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import { truncate } from "@t3tools/shared/String";
import { useNavigate } from "@tanstack/react-router";
import type { Dispatch, SetStateAction } from "react";
import { newMessageId, newThreadId, randomUUID } from "~/lib/utils";
import {
  useComposerDraftStore,
  type ComposerThreadDraftState,
  type ComposerThreadTarget,
} from "../../composerDraftStore";
import type { ComposerHandleRef } from "../../composerHandleContext";
import type { buildMessageContext } from "../../lib/composerContextRecords";
import {
  buildPlanImplementationPrompt,
  buildPlanImplementationThreadTitle,
} from "../../proposedPlan";
import type { LatestProposedPlanState } from "../../session-logic";
import { readEnvironmentScope } from "../../state/session";
import type { ChatMessage } from "../../types";
import { stackedThreadToast, toastManager } from "../ui/toast";
import {
  formatOutgoingPrompt,
  type makeThreadErrorSetter,
  waitForStartedServerThread,
} from "../ChatView.logic";
import { draftSendState } from "./chatViewSend";
import { dispatchPlanFollowUp, planFollowUpSource } from "../../jones/threadMonitor/planFollowUp";
import type { useLocalDispatchState } from "./useLocalDispatchState";
import type { useTimelineFollow } from "./useTimelineFollow";

type LocalDispatchState = ReturnType<typeof useLocalDispatchState>;

interface PlanSendBase {
  readonly environmentId: EnvironmentId;
  readonly routeThreadKey: string;
  readonly activeThread: EnvironmentThreadShell | undefined;
  readonly activeThreadBranch: string | null;
  readonly activeProposedPlan: LatestProposedPlanState | null;
  readonly isServerThread: boolean;
  readonly isSendBusy: boolean;
  readonly composerRef: ComposerHandleRef;
  readonly composerDraftTarget: ComposerThreadTarget;
  readonly beginLocalDispatch: LocalDispatchState["beginLocalDispatch"];
  readonly resetLocalDispatch: LocalDispatchState["resetLocalDispatch"];
}

interface PlanFollowUpSendInput extends PlanSendBase {
  readonly runtimeMode: RuntimeMode;
  readonly localCheckoutBranchMismatch: { readonly currentBranch: string } | null;
  readonly setOptimisticUserMessages: Dispatch<SetStateAction<ChatMessage[]>>;
  readonly setThreadError: ReturnType<typeof makeThreadErrorSetter>;
  readonly anchorNewTurn: ReturnType<typeof useTimelineFollow>["anchorNewTurn"];
  readonly persistThreadSettingsForNextTurn: (input: {
    threadId: EnvironmentThreadShell["id"];
    createdAt: string;
    branch?: string;
    runtimeMode: RuntimeMode;
    interactionMode: ProviderInteractionMode;
  }) => Promise<AtomCommandResult<void, unknown>>;
}

/**
 * Answers a proposed plan on this thread: refine it in plan mode, or implement it here
 * (which links the turn to the plan). Resolves whether the turn started.
 */
export function usePlanFollowUpSend(input: PlanFollowUpSendInput) {
  const {
    environmentId,
    routeThreadKey,
    activeThread,
    activeProposedPlan,
    isServerThread,
    isSendBusy,
    composerRef,
    composerDraftTarget,
    beginLocalDispatch,
    resetLocalDispatch,
    runtimeMode,
    localCheckoutBranchMismatch,
    setOptimisticUserMessages,
    setThreadError,
    anchorNewTurn,
    persistThreadSettingsForNextTurn,
  } = input;
  const startThreadTurn = usePendingTurn();
  const setComposerDraftInteractionMode = useComposerDraftStore(
    (store) => store.setInteractionMode,
  );
  const { sendInFlight: sendInFlightRef } = draftSendState(routeThreadKey);

  return async function submitPlanFollowUp({
    text,
    context,
    interactionMode: nextInteractionMode,
    onAccepted,
    onRejected,
  }: {
    text: string;
    context?: ReturnType<typeof buildMessageContext>;
    interactionMode: "default" | "plan";
    onAccepted?: () => void;
    onRejected?: () => void;
  }) {
    if (
      !readEnvironmentScope(environmentId, AuthOrchestrationOperateScope) ||
      !activeThread ||
      !isServerThread ||
      isSendBusy ||
      sendInFlightRef.current
    ) {
      return false;
    }

    const trimmed = text.trim();
    if (!trimmed) {
      return false;
    }

    const sendCtx = composerRef.current?.getSendContext();
    if (!sendCtx?.providerAvailable || !sendCtx.interactionModeEnabled) {
      return false;
    }

    const threadIdForSend = activeThread.id;
    const submittedDraft = useComposerDraftStore.getState().getComposerDraft(composerDraftTarget);
    const selection = composerSelectionForDispatch(sendCtx.selectedModelSelection, submittedDraft);
    const messageIdForSend = newMessageId();
    const messageCreatedAt = new Date().toISOString();
    const outgoingMessageText = formatOutgoingPrompt({
      provider: sendCtx.selectedProvider,
      model: sendCtx.selectedModel,
      models: sendCtx.selectedProviderModels,
      effort: sendCtx.selectedPromptEffort,
      text: trimmed,
    });

    sendInFlightRef.current = true;
    beginLocalDispatch({ preparingWorktree: false });
    setThreadError(threadIdForSend, null);

    anchorNewTurn(
      scopedThreadKey(scopeThreadRef(activeThread.environmentId, threadIdForSend)),
      messageIdForSend,
    );

    setOptimisticUserMessages((existing) => [
      ...existing,
      {
        id: messageIdForSend,
        role: "user",
        text: outgoingMessageText,
        ...(context ? { context } : {}),
        runId: null,
        createdAt: messageCreatedAt,
        updatedAt: messageCreatedAt,
        streaming: false,
      },
    ]);

    let accepted = false;
    pendingSends.whenAccepted(environmentId, threadIdForSend, messageIdForSend, () => {
      accepted = true;
      acknowledgeComposerSelection(composerDraftTarget, submittedDraft);
      onAccepted?.();
    });
    pendingSends.whenRejected(environmentId, threadIdForSend, messageIdForSend, () =>
      onRejected?.(),
    );
    const followUpResult = await dispatchPlanFollowUp({
      persistSettings: () =>
        persistThreadSettingsForNextTurn({
          threadId: threadIdForSend,
          createdAt: messageCreatedAt,
          ...(localCheckoutBranchMismatch
            ? { branch: localCheckoutBranchMismatch.currentBranch }
            : {}),
          runtimeMode,
          interactionMode: nextInteractionMode,
        }),
      onSettingsPersisted: () =>
        setComposerDraftInteractionMode(
          scopeThreadRef(activeThread.environmentId, threadIdForSend),
          nextInteractionMode,
        ),
      startTurn: () =>
        startThreadTurn({
          environmentId,
          input: {
            threadId: threadIdForSend,
            message: {
              messageId: messageIdForSend,
              role: "user",
              text: outgoingMessageText,
              ...(context ? { context } : {}),
              attachments: [],
            },
            ...selection,
            titleSeed: activeThread.title,
            runtimeMode,
            interactionMode: nextInteractionMode,
            ...planFollowUpSource(nextInteractionMode, activeThread.id, activeProposedPlan?.id),
            createdAt: messageCreatedAt,
          },
        }),
    });
    const failure = followUpResult._tag === "Failure" ? followUpResult : null;

    if (failure === null || accepted) {
      sendInFlightRef.current = false;
      resetLocalDispatch();
      return true;
    }

    pendingSends.failBeforeDispatch(environmentId, threadIdForSend, messageIdForSend);
    if (!isAtomCommandInterrupted(failure)) {
      const error = squashAtomCommandFailure(failure);
      setThreadError(
        threadIdForSend,
        error instanceof Error ? error.message : "Failed to send plan follow-up.",
      );
    }
    sendInFlightRef.current = false;
    resetLocalDispatch();
    return pendingSends.get(environmentId, threadIdForSend, messageIdForSend)?.delivery ===
      "uncertain"
      ? null
      : false;
  };
}

interface ImplementPlanInNewThreadInput extends PlanSendBase {
  readonly activeProject: EnvironmentProject | null;
  readonly activeEnvironmentUnavailable: boolean;
  readonly defaultRuntimeMode: RuntimeMode;
}

/**
 * Starts a fresh thread that implements the active proposed plan, then opens it.
 * Navigation failure leaves the accepted thread intact.
 */
export function useImplementPlanInNewThread(input: ImplementPlanInNewThreadInput) {
  const {
    environmentId,
    routeThreadKey,
    activeThread,
    activeThreadBranch,
    activeProposedPlan,
    isServerThread,
    isSendBusy,
    composerRef,
    composerDraftTarget,
    beginLocalDispatch,
    resetLocalDispatch,
    activeProject,
    activeEnvironmentUnavailable,
    defaultRuntimeMode,
  } = input;
  const navigate = useNavigate();
  const createThread = useOrchestrationCommand(threadEnvironment.create, { reportFailure: false });
  const prepare = useOrchestrationCommand(threadEnvironment.prepareTurn, { reportFailure: false });
  const dispatch = useOrchestrationCommand(threadEnvironment.dispatchPreparedTurn, {
    reportFailure: false,
  });
  const { sendInFlight: sendInFlightRef } = draftSendState(routeThreadKey);

  return async (retainedSlot?: PlanCreationSlot) => {
    const sourcePlanId = retainedSlot?.sourcePlanId ?? activeProposedPlan?.id;
    if (
      !readEnvironmentScope(environmentId, AuthOrchestrationOperateScope) ||
      !activeThread ||
      !sourcePlanId ||
      (retainedSlot &&
        (retainedSlot.environmentId !== environmentId ||
          retainedSlot.sourceThreadId !== activeThread.id)) ||
      !isServerThread ||
      isSendBusy ||
      activeEnvironmentUnavailable ||
      sendInFlightRef.current
    ) {
      return;
    }

    sendInFlightRef.current = true;
    beginLocalDispatch({ preparingWorktree: false });
    let submittedDraft: ComposerThreadDraftState | null | undefined;
    return (async () => {
      const result = await planCreation.run({
        slot: {
          environmentId,
          sourceThreadId: activeThread.id,
          sourcePlanId,
          action: "implement-in-new-thread",
        },
        // A retained action reaches neither the current picker nor mutable defaults.
        allocate: async () => {
          // A recovery control can only resume its retained slot, including after plan completion.
          if (retainedSlot || !activeProject || !activeProposedPlan)
            throw new Error("The retained plan action needs reconciliation.");
          const sendCtx = composerRef.current?.getSendContext();
          if (!sendCtx?.providerAvailable || !sendCtx.interactionModeEnabled)
            throw new Error("Choose an available provider before starting this plan.");
          submittedDraft = useComposerDraftStore.getState().getComposerDraft(composerDraftTarget);
          const selection = composerSelectionForDispatch(
            sendCtx.selectedModelSelection,
            submittedDraft,
          );
          const createdAt = new Date().toISOString();
          const nextThreadId = newThreadId();
          const messageId = newMessageId();
          const prompt = formatOutgoingPrompt({
            provider: sendCtx.selectedProvider,
            model: sendCtx.selectedModel,
            models: sendCtx.selectedProviderModels,
            effort: sendCtx.selectedPromptEffort,
            text: buildPlanImplementationPrompt(activeProposedPlan.planMarkdown),
          });
          const create = {
            commandId: CommandId.make(randomUUID()),
            threadId: nextThreadId,
            projectId: activeProject.id,
            title: truncate(buildPlanImplementationThreadTitle(activeProposedPlan.planMarkdown)),
            modelSelection: sendCtx.selectedModelSelection,
            runtimeMode: defaultRuntimeMode,
            interactionMode: "default" as const,
            branch: activeThreadBranch,
            worktreePath: activeThread.worktreePath,
            createdAt,
          };
          const prepared = await prepare({
            environmentId,
            input: {
              commandId: CommandId.make(randomUUID()),
              threadId: nextThreadId,
              message: { messageId, role: "user", text: prompt, attachments: [] },
              ...selection,
              runtimeMode: defaultRuntimeMode,
              interactionMode: "default",
              dispatchMode: "start",
              sourceProposedPlan: { threadId: activeThread.id, planId: activeProposedPlan.id },
              createdAt,
            },
          });
          if (prepared._tag === "Failure") throw squashAtomCommandFailure(prepared);
          return decodePlanCreationOperation({
            schemaVersion: 1,
            environmentId,
            sourceThreadId: activeThread.id,
            sourcePlanId: activeProposedPlan.id,
            action: "implement-in-new-thread",
            phase: "create-pending",
            create,
            prepared: prepared.value,
          });
        },
        create: (input) => createThread({ environmentId, input }),
        dispatch: (input) => dispatch({ environmentId, input }),
        admitted: (operation) => {
          const request = operation.prepared;
          if (
            request.method !== "orchestration.dispatchCommand" ||
            request.payload.type !== "message.dispatch"
          )
            throw new Error("Saved plan message needs reconciliation.");
          const target = scopeThreadRef(environmentId, operation.create.threadId);
          const recovery = createPendingPromptRecovery(target, request.payload.text);
          pendingSends.attachThread(environmentId, target.threadId, {
            ...pendingNewThread(
              activeThread,
              target.threadId,
              operation.create.title,
              operation.create.modelSelection,
              operation.create.createdAt,
            ),
            runtimeMode: operation.create.runtimeMode,
            interactionMode: operation.create.interactionMode,
            branch: operation.create.branch,
            worktreePath: operation.create.worktreePath,
          });
          const capturedDraft = submittedDraft;
          return {
            accepted: () => {
              recovery.accepted();
              // A reload has no reference witness for the original picker interaction.
              if (capturedDraft !== undefined)
                acknowledgeComposerSelection(composerDraftTarget, capturedDraft);
            },
            messageRejected: recovery.rejected,
          };
        },
      });
      const accepted = result.status === "accepted";
      const target = result.operation?.create.threadId;
      if (target && (accepted || result.status === "message-rejected")) {
        const opened = await settlePromise(async () => {
          if (accepted) await waitForStartedServerThread(scopeThreadRef(environmentId, target));
          await navigate({
            to: "/$environmentId/$threadId",
            params: { environmentId, threadId: target },
          });
        });
        if (opened._tag === "Success" && accepted) return;
        if (opened._tag === "Failure") {
          toastManager.add(
            stackedThreadToast({
              type: "error",
              title: accepted
                ? "Thread started; could not open it"
                : "Thread created; could not open it",
              description:
                "The saved plan action still links to this thread. Open it from the sidebar or try this action again.",
            }),
          );
          return;
        }
      }
      if (!accepted)
        toastManager.add(
          stackedThreadToast({
            type: "error",
            title:
              result.status === "create-rejected"
                ? "Could not create thread"
                : result.status === "message-rejected"
                  ? "Thread created; could not send the plan"
                  : result.status === "held"
                    ? "Plan action could not be saved or reconciled"
                    : "Thread delivery needs confirmation",
            description:
              result.status === "create-rejected"
                ? "The server rejected creation. You can edit the selection and try again."
                : result.status === "message-rejected"
                  ? "The implementation prompt is kept on the created thread."
                  : "The plan is kept. Retry this action to reconcile the same saved request.",
          }),
        );
    })().finally(() => {
      sendInFlightRef.current = false;
      resetLocalDispatch();
    });
  };
}
