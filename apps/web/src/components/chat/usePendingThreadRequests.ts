import { projectPendingUserInputAnswer as projectJonesAnswer } from "../../jones/questionAnswer/threadReferenceAnswer";
import { DisplacedAnswerLedger } from "../../jones/questionAnswer/displacedAnswer";
import { derivePendingUserInputProgress } from "../../jones/questionAnswer/answerProgress";
import { supportsUserResponseAnswerNotes } from "@t3tools/contracts/jones/userResponseRequest";
import type { ThreadStructure } from "@t3tools/client-runtime/state/shell";
import {
  AuthOrchestrationOperateScope,
  type EnvironmentId,
  type ProviderApprovalDecision,
  type RuntimeRequestId,
  type ScopedThreadRef,
  type ThreadId,
  type UserInputAttachments,
} from "@t3tools/contracts";
import {
  derivePendingThreadRequests,
  seedUserInputDraftAnswers,
  userInputDraftKey,
} from "@t3tools/client-runtime/state/thread-requests";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { useRef, useState, type SetStateAction } from "react";
import { useShallow } from "zustand/react/shallow";
import {
  type ComposerThreadDraftState,
  type DraftId,
  useComposerDraftStore,
} from "../../composerDraftStore";
import { useStableCallback } from "../../hooks/useStableCallback";
import {
  getUploadedAttachments,
  refreshExpiredAttachmentUploads,
  useAttachmentUploadStore,
} from "../../lib/attachmentUploadQueue";
import { composerDraftContentChanged } from "./composerSend.logic";
import {
  buildPendingUserInputAnswers,
  setPendingUserInputCustomAnswer,
  togglePendingUserInputOptionSelection,
  type PendingUserInputDraftAnswer,
} from "../../jones/questionAnswer/pendingAnswers";
import {
  carryDisplacedCustomAnswerIntoPrompt,
  answerContextRecords,
  projectPendingUserInputAnswer,
  threadContextsReferencedByAnswer,
} from "../../pendingUserInput";
import {
  questionAttachmentDraftId,
  usePendingUserInputAnswers,
  usePendingUserInputQuestionIndex,
  useQuestionAttachmentPreparation,
} from "../../questionAttachments";
import {
  derivePendingApprovals,
  derivePendingUserInputs,
  type PendingUserInput,
} from "../../session-logic";
import { readEnvironmentScope } from "../../state/session";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";
import { toastManager } from "../ui/toast";

const EMPTY_PENDING_USER_INPUT_ANSWERS: Record<string, PendingUserInputDraftAnswer> = {};
const NO_PENDING_REQUESTS = { approvals: [], userInputs: [] } as const;
const NO_PENDING_APPROVALS: ReturnType<typeof derivePendingApprovals> = [];
const NO_PENDING_USER_INPUTS: ReturnType<typeof derivePendingUserInputs> = [];

interface PendingUserInputDraftScope {
  readonly environmentId: EnvironmentId;
  readonly activeThreadId: ThreadId | null;
  readonly activePendingUserInput: PendingUserInput | null;
  readonly supportsQuestionAttachments: boolean;
}

function pendingUserInputDraftKeys(scope: PendingUserInputDraftScope) {
  const { environmentId, activeThreadId, activePendingUserInput } = scope;
  return {
    requestKey: userInputDraftKey(environmentId, activeThreadId, activePendingUserInput?.requestId),
    questionKeys:
      activeThreadId && activePendingUserInput
        ? activePendingUserInput.questions.map((question) =>
            questionAttachmentDraftId(
              environmentId,
              activeThreadId,
              activePendingUserInput.requestId,
              question.id,
            ),
          )
        : [],
  };
}

const selectQuestionComposerDrafts =
  (keys: ReadonlyArray<string>) =>
  (state: { readonly draftsByThreadKey: Record<string, ComposerThreadDraftState> }) =>
    Object.fromEntries(keys.map((key) => [key, state.draftsByThreadKey[key]]));

const selectQuestionUploadsBlocked =
  (
    keys: ReadonlyArray<string>,
    questionComposerDrafts: Record<string, ComposerThreadDraftState | undefined>,
    environmentId: EnvironmentId,
  ) =>
  (state: ReturnType<typeof useAttachmentUploadStore.getState>) =>
    Object.fromEntries(
      keys.map((key) => {
        const draft = questionComposerDrafts[key];
        const attachments = draft ? [...draft.images, ...draft.files] : [];
        return [
          key,
          attachments.some((attachment) => {
            const upload = state.uploadsByImageId[attachment.id];
            return upload?.status !== "ready" || upload.environmentId !== environmentId;
          }),
        ];
      }),
    );

const selectQuestionPreparations =
  (keys: ReadonlyArray<string>) => (state: { readonly counts: Record<string, number> }) =>
    Object.fromEntries(keys.map((key) => [key, state.counts[key] ?? 0]));

function deriveActivePendingDraft(
  scope: PendingUserInputDraftScope,
  storedPendingAnswerDrafts: Record<string, PendingUserInputDraftAnswer>,
  activePendingQuestionIndex: number,
  questionComposerDrafts: Record<string, ComposerThreadDraftState | undefined>,
  questionUploadsBlocked: Record<string, boolean>,
  questionPreparations: Record<string, number>,
) {
  const { environmentId, activeThreadId, activePendingUserInput, supportsQuestionAttachments } =
    scope;
  // Suggested answers are shown until the user edits them; only edits are stored.
  const activePendingAnswerDrafts: Record<string, PendingUserInputDraftAnswer> =
    activePendingUserInput
      ? seedUserInputDraftAnswers(activePendingUserInput.questions, storedPendingAnswerDrafts)
      : storedPendingAnswerDrafts;
  const activePendingDraftAnswers =
    !activePendingUserInput || !activeThreadId
      ? EMPTY_PENDING_USER_INPUT_ANSWERS
      : Object.fromEntries(
          activePendingUserInput.questions.map((question) => {
            const key = questionAttachmentDraftId(
              environmentId,
              activeThreadId,
              activePendingUserInput.requestId,
              question.id,
            );
            const draft = questionComposerDrafts[key];
            const attachments = draft ? [...draft.images, ...draft.files] : [];
            return [
              question.id,
              {
                ...activePendingAnswerDrafts[question.id],
                attachmentCount: attachments.length,
                attachmentsBlocked:
                  (attachments.length > 0 && !supportsQuestionAttachments) ||
                  (questionPreparations[key] ?? 0) > 0 ||
                  questionUploadsBlocked[key] === true,
              },
            ];
          }),
        );
  const activePendingProgress = activePendingUserInput
    ? derivePendingUserInputProgress(
        activePendingUserInput.questions,
        activePendingDraftAnswers,
        activePendingQuestionIndex,
        { notes: supportsUserResponseAnswerNotes(activePendingUserInput.responseCapability) },
      )
    : null;
  const activePendingResolvedAnswers = activePendingUserInput
    ? buildPendingUserInputAnswers(activePendingUserInput.questions, activePendingDraftAnswers, {
        notes: supportsUserResponseAnswerNotes(activePendingUserInput.responseCapability),
      })
    : null;
  return {
    activePendingDraftAnswers,
    activePendingQuestionIndex,
    activePendingProgress,
    activePendingResolvedAnswers,
  };
}

/**
 * The open question's answers, position and progress. These change on every keystroke, so
 * the composer subscribes to them rather than the chat view.
 */
export function usePendingUserInputDraft(scope: PendingUserInputDraftScope) {
  const { requestKey, questionKeys: keys } = pendingUserInputDraftKeys(scope);
  const storedPendingAnswerDrafts = usePendingUserInputAnswers(
    (state) => state[requestKey] ?? EMPTY_PENDING_USER_INPUT_ANSWERS,
  );
  const activePendingQuestionIndex = usePendingUserInputQuestionIndex(
    (state) => state[requestKey] ?? 0,
  );
  const questionComposerDrafts = useComposerDraftStore(
    useShallow(selectQuestionComposerDrafts(keys)),
  );
  const questionUploadsBlocked = useAttachmentUploadStore(
    useShallow(selectQuestionUploadsBlocked(keys, questionComposerDrafts, scope.environmentId)),
  );
  const questionPreparations = useQuestionAttachmentPreparation(
    useShallow(selectQuestionPreparations(keys)),
  );
  return deriveActivePendingDraft(
    scope,
    storedPendingAnswerDrafts,
    activePendingQuestionIndex,
    questionComposerDrafts,
    questionUploadsBlocked,
    questionPreparations,
  );
}

// Request callbacks read drafts when called, so the chat view never subscribes to them.
function readPendingUserInputDraft(scope: PendingUserInputDraftScope) {
  const { requestKey, questionKeys: keys } = pendingUserInputDraftKeys(scope);
  const questionComposerDrafts = selectQuestionComposerDrafts(keys)(
    useComposerDraftStore.getState(),
  );
  return deriveActivePendingDraft(
    scope,
    usePendingUserInputAnswers.getState()[requestKey] ?? EMPTY_PENDING_USER_INPUT_ANSWERS,
    usePendingUserInputQuestionIndex.getState()[requestKey] ?? 0,
    questionComposerDrafts,
    selectQuestionUploadsBlocked(
      keys,
      questionComposerDrafts,
      scope.environmentId,
    )(useAttachmentUploadStore.getState()),
    selectQuestionPreparations(keys)(useQuestionAttachmentPreparation.getState()),
  );
}

function makePendingCustomAnswerSetter(input: {
  readonly activePendingUserInput: PendingUserInput | null;
  readonly activePendingRequestKey: string;
  readonly withdrawRestoredAnswer: (questionId: string, value: string) => void;
  readonly setPendingUserInputAnswersByRequestId: (
    update: SetStateAction<Record<string, Record<string, PendingUserInputDraftAnswer>>>,
  ) => void;
}) {
  return (questionId: string, value: string) => {
    const { activePendingUserInput, activePendingRequestKey } = input;
    if (!activePendingUserInput) {
      return;
    }
    const question = activePendingUserInput.questions.find((entry) => entry.id === questionId);
    if (!question || question.allowCustomAnswer === false) {
      return;
    }
    input.withdrawRestoredAnswer(questionId, value);
    input.setPendingUserInputAnswersByRequestId((existing) => ({
      ...existing,
      [activePendingRequestKey]: {
        ...existing[activePendingRequestKey],
        [questionId]: setPendingUserInputCustomAnswer(
          question,
          existing[activePendingRequestKey]?.[questionId],
          value,
          { notes: supportsUserResponseAnswerNotes(activePendingUserInput.responseCapability) },
        ),
      },
    }));
    // Resetting selection before rendering can re-enter Lexical with stale skill-token offsets.
  };
}

export function usePendingThreadRequests(input: {
  readonly environmentId: EnvironmentId;
  readonly activeThreadId: ThreadId | null;
  readonly serverProjection: ThreadStructure | null;
  readonly supportsQuestionAttachments: boolean;
  readonly composerDraftTarget: ScopedThreadRef | DraftId;
  readonly setComposerDraftPrompt: (target: ScopedThreadRef | DraftId, prompt: string) => void;
  readonly setThreadError: (threadId: ThreadId, error: string | null) => void;
}) {
  const {
    environmentId,
    activeThreadId,
    serverProjection,
    supportsQuestionAttachments,
    composerDraftTarget,
    setComposerDraftPrompt,
    setThreadError,
  } = input;
  const respondToThreadApproval = useOrchestrationCommand(threadEnvironment.respondToApproval, {
    reportFailure: false,
  });
  const respondToThreadUserInput = useOrchestrationCommand(threadEnvironment.respondToUserInput, {
    reportFailure: false,
  });
  const dismissThreadUserInput = useAtomCommand(threadEnvironment.dismissUserInput, {
    reportFailure: false,
  });
  const userInputResponsesInFlight = useRef(new Set<string>());
  // In the order the text went into the thread draft, which is how a copy is found again.
  const displacedAnswers = useRef(new DisplacedAnswerLedger());
  // This hook outlives a thread switch, and each thread has its own draft.
  const displacedDraftKey =
    typeof composerDraftTarget === "string"
      ? composerDraftTarget
      : `${composerDraftTarget.environmentId}:${composerDraftTarget.threadId}`;
  const [respondingRequestIds, setRespondingRequestIds] = useState<RuntimeRequestId[]>([]);
  const [respondingUserInputRequestIds, setRespondingUserInputRequestIds] = useState<
    RuntimeRequestId[]
  >([]);
  const setPendingUserInputAnswersByRequestId = usePendingUserInputAnswers.setState;
  const setPendingUserInputQuestionIndexByRequestId = usePendingUserInputQuestionIndex.setState;
  const pendingRequests =
    serverProjection !== null && serverProjection.thread.id === activeThreadId
      ? derivePendingThreadRequests(serverProjection)
      : NO_PENDING_REQUESTS;
  // The projection changes on every streamed event, so shared empties keep the
  // composer and approval panel from re-rendering while nothing is pending.
  const pendingApprovals =
    pendingRequests.approvals.length === 0
      ? NO_PENDING_APPROVALS
      : derivePendingApprovals(pendingRequests.approvals);
  const pendingUserInputs =
    pendingRequests.userInputs.length === 0
      ? NO_PENDING_USER_INPUTS
      : derivePendingUserInputs(pendingRequests.userInputs);
  const activePendingUserInput = pendingUserInputs[0] ?? null;
  const activePendingRequestKey = userInputDraftKey(
    environmentId,
    activeThreadId,
    activePendingUserInput?.requestId,
  );
  const draftScope = {
    environmentId,
    activeThreadId,
    activePendingUserInput,
    supportsQuestionAttachments,
  };
  const activePendingIsResponding = activePendingUserInput
    ? activePendingUserInput.responseCapability === "not_resumable" ||
      respondingUserInputRequestIds.includes(activePendingUserInput.requestId)
    : false;
  const activePendingApproval = pendingApprovals[0] ?? null;

  const onRespondToApproval = async (
    requestId: RuntimeRequestId,
    decision: ProviderApprovalDecision,
    feedback?: string,
  ) => {
    if (!activeThreadId || !readEnvironmentScope(environmentId, AuthOrchestrationOperateScope))
      return;
    if (
      pendingApprovals.find((approval) => approval.requestId === requestId)?.responseCapability !==
      "live"
    )
      return;

    setRespondingRequestIds((existing) =>
      existing.includes(requestId) ? existing : [...existing, requestId],
    );
    const result = await respondToThreadApproval({
      environmentId,
      input: {
        threadId: activeThreadId,
        requestId,
        decision,
        ...(feedback === undefined ? {} : { feedback }),
      },
    });
    if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
      const error = squashAtomCommandFailure(result);
      setThreadError(
        activeThreadId,
        error instanceof Error ? error.message : "Failed to submit approval decision.",
      );
    }
    setRespondingRequestIds((existing) => existing.filter((id) => id !== requestId));
    return result;
  };
  const onRespondToApprovalStable = useStableCallback(onRespondToApproval);

  const onRespondToUserInput = async (
    requestId: RuntimeRequestId,
    answers: Record<string, unknown>,
  ) => {
    if (!activeThreadId || !readEnvironmentScope(environmentId, AuthOrchestrationOperateScope))
      return;
    const pendingInput = pendingUserInputs.find((input) => input.requestId === requestId);
    if (!pendingInput || pendingInput.responseCapability === "not_resumable") return;
    const responseKey = JSON.stringify([environmentId, activeThreadId, requestId]);
    if (userInputResponsesInFlight.current.has(responseKey)) return;
    userInputResponsesInFlight.current.add(responseKey);
    const answerDraftKey = userInputDraftKey(environmentId, activeThreadId, requestId);
    const answerDraftBeforePreparation = usePendingUserInputAnswers.getState()[answerDraftKey];
    setRespondingUserInputRequestIds((existing) =>
      existing.includes(requestId) ? existing : [...existing, requestId],
    );
    const prepareAndRespond = async () => {
      const attachmentsByQuestionId = new Map<string, UserInputAttachments[string]>();
      const providerAnswers = { ...answers };
      const threadDraft = useComposerDraftStore.getState().getComposerDraft(composerDraftTarget);
      const questionDrafts = pendingInput.questions.map((question) => {
        const target = questionAttachmentDraftId(
          environmentId,
          activeThreadId,
          requestId,
          question.id,
        );
        const draft = useComposerDraftStore.getState().getComposerDraft(target);
        return { question, target, draft };
      });
      for (const { question, target, draft } of questionDrafts) {
        if ((useQuestionAttachmentPreparation.getState().counts[target] ?? 0) > 0) return;
        providerAnswers[question.id] = projectPendingUserInputAnswer(
          projectJonesAnswer(answers[question.id], draft?.threadContexts ?? [], environmentId),
          answerContextRecords(draft, threadDraft),
        );
        const attachments = draft ? [...draft.images, ...draft.files] : [];
        if (attachments.length === 0) continue;
        await refreshExpiredAttachmentUploads({
          environmentId,
          images: attachments,
          draftTarget: target,
        });
        if (
          composerDraftContentChanged(
            draft,
            useComposerDraftStore.getState().getComposerDraft(target),
          )
        ) {
          setThreadError(
            activeThreadId,
            "Answer changed while preparing attachments. Review it and send again.",
          );
          return;
        }
        const uploaded = getUploadedAttachments({ environmentId, images: attachments });
        if (!uploaded) {
          setThreadError(
            activeThreadId,
            "Wait for attachments to finish uploading, or remove failed uploads.",
          );
          return;
        }
        attachmentsByQuestionId.set(question.id, uploaded as UserInputAttachments[string]);
      }
      if (
        usePendingUserInputAnswers.getState()[answerDraftKey] !== answerDraftBeforePreparation ||
        questionDrafts.some(
          ({ target, draft }) =>
            (useQuestionAttachmentPreparation.getState().counts[target] ?? 0) > 0 ||
            composerDraftContentChanged(
              draft,
              useComposerDraftStore.getState().getComposerDraft(target),
            ),
        )
      ) {
        setThreadError(
          activeThreadId,
          "Answer changed while preparing attachments. Review it and send again.",
        );
        return;
      }
      const result = await respondToThreadUserInput({
        environmentId,
        input: {
          threadId: activeThreadId,
          requestId,
          answers: providerAnswers,
          ...(attachmentsByQuestionId.size > 0
            ? { attachmentsByQuestionId: Object.fromEntries(attachmentsByQuestionId) }
            : {}),
        },
      });
      if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
        const error = squashAtomCommandFailure(result);
        setThreadError(
          activeThreadId,
          error instanceof Error ? error.message : "Failed to submit user input.",
        );
      }
      return result;
    };
    return prepareAndRespond().finally(() => {
      userInputResponsesInFlight.current.delete(responseKey);
      setRespondingUserInputRequestIds((existing) => existing.filter((id) => id !== requestId));
    });
  };

  const onDismissUserInput = async (requestId: RuntimeRequestId) => {
    if (!activeThreadId) return;

    setRespondingUserInputRequestIds((existing) =>
      existing.includes(requestId) ? existing : [...existing, requestId],
    );
    const result = await dismissThreadUserInput({
      environmentId,
      input: { threadId: activeThreadId, requestId },
    });
    if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
      const error = squashAtomCommandFailure(result);
      setThreadError(
        activeThreadId,
        error instanceof Error ? error.message : "Failed to dismiss the question.",
      );
    }
    setRespondingUserInputRequestIds((existing) => existing.filter((id) => id !== requestId));
    return result;
  };
  const onDismissUserInputStable = useStableCallback(onDismissUserInput);

  const setActivePendingUserInputQuestionIndex = (nextQuestionIndex: number) => {
    if (!activePendingUserInput) {
      return;
    }
    setPendingUserInputQuestionIndexByRequestId((existing) => ({
      ...existing,
      [activePendingRequestKey]: Math.max(
        0,
        Math.min(nextQuestionIndex, activePendingUserInput.questions.length - 1),
      ),
    }));
  };

  const onSelectActivePendingUserInputOption = (questionId: string, optionValue: string) => {
    if (!activePendingUserInput || activePendingIsResponding) return;
    const question = activePendingUserInput.questions.find((entry) => entry.id === questionId);
    if (
      !question ||
      !question.options.some((option) => (option.value ?? option.label) === optionValue)
    )
      return;
    const fresh = readPendingUserInputDraft(draftScope);
    const currentAnswers = fresh.activePendingDraftAnswers;
    const notes =
      supportsUserResponseAnswerNotes(activePendingUserInput.responseCapability) &&
      !question.multiSelect;
    const displacedAnswer = notes ? undefined : currentAnswers[questionId]?.customAnswer;
    const currentPrompt =
      useComposerDraftStore.getState().getComposerDraft(composerDraftTarget)?.prompt ?? "";
    const nextPrompt = carryDisplacedCustomAnswerIntoPrompt(currentPrompt, displacedAnswer);
    if (nextPrompt !== currentPrompt) {
      setComposerDraftPrompt(composerDraftTarget, nextPrompt);
      displacedAnswers.current.record(
        displacedDraftKey,
        `${activePendingRequestKey}:${questionId}`,
        displacedAnswer ?? "",
        currentPrompt,
        nextPrompt,
      );
      toastManager.add({
        type: "info",
        title: "Your typed text was set aside",
        description: "It will be back in the composer once the question is answered.",
        data: { threadRef: activeThreadId ? { environmentId, threadId: activeThreadId } : null },
      });
      if (activeThreadId) {
        const store = useComposerDraftStore.getState();
        const questionTarget = questionAttachmentDraftId(
          environmentId,
          activeThreadId,
          activePendingUserInput.requestId,
          questionId,
        );
        store.addThreadContexts(
          composerDraftTarget,
          threadContextsReferencedByAnswer(
            displacedAnswer ?? "",
            store.getComposerDraft(questionTarget)?.threadContexts ?? [],
          ),
          { appendReference: false },
        );
      }
    }
    const nextAnswer = togglePendingUserInputOptionSelection(
      question,
      question.multiSelect
        ? currentAnswers[questionId]
        : { ...currentAnswers[questionId], selectedOptionValues: [] },
      optionValue,
      { notes },
    );
    const drafts = { ...currentAnswers, [questionId]: nextAnswer };
    setPendingUserInputAnswersByRequestId((existing) => ({
      ...existing,
      [activePendingRequestKey]: drafts,
    }));
    if (question.multiSelect) return;
    const answers = buildPendingUserInputAnswers(activePendingUserInput.questions, drafts, {
      notes,
    });
    if (answers) void onRespondToUserInput(activePendingUserInput.requestId, answers);
    else
      setActivePendingUserInputQuestionIndex(
        derivePendingUserInputProgress(
          activePendingUserInput.questions,
          drafts,
          fresh.activePendingQuestionIndex,
          { notes },
        ).firstUnansweredIndex,
      );
  };
  const onSelectActivePendingUserInputOptionStable = useStableCallback(
    onSelectActivePendingUserInputOption,
  );

  // Undo can put set-aside text back into the answer; left in the draft too, it is sent twice.
  const withdrawRestoredAnswer = useStableCallback((questionId: string, value: string) => {
    const key = `${activePendingRequestKey}:${questionId}`;
    const currentPrompt =
      useComposerDraftStore.getState().getComposerDraft(composerDraftTarget)?.prompt ?? "";
    const nextPrompt = displacedAnswers.current.withdraw(
      displacedDraftKey,
      key,
      value,
      currentPrompt,
    );
    if (nextPrompt !== currentPrompt) setComposerDraftPrompt(composerDraftTarget, nextPrompt);
  });
  const changeAnswer = makePendingCustomAnswerSetter({
    activePendingUserInput,
    activePendingRequestKey,
    withdrawRestoredAnswer,
    setPendingUserInputAnswersByRequestId,
  });

  const onChangeActivePendingUserInputCustomAnswer = useStableCallback(
    (questionId: string, value: string) => {
      if (
        !activePendingUserInput ||
        activePendingIsResponding ||
        userInputResponsesInFlight.current.has(
          JSON.stringify([environmentId, activeThreadId, activePendingUserInput.requestId]),
        )
      )
        return;
      changeAnswer(questionId, value);
    },
  );

  const onAdvanceActivePendingUserInput = () => {
    if (!activePendingUserInput || activePendingUserInput.responseCapability === "not_resumable") {
      return;
    }
    const { activePendingProgress, activePendingResolvedAnswers } =
      readPendingUserInputDraft(draftScope);
    if (!activePendingProgress) return;
    if (activePendingProgress.isLastQuestion) {
      if (activePendingResolvedAnswers) {
        void onRespondToUserInput(activePendingUserInput.requestId, activePendingResolvedAnswers);
      }
      return;
    }
    if (!activePendingProgress.canAdvance) return;
    setActivePendingUserInputQuestionIndex(activePendingProgress.questionIndex + 1);
  };
  const onAdvanceActivePendingUserInputStable = useStableCallback(onAdvanceActivePendingUserInput);

  const onPreviousActivePendingUserInputQuestion = () => {
    const { activePendingProgress } = readPendingUserInputDraft(draftScope);
    if (!activePendingProgress) {
      return;
    }
    setActivePendingUserInputQuestionIndex(Math.max(activePendingProgress.questionIndex - 1, 0));
  };
  const onPreviousActivePendingUserInputQuestionStable = useStableCallback(
    onPreviousActivePendingUserInputQuestion,
  );

  return {
    pendingApprovals,
    pendingUserInputs,
    activePendingApproval,
    activePendingUserInput,
    activePendingIsResponding,
    respondingRequestIds: [...respondingRequestIds, ...respondingUserInputRequestIds],
    onRespondToApproval: onRespondToApprovalStable,
    onDismissUserInput: onDismissUserInputStable,
    onSelectActivePendingUserInputQuestion: setActivePendingUserInputQuestionIndex,
    onSelectActivePendingUserInputOption: onSelectActivePendingUserInputOptionStable,
    onChangeActivePendingUserInputCustomAnswer,
    onAdvanceActivePendingUserInput,
    onAdvanceActivePendingUserInputStable,
    onPreviousActivePendingUserInputQuestion: onPreviousActivePendingUserInputQuestionStable,
  };
}
