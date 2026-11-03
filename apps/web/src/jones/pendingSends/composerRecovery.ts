import { useComposerDraftStore, type ComposerThreadDraftState, type ComposerThreadTarget } from "../../composerDraftStore";
import {
  clearSubmittedComposer,
  restoreFailedComposerSend,
} from "../../components/chat/composerSendRecovery";

// A launch may promote its draft while the first message is still unconfirmed.
export function createPendingComposerRecovery(
  readOwner: () => Parameters<typeof restoreFailedComposerSend>[0] | null,
) {
  let restored: { draft: ComposerThreadDraftState | null } | undefined;
  return {
    rejected() {
      if (restored) return;
      const owner = readOwner();
      if (owner && restoreFailedComposerSend(owner)) {
        restored = { draft: useComposerDraftStore.getState().getComposerDraft(owner.composerDraftTarget) };
      }
    },
    accepted() {
      const owner = readOwner();
      if (!restored || !owner) return;
      const cleared = clearSubmittedComposer({
        ...owner,
        expectedDraft: restored.draft,
        resetCursor: () => owner.resetCursor({ cursor: 0, prompt: "", detectTrigger: false }),
      });
      if (
        cleared &&
        !owner.backgroundDraftOpened &&
        owner.currentRouteThreadKeyRef.current === owner.routeThreadKey
      ) {
        owner.composerImagesRef.current = [];
        owner.composerFilesRef.current = [];
        owner.composerTerminalContextsRef.current = [];
      }
      restored = undefined;
    },
  };
}

export function createPendingPromptRecovery(target: ComposerThreadTarget, prompt: string) {
  const expectedDraft = useComposerDraftStore.getState().getComposerDraft(target);
  return createPendingComposerRecovery(() => ({
    routeThreadKey: "pending-plan",
    currentRouteThreadKeyRef: { current: null },
    composerDraftTarget: target,
    promptRef: { current: "" },
    expectedDraft,
    isCurrentSend: () => true,
    backgroundDraftOpened: true,
    composerImagesRef: { current: [] },
    composerFilesRef: { current: [] },
    composerTerminalContextsRef: { current: [] },
    snapshot: { prompt, images: [], files: [], terminalContexts: [], previewAnnotations: [], reviewComments: [], threadContexts: [] },
    resetCursor: () => {},
  }));
}
