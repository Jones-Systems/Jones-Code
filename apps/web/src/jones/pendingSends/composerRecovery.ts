import { useComposerDraftStore, type ComposerThreadDraftState } from "../../composerDraftStore";
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
        restored = {
          draft: useComposerDraftStore.getState().getComposerDraft(owner.composerDraftTarget),
        };
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
        if (owner.composerEnvironmentContextsRef) owner.composerEnvironmentContextsRef.current = [];
      }
      restored = undefined;
    },
  };
}
