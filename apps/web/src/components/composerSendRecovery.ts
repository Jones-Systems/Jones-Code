import {
  composerDraftHasUserContent,
  useComposerDraftStore,
  type ComposerThreadTarget,
  type ComposerThreadDraftState,
} from "../composerDraftStore";
import { cloneComposerImageForRetry } from "./ChatView.logic";
import { collapseExpandedComposerCursor } from "../composer-logic";

type Ref<T> = { current: T };
type SendSnapshot = Pick<
  ComposerThreadDraftState,
  | "prompt"
  | "images"
  | "files"
  | "terminalContexts"
  | "previewAnnotations"
  | "reviewComments"
  | "threadContexts"
>;
type SendOwner = {
  routeThreadKey: string;
  currentRouteThreadKeyRef: Ref<string | null>;
  composerDraftTarget: ComposerThreadTarget;
  promptRef: Ref<string>;
};
type RecoveryCursor = { cursor: number; prompt: string; detectTrigger: boolean };

export function clearSubmittedComposer(input: SendOwner & { resetCursor: () => void }): void {
  const isDisplayed = input.currentRouteThreadKeyRef.current === input.routeThreadKey;
  if (isDisplayed) input.promptRef.current = "";
  useComposerDraftStore.getState().clearComposerContent(input.composerDraftTarget);
  if (isDisplayed) input.resetCursor();
}

export function restoreFailedComposerSend(
  input: SendOwner & {
    backgroundDraftOpened: boolean;
    composerImagesRef: Ref<SendSnapshot["images"]>;
    composerFilesRef: Ref<SendSnapshot["files"]>;
    composerTerminalContextsRef: Ref<SendSnapshot["terminalContexts"]>;
    snapshot: SendSnapshot;
    onRestore?: () => void;
    resetCursor: (options: RecoveryCursor) => void;
  },
): boolean {
  const isDisplayed =
    !input.backgroundDraftOpened && input.currentRouteThreadKeyRef.current === input.routeThreadKey;
  const store = useComposerDraftStore.getState();
  const draft = store.getComposerDraft(input.composerDraftTarget);
  const canRestore = isDisplayed
    ? input.promptRef.current.length === 0 &&
      input.composerImagesRef.current.length === 0 &&
      input.composerFilesRef.current.length === 0 &&
      input.composerTerminalContextsRef.current.length === 0 &&
      (draft?.previewAnnotations.length ?? 0) === 0 &&
      (draft?.reviewComments.length ?? 0) === 0 &&
      (draft?.threadContexts.length ?? 0) === 0
    : !composerDraftHasUserContent(draft);
  if (!canRestore) return false;

  input.onRestore?.();
  const snapshot = input.snapshot;
  const images = snapshot.images.map(cloneComposerImageForRetry);
  if (isDisplayed) {
    input.promptRef.current = snapshot.prompt;
    input.composerImagesRef.current = images;
    input.composerFilesRef.current = snapshot.files;
    input.composerTerminalContextsRef.current = snapshot.terminalContexts;
  }
  store.setPrompt(input.composerDraftTarget, snapshot.prompt);
  store.addImages(input.composerDraftTarget, images);
  store.addFiles(input.composerDraftTarget, snapshot.files);
  store.setTerminalContexts(input.composerDraftTarget, snapshot.terminalContexts);
  store.setPreviewAnnotations(input.composerDraftTarget, snapshot.previewAnnotations);
  store.setReviewComments(input.composerDraftTarget, snapshot.reviewComments);
  store.setThreadContexts(input.composerDraftTarget, snapshot.threadContexts);
  if (isDisplayed) {
    input.resetCursor({
      cursor: collapseExpandedComposerCursor(snapshot.prompt, snapshot.prompt.length),
      prompt: snapshot.prompt,
      detectTrigger: true,
    });
  }
  return true;
}
