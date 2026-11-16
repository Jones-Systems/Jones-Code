import { useComposerDraftStore, type ComposerThreadTarget } from "../../composerDraftStore";
import type { ModelSelection } from "@t3tools/contracts";
import { hasExplicitComposerModelSelection } from "../../lib/chatThreadActions";

export function composerSelectionForDispatch(
  modelSelection: ModelSelection,
  draft: Parameters<typeof hasExplicitComposerModelSelection>[0],
) {
  return {
    modelSelection,
    modelSelectionExplicit: hasExplicitComposerModelSelection(draft),
  };
}

export function requiredComposerProvider(
  draft: Parameters<typeof hasExplicitComposerModelSelection>[0],
  saved: ModelSelection | null | undefined,
  isLocalDraftThread: boolean,
) {
  if (isLocalDraftThread) return undefined;
  return hasExplicitComposerModelSelection(draft) ? draft?.activeProvider : saved?.instanceId;
}

// Consume only the accepted pick; a newer picker change must survive the send acknowledgement.
export function acknowledgeComposerSelection(
  target: ComposerThreadTarget,
  submitted: Parameters<typeof hasExplicitComposerModelSelection>[0],
): void {
  if (!hasExplicitComposerModelSelection(submitted)) return;
  const store = useComposerDraftStore.getState();
  const current = store.getComposerDraft(target);
  if (
    current?.activeProvider !== submitted?.activeProvider ||
    current?.modelSelectionByProvider !== submitted?.modelSelectionByProvider
  )
    return;
  const selection = current?.activeProvider
    ? current.modelSelectionByProvider[current.activeProvider]
    : undefined;
  if (selection) store.setModelSelection(target, selection, { replaceOptions: true });
}
