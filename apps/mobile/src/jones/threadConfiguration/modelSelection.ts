import type { ModelSelection } from "@t3tools/contracts";

interface QueuedModelSelection {
  readonly modelSelection?: ModelSelection;
  readonly modelSelectionExplicit?: boolean;
  readonly creation?: unknown;
}

export function captureFollowUpModelSelection(
  draftPick: ModelSelection | undefined,
  savedSelection: ModelSelection,
) {
  return {
    modelSelection: draftPick ?? savedSelection,
    modelSelectionExplicit: draftPick !== undefined,
  };
}

export function queuedModelSelectionForDraft(message: QueuedModelSelection) {
  // An inherited snapshot must not become an intentional pick when recovery restores content.
  return message.modelSelectionExplicit === false && message.creation === undefined
    ? undefined
    : message.modelSelection;
}

export function resolveQueuedModelSelection(
  message: QueuedModelSelection,
  savedSelection: ModelSelection,
): ModelSelection {
  // Missing markers keep legacy replay semantics. Creations always own their selection.
  return queuedModelSelectionForDraft(message) ?? savedSelection;
}

export function consumeSubmittedModelPick<
  Draft extends { readonly modelSelection?: ModelSelection },
>(draft: Draft, submittedPick: ModelSelection | undefined): Draft {
  // Identity protects a later picker edit, including a deliberate re-selection of the same value.
  if (submittedPick === undefined || draft.modelSelection !== submittedPick) return draft;
  return { ...draft, modelSelection: undefined };
}
