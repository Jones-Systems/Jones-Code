import type { ModelSelection } from "@t3tools/contracts";

interface SelectionSnapshot {
  readonly modelSelection?: ModelSelection;
}

interface QueuedSelection extends SelectionSnapshot {
  readonly modelSelectionExplicit?: boolean;
}

// Track only in-flight restores. Tokens survive empty-draft deletion without
// retaining every thread visited during the app session or adding stored fields.
const interactions = new Map<string, { identity: object; readers: number }>();
const snapshots = new WeakMap<object, object | undefined>();

export function trackComposerModelSelectionInteractions(draftKey: string): () => void {
  const interaction = interactions.get(draftKey) ?? { identity: {}, readers: 0 };
  interaction.readers += 1;
  interactions.set(draftKey, interaction);
  let released = false;
  return () => {
    if (released) return;
    released = true;
    interaction.readers -= 1;
    if (interaction.readers === 0) interactions.delete(draftKey);
  };
}

export function recordComposerModelSelectionInteraction(draftKey: string): void {
  const interaction = interactions.get(draftKey);
  if (interaction) interaction.identity = {};
}

export function captureComposerModelSelectionSnapshot<T extends SelectionSnapshot>(
  draftKey: string,
  snapshot: T,
): T {
  snapshots.set(snapshot, interactions.get(draftKey)?.identity);
  return snapshot;
}

export function retainComposerModelSelectionSnapshot<T extends SelectionSnapshot>(
  snapshot: T,
  source: SelectionSnapshot,
): T {
  if (snapshots.has(source)) snapshots.set(snapshot, snapshots.get(source));
  return snapshot;
}

export function composerModelSelectionSnapshotIsCurrent(
  draftKey: string,
  snapshot: SelectionSnapshot,
): boolean {
  return (
    !snapshots.has(snapshot) || snapshots.get(snapshot) === interactions.get(draftKey)?.identity
  );
}

export function outboxSelectionCommandFields(message: QueuedSelection): {
  readonly modelSelectionExplicit?: boolean;
} {
  return message.modelSelectionExplicit === undefined
    ? {}
    : { modelSelectionExplicit: message.modelSelectionExplicit };
}

export function outboxSelectionRestoreSettings(
  message: QueuedSelection,
  draftKey: string,
  before: SelectionSnapshot,
  current: SelectionSnapshot,
): { readonly modelSelection?: ModelSelection } {
  return message.modelSelectionExplicit === true &&
    message.modelSelection !== undefined &&
    before.modelSelection === undefined &&
    current.modelSelection === undefined &&
    composerModelSelectionSnapshotIsCurrent(draftKey, before)
    ? { modelSelection: message.modelSelection }
    : {};
}
