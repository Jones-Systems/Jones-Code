import { PROVIDER_SEND_TURN_MAX_ATTACHMENTS } from "@t3tools/contracts";
import { canRestoreQueuedThreadMessage } from "../jones/threadConfiguration/outboxDelivery";
import {
  composerModelSelectionSnapshotIsCurrent,
  retainComposerModelSelectionSnapshot,
  trackComposerModelSelectionInteractions,
} from "../jones/threadConfiguration/outboxSelection";
import { scopedThreadKey } from "../lib/scopedEntities";
import { appAtomRegistry } from "./atom-registry";
import {
  confirmThreadOutboxMessageQueued,
  threadOutboxRevision,
  type QueuedThreadMessage,
} from "./thread-outbox";
import { removeThreadOutboxMessage } from "./thread-outbox-removal";
import {
  flushComposerDrafts,
  getComposerDraftSnapshot,
  mergeComposerDraftContent,
  undoComposerDraftMerge,
  updateComposerDraftSettings,
  waitForComposerDraftsLoaded,
} from "./use-composer-drafts";
import {
  dispatchingQueuedMessageIdAtom,
  editingQueuedMessageIdsAtom,
  holdEditingQueuedMessage,
  releaseEditingQueuedMessage,
} from "./use-thread-outbox";

/** Take delivery ownership before any await; the durable draft then takes ownership of the files. */
export async function editPendingThreadMessage(message: QueuedThreadMessage): Promise<boolean> {
  if (
    message.creation ||
    !canRestoreQueuedThreadMessage(message) ||
    appAtomRegistry.get(dispatchingQueuedMessageIdAtom) === message.messageId ||
    appAtomRegistry.get(editingQueuedMessageIdsAtom)[message.messageId]
  ) {
    return false;
  }
  holdEditingQueuedMessage(message.messageId);
  const draftKey = scopedThreadKey(message.environmentId, message.threadId);
  const revision = threadOutboxRevision(message.messageId);
  const canTransfer = () => canRestoreQueuedThreadMessage(message) &&
    threadOutboxRevision(message.messageId) === revision &&
    appAtomRegistry.get(dispatchingQueuedMessageIdAtom) !== message.messageId;
  const releaseSelectionTracking = trackComposerModelSelectionInteractions(draftKey);
  let rollback: {
    snapshot: ReturnType<typeof getComposerDraftSnapshot>;
    merged: ReturnType<typeof getComposerDraftSnapshot>;
  } | null = null;
  try {
    if (!(await confirmThreadOutboxMessageQueued(message)) || !canTransfer()) return false;
    await waitForComposerDraftsLoaded();
    if (!canTransfer()) return false;
    const snapshot = getComposerDraftSnapshot(draftKey);
    const attachmentIds = new Set(snapshot.attachments.map((attachment) => attachment.id));
    for (const attachment of message.attachments) attachmentIds.add(attachment.id);
    if (attachmentIds.size > PROVIDER_SEND_TURN_MAX_ATTACHMENTS) {
      throw new Error("Remove attachments from the composer before editing this message.");
    }
    let mergedDraft = snapshot;
    await mergeComposerDraftContent(draftKey, message, { onMerged: (draft) => {
      mergedDraft = retainComposerModelSelectionSnapshot(draft, snapshot);
      rollback = { snapshot, merged: mergedDraft };
    } });
    if (!canTransfer()) return false;
    const currentDraft = getComposerDraftSnapshot(draftKey);
    const settings = {
      ...(message.modelSelection && currentDraft.modelSelection === mergedDraft.modelSelection &&
        composerModelSelectionSnapshotIsCurrent(draftKey, snapshot)
        ? { modelSelection: message.modelSelection } : {}),
      ...(message.runtimeMode && currentDraft.runtimeMode === mergedDraft.runtimeMode
        ? { runtimeMode: message.runtimeMode } : {}),
      ...(message.interactionMode && currentDraft.interactionMode === mergedDraft.interactionMode
        ? { interactionMode: message.interactionMode } : {}),
    };
    const updated = updateComposerDraftSettings(draftKey, settings);
    rollback = { snapshot, merged: retainComposerModelSelectionSnapshot({
      ...mergedDraft,
      ...settings,
      ...(settings.modelSelection === undefined ? {} : { modelSelection: updated.modelSelection }),
    }, updated) };
    await flushComposerDrafts();
    if (!canTransfer() || !(await confirmThreadOutboxMessageQueued(message)) || !canTransfer()) return false;
    if (!(await removeThreadOutboxMessage(message, revision, canTransfer))) return false;
    rollback = null;
    return true;
  } finally {
    try {
      if (rollback) await undoComposerDraftMerge(draftKey, rollback.snapshot, rollback.merged);
    } finally {
      releaseSelectionTracking();
      releaseEditingQueuedMessage(message.messageId);
    }
  }
}
