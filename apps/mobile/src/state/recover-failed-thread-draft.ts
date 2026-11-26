import type { QueuedThreadMessage } from "./thread-outbox-model";
import {
  canRestoreQueuedThreadMessage,
  queuedThreadProvenTarget,
} from "../jones/threadConfiguration/outboxDelivery";
import { scopedThreadKey } from "../lib/scopedEntities";
import { abortComposerDraftOperations } from "./composerDraftOperations";
import { restoredNewTaskDraftKey } from "./new-task-draft-key";
import {
  appendComposerDraftAttachments,
  clearComposerDraftContent,
  flushComposerDrafts,
  getComposerDraftSnapshot,
  mergeComposerDraftContent,
  sameComposerDraftState,
  undoComposerDraftMerge,
  type ComposerDraft,
} from "./use-composer-drafts";
import type { DraftComposerAttachment } from "../lib/composerImages";

export async function recoverFailedThreadDraft(message: QueuedThreadMessage): Promise<void> {
  const canRecover = () =>
    canRestoreQueuedThreadMessage(message) && queuedThreadProvenTarget(message) === undefined;
  if (!canRecover()) return;
  const sourceKey = scopedThreadKey(message.environmentId, message.threadId);
  const targetKey = restoredNewTaskDraftKey(message.messageId);
  const source = getComposerDraftSnapshot(sourceKey);
  if (source.text.length === 0 && source.attachments.length === 0) return;
  const target = getComposerDraftSnapshot(targetKey);
  let merged: ComposerDraft = target;
  try {
    await mergeComposerDraftContent(
      targetKey,
      {
        text: source.text,
        context: source.context,
        attachments: [],
      },
      {
        onMerged: (draft) => {
          merged = draft;
        },
      },
    );
    if (!canRecover()) {
      await undoComposerDraftMerge(targetKey, target, merged);
      return;
    }
    const existingIds = new Set(
      getComposerDraftSnapshot(targetKey).attachments.map((attachment) => attachment.id),
    );
    const added = source.attachments.filter((attachment) => !existingIds.has(attachment.id));
    appendComposerDraftAttachments(targetKey, added, { restore: true });
    merged = { ...merged, attachments: [...merged.attachments, ...added] };
    // Recovery may exceed caps; preserve every file and leave trimming to the editor.
    await flushComposerDrafts();
    if (!canRecover()) {
      await undoComposerDraftMerge(targetKey, target, merged);
      return;
    }
    if (sameComposerDraftState(source, getComposerDraftSnapshot(sourceKey))) {
      clearComposerDraftContent(sourceKey);
    }
  } catch (error) {
    if (!canRecover()) {
      await undoComposerDraftMerge(targetKey, target, merged);
      return;
    }
    throw error;
  }
}

function attachmentsMissingFrom(
  draftKey: string,
  attachments: ReadonlyArray<DraftComposerAttachment>,
) {
  const existingIds = new Set(
    getComposerDraftSnapshot(draftKey).attachments.map((attachment) => attachment.id),
  );
  return attachments.filter((attachment) => !existingIds.has(attachment.id));
}
