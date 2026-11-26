import { collectComposerContextReferences } from "@t3tools/shared/composerContextReferences";
import type { QueuedThreadMessage } from "./thread-outbox-model";
import {
  canRestoreQueuedThreadMessage,
  queuedThreadProvenTarget,
} from "../jones/threadConfiguration/outboxDelivery";
import { scopedThreadKey } from "../lib/scopedEntities";
import { abortComposerDraftOperations } from "./composerDraftOperations";
import { restoredNewTaskDraftKey } from "./new-task-draft-key";
import {
  clearComposerDraftContent,
  getComposerDraftSnapshot,
  mergeComposerDraftContent,
  sameComposerDraftState,
  undoComposerDraftMerge,
  type ComposerDraft,
  type ComposerDraftContent,
} from "./use-composer-drafts";

export async function recoverFailedThreadDraft(message: QueuedThreadMessage): Promise<void> {
  const canRecover = () =>
    canRestoreQueuedThreadMessage(message) && queuedThreadProvenTarget(message) === undefined;
  if (!canRecover()) return;
  const sourceKey = scopedThreadKey(message.environmentId, message.threadId);
  const targetKey = restoredNewTaskDraftKey(message.messageId);
  const source = getComposerDraftSnapshot(sourceKey);
  if (source.text.length === 0 && source.attachments.length === 0) {
    abortComposerDraftOperations(sourceKey);
    return;
  }
  const target = getComposerDraftSnapshot(targetKey);
  let merged: ComposerDraft = target;
  let sourceToClear: ComposerDraft | undefined;
  let lateMerge: { before: ComposerDraft; after: ComposerDraft } | undefined;
  try {
    await mergeComposerDraftContent(targetKey, source, {
      restoreAttachments: true,
      onMerged: (draft) => {
        merged = draft;
      },
    });
    if (canRecover()) {
      const currentSource = getComposerDraftSnapshot(sourceKey);
      const latePaste = appendedPaste(source, currentSource);
      if (latePaste || sameComposerDraftState(source, currentSource)) {
        abortComposerDraftOperations(sourceKey);
        if (latePaste && canRecover()) {
          const before = getComposerDraftSnapshot(targetKey);
          const addition = { before, after: before };
          lateMerge = addition;
          await mergeComposerDraftContent(targetKey, latePaste, {
            restoreAttachments: true,
            onMerged: (after) => {
              addition.after = after;
            },
          });
        }
        sourceToClear = currentSource;
      }
    }
  } catch (error) {
    if (canRecover()) {
      if (lateMerge) await undoComposerDraftMerge(targetKey, target, merged, lateMerge);
      throw error;
    }
  }
  if (!canRecover()) {
    await undoComposerDraftMerge(targetKey, target, merged, lateMerge);
    return;
  }
  if (sourceToClear && sameComposerDraftState(sourceToClear, getComposerDraftSnapshot(sourceKey))) {
    clearComposerDraftContent(sourceKey);
  }
}

/** Recognize references appended by a paste without treating newer edits as moved. */
function appendedPaste(source: ComposerDraft, current: ComposerDraft): ComposerDraftContent | null {
  const attachments = current.attachments.slice(source.attachments.length);
  if (
    attachments.length === 0 ||
    !source.attachments.every((attachment, index) => current.attachments[index] === attachment) ||
    !current.text.startsWith(source.text) ||
    !sameComposerDraftState(source, {
      ...current,
      attachments: source.attachments,
      text: source.text,
      context: source.context,
    })
  ) {
    return null;
  }
  const ids = new Set(attachments.map((attachment) => attachment.id));
  const allRecords = current.context?.records ?? [];
  const records = allRecords.filter(
    (record) => "attachmentId" in record && ids.has(record.attachmentId),
  );
  const retained = allRecords.filter((record) => !records.includes(record));
  const previous = source.context?.records ?? [];
  if (
    retained.length !== previous.length ||
    retained.some((record, index) => record !== previous[index])
  ) {
    return null;
  }
  const suffix = current.text.slice(source.text.length).trim();
  const references = collectComposerContextReferences(suffix);
  if (
    references.map((reference) => reference.source).join(" ") !== suffix ||
    references.length !== records.length ||
    !references.every((reference) =>
      records.some((record) => record.contextId === reference.contextId),
    )
  ) {
    return null;
  }
  return {
    text: suffix,
    attachments,
    context: records.length === 0 ? undefined : { version: 1, records },
  };
}
