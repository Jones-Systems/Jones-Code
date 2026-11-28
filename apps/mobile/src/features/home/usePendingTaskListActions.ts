import { errorDisplayMessage } from "@t3tools/client-runtime/errors";
import * as Dialog from "../../components/ui/Dialog";
import { msg } from "gt-react-native";
import { useCallback } from "react";

import { m } from "../../i18n/messages";
import { automaticLaunchNeedsReconciliation } from "../../jones/threadConfiguration/outboxDelivery";
import { scopedThreadKey } from "../../lib/scopedEntities";

import { removeThreadOutboxMessage } from "../../state/thread-outbox-removal";
import { threadOutboxManager } from "../../state/thread-outbox";
import { clearComposerDraftContent } from "../../state/use-composer-drafts";
import type { PendingNewTask } from "../../state/use-pending-new-tasks";
import { appAtomRegistry } from "../../state/atom-registry";
import {
  editingQueuedMessageIdsAtom,
  holdEditingQueuedMessage,
  releaseEditingQueuedMessage,
} from "../../state/use-thread-outbox";
import { withThreadDismissal } from "./thread-dismissal";

export function usePendingTaskListActions(
  selectThreadRoute: (params: ReactNavigation.RootParamList["Thread"]) => void,
): {
  readonly openPendingTask: (pendingTask: PendingNewTask) => void;
  readonly confirmDeletePendingTask: (pendingTask: PendingNewTask) => void;
} {
  const openPendingTask = useCallback(
    (pendingTask: PendingNewTask) => {
      selectThreadRoute(
        pendingTask.kind === "pending"
          ? {
              environmentId: String(pendingTask.environmentId),
              threadId: String(pendingTask.message.threadId),
            }
          : {
              draft: {
                environmentId: String(pendingTask.environmentId),
                projectId: String(pendingTask.projectId),
                draftId: pendingTask.draftKey,
              },
            },
      );
    },
    [selectThreadRoute],
  );

  const confirmDeletePendingTask = useCallback((pendingTask: PendingNewTask) => {
    if (pendingTask.kind === "draft") {
      Dialog.alert(
        m(msg("Discard draft?")),
        m(msg("“{title}” will be removed.", { title: pendingTask.title })),
        [
          { text: m(msg("Cancel")), style: "cancel" },
          {
            text: m(msg("Discard", { $context: "verb, discards a draft" })),
            style: "destructive",
            onPress: () => {
              void withThreadDismissal(
                pendingTask.key,
                async () =>
                  clearComposerDraftContent(pendingTask.draftKey, {
                    clearModelSelection: true,
                    clearWorkspaceSelection: true,
                  }),
                () => true,
              );
            },
          },
        ],
      );
      return;
    }
    const { message } = pendingTask;
    const key = scopedThreadKey(message.environmentId, message.threadId);
    const canDiscard = () => {
      const queued = appAtomRegistry.get(threadOutboxManager.queuedMessagesByThreadKeyAtom);
      const latest = queued[key]?.find((entry) => entry.messageId === message.messageId);
      return latest === undefined || !automaticLaunchNeedsReconciliation(latest);
    };
    const showVerifying = () =>
      Dialog.alert(
        m(msg("Verifying task delivery")),
        m(msg("The task may already be on the server. Reconnect to check its delivery.")),
      );
    if (!canDiscard()) {
      showVerifying();
      return;
    }
    Dialog.alert(
      m(msg("Delete pending task?")),
      m(
        msg("“{title}” has not been sent yet and will be removed from the outbox.", {
          title: pendingTask.title,
        }),
      ),
      [
        { text: m(msg("Cancel")), style: "cancel" },
        {
          text: m(msg("Delete", { $context: "verb, deletes a pending task" })),
          style: "destructive",
          onPress: () => {
            if (!canDiscard()) {
              showVerifying();
              return;
            }
            // Hold delivery from confirmation so reconnects cannot send a task during removal.
            // The editor can release the shared hold mid-animation; remove edited tasks at once.
            // Keep an editor's hold unless removal succeeds.
            const messageId = pendingTask.message.messageId;
            const heldByEditor = appAtomRegistry.get(editingQueuedMessageIdsAtom)[messageId];
            holdEditingQueuedMessage(messageId);
            const releaseUnlessEditing = () => {
              if (!heldByEditor) releaseEditingQueuedMessage(messageId);
            };
            const remove = () => removeThreadOutboxMessage(message, undefined, canDiscard);
            void (
              heldByEditor
                ? remove()
                : withThreadDismissal(pendingTask.key, remove, (removed) => removed)
            )
              .then((removed) => {
                if (removed) releaseEditingQueuedMessage(messageId);
                else {
                  releaseUnlessEditing();
                  if (!canDiscard()) showVerifying();
                }
              })
              .catch((error) => {
                releaseUnlessEditing();
                Dialog.alert(
                  m(msg("Could not delete pending task")),
                  error instanceof Error
                    ? (errorDisplayMessage(error, m) ?? error.message)
                    : m(msg("The pending task could not be removed.")),
                );
              });
          },
        },
      ],
    );
  }, []);

  return { openPendingTask, confirmDeletePendingTask };
}
