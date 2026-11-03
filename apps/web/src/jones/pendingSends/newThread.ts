import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type { EnvironmentId, ModelSelection, ThreadId } from "@t3tools/contracts";
import type { PendingSend } from "./store";
import { buildLocalDraftThread } from "../../components/ChatView.logic";

export function provenPendingThread(
  entries: readonly PendingSend[],
  environmentId: EnvironmentId,
  threadId: ThreadId,
): EnvironmentThreadShell | undefined {
  return entries.find(
    (entry) =>
      entry.environmentId === environmentId &&
      entry.threadId === threadId &&
      (entry.creationAccepted === true ||
        (entry.deliveryEvidence?.kind === "launch" &&
          entry.deliveryEvidence.creation.status === "accepted" &&
          entry.deliveryEvidence.creation.threadId === threadId)),
  )?.sidebarThread;
}

export function canSendAfterInitialMessageRejection(
  entries: readonly PendingSend[],
  environmentId: EnvironmentId,
  threadId: ThreadId,
): boolean {
  const targetEntries = entries.filter(
    (entry) => entry.environmentId === environmentId && entry.threadId === threadId,
  );
  // The store retains a confirmed rejection through later unknown replies. A newer
  // unsettled send must still wait for its own evidence before another identity is sent.
  return (
    targetEntries.every((entry) => entry.delivery === "failed") &&
    targetEntries.some(
      (entry) =>
        entry.deliveryEvidence?.kind === "launch" &&
        entry.deliveryEvidence.creation.status === "accepted" &&
        entry.deliveryEvidence.creation.threadId === threadId,
    )
  );
}

export function pendingNewThread(
  template: EnvironmentThreadShell,
  id: ThreadId,
  title: string,
  modelSelection: ModelSelection,
  createdAt: string,
): EnvironmentThreadShell {
  const thread = buildLocalDraftThread(
    id,
    {
      threadId: id,
      environmentId: template.environmentId,
      projectId: template.projectId,
      logicalProjectKey: "pending-send",
      createdAt,
      runtimeMode: template.runtimeMode,
      interactionMode: template.interactionMode,
      branch: template.branch,
      worktreePath: null,
      envMode: "local",
      startFromOrigin: false,
    },
    modelSelection,
  );
  return { ...thread, title, source: { ...thread.source, title } };
}
