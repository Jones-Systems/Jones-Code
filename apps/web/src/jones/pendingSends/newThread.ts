import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type { ModelSelection, ThreadId } from "@t3tools/contracts";
import { buildLocalDraftThread } from "../../components/ChatView.logic";

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
