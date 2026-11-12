import type { OrchestrationV2ThreadShell, ThreadId } from "@t3tools/contracts";
import {
  derivePendingBackgroundWork,
  turnItemUpdateCanEndBackgroundWork,
} from "@t3tools/shared/orchestrationV2PendingBackgroundWork";

type BannerShell = Pick<
  OrchestrationV2ThreadShell,
  "id" | "pendingBackgroundTasks" | "activityRunStatus" | "status"
>;

export function deriveDependencyBannerTasks(
  input: Omit<Parameters<typeof derivePendingBackgroundWork>[0], "dependencies"> & {
    readonly threadId: ThreadId;
    readonly shell?: BannerShell | null | undefined;
    /** Both watermarks use the global orchestration event sequence, including thread-filtered reads. */
    readonly detailAppliedSequence?: number | null | undefined;
    readonly shellSnapshotSequence?: number | null | undefined;
  },
) {
  const shell = input.shell?.id === input.threadId ? input.shell : null;
  const activity = shell?.activityRunStatus === undefined ? shell?.status : shell.activityRunStatus;
  const shellWorking =
    activity === "preparing" || activity === "starting" || activity === "running";
  const detailIsCurrent =
    input.detailAppliedSequence != null &&
    input.shellSnapshotSequence != null &&
    input.detailAppliedSequence >= input.shellSnapshotSequence;
  const terminalTaskIds = new Set(
    detailIsCurrent
      ? input.turnItems.filter(turnItemUpdateCanEndBackgroundWork).map((item) => {
          const nativeId = item.nativeItemRef?.nativeId;
          return typeof nativeId === "string" && nativeId.length > 0 ? nativeId : String(item.id);
        })
      : [],
  );
  return derivePendingBackgroundWork({
    ...input,
    ...(shellWorking ? { hasActiveRun: true } : {}),
    dependencies: (shell?.pendingBackgroundTasks ?? []).filter(
      (task) => !terminalTaskIds.has(task.taskId),
    ),
  });
}
