import type { OrchestrationV2ThreadShell, ThreadId } from "@t3tools/contracts";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";

type BannerShell = Pick<
  OrchestrationV2ThreadShell,
  "id" | "pendingBackgroundTasks" | "activityRunStatus" | "status"
>;

export function deriveDependencyBannerTasks(
  input: Omit<Parameters<typeof derivePendingBackgroundWork>[0], "dependencies"> & {
    readonly threadId: ThreadId;
    readonly shell?: BannerShell | null | undefined;
  },
) {
  const shell = input.shell?.id === input.threadId ? input.shell : null;
  const activity = shell?.activityRunStatus === undefined ? shell?.status : shell.activityRunStatus;
  const shellWorking =
    activity === "preparing" || activity === "starting" || activity === "running";
  return derivePendingBackgroundWork({
    ...input,
    ...(shellWorking ? { hasActiveRun: true } : {}),
    dependencies: (shell?.pendingBackgroundTasks ?? []).filter((task) =>
      task.taskId.startsWith("jones-dependency:"),
    ),
  });
}
