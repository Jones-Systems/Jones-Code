import type { ScopedThreadRef } from "@t3tools/contracts";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import { suppressThreadReadDwell } from "@t3tools/client-runtime/jones/thread-read-dwell";
import { readEnvironmentSupportsVisitedTracking, readThreadShell } from "../../state/entities";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { useUiStateStore } from "../../uiStateStore";

export function useMarkThreadUnread() {
  const markThreadUnreadMutation = useAtomCommand(threadEnvironment.markUnread, {
    reportFailure: false,
  });
  const markThreadUnreadLocal = useUiStateStore((state) => state.markThreadUnread);
  return (target: ScopedThreadRef) => {
    suppressThreadReadDwell(scopedThreadKey(target));
    if (readEnvironmentSupportsVisitedTracking(target.environmentId)) {
      void markThreadUnreadMutation({
        environmentId: target.environmentId,
        input: { threadId: target.threadId },
      });
      return;
    }
    const thread = readThreadShell(target);
    markThreadUnreadLocal(scopedThreadKey(target), thread?.latestRun?.completedAt);
  };
}
