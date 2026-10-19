import { useEffect, useRef } from "react";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import { createThreadReadDwell } from "@t3tools/client-runtime/jones/thread-read-dwell";
import { resolveThreadLastVisitedAt } from "../../components/Sidebar.logic";
import { useUiStateStore } from "../../uiStateStore";
import { threadEnvironment } from "../../state/threads";
import { useThreadShell } from "../../state/entities";
import { useAtomCommand } from "../../state/use-atom-command";

// Keep the streaming watermark subscription in a leaf so ChatView can read the quiet shell.
export function ThreadVisitWatermark(props: { readonly threadRef: ScopedThreadRef }) {
  const serverThread = useThreadShell(props.threadRef);
  const threadKey = scopedThreadKey(props.threadRef);
  const markThreadVisited = useUiStateStore((store) => store.markThreadVisited);
  const localLastVisitedAt = useUiStateStore((store) => store.threadLastVisitedAtById[threadKey]);
  const visitThread = useAtomCommand(threadEnvironment.visit, { reportFailure: false });
  const controllerRef = useRef<ReturnType<typeof createThreadReadDwell> | null>(null);

  useEffect(() => {
    const controller = createThreadReadDwell();
    controllerRef.current = controller;
    const onVisibility = () =>
      controller.setVisible(document.visibilityState === "visible" && document.hasFocus());
    const onBlur = () => controller.setVisible(false);
    document.addEventListener("visibilitychange", onVisibility);
    window.addEventListener("focus", onVisibility);
    window.addEventListener("blur", onBlur);
    return () => {
      controller.dispose();
      controllerRef.current = null;
      document.removeEventListener("visibilitychange", onVisibility);
      window.removeEventListener("focus", onVisibility);
      window.removeEventListener("blur", onBlur);
    };
  }, []);

  useEffect(() => {
    controllerRef.current?.update(
      serverThread === null
        ? null
        : {
            threadKey,
            updatedAt: serverThread.updatedAt,
            lastVisitedAt: resolveThreadLastVisitedAt(
              serverThread.lastVisitedAt,
              localLastVisitedAt,
            ),
            completedAt: serverThread.latestRun?.completedAt ?? null,
            visible: document.visibilityState === "visible" && document.hasFocus(),
            onVisit: (visitedAt) => {
              if (serverThread.lastVisitedAt === undefined) {
                markThreadVisited(threadKey, visitedAt);
                return;
              }
              void visitThread({
                environmentId: serverThread.environmentId,
                input: { threadId: serverThread.id, visitedAt },
              });
            },
          },
    );
  }, [localLastVisitedAt, markThreadVisited, serverThread, threadKey, visitThread]);
  return null;
}
