import { useNavigate, useRouter } from "@tanstack/react-router";
import { useRef } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import { useQuietThreadShell } from "../../state/entities";
import { useEnvironment } from "../../state/environments";
import { threadTitlesRead } from "../../state/orchestration";
import { useAtomCommand } from "../../state/use-atom-command";
import { toastManager } from "../../components/ui/toast";
import { buildThreadRouteParams } from "../../threadRoutes";
import { resolveParentRef, type ParentSource } from "./model";

export function useParentLink(
  environmentId: EnvironmentId,
  thread: ParentSource | null | undefined,
) {
  const target = resolveParentRef(environmentId, thread);
  // Keep the delegation-time title until this client sees the scoped parent itself.
  const shell = useQuietThreadShell(target?.ref ?? null);
  const environment = useEnvironment(target?.ref.environmentId ?? null);
  const navigate = useNavigate();
  const router = useRouter();
  const readParent = useAtomCommand(threadTitlesRead);
  const opening = useRef(false);
  if (!target) return null;
  const environmentLabel = environment?.label ?? target.fallbackLabel;
  // Only a connected remote environment can open its parent thread.
  const unavailableReason =
    target.remote && environment?.connection.phase !== "connected"
      ? `Parent thread is on ${environmentLabel || "another environment"}. Connect to it to open the parent.`
      : undefined;
  return {
    ...target,
    threadId: target.ref.threadId,
    title: shell?.title || target.fallbackTitle,
    ...(target.remote ? { environmentLabel } : {}),
    unavailableReason,
    ...(unavailableReason
      ? {}
      : {
          open: async () => {
            if (target.remote) {
              if (opening.current) return;
              opening.current = true;
              const location = router.latestLocation;
              const result = await readParent({
                environmentId: target.ref.environmentId,
                input: { threadIds: [target.ref.threadId] },
              });
              opening.current = false;
              // The parent can be absent from the local shell or deleted remotely.
              // A late answer must not pull the user back after navigation.
              if (router.latestLocation !== location) return;
              if (result._tag === "Failure") {
                toastManager.add({ type: "error", title: "Could not open the parent thread" });
                return;
              }
              if (result.value[target.ref.threadId] === undefined) {
                toastManager.add({ type: "info", title: "The parent thread was deleted" });
                return;
              }
            }
            void navigate({
              to: "/$environmentId/$threadId",
              params: buildThreadRouteParams(target.ref),
            });
          },
        }),
  };
}
