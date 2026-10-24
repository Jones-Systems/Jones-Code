import { useNavigate } from "@tanstack/react-router";
import type { EnvironmentId } from "@t3tools/contracts";
import { useQuietThreadShell } from "../../state/entities";
import { useEnvironment } from "../../state/environments";
import { buildThreadRouteParams } from "../../threadRoutes";
import { resolveParentRef, type ParentSource } from "./model";

export function useParentLink(
  environmentId: EnvironmentId,
  thread: ParentSource | null | undefined,
) {
  const target = resolveParentRef(environmentId, thread);
  const shell = useQuietThreadShell(target?.ref ?? null);
  const environment = useEnvironment(target?.ref.environmentId ?? null);
  const navigate = useNavigate();
  if (!target) return null;
  const environmentLabel = environment?.label ?? target.fallbackLabel;
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
          open: () => {
            void navigate({
              to: "/$environmentId/$threadId",
              params: buildThreadRouteParams(target.ref),
            });
          },
        }),
  };
}
