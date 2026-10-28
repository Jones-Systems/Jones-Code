import { useQuietThreadShell } from "../../state/entities";
import { usePendingPreviewRelease } from "./previews";
import { useEffect } from "react";
import * as Option from "effect/Option";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { useEnvironmentThread } from "../../state/threads";
import { usePendingSends } from "./hooks";
import { pendingSends } from "./store";

function ReconcileThread(props: { environmentId: EnvironmentId; threadId: ThreadId }) {
  const shell = useQuietThreadShell({
    environmentId: props.environmentId,
    threadId: props.threadId,
  });
  const state = useEnvironmentThread(
    shell ? props.environmentId : null,
    shell ? props.threadId : null,
  );
  const projection = Option.getOrNull(state.data);
  usePendingPreviewRelease(props.environmentId, props.threadId, projection);
  // oxlint-disable-next-line t3code/react-effect-budget -- Reconcile delivery lifetime with authoritative subscriptions, including inactive routes.
  useEffect(() => {
    if (projection && state.status === "live")
      pendingSends.reconcile(props.environmentId, props.threadId, projection);
  }, [props.environmentId, props.threadId, projection, state.status]);
  return null;
}
export function PendingSendsCoordinator() {
  const entries = usePendingSends();
  const threads = new Map(
    entries.map((entry) => [JSON.stringify([entry.environmentId, entry.threadId]), entry]),
  );
  return (
    <>
      {[...threads].map(([key, entry]) => (
        <ReconcileThread key={key} environmentId={entry.environmentId} threadId={entry.threadId} />
      ))}
    </>
  );
}
