import type { EnvironmentId, RunId, ThreadId } from "@t3tools/contracts";
import { useState } from "react";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";

export function useQueuedRunRemoval(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly queuedRunIds: ReadonlyArray<RunId>;
  readonly blocked: () => boolean;
}) {
  const cancel = useAtomCommand(threadEnvironment.cancelQueuedRun);
  const identity = `${input.environmentId}:${input.threadId}`;
  const [scope, setScope] = useState(() => ({
    identity,
    inFlight: new Set<RunId>(),
    accepted: new Set<RunId>(),
  }));
  if (scope.identity !== identity) {
    setScope({ identity, inFlight: new Set<RunId>(), accepted: new Set<RunId>() });
  }
  const [status, setStatus] = useState(() => ({
    scope,
    pending: new Set<RunId>(),
    accepted: new Set<RunId>(),
    failed: new Set<RunId>(),
  }));
  if (status.scope !== scope) {
    setStatus({ scope, pending: new Set(), accepted: new Set(), failed: new Set() });
  }
  const pending = status.scope === scope ? status.pending : new Set<RunId>();
  const accepted = status.scope === scope ? status.accepted : new Set<RunId>();
  const failed = status.scope === scope ? status.failed : new Set<RunId>();
  const busy = pending.size > 0 || input.queuedRunIds.some((id) => accepted.has(id));

  return {
    busy,
    isBusy: () =>
      scope.inFlight.size > 0 || input.queuedRunIds.some((id) => scope.accepted.has(id)),
    isRemoving: (id: RunId) => pending.has(id) || accepted.has(id),
    failedCount: input.queuedRunIds.filter((id) => failed.has(id)).length,
    async remove(runIds: ReadonlyArray<RunId>) {
      if (input.blocked()) return;
      const snapshot = [...new Set(runIds)].filter(
        (id) =>
          input.queuedRunIds.includes(id) && !scope.inFlight.has(id) && !scope.accepted.has(id),
      );
      if (snapshot.length === 0) return;
      for (const id of snapshot) scope.inFlight.add(id);
      setStatus((current) => ({
        ...current,
        pending: new Set([...current.pending, ...snapshot]),
        failed: new Set([...current.failed].filter((id) => !snapshot.includes(id))),
      }));
      await Promise.all(
        snapshot.map(async (runId) => {
          const success = await Promise.resolve()
            .then(() =>
              cancel({
                environmentId: input.environmentId,
                input: { threadId: input.threadId, runId },
              }),
            )
            .then(
              (result) => result._tag === "Success",
              () => false,
            );
          // Accepted IDs stay reserved until their authoritative rows disappear.
          if (success) scope.accepted.add(runId);
          scope.inFlight.delete(runId);
          setStatus((current) => {
            if (current.scope !== scope) return current;
            const next = {
              ...current,
              pending: new Set(current.pending),
              accepted: new Set(current.accepted),
              failed: new Set(current.failed),
            };
            next.pending.delete(runId);
            (success ? next.accepted : next.failed).add(runId);
            return next;
          });
        }),
      );
    },
  };
}
