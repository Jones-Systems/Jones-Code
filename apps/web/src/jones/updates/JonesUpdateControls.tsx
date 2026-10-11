import {
  jonesUpdateActionError,
  jonesUpdatePresentation,
} from "@t3tools/client-runtime/jones/updates";
import type { EnvironmentId } from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { useState } from "react";
import { jonesUpdates } from "./jonesUpdates";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../../components/ui/button";

export function JonesUpdateControls({ environmentId }: { readonly environmentId: EnvironmentId }) {
  const observation = useAtomValue(jonesUpdates.observation(environmentId));
  const state = observation.state;
  const action = useAtomCommand(jonesUpdates.action, { reportFailure: false });
  const [pending, setPending] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  if (state === null)
    return observation.freshness === "stale" ? (
      <p role="status" className="mt-2 text-xs text-muted-foreground">
        {observation.message}
      </p>
    ) : null;
  const run = async (input: Parameters<typeof action>[0]["input"]) => {
    setPending(true);
    setActionError(null);
    try {
      setActionError(jonesUpdateActionError(await action({ environmentId, input })));
    } finally {
      setPending(false);
    }
  };
  const presentation = jonesUpdatePresentation(state);
  const busy = pending || presentation.busy || observation.freshness !== "fresh";
  const provenance = state.provenance;
  return (
    <div className="mt-2 flex max-w-lg flex-col gap-2 text-xs">
      <p className="text-muted-foreground">Jones main · {presentation.message}</p>
      {observation.freshness !== "fresh" ? <p role="status">{observation.message}</p> : null}
      {actionError ? <p role="alert">{actionError}</p> : null}
      {state.updateId ? <p className="text-muted-foreground">Update {state.updateId}</p> : null}
      {presentation.outcomeMessage ? <p role="status">{presentation.outcomeMessage}</p> : null}
      {state.migrationPlan ? (
        <p className="text-muted-foreground">
          Pending migrations: {state.migrationPlan.pendingUpstream.length} upstream,{" "}
          {state.migrationPlan.pendingJones.length} Jones
        </p>
      ) : null}
      {state.recovery ? (
        <p className="text-muted-foreground">
          Recovery {state.recovery.method} · {(state.recovery.bytes / 1024 ** 3).toFixed(2)} GiB
        </p>
      ) : null}
      {provenance ? (
        <p className="text-muted-foreground">
          {provenance.version ?? provenance.sourceSha.slice(0, 8)}
        </p>
      ) : null}
      <div className="flex flex-wrap gap-2">
        <Button
          size="xs"
          variant="outline"
          disabled={busy || !state.capability.check}
          onClick={() => void run({ action: "check" })}
        >
          Check for builds
        </Button>
        {state.stagedHandle !== undefined ? (
          <Button
            size="xs"
            disabled={busy || !state.capability.install || state.currentVersion === undefined}
            onClick={() => {
              if (state.stagedHandle === undefined || state.currentVersion === undefined) return;
              void run({
                action: "install",
                input: {
                  stagedHandle: state.stagedHandle,
                  environmentId,
                  currentVersion: state.currentVersion,
                },
              });
            }}
          >
            Install
          </Button>
        ) : provenance !== undefined ? (
          <Button
            size="xs"
            disabled={busy || !state.capability.download || state.phase !== "available"}
            onClick={() =>
              void run({
                action: "download",
                input: { artifactId: provenance.artifactId, sourceSha: provenance.sourceSha },
              })
            }
          >
            Download
          </Button>
        ) : null}
      </div>
      {state.capability.reason === "bootstrap-required" ? (
        <p className="text-muted-foreground">
          This host needs a qualified launcher before installation.
        </p>
      ) : null}
    </div>
  );
}
