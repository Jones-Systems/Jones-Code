import type { EnvironmentId } from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { useState } from "react";
import { jonesUpdates } from "./jonesUpdates";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../../components/ui/button";

export function JonesUpdateControls({ environmentId }: { readonly environmentId: EnvironmentId }) {
  const state = useAtomValue(jonesUpdates.value(environmentId));
  const action = useAtomCommand(jonesUpdates.action, { reportFailure: false });
  const [pending, setPending] = useState(false);
  if (state === null) return null;
  const run = async (input: Parameters<typeof action>[0]["input"]) => {
    setPending(true);
    try {
      await action({ environmentId, input });
    } finally {
      setPending(false);
    }
  };
  const busy =
    pending ||
    ["checking", "building", "downloading", "verifying", "preparing", "installing"].includes(
      state.phase,
    );
  const provenance = state.provenance;
  return (
    <div className="mt-2 flex max-w-lg flex-col gap-2 text-xs">
      <p className="text-muted-foreground">Jones main · {state.message ?? state.phase}</p>
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
