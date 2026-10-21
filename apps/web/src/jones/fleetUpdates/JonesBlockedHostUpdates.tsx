import { Tooltip, TooltipTrigger, TooltipPopup } from "../../components/ui/tooltip";
import { outcomeBuildDetail } from "../updates/buildPairs";
import { jonesUpdatePresentation } from "@t3tools/client-runtime/jones/updates";
import { useEffect, useRef, useState } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type { JonesUpdateBridgeInput } from "@t3tools/client-runtime/jones/fleet-updates";
import { createRuntimeCommand } from "@t3tools/client-runtime/state/runtime";
import { connectionAtomRuntime } from "../../connection/runtime";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../../components/ui/button";
import { useStableCallback } from "../../hooks/useStableCallback";
import type { JonesSourceCurrency } from "./sourceCurrency";
import { executeBlockedHostUpdate } from "./blockedHostCommand";

const command = createRuntimeCommand(connectionAtomRuntime, {
  label: "jones:update-incompatible-host",
  concurrency: { mode: "serial", key: (input) => input.environmentId },
  execute: executeBlockedHostUpdate,
});

/** An incompatible host cannot mount the ordinary connected-environment updater. */
export function JonesBlockedHostUpdates({
  environmentId,
  historical = false,
}: {
  readonly environmentId: EnvironmentId;
  readonly historical?: boolean;
}) {
  const execute = useAtomCommand(command, { reportFailure: false });
  const [state, setState] = useState<JonesUpdateState | null>(null);
  const [pending, setPending] = useState(true);
  const [message, setMessage] = useState<string>();
  const [unknownInstall, setUnknownInstall] = useState(false);
  const [currency, setCurrency] = useState<JonesSourceCurrency>("unknown");
  const requestGeneration = useRef(0);
  const run = useStableCallback((target: EnvironmentId, request: JonesUpdateBridgeInput) => {
    const generation = requestGeneration.current;
    return execute({ environmentId: target, request }).then((result) => {
      if (generation !== requestGeneration.current) return;
      setPending(false);
      if (result._tag === "Failure") {
        if (request.action === "install") setUnknownInstall(true);
        setMessage(
          request.action === "install"
            ? "Installation acceptance could not be confirmed. Refresh to inspect the host; further installation requires reconciliation."
            : "The host update endpoint could not be reached or authorized. Refresh after checking the connection; older hosts may need launcher setup on the host.",
        );
        return;
      }
      setState(result.value.state);
      setCurrency(result.value.currency);
      setMessage(
        result.value.state === null
          ? "This host does not provide qualified Jones updates. It needs host-side setup."
          : result.value.state.message,
      );
    });
  });

  const requestUpdate = (request: JonesUpdateBridgeInput) => {
    setPending(true);
    return run(environmentId, request);
  };

  useEffect(() => {
    void run(environmentId, { action: "state" });
    return () => {
      requestGeneration.current += 1;
    };
  }, [environmentId, run]);

  const busy = pending || state?.phase === "preparing" || state?.phase === "installing";
  return (
    <div className="flex max-w-md flex-col gap-2 text-xs">
      {historical ? (
        <p>Previously required an update. The saved connection remains switched off.</p>
      ) : null}
      <p role="status">{message ?? "Reading qualified Jones update support…"}</p>
      <p className="text-muted-foreground">
        {currency === "current"
          ? "Source matches this app."
          : "Source relationship has not been verified."}
      </p>
      {state?.outcome ? (
        <Tooltip>
          <TooltipTrigger
            render={
              <p
                tabIndex={0}
                aria-label={`${jonesUpdatePresentation(state).outcomeMessage}; ${outcomeBuildDetail(state)}`}
              />
            }
          >
            {jonesUpdatePresentation(state).outcomeMessage}
          </TooltipTrigger>
          <TooltipPopup side="top">{outcomeBuildDetail(state)}</TooltipPopup>
        </Tooltip>
      ) : null}
      <div className="flex flex-wrap gap-2">
        <Button
          size="xs"
          variant="outline"
          disabled={pending}
          onClick={() => void requestUpdate({ action: "state" })}
        >
          Refresh status
        </Button>
        {state?.capability.check ? (
          <Button
            size="xs"
            variant="outline"
            disabled={busy || unknownInstall}
            onClick={() => void requestUpdate({ action: "check" })}
          >
            Check for builds
          </Button>
        ) : null}
        {state?.phase === "available" && state.provenance && state.capability.download ? (
          <Button
            size="xs"
            disabled={busy || unknownInstall}
            onClick={() => {
              if (state.provenance)
                void requestUpdate({
                  action: "download",
                  input: {
                    artifactId: state.provenance.artifactId,
                    sourceSha: state.provenance.sourceSha,
                  },
                });
            }}
          >
            Download
          </Button>
        ) : null}
        {state?.stagedHandle && state.currentVersion && state.capability.install ? (
          <Button
            size="xs"
            disabled={busy || unknownInstall}
            onClick={() => {
              if (state.stagedHandle && state.currentVersion)
                void requestUpdate({
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
        ) : null}
      </div>
    </div>
  );
}
