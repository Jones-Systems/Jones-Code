import { useCallback, useEffect, useState } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import {
  EnvironmentRegistry,
  orchestrationProtocolCompatibilityError,
} from "@t3tools/client-runtime/connection";
import {
  requestJonesUpdateWithDescriptor,
  type JonesUpdateBridgeInput,
} from "@t3tools/client-runtime/jones/fleet-updates";
import { createRuntimeCommand } from "@t3tools/client-runtime/state/runtime";
import * as Effect from "effect/Effect";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { connectionAtomRuntime } from "../../connection/runtime";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../../components/ui/button";
import { APP_SOURCE_SHA } from "../../branding";
import { resolveJonesSourceCurrency, type JonesSourceCurrency } from "./sourceCurrency";

const command = createRuntimeCommand(connectionAtomRuntime, {
  label: "jones:update-incompatible-host",
  concurrency: { mode: "serial", key: (input) => input.environmentId },
  execute: Effect.fn(function* (input: {
    readonly environmentId: EnvironmentId;
    readonly request: JonesUpdateBridgeInput;
  }) {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const entry = (yield* SubscriptionRef.get(registry.entries)).get(input.environmentId);
    if (entry === undefined) {
      return yield* new EnvironmentRegistry.EnvironmentNotRegisteredError({
        environmentId: input.environmentId,
      });
    }
    const result = yield* requestJonesUpdateWithDescriptor(entry, input.request);
    if (
      input.request.action === "state" &&
      result.state !== null &&
      entry.serverUpdateRequired === true &&
      orchestrationProtocolCompatibilityError(result.descriptor) === null &&
      (yield* SubscriptionRef.get(registry.entries)).get(input.environmentId) === entry
    ) {
      yield* registry.setCompatibility(input.environmentId, null);
      yield* registry.setEnabled(input.environmentId, true);
    }
    return {
      state: result.state,
      currency: resolveJonesSourceCurrency({
        installedSource: result.descriptor.jonesSource?.sha,
        targetSource: APP_SOURCE_SHA,
      }),
    };
  }),
});

/** An incompatible host cannot mount the ordinary connected-environment updater. */
export function JonesBlockedHostUpdates({
  environmentId,
}: {
  readonly environmentId: EnvironmentId;
}) {
  const execute = useAtomCommand(command, { reportFailure: false });
  const [state, setState] = useState<JonesUpdateState | null>(null);
  const [pending, setPending] = useState(false);
  const [message, setMessage] = useState<string>();
  const [unknownInstall, setUnknownInstall] = useState(false);
  const [currency, setCurrency] = useState<JonesSourceCurrency>("unknown");
  const run = useCallback(
    async (request: JonesUpdateBridgeInput) => {
      setPending(true);
      const result = await execute({ environmentId, request });
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
    },
    [environmentId, execute],
  );

  useEffect(() => {
    void run({ action: "state" });
  }, [run]);

  const busy = pending || state?.phase === "preparing" || state?.phase === "installing";
  return (
    <div className="flex max-w-md flex-col gap-2 text-xs">
      <p role="status">{message ?? "Reading qualified Jones update support…"}</p>
      <p className="text-muted-foreground">
        {currency === "current"
          ? "Source matches this app."
          : "Source relationship has not been verified."}
      </p>
      {state?.outcome ? (
        <p>
          {state.outcome.status}: {state.outcome.fromVersion} → {state.outcome.targetVersion}
        </p>
      ) : null}
      <div className="flex flex-wrap gap-2">
        <Button
          size="xs"
          variant="outline"
          disabled={pending}
          onClick={() => void run({ action: "state" })}
        >
          Refresh status
        </Button>
        {state?.capability.check ? (
          <Button
            size="xs"
            variant="outline"
            disabled={busy || unknownInstall}
            onClick={() => void run({ action: "check" })}
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
                void run({
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
        ) : null}
      </div>
    </div>
  );
}
