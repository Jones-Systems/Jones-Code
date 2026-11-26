import { m } from "../i18n/messages";
import { thrownErrorMessage } from "@t3tools/client-runtime/errors";
import { AuthProvidersManageScope, PROVIDER_DISPLAY_NAMES } from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  settlePromise,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { useRef, useState } from "react";
import { T, useGT } from "gt-react";

import { useEnvironments } from "~/state/environments";
import { serverEnvironment } from "~/state/server";
import { useEnvironmentsWithScope } from "~/state/session";
import { useAtomCommand } from "~/state/use-atom-command";
import { Button } from "./ui/button";
import { stackedThreadToast, toastManager } from "./ui/toast";
import { SharedTooltipTrigger } from "./ui/shared-tooltip";

export { useJonesProviderUpdates as useProviderUpdates } from "../jones/updates/useProviderUpdates";
import { useJonesProviderUpdates as useProviderUpdates } from "../jones/updates/useProviderUpdates";

// `refreshModels` always drops the server's hour-long latest-version cache. `modelCatalogOnly`
// drops it only when the model manifest changed.
export function useCheckProviderUpdates() {
  const gt = useGT();
  const { environments } = useEnvironments();
  const manageableIds = useEnvironmentsWithScope(environments, AuthProvidersManageScope);
  const refreshProviders = useAtomCommand(serverEnvironment.refreshProviders, {
    reportFailure: false,
  });
  const inFlight = useRef<Promise<void> | null>(null);

  const check = async (quiet: boolean) => {
    const failures = await Promise.all(
      environments
        .filter(
          (environment) =>
            environment.connection.phase === "connected" &&
            manageableIds.has(environment.environmentId),
        )
        .map(async ({ environmentId, label }) => {
          const result = await refreshProviders({ environmentId, input: { refreshModels: true } });
          if (result._tag !== "Failure" || isAtomCommandInterrupted(result)) return [];
          const error = squashAtomCommandFailure(result);
          return [`${label}: ${thrownErrorMessage(error, m) ?? gt("Refresh failed.")}`];
        }),
    );
    const lines = failures.flat();
    if (quiet || lines.length === 0) return;
    toastManager.add(
      stackedThreadToast({
        type: "error",
        title: gt("Could not check provider updates"),
        description: <span className="whitespace-pre-line">{lines.join("\n")}</span>,
      }),
    );
  };

  // A repeat click must not start a second model rediscovery while one is running.
  return ({ quiet = false }: { readonly quiet?: boolean } = {}) => {
    if (inFlight.current) return inFlight.current;
    const running = check(quiet).finally(() => {
      inFlight.current = null;
    });
    inFlight.current = running;
    return running;
  };
}

export function ProviderUpdatesAction() {
  const { machines, updateAll, completed, canManage } = useProviderUpdates();
  const pending = useRef(false);
  const [capturedMachines, setCapturedMachines] = useState(machines);
  const [isPending, setIsPending] = useState(false);
  if (machines.length === 0 && !isPending) {
    return null;
  }

  const handleUpdate = async () => {
    if (pending.current) return;
    pending.current = true;
    setCapturedMachines(machines);
    setIsPending(true);
    await settlePromise(() => updateAll());
    pending.current = false;
    setIsPending(false);
  };

  return (
    <SharedTooltipTrigger
      render={
        <Button
          size="xs"
          variant="ghost-muted"
          disabled={isPending}
          onClick={() => void handleUpdate()}
        >
          {isPending ? <T>Updating…</T> : <T>Update all</T>}
        </Button>
      }
      label={machines.map((machine) => (
        <div key={machine.environmentId}>
          {machine.label}:{" "}
          {machine.candidates
            .map((candidate) => PROVIDER_DISPLAY_NAMES[candidate.driver] ?? candidate.driver)
            .join(", ")}
        </div>
      ))}
    />
  );
}
