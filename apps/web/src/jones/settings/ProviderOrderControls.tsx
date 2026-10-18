import {
  AuthProvidersManageScope,
  defaultInstanceIdForDriver,
  type EnvironmentId,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { ArrowDownIcon, ArrowUpIcon } from "lucide-react";
import { useRef, useState } from "react";
import { Button } from "~/components/ui/button";
import { toastManager } from "~/components/ui/toast";
import { SettingsRow } from "~/components/settings/settingsLayout";
import { useEnvironmentSettings } from "~/hooks/useSettings";
import { useEnvironmentScope } from "~/state/session";
import { serverEnvironment } from "~/state/server";
import { useAtomCommand } from "~/state/use-atom-command";
import { moveProviderInstance, type ProviderOrderDirection } from "./providerOrder";

export function ProviderOrderControls({
  environmentId,
  instanceId,
  readOnly,
}: {
  readonly environmentId: EnvironmentId;
  readonly instanceId: ProviderInstanceId;
  readonly readOnly: boolean;
}) {
  const { providerInstances } = useEnvironmentSettings(environmentId);
  const canManageProviders = useEnvironmentScope(environmentId, AuthProvidersManageScope);
  const persist = useAtomCommand(serverEnvironment.updateSettings, { reportFailure: false });
  const pending = useRef(false);
  const [isSaving, setIsSaving] = useState(false);
  const instance = providerInstances[instanceId];
  if (!instance || instanceId === defaultInstanceIdForDriver(instance.driver)) return null;
  const displayName = instance.displayName?.trim() || String(instance.driver);

  const move = async (direction: ProviderOrderDirection) => {
    if (readOnly || !canManageProviders || pending.current) return;
    const next = moveProviderInstance(providerInstances, instanceId, direction);
    if (next === providerInstances) return;
    pending.current = true;
    setIsSaving(true);
    await (async () => {
      const result = await persist({
        environmentId,
        input: { patch: { providerInstances: next } },
      });
      if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
        const error = squashAtomCommandFailure(result);
        toastManager.add({
          type: "error",
          title: "Could not reorder providers",
          description: error instanceof Error ? error.message : "The settings update failed.",
        });
      }
    })().finally(() => {
      pending.current = false;
      setIsSaving(false);
    });
  };

  return (
    <SettingsRow
      title="Provider order"
      description="Move this provider within its provider type. The default stays first."
      control={
        <div className="flex items-center gap-1" aria-busy={isSaving}>
          {(["up", "down"] as const).map((direction) => (
            <Button
              key={direction}
              size="icon-xs"
              variant="ghost-muted"
              aria-label={`Move ${displayName} ${direction}`}
              title={`Move ${displayName} ${direction}`}
              disabled={
                readOnly ||
                !canManageProviders ||
                isSaving ||
                moveProviderInstance(providerInstances, instanceId, direction) === providerInstances
              }
              onClick={() => void move(direction)}
            >
              {direction === "up" ? <ArrowUpIcon /> : <ArrowDownIcon />}
            </Button>
          ))}
        </div>
      }
    />
  );
}
