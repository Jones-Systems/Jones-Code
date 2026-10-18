import { useAtomValue } from "@effect/atom-react";
import { type DesktopCompanionConfigureInput, type EnvironmentId } from "@t3tools/contracts";
import { useState } from "react";
import { useEnvironments } from "../../state/environments";
import { useSettingsScope } from "../../components/settings/SettingsScopeContext";
import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectPopup,
  SelectItem,
} from "../../components/ui/select";
import { toastManager } from "../../components/ui/toast";
import { companionStateAtom } from "./state.ts";
import { BrowserHostSelect } from "./HostControls.tsx";

export function BrowserHostsSettings() {
  const state = useAtomValue(companionStateAtom);
  const { environments } = useEnvironments();
  const scope = useSettingsScope();
  const [pending, setPending] = useState(false);
  const configure = async (change: Partial<DesktopCompanionConfigureInput>) => {
    const bridge = window.desktopBridge?.previewCompanion;
    if (!bridge || !state) return;
    setPending(true);
    try {
      await bridge.configure({
        enabled: state.config.enabled,
        environmentId: state.config.environmentId,
        label: state.config.label,
        browserOnly: state.config.browserOnly,
        ...change,
      });
    } catch {
      toastManager.add({ type: "error", title: "Could not update browser host" });
    } finally {
      setPending(false);
    }
  };
  return (
    <div className="space-y-3 border-b py-4" data-browser-host-settings>
      <h3 className="text-sm font-medium">Browser hosts</h3>
      <p className="text-xs text-muted-foreground">
        Choose where new preview tabs run. Existing tabs stay bound to their original host.
      </p>
      {scope.environments.map((environment) => (
        <div className="flex items-center justify-between gap-3" key={environment.environmentId}>
          <span className="text-sm">{environment.label}</span>
          <BrowserHostSelect environmentId={environment.environmentId} />
        </div>
      ))}
      {state ? (
        <div className="space-y-3 rounded-md border p-3">
          <div className="flex items-center justify-between gap-2">
            <span className="text-sm">
              Use this desktop as a browser host · {state.status.replaceAll("_", " ")}
            </span>
            <Button
              size="sm"
              variant="outline"
              disabled={pending || (!state.config.enabled && !state.config.environmentId)}
              onClick={() => void configure({ enabled: !state.config.enabled })}
            >
              {state.config.enabled ? "Disable connection" : "Enable connection"}
            </Button>
          </div>
          <Input
            aria-label="Browser host label"
            defaultValue={state.config.label}
            key={state.config.hostId + state.config.label}
            maxLength={64}
            disabled={pending}
            onBlur={(event) => {
              const label = event.target.value.trim();
              if (label && label !== state.config.label) void configure({ label });
            }}
          />
          <Select
            disabled={pending}
            value={state.config.environmentId ?? "unpaired"}
            onValueChange={(value) => {
              if (value)
                void configure({
                  environmentId: value === "unpaired" ? null : (value as EnvironmentId),
                  ...(value === "unpaired" ? { enabled: false } : {}),
                });
            }}
          >
            <SelectTrigger aria-label="Browser host environment">
              <SelectValue>
                {environments.find(
                  (environment) => environment.environmentId === state.config.environmentId,
                )?.label ?? "Choose saved environment"}
              </SelectValue>
            </SelectTrigger>
            <SelectPopup>
              <SelectItem value="unpaired">Unpaired</SelectItem>
              {environments
                .filter((environment) => environment.entry.enabled)
                .map((environment) => (
                  <SelectItem value={environment.environmentId} key={environment.environmentId}>
                    {environment.label}
                  </SelectItem>
                ))}
            </SelectPopup>
          </Select>
          <label className="flex items-center gap-2 text-sm">
            <input
              type="checkbox"
              checked={state.config.browserOnly}
              disabled={pending || state.browserOnlyLocked === true}
              onChange={(event) => void configure({ browserOnly: event.target.checked })}
            />
            {state.browserOnlyLocked
              ? "Browser-only desktop (fixed for this app)"
              : "Browser-only desktop (requires restart)"}
          </label>
          <p className="text-xs text-muted-foreground">
            Browser-only mode keeps the local backend off, including while disconnected or unpaired.
            Downloads, uploads, recording, profile clearing and pop-up sign-in are unsupported.
          </p>
          <Button
            size="sm"
            variant="outline"
            disabled={pending || !state.config.enabled}
            onClick={() =>
              void window.desktopBridge?.previewCompanion?.retry().catch(() => undefined)
            }
          >
            Retry connection
          </Button>
        </div>
      ) : null}
    </div>
  );
}
