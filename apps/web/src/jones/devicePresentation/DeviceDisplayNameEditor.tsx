import { useState } from "react";
import * as Schema from "effect/Schema";
import { AuthSettingsWriteScope, type EnvironmentId } from "@t3tools/contracts";
import { DeviceDisplayName, deviceDisplayLabel } from "@t3tools/contracts/jones/devicePresentation";
import { useEnvironment } from "../../state/environments";
import { useEnvironmentScope } from "../../state/session";
import { useUpdateEnvironmentSettings } from "../../hooks/useSettings";
import { MenuSub, MenuSubTrigger, MenuSubPopup } from "../../components/ui/menu";
import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";

const isDeviceDisplayName = Schema.is(DeviceDisplayName);

export function DeviceDisplayNameEditor({ environmentId }: { environmentId: EnvironmentId }) {
  const environment = useEnvironment(environmentId);
  const writable = useEnvironmentScope(environmentId, AuthSettingsWriteScope);
  const update = useUpdateEnvironmentSettings(environmentId);
  const [draft, setDraft] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const config = environment?.serverConfig;
  const current = config?.settings.deviceDisplayName ?? "";
  const value = draft ?? current;
  const lock =
    environment?.connection.phase !== "connected" || !config
      ? "Connect to this environment to edit its device display name."
      : config.environment.capabilities?.deviceDisplayName !== true
        ? "This server does not support device display names."
        : !writable
          ? "Your session cannot change this environment's settings."
          : null;
  const save = (name: string | null) => {
    if (lock || saving) return;
    setSaving(true);
    setError(null);
    update(
      { deviceDisplayName: name },
      {
        onSaved: () => {
          setSaving(false);
          setDraft(null);
        },
        onFailed: () => {
          setSaving(false);
          setError("Device name was not saved. Reconnect and try again.");
        },
      },
    );
  };
  return (
    <MenuSub>
      <MenuSubTrigger>Device display name</MenuSubTrigger>
      <MenuSubPopup>
        <form
          className="flex w-72 flex-col gap-3 p-3"
          onKeyDown={(event) => event.stopPropagation()}
          onSubmit={(event) => {
            event.preventDefault();
            const name = value.trim();
            if (name && !isDeviceDisplayName(name)) {
              setError("Use 1–64 characters without control or formatting characters.");
              return;
            }
            save(name || null);
          }}
        >
          <p className="text-sm">Shown to all clients connected to this device.</p>
          <p className="break-all text-xs text-muted-foreground">
            Server: {config?.environment.label ?? "Unavailable"}
            <br />
            Connection: {environment?.label ?? "Unavailable"}
            <br />
            ID: {environmentId}
          </p>
          <Input
            aria-label="Device display name"
            maxLength={64}
            value={value}
            placeholder={deviceDisplayLabel(
              environment?.label ?? "Unknown environment",
              config?.environment.platform.machine,
            )}
            disabled={lock !== null || saving}
            onChange={(event) => setDraft(event.target.value)}
          />
          <p className="text-xs text-muted-foreground">
            Current: {environment?.displayLabel ?? "Unavailable"}
          </p>
          {lock ? (
            <p role="status" className="text-xs">
              {lock}
            </p>
          ) : null}
          {error ? (
            <p role="alert" className="text-xs">
              {error}
            </p>
          ) : null}
          <div className="flex gap-2">
            <Button type="submit" size="sm" disabled={lock !== null || saving}>
              {saving ? "Saving…" : "Save"}
            </Button>
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={lock !== null || saving || !current}
              onClick={() => save(null)}
            >
              Reset to automatic
            </Button>
          </div>
        </form>
      </MenuSubPopup>
    </MenuSub>
  );
}
