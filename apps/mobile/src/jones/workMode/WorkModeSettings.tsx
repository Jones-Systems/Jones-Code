import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId } from "@t3tools/contracts";
import { useRef, useState } from "react";
import { View } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { SettingsSection } from "../../features/settings/components/SettingsSection";
import { SettingsSwitchRow } from "../../features/settings/components/SettingsSwitchRow";
import { useEnvironmentPresentation } from "../../state/presentation";
import { serverEnvironment } from "../../state/server";
import { environmentSession } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";

export function WorkModeSettings({ environmentId }: { readonly environmentId: EnvironmentId }) {
  const { isReady, presentation } = useEnvironmentPresentation(environmentId);
  const initialConfig = useAtomValue(environmentSession.initialConfigValueAtom(environmentId));
  const updateSettings = useAtomCommand(serverEnvironment.updateSettings, {
    label: "change Work mode",
  });
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const config = presentation?.serverConfig;
  const capabilities = config?.environment.capabilities;
  const supported =
    capabilities?.workMode === true && typeof config?.settings.workModeEnabled === "boolean";
  const unavailable =
    !isReady ||
    !presentation ||
    !presentation.entry.enabled ||
    presentation.connection.phase !== "connected"
      ? "Connect this environment to change Work mode."
      : !initialConfig || !config
        ? "Wait for this environment's settings to load."
        : !supported
          ? "Work mode is unavailable on this server."
          : null;
  const disabled = unavailable !== null || pending;

  async function change(enabled: boolean) {
    if (disabled || pendingRef.current) return;
    pendingRef.current = true;
    setPending(true);
    setError(null);
    try {
      const result = await updateSettings({
        environmentId,
        input: { patch: { workModeEnabled: enabled } },
      });
      if (result._tag !== "Success" || result.value.workModeEnabled !== enabled) {
        setError("Could not save Work mode. Reconnect to confirm the saved state before retrying.");
      }
    } catch {
      setError("Could not save Work mode. Reconnect to confirm the saved state before retrying.");
    } finally {
      pendingRef.current = false;
      setPending(false);
    }
  }

  return (
    <View className="gap-2">
      <SettingsSection title="Work mode">
        <SettingsSwitchRow
          icon="clock"
          label={pending ? "Work mode · Saving…" : "Work mode"}
          subtitle="Experimental: refreshes idle top-level threads after 55 minutes. Can consume usage; cache savings are not guaranteed. Applies only to this environment."
          disabled={disabled}
          value={config?.settings.workModeEnabled === true}
          onValueChange={(value) => void change(value)}
        />
      </SettingsSection>
      {error || unavailable ? (
        <Text accessibilityLiveRegion="polite" className="px-2 text-sm text-foreground-muted">
          {error ?? unavailable}
        </Text>
      ) : null}
    </View>
  );
}
