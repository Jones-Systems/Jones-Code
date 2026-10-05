import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId } from "@t3tools/contracts";
import { useState } from "react";
import { View } from "react-native";
import { AppText as Text } from "../../components/AppText";
import { jonesUpdates } from "../../state/jonesUpdates";
import { useAtomCommand } from "../../state/use-atom-command";
import { SettingsSection } from "./components/SettingsSection";
import { SettingsActionRow } from "./components/SettingsActionRow";

export function JonesUpdateControls({
  environmentId,
  allowed,
}: {
  readonly environmentId: EnvironmentId;
  readonly allowed: boolean;
}) {
  const state = useAtomValue(jonesUpdates.value(environmentId));
  const action = useAtomCommand(jonesUpdates.action);
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
    ["checking", "downloading", "verifying", "preparing", "installing"].includes(state.phase);
  const provenance = state.provenance;
  return (
    <SettingsSection title="Jones main builds">
      <View className="gap-2 p-4">
        <Text className="text-sm text-foreground-muted">{state.message ?? state.phase}</Text>
      </View>
      <SettingsActionRow
        icon="arrow.clockwise"
        label="Check for builds"
        disabled={!allowed || busy || !state.capability.check}
        onPress={() => void run({ action: "check" })}
      />
      {state.stagedHandle !== undefined ? (
        <SettingsActionRow
          icon="arrow.up.circle"
          label="Install downloaded build"
          disabled={!allowed || busy || !state.capability.install}
          onPress={() => {
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
        />
      ) : provenance !== undefined ? (
        <SettingsActionRow
          icon="arrow.down.circle"
          label="Download"
          disabled={!allowed || busy || !state.capability.download || state.phase !== "available"}
          onPress={() =>
            void run({
              action: "download",
              input: { artifactId: provenance.artifactId, sourceSha: provenance.sourceSha },
            })
          }
        />
      ) : null}
    </SettingsSection>
  );
}
