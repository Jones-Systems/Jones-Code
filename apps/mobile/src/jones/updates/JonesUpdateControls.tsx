import { jonesBuildLabels, type JonesBuildRef } from "@t3tools/shared/jones/buildTime";
import { JonesBuildVersion } from "./JonesBuildVersion";
import {
  jonesCandidatePresentation,
  jonesUpdateActionError,
  jonesUpdatePresentation,
} from "@t3tools/client-runtime/jones/updates";
import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId } from "@t3tools/contracts";
import { useState } from "react";
import { View } from "react-native";
import { AppText as Text } from "../../components/ui/AppText";
import { jonesUpdates } from "./jonesUpdates";
import { useAtomCommand } from "../../state/use-atom-command";
import { SettingsSection } from "../../features/settings/components/SettingsSection";
import { SettingsActionRow } from "../../features/settings/components/SettingsActionRow";

export function JonesUpdateControls({
  environmentId,
  allowed,
}: {
  readonly environmentId: EnvironmentId;
  readonly allowed: boolean;
}) {
  const observation = useAtomValue(jonesUpdates.observation(environmentId));
  const state = observation.state;
  const action = useAtomCommand(jonesUpdates.action);
  const [pending, setPending] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  if (state === null)
    return observation.freshness === "stale" ? (
      <Text className="p-4 text-sm text-foreground-muted">{observation.message}</Text>
    ) : null;
  const run = async (input: Parameters<typeof action>[0]["input"]) => {
    setPending(true);
    setActionError(null);
    await (async () => {
      setActionError(jonesUpdateActionError(await action({ environmentId, input })));
    })().finally(() => setPending(false));
  };
  const presentation = jonesUpdatePresentation(state);
  const busy = pending || presentation.busy || observation.freshness !== "fresh";
  const provenance = state.provenance;
  const candidate = jonesCandidatePresentation(state);
  const builds: Array<{ title: string; build: JonesBuildRef }> = [];
  if (state.currentVersion !== undefined)
    builds.push({
      title: "Installed",
      build: {
        version: state.currentVersion,
        startedAt: state.currentBuildStartedAt,
        sourceSha: state.installedSource?.sha,
      },
    });
  if (candidate !== undefined)
    builds.push({
      title: candidate.label,
      build: {
        version: candidate.version ?? "Version not reported",
        startedAt: candidate.buildStartedAt,
        sourceSha: candidate.sourceSha,
      },
    });
  const labels = jonesBuildLabels(builds.map(({ build }) => build));
  return (
    <SettingsSection title="Jones main builds">
      <View className="gap-2 p-4">
        {builds.map(({ title, build }, index) => (
          <JonesBuildVersion key={title} title={title} build={build} label={labels[index]!} />
        ))}
        <Text className="text-sm text-foreground-muted">{presentation.message}</Text>
        {observation.freshness !== "fresh" ? (
          <Text className="text-sm text-foreground-muted">{observation.message}</Text>
        ) : null}
        {actionError ? (
          <Text accessibilityRole="alert" className="text-sm text-foreground-muted">
            {actionError}
          </Text>
        ) : null}
        {state.updateId ? (
          <Text className="text-sm text-foreground-muted">Update {state.updateId}</Text>
        ) : null}
        {presentation.outcomeMessage ? (
          <View className="gap-1">
            <Text className="text-sm text-foreground-muted">{presentation.outcomeMessage}</Text>
            <Text selectable className="text-sm text-foreground-muted">
              {state.outcome?.fromVersion} → {state.outcome?.targetVersion}
            </Text>
          </View>
        ) : null}
        {state.migrationPlan ? (
          <Text className="text-sm text-foreground-muted">
            Pending migrations: {state.migrationPlan.pendingUpstream.length} upstream,{" "}
            {state.migrationPlan.pendingJones.length} Jones
          </Text>
        ) : null}
        {state.recovery ? (
          <Text className="text-sm text-foreground-muted">
            Recovery {state.recovery.method} · {(state.recovery.bytes / 1024 ** 3).toFixed(2)} GiB
          </Text>
        ) : null}
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
          disabled={
            !allowed || busy || !state.capability.install || state.currentVersion === undefined
          }
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
