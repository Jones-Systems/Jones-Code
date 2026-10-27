import { jonesBuildLabels, type JonesBuildRef } from "@t3tools/shared/jones/buildTime";
import { JonesBuildVersion } from "./JonesBuildVersion";
import {
  jonesCandidatePresentation,
  jonesFollowerHold,
  jonesFollowerSummary,
  jonesUpdatePresentation,
} from "@t3tools/client-runtime/jones/updates";
import { RegistryContext, useAtomValue } from "@effect/atom-react";
import type { EnvironmentId } from "@t3tools/contracts";
import { useContext, useEffect, useLayoutEffect, useMemo, useRef, useSyncExternalStore } from "react";
import * as Schema from "effect/Schema";
import { JonesOperationReceipt, JonesOperationReadResult, JonesRecoveryPreflight } from "@t3tools/contracts/jones/jonesOperations";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { jonesTargetStatus } from "@t3tools/client-runtime/jones/updates/target-status";
import { View } from "react-native";
import { AppText as Text } from "../../components/ui/AppText";
import { createMobileJonesActions, jonesUpdates, mobileJonesObservation, mobileJonesUpdateSession } from "./jonesUpdates";
import { useAtomCommand } from "../../state/use-atom-command";
import { SettingsSection } from "../../features/settings/components/SettingsSection";
import { SettingsActionRow } from "../../features/settings/components/SettingsActionRow";

const decodeOperationReceipt = Schema.decodeUnknownSync(JonesOperationReceipt);
const decodeRecoveryPreflight = Schema.decodeUnknownSync(JonesRecoveryPreflight);
const decodeOperationReadResult = Schema.decodeUnknownSync(JonesOperationReadResult);

export function JonesUpdateControls({
  environmentId,
  allowed,
}: {
  readonly environmentId: EnvironmentId;
  readonly allowed: boolean;
}) {
  const observed = useAtomValue(jonesUpdates.observation(environmentId));
  const registry = useContext(RegistryContext);
  const action = useAtomCommand(jonesUpdates.action, { reportFailure: false, reportDefect: false });
  const operation = useAtomCommand(jonesUpdates.operation, { reportFailure: false, reportDefect: false });
  const stop = useAtomCommand(jonesUpdates.stop, { reportFailure: false, reportDefect: false });
  const session = mobileJonesUpdateSession(environmentId);
  const saved = useSyncExternalStore(session.subscribe, session.getSnapshot, session.getSnapshot);
  const authority = useRef({ environmentId, allowed, mounted: false });
  // oxlint-disable-next-line t3code/react-effect-budget -- Native Alert callbacks must see committed host/permission authority and lose it synchronously on unmount.
  useLayoutEffect(() => {
    authority.current = { environmentId, allowed, mounted: true };
    return () => { authority.current.mounted = false; };
  }, [environmentId, allowed]);
  // oxlint-disable-next-line t3code/react-effect-budget -- Fresh subscription receipts reconcile the external session retained across screen remounts and lost replies.
  useEffect(() => {
    const receipt = observed.state?.operation;
    if (observed.freshness === "fresh" && receipt?.environmentId === environmentId) session.observe(receipt);
  }, [observed, session, environmentId]);
  const actions = useMemo(() => createMobileJonesActions({
    environmentId,
    readLatest: () => ({
      observation: registry.get(jonesUpdates.observation(environmentId)),
      canOperate: authority.current.mounted && authority.current.environmentId === environmentId && authority.current.allowed,
    }),
    transport: {
      legacy: async (input) => commandValue(await action({ environmentId, input })),
      start: async (input) => decodeOperationReceipt(commandValue(await operation({ environmentId, input: { action: "operationStart", input } }))),
      preflight: async (input) => decodeRecoveryPreflight(commandValue(await operation({ environmentId, input: { action: "recoveryPreflight", input } }))),
      read: async (operationId) => decodeOperationReadResult(commandValue(await operation({ environmentId, input: { action: "operationRead", input: { environmentId, operationIds: [operationId] } } }))),
      stop: async (input) => decodeOperationReceipt(commandValue(await stop({ environmentId, input: { action: "operationStop", input } }))),
    },
  }), [environmentId, registry, action, operation, stop]);
  const observation = mobileJonesObservation(observed, saved);
  const state = observation.state;
  const presentation = state === null ? undefined : jonesUpdatePresentation(state);
  const status = jonesTargetStatus(observation, {
    canOperate: allowed && state?.environmentId === environmentId,
    targetSource: state?.provenance?.sourceSha,
    capability: observation.freshness === "fresh" ? state?.capability ?? null : null,
  });
  const displayedOperation = state?.operation ?? saved.receipt;
  const outstanding = status.operation !== undefined && ["accepted", "running", "stop-requested", "adopting", "handed-off", "held"].includes(status.operation.phase);
  const busy = saved.busy || saved.uncertainty !== undefined || observation.freshness !== "fresh";
  const followerHold = state === null ? undefined : jonesFollowerHold(state);
  const followerSummary = state === null ? undefined : jonesFollowerSummary(state);
  const candidate = state === null ? undefined : jonesCandidatePresentation(state);
  const builds: Array<{ title: string; build: JonesBuildRef }> = [];
  if (state?.currentVersion !== undefined)
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
        <Text className="text-sm text-foreground-muted">{status.headline}</Text>
        {status.check !== undefined ? (
          <Text className="text-sm text-foreground-muted">{status.check.text}</Text>
        ) : null}
        {displayedOperation !== undefined ? (
          <>
            <Text selectable className="text-sm text-foreground-muted">
              Operation {displayedOperation.operationId} · {displayedOperation.phase}
            </Text>
            {displayedOperation.message ? <Text className="text-sm text-foreground-muted">{displayedOperation.message}</Text> : null}
            {displayedOperation.stop !== undefined ? (
              <Text className="text-sm text-foreground-muted">Stop: {displayedOperation.stop.result ?? "requested"}</Text>
            ) : null}
            {displayedOperation.correlation !== undefined ? (
              <Text className="text-sm text-foreground-muted">Part of the Laptop’s update plan</Text>
            ) : null}
          </>
        ) : null}
        {saved.uncertainty !== undefined ? (
          <Text accessibilityRole="alert" selectable className="text-sm text-foreground-muted">
            Not confirmed · Operation {saved.request?.operationId ?? saved.receipt?.operationId ?? saved.operationId}. Nothing will be resent.
          </Text>
        ) : null}
        {followerSummary !== undefined ? (
          <Text className="text-sm text-foreground-muted">
            {followerHold === undefined ? followerSummary : followerHold}
          </Text>
        ) : null}
        {observation.freshness !== "fresh" ? (
          <Text className="text-sm text-foreground-muted">{observation.message}</Text>
        ) : null}
        {saved.message ? (
          <Text accessibilityRole="alert" className="text-sm text-foreground-muted">
            {saved.message}
          </Text>
        ) : null}
        {state?.updateId ? (
          <Text className="text-sm text-foreground-muted">Update {state.updateId}</Text>
        ) : null}
        {presentation?.outcomeMessage ? (
          <View className="gap-1">
            <Text className="text-sm text-foreground-muted">{presentation.outcomeMessage}</Text>
            <Text selectable className="text-sm text-foreground-muted">
              {state?.outcome?.fromVersion} → {state?.outcome?.targetVersion}
            </Text>
          </View>
        ) : null}
        {state?.migrationPlan ? (
          <Text className="text-sm text-foreground-muted">
            Pending migrations: {state.migrationPlan.pendingUpstream.length} upstream,{" "}
            {state.migrationPlan.pendingJones.length} Jones
          </Text>
        ) : null}
        {state?.recovery ? (
          <Text className="text-sm text-foreground-muted">
            Recovery {state.recovery.method} · {(state.recovery.bytes / 1024 ** 3).toFixed(2)} GiB
          </Text>
        ) : null}
      </View>
      {state !== null ? (
        <SettingsActionRow
          icon="arrow.clockwise"
          label="Check for builds"
          disabled={!allowed || busy || outstanding || presentation?.busy || !state.capability.check}
          onPress={() => void actions.check()}
        />
      ) : null}
      {state?.capability.operations === 1 && (saved.request !== undefined || state.operation !== undefined) ? (
        <SettingsActionRow
          icon="arrow.clockwise"
          label="Read operation status"
          disabled={observation.freshness !== "fresh" || saved.reading}
          onPress={() => void actions.read()}
        />
      ) : null}
      {state?.capability.operations === 1 && state.operation !== undefined && state.operation.action.kind !== "install" && outstanding ? (
        <SettingsActionRow
          icon="stop.circle"
          label={state.operation.phase === "stop-requested" || saved.stopping ? "Stop requested…" : "Stop download"}
          disabled={!status.actions.stop || saved.stopping || saved.stopId !== undefined || state.operation.stop !== undefined || saved.uncertainty !== undefined}
          onPress={() => void actions.stop()}
        />
      ) : null}
      {state?.stagedHandle !== undefined ? (
        <SettingsActionRow
          icon="arrow.up.circle"
          label="Install downloaded build"
          disabled={busy || !status.actions.install || followerHold !== undefined}
          onPress={() => void actions.run("install")}
        />
      ) : state?.provenance !== undefined ? (
        <SettingsActionRow
          icon="arrow.down.circle"
          label="Download"
          disabled={busy || !status.actions.download || followerHold !== undefined}
          onPress={() => void actions.run("download")}
        />
      ) : null}
    </SettingsSection>
  );
}

function commandValue<A>(result: AtomCommandResult<A, unknown>): A {
  if (result._tag !== "Success") throw new Error("Host request was not confirmed");
  return result.value;
}
