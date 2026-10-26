import { type EnvironmentId, type ServerProvider } from "@t3tools/contracts";
import { type ProviderInstanceEntry } from "../../providerInstances";
import {
  hasCompleteProviderWorkspaceSnapshot,
  hasCurrentProviderWorkspaceSnapshot,
} from "@t3tools/client-runtime/providerSkills";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { useEffect, useEffectEvent, useState } from "react";
import { useStableCallback } from "../../hooks/useStableCallback";

// Jones retry policy gives each provider/workspace selection its own failure budget.
import {
  createProviderWorkspaceRefresh,
  providerWorkspaceRefreshKey,
} from "@t3tools/client-runtime/jones/provider-workspace-refresh";

/**
 * Keeps the selected provider's workspace snapshot fresh for the composer's
 * cwd. Returns a check to run when the user types.
 */
export function useProviderWorkspaceSnapshotRefresh(input: {
  readonly environmentId: EnvironmentId;
  readonly gitCwd: string | null;
  readonly selectedProviderEntry: ProviderInstanceEntry | undefined;
  readonly selectedProviderStatus: ServerProvider | null;
}) {
  const { environmentId, gitCwd, selectedProviderEntry, selectedProviderStatus } = input;
  const refreshProviders = useAtomCommand(serverEnvironment.refreshProviders, {
    reportFailure: false,
  });
  const [workspaceRefresh] = useState(createProviderWorkspaceRefresh);
  const [workspaceRefreshRetry, setWorkspaceRefreshRetry] = useState(0);
  const workspaceRefreshScopeKey =
    gitCwd && selectedProviderEntry
      ? providerWorkspaceRefreshKey(environmentId, selectedProviderEntry.instanceId, gitCwd)
      : null;
  useEffect(() => () => workspaceRefresh.pause(), [workspaceRefresh]);
  const refreshWorkspaceSnapshotIfStale = () => {
    if (workspaceRefreshScopeKey === null || !gitCwd || !selectedProviderEntry) {
      workspaceRefresh.update(null);
      return;
    }
    workspaceRefresh.update({
      key: workspaceRefreshScopeKey,
      hasSnapshot: hasCompleteProviderWorkspaceSnapshot(selectedProviderStatus, gitCwd),
      isCurrent: hasCurrentProviderWorkspaceSnapshot(selectedProviderStatus, gitCwd, Date.now()),
      retryPending:
        selectedProviderStatus?.workspaceSnapshots?.some(
          (snapshot) => snapshot.cwd === gitCwd && snapshot.slashCommandsPending === true,
        ) ?? false,
      onRetry: () => setWorkspaceRefreshRetry((retry) => retry + 1),
      refresh: () =>
        refreshProviders({
          environmentId,
          input: { instanceId: selectedProviderEntry.instanceId, cwd: gitCwd },
        }).then(
          (result) =>
            result._tag === "Success" &&
            hasCompleteProviderWorkspaceSnapshot(
              result.value.providers.find(
                (provider) => provider.instanceId === selectedProviderEntry.instanceId,
              ),
              gitCwd,
            ),
        ),
    });
  };
  const refreshWorkspaceSnapshotOnChange = useEffectEvent(refreshWorkspaceSnapshotIfStale);
  useEffect(() => {
    refreshWorkspaceSnapshotOnChange();
    // The Effect Event reads current values; these keys deliberately trigger freshness checks.
    // oxlint-disable-next-line react/exhaustive-effect-dependencies
  }, [workspaceRefreshScopeKey, selectedProviderStatus, workspaceRefreshRetry]);
  return useStableCallback(refreshWorkspaceSnapshotIfStale);
}
