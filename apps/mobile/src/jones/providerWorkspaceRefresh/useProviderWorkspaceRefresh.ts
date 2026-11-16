import {
  createProviderWorkspaceRefresh,
  providerWorkspaceRefreshKey,
} from "@t3tools/client-runtime/jones/provider-workspace-refresh";
import { hasCompleteProviderWorkspaceSnapshot } from "@t3tools/client-runtime/providerSkills";
import {
  isProviderWorkspaceSnapshotCurrent,
  type EnvironmentId,
  type ServerProvider,
} from "@t3tools/contracts";
import { useEffect, useState } from "react";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";

export function useProviderWorkspaceRefresh(input: {
  readonly environmentId: EnvironmentId | null;
  readonly projectCwd: string | null;
  readonly provider: ServerProvider | null;
  readonly draftMessage: string;
}) {
  const [policy] = useState(createProviderWorkspaceRefresh);
  const [retry, setRetry] = useState(0);
  const refreshProviders = useAtomCommand(serverEnvironment.refreshProviders, {
    reportFailure: false,
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Own the retry timer for the mounted composer while retaining its per-selection budgets across effect rebuilds.
  useEffect(() => () => policy.pause(), [policy]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Keep the selected server workspace snapshot fresh as external drafts and provider observations change.
  useEffect(() => {
    const { environmentId, projectCwd, provider } = input;
    if (!environmentId || !projectCwd || !provider) {
      policy.update(null);
      return;
    }
    const snapshot = provider.workspaceSnapshots?.find((entry) => entry.cwd === projectCwd);
    policy.update({
      key: providerWorkspaceRefreshKey(environmentId, provider.instanceId, projectCwd),
      hasSnapshot: hasCompleteProviderWorkspaceSnapshot(provider, projectCwd),
      isCurrent: Boolean(
        snapshot &&
        !snapshot.slashCommandsPending &&
        isProviderWorkspaceSnapshotCurrent(snapshot, Date.now()),
      ),
      retryPending: snapshot?.slashCommandsPending === true,
      onRetry: () => setRetry((previous) => previous + 1),
      refresh: () =>
        refreshProviders({
          environmentId,
          input: { instanceId: provider.instanceId, cwd: projectCwd },
        }).then(
          (result) =>
            result._tag === "Success" &&
            hasCompleteProviderWorkspaceSnapshot(
              result.value.providers.find((entry) => entry.instanceId === provider.instanceId),
              projectCwd,
            ),
        ),
    });
    // oxlint-disable-next-line react/exhaustive-effect-dependencies -- Typing and timer wakeups deliberately recheck freshness for the same selection.
  }, [input, policy, refreshProviders, retry]);
}
