// @effect-diagnostics globalDate:off - Mounted provider scans use wall-clock snapshot expiry.
import { Atom } from "effect/reactivity";
import { runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import { hasCompleteProviderWorkspaceSnapshot } from "@t3tools/client-runtime/providerSkills";
import {
  isProviderWorkspaceSnapshotCurrent,
  isProviderWorkspaceSnapshotPending,
  type EnvironmentId,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import {
  createProviderWorkspaceRefresh,
  providerWorkspaceRefreshKey,
} from "@t3tools/client-runtime/jones/provider-workspace-refresh";
import { serverEnvironment } from "../../state/server";

const scans = Atom.family((key: string) => {
  const [environmentId, instanceId, cwd] = JSON.parse(key) as [
    EnvironmentId,
    ProviderInstanceId,
    string,
  ];
  const policy = createProviderWorkspaceRefresh();
  return Atom.make((get) => {
    const providers = serverEnvironment.providersValueAtom(environmentId);
    const update = () => {
      const provider = get.registry
        .get(providers)
        ?.find((entry) => entry.instanceId === instanceId);
      if (!provider) {
        policy.update(null);
        return;
      }
      const snapshot = provider.workspaceSnapshots?.find((entry) => entry.cwd === cwd);
      policy.update({
        key,
        hasSnapshot: hasCompleteProviderWorkspaceSnapshot(provider, cwd),
        isCurrent: Boolean(
          snapshot &&
          !isProviderWorkspaceSnapshotPending(snapshot) &&
          isProviderWorkspaceSnapshotCurrent(snapshot, Date.now()),
        ),
        retryPending: Boolean(snapshot && isProviderWorkspaceSnapshotPending(snapshot)),
        onRetry: update,
        refresh: async () => {
          const result = await runAtomCommand(
            get.registry,
            serverEnvironment.refreshProviders,
            { environmentId, input: { instanceId, cwd } },
            { reportFailure: false },
          );
          // Digest negotiation may intentionally omit the provider array in the reply.
          return (
            result._tag === "Success" &&
            hasCompleteProviderWorkspaceSnapshot(
              get.registry.get(providers)?.find((entry) => entry.instanceId === instanceId),
              cwd,
            )
          );
        },
      });
    };
    get.subscribe(providers, update, { immediate: true });
    get.addFinalizer(() => policy.pause());
  });
});

export function mobileProviderWorkspaceSnapshotRefresh(input: {
  readonly environmentId: EnvironmentId;
  readonly instanceId: ProviderInstanceId;
  readonly cwd: string;
}) {
  return scans(providerWorkspaceRefreshKey(input.environmentId, input.instanceId, input.cwd));
}
