import { AuthProvidersManageScope, type EnvironmentId } from "@t3tools/contracts";
import { useSyncExternalStore } from "react";
import {
  collectProviderUpdateCandidates,
  providerUpdateNotificationKey,
} from "@t3tools/client-runtime/jones/provider-updates";
import { appAtomRegistry } from "~/rpc/atomRegistry";
import { environmentPresentations } from "~/state/presentation";
import { readEnvironmentScope, useEnvironmentsWithScope } from "~/state/session";
import { useEnvironmentProviders, useEnvironments } from "~/state/environments";
import { serverEnvironment } from "~/state/server";
import { useAtomCommand } from "~/state/use-atom-command";
import { stackedThreadToast, toastManager } from "~/components/ui/toast";
import {
  buildLocalEnvironmentUpdateGroups,
  environmentGroupsWithUpdates,
  getProviderUpdateRunToastView,
} from "~/components/ProviderUpdateLaunchNotification.logic";
import { createProviderUpdateController } from "./providerUpdateController";

const controller = createProviderUpdateController({
  readCurrent: (environmentId) => {
    const current = appAtomRegistry.get(environmentPresentations.presentationAtom(environmentId));
    return {
      connected: current?.entry.enabled === true && current.connection.phase === "connected",
      permitted: readEnvironmentScope(environmentId, AuthProvidersManageScope),
      providers:
        appAtomRegistry
          .get(environmentPresentations.providersByEnvironmentAtom)
          .get(environmentId) ?? [],
    };
  },
});

export function useJonesProviderUpdates() {
  const { environments } = useEnvironments();
  const providersByEnvironment = useEnvironmentProviders();
  const permitted = useEnvironmentsWithScope(environments, AuthProvidersManageScope);
  const updateProvider = useAtomCommand(serverEnvironment.updateProvider, { reportFailure: false });
  const attempts = useSyncExternalStore(
    controller.subscribe,
    controller.getSnapshot,
    controller.getSnapshot,
  );
  const { groups, isAnySettling } = buildLocalEnvironmentUpdateGroups(
    environments.map((environment) => ({
      environmentId: environment.environmentId,
      label: environment.label,
      isPrimary: environment.entry.target._tag === "PrimaryConnectionTarget",
      connectionState:
        environment.entry.enabled && environment.connection.phase === "connected"
          ? "ready"
          : environment.entry.enabled &&
              (environment.connection.phase === "connecting" ||
                environment.connection.phase === "reconnecting")
            ? "connecting"
            : "disconnected",
      providers: providersByEnvironment.get(environment.environmentId) ?? [],
    })),
  );
  const machines = environmentGroupsWithUpdates(groups);
  const updateEnvironment = (environmentId: EnvironmentId) => {
    const machine =
      machines.find((entry) => entry.environmentId === environmentId) ??
      attempts.get(environmentId)?.machine;
    return machine ? controller.start(machine, updateProvider) : Promise.resolve([]);
  };
  const updateAll = async ({ quiet = false }: { readonly quiet?: boolean } = {}) => {
    const runs = (
      await Promise.all(machines.map((machine) => controller.start(machine, updateProvider)))
    ).flat();
    const view = quiet ? null : getProviderUpdateRunToastView(runs);
    if (view)
      toastManager.add(
        stackedThreadToast({
          ...view,
          description: <span className="whitespace-pre-line">{view.description}</span>,
        }),
      );
    return runs;
  };
  const noticeGroups = groups.flatMap((group) => {
    const candidates = collectProviderUpdateCandidates(group.providers);
    const attempt = attempts.get(group.environmentId);
    const environment = environments.find((entry) => entry.environmentId === group.environmentId);
    return candidates.length > 0 || attempt
      ? [
          {
            ...group,
            allCandidates: candidates,
            attempt,
            canManage: permitted.has(group.environmentId),
            connected:
              environment?.entry.enabled === true && environment.connection.phase === "connected",
          },
        ]
      : [];
  });
  const noticeKey = noticeGroups
    .map(
      (group) =>
        `${group.environmentId}=${providerUpdateNotificationKey(group.allCandidates) ?? ""}`,
    )
    .filter((key) => !key.endsWith("="))
    .sort()
    .join("|");
  const completed = [...attempts.values()].flatMap((attempt) => attempt.runs);
  return {
    machines,
    isAnySettling,
    groups: noticeGroups,
    noticeKey,
    updateAll,
    updateEnvironment,
    attempts,
    completed,
    canManage: machines.some((machine) => permitted.has(machine.environmentId)),
  };
}
