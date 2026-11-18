import { useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateProvenance } from "@t3tools/contracts/jones/jonesUpdates";
import type { JonesUpdateObservation } from "@t3tools/client-runtime/jones/updates";
import { APP_SOURCE_SHA } from "../../branding";
import type {
  UpdateEverythingMachine,
  UpdateEverythingPlan,
} from "../../components/sidebar/updateEverything.logic";
import { jonesUpdates } from "../updates/jonesUpdates";
import { useInstallPlan } from "./useInstallPlan";
import { handOffJonesTargets, partitionUpdateEverything } from "./entrypoints";

export function describeJonesBulkTargets(
  targets: readonly UpdateEverythingMachine[],
  observations: ReadonlyMap<EnvironmentId, JonesUpdateObservation>,
  target: { readonly source?: string | undefined; readonly version: string },
  laptop: { readonly environmentId: EnvironmentId | null; readonly label: string } | undefined,
) {
  const machines: NonNullable<UpdateEverythingPlan["jonesMachines"]>[number][] = targets.map(
    ({ environmentId, label, connected, serverConfig }) => {
      const observation = observations.get(environmentId);
      const state = observation?.freshness === "fresh" ? observation.state : null;
      const installedSource =
        state?.installedSource?.sha ?? serverConfig?.environment.jonesSource?.sha;
      const installedVersion = state?.currentVersion ?? serverConfig?.environment.serverVersion;
      const installed = target.source === undefined ? installedVersion : installedSource;
      return {
        environmentId,
        label,
        ...(connected && installed !== undefined
          ? installed === (target.source ?? target.version)
            ? { skipped: "Already on selected build" }
            : {}
          : { skipped: "Couldn’t verify running build" }),
      };
    },
  );
  const hasPeerUpdates = machines.some((machine) => machine.skipped === undefined);
  if (laptop !== undefined) machines.unshift({ ...laptop, hostedByThisApp: true });
  return { machines, hasPeerUpdates };
}

const bulkObservations = Atom.family((key: string) =>
  Atom.make((get) => {
    const ids = JSON.parse(key) as EnvironmentId[];
    return new Map(ids.map((id) => [id, get(jonesUpdates.observation(id))]));
  }),
);

/** Keep every Jones machine visible, including unsupported and already-current targets. */
export function useJonesBulkUpdates(
  machines: readonly UpdateEverythingMachine[],
  targetVersion: string,
  localVersion: string | null,
  desktopTargetSource?: string,
) {
  const status = useInstallPlan();
  const observations = useAtomValue(
    bulkObservations(JSON.stringify(machines.map((m) => m.environmentId).sort())),
  );
  const partition = partitionUpdateEverything(
    machines,
    (machine) =>
      machine.serverConfig?.environment.jonesSource !== undefined ||
      observations.get(machine.environmentId)?.state?.source === "jones-actions",
  );
  const targets = partition.jones.filter((machine) => !machine.hostedByThisApp);
  const laptop = machines.find((machine) => machine.hostedByThisApp);
  const runningSource =
    status.desktop?.jones?.installedSource?.sha ??
    laptop?.serverConfig?.environment.jonesSource?.sha ??
    APP_SOURCE_SHA;
  const managesLocal =
    typeof window !== "undefined" &&
    window.desktopBridge?.installPlan !== undefined &&
    localVersion !== null;
  const localTarget = managesLocal ? desktopTargetSource : undefined;
  const { machines: reviewMachines, hasPeerUpdates } = describeJonesBulkTargets(
    targets,
    observations,
    {
      source: status.native?.current?.target.sourceSha ?? localTarget ?? runningSource,
      version: targetVersion,
    },
    localTarget !== undefined && localTarget !== runningSource
      ? {
          environmentId: laptop?.environmentId ?? status.native?.laptop.environmentId ?? null,
          label: laptop?.label ?? "Laptop",
        }
      : undefined,
  );
  return {
    other: partition.other,
    reviewMachines,
    managesLocal,
    hasUpdates: hasPeerUpdates,
    handOff: (reviewedBuild?: JonesUpdateProvenance) => {
      if (reviewedBuild !== undefined)
        return handOffJonesTargets(targets, undefined, reviewedBuild);
      return targets.length === 0 &&
        (typeof window === "undefined" ||
          window.desktopBridge?.installPlan === undefined ||
          localVersion === null)
        ? Promise.resolve([])
        : handOffJonesTargets(
            targets.map(({ environmentId, label }) => ({ environmentId, label })),
          ).catch(() =>
            targets.map(({ environmentId, label }) => ({
              environmentId,
              label,
              handedOff: false,
              message:
                "Native plan handoff not confirmed. Open Install status; no Jones self-update was sent.",
            })),
          );
    },
  };
}
