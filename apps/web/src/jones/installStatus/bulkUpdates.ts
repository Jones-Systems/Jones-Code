import { useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import type { EnvironmentId } from "@t3tools/contracts";
import type { UpdateEverythingMachine } from "../../components/sidebar/updateEverything.logic";
import { jonesUpdates } from "../updates/jonesUpdates";
import { useInstallPlan } from "./useInstallPlan";
import { handOffJonesTargets, partitionUpdateEverything, targetForDesktop } from "./entrypoints";

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
  const targets = partition.jones.filter((m) => !m.hostedByThisApp);
  const targetSource =
    status.native?.current?.target.sourceSha ?? targetForDesktop(status.desktop)?.sourceSha;
  return {
    other: partition.other,
    targets,
    managesLocal:
      typeof window !== "undefined" &&
      window.desktopBridge?.installPlan !== undefined &&
      localVersion !== null,
    hasUpdates: targets.some(
      (machine) =>
        machine.serverConfig?.environment.serverVersion !== targetVersion ||
        (targetSource !== undefined &&
          (observations.get(machine.environmentId)?.state?.installedSource?.sha ??
            machine.serverConfig?.environment.jonesSource?.sha) !== targetSource),
    ),
    handOff: () =>
      targets.length === 0 &&
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
          ),
  };
}
