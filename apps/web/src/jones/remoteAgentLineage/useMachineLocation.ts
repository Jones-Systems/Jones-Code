import type { EnvironmentId } from "@t3tools/contracts";
import { isElectron } from "../../env";
import { useEnvironment } from "../../state/environments";
import { isClientLocalMachine, resolveMachineIdentity } from "./model";

export function useMachineLocation(environmentId: EnvironmentId, fallbackLabel?: string | null) {
  const environment = useEnvironment(environmentId);
  if (isClientLocalMachine(environment?.entry?.target, isElectron)) return null;
  const machine = resolveMachineIdentity(
    environment?.label ?? fallbackLabel,
    environment?.serverConfig?.environment.platform.machine,
  );
  return { ...machine, shortLabel: environment?.displayLabel ?? machine.shortLabel };
}
