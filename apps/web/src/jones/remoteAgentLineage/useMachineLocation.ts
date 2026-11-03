import type { EnvironmentId } from "@t3tools/contracts";
import { isElectron } from "../../env";
import { useEnvironment } from "../../state/environments";
import { isClientLocalMachine, resolveMachineIdentity } from "./model";

export function useMachineLocation(environmentId: EnvironmentId, fallbackLabel?: string) {
  const environment = useEnvironment(environmentId);
  return isClientLocalMachine(environment?.entry?.target, isElectron)
    ? null
    : resolveMachineIdentity(
        environment?.label ?? fallbackLabel,
        environment?.serverConfig?.environment.platform.machine,
      );
}
