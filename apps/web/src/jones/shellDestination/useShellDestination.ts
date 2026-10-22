import { AuthTerminalOperateScope } from "@t3tools/contracts";
import type { ShellDestinationFence } from "@t3tools/contracts/jones/shellDestinationFence";
import { useEnvironments } from "../../state/environments";
import { useEnvironmentScope, readEnvironmentScope } from "../../state/session";
import { environmentPresentations } from "../../state/presentation";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { resolveShellDestination } from "./model";

export function useShellDestination(fence: ShellDestinationFence, sourceEnvironmentId: string) {
  const { environments } = useEnvironments();
  const target =
    fence._tag === "Valid"
      ? (environments.find((environment) => environment.environmentId === fence.environmentId) ??
        null)
      : null;
  const canOperate = useEnvironmentScope(target?.environmentId ?? null, AuthTerminalOperateScope);
  const destination = resolveShellDestination(
    fence,
    sourceEnvironmentId,
    environments.map((environment) => ({
      environmentId: environment.environmentId,
      label: environment.label,
      serverLabel: environment.serverConfig?.environment.label,
      phase: environment.connection.phase,
      cwd: environment.serverConfig?.cwd ?? null,
      canOperate: environment === target && canOperate,
    })),
  );
  return { destination, target };
}

export function readShellDestination(fence: ShellDestinationFence, sourceEnvironmentId: string) {
  const presentations = appAtomRegistry.get(environmentPresentations.presentationsAtom);
  return resolveShellDestination(
    fence,
    sourceEnvironmentId,
    [...presentations].map(([environmentId, presentation]) => ({
      environmentId,
      label: presentation.entry.target.label,
      serverLabel: presentation.serverConfig?.environment.label,
      phase: presentation.connection.phase,
      cwd: presentation.serverConfig?.cwd ?? null,
      canOperate: readEnvironmentScope(environmentId, AuthTerminalOperateScope),
    })),
  );
}
