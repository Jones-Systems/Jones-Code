import type { AgentScopeCommand, AgentScopeShape } from "@t3tools/shared/AgentScope";
import type { ChildProcess } from "effect/process";
import { ProcessPlacementError } from "./processPlacement.ts";

// Command-local provenance only: no persisted identity or runtime census.
const scopes = new WeakMap<
  ChildProcess.Command,
  {
    readonly owner: AgentScopeShape;
    readonly launch: AgentScopeCommand;
    claimed: boolean;
  }
>();

export function retainScopedCommand<T extends ChildProcess.Command>(
  command: T,
  owner: AgentScopeShape,
  launch: AgentScopeCommand,
): T {
  scopes.set(command, { owner, launch, claimed: false });
  return command;
}

export function claimScopedCommand(
  command: ChildProcess.Command,
  owner: AgentScopeShape,
): AgentScopeCommand | undefined {
  const retained = scopes.get(command);
  if (retained === undefined) return undefined;
  if (retained.owner !== owner || retained.claimed)
    throw new ProcessPlacementError("scoped command owner changed or launch already claimed");
  retained.claimed = true;
  return retained.launch;
}
