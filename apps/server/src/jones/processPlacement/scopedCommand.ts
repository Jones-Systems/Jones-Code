import {
  AgentScopeStopError,
  type AgentScopeCommand,
  type AgentScopeShape,
} from "@t3tools/shared/AgentScope";
import type { ChildProcess } from "effect/process";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import { ProcessPlacementError } from "./processPlacement.ts";

// Command-local provenance only: no persisted identity or runtime census.
const scopes = new WeakMap<
  ChildProcess.Command,
  {
    readonly owner: AgentScopeShape;
    readonly launch: AgentScopeCommand;
    claimed: boolean;
    readonly retirement: Deferred.Deferred<void, AgentScopeStopError>;
    readonly receipt: CapturedScopedRetirement | undefined;
    retirementObserved: boolean;
  }
>();

const capturedScope: unique symbol = Symbol("captured-scoped-retirement");
export interface CapturedScopedRetirement {
  readonly [capturedScope]: true;
}
const retirements = new WeakMap<
  object,
  {
    readonly unit: string;
    readonly completion: Deferred.Deferred<void, AgentScopeStopError>;
  }
>();

export function retainScopedCommand<T extends ChildProcess.Command>(
  command: T,
  owner: AgentScopeShape,
  launch: AgentScopeCommand,
): T {
  if (scopes.has(command)) throw new ProcessPlacementError("scoped command already retained");
  const retirement = Deferred.makeUnsafe<void, AgentScopeStopError>();
  // A function or unit name alone is not a receipt. Only the original retained
  // launch can supply the actual finalizer outcome, and it is never re-invoked.
  const receipt: CapturedScopedRetirement | undefined =
    launch.stop === undefined || launch.unit === undefined || launch.unit.length === 0
      ? undefined
      : Object.freeze<CapturedScopedRetirement>({ [capturedScope]: true });
  if (receipt !== undefined && launch.unit !== undefined)
    retirements.set(receipt, { unit: launch.unit, completion: retirement });
  scopes.set(command, {
    owner,
    launch,
    claimed: false,
    retirement,
    receipt,
    retirementObserved: false,
  });
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

export function captureScopedCommandRetirement(
  command: ChildProcess.Command,
  owner: AgentScopeShape,
): CapturedScopedRetirement | undefined {
  const retained = scopes.get(command);
  return retained?.owner === owner ? retained.receipt : undefined;
}

/** Observe the exact already-claimed finalizer. Preserve its result and the
 * caller's existing logging policy; this wrapper sends no stop of its own. */
export const observeScopedCommandRetirement = Effect.fn("scopedCommand.observeRetirement")(
  function* (
    command: ChildProcess.Command,
    owner: AgentScopeShape,
    stop: Effect.Effect<void, AgentScopeStopError>,
  ) {
    const retained = scopes.get(command);
    if (
      retained === undefined ||
      retained.owner !== owner ||
      !retained.claimed ||
      retained.launch.stop !== stop
    )
      return yield* new AgentScopeStopError({
        unit: "<unbound>",
        detail: "Captured scope retirement owner changed",
      });
    if (retained.retirementObserved) return yield* Deferred.await(retained.retirement);
    retained.retirementObserved = true;
    return yield* stop.pipe(
      Effect.onExit((exit) => Deferred.done(retained.retirement, exit).pipe(Effect.asVoid)),
    );
  },
);

/** Original captured receipt only: serialization and current-thread scope maps
 * cannot reconstruct retirement, including after a runtime replacement. */
export const awaitCapturedScopedRetirement = Effect.fn("scopedCommand.awaitCapturedRetirement")(
  function* (receipt: unknown) {
    const captured =
      typeof receipt === "object" && receipt !== null ? retirements.get(receipt) : undefined;
    if (captured === undefined)
      return yield* new AgentScopeStopError({
        unit: "<unbound>",
        detail: "No original scoped retirement receipt",
      });
    yield* Deferred.await(captured.completion);
    return { unit: captured.unit };
  },
);
