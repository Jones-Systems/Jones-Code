import type { EnvironmentId } from "@t3tools/contracts";
import type { FleetDesktopState } from "@t3tools/contracts/jones/fleet-updates";
import { EnvironmentRegistry } from "@t3tools/client-runtime/connection";
import { requestFleetHost, type FleetHostRequest } from "@t3tools/client-runtime/jones/fleet-updates";
import { createRuntimeCommand, runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import * as Effect from "effect/Effect";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom } from "effect/reactivity";
import { connectionAtomRuntime } from "../../connection/runtime";
import { appAtomRegistry } from "../../rpc/atomRegistry";

export const fleetDesktopState = Atom.make<FleetDesktopState | null>(null);
export const fleetStatusError = Atom.make<string | null>(null);
export const fleetHostCommand = createRuntimeCommand(connectionAtomRuntime, {
  label: "jones:fleet-host",
  concurrency: { mode: "serial", key: (input) => input.environmentId },
  execute: Effect.fn(function* (input: { readonly environmentId: EnvironmentId; readonly request: FleetHostRequest }) {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const entry = (yield* SubscriptionRef.get(registry.entries)).get(input.environmentId);
    if (entry === undefined) return yield* new EnvironmentRegistry.EnvironmentNotRegisteredError({ environmentId: input.environmentId });
    return yield* requestFleetHost(entry, input.request);
  }),
});

export async function fleetHost(environmentId: EnvironmentId, request: FleetHostRequest) {
  const result = await runAtomCommand(appAtomRegistry, fleetHostCommand, { environmentId, request }, { reportFailure: false, reportDefect: false });
  if (result._tag !== "Success") throw new Error("Fleet host request could not be completed.");
  return result.value;
}

export async function refreshFleetDesktopState() {
  const bridge = window.desktopBridge?.fleetUpdates;
  if (bridge === undefined) return null;
  const state = await bridge({ action: "read" });
  appAtomRegistry.set(fleetDesktopState, state);
  appAtomRegistry.set(fleetStatusError, null);
  return state;
}
