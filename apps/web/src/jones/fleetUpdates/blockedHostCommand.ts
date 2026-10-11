import type { EnvironmentId } from "@t3tools/contracts";
import {
  EnvironmentRegistry,
  type ConnectionCatalogEntry,
} from "@t3tools/client-runtime/connection";
import {
  requestJonesUpdateWithDescriptor,
  type JonesUpdateBridgeInput,
} from "@t3tools/client-runtime/jones/fleet-updates";
import * as Effect from "effect/Effect";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { APP_SOURCE_SHA } from "../../branding";
import { resolveJonesSourceCurrency } from "./sourceCurrency";

/** Updating a host does not change the owner's connection enablement choice. */
export function makeBlockedHostUpdate<E, R>(
  request: (
    entry: ConnectionCatalogEntry,
    input: JonesUpdateBridgeInput,
  ) => Effect.Effect<Effect.Success<ReturnType<typeof requestJonesUpdateWithDescriptor>>, E, R>,
) {
  return Effect.fn(function* (input: {
    readonly environmentId: EnvironmentId;
    readonly request: JonesUpdateBridgeInput;
  }) {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const entry = (yield* SubscriptionRef.get(registry.entries)).get(input.environmentId);
    if (entry === undefined)
      return yield* new EnvironmentRegistry.EnvironmentNotRegisteredError({
        environmentId: input.environmentId,
      });
    const result = yield* request(entry, input.request);
    return {
      state: result.state,
      currency: resolveJonesSourceCurrency({
        installedSource: result.descriptor.jonesSource?.sha,
        targetSource: APP_SOURCE_SHA,
      }),
    };
  });
}

export const executeBlockedHostUpdate = makeBlockedHostUpdate(requestJonesUpdateWithDescriptor);
