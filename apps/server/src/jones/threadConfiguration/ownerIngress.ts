import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

// Private authority, provided only inside authenticated client command ingress.
// A wire author or an agent's MCP setter is not owner-selection authority.
export const OwnerSelectionIngress = Context.Reference<boolean>(
  "jones/threadConfiguration/OwnerSelectionIngress",
  { defaultValue: () => false },
);

export const withOwnerSelectionIngress = Effect.provideService(OwnerSelectionIngress, true);
