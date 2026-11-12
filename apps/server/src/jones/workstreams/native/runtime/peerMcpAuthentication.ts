import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Auth from "../../../../auth/EnvironmentAuth.ts";
import type * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Pins from "./peerSessionPins.ts";

/** Optional Workstreams qualification after general native MCP authentication. */
export class PeerMcpAuthentication extends Context.Service<
  PeerMcpAuthentication,
  {
    readonly register: (
      nativeRequest: Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0],
      exactFinalMcpScope: Mcp.McpInvocationScope,
    ) => Effect.Effect<void>;
  }
>()("t3/jones/workstreams/native/runtime/peerMcpAuthentication") {}

const make = Effect.gen(function* () {
  const native = yield* Pins.NativePeerSessions;
  return PeerMcpAuthentication.of({
    register: Effect.fn("PeerMcpAuthentication.register")(function* (request, scope) {
      if (!scope.client?.linked) return;
      // authenticate clears prior object registration before validating; refusal mints no proof.
      yield* native
        .authenticate(request, scope)
        .pipe(Effect.catchTags({ PeerSessionError: () => Effect.void }));
    }),
  });
});

/** @public Share NativePeerSessions with pins/Guard; never reconstruct the final scope here. */
export const layer = Layer.effect(PeerMcpAuthentication, make);
