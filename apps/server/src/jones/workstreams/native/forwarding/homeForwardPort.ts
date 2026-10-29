import type * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type { McpInvocationScope } from "../../../../mcp/McpInvocationContext.ts";
import type { RegistryError } from "../registry/session.ts";

/** Fixed invocation negotiation is the readback contract itself; no tools/list claim. */
export const HOME_FORWARD_TOOL = "jones_workstreams_home_forward";
export const HOME_FORWARD_READBACK_TOOL = "jones_workstreams_home_forward_readback";

export class HomeTools extends Context.Service<
  HomeTools,
  {
    readonly forward: (
      input: unknown,
      invocation: McpInvocationScope,
    ) => Effect.Effect<Forward.WorkstreamsForwardResult, RegistryError>;
    readonly readback: (
      input: unknown,
      invocation: McpInvocationScope,
    ) => Effect.Effect<Forward.WorkstreamsForwardReadbackResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/homeForwardPort/HomeTools") {}
