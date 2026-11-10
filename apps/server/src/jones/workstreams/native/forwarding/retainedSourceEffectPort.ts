import type * as Wire from "@t3tools/contracts/jones/workstreamsRetainedSourceEffect";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type { McpInvocationScope } from "../../../../mcp/McpInvocationContext.ts";
import type { RegistryError } from "../registry/session.ts";

export const SOURCE_RETAINED_EFFECT_TOOL = "jones_workstreams_source_retained_effect";

/** Fixed provider carrier only. The implementation selects the original caller from encrypted local intent. */
export class RetainedSourceEffects extends Context.Service<
  RetainedSourceEffects,
  {
    readonly invoke: (
      request: Wire.WorkstreamsRetainedSourceEffectRequestV1,
      exactInvocation: McpInvocationScope,
    ) => Effect.Effect<Wire.WorkstreamsRetainedSourceEffectResultV1, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/retainedSourceEffectPort/RetainedSourceEffects") {}
