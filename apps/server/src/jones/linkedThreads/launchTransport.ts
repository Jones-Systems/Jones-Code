import type { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { Tool } from "effect/ai";
import type { ThreadStartGate } from "../../mcp/McpToolAccess.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import { launchThread } from "../../mcp/toolkits/project/handlers.ts";
import type { ProjectToolkit } from "../../mcp/toolkits/project/tools.ts";

export class HandoffLaunch extends Context.Service<
  HandoffLaunch,
  {
    readonly launch: (
      input: Tool.Parameters<typeof ProjectToolkit.tools.t3_thread_launch>,
      gate: ThreadStartGate,
    ) => Effect.Effect<
      Tool.Success<typeof ProjectToolkit.tools.t3_thread_launch>,
      OrchestratorMcpFailure,
      Invocation.McpInvocationContext
    >;
  }
>()("t3/jones/linkedThreads/launchTransport/HandoffLaunch") {}
const make = Effect.gen(function* () {
  const context =
    yield* Effect.context<
      Exclude<Effect.Services<ReturnType<typeof launchThread>>, Invocation.McpInvocationContext>
    >();
  return {
    launch: (
      input: Tool.Parameters<typeof ProjectToolkit.tools.t3_thread_launch>,
      gate: ThreadStartGate,
    ) => launchThread(input, gate).pipe(Effect.provide(context)),
  };
});
export const layer = Layer.effect(HandoffLaunch, make);
