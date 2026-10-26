import { McpCapabilityUnavailableError, OrchestratorMcpFailure } from "@t3tools/contracts";
import {
  WorkstreamsHomeReadResult,
  WorkstreamsHomeReadResultRequest,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Protocol from "../../workstreams/native/forwarding/homeReadProtocol.ts";

/** Additive single-hop protocol. Registration requires the application-owned qualified service. */
export const toolkit = Toolkit.make(
  Tool.make("jones_workstreams_home_read_result", {
    description:
      "Read a coherent native Workstreams home result through the authenticated, bound peer session. Retain the returned envelope and authorization revision with the route value. Omitted placement_contract selects frozen v1; native placement v2 requires explicit negotiation. This tool cannot relay to another environment, export data, change state, or establish a peer binding.",
    parameters: WorkstreamsHomeReadResultRequest,
    success: WorkstreamsHomeReadResult,
    failure: Schema.Union([McpCapabilityUnavailableError, OrchestratorMcpFailure]),
    failureMode: "return",
    dependencies: [Invocation.McpInvocationContext, ThreadManagementService],
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);

export const handlers = Access.toLayer(
  toolkit,
  Effect.gen(function* () {
    const protocol = yield* Protocol.HomeReadProtocol;
    return {
      jones_workstreams_home_read_result: Access.reads((request) =>
        Effect.gen(function* () {
          yield* Invocation.requireMcpCapability("orchestration");
          return yield* protocol.read(request).pipe(
            Effect.mapError(
              (cause) =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: `Native Workstreams read refused: ${cause.code}.`,
                }),
            ),
          );
        }),
      ),
    };
  }),
);
