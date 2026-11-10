import { McpCapabilityUnavailableError, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Home from "../../workstreams/native/runtime/homeBind.ts";

export const toolkit = Toolkit.make(
  Tool.make("jones_workstreams_home_bind_setup", {
    description:
      "Read the exact prepared native Workstreams home setup through its qualified native peer session. This is setup-only and cannot read normal Workstreams data or relay elsewhere.",
    parameters: Wire.WorkstreamsHomeBindSetupReadbackRequestV2,
    success: Wire.WorkstreamsHomeBindSetupReadbackV2,
    failure: Schema.Union([McpCapabilityUnavailableError, OrchestratorMcpFailure]),
    failureMode: "return",
    dependencies: [Invocation.McpInvocationContext, ThreadManagementService],
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("jones_workstreams_home_bind", {
    description:
      "Commit or replay the exact owner-qualified native Workstreams home bind. Requires the existing prepared peer association, principal and grant. This tool creates no principal or grant and cannot relay elsewhere.",
    parameters: Wire.WorkstreamsHomeBindRequestV2,
    success: Wire.WorkstreamsHomeBindCommittedV2,
    failure: Schema.Union([McpCapabilityUnavailableError, OrchestratorMcpFailure]),
    failureMode: "return",
    dependencies: [Invocation.McpInvocationContext, ThreadManagementService],
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("jones_workstreams_home_bind_readback", {
    description:
      "Reconcile one exact native home bind ID and request digest under current qualified peer authority. Unknown outcomes remain unknown; this tool never dispatches or replaces a bind.",
    parameters: Wire.WorkstreamsHomeBindReadbackRequestV2,
    success: Wire.WorkstreamsHomeBindReadbackV2,
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
    const home = yield* Home.HomeBind;
    return {
      jones_workstreams_home_bind_setup: Access.reads((request) =>
        Effect.gen(function* () {
          yield* Invocation.requireMcpCapability("orchestration");
          const invocation = yield* Invocation.McpInvocationContext;
          return yield* home.setupReadback(request, invocation).pipe(
            Effect.mapError(
              (cause) =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: `Native Workstreams setup read refused: ${cause.code}.`,
                }),
            ),
          );
        }),
      ),
      // Linked access is restricted to this fixed domain operation. HomeBind requires the exact
      // authenticated invocation and owner-qualified preparation; general forwarding is unchanged.
      jones_workstreams_home_bind: Access.writes(
        (request) =>
          Effect.gen(function* () {
            yield* Invocation.requireMcpCapability("orchestration");
            const invocation = yield* Invocation.McpInvocationContext;
            return yield* home.bind(request, invocation).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: `Native Workstreams bind refused: ${cause.code}.`,
                  }),
              ),
            );
          }),
        { linkedCallers: "allowed" },
      ),
      jones_workstreams_home_bind_readback: Access.reads((request) =>
        Effect.gen(function* () {
          yield* Invocation.requireMcpCapability("orchestration");
          const invocation = yield* Invocation.McpInvocationContext;
          return yield* home.readback(request, invocation).pipe(
            Effect.mapError(
              (cause) =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: `Native Workstreams bind readback refused: ${cause.code}.`,
                }),
            ),
          );
        }),
      ),
    };
  }),
);
