import { McpCapabilityUnavailableError, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as W from "@t3tools/contracts/jones/workstreamsPeerSetup";
import * as NativeWire from "@t3tools/contracts/jones/workstreamsNative";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Setup from "../../workstreams/native/runtime/peerSetup.ts";

// MCP requires an object-root JSON Schema; the frozen decoder still enforces
// each closed request variant, including the source-intent-only digest.
const readParameters = NativeWire.closedWorkstreamsNative(
  Schema.Struct({
    contract: Schema.Literal(W.WORKSTREAMS_PEER_SETUP_CONTRACT),
    operation: Schema.Literals(["context", "source_intent"]),
    ...W.WorkstreamsPeerSetupAddress.fields,
    request_sha256: Schema.optionalKey(NativeWire.WorkstreamsNativeDigest),
    receiver_environment_id: NativeWire.WorkstreamsNativeId,
  }),
).pipe(Schema.decodeTo(W.WorkstreamsPeerSetupReadRequest));

export const toolkit = Toolkit.make(
  Tool.make("jones_workstreams_peer_setup_read", {
    description:
      "Read fixed native peer setup context or the exact retained source intent. Requires genuine native peer authentication; never registers sessions or forwards arbitrary tools.",
    parameters: readParameters,
    success: W.WorkstreamsPeerSetupReadResult,
    failure: Schema.Union([McpCapabilityUnavailableError, OrchestratorMcpFailure]),
    failureMode: "return",
    dependencies: [Invocation.McpInvocationContext, ThreadManagementService],
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("jones_workstreams_peer_setup_apply", {
    description:
      "Prepare the selected home, accept the selected member, or explicitly resume its retained setup. Uses only the fixed authenticated peer and original retained request; no receipt import or second bind after uncertainty.",
    parameters: W.WorkstreamsPeerSetupApplyRequest,
    success: W.WorkstreamsPeerSetupResult,
    failure: Schema.Union([McpCapabilityUnavailableError, OrchestratorMcpFailure]),
    failureMode: "return",
    dependencies: [Invocation.McpInvocationContext, ThreadManagementService],
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);
export const handlers = Access.toLayer(
  toolkit,
  Effect.gen(function* () {
    const setup = yield* Setup.PeerSetup;
    return {
      jones_workstreams_peer_setup_read: Access.reads((request) =>
        Effect.gen(function* () {
          yield* Invocation.requireMcpCapability("orchestration");
          return yield* setup.read(request, yield* Invocation.McpInvocationContext).pipe(
            Effect.mapError(
              (cause) =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "Native peer setup read refused: " + cause.code + ".",
                }),
            ),
          );
        }),
      ),
      jones_workstreams_peer_setup_apply: Access.writes(
        (request) =>
          Effect.gen(function* () {
            yield* Invocation.requireMcpCapability("orchestration");
            return yield* setup.apply(request, yield* Invocation.McpInvocationContext).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: "Native peer setup apply refused: " + cause.code + ".",
                  }),
              ),
            );
          }),
        { linkedCallers: "allowed" },
      ),
    };
  }),
);
