import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import { McpSchema, Tool, Toolkit } from "effect/ai";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Protocol from "./fleetProtocol.ts";
import { PersonalWorkstreams } from "./service.ts";

const unavailable = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: "This session is not approved for Workstream replication.",
  });
const enabledForPeer = () => {
  const fiber = Fiber.getCurrent();
  const scope = fiber && Context.getOrUndefined(fiber.context, Invocation.McpInvocationContext);
  return scope?.client?.linked === true && !scope.thread && scope.client.access !== "read-only";
};
const peer = Invocation.McpInvocationContext.pipe(
  Effect.flatMap((scope) =>
    scope.thread ||
    !scope.client?.linked ||
    !scope.capabilities.has("orchestration") ||
    scope.client.access === "read-only"
      ? Effect.fail(unavailable())
      : Effect.succeed(scope.client),
  ),
);

const toolOptions = {
  dependencies: [Invocation.McpInvocationContext, PersonalWorkstreams, ThreadManagementService],
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
};
const isMcpFailure = Schema.is(OrchestratorMcpFailure);

export const toolkit = Toolkit.make(
  Tool.make("t3_workstream_fleet_enroll", {
    ...toolOptions,
    description: "Check this Workstreams link approval or ask the receiving owner to approve it.",
    parameters: Protocol.FleetPeerEnrollmentInput,
    success: Protocol.FleetPeerEnrollment,
  })
    .annotate(Tool.Idempotent, true)
    .annotate(McpSchema.EnabledWhen, enabledForPeer),
  Tool.make("t3_workstream_definition_read", {
    ...toolOptions,
    description: "Read an exact Workstream definition and its last receipt over an approved link.",
    parameters: Protocol.FleetPeerReadDefinition,
    success: Protocol.FleetPeerDefinition,
  })
    .annotate(Tool.Readonly, true)
    .annotate(McpSchema.EnabledWhen, enabledForPeer),
  Tool.make("t3_workstream_definitions_list", {
    ...toolOptions,
    description:
      "List exact Workstream definitions once when an approved authority enrolls this environment.",
    parameters: Protocol.FleetPeerEnrollmentInput,
    success: Protocol.FleetPeerDefinitions,
  })
    .annotate(Tool.Readonly, true)
    .annotate(McpSchema.EnabledWhen, enabledForPeer),
  Tool.make("t3_workstream_definition_apply", {
    ...toolOptions,
    description:
      "Apply a Workstream definition from its approved authority using an exact precondition.",
    parameters: Protocol.FleetPeerApplyDefinition,
    success: Protocol.FleetPeerApplyResult,
  })
    .annotate(Tool.Idempotent, true)
    .annotate(McpSchema.EnabledWhen, enabledForPeer),
);

export const handlers = Access.toLayer(toolkit, {
  t3_workstream_fleet_enroll: Access.writes(
    ({ authorityEnvironmentId }) =>
      Effect.gen(function* () {
        const caller = yield* peer;
        return yield* (yield* PersonalWorkstreams).proposeFollower(authorityEnvironmentId, caller);
      }).pipe(Effect.mapError(unavailable)),
    { linkedCallers: "allowed" },
  ),
  t3_workstream_definition_read: Access.reads(({ authorityEnvironmentId, workstreamId }) =>
    Effect.gen(function* () {
      const caller = yield* peer;
      return yield* (yield* PersonalWorkstreams).readPeerDefinition(
        authorityEnvironmentId,
        workstreamId,
        caller,
      );
    }).pipe(Effect.mapError(unavailable)),
  ),
  t3_workstream_definitions_list: Access.reads(({ authorityEnvironmentId }) =>
    Effect.gen(function* () {
      const caller = yield* peer;
      return yield* (yield* PersonalWorkstreams).readPeerDefinitions(
        authorityEnvironmentId,
        caller,
      );
    }).pipe(Effect.mapError(unavailable)),
  ),
  t3_workstream_definition_apply: Access.writes(
    (input) =>
      Effect.gen(function* () {
        const caller = yield* peer;
        return yield* (yield* PersonalWorkstreams).applyPeerDefinition(input, caller);
      }).pipe(
        Effect.mapError((error) =>
          isMcpFailure(error) || error.code === "authority_required"
            ? unavailable()
            : new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Could not confirm the Workstream write.",
              }),
        ),
      ),
    { linkedCallers: "allowed" },
  ),
});
