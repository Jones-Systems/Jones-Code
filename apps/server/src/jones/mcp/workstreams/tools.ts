import {
  McpCapabilityUnavailableError,
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpFailure,
  ThreadId,
} from "@t3tools/contracts";
import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
const target = { environmentId: OrchestratorMcpEnvironmentTarget };
const definitionResult = Schema.Struct({
  outcome: Schema.Literals(["applied", "already_satisfied", "superseded"]),
  workstream: P.PersonalWorkstream,
});
export const assignmentResult = Schema.Struct({
  outcome: Schema.optionalKey(Schema.Literals(["applied", "already_satisfied"])),
  threadId: ThreadId,
  workstreamId: Schema.NullOr(P.PersonalWorkstreamId),
  workstreamName: Schema.NullOr(Schema.String),
  revision: Schema.Number,
});
const dependencies = [McpInvocationContext, ThreadManagementService];
const failure = Schema.Union([
  McpCapabilityUnavailableError,
  OrchestratorMcpFailure,
  P.WorkstreamsPersonalError,
]);
export const WorkstreamToolkit = Toolkit.make(
  Tool.make("t3_workstreams_list", {
    description:
      "List live personal Workstreams and local thread counts on this or a linked environment. Definitions replicate only when a writable client bridges servers.",
    parameters: Schema.Struct(target),
    success: Schema.Struct({
      environmentId: Schema.String,
      sequence: Schema.Number,
      workstreams: Schema.Array(
        Schema.Struct({
          id: Schema.String,
          name: Schema.String,
          lifecycle: P.PersonalWorkstreamLifecycle,
          color: P.PersonalWorkstreamColor,
          orderKey: P.PersonalWorkstreamOrderKey,
          threadCount: Schema.Number,
        }),
      ),
    }),
    failure,
    failureMode: "return",
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_workstream_create", {
    description:
      "Create a personal Workstream with a stable caller-chosen id. Retain this same id on retry; creation does not assign a thread.",
    parameters: Schema.Struct({
      ...target,
      id: P.PersonalWorkstreamId,
      name: P.PersonalWorkstreamName,
      lifecycle: Schema.optionalKey(P.PersonalWorkstreamLifecycle),
      color: Schema.optionalKey(P.PersonalWorkstreamColor),
    }),
    success: definitionResult,
    failure,
    failureMode: "return",
    dependencies,
  }).annotate(Tool.Readonly, false),
  Tool.make("t3_workstream_update", {
    description:
      "Edit or delete a personal Workstream using its current local definition. Deletion is terminal for its id.",
    parameters: Schema.Struct({
      ...target,
      id: P.PersonalWorkstreamId,
      name: Schema.optionalKey(P.PersonalWorkstreamName),
      lifecycle: Schema.optionalKey(P.PersonalWorkstreamLifecycle),
      color: Schema.optionalKey(P.PersonalWorkstreamColor),
      orderKey: Schema.optionalKey(P.PersonalWorkstreamOrderKey),
      delete: Schema.optionalKey(Schema.Boolean),
    }),
    success: definitionResult,
    failure,
    failureMode: "return",
    dependencies,
  }).annotate(Tool.Readonly, false),
  Tool.make("t3_thread_workstream_read", {
    description:
      "Read a local thread's personal assignment and revision. threadId defaults to the calling thread; environmentId alone selects the target server.",
    parameters: Schema.Struct({ ...target, threadId: Schema.optionalKey(ThreadId) }),
    success: assignmentResult,
    failure,
    failureMode: "return",
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_thread_workstream_set", {
    description:
      "Assign or unassign a thread using expectedRevision from read. Keep the revision on retry; precondition_failed means another move won. The source carries a known definition to a linked target.",
    parameters: Schema.Struct({
      ...target,
      threadId: Schema.optionalKey(ThreadId),
      workstreamId: Schema.NullOr(P.PersonalWorkstreamId),
      expectedRevision: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
      workstream: Schema.optionalKey(P.PersonalWorkstream),
    }),
    success: assignmentResult,
    failure,
    failureMode: "return",
    dependencies,
  }).annotate(Tool.Readonly, false),
);
