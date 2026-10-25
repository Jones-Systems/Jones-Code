import {
  McpCapabilityUnavailableError,
  OrchestratorMcpFailure,
  T3WorkstreamListResult,
} from "@t3tools/contracts";
import * as Operations from "@t3tools/contracts/jones/workstreamOperations";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";

/** Production adapter must delegate to the same operation service used by HTTP.
 * It must preserve conflict errors and qualify foreign thread/environment IDs locally.
 * Registration requires the application-owned service; no raw registry write fallback.
 */
export class WorkstreamToolsBackend extends Context.Service<
  WorkstreamToolsBackend,
  {
    readonly list: (input: {
      readonly limit?: number;
      readonly cursor?: string;
    }) => Effect.Effect<typeof T3WorkstreamListResult.Type, OrchestratorMcpFailure>;
    readonly submit: (
      input: Operations.WorkstreamOperationSubmitRequest,
    ) => Effect.Effect<
      Operations.WorkstreamOperationResult,
      OrchestratorMcpFailure | Operations.EnvironmentWorkstreamOperationConflictError
    >;
    readonly read: (
      input: Operations.WorkstreamOperationReadRequest,
    ) => Effect.Effect<Operations.WorkstreamOperationReadResponse, OrchestratorMcpFailure>;
    readonly setupStatus: () => Effect.Effect<
      Operations.WorkstreamSetupStatus,
      OrchestratorMcpFailure
    >;
  }
>()("t3/jones/mcp/workstreams/tools/WorkstreamToolsBackend") {}

// Reject extra wire keys before Struct decoding can discard them.
const closed = <Fields extends Schema.Struct.Fields>(schema: Schema.Struct<Fields>) =>
  Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter(
        (value) =>
          typeof value === "object" &&
          value !== null &&
          !Array.isArray(value) &&
          Reflect.ownKeys(value).every((key) => Object.hasOwn(schema.fields, key)),
      ),
    ),
  );

const dependencies = [McpInvocationContext, ThreadManagementService];
const failure = Schema.Union([
  McpCapabilityUnavailableError,
  OrchestratorMcpFailure,
  Operations.EnvironmentWorkstreamOperationConflictError,
]);
const wait_ms = Operations.WorkstreamOperationSubmitRequest.fields.wait_ms;
const retry =
  "Supply an explicit operation_id and retain identical request fields on retry. in_progress/reconciling and CONFIG failures (foreign_thread, registry_mismatch, permission_denied, offline, not_configured, registration_unavailable) require the SAME ID and unchanged request. Start a new ID only after a settled or sealed result. Read status after a lost reply; never blindly mint a new ID. After contention, start a new ID. Only committed/already_satisfied confirms success.";

export const WorkstreamToolkit = Toolkit.make(
  Tool.make("t3_workstreams_list", {
    description:
      "List this environment's Workstreams. Obtain request registry_id and owner_id from t3_workstreams_status registry.registry_id and registry.owner_id; the legacy list binding.registryId is an HTTPS origin, not the operation registry UUID. Follow the returned cursor for more. Choose an existing workstream, or create one, then set the thread's primary assignment. No cross-environment proxy.",
    parameters: closed(
      Schema.Struct({
        limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
        cursor: Schema.optionalKey(Schema.String),
      }),
    ),
    success: T3WorkstreamListResult,
    failure,
    failureMode: "return",
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("t3_workstream_create", {
    description: `Create a Workstream in the local registry with origin.kind agent. Obtain the signed registry UUID from t3_workstreams_status registry.registry_id, never list binding.registryId. Use the returned workstream_id in t3_thread_workstream_set; creation alone does not assign a thread. ${retry}`,
    parameters: closed(
      Schema.Struct({
        request: Operations.WorkstreamCreateRequest.check(
          Schema.makeFilter((r) => r.origin.kind === "agent"),
        ),
        wait_ms,
      }),
    ),
    success: Operations.WorkstreamOperationResult,
    failure,
    failureMode: "return",
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("t3_thread_workstream_set", {
    description: `Assign, move, or unassign a local thread. Obtain target.registry_id from t3_workstreams_status registry.registry_id, never list binding.registryId. Read its assignment first and set expected to that observation (or explicitly any). Use target.kind unassigned to remove primary membership. Foreign threads are rejected locally. origin.kind must be agent. ${retry}`,
    parameters: closed(
      Schema.Struct({
        request: Operations.WorkstreamSetPrimaryRequest.check(
          Schema.makeFilter((r) => r.origin.kind === "agent"),
        ),
        wait_ms,
      }),
    ),
    success: Operations.WorkstreamOperationResult,
    failure,
    failureMode: "return",
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("t3_thread_workstream_read", {
    description:
      "Read a local thread's primary assignment and open operation. unverified/unavailable is not unassigned. Reading does not register or verify the thread. Foreign environments are refused, never proxied.",
    parameters: closed(Schema.Struct({ thread: Operations.WorkstreamOperationThread })),
    success: Operations.WorkstreamOperationReadResponse,
    failure,
    failureMode: "return",
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("t3_workstream_operation_status", {
    description:
      "Read an operation by its operation_id, or all open operations with open:true. This returns the latest retained result without refreshing a failure. After restart or eviction, null means resubmit the identical request with its SAME ID, not that it had no effect.",
    parameters: closed(
      Schema.Struct({
        operation_id: Schema.optional(Operations.WorkstreamCreateRequest.fields.operation_id),
        open: Schema.optional(Schema.Literal(true)),
      }),
    ).check(
      Schema.makeFilter((input) => (input.operation_id !== undefined) !== (input.open === true)),
    ),
    success: Operations.WorkstreamOperationReadResponse,
    failure,
    failureMode: "return",
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("t3_workstreams_status", {
    description:
      "Read Workstreams setup for this local environment: activation, registry access, source availability, placement readiness and repair documentation. No secrets or configuration changes.",
    parameters: Tool.EmptyParams,
    success: Operations.WorkstreamSetupStatus,
    failure,
    failureMode: "return",
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);
