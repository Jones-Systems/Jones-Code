import {
  CommandId,
  EnvironmentId,
  McpCapabilityUnavailableError,
  NativeInvocationContext,
  NonNegativeInt,
  OrganizationThreadMetadataPage,
  ThreadId,
  T3PlacementLoadRequest,
  Workstream,
  WorkstreamCommand,
  WorkstreamReceipt,
  T3PlacementResult,
  T3WorkstreamListResult,
  WorkstreamDetail,
  WorkstreamMembershipPage,
  WorkstreamReferencePage,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import { HttpServer } from "effect/unstable/http";
import { ServerConfig } from "../../../config.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import {
  WorkstreamGateway,
  WorkstreamGatewayError,
} from "../../../workstreams/WorkstreamGateway.ts";

export class OrganizationToolError extends Schema.TaggedError<OrganizationToolError>()(
  "OrganizationToolError",
  {
    reason: Schema.String,
    threadId: Schema.optional(Schema.String),
    commandId: Schema.optional(Schema.String),
    expectedSnapshotSequence: Schema.optional(NonNegativeInt),
    snapshotSequence: Schema.optional(NonNegativeInt),
    cause: Schema.optional(Schema.Defect()),
  },
) {}
const failure = Schema.Union([
  McpCapabilityUnavailableError,
  WorkstreamGatewayError,
  OrganizationToolError,
]);
const dependencies = [McpInvocationContext, ThreadManagementService, WorkstreamGateway];
const limit = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }));
const offset = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const cursor = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(2048));
const page = { limit: Schema.optional(limit), cursor: Schema.optional(cursor) };
const id = Workstream.fields.workstream_id;
const version = Workstream.fields.version;
const membership = { workstream_id: id, expected_version: version };
export const OrganizationCommand = Schema.Struct({
  command_id: WorkstreamCommand.fields.command_id,
  expected_server_generation: WorkstreamCommand.fields.expected_server_generation,
  expected_registry_version: WorkstreamCommand.fields.expected_registry_version,
  action: Schema.Union([
    Schema.Struct({
      operation: Schema.Literals(["attach_primary", "reattach_primary"]),
      ...membership,
      native_reference_id: id,
    }),
    Schema.Struct({
      operation: Schema.Literal("remove_membership"),
      ...membership,
      membership_id: id,
    }),
    Schema.Struct({
      operation: Schema.Literal("move_primary"),
      source_workstream_id: id,
      expected_source_version: version,
      source_membership_id: id,
      destination_workstream_id: id,
      expected_destination_version: version,
    }),
  ]),
});
export const OrganizationThread = Schema.Struct({
  threadId: ThreadId,
  title: Schema.String,
  projectId: Schema.String,
  pinnedAt: Schema.NullOr(Schema.String),
  pinOrderKey: Schema.NullOr(Schema.String),
  activeOrderKey: Schema.NullOr(Schema.String),
  snoozedUntil: Schema.NullOr(Schema.String),
  settledOverride: Schema.NullOr(Schema.Literals(["active", "settled"])),
  settledAt: Schema.NullOr(Schema.String),
  archivedAt: Schema.NullOr(Schema.String),
});
export const OrderKey = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(256),
  Schema.isPattern(/^[0-9A-Za-z]+$/),
);
const nativeResult = Schema.Struct({
  commandId: CommandId,
  sequence: Schema.Int,
  readback: Schema.Literals(["observed", "pending", "unknown"]),
  thread: Schema.optional(OrganizationThread),
});
const annotation = <T extends Tool.Any>(tool: T, readonly: boolean): T =>
  tool
    .annotate(Tool.Readonly, readonly)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, readonly)
    .annotate(Tool.OpenWorld, false) as T;
export const OrganizationToolkit = Toolkit.make(
  annotation(
    Tool.make("get_invocation_context", {
      description:
        "Read the authenticated current invocation's environment and thread IDs, effective base directory, proved loopback origin, and bundled server version. A null origin means no loopback route is proved; server generation is unavailable. This is not runtime attestation.",
      // The flipped guard rejects raw keys before Struct strips them. The outer check
      // supplies MCP's required object schema, which an empty Struct does not generate.
      parameters: Schema.flip(
        Schema.flip(Schema.Struct({})).check(
          Schema.makeFilter((value) => Reflect.ownKeys(value).length === 0),
        ),
      ).check(
        Schema.isMaxProperties(0, {
          toJsonSchema: () => ({ type: "object", maxProperties: 0 }),
        }),
      ),
      success: NativeInvocationContext,
      failure: McpCapabilityUnavailableError,
      dependencies: [McpInvocationContext, ServerConfig, HttpServer.HttpServer],
    }),
    true,
  ),
  annotation(
    Tool.make("list_organization_thread_metadata", {
      description:
        "Read a bounded page of environment-local thread navigation and activity metadata without conversation bodies. projectionUpdatedAt is a row metadata timestamp, not activity. V2 run IDs and nullable timestamps are reported as v2Activity; unavailable historical latestTurn and session facts remain null. Pass the first page's snapshotSequence as expectedSnapshotSequence on later pages; restart pagination if it changes. The watermark covers persisted projection state, not an atomic whole census or the separately sampled background state. Background coverage remains unknown; no background task objects are returned. Queued-work coverage remains unknown. This grants no attestation, custody, or exclusive ownership.",
      parameters: Schema.Struct({
        limit: Schema.optional(limit),
        offset: Schema.optional(offset),
        expectedSnapshotSequence: Schema.optional(NonNegativeInt),
      }),
      success: OrganizationThreadMetadataPage,
      failure: Schema.Union([McpCapabilityUnavailableError, OrganizationToolError]),
      dependencies: [McpInvocationContext, ThreadManagementService],
    }),
    true,
  ),
  annotation(
    Tool.make("list_organization_threads", {
      description:
        "List bounded local thread navigation metadata, without conversation content. Use exact IDs and exposed order keys. Pagination uses offset; a changing snapshot may change page boundaries.",
      parameters: Schema.Struct({ limit: Schema.optional(limit), offset: Schema.optional(offset) }),
      success: Schema.Struct({
        environmentId: EnvironmentId,
        threads: Schema.Array(OrganizationThread),
        nextOffset: Schema.NullOr(Schema.Int),
      }),
      failure,
      dependencies,
    }),
    true,
  ),
  annotation(
    Tool.make("list_workstreams", {
      description: "Read a bounded workstream page and current registry capabilities and versions.",
      parameters: Schema.Struct(page),
      success: T3WorkstreamListResult,
      failure,
      dependencies,
    }),
    true,
  ),
  annotation(
    Tool.make("list_workstream_references", {
      description:
        "Read a bounded page of registered native references, including references without membership. Preserve registration and attestation state; a reference is not permission to enroll or verify it.",
      parameters: Schema.Struct(page),
      success: WorkstreamReferencePage,
      failure,
      dependencies,
    }),
    true,
  ),
  annotation(
    Tool.make("read_workstream", {
      description:
        "Read one workstream and a bounded membership page. Pass the membership cursor for further pages.",
      parameters: Schema.Struct({ workstreamId: id, ...page }),
      success: Schema.Struct({ detail: WorkstreamDetail, memberships: WorkstreamMembershipPage }),
      failure,
      dependencies,
    }),
    true,
  ),
  annotation(
    Tool.make("list_thread_placements", {
      description:
        "Load attested thread placements for an exact bounded native identity inventory. Preserve unresolved IDs and attestation failures.",
      parameters: T3PlacementLoadRequest,
      success: T3PlacementResult,
      failure,
      dependencies,
    }),
    true,
  ),
  annotation(
    Tool.make("submit_workstream_command", {
      description:
        "Submit one primary membership attach, reattach, move, or membership removal. Requires current versions and exact command ID. Returns the existing receipt; pending or unknown effects require exact receipt lookup, never blind retry. Sequential commands are not atomic bulk movement.",
      parameters: OrganizationCommand,
      success: WorkstreamReceipt,
      failure,
      dependencies,
    }),
    false,
  ),
  annotation(
    Tool.make("read_workstream_command", {
      description: "Read the receipt of one exact workstream command ID.",
      parameters: Schema.Struct({ commandId: WorkstreamCommand.fields.command_id }),
      success: WorkstreamReceipt,
      failure,
      dependencies,
    }),
    true,
  ),
  annotation(
    Tool.make("reorder_workstream", {
      description:
        "Change only workstream sort order, preserving current name, lifecycle and progress. Requires exact current workstream and registry versions.",
      parameters: Schema.Struct({
        commandId: WorkstreamCommand.fields.command_id,
        workstreamId: id,
        expectedVersion: version,
        expectedServerGeneration: WorkstreamCommand.fields.expected_server_generation,
        expectedRegistryVersion: WorkstreamCommand.fields.expected_registry_version,
        sortOrder: Workstream.fields.sort_order,
      }),
      success: WorkstreamReceipt,
      failure,
      dependencies,
    }),
    false,
  ),
  annotation(
    Tool.make("set_thread_pinned", {
      description:
        "Pin or unpin one local thread through native commands. Pinning cannot restore a snoozed, settled or archived thread. Supply a new initial orderKey when needed; re-pinning retains its existing slot.",
      parameters: Schema.Struct({
        commandId: CommandId,
        threadId: ThreadId,
        pinned: Schema.Boolean,
        orderKey: Schema.optional(OrderKey),
      }),
      success: nativeResult,
      failure,
      dependencies,
    }),
    false,
  ),
  annotation(
    Tool.make("reorder_thread", {
      description:
        "Set one local thread's validated order key in pinned or active order. Pinned order requires a pinned thread; active order is independent of pin state. Parked threads cannot be reordered. No provider or lifecycle command is exposed.",
      parameters: Schema.Struct({
        commandId: CommandId,
        threadId: ThreadId,
        list: Schema.Literals(["pinned", "active"]),
        orderKey: OrderKey,
      }),
      success: nativeResult,
      failure,
      dependencies,
    }),
    false,
  ),
);
