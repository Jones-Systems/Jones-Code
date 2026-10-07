import { McpCapabilityUnavailableError, NonNegativeInt } from "@t3tools/contracts";
import {
  NativeInvocationContext,
  OrganizationThreadMetadataPage,
} from "@t3tools/contracts/jones/organizationMetadata";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import {
  OrganizationMetadataMcpService,
  OrganizationMetadataError,
} from "./OrganizationMetadataMcpService.ts";

// Keep the raw-key guard before Struct decoding can discard extra properties.
const emptyInput = Schema.flip(
  Schema.flip(Schema.Struct({})).check(
    Schema.makeFilter((value) => Reflect.ownKeys(value).length === 0),
  ),
).check(Schema.isMaxProperties(0, { toJsonSchema: () => ({ type: "object", maxProperties: 0 }) }));
const dependencies = [McpInvocationContext, OrganizationMetadataMcpService];
export const OrganizationMetadataToolkit = Toolkit.make(
  Tool.make("get_invocation_context", {
    description:
      "Read the authenticated thread and environment IDs, effective base directory, proved bound loopback origin, and bundled server version. A null origin means no loopback route is proved; server generation is unavailable. This is not runtime attestation.",
    parameters: emptyInput,
    success: NativeInvocationContext,
    failure: McpCapabilityUnavailableError,
    failureMode: "return",
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("list_organization_thread_metadata", {
    description:
      "Read environment-wide active and archived thread navigation and activity metadata without conversation bodies. Pass the first page's snapshotSequence on later pages; restart if the watermark changes. The watermark covers persisted projection state; separately sampled runtime state is not an atomic census. projectionUpdatedAt is a metadata timestamp, not activity. This grants no custody, attestation, or exclusive ownership.",
    parameters: Schema.Struct({
      limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
      offset: Schema.optional(
        NonNegativeInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
      ),
      expectedSnapshotSequence: Schema.optional(NonNegativeInt),
    }),
    success: OrganizationThreadMetadataPage,
    failure: Schema.Union([McpCapabilityUnavailableError, OrganizationMetadataError]),
    failureMode: "return",
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);
