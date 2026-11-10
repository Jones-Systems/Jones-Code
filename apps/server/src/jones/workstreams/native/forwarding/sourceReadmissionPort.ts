import type * as Parent from "@t3tools/contracts/jones/workstreamsForwardQualification";
import type * as Raw from "@t3tools/contracts/jones/workstreamsForwardReadmission";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type { McpInvocationScope } from "../../../../mcp/McpInvocationContext.ts";
import type { RegistryError } from "../registry/session.ts";

export const SOURCE_READMISSION_TOOL = "jones_workstreams_source_readmission";

/** Lookup observations only; no grant, acceptance, dispatch ticket or reusable currentness proof escapes. */
export class SourceReadmission extends Context.Service<
  SourceReadmission,
  {
    readonly lookupParent: (
      request: Parent.WorkstreamsSourceParentReadmissionRequestV3,
      exactInvocation: McpInvocationScope,
    ) => Effect.Effect<Parent.WorkstreamsSourceParentReadmissionResponseV3, RegistryError>;
    readonly lookupRaw: (
      request: Raw.WorkstreamsSourceReadmissionRequestV2,
      exactInvocation: McpInvocationScope,
    ) => Effect.Effect<Raw.WorkstreamsSourceReadmissionResponseV2, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/sourceReadmissionPort/SourceReadmission") {}
