import { McpCapabilityUnavailableError, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Parent from "@t3tools/contracts/jones/workstreamsForwardQualification";
import * as Raw from "@t3tools/contracts/jones/workstreamsForwardReadmission";
import * as NativeWire from "@t3tools/contracts/jones/workstreamsNative";
import * as RetainedWire from "@t3tools/contracts/jones/workstreamsRetainedSourceEffect";
import {
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeSettlementRequest,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Home from "../../workstreams/native/forwarding/homeForwardPort.ts";
import * as Source from "../../workstreams/native/forwarding/sourceReadmissionPort.ts";
import * as Retained from "../../workstreams/native/forwarding/retainedSourceEffectPort.ts";
import type { RegistryError } from "../../workstreams/native/registry/session.ts";

const failure = Schema.Union([McpCapabilityUnavailableError, OrchestratorMcpFailure]);
const dependencies = [Invocation.McpInvocationContext, ThreadManagementService] as const;

const forwardParameters = NativeWire.closedWorkstreamsNative(
  Schema.Struct({
    contract: Schema.Literal(Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT),
    kind: Schema.Literals(["command", "operation", "appearance"]),
    identity: Forward.WorkstreamsForwardIdentity,
    request: Schema.Union([
      NativeWire.WorkstreamsNativeHomeCommand,
      NativeWire.WorkstreamsNativeHomeOperation,
      Forward.WorkstreamsNativeHomeAppearance,
    ]),
    request_body: Schema.optionalKey(Schema.String),
  }),
).pipe(Schema.decodeTo(Forward.WorkstreamsForwardRequest));
const sourceRequest = Schema.Union([
  Raw.WorkstreamsSourceReadmissionRequestV2,
  Parent.WorkstreamsSourceParentReadmissionRequestV3,
]);
const sourceParameters = NativeWire.closedWorkstreamsNative(
  Schema.Struct({
    contract: Schema.Literals([
      Raw.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2,
      Parent.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
    ]),
    accepted_forward: Forward.WorkstreamsForwardIdentity,
    command_id: Schema.optionalKey(NativeWire.WorkstreamsNativeCommandId),
    command_request_sha256: Schema.optionalKey(NativeWire.WorkstreamsNativeDigest),
    receiving_home_pin_id: Schema.optionalKey(NativeWire.WorkstreamsNativeId),
    receiving_home_pin_revision: Schema.optionalKey(NativeWire.WorkstreamsNativeVersion),
  }),
).pipe(Schema.decodeTo(sourceRequest));

export const homeToolkit = Toolkit.make(
  Tool.make(Home.HOME_FORWARD_TOOL, {
    description:
      "Execute or replay a native Workstreams command, operation, or appearance write at its bound home. Requires the authenticated peer and current original-caller authority. Cannot establish a binding or redirect the request.",
    parameters: forwardParameters,
    success: Forward.WorkstreamsForwardResult,
    failure,
    failureMode: "return",
    dependencies: [...dependencies],
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, true)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make(Home.HOME_FORWARD_READBACK_TOOL, {
    description:
      "Look up the retained outcome of one exact native Workstreams forward at its original home. Unknown remains unknown and never authorizes another dispatch.",
    parameters: Forward.WorkstreamsForwardReadbackRequest,
    success: Forward.WorkstreamsForwardReadbackResult,
    failure,
    failureMode: "return",
    dependencies: [...dependencies],
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);

export const sourceToolkit = Toolkit.make(
  Tool.make(Source.SOURCE_READMISSION_TOOL, {
    description:
      "Readmit the original caller of an exact locally retained native Workstreams forward. Version 3 qualifies a parent; version 2 qualifies a raw command. This lookup cannot accept, dispatch, settle, relay, or grant permission.",
    parameters: sourceParameters,
    success: Schema.Union([
      Raw.WorkstreamsSourceReadmissionResponseV2,
      Parent.WorkstreamsSourceParentReadmissionResponseV3,
    ]),
    failure,
    failureMode: "return",
    dependencies: [...dependencies],
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);

const invoke = Effect.fn("NativeForwardMcp.invoke")(function* <A>(
  run: (invocation: Invocation.McpInvocationScope) => Effect.Effect<A, RegistryError>,
  write = false,
) {
  yield* Invocation.requireMcpCapability("orchestration");
  const invocation = yield* Invocation.McpInvocationContext;
  if (
    write &&
    invocation.client &&
    Invocation.clientModeCeiling(invocation.client).interactionMode === "plan"
  )
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "Native Workstreams writes are unavailable in plan mode.",
    });
  return yield* run(invocation).pipe(
    Effect.mapError(
      (cause) =>
        new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: `Native Workstreams forwarding refused: ${cause.code}.`,
        }),
    ),
  );
});

export const homeHandlers = Access.toLayer(
  homeToolkit,
  Effect.gen(function* () {
    const home = yield* Home.HomeTools;
    return {
      jones_workstreams_home_forward: Access.writes(
        (request) => invoke((invocation) => home.forward(request, invocation), true),
        { linkedCallers: "allowed" },
      ),
      jones_workstreams_home_forward_readback: Access.reads((request) =>
        invoke((invocation) => home.readback(request, invocation)),
      ),
    };
  }),
);

export const sourceHandlers = Access.toLayer(
  sourceToolkit,
  Effect.gen(function* () {
    const source = yield* Source.SourceReadmission;
    return {
      jones_workstreams_source_readmission: Access.reads((request) =>
        invoke((invocation) =>
          Effect.gen(function* () {
            if (request.contract === Parent.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3)
              return yield* source.lookupParent(request, invocation);
            return yield* source.lookupRaw(request, invocation);
          }),
        ),
      ),
    };
  }),
);

const retainedParameters = NativeWire.closedWorkstreamsNative(
  Schema.Struct({
    contract: Schema.Literal(RetainedWire.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1),
    source_readmission: Raw.WorkstreamsSourceReadmissionRequestV2,
    receiving_home_pin_id: NativeWire.WorkstreamsNativeId,
    receiving_home_pin_revision: NativeWire.WorkstreamsNativeVersion,
    kind: Schema.Literals(["attest", "settle_lookup", "settle"]),
    request: Schema.Union([
      WorkstreamsNativeAttestationRequest,
      WorkstreamsNativeSettlementRequest,
    ]),
  }),
).pipe(Schema.decodeTo(RetainedWire.WorkstreamsRetainedSourceEffectRequestV1));

export const retainedSourceToolkit = Toolkit.make(
  Tool.make(Retained.SOURCE_RETAINED_EFFECT_TOOL, {
    description:
      "Invoke the source provider for one exact accepted raw Workstreams command. Requires authenticated home and original-caller readmission. Lookup remains observation-only; this tool cannot accept a new forward, retarget it, or relay arbitrary work.",
    parameters: retainedParameters,
    success: RetainedWire.WorkstreamsRetainedSourceEffectResultV1,
    failure,
    failureMode: "return",
    dependencies: [...dependencies],
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, true)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);

export const retainedSourceHandlers = Access.toLayer(
  retainedSourceToolkit,
  Effect.gen(function* () {
    const source = yield* Retained.RetainedSourceEffects;
    return {
      jones_workstreams_source_retained_effect: Access.writes(
        (request) => invoke((invocation) => source.invoke(request, invocation), true),
        { linkedCallers: "allowed" },
      ),
    };
  }),
);
