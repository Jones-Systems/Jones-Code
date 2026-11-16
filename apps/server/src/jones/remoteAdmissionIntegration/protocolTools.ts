import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as DTO from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Access from "../../mcp/McpToolAccess.ts";
import { type makePeerProtocol } from "./PeerProtocol.ts";
import { ReceiverInspection } from "./Inspection.ts";

// MCP requires an object root. Keep the contract's exact field schemas and decode
// back into its union after checking the raw keys, before optional JSON fields
// can normalize null to absence or Struct decoding can discard another selector.
const observationFields = {
  clientRequestId: Schema.optionalKey(
    DTO.RemoteAdmissionObservationInput.members[0].fields.clientRequestId,
  ),
  attemptId: Schema.optionalKey(DTO.RemoteAdmissionObservationInput.members[1].fields.attemptId),
  observationKey: Schema.optionalKey(
    DTO.RemoteAdmissionObservationInput.members[1].fields.observationKey,
  ),
};
const observationSelector = Schema.makeFilter<{
  readonly clientRequestId?: string;
  readonly attemptId?: string;
  readonly observationKey?: string;
}>(
  (input) =>
    Object.hasOwn(input, "clientRequestId")
      ? !Object.hasOwn(input, "attemptId") && !Object.hasOwn(input, "observationKey")
      : Object.hasOwn(input, "attemptId") && Object.hasOwn(input, "observationKey"),
  {
    expected: "either clientRequestId or both attemptId and observationKey, never mixed selectors",
    toJsonSchema: () => ({
      oneOf: [
        {
          required: ["clientRequestId"],
          not: { anyOf: [{ required: ["attemptId"] }, { required: ["observationKey"] }] },
        },
        { required: ["attemptId", "observationKey"], not: { required: ["clientRequestId"] } },
      ],
    }),
  },
);
const ObservationParameters = Schema.flip(
  Schema.flip(Schema.Struct(observationFields)).check(observationSelector),
).pipe(Schema.decodeTo(DTO.RemoteAdmissionObservationInput));
const AckParameters = Schema.flip(
  Schema.flip(
    Schema.Struct({
      ...observationFields,
      disposition: DTO.RemoteAdmissionAckInput.members[0].fields.disposition,
    }),
  ).check(observationSelector),
).pipe(Schema.decodeTo(DTO.RemoteAdmissionAckInput));
const RecoveryStatusParameters = Schema.flip(
  Schema.flip(
    Schema.Struct({
      locator: Schema.optionalKey(DTO.RemoteAdmissionRecoveryStatusInput.members[0].fields.locator),
      clientRequestId: Schema.optionalKey(
        DTO.RemoteAdmissionRecoveryStatusInput.members[1].fields.clientRequestId,
      ),
      originThreadId: Schema.optionalKey(
        DTO.RemoteAdmissionRecoveryStatusInput.members[1].fields.originThreadId,
      ),
    }),
  ).check(
    Schema.makeFilter(
      (input) =>
        Object.hasOwn(input, "locator")
          ? !Object.hasOwn(input, "clientRequestId") && !Object.hasOwn(input, "originThreadId")
          : Object.hasOwn(input, "clientRequestId") && Object.hasOwn(input, "originThreadId"),
      {
        expected:
          "either locator or both clientRequestId and originThreadId, never mixed selectors",
        toJsonSchema: () => ({
          oneOf: [
            {
              required: ["locator"],
              not: { anyOf: [{ required: ["clientRequestId"] }, { required: ["originThreadId"] }] },
            },
            { required: ["clientRequestId", "originThreadId"], not: { required: ["locator"] } },
          ],
        }),
      },
    ),
  ),
).pipe(Schema.decodeTo(DTO.RemoteAdmissionRecoveryStatusInput));

export class ReceiverPeerProtocol extends Context.Service<
  ReceiverPeerProtocol,
  ReturnType<typeof makePeerProtocol>
>()("t3/jones/remoteAdmissionIntegration/protocolTools/ReceiverPeerProtocol") {}
const dependencies = [Invocation.McpInvocationContext, ReceiverPeerProtocol];
const ObserveTool = Tool.make("t3_remote_admission_observe", {
  description:
    "Read this linked session’s exact retained launch observation. Reading registers or acknowledges nothing.",
  parameters: ObservationParameters,
  success: DTO.RemoteAdmissionObservation,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
}).annotate(Tool.Readonly, true);
const AckTool = Tool.make("t3_remote_admission_ack", {
  description:
    "Explicit acknowledgement after a committed original/retry wake or recorded suppression, bound to this exact session and observation.",
  parameters: AckParameters,
  success: DTO.RemoteAdmissionAckResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
}).annotate(Tool.Idempotent, true);
const PeerStatusTool = Tool.make("t3_remote_admission_peer_status", {
  description:
    "Pure status filtered to this exact linked session. No owner data or registration effects.",
  parameters: DTO.RemoteAdmissionPeerStatusInput,
  success: DTO.RemoteAdmissionPeerStatus,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
}).annotate(Tool.Readonly, true);
const CapabilitiesRegisterTool = Tool.make("t3_remote_admission_capabilities_register", {
  description:
    "Explicitly register independent protocol versions for this session and coordinator generation. Grants no authority.",
  parameters: DTO.RemoteAdmissionCapabilitiesRegisterInput,
  success: DTO.RemoteAdmissionCapabilitiesRegisterResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
}).annotate(Tool.Idempotent, true);
const AttemptRegisterTool = Tool.make("t3_remote_admission_attempt_register", {
  description: "Register a durably recorded retry observation. Does not launch the child directly.",
  parameters: DTO.RemoteAdmissionAttemptRegisterInput,
  success: DTO.RemoteAdmissionAttemptRegisterResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
}).annotate(Tool.Idempotent, true);
const NoticeRegisterTool = Tool.make("t3_remote_admission_notice_register", {
  description: "Register a distinct durably recorded recovered-result observation.",
  parameters: DTO.RemoteAdmissionNoticeRegisterInput,
  success: DTO.RemoteAdmissionNoticeRegisterResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
}).annotate(Tool.Idempotent, true);
const NoticeAckTool = Tool.make("t3_remote_admission_notice_ack", {
  description: "Acknowledge only a committed wake or recorded suppression for this exact notice.",
  parameters: DTO.RemoteAdmissionNoticeAckInput,
  success: DTO.RemoteAdmissionAckResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
}).annotate(Tool.Idempotent, true);
const RecoveryStatusTool = Tool.make("t3_remote_admission_recovery_status", {
  description:
    "Authorized pure inspection of an exact request. A copied locator confers no access. Retry, Recheck and Restore remain owner UI actions.",
  parameters: RecoveryStatusParameters,
  success: DTO.RemoteAdmissionRecoveryStatus,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [Invocation.McpInvocationContext, ReceiverInspection],
}).annotate(Tool.Readonly, true);
export const toolkit = Toolkit.make(
  ObserveTool,
  AckTool,
  PeerStatusTool,
  CapabilitiesRegisterTool,
  AttemptRegisterTool,
  NoticeRegisterTool,
  NoticeAckTool,
  RecoveryStatusTool,
);
const isOrchestratorMcpFailure = Schema.is(OrchestratorMcpFailure);
const failure = (cause: unknown) =>
  isOrchestratorMcpFailure(cause)
    ? cause
    : new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "Remote request protocol is unavailable.",
      });
export const handlers = Access.toLayer(toolkit, {
  t3_remote_admission_observe: Access.reads((input) =>
    Effect.gen(function* () {
      const s = yield* Invocation.McpInvocationContext;
      const p = yield* ReceiverPeerProtocol;
      return yield* p.observe(s, input);
    }).pipe(Effect.mapError(failure)),
  ),
  t3_remote_admission_ack: Access.reads((input) =>
    Effect.gen(function* () {
      const s = yield* Invocation.McpInvocationContext;
      const p = yield* ReceiverPeerProtocol;
      return yield* p.ack(s, input);
    }).pipe(Effect.mapError(failure)),
  ),
  t3_remote_admission_peer_status: Access.reads((input) =>
    Effect.gen(function* () {
      const s = yield* Invocation.McpInvocationContext;
      const p = yield* ReceiverPeerProtocol;
      return yield* p.status(s, input);
    }).pipe(Effect.mapError(failure)),
  ),
  t3_remote_admission_capabilities_register: Access.reads((input) =>
    Effect.gen(function* () {
      const s = yield* Invocation.McpInvocationContext;
      const p = yield* ReceiverPeerProtocol;
      return yield* p.registerCapabilities(s, input);
    }).pipe(Effect.mapError(failure)),
  ),
  t3_remote_admission_attempt_register: Access.reads((input) =>
    Effect.gen(function* () {
      const s = yield* Invocation.McpInvocationContext;
      const p = yield* ReceiverPeerProtocol;
      return yield* p.registerAttempt(s, input);
    }).pipe(Effect.mapError(failure)),
  ),
  t3_remote_admission_notice_register: Access.reads((input) =>
    Effect.gen(function* () {
      const s = yield* Invocation.McpInvocationContext;
      const p = yield* ReceiverPeerProtocol;
      return yield* p.registerNotice(s, input);
    }).pipe(Effect.mapError(failure)),
  ),
  t3_remote_admission_notice_ack: Access.reads((input) =>
    Effect.gen(function* () {
      const s = yield* Invocation.McpInvocationContext;
      const p = yield* ReceiverPeerProtocol;
      return yield* p.ackNotice(s, input);
    }).pipe(Effect.mapError(failure)),
  ),
  t3_remote_admission_recovery_status: Access.reads((input) =>
    ReceiverInspection.pipe(Effect.flatMap((p) => p.status(input))),
  ),
});
