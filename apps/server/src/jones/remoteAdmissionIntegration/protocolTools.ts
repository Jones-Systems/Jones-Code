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

export class ReceiverPeerProtocol extends Context.Service<
  ReceiverPeerProtocol,
  ReturnType<typeof makePeerProtocol>
>()("t3/jones/remoteAdmissionIntegration/protocolTools/ReceiverPeerProtocol") {}
const dependencies = [Invocation.McpInvocationContext, ReceiverPeerProtocol];
const ObserveTool = Tool.make("t3_remote_admission_observe", {
  description:
    "Read this linked session’s exact retained launch observation. Reading registers or acknowledges nothing.",
  parameters: DTO.RemoteAdmissionObservationInput,
  success: DTO.RemoteAdmissionObservation,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
}).annotate(Tool.Readonly, true);
const AckTool = Tool.make("t3_remote_admission_ack", {
  description:
    "Explicit acknowledgement after a committed original/retry wake or recorded suppression, bound to this exact session and observation.",
  parameters: DTO.RemoteAdmissionAckInput,
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
  parameters: DTO.RemoteAdmissionRecoveryStatusInput,
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
const failure = (cause: unknown) =>
  Schema.is(OrchestratorMcpFailure)(cause)
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
