import {
  OrchestratorMcpFailure,
  RemoteAdmissionDigest,
  RemoteAdmissionPublicStatus,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as RemoteAdmissionIntake from "./RemoteAdmissionIntake.ts";

export const CancelRemoteAdmissionTool = Tool.make("t3_remote_admission_cancel", {
  description:
    "Withdraw a pending remote request before it starts. Only its originating connection may cancel it. This never approves a request.",
  parameters: Schema.Struct({ id: Schema.NonEmptyString, bindingDigest: RemoteAdmissionDigest }),
  success: RemoteAdmissionPublicStatus,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    RemoteAdmissionIntake.RemoteAdmissionIntake,
  ],
})
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.Destructive, false);
export const toolkit = Toolkit.make(CancelRemoteAdmissionTool);
export const handlers = McpToolAccess.toLayer(toolkit, {
  t3_remote_admission_cancel: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const intake = yield* RemoteAdmissionIntake.RemoteAdmissionIntake;
      if (!scope.client)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Only the originating remote connection may withdraw a request.",
        });
      return yield* intake.withdraw(input, scope.client.sessionId).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "request_conflict",
              message: "This remote request cannot be withdrawn by this connection.",
            }),
        ),
      );
    }),
  ),
});
