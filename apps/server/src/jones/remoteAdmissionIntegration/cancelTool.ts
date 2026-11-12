import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { Toolkit } from "effect/ai";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as RemoteAdmissionIntake from "./RemoteAdmissionIntake.ts";
import { CancelRemoteAdmissionTool } from "./tools.ts";

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
