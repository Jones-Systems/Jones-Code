import {
  OrchestratorMcpFailure,
  RemoteAdmissionDigest,
  RemoteAdmissionPublicStatus,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool } from "effect/ai";
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
