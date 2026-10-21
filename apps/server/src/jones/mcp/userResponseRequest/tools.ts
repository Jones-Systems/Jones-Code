import { McpCapabilityUnavailableError, OrchestratorMcpFailure } from "@t3tools/contracts";
import {
  UserResponseRequestInput,
  UserResponseRequestResult,
} from "@t3tools/contracts/jones/userResponseRequest";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import { UserResponseRequestMcpService } from "./UserResponseRequestMcpService.ts";

export const UserResponseRequestToolkit = Toolkit.make(
  Tool.make("t3_request_user_response", {
    description:
      "Request an explicit owner action in your current thread: a handoff, plan review, or decision. Give one nonblank instruction line (up to 500 characters). Free text is always available; omit choices unless there are real alternatives (at most four). Returns immediately after durable acceptance; the request survives completion and restart. Reuse clientRequestId only for the identical request. Retries require an eligible live calling provider; a completed caller cannot retry. Do not use for finished results, secrets, or native permission prompts. Do not enter app/provider Plan mode. Only claim the request exists after acceptance.",
    parameters: UserResponseRequestInput,
    success: UserResponseRequestResult,
    failure: Schema.Union([McpCapabilityUnavailableError, OrchestratorMcpFailure]),
    failureMode: "return",
    dependencies: [McpInvocationContext, ThreadManagementService, UserResponseRequestMcpService],
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);
