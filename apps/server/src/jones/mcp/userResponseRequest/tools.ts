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
      'Request an explicit owner action in your current thread: a handoff, plan review, or decision. Post involved context, renders or links in chat first. Give one nonblank Markdown instruction (up to 1,500 Unicode code points, optional choices ≤4). For an action handoff, supply a specific completion choice (for example, "Browser panel is open") and any known blocker choices, or set defaultDone=true to supply the standard "Done" option when choices are absent. Keep free text for problems or details. These are normal input options; clicking a single choice submits it immediately (or advances a multi-question request). For a free-text review or decision, omit defaultDone. Or use questions: 1–5 brief Markdown prompts (short setup, short bullets, question last; ≤1,500 code points each, a ceiling not a target), with optional header and up to six choices {label, description?}. Descriptions explain impact when useful. When asking the owner to choose between two or more genuine alternatives, put your recommended option first, suffix its label with "(Recommended)", and briefly explain why in its description or the prompt. Apply this to native input tools too. Do not invent a recommendation for action-status or action-result choices (for example, "Done" / "Blocked"), acknowledgements, or a request with only one option. The owner answers sequentially; free text is always available; nothing is preselected. Returns immediately after durable acceptance; the request survives completion and restart. Reuse clientRequestId only for the identical request. Retries require an eligible live calling provider; a completed caller cannot retry. Do not use for finished results, secrets, or native permission prompts. Do not enter app/provider Plan mode. Only claim the request exists after acceptance.',
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
