import {
  OrchestratorMcpFailure,
  PreviewAutomationError,
  PreviewRecordingOperationInput,
  PreviewRecordingOperationResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool } from "effect/ai";

import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import * as PreviewAutomationBroker from "../../mcp/PreviewAutomationBroker.ts";

export const PreviewRecordingOperationTool = Tool.make("preview_recording_operation", {
  description:
    "Read retained broker evidence for a recording start/stop on an explicit tab. Optional handle must contain the original requestId, clientId and connectionId. Without it, discover a unique retained operation or bounded ambiguity; this is neither latest/current recorder state nor complete history. Dispatch-unbound operations cannot be discovered. Settled means an exact late reply was accepted, not native completion or artifact transfer. Unavailable means not_retained, including after eviction or restart. Never contacts a host, changes a fence, retries recording or claims a file. An older server without this tool is unsupported; do not fall back to a mutation.",
  parameters: PreviewRecordingOperationInput,
  success: PreviewRecordingOperationResult,
  failure: Schema.Union([PreviewAutomationError, OrchestratorMcpFailure]),
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    PreviewAutomationBroker.PreviewAutomationBroker,
  ],
})
  .annotate(Tool.Title, "Read recording operation evidence")
  .annotate(Tool.Strict, true)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

export const handlers = {
  preview_recording_operation: McpToolAccess.readsAsCaller(
    (input: PreviewRecordingOperationInput) =>
      Effect.gen(function* () {
        const scope = yield* McpInvocationContext.requireThreadMcpCapability("preview");
        const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
        return yield* broker.readRecordingOperation({ scope, input });
      }),
  ),
} satisfies McpToolAccess.Handlers<{
  preview_recording_operation: typeof PreviewRecordingOperationTool;
}>;
