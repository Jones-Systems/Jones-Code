import {
  OrchestratorMcpFailure,
  SettlementRestoreMcpInput,
  SettlementRestoreMcpResult,
} from "@t3tools/contracts";
import { Tool, Toolkit } from "effect/ai";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import { SettlementRestore } from "../../settlementRestore/SettlementRestore.ts";

export const SettlementRestoreToolkit = Toolkit.make(
  Tool.make("t3_thread_settlement_restore", {
    description:
      "Inspect or undo one exact settlement of a thread. inspect returns the thread's saved placement from before its latest settle and whether it can still be restored. restore puts back that thread's own pin, pin order, active-list position and wake time, but only for the settleCommandId you observed: a newer settle, un-settle, pin, archive, reorder or snooze wins and the restore is rejected. It never revives pull request watches, cancelled requests, queued runs or provider sessions, and it leaves snooze alone. Restore only when the user asks to undo a settlement. Reuse clientRequestId when retrying; it maps to a stable restore command.",
    parameters: SettlementRestoreMcpInput,
    success: SettlementRestoreMcpResult,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
    dependencies: [McpInvocationContext, ThreadManagementService, SettlementRestore],
  })
    .annotate(Tool.Title, "Undo a thread settlement")
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);
