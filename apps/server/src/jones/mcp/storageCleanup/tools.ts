import {
  McpCapabilityUnavailableError,
  OrchestratorMcpFailure,
  StorageCleanupReportPage,
  StorageCleanupReportReadInput,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import { StorageCleanup } from "../../../storageCleanup.ts";
import { StorageCleanupReportReadError } from "../../storageCleanup/ReportRead.ts";

export const StorageCleanupReportToolkit = Toolkit.make(
  Tool.make("t3_storage_cleanup_report", {
    description:
      "Read the latest session-only aggregate cleanup report without running cleanup, evaluating proposals, probing files, or retiring worktrees. report:null means no report exists; an empty finished report is distinct. Reads retained full-detail entries, default 10 and maximum 50 per page, within a 256 KiB serialized MCP result budget; an oversized next entry is refused without truncation. Pass nextCursor as cursor with expectedSnapshot from the first page and unchanged exact worktreePath/threadId filters. Nonzero cursors require expectedSnapshot; changed reports, environments, or filters are refused. retainedTotal and matchedCount count retained rows only. Normal cleanup retains at most 200 entries: report.omittedCount counts unretained report rows, while a proposal's omittedCount means not evaluated. Omitted rows cannot be recovered here; neither paging completion nor zero omissions proves every worktree was evaluated or cleanup is complete. No persisted history or mutation authority.",
    parameters: StorageCleanupReportReadInput,
    success: StorageCleanupReportPage,
    failure: Schema.Union([
      McpCapabilityUnavailableError,
      OrchestratorMcpFailure,
      StorageCleanupReportReadError,
    ]),
    failureMode: "return",
    dependencies: [McpInvocationContext, StorageCleanup],
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);
