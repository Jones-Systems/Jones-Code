import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { requireMcpCapability } from "../../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import * as StorageCleanup from "../../../storageCleanup.ts";
import {
  pageStorageCleanupReport,
  StorageCleanupReportReadError,
} from "../../storageCleanup/ReportRead.ts";
import { StorageCleanupReportToolkit } from "./tools.ts";

const isReportReadError = Schema.is(StorageCleanupReportReadError);

export const StorageCleanupReportHandlersLive = McpToolAccess.toLayer(StorageCleanupReportToolkit, {
  t3_storage_cleanup_report: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const caller = yield* requireMcpCapability("orchestration");
      const cleanup = yield* StorageCleanup.StorageCleanup;
      const report = yield* cleanup.latestReport;
      const page = pageStorageCleanupReport(caller.environmentId, report, input);
      return isReportReadError(page) ? yield* page : page;
    }),
  ),
});
