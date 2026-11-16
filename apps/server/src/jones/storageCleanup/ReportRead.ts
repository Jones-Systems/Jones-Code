// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Hash the exact in-memory report and measure the serialized response without filesystem or cleanup effects.
import * as NodeCrypto from "node:crypto";
import type {
  EnvironmentId,
  StorageCleanupReport,
  StorageCleanupReportPage,
  StorageCleanupReportReadInput,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const MAX_STORAGE_CLEANUP_REPORT_RESPONSE_BYTES = 256 * 1024;

export class StorageCleanupReportReadError extends Schema.TaggedError<StorageCleanupReportReadError>()(
  "StorageCleanupReportReadError",
  {
    reason: Schema.Literals([
      "invalid_cursor",
      "invalid_limit",
      "invalid_snapshot",
      "snapshot_required",
      "stale_snapshot",
      "response_too_large",
    ]),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "invalid_cursor":
        return "Cursor must be a nonnegative safe integer within the matched retained entries.";
      case "invalid_limit":
        return "Limit must be an integer from 1 to 50.";
      case "invalid_snapshot":
        return "expectedSnapshot must be a lowercase SHA-256 digest.";
      case "snapshot_required":
        return "A nonzero cursor requires expectedSnapshot from the first page.";
      case "stale_snapshot":
        return "The report, environment, or filters changed. Restart from cursor zero without expectedSnapshot.";
      case "response_too_large":
        return "Report metadata or the next complete entry exceeds the response budget. No detail was truncated.";
    }
  }
}

export function pageStorageCleanupReport(
  environmentId: EnvironmentId,
  report: StorageCleanupReport | null,
  input: Omit<StorageCleanupReportReadInput, "environmentId">,
): StorageCleanupReportPage | StorageCleanupReportReadError {
  const cursor = input.cursor ?? 0;
  const limit = input.limit ?? 10;
  if (!Number.isSafeInteger(cursor) || cursor < 0)
    return new StorageCleanupReportReadError({ reason: "invalid_cursor" });
  if (!Number.isInteger(limit) || limit < 1 || limit > 50)
    return new StorageCleanupReportReadError({ reason: "invalid_limit" });
  if (
    input.expectedSnapshot !== undefined &&
    !/^[a-f0-9]{64}(?![\s\S])/.test(input.expectedSnapshot)
  )
    return new StorageCleanupReportReadError({ reason: "invalid_snapshot" });
  if (cursor !== 0 && input.expectedSnapshot === undefined)
    return new StorageCleanupReportReadError({ reason: "snapshot_required" });

  const selectors = {
    worktreePath: input.worktreePath ?? null,
    threadId: input.threadId ?? null,
  };
  const snapshotId = NodeCrypto.createHash("sha256")
    .update(JSON.stringify({ environmentId, selectors, report }))
    .digest("hex");
  if (input.expectedSnapshot !== undefined && input.expectedSnapshot !== snapshotId)
    return new StorageCleanupReportReadError({ reason: "stale_snapshot" });

  const matched = (report?.entries ?? []).filter(
    (entry) =>
      (selectors.worktreePath === null ||
        (entry.kind === "worktree" && entry.path === selectors.worktreePath)) &&
      (selectors.threadId === null || entry.threadId === selectors.threadId),
  );
  if (cursor > matched.length)
    return new StorageCleanupReportReadError({ reason: "invalid_cursor" });

  const makePage = (entries: StorageCleanupReport["entries"]): StorageCleanupReportPage => ({
    authorityEffect: "none",
    environmentId,
    retention: "session-only",
    snapshotId,
    selectors,
    retainedTotal: report?.entries.length ?? 0,
    matchedCount: matched.length,
    cursor,
    nextCursor: cursor + entries.length < matched.length ? cursor + entries.length : null,
    report: report === null ? null : { ...report, entries },
  });
  // MCP emits both structured content and JSON text. Budget their serialized envelope,
  // including JSON escaping, rather than bounding only the number of nested entries.
  const fits = (page: StorageCleanupReportPage) =>
    Buffer.byteLength(
      JSON.stringify({
        content: [{ type: "text", text: JSON.stringify(page) }],
        structuredContent: page,
        isError: false,
      }),
      "utf8",
    ) <= MAX_STORAGE_CLEANUP_REPORT_RESPONSE_BYTES;
  let page = makePage([]);
  if (!fits(page)) return new StorageCleanupReportReadError({ reason: "response_too_large" });
  const entries: StorageCleanupReport["entries"][number][] = [];
  for (const entry of matched.slice(cursor, cursor + limit)) {
    const candidate = makePage([...entries, entry]);
    if (!fits(candidate)) {
      if (entries.length === 0)
        return new StorageCleanupReportReadError({ reason: "response_too_large" });
      break;
    }
    entries.push(entry);
    page = candidate;
  }
  return page;
}
