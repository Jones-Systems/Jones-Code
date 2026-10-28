import * as Effect from "effect/Effect";
import * as ServerCompute from "../serverCompute/ServerCompute.ts";
import type { ComputeFailure } from "../serverCompute/failure.ts";
import type { ProjectEntryKind } from "@t3tools/contracts";
import type { WorkspaceKey, WorkspaceJobs } from "../serverParallelism/computeContract.ts";
import type { WorkspaceSearchIndex } from "../../workspace/WorkspaceSearchIndex.ts";
import { WorkspaceSearchIndexCreateFailed, WorkspaceSearchIndexScanTimedOut,
  WorkspaceSearchIndexSearchFailed, WorkspaceSearchIndexRefreshFailed } from "../serverCompute/workspaceErrors.ts";

/** Only this boundary translates bounded compute failures into the existing workspace error tags. */
export const makeSearchWorkerProxy = Effect.fn("WorkspaceSearchIndex.proxy")(function* (key: WorkspaceKey) {
  const compute = yield* ServerCompute.ServerCompute;
  const timeout = () => new WorkspaceSearchIndexScanTimedOut({ cwd: key.cwd, timeout: "15 seconds" });
  const handle = yield* compute.acquireLogical(key).pipe(Effect.mapError((cause) =>
    cause.scanTimedOut ? timeout() : new WorkspaceSearchIndexCreateFailed({ cwd: key.cwd, reason: cause.reason, cause })));
  const searchFailure = (cause: ComputeFailure, queryLength: number, pageSize: number) =>
    new WorkspaceSearchIndexSearchFailed({ cwd: key.cwd, queryLength, pageSize, reason: cause.reason, cause });
  const list: WorkspaceSearchIndex["Service"]["list"] = Effect.fn("WorkspaceSearchIndex.list")(() =>
    compute.run(handle, "list", undefined).pipe(Effect.mapError((cause) => searchFailure(cause, 0, 25_002))));
  const search: WorkspaceSearchIndex["Service"]["search"] = Effect.fn("WorkspaceSearchIndex.search")((query: string, limit: number, kind?: ProjectEntryKind, imageOnly?: boolean) =>
    compute.run(handle, "search", { query, limit,
      ...(kind === undefined ? {} : { kind }), ...(imageOnly === undefined ? {} : { imageOnly }) })
      .pipe(Effect.mapError((cause) => searchFailure(cause, query.length, imageOnly ? 25_002 : Math.max(1, limit + 1)))));
  const searchContents: WorkspaceSearchIndex["Service"]["searchContents"] = Effect.fn("WorkspaceSearchIndex.searchContents")((input: WorkspaceJobs["searchContents"]["input"]) =>
    compute.run(handle, "searchContents", { query: input.query, limit: input.limit,
      caseSensitive: input.caseSensitive, wholeWord: input.wholeWord, useRegex: input.useRegex })
      .pipe(Effect.mapError((cause) => searchFailure(cause, input.query.length, input.limit))));
  const refresh: WorkspaceSearchIndex["Service"]["refresh"] = Effect.fn("WorkspaceSearchIndex.refresh")(() =>
    compute.run(handle, "refresh", undefined).pipe(Effect.mapError((cause) =>
      cause.scanTimedOut ? timeout() : new WorkspaceSearchIndexRefreshFailed({ cwd: key.cwd, reason: cause.reason, cause }))));
  return { list, search, searchContents, refresh };
});
