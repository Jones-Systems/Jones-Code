// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Native work and pinned-package metadata checks run only in the dedicated child.
import * as NodeModule from "node:module";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";

import type {
  DirItem,
  DirSearchResult,
  FileItem,
  FileFinder as FileFinderType,
  MixedItem,
  MixedSearchResult,
  Result,
  SearchResult,
} from "@ff-labs/fff-node";
import * as Effect from "effect/Effect";

import type {
  ProjectEntry,
  ProjectSearchContentsInput,
  ProjectSearchEntriesResult,
} from "@t3tools/contracts";
import { isProjectFaviconPath } from "@t3tools/shared/projectFavicon";

import { collectContentSearchPages } from "./contentSearchPages.ts";

import type { WorkspaceSearchIndex } from "../../workspace/WorkspaceSearchIndex.ts";
import type { WorkspaceIndexVariant as WorkspaceSearchIndexVariant } from "../serverParallelism/computeContract.ts";
import {
  WorkspaceSearchIndexCreateFailed,
  WorkspaceSearchIndexScanTimedOut,
  WorkspaceSearchIndexSearchFailed,
  WorkspaceSearchIndexRefreshFailed,
  WorkspaceSearchIndexDestroyFailed,
} from "../serverCompute/workspaceErrors.ts";
// fff-node stays external to the CLI bundle because it dlopens a native
// library. A static `import` of an external package is a hard error inside a
// Node single-executable (only built-ins resolve there), so load it through
// `require`, which reads from the real filesystem in every runtime.
const requireForFff = NodeModule.createRequire(import.meta.url);
// Resolve through the filesystem, including the repository's ASAR/native-loading patch.
// Refuse an accidentally substituted package before creating any finder.
let packageDirectory = dirname(requireForFff.resolve("@ff-labs/fff-node"));
let pinned = false;
for (let depth = 0; depth < 4; depth++) {
  try {
    const metadata = JSON.parse(readFileSync(join(packageDirectory, "package.json"), "utf8")) as {
      name?: string;
      version?: string;
    };
    if (metadata.name === "@ff-labs/fff-node") {
      pinned = metadata.version === "0.11.0";
      break;
    }
  } catch {
    /* The resolved JS entry may be nested below the package root. */
  }
  packageDirectory = dirname(packageDirectory);
}
if (!pinned) throw new Error("Unsupported workspace native package.");
const { FileFinder } = requireForFff("@ff-labs/fff-node") as typeof import("@ff-labs/fff-node");

const WORKSPACE_INDEX_MAX_ENTRIES = 25_000;
const WORKSPACE_INDEX_PAGE_SIZE = WORKSPACE_INDEX_MAX_ENTRIES + 2;
const WORKSPACE_INDEX_SCAN_TIMEOUT = "15 seconds";
const WORKSPACE_INDEX_SCAN_TIMEOUT_MS = 15_000;
const CONTENT_SEARCH_MAX_MATCHES_PER_FILE = 100;

function toPosixPath(input: string): string {
  return input.replaceAll("\\", "/");
}

function trimDirectorySeparator(input: string): string {
  return input.endsWith("/") ? input.slice(0, -1) : input;
}

function parentPathOf(input: string): string | undefined {
  const separatorIndex = input.lastIndexOf("/");
  return separatorIndex === -1 ? undefined : input.slice(0, separatorIndex);
}

function toProjectEntry(item: MixedItem): ProjectEntry | null {
  const normalizedPath = trimDirectorySeparator(toPosixPath(item.item.relativePath));
  if (!normalizedPath) {
    return null;
  }

  return {
    path: normalizedPath,
    kind: item.type,
  };
}

function toFileEntry(item: FileItem): ProjectEntry | null {
  const normalizedPath = trimDirectorySeparator(toPosixPath(item.relativePath));
  return normalizedPath ? { path: normalizedPath, kind: "file" } : null;
}

function toDirectoryEntry(item: DirItem): ProjectEntry | null {
  const normalizedPath = trimDirectorySeparator(toPosixPath(item.relativePath));
  return normalizedPath ? { path: normalizedPath, kind: "directory" } : null;
}

function mapFileSearchResult(
  result: SearchResult,
  limit: number,
  imageOnly = false,
): ProjectSearchEntriesResult {
  const entries = result.items.flatMap((item) => {
    const entry = toFileEntry(item);
    return entry && (!imageOnly || isProjectFaviconPath(entry.path)) ? [entry] : [];
  });
  return {
    entries: entries.slice(0, limit),
    truncated: entries.length > limit || result.totalMatched > result.items.length,
  };
}

function mapDirectorySearchResult(
  result: DirSearchResult,
  limit: number,
): ProjectSearchEntriesResult {
  const entries = result.items.flatMap((item) => {
    const entry = toDirectoryEntry(item);
    return entry ? [entry] : [];
  });
  const rootDirectoryCount = result.items.some((item) => item.relativePath.length === 0) ? 1 : 0;
  return {
    entries: entries.slice(0, limit),
    truncated: result.totalMatched - rootDirectoryCount > limit,
  };
}

function mapMixedSearchResult(
  result: MixedSearchResult,
  limit: number,
): { readonly entries: ProjectEntry[]; readonly truncated: boolean } {
  const entries: ProjectEntry[] = [];
  for (const item of result.items) {
    const entry = toProjectEntry(item);
    if (entry) {
      entries.push(entry);
    }
    if (entries.length >= limit) {
      break;
    }
  }

  const rootDirectoryCount = result.items.some(
    (item) => item.type === "directory" && item.item.relativePath.length === 0,
  )
    ? 1
    : 0;
  return {
    entries,
    truncated: result.totalMatched - rootDirectoryCount > limit,
  };
}

const WORD_CHARACTER = /[\p{Letter}\p{Mark}\p{Number}_]/u;

function codePointAt(line: string, index: number): string | undefined {
  const codePoint = line.codePointAt(index);
  return codePoint === undefined ? undefined : String.fromCodePoint(codePoint);
}

function codePointBefore(line: string, index: number): string | undefined {
  if (index <= 0) return undefined;
  const previousCodeUnit = line.charCodeAt(index - 1);
  const previousIndex =
    previousCodeUnit >= 0xdc00 && previousCodeUnit <= 0xdfff ? index - 2 : index - 1;
  return codePointAt(line, previousIndex);
}

function buildContentSearchQuery(input: Omit<ProjectSearchContentsInput, "cwd">): {
  readonly searchQuery: string;
  readonly regexMode: boolean;
} {
  if (input.caseSensitive) {
    return { searchQuery: input.query, regexMode: input.useRegex };
  }
  return input.useRegex
    ? { searchQuery: `(?i)${input.query}`, regexMode: true }
    : { searchQuery: input.query.toLowerCase(), regexMode: false };
}

function mapContentMatchRanges(
  line: string,
  byteRanges: ReadonlyArray<readonly [number, number]>,
): Array<{ readonly start: number; readonly end: number }> {
  const lineBytes = Buffer.from(line);
  const toStringIndex = (byteOffset: number) => lineBytes.subarray(0, byteOffset).toString().length;
  return byteRanges.map(([startByte, endByte]) => ({
    start: toStringIndex(startByte),
    end: toStringIndex(endByte),
  }));
}

// Post-filter whole words: consuming boundary regexes swallow adjacent separators and widen ranges; \b misses punctuation edges.
function isWholeWordRange(
  line: string,
  range: { readonly start: number; readonly end: number },
): boolean {
  if (range.end <= range.start) return false;
  const isWord = (character: string | undefined) =>
    character !== undefined && WORD_CHARACTER.test(character);
  const leftIsBoundary =
    range.start === 0 ||
    !isWord(codePointBefore(line, range.start)) ||
    !isWord(codePointAt(line, range.start));
  const rightIsBoundary =
    range.end >= line.length ||
    !isWord(codePointAt(line, range.end)) ||
    !isWord(codePointBefore(line, range.end));
  return leftIsBoundary && rightIsBoundary;
}

function withDirectoryAncestors(entries: ReadonlyArray<ProjectEntry>): ProjectEntry[] {
  const entryByPath = new Map(entries.map((entry) => [entry.path, entry]));
  for (const entry of entries) {
    let parentPath = parentPathOf(entry.path);
    while (parentPath) {
      if (!entryByPath.has(parentPath)) {
        entryByPath.set(parentPath, { path: parentPath, kind: "directory" });
      }
      parentPath = parentPathOf(parentPath);
    }
  }
  return [...entryByPath.values()];
}

const createFinder = Effect.fn("WorkspaceSearchIndex.createFinder")(function* (
  cwd: string,
  variant: WorkspaceSearchIndexVariant,
  qualification?: { readonly logFilePath: string },
) {
  const result = yield* Effect.try({
    try: () =>
      FileFinder.create({
        basePath: cwd,
        ...(qualification ? { logFilePath: qualification.logFilePath } : {}),
        disableMmapCache: true,
        // Content indexing costs CPU/memory; path-only consumers use the lightweight index.
        disableContentIndexing: variant !== "content",
        disableWatch: false,
        cacheBudgetMaxFiles: 1024,
        cacheBudgetMaxBytes: 32 * 1024 * 1024,
        // fff also applies its cache item limit to uncached grep reads. Keep its
        // default searchable file size; the byte/count limits still bound retained cache.
        aiMode: false,
        enableFsRootScanning: true,
        enableHomeDirScanning: true,
      }),
    catch: (cause) =>
      new WorkspaceSearchIndexCreateFailed({
        cwd,
        reason: "FileFinder.create threw unexpectedly.",
        cause,
      }),
  });
  if (result.ok) return result.value;
  return yield* new WorkspaceSearchIndexCreateFailed({
    cwd,
    reason: result.error,
  });
});

const waitForIndexReady = Effect.fn("WorkspaceSearchIndex.waitForIndexReady")(function* <E>(
  cwd: string,
  finder: FileFinderType,
  onFailure: (input: { readonly reason: string; readonly cause?: unknown }) => E,
): Effect.fn.Return<void, E | WorkspaceSearchIndexScanTimedOut> {
  const result = yield* Effect.tryPromise({
    try: () => finder.waitForIndexReady(WORKSPACE_INDEX_SCAN_TIMEOUT_MS),
    catch: (cause) =>
      onFailure({
        reason: "FileFinder.waitForIndexReady rejected unexpectedly.",
        cause,
      }),
  }).pipe(Effect.uninterruptible); // fff readiness polling has no AbortSignal; retain ownership until its Promise settles.
  if (!result.ok) {
    return yield* Effect.fail(onFailure({ reason: result.error }));
  }
  if (!result.value) {
    return yield* new WorkspaceSearchIndexScanTimedOut({
      cwd,
      timeout: WORKSPACE_INDEX_SCAN_TIMEOUT,
    });
  }
});

export const make = Effect.fn("WorkspaceSearchIndex.make")(function* (
  cwd: string,
  variant: WorkspaceSearchIndexVariant = "paths",
  qualification?: { readonly logFilePath: string },
) {
  const finder = yield* Effect.acquireRelease(createFinder(cwd, variant, qualification), (finder) =>
    Effect.try({
      try: () => finder.destroy(),
      catch: (cause) => new WorkspaceSearchIndexDestroyFailed({ cwd, cause }),
    }).pipe(Effect.orDie),
  );
  let initialScanTimedOut = false;
  yield* waitForIndexReady(
    cwd,
    finder,
    ({ reason, cause }) =>
      new WorkspaceSearchIndexCreateFailed({
        cwd,
        reason,
        cause,
      }),
  ).pipe(
    Effect.catchTags({
      WorkspaceSearchIndexScanTimedOut: (error) =>
        variant === "paths"
          ? Effect.sync(() => {
              initialScanTimedOut = true;
            })
          : Effect.fail(error),
    }),
  );

  const hasIncompleteInitialScan = () => initialScanTimedOut && finder.isScanning();

  const runSearch = Effect.fn("WorkspaceSearchIndex.runSearch")(function* <A>(
    query: string,
    pageSize: number,
    operation: "directorySearch" | "fileSearch" | "grep" | "mixedSearch",
    execute: () => Result<A>,
  ): Effect.fn.Return<A, WorkspaceSearchIndexSearchFailed> {
    const result = yield* Effect.try({
      try: execute,
      catch: (cause) =>
        new WorkspaceSearchIndexSearchFailed({
          cwd,
          queryLength: query.length,
          pageSize,
          reason: `FileFinder.${operation} threw unexpectedly.`,
          cause,
        }),
    });
    if (!result.ok) {
      return yield* new WorkspaceSearchIndexSearchFailed({
        cwd,
        queryLength: query.length,
        pageSize,
        reason: result.error,
      });
    }
    return result.value;
  });

  const refresh: WorkspaceSearchIndex["Service"]["refresh"] = Effect.fn(
    "WorkspaceSearchIndex.refresh",
  )(function* () {
    const result = yield* Effect.try({
      try: () => finder.scanFiles(),
      catch: (cause) =>
        new WorkspaceSearchIndexRefreshFailed({
          cwd,
          reason: "FileFinder.scanFiles threw unexpectedly.",
          cause,
        }),
    });
    if (!result.ok) {
      return yield* new WorkspaceSearchIndexRefreshFailed({
        cwd,
        reason: result.error,
      });
    }
    yield* waitForIndexReady(
      cwd,
      finder,
      ({ reason, cause }) =>
        new WorkspaceSearchIndexRefreshFailed({
          cwd,
          reason,
          cause,
        }),
    );
  });

  const list: WorkspaceSearchIndex["Service"]["list"] = Effect.fn("WorkspaceSearchIndex.list")(
    function* () {
      const incompleteBeforeQuery = hasIncompleteInitialScan();
      const result = yield* runSearch("", WORKSPACE_INDEX_PAGE_SIZE, "mixedSearch", () =>
        finder.mixedSearch("", { pageSize: WORKSPACE_INDEX_PAGE_SIZE, maxThreads: 1 }),
      );
      const mapped = mapMixedSearchResult(result, WORKSPACE_INDEX_MAX_ENTRIES);
      const sortedEntries = withDirectoryAncestors(mapped.entries).toSorted((left, right) =>
        left.path.localeCompare(right.path),
      );
      const entries = sortedEntries.slice(0, WORKSPACE_INDEX_MAX_ENTRIES);
      return {
        entries,
        truncated:
          incompleteBeforeQuery || mapped.truncated || entries.length < sortedEntries.length,
      };
    },
  );

  const search: WorkspaceSearchIndex["Service"]["search"] = Effect.fn(
    "WorkspaceSearchIndex.search",
  )(function* (query, limit, kind, imageOnly) {
    const pageSize = imageOnly ? WORKSPACE_INDEX_PAGE_SIZE : Math.max(1, limit + 1);
    const incompleteBeforeQuery = hasIncompleteInitialScan();
    if (kind === "file" || imageOnly) {
      const result = yield* runSearch(query, pageSize, "fileSearch", () =>
        finder.fileSearch(query, { pageSize, maxThreads: 1 }),
      );
      const mapped = mapFileSearchResult(result, limit, imageOnly);
      return { ...mapped, truncated: incompleteBeforeQuery || mapped.truncated };
    }
    if (kind === "directory") {
      const result = yield* runSearch(query, pageSize, "directorySearch", () =>
        finder.directorySearch(query, { pageSize, maxThreads: 1 }),
      );
      const mapped = mapDirectorySearchResult(result, limit);
      return { ...mapped, truncated: incompleteBeforeQuery || mapped.truncated };
    }
    const result = yield* runSearch(query, pageSize, "mixedSearch", () =>
      finder.mixedSearch(query, { pageSize, maxThreads: 1 }),
    );
    const mapped = mapMixedSearchResult(result, limit);
    return { ...mapped, truncated: incompleteBeforeQuery || mapped.truncated };
  });

  const searchContents: WorkspaceSearchIndex["Service"]["searchContents"] = Effect.fn(
    "WorkspaceSearchIndex.searchContents",
  )(function* (input) {
    const { searchQuery, regexMode } = buildContentSearchQuery(input);
    // Grep cursors advance by file, so whole-word post-filtering needs enough
    // raw candidates from the current file before moving to the next one.
    const rawPageSize = input.wholeWord
      ? Math.max(input.limit, CONTENT_SEARCH_MAX_MATCHES_PER_FILE)
      : input.limit;
    return yield* collectContentSearchPages({
      limit: input.limit,
      searchPage: (cursor, budget) =>
        runSearch(input.query, input.limit, "grep", () =>
          finder.grep(searchQuery, {
            mode: regexMode ? "regex" : "plain",
            smartCase: !input.caseSensitive && !regexMode,
            maxMatchesPerFile: Math.min(CONTENT_SEARCH_MAX_MATCHES_PER_FILE, rawPageSize),
            pageSize: rawPageSize,
            cursor,
            ...budget,
          }),
        ),
      mapMatch: (match) => {
        const matchRanges = mapContentMatchRanges(match.lineContent, match.matchRanges).filter(
          (range) => !input.wholeWord || isWholeWordRange(match.lineContent, range),
        );
        if (matchRanges.length === 0) return undefined;
        return {
          path: toPosixPath(match.relativePath),
          lineNumber: match.lineNumber,
          lineContent: match.lineContent,
          matchRanges,
        };
      },
    });
  });

  return {
    list,
    refresh,
    search,
    searchContents,
    status: () => {
      const progress = finder.getScanProgress();
      if (!progress.ok) throw new Error("Native scan state unavailable.");
      // Search readiness only: post-scan work and watcher-triggered native work may still exist.
      // Physical ownership is reclaimed exclusively by captured child exit, never by this sample.
      return {
        scanning: progress.value.isScanning,
        warmupComplete: progress.value.isWarmupComplete,
      };
    },
  };
});
