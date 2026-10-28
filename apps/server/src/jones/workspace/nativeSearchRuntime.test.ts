// @effect-diagnostics nodeBuiltinImport:off -- Native parity fixtures own only temporary synthetic workspaces.
import {
  FileFinder,
  type FileItem,
  type GrepCursor,
  type GrepOptions,
  type GrepResult,
} from "@ff-labs/fff-node";
import { afterEach, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { vi } from "vite-plus/test";

import * as WorkspaceSearchIndex from "./nativeSearchRuntime.ts";

afterEach(() => {
  vi.restoreAllMocks();
});

function fileItem(relativePath: string): FileItem {
  return {
    relativePath,
    fileName: relativePath.slice(relativePath.lastIndexOf("/") + 1),
    size: 1,
    modified: 0,
    accessFrecencyScore: 0,
    modificationFrecencyScore: 0,
    totalFrecencyScore: 0,
    gitStatus: "clean",
  };
}

it.effect("filters image searches before applying the result limit", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const items = [
        ...Array.from({ length: 200 }, (_, index) => fileItem(`src/file-${index}.ts`)),
        fileItem("public/icon.svg"),
      ];
      const fileSearch = vi.fn(() => ({
        ok: true as const,
        value: {
          items,
          scores: [],
          totalMatched: items.length,
          totalFiles: items.length,
        },
      }));
      const finder = {
        destroy: vi.fn(),
        waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: true })),
        fileSearch,
      } as unknown as FileFinder;
      vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: finder });

      const searchIndex = yield* WorkspaceSearchIndex.make("/workspace/project");
      const resultWithoutKind = yield* searchIndex.search("", 200, undefined, true);
      const resultWithDirectoryKind = yield* searchIndex.search("", 200, "directory", true);

      expect(resultWithoutKind.entries).toEqual([{ kind: "file", path: "public/icon.svg" }]);
      expect(resultWithDirectoryKind.entries).toEqual([{ kind: "file", path: "public/icon.svg" }]);
      expect(fileSearch).toHaveBeenCalledTimes(2);
      expect(fileSearch).toHaveBeenCalledWith("", { pageSize: 25_002, maxThreads: 1 });
    }),
  ),
);

it.effect("continues whole-word searches after a filtered grep page", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const nextCursor = {
        __brand: "GrepCursor",
        _offset: 1,
      } as GrepCursor;
      const grepResult = (
        lineContent: string,
        matchRanges: Array<[number, number]>,
        cursor: GrepCursor | null,
      ): GrepResult => ({
        items: [
          {
            relativePath: "src/words.ts",
            fileName: "words.ts",
            gitStatus: "unmodified",
            size: lineContent.length,
            modified: 0,
            isBinary: false,
            totalFrecencyScore: 0,
            accessFrecencyScore: 0,
            modificationFrecencyScore: 0,
            lineNumber: 1,
            col: 0,
            byteOffset: 0,
            lineContent,
            matchRanges,
          },
        ],
        totalMatched: 1,
        totalFilesSearched: 1,
        totalFiles: 1,
        filteredFileCount: 1,
        nextCursor: cursor,
      });
      const grep = vi.fn((_query: string, options?: GrepOptions) =>
        options?.cursor
          ? { ok: true as const, value: grepResult("needle", [[0, 6]], null) }
          : {
              ok: true as const,
              value: grepResult("needleSuffix", [[0, 6]], nextCursor),
            },
      );
      const finder = {
        destroy: vi.fn(),
        waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: true })),
        grep,
      } as unknown as FileFinder;
      vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: finder });

      const searchIndex = yield* WorkspaceSearchIndex.make("/workspace/project", "content");
      const result = yield* searchIndex.searchContents({
        query: "needle",
        limit: 1,
        caseSensitive: true,
        wholeWord: true,
        useRegex: false,
      });

      expect(result).toEqual({
        matches: [
          {
            path: "src/words.ts",
            lineNumber: 1,
            lineContent: "needle",
            matchRanges: [{ start: 0, end: 6 }],
          },
        ],
        truncated: false,
      });
      expect(grep).toHaveBeenCalledTimes(2);
      expect(grep.mock.calls[1]?.[1]?.cursor).toBe(nextCursor);
    }),
  ),
);

it.effect.each(["paths", "content"] as const)(
  "waits for %s index readiness after refresh scans",
  (variant) =>
    Effect.scoped(
      Effect.gen(function* () {
        const started = Promise.withResolvers<void>();
        const ready = Promise.withResolvers<Awaited<ReturnType<FileFinder["waitForIndexReady"]>>>();
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            ready.resolve({ ok: true, value: true });
          }),
        );
        const waitForIndexReady = vi.fn<FileFinder["waitForIndexReady"]>(async () => ({
          ok: true,
          value: true,
        }));
        const scanFiles = vi.fn(() => ({ ok: true as const, value: undefined }));
        const finder = { destroy: vi.fn(), waitForIndexReady, scanFiles } as unknown as FileFinder;
        vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: finder });
        const index = yield* WorkspaceSearchIndex.make("/workspace/project", variant);
        waitForIndexReady.mockImplementationOnce(() => {
          started.resolve();
          return ready.promise;
        });

        const refreshing = yield* index.refresh().pipe(Effect.forkChild);
        yield* Effect.promise(() => started.promise);
        expect(scanFiles).toHaveBeenCalledTimes(1);
        expect(refreshing.pollUnsafe()).toBeUndefined();
        ready.resolve({ ok: true, value: true });
        yield* Fiber.join(refreshing);
      }),
    ),
);

it.effect("sets the pinned native cache and watcher controls without a grep file-size filter", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const grep = vi.fn((_query: string, _options?: GrepOptions) => ({
        ok: true as const,
        value: {
          items: [],
          totalMatched: 0,
          totalFilesSearched: 0,
          totalFiles: 0,
          filteredFileCount: 0,
          nextCursor: null,
        },
      }));
      const finder = {
        destroy: vi.fn(),
        waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: true })),
        grep,
      } as unknown as FileFinder;
      const create = vi
        .spyOn(FileFinder, "create")
        .mockReturnValueOnce({ ok: true, value: finder });
      const runtime = yield* WorkspaceSearchIndex.make("/synthetic", "content");
      yield* runtime.searchContents({
        query: "rare",
        limit: 3,
        caseSensitive: true,
        wholeWord: false,
        useRegex: false,
      });
      expect(create).toHaveBeenCalledWith(
        expect.objectContaining({
          disableWatch: false,
          disableMmapCache: true,
          disableContentIndexing: false,
          cacheBudgetMaxFiles: 1024,
          cacheBudgetMaxBytes: 32 * 1024 * 1024,
        }),
      );
      expect(create.mock.calls[0]?.[0]).not.toHaveProperty("cacheBudgetMaxFileSize");
      const options = grep.mock.calls[0]?.[1] as GrepOptions | undefined;
      expect(options).toMatchObject({ timeBudgetMs: 10, enforceTimeBudget: true });
      expect(options).not.toHaveProperty("maxFileSize");
    }),
  ),
);

it.effect("retains sorted directory ancestors and the 25000-entry listing boundary", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const mixedSearch = vi.fn(() => ({
        ok: true as const,
        value: {
          items: Array.from({ length: 25_002 }, (_, i) => ({
            type: "file" as const,
            item: fileItem(`src/nested/${String(i).padStart(5, "0")}.ts`),
          })),
          scores: [],
          totalMatched: 25_002,
          totalFiles: 25_002,
          totalDirs: 0,
        },
      }));
      const finder = {
        destroy: vi.fn(),
        waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: true })),
        mixedSearch,
      } as unknown as FileFinder;
      vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: finder });
      const runtime = yield* WorkspaceSearchIndex.make("/synthetic");
      const result = yield* runtime.list();
      expect(result.entries).toHaveLength(25_000);
      expect(result.truncated).toBe(true);
      expect(result.entries.slice(0, 2)).toEqual([
        { path: "src", kind: "directory" },
        { path: "src/nested", kind: "directory" },
      ]);
      expect(mixedSearch).toHaveBeenCalledWith("", { pageSize: 25_002, maxThreads: 1 });
    }),
  ),
);

it.effect("allows partial path readiness while retaining native scan/warmup observations", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const finder = {
        destroy: vi.fn(),
        waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: false })),
        isScanning: () => true,
        getScanProgress: () => ({
          ok: true as const,
          value: { isScanning: true, isWarmupComplete: false },
        }),
        mixedSearch: () => ({
          ok: true as const,
          value: { items: [], scores: [], totalMatched: 0, totalFiles: 0, totalDirs: 0 },
        }),
      } as unknown as FileFinder;
      vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: finder });
      const runtime = yield* WorkspaceSearchIndex.make("/synthetic", "paths");
      expect(yield* runtime.list()).toEqual({ entries: [], truncated: true });
      expect(runtime.status()).toEqual({ scanning: true, warmupComplete: false });
    }),
  ),
);

it.effect(
  "preserves actual pinned-native Unicode whole-word matches and refresh over synthetic files",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* Effect.promise(() => import("node:fs/promises"));
        const os = yield* Effect.promise(() => import("node:os"));
        const path = yield* Effect.promise(() => import("node:path"));
        const directory = yield* Effect.acquireRelease(
          Effect.promise(async () =>
            fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "jones-native-compute-"))),
          ),
          (directory) => Effect.promise(() => fs.rm(directory, { recursive: true, force: true })),
        );
        yield* Effect.promise(() =>
          fs.writeFile(
            path.join(directory, "unicode.txt"),
            "café needle needleSuffix\n前 😀needle.\n",
          ),
        );
        const runtime = yield* WorkspaceSearchIndex.make(directory, "content");
        const result = yield* runtime.searchContents({
          query: "needle",
          limit: 20,
          caseSensitive: true,
          wholeWord: true,
          useRegex: false,
        });
        expect(result.matches.map((m) => ({ line: m.lineNumber, ranges: m.matchRanges }))).toEqual([
          { line: 1, ranges: [{ start: 5, end: 11 }] },
          { line: 2, ranges: [{ start: 4, end: 10 }] },
        ]);
        yield* Effect.promise(() =>
          fs.writeFile(path.join(directory, "fresh.txt"), "rare-fresh-match\n"),
        );
        yield* runtime.refresh();
        const refreshed = yield* runtime.searchContents({
          query: "rare-fresh-match",
          limit: 2,
          caseSensitive: true,
          wholeWord: false,
          useRegex: false,
        });
        expect(refreshed.matches.map((m) => m.path)).toEqual(["fresh.txt"]);
        // The cache budget must not narrow the native default searchable file-size range.
        yield* Effect.promise(() =>
          fs.writeFile(
            path.join(directory, "large.txt"),
            "x".repeat(1024 * 1024 + 100) + "\nlarge-file-match\n",
          ),
        );
        yield* runtime.refresh();
        const large = yield* runtime.searchContents({
          query: "large-file-match",
          limit: 2,
          caseSensitive: true,
          wholeWord: false,
          useRegex: false,
        });
        expect(large.matches.map((m) => m.path)).toEqual(["large.txt"]);
      }),
    ),
);


it.effect("passes the supported qualification log option only when explicitly supplied", () =>
  Effect.scoped(Effect.gen(function* () {
    const finder = {
      destroy: vi.fn(),
      waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: true })),
    } as unknown as FileFinder;
    const create = vi.spyOn(FileFinder, "create").mockReturnValue({ ok: true, value: finder });
    yield* WorkspaceSearchIndex.make("/synthetic/ordinary");
    expect(create.mock.calls[0]?.[0]).not.toHaveProperty("logFilePath");
    yield* WorkspaceSearchIndex.make("/synthetic/blocked-workspace", "paths", {
      logFilePath: "/synthetic/barrier/native-block.log",
    });
    expect(create.mock.calls[1]?.[0]).toHaveProperty("logFilePath", "/synthetic/barrier/native-block.log");
  })),
);
