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
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import { vi } from "vite-plus/test";

import * as NodeFS from "node:fs";
import * as NativeRuntime from "./nativeSearchRuntime.ts";
import * as SearchGitProbe from "./SearchGitProbe.ts";
import { nativeSearchProbeTestLayer } from "../serverParallelism/nativeWorkspace.testkit.ts";
import type { Bootstrap } from "../serverCompute/protocol.ts";
import { WorkspaceSearchProbe } from "../serverCompute/searchProbeClient.ts";
import { probeFailure } from "../serverCompute/searchProbeProtocol.ts";
import { WorkspaceSearchIndexScanTimedOut } from "../serverCompute/workspaceErrors.ts";
const WorkspaceSearchIndex = {
  ...NativeRuntime,
  make: (...args: Parameters<typeof NativeRuntime.make>) =>
    NativeRuntime.make(...args).pipe(
      Effect.provide(
        NodeFS.existsSync(args[0])
          ? nativeSearchProbeTestLayer(args[0])
          : Layer.succeed(WorkspaceSearchProbe, {
              // Explicit nonexistent-path native-unit fixture. This is completed Git
              // non-repository evidence in the fake boundary, never a production fallback.
              run: () =>
                Effect.succeed({
                  exitCode: 128,
                  stdout: "",
                  stderr: "fatal: not a git repository",
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                }),
              beginScan: Effect.succeed({
                owner: { kind: "scan" as const, epoch: 1 },
                end: Effect.void,
              }),
            }),
      ),
    ),
};

const nativeServices = VcsProcess.layer.pipe(Layer.provideMerge(NodeServices.layer));
const withNativeServices = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect).pipe(Effect.provide(nativeServices));

// Receiving fixtures expose synchronous fake methods. Adapt their explicit results
// to the pinned upstream async API; production never uses this fixture adapter.
function asyncFinder(finder: FileFinder): FileFinder {
  const empty = {
    ok: true as const,
    value: { items: [], scores: [], totalMatched: 0, totalFiles: 0, totalDirs: 0 },
  };
  return Object.assign(finder, {
    fileSearchAsync: async (...args: Parameters<FileFinder["fileSearch"]>) =>
      finder.fileSearch(...args),
    mixedSearchAsync: async (...args: Parameters<FileFinder["mixedSearch"]>) =>
      finder.mixedSearch?.(...args) ?? empty,
    directorySearchAsync: async (...args: Parameters<FileFinder["directorySearch"]>) =>
      finder.directorySearch?.(...args) ?? empty,
    grepAsync: async (...args: Parameters<FileFinder["grep"]>) => finder.grep(...args),
    scanFilesAsync: async () => finder.scanFiles(),
    // Both refresh variants use the fixture's explicit native rescan result.
    reindexAsync: async () => finder.scanFiles(),
    getScanProgressAsync: async () =>
      finder.getScanProgress?.() ?? {
        ok: true as const,
        value: { isScanning: false, isWarmupComplete: true },
      },
    watchAsync: async () => ({ ok: true as const, value: () => {} }),
  });
}

afterEach(() => {
  vi.restoreAllMocks();
});

it.each([
  { timeout: 40, readyAt: 15, checks: [0, 5, 15], ready: true },
  { timeout: 132, readyAt: Infinity, checks: [0, 5, 15, 35, 75, 125, 132], ready: false },
])("polls scan readiness promptly and respects a $timeout ms deadline", async (scenario) => {
  vi.useFakeTimers({ now: 0 });
  try {
    const checks: number[] = [];
    const finder = {
      ensureAlive: () => ({ ok: true }),
      getScanProgressAsync: async () => {
        const at = vi.getMockedSystemTime()!.getTime();
        checks.push(at);
        return {
          ok: true,
          value: { isScanning: at < 5, isWarmupComplete: at >= scenario.readyAt },
        };
      },
    } as unknown as FileFinder;
    const complete = vi.fn();
    void FileFinder.prototype.waitForIndexReady.call(finder, scenario.timeout).then(complete);
    await vi.advanceTimersByTimeAsync(scenario.timeout);
    expect(complete).toHaveBeenCalledExactlyOnceWith({ ok: true, value: scenario.ready });
    expect(checks).toEqual(scenario.checks);
  } finally {
    vi.useRealTimers();
  }
});

it.effect.each(["list", "names", "contents"] as const)(
  "retains the triggering %s query/page evidence and immediate probe failure",
  (operation) =>
    withNativeServices(
      Effect.gen(function* () {
        // Only the native external dependency is fake; the runtime's error mapping is real.
        const finder = {
          destroy: vi.fn(),
          waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: true })),
          getScanProgress: () => ({
            ok: true as const,
            value: { isScanning: true, isWarmupComplete: false },
          }),
        } as unknown as FileFinder;
        vi.spyOn(FileFinder, "create").mockReturnValueOnce({
          ok: true,
          value: asyncFinder(finder),
        });
        const cause = probeFailure("queue-timeout");
        const probes = WorkspaceSearchProbe.of({
          run: (input) =>
            input.kind === "ignoredPaths"
              ? Effect.succeed({
                  exitCode: 0,
                  stdout: "",
                  stderr: "",
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                })
              : Effect.fail(cause),
          beginScan: Effect.fail(cause),
        });
        const runtime = yield* NativeRuntime.make(
          "/synthetic/evidence",
          operation === "contents" ? "content" : "paths",
        ).pipe(Effect.provideService(WorkspaceSearchProbe, probes));
        const query = "[λ-needle";
        const error = yield* operation === "list"
          ? runtime.list().pipe(Effect.flip)
          : operation === "names"
            ? runtime.search(query, 4, "file").pipe(Effect.flip)
            : runtime
                .searchContents({
                  query,
                  limit: 4,
                  caseSensitive: true,
                  wholeWord: false,
                  useRegex: false,
                })
                .pipe(Effect.flip);
        expect(error).toMatchObject({
          _tag: "WorkspaceSearchIndexSearchFailed",
          cwd: "/synthetic/evidence",
          queryLength: operation === "list" ? 0 : query.length,
          pageSize: operation === "list" ? 25_002 : operation === "names" ? 5 : 4,
          reason: cause.message,
        });
        if (error._tag === "WorkspaceSearchIndexSearchFailed") expect(error.cause).toBe(cause);
      }),
    ),
);

it.effect(
  "propagates the real content fallback scan timeout without changing its 15-second boundary",
  () =>
    withNativeServices(
      Effect.gen(function* () {
        const waitForIndexReady = vi.fn<FileFinder["waitForIndexReady"]>(async () => ({
          ok: true,
          value: false,
        }));
        const finder = {
          destroy: vi.fn(),
          waitForIndexReady,
          getScanProgress: () => ({
            ok: true as const,
            value: { isScanning: true, isWarmupComplete: false },
          }),
        } as unknown as FileFinder;
        vi.spyOn(FileFinder, "create").mockReturnValueOnce({
          ok: true,
          value: asyncFinder(finder),
        });
        const runtime = yield* NativeRuntime.make("/synthetic/timeout", "content").pipe(
          Effect.provideService(
            WorkspaceSearchProbe,
            WorkspaceSearchProbe.of({
              run: () =>
                Effect.succeed({
                  exitCode: 128,
                  stdout: "",
                  stderr: "fatal: not a git repository",
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                }),
              beginScan: Effect.die("Content fallback must not start a path scan"),
            }),
          ),
        );
        const error = yield* runtime
          .searchContents({
            query: "needle",
            limit: 4,
            caseSensitive: true,
            wholeWord: false,
            useRegex: false,
          })
          .pipe(Effect.flip);
        expect(error).toBeInstanceOf(WorkspaceSearchIndexScanTimedOut);
        expect(error).toMatchObject({ cwd: "/synthetic/timeout", timeout: "15 seconds" });
        expect(waitForIndexReady.mock.calls).toEqual([[15_000]]);
      }),
    ),
);

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
  withNativeServices(
    Effect.gen(function* () {
      const items = [
        ...Array.from({ length: 200 }, (_, index) => fileItem(`src/file-${index}.ts`)),
        fileItem("public/icon.svg"),
      ];
      const fileSearch = vi.fn(
        (_query: string, _options?: { pageSize?: number; maxThreads?: number }) => ({
          ok: true as const,
          value: {
            items,
            scores: [],
            totalMatched: items.length,
            totalFiles: items.length,
          },
        }),
      );
      const finder = {
        destroy: vi.fn(),
        waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: true })),
        fileSearch,
      } as unknown as FileFinder;
      vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: asyncFinder(finder) });

      const searchIndex = yield* WorkspaceSearchIndex.make("/workspace/project");
      const resultWithoutKind = yield* searchIndex.search("", 200, undefined, true);
      const resultWithDirectoryKind = yield* searchIndex.search("", 200, "directory", true);

      expect(resultWithoutKind.entries).toEqual([{ kind: "file", path: "public/icon.svg" }]);
      expect(resultWithDirectoryKind.entries).toEqual([{ kind: "file", path: "public/icon.svg" }]);
      expect(fileSearch).toHaveBeenCalledTimes(2);
      expect(fileSearch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ maxThreads: 1 }),
      );
      expect(
        fileSearch.mock.calls.every((call) => (call[1] as { pageSize: number }).pageSize <= 25_002),
      ).toBe(true);
    }),
  ),
);

it.effect("continues whole-word searches after a filtered grep page", () =>
  withNativeServices(
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
      vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: asyncFinder(finder) });

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

it.live(
  "selects whole-word Git candidates from the trusted search request before the file cap",
  () =>
    withNativeServices(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "jones-git-whole-word-" }),
        );
        const git = yield* VcsProcess.VcsProcess;
        yield* git.run({ operation: "test.gitInit", command: "git", args: ["init"], cwd });
        yield* fs.writeFileString(
          path.join(cwd, "words.txt"),
          `${"needleSuffix\n".repeat(100)}Needle!\n`,
        );

        const bootstrap: Bootstrap = {
          version: 1,
          build: "a".repeat(64),
          authorityId: "authority",
          slotId: 0,
          slotGeneration: 1,
          nonce: "nonce",
        };
        const token = {
          authorityId: bootstrap.authorityId,
          slotId: bootstrap.slotId,
          slotGeneration: bootstrap.slotGeneration,
          keyId: "key",
          incarnation: 1,
        };
        const probe = yield* SearchGitProbe.make;
        yield* probe.bind({ bootstrap, token, key: { cwd, variant: "content" } });
        yield* probe.opened(token);
        const clock = yield* SearchGitProbe.SearchProbeClock;

        const grep = Effect.fn(function* (requestId: number, wholeWord: boolean) {
          const owner = { kind: "foreground" as const, requestId };
          yield* probe.foregroundBegin({
            token,
            requestId,
            operation: "searchContents",
            input: { query: "needle", caseSensitive: false, useRegex: false, wholeWord, limit: 10 },
            deadlineMonotonicMs: clock.now() + 30_000,
          });
          const result = yield* probe.run(token, owner, requestId, {
            kind: "grep",
            pattern: "needle",
            ignoreCase: true,
            onlyMatching: false,
            regexMode: false,
          });
          yield* probe.acknowledge(token, owner, requestId);
          yield* probe.foregroundEnd(token, requestId);
          expect(result.exitCode).toBe(0);
          expect(result.stdoutTruncated).toBe(false);
          return result.stdout;
        });

        const substrings = yield* grep(1, false);
        expect(substrings.trimEnd().split("\n")).toHaveLength(100);
        expect(substrings).not.toContain("Needle!");
        expect(yield* grep(2, true)).toBe("words.txt\0" + "101\0Needle!\n");
      }),
    ),
);

it.effect.each(["paths", "content"] as const)(
  "waits for %s index readiness after refresh scans",
  (variant) =>
    withNativeServices(
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
        vi.spyOn(FileFinder, "create").mockReturnValueOnce({
          ok: true,
          value: asyncFinder(finder),
        });
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
  withNativeServices(
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
        .mockReturnValueOnce({ ok: true, value: asyncFinder(finder) });
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
  withNativeServices(
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
      vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: asyncFinder(finder) });
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
  withNativeServices(
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
      vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: asyncFinder(finder) });
      const runtime = yield* WorkspaceSearchIndex.make("/synthetic", "paths");
      expect(yield* runtime.list()).toEqual({ entries: [], truncated: true });
      expect(yield* Effect.promise(() => runtime.status())).toEqual({
        scanning: true,
        warmupComplete: false,
      });
    }),
  ),
);

it.effect(
  "preserves actual pinned-native Unicode whole-word matches and refresh over synthetic files",
  () =>
    withNativeServices(
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
  withNativeServices(
    Effect.gen(function* () {
      const finder = {
        destroy: vi.fn(),
        waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: true })),
      } as unknown as FileFinder;
      const create = vi
        .spyOn(FileFinder, "create")
        .mockReturnValue({ ok: true, value: asyncFinder(finder) });
      yield* WorkspaceSearchIndex.make("/synthetic/ordinary");
      expect(create.mock.calls[0]?.[0]).not.toHaveProperty("logFilePath");
      yield* WorkspaceSearchIndex.make("/synthetic/blocked-workspace", "paths", {
        logFilePath: "/synthetic/barrier/native-block.log",
      });
      expect(create.mock.calls[1]?.[0]).toHaveProperty(
        "logFilePath",
        "/synthetic/barrier/native-block.log",
      );
    }),
  ),
);

it.effect.each(["scan-failure", "false-readiness"] as const)(
  "strict content refresh fails honestly on %s",
  (mode) =>
    withNativeServices(
      Effect.gen(function* () {
        const scanFiles = vi.fn(() =>
          mode === "scan-failure"
            ? { ok: false as const, error: "scan refused" }
            : { ok: true as const, value: undefined },
        );
        const waitForIndexReady = vi.fn<FileFinder["waitForIndexReady"]>(async () => ({
          ok: true,
          value: true,
        }));
        const finder = { destroy: vi.fn(), waitForIndexReady, scanFiles } as unknown as FileFinder;
        vi.spyOn(FileFinder, "create").mockReturnValueOnce({
          ok: true,
          value: asyncFinder(finder),
        });
        const runtime = yield* WorkspaceSearchIndex.make("/workspace/project", "content");
        waitForIndexReady.mockResolvedValueOnce({ ok: true, value: false });
        const result = yield* runtime.refreshContent().pipe(Effect.result);
        expect(result).toMatchObject({
          _tag: "Failure",
          failure: {
            _tag:
              mode === "scan-failure"
                ? "WorkspaceSearchIndexRefreshFailed"
                : "WorkspaceSearchIndexScanTimedOut",
          },
        });
        expect(scanFiles).toHaveBeenCalledTimes(1);
        expect(waitForIndexReady.mock.calls).toEqual(mode === "scan-failure" ? [] : [[15000]]);
      }),
    ),
);

it.effect("strict content cancellation retains the finder until native readiness settles", () =>
  withNativeServices(
    Effect.gen(function* () {
      const entered = Promise.withResolvers<void>();
      const ready = Promise.withResolvers<Awaited<ReturnType<FileFinder["waitForIndexReady"]>>>();
      const destroy = vi.fn();
      const waitForIndexReady = vi.fn<FileFinder["waitForIndexReady"]>(async () => ({
        ok: true,
        value: true,
      }));
      const finder = {
        destroy,
        waitForIndexReady,
        scanFiles: vi.fn(() => ({ ok: true, value: undefined })),
      } as unknown as FileFinder;
      vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: asyncFinder(finder) });
      yield* Effect.gen(function* () {
        const runtime = yield* WorkspaceSearchIndex.make("/workspace/project", "content");
        waitForIndexReady.mockImplementationOnce(() => {
          entered.resolve();
          return ready.promise;
        });
        const work = yield* runtime.refreshContent().pipe(Effect.forkChild);
        yield* Effect.promise(() => entered.promise);
        const cancellation = yield* Fiber.interrupt(work).pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        expect(cancellation.pollUnsafe()).toBeUndefined();
        expect(destroy).not.toHaveBeenCalled();
        ready.resolve({ ok: true, value: true });
        yield* Fiber.join(cancellation);
        expect(work.pollUnsafe()).toMatchObject({ _tag: "Failure" });
      }).pipe(Effect.ensuring(Effect.sync(() => ready.resolve({ ok: true, value: true }))));
    }),
  ),
);

it.effect("strict real native scans expose successive versions of the same physical file", () =>
  withNativeServices(
    Effect.gen(function* () {
      const fs = yield* Effect.promise(() => import("node:fs/promises"));
      const os = yield* Effect.promise(() => import("node:os"));
      const path = yield* Effect.promise(() => import("node:path"));
      const directory = yield* Effect.acquireRelease(
        Effect.promise(async () =>
          fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "jones-native-versions-"))),
        ),
        (root) => Effect.promise(() => fs.rm(root, { recursive: true, force: true })),
      );
      const file = path.join(directory, "same.md");
      yield* Effect.promise(() => fs.writeFile(file, "Plan\n"));
      const runtime = yield* WorkspaceSearchIndex.make(directory, "content");
      const query = (text: string) =>
        runtime.searchContents({
          query: text,
          limit: 10,
          caseSensitive: true,
          wholeWord: true,
          useRegex: false,
        });
      expect((yield* query("Plan")).matches.map((match) => match.lineContent)).toEqual(["Plan"]);
      let previous = "Plan";
      for (const version of ["Replacement", "Successive", "Final"]) {
        yield* Effect.promise(() => fs.writeFile(file, version + "\n"));
        yield* runtime.refreshContent();
        expect((yield* query(previous)).matches).toEqual([]);
        expect((yield* query(version)).matches).toEqual([
          {
            path: "same.md",
            lineNumber: 1,
            lineContent: version,
            matchRanges: [{ start: 0, end: version.length }],
          },
        ]);
        expect(yield* Effect.promise(() => fs.readFile(file, "utf8"))).toBe(version + "\n");
        previous = version;
      }
    }),
  ),
);

it.effect(
  "strict refresh cancellation retains its finder while the native restart is pending",
  () =>
    withNativeServices(
      Effect.gen(function* () {
        const entered = Promise.withResolvers<void>();
        const restarted = Promise.withResolvers<Awaited<ReturnType<FileFinder["reindexAsync"]>>>();
        const destroy = vi.fn();
        const finder = asyncFinder({
          destroy,
          waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: true })),
        } as unknown as FileFinder);
        const reindex = vi.fn<FileFinder["reindexAsync"]>((newPath) => {
          expect(newPath).toBe("/workspace/project");
          entered.resolve();
          return restarted.promise;
        });
        finder.reindexAsync = reindex;
        vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: finder });
        yield* Effect.gen(function* () {
          const runtime = yield* WorkspaceSearchIndex.make("/workspace/project", "content");
          const work = yield* runtime.refreshContent().pipe(Effect.forkChild);
          yield* Effect.promise(() => entered.promise);
          const cancellation = yield* Fiber.interrupt(work).pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          expect(cancellation.pollUnsafe()).toBeUndefined();
          expect(destroy).not.toHaveBeenCalled();
          restarted.resolve({ ok: true, value: undefined });
          yield* Fiber.join(cancellation);
          expect(work.pollUnsafe()).toMatchObject({ _tag: "Failure" });
          expect(reindex).toHaveBeenCalledTimes(1);
        }).pipe(
          Effect.ensuring(Effect.sync(() => restarted.resolve({ ok: true, value: undefined }))),
        );
      }),
    ),
);
