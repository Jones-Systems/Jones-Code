import { FileFinder, type GrepCursor, type GrepMatch, type GrepResult } from "@ff-labs/fff-node";
import { afterEach, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import { vi } from "vite-plus/test";

import * as WorkspaceSearchIndex from "../../workspace/WorkspaceSearchIndex.ts";

const pendingImmediates = new Set<NodeJS.Immediate>();

afterEach(() => {
  for (const immediate of pendingImmediates) clearImmediate(immediate);
  pendingImmediates.clear();
  vi.restoreAllMocks();
});

const input = {
  query: "needle",
  limit: 2,
  caseSensitive: true,
  wholeWord: false,
  useRegex: false,
};

function cursor(offset: number): GrepCursor {
  return { __brand: "GrepCursor", _offset: offset } as GrepCursor;
}

function match(lineContent = "needle", matchRanges: Array<[number, number]> = [[0, 6]]): GrepMatch {
  return {
    relativePath: "src/words.ts",
    fileName: "words.ts",
    gitStatus: "unmodified",
    size: Buffer.byteLength(lineContent),
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
  };
}

function page(items: GrepMatch[], nextCursor: GrepCursor | null): GrepResult {
  return {
    items,
    totalMatched: items.length,
    totalFilesSearched: 1,
    totalFiles: 10,
    filteredFileCount: 10,
    nextCursor,
  };
}

function makeIndex(grep: FileFinder["grep"]) {
  const destroy = vi.fn();
  const finder = {
    destroy,
    waitForIndexReady: vi.fn(async () => ({ ok: true as const, value: true })),
    grep,
  } as unknown as FileFinder;
  vi.spyOn(FileFinder, "create").mockReturnValueOnce({ ok: true, value: finder });
  return { index: WorkspaceSearchIndex.make("/workspace/project", "content"), destroy };
}

it.effect(
  "enforces small native budgets and lets a Node immediate run between continued pages",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const next = cursor(1);
        let probeRan = false;
        const grep = vi.fn<FileFinder["grep"]>((_query, options) => {
          expect(options?.enforceTimeBudget).toBe(true);
          expect(options?.timeBudgetMs).toBeGreaterThan(0);
          expect(options?.timeBudgetMs).toBeLessThanOrEqual(10);
          if (options?.cursor === null) {
            pendingImmediates.add(
              setImmediate(() => {
                probeRan = true;
              }),
            );
            return { ok: true, value: page([], next) };
          }
          expect(probeRan).toBe(true);
          expect(options?.cursor).toBe(next);
          return {
            ok: true,
            value: page([{ ...match("😀 café", [[5, 10]]), relativePath: "src\\words.ts" }], null),
          };
        });
        const { index } = makeIndex(grep);
        const searchIndex = yield* index;
        const result = yield* searchIndex.searchContents({
          ...input,
          query: "CAFÉ",
          caseSensitive: false,
        });

        expect(result).toEqual({
          matches: [
            {
              path: "src/words.ts",
              lineNumber: 1,
              lineContent: "😀 café",
              matchRanges: [{ start: 3, end: 7 }],
            },
          ],
          truncated: false,
        });
        expect(grep).toHaveBeenCalledTimes(2);
        expect(grep).toHaveBeenCalledWith(
          "café",
          expect.objectContaining({
            mode: "plain",
            smartCase: true,
            pageSize: 2,
            maxMatchesPerFile: 2,
          }),
        );
      }),
    ),
);

it.effect.each([false, true])(
  "continues zero-hit pages beyond the total deadline until completion or a rare match (%s)",
  (rareMatch) =>
    Effect.scoped(
      Effect.gen(function* () {
        let now = 0;
        vi.spyOn(performance, "now").mockImplementation(() => now);
        const cursors = [cursor(1), cursor(2), cursor(3)];
        let calls = 0;
        const grep = vi.fn<FileFinder["grep"]>((_query, options) => {
          const call = ++calls;
          expect(options?.cursor).toBe(call === 1 ? null : cursors[call - 2]);
          expect(options?.timeBudgetMs).toBe(10);
          now += 300;
          return {
            ok: true,
            value:
              call < 3
                ? page([], cursors[call - 1]!)
                : page(rareMatch ? [match()] : [], rareMatch ? cursors[2]! : null),
          };
        });
        const { index } = makeIndex(grep);
        const searchIndex = yield* index;
        const result = yield* searchIndex.searchContents(input);

        expect(grep).toHaveBeenCalledTimes(3);
        expect(result.matches).toHaveLength(rareMatch ? 1 : 0);
        expect(result.truncated).toBe(rareMatch);
      }),
    ),
);

it.effect(
  "keeps the original total deadline after a hit and caps a final native slice to the remainder",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let now = 0;
        vi.spyOn(performance, "now").mockImplementation(() => now);
        const next = cursor(1);
        const grep = vi.fn<FileFinder["grep"]>((_query, options) => {
          if (options?.cursor === null) {
            now = 244;
            return { ok: true, value: page([match()], next) };
          }
          expect(options?.timeBudgetMs).toBe(6);
          now = 250;
          return { ok: true, value: page([], cursor(2)) };
        });
        const { index } = makeIndex(grep);
        const searchIndex = yield* index;
        const result = yield* searchIndex.searchContents(input);

        expect(grep).toHaveBeenCalledTimes(2);
        expect(result.matches).toHaveLength(1);
        expect(result.truncated).toBe(true);
      }),
    ),
);

it.effect("does not start another native call when the deadline expires during the Node turn", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let now = 0;
      vi.spyOn(performance, "now").mockImplementation(() => now);
      const grep = vi.fn<FileFinder["grep"]>(() => {
        now = 249;
        pendingImmediates.add(
          setImmediate(() => {
            now = 250;
          }),
        );
        return { ok: true, value: page([match()], cursor(1)) };
      });
      const { index } = makeIndex(grep);
      const searchIndex = yield* index;
      const result = yield* searchIndex.searchContents(input);

      expect(grep).toHaveBeenCalledTimes(1);
      expect(result.matches).toHaveLength(1);
      expect(result.truncated).toBe(true);
    }),
  ),
);

it.effect("preserves the result limit and first regex fallback across empty pages", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const next = cursor(1);
      const grep = vi.fn<FileFinder["grep"]>((_query, options) => ({
        ok: true,
        value:
          options?.cursor === null
            ? { ...page([], next), regexFallbackError: "first compile failure" }
            : {
                ...page([match(), match(), match()], null),
                regexFallbackError: "later compile failure",
              },
      }));
      const { index } = makeIndex(grep);
      const searchIndex = yield* index;
      const result = yield* searchIndex.searchContents({
        ...input,
        query: "[",
        useRegex: true,
        caseSensitive: false,
        wholeWord: true,
      });

      expect(result.matches).toHaveLength(2);
      expect(result.truncated).toBe(true);
      expect(result.regexFallbackError).toBe("first compile failure");
      expect(grep).toHaveBeenCalledTimes(2);
      expect(grep).toHaveBeenCalledWith(
        "(?i)[",
        expect.objectContaining({ mode: "regex", smartCase: false }),
      );
    }),
  ),
);

it.effect.each([null, cursor(1)])(
  "stops at the result limit and reports the native continuation (%s)",
  (next) =>
    Effect.scoped(
      Effect.gen(function* () {
        const grep = vi.fn<FileFinder["grep"]>(() => ({
          ok: true,
          value: page([match(), match()], next),
        }));
        const { index } = makeIndex(grep);
        const searchIndex = yield* index;
        const result = yield* searchIndex.searchContents(input);

        expect(grep).toHaveBeenCalledTimes(1);
        expect(result.matches).toHaveLength(2);
        expect(result.truncated).toBe(next !== null);
      }),
    ),
);

it.effect(
  "resumes a whole-word filtered page at its native cursor without changing raw page capacity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const next = cursor(1);
        const grep = vi.fn<FileFinder["grep"]>((_query, options) => {
          expect(options?.pageSize).toBe(100);
          expect(options?.maxMatchesPerFile).toBe(100);
          return {
            ok: true,
            value:
              options?.cursor === null
                ? page([match("needleSuffix")], next)
                : page([match()], null),
          };
        });
        const { index } = makeIndex(grep);
        const searchIndex = yield* index;
        const result = yield* searchIndex.searchContents({ ...input, wholeWord: true, limit: 1 });

        expect(grep).toHaveBeenCalledTimes(2);
        expect(grep.mock.calls[1]?.[1]?.cursor).toBe(next);
        expect(result.matches.map((item) => item.lineContent)).toEqual(["needle"]);
        expect(result.truncated).toBe(false);
      }),
    ),
);

it.effect("applies the deadline after a raw match even if whole-word filtering removes it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let now = 0;
      vi.spyOn(performance, "now").mockImplementation(() => now);
      const grep = vi.fn<FileFinder["grep"]>(() => {
        now = 251;
        return { ok: true, value: page([match("needleSuffix")], cursor(1)) };
      });
      const { index } = makeIndex(grep);
      const searchIndex = yield* index;
      expect(yield* searchIndex.searchContents({ ...input, wholeWord: true })).toEqual({
        matches: [],
        truncated: true,
      });
      expect(grep).toHaveBeenCalledTimes(1);
    }),
  ),
);

it.effect(
  "interrupts a pending page wait, clears its immediate, and leaves the finder in its owning scope",
  () =>
    Effect.gen(function* () {
      const scheduled = Promise.withResolvers<NodeJS.Immediate>();
      const originalSetImmediate = globalThis.setImmediate;
      const clear = vi.spyOn(globalThis, "clearImmediate");
      const grep = vi.fn<FileFinder["grep"]>(() => {
        // Hold exactly the next page wait so cancellation cannot race its callback.
        vi.spyOn(globalThis, "setImmediate").mockImplementationOnce(() => {
          const handle = originalSetImmediate(() => {});
          pendingImmediates.add(handle);
          scheduled.resolve(handle);
          return handle;
        });
        return { ok: true, value: page([], cursor(1)) };
      });
      const { index, destroy } = makeIndex(grep);
      yield* Effect.scoped(
        Effect.gen(function* () {
          const searchIndex = yield* index;
          const searching = yield* searchIndex.searchContents(input).pipe(Effect.forkChild);
          const handle = yield* Effect.promise(() => scheduled.promise);
          yield* Fiber.interrupt(searching);

          expect(Exit.isFailure(yield* Fiber.await(searching))).toBe(true);
          expect(clear).toHaveBeenCalledWith(handle);
          expect(grep).toHaveBeenCalledTimes(1);
          expect(destroy).not.toHaveBeenCalled();
        }),
      );
      expect(destroy).toHaveBeenCalledTimes(1);
    }),
);

it.effect.each([false, true])(
  "keeps native grep failures in the existing typed error channel (throws: %s)",
  (throws) =>
    Effect.scoped(
      Effect.gen(function* () {
        const cause = new Error("native failure");
        const grep = vi.fn<FileFinder["grep"]>((_query, options) => {
          if (options?.cursor === null) return { ok: true, value: page([], cursor(1)) };
          if (throws) throw cause;
          return { ok: false, error: "native failure" };
        });
        const { index } = makeIndex(grep);
        const searchIndex = yield* index;
        const result = yield* searchIndex.searchContents(input).pipe(Effect.flip);

        expect(result).toBeInstanceOf(WorkspaceSearchIndex.WorkspaceSearchIndexSearchFailed);
        expect(result).toMatchObject({
          cwd: "/workspace/project",
          queryLength: 6,
          pageSize: 2,
          reason: throws ? "FileFinder.grep threw unexpectedly." : "native failure",
          ...(throws ? { cause } : {}),
        });
        expect(grep).toHaveBeenCalledTimes(2);
      }),
    ),
);
