import type { GrepCursor, GrepMatch, GrepResult } from "@ff-labs/fff-node";
import type { ProjectSearchContentsResult } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

const CONTENT_SEARCH_TIME_BUDGET_MS = 250;
const CONTENT_SEARCH_SLICE_MS = 10;

const nextNodeTurn = Effect.callback<void>((resume) => {
  const immediate = setImmediate(() => resume(Effect.void));
  return Effect.sync(() => clearImmediate(immediate));
});

// The caller's WorkspaceSearchIndex.searchContents span covers these per-page steps.
export const collectContentSearchPages = Effect.fnUntraced(function* <E>(options: {
  readonly limit: number;
  readonly searchPage: (
    cursor: GrepCursor | null,
    budget: { readonly timeBudgetMs: number; readonly enforceTimeBudget: true },
  ) => Effect.Effect<GrepResult, E>;
  readonly mapMatch: (
    match: GrepMatch,
  ) => ProjectSearchContentsResult["matches"][number] | undefined;
}): Effect.fn.Return<ProjectSearchContentsResult, E> {
  const deadline = performance.now() + CONTENT_SEARCH_TIME_BUDGET_MS;
  const matches: Array<ProjectSearchContentsResult["matches"][number]> = [];
  let nextCursor: GrepCursor | null = null;
  let hasNativeMatch = false;
  let regexFallbackError: string | undefined;

  do {
    const result: GrepResult = yield* options.searchPage(nextCursor, {
      timeBudgetMs: hasNativeMatch
        ? Math.min(CONTENT_SEARCH_SLICE_MS, Math.max(1, Math.ceil(deadline - performance.now())))
        : CONTENT_SEARCH_SLICE_MS,
      enforceTimeBudget: true,
    });
    hasNativeMatch ||= result.items.length > 0;
    for (const match of result.items) {
      const mapped = options.mapMatch(match);
      if (mapped !== undefined) matches.push(mapped);
    }
    nextCursor = result.nextCursor;
    regexFallbackError ??= result.regexFallbackError;

    if (matches.length >= options.limit || nextCursor === null) break;
    // Native grep normally ignores its deadline until a hit. Preserve rare-match
    // discovery while bounding each synchronous call, including zero-hit pages.
    if (hasNativeMatch && performance.now() >= deadline) break;
    // Effect.yieldNow alone need not let Node process I/O or cancellation.
    yield* nextNodeTurn;
    if (hasNativeMatch && performance.now() >= deadline) break;
  } while (true);

  return {
    matches: matches.slice(0, options.limit),
    truncated: matches.length > options.limit || nextCursor !== null,
    ...(regexFallbackError !== undefined ? { regexFallbackError } : {}),
  };
});
