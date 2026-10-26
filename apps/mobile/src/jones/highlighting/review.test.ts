import { describe, expect, it, vi } from "vite-plus/test";
import { highlightCode } from "../../lib/highlights";
import { resolveHighlightLanguage } from "../../lib/highlightLanguage";
import { highlightReviewSelectedLines } from "../../features/review/highlightReviewSelectedLines";
import type { ReviewRenderableLineRow } from "../../features/review/reviewModel";

vi.mock("expo", async () => {
  const { createWasmHighlightsModule } = await import("../../lib/highlights.testing");
  return { requireOptionalNativeModule: () => createWasmHighlightsModule() };
});

describe("Jones review highlighting on the native tokenizer", () => {
  it.each(["light", "dark"] as const)(
    "preserves cold source and diff-prefixed TSX snippets in %s",
    (scheme) => {
      const source = "const answer: number = 42;";
      const lines = [
        "- onClick={() => submitOrder(cart)}",
        "+ onClick={handleSubmit}",
        "+ disabled={isSubmitting}",
      ];
      const highlighted = highlightCode(source, "ts", scheme);
      expect(
        highlighted
          .flat()
          .map((token) => token.content)
          .join(""),
      ).toBe(source);
      expect(highlighted.flat().some((token) => token.color !== null)).toBe(true);
      const snippets = highlightCode(lines.join("\n"), "tsx", scheme);
      expect(snippets.map((line) => line.map((token) => token.content).join(""))).toEqual(lines);
      expect(
        new Set(
          snippets
            .flat()
            .map((token) => token.color)
            .filter(Boolean),
        ).size,
      ).toBeGreaterThan(1);
      const longLine = `const value = "${"a".repeat(1_100)}";`;
      expect(highlightCode(longLine, "ts", scheme)).toEqual([
        [{ content: longLine, color: null, fontStyle: null }],
      ]);
      expect(resolveHighlightLanguage({ path: "example.ts" })).toBe("typescript");
    },
  );

  it("retains word diff emphasis for paired deletion and addition lines", () => {
    const lines: ReviewRenderableLineRow[] = [
      {
        kind: "line",
        id: "delete",
        change: "delete",
        oldLineNumber: 1,
        newLineNumber: null,
        content: "const before = 1;",
        additionTokenIndex: null,
        deletionTokenIndex: 0,
        comparison: { change: "add", tokenIndex: 0 },
      },
      {
        kind: "line",
        id: "add",
        change: "add",
        oldLineNumber: null,
        newLineNumber: 1,
        content: "const after = 2;",
        additionTokenIndex: 0,
        deletionTokenIndex: null,
        comparison: { change: "delete", tokenIndex: 0 },
      },
    ];
    const tokens = highlightReviewSelectedLines({ filePath: "example.txt", lines, theme: "light" });
    expect(tokens.delete?.some((token) => token.diffHighlight === true)).toBe(true);
    expect(tokens.add?.some((token) => token.diffHighlight === true)).toBe(true);
    expect(tokens.delete?.map((token) => token.content).join("")).toBe(lines[0]!.content);
    expect(tokens.add?.map((token) => token.content).join("")).toBe(lines[1]!.content);
  });
});
