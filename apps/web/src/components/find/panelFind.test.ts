// @vitest-environment jsdom
import { describe, expect, it } from "vite-plus/test";
import { collectFindRanges, findTextMatches } from "./panelFind";

describe("panel find text", () => {
  it("finds literal case-insensitive occurrences and keeps Unicode offsets", () => {
    expect(findTextMatches("A [x] and [X]", "[x]")).toEqual([
      { start: 2, end: 5 },
      { start: 10, end: 13 },
    ]);
    expect(findTextMatches("İ Next next", "next")).toEqual([
      { start: 2, end: 6 },
      { start: 7, end: 11 },
    ]);
    expect(findTextMatches("test", "")).toEqual([]);
  });
  it("matches across inline formatting but excludes other panels and hidden controls", () => {
    document.body.innerHTML = `<div data-find-scope="page"><p>A <strong>work</strong>tree setting</p><div hidden>worktree</div><div style="display:none">worktree</div><div data-panel-find-bar>worktree</div><div contenteditable="true">worktree</div><div data-find-scope="side"><p>worktree</p></div></div>`;
    const root = document.body.firstElementChild as HTMLElement;
    const ranges = collectFindRanges(root, "worktree");
    expect(ranges.map((range) => range.toString())).toEqual(["worktree"]);
    expect(ranges[0]?.startContainer.parentElement?.tagName).toBe("STRONG");
  });
  it("does not match phrases spanning separate paragraphs", () => {
    document.body.innerHTML = `<div data-find-scope="page"><p>work</p><p>tree</p></div>`;
    expect(collectFindRanges(document.body.firstElementChild as HTMLElement, "worktree")).toEqual(
      [],
    );
  });
});
