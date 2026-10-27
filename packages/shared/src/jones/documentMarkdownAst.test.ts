import { describe, expect, it } from "vite-plus/test";
import { markdownHeadingPositions } from "./documentMarkdownAst.ts";

describe("parse-only document heading positions", () => {
  it.each([
    { name: "list-contained fence", source: "- ```md\n  ## [example] Not a section\n  ```\n## [real] Real section\nbody",
      headings: ["## [real] Real section"] },
    { name: "empty list fence", source: "-\n  ```md\n  ## [example] Decoy\n## [real] Real section\nbody",
      headings: ["## [real] Real section"] },
    { name: "Setext precedence", source: "Title\n-\n  ```md\n  ## [example] Decoy\n## [code] Still code\n```\n## [real] Real section\nbody",
      headings: ["Title\n-", "## [real] Real section"] },
  ])("uses real block parsing for $name", ({ source, headings }) => {
    expect(markdownHeadingPositions(source).map(({ start, end }) => source.slice(start.offset, end.offset)))
      .toEqual(headings);
  });
  it("retains UTF-16 offsets, physical lines, BOM, mixed endings and a missing final newline", () => {
    const source = "\ufeff😀\r\n  ## [one] Raw &amp; \\*title\\*\r### [two] Child\nTitle\n-";
    const one = "## [one] Raw &amp; \\*title\\*";
    const two = "### [two] Child";
    expect(markdownHeadingPositions(source)).toEqual([
      { depth: 2, start: { line: 2, offset: 7 }, end: { line: 2, offset: 7 + one.length } },
      { depth: 3, start: { line: 3, offset: source.indexOf(two) },
        end: { line: 3, offset: source.indexOf(two) + two.length } },
      { depth: 2, start: { line: 4, offset: source.indexOf("Title") },
        end: { line: 5, offset: source.length } },
    ]);
    expect(Buffer.byteLength(source.slice(0, 7))).not.toBe(7);
  });
  it("traverses heading containers in source order without changing their physical positions", () => {
    const source = "- ## Marker\n  ### Continued\n\n> ## Quoted\n\n  # Root";
    const raw = ["## Marker", "### Continued", "## Quoted", "# Root"];
    const headings = markdownHeadingPositions(source);
    expect(headings.map((heading) => heading.depth)).toEqual([2, 3, 2, 1]);
    expect(headings.map(({ start, end }) => source.slice(start.offset, end.offset))).toEqual(raw);
    expect(headings.map(({ start }) => start.line)).toEqual([1, 2, 4, 6]);
  });
  it.each(["\n", "\r", "\r\n"])("maps leading-BOM parser coordinates to original UTF-16 positions across %j endings", (ending) => {
    const first = "## [first] H😀";
    const nested = "### [nested] N\ufeff😀";
    const later = "## [later] Later";
    for (const prefix of ["", "\ufeff"]) {
      const source = prefix + [first, "- item", `  ${nested}`, later].join(ending);
      const headings = markdownHeadingPositions(source);
      expect(headings).toEqual([
        { depth: 2, start: { line: 1, offset: prefix.length },
          end: { line: 1, offset: prefix.length + first.length } },
        { depth: 3, start: { line: 3, offset: source.indexOf(nested) },
          end: { line: 3, offset: source.indexOf(nested) + nested.length } },
        { depth: 2, start: { line: 4, offset: source.indexOf(later) },
          end: { line: 4, offset: source.length } },
      ]);
      expect(headings.map(({ start, end }) => source.slice(start.offset, end.offset)))
        .toEqual([first, nested, later]);
    }
  });
  it("excludes code, raw HTML and escaped markers, including a tab-indented list fence", () => {
    const source = "<div>\n## [html] Hidden\n</div>\n\n    ## [indented] Hidden\n\n\\## [escaped] Hidden\n\n" +
      "-\t```md\n\t## [fenced] Hidden\n\t```\n\n## [real] Visible";
    const headings = markdownHeadingPositions(source);
    expect(headings).toHaveLength(1);
    expect(source.slice(headings[0]!.start.offset, headings[0]!.end.offset)).toBe("## [real] Visible");
  });
});
