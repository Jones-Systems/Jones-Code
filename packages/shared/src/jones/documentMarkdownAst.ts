import type { Nodes } from "mdast";
import remarkParse from "remark-parse";
import { unified } from "unified";

// Offsets index the original JavaScript string in UTF-16 code units, never blob bytes.
export interface MarkdownHeadingPosition {
  readonly depth: 1 | 2 | 3 | 4 | 5 | 6;
  readonly start: { readonly line: number; readonly offset: number };
  readonly end: { readonly line: number; readonly offset: number };
}

const parser = unified().use(remarkParse).freeze();

export function markdownHeadingPositions(source: string): readonly MarkdownHeadingPosition[] {
  // Micromark consumes one initial BOM without including it in any subsequent offset.
  const initialBomOffset = source.startsWith("\ufeff") ? 1 : 0;
  const lineStarts = [0];
  const lineEnds: number[] = [];
  for (const ending of source.matchAll(/\r\n|\r|\n/g)) {
    lineEnds.push(ending.index);
    lineStarts.push(ending.index + ending[0].length);
  }
  lineEnds.push(source.length);
  const point = (value: { readonly line: number; readonly offset?: number } | undefined) => {
    if (value === undefined || !Number.isInteger(value.line) || value.line < 1 || value.line > lineStarts.length ||
      typeof value.offset !== "number" || !Number.isInteger(value.offset) || value.offset < 0) {
      throw new Error("Invalid Markdown heading position.");
    }
    const offset = value.offset + initialBomOffset;
    if (offset < lineStarts[value.line - 1]! || offset > lineEnds[value.line - 1]!) {
      throw new Error("Invalid Markdown heading position.");
    }
    return { line: value.line, offset };
  };
  const headings: MarkdownHeadingPosition[] = [];
  const pending: Nodes[] = [parser.parse(source)];
  while (pending.length > 0) {
    const node = pending.pop()!;
    if (node.type === "heading") {
      const start = point(node.position?.start);
      const end = point(node.position?.end);
      if (!Number.isInteger(node.depth) || node.depth < 1 || node.depth > 6 || end.line < start.line ||
        end.offset <= start.offset || (headings.length > 0 && start.offset < headings[headings.length - 1]!.end.offset)) {
        throw new Error("Invalid Markdown heading position.");
      }
      headings.push({ depth: node.depth, start, end });
    }
    if ("children" in node) {
      for (let index = node.children.length - 1; index >= 0; index--) pending.push(node.children[index]!);
    }
  }
  return headings;
}
