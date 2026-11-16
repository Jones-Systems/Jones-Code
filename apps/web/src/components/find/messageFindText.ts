import { unified } from "unified";
import remarkParse from "remark-parse";
import remarkGfm from "remark-gfm";
import type { Root, RootContent } from "mdast";
import { renderCodexDirectivesForCopy } from "@t3tools/client-runtime/codex-markdown-directives";

const parser = unified().use(remarkParse).use(remarkGfm).freeze();
const cache = new WeakMap<object, { source: string; text: string }>();

function nodeText(node: Root | RootContent): string {
  if (node.type === "definition" || node.type === "html" || node.type === "image") return "";
  if ("value" in node) return node.value;
  if (node.type === "break") return "\n";
  if ("children" in node) {
    const separator = ["root", "blockquote", "list", "listItem", "table", "tableRow"].includes(
      node.type,
    )
      ? "\n"
      : "";
    return node.children.map(nodeText).join(separator);
  }
  return "";
}

// Index lazily while find is open; unchanged messages reuse their parsed text.
export function messageFindText(message: { text: string }): string {
  const cached = cache.get(message);
  if (cached?.source === message.text) return cached.text;
  const text = nodeText(parser.parse(renderCodexDirectivesForCopy(message.text)));
  cache.set(message, { source: message.text, text });
  return text;
}
