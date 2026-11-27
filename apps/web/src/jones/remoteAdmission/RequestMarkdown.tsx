import type { Element, ElementContent, RootContent } from "hast";
import { toJsxRuntime, type Components } from "hast-util-to-jsx-runtime";
import type { ReactNode } from "react";
import { Fragment, jsx, jsxs } from "react/jsx-runtime";

import { markdownTree } from "~/markdown/wordsHast";

const allowedElements = new Set([
  "p",
  "br",
  "strong",
  "em",
  "del",
  "code",
  "pre",
  "blockquote",
  "ul",
  "ol",
  "li",
  "hr",
  "table",
  "thead",
  "tbody",
  "tr",
  "th",
  "td",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "a",
  "img",
  "input",
]);

function markTaskList(list: Element) {
  for (const item of list.children) {
    if (item.type !== "element" || item.tagName !== "li") continue;
    const first = item.children.find((child) => child.type === "element");
    const marker = first?.tagName === "p" ? first.children[0] : first;
    if (marker?.type === "element" && marker.tagName === "input") {
      item.properties.className = ["task-list-item"];
      list.properties.className = ["contains-task-list"];
    }
  }
}

function previewChildren(children: readonly RootContent[]): ElementContent[] {
  return children.flatMap((child): ElementContent[] => {
    if (child.type === "text") return [child];
    if (child.type !== "element") return [];
    child.children = previewChildren(child.children);
    if (child.tagName === "ul" || child.tagName === "ol") markTaskList(child);
    return allowedElements.has(child.tagName) ? [child] : child.children;
  });
}

function PreviewHeading({ children }: { children?: ReactNode }) {
  return <p className="font-semibold">{children}</p>;
}

const components: Partial<Components> = {
  a: ({ children: text, href }) => (
    <>
      {String(text) === href ? (
        text
      ) : (
        <>
          {text}
          {href ? ` (${href})` : ""}
        </>
      )}
    </>
  ),
  img: ({ alt, src }) => (
    <>
      [image: {alt ?? ""}] ({typeof src === "string" ? src : ""})
    </>
  ),
  input: ({ checked }) => (
    <span>
      {checked ? "☑" : "☐"}
      <span className="sr-only">{checked ? "Done" : "Not done"}</span>
    </span>
  ),
  h1: PreviewHeading,
  h2: PreviewHeading,
  h3: PreviewHeading,
  h4: PreviewHeading,
  h5: PreviewHeading,
  h6: PreviewHeading,
  pre: ({ children: text }) => <pre>{text}</pre>,
  code: ({ children: text }) => <code>{text}</code>,
};

export function RequestMarkdown({ children }: { readonly children: string }) {
  // Review shows authored destinations and raw HTML as text.
  const tree = markdownTree(children, { escapeHtml: true });
  tree.children = previewChildren(tree.children);
  return (
    <div className="chat-markdown max-h-60 min-w-0 overflow-auto overscroll-contain wrap-anywhere [&_pre]:whitespace-pre-wrap [&_pre]:wrap-anywhere [&_table]:table-fixed [&_table]:w-full">
      {toJsxRuntime(tree, { Fragment, jsx, jsxs, components })}
    </div>
  );
}
