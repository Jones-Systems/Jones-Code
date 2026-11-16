export interface FindTextMatch {
  start: number;
  end: number;
}

export function findTextMatches(text: string, query: string): FindTextMatch[] {
  if (!query) return [];
  // Escaping a literal RegExp preserves UTF-16 offsets even for case folds
  // whose lowercase representation has a different length.
  const expression = new RegExp(query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "giu");
  return Array.from(text.matchAll(expression), (match) => ({
    start: match.index,
    end: match.index + match[0].length,
  }));
}

export function collectFindRanges(root: HTMLElement, query: string): Range[] {
  if (!query) return [];
  const ranges: Range[] = [];
  const blocks = new Map<Element, Text[]>();
  const walker = root.ownerDocument.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
    acceptNode(node) {
      const parent = node.parentElement;
      if (
        !parent ||
        parent.closest(
          '[data-panel-find-bar], [hidden], [inert], [aria-hidden="true"], script, style, textarea, input, [contenteditable="true"]',
        ) ||
        parent.closest("[data-find-scope]") !== root.closest("[data-find-scope]")
      )
        return NodeFilter.FILTER_REJECT;
      for (let element: Element | null = parent; element; element = element.parentElement) {
        const style = getComputedStyle(element);
        if (style.display === "none" || style.visibility === "hidden")
          return NodeFilter.FILTER_REJECT;
        if (element === root) break;
      }
      return NodeFilter.FILTER_ACCEPT;
    },
  });
  while (walker.nextNode()) {
    const node = walker.currentNode as Text;
    const block =
      node.parentElement?.closest(
        "p, li, pre, h1, h2, h3, h4, h5, h6, td, th, button, label, [data-find-item]",
      ) ?? root;
    const nodes = blocks.get(block) ?? [];
    nodes.push(node);
    blocks.set(block, nodes);
  }
  for (const nodes of blocks.values()) {
    const text = nodes.map((node) => node.data).join("");
    for (const match of findTextMatches(text, query)) {
      const range = root.ownerDocument.createRange();
      let offset = 0;
      for (const node of nodes) {
        const end = offset + node.length;
        if (match.start >= offset && match.start < end) range.setStart(node, match.start - offset);
        if (match.end > offset && match.end <= end) {
          range.setEnd(node, match.end - offset);
          break;
        }
        offset = end;
      }
      ranges.push(range);
    }
  }
  return ranges;
}

export function scrollFindRangeIntoView(range: Range) {
  const element = range.startContainer.parentElement;
  if (!element) return;
  const rect = range.getBoundingClientRect();
  // Scroll the match itself, rather than the top of a potentially long message.
  for (let parent = element.parentElement; parent; parent = parent.parentElement) {
    const style = getComputedStyle(parent);
    const bounds = parent.getBoundingClientRect();
    if (
      /(auto|scroll)/.test(style.overflowY) &&
      (rect.top < bounds.top + 48 || rect.bottom > bounds.bottom)
    ) {
      parent.scrollTop += rect.top - bounds.top - bounds.height / 2;
    }
    if (
      /(auto|scroll)/.test(style.overflowX) &&
      (rect.left < bounds.left || rect.right > bounds.right)
    ) {
      parent.scrollLeft += rect.left - bounds.left - bounds.width / 2;
    }
  }
}
