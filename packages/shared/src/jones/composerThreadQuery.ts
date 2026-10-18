export function oneSpaceThreadQuery(
  text: string,
  cursor: number,
  isBoundary: (char: string) => boolean,
): { query: string; rangeStart: number; rangeEnd: number } | null {
  let index = cursor - 1;
  let hasSpace = false;
  while (index >= 0) {
    const char = text[index]!;
    if (char === " ") {
      if (text[index - 1] === " ") return null;
      hasSpace = true;
    } else if (isBoundary(char) || /\s/u.test(char) || char === "\uFFFC") {
      return null;
    } else if (char === "@") {
      const previous = text[index - 1];
      if (
        previous === undefined ||
        isBoundary(previous) ||
        /\s/u.test(previous) ||
        previous === "\uFFFC"
      ) {
        if (!hasSpace) return null;
        const query = text.slice(index + 1, cursor);
        const firstWord = query.slice(0, query.indexOf(" "));
        // Legacy file chips use bare @paths; their delimiter must still close the picker.
        if (!firstWord || /[./\\"@]/u.test(firstWord)) return null;
        return { query, rangeStart: index, rangeEnd: cursor };
      }
    }
    index -= 1;
  }
  return null;
}
