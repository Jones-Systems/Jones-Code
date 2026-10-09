export function oneSpaceThreadQuery(
  text: string,
  cursor: number,
  isBoundary: (char: string) => boolean,
): { query: string; rangeStart: number; rangeEnd: number } | null {
  let index = cursor - 1;
  let spaces = 0;
  while (index >= 0) {
    const char = text[index]!;
    if (char === " ") {
      spaces += 1;
      if (spaces === 2) break;
    } else if (isBoundary(char) || /\s/u.test(char) || char === "\uFFFC") {
      break;
    }
    index -= 1;
  }
  const rangeStart = index + 1;
  const token = text.slice(rangeStart, cursor);
  if (spaces === 0 || !token.startsWith("@")) return null;
  const query = token.slice(1);
  const firstWord = query.slice(0, query.indexOf(" "));
  // Legacy file chips use bare @paths; their delimiter must still close the picker.
  if (!firstWord || /[./\\"@]/u.test(firstWord)) return null;
  return { query, rangeStart, rangeEnd: cursor };
}
