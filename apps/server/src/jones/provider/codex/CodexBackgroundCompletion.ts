function simpleCommandWords(command: string): string[] | undefined {
  // Reject shell composition and expansion rather than guessing which command expired.
  if (/[;&|<>$`()\\\r\n]/.test(command)) return undefined;
  const words: string[] = [];
  const token = /\s*(?:"([^"\\]*)"|'([^'\\]*)'|([^\s"'\\]+))(?=\s|$)/gy;
  const text = command.trim();
  while (token.lastIndex < text.length) {
    const match = token.exec(text);
    if (match === null) return undefined;
    words.push(match[1] ?? match[2] ?? match[3]!);
  }
  return words;
}

const durationPattern = /^\d+(?:\.\d+)?[smhd]?$/;

export function isExpectedCodexBackgroundTermination(item: {
  readonly command: string;
  readonly exitCode?: number | null | undefined;
}): boolean {
  if (item.exitCode !== 124) return false;
  const command = item.command.trim();
  const wrapper = /^(?:\/[^\s"'\\]+\/)?(?:sh|bash|zsh|dash)\s+-(?:lc|c)\s+(['"])([^\r\n]*)\1$/.exec(
    command,
  );
  const words = simpleCommandWords(wrapper?.[2] ?? command);
  if (words === undefined || !/^(?:.*\/)?timeout$/.test(words[0] ?? "")) return false;
  let index = 1;
  while (words[index]?.startsWith("-")) {
    const option = words[index++];
    if (option === "--") break;
    if (option === "--foreground") continue;
    if (option === "-k" || option === "--kill-after") {
      if (!durationPattern.test(words[index++] ?? "")) return false;
    } else if (option === "-s" || option === "--signal") {
      if (!/^(?:SIG)?[A-Z0-9]+$/.test(words[index++] ?? "")) return false;
    } else {
      return false;
    }
  }
  const duration = words[index++] ?? "";
  if (!durationPattern.test(duration) || Number.parseFloat(duration) <= 0) return false;
  if (!/^(?:.*\/)?python(?:3(?:\.\d+)?)?$/.test(words[index++] ?? "")) return false;
  while (words[index] === "-u" || words[index] === "-B") index++;
  return words[index] === "-m" && words[index + 1] === "http.server";
}
