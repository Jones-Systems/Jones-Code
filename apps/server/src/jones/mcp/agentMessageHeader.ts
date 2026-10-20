const sentTimeFormat = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

const escapeTitle = (title: string) =>
  title
    .replace(/\s+/g, " ")
    .trim()
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/[\\`*_[\]{}()#+.!|~-]/g, "\\$&");

export function composeAgentMessage(input: {
  readonly body: string;
  readonly acceptedAt: number;
  readonly sender?: { readonly id: string; readonly title?: string | null };
}): string {
  // Slash commands depend on the original prefix and sometimes the entire text.
  if (input.body.trimStart().startsWith("/")) return input.body;

  const parts = Object.fromEntries(
    sentTimeFormat.formatToParts(input.acceptedAt).map(({ type, value }) => [type, value]),
  );
  const sentAt = `${parts.month}/${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
  const sender =
    input.sender === undefined
      ? "unavailable"
      : `[${escapeTitle(input.sender.title ?? "") || "Untitled thread"}](t3-thread://v1/${input.sender.id})`;
  return `**T3M** · From ${sender} · Sent: ${sentAt}\n\n${input.body}`;
}
