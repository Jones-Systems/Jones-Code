import type { TimestampFormat } from "@t3tools/contracts/settings";

const timeZone = "America/New_York";
const easternDate = new Intl.DateTimeFormat("en-US-u-ca-gregory-nu-latn", {
  timeZone,
  year: "numeric",
  month: "numeric",
  day: "numeric",
});
const zoneName = new Intl.DateTimeFormat("en-US", { timeZone, timeZoneName: "short" });

function calendar(ms: number) {
  const parts = easternDate.formatToParts(ms);
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    Number(parts.find((value) => value.type === type)?.value);
  const year = part("year");
  return { year, day: Date.UTC(year, part("month") - 1, part("day")) };
}

export function formatRequestTime(
  iso: string,
  timestampFormat: TimestampFormat,
  {
    nowMs = Date.now(),
    locale,
    full = false,
  }: {
    readonly nowMs?: number;
    readonly locale?: string;
    readonly full?: boolean;
  } = {},
): string {
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) return "Invalid time";
  const date = calendar(ms);
  const now = calendar(nowMs);
  const clock = new Intl.DateTimeFormat(locale, {
    timeZone,
    hour: "numeric",
    minute: "2-digit",
    ...(full ? { second: "2-digit" } : {}),
    ...(timestampFormat === "12-hour"
      ? { hour12: true }
      : timestampFormat === "24-hour"
        ? { hourCycle: "h23" as const }
        : {}),
  }).format(ms);
  const zone = zoneName.formatToParts(ms).find((part) => part.type === "timeZoneName")?.value;
  const time = `${clock} ${zone ?? "America/New_York"}`;
  if (!full && date.day === now.day) return time;
  if (!full && now.day - date.day === 86_400_000) return `yesterday at ${time}`;
  const day = new Intl.DateTimeFormat(locale, {
    timeZone,
    month: "numeric",
    day: "numeric",
    ...(full || date.year !== now.year ? { year: "numeric" } : {}),
  }).format(ms);
  return `${day} ${time}`;
}
