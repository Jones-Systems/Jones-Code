import type { TimestampFormat } from "@t3tools/contracts/settings";

function getTimestampFormatOptions(
  timestampFormat: TimestampFormat,
  includeSeconds: boolean,
): Intl.DateTimeFormatOptions {
  const baseOptions: Intl.DateTimeFormatOptions = {
    hour: "numeric",
    minute: "2-digit",
    ...(includeSeconds ? { second: "2-digit" } : {}),
  };

  if (timestampFormat === "locale") {
    return baseOptions;
  }

  return {
    ...baseOptions,
    hour12: timestampFormat === "12-hour",
  };
}

function resolveTimestampLocale(systemLocale: string | null | undefined): string | undefined {
  const tag = systemLocale?.trim();
  if (!tag) return undefined;

  try {
    // Reject structurally invalid tags; let ICU fall back for valid tags it lacks data for.
    Intl.DateTimeFormat.supportedLocalesOf([tag]);
    return tag;
  } catch {
    return undefined;
  }
}

function readHostSystemLocale(): string | null {
  if (typeof window === "undefined") return null;
  return window.desktopBridge?.getSystemLocale() ?? null;
}

export const timestampLocale = resolveTimestampLocale(readHostSystemLocale());

// A formatter keeps the default zone it was built in, so default-zone formatters run in UTC
// on the wall-clock time, which Date reads from the current zone.
export function wallClockAsUtc(date: Date): Date {
  return new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
}

const timestampFormatterCache = new Map<string, Intl.DateTimeFormat>();

export function getTimestampFormatter(
  timestampFormat: TimestampFormat,
  includeSeconds: boolean,
  timeZone = "UTC",
  locale = timestampLocale,
): Intl.DateTimeFormat {
  const cacheKey = `${timestampFormat}:${includeSeconds ? "seconds" : "minutes"}:${timeZone}:${locale ?? ""}`;
  const cachedFormatter = timestampFormatterCache.get(cacheKey);
  if (cachedFormatter) {
    return cachedFormatter;
  }

  const hourCycle =
    timestampFormat === "locale" && locale !== timestampLocale
      ? getTimestampFormatter(timestampFormat, includeSeconds).resolvedOptions().hourCycle
      : undefined;
  const formatter = new Intl.DateTimeFormat(locale, {
    ...getTimestampFormatOptions(timestampFormat, includeSeconds),
    ...(hourCycle === undefined ? {} : { hourCycle }),
    timeZone,
  });
  timestampFormatterCache.set(cacheKey, formatter);
  return formatter;
}

export function formatTime(
  date: Date,
  timestampFormat: TimestampFormat,
  timeZone?: string,
): string {
  return timeZone
    ? getTimestampFormatter(timestampFormat, false, timeZone).format(date)
    : getTimestampFormatter(timestampFormat, false).format(wallClockAsUtc(date));
}

export function resolveHour12(timestampFormat: TimestampFormat): boolean {
  return getTimestampFormatter(timestampFormat, false).resolvedOptions().hour12 ?? false;
}

export function parseTimestampDate(isoDate: string): Date | null {
  const date = new Date(isoDate);
  return Number.isNaN(date.getTime()) ? null : date;
}

const numericDateOptions: Intl.DateTimeFormatOptions = { month: "numeric", day: "numeric" };
const numericDateWithYearOptions: Intl.DateTimeFormatOptions = {
  ...numericDateOptions,
  year: "numeric",
};
const numericDateFormatter = new Intl.DateTimeFormat(timestampLocale, {
  ...numericDateOptions,
  timeZone: "UTC",
});
const numericDateWithYearFormatter = new Intl.DateTimeFormat(timestampLocale, {
  ...numericDateWithYearOptions,
  timeZone: "UTC",
});

interface CalendarDay {
  year: number;
  /** Days since the epoch, so differences are whole days across DST shifts. */
  index: number;
}

export function calendarDay(date: Date, timeZone: string | undefined): CalendarDay {
  if (!timeZone) {
    const year = date.getFullYear();
    return { year, index: Date.UTC(year, date.getMonth(), date.getDate()) / 86_400_000 };
  }
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "numeric",
    day: "numeric",
  }).formatToParts(date);
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    Number(parts.find((entry) => entry.type === type)?.value);
  const year = part("year");
  return { year, index: Date.UTC(year, part("month") - 1, part("day")) / 86_400_000 };
}

export function formatNumericDate(
  date: Date,
  sameYear: boolean,
  timeZone: string | undefined,
): string {
  if (!timeZone) {
    return (sameYear ? numericDateFormatter : numericDateWithYearFormatter).format(
      wallClockAsUtc(date),
    );
  }
  return new Intl.DateTimeFormat(timestampLocale, {
    ...(sameYear ? numericDateOptions : numericDateWithYearOptions),
    timeZone,
  }).format(date);
}

export function formatDayAwareTimestamp(
  isoDate: string,
  timestampFormat: TimestampFormat,
  nowMs: number = Date.now(),
  timeZone?: string,
  yesterdayAt: (time: string) => string = (time) => `yesterday at ${time}`,
): string {
  const date = parseTimestampDate(isoDate);
  if (!date) return "";
  const time = formatTime(date, timestampFormat, timeZone);

  const today = calendarDay(new Date(nowMs), timeZone);
  const messageDay = calendarDay(date, timeZone);
  const dayDiff = today.index - messageDay.index;

  if (dayDiff <= 0) return time;
  if (dayDiff === 1) return yesterdayAt(time);
  return `${formatNumericDate(date, messageDay.year === today.year, timeZone)} ${time}`;
}
