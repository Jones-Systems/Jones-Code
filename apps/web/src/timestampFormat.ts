import type { TimestampFormat } from "@t3tools/contracts/settings";
import { type MessageResolver, m, getActiveLocale } from "./i18n/messages.ts";
import { DEFAULT_LOCALE } from "./i18n/locales.ts";
import { msg } from "gt-react";

import {
  calendarDay,
  formatDayAwareTimestamp as dayAware,
  formatNumericDate,
  formatTime,
  getTimestampFormatter,
  parseTimestampDate,
  timestampLocale,
  wallClockAsUtc,
} from "./lib/timestampClock.ts";
export { parseTimestampDate, resolveHour12 } from "./lib/timestampClock.ts";

const WEEKDAY_INDEXES = [0, 1, 2, 3, 4, 5, 6] as const;
type WeekdayIndex = (typeof WEEKDAY_INDEXES)[number];

type LocaleWithWeekInfo = Intl.Locale & {
  readonly weekInfo?: { readonly firstDay: number };
  getWeekInfo?: () => { readonly firstDay: number };
};

/**
 * First weekday of a locale as a `Date#getDay` index (0 is Sunday), or
 * `undefined` when the runtime has no week data, so callers keep their own
 * default.
 */
function resolveWeekStartsOn(locale: string | undefined): WeekdayIndex | undefined {
  try {
    const resolved: LocaleWithWeekInfo = new Intl.Locale(
      locale ?? Intl.DateTimeFormat().resolvedOptions().locale,
    );
    // Week info counts Monday as 1 and Sunday as 7.
    const firstDay = resolved.getWeekInfo?.().firstDay ?? resolved.weekInfo?.firstDay;
    return firstDay === undefined ? undefined : WEEKDAY_INDEXES[firstDay % 7];
  } catch {
    return undefined;
  }
}

export const weekStartsOn = resolveWeekStartsOn(timestampLocale);

// English keeps its ordinal day-before-month form; other UI languages use their own long date.
const monthNameFormatter = new Intl.DateTimeFormat("en-US", { month: "long", timeZone: "UTC" });
const longDateFormatterCache = new Map<string, Intl.DateTimeFormat>();

function getLongDateFormatter(locale: string): Intl.DateTimeFormat {
  let formatter = longDateFormatterCache.get(locale);
  if (!formatter) {
    formatter = new Intl.DateTimeFormat(locale, {
      day: "numeric",
      month: "long",
      year: "numeric",
      timeZone: "UTC",
    });
    longDateFormatterCache.set(locale, formatter);
  }
  return formatter;
}

function ordinalSuffix(day: number): string {
  const lastTwo = day % 100;
  if (lastTwo >= 11 && lastTwo <= 13) return "th";
  switch (day % 10) {
    case 1:
      return "st";
    case 2:
      return "nd";
    case 3:
      return "rd";
    default:
      return "th";
  }
}

export function formatChatTimestampTooltip(
  isoDate: string,
  timestampFormat: TimestampFormat,
  resolve: MessageResolver = m,
): string {
  const date = parseTimestampDate(isoDate);
  if (!date) return "";
  const time = formatTime(date, timestampFormat);
  const locale = getActiveLocale();
  if (locale !== DEFAULT_LOCALE) {
    const longDate = getLongDateFormatter(locale).format(wallClockAsUtc(date));
    return resolve(
      msg("{time}, {date}", { time, date: longDate, $context: "message time, then its date" }),
    );
  }
  const day = date.getDate();
  const month = monthNameFormatter.format(wallClockAsUtc(date));
  const year = date.getFullYear();
  return `${time}, ${day}${ordinalSuffix(day)} ${month} ${year}`;
}

export function formatFullTimestamp(isoDate: string, timestampFormat: TimestampFormat): string {
  return new Date(isoDate).toLocaleString(
    undefined,
    timestampFormat === "locale" ? undefined : { hour12: timestampFormat === "12-hour" },
  );
}

export function formatShortTimestamp(
  isoDate: string,
  timestampFormat: TimestampFormat,
  locale?: string,
): string {
  const date = parseTimestampDate(isoDate);
  if (!date) return "";
  return getTimestampFormatter(timestampFormat, false, "UTC", locale).format(wallClockAsUtc(date));
}

const dateTimeFormatterCache = new Map<TimestampFormat, Intl.DateTimeFormat>();

export function formatDateTimeTimestamp(isoDate: string, timestampFormat: TimestampFormat): string {
  const date = parseTimestampDate(isoDate);
  if (!date) return "";
  let formatter = dateTimeFormatterCache.get(timestampFormat);
  if (!formatter) {
    formatter = new Intl.DateTimeFormat(timestampLocale, {
      dateStyle: "medium",
      timeStyle: "short",
      ...(timestampFormat === "locale" ? {} : { hour12: timestampFormat === "12-hour" }),
    });
    dateTimeFormatterCache.set(timestampFormat, formatter);
  }
  return formatter.format(date);
}

/** A wall-clock `H:MM` or `HH:MM` that names no date or zone, such as a daily schedule's time. */
export function formatTimeOfDay(timeOfDay: string, timestampFormat: TimestampFormat): string {
  const match = /^(\d{1,2}):(\d{2})$/.exec(timeOfDay);
  if (!match) return timeOfDay;
  // Default-zone formatters run in UTC, so the host zone cannot shift the wall time.
  return getTimestampFormatter(timestampFormat, false).format(
    Date.UTC(2000, 0, 1, Number(match[1]), Number(match[2])),
  );
}

export function formatDayAwareTimestamp(
  isoDate: string,
  timestampFormat: TimestampFormat,
  nowMs: number = Date.now(),
  timeZone?: string,
  resolve: MessageResolver = m,
): string {
  return dayAware(isoDate, timestampFormat, nowMs, timeZone, (time) =>
    resolve(msg("yesterday at {time}", { time })),
  );
}

/** `timeZone` overrides the system's current zone. */
export function formatUpcomingTimestamp(
  isoDate: string,
  timestampFormat: TimestampFormat,
  nowMs: number = Date.now(),
  timeZone?: string,
  resolve: MessageResolver = m,
): string {
  const date = parseTimestampDate(isoDate);
  if (!date) return "";
  const time = formatTime(date, timestampFormat, timeZone);

  const today = calendarDay(new Date(nowMs), timeZone);
  const targetDay = calendarDay(date, timeZone);
  const dayDiff = targetDay.index - today.index;

  if (dayDiff < 0) {
    return formatDayAwareTimestamp(isoDate, timestampFormat, nowMs, timeZone, resolve);
  }
  if (dayDiff === 0) return time;
  if (dayDiff === 1) return resolve(msg("tomorrow at {time}", { time }));
  return `${formatNumericDate(date, targetDay.year === today.year, timeZone)} ${time}`;
}

/** `label` is the whole phrase ("5m ago"); `value` and `suffix` split it for styled layouts. */
type RelativeTimeParts = { value: string; suffix: string | null; label: string };
export type RelativeTimeState =
  | { status: "missing" }
  | { status: "invalid" }
  | ({ status: "relative" } & RelativeTimeParts);

const MINUTE_MS = 60_000;
const HOUR_MS = 3_600_000;
const DAY_MS = 86_400_000;

function relativeTimeUnitMs(elapsedMs: number): number {
  if (elapsedMs < HOUR_MS) return MINUTE_MS;
  if (elapsedMs < DAY_MS) return HOUR_MS;
  return DAY_MS;
}

/** Elapsed time floored to formatRelativeTime precision; equal labels yield equal values. */
export function relativeTimeElapsedMs(isoDate: string, nowMs: number = Date.now()): number | null {
  const date = parseTimestampDate(isoDate);
  if (!date) return null;
  const elapsedMs = Math.max(0, nowMs - date.getTime());
  const unitMs = relativeTimeUnitMs(elapsedMs);
  return Math.floor(elapsedMs / unitMs) * unitMs;
}

export function msUntilRelativeTimeChange(
  isoDate: string,
  nowMs: number = Date.now(),
): number | null {
  const date = parseTimestampDate(isoDate);
  if (!date) return null;
  const elapsedMs = Math.max(0, nowMs - date.getTime());
  const unitMs = relativeTimeUnitMs(elapsedMs);
  const nextChangeMs = date.getTime() + (Math.floor(elapsedMs / unitMs) + 1) * unitMs;
  // Delays above 2^31-1 fire immediately; cap safely and let callers reschedule.
  return Math.min(nextChangeMs - nowMs, DAY_MS);
}

const secondsLabel = (count: number, resolve: MessageResolver) =>
  resolve(msg("{count}s", { count, $context: "seconds, abbreviated" }));
const minutesLabel = (count: number, resolve: MessageResolver) =>
  resolve(msg("{count}m", { count, $context: "minutes, abbreviated" }));
const hoursLabel = (count: number, resolve: MessageResolver) =>
  resolve(msg("{count}h", { count, $context: "hours, abbreviated" }));
const daysLabel = (count: number, resolve: MessageResolver) =>
  resolve(msg("{count}d", { count, $context: "days, abbreviated" }));

function relativeElapsedLabel(elapsedMs: number, resolve: MessageResolver): string {
  const seconds = Math.floor(elapsedMs / 1000);
  if (seconds < 60) return resolve(msg("just now"));
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return resolve(msg("{count}m ago", { count: minutes }));
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return resolve(msg("{count}h ago", { count: hours }));
  return resolve(msg("{count}d ago", { count: Math.floor(hours / 24) }));
}

export function formatRelativeElapsed(
  elapsedMs: number,
  resolve: MessageResolver = m,
): RelativeTimeParts {
  const label = relativeElapsedLabel(elapsedMs, resolve);
  const seconds = Math.floor(elapsedMs / 1000);
  if (seconds < 60) return { value: label, suffix: null, label };
  const ago = resolve(msg("ago", { $context: "follows an elapsed time such as 5m" }));
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return { value: minutesLabel(minutes, resolve), suffix: ago, label };
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return { value: hoursLabel(hours, resolve), suffix: ago, label };
  const days = Math.floor(hours / 24);
  return { value: daysLabel(days, resolve), suffix: ago, label };
}

export function formatRelativeTime(
  isoDate: string,
  nowMs: number = Date.now(),
  resolve: MessageResolver = m,
): RelativeTimeParts | null {
  const elapsedMs = relativeTimeElapsedMs(isoDate, nowMs);
  return elapsedMs === null ? null : formatRelativeElapsed(elapsedMs, resolve);
}

export function formatRelativeTimeLabel(
  isoDate: string,
  nowMs: number = Date.now(),
  resolve: MessageResolver = m,
) {
  const elapsedMs = relativeTimeElapsedMs(isoDate, nowMs);
  return elapsedMs === null ? "" : relativeElapsedLabel(elapsedMs, resolve);
}

export function formatElapsedDurationLabel(
  isoDate: string,
  nowMs: number = Date.now(),
  resolve: MessageResolver = m,
): string {
  const date = parseTimestampDate(isoDate);
  if (!date) return "";
  const diffMs = nowMs - date.getTime();
  if (diffMs <= 0) return resolve(msg("just now"));

  const seconds = Math.floor(diffMs / 1000);
  if (seconds < 5) return resolve(msg("just now"));
  if (seconds < 60) return secondsLabel(seconds, resolve);

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return minutesLabel(minutes, resolve);

  const hours = Math.floor(minutes / 60);
  if (hours < 24) return hoursLabel(hours, resolve);

  const days = Math.floor(hours / 24);
  return daysLabel(days, resolve);
}

/** Pass nowMs to match a parent-driven render tick. */
export function formatExpiresInLabel(
  isoDate: string,
  nowMs: number = Date.now(),
  resolve: MessageResolver = m,
): string {
  const date = parseTimestampDate(isoDate);
  if (!date) return "";
  const diffMs = date.getTime() - nowMs;
  if (diffMs <= 0) return resolve(msg("Expired"));

  const totalSeconds = Math.floor(diffMs / 1000);
  if (totalSeconds < 5) return resolve(msg("Expires in a moment"));
  const expiresIn = (parts: string[]) =>
    resolve(msg("Expires in {duration}", { duration: parts.join(" ") }));
  if (totalSeconds < 60) return expiresIn([secondsLabel(totalSeconds, resolve)]);

  if (totalSeconds < 3600) {
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return seconds === 0
      ? expiresIn([minutesLabel(minutes, resolve)])
      : expiresIn([minutesLabel(minutes, resolve), secondsLabel(seconds, resolve)]);
  }

  if (totalSeconds < 86_400) {
    const hours = Math.floor(totalSeconds / 3600);
    const rem = totalSeconds % 3600;
    const minutes = Math.floor(rem / 60);
    const seconds = rem % 60;
    const parts = [hoursLabel(hours, resolve)];
    if (minutes > 0) parts.push(minutesLabel(minutes, resolve));
    if (seconds > 0) parts.push(secondsLabel(seconds, resolve));
    return expiresIn(parts);
  }

  const days = Math.floor(totalSeconds / 86_400);
  const remAfterDays = totalSeconds % 86_400;
  if (remAfterDays === 0) return expiresIn([daysLabel(days, resolve)]);
  const hours = Math.floor(remAfterDays / 3600);
  const rem = remAfterDays % 3600;
  const minutes = Math.floor(rem / 60);
  const seconds = rem % 60;
  const parts = [daysLabel(days, resolve)];
  if (hours > 0) parts.push(hoursLabel(hours, resolve));
  if (minutes > 0) parts.push(minutesLabel(minutes, resolve));
  if (seconds > 0) parts.push(secondsLabel(seconds, resolve));
  return expiresIn(parts);
}
