import { UsageDay, type UsageSummaryInput } from "@t3tools/contracts";

const HOUR_MS = 60 * 60 * 1000;
const MINUTE_MS = 60 * 1000;
const LOCAL_DATE_TIME_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/;

export type CustomUsageWindowValidation =
  | { readonly ok: true; readonly window: UsageSummaryInput }
  | { readonly ok: false; readonly error: string };

export function toLocalDateTimeValue(date: Date): string {
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function parseLocalDateTime(
  value: string,
  originalTime?: string,
): { readonly date: Date; readonly ambiguous: boolean } | null {
  if (!LOCAL_DATE_TIME_PATTERN.test(value)) return null;
  if (originalTime !== undefined) {
    const original = new Date(originalTime);
    if (!Number.isNaN(original.getTime()) && toLocalDateTimeValue(original) === value) {
      return { date: original, ambiguous: false };
    }
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime()) || toLocalDateTimeValue(date) !== value) return null;
  const offset = date.getTimezoneOffset();
  const ambiguous = [-24, 24].some((hours) => {
    const neighboringOffset = new Date(date.getTime() + hours * HOUR_MS).getTimezoneOffset();
    if (neighboringOffset === offset) return false;
    const alternative = new Date(date.getTime() + (neighboringOffset - offset) * MINUTE_MS);
    return toLocalDateTimeValue(alternative) === value;
  });
  return { date, ambiguous };
}

function exactWindow(since: Date, until: Date, resolution: "exactDay" | "hour"): UsageSummaryInput {
  let timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  let format: Intl.DateTimeFormat;
  try {
    format = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
  } catch {
    timeZone = "UTC";
    format = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
  }
  return {
    sinceDay: UsageDay.make(format.format(since)),
    untilDay: UsageDay.make(format.format(until)),
    timeZone,
    resolution,
    sinceTime: since.toISOString(),
    untilTime: until.toISOString(),
  };
}

export function makeRollingUsageWindow(hours: number, now = new Date()): UsageSummaryInput {
  if (!Number.isInteger(hours) || hours < 1 || hours > 24) {
    throw new RangeError("A rolling usage window must be from 1 to 24 whole hours");
  }
  const untilMs = Math.floor(now.getTime() / MINUTE_MS) * MINUTE_MS;
  const until = new Date(untilMs);
  const since = new Date(untilMs - hours * HOUR_MS);
  return exactWindow(since, until, "hour");
}

export function validateCustomUsageWindow(
  sinceValue: string,
  untilValue: string,
  now = new Date(),
  originalBounds?: Pick<UsageSummaryInput, "sinceTime" | "untilTime">,
): CustomUsageWindowValidation {
  if (!sinceValue || !untilValue) {
    return { ok: false, error: "Enter both a start and end date and time." };
  }
  const sinceResult = parseLocalDateTime(sinceValue, originalBounds?.sinceTime);
  const untilResult = parseLocalDateTime(untilValue, originalBounds?.untilTime);
  if (sinceResult === null || untilResult === null) {
    return { ok: false, error: "Enter valid local dates and times." };
  }
  if (sinceResult.ambiguous || untilResult.ambiguous) {
    return {
      ok: false,
      error:
        "This local time occurs twice when clocks move back. Choose a time outside the repeated hour or keep the original range time unchanged.",
    };
  }
  const since = sinceResult.date;
  const until = untilResult.date;
  if (until.getTime() <= since.getTime()) {
    return { ok: false, error: "End date and time must be after the start." };
  }
  if (until.getTime() > now.getTime()) {
    return { ok: false, error: "End date and time cannot be in the future." };
  }

  const resolution = until.getTime() - since.getTime() <= 24 * HOUR_MS ? "hour" : "exactDay";
  return { ok: true, window: exactWindow(since, until, resolution) };
}
