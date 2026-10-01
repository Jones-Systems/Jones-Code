// @effect-diagnostics globalDate:off -- Date-time local inputs intentionally use the viewer's zone.
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  makeRollingUsageWindow,
  toLocalDateTimeValue,
  validateCustomUsageWindow,
} from "./usageDateRange";

afterEach(() => vi.unstubAllEnvs());

describe("usage date ranges", () => {
  it.each([1, 3, 6, 12])("builds an exact %ih rolling window", (hours) => {
    vi.stubEnv("TZ", "UTC");
    const window = makeRollingUsageWindow(hours, new Date("2026-09-18T12:37:42.123Z"));

    expect(window.resolution).toBe("hour");
    expect(window.sinceTime).toBe(`2026-09-18T${String(12 - hours).padStart(2, "0")}:37:00.000Z`);
    expect(window.untilTime).toBe("2026-09-18T12:37:00.000Z");
  });

  it("preserves exact boundaries and switches to daily buckets for longer ranges", () => {
    vi.stubEnv("TZ", "UTC");
    const validation = validateCustomUsageWindow(
      "2026-09-15T10:30",
      "2026-09-18T12:37",
      new Date("2026-09-18T12:38:00.000Z"),
    );

    expect(validation).toMatchObject({
      ok: true,
      window: {
        sinceDay: "2026-09-15",
        untilDay: "2026-09-18",
        timeZone: "UTC",
        resolution: "exactDay",
        sinceTime: "2026-09-15T10:30:00.000Z",
        untilTime: "2026-09-18T12:37:00.000Z",
      },
    });
  });

  it("uses the viewer's zone to build exact hourly bounds across DST", () => {
    vi.stubEnv("TZ", "America/New_York");
    const validation = validateCustomUsageWindow(
      "2026-03-08T01:00",
      "2026-03-08T04:00",
      new Date("2026-03-08T09:00:00.000Z"),
    );

    expect(validation).toMatchObject({
      ok: true,
      window: {
        sinceDay: "2026-03-08",
        untilDay: "2026-03-08",
        timeZone: "America/New_York",
        resolution: "hour",
        sinceTime: "2026-03-08T06:00:00.000Z",
        untilTime: "2026-03-08T08:00:00.000Z",
      },
    });
  });

  it("rejects missing, reversed, and future ranges", () => {
    vi.stubEnv("TZ", "UTC");
    const now = new Date("2026-09-18T12:00:00.000Z");

    expect(validateCustomUsageWindow("", "2026-09-18T11:00", now)).toMatchObject({
      ok: false,
      error: "Enter both a start and end date and time.",
    });
    expect(validateCustomUsageWindow("2026-09-18T11:00", "2026-09-18T10:00", now)).toMatchObject({
      ok: false,
      error: "End date and time must be after the start.",
    });
    expect(validateCustomUsageWindow("2026-09-18T11:00", "2026-09-18T12:01", now)).toMatchObject({
      ok: false,
      error: "End date and time cannot be in the future.",
    });
  });

  it("rejects nonexistent local times and formats an existing local value", () => {
    vi.stubEnv("TZ", "America/New_York");

    expect(
      validateCustomUsageWindow("2026-03-08T02:30", "2026-03-08T03:30", new Date()),
    ).toMatchObject({
      ok: false,
      error: "Enter valid local dates and times.",
    });
    expect(toLocalDateTimeValue(new Date("2026-09-18T12:37:00.000Z"))).toBe("2026-09-18T08:37");
  });
});
