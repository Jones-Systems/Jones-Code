import { describe, expect, it } from "vite-plus/test";
import { formatRequestTime } from "./requestTime";

const normalize = (text: string) => text.replace(/[\u202f\u00a0]/gu, " ");
const format = (
  iso: string,
  now: string,
  full = false,
  mode: "12-hour" | "24-hour" | "locale" = "12-hour",
) => normalize(formatRequestTime(iso, mode, { nowMs: Date.parse(now), locale: "en-US", full }));

describe("Requests Eastern clock", () => {
  it.each([
    ["2026-03-08T06:59:59Z", "1:59 AM EST"],
    ["2026-03-08T07:00:00Z", "3:00 AM EDT"],
    ["2026-11-01T05:59:59Z", "1:59 AM EDT"],
    ["2026-11-01T06:00:00Z", "1:00 AM EST"],
  ])("formats the DST boundary %s", (iso, expected) => {
    expect(format(iso, iso)).toBe(expected);
  });
  it("compares Eastern days rather than UTC dates", () => {
    expect(format("2026-10-24T01:00:00Z", "2026-10-24T12:00:00Z")).toBe("yesterday at 9:00 PM EDT");
    expect(format("2026-10-23T23:00:00Z", "2026-10-24T01:00:00Z")).toBe("7:00 PM EDT");
    expect(format("2026-03-08T04:30:00Z", "2026-03-09T03:30:00Z")).toBe(
      "yesterday at 11:30 PM EST",
    );
  });
  it("dates earlier, future and other-year days and includes seconds in details", () => {
    const now = "2026-10-24T12:00:00Z";
    expect(format("2026-10-03T13:00:00Z", now)).toBe("10/3 9:00 AM EDT");
    expect(format("2025-12-30T14:00:00Z", now)).toBe("12/30/2025 9:00 AM EST");
    expect(format("2026-10-25T13:00:00Z", now)).toBe("10/25 9:00 AM EDT");
    expect(format("2027-01-01T14:00:00Z", now)).toBe("1/1/2027 9:00 AM EST");
    expect(format(now, now, true)).toBe("10/24/2026 8:00:00 AM EDT");
  });
  it("respects 24-hour and locale clocks while retaining the zone", () => {
    expect(format("2026-10-24T04:00:00Z", "2026-10-24T12:00:00Z", false, "24-hour")).toBe(
      "00:00 EDT",
    );
    expect(format("2026-10-24T12:00:00Z", "2026-10-24T12:00:00Z", false, "locale")).toBe(
      "8:00 AM EDT",
    );
    expect(formatRequestTime("invalid", "locale")).toBe("Invalid time");
  });
});
