import { describe, expect, it } from "vite-plus/test";
import { jonesBuildLabel, jonesBuildLabels, parseJonesBuildStartedAt } from "./buildTime.ts";

const version = "0.0.45-preview.20261021.12345678.2";
const ref = (startedAt?: string) => ({ version, startedAt });

describe("Jones build time labels", () => {
  it.each([
    ["2026-10-20T21:13:43Z", "1020·1713", "EDT"],
    ["2026-01-15T17:05:00Z", "0115·1205", "EST"],
    ["2026-03-08T06:59:59Z", "0308·0159", "EST"],
    ["2026-03-08T07:00:00Z", "0308·0300", "EDT"],
    ["2026-11-01T05:30:00Z", "1101·0130", "EDT"],
    ["2026-11-01T06:30:00Z", "1101·0130", "EST"],
    ["2027-01-01T04:59:00Z", "1231·2359", "EST"],
    ["2027-01-01T05:00:00Z", "0101·0000", "EST"],
    ["2028-02-29T15:00:00Z", "0229·1000", "EST"],
    ["2026-10-21T02:10:00Z", "1020·2210", "EDT"],
  ])("formats %s in Eastern time", (startedAt, text, zone) => {
    const label = jonesBuildLabel(ref(startedAt));
    expect(label.text).toBe(text);
    expect(label.timed).toBe(true);
    expect(label.detail).toContain(` ${zone} (${startedAt})`);
  });
  it("provides full time detail and a spoken label", () => {
    const build = ref("2026-10-20T21:13:43Z");
    expect(jonesBuildLabel(build, "day")).toEqual({
      text: "20·1713",
      timed: true,
      spoken: "Built October 20, 17:13 Eastern",
      detail: "Built Tue, Oct 20, 2026, 17:13:43 EDT (2026-10-20T21:13:43Z)",
    });
    expect(jonesBuildLabel(ref("2027-01-01T04:59:00Z")).detail).toContain("2026");
  });
  it.each([
    undefined,
    null,
    "0",
    "",
    "2026-02-29T12:00:00Z",
    "2026-04-31T12:00:00Z",
    "2026-13-01T12:00:00Z",
    "2026-10-20T24:00:00Z",
    "2026-10-20T21:13:60Z",
    "2026-10-20T21:13:43+00:00",
    "2026-10-20T21:13Z",
    "2026-10-20T21:13:43.1234Z",
  ])("rejects invalid time %s without inventing one", (value) => {
    expect(parseJonesBuildStartedAt(value)).toBeUndefined();
    expect(
      jonesBuildLabel({ version, startedAt: typeof value === "string" ? value : undefined }),
    ).toEqual({
      text: version,
      timed: false,
      spoken: "Build time not recorded",
      detail: "Build time not recorded",
    });
  });
  it.each([
    "2028-02-29T12:00:00Z",
    "2026-10-20T21:13:43.1Z",
    "2026-10-20T21:13:43.12Z",
    "2026-10-20T21:13:43.123Z",
  ])("preserves valid timestamps exactly", (value) =>
    expect(parseJonesBuildStartedAt(value)).toBe(value),
  );
  it("uses only a valid source SHA for the missing-time fallback", () => {
    expect(jonesBuildLabel({ version, sourceSha: "a".repeat(40) }).text).toBe("aaaaaaa");
    expect(jonesBuildLabel({ version, sourceSha: "unknown" }).text).toBe(version);
  });
  it("promotes day collisions to month-day", () => {
    expect(
      jonesBuildLabels(
        [
          { version: "a", startedAt: "2026-09-20T21:13:43Z" },
          { version: "b", startedAt: "2026-10-20T21:13:43Z" },
        ],
        "day",
      ).map((label) => label.text),
    ).toEqual(["0920·1713", "1020·1713"]);
  });
  it("distinguishes repeated fall-back wall times by zone", () => {
    expect(
      jonesBuildLabels([
        { version: "a", startedAt: "2026-11-01T05:30:00Z" },
        { version: "b", startedAt: "2026-11-01T06:30:00Z" },
      ]).map((label) => label.text),
    ).toEqual(["1101·0130 EDT", "1101·0130 EST"]);
  });
  it("distinguishes same-minute builds by seconds without changing identity", () => {
    const builds = [
      { version: "a", startedAt: "2026-10-20T21:13:43Z" },
      { version: "b", startedAt: "2026-10-20T21:13:47Z" },
    ];
    const before = JSON.stringify(builds);
    expect(jonesBuildLabels(builds).map((label) => label.text)).toEqual([
      "1020·1713:43",
      "1020·1713:47",
    ]);
    expect(JSON.stringify(builds)).toBe(before);
    expect(
      jonesBuildLabels([builds[0]!, { ...builds[1]!, version: "a" }]).map((label) => label.text),
    ).toEqual(["1020·1713", "1020·1713"]);
  });
  it("keeps distinct known sources when an unknown source appears first", () => {
    const startedAt = "2026-10-20T21:13:43Z";
    const builds = [
      { version, startedAt },
      { version, startedAt, sourceSha: "a".repeat(40) },
      { version, startedAt, sourceSha: "b".repeat(40) },
    ];
    const labels = jonesBuildLabels(builds);
    expect(labels[1]!.text).not.toBe(labels[2]!.text);
    expect(labels[1]!.text).toContain("aaaaaaa");
    expect(labels[2]!.text).toContain("bbbbbbb");
  });
  it("resolves source prefixes and version collisions using full identity", () => {
    const startedAt = "2026-10-20T21:13:43Z";
    expect(
      jonesBuildLabels([
        { version: "a", startedAt },
        { version: "b", startedAt },
      ]).map((label) => label.text),
    ).toEqual(["a", "b"]);
    expect(
      jonesBuildLabels([
        { version, startedAt, sourceSha: "a".repeat(40) },
        { version, startedAt, sourceSha: "b".repeat(40) },
      ]).map((label) => label.text),
    ).toEqual(["1020·1713:43· aaaaaaa", "1020·1713:43· bbbbbbb"]);
    const a = "a".repeat(40),
      b = "a".repeat(39) + "b";
    expect(
      jonesBuildLabels([
        { version, sourceSha: a },
        { version, sourceSha: b },
      ]).map((label) => label.text),
    ).toEqual([`${version}· ${a}`, `${version}· ${b}`]);
  });
});
