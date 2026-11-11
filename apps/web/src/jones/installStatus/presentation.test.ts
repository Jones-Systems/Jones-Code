import { describe, expect, it } from "vite-plus/test";
import type { JonesUpdateObservation } from "@t3tools/client-runtime/jones/updates";
import { deviceLiveness, packageCheckLine, panelPresentation } from "./presentation";
import { lastHeardLabel, relativeAge, updateCheckFailedLabel } from "./copy";

const now = Date.parse("2026-11-10T12:00:00Z");
const minutes = (n: number) => n * 60_000;
const observation = (
  freshness: JonesUpdateObservation["freshness"],
  heardAt?: number,
): JonesUpdateObservation => ({
  state: null,
  freshness,
  ...(heardAt === undefined ? {} : { heardAt }),
});

describe("device liveness", () => {
  it("shows nothing while heard within 3 minutes, counting the long-poll heartbeat", () => {
    expect(
      deviceLiveness({ observation: observation("fresh", now - minutes(2)) }, undefined, now),
    ).toEqual({ kind: "live" });
    expect(deviceLiveness({ observation: observation("fresh") }, undefined, now)).toEqual({
      kind: "live",
    });
  });

  it("is quiet from 3 to 15 minutes with a relative last-heard time", () => {
    expect(
      deviceLiveness({ observation: observation("fresh", now - minutes(4)) }, undefined, now),
    ).toEqual({ kind: "quiet", text: "last heard 4m ago" });
    expect(
      deviceLiveness({ observation: observation("stale", now - minutes(15)) }, undefined, now),
    ).toEqual({ kind: "quiet", text: "last heard 15m ago" });
  });

  it("is unreachable after 15 minutes of silence or 3 failed refreshes in a row", () => {
    expect(
      deviceLiveness({ observation: observation("stale", now - minutes(16)) }, undefined, now),
    ).toEqual({ kind: "unreachable", text: "Can't reach · last heard 16m ago" });
    expect(
      deviceLiveness({ observation: observation("fresh", now - minutes(1)) }, { failures: 3 }, now),
    ).toEqual({ kind: "unreachable", text: "Can't reach · last heard 1m ago" });
    expect(deviceLiveness({ observation: observation("stale") }, undefined, now)).toEqual({
      kind: "unreachable",
      text: "Can't reach · not heard yet",
    });
  });

  it("uses the latest of heartbeat, fleet read and automatic refresh", () => {
    expect(
      deviceLiveness(
        {
          observation: observation("stale", now - minutes(20)),
          lastObservedAt: new Date(now - minutes(10)).toISOString(),
        },
        { failures: 0, lastSuccessAt: now - minutes(1) },
        now,
      ),
    ).toEqual({ kind: "live" });
  });

  it("never flags loading or unsupported hosts", () => {
    expect(deviceLiveness({ observation: observation("loading") }, { failures: 5 }, now)).toEqual({
      kind: "live",
    });
    expect(deviceLiveness({ observation: observation("unsupported") }, undefined, now)).toEqual({
      kind: "live",
    });
  });
});

describe("copy", () => {
  it("formats relative ages", () => {
    expect(relativeAge(now - 30_000, now)).toBe("just now");
    expect(relativeAge(now - minutes(12), now)).toBe("12m ago");
    expect(relativeAge(now - minutes(125), now)).toBe("2h ago");
    expect(lastHeardLabel(undefined, now)).toBe("not heard yet");
    expect(updateCheckFailedLabel(new Date(now - minutes(12)).toISOString(), now)).toBe(
      "Update check failed 12m ago",
    );
    expect(updateCheckFailedLabel(undefined, now)).toBe("Update check failed");
  });
});

describe("package check line", () => {
  type Row = NonNullable<ReturnType<typeof panelPresentation>>["rows"][number];
  const failure = {
    kind: "failure" as const,
    text: "The update check failed. Check the connection or authentication.",
    checkedAt: new Date(now - minutes(12)).toISOString(),
    bounded: false,
  };
  const row = (patch: Partial<Row>) =>
    ({
      phase: "not-updated",
      targetBuildRunning: false,
      status: { check: failure },
      ...patch,
    }) as Row;
  const idle = { observation: observation("fresh") };

  it("dates a failed check when nothing is active or staged", () => {
    expect(packageCheckLine(row({}), idle, false, now)).toBe("Update check failed 12m ago");
  });

  it("hides the check while work is active, a build is staged, or the target already runs", () => {
    expect(packageCheckLine(row({}), idle, true, now)).toBeUndefined();
    expect(packageCheckLine(row({ phase: "staged" }), idle, false, now)).toBeUndefined();
    expect(
      packageCheckLine(
        row({}),
        {
          observation: {
            freshness: "fresh",
            state: {
              source: "jones-actions",
              channel: "jones-main",
              phase: "staged",
              stagedHandle: "stage",
              capability: { check: true, download: true, install: true },
            },
          },
        },
        false,
        now,
      ),
    ).toBeUndefined();
    expect(packageCheckLine(row({ targetBuildRunning: true }), idle, false, now)).toBeUndefined();
    expect(packageCheckLine(row({ phase: "updated-elsewhere" }), idle, false, now)).toBeUndefined();
  });

  it("keeps other check results undated", () => {
    expect(
      packageCheckLine(
        row({
          status: {
            check: { ...failure, kind: "available", text: "A qualified package is available." },
          } as Row["status"],
        }),
        idle,
        false,
        now,
      ),
    ).toBe("A qualified package is available.");
  });
});
