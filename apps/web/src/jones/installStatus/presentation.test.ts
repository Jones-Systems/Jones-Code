import { describe, expect, it } from "vite-plus/test";
import type { JonesUpdateObservation } from "@t3tools/client-runtime/jones/updates";
import { installProgressView } from "@t3tools/client-runtime/jones/install-plan";
import type { JonesUpdateProgress } from "@t3tools/contracts/jones/jonesUpdates";
import {
  deviceAttention,
  deviceLiveness,
  packageCheckLine,
  panelPresentation,
} from "./presentation";
import {
  formatElapsed,
  heardAgoLabel,
  lastHeardLabel,
  relativeAge,
  updateCheckFailedLabel,
} from "./copy";

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
      deviceLiveness({ observation: observation("stale", now - minutes(4)) }, { failures: 3 }, now),
    ).toEqual({ kind: "unreachable", text: "Can't reach · last heard 4m ago" });
    expect(
      deviceLiveness(
        { observation: observation("stale"), lastObservedAt: new Date(now).toISOString() },
        { failures: 3 },
        now,
      ),
    ).toEqual({ kind: "unreachable", text: "Can't reach · last heard just now" });
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

  it("ignores refresh timeouts behind the operation queue while the status stream is fresh", () => {
    // A tick's operation read shares the host's serial operation queue. Three 10-second timeouts
    // there must not mark a host unreachable while its status stream heard it under 3 minutes ago.
    expect(
      deviceLiveness(
        { observation: observation("fresh", now - minutes(2)) },
        { failures: 3, lastSuccessAt: now - minutes(20) },
        now,
      ),
    ).toEqual({ kind: "live" });
    expect(
      deviceLiveness({ observation: observation("fresh", now - minutes(3)) }, { failures: 3 }, now),
    ).toEqual({ kind: "unreachable", text: "Can't reach · last heard 3m ago" });
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

  it("formats step timers and the detail heard line", () => {
    expect(formatElapsed(0)).toBe("0:00");
    expect(formatElapsed(38_400)).toBe("0:38");
    expect(formatElapsed(102_000)).toBe("1:42");
    expect(formatElapsed(minutes(59) + 59_999)).toBe("59:59");
    expect(formatElapsed(minutes(60))).toBe("1:00:00");
    expect(formatElapsed(minutes(125) + 7_000)).toBe("2:05:07");
    expect(formatElapsed(-5_000)).toBe("0:00");
    expect(heardAgoLabel(now - 12_400, now)).toBe("Last heard 12s ago");
    expect(heardAgoLabel(now - minutes(4), now)).toBe("Last heard 4m ago");
    expect(heardAgoLabel(undefined, now)).toBe("Not heard yet");
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

describe("package check line with a host check record", () => {
  type Row = NonNullable<ReturnType<typeof panelPresentation>>["rows"][number];
  const row = (check?: { kind: string; checkedAt?: string }) =>
    ({
      phase: "not-updated",
      targetBuildRunning: false,
      status:
        check === undefined
          ? {}
          : { check: { text: "A qualified package is available.", bounded: false, ...check } },
    }) as Row;
  const host = (lastCheck: { at: string; outcome: "succeeded" | "failed" }) => ({
    observation: {
      freshness: "fresh" as const,
      state: {
        source: "jones-actions" as const,
        channel: "jones-main" as const,
        phase: "available" as const,
        capability: { check: true, download: true, install: true },
        lastCheck,
      },
    },
  });

  it("dates a failed check from lastCheck, falling back to the classification time", () => {
    const failedAt = new Date(now - minutes(12)).toISOString();
    expect(
      packageCheckLine(
        row({ kind: "failure", checkedAt: new Date(now - minutes(40)).toISOString() }),
        host({ at: failedAt, outcome: "failed" }),
        false,
        now,
      ),
    ).toBe("Update check failed 12m ago");
    expect(packageCheckLine(row(), host({ at: failedAt, outcome: "failed" }), false, now)).toBe(
      "Update check failed 12m ago",
    );
    expect(
      packageCheckLine(
        row({ kind: "failure", checkedAt: new Date(now - minutes(40)).toISOString() }),
        { observation: observation("fresh") },
        false,
        now,
      ),
    ).toBe("Update check failed 40m ago");
  });

  it("never reports a failure the host's last check has since cleared", () => {
    const at = new Date(now - minutes(1)).toISOString();
    expect(
      packageCheckLine(row({ kind: "failure" }), host({ at, outcome: "succeeded" }), false, now),
    ).toBeUndefined();
    expect(
      packageCheckLine(row({ kind: "available" }), host({ at, outcome: "succeeded" }), false, now),
    ).toBe("A qualified package is available.");
  });
});

describe("device attention", () => {
  const iso = (ms: number) => new Date(ms).toISOString();
  const record = (patch: Partial<JonesUpdateProgress>): JonesUpdateProgress => ({
    schema: 1,
    kind: "install",
    operationId: "op",
    phase: "prep",
    stepIndex: 3,
    stepCount: 7,
    stepKey: "startup-readiness",
    stepLabel: "Check startup readiness",
    stepStartedAt: iso(now - minutes(1)),
    startedAt: iso(now - minutes(2)),
    updatedAt: iso(now - minutes(1)),
    reportedAt: iso(now),
    ...patch,
  });
  const view = (patch: Partial<JonesUpdateProgress>, receivedAt = now) =>
    installProgressView(record(patch), receivedAt);
  const live = { kind: "live" } as const;
  const unreachable = { kind: "unreachable", text: "Can't reach · last heard 18m ago" } as const;

  it("is quiet for running work that progresses", () => {
    expect(deviceAttention(view({}), live, now)).toEqual({ kind: "none" });
    expect(deviceAttention(undefined, live, now)).toEqual({ kind: "none" });
  });

  it("warns when a reachable device has made no progress for 3 minutes mid-step", () => {
    expect(deviceAttention(view({ updatedAt: iso(now - minutes(3)) }), live, now)).toEqual({
      kind: "stalled",
      text: "No progress for 3m",
    });
    expect(
      deviceAttention(view({ phase: "done", updatedAt: iso(now - minutes(30)) }), live, now),
    ).toEqual({ kind: "none" });
  });

  it("measures staleness on the host clock", () => {
    // The host runs 10 minutes behind; its updatedAt two host-minutes ago is not stale.
    const skewed = view(
      { updatedAt: iso(now - minutes(12)), reportedAt: iso(now - minutes(10)) },
      now,
    );
    expect(skewed.skewMs).toBe(-minutes(10));
    expect(deviceAttention(skewed, live, now)).toEqual({ kind: "none" });
  });

  it("expects silence during a restart until 10 minutes, then says so", () => {
    const restart = (startedMinutesAgo: number) =>
      view({
        phase: "install",
        stepIndex: 6,
        stepKey: "restart",
        stepLabel: "Restart",
        stepStartedAt: iso(now - minutes(startedMinutesAgo)),
        updatedAt: iso(now - minutes(startedMinutesAgo)),
      });
    expect(deviceAttention(restart(4), unreachable, now)).toEqual({ kind: "none" });
    expect(deviceAttention(restart(9), { kind: "quiet", text: "last heard 9m ago" }, now)).toEqual({
      kind: "none",
    });
    expect(deviceAttention(restart(11), unreachable, now)).toEqual({
      kind: "restart-silent",
      text: "No word since restart · 11m",
    });
  });

  it("passes other silence through unchanged", () => {
    expect(deviceAttention(view({}), unreachable, now)).toEqual(unreachable);
    expect(deviceAttention(undefined, { kind: "quiet", text: "last heard 4m ago" }, now)).toEqual({
      kind: "quiet",
      text: "last heard 4m ago",
    });
  });
});
