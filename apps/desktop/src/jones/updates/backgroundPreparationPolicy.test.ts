import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  BACKGROUND_MAX_UNRECLAIMED,
  BACKGROUND_SETTLE_MS,
  decideBackgroundPreparation,
  type PreparationInput,
} from "./backgroundPreparationPolicy.ts";

const older = { source: "a".repeat(40), version: "0.0.44-preview.20261002.10.1" };
const newer = { source: "b".repeat(40), version: "0.0.44-preview.20261002.11.1" };
const now = DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-20T12:00:00Z"));
const iso = (time: number) => DateTime.formatIso(DateTime.makeUnsafe(time));

const input = (overrides: Partial<PreparationInput> = {}): PreparationInput => ({
  enabled: true,
  now,
  hold: undefined,
  busy: null,
  staged: undefined,
  latest: newer,
  latestSince: now - BACKGROUND_SETTLE_MS,
  declinedSource: undefined,
  retryAt: undefined,
  unreclaimed: 0,
  ...overrides,
});

describe("decideBackgroundPreparation", () => {
  it("does nothing while off, even with a settled newer build", () => {
    expect(decideBackgroundPreparation(input({ enabled: false }))).toEqual({
      action: "none",
      status: { enabled: false, phase: "off" },
    });
  });

  it("downloads a settled newer build into an empty slot", () => {
    expect(decideBackgroundPreparation(input())).toEqual({
      action: "download",
      target: newer,
      status: { enabled: true, phase: "preparing", targetVersion: newer.version },
    });
  });

  it("waits for a newer build to settle before acting", () => {
    expect(decideBackgroundPreparation(input({ latestSince: now - 1000 }))).toEqual({
      action: "none",
      status: {
        enabled: true,
        phase: "settling",
        targetVersion: newer.version,
        nextAt: iso(now - 1000 + BACKGROUND_SETTLE_MS),
      },
    });
  });

  it("replaces an unaccepted older stage with the latest build", () => {
    expect(decideBackgroundPreparation(input({ staged: older }))).toEqual({
      action: "replace",
      staged: older,
      target: newer,
      status: {
        enabled: true,
        phase: "preparing",
        targetVersion: newer.version,
        replacingVersion: older.version,
      },
    });
  });

  it("rests when the stage is already the latest build", () => {
    expect(decideBackgroundPreparation(input({ staged: newer }))).toEqual({
      action: "none",
      status: { enabled: true, phase: "ready", targetVersion: newer.version },
    });
    expect(decideBackgroundPreparation(input({ latest: undefined }))).toEqual({
      action: "none",
      status: { enabled: true, phase: "watching" },
    });
  });

  it("pins an accepted install's stage and names the waiting build", () => {
    expect(
      decideBackgroundPreparation(input({ staged: older, hold: { kind: "install" } })),
    ).toEqual({
      action: "none",
      status: { enabled: true, phase: "pinned", targetVersion: newer.version },
    });
  });

  it("reports blocked and unavailable holds with their reasons", () => {
    expect(
      decideBackgroundPreparation(input({ hold: { kind: "blocked", message: "Reconcile." } }))
        .status,
    ).toEqual({ enabled: true, phase: "blocked", reason: "Reconcile." });
    expect(decideBackgroundPreparation(input({ hold: { kind: "unavailable" } })).status).toEqual({
      enabled: true,
      phase: "unavailable",
    });
  });

  it("never starts work while any other action holds the controller", () => {
    expect(decideBackgroundPreparation(input({ busy: "download" }))).toEqual({
      action: "none",
      status: { enabled: true, phase: "preparing", targetVersion: newer.version },
    });
    expect(decideBackgroundPreparation(input({ busy: "check", staged: older })).action).toBe(
      "none",
    );
  });

  it("does not re-prepare a build the user discarded", () => {
    expect(decideBackgroundPreparation(input({ declinedSource: newer.source }))).toEqual({
      action: "none",
      status: {
        enabled: true,
        phase: "watching",
        reason: `You discarded ${newer.version}; a newer build will be prepared.`,
      },
    });
  });

  it("backs off a failed source until its retry time", () => {
    const retryAt = now + 60_000;
    expect(decideBackgroundPreparation(input({ retryAt }))).toEqual({
      action: "none",
      status: {
        enabled: true,
        phase: "backoff",
        targetVersion: newer.version,
        nextAt: iso(retryAt),
      },
    });
    expect(decideBackgroundPreparation(input({ retryAt: now })).action).toBe("download");
  });

  it("pauses replacement, not download, when replaced payloads accumulate", () => {
    const capped = input({ staged: older, unreclaimed: BACKGROUND_MAX_UNRECLAIMED });
    expect(decideBackgroundPreparation(capped)).toEqual({
      action: "none",
      status: {
        enabled: true,
        phase: "blocked",
        targetVersion: newer.version,
        reason: `${BACKGROUND_MAX_UNRECLAIMED} replaced downloads could not be removed, so replacing the download is paused. ${older.version} can still be installed.`,
      },
    });
    expect(decideBackgroundPreparation({ ...capped, staged: undefined }).action).toBe("download");
  });
});
