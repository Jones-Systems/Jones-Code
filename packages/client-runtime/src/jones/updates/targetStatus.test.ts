import { expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { jonesCheckOutcome, jonesTargetStatus } from "./targetStatus.ts";

const env = EnvironmentId.make("status-host"),
  source = "a".repeat(40);
const id = "11111111-1111-4111-8111-111111111111";
const state: JonesUpdateState = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "downloading",
  environmentId: env,
  capability: { check: true, download: true, install: true, operations: 1 },
  operation: {
    protocol: 1,
    operationId: id,
    environmentId: env,
    action: { kind: "download-exact", targetSource: source },
    expectedInstalledSource: "b".repeat(40),
    phase: "running",
    updatedAt: "2026-10-27T05:00:00Z",
    correlation: { planId: id, deviceKey: env },
  },
};

it("keeps held/unknown receipts and correlation visible while revoking action authority on stale or downgraded capability", () => {
  expect(
    jonesTargetStatus({ state, freshness: "fresh" }, { canOperate: true, planId: id }),
  ).toMatchObject({
    operation: { operationId: id, planCorrelated: true },
    actions: { stop: true },
  });
  for (const observed of [
    { state, freshness: "stale" as const },
    {
      state: { ...state, capability: { check: true, download: true, install: true } },
      freshness: "fresh" as const,
    },
  ])
    expect(jonesTargetStatus(observed, { canOperate: true }).actions.stop).toBeUndefined();
  expect(
    jonesTargetStatus({ state, freshness: "fresh" }, { canOperate: true, capability: null }).actions
      .stop,
  ).toBeUndefined();
  for (const receipt of [
    { ...state.operation!, phase: "held" as const },
    { ...state.operation!, code: "launcher-unconfirmed" },
  ]) {
    const status = jonesTargetStatus(
      { state: { ...state, operation: receipt }, freshness: "fresh" },
      { canOperate: true },
    );
    expect(status.operation?.operationId).toBe(id);
    expect(status.actions.stop).toBeUndefined();
    expect(status.actions.download).toBeUndefined();
    expect(status.actions.install).toBeUndefined();
  }
});

it("distinguishes bounded discovery, pending qualification, failure and transport freshness without changing legacy phase", () => {
  const classifications = [
    "qualification-pending",
    "qualification-failed",
    "no-eligible-package-in-window",
    "incomplete-discovery",
    "failure",
  ] as const;
  const texts = classifications.map(
    (kind) =>
      jonesTargetStatus({
        state: {
          ...state,
          phase: "blocked",
          checkClassification: {
            kind,
            checkedAt: "2026-10-27T05:00:00Z",
            discovery: { termination: "window-ended", bounded: true, unresolvedLimits: [] },
          },
        },
        freshness: "fresh",
      }).check?.text,
  );
  expect(new Set(texts).size).toBe(classifications.length);
  expect(texts[2]).toContain("search window");
  expect(texts.join(" ")).not.toContain("exhaustive");
  expect(state.phase).toBe("downloading");
});

it.each(["available", "downloading", "staged"] as const)(
  "shows a confirmed stopped receipt instead of lagging %s package status",
  (phase) => {
    const receipt = {
      ...state.operation!,
      phase: "stopped" as const,
      stop: {
        stopId: "22222222-2222-4222-8222-222222222222",
        requestedAt: state.operation!.updatedAt,
        result: "stopped" as const,
      },
    };
    const observed = { ...state, phase, operation: receipt };
    const status = jonesTargetStatus({ state: observed, freshness: "fresh" }, { canOperate: true });
    expect(status.headline).toBe("Download stopped");
    expect(status.operation).toMatchObject({
      operationId: id,
      phase: "stopped",
      held: false,
      unknown: false,
    });
    expect(status.actions.stop).toBeUndefined();
    expect(observed.phase).toBe(phase);
    // A terminal phase with unconfirmed native effects remains held by its exact outcome.
    const unknown = jonesTargetStatus(
      {
        state: { ...observed, operation: { ...receipt, code: "launcher-unconfirmed" } },
        freshness: "fresh",
      },
      { canOperate: true },
    );
    expect(unknown.headline).toBe("Not confirmed");
    expect(unknown.actions.download).toBeUndefined();
    expect(unknown.actions.install).toBeUndefined();
  },
);

it("removes both install paths only for already-running staged builds", () => {
  const { operation: _operation, ...idle } = state;
  const staged: JonesUpdateState = {
    ...idle,
    phase: "staged",
    currentVersion: "old",
    stagedHandle: "stage",
    installedSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "b".repeat(40),
      tree: "c".repeat(40),
    },
    provenance: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: "c".repeat(40),
      artifactId: 1,
      workflow: "synthetic",
      runId: 1,
      runAttempt: 1,
      artifactDigest: "d".repeat(64),
      platform: "linux",
      architecture: "x64",
      version: "new",
    },
  };
  for (const targetSource of [undefined, source]) {
    const options = { canOperate: true, ...(targetSource === undefined ? {} : { targetSource }) };
    expect(jonesTargetStatus({ state: staged, freshness: "fresh" }, options).actions.install).toBe(
      true,
    );
    const status = jonesTargetStatus(
      { state: { ...staged, stagedAlreadyRunning: true }, freshness: "fresh" },
      options,
    );
    expect(status.actions.install).toBeUndefined();
    expect(status.actions.reasons).toContain(
      "The downloaded build is already running on this host. Nothing to install.",
    );
  }
});

it("dates failed checks by their own receipt, clears stale generic failure after success, and retains classifications", () => {
  const failure = { kind: "failure" as const, checkedAt: "2026-01-10T15:00:00Z" };
  const failed = {
    outcome: "failed" as const,
    at: "2026-07-10T15:05:00Z",
    reason: "Do not repeat this host reason",
  };
  for (const checkClassification of [
    undefined,
    failure,
    { kind: "available" as const, checkedAt: failure.checkedAt },
  ]) {
    const observed = {
      ...state,
      checkedAt: "2099-01-01T00:00:00Z",
      ...(checkClassification === undefined ? {} : { checkClassification }),
      lastCheck: failed,
    };
    expect(jonesCheckOutcome(observed)).toMatchObject({
      kind: "failure",
      checkedAt: failed.at,
      text: "The update check failed. Check the connection or authentication. Checked Jul 10, 2026, 11:05 AM EDT.",
    });
    expect(jonesCheckOutcome(observed)?.text).not.toContain(failed.reason);
  }
  expect(
    jonesCheckOutcome({
      ...state,
      checkClassification: failure,
      lastCheck: { outcome: "succeeded", at: failed.at },
    }),
  ).toBeUndefined();
  expect(jonesCheckOutcome({ ...state, checkClassification: failure })?.text).toBe(
    "The update check failed. Check the connection or authentication. Checked Jan 10, 2026, 10:00 AM EST.",
  );
  for (const kind of [
    "available",
    "target-already-running",
    "qualification-pending",
    "qualification-failed",
    "no-eligible-package-in-window",
    "incomplete-discovery",
  ] as const) {
    for (const lastCheck of [undefined, { outcome: "succeeded" as const, at: failed.at }]) {
      const observed = {
        ...state,
        ...(lastCheck === undefined ? {} : { lastCheck }),
        checkClassification: { ...failure, kind },
      };
      expect(jonesCheckOutcome(observed)).toMatchObject({ kind, checkedAt: failure.checkedAt });
      expect(jonesCheckOutcome(observed)?.text).toContain("Jan 10, 2026, 10:00 AM EST");
    }
  }
  expect(jonesCheckOutcome(null)).toBeUndefined();
  expect(jonesCheckOutcome(state)).toBeUndefined();
  expect(
    jonesCheckOutcome({ ...state, lastCheck: { outcome: "succeeded", at: failed.at } }),
  ).toBeUndefined();
  for (const lastCheck of [undefined, { ...failed, at: "invalid" }]) {
    expect(
      jonesCheckOutcome({
        ...state,
        checkedAt: failed.at,
        ...(lastCheck === undefined ? {} : { lastCheck }),
        checkClassification: { ...failure, checkedAt: "invalid" },
      })?.text,
    ).toBe("The update check failed. Check the connection or authentication.");
  }
  expect(
    jonesTargetStatus({ state: { ...state, checkClassification: failure }, freshness: "fresh" })
      .check?.text,
  ).toBe("The update check failed. Check the connection or authentication.");
});
