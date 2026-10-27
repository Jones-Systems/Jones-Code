import { expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { jonesTargetStatus } from "./targetStatus.ts";

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
