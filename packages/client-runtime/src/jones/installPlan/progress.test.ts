// @effect-diagnostics globalDate:off - Fixed timestamps exercise host-clock skew and step durations.
import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateProgress, JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { startPlan } from "./model.ts";
import { installPlanPresentation, planSummary } from "./presentation.ts";
import {
  hostElapsedMs,
  installProgressView,
  progressClockSkewMs,
  progressForOperation,
} from "./progress.ts";

const at = (seconds: number) => new Date(Date.parse("2026-11-10T12:00:00Z") + seconds * 1000);
const iso = (seconds: number) => at(seconds).toISOString();
const operationId = "33333333-3333-4333-8333-333333333333";
const INSTALL_STEPS = [
  ["mark-threads", "Mark running threads"],
  ["verify-runtime", "Verify runtime"],
  ["startup-readiness", "Check startup readiness"],
  ["capture-continuity", "Capture continuity"],
  ["launcher-handoff", "Hand off to launcher"],
  ["restart", "Restart"],
  ["confirm-restart", "Confirm restart"],
] as const;

function progress(patch: Partial<JonesUpdateProgress> = {}): JonesUpdateProgress {
  const stepIndex = patch.stepIndex ?? 3;
  return {
    schema: 1,
    kind: "install",
    operationId,
    phase: "prep",
    stepIndex,
    stepCount: 7,
    stepKey: INSTALL_STEPS[stepIndex - 1]![0],
    stepLabel: INSTALL_STEPS[stepIndex - 1]![1],
    stepStartedAt: iso(60),
    startedAt: iso(0),
    updatedAt: iso(60),
    reportedAt: iso(102),
    steps: INSTALL_STEPS.map(([key, label], offset) => ({
      key,
      label,
      ...(offset + 1 < stepIndex
        ? { startedAt: iso(offset * 20), endedAt: iso(offset * 20 + 20) }
        : offset + 1 === stepIndex
          ? { startedAt: iso(60) }
          : {}),
    })),
    ...patch,
  };
}

describe("progress for the ledger's operation", () => {
  it("shows progress only for the latest operation, or any progress without a plan", () => {
    const record = progress();
    expect(progressForOperation(record, operationId, false)).toBe(record);
    expect(progressForOperation(record, "44444444-4444-4444-8444-444444444444", false)).toBe(
      undefined,
    );
    expect(progressForOperation(record, undefined, false)).toBe(undefined);
    const { operationId: _omitted, ...anonymous } = record;
    expect(progressForOperation(anonymous, operationId, false)).toBe(undefined);
    expect(progressForOperation(record, undefined, true)).toBe(record);
    expect(progressForOperation(undefined, operationId, true)).toBe(undefined);
  });
});

describe("clock skew", () => {
  it("measures host time from the client clock corrected by reportedAt", () => {
    // The host clock runs 5 minutes ahead: it served the record at 12:01:42 host time, and the
    // client received it at 11:56:42 client time.
    const record = progress({ reportedAt: iso(102) });
    const receivedAt = at(102 - 300).getTime();
    const skew = progressClockSkewMs(record, receivedAt);
    expect(skew).toBe(300_000);
    // At receipt the current step (started 12:01:00 host) has run 42 seconds.
    expect(hostElapsedMs(Date.parse(record.stepStartedAt), receivedAt, skew)).toBe(42_000);
    expect(hostElapsedMs(Date.parse(record.stepStartedAt), receivedAt + 60_000, skew)).toBe(
      102_000,
    );
    // Without a receive time the client assumes no skew, and never shows negative time.
    expect(progressClockSkewMs(record, undefined)).toBe(0);
    expect(hostElapsedMs(Date.parse(record.stepStartedAt), receivedAt, 0)).toBe(0);
  });
});

describe("progress view", () => {
  const text = (patch: Partial<JonesUpdateProgress>) =>
    installProgressView(progress(patch), 0).text;

  it("names each phase on one line", () => {
    expect(text({ phase: "prep", stepIndex: 3 })).toBe("Preparing 3 of 7");
    expect(
      text({
        kind: "download",
        phase: "download",
        stepIndex: 1,
        stepCount: 2,
        stepKey: "download",
        stepLabel: "Download",
        steps: [],
      }),
    ).toBe("Downloading");
    expect(
      text({
        kind: "download",
        phase: "verify",
        stepIndex: 2,
        stepCount: 2,
        stepKey: "verify-stage",
      }),
    ).toBe("Checking download");
    expect(text({ phase: "install", stepIndex: 5 })).toBe("Installing");
    expect(text({ phase: "install", stepIndex: 6 })).toBe("Restarting");
    expect(text({ phase: "verify", stepIndex: 7 })).toBe("Confirming");
    expect(text({ phase: "done", stepIndex: 7 })).toBe("Updated · verified");
    expect(text({ kind: "download", phase: "done", stepIndex: 2, stepCount: 2 })).toBeUndefined();
    expect(text({ phase: "failed", stepIndex: 3 })).toBe("Install failed at step 3 of 7");
    expect(text({ kind: "download", phase: "failed", stepIndex: 1, stepCount: 2 })).toBe(
      "Download failed at step 1 of 2",
    );
  });

  it("lists done steps with durations, the current step and pending steps", () => {
    const view = installProgressView(progress({ stepIndex: 3 }), at(102).getTime());
    expect(view).toMatchObject({ active: true, restarting: false, skewMs: 0 });
    expect(view.steps.map((s) => [s.key, s.status, s.durationMs])).toEqual([
      ["mark-threads", "done", 20_000],
      ["verify-runtime", "done", 20_000],
      ["startup-readiness", "current", undefined],
      ["capture-continuity", "pending", undefined],
      ["launcher-handoff", "pending", undefined],
      ["restart", "pending", undefined],
      ["confirm-restart", "pending", undefined],
    ]);
  });

  it("marks the failed step and keeps the error", () => {
    const view = installProgressView(
      progress({
        phase: "failed",
        stepIndex: 3,
        error: { code: "startup-not-ready", message: "Startup readiness timed out." },
      }),
      undefined,
    );
    expect(view.active).toBe(false);
    expect(view.steps.map((s) => s.status)).toEqual([
      "done",
      "done",
      "failed",
      "pending",
      "pending",
      "pending",
      "pending",
    ]);
    expect(view.error).toEqual({
      code: "startup-not-ready",
      message: "Startup readiness timed out.",
    });
  });

  it("flags the restart step and finishes every step when done", () => {
    expect(installProgressView(progress({ phase: "install", stepIndex: 6 }), 0).restarting).toBe(
      true,
    );
    const done = installProgressView(progress({ phase: "done", stepIndex: 7 }), 0);
    expect(done.active).toBe(false);
    expect(done.steps.every((s) => s.status === "done")).toBe(true);
  });
});

describe("plan rows with progress", () => {
  const source = "a".repeat(40),
    tree = "b".repeat(40),
    host = EnvironmentId.make("vps"),
    planId = "11111111-1111-4111-8111-111111111111",
    now = iso(0);
  const base = startPlan({
    planId,
    now,
    origin: "update-button",
    laptopEnvironmentId: EnvironmentId.make("laptop"),
    select: [{ environmentId: host, label: "VPS", route: "operations" }],
    target: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: tree,
      version: "new",
    },
    from: { version: "old" },
  });
  const gate = {
    openedAt: now,
    transactionId: "tx",
    activeGeneration: "gen",
    basis: "plan-install" as const,
    activeSource: source,
    activeTree: tree,
  };
  const receipt: JonesOperationReceipt = {
    protocol: 1,
    operationId,
    environmentId: host,
    expectedInstalledSource: "c".repeat(40),
    action: { kind: "install", stagedHandle: "stage", currentVersion: "old", targetSource: source },
    phase: "running",
    acceptedAt: now,
    jobId: operationId,
    updatedAt: now,
  };
  const state = (
    operation = receipt,
    step: "download" | "install" = "install",
  ): JonesInstallPlanState => ({
    schema: 1,
    current: {
      ...base,
      laptop: { ...base.laptop, gate },
      devices: [
        {
          ...base.devices[0]!,
          operations: [
            {
              operationId: operation.operationId,
              step,
              lease: { claimant: "fixture", reservedAt: now, leaseUntil: now },
              request: { kind: "operation-start", input: operation },
              receipt: operation,
            },
          ],
        },
      ],
    },
    last: null,
    laptop: { observedAt: now, activationBlocked: false },
    view: { complete: false },
  });
  const live = (record?: JonesUpdateProgress): JonesUpdateState => ({
    source: "jones-actions",
    channel: "jones-main",
    phase: "installing",
    environmentId: host,
    capability: { check: true, download: true, install: true, exactSource: true, operations: 1 },
    operation: receipt,
    ...(record === undefined ? {} : { progress: record }),
  });
  const row = (value: JonesUpdateState, native = state()) =>
    installPlanPresentation(native, {
      now: at(102).getTime(),
      devices: new Map([
        [
          host,
          {
            observation: { state: value, freshness: "fresh", heardAt: at(102).getTime() },
            canOperate: true,
          },
        ],
      ]),
    });

  it("maps matching host progress onto the row and the header", () => {
    const view = row(live(progress()));
    expect(view.rows[0]).toMatchObject({
      text: "Preparing 3 of 7",
      activeWork: "install",
      progress: { stepIndex: 3, active: true, skewMs: 0 },
    });
    expect(view.summary).toBe("1 installing · 0 of 1 done");
  });

  it("ignores progress from another operation and falls back to phase text", () => {
    const other = row(live(progress({ operationId: "44444444-4444-4444-8444-444444444444" })));
    expect(other.rows[0]!.progress).toBeUndefined();
    expect(other.rows[0]!.text).toBe("Installing · restarting");
    const old = row(live());
    expect(old.rows[0]!.progress).toBeUndefined();
    expect(old.rows[0]!.text).toBe("Installing · restarting");
  });

  it("never offers install for a staged build that is already running", () => {
    const staged: JonesOperationReceipt = {
      ...receipt,
      action: { kind: "download-exact", targetSource: source },
      phase: "completed",
      staged: { stagedHandle: "stage", sourceSha: source, sourceTree: tree, artifactId: 1 },
    };
    const native = state(staged, "download");
    const host_ = {
      ...live(),
      phase: "staged" as const,
      operation: staged,
      stagedHandle: "stage",
      currentVersion: "old",
    };
    expect(row(host_, native).rows[0]!.text).toBe("Downloaded · install when ready");
    const running = row({ ...host_, stagedAlreadyRunning: true }, native).rows[0]!;
    expect(running.text).toBe("Downloaded · already running this build");
    expect(running.text).not.toContain("install when ready");
    expect(running.actions.install).toBe(false);
  });

  it("summarizes installing, downloading and done counts", () => {
    expect(
      planSummary([
        { settled: false, activeWork: "install" },
        { settled: false, activeWork: "download" },
        { settled: true, activeWork: undefined },
      ]),
    ).toBe("1 installing · 1 downloading · 1 of 3 done");
    expect(planSummary([{ settled: true, activeWork: undefined }])).toBe("1 of 1 done");
  });
});
