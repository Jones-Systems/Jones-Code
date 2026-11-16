// @effect-diagnostics globalDate:off - Fixed timestamps and injected clocks exercise receipt, lease and review behavior.
import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlan,
  JonesInstallPlanDevice,
  JonesInstallPlanHostOperation,
} from "@t3tools/contracts/jones/installPlan";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import {
  applyEdit,
  canSupersede,
  deviceSettled,
  deviceState,
  isPlanComplete,
  laptopSettled,
  nextRunnerStep,
  selectability,
  startPlan,
  validateDirectiveEvidence,
  validateReceipt,
} from "./model.ts";

const now = "2026-10-27T05:00:00.000Z";
const env = EnvironmentId.make("host");
const laptop = EnvironmentId.make("laptop");
const id = "11111111-1111-4111-8111-111111111111";
const operationId = "22222222-2222-4222-8222-222222222222";
const source = "a".repeat(40);
const tree = "b".repeat(40);
const installed = "c".repeat(40);
const selection = { environmentId: env, label: "Host", route: "operations" as const };
const base = () =>
  startPlan({
    planId: id,
    origin: "update-button",
    target: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: tree,
      version: "new",
    },
    from: { version: "old", sourceSha: installed },
    laptopEnvironmentId: laptop,
    select: [selection],
    now,
  });
const opened = (): JonesInstallPlan => {
  const plan = base();
  return {
    ...plan,
    laptop: {
      ...plan.laptop,
      gate: {
        openedAt: now,
        basis: "plan-install",
        transactionId: "tx",
        activeGeneration: "generation",
        activeSource: source,
        activeTree: tree,
      },
    },
  };
};
const operation = (): JonesInstallPlanHostOperation => ({
  operationId,
  step: "download",
  lease: { claimant: "window:a", reservedAt: now, leaseUntil: "2026-10-27T05:02:00.000Z" },
  request: {
    kind: "operation-start",
    input: {
      operationId,
      environmentId: env,
      expectedInstalledSource: installed,
      action: { kind: "download-exact", targetSource: source },
      correlation: { planId: id, deviceKey: env },
    },
  },
});
const receipt = (phase: JonesOperationReceipt["phase"] = "accepted"): JonesOperationReceipt => {
  const request = operation().request;
  if (request.kind !== "operation-start") throw new Error("fixture");
  return { ...request.input, protocol: 1, phase, acceptedAt: now, updatedAt: now };
};
const state: JonesUpdateState = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "no-new",
  environmentId: env,
  capability: { check: true, download: true, install: true, exactSource: true, operations: 1 },
  installedSource: { repository: "Jones-Systems/Jones-Code", sha: installed, tree },
  currentVersion: "old",
};

describe("install plan model", () => {
  it("uses the current membership for completion and locks a device reserved while Edit was open", () => {
    const plan = opened();
    expect(isPlanComplete(plan)).toBe(false);
    const removed = applyEdit(plan, { expectedRevision: 0, select: [], deselect: [env] }, now);
    expect(isPlanComplete(removed.plan)).toBe(true);
    expect(removed.applied[0]?.result).toBe("removed");
    const added = applyEdit(
      removed.plan,
      { expectedRevision: 1, select: [selection], deselect: [] },
      now,
    );
    expect(isPlanComplete(added.plan)).toBe(false);
    const raced = { ...plan, devices: [{ ...plan.devices[0]!, operations: [operation()] }] };
    expect(
      applyEdit(raced, { expectedRevision: 0, select: [], deselect: [env] }, now).applied[0]
        ?.result,
    ).toBe("locked-started");
    expect(
      applyEdit(added.plan, { expectedRevision: 0, select: [], deselect: [env] }, now).refused,
    ).toBe("stale-revision");
    expect(plan.devices[0]?.operations).toEqual([]);
  });

  it("settles Laptop-only from native gate and leaves staged, held and unknown work open", () => {
    const plan = opened();
    expect(isPlanComplete({ ...plan, devices: [] })).toBe(true);
    for (const phase of ["accepted", "running", "stop-requested", "held", "handed-off"] as const) {
      const device = {
        ...plan.devices[0]!,
        operations: [{ ...operation(), receipt: receipt(phase) }],
      };
      expect(deviceSettled(plan, device)).toBe(false);
      expect(isPlanComplete({ ...plan, devices: [device] })).toBe(false);
    }
    const staged = {
      ...receipt("completed"),
      staged: { stagedHandle: "stage", sourceSha: source, sourceTree: tree, artifactId: 1 },
    };
    expect(
      deviceState(plan, { ...plan.devices[0]!, operations: [{ ...operation(), receipt: staged }] }),
    ).toBe("staged");
    const unknown = { ...plan.devices[0]!, operations: [operation()] };
    expect(deviceState(plan, unknown, Date.parse(now) + 120001)).toBe("unknown");
    expect(deviceSettled(plan, unknown)).toBe(false);
    const genericBlocked = { ...state, phase: "blocked" as const };
    expect(
      nextRunnerStep(
        plan,
        unknown,
        { state: genericBlocked, freshness: "fresh" },
        null,
        Date.parse(now) + 120001,
      ).kind,
    ).toBe("claim-fence");
  });

  it("does not use an unchanged source, new revision, handoff or absence to settle unknown", () => {
    const plan = opened();
    const device = { ...plan.devices[0]!, operations: [operation()] };
    expect(
      nextRunnerStep(
        { ...plan, revision: 99 },
        device,
        {
          state: {
            ...state,
            revision: 0,
            installedSource: { ...state.installedSource!, sha: source },
          },
          freshness: "fresh",
        },
        null,
        Date.parse(now) + 120001,
      ),
    ).toEqual({ kind: "claim-fence", operationId });
    expect(deviceSettled(plan, device)).toBe(false);
    expect(
      deviceSettled(plan, {
        ...device,
        operations: [{ ...operation(), receipt: receipt("fenced") }],
      }),
    ).toBe(false);
  });

  it("requires request binding, monotone transitions, stable stage and terminal immutability", () => {
    const request = operation().request;
    if (request.kind !== "operation-start") throw new Error("fixture");
    const accepted = receipt();
    const running = { ...accepted, phase: "running" as const, jobId: operationId };
    expect(validateReceipt(request.input, accepted)).toBe(true);
    expect(validateReceipt(accepted, running)).toBe(true);
    for (const forged of [
      { ...running, environmentId: laptop },
      { ...running, correlation: { planId: operationId, deviceKey: env } },
      { ...running, expectedInstalledSource: source },
      { ...running, action: { kind: "download-exact" as const, targetSource: tree } },
    ])
      expect(validateReceipt(accepted, forged)).toBe(false);
    expect(validateReceipt(running, accepted)).toBe(false);
    const { acceptedAt: _accepted, ...withoutAcceptance } = running;
    expect(validateReceipt(running, { ...withoutAcceptance, phase: "fenced" })).toBe(false);
    expect(validateReceipt(running, { ...running, phase: "completed" })).toBe(false);
    const completed: JonesOperationReceipt = {
      ...running,
      phase: "completed",
      staged: { stagedHandle: "stage", sourceSha: source, sourceTree: tree, artifactId: 1 },
    };
    expect(validateReceipt(running, completed)).toBe(true);
    expect(validateReceipt(completed, completed)).toBe(true);
    expect(validateReceipt(completed, { ...completed, phase: "running" })).toBe(false);
    expect(
      validateReceipt(completed, {
        ...completed,
        staged: { ...completed.staged!, stagedHandle: "other" },
      }),
    ).toBe(false);
  });

  it("requires an exact install outcome; failure retains the reviewed stage", () => {
    const plan = opened();
    const install = {
      ...receipt(),
      action: {
        kind: "install" as const,
        stagedHandle: "stage",
        currentVersion: "old",
        targetSource: source,
      },
    };
    const request = {
      operationId,
      environmentId: env,
      action: install.action,
      expectedInstalledSource: installed,
      ...(install.correlation === undefined ? {} : { correlation: install.correlation }),
    };
    expect(validateReceipt(request, { ...install, phase: "completed" })).toBe(false);
    const completed: JonesOperationReceipt = {
      ...install,
      phase: "completed",
      updateId: operationId,
      outcome: { status: "committed", fromVersion: "old", targetVersion: "new" },
    };
    expect(validateReceipt(request, completed)).toBe(true);
    for (const phase of ["refused", "failed", "interrupted"] as const)
      expect(
        deviceState(plan, {
          ...plan.devices[0]!,
          operations: [{ ...operation(), step: "install", receipt: { ...install, phase } }],
        }),
      ).toBe("staged");
    expect(
      deviceSettled(plan, {
        ...plan.devices[0]!,
        operations: [{ ...operation(), step: "install", receipt: completed }],
      }),
    ).toBe(true);
  });

  it("keeps Stop requested pending and a too-late Stop staged; accepts only a bound preacceptance fence", () => {
    const plan = opened();
    const requested = { ...receipt("stop-requested"), stop: { stopId: id, requestedAt: now } };
    expect(
      deviceSettled(plan, {
        ...plan.devices[0]!,
        operations: [{ ...operation(), receipt: requested }],
      }),
    ).toBe(false);
    const completed: JonesOperationReceipt = {
      ...requested,
      phase: "completed",
      stop: { ...requested.stop, result: "too-late" },
      staged: { stagedHandle: "stage", sourceSha: source, sourceTree: tree, artifactId: 1 },
    };
    expect(validateReceipt(requested, completed)).toBe(true);
    expect(
      deviceState(plan, {
        ...plan.devices[0]!,
        operations: [{ ...operation(), receipt: completed }],
      }),
    ).toBe("staged");
    const intent = operation().request;
    if (intent.kind !== "operation-start") throw new Error("fixture");
    const fenced: JonesOperationReceipt = {
      ...intent.input,
      protocol: 1,
      phase: "fenced",
      updatedAt: now,
    };
    expect(validateReceipt(intent.input, fenced)).toBe(true);
    expect(
      validateReceipt(intent.input, {
        ...fenced,
        expectedInstalledSource: "0".repeat(40),
        action: { kind: "download-exact", targetSource: "0".repeat(40) },
      }),
    ).toBe(false);
  });

  it("settles unstarted followers only after a proven Laptop failure, with no outstanding leases", () => {
    const plan = base();
    expect(laptopSettled(plan)).toBe(false);
    const failed: JonesInstallPlan = {
      ...plan,
      laptop: {
        ...plan.laptop,
        operations: [
          {
            operationId,
            step: "download",
            requestedAt: now,
            result: { status: "failed", at: now },
          },
        ],
      },
    };
    expect(deviceState(failed, failed.devices[0]!)).toBe("not-started");
    expect(isPlanComplete(failed)).toBe(true);
    expect(
      isPlanComplete({
        ...failed,
        devices: [{ ...failed.devices[0]!, operations: [operation()] }],
      }),
    ).toBe(false);
    expect(canSupersede(plan)).toBe(true);
    expect(
      canSupersede({ ...failed, devices: [{ ...failed.devices[0]!, operations: [operation()] }] }),
    ).toBe(false);
  });

  it("requires directive negotiation for enrolled hosts and exact evidence after CAS", () => {
    const input = {
      directiveId: operationId,
      enrollmentId: id,
      environmentId: env,
      repository: "Jones-Systems/Jones-Code" as const,
      targetSource: source,
      targetTree: tree,
      expectedDirectiveId: null,
    };
    const status = {
      operationProtocol: 1 as const,
      planDirectiveProtocol: 1 as const,
      environmentId: env,
      enrollment: {
        enrollmentId: id,
        environmentId: env,
        enabled: true,
        continueRunningThreads: false,
      },
      operation: null,
      update: state,
      follower: {
        protocol: 1 as const,
        state: "waiting-for-leader" as const,
        leaderSource: null,
        planDirective: {
          directiveId: operationId,
          targetSource: source,
          targetTree: tree,
          issuedAt: now,
          mode: "manual-review" as const,
        },
      },
    };
    expect(selectability({ state, freshness: "fresh" }, status)).toMatchObject({
      selectable: true,
      route: "follower-operations",
    });
    const { planDirectiveProtocol: _protocol, ...legacyStatus } = status;
    expect(selectability({ state, freshness: "fresh" }, legacyStatus).selectable).toBe(false);
    expect(selectability({ state, freshness: "stale" }, status).selectable).toBe(false);
    expect(validateDirectiveEvidence(input, status)).toBe(true);
    expect(validateDirectiveEvidence(input, { ...status, environmentId: laptop })).toBe(false);
    expect(
      validateDirectiveEvidence(input, {
        ...status,
        follower: {
          ...status.follower,
          planDirective: { ...status.follower.planDirective, targetTree: source },
        },
      }),
    ).toBe(false);
  });

  it("waits for exact follower enrollment before claiming a directive", () => {
    const plan = opened();
    const device: JonesInstallPlanDevice = {
      ...plan.devices[0]!,
      route: "follower-operations",
      enrollmentId: id,
    };
    const observation = { state, freshness: "fresh" as const };
    const status = {
      operationProtocol: 1 as const,
      planDirectiveProtocol: 1 as const,
      environmentId: env,
      enrollment: {
        enrollmentId: id,
        environmentId: env,
        enabled: true,
        continueRunningThreads: false,
      },
      operation: null,
      update: state,
    };
    for (const missing of [null, undefined]) {
      expect(nextRunnerStep(plan, device, observation, missing, Date.parse(now)).kind).toBe("wait");
    }
    expect(
      nextRunnerStep(plan, device, observation, { ...status, enrollment: null }, Date.parse(now))
        .kind,
    ).toBe("wait");
    expect(
      nextRunnerStep(
        plan,
        device,
        observation,
        { ...status, enrollment: { ...status.enrollment, enrollmentId: operationId } },
        Date.parse(now),
      ).kind,
    ).toBe("wait");
    expect(nextRunnerStep(plan, device, observation, status, Date.parse(now))).toEqual({
      kind: "claim-directive",
      enrollmentId: id,
      expectedDirectiveId: null,
    });
    const previous = {
      directiveId: operationId,
      targetSource: source,
      targetTree: tree,
      mode: "manual-review" as const,
      issuedAt: now,
    };
    expect(
      nextRunnerStep(
        plan,
        device,
        observation,
        {
          ...status,
          follower: {
            protocol: 1,
            leaderSource: null,
            state: "waiting-for-leader",
            planDirective: previous,
          },
        },
        Date.parse(now),
      ),
    ).toEqual({ kind: "claim-directive", enrollmentId: id, expectedDirectiveId: operationId });
  });
});
