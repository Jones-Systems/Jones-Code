// @effect-diagnostics globalDate:off - Fixed timestamps and injected clocks exercise receipt, lease and review behavior.
import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlan, JonesInstallPlanGrant, JonesInstallPlanResult, JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import type { JonesOperationReceipt, JonesOperationStartInput } from "@t3tools/contracts/jones/jonesOperations";
import { startPlan } from "./model.ts";
import { advanceInstallPlan, type InstallPlanDriver } from "./runner.ts";

const env = EnvironmentId.make("host");
const laptop = EnvironmentId.make("laptop");
const planId = "11111111-1111-4111-8111-111111111111";
const opId = "22222222-2222-4222-8222-222222222222";
const enrollmentId = "33333333-3333-4333-8333-333333333333";
const source = "a".repeat(40), tree = "b".repeat(40), installed = "c".repeat(40);
const now = "2026-10-27T05:00:00.000Z";
const leaseUntil = "2026-10-27T05:02:00.000Z";
const request: JonesOperationStartInput = { operationId: opId, environmentId: env, expectedInstalledSource: installed,
  action: { kind: "download-exact", targetSource: source }, correlation: { planId, deviceKey: env } };
function fixture() {
  let plan = startPlan({ planId, origin: "update-button", target: { repository: "Jones-Systems/Jones-Code", sourceSha: source,
    sourceTree: tree, version: "new" }, from: { version: "old" }, laptopEnvironmentId: laptop,
    select: [{ environmentId: env, label: "Host", route: "operations" }], now });
  plan = { ...plan, laptop: { ...plan.laptop, gate: { openedAt: now, basis: "plan-install", transactionId: "tx",
    activeGeneration: "gen", activeSource: source, activeTree: tree } } };
  const state = (): JonesInstallPlanState => ({ schema: 1, current: plan, last: null,
    laptop: { observedAt: now, activationBlocked: false }, view: { complete: false } });
  const grant: JonesInstallPlanGrant = { planId, environmentId: env, operationId: opId, step: "download",
    request: { kind: "operation-start", input: request }, leaseUntil };
  const receipt: JonesOperationReceipt = { ...request, protocol: 1, phase: "accepted", acceptedAt: now, updatedAt: now };
  const desktop = vi.fn<InstallPlanDriver["desktop"]>(async (r): Promise<JonesInstallPlanResult> =>
    ({ state: state(), ...(r.type === "claim" ? { grant } : {}) }));
  const observe = vi.fn<InstallPlanDriver["observe"]>(async () => ({ fleetStatus: null, observation: { freshness: "fresh", state: {
    source: "jones-actions", channel: "jones-main", environmentId: env, phase: "no-new", currentVersion: "old",
    installedSource: { repository: "Jones-Systems/Jones-Code", sha: installed, tree },
    capability: { check: true, download: true, install: true, exactSource: true, operations: 1 },
  } } }));
  const operation = vi.fn<InstallPlanDriver["operation"]>(async (_env, r) => r.action === "operationRead" ?
    { receipts: [{ operationId: opId, receipt }] } : receipt);
  const host = vi.fn<InstallPlanDriver["host"]>(async () => { throw new Error("unused"); });
  return { driver: { claimant: "window:a", now: () => Date.parse(now), cancelled: () => false, desktop, observe, operation, host },
    plan: () => plan, setPlan: (p: JonesInstallPlan) => { plan = p; }, grant, receipt, desktop, operation, observe, host };
}

describe("bounded install plan runner", () => {
  it("claims before download, reports exact acceptance and never installs", async () => {
    const f = fixture();
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim", "report"]);
    expect(f.desktop.mock.calls[1]?.[0]).toMatchObject({ step: "download", explicit: false });
    expect(f.operation.mock.calls[0]?.[1]).toEqual({ action: "operationStart", input: request });
    expect(f.desktop.mock.invocationCallOrder[1]).toBeLessThan(f.operation.mock.invocationCallOrder[0]!);
  });

  it("keeps excluded devices and a closed native barrier inert", async () => {
    for (const removed of [true, false]) {
      const f = fixture(), p = f.plan();
      f.setPlan(removed ? { ...p, devices: [{ ...p.devices[0]!, membership: "removed" }] } :
        { ...p, laptop: { ...p.laptop, gateClosed: { at: now, reason: "changed" } } });
      await advanceInstallPlan(f.driver);
      expect(f.operation).not.toHaveBeenCalled();
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
    }
  });

  it("resends only its own identical live reservation and fences an abandoned one", async () => {
    const f = fixture(), p = f.plan();
    const reserved = { operationId: opId, step: "download" as const, request: f.grant.request as Extract<JonesInstallPlanGrant["request"], { kind: "operation-start" }>,
      lease: { claimant: "window:a", reservedAt: now, leaseUntil } };
    f.setPlan({ ...p, devices: [{ ...p.devices[0]!, operations: [reserved] }] });
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls[0]?.[1]).toEqual({ action: "operationStart", input: request });
    f.operation.mockClear();
    f.driver.now = () => Date.parse(leaseUntil) + 1;
    f.desktop.mockImplementation(async (r) => ({ state: { schema: 1, current: f.plan(), last: null, laptop: { observedAt: now, activationBlocked: false }, view: { complete: false } },
      ...(r.type === "claim" ? { grant: { ...f.grant, step: "fence", leaseUntil: "2026-10-27T05:04:00.000Z", request: { kind: "operation-fence", input: request } } } : {}) }));
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls[0]?.[1]).toEqual({ action: "operationFence", input: request });
  });

  it("reads after lost acceptance and preserves null receipts without another action", async () => {
    const f = fixture(), p = f.plan();
    f.setPlan({ ...p, devices: [{ ...p.devices[0]!, operations: [{ operationId: opId, step: "download",
      request: { kind: "operation-start", input: request }, lease: { claimant: "other", reservedAt: now, leaseUntil }, receipt: f.receipt }] }] });
    f.operation.mockResolvedValue({ receipts: [{ operationId: opId, receipt: null }] });
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls.map(([, r]) => r.action)).toEqual(["operationRead"]);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
    f.operation.mockResolvedValue({ receipts: [{ operationId: opId, receipt: { ...f.receipt, environmentId: laptop } }] });
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls.filter(([r]) => r.type === "report")).toHaveLength(0);
  });

  it("does not borrow another window's lease or perform explicit install/Stop", async () => {
    const f = fixture(), p = f.plan();
    for (const step of ["download", "install"] as const) {
      f.setPlan({ ...p, devices: [{ ...p.devices[0]!, operations: [{ operationId: opId, step, request: { kind: "operation-start", input: request },
        lease: { claimant: step === "install" ? "window:a" : "window:other", reservedAt: now, leaseUntil } }] }] });
      await advanceInstallPlan(f.driver);
    }
    expect(f.operation).not.toHaveBeenCalled();
  });

  it("requires the enrolled directive receipt before a later pass may download", async () => {
    const f = fixture(), p = f.plan();
    f.setPlan({ ...p, devices: [{ ...p.devices[0]!, route: "follower-operations", enrollmentId }] });
    const observed = await f.observe(env);
    const fleet = { environmentId: env, operationProtocol: 1 as const, planDirectiveProtocol: 1 as const,
      enrollment: { environmentId: env, enrollmentId, enabled: true, continueRunningThreads: false }, operation: null, update: observed.observation.state,
      follower: { protocol: 1 as const, leaderSource: installed, state: "waiting-for-leader" as const } };
    f.observe.mockResolvedValue({ ...observed, fleetStatus: fleet });
    const directive = { directiveId: opId, environmentId: env, enrollmentId, repository: "Jones-Systems/Jones-Code" as const,
      targetSource: source, targetTree: tree, expectedDirectiveId: null };
    f.desktop.mockResolvedValue({ state: { schema: 1, current: f.plan(), last: null, laptop: { observedAt: now, activationBlocked: false }, view: { complete: false } },
      grant: { ...f.grant, step: "directive", request: { kind: "plan-directive", input: directive } } });
    f.host.mockResolvedValue({ ...fleet, follower: { ...fleet.follower, planDirective: { directiveId: opId,
      targetSource: source, targetTree: tree, mode: "manual-review", issuedAt: now } } });
    await advanceInstallPlan(f.driver);
    expect(f.host.mock.calls[0]?.[1]).toEqual({ action: "planDirective", input: directive });
    expect(f.operation).not.toHaveBeenCalled();
    expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({ type: "report", evidence: { kind: "directive" } });
  });
});
