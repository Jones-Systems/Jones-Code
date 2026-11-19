// @effect-diagnostics globalDate:off - Fixed timestamps and injected clocks exercise receipt, lease and review behavior.
import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlan,
  JonesInstallPlanGrant,
  JonesInstallPlanHostOperation,
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
  JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import { startPlan } from "./model.ts";
import { advanceInstallPlan, type InstallPlanDriver } from "./runner.ts";

const env = EnvironmentId.make("host");
const otherEnv = EnvironmentId.make("other-host");
const laptop = EnvironmentId.make("laptop");
const planId = "11111111-1111-4111-8111-111111111111";
const opId = "22222222-2222-4222-8222-222222222222";
const enrollmentId = "33333333-3333-4333-8333-333333333333";
const previousDirectiveId = "44444444-4444-4444-8444-444444444444";
const otherPlanId = "55555555-5555-4555-8555-555555555555";
const otherOpId = "66666666-6666-4666-8666-666666666666";
const source = "a".repeat(40),
  tree = "b".repeat(40),
  installed = "c".repeat(40);
const now = "2026-10-27T05:00:00.000Z";
const leaseUntil = "2026-10-27T05:02:00.000Z";
const request: JonesOperationStartInput = {
  operationId: opId,
  environmentId: env,
  expectedInstalledSource: installed,
  action: { kind: "download-exact", targetSource: source },
  correlation: { planId, deviceKey: env },
};
function fixture(recoveryHistory?: 1) {
  let plan = startPlan({
    planId,
    origin: "update-button",
    target: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: tree,
      version: "new",
    },
    from: { version: "old" },
    laptopEnvironmentId: laptop,
    select: [{ environmentId: env, label: "Host", route: "operations" }],
    now,
  });
  plan = {
    ...plan,
    laptop: {
      ...plan.laptop,
      gate: {
        openedAt: now,
        basis: "plan-install",
        transactionId: "tx",
        activeGeneration: "gen",
        activeSource: source,
        activeTree: tree,
      },
    },
  };
  const state = (): JonesInstallPlanState => ({
    schema: 1,
    current: plan,
    last: null,
    laptop: { observedAt: now, activationBlocked: false },
    view: { complete: false, ...(recoveryHistory === undefined ? {} : { recoveryHistory }) },
  });
  const grant: JonesInstallPlanGrant = {
    planId,
    environmentId: env,
    operationId: opId,
    step: "download",
    request: { kind: "operation-start", input: request },
    leaseUntil,
  };
  const receipt: JonesOperationReceipt = {
    ...request,
    protocol: 1,
    phase: "accepted",
    acceptedAt: now,
    updatedAt: now,
  };
  const desktop = vi.fn<InstallPlanDriver["desktop"]>(
    async (r): Promise<JonesInstallPlanResult> => ({
      state: state(),
      ...(r.type === "claim" ? { grant } : {}),
    }),
  );
  const observe = vi.fn<InstallPlanDriver["observe"]>(async () => ({
    canOperate: true,
    fleetStatus: null,
    observation: {
      freshness: "fresh",
      state: {
        source: "jones-actions",
        channel: "jones-main",
        environmentId: env,
        phase: "no-new",
        currentVersion: "old",
        installedSource: { repository: "Jones-Systems/Jones-Code", sha: installed, tree },
        capability: {
          check: true,
          download: true,
          install: true,
          exactSource: true,
          operations: 1,
        },
      },
    },
  }));
  const operation = vi.fn<InstallPlanDriver["operation"]>(async (_env, r) =>
    r.action === "operationRead" ? { receipts: [{ operationId: opId, receipt }] } : receipt,
  );
  const host = vi.fn<InstallPlanDriver["host"]>(async () => {
    throw new Error("unused");
  });
  const canDispatch = vi.fn<InstallPlanDriver["canDispatch"]>(() => true);
  return {
    driver: {
      claimant: "window:a",
      now: () => Date.parse(now),
      cancelled: () => false,
      desktop,
      observe,
      canDispatch,
      operation,
      host,
    },
    plan: () => plan,
    state,
    setPlan: (p: JonesInstallPlan) => {
      plan = p;
    },
    grant,
    receipt,
    desktop,
    operation,
    observe,
    canDispatch,
    host,
  };
}

function reservation(claimant = "window:a"): JonesInstallPlanHostOperation {
  return {
    operationId: opId,
    step: "download",
    request: { kind: "operation-start", input: request },
    lease: { claimant, reservedAt: now, leaseUntil },
  };
}

function withReservation(plan: JonesInstallPlan, operation = reservation()): JonesInstallPlan {
  return { ...plan, devices: [{ ...plan.devices[0]!, operations: [operation] }] };
}

function nextPlan(plan: JonesInstallPlan): JonesInstallPlan {
  return {
    ...plan,
    planId: otherPlanId,
    revision: 0,
    target: { ...plan.target, version: "newer", sourceSha: "e".repeat(40) },
    laptop: { environmentId: plan.laptop.environmentId, operations: [] },
    devices: [{ ...plan.devices[0]!, environmentId: otherEnv, label: "Other", operations: [] }],
  };
}

describe("bounded install plan runner", () => {
  it("claims before download, reports exact acceptance and never installs", async () => {
    const f = fixture();
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim", "report"]);
    expect(f.desktop.mock.calls[1]?.[0]).toMatchObject({ step: "download", explicit: false });
    expect(f.operation.mock.calls[0]?.[1]).toEqual({ action: "operationStart", input: request });
    expect(f.desktop.mock.invocationCallOrder[1]).toBeLessThan(
      f.operation.mock.invocationCallOrder[0]!,
    );
  });

  it.each([
    {
      name: "refused",
      refusal: { code: "unsupported", message: "Read refused." },
    },
    {
      name: "read-only",
      readOnly: { code: "invalid", message: "Read-only ledger." },
    },
  ] as const)("does not act on a $name initial read even with a current plan", async (test) => {
    const f = fixture(1);
    f.desktop.mockResolvedValueOnce(
      test.name === "refused"
        ? { state: f.state(), refused: test.refusal }
        : { state: { ...f.state(), readOnly: test.readOnly } },
    );
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls).toEqual([[{ type: "read" }]]);
    expect(f.observe).not.toHaveBeenCalled();
    expect(f.operation).not.toHaveBeenCalled();
    expect(f.host).not.toHaveBeenCalled();
  });

  it("a close before a delayed claim leaves the archived request unsent as plan B becomes current", async () => {
    const f = fixture(1);
    const a = f.plan();
    const b = nextPlan(a);
    let state = f.state();
    const claimArrived = Promise.withResolvers<JonesInstallPlanRequest>();
    const evaluateClaim = Promise.withResolvers<void>();
    f.desktop.mockImplementation(async (request) => {
      if (request.type !== "claim") return { state };
      claimArrived.resolve(request);
      await evaluateClaim.promise;
      return request.planId === state.current?.planId
        ? { state, grant: f.grant }
        : { state, refused: { code: "plan-mismatch", message: "The plan is closed." } };
    });
    const pass = advanceInstallPlan(f.driver);
    const claim = await Promise.race([claimArrived.promise, pass]);
    state = {
      ...state,
      current: b,
      last: { ...a, dismissedAt: now, cancelled: true, revision: a.revision + 1 },
    };
    evaluateClaim.resolve();
    await pass;
    expect(claim).toMatchObject({
      type: "claim",
      planId,
      environmentId: env,
      step: "download",
      explicit: false,
    });
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim"]);
    expect(f.operation).not.toHaveBeenCalled();
    expect(f.host).not.toHaveBeenCalled();
  });

  it.each([false, true])(
    "sends an issued grant after close and reports to its original plan (disposed: %s)",
    async (disposeBeforeReceipt) => {
      const f = fixture(1);
      const a = f.plan();
      const b = nextPlan(a);
      let state = f.state();
      const granted = Promise.withResolvers<void>();
      const deliverGrant = Promise.withResolvers<void>();
      const sent = Promise.withResolvers<void>();
      const deliverReceipt = Promise.withResolvers<JonesOperationReceipt>();
      let disposed = false;
      f.operation.mockImplementation(async () => {
        sent.resolve();
        return deliverReceipt.promise;
      });
      f.desktop.mockImplementation(async (r) => {
        if (r.type === "claim" && r.planId === state.current?.planId && r.environmentId === env) {
          state = { ...state, current: withReservation(a) };
          const authorized = state;
          granted.resolve();
          await deliverGrant.promise;
          return { state: authorized, grant: f.grant };
        }
        if (
          r.type === "report" &&
          r.planId === state.last?.planId &&
          r.environmentId === env &&
          r.operationId === state.last.devices[0]?.operations[0]?.operationId &&
          r.evidence.kind === "receipt"
        ) {
          const archived = state.last;
          state = {
            ...state,
            last: withReservation(
              { ...archived, revision: archived.revision + 1 },
              { ...reservation(), receipt: r.evidence.receipt },
            ),
          };
        }
        return { state };
      });
      const pass = advanceInstallPlan({ ...f.driver, cancelled: () => disposed });
      await Promise.race([granted.promise, pass]);
      const closed = state.current!;
      state = {
        ...state,
        current: b,
        last: { ...closed, dismissedAt: now, cancelled: true, revision: closed.revision + 1 },
      };
      deliverGrant.resolve();
      await Promise.race([sent.promise, pass]);
      disposed = disposeBeforeReceipt;
      deliverReceipt.resolve(f.receipt);
      await pass;

      expect(f.operation.mock.calls).toEqual([[env, { action: "operationStart", input: request }]]);
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim", "report"]);
      expect(f.desktop.mock.calls.at(-1)?.[0]).toEqual({
        type: "report",
        planId,
        environmentId: env,
        operationId: opId,
        evidence: { kind: "receipt", receipt: f.receipt },
      });
      expect(state.current).toBe(b);
      expect(state.last).toMatchObject({
        planId,
        devices: [{ environmentId: env, operations: [{ operationId: opId, receipt: f.receipt }] }],
      });
    },
  );

  it("runs a disjoint active device without reading, replaying, or fencing archived reservations", async () => {
    const f = fixture(1);
    const a = f.plan();
    const b = nextPlan(a);
    const archive = {
      ...withReservation(a),
      dismissedAt: now,
      cancelled: true as const,
      revision: a.revision + 1,
    };
    const state: JonesInstallPlanState = {
      ...f.state(),
      documentRevision: 2,
      current: null,
      last: archive,
      recoveryHistory: [
        {
          planId,
          revision: archive.revision,
          targetVersion: a.target.version,
          closedAt: now,
          pendingLaptop: false,
          pendingDevices: [{ environmentId: env, label: "Host" }],
        },
      ],
    };
    f.desktop.mockResolvedValue({ state, historyPlan: archive });
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls).toEqual([[{ type: "read" }]]);
    expect(f.observe).not.toHaveBeenCalled();
    expect(f.operation).not.toHaveBeenCalled();

    const active = {
      ...b,
      laptop: {
        ...b.laptop,
        gate: { ...a.laptop.gate!, transactionId: "tx-b", activeSource: b.target.sourceSha },
      },
    };
    const bInput: JonesOperationStartInput = {
      ...request,
      environmentId: otherEnv,
      operationId: otherOpId,
      action: { kind: "download-exact", targetSource: b.target.sourceSha },
      correlation: { planId: otherPlanId, deviceKey: otherEnv },
    };
    f.desktop.mockImplementation(async (r) => ({
      state: { ...state, current: active },
      ...(r.type === "claim"
        ? {
            grant: {
              ...f.grant,
              planId: otherPlanId,
              environmentId: otherEnv,
              operationId: otherOpId,
              request: { kind: "operation-start", input: bInput },
            },
          }
        : {}),
    }));
    const observed = await f.observe(otherEnv);
    f.observe.mockClear();
    f.observe.mockResolvedValue({
      ...observed,
      observation: {
        ...observed.observation,
        state: { ...observed.observation.state!, environmentId: otherEnv },
      },
    });
    f.operation.mockResolvedValue({ ...f.receipt, ...bInput });
    f.desktop.mockClear();
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim", "report"]);
    expect(f.observe.mock.calls).toEqual([[otherEnv]]);
    expect(f.operation.mock.calls).toEqual([
      [otherEnv, { action: "operationStart", input: bInput }],
    ]);
    expect(f.host).not.toHaveBeenCalled();
  });

  it.each(["removed", "changed", "paused"] as const)(
    "keeps %s devices or gates inert",
    async (kind) => {
      const f = fixture(),
        p = f.plan();
      f.setPlan(
        kind === "removed"
          ? { ...p, devices: [{ ...p.devices[0]!, membership: "removed" }] }
          : {
              ...p,
              laptop: {
                ...p.laptop,
                gateClosed: { at: now, reason: kind === "paused" ? "blocked" : "changed" },
                ...(kind === "paused" ? { gatePause: true as const } : {}),
              },
            },
      );
      await advanceInstallPlan(f.driver);
      expect(f.operation).not.toHaveBeenCalled();
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
    },
  );

  it.each(["new download", "download replay", "fence"] as const)(
    "uses no lease or host mutation for a read-only peer's %s",
    async (step) => {
      const f = fixture(1);
      if (step !== "new download") {
        const plan = f.plan();
        f.setPlan({
          ...plan,
          devices: [
            {
              ...plan.devices[0]!,
              operations: [
                {
                  operationId: opId,
                  step: "download",
                  request: { kind: "operation-start", input: request },
                  lease: {
                    claimant: f.driver.claimant,
                    reservedAt: now,
                    leaseUntil: step === "fence" ? now : leaseUntil,
                  },
                },
              ],
            },
          ],
        });
      }
      const observed = await f.observe(env);
      f.observe.mockResolvedValueOnce({ ...observed, canOperate: false });
      await advanceInstallPlan(f.driver);
      f.canDispatch.mockReturnValue(false);
      await advanceInstallPlan(f.driver);
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "read"]);
      expect(f.operation).not.toHaveBeenCalled();
      expect(f.host).not.toHaveBeenCalled();
    },
  );

  it("does not dispatch when authority changes during a native claim", async () => {
    const f = fixture();
    const claim = f.desktop.getMockImplementation()!;
    f.desktop.mockImplementation(async (request) => {
      const result = await claim(request);
      if (request.type === "claim") f.canDispatch.mockReturnValue(false);
      return result;
    });
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim"]);
    expect(f.operation).not.toHaveBeenCalled();
  });

  it("resends only its own identical live reservation and fences an abandoned one", async () => {
    const f = fixture(1);
    f.setPlan(withReservation(f.plan()));
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls[1]?.[0]).toMatchObject({ step: "download", operationId: opId });
    expect(f.operation.mock.calls[0]?.[1]).toEqual({ action: "operationStart", input: request });
    f.operation.mockClear();
    f.driver.now = () => Date.parse(leaseUntil) + 1;
    f.desktop.mockImplementation(async (r) => ({
      state: {
        schema: 1,
        current: f.plan(),
        last: null,
        laptop: { observedAt: now, activationBlocked: false },
        view: { complete: false },
      },
      ...(r.type === "claim"
        ? {
            grant: {
              ...f.grant,
              step: "fence",
              leaseUntil: "2026-10-27T05:04:00.000Z",
              request: { kind: "operation-fence", input: request },
            },
          }
        : {}),
    }));
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls[0]?.[1]).toEqual({ action: "operationFence", input: request });
  });

  it("does not mint a new lease if the original is fenced before the delayed reclaim", async () => {
    const f = fixture(1);
    const reserved = withReservation(f.plan());
    f.setPlan(reserved);
    const waiting = Promise.withResolvers<void>();
    const evaluateClaim = Promise.withResolvers<void>();
    let replacementLeases = 0;
    f.desktop.mockImplementation(async (r) => {
      if (r.type !== "claim") return { state: f.state() };
      waiting.resolve();
      await evaluateClaim.promise;
      if (r.operationId === opId)
        return {
          state: f.state(),
          refused: { code: "outstanding-operation", message: "The original is fenced." },
        };
      // An unbound request after fencing is indistinguishable from a fresh schedule.
      replacementLeases++;
      return {
        state: f.state(),
        grant: {
          ...f.grant,
          operationId: otherOpId,
          request: { kind: "operation-start", input: { ...request, operationId: otherOpId } },
        },
      };
    });
    const pass = advanceInstallPlan(f.driver);
    await Promise.race([waiting.promise, pass]);
    f.setPlan(
      withReservation(
        { ...reserved, revision: reserved.revision + 1 },
        {
          ...reservation(),
          receipt: { ...request, protocol: 1, phase: "fenced", updatedAt: leaseUntil },
        },
      ),
    );
    evaluateClaim.resolve();
    await pass;
    expect(f.desktop.mock.calls[1]?.[0]).toMatchObject({ step: "download", operationId: opId });
    expect(replacementLeases).toBe(0);
    expect(f.operation).not.toHaveBeenCalled();
  });

  it("reads an old native download reservation without inferring absence and fences on expiry", async () => {
    const f = fixture();
    f.setPlan(withReservation(f.plan()));
    f.operation.mockResolvedValueOnce({ receipts: [{ operationId: opId, receipt: null }] });
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls).toEqual([[{ type: "read" }]]);
    expect(f.operation.mock.calls).toEqual([
      [env, { action: "operationRead", input: { environmentId: env, operationIds: [opId] } }],
    ]);

    f.driver.now = () => Date.parse(leaseUntil) + 1;
    const fenced: JonesOperationReceipt = {
      ...request,
      protocol: 1,
      phase: "fenced",
      updatedAt: leaseUntil,
    };
    f.operation.mockResolvedValueOnce(fenced);
    f.desktop.mockImplementation(async (r) => ({
      state: f.state(),
      ...(r.type === "claim"
        ? {
            grant: {
              ...f.grant,
              step: "fence",
              leaseUntil: "2026-10-27T05:04:00.000Z",
              request: { kind: "operation-fence", input: request },
            },
          }
        : {}),
    }));
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls.at(-1)).toEqual([
      env,
      { action: "operationFence", input: request },
    ]);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "read", "claim", "report"]);
    expect(f.desktop.mock.calls.at(-1)?.[0]).toEqual({
      type: "report",
      planId,
      environmentId: env,
      operationId: opId,
      evidence: { kind: "receipt", receipt: fenced },
    });
  });

  it.each(["another operation", "start payload"] as const)(
    "rejects a fence grant for %s",
    async (kind) => {
      const f = fixture(1);
      f.setPlan(withReservation(f.plan()));
      f.driver.now = () => Date.parse(leaseUntil);
      f.desktop.mockImplementation(async (r) => ({
        state: f.state(),
        ...(r.type === "claim"
          ? {
              grant: {
                ...f.grant,
                step: "fence",
                leaseUntil: "2026-10-27T05:04:00.000Z",
                ...(kind === "another operation"
                  ? {
                      operationId: otherOpId,
                      request: {
                        kind: "operation-fence",
                        input: { ...request, operationId: otherOpId },
                      },
                    }
                  : {}),
              },
            }
          : {}),
      }));
      await advanceInstallPlan(f.driver);
      expect(f.desktop.mock.calls[1]?.[0]).toMatchObject({ step: "fence", operationId: opId });
      expect(f.operation).not.toHaveBeenCalled();
    },
  );

  it("reads after lost acceptance and preserves null receipts without another action", async () => {
    const f = fixture(),
      p = f.plan();
    f.setPlan({
      ...p,
      devices: [
        {
          ...p.devices[0]!,
          operations: [
            {
              operationId: opId,
              step: "download",
              request: { kind: "operation-start", input: request },
              lease: { claimant: "other", reservedAt: now, leaseUntil },
              receipt: f.receipt,
            },
          ],
        },
      ],
    });
    f.operation.mockResolvedValue({ receipts: [{ operationId: opId, receipt: null }] });
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls.map(([, r]) => r.action)).toEqual(["operationRead"]);
    expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
    f.operation.mockResolvedValue({
      receipts: [{ operationId: opId, receipt: { ...f.receipt, environmentId: laptop } }],
    });
    await advanceInstallPlan(f.driver);
    expect(f.desktop.mock.calls.filter(([r]) => r.type === "report")).toHaveLength(0);
    const observed = await f.observe(env);
    f.observe.mockResolvedValue({ ...observed, canOperate: false });
    f.canDispatch.mockReturnValue(false);
    f.operation.mockResolvedValue({ receipts: [{ operationId: opId, receipt: f.receipt }] });
    await advanceInstallPlan(f.driver);
    expect(f.operation.mock.calls.at(-1)?.[1]).toMatchObject({
      action: "operationRead",
      input: { environmentId: env, operationIds: [opId] },
    });
    expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({
      type: "report",
      operationId: opId,
      evidence: { kind: "receipt", receipt: f.receipt },
    });
    expect(f.desktop.mock.calls.some(([r]) => r.type === "claim")).toBe(false);
  });

  it.each([
    {
      name: "receipt-only support",
      capability: { check: false, download: false, install: false, operationReceipts: 1 },
    },
    {
      name: "download disabled",
      capability: { check: true, download: false, install: true, operations: 1, exactSource: true },
    },
    {
      name: "install disabled",
      capability: { check: true, download: true, install: false, operations: 1, exactSource: true },
    },
  ] as const)(
    "reads exact receipts and never claims a lease with $name",
    async ({ capability }) => {
      const f = fixture();
      const plan = f.plan();
      const observed = await f.observe(env);
      const before = observed.observation.state!;
      const limited = {
        ...observed,
        observation: { ...observed.observation, state: { ...before, capability } },
      };
      const reservation = {
        operationId: opId,
        step: "download" as const,
        request: { kind: "operation-start" as const, input: request },
        lease: { claimant: f.driver.claimant, reservedAt: now, leaseUntil: now },
      };
      const setOperation = (operation = reservation) => {
        f.setPlan({ ...plan, devices: [{ ...plan.devices[0]!, operations: [operation] }] });
      };

      f.observe.mockResolvedValue(limited);
      for (const expiresAt of [now, leaseUntil]) {
        setOperation({ ...reservation, lease: { ...reservation.lease, leaseUntil: expiresAt } });
        f.operation.mockResolvedValueOnce({ receipts: [{ operationId: opId, receipt: null }] });
        await advanceInstallPlan(f.driver);
        expect(f.operation).toHaveBeenLastCalledWith(env, {
          action: "operationRead",
          input: { environmentId: env, operationIds: [opId] },
        });
      }
      expect(f.operation).toHaveBeenCalledTimes(2);
      expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read", "read"]);

      f.operation.mockClear();
      const accepted = { ...reservation, receipt: f.receipt };
      setOperation(accepted);
      f.observe.mockResolvedValueOnce({
        ...limited,
        observation: {
          ...limited.observation,
          state: { ...before, capability, environmentId: laptop },
        },
      });
      await advanceInstallPlan(f.driver);
      f.observe.mockResolvedValueOnce({
        ...limited,
        observation: { ...limited.observation, freshness: "stale" },
      });
      await advanceInstallPlan(f.driver);
      expect(f.operation).not.toHaveBeenCalled();

      f.observe.mockResolvedValueOnce({ ...limited, canOperate: false });
      f.canDispatch.mockReturnValue(false);
      await advanceInstallPlan(f.driver);
      expect(f.desktop.mock.calls.at(-1)?.[0]).toEqual({
        type: "report",
        planId,
        environmentId: env,
        operationId: opId,
        evidence: { kind: "receipt", receipt: f.receipt },
      });

      f.setPlan(plan);
      f.canDispatch.mockReturnValue(true);
      await advanceInstallPlan(f.driver);
      expect(f.operation).toHaveBeenCalledTimes(1);
      expect(f.desktop.mock.calls.some(([r]) => r.type === "claim")).toBe(false);
      expect(f.host).not.toHaveBeenCalled();
    },
  );

  it("does not borrow another window's lease or perform explicit install/Stop", async () => {
    const f = fixture(),
      p = f.plan();
    for (const step of ["download", "install"] as const) {
      f.setPlan({
        ...p,
        devices: [
          {
            ...p.devices[0]!,
            operations: [
              {
                operationId: opId,
                step,
                request: { kind: "operation-start", input: request },
                lease: {
                  claimant: step === "install" ? "window:a" : "window:other",
                  reservedAt: now,
                  leaseUntil,
                },
              },
            ],
          },
        ],
      });
      await advanceInstallPlan(f.driver);
    }
    expect(f.operation).not.toHaveBeenCalled();
  });

  const directiveReplies = [
    "reply",
    "reply after disposal",
    "lost reply",
    "first-send conflict",
    "no recorded host effect",
  ] as const;
  it.each(directiveReplies)("reconciles the directive after %s", async (result) => {
    const f = fixture(),
      p = f.plan();
    f.setPlan({
      ...p,
      devices: [{ ...p.devices[0]!, route: "follower-operations", enrollmentId }],
    });
    const observed = await f.observe(env);
    const fleet = {
      environmentId: env,
      operationProtocol: 1 as const,
      planDirectiveProtocol: 1 as const,
      enrollment: {
        environmentId: env,
        enrollmentId,
        enabled: true,
        continueRunningThreads: false,
      },
      operation: null,
      update: observed.observation.state,
      follower: {
        protocol: 1 as const,
        leaderSource: installed,
        state: "waiting-for-leader" as const,
        planDirective: {
          directiveId: previousDirectiveId,
          targetSource: installed,
          targetTree: tree,
          mode: "manual-review" as const,
          issuedAt: now,
        },
      },
    };
    f.observe.mockResolvedValue({ ...observed, fleetStatus: fleet });
    const directive = {
      directiveId: opId,
      environmentId: env,
      enrollmentId,
      repository: "Jones-Systems/Jones-Code" as const,
      targetSource: source,
      targetTree: tree,
      expectedDirectiveId: previousDirectiveId,
    };
    const state = (): JonesInstallPlanState => ({
      schema: 1,
      current: f.plan(),
      last: null,
      laptop: { observedAt: now, activationBlocked: false },
      view: { complete: false },
    });
    const grant: JonesInstallPlanGrant = {
      ...f.grant,
      step: "directive",
      request: { kind: "plan-directive", input: directive },
    };
    f.desktop.mockResolvedValue({ state: state(), grant });
    const applied = {
      ...fleet,
      follower: {
        ...fleet.follower,
        planDirective: {
          directiveId: opId,
          targetSource: source,
          targetTree: tree,
          mode: "manual-review" as const,
          issuedAt: now,
        },
      },
    };
    if (result === "reply") {
      f.observe.mockResolvedValueOnce({ ...observed, canOperate: false, fleetStatus: fleet });
      await advanceInstallPlan(f.driver);
      expect(f.desktop.mock.calls.map(([request]) => request.type)).toEqual(["read"]);
      expect(f.host).not.toHaveBeenCalled();
      f.desktop.mockClear();
    }
    let disposed = false;
    const replied = result === "reply" || result === "reply after disposal";
    if (!replied)
      f.host.mockRejectedValueOnce({
        _tag: "FleetHostError",
        reason: result === "first-send conflict" ? "conflict" : "storage",
        message: result === "first-send conflict" ? "Host busy." : "sync failed",
      });
    else
      f.host.mockImplementationOnce(async () => {
        disposed = result === "reply after disposal";
        return applied;
      });

    await advanceInstallPlan({ ...f.driver, cancelled: () => disposed });
    expect(f.host.mock.calls[0]?.[1]).toEqual({ action: "planDirective", input: directive });
    if (!replied) {
      expect(f.desktop.mock.calls.map(([input]) => input.type)).toEqual(["read", "claim"]);
      const pending = f.plan();
      f.setPlan({
        ...pending,
        devices: [
          {
            ...pending.devices[0]!,
            operations: [
              {
                operationId: opId,
                step: "directive",
                request: { kind: "plan-directive", input: directive },
                lease: { claimant: f.driver.claimant, reservedAt: now, leaseUntil },
              },
            ],
          },
        ],
      });
      const resumed = { ...f.driver, claimant: "window:b", now: () => Date.parse(leaseUntil) };
      const update = observed.observation.state!;
      const limited = {
        ...observed,
        observation: {
          ...observed.observation,
          state: { ...update, capability: { ...update.capability, download: false } },
        },
      };
      if (result === "lost reply") {
        f.desktop.mockResolvedValue({ state: state() });
        f.observe.mockResolvedValue({ ...limited, canOperate: false, fleetStatus: applied });
        f.canDispatch.mockReturnValue(false);
        await advanceInstallPlan(resumed);
        expect(f.host).toHaveBeenCalledTimes(1);
      } else {
        const changed = {
          ...applied,
          follower: {
            ...applied.follower,
            planDirective: { ...applied.follower.planDirective, directiveId: enrollmentId },
          },
        };
        f.observe.mockResolvedValue({ ...observed, fleetStatus: changed });
        await advanceInstallPlan(resumed);
        expect(f.host).toHaveBeenCalledTimes(1);
        f.observe.mockResolvedValue({ ...observed, canOperate: false, fleetStatus: fleet });
        f.desktop.mockClear();
        await advanceInstallPlan(resumed);
        expect(f.host).toHaveBeenCalledTimes(1);
        expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
        f.observe.mockResolvedValue({ ...limited, fleetStatus: fleet });
        f.desktop.mockClear();
        await advanceInstallPlan(resumed);
        expect(f.host).toHaveBeenCalledTimes(1);
        expect(f.desktop.mock.calls.map(([r]) => r.type)).toEqual(["read"]);
        f.observe.mockResolvedValue({ ...observed, fleetStatus: fleet });
        f.desktop.mockResolvedValue({
          state: state(),
          grant: {
            ...grant,
            leaseUntil: new Date(Date.parse(leaseUntil) + 120_000).toISOString(),
          },
        });
        f.host.mockRejectedValueOnce({
          _tag: "FleetHostError",
          reason: "conflict",
          message: "Host busy.",
        });
        await advanceInstallPlan(resumed);
        expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({
          type: "claim",
          operationId: opId,
          claimant: "window:b",
          expectedDirectiveId: previousDirectiveId,
        });
        expect(f.host.mock.calls[1]?.[1]).toEqual({ action: "planDirective", input: directive });
        f.host.mockResolvedValueOnce(applied);
        await advanceInstallPlan(resumed);
        expect(f.host.mock.calls[2]?.[1]).toEqual({ action: "planDirective", input: directive });
      }
    }
    expect(f.operation).not.toHaveBeenCalled();
    expect(f.desktop.mock.calls.at(-1)?.[0]).toMatchObject({
      type: "report",
      planId,
      environmentId: env,
      operationId: opId,
      evidence: { kind: "directive" },
    });
  });
});
