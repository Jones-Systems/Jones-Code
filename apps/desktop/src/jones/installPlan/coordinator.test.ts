// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Deterministic synthetic native and host evidence; no real processes or install effects.
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, type DesktopUpdateState } from "@t3tools/contracts";
import type {
  JonesInstallPlanLaptopEvidence,
  JonesInstallPlanState,
  JonesInstallPlanReview,
  JonesInstallPlanGrant,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesRecoveryPreflight,
  JonesOperationReceipt,
} from "@t3tools/contracts/jones/jonesOperations";
import type { JonesDesktopUpdateController } from "../updates/JonesDesktopUpdates.ts";
import { createInstallPlanCoordinator } from "./coordinator.ts";
import type { InstallPlanStore, PlanSnapshot } from "./store.ts";

const laptop = EnvironmentId.make("laptop"),
  remote = EnvironmentId.make("host");
const source = "a".repeat(40),
  tree = "b".repeat(40),
  old = "c".repeat(40),
  transaction = "d".repeat(64);
const target = {
  repository: "Jones-Systems/Jones-Code",
  sourceSha: source,
  sourceTree: tree,
  version: "new",
} as const;
const selection = { environmentId: remote, label: "Host", route: "operations" } as const;
const timestamp = "2026-10-27T00:00:00.000Z";
const id = (n: number) => `${String(n).padStart(8, "0")}-1111-4111-8111-111111111111`;
function fixture() {
  let clock = Date.parse(timestamp),
    sequence = 0;
  let snapshot: PlanSnapshot = {
    document: { schema: 1, current: null, last: null },
    dispositions: { schema: 1, targets: [] },
  };
  let queue: Promise<unknown> = Promise.resolve();
  const store: InstallPlanStore = {
    transact: (work) => {
      const pending = queue.then(async () => {
        const next = await work(snapshot);
        snapshot = { ...snapshot, document: next.document, dispositions: next.dispositions };
        return next.result;
      });
      queue = pending.catch(() => undefined);
      return pending;
    },
  };
  let evidence: JonesInstallPlanLaptopEvidence = {
    observedAt: timestamp,
    environmentId: laptop,
    activeVersion: "new",
    activeGeneration: transaction,
    activeSource: source,
    activeTree: tree,
    committedTransactionId: transaction,
    activationBlocked: false,
  };
  let staged = false;
  const controller = {
    state: {
      currentVersion: "old",
      jones: {
        source: "jones-actions",
        channel: "jones-main",
        phase: "available",
        environmentId: laptop,
        capability: { check: true, download: true, install: true },
        provenance: {
          repository: target.repository,
          sourceSha: source,
          sourceTree: tree,
          artifactId: 1,
        },
      },
    } as DesktopUpdateState,
    busy: null,
    get stagedBuild() {
      return staged ? { handle: "stage", source, version: "new" } : undefined;
    },
    recoveryPreflight: vi.fn(
      async (input: JonesRecoveryPreflight["binding"]) =>
        ({
          protocol: 1,
          preflightId: id(90),
          measuredAt: timestamp,
          expiresAt: new Date(clock + 300_000).toISOString(),
          binding: input,
          firstInstall: false,
          stateBytes: 500,
          volumes: [
            { role: "state", availableBytes: 400, copyRequiredBytes: 900, cloneRequiredBytes: 400 },
          ],
          clone: { status: "proved" },
          policies: { cloneOrCopy: { admissible: false }, cloneRequired: { admissible: true } },
        }) satisfies JonesRecoveryPreflight,
    ),
    validateRecoveryReview: vi.fn(async () => {}),
    laptopEvidence: async () => evidence,
    isStaged: vi.fn(() => staged),
    download: vi.fn(async () => ({ accepted: true, completed: true })),
    stopDownload: vi.fn(async () => "stopped" as const),
    install: vi.fn(async () => ({ accepted: true, completed: false, failed: false })),
  } satisfies Pick<
    JonesDesktopUpdateController,
    | "state"
    | "busy"
    | "laptopEvidence"
    | "download"
    | "stopDownload"
    | "install"
    | "isStaged"
    | "stagedBuild"
    | "recoveryPreflight"
    | "validateRecoveryReview"
  >;
  const states: JonesInstallPlanState[] = [];
  const options = {
    store,
    controller,
    localEnvironmentId: () => laptop,
    track: <A>(work: () => Promise<A>) => work(),
    broadcast: (state: JonesInstallPlanState) => {
      states.push(state);
    },
    now: () => clock,
    uuid: () => id(++sequence),
    workerAbsent: () => false,
  };
  const coordinator = createInstallPlanCoordinator(options);
  const start = async (select = [selection]) => {
    const result = await coordinator.request({
      type: "start",
      origin: "update-button",
      target,
      select,
    });
    expect(result.refused).toBeUndefined();
    return result.state.current!;
  };
  const claim = async (planId: string, claimant = "window-a") =>
    coordinator.request({
      type: "claim",
      planId,
      environmentId: remote,
      claimant,
      step: "download",
      explicit: false,
      action: { kind: "download-exact", targetSource: source },
      expectedInstalledSource: old,
    });
  const receipt = (
    grant: JonesInstallPlanGrant,
    phase: JonesOperationReceipt["phase"] = "running",
  ): JonesOperationReceipt => {
    if (grant.request.kind !== "operation-start") throw new Error("Expected start grant.");
    return {
      protocol: 1,
      ...grant.request.input,
      phase,
      acceptedAt: timestamp,
      updatedAt: new Date(clock).toISOString(),
      ...(phase === "completed"
        ? {
            staged: {
              stagedHandle: "stage",
              sourceSha: source,
              sourceTree: tree,
              artifactId: 1,
              version: "new",
            },
          }
        : {}),
    };
  };
  return {
    coordinator,
    controller,
    options,
    states,
    start,
    claim,
    receipt,
    setEvidence: (value: {
      [K in keyof JonesInstallPlanLaptopEvidence]?: JonesInstallPlanLaptopEvidence[K] | undefined;
    }) => {
      evidence = Object.fromEntries(
        Object.entries({ ...evidence, ...value }).filter(([, v]) => v !== undefined),
      ) as JonesInstallPlanLaptopEvidence;
    },
    setStaged: () => {
      staged = true;
    },
    advance: () => {
      clock += 120_001;
    },
    snapshot: () => snapshot,
  };
}
afterEach(() => vi.restoreAllMocks());
describe("native client command reservations", () => {
  it("reconciles a lost Laptop handoff across coordinator recreation, refuses changed-body reuse and never starts twice", async () => {
    const f = fixture();
    let finish!: (value: { accepted: boolean; completed: boolean }) => void;
    let settled!: () => void;
    const terminal = new Promise<void>((resolve) => {
      settled = resolve;
    });
    f.controller.download.mockImplementation(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const native = createInstallPlanCoordinator({
      ...f.options,
      broadcast: (state) => {
        if (state.current?.laptop.operations.at(-1)?.result?.status === "completed") settled();
      },
    });
    const request = {
      type: "start",
      origin: "connections",
      target,
      select: [],
      laptop: { action: "download" },
      command: {
        commandId: id(90),
        environmentId: laptop,
        targetSource: source,
        action: "download",
      },
    } as const;
    const accepted = await native.request(request);
    expect(accepted.command?.outcome).toBe("reserved");
    expect(accepted.command?.bodyHash).toMatch(/^[a-f0-9]{64}$/);
    const restored = createInstallPlanCoordinator(f.options);
    expect((await restored.request({ type: "read", commandId: id(90) })).command?.outcome).toBe(
      "reserved",
    );
    expect((await restored.request(request)).grant).toBeUndefined();
    expect(f.controller.download).toHaveBeenCalledTimes(1);
    expect((await restored.request({ ...request, select: [selection] })).refused?.code).toBe(
      "invalid-evidence",
    );
    finish({ accepted: true, completed: true });
    await terminal;
    const proof = await restored.request({ type: "read", commandId: id(90) });
    expect(proof.command).toMatchObject({
      command: request.command,
      outcome: "terminal",
      operationId: accepted.command?.operationId,
    });
    expect(f.controller.download).toHaveBeenCalledTimes(1);
  });

  it("keeps a lost remote claim reserved until its exact target fence, then allows a new explicit identity", async () => {
    const f = fixture(),
      plan = await f.start();
    const request = {
      type: "claim",
      planId: plan.planId,
      environmentId: remote,
      claimant: "window-command",
      step: "download",
      explicit: true,
      action: { kind: "download-exact", targetSource: source },
      expectedInstalledSource: old,
      command: {
        commandId: id(91),
        environmentId: remote,
        targetSource: source,
        action: "download-exact",
        planId: plan.planId,
        expectedRevision: plan.revision,
      },
    } as const;
    const accepted = await f.coordinator.request(request);
    expect(accepted.grant).toBeDefined();
    const restored = createInstallPlanCoordinator(f.options);
    expect((await restored.request({ type: "read", commandId: id(91) })).command?.outcome).toBe(
      "reserved",
    );
    expect((await restored.request(request)).grant).toBeUndefined();
    f.advance();
    const fence = await restored.request({
      type: "claim",
      planId: plan.planId,
      environmentId: remote,
      claimant: "window-fence",
      step: "fence",
      explicit: false,
      operationId: accepted.grant!.operationId,
    });
    expect(fence.grant?.request.kind).toBe("operation-fence");
    if (fence.grant?.request.kind !== "operation-fence") throw new Error("No fence granted");
    const fenced: JonesOperationReceipt = {
      protocol: 1,
      ...fence.grant.request.input,
      phase: "fenced",
      updatedAt: "2026-10-27T00:02:00.001Z",
    };
    await restored.request({
      type: "report",
      planId: plan.planId,
      environmentId: remote,
      operationId: fenced.operationId,
      evidence: { kind: "receipt", receipt: fenced },
    });
    const proof = await restored.request({ type: "read", commandId: id(91) });
    expect(proof.command?.outcome).toBe("terminal");
    const next = await restored.request({
      ...request,
      command: {
        ...request.command,
        commandId: id(92),
        expectedRevision: proof.state.current!.revision,
      },
    });
    expect(next.grant).toBeDefined();
    expect(next.grant!.operationId).not.toBe(accepted.grant!.operationId);
  });
});
describe("native install plan coordinator", () => {
  it("requires native committed evidence; renderer reports and source equality cannot open the Laptop gate", async () => {
    const f = fixture();
    f.setEvidence({ committedTransactionId: undefined });
    const refused = await f.coordinator.request({
      type: "start",
      origin: "update-button",
      target,
      select: [selection],
    });
    expect(refused.refused?.code).toBe("laptop-action-required");
    const download = Promise.withResolvers<{ accepted: boolean; completed: boolean }>();
    f.controller.download.mockImplementation(() => download.promise);
    const started = await f.coordinator.request({
      type: "start",
      origin: "update-button",
      target,
      select: [selection],
      laptop: { action: "download" },
    });
    const planId = started.state.current!.planId;
    expect((await f.claim(planId)).refused?.code).toBe("gate-closed");
    expect(started.state.view.complete).toBe(false);
    download.resolve({ accepted: true, completed: true });
  });

  it("uses a native CAS lease across windows and resends exactly one original intent", async () => {
    const f = fixture(),
      plan = await f.start();
    const [a, b] = await Promise.all([f.claim(plan.planId), f.claim(plan.planId, "window-b")]);
    expect(a.grant).toBeDefined();
    expect(b.refused?.code).toBe("claimed");
    expect((await f.claim(plan.planId)).grant).toEqual(a.grant);
    expect(f.snapshot().document.current?.devices[0]?.operations).toHaveLength(1);
    f.setEvidence({ activeGeneration: "changed" });
    expect((await f.claim(plan.planId)).refused?.code).toBe("laptop-changed");
  });

  it("fences an expired reservation with the full original binding, never a sentinel", async () => {
    const f = fixture(),
      plan = await f.start();
    const reserved = (await f.claim(plan.planId)).grant!;
    f.advance();
    const fenced = await f.coordinator.request({
      type: "claim",
      planId: plan.planId,
      environmentId: remote,
      claimant: "window-b",
      step: "fence",
      explicit: false,
      operationId: reserved.operationId,
    });
    expect(fenced.grant?.request).toEqual({
      kind: "operation-fence",
      input: reserved.request.input,
    });
    expect(fenced.state.view.complete).toBe(false);
    const { acceptedAt: _acceptedAt, ...intent } = f.receipt(reserved);
    const tombstone: JonesOperationReceipt = {
      ...intent,
      phase: "fenced",
      updatedAt: "2026-10-27T00:02:00.001Z",
    };
    const report = await f.coordinator.request({
      type: "report",
      planId: plan.planId,
      environmentId: remote,
      operationId: reserved.operationId,
      evidence: { kind: "receipt", receipt: tombstone },
    });
    expect(report.refused).toBeUndefined();
    expect(report.state.view.complete).toBe(false);
    expect((await f.claim(plan.planId, "window-b")).grant?.operationId).not.toBe(
      reserved.operationId,
    );
  });

  it("rejects rebound, missing-correlation, regressing and forged completion receipts without advancing", async () => {
    const f = fixture(),
      plan = await f.start(),
      grant = (await f.claim(plan.planId)).grant!;
    const report = (receipt: JonesOperationReceipt) =>
      f.coordinator.request({
        type: "report",
        planId: plan.planId,
        environmentId: remote,
        operationId: grant.operationId,
        evidence: { kind: "receipt", receipt },
      });
    const running = f.receipt(grant);
    const { correlation: _correlation, ...uncorrelated } = running;
    expect((await report(uncorrelated)).refused?.code).toBe("invalid-evidence");
    expect((await report({ ...running, expectedInstalledSource: source })).refused?.code).toBe(
      "invalid-evidence",
    );
    expect((await report(running)).refused).toBeUndefined();
    expect((await report({ ...running, phase: "accepted" })).refused?.code).toBe(
      "invalid-evidence",
    );
    expect((await report({ ...running, phase: "completed" })).refused?.code).toBe(
      "invalid-evidence",
    );
    expect(
      (await report({ ...running, phase: "held", code: "unreaped" })).state.view.complete,
    ).toBe(false);
  });

  it("locks a started row against Edit and keeps an unknown reservation after restart", async () => {
    const f = fixture(),
      plan = await f.start();
    await f.claim(plan.planId);
    f.advance();
    const restarted = createInstallPlanCoordinator(f.options);
    const state = (await restarted.request({ type: "read" })).state;
    const edited = await restarted.request({
      type: "edit",
      planId: plan.planId,
      expectedRevision: state.current!.revision,
      select: [],
      deselect: [remote],
    });
    expect(edited.applied?.[0]?.result).toBe("locked-started");
    expect(
      (
        await restarted.request({
          type: "dismiss",
          planId: plan.planId,
          expectedRevision: edited.state.current!.revision,
        })
      ).refused?.code,
    ).toBe("not-complete");
  });

  it("requires a matching directive before downloads and preserves an expired directive as unknown", async () => {
    const f = fixture();
    const enrollmentId = id(99);
    const result = await f.coordinator.request({
      type: "start",
      origin: "update-button",
      target,
      select: [{ ...selection, route: "follower-operations", enrollmentId }],
    });
    const planId = result.state.current!.planId;
    expect((await f.claim(planId)).refused?.code).toBe("directive-required");
    const directive = await f.coordinator.request({
      type: "claim",
      planId,
      environmentId: remote,
      claimant: "window-a",
      step: "directive",
      explicit: false,
      enrollmentId,
      expectedDirectiveId: null,
    });
    expect(directive.grant?.request).toEqual({
      kind: "plan-directive",
      input: {
        directiveId: directive.grant!.operationId,
        enrollmentId,
        environmentId: remote,
        repository: target.repository,
        targetSource: source,
        targetTree: tree,
        expectedDirectiveId: null,
      },
    });
    f.advance();
    expect(
      (
        await f.coordinator.request({
          type: "claim",
          planId,
          environmentId: remote,
          claimant: "window-b",
          step: "fence",
          explicit: false,
          operationId: directive.grant!.operationId,
        })
      ).refused?.code,
    ).toBe("invalid-evidence");
    expect((await f.claim(planId)).state.view.complete).toBe(false);
  });

  it("keeps a completed download open until ended and retains publication suppression after Done", async () => {
    const f = fixture(),
      plan = await f.start(),
      grant = (await f.claim(plan.planId)).grant!;
    const staged = await f.coordinator.request({
      type: "report",
      planId: plan.planId,
      environmentId: remote,
      operationId: grant.operationId,
      evidence: { kind: "receipt", receipt: f.receipt(grant, "completed") },
    });
    expect(staged.state.view.complete).toBe(false);
    const ended = await f.coordinator.request({
      type: "end",
      planId: plan.planId,
      environmentId: remote,
      expectedRevision: staged.state.current!.revision,
    });
    expect(ended.state.view.complete).toBe(true);
    const dismissed = await f.coordinator.request({
      type: "dismiss",
      planId: plan.planId,
      expectedRevision: ended.state.current!.revision,
    });
    expect(dismissed.state.current).toBeNull();
    expect(dismissed.state.last?.dismissedAt).toBeDefined();
    expect((await f.coordinator.request({ type: "publication" })).publication).toMatchObject({
      kind: "withheld",
      code: "plan-managed",
    });
    expect(await createInstallPlanCoordinator(f.options).isPlanManaged(source)).toBe(true);
  });

  it("refuses unreviewed installs even when a download has completed", async () => {
    const f = fixture(),
      plan = await f.start(),
      grant = (await f.claim(plan.planId)).grant!;
    await f.coordinator.request({
      type: "report",
      planId: plan.planId,
      environmentId: remote,
      operationId: grant.operationId,
      evidence: { kind: "receipt", receipt: f.receipt(grant, "completed") },
    });
    const result = await f.coordinator.request({
      type: "claim",
      planId: plan.planId,
      environmentId: remote,
      claimant: "window-a",
      step: "install",
      explicit: true,
      action: {
        kind: "install",
        stagedHandle: "stage",
        currentVersion: "old",
        targetSource: source,
      },
      expectedInstalledSource: old,
    });
    expect(result.refused?.code).toBe("invalid-evidence");
    expect(result.state.view.complete).toBe(false);
  });

  it("accepts a native install binding before handoff and settles only its exact native outcome", async () => {
    const f = fixture();
    f.setStaged();
    f.setEvidence({ activeSource: old, committedTransactionId: undefined });
    const review: JonesInstallPlanReview = {
      stagedHandle: "stage",
      targetSource: source,
      currentVersion: "old",
      expectedInstalledSource: old,
      reviewedAt: timestamp,
    };
    const started = await f.coordinator.request({
      type: "start",
      origin: "update-button",
      target,
      select: [],
      laptop: { action: "install", review },
    });
    const operationId = started.state.current!.laptop.operations[0]!.operationId;
    const binding = {
      operationId,
      stagedHandle: "stage",
      targetSource: source,
      targetTree: tree,
      transactionId: transaction,
      fromGeneration: "previous",
    };
    await expect(
      f.coordinator.bindLaptopInstall({ ...binding, targetTree: old }),
    ).rejects.toThrow();
    await f.coordinator.bindLaptopInstall(binding);
    await f.coordinator.recordLaptopOutcome({
      transactionId: "unrelated",
      status: "committed",
      activeGeneration: transaction,
      activeSource: source,
      activeTree: tree,
    });
    expect((await f.coordinator.request({ type: "read" })).state.view.complete).toBe(false);
    await f.coordinator.recordLaptopOutcome({
      transactionId: transaction,
      status: "committed",
      activeGeneration: transaction,
      activeSource: source,
      activeTree: tree,
    });
    f.setEvidence({ activeSource: source, committedTransactionId: transaction });
    const complete = await f.coordinator.request({ type: "read" });
    expect(complete.state.current?.laptop.gate?.basis).toBe("plan-install");
    expect(complete.state.view.complete).toBe(true);
  });

  it("holds a lost Laptop download across restart without treating absent payload as exit proof", async () => {
    const f = fixture();
    f.setEvidence({ activeSource: old, committedTransactionId: undefined });
    const pending = Promise.withResolvers<{ accepted: boolean; completed: boolean }>();
    f.controller.download.mockImplementation(() => pending.promise);
    await f.coordinator.request({
      type: "start",
      origin: "update-button",
      target,
      select: [],
      laptop: { action: "download" },
    });
    const restarted = createInstallPlanCoordinator(f.options);
    const result = await restarted.request({ type: "read" });
    expect(result.state.current?.laptop.operations[0]?.result).toMatchObject({
      status: "held",
      code: "preparation-unconfirmed",
    });
    expect(result.state.view.complete).toBe(false);
    pending.resolve({ accepted: true, completed: false });
  });
});

it("releases only a captured transfer with positive reap proof and preserves admitted copy holds", async () => {
  for (const admitted of [false, true]) {
    const f = fixture();
    f.setEvidence({ activeSource: old, committedTransactionId: undefined });
    const pending = Promise.withResolvers<{ accepted: boolean; completed: boolean }>();
    f.controller.download.mockImplementation(() => pending.promise);
    const started = await f.coordinator.request({
      type: "start",
      origin: "update-button",
      target,
      select: [],
      laptop: { action: "download" },
    });
    const operationId = started.state.current!.laptop.operations[0]!.operationId;
    await f.coordinator.recordLaptopWorker({ operationId, pid: 4242, processGroup: 4242 });
    if (admitted) await f.coordinator.recordLaptopAdmission(operationId);
    const restarted = createInstallPlanCoordinator({ ...f.options, workerAbsent: () => true });
    const state = (await restarted.request({ type: "read" })).state;
    expect(state.current?.laptop.operations[0]?.result?.status).toBe(
      admitted ? "held" : "interrupted",
    );
    expect(state.view.complete).toBe(!admitted);
    expect(await restarted.preparationHeld()).toBe(admitted);
    expect(await restarted.backgroundSuppressed(source)).toBe(true);
    pending.resolve({ accepted: true, completed: false });
  }
});

it("forwards native preflight and the identical reviewed opaque proof to the bound Laptop install", async () => {
  const f = fixture();
  f.setStaged();
  const native = f.controller.state.jones!;
  f.controller.state = {
    ...f.controller.state,
    jones: {
      ...native,
      capability: { ...native.capability, recoveryPolicy: 1 },
      installedSource: { repository: target.repository, sha: old, tree },
      stagedHandle: "stage",
    },
  };
  const plan = await f.start();
  const inspected = await f.coordinator.request({
    type: "laptop",
    planId: plan.planId,
    action: "preflight",
  });
  expect(inspected.preflight?.preflightId).toBe(id(90));
  expect(f.controller.recoveryPreflight).toHaveBeenCalledWith({
    environmentId: laptop,
    stagedHandle: "stage",
    currentVersion: "old",
    expectedInstalledSource: old,
  });
  const recovery = {
    policy: "clone-required",
    preflightId: inspected.preflight!.preflightId,
  } as const;
  const review = {
    reviewedAt: timestamp,
    stagedHandle: "stage",
    targetSource: source,
    currentVersion: "old",
    expectedInstalledSource: old,
    recovery,
  };
  const result = await f.coordinator.request({
    type: "laptop",
    planId: plan.planId,
    action: "install",
    review,
  });
  expect(result.refused).toBeUndefined();
  const operationId = result.state.current!.laptop.operations.at(-1)!.operationId;
  expect(f.controller.validateRecoveryReview).toHaveBeenCalledWith(
    inspected.preflight!.binding,
    recovery,
  );
  await vi.waitFor(() =>
    expect(f.controller.install).toHaveBeenCalledWith("stage", undefined, {
      operationId,
      recovery,
    }),
  );
});

it("does not reserve or dispatch an install when native recovery proof refuses", async () => {
  const { RecoveryPreflightRefused } = await import("../updates/recoveryPreflight.ts");
  const f = fixture();
  f.setStaged();
  const plan = await f.start();
  f.controller.validateRecoveryReview.mockRejectedValueOnce(
    new RecoveryPreflightRefused(
      "recovery-preflight-stale",
      "Native generation changed. Review again.",
    ),
  );
  const result = await f.coordinator.request({
    type: "laptop",
    planId: plan.planId,
    action: "install",
    review: {
      reviewedAt: timestamp,
      stagedHandle: "stage",
      targetSource: source,
      currentVersion: "old",
      expectedInstalledSource: old,
      recovery: { policy: "clone-required", preflightId: id(90) },
    },
  });
  expect(result.refused).toMatchObject({
    code: "invalid-evidence",
    message: expect.stringContaining("recovery-preflight-stale"),
  });
  expect(result.state.current!.laptop.operations).toHaveLength(0);
  expect(f.controller.install).not.toHaveBeenCalled();
});

it("reviews a cold native stage before start, then uses the same proof without inventing a plan", async () => {
  const f = fixture();
  f.setStaged();
  f.setEvidence({ committedTransactionId: undefined, activeSource: old, activeVersion: "old" });
  const input = {
    environmentId: laptop,
    stagedHandle: "stage",
    currentVersion: "old",
    expectedInstalledSource: old,
  };
  const before = structuredClone(f.snapshot());
  const inspected = await f.coordinator.request({ type: "recovery-preflight", input });
  expect(inspected.preflight?.binding).toEqual(input);
  expect(inspected.state.current).toBeNull();
  expect(f.snapshot()).toEqual(before);
  expect(f.states).toHaveLength(0);
  expect(f.controller.install).not.toHaveBeenCalled();
  const recovery = {
    policy: "clone-required",
    preflightId: inspected.preflight!.preflightId,
  } as const;
  const result = await f.coordinator.request({
    type: "start",
    origin: "update-button",
    target,
    select: [],
    laptop: {
      action: "install",
      review: { ...input, targetSource: source, reviewedAt: timestamp, recovery },
    },
  });
  expect(result.refused).toBeUndefined();
  const operationId = result.state.current!.laptop.operations.at(-1)!.operationId;
  expect(f.controller.validateRecoveryReview).toHaveBeenCalledWith(input, recovery);
  await vi.waitFor(() =>
    expect(f.controller.install).toHaveBeenCalledWith("stage", undefined, {
      operationId,
      recovery,
    }),
  );
});

it("independent preflight leaves an existing plan and dispositions byte-for-byte unchanged", async () => {
  const f = fixture();
  await f.start();
  const before = structuredClone(f.snapshot());
  const result = await f.coordinator.request({
    type: "recovery-preflight",
    input: {
      environmentId: laptop,
      stagedHandle: "stage",
      currentVersion: "old",
      expectedInstalledSource: old,
    },
  });
  expect(result.preflight).toBeDefined();
  expect(f.snapshot()).toEqual(before);
  const refused = await f.coordinator.request({
    type: "recovery-preflight",
    input: {
      environmentId: remote,
      stagedHandle: "stage",
      currentVersion: "old",
      expectedInstalledSource: old,
    },
  });
  expect(refused.refused?.code).toBe("invalid-evidence");
  expect(f.controller.recoveryPreflight).toHaveBeenCalledTimes(1);
});
