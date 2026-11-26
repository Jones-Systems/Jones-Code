// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Deterministic synthetic native and host evidence; no real processes or install effects.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { it as effectIt } from "@effect/vitest";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, type DesktopUpdateState } from "@t3tools/contracts";
import type { FleetPlanDirectiveFenceEvidence } from "@t3tools/contracts/jones/fleet-updates";
import { JONES_INSTALL_PLAN_CLI_RESPONSE_BYTES } from "@t3tools/contracts/jones/installPlan";
import type {
  JonesInstallPlanCliRequest,
  JonesInstallPlanLaptopEvidence,
  JonesInstallPlanSelection,
  JonesInstallPlanState,
  JonesInstallPlanRequest,
  JonesInstallPlanReview,
  JonesInstallPlanGrant,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesRecoveryPreflight,
  JonesOperationReceipt,
} from "@t3tools/contracts/jones/jonesOperations";
import * as Effect from "effect/Effect";
import type { JonesDesktopUpdateController } from "../updates/JonesDesktopUpdates.ts";
import { dispatchInstallPlanCli } from "./cliControl.ts";
import { createInstallPlanCoordinator } from "./coordinator.ts";
import { createInstallPlanStore, type InstallPlanStore, type PlanSnapshot } from "./store.ts";

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
const persistentRoots = new Set<string>();
function fixture(persistentStore?: InstallPlanStore) {
  let clock = Date.parse(timestamp),
    sequence = 0;
  let snapshot: PlanSnapshot = {
    document: { schema: 1, current: null, last: null },
    dispositions: { schema: 1, targets: [] },
  };
  let interceptWrite: ((commit: () => void) => Promise<void>) | undefined;
  let queue: Promise<unknown> = Promise.resolve();
  const store: InstallPlanStore = {
    transact: (work) => {
      const pending = queue.then(async () => {
        const next = await work(snapshot);
        if (next.document !== snapshot.document || next.dispositions !== snapshot.dispositions) {
          const commit = () => {
            snapshot = { ...snapshot, document: next.document, dispositions: next.dispositions };
          };
          const intercept = interceptWrite;
          interceptWrite = undefined;
          if (intercept === undefined) commit();
          else await intercept(commit);
        }
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
          version: target.version,
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
    laptopEvidence: vi.fn(async () => evidence),
    retiredLaptopInstall: vi.fn(async () => false),
    discard: vi.fn(async (_handle: string) => ({ accepted: true, completed: true })),
    laptopStageSelectionName: vi.fn(async (_handle: string) => "selection.json"),
    isLaptopStageAbsent: vi.fn(async (_name: string, _handle: string) => false),
    isStaged: vi.fn(() => staged),
    download: vi.fn(async (): ReturnType<JonesDesktopUpdateController["download"]> => ({
      accepted: true,
      completed: true,
    })),
    stopDownload: vi.fn(async () => "stopped" as const),
    install: vi.fn(
      async (
        _handle?: string,
        _campaign?: string,
        _plan?: {
          readonly operationId: string;
          readonly recovery?: JonesInstallPlanReview["recovery"];
        },
      ) => ({ accepted: true, completed: false, failed: false }),
    ),
  } satisfies Pick<
    JonesDesktopUpdateController,
    | "state"
    | "busy"
    | "laptopEvidence"
    | "retiredLaptopInstall"
    | "discard"
    | "laptopStageSelectionName"
    | "isLaptopStageAbsent"
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
    store: persistentStore ?? store,
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
  const start = async (select: readonly JonesInstallPlanSelection[] = [selection]) => {
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
    setStaged: (value = true) => {
      staged = value;
    },
    interceptNextWrite: (intercept: (commit: () => void) => Promise<void>) => {
      interceptWrite = intercept;
    },
    advance: () => {
      clock += 120_001;
    },
    snapshot: () => snapshot,
  };
}
afterEach(async () => {
  vi.restoreAllMocks();
  for (const home of persistentRoots) {
    await NodeFSP.rm(home, { recursive: true, force: true });
    persistentRoots.delete(home);
  }
});
it("keeps ordinary Done separate from closing a plan superseded by a manual install", async () => {
  const f = fixture();
  const plan = await f.start();
  const grant = (await f.claim(plan.planId)).grant!;
  const nextGeneration = "1".repeat(64);
  f.setEvidence({
    activeGeneration: nextGeneration,
    activeSource: old,
    activeVersion: "manual build",
    committedTransactionId: undefined,
  });
  const observed = (await f.coordinator.request({ type: "read" })).state;
  expect(observed.current?.laptop.gateClosed?.reason).toBe("changed");
  const done = await f.coordinator.request({
    type: "dismiss",
    planId: plan.planId,
    expectedRevision: observed.current!.revision,
  });
  expect(done.refused?.code).toBe("not-complete");
  expect(done.state.current?.planId).toBe(plan.planId);
  const retained = f.snapshot().dispositions;
  const publication = { type: "publication", acceptOrdinaryRecovery: 1 } as const;
  const before = (await f.coordinator.request(publication)).publication;
  const close = {
    type: "close-after-manual-install",
    planId: plan.planId,
    expectedRevision: observed.current!.revision,
    ...observed.view.manualClose!,
    devices: [{ environmentId: remote, kind: "unverified", observedAt: Date.parse(timestamp) }],
  } as const;
  const receipt = f.receipt(grant, "completed");
  expect(
    (await f.coordinator.request({ ...close, expectedRevision: plan.revision })).refused?.code,
  ).toBe("stale-revision");
  expect((await f.coordinator.request({ ...close, devices: [] })).refused?.code).toBe(
    "invalid-evidence",
  );
  expect(
    (await f.coordinator.request({ ...close, activeGeneration: transaction })).refused?.code,
  ).toBe("invalid-evidence");
  f.advance();
  expect((await f.coordinator.request(close)).refused?.code).toBe("invalid-evidence");
  const fresh = { ...close, devices: [{ ...close.devices[0], observedAt: f.options.now() }] };
  const closed = await f.coordinator.request(fresh);
  expect(closed.refused).toBeUndefined();
  expect(closed.state.current).toBeNull();
  expect(closed.state.last).toMatchObject({
    planId: plan.planId,
    manualClosure: {
      activeGeneration: nextGeneration,
      laptop: close.laptop,
      devices: fresh.devices,
    },
    devices: [{ operations: [{ operationId: grant.operationId }] }],
  });
  expect(closed.state.last?.devices[0]?.operations[0]?.receipt).toBeUndefined();
  expect(closed.state.recoveryHistory?.[0]?.pendingDevices).toEqual([
    { environmentId: remote, label: "Host" },
  ]);
  const reported = await f.coordinator.request({
    type: "report",
    planId: plan.planId,
    environmentId: remote,
    operationId: grant.operationId,
    evidence: { kind: "receipt", receipt },
  });
  expect(reported.state.last?.devices[0]?.operations[0]?.receipt).toEqual(receipt);
  expect(reported.state.last?.manualClosure).toEqual(closed.state.last?.manualClosure);
  expect(f.snapshot().dispositions).toEqual(retained);
  expect((await f.coordinator.request(publication)).publication).toEqual(before);
  expect((await f.claim(plan.planId)).refused?.code).toBe("plan-mismatch");
  f.setEvidence({
    ordinaryRecovery: {
      activeGeneration: nextGeneration,
      transactionId: `manual-${nextGeneration}`,
      manifestSha256: "2".repeat(64),
      receiptSha256: "3".repeat(64),
    },
  });
  const next = await f.coordinator.request({
    type: "start",
    origin: "update-button",
    target: { ...target, sourceSha: old, version: close.laptop.version },
    select: [],
  });
  expect(next.refused).toBeUndefined();
  expect(next.state.current?.planId).not.toBe(plan.planId);
});

describe("closed plan recovery", () => {
  const peer = { ...selection, environmentId: EnvironmentId.make("peer"), label: "Peer" };
  const downloadRequest = (planId: string) =>
    ({
      type: "claim",
      planId,
      environmentId: remote,
      claimant: "window-a",
      step: "download",
      explicit: false,
      action: { kind: "download-exact", targetSource: source },
      expectedInstalledSource: old,
    }) as const;
  const cancelRequest = (plan: { planId: string; revision: number }) =>
    ({
      type: "cancel-plan",
      planId: plan.planId,
      expectedRevision: plan.revision,
    }) as const;
  const reportRequest = (planId: string, receipt: JonesOperationReceipt) =>
    ({
      type: "report",
      planId,
      environmentId: receipt.environmentId,
      operationId: receipt.operationId,
      evidence: { kind: "receipt", receipt },
    }) as const;

  it.each(["claim", "close"])(
    "serializes %s first and gives no forward grant after closure",
    async (first) => {
      const f = fixture();
      const plan = await f.start();
      const cancel = () => f.coordinator.request(cancelRequest(plan));
      const claim = () => f.claim(plan.planId);
      if (first === "close") {
        const [closed, denied] = await Promise.all([cancel(), claim()]);
        expect(closed.state.last?.devices[0]?.operations).toEqual([]);
        expect(denied.refused?.code).toBe("plan-mismatch");
        expect(denied.grant).toBeUndefined();
        return;
      }
      const [issued, denied] = await Promise.all([claim(), cancel()]);
      expect(issued.grant).toBeDefined();
      expect(denied.refused?.code).toBe("stale-revision");
      const closed = await f.coordinator.request(cancelRequest(issued.state.current!));
      const grant = issued.grant!;
      expect(
        (
          await f.coordinator.request({
            ...downloadRequest(plan.planId),
            operationId: grant.operationId,
          })
        ).grant,
      ).toBeUndefined();
      const late = f.receipt(grant, "running");
      const received = await f.coordinator.request(reportRequest(plan.planId, late));
      expect(received.state.current).toBeNull();
      expect(received.state.last?.devices[0]?.operations[0]?.receipt).toEqual(late);
      expect(received.state.documentRevision).toBe(closed.state.documentRevision! + 1);
    },
  );

  it("gives an exact reclaim only its outstanding reservation and saves no lease on refusal", async () => {
    const f = fixture();
    const plan = await f.start([selection, peer]);
    const forward = downloadRequest(plan.planId);
    const refuseWithoutWrite = async (claim: JonesInstallPlanRequest) => {
      const before = f.snapshot();
      const result = await f.coordinator.request(claim);
      expect(result.refused).toBeDefined();
      expect(result.grant).toBeUndefined();
      expect(f.snapshot()).toBe(before);
    };
    await refuseWithoutWrite({ ...forward, operationId: id(103) });
    const operation = (await f.coordinator.request(forward)).grant!;
    const reclaim = { ...forward, operationId: operation.operationId };
    await refuseWithoutWrite({ ...reclaim, expectedInstalledSource: source });
    await refuseWithoutWrite({ ...reclaim, step: "install" });
    await refuseWithoutWrite({ ...reclaim, environmentId: peer.environmentId });
    await refuseWithoutWrite({ ...reclaim, planId: id(104) });
    await refuseWithoutWrite({ ...reclaim, claimant: "other-window" });
    const reserved = f.snapshot();
    expect((await f.coordinator.request(reclaim)).grant).toEqual(operation);
    expect(f.snapshot()).toBe(reserved);

    await f.coordinator.request(reportRequest(plan.planId, f.receipt(operation, "completed")));
    await refuseWithoutWrite({ ...reclaim, explicit: true });

    const peerClaim = { ...forward, environmentId: peer.environmentId };
    const peerOperation = (await f.coordinator.request(peerClaim)).grant!;
    f.advance();
    expect(
      (
        await f.coordinator.request({
          ...peerClaim,
          step: "fence",
          explicit: true,
          operationId: peerOperation.operationId,
        })
      ).grant,
    ).toBeDefined();
    await refuseWithoutWrite({ ...peerClaim, operationId: peerOperation.operationId });
    const { acceptedAt: _acceptedAt, ...fenced } = f.receipt(peerOperation, "fenced");
    await f.coordinator.request(reportRequest(plan.planId, fenced));
    await refuseWithoutWrite({ ...peerClaim, operationId: peerOperation.operationId });
  });

  it.each(["fenced", "running"] as const)(
    "fences an archived unseen intent and records the host's %s result",
    async (phase) => {
      const f = fixture();
      const plan = await f.start();
      const initial = await f.coordinator.request(downloadRequest(plan.planId));
      const operation = initial.grant!;
      const reclaim = { ...downloadRequest(plan.planId), operationId: operation.operationId };
      expect(
        (await f.coordinator.request({ ...reclaim, operationId: id(103) })).grant,
      ).toBeUndefined();
      expect((await f.coordinator.request(reclaim)).grant).toEqual(operation);
      const closed = await f.coordinator.request(cancelRequest(initial.state.current!));
      const fence = {
        type: "claim",
        planId: plan.planId,
        environmentId: remote,
        claimant: "owner",
        step: "fence",
        explicit: true,
        operationId: operation.operationId,
        expectedRevision: closed.state.last!.revision,
      } as const;
      expect((await f.coordinator.request(fence)).refused?.code).toBe("outstanding-operation");
      f.advance();
      expect((await f.coordinator.request({ ...fence, explicit: false })).grant).toBeUndefined();
      expect(
        (await f.coordinator.request({ ...fence, expectedRevision: plan.revision })).refused?.code,
      ).toBe("stale-revision");
      const issued = await f.coordinator.request(fence);
      expect(issued.grant?.request).toEqual({
        kind: "operation-fence",
        input: operation.request.input,
      });
      const { acceptedAt, ...receipt } = f.receipt(operation, phase);
      const recorded = await f.coordinator.request(
        reportRequest(plan.planId, {
          ...receipt,
          ...(phase === "running" ? { acceptedAt: acceptedAt! } : {}),
        }),
      );
      expect(recorded.state.last?.devices[0]?.operations[0]?.receipt?.phase).toBe(phase);
      if (phase === "fenced") expect(recorded.state.recoveryHistory).toEqual([]);
      else {
        const stop = await f.coordinator.request({
          ...fence,
          step: "stop",
          expectedRevision: recorded.state.last!.revision,
        });
        expect(stop.grant?.request).toMatchObject({
          kind: "operation-stop",
          input: { operationId: operation.operationId, environmentId: remote },
        });
      }
      expect((await f.coordinator.request(reclaim)).grant).toBeUndefined();
    },
  );

  it.each(["fenced", "applied", "superseded"] as const)(
    "recovers a captured follower setup after closure with exact %s proof",
    async (outcome) => {
      const f = fixture();
      const enrollmentId = id(106);
      const follower = { ...selection, route: "follower-operations", enrollmentId } as const;
      const plan = await f.start([follower]);
      const claim = {
        type: "claim",
        planId: plan.planId,
        environmentId: remote,
        step: "directive",
        explicit: false,
        claimant: "window-a",
        enrollmentId,
        expectedDirectiveId: id(107),
      } as const;
      const reserved = await f.coordinator.request(claim);
      const grant = reserved.grant!;
      if (grant.request.kind !== "plan-directive") throw new Error("Expected setup grant.");
      const input = grant.request.input;
      const start = { type: "start", origin: "settings", target, select: [follower] } as const;
      const closed = await f.coordinator.request(cancelRequest(reserved.state.current!));
      expect(closed.state.recoveryHistory?.[0]?.pendingDevices).toEqual([
        { environmentId: remote, label: "Host" },
      ]);
      expect(
        (await f.coordinator.request({ ...claim, operationId: grant.operationId })).grant,
      ).toBeUndefined();
      expect((await f.coordinator.request(start)).refused?.code).toBe("outstanding-operation");
      const fence = {
        type: "claim",
        planId: plan.planId,
        environmentId: remote,
        claimant: "owner",
        step: "fence",
        explicit: true,
        operationId: grant.operationId,
        enrollmentId,
        expectedRevision: closed.state.last!.revision,
      } as const;
      const proof: FleetPlanDirectiveFenceEvidence =
        outcome === "applied"
          ? {
              protocol: 1,
              input,
              outcome,
              directive: {
                directiveId: input.directiveId,
                targetSource: input.targetSource,
                targetTree: input.targetTree,
                mode: "manual-review",
                issuedAt: timestamp,
              },
            }
          : { protocol: 1, input, outcome };
      const report = (expectedRevision: number, evidence = proof) =>
        f.coordinator.request({
          type: "report",
          planId: plan.planId,
          environmentId: remote,
          operationId: grant.operationId,
          evidence: { kind: "directive-fence", expectedRevision, proof: evidence },
        });
      expect((await report(fence.expectedRevision)).refused?.code).toBe("invalid-evidence");
      expect(
        (await f.coordinator.request({ ...fence, enrollmentId: id(108) })).grant,
      ).toBeUndefined();
      const issued = await f.coordinator.request(fence);
      const revision = issued.state.last!.revision;
      expect(issued.grant?.request).toEqual({
        kind: "plan-directive-fence",
        input,
        expectedRevision: revision,
      });
      expect(issued.grant?.leaseUntil).toBe(grant.leaseUntil); // The captured forward lease is still live.
      expect((await f.coordinator.request({ ...fence, expectedRevision: revision })).grant).toEqual(
        issued.grant,
      );
      expect((await report(fence.expectedRevision)).refused?.code).toBe("stale-revision");
      expect(
        (await report(revision, { ...proof, input: { ...input, expectedDirectiveId: null } }))
          .refused?.code,
      ).toBe("invalid-evidence");
      expect(
        (
          await f.coordinator.request({
            type: "report",
            planId: plan.planId,
            environmentId: remote,
            operationId: grant.operationId,
            evidence: { kind: "directive-refused", reason: "conflict", message: "Host busy." },
          })
        ).refused?.code,
      ).toBe("invalid-evidence");
      expect((await f.coordinator.request(start)).refused?.code).toBe("outstanding-operation");
      const saved = await report(revision);
      expect(saved.refused).toBeUndefined();
      const operation = saved.state.last?.devices[0]?.operations[0];
      expect(operation?.request).toEqual(grant.request);
      expect(
        outcome === "applied" ? operation?.directive?.kind : operation?.directiveFence?.outcome,
      ).toBe(outcome);
      expect(saved.state.recoveryHistory).toEqual([]);
      expect((await f.coordinator.request(start)).state.current?.planId).not.toBe(plan.planId);
    },
  );

  it("keeps A and its command after B closes and C starts, with holds scoped by device across source and route changes", async () => {
    const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-plan-restart-"));
    persistentRoots.add(home);
    const binding = { home, profile: undefined };
    const f = fixture(createInstallPlanStore(binding));
    const a = await f.start();
    const request = {
      ...downloadRequest(a.planId),
      explicit: true,
      command: {
        commandId: id(105),
        environmentId: remote,
        targetSource: source,
        action: "download-exact",
        planId: a.planId,
        expectedRevision: a.revision,
      },
    } as const;
    const reserved = await f.coordinator.request(request);
    const held = {
      ...f.receipt(reserved.grant!),
      phase: "held",
      code: "cleanup-unconfirmed",
      jobId: id(106),
    } as const;
    const aHeld = await f.coordinator.request(reportRequest(a.planId, held));
    await f.coordinator.request(cancelRequest(aHeld.state.current!));
    const changedRoute = {
      ...selection,
      route: "follower-operations",
      enrollmentId: id(107),
    } as const;
    const b = await f.coordinator.request({
      type: "start",
      origin: "settings",
      target,
      select: [changedRoute, peer],
    });
    expect(b.state.current?.devices.map((device) => device.environmentId)).toEqual([
      peer.environmentId,
    ]);
    expect(b.applied).toMatchObject([{ environmentId: remote, result: "not-selectable" }]);
    const excluded = await f.coordinator.request({
      type: "edit",
      planId: b.state.current!.planId,
      expectedRevision: b.state.current!.revision,
      select: [selection],
      deselect: [],
    });
    expect(excluded.applied).toMatchObject([{ environmentId: remote, result: "not-selectable" }]);
    const closedB = await f.coordinator.request(cancelRequest(b.state.current!));
    const anotherSource = "e".repeat(40);
    f.setEvidence({ activeSource: anotherSource });
    const c = await f.coordinator.request({
      type: "start",
      origin: "settings",
      target: { ...target, sourceSha: anotherSource },
      select: [changedRoute, peer],
    });
    expect(c.state.last?.planId).toBe(closedB.state.last?.planId);
    expect(c.state.current?.devices.map((device) => device.environmentId)).toEqual([
      peer.environmentId,
    ]);
    const restarted = createInstallPlanCoordinator({
      ...f.options,
      store: createInstallPlanStore(binding),
    });
    const detail = await restarted.request({ type: "read-history", planId: a.planId });
    expect(detail.historyPlan?.devices[0]?.operations[0]?.receipt).toEqual(held);
    expect(detail.state.documentRevision).toBe(c.state.documentRevision);
    const heldCommand = await restarted.request(request);
    expect(heldCommand.command?.outcome).toBe("reserved");
    expect(heldCommand.grant).toBeUndefined();
    const cleanup = { ...held, phase: "interrupted", code: "cleanup-recovered" } as const;
    expect(
      (await restarted.request(reportRequest(a.planId, { ...cleanup, jobId: id(109) }))).refused
        ?.code,
    ).toBe("invalid-evidence");
    expect(
      (
        await restarted.request(
          reportRequest(a.planId, { ...cleanup, environmentId: peer.environmentId }),
        )
      ).refused,
    ).toBeDefined();
    expect(
      (await restarted.request(reportRequest(c.state.current!.planId, cleanup))).refused,
    ).toBeDefined();
    const settled = await restarted.request(reportRequest(a.planId, cleanup));
    expect(settled.state.current).toEqual(c.state.current);
    expect(settled.state.last).toEqual(c.state.last);
    expect(settled.state.recoveryHistory).toEqual([]);
    expect((await restarted.request({ type: "read", commandId: id(105) })).command).toMatchObject({
      planId: a.planId,
      operationId: reserved.grant!.operationId,
      outcome: "terminal",
      code: "cleanup-recovered",
    });
    expect((await restarted.request(reportRequest(a.planId, held))).refused?.code).toBe(
      "invalid-evidence",
    );
    const edited = await restarted.request({
      type: "edit",
      planId: c.state.current!.planId,
      expectedRevision: c.state.current!.revision,
      select: [changedRoute],
      deselect: [],
    });
    expect(edited.applied).toMatchObject([{ environmentId: remote, result: "added" }]);
  });

  it("retains all eight unresolved plans and allows closure before refusing an additional slot", async () => {
    const f = fixture();
    const archived: { planId: string; receipt: JonesOperationReceipt }[] = [];
    for (let n = 0; n < 8; n++) {
      const environmentId = EnvironmentId.make(`peer-${n}`);
      const plan = await f.start([{ ...peer, environmentId }]);
      const issued = await f.coordinator.request({
        ...downloadRequest(plan.planId),
        environmentId,
      });
      expect(issued.grant).toBeDefined();
      archived.push({ planId: plan.planId, receipt: f.receipt(issued.grant!, "failed") });
      const closed = await f.coordinator.request(cancelRequest(issued.state.current!));
      expect(closed.refused).toBeUndefined();
      expect(closed.state.recoveryHistory).toHaveLength(n + 1);
    }
    const start = { type: "start", origin: "settings", target, select: [peer] } as const;
    const next = await f.coordinator.request(start);
    expect(next.refused?.code).toBe("bounds");
    expect(next.state.last?.planId).toBe(archived.at(-1)?.planId);
    expect(next.state.recoveryHistory).toHaveLength(8);
    const first = archived[0]!;
    expect(
      (await f.coordinator.request(reportRequest(first.planId, first.receipt))).refused,
    ).toBeUndefined();
    const admitted = await f.coordinator.request(start);
    expect(admitted.refused).toBeUndefined();
    expect(admitted.state.last?.planId).toBe(archived.at(-1)?.planId);
    expect(admitted.state.recoveryHistory).toHaveLength(7);
    expect(
      (await f.coordinator.request({ type: "read-history", planId: first.planId })).refused?.code,
    ).toBe("plan-mismatch");
    expect(f.snapshot().dispositions.targets.map(({ targetSource }) => targetSource)).toEqual([
      source,
    ]);
  });

  effectIt.effect("reads back one lost CLI cancel after the next plan closes", () =>
    Effect.gen(function* () {
      const f = fixture();
      const reviewed = yield* Effect.promise(async () => {
        const a = await f.start();
        const grant = await f.claim(a.planId);
        const cancel = cancelRequest(grant.state.current!);
        await f.coordinator.request(cancel); // The caller loses the original reply.
        const b = await f.start([peer]);
        await f.coordinator.request(cancelRequest(b));
        await f.start([peer]);
        return cancel;
      });
      const restarted = createInstallPlanCoordinator(f.options);
      const send = (expectedRevision: number, environmentId = laptop) =>
        dispatchInstallPlanCli(
          {
            version: 1,
            requestId: id(121),
            type: "jones-install-plan",
            action: { ...reviewed, expectedRevision, environmentId, confirmed: true },
          },
          (input) => Effect.promise(() => restarted.request(input)),
        );
      expect(yield* send(reviewed.expectedRevision)).toMatchObject({
        ok: true,
        result: { historyPlan: { planId: reviewed.planId, cancelled: true } },
      });
      expect(yield* send(reviewed.expectedRevision + 1)).toMatchObject({
        ok: true,
        result: { refused: { code: "stale-revision" } },
      });
      expect(yield* send(reviewed.expectedRevision, remote)).toMatchObject({
        ok: false,
        code: "environment-mismatch",
      });
    }),
  );

  it("leaves room for closing and growing a legal receipt near the forward admission budget", async () => {
    const f = fixture();
    const nearBudget = { ...target, version: "v".repeat(1_752_000) };
    const start = await f.coordinator.request({
      type: "start",
      origin: "update-button",
      target: nearBudget,
      select: [selection, peer],
    });
    const active = start.state.current!;
    const first = await f.coordinator.request(downloadRequest(active.planId));
    expect(first.grant).toBeDefined();
    const excess = await f.coordinator.request({
      ...downloadRequest(active.planId),
      environmentId: peer.environmentId,
    });
    expect(excess.refused?.code).toBe("bounds");
    expect(excess.grant).toBeUndefined();
    const closed = await f.coordinator.request(cancelRequest(first.state.current!));
    expect(closed.refused).toBeUndefined();
    const receipt = { ...f.receipt(first.grant!, "completed"), message: "\u0000".repeat(1024) };
    const late = await f.coordinator.request(reportRequest(active.planId, receipt));
    expect(late.refused).toBeUndefined();
    expect(late.state.last?.devices[0]?.operations[0]?.receipt?.message?.length).toBe(1024);
    expect(late.state.recoveryHistory).toEqual([]);
  });

  effectIt.effect(
    "preserves the exact detail when current, latest and history exceed the CLI frame together",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const plan = yield* Effect.promise(() => f.start());
        const current = { ...plan, from: { version: "é".repeat(490_000) } };
        const last = { ...current, planId: id(121), dismissedAt: timestamp };
        const older = { ...current, planId: id(122), dismissedAt: timestamp };
        const base = f.states.at(-1)!;
        const read = (planId: string) =>
          dispatchInstallPlanCli(
            {
              version: 1,
              requestId: id(123),
              type: "jones-install-plan",
              action: { type: "read", planId },
            },
            (request) =>
              Effect.succeed({
                state: { ...base, current, last },
                ...(request.type === "read-history" ? { historyPlan: older } : {}),
              }),
          );
        const latest = yield* read(last.planId);
        expect(latest).toMatchObject({
          ok: true,
          result: { state: { last: { planId: last.planId } } },
        });
        if (!latest.ok) throw new Error("Missing latest plan.");
        expect(latest.result.historyPlan).toBeUndefined();
        expect(latest.projection).toBeUndefined();
        const detail = yield* read(older.planId);
        expect(detail).toMatchObject({
          ok: true,
          projection: "plan-detail",
          result: { historyPlan: { planId: older.planId }, state: { current: null, last: null } },
        });
        expect(Buffer.byteLength(JSON.stringify(detail)) + 1).toBeLessThan(
          JONES_INSTALL_PLAN_CLI_RESPONSE_BYTES,
        );
      }),
  );

  it.each(["cancel", "Done"] as const)(
    "preserves the exact launcher receipt and Laptop proof when closing by %s",
    async (closure) => {
      const f = fixture();
      const plan = await f.start();
      const download = (await f.claim(plan.planId)).grant!;
      await f.coordinator.request(reportRequest(plan.planId, f.receipt(download, "completed")));
      const review = {
        stagedHandle: "stage",
        currentVersion: "old",
        expectedInstalledSource: old,
        targetSource: source,
        reviewedAt: timestamp,
      };
      const started = await f.coordinator.request({
        ...downloadRequest(plan.planId),
        step: "install",
        explicit: true,
        action: {
          kind: "install",
          stagedHandle: review.stagedHandle,
          currentVersion: review.currentVersion,
          targetSource: source,
        },
        review,
      });
      const install = started.grant!;
      expect(install).toBeDefined();
      const pending: JonesOperationReceipt =
        closure === "cancel"
          ? { ...f.receipt(install, "handed-off"), updateId: install.operationId }
          : { ...f.receipt(install, "running"), code: "launcher-unconfirmed" };
      const reported = await f.coordinator.request(reportRequest(plan.planId, pending));
      const proof = reported.state.current!.laptop.gate;
      const closed =
        closure === "cancel"
          ? await f.coordinator.request(cancelRequest(reported.state.current!))
          : await (async () => {
              const verified = await f.coordinator.request({
                type: "end",
                planId: plan.planId,
                environmentId: remote,
                expectedRevision: reported.state.current!.revision,
                runningTarget: {
                  build: { sourceSha: source, sourceTree: tree, version: "verified" },
                  observedAt: f.options.now(),
                },
              });
              expect(verified.state.view.complete).toBe(true);
              return f.coordinator.request({
                type: "dismiss",
                planId: plan.planId,
                expectedRevision: verified.state.current!.revision,
              });
            })();
      expect(closed.state.last?.devices[0]?.operations[1]?.receipt).toEqual(pending);
      expect(closed.state.recoveryHistory?.[0]?.planId).toBe(plan.planId);
      await f.start([peer]);
      const { code: _code, ...intent } = pending;
      const completed = {
        ...intent,
        phase: "completed",
        updateId: install.operationId,
        outcome: { status: "committed", fromVersion: "old", targetVersion: "new" },
      } as const;
      expect(
        (
          await f.coordinator.request(
            reportRequest(plan.planId, { ...completed, updateId: id(125) }),
          )
        ).refused?.code,
      ).toBe("invalid-evidence");
      const finished = await f.coordinator.request(reportRequest(plan.planId, completed));
      expect(finished.state.last?.laptop.gate).toEqual(proof);
      expect(finished.state.last?.devices[0]?.verifiedElsewhere).toEqual(
        closed.state.last?.devices[0]?.verifiedElsewhere,
      );
      expect(finished.state.last?.devices[0]?.operations[1]?.receipt).toEqual(completed);
      expect(finished.state.recoveryHistory).toEqual([]);
    },
  );

  it("finishes only a previously begun device abandonment after closure", async () => {
    const f = fixture();
    const plan = await f.start();
    const grant = (await f.claim(plan.planId)).grant!;
    const staged = f.receipt(grant, "completed");
    const owned = { ...staged, staged: { ...staged.staged!, createdByOperation: true } } as const;
    const recorded = await f.coordinator.request(reportRequest(plan.planId, owned));
    const abandon = { type: "abandon-device", planId: plan.planId, environmentId: remote } as const;
    const begun = await f.coordinator.request({
      ...abandon,
      phase: "begin",
      expectedRevision: recorded.state.current!.revision,
    });
    const closed = await f.coordinator.request(cancelRequest(begun.state.current!));
    expect(closed.state.recoveryHistory?.[0]?.pendingDevices[0]?.environmentId).toBe(remote);
    expect(
      (
        await f.coordinator.request({
          ...abandon,
          phase: "begin",
          expectedRevision: closed.state.last!.revision,
        })
      ).refused?.code,
    ).toBe("plan-mismatch");
    expect(
      (
        await f.coordinator.request({
          ...abandon,
          phase: "finish",
          expectedRevision: begun.state.current!.revision,
        })
      ).refused?.code,
    ).toBe("stale-revision");
    expect(
      (
        await f.coordinator.request({
          ...abandon,
          phase: "finish",
          expectedRevision: closed.state.last!.revision,
        })
      ).refused?.code,
    ).toBe("invalid-evidence");
    const retired = await f.coordinator.request(
      reportRequest(plan.planId, { ...owned, stageRetiredAt: timestamp }),
    );
    const finished = await f.coordinator.request({
      ...abandon,
      phase: "finish",
      expectedRevision: retired.state.last!.revision,
    });
    expect(finished.state.last?.devices[0]).toMatchObject({
      membership: "removed",
      archivedOperationCount: 1,
    });
    expect(finished.state.recoveryHistory).toEqual([]);
  });
});

describe("ordinary recovery proof", () => {
  const proof = {
    transactionId: `manual-${transaction}`,
    activeGeneration: transaction,
    manifestSha256: "e".repeat(64),
    receiptSha256: "f".repeat(64),
  };
  const recovered = () => {
    const f = fixture();
    f.setEvidence({ committedTransactionId: undefined, ordinaryRecovery: proof });
    return f;
  };

  it("starts a host-only match after guided recovery of the running Laptop", async () => {
    const f = recovered();
    const result = await f.coordinator.request({
      type: "start",
      origin: "connections",
      target,
      select: [selection],
      expectedRunningGeneration: transaction,
    });
    expect(result.refused).toBeUndefined();
    expect(result.state.current).toMatchObject({
      hostOnly: true,
      target,
      devices: [{ environmentId: remote }],
      laptop: {
        operations: [],
        gate: { basis: "prior-verified", verificationKind: "ordinary-recovery" },
      },
    });
    expect(f.controller.download).not.toHaveBeenCalled();
    expect(f.controller.install).not.toHaveBeenCalled();
    expect(f.snapshot().dispositions.targets).toEqual([]);
    const request = { type: "publication", acceptOrdinaryRecovery: 1 } as const;
    expect((await f.coordinator.request(request)).publication).toMatchObject({
      kind: "withheld",
      code: "plan-managed",
    });
    expect(
      (await f.coordinator.request({ ...request, acceptHostOnlyHolds: 1 })).publication,
    ).toEqual({
      kind: "ordinary-verified",
      source: { repository: target.repository, sha: source, tree },
      heldEnvironments: [remote],
    });
  });

  it.each(["stale", "changed-generation", "missing-proof", "activation-blocked"] as const)(
    "refuses a match with %s native evidence before persisting intent",
    async (change) => {
      const f = recovered();
      if (change === "stale") f.advance();
      if (change === "missing-proof") f.setEvidence({ ordinaryRecovery: undefined });
      if (change === "activation-blocked") f.setEvidence({ activationBlocked: true });
      const result = await f.coordinator.request({
        type: "start",
        origin: "connections",
        target,
        select: [selection],
        expectedRunningGeneration: change === "changed-generation" ? "7".repeat(64) : transaction,
      });
      expect(result.refused?.code).toBe("invalid-evidence");
      expect(result.state.current).toBeNull();
      expect(f.snapshot().dispositions.targets).toEqual([]);
      if (change !== "changed-generation")
        expect(
          (await f.coordinator.request({ type: "publication", acceptOrdinaryRecovery: 1 }))
            .publication?.kind,
        ).toBe("withheld");
    },
  );

  it("keeps a plan that appeared before a host-only match reached native admission", async () => {
    const f = recovered();
    const current = await f.start([]);
    const result = await f.coordinator.request({
      type: "start",
      origin: "connections",
      target,
      select: [selection],
      expectedRunningGeneration: transaction,
    });
    expect(result.refused?.code).toBe("plan-active");
    expect(result.state.current).toEqual(current);
  });

  it("requires publication opt-in and preserves plan dispositions after Done", async () => {
    const f = recovered();
    expect((await f.coordinator.request({ type: "publication" })).publication).toMatchObject({
      kind: "withheld",
      code: "unproved",
    });
    const publish = () => f.coordinator.request({ type: "publication", acceptOrdinaryRecovery: 1 });
    expect((await publish()).publication).toMatchObject({
      kind: "ordinary-verified",
      source: { sha: source, tree },
    });
    const plan = await f.start([]);
    expect(plan.laptop.gate).toMatchObject({
      basis: "prior-verified",
      verificationKind: "ordinary-recovery",
      ordinaryRecovery: proof,
    });
    const dispositions = structuredClone(f.snapshot().dispositions);
    const dismissed = await f.coordinator.request({
      type: "dismiss",
      planId: plan.planId,
      expectedRevision: plan.revision,
    });
    expect(dismissed.refused).toBeUndefined();
    expect((await publish()).publication).toMatchObject({ kind: "withheld", code: "plan-managed" });
    expect(f.snapshot().dispositions).toEqual(dispositions);
    f.setEvidence({
      activeSource: "9".repeat(40),
      activeGeneration: "8".repeat(64),
      ordinaryRecovery: {
        ...proof,
        transactionId: `manual-${"8".repeat(64)}`,
        activeGeneration: "8".repeat(64),
      },
    });
    expect((await publish()).publication).toMatchObject({
      kind: "ordinary-verified",
      source: { sha: "9".repeat(40) },
    });
    expect(f.snapshot().dispositions).toEqual(dispositions);
    expect(f.controller.install).not.toHaveBeenCalled();
  });

  it.each(["manifest", "receipt", "generation", "warm-fallback"])(
    "closes an ordinary gate when its exact proof changes: %s",
    async (change) => {
      const f = recovered();
      await f.start();
      f.setEvidence({ ordinaryRecovery: undefined });
      const paused = await f.coordinator.request({ type: "read" });
      expect(paused.state.current!.laptop.gatePause).toBe(true);
      f.setEvidence(
        change === "warm-fallback"
          ? { committedTransactionId: transaction }
          : {
              ordinaryRecovery: {
                ...proof,
                ...(change === "manifest" ? { manifestSha256: "9".repeat(64) } : {}),
                ...(change === "receipt" ? { receiptSha256: "9".repeat(64) } : {}),
                ...(change === "generation" ? { activeGeneration: "9".repeat(64) } : {}),
              },
            },
      );
      const result = await f.coordinator.request({ type: "read" });
      expect(result.state.current!.laptop.gateClosed?.reason).toBe("changed");
      expect(result.state.current!.laptop.gatePause).toBeUndefined();
      f.setEvidence({ ordinaryRecovery: proof, committedTransactionId: undefined });
      expect((await f.coordinator.request({ type: "read" })).state.current).toEqual(
        result.state.current,
      );
    },
  );

  it.each(["ordinary-recovery", "committed"])(
    "pauses the %s gate while proof is unavailable and resumes its exact reservation",
    async (kind) => {
      const f = kind === "ordinary-recovery" ? recovered() : fixture();
      const plan = await f.start();
      const original = (await f.claim(plan.planId)).grant!;
      f.setEvidence({ ordinaryRecovery: undefined, committedTransactionId: undefined });
      const paused = (await f.coordinator.request({ type: "read" })).state.current!;
      expect(paused.laptop).toMatchObject({
        gate: plan.laptop.gate,
        gatePause: true,
        gateClosed: { reason: "blocked" },
      });
      expect(paused.revision).toBeGreaterThan(plan.revision);
      expect(await f.claim(plan.planId)).toMatchObject({ refused: { code: "gate-closed" } });

      const reopened = createInstallPlanCoordinator(f.options);
      f.setEvidence({
        ...(kind === "ordinary-recovery"
          ? { ordinaryRecovery: proof }
          : { committedTransactionId: transaction }),
        activationBlocked: true,
      });
      expect((await reopened.request({ type: "read" })).state.current).toEqual(paused);
      f.setEvidence({ activationBlocked: false });
      const resumed = (await reopened.request({ type: "read" })).state.current!;
      expect(resumed.laptop.gatePause).toBeUndefined();
      expect(resumed.laptop.gateClosed).toBeUndefined();
      expect(resumed.laptop.gate).toEqual(plan.laptop.gate);
      expect(resumed.revision).toBeGreaterThan(paused.revision);
      expect((await f.claim(plan.planId)).grant).toEqual(original);
    },
  );

  it("holds new plan actions and directive replays while Laptop proof is paused", async () => {
    const f = recovered();
    const enrollmentId = id(120);
    const plan = await f.start([{ ...selection, route: "follower-operations", enrollmentId }]);
    const directive = {
      type: "claim",
      planId: plan.planId,
      environmentId: remote,
      claimant: "window-a",
      step: "directive",
      explicit: false,
      enrollmentId,
      expectedDirectiveId: null,
    } as const;
    const reserved = await f.coordinator.request(directive);
    expect(reserved.grant).toBeDefined();
    f.advance();
    f.setEvidence({ ordinaryRecovery: undefined, activeTree: undefined });
    const refuse = { refused: { code: "gate-closed" } };
    const replay = { ...directive, operationId: reserved.grant!.operationId };
    expect(await f.coordinator.request(replay)).toMatchObject(refuse);
    for (const action of ["download", "install"] as const)
      expect(
        await f.coordinator.request({ type: "laptop", planId: plan.planId, action }),
      ).toMatchObject(refuse);
    expect(
      await f.coordinator.request({
        type: "claim",
        planId: plan.planId,
        environmentId: remote,
        claimant: "window-a",
        step: "install",
        explicit: true,
      }),
    ).toMatchObject(refuse);
    f.setEvidence({ ordinaryRecovery: proof });
    expect(await f.coordinator.request(replay)).toMatchObject(refuse);
    f.setEvidence({ activeTree: tree });
    expect((await f.coordinator.request(replay)).grant?.request).toEqual(reserved.grant?.request);
    expect(f.controller.download).not.toHaveBeenCalled();
    expect(f.controller.install).not.toHaveBeenCalled();
  });

  it("does not reopen a legacy blocked gate even with the original proof", async () => {
    const f = recovered();
    await f.start();
    await f.options.store.transact(async (snapshot) => {
      const current = snapshot.document.current!;
      return {
        ...snapshot,
        document: {
          ...snapshot.document,
          current: {
            ...current,
            laptop: { ...current.laptop, gateClosed: { at: timestamp, reason: "blocked" } },
          },
        },
        result: undefined,
      };
    });
    const reopened = createInstallPlanCoordinator(f.options);
    const before = f.snapshot().document.current;
    expect((await reopened.request({ type: "read" })).state.current).toEqual(before);
    expect(await f.claim(before!.planId)).toMatchObject({ refused: { code: "laptop-changed" } });
  });

  it("latches a changed Laptop identity during a native verification block", async () => {
    const f = recovered();
    await f.start();
    f.setEvidence({ activationBlocked: true, ordinaryRecovery: undefined });
    expect((await f.coordinator.request({ type: "read" })).state.current!.laptop.gatePause).toBe(
      true,
    );
    f.setEvidence({ environmentId: remote });
    const changed = (await f.coordinator.request({ type: "read" })).state.current!;
    expect(changed.laptop.gateClosed?.reason).toBe("changed");
    expect(changed.laptop.gatePause).toBeUndefined();
    f.setEvidence({ environmentId: laptop, activationBlocked: false, ordinaryRecovery: proof });
    expect((await f.coordinator.request({ type: "read" })).state.current).toEqual(changed);
  });

  it("retains the gate closure when a replacement plan's first action is refused", async () => {
    const f = recovered();
    const plan = await f.start();
    f.setEvidence({ ordinaryRecovery: { ...proof, receiptSha256: "9".repeat(64) } });
    const refused = await f.coordinator.request({
      type: "start",
      origin: "update-button",
      target: { ...target, sourceSha: old },
      select: [],
      laptop: { action: "download" },
    });
    expect(refused.refused?.code).toBe("not-selectable");
    expect(refused.state.current).toMatchObject({
      planId: plan.planId,
      laptop: { gateClosed: { reason: "changed" } },
    });
    f.setEvidence({ ordinaryRecovery: proof });
    expect(await f.claim(plan.planId)).toMatchObject({ refused: { code: "laptop-changed" } });
  });

  it("does not reinterpret a warm gate as ordinary recovery", async () => {
    const f = fixture();
    await f.start();
    f.setEvidence({ committedTransactionId: undefined, ordinaryRecovery: proof });
    const result = await f.coordinator.request({ type: "read" });
    expect(result.state.current!.laptop.gateClosed?.reason).toBe("changed");
  });

  it("closes a partially persisted ordinary gate instead of falling back", async () => {
    const f = recovered();
    await f.start();
    await f.options.store.transact(async (snapshot) => {
      const plan = snapshot.document.current!;
      const { ordinaryRecovery: _proof, ...gate } = plan.laptop.gate!;
      return {
        ...snapshot,
        document: {
          ...snapshot.document,
          current: {
            ...plan,
            laptop: { ...plan.laptop, gate },
          },
        },
        result: undefined,
      };
    });
    const result = await f.coordinator.request({ type: "read" });
    expect(result.state.current!.laptop.gateClosed?.reason).toBe("changed");
  });
});
describe("Laptop install recovery", () => {
  const review: JonesInstallPlanReview = {
    stagedHandle: "stage",
    targetSource: source,
    currentVersion: "old",
    expectedInstalledSource: old,
    reviewedAt: timestamp,
  };
  const startInstall = async (
    f: ReturnType<typeof fixture>,
    select = [] as (typeof selection)[],
    coordinator = f.coordinator,
  ) => {
    f.setStaged();
    f.setEvidence({
      activeSource: old,
      activeGeneration: "previous",
      committedTransactionId: undefined,
    });
    return coordinator.request({
      type: "start",
      origin: "update-button",
      target,
      select,
      laptop: { action: "install", review },
    });
  };
  const bind = (f: ReturnType<typeof fixture>, operationId: string) =>
    f.coordinator.bindLaptopInstall({
      operationId,
      stagedHandle: "stage",
      targetSource: source,
      targetTree: tree,
      transactionId: transaction,
      fromGeneration: "previous",
    });

  it("does not call native install when reservation storage fails before the write", async () => {
    const f = fixture();
    f.interceptNextWrite(async () => {
      throw new Error("Lost the plan journal before commit.");
    });
    expect(await startInstall(f)).toMatchObject({ refused: { code: "storage" } });
    expect(f.snapshot().document.current).toBeNull();
    expect(f.controller.install).not.toHaveBeenCalled();
  });

  it("retains a durable unbound reservation across shutdown before dispatch", async () => {
    const f = fixture();
    const written = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    f.interceptNextWrite(async (commit) => {
      commit();
      written.resolve();
      await release.promise;
    });
    const starting = startInstall(f);
    await written.promise;
    const pending = f.snapshot().document.current!.laptop.operations[0]!;
    expect(pending.prebinding).toEqual({
      fromGeneration: "previous",
      fromSource: old,
      fromTree: tree,
      stagedHandle: "stage",
    });
    expect(f.controller.install).not.toHaveBeenCalled();
    f.coordinator.dispose();
    const reopened = createInstallPlanCoordinator(f.options);
    const read = () => reopened.request({ type: "read" });
    f.controller.laptopEvidence.mockClear();
    const checking = read();
    try {
      await Promise.resolve();
      expect(f.controller.laptopEvidence).not.toHaveBeenCalled();
    } finally {
      release.resolve();
    }
    await starting;
    expect((await checking).state.current!.laptop.operations[0]!.result).toMatchObject({
      status: "held",
      code: "native-unconfirmed",
    });
    expect(f.controller.laptopEvidence).toHaveBeenCalledExactlyOnceWith({
      ...pending.prebinding,
      environmentId: laptop,
      targetSource: source,
      targetTree: tree,
      currentVersion: "old",
    });
    f.setEvidence({ startupInspectionComplete: true });
    const settled = (await read()).state.current!;
    expect(settled.laptop.operations[0]).toMatchObject({
      operationId: pending.operationId,
      result: { status: "interrupted", code: "install-not-dispatched" },
    });
    expect(f.controller.isStaged).toHaveBeenLastCalledWith({
      targetSource: source,
      targetTree: tree,
      stagedHandle: "stage",
      currentVersion: "old",
      expectedInstalledSource: old,
    });
    expect(f.controller.install).not.toHaveBeenCalled();
    const retried = await reopened.request({
      type: "laptop",
      planId: settled.planId,
      action: "install",
      review,
    });
    expect(retried.refused).toBeUndefined();
    expect(retried.state.current!.laptop.operations[1]!.operationId).not.toBe(pending.operationId);
    expect(f.controller.install).toHaveBeenCalledTimes(1);
  });

  it("keeps the local dispatch reservation pending and fences a delayed callback on disposal", async () => {
    const f = fixture();
    const tracking = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const finished = Promise.withResolvers<void>();
    const coordinator = createInstallPlanCoordinator({
      ...f.options,
      track: async (work) => {
        tracking.resolve();
        await release.promise;
        try {
          return await work();
        } finally {
          finished.resolve();
        }
      },
    });
    f.setEvidence({ startupInspectionComplete: true });
    await startInstall(f, [], coordinator);
    await tracking.promise;
    const read = await coordinator.request({ type: "read" });
    expect(read.state.current!.laptop.operations[0]!.result?.status).toBe("running");
    coordinator.dispose();
    release.resolve();
    await finished.promise;
    expect(f.controller.install).not.toHaveBeenCalled();
    const restarted = createInstallPlanCoordinator(f.options);
    expect(
      (await restarted.request({ type: "read" })).state.current!.laptop.operations[0]!.result,
    ).toMatchObject({ status: "interrupted", code: "install-not-dispatched" });
  });

  it("does not acknowledge a binding that commits during disposal", async () => {
    const f = fixture();
    const native = Promise.withResolvers<Awaited<ReturnType<typeof f.controller.install>>>();
    f.controller.install.mockImplementationOnce(() => native.promise);
    const started = await startInstall(f);
    const operationId = started.state.current!.laptop.operations[0]!.operationId;
    const written = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    f.interceptNextWrite(async (commit) => {
      commit();
      written.resolve();
      await release.promise;
    });
    const binding = bind(f, operationId);
    await written.promise;
    const saved = f.snapshot().document.current!.laptop;
    expect(saved.binding?.operationId).toBe(operationId);
    expect(saved.operations[0]?.prebinding).toBeUndefined();
    f.coordinator.dispose();
    release.resolve();
    await expect(binding).rejects.toThrow("coordinator is closing");
    native.resolve({ accepted: false, completed: false, failed: true });

    f.setEvidence({ startupInspectionComplete: true });
    const restarted = createInstallPlanCoordinator(f.options);
    const held = (await restarted.request({ type: "read" })).state.current!.laptop;
    expect(held.binding).toEqual(saved.binding);
    expect(held.operations[0]!.result).toMatchObject({
      status: "held",
      code: "native-unconfirmed",
    });
  });

  it("makes a paused gate permanently closed when its native attempt rolls back", async () => {
    const f = fixture();
    const { state } = await startInstall(f, [selection]);
    await bind(f, state.current!.laptop.operations[0]!.operationId);
    const committed = {
      transactionId: transaction,
      status: "committed",
      activeGeneration: transaction,
      activeSource: source,
      activeTree: tree,
    } as const;
    await f.coordinator.recordLaptopOutcome(committed);
    f.setEvidence({
      activeGeneration: transaction,
      activeSource: source,
      committedTransactionId: transaction,
    });
    const read = () => f.coordinator.request({ type: "read" });
    expect((await read()).state.current!.laptop.gate?.basis).toBe("plan-install");
    f.setEvidence({ committedTransactionId: undefined });
    expect((await read()).state.current!.laptop.gatePause).toBe(true);
    await f.coordinator.recordLaptopOutcome({
      ...committed,
      status: "rolled-back",
      activeGeneration: "previous",
      activeSource: old,
    });
    f.setEvidence({ committedTransactionId: transaction });
    const closed = (await read()).state.current!.laptop;
    expect(closed.gateClosed?.reason).toBe("rolled-back");
    expect(closed.gatePause).toBeUndefined();
  });

  it.each([
    "stage",
    "generation",
    "source",
    "tree",
    "blocked",
    "busy",
    "worker",
    "binding",
    "legacy",
  ] as const)(
    "keeps the unbound attempt held when restart evidence is ambiguous: %s",
    async (scenario) => {
      const f = fixture();
      await startInstall(f);
      f.coordinator.dispose();
      f.setEvidence({
        startupInspectionComplete: true,
        ...(scenario === "generation" ? { activeGeneration: "other" } : {}),
        ...(scenario === "source" ? { activeSource: source } : {}),
        ...(scenario === "tree" ? { activeTree: old } : {}),
        ...(scenario === "blocked" ? { activationBlocked: true } : {}),
      });
      if (scenario === "stage") f.setStaged(false);
      if (scenario === "legacy" || scenario === "worker" || scenario === "binding")
        await f.options.store.transact(async (snapshot) => {
          const current = snapshot.document.current!;
          const operation = current.laptop.operations[0]!;
          const { prebinding: _reservation, ...legacy } = operation;
          return {
            ...snapshot,
            document: {
              ...snapshot.document,
              current: {
                ...current,
                laptop: {
                  ...current.laptop,
                  operations: [
                    scenario === "legacy"
                      ? legacy
                      : scenario === "worker"
                        ? { ...operation, worker: { pid: 50, processGroup: 50 } }
                        : operation,
                  ],
                  ...(scenario !== "binding"
                    ? {}
                    : {
                        binding: {
                          operationId: operation.operationId,
                          stagedHandle: "stage",
                          targetSource: source,
                          targetTree: tree,
                          transactionId: transaction,
                          fromGeneration: "previous",
                          boundAt: timestamp,
                        },
                      }),
                },
              },
            },
            result: undefined,
          };
        });
      const restarted = createInstallPlanCoordinator({
        ...f.options,
        ...(scenario === "busy"
          ? { controller: { ...f.controller, busy: "download" as const } }
          : {}),
      });
      const retainedBinding = f.snapshot().document.current!.laptop.binding;
      f.controller.laptopEvidence.mockClear();
      const read = await restarted.request({ type: "read" });
      expect(read.state.current!.laptop.operations[0]!.result).toMatchObject({
        status: "held",
        code: "native-unconfirmed",
      });
      expect(read.state.current!.laptop.binding).toEqual(retainedBinding);
      if (["busy", "worker", "legacy", "binding"].includes(scenario))
        expect(f.controller.laptopEvidence).toHaveBeenCalledExactlyOnceWith(undefined);
      expect(f.controller.install).toHaveBeenCalledTimes(1);
    },
  );

  it("reconciles the same verified prebinding in cancelled history", async () => {
    const f = fixture();
    const began = await startInstall(f);
    f.coordinator.dispose();
    const restarted = createInstallPlanCoordinator(f.options);
    const held = (await restarted.request({ type: "read" })).state.current!;
    expect(held.laptop.operations[0]!.result?.status).toBe("held");
    expect(
      (
        await restarted.request({
          type: "cancel-plan",
          planId: began.state.current!.planId,
          expectedRevision: held.revision,
        })
      ).state.current,
    ).toBeNull();
    f.setEvidence({ startupInspectionComplete: true });
    const archived = (await restarted.request({ type: "read" })).state.last!;
    expect(archived.cancelled).toBe(true);
    expect(archived.laptop.operations[0]!.result).toMatchObject({
      status: "interrupted",
      code: "install-not-dispatched",
    });
    expect(await restarted.preparationHeld()).toBe(false);
  });

  it.each([true, false])(
    "settles a held attempt on verified manual startup (same target: %s)",
    async (sameTarget) => {
      const f = fixture();
      const { state } = await startInstall(f, [selection]);
      const plan = state.current!;
      const operationId = plan.laptop.operations[0]!.operationId;
      await bind(f, operationId);
      const restored = createInstallPlanCoordinator(f.options);
      const held = await restored.request({ type: "read" });
      expect(held.state.current?.laptop.operations[0]?.result?.status).toBe("held");
      const generation = "8".repeat(64);
      f.setEvidence({
        activeSource: sameTarget ? source : "9".repeat(40),
        activeTree: tree,
        activeGeneration: generation,
      });
      expect((await restored.request({ type: "read" })).state.current?.laptop.gate).toBeUndefined();
      f.setEvidence({
        ordinaryRecovery: {
          transactionId: `manual-${generation}`,
          activeGeneration: generation,
          manifestSha256: "e".repeat(64),
          receiptSha256: "f".repeat(64),
        },
      });
      const checked = await restored.request({ type: "read" });
      expect(checked.state.current?.laptop.operations[0]?.result).toMatchObject({
        status: "interrupted",
        code: sameTarget ? "target-verified-elsewhere" : "install-not-active",
      });
      if (sameTarget) {
        expect(checked.state.current?.laptop.gate).toMatchObject({
          basis: "prior-verified",
          verificationKind: "ordinary-recovery",
        });
        expect(checked.state.view.manualClose).toBeUndefined();
      } else {
        expect(checked.state.current?.laptop.gate).toBeUndefined();
        const closed = await restored.request({
          type: "close-after-manual-install",
          planId: plan.planId,
          expectedRevision: checked.state.current!.revision,
          ...checked.state.view.manualClose!,
          devices: [{ environmentId: remote, kind: "unverified", observedAt: f.options.now() }],
        });
        expect(closed.refused).toBeUndefined();
        expect(closed.state.current).toBeNull();
      }
      expect(f.controller.install).toHaveBeenCalledTimes(1);
    },
  );

  it("cancels a held Laptop rollout, retains its native fence and reconciles late proof in history", async () => {
    const f = fixture();
    const { state } = await startInstall(f, [selection]);
    const plan = state.current!;
    const operationId = plan.laptop.operations[0]!.operationId;
    await bind(f, operationId);
    const restored = createInstallPlanCoordinator(f.options);
    const held = await restored.request({ type: "read" });
    const cancel = {
      type: "cancel-plan" as const,
      planId: plan.planId,
      expectedRevision: held.state.current!.revision,
    };
    expect(
      (await restored.request({ ...cancel, expectedRevision: plan.revision })).refused?.code,
    ).toBe("stale-revision");
    const cancelled = await restored.request(cancel);
    expect(cancelled.refused).toBeUndefined();
    expect(cancelled.state.current).toBeNull();
    expect(cancelled.state.last).toMatchObject({
      cancelled: true,
      laptop: { operations: [{ operationId, result: { status: "held" } }] },
    });
    const reopened = createInstallPlanCoordinator(f.options);
    expect(await reopened.preparationHeld()).toBe(true);
    expect(
      (await reopened.request({ type: "start", origin: "update-button", target, select: [] }))
        .refused?.code,
    ).toBe("outstanding-operation");
    expect((await f.claim(plan.planId)).refused?.code).toBe("plan-mismatch");
    f.setEvidence({
      activeGeneration: transaction,
      committedTransactionId: transaction,
      activeSource: source,
    });
    const reconciled = await reopened.request({ type: "read" });
    expect(reconciled.state.current).toBeNull();
    expect(reconciled.state.last?.laptop.gate).toBeUndefined();
    expect(reconciled.state.last?.laptop.operations[0]?.result?.status).toBe("committed");
    expect(await reopened.preparationHeld()).toBe(false);
    expect(f.controller.install).toHaveBeenCalledTimes(1);
  });

  it("lets the exact issued Laptop install bind and finish after cancellation without opening a gate", async () => {
    const f = fixture();
    const entered = Promise.withResolvers<void>();
    const finish = Promise.withResolvers<{
      accepted: boolean;
      completed: boolean;
      failed: boolean;
    }>();
    f.controller.install.mockImplementationOnce(async () => {
      entered.resolve();
      return finish.promise;
    });
    const { state } = await startInstall(f);
    await entered.promise;
    const plan = state.current!;
    const operationId = plan.laptop.operations[0]!.operationId;
    const closed = await f.coordinator.request({
      type: "cancel-plan",
      planId: plan.planId,
      expectedRevision: plan.revision,
    });
    expect(closed.state.recoveryHistory?.[0]).toMatchObject({
      planId: plan.planId,
      pendingLaptop: true,
    });
    expect(await f.coordinator.preparationHeld()).toBe(true);
    expect(await f.coordinator.preparationHeld(id(102))).toBe(true);
    expect(await f.coordinator.preparationHeld(operationId)).toBe(false);
    await expect(bind(f, id(102))).rejects.toThrow();
    await bind(f, operationId);
    finish.resolve({ accepted: true, completed: false, failed: false });
    await f.coordinator.recordLaptopOutcome({
      transactionId: transaction,
      status: "committed",
      activeGeneration: transaction,
      activeSource: source,
      activeTree: tree,
    });
    await f.coordinator.recordLaptopOutcome({
      transactionId: transaction,
      status: "rolled-back",
      activeGeneration: "previous",
      activeSource: old,
      activeTree: tree,
    });
    const readback = await f.coordinator.request({ type: "read-history", planId: plan.planId });
    expect(readback.state.current).toBeNull();
    expect(readback.historyPlan?.laptop.gate).toBeUndefined();
    expect(readback.historyPlan?.laptop.operations[0]?.result?.status).toBe("committed");
    expect(readback.state.recoveryHistory).toEqual([]);
    expect(await f.coordinator.preparationHeld()).toBe(false);
  });

  effectIt.effect("only cancels the exact Laptop plan revision through the CLI control", () =>
    Effect.gen(function* () {
      const f = fixture();
      const { state } = yield* Effect.promise(() => startInstall(f, [selection]));
      const plan = state.current!;
      const request = (action: JonesInstallPlanCliRequest["action"]) =>
        dispatchInstallPlanCli(
          { version: 1, requestId: id(92), type: "jones-install-plan", action },
          (input) => Effect.promise(() => f.coordinator.request(input)),
        );
      const read = yield* request({ type: "read" });
      expect(read).toMatchObject({
        ok: true,
        result: { state: { current: { planId: plan.planId } } },
      });
      const current = yield* Effect.promise(() => f.coordinator.request({ type: "read" }));
      const cancel = {
        type: "cancel-plan",
        confirmed: true,
        environmentId: laptop,
        planId: plan.planId,
        expectedRevision: current.state.current!.revision,
      } as const;
      expect(yield* request({ ...cancel, environmentId: remote })).toMatchObject({
        ok: false,
        code: "environment-mismatch",
      });
      expect(
        yield* request({ ...cancel, expectedRevision: cancel.expectedRevision + 1 }),
      ).toMatchObject({
        ok: true,
        result: { refused: { code: "stale-revision" } },
      });
      const cancelled = yield* request(cancel);
      expect(cancelled).toMatchObject({
        ok: true,
        result: { state: { current: null, last: { planId: plan.planId, cancelled: true } } },
      });
      expect(yield* request(cancel)).toEqual(cancelled);
    }),
  );

  it("archives a plan with a reserved remote operation when cancellation is requested", async () => {
    const f = fixture();
    const plan = await f.start();
    const claimed = await f.claim(plan.planId);
    expect(claimed.grant).toBeDefined();
    const cancelled = await f.coordinator.request({
      type: "cancel-plan",
      planId: plan.planId,
      expectedRevision: claimed.state.current!.revision,
    });
    expect(cancelled.refused).toBeUndefined();
    expect(cancelled.state.current).toBeNull();
    expect(cancelled.state.last?.devices).toEqual(claimed.state.current?.devices);
  });

  it("cancels pending rollout work and keeps the exact remote cleanup available in history", async () => {
    const f = fixture();
    const plan = await f.start();
    const grant = (await f.claim(plan.planId)).grant!;
    const held = {
      ...f.receipt(grant),
      phase: "held",
      code: "cleanup-unconfirmed",
      jobId: id(121),
    } as const;
    const report = (receipt: JonesOperationReceipt) =>
      f.coordinator.request({
        type: "report",
        planId: plan.planId,
        environmentId: remote,
        operationId: grant.operationId,
        evidence: { kind: "receipt", receipt },
      });
    const recorded = await report(held);
    expect(recorded.refused).toBeUndefined();
    const cancelled = await f.coordinator.request({
      type: "cancel-plan",
      planId: plan.planId,
      expectedRevision: recorded.state.current!.revision,
    });
    expect(cancelled.state.current).toBeNull();
    expect(cancelled.state.last?.devices[0]?.operations[0]?.receipt).toEqual(held);
    expect((await f.claim(plan.planId)).refused?.code).toBe("plan-mismatch");
    const start = { type: "start", origin: "update-button", target, select: [selection] } as const;
    expect((await f.coordinator.request(start)).refused?.code).toBe("outstanding-operation");
    const released = { ...held, phase: "interrupted", code: "cleanup-recovered" } as const;
    expect((await report({ ...released, jobId: id(122) })).refused?.code).toBe("invalid-evidence");
    const settled = await report(released);
    expect(settled.state.current).toBeNull();
    expect(settled.state.last?.devices[0]?.operations[0]?.receipt).toEqual(released);
    expect((await f.coordinator.request(start)).state.current).not.toBeNull();
  });

  it("records a failed result after binding and permits an explicit reviewed retry", async () => {
    const f = fixture();
    f.controller.install.mockImplementationOnce(async (_handle, _campaign, plan) => {
      await bind(f, plan!.operationId);
      return { accepted: false, completed: false, failed: true };
    });
    const started = await startInstall(f);
    const operationId = started.state.current!.laptop.operations[0]!.operationId;
    await vi.waitFor(() =>
      expect(f.snapshot().document.current?.laptop.operations[0]?.result).toMatchObject({
        status: "failed",
        code: "install-failed",
      }),
    );
    const retried = await f.coordinator.request({
      type: "laptop",
      planId: started.state.current!.planId,
      action: "install",
      review,
    });
    expect(retried.refused).toBeUndefined();
    expect(retried.state.current!.laptop.operations[0]!.operationId).toBe(operationId);
    expect(retried.state.current!.laptop.operations).toHaveLength(2);
  });

  it("records a retry that reuses a failed closed plan's transaction without changing the archive", async () => {
    const f = fixture();
    f.controller.install.mockImplementationOnce(async (_handle, _campaign, plan) => {
      await bind(f, plan!.operationId);
      return { accepted: false, completed: false, failed: true };
    });
    const first = (await startInstall(f)).state.current!;
    await vi.waitFor(() =>
      expect(f.snapshot().document.current?.laptop.operations[0]?.result?.status).toBe("failed"),
    );
    const closed = await f.coordinator.request({
      type: "cancel-plan",
      planId: first.planId,
      expectedRevision: f.snapshot().document.current!.revision,
    });
    const original = closed.state.last!;
    expect(original.laptop.binding?.transactionId).toBe(transaction);
    expect(original.laptop.operations[0]?.result).toMatchObject({
      status: "failed",
      code: "install-failed",
    });

    const bound = Promise.withResolvers<void>();
    f.controller.install.mockImplementationOnce(async (_handle, _campaign, plan) => {
      await bind(f, plan!.operationId);
      bound.resolve();
      return { accepted: true, completed: false, failed: false };
    });
    const retry = (await startInstall(f)).state.current!;
    expect(retry.planId).not.toBe(first.planId);
    await bound.promise;
    expect(f.snapshot().document.current?.laptop.binding?.transactionId).toBe(transaction);
    const committed = {
      transactionId: transaction,
      status: "committed",
      activeGeneration: transaction,
      activeSource: source,
      activeTree: tree,
    } as const;
    await f.coordinator.recordLaptopOutcome(committed);
    expect(f.snapshot().document.current?.laptop.operations[0]?.result?.status).toBe("committed");
    expect(f.snapshot().document.last).toEqual(original);

    f.setEvidence({
      activeGeneration: transaction,
      activeSource: source,
      committedTransactionId: transaction,
    });
    const read = await f.coordinator.request({ type: "read" });
    expect(read.state.current?.laptop.gate?.basis).toBe("plan-install");
    await f.coordinator.recordLaptopOutcome({
      ...committed,
      status: "rolled-back",
      activeGeneration: "previous",
      activeSource: old,
    });
    expect(f.snapshot().document.current?.laptop.gateClosed?.reason).toBe("rolled-back");
    expect(f.snapshot().document.last).toEqual(original);
  });

  it("keeps a failed bound handoff held while native activation is unresolved", async () => {
    const f = fixture();
    f.controller.install.mockImplementationOnce(async (_handle, _campaign, plan) => {
      await bind(f, plan!.operationId);
      f.setEvidence({ activationBlocked: true });
      return { accepted: false, completed: false, failed: true };
    });
    const started = await startInstall(f);
    await vi.waitFor(() =>
      expect(f.snapshot().document.current?.laptop.operations[0]?.result).toMatchObject({
        status: "held",
        code: "native-unconfirmed",
      }),
    );
    const retried = await f.coordinator.request({
      type: "laptop",
      planId: started.state.current!.planId,
      action: "install",
      review,
    });
    expect(retried.refused?.code).toBe("outstanding-operation");
    expect(f.controller.install).toHaveBeenCalledTimes(1);
  });

  it.each(["retired", "external", "blocked", "wrong-source", "wrong-tree", "unverified"])(
    "reconciles a stale bound install only with proved native evidence: %s",
    async (scenario) => {
      const f = fixture();
      f.controller.state = {
        ...f.controller.state,
        jones: { ...f.controller.state.jones!, phase: "staged" },
      };
      const started = await startInstall(f);
      const operationId = started.state.current!.laptop.operations[0]!.operationId;
      await bind(f, operationId);
      if (scenario === "retired") f.controller.retiredLaptopInstall.mockResolvedValue(true);
      if (scenario !== "retired")
        f.setEvidence({
          activeSource: scenario === "wrong-source" ? old : source,
          activeTree: scenario === "wrong-tree" ? old : tree,
          activeGeneration: "external-generation",
          committedTransactionId: scenario === "unverified" ? undefined : "external-generation",
          activationBlocked: scenario === "blocked",
        });
      const restored = createInstallPlanCoordinator(f.options);
      const state = (await restored.request({ type: "read" })).state;
      const result = state.current!.laptop.operations[0]!.result;
      if (scenario === "retired" || scenario === "external") {
        expect(result).toMatchObject({
          status: "interrupted",
          code: scenario === "external" ? "target-verified-elsewhere" : "install-not-active",
        });
        expect(state.current!.laptop.binding?.transactionId).toBe(transaction);
        expect(state.current!.laptop.gate?.basis).toBe(
          scenario === "external" ? "prior-verified" : undefined,
        );
      } else {
        expect(result?.status).toBe("held");
        expect(state.current!.laptop.gate).toBeUndefined();
        expect(state.view.complete).toBe(false);
      }
      expect(f.controller.install).toHaveBeenCalledTimes(1);
    },
  );

  it("ends the retired A-to-B attempt on receiving C without verifying B, then admits a fresh C plan", async () => {
    const f = fixture();
    const started = await startInstall(f);
    const planId = started.state.current!.planId;
    const operationId = started.state.current!.laptop.operations[0]!.operationId;
    await bind(f, operationId);
    f.controller.retiredLaptopInstall.mockResolvedValue(true);
    f.setEvidence({ activeSource: source, activeGeneration: "manual-B" });
    const receivingSource = "9".repeat(40);
    f.setEvidence({ activeSource: receivingSource, activeGeneration: "receiving-C" });
    const restored = createInstallPlanCoordinator(f.options);
    const state = (await restored.request({ type: "read" })).state;
    expect(state.current!.laptop.operations[0]!.result).toMatchObject({
      status: "interrupted",
      code: "install-not-active",
    });
    expect(state.current!.laptop.gate).toBeUndefined();
    expect(state.view.complete).toBe(true);
    const dismissed = await restored.request({
      type: "dismiss",
      planId,
      expectedRevision: state.current!.revision,
    });
    expect(dismissed.refused).toBeUndefined();
    const next = await restored.request({
      type: "start",
      origin: "update-button",
      target: { ...target, sourceSha: receivingSource },
      select: [],
      laptop: {
        action: "install",
        review: {
          ...review,
          targetSource: receivingSource,
          expectedInstalledSource: receivingSource,
        },
      },
    });
    expect(next.refused).toBeUndefined();
    expect(next.state.current!.target.sourceSha).toBe(receivingSource);
    expect(next.state.last!.laptop.operations[0]!.operationId).toBe(operationId);
  });
});
describe("native client command reservations", () => {
  it("recovers an orphaned V1 claim from the terminal host receipt and retains its original replay body", async () => {
    const f = fixture();
    const original = await f.start();
    const request = {
      type: "claim",
      planId: original.planId,
      environmentId: remote,
      claimant: "original-window",
      step: "download",
      explicit: true,
      action: { kind: "download-exact", targetSource: source },
      expectedInstalledSource: old,
      command: {
        commandId: id(89),
        environmentId: remote,
        targetSource: source,
        action: "download-exact",
        planId: original.planId,
        expectedRevision: original.revision,
      },
    } as const;
    const reserved = await f.coordinator.request(request);
    const entry = f.snapshot().document.commands![0]!;
    const { bodyHash: _unrecorded, ...legacyReceipt } = entry.receipt;
    const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-orphan-v1-"));
    persistentRoots.add(home);
    const open = () => createInstallPlanStore({ home, profile: undefined });
    const last = { ...original, planId: id(88), devices: [], dismissedAt: timestamp };
    await open().transact(async () => ({
      document: {
        schema: 1,
        current: null,
        last,
        commands: [{ body: entry.body, receipt: legacyReceipt }],
      },
      dispositions: f.snapshot().dispositions,
      result: undefined,
    }));
    const coordinator = createInstallPlanCoordinator({ ...f.options, store: open() });
    const read = () => coordinator.request({ type: "read", commandId: request.command.commandId });
    const migrated = await read();
    expect(migrated.state.last).toEqual(last);
    expect(migrated.state.orphanedCommands).toEqual([reserved.command]);
    const start = { type: "start", origin: "settings", target, select: [selection] } as const;
    expect((await coordinator.request(start)).refused?.code).toBe("outstanding-operation");
    expect((await coordinator.request(request)).grant).toBeUndefined();
    expect(
      (await coordinator.request({ ...request, claimant: "different-window" })).refused?.code,
    ).toBe("invalid-evidence");
    const terminal = f.receipt(reserved.grant!, "completed");
    const settle = (receipt: JonesOperationReceipt, bodyHash = entry.body) =>
      coordinator.request({
        type: "reconcile-orphan",
        commandId: request.command.commandId,
        bodyHash,
        receipt,
      });
    for (const wrong of [
      { ...terminal, operationId: id(87) },
      { ...terminal, correlation: { ...terminal.correlation!, planId: last.planId } },
      { ...terminal, action: { kind: "download-exact" as const, targetSource: old } },
      { ...terminal, phase: "running" as const, code: "launcher-unconfirmed" },
    ]) {
      expect((await settle(wrong)).refused?.code).toBe("invalid-evidence");
    }
    expect((await settle(terminal, "f".repeat(64))).refused?.code).toBe("invalid-evidence");
    expect((await read()).command?.outcome).toBe("reserved");
    const settled = await settle(terminal);
    expect(settled.command).toEqual({ ...reserved.command, outcome: "terminal" });
    expect(settled.state.orphanedCommands).toBeUndefined();
    expect(settled.state.last).toEqual(last);
    expect((await settle(terminal)).command).toEqual(settled.command);
    const restarted = createInstallPlanCoordinator({ ...f.options, store: open() });
    const replay = await restarted.request(request);
    expect(replay.command).toEqual(settled.command);
    expect(replay.grant).toBeUndefined();
    expect((await restarted.request(start)).state.current?.devices[0]?.environmentId).toBe(remote);
    await open().transact(async (snapshot) => {
      expect(snapshot.document.commands?.[0]).toEqual({
        ...entry,
        receipt: settled.command,
        orphanReceipt: terminal,
      });
      return { ...snapshot, result: undefined };
    });
  });

  it("keeps an orphaned Laptop reservation held when its original native binding is gone", async () => {
    const f = fixture();
    await f.options.store.transact(async (snapshot) => ({
      ...snapshot,
      document: {
        schema: 1,
        current: null,
        last: null,
        commands: [
          {
            body: "a".repeat(64),
            receipt: {
              command: {
                commandId: id(86),
                environmentId: laptop,
                targetSource: source,
                action: "download",
              },
              requestType: "start",
              planId: id(85),
              operationId: id(84),
              outcome: "reserved",
            },
          },
        ],
      },
      result: undefined,
    }));
    const visible = await f.coordinator.request({ type: "read", commandId: id(86) });
    expect(visible.state.orphanedCommands).toEqual([visible.command]);
    expect(await f.coordinator.preparationHeld()).toBe(true);
    expect(
      (await f.coordinator.request({ type: "start", origin: "update-button", target, select: [] }))
        .refused?.code,
    ).toBe("outstanding-operation");
    expect(f.controller.download).not.toHaveBeenCalled();
  });

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
  it("leaves no plan or download when the reviewed artifact changed", async () => {
    const f = fixture();
    const result = await f.coordinator.request({
      type: "start",
      origin: "update-everything",
      target: { ...target, laptopArtifactId: 2 },
      select: [],
      laptop: { action: "download" },
    });
    expect(result.refused?.code).toBe("not-selectable");
    expect(f.snapshot().document.current).toBeNull();
    expect(f.controller.download).not.toHaveBeenCalled();
  });

  it("retains the reviewed artifact in the plan so a retry cannot select a rebuild", async () => {
    const f = fixture();
    const result = await f.coordinator.request({
      type: "start",
      origin: "update-everything",
      target: { ...target, laptopArtifactId: 1 },
      select: [],
      laptop: { action: "download" },
    });
    expect(result.state.current?.target.laptopArtifactId).toBe(1);
    await vi.waitFor(() =>
      expect(f.snapshot().document.current?.laptop.operations.at(-1)?.result?.status).toBe(
        "completed",
      ),
    );
    const jones = f.controller.state.jones!;
    f.controller.state = {
      ...f.controller.state,
      jones: { ...jones, provenance: { ...jones.provenance!, artifactId: 2 } },
    };
    const retry = await f.coordinator.request({
      type: "laptop",
      planId: result.state.current!.planId,
      action: "download",
    });
    expect(retry.refused?.code).toBe("not-selectable");
    expect(f.controller.download).toHaveBeenCalledTimes(1);
  });

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

  it.each([false, true])(
    "accepts a host Stop independently of a local claim: %s",
    async (claimed) => {
      const f = fixture();
      const plan = await f.start();
      const grant = (await f.claim(plan.planId)).grant!;
      const report = (receipt: JonesOperationReceipt) =>
        f.coordinator.request({
          type: "report",
          planId: plan.planId,
          environmentId: remote,
          operationId: grant.operationId,
          evidence: { kind: "receipt", receipt },
        });
      const running = f.receipt(grant);
      expect((await report(running)).refused).toBeUndefined();
      f.setEvidence({ committedTransactionId: undefined, activationBlocked: true });
      const stop = claimed
        ? (
            await f.coordinator.request({
              type: "claim",
              planId: plan.planId,
              environmentId: remote,
              claimant: "window-a",
              step: "stop",
              explicit: true,
              operationId: grant.operationId,
            })
          ).state.current!.devices[0]!.operations[0]!.stop
        : undefined;
      if (claimed) expect(stop?.stopId).toBeDefined();
      const hostStop = {
        ...running,
        phase: "stop-requested",
        stop: { stopId: id(100), requestedAt: timestamp },
      } as const;
      expect((await report({ ...hostStop, expectedInstalledSource: source })).refused?.code).toBe(
        "invalid-evidence",
      );
      const observed = await report(hostStop);
      expect(observed.refused).toBeUndefined();
      expect(observed.state.current!.devices[0]!.operations[0]).toMatchObject({
        receipt: hostStop,
      });
      expect(observed.state.current!.devices[0]!.operations[0]!.stop).toEqual(stop);
      expect(
        (await report({ ...hostStop, stop: { ...hostStop.stop, stopId: id(101) } })).refused?.code,
      ).toBe("invalid-evidence");
      expect((await report(running)).refused?.code).toBe("invalid-evidence");
      const stopped = {
        ...hostStop,
        phase: "stopped",
        stop: { ...hostStop.stop, result: "stopped" },
      } as const;
      expect((await report(stopped)).state.current!.devices[0]!.operations[0]).toMatchObject({
        receipt: stopped,
      });
    },
  );

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

  it("requires a matching directive before downloads and regrants the exact expired request", async () => {
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
    const claim = {
      type: "claim",
      planId,
      environmentId: remote,
      claimant: "window-a",
      step: "directive",
      explicit: false,
      enrollmentId,
      expectedDirectiveId: null,
    } as const;
    const directive = await f.coordinator.request(claim);
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
    const replay = { ...claim, claimant: "window-b", operationId: directive.grant!.operationId };
    expect((await f.coordinator.request({ ...replay, operationId: id(123) })).refused?.code).toBe(
      "invalid-evidence",
    );
    expect(
      (await f.coordinator.request({ ...replay, expectedDirectiveId: id(123) })).refused?.code,
    ).toBe("claimed");
    const renewed = await f.coordinator.request(replay);
    expect(renewed.grant).toMatchObject({
      operationId: directive.grant!.operationId,
      request: directive.grant!.request,
    });
    expect(renewed.grant!.leaseUntil).not.toBe(directive.grant!.leaseUntil);
    expect(renewed.state.current?.devices[0]?.operations).toHaveLength(1);
    expect((await f.coordinator.request({ ...replay, claimant: "window-c" })).refused?.code).toBe(
      "claimed",
    );
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
    const cancel = {
      type: "cancel-plan",
      planId,
      expectedRevision: renewed.state.current!.revision,
    } as const;
    const cancelled = await f.coordinator.request(cancel);
    expect(cancelled.state.current).toBeNull();
    expect(cancelled.state.last?.devices[0]?.operations[0]?.directive).toBeUndefined();
    expect(cancelled.state.recoveryHistory?.[0]?.pendingDevices[0]?.environmentId).toBe(remote);
    expect((await f.coordinator.request(replay)).refused?.code).toBe("plan-mismatch");
    const observed = await f.coordinator.request({
      type: "report",
      planId,
      environmentId: remote,
      operationId: replay.operationId,
      evidence: {
        kind: "directive",
        status: {
          environmentId: remote,
          operationProtocol: 1,
          planDirectiveProtocol: 1,
          enrollment: {
            enrollmentId,
            environmentId: remote,
            enabled: true,
            continueRunningThreads: false,
          },
          operation: null,
          update: null,
          follower: {
            protocol: 1,
            state: "waiting-for-leader",
            leaderSource: null,
            planDirective: {
              directiveId: replay.operationId,
              targetSource: source,
              targetTree: tree,
              mode: "manual-review",
              issuedAt: timestamp,
            },
          },
        },
      },
    });
    expect(observed.state.current).toBeNull();
    expect(observed.state.last?.devices[0]?.operations[0]?.directive?.kind).toBe("applied");
    expect(observed.state.recoveryHistory).toEqual([]);
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
    f.setEvidence({
      activeSource: old,
      activeGeneration: "previous",
      committedTransactionId: undefined,
    });
    const native = Promise.withResolvers<Awaited<ReturnType<typeof f.controller.install>>>();
    f.controller.install.mockImplementationOnce(() => native.promise);
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
    await expect(
      f.coordinator.bindLaptopInstall({ ...binding, fromGeneration: "other" }),
    ).rejects.toThrow();
    f.setEvidence({ activeTree: old });
    await expect(f.coordinator.bindLaptopInstall(binding)).rejects.toThrow();
    f.setEvidence({ activeTree: tree });
    await f.coordinator.bindLaptopInstall(binding);
    await f.coordinator.bindLaptopInstall(binding);
    native.resolve({ accepted: true, completed: false, failed: false });
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

  it("stops only the latest Laptop download selected before a proof pause", async () => {
    const f = fixture();
    const plan = await f.start();
    const download = { type: "laptop", planId: plan.planId, action: "download" } as const;
    const initial = Promise.withResolvers<{ accepted: boolean; completed: boolean }>();
    f.controller.download.mockImplementationOnce(() => initial.promise);
    const first = (await f.coordinator.request(download)).state.current!.laptop.operations[0]!;
    const completed = Promise.withResolvers<void>();
    f.interceptNextWrite(async (commit) => {
      commit();
      completed.resolve();
    });
    initial.resolve({ accepted: true, completed: true });
    await completed.promise;

    const next = Promise.withResolvers<{ accepted: boolean; completed: boolean }>();
    f.controller.download.mockImplementationOnce(() => next.promise);
    const latest = (await f.coordinator.request(download)).state.current!.laptop.operations.at(-1)!;
    expect(latest.operationId).not.toBe(first.operationId);
    f.setEvidence({ committedTransactionId: undefined });
    const stop = { ...download, action: "stop" } as const;
    const legacy = await f.coordinator.request(stop);
    expect(legacy.refused).toEqual({
      code: "invalid-evidence",
      message: "Stop needs the current Laptop download ID. Refresh Update Status and try again.",
    });
    expect(legacy.state.current!.laptop.operations.at(-1)).toEqual(latest);
    expect(f.controller.stopDownload).not.toHaveBeenCalled();
    const stale = await f.coordinator.request({ ...stop, operationId: first.operationId });
    expect(stale.refused?.code).toBe("invalid-evidence");
    expect(stale.state.current!.laptop.gatePause).toBe(true);
    expect(stale.state.current!.laptop.operations.at(-1)).toEqual(latest);
    expect(f.controller.stopDownload).not.toHaveBeenCalled();
    const stopped = await f.coordinator.request({ ...stop, operationId: latest.operationId });
    expect(stopped.refused).toBeUndefined();
    expect(stopped.state.current!.laptop.operations.at(-1)).toMatchObject({
      step: "stop",
      of: latest.operationId,
    });
    expect(f.controller.stopDownload).toHaveBeenCalledExactlyOnceWith({
      sourceSha: source,
      operationId: latest.operationId,
    });
    next.resolve({ accepted: true, completed: false });
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
      installedSource: { repository: target.repository, sha: source, tree },
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
    expectedInstalledSource: source,
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
    expectedInstalledSource: source,
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
      expectedInstalledSource: source,
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

it("abandons only the Laptop stage created by the plan", async () => {
  const f = fixture();
  const originalPlan = await f.start([]);
  f.setEvidence({ activeSource: old, committedTransactionId: undefined });
  const abandon = () =>
    f.coordinator.request({ type: "laptop", planId: originalPlan.planId, action: "abandon-stage" });
  f.setStaged();
  expect((await abandon()).refused?.code).toBe("invalid-evidence");
  expect(f.controller.discard).not.toHaveBeenCalled();
});

it.each([false, true])(
  "rechecks a Laptop abandonment after losing the discard result (closed: %s)",
  async (closed) => {
    const f = fixture();
    f.setEvidence({ activeSource: old, committedTransactionId: undefined });
    f.controller.download.mockImplementation(async () => {
      f.setStaged();
      return { accepted: true, completed: true, createdStagedHandle: "stage" };
    });
    const started = await f.coordinator.request({
      type: "start",
      origin: "update-button",
      target,
      select: [],
      laptop: { action: "download" },
    });
    expect(started.refused).toBeUndefined();
    const planId = started.state.current!.planId;
    const abandon = () =>
      f.coordinator.request({ type: "laptop", planId, action: "abandon-stage" });
    const retained = () => f.snapshot().document.current ?? f.snapshot().document.last;
    const operation = () => retained()?.laptop.operations.at(-1);
    await vi.waitFor(() => expect(operation()?.createdStagedHandle).toBe("stage"));
    const discard = Promise.withResolvers<{ accepted: boolean; completed: boolean }>();
    f.controller.discard.mockImplementationOnce(() => discard.promise);
    const retiring = await abandon();
    expect(retiring.refused).toBeUndefined();
    if (closed) {
      const cancelled = await f.coordinator.request({
        type: "cancel-plan",
        planId,
        expectedRevision: retiring.state.current!.revision,
      });
      expect(cancelled.state.current).toBeNull();
      expect(cancelled.state.recoveryHistory?.[0]?.pendingLaptop).toBe(true);
    }
    discard.resolve({ accepted: false, completed: false });
    const abandonment = () => retained()?.laptop.stageAbandonment?.status;
    await vi.waitFor(() => expect(abandonment()).toBe("unconfirmed"));
    const reopened = createInstallPlanCoordinator(f.options);
    expect(await reopened.preparationHeld()).toBe(true);
    if (closed) {
      const read = await reopened.request({ type: "read-history", planId });
      const wrong = await reopened.request({
        type: "laptop",
        planId,
        action: "abandon-stage",
        expectedRevision: read.historyPlan!.revision,
        operationId: id(126),
      });
      expect(wrong.refused?.code).toBe("invalid-evidence");
    }
    f.setEvidence({ activationBlocked: true });
    f.controller.isLaptopStageAbsent.mockResolvedValueOnce(true);
    await reopened.request({ type: "read" });
    expect(abandonment()).toBe("retired");
    expect(operation()?.result).toMatchObject({ status: "stopped", code: "stage-retired" });
    expect(f.controller.discard.mock.calls).toEqual([["stage"]]);
    expect(f.controller.isLaptopStageAbsent).toHaveBeenCalledWith("selection.json", "stage");
    expect(f.controller.install).not.toHaveBeenCalled();
  },
);

it("keeps an abandoned attempt in history and admits a distinct download after re-adding", async () => {
  const f = fixture();
  const plan = await f.start();
  const grant = (await f.claim(plan.planId)).grant!;
  const request = (revision: number, phase: "begin" | "finish") =>
    f.coordinator.request({
      type: "abandon-device",
      phase,
      planId: plan.planId,
      environmentId: remote,
      expectedRevision: revision,
    });
  const current = () => f.snapshot().document.current!;
  expect((await request(current().revision, "begin")).refused?.code).toBe("outstanding-operation");
  await f.coordinator.request({
    type: "report",
    planId: plan.planId,
    environmentId: remote,
    operationId: grant.operationId,
    evidence: { kind: "receipt", receipt: { ...f.receipt(grant), phase: "failed" } },
  });
  expect((await request(current().revision, "begin")).refused).toBeUndefined();
  expect((await f.claim(plan.planId)).refused).toBeDefined();
  expect((await request(current().revision, "finish")).refused).toBeUndefined();
  expect(current().devices[0]).toMatchObject({ membership: "removed", archivedOperationCount: 1 });
  await f.coordinator.request({
    type: "edit",
    planId: plan.planId,
    expectedRevision: current().revision,
    select: [selection],
    deselect: [],
  });
  const retry = (await f.claim(plan.planId)).grant;
  expect(retry?.operationId).toBeDefined();
  expect(retry?.operationId).not.toEqual(grant.operationId);
  expect(current().devices[0]?.operations.map((operation) => operation.operationId)).toEqual([
    grant.operationId,
    retry?.operationId,
  ]);
});

it("keeps proof of a running external build without claiming it was installed by the plan", async () => {
  const f = fixture();
  const plan = await f.start();
  const end = (sourceTree: string) =>
    f.coordinator.request({
      type: "end",
      planId: plan.planId,
      environmentId: remote,
      expectedRevision: plan.revision,
      runningTarget: {
        build: { version: target.version, sourceSha: source, sourceTree },
        observedAt: f.options.now(),
      },
    });
  expect((await end("f".repeat(40))).refused?.code).toBe("invalid-evidence");
  const result = await end(tree);
  expect(result.refused).toBeUndefined();
  expect(result.state.current?.devices[0]).toMatchObject({
    verifiedElsewhere: { version: target.version },
    operations: [],
  });
  expect(result.state.view.complete).toBe(true);
});

it.each(["installed", "replaced"] as const)(
  "finishes abandonment after the selected stage was %s externally",
  async (disposition) => {
    const f = fixture();
    const plan = await f.start();
    const grant = (await f.claim(plan.planId)).grant!;
    const downloaded = f.receipt(grant, "completed");
    const report = await f.coordinator.request({
      type: "report",
      planId: plan.planId,
      environmentId: remote,
      operationId: grant.operationId,
      evidence: {
        kind: "receipt",
        receipt: { ...downloaded, staged: { ...downloaded.staged!, createdByOperation: true } },
      },
    });
    expect(report.refused).toBeUndefined();
    const abandon = { type: "abandon-device", planId: plan.planId, environmentId: remote } as const;
    const begun = await f.coordinator.request({
      ...abandon,
      phase: "begin",
      expectedRevision: report.state.current!.revision,
    });
    expect(begun.refused).toBeUndefined();
    const finish = {
      ...abandon,
      phase: "finish",
      expectedRevision: begun.state.current!.revision,
    } as const;
    expect((await f.coordinator.request(finish)).refused?.code).toBe("invalid-evidence");
    const replacement = {
      operationId: grant.operationId,
      stagedHandle: "replacement-stage",
      observedAt: f.options.now(),
    };
    if (disposition === "replaced") {
      expect(
        (
          await f.coordinator.request({
            ...finish,
            stageReplacement: { ...replacement, stagedHandle: downloaded.staged!.stagedHandle },
          })
        ).refused?.code,
      ).toBe("invalid-evidence");
    }
    const result = await f.coordinator.request({
      ...finish,
      ...(disposition === "replaced"
        ? { stageReplacement: replacement }
        : {
            runningTarget: {
              build: { version: target.version, sourceSha: source, sourceTree: tree },
              observedAt: f.options.now(),
            },
          }),
    });
    expect(result.refused).toBeUndefined();
    expect(result.state.view.complete).toBe(true);
    const device = result.state.current!.devices[0]!;
    expect(device.abandoning).toBeUndefined();
    if (disposition === "installed") expect(device.verifiedElsewhere?.version).toBe(target.version);
    else expect(device).toMatchObject({ membership: "removed", stageReplacement: replacement });
    expect(device.operations[0]?.receipt?.stageRetiredAt).toBeUndefined();
  },
);
