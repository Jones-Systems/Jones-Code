import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, type DesktopUpdateState } from "@t3tools/contracts";
import type {
  JonesInstallPlan,
  JonesInstallPlanGrant,
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
  JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationReceipt,
  JonesRecoveryPreflight,
} from "@t3tools/contracts/jones/jonesOperations";
import { startPlan } from "@t3tools/client-runtime/jones/install-plan";
import type { JonesUpdateObservation } from "@t3tools/client-runtime/jones/updates";
import {
  describeUpdateEverything,
  updateEverythingRowHasWork,
  type UpdateEverythingMachine,
} from "../../components/sidebar/updateEverything.logic";
import { describeJonesBulkTargets } from "./bulkUpdates";
import {
  routeInstallStatusPill,
  routeLaptopDownload,
  routeLaptopInstall,
  partitionUpdateEverything,
  legacyConnectionsGate,
} from "./entrypoints";
import {
  fencePanelOperation,
  performPanelAction,
  refreshDeviceEvidence,
  stopPanelDownload,
} from "./actions";
import { createInstallStatusStore, installStatusStore, type InstallStatusDevice } from "./panelUi";
import {
  bindInstallPlanServices,
  confirmClosedPlan,
  installPlanServices,
  type InstallPlanServices,
} from "./services";
import { abandonDevice, deviceAbandonTarget } from "./abandonDevice";

const environmentId = EnvironmentId.make("laptop-test");
const source = "a".repeat(40);
const target = {
  repository: "Jones-Systems/Jones-Code" as const,
  sourceSha: source,
  sourceTree: "b".repeat(40),
  version: "0.1.2",
};
const plan = startPlan({
  planId: "11111111-1111-4111-8111-111111111111",
  origin: "update-button",
  target,
  from: { version: "0.1.1" },
  laptopEnvironmentId: environmentId,
  select: [],
  now: "2026-10-27T04:00:00.000Z",
});
const native = (
  current: JonesInstallPlanState["current"],
  last: JonesInstallPlanState["last"] = null,
): JonesInstallPlanState => ({
  schema: 1,
  current,
  last,
  laptop: { environmentId, observedAt: "2026-10-27T04:00:00.000Z", activationBlocked: false },
  view: { complete: false },
});
const desktop: DesktopUpdateState = {
  enabled: true,
  status: "available",
  channel: "latest",
  currentVersion: "0.1.1",
  availableVersion: "0.1.2",
  downloadedVersion: null,
  hostArch: "arm64",
  appArch: "arm64",
  runningUnderArm64Translation: false,
  releaseNotes: [],
  omittedReleaseCount: 0,
  downloadPercent: null,
  checkedAt: null,
  message: null,
  errorContext: null,
  canRetry: false,
  jones: {
    source: "jones-actions",
    channel: "jones-main",
    phase: "available",
    capability: { check: true, download: true, install: true },
    provenance: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: target.sourceTree,
      version: target.version,
      workflow: "fixture-only",
      runId: 1,
      runAttempt: 1,
      artifactId: 99,
      artifactDigest: "c".repeat(64),
      platform: "darwin",
      architecture: "arm64",
    },
  },
};

describe("bound Install status actions", () => {
  const hostId = EnvironmentId.make("stop-host");
  const laptopOperationId = "22222222-2222-4222-8222-222222222222";
  const hostOperationId = "33333333-3333-4333-8333-333333333333";
  const earlierOperationId = "44444444-4444-4444-8444-444444444444";
  const localStopId = "55555555-5555-4555-8555-555555555555";
  const hostStopId = "66666666-6666-4666-8666-666666666666";
  const at = plan.createdAt;
  const input = {
    operationId: hostOperationId,
    environmentId: hostId,
    action: { kind: "download-exact", targetSource: source },
    expectedInstalledSource: "d".repeat(40),
    correlation: { planId: plan.planId, deviceKey: hostId },
  } as const;
  const running: JonesOperationReceipt = {
    protocol: 1,
    ...input,
    phase: "running",
    acceptedAt: at,
    updatedAt: at,
  };
  const grant: JonesInstallPlanGrant = {
    planId: plan.planId,
    environmentId: hostId,
    operationId: hostOperationId,
    leaseUntil: "2026-10-27T04:02:00.000Z",
    step: "stop",
    request: {
      kind: "operation-stop",
      input: { environmentId: hostId, operationId: hostOperationId, stopId: localStopId },
    },
  };
  const hostReceipt: JonesOperationReceipt = {
    ...running,
    phase: "stop-requested",
    stop: { stopId: hostStopId, requestedAt: at },
  };
  function bindFixture(initialGrant: JonesInstallPlanGrant = grant) {
    const state = native({
      ...plan,
      laptop: {
        ...plan.laptop,
        operations: [
          {
            operationId: laptopOperationId,
            step: "download",
            requestedAt: at,
            result: { status: "running", at },
          },
        ],
      },
      devices: [
        {
          environmentId: hostId,
          label: "Stop host",
          route: "operations",
          membership: "selected",
          addedAt: at,
          operations: [
            {
              operationId: hostOperationId,
              step: "download",
              request: { kind: "operation-start", input },
              receipt: running,
              lease: { claimant: "window:a", reservedAt: at, leaseUntil: grant.leaseUntil },
            },
          ],
        },
      ],
    });
    const installPlan = vi.fn(
      async (request: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> => ({
        state,
        ...(request.type === "claim" ? { grant: initialGrant } : {}),
      }),
    );
    const device: InstallStatusDevice = {
      environmentId: hostId,
      label: "Stop host",
      canOperate: true,
      observation: {
        freshness: "fresh",
        state: {
          ...desktop.jones!,
          environmentId: hostId,
          phase: "downloading",
          capability: { ...desktop.jones!.capability, operations: 1 },
          operation: running,
        },
      },
    };
    const unused = async () => {
      throw new Error("Unexpected host action.");
    };
    const stop = vi.fn<InstallPlanServices["stop"]>(async () => hostReceipt);
    const devices = vi.fn(() => [device]);
    const services: InstallPlanServices = {
      bridge: { installPlan, getUpdateState: async () => desktop, onUpdateState: () => () => {} },
      devices,
      observe: async () => device,
      legacy: unused,
      operation: unused,
      stop,
      host: unused,
      confirm: unused,
      chooseRecovery: unused,
      now: () => Date.parse(at),
    };
    const store = createInstallStatusStore();
    return {
      installPlan,
      stop,
      state,
      devices,
      services,
      store,
      unbind: bindInstallPlanServices(services, store),
    };
  }

  it.each(["laptop", "device"] as const)("refuses to retarget a stale %s Stop", async (kind) => {
    const f = bindFixture();
    try {
      const displayed = {
        kind,
        planId: plan.planId,
        environmentId: kind === "laptop" ? environmentId : hostId,
        operationId: kind === "laptop" ? laptopOperationId : hostOperationId,
      };
      await expect(
        stopPanelDownload({ ...displayed, operationId: earlierOperationId }),
      ).rejects.toThrow("This download is no longer current");
      await expect(stopPanelDownload({ ...displayed, planId: earlierOperationId })).rejects.toThrow(
        "This download is no longer current",
      );
      expect(f.installPlan.mock.calls.map(([request]) => request.type)).toEqual(["read", "read"]);
      expect(f.stop).not.toHaveBeenCalled();
    } finally {
      f.unbind();
    }
  });

  it.each(["read", "claim"] as const)(
    "blocks Stop when capability downgrades during the native %s",
    async (downgradeOn) => {
      const f = bindFixture();
      try {
        const device = await f.services.observe(hostId);
        f.installPlan.mockImplementation(async (request) => {
          if (request.type === downgradeOn)
            f.devices.mockReturnValue([
              {
                ...device,
                observation: {
                  freshness: "fresh",
                  state: {
                    ...device.observation.state!,
                    capability: {
                      check: false,
                      download: false,
                      install: false,
                      operationReceipts: 1,
                    },
                  },
                },
              },
            ]);
          return { state: f.state, ...(request.type === "claim" ? { grant } : {}) };
        });
        await expect(
          stopPanelDownload({
            kind: "device",
            planId: plan.planId,
            environmentId: hostId,
            operationId: hostOperationId,
          }),
        ).rejects.toThrow("Fresh authorized operation capability is required.");
        expect(f.installPlan.mock.calls.map(([request]) => request.type)).toEqual(
          downgradeOn === "read" ? ["read"] : ["read", "claim"],
        );
        expect(f.stop).not.toHaveBeenCalled();
      } finally {
        f.unbind();
      }
    },
  );

  it("keeps the exact Stop grant binding and reports the host's first Stop ID", async () => {
    const displayed = {
      kind: "device",
      planId: plan.planId,
      environmentId: hostId,
      operationId: hostOperationId,
    } as const;
    const mismatched = bindFixture({
      ...grant,
      request: {
        kind: "operation-stop",
        input: { operationId: earlierOperationId, environmentId: hostId, stopId: localStopId },
      },
    });
    try {
      await expect(stopPanelDownload(displayed)).rejects.toThrow(
        "does not match this Stop request",
      );
      expect(mismatched.stop).not.toHaveBeenCalled();
    } finally {
      mismatched.unbind();
    }
    const f = bindFixture();
    try {
      await stopPanelDownload(displayed);
      expect(f.installPlan.mock.calls[1]?.[0]).toMatchObject({
        type: "claim",
        step: "stop",
        explicit: true,
        planId: plan.planId,
        environmentId: hostId,
        operationId: hostOperationId,
      });
      expect(f.stop).toHaveBeenCalledExactlyOnceWith(hostId, {
        action: "operationStop",
        input: { environmentId: hostId, operationId: hostOperationId, stopId: localStopId },
      });
      expect(f.installPlan.mock.calls[2]?.[0]).toEqual({
        type: "report",
        planId: plan.planId,
        environmentId: hostId,
        operationId: hostOperationId,
        evidence: { kind: "receipt", receipt: hostReceipt },
      });
    } finally {
      f.unbind();
    }
  });

  it.each(["remove", "end", "download", "install"] as const)(
    "refuses a displayed plan A %s when native now reads plan B",
    async (action) => {
      const f = bindFixture();
      try {
        f.installPlan.mockResolvedValue({
          state: { ...f.state, current: { ...f.state.current!, planId: earlierOperationId } },
        });
        await expect(
          performPanelAction(hostId, action, {
            planId: plan.planId,
            expectedRevision: f.state.current!.revision,
            operationId: hostOperationId,
          }),
        ).rejects.toThrow("changed");
        expect(f.installPlan.mock.calls.map(([request]) => request.type)).toEqual(["read"]);
      } finally {
        f.unbind();
      }
    },
  );

  it("keeps the original creator and handle across abandonment confirmation in the same plan", async () => {
    const f = bindFixture();
    const current = f.state.current!;
    const device = current.devices[0]!;
    const operation = device.operations[0]!;
    const staged = {
      ...running,
      phase: "completed" as const,
      staged: {
        stagedHandle: "owned-first",
        sourceSha: source,
        sourceTree: target.sourceTree,
        artifactId: 8,
        createdByOperation: true as const,
      },
    };
    const withStage = {
      ...current,
      devices: [
        {
          ...device,
          operations: [{ ...operation, receipt: staged }],
        },
      ],
    };
    let state: JonesInstallPlanState = {
      ...f.state,
      current: withStage,
      view: { complete: false, deviceRecovery: 1 },
    };
    f.installPlan.mockImplementation(async () => ({ state }));
    const remote = vi.fn(f.services.operation);
    const unbind = bindInstallPlanServices(
      {
        ...f.services,
        operation: remote,
        confirm: async () => {
          state = {
            ...state,
            current: {
              ...withStage,
              revision: withStage.revision + 1,
              devices: [
                {
                  ...device,
                  addedAt: "2026-10-27T04:02:00Z",
                  operations: [
                    {
                      ...operation,
                      operationId: earlierOperationId,
                      receipt: {
                        ...staged,
                        operationId: earlierOperationId,
                        staged: { ...staged.staged, stagedHandle: "replacement" },
                      },
                    },
                  ],
                },
              ],
            },
          };
          return true;
        },
      },
      f.store,
    );
    try {
      await expect(abandonDevice(deviceAbandonTarget(withStage, hostId, false)!)).rejects.toThrow(
        "changed",
      );
      expect(f.installPlan.mock.calls.map(([request]) => request.type)).toEqual(["read", "read"]);
      expect(remote).not.toHaveBeenCalled();
    } finally {
      unbind();
      f.unbind();
    }
  });

  it("confirms closure of the exact plan even after another plan becomes Last update", async () => {
    const f = bindFixture();
    const first = {
      ...f.state.current!,
      revision: 2,
      dismissedAt: at,
      cancelled: true as const,
      cancelledFromRevision: 1,
    };
    const second = { ...first, planId: earlierOperationId, revision: 1 };
    const state = {
      ...f.state,
      current: null,
      last: second,
      documentRevision: 4,
      view: { complete: false, recoveryHistory: 1 as const },
    };
    f.store.update({ native: state });
    f.installPlan.mockImplementation(async (request) => ({
      state,
      ...(request.type === "read-history" && request.planId === first.planId
        ? { historyPlan: first }
        : {}),
    }));
    try {
      await expect(
        confirmClosedPlan(
          installPlanServices()!,
          first.planId,
          (closed) => closed.cancelledFromRevision === 1,
          { state },
        ),
      ).resolves.toEqual(first);
      expect(f.installPlan).toHaveBeenCalledExactlyOnceWith({
        type: "read-history",
        planId: first.planId,
      });
      expect(f.store.get().native?.last?.planId).toBe(second.planId);
    } finally {
      f.unbind();
    }
  });

  it.each([true, false])(
    "validates the original archived Fence grant body (matching %s)",
    async (matching) => {
      const f = bindFixture();
      const current = f.state.current!;
      const device = current.devices[0]!;
      const { receipt: _receipt, ...pending } = device.operations[0]!;
      let closed: JonesInstallPlan = {
        ...current,
        revision: 7,
        dismissedAt: at,
        cancelled: true as const,
        devices: [
          { ...device, operations: [{ ...pending, lease: { ...pending.lease, leaseUntil: at } }] },
        ],
      };
      const state = {
        ...f.state,
        current: { ...plan, planId: earlierOperationId },
        last: closed,
        documentRevision: 2,
        view: { complete: false, recoveryHistory: 1 as const },
      };
      const accepted = { ...running, phase: "accepted" as const };
      f.installPlan.mockImplementation(async (request) => {
        if (request.type === "report") {
          const recorded = closed.devices[0]!;
          closed = {
            ...closed,
            revision: closed.revision + 1,
            devices: [
              { ...recorded, operations: [{ ...recorded.operations[0]!, receipt: accepted }] },
            ],
          };
        }
        return {
          state: { ...state, last: closed },
          ...(request.type === "read-history" ? { historyPlan: closed } : {}),
          ...(request.type === "claim"
            ? {
                grant: {
                  ...grant,
                  step: "fence",
                  request: {
                    kind: "operation-fence",
                    input: matching ? input : { ...input, expectedInstalledSource: "e".repeat(40) },
                  },
                },
              }
            : {}),
        };
      });
      const operation = vi.fn(async () => accepted);
      const unbind = bindInstallPlanServices({ ...f.services, operation }, f.store);
      f.store.update({ native: state });
      try {
        const action = fencePanelOperation({
          planId: closed.planId,
          environmentId: hostId,
          operationId: hostOperationId,
          expectedRevision: closed.revision,
          archived: true,
        });
        if (matching) {
          await action;
          expect(operation).toHaveBeenCalledExactlyOnceWith(hostId, {
            action: "operationFence",
            input,
          });
          expect(f.installPlan).toHaveBeenCalledWith({
            type: "report",
            planId: closed.planId,
            environmentId: hostId,
            operationId: hostOperationId,
            evidence: { kind: "receipt", receipt: accepted },
          });
        } else {
          await expect(action).rejects.toThrow("does not match the original operation");
          expect(operation).not.toHaveBeenCalled();
        }
        expect(f.installPlan).toHaveBeenCalledWith({
          type: "claim",
          step: "fence",
          explicit: true,
          planId: closed.planId,
          environmentId: hostId,
          operationId: hostOperationId,
          expectedRevision: 7,
          claimant: expect.any(String),
        });
        expect(f.store.get().native?.current?.planId).toBe(earlierOperationId);
      } finally {
        unbind();
        f.unbind();
      }
    },
  );

  it.each(["operationReceipts", "operations"] as const)(
    "%s supports exact readback with disabled updates and requires current scope for explicit refresh",
    async (receiptCapability) => {
      const f = bindFixture();
      const current = f.state.current!;
      const device = current.devices[0]!;
      const receipt: JonesOperationReceipt = {
        ...running,
        action: {
          kind: "install",
          targetSource: source,
          currentVersion: "old",
          stagedHandle: "stage",
        },
      };
      const planWithInstall = {
        ...current,
        devices: [
          {
            ...device,
            operations: [
              {
                ...device.operations[0]!,
                step: "install" as const,
                request: { kind: "operation-start" as const, input: receipt },
                receipt,
              },
            ],
          },
        ],
      };
      const observed = await f.services.observe(hostId);
      const status = {
        ...observed.observation.state!,
        capability: {
          check: false,
          download: false,
          install: false,
          [receiptCapability]: 1 as const,
        },
      };
      const authorized = { ...observed, observation: { ...observed.observation, state: status } };
      const operation = vi.fn<InstallPlanServices["operation"]>(async () => ({
        receipts: [{ operationId: hostOperationId, receipt }],
      }));
      let canOperate = false;
      let liveObservation: InstallStatusDevice["observation"] = authorized.observation;
      const evidenceServices = {
        ...f.services,
        devices: () => [{ ...authorized, observation: liveObservation, canOperate }],
        operation,
      };
      const unbindEvidence = bindInstallPlanServices(evidenceServices, f.store);
      const binding = installPlanServices()!;
      const check = (observed: InstallStatusDevice, refreshInstallOutcome: boolean) =>
        refreshDeviceEvidence(binding, hostId, planWithInstall, observed, {
          refreshInstallOutcome,
        });
      const exactRead = { environmentId: hostId, operationIds: [hostOperationId] };
      try {
        await check(authorized, true);
        expect(operation).toHaveBeenLastCalledWith(hostId, {
          action: "operationRead",
          input: exactRead,
        });
        canOperate = true;
        await check(authorized, false);
        expect(operation).toHaveBeenLastCalledWith(hostId, {
          action: "operationRead",
          input: exactRead,
        });
        await check(authorized, true);
        expect(operation).toHaveBeenLastCalledWith(hostId, {
          action: "operationRead",
          input: { ...exactRead, refreshInstallOutcome: hostOperationId },
        });
        expect(f.installPlan).toHaveBeenLastCalledWith({
          type: "report",
          planId: plan.planId,
          environmentId: hostId,
          operationId: hostOperationId,
          evidence: { kind: "receipt", receipt },
        });
        const directlyRead = {
          ...authorized,
          directStatus: true as const,
          observation: { ...authorized.observation, heardAt: 20 },
        };
        liveObservation = { freshness: "stale", state: status, heardAt: 10 };
        await check(directlyRead, true);
        expect(operation).toHaveBeenLastCalledWith(hostId, {
          action: "operationRead",
          input: { ...exactRead, refreshInstallOutcome: hostOperationId },
        });

        liveObservation = {
          freshness: "fresh",
          heardAt: 21,
          state: { ...status, capability: { check: true, download: false, install: false } },
        };
        const limited = await check(directlyRead, true);
        expect(operation).toHaveBeenLastCalledWith(hostId, {
          action: "operationRead",
          input: exactRead,
        });
        expect(limited).toContain("Checking its launcher requires fresh status and permission.");

        liveObservation = {
          freshness: "fresh",
          heardAt: 20,
          state: { ...status, capability: { check: true, download: false, install: false } },
        };
        await check(directlyRead, true);
        expect(operation).toHaveBeenLastCalledWith(hostId, {
          action: "operationRead",
          input: exactRead,
        });

        liveObservation = { freshness: "unsupported", state: null, heardAt: 10, unsupportedAt: 21 };
        await check(directlyRead, true);
        expect(operation).toHaveBeenLastCalledWith(hostId, {
          action: "operationRead",
          input: exactRead,
        });

        await check({ ...authorized, observation: { freshness: "stale", state: status } }, true);
        await check(
          {
            ...authorized,
            observation: { freshness: "fresh", state: { ...status, environmentId } },
          },
          true,
        );
        expect(operation).toHaveBeenCalledTimes(7);

        liveObservation = { freshness: "stale", state: status, heardAt: 10 };
        const unbind = bindInstallPlanServices(
          {
            ...binding.services,
            observe: async () => ({ ...authorized, observation: liveObservation }),
            readStatus: async () => status,
          },
          f.store,
        );
        try {
          const state = { ...f.state, current: planWithInstall };
          f.installPlan.mockResolvedValueOnce({
            state: { ...state, readOnly: { code: "invalid", message: "The plan is read-only." } },
          });
          await performPanelAction(hostId, "check", { planId: plan.planId });
          expect(operation).toHaveBeenLastCalledWith(hostId, {
            action: "operationRead",
            input: exactRead,
          });
          expect(f.store.get().error).toBe("The plan is read-only.");

          f.installPlan.mockResolvedValueOnce({
            state,
            refused: { code: "storage", message: "The plan read was refused." },
          });
          await performPanelAction(hostId, "check", { planId: plan.planId });
          expect(operation).toHaveBeenLastCalledWith(hostId, {
            action: "operationRead",
            input: exactRead,
          });
          expect(f.store.get().error).toBe("The plan read was refused.");
        } finally {
          unbind();
        }
        expect(f.stop).not.toHaveBeenCalled();
      } finally {
        unbindEvidence();
        f.unbind();
      }
    },
  );
});

describe("install entrypoint routing", () => {
  it("settles an unresolved directive only from exact fresh host status", async () => {
    const hostId = EnvironmentId.make("vps");
    const enrollmentId = "22222222-2222-4222-8222-222222222222";
    const directiveId = "33333333-3333-4333-8333-333333333333";
    const observedAt = "2026-10-27T04:00:00.000Z";
    const directive = {
      directiveId,
      targetSource: target.sourceSha,
      targetTree: target.sourceTree,
      mode: "manual-review" as const,
      issuedAt: observedAt,
    };
    const state = native({
      ...plan,
      devices: [
        {
          environmentId: hostId,
          enrollmentId,
          label: "VPS",
          route: "follower-operations",
          membership: "selected",
          addedAt: observedAt,
          operations: [
            {
              operationId: directiveId,
              step: "directive",
              lease: { claimant: "window:a", reservedAt: observedAt, leaseUntil: observedAt },
              request: {
                kind: "plan-directive",
                input: {
                  directiveId,
                  environmentId: hostId,
                  enrollmentId,
                  repository: target.repository,
                  expectedDirectiveId: null,
                  targetSource: target.sourceSha,
                  targetTree: target.sourceTree,
                },
              },
            },
          ],
        },
      ],
    });
    const installPlan = vi.fn(
      async (_request: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> => ({ state }),
    );
    const fleetStatus = {
      environmentId: hostId,
      operationProtocol: 1 as const,
      planDirectiveProtocol: 1 as const,
      enrollment: {
        environmentId: hostId,
        enrollmentId,
        enabled: true,
        continueRunningThreads: false,
      },
      operation: null,
      update: null,
      follower: {
        protocol: 1 as const,
        leaderSource: target.sourceSha,
        state: "waiting-for-leader" as const,
        planDirective: directive,
      },
    };
    const exact: InstallStatusDevice = {
      environmentId: hostId,
      label: "VPS",
      canOperate: true,
      observation: {
        freshness: "fresh",
        state: {
          environmentId: hostId,
          source: "jones-actions",
          channel: "jones-main",
          phase: "no-new",
          currentVersion: "0.1.1",
          capability: { check: true, download: true, install: true, operations: 1 },
        },
      },
      fleetStatus,
    };
    const unused = vi.fn(async () => {
      throw new Error("No host mutation is allowed.");
    });
    const observe = vi.fn(async () => exact);
    const services: InstallPlanServices = {
      bridge: { installPlan, getUpdateState: async () => desktop, onUpdateState: () => () => {} },
      devices: () => [exact],
      observe,
      legacy: unused,
      operation: unused,
      stop: unused,
      host: unused,
      confirm: unused,
      chooseRecovery: unused,
      now: () => 0,
    };
    const unbind = bindInstallPlanServices(services, createInstallStatusStore());
    try {
      observe.mockResolvedValueOnce({
        ...exact,
        fleetStatus: {
          ...fleetStatus,
          follower: {
            ...fleetStatus.follower,
            planDirective: { ...directive, directiveId: "44444444-4444-4444-8444-444444444444" },
          },
        },
      });
      await performPanelAction(hostId, "check");
      observe.mockResolvedValueOnce({
        ...exact,
        observation: { ...exact.observation, freshness: "stale" },
      });
      await performPanelAction(hostId, "check");
      expect(installPlan.mock.calls.map(([request]) => request.type)).toEqual(["read", "read"]);

      await performPanelAction(hostId, "check");
      expect(installPlan.mock.calls.at(-1)?.[0]).toEqual({
        type: "report",
        planId: plan.planId,
        environmentId: hostId,
        operationId: directiveId,
        evidence: { kind: "directive", status: fleetStatus },
      });
      expect(unused).not.toHaveBeenCalled();
    } finally {
      unbind();
    }
  });

  it("requires the full matching ordinary gate proof for legacy Connections", async () => {
    const ordinaryRecovery = {
      transactionId: `manual-${"d".repeat(64)}`,
      activeGeneration: "d".repeat(64),
      manifestSha256: "e".repeat(64),
      receiptSha256: "f".repeat(64),
    };
    const gate = {
      openedAt: "2026-10-27T04:00:00Z",
      basis: "prior-verified" as const,
      verificationKind: "ordinary-recovery" as const,
      ordinaryRecovery,
      transactionId: ordinaryRecovery.transactionId,
      activeGeneration: ordinaryRecovery.activeGeneration,
      activeSource: source,
      activeTree: target.sourceTree,
    };
    let state = native({ ...plan, laptop: { ...plan.laptop, gate } });
    state = {
      ...state,
      laptop: {
        ...state.laptop,
        ordinaryRecovery,
        activeGeneration: ordinaryRecovery.activeGeneration,
        activeSource: source,
        activeTree: target.sourceTree,
      },
    };
    const installPlan = vi.fn(async (): Promise<JonesInstallPlanResult> => ({ state }));
    const unused = async () => {
      throw new Error("No host mutation is allowed.");
    };
    const services: InstallPlanServices = {
      bridge: { installPlan, getUpdateState: async () => desktop, onUpdateState: () => () => {} },
      devices: () => [],
      observe: unused,
      legacy: unused,
      operation: unused,
      stop: unused,
      host: unused,
      confirm: unused,
      chooseRecovery: unused,
      now: () => 0,
    };
    vi.stubGlobal("window", { desktopBridge: services.bridge });
    const unbind = bindInstallPlanServices(services, createInstallStatusStore());
    try {
      expect(await legacyConnectionsGate(source, environmentId)).toBeNull();
      const valid = state;
      const { ordinaryRecovery: _gateProof, ...unprovedGate } = gate;
      for (const ordinary of [
        undefined,
        { ...ordinaryRecovery, transactionId: "manual-" + "9".repeat(64) },
        { ...ordinaryRecovery, activeGeneration: "9".repeat(64) },
      ]) {
        state = {
          ...valid,
          current: {
            ...plan,
            laptop: {
              ...plan.laptop,
              gate: ordinary === undefined ? unprovedGate : { ...gate, ordinaryRecovery: ordinary },
            },
          },
        };
        expect(await legacyConnectionsGate(source, environmentId)).not.toBeNull();
      }
      const { ordinaryRecovery: _liveProof, ...unproved } = valid.laptop;
      state = { ...valid, laptop: unproved };
      expect(await legacyConnectionsGate(source, environmentId)).not.toBeNull();
      const peer = EnvironmentId.make("retained-peer");
      state = {
        ...valid,
        documentRevision: 4,
        view: { complete: false, recoveryHistory: 1 },
        recoveryHistory: [
          {
            planId: "22222222-2222-4222-8222-222222222222",
            revision: 3,
            targetVersion: "earlier build",
            closedAt: plan.createdAt,
            pendingLaptop: false,
            pendingDevices: [{ environmentId: peer, label: "Peer" }],
          },
        ],
      };
      expect(await legacyConnectionsGate(source, peer)).toContain("outstanding work");
      expect(await legacyConnectionsGate(source, environmentId)).toBeNull();
      state = { ...state, readOnly: { code: "invalid", message: "Native plan is read-only." } };
      expect(await legacyConnectionsGate(source, environmentId)).toBe("Native plan is read-only.");
      state = { ...valid, documentRevision: 5 };
      installPlan.mockResolvedValueOnce({
        state,
        refused: { code: "storage", message: "Native plan read refused." },
      });
      expect(await legacyConnectionsGate(source, environmentId)).toBe("Native plan read refused.");
    } finally {
      unbind();
      vi.unstubAllGlobals();
    }
  });
  it("never serializes a client-only legacy review into native Laptop IPC", async () => {
    const bridge = { installPlan: vi.fn() };
    await expect(
      routeLaptopInstall(bridge, desktop, {
        kind: "legacy-staged",
        environmentId,
        reviewedAt: "2026-10-27T04:00:00Z",
        stagedHandle: "legacy-stage",
        currentVersion: "old",
        targetSource: source,
      }),
    ).rejects.toThrow("full native review");
    expect(bridge.installPlan).not.toHaveBeenCalled();
  });
  it.each(["downloading", "preparing", "installing", "blocked"] as const)(
    "opens a current plan during %s without a Laptop or host mutation",
    async (phase) => {
      const requests: JonesInstallPlanRequest[] = [];
      const bridge = {
        installPlan: vi.fn(async (request: JonesInstallPlanRequest) => {
          requests.push(request);
          return { state: native(plan) };
        }),
      };
      expect(
        await routeInstallStatusPill(
          bridge,
          { ...desktop, jones: { ...desktop.jones!, phase } },
          true,
        ),
      ).toBe("opened");
      expect(requests).toEqual([{ type: "read" }]);
      expect(installStatusStore.get().open).toBe(true);
    },
  );
  it("retains the old bridge route and opens the preview only before other updates", async () => {
    expect(await routeInstallStatusPill({}, desktop, false)).toBe("legacy");
    const bridge = { installPlan: vi.fn(async () => ({ state: native(null) })) };
    expect(await routeInstallStatusPill(bridge, desktop, true)).toBe("legacy");
    expect(await routeInstallStatusPill(bridge, desktop, false)).toBe("opened");
    expect(installStatusStore.get().previewSelection).toEqual([]);
  });
  it("reopens last read-only and leaves the existing Check action to the caller", async () => {
    const bridge = {
      installPlan: vi.fn(async () => ({
        state: native(null, { ...plan, dismissedAt: "2026-10-27T04:10:00.000Z" }),
      })),
    };
    const { provenance: _provenance, ...jones } = desktop.jones!;
    expect(
      await routeInstallStatusPill(
        bridge,
        { ...desktop, jones: { ...jones, phase: "no-new" } },
        false,
      ),
    ).toBe("last-and-check");
    expect(installStatusStore.get().showLast).toBe(true);
    expect(bridge.installPlan).toHaveBeenCalledExactlyOnceWith({ type: "read" });
  });
  it("starts Laptop through native plan intent and never prepares a fleet campaign", async () => {
    const requests: JonesInstallPlanRequest[] = [];
    const bridge = {
      installPlan: vi.fn(async (request: JonesInstallPlanRequest) => {
        requests.push(request);
        return { state: native(request.type === "start" ? plan : null) };
      }),
    };
    await routeLaptopDownload(bridge, desktop);
    expect(requests.map((r) => r.type)).toEqual(["read", "start"]);
    expect(requests[1]).toMatchObject({
      type: "start",
      select: [],
      laptop: { action: "download" },
      target,
    });
  });
  it.each(["clone-required", null] as const)(
    "reviews a cold Laptop stage without creating a plan before recovery choice %s",
    async (policy) => {
      const requests: JonesInstallPlanRequest[] = [];
      const staged: DesktopUpdateState = {
        ...desktop,
        status: "downloaded",
        downloadedVersion: target.version,
        jones: {
          ...desktop.jones!,
          phase: "staged",
          stagedHandle: "cold-stage",
          currentVersion: desktop.currentVersion,
          installedSource: {
            repository: "Jones-Systems/Jones-Code",
            sha: "c".repeat(40),
            tree: "d".repeat(40),
          },
          capability: { ...desktop.jones!.capability, recoveryPolicy: 1 },
        },
      };
      const measured: JonesRecoveryPreflight = {
        protocol: 1,
        preflightId: "22222222-2222-4222-8222-222222222222",
        measuredAt: "2026-10-27T04:00:00.000Z",
        expiresAt: "2026-10-27T04:05:00.000Z",
        binding: {
          environmentId,
          stagedHandle: "cold-stage",
          currentVersion: desktop.currentVersion,
          expectedInstalledSource: "c".repeat(40),
        },
        firstInstall: false,
        stateBytes: 1024,
        volumes: [],
        clone: { status: "proved" },
        policies: { cloneOrCopy: { admissible: false }, cloneRequired: { admissible: true } },
      };
      const installPlan = vi.fn(
        async (request: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> => {
          requests.push(request);
          if (request.type === "recovery-preflight")
            return { state: native(null), preflight: measured };
          if (request.type === "read") return { state: native(null) };
          if (request.type === "start") return { state: native(plan) };
          throw new Error("Unexpected plan mutation before review");
        },
      );
      const unused = async () => {
        throw new Error("Unexpected host dispatch");
      };
      const services: InstallPlanServices = {
        bridge: { installPlan, getUpdateState: async () => staged, onUpdateState: () => () => {} },
        devices: () => [],
        observe: unused,
        legacy: unused,
        operation: unused,
        stop: unused,
        host: unused,
        confirm: vi.fn(async () => true),
        chooseRecovery: vi.fn(async () => policy),
        now: () => 1793073600000,
      };
      const unbind = bindInstallPlanServices(services, createInstallStatusStore());
      try {
        const result = routeLaptopInstall(services.bridge, staged);
        if (policy === null) await expect(result).rejects.toThrow("Nothing was sent.");
        else await expect(result).resolves.toMatchObject({ accepted: true, completed: false });
        expect(services.confirm).toHaveBeenCalledOnce();
        expect(services.chooseRecovery).toHaveBeenCalledExactlyOnceWith(measured);
        expect(requests.map((request) => request.type)).toEqual(
          policy === null
            ? ["read", "read", "recovery-preflight"]
            : ["read", "read", "recovery-preflight", "read", "start"],
        );
        expect(requests[2]).toEqual({ type: "recovery-preflight", input: measured.binding });
        if (policy !== null)
          expect(requests[4]).toMatchObject({
            type: "start",
            laptop: {
              action: "install",
              review: {
                recovery: { policy, preflightId: measured.preflightId },
                stagedHandle: "cold-stage",
              },
            },
          });
      } finally {
        unbind();
      }
    },
  );
  it("partitions all Jones machines without dropping current, offline or unsupported targets", () => {
    const machines = [
      { environmentId: EnvironmentId.make("vps"), label: "VPS", jones: true, current: true },
      { environmentId: EnvironmentId.make("mini"), label: "Mini", jones: true, offline: true },
      {
        environmentId: EnvironmentId.make("desktop"),
        label: "Desktop",
        jones: true,
        supported: false,
      },
      { environmentId: EnvironmentId.make("npm"), label: "npm", jones: false },
    ];
    const result = partitionUpdateEverything(machines, (machine) => machine.jones);
    expect(result.jones.map((m) => m.label)).toEqual(["VPS", "Mini", "Desktop"]);
    expect(result.other.map((m) => m.label)).toEqual(["npm"]);
  });
});

describe("Jones machine update counts", () => {
  const laptop = { environmentId, label: "Laptop" };
  const mini = peer("Mac Mini");
  const vps = peer("VPS");
  const testServer = peer("Test Server");
  const peers = [mini, vps, testServer];
  const offered = { source, version: "0.1.9-preview.20261118.120.1" };
  const linuxVersion = "0.1.9-preview.20261118.121.1";

  function peer(label: string): UpdateEverythingMachine {
    return {
      environmentId: EnvironmentId.make(label),
      label,
      connected: true,
      hostedByThisApp: false,
      canMaintain: true,
      serverConfig: null,
      updateState: { status: "idle" },
    };
  }

  function running(sha: string, version: string): JonesUpdateObservation {
    return {
      freshness: "fresh",
      state: {
        phase: "no-new",
        source: "jones-actions",
        channel: "jones-main",
        capability: { check: true, download: false, install: false },
        currentVersion: version,
        installedSource: { repository: target.repository, sha, tree: target.sourceTree },
      },
    };
  }

  function describeReview(machines: ReturnType<typeof describeJonesBulkTargets>["machines"]) {
    return describeUpdateEverything({
      localVersion: null,
      localMachines: [],
      jonesMachines: machines,
      servers: [],
      providerMachines: [],
      skipped: [],
    });
  }

  it("counts Laptop once and counts each peer that needs the candidate source", () => {
    const formerSource = "c".repeat(40);
    const observations = new Map(
      peers.map((machine) => [machine.environmentId, running(formerSource, offered.version)]),
    );
    const { machines, hasPeerUpdates } = describeJonesBulkTargets(
      peers,
      observations,
      offered,
      laptop,
    );
    const { summary, rows } = describeReview(machines);
    expect(hasPeerUpdates).toBe(true);
    expect(summary).toBe("Update 4 Jones machines");
    expect(rows.filter(updateEverythingRowHasWork).map((row) => row.label)).toEqual([
      "Laptop",
      "Mac Mini",
      "VPS",
      "Test Server",
    ]);
    expect(rows[0]?.jonesHandoffNotice).toBe("Install status on Laptop; review before installing");
  });

  it("ignores different package versions for a matching source and skips unverified peers", () => {
    const observations = new Map([
      [mini.environmentId, running(source, offered.version)],
      [vps.environmentId, running(source, linuxVersion)],
      [testServer.environmentId, running("c".repeat(40), linuxVersion)],
    ]);
    const { machines, hasPeerUpdates } = describeJonesBulkTargets(
      [mini, vps, { ...testServer, connected: false }],
      observations,
      offered,
      laptop,
    );
    const { summary, rows } = describeReview(machines);
    expect(hasPeerUpdates).toBe(false);
    expect(summary).toBe("Update 1 Jones machine");
    expect(rows.filter(updateEverythingRowHasWork).map((row) => row.label)).toEqual(["Laptop"]);
    expect(rows.map((row) => row.skipped)).toEqual([
      null,
      "Already on selected build",
      "Already on selected build",
      "Couldn’t verify running build",
    ]);
  });
});
