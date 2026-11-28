import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, type DesktopUpdateState } from "@t3tools/contracts";
import type {
  JonesInstallPlan,
  JonesInstallPlanCommandReceipt,
  JonesInstallPlanGrant,
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
  JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationReceipt,
  JonesRecoveryPreflight,
} from "@t3tools/contracts/jones/jonesOperations";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import {
  advanceInstallPlan,
  startPlan,
  STATUS_POLL_CADENCE,
} from "@t3tools/client-runtime/jones/install-plan";
import type { JonesUpdateObservation } from "@t3tools/client-runtime/jones/updates";
import { JonesOperationNotSentError } from "@t3tools/client-runtime/jones/updates";
import {
  describeUpdateEverything,
  updateEverythingRowHasWork,
  type UpdateEverythingMachine,
} from "../../components/sidebar/updateEverything.logic";
import { describeJonesBulkTargets } from "./bulkUpdates";
import {
  connectionsPlanDispatch,
  routeInstallStatusPill,
  routeLaptopDownload,
  routeLaptopInstall,
  partitionUpdateEverything,
  legacyConnectionsGate,
} from "./entrypoints";
import {
  checkOrphanedPlanCommand,
  fencePanelOperation,
  inspectPanelCleanup,
  performPanelAction,
  refreshDeviceEvidence,
  stopPanelDownload,
} from "./actions";
import { createInstallStatusStore, installStatusStore, type InstallStatusDevice } from "./panelUi";
import {
  bindInstallPlanServices,
  confirmClosedPlan,
  currentPlanHostAuthority,
  installPlanServices,
  requestInstallPlan,
  type InstallPlanServices,
} from "./services";
import { abandonDevice, deviceAbandonTarget } from "./abandonDevice";
import { startInstallStatusPoller } from "./InstallStatusPoller";
import { startInstallPlanController } from "./InstallPlanController";

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

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

describe("Connections Match laptop", () => {
  const hostId = EnvironmentId.make("test-server");
  const runningSource = "6".repeat(40);
  const runningTree = "7".repeat(40);
  const generation = "8".repeat(64);
  const observedAt = plan.createdAt;
  const installed = {
    repository: target.repository,
    sha: runningSource,
    tree: runningTree,
  };
  const runningTarget = {
    repository: target.repository,
    sourceSha: runningSource,
    sourceTree: runningTree,
    version: "0.1.9-preview.20261119.44567304533.1",
  };

  it.each(["ready", "matching-source", "stale", "different-plan"] as const)(
    "uses only verified running evidence for exact handoff (%s)",
    async (scenario) => {
      const state: JonesInstallPlanState = {
        ...native(scenario === "different-plan" ? plan : null),
        view: { complete: false, matchRunningLaptop: 1 },
        laptop: {
          ...native(null).laptop,
          observedAt,
          activeSource: runningSource,
          activeTree: runningTree,
          activeVersion: runningTarget.version,
          activeGeneration: generation,
          committedTransactionId: generation,
        },
      };
      const device: InstallStatusDevice = {
        environmentId: hostId,
        label: "Test Server",
        canOperate: true,
        fleetStatus: null,
        observation: {
          freshness: "fresh",
          state: {
            ...desktop.jones!,
            environmentId: hostId,
            capability: { ...desktop.jones!.capability, operations: 1, exactSource: true },
            currentVersion: "0.1.9-preview.20261119.44567304618.1",
            installedSource: {
              ...installed,
              sha: scenario === "matching-source" ? runningSource : "4".repeat(40),
            },
          },
        },
      };
      const unused = async () => {
        throw new Error("Unexpected host dispatch during native handoff");
      };
      const getUpdateState = vi.fn(async () => desktop);
      const services: InstallPlanServices = {
        bridge: {
          installPlan: vi.fn(async () => ({ state })),
          getUpdateState,
          onUpdateState: () => () => {},
        },
        devices: () => [device],
        observe: async () => device,
        host: unused,
        legacy: unused,
        operation: unused,
        stop: unused,
        confirm: unused,
        chooseRecovery: unused,
        now: () => Date.parse(observedAt) + (scenario === "stale" ? 120_001 : 0),
      };
      const dispatch = vi.fn(async (_request: JonesInstallPlanRequest) => ({ state }));
      const unbind = bindInstallPlanServices(services, createInstallStatusStore());
      try {
        const { planGate } = connectionsPlanDispatch(
          installPlanServices(),
          { kind: "download-exact", targetSource: runningSource },
          undefined,
          () => dispatch,
          () => true,
          generation,
        );
        const result = planGate(hostId, runningSource);
        if (scenario === "stale" || scenario === "different-plan")
          await expect(result).resolves.toMatchObject({
            kind: "refused",
            message: expect.stringContaining(
              scenario === "stale"
                ? "verified running build changed"
                : "Review the current Laptop plan",
            ),
          });
        else if (scenario === "matching-source")
          await expect(result).resolves.toMatchObject({
            kind: "refused",
            message: expect.stringContaining("Already on"),
          });
        else {
          await expect(result).resolves.toMatchObject({ kind: "handled" });
          expect(dispatch).toHaveBeenCalledExactlyOnceWith(
            {
              type: "start",
              origin: "connections",
              expectedRunningGeneration: generation,
              target: runningTarget,
              select: [{ environmentId: hostId, label: "Test Server", route: "operations" }],
            },
            state,
          );
        }
        if (scenario !== "ready") expect(dispatch).not.toHaveBeenCalled();
        expect(getUpdateState).not.toHaveBeenCalled();
      } finally {
        unbind();
      }
    },
  );
});

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
  const ordinaryFleet: FleetHostStatus = {
    environmentId: hostId,
    operationProtocol: 1,
    enrollment: null,
    update: null,
    operation: null,
  };
  const enrolledFleet: FleetHostStatus = {
    ...ordinaryFleet,
    enrollment: {
      environmentId: hostId,
      enrollmentId: earlierOperationId,
      enabled: true,
      continueRunningThreads: false,
    },
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
    let saved = state;
    const installPlan = vi.fn(
      async (request: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> => {
        if (request.type !== "claim") return { state: saved };
        const member = state.current!.devices[0]!;
        if (initialGrant.request.kind === "operation-stop")
          saved = {
            ...state,
            current: {
              ...state.current!,
              devices: [
                {
                  ...member,
                  operations: [
                    {
                      ...member.operations[0]!,
                      stop: {
                        stopId: initialGrant.request.input.stopId,
                        lease: {
                          claimant: request.claimant,
                          reservedAt: at,
                          leaseUntil: initialGrant.leaseUntil,
                        },
                      },
                    },
                  ],
                },
              ],
            },
          };
        return { state: saved, grant: initialGrant };
      },
    );
    const device: InstallStatusDevice = {
      environmentId: hostId,
      label: "Stop host",
      canOperate: true,
      fleetStatus: ordinaryFleet,
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

  it.each(["current reservation", "closure observed before stale reply"] as const)(
    "authorizes setup at the final native read only for a %s",
    async (scenario) => {
      const f = bindFixture();
      const enrollmentId = earlierOperationId;
      const directive = {
        directiveId: hostOperationId,
        environmentId: hostId,
        enrollmentId,
        repository: target.repository,
        targetSource: target.sourceSha,
        targetTree: target.sourceTree,
        expectedDirectiveId: null,
      };
      const device = {
        ...f.state.current!.devices[0]!,
        route: "follower-operations" as const,
        enrollmentId,
        operations: [],
      };
      let active: JonesInstallPlan = {
        ...f.state.current!,
        revision: 7,
        laptop: {
          ...plan.laptop,
          gate: {
            openedAt: at,
            basis: "plan-install" as const,
            transactionId: "committed",
            activeGeneration: "generation",
            activeSource: source,
            activeTree: target.sourceTree,
          },
        },
        devices: [device],
      };
      const state = (): JonesInstallPlanState => ({
        ...native(active),
        documentRevision: active.revision,
        view: { complete: false, recoveryHistory: 1 },
      });
      const observed = await f.services.observe(hostId);
      const fleetStatus: FleetHostStatus = {
        ...enrolledFleet,
        planDirectiveProtocol: 1,
        follower: { protocol: 1, leaderSource: null, state: "waiting-for-leader" },
      };
      const hostDevice: InstallStatusDevice = {
        ...observed,
        fleetStatus,
        observation: {
          freshness: "fresh",
          state: {
            ...observed.observation.state!,
            phase: "available",
            installedSource: {
              repository: target.repository,
              sha: "c".repeat(40),
              tree: "d".repeat(40),
            },
          },
        },
      };
      const sendDecision = deferred<boolean>();
      let onPlan: ((value: JonesInstallPlanState) => void) | undefined;
      f.installPlan.mockImplementation(async (request) => {
        if (request.type === "claim") {
          const reservation = {
            operationId: directive.directiveId,
            step: "directive" as const,
            request: { kind: "plan-directive" as const, input: directive },
          };
          active = {
            ...active,
            revision: 8,
            devices: [
              {
                ...device,
                operations: [
                  {
                    ...reservation,
                    lease: {
                      claimant: request.claimant,
                      reservedAt: at,
                      leaseUntil: grant.leaseUntil,
                    },
                  },
                ],
              },
            ],
          };
          return {
            state: state(),
            grant: {
              ...reservation,
              planId: plan.planId,
              environmentId: hostId,
              leaseUntil: grant.leaseUntil,
            },
          };
        }
        const reply = state();
        if (
          request.type === "read" &&
          active.revision === 8 &&
          scenario === "closure observed before stale reply"
        )
          onPlan?.({
            ...reply,
            current: null,
            last: { ...active, dismissedAt: at, revision: 9 },
            documentRevision: 9,
          });
        return { state: reply };
      });
      const host = vi.fn<InstallPlanServices["host"]>(async (_id, request) => {
        if (request.action !== "planDirective") throw new Error("Unexpected host command.");
        const allowed = await Promise.resolve()
          .then(() => request.beforeSend?.())
          .catch(() => false);
        sendDecision.resolve(allowed === true);
        if (!allowed) throw new Error("The setup did not leave the queue.");
        return fleetStatus;
      });
      const controller = startInstallPlanController(
        {
          ...f.services,
          bridge: {
            ...f.services.bridge,
            onInstallPlan: (listener) => {
              onPlan = listener;
              return () => {
                onPlan = undefined;
              };
            },
          },
          devices: () => [hostDevice],
          observe: async () => hostDevice,
          host,
        },
        f.store,
      );
      try {
        await expect(sendDecision.promise).resolves.toBe(scenario === "current reservation");
        expect(host).toHaveBeenCalledExactlyOnceWith(hostId, {
          action: "planDirective",
          input: directive,
          beforeSend: expect.any(Function),
        });
        expect(f.store.get().native?.current?.planId ?? null).toBe(
          scenario === "current reservation" ? plan.planId : null,
        );
      } finally {
        controller.close();
        f.unbind();
      }
    },
  );

  it.each([
    "same route",
    "new failed read",
    "changed route",
    "lost read scope",
    "unbound after dispatch",
    "fence with unknown fleet status",
    "fence with a retained contradiction",
  ] as const)(
    "uses live enrollment and read authority in the background runner for %s",
    async (scenario) => {
      const f = bindFixture();
      const observed = await f.services.observe(hostId);
      const { fleetStatus: _previous, ...withoutFleet } = observed;
      const current = f.state.current!;
      const member = current.devices[0]!;
      const { receipt: _receipt, ...pending } = member.operations[0]!;
      const fencing = scenario.startsWith("fence");
      const reservation = fencing
        ? { ...pending, lease: { ...pending.lease, leaseUntil: at } }
        : pending;
      let state: JonesInstallPlanState = {
        ...f.state,
        view: { ...f.state.view, recoveryHistory: 1 },
        current: {
          ...current,
          devices: [{ ...member, operations: [reservation] }],
          laptop: {
            ...current.laptop,
            gate: {
              openedAt: at,
              basis: "plan-install",
              transactionId: "fixture",
              activeGeneration: "fixture",
              activeSource: source,
              activeTree: target.sourceTree,
            },
          },
        },
      };
      let latest = fencing ? withoutFleet : observed;
      let projection: InstallStatusDevice = withoutFleet;
      let canRead = true;
      const enrolled = {
        ...withoutFleet,
        fleetStatus: {
          environmentId: hostId,
          operationProtocol: 1 as const,
          planDirectiveProtocol: 1 as const,
          enrollment: {
            environmentId: hostId,
            enrollmentId: earlierOperationId,
            enabled: true,
            continueRunningThreads: false,
          },
          operation: null,
          update: null,
        },
      };
      const arrived = deferred();
      const reply = deferred();
      f.installPlan.mockImplementation(async (request) => {
        if (request.type !== "claim") return { state };
        arrived.resolve();
        await reply.promise;
        if (fencing)
          state = {
            ...state,
            current: {
              ...state.current!,
              devices: [
                {
                  ...member,
                  operations: [
                    {
                      ...reservation,
                      fence: {
                        lease: {
                          claimant: request.claimant,
                          reservedAt: at,
                          leaseUntil: grant.leaseUntil,
                        },
                      },
                    },
                  ],
                },
              ],
            },
          };
        return {
          state,
          grant: {
            ...grant,
            step: fencing ? "fence" : "download",
            request: { kind: fencing ? "operation-fence" : "operation-start", input },
          },
        };
      });
      const unbind = bindInstallPlanServices(
        {
          ...f.services,
          devices: () => [projection],
          observe: async () => latest,
          canReadReceipts: () => canRead,
        },
        f.store,
      );
      const binding = installPlanServices()!;
      const operation = vi.fn<Parameters<typeof advanceInstallPlan>[0]["operation"]>(
        async (_, request) => {
          if (request.action === "operationRead")
            return { receipts: [{ operationId: hostOperationId, receipt: running }] };
          if (!("beforeSend" in request) || !(await request.beforeSend?.()))
            throw new JonesOperationNotSentError({});
          if (scenario === "unbound after dispatch") unbind();
          return running;
        },
      );
      try {
        const pass = advanceInstallPlan({
          claimant: "window:a",
          now: binding.services.now,
          cancelled: () => installPlanServices() !== binding,
          desktop: (request) => requestInstallPlan(request, binding),
          observe: binding.services.observe,
          authority: (device) => currentPlanHostAuthority(binding, device),
          operation,
          host: binding.services.host,
        });
        await Promise.race([arrived.promise, pass]);
        if (scenario === "new failed read" || scenario === "lost read scope") {
          // A successful cache projection cannot restore authority after a later direct failure.
          projection = { ...observed, canOperate: false };
          latest = withoutFleet;
          canRead = scenario !== "lost read scope";
          await binding.services.observe(hostId);
        } else if (scenario === "changed route") {
          projection = enrolled;
        } else if (scenario === "fence with a retained contradiction") {
          latest = enrolled;
          await binding.services.observe(hostId);
          latest = withoutFleet;
          await binding.services.observe(hostId);
        }
        reply.resolve();
        await pass;
        expect(f.installPlan.mock.calls[1]?.[0]).toMatchObject({
          type: "claim",
          step: fencing ? "fence" : "download",
          operationId: hostOperationId,
        });
        if (scenario === "lost read scope") {
          expect(operation).not.toHaveBeenCalled();
          expect(f.installPlan.mock.calls.map(([r]) => r.type)).toEqual(["read", "claim"]);
        } else {
          const starts = scenario === "same route" || scenario === "unbound after dispatch";
          const sends = starts || scenario === "fence with unknown fleet status";
          expect(operation.mock.calls).toEqual([
            [
              hostId,
              starts
                ? { action: "operationStart", input, beforeSend: expect.any(Function) }
                : scenario === "fence with unknown fleet status"
                  ? { action: "operationFence", input, beforeSend: expect.any(Function) }
                  : {
                      action: "operationRead",
                      input: { environmentId: hostId, operationIds: [hostOperationId] },
                    },
            ],
          ]);
          expect(f.installPlan.mock.calls.map(([r]) => r.type)).toEqual([
            "read",
            "claim",
            ...(sends ? ["read"] : []),
            "report",
          ]);
          expect(f.installPlan).toHaveBeenLastCalledWith({
            type: "report",
            planId: current.planId,
            environmentId: hostId,
            operationId: hostOperationId,
            evidence: { kind: "receipt", receipt: running, ...(sends ? {} : { ledgerRead: true }) },
          });
        }
      } finally {
        reply.resolve();
        unbind();
        f.unbind();
      }
    },
  );

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
        beforeSend: expect.any(Function),
        input: { environmentId: hostId, operationId: hostOperationId, stopId: localStopId },
      });
      expect(f.installPlan.mock.calls.at(-1)?.[0]).toEqual({
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

  it("stops the exact active download behind a different outstanding operation", async () => {
    const f = bindFixture();
    const member = f.state.current!.devices[0]!;
    const download = member.operations[0]!;
    const earlier = {
      ...download,
      operationId: earlierOperationId,
      receipt: { ...running, operationId: earlierOperationId },
      request: {
        kind: "operation-start" as const,
        input: { ...input, operationId: earlierOperationId },
      },
    };
    const state = f.state;
    let operations = [earlier, download];
    f.installPlan.mockImplementation(async (request) => {
      if (request.type === "claim") {
        expect(request.operationId).toBe(hostOperationId);
        operations = [
          earlier,
          {
            ...download,
            stop: {
              stopId: localStopId,
              lease: { claimant: request.claimant, reservedAt: at, leaseUntil: grant.leaseUntil },
            },
          },
        ];
      }
      return {
        state: { ...state, current: { ...state.current!, devices: [{ ...member, operations }] } },
        ...(request.type === "claim" ? { grant } : {}),
      };
    });
    try {
      await stopPanelDownload({
        kind: "device",
        planId: plan.planId,
        environmentId: hostId,
        operationId: hostOperationId,
      });
      expect(f.stop).toHaveBeenCalledExactlyOnceWith(hostId, {
        action: "operationStop",
        beforeSend: expect.any(Function),
        input: { environmentId: hostId, operationId: hostOperationId, stopId: localStopId },
      });
      expect(f.installPlan).toHaveBeenLastCalledWith({
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

  it.each(["changed enrollment", "different host", "expired in queue"] as const)(
    "qualifies exact archived Stop with %s",
    async (scope) => {
      const sameHost = scope !== "different host";
      let now = Date.parse(at);
      const f = bindFixture();
      let closed = { ...f.state.current!, dismissedAt: at };
      const state = {
        ...native(null, closed),
        view: { complete: false, recoveryHistory: 1 as const },
      };
      f.installPlan.mockImplementation(async (request) => {
        if (request.type === "claim") {
          const member = closed.devices[0]!;
          closed = {
            ...closed,
            revision: closed.revision + 1,
            devices: [
              {
                ...member,
                operations: [
                  {
                    ...member.operations[0]!,
                    stop: {
                      stopId: localStopId,
                      lease: {
                        claimant: request.claimant,
                        reservedAt: at,
                        leaseUntil: grant.leaseUntil,
                      },
                    },
                  },
                ],
              },
            ],
          };
        }
        if (request.type === "report") {
          const member = closed.devices[0]!;
          closed = {
            ...closed,
            revision: closed.revision + 1,
            devices: [
              { ...member, operations: [{ ...member.operations[0]!, receipt: hostReceipt }] },
            ],
          };
        }
        return {
          state: { ...state, last: closed },
          ...(request.type === "read-history" ? { historyPlan: closed } : {}),
          ...(request.type === "claim" ? { grant } : {}),
        };
      });
      const observed = {
        ...(await f.services.observe(hostId)),
        fleetStatus: sameHost ? enrolledFleet : { ...enrolledFleet, environmentId },
      };
      const sent = vi.fn();
      f.stop.mockImplementation(async (_id, command) => {
        if (scope === "expired in queue") now = Date.parse(grant.leaseUntil);
        if (!(await command.beforeSend?.())) throw new JonesOperationNotSentError({});
        sent(command.input);
        return hostReceipt;
      });
      const unbind = bindInstallPlanServices(
        {
          ...f.services,
          devices: () => [observed],
          observe: async () => observed,
          now: () => now,
        },
        f.store,
      );
      f.store.update({ native: state });
      const stop = () =>
        stopPanelDownload({
          kind: "device",
          archived: true,
          planId: closed.planId,
          expectedRevision: closed.revision,
          environmentId: hostId,
          operationId: hostOperationId,
        });
      try {
        if (scope === "expired in queue") {
          await expect(stop()).rejects.toBeInstanceOf(JonesOperationNotSentError);
          expect(f.installPlan.mock.calls.some(([request]) => request.type === "report")).toBe(
            false,
          );
          expect(sent).not.toHaveBeenCalled();
        } else if (sameHost) {
          await stop();
          expect(sent).toHaveBeenCalledTimes(1);
          expect(f.stop).toHaveBeenCalledExactlyOnceWith(hostId, {
            action: "operationStop",
            beforeSend: expect.any(Function),
            input: { operationId: hostOperationId, environmentId: hostId, stopId: localStopId },
          });
        } else {
          await expect(stop()).rejects.toThrow(
            "Fresh authorized operation capability is required.",
          );
          expect(f.stop).not.toHaveBeenCalled();
          expect(f.installPlan.mock.calls.map(([request]) => request.type)).toEqual([
            "read-history",
          ]);
        }
      } finally {
        unbind();
        f.unbind();
      }
    },
  );

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

  it.each([
    "lost release reply",
    "unconfirmed fresh abandonment",
    "unsupported release on resume",
    "refused release on resume",
    "new attempt before retry",
    "abandonment cleared before retry",
    "applied directive changed before retry",
    "predecessor changed before retry",
    "different live directive before retry",
    "different enrollment before retry",
    "permission revoked before retry",
    "permission revoked after release",
    "read scope lost after release",
    "plan closed before release send",
    "plan closed after release send",
    "plan closed before the final read reply",
    "plan closed before the native save",
    "plan closed before the native save with Read revoked",
  ] as const)("binds follower abandonment to exact release evidence after %s", async (scenario) => {
    const f = bindFixture();
    const current = f.state.current!;
    const enrollmentId = earlierOperationId;
    const directive = {
      directiveId: hostOperationId,
      environmentId: hostId,
      enrollmentId,
      repository: target.repository,
      targetSource: source,
      targetTree: target.sourceTree,
      expectedDirectiveId: null,
    } as const;
    const resume = scenario !== "unconfirmed fresh abandonment";
    let closed = resume && !scenario.startsWith("plan closed");

    const applied = {
      directiveId: directive.directiveId,
      targetSource: directive.targetSource,
      targetTree: directive.targetTree,
      mode: "manual-review" as const,
      issuedAt: at,
    };
    const operation = {
      operationId: directive.directiveId,
      step: "directive" as const,
      request: { kind: "plan-directive" as const, input: directive },
      lease: { claimant: "window:a", reservedAt: at, leaseUntil: grant.leaseUntil },
      directive: { kind: "applied" as const, observedAt: at, directive: applied },
    };
    const device = {
      ...current.devices[0]!,
      route: "follower-operations" as const,
      enrollmentId,
      ...(resume ? { abandoning: true as const } : {}),
      operations: [operation],
    };
    let retained: JonesInstallPlan = {
      ...current,
      revision: 7,
      ...(closed ? { dismissedAt: at } : {}),
      devices: [device],
    };
    const state = () => ({
      ...native(closed ? null : retained, closed ? retained : null),
      documentRevision: retained.revision,
      view: { complete: false, recoveryHistory: 1 as const, deviceRecovery: 1 as const },
    });
    const observed = await f.services.observe(hostId);
    let released = false;
    let canOperate = true;
    let canRead = true;
    let liveEnrollmentId: string = enrollmentId;
    let liveDirectiveId: string = directive.directiveId;
    const fleetStatus = (): FleetHostStatus => ({
      environmentId: hostId,
      operationProtocol: 1 as const,
      ...(scenario === "unsupported release on resume" ? {} : { releasePlanDirective: 1 as const }),
      enrollment: {
        environmentId: hostId,
        enrollmentId: liveEnrollmentId,
        enabled: true,
        continueRunningThreads: false,
      },
      operation: null,
      update: observed.observation.state,
      follower: {
        protocol: 1,
        leaderSource: source,
        state: "waiting-for-leader",
        ...(!released && scenario !== "refused release on resume"
          ? {
              planDirective: { ...applied, directiveId: liveDirectiveId },
            }
          : {}),
      },
      ...(!released &&
      scenario !== "unconfirmed fresh abandonment" &&
      scenario !== "refused release on resume"
        ? { releasableDirectiveId: liveDirectiveId }
        : {}),
    });
    const observe = () => ({ ...observed, canOperate, fleetStatus: fleetStatus() });
    const revise = (patch: Partial<JonesInstallPlan["devices"][number]>) => {
      retained = {
        ...retained,
        revision: retained.revision + 1,
        devices: [{ ...device, ...patch }],
      };
    };
    const closePlan = () => {
      closed = true;
      retained = { ...retained, dismissedAt: at, revision: retained.revision + 1 };
    };
    const closeWhileSaving = scenario.startsWith("plan closed before the native save");
    const sent = vi.fn();
    const host = vi.fn<InstallPlanServices["host"]>(async (id, request) => {
      if (request.action !== "releaseDirective") throw new Error("Unexpected host command.");
      if (scenario === "plan closed before release send") closePlan();
      if (!(await request.beforeSend?.())) throw new Error("The release did not leave the queue.");
      sent(id, request.input);
      if (scenario === "plan closed after release send") closePlan();
      if (scenario.includes("before retry")) {
        if (scenario === "new attempt before retry") revise({ addedAt: grant.leaseUntil });
        if (scenario === "abandonment cleared before retry") {
          const { abandoning: _abandonment, ...member } = device;
          retained = { ...retained, revision: retained.revision + 1, devices: [member] };
        }
        if (scenario === "applied directive changed before retry")
          revise({
            operations: [
              {
                ...operation,
                directive: {
                  ...operation.directive,
                  directive: { ...applied, issuedAt: grant.leaseUntil },
                },
              },
            ],
          });
        if (scenario === "predecessor changed before retry")
          revise({
            operations: [
              {
                ...operation,
                request: {
                  ...operation.request,
                  input: { ...directive, expectedDirectiveId: localStopId },
                },
              },
            ],
          });
        if (scenario === "different live directive before retry") liveDirectiveId = localStopId;
        if (scenario === "different enrollment before retry") liveEnrollmentId = localStopId;
        if (scenario === "permission revoked before retry") canOperate = false;
        if (await request.beforeSend?.()) sent(id, request.input);
        throw new Error("No release was verified by the host.");
      }
      if (scenario === "refused release on resume") throw new Error("Release was refused.");
      if (scenario === "lost release reply" && !released) {
        released = true;
        throw new Error("The release response was lost.");
      }
      released = true;
      if (scenario === "permission revoked after release") canOperate = false;
      if (scenario === "read scope lost after release") canRead = false;
      return { ...fleetStatus(), releasedDirectiveId: directive.directiveId };
    });
    f.installPlan.mockImplementation(async (request) => {
      if (
        released &&
        request.type === "read" &&
        !closed &&
        scenario === "plan closed before the final read reply"
      ) {
        const stale = state();
        closePlan();
        liveEnrollmentId = localStopId;
        f.store.update({ native: state() });
        return { state: stale };
      }
      if (request.type === "abandon-device" && request.phase === "finish") {
        expect(request.planId).toBe(retained.planId);
        expect(request.environmentId).toBe(hostId);
        expect(request.directiveRelease?.releasedDirectiveId).toBe(directive.directiveId);
        expect(request.directiveRelease?.enrollment?.enrollmentId).toBe(enrollmentId);
        if (closeWhileSaving && !closed) {
          expect(request.expectedRevision).toBe(7);
          closePlan();
          liveEnrollmentId = localStopId;
          if (scenario === "plan closed before the native save with Read revoked") canRead = false;
          return {
            state: state(),
            refused: { code: "stale-revision", message: "The plan changed." },
          };
        }
        expect(request.expectedRevision).toBe(retained.revision);
        const { abandoning: _abandoning, ...member } = device;
        retained = {
          ...retained,
          revision: retained.revision + 1,
          devices: [{ ...member, membership: "removed" }],
        };
      }
      return {
        state: state(),
        ...(request.type === "read-history" ? { historyPlan: retained } : {}),
      };
    });
    const unbind = bindInstallPlanServices(
      {
        ...f.services,
        devices: () => [observe()],
        observe: async () => observe(),
        canReadReceipts: () => canRead,
        confirm: async () => true,
        host,
      },
      f.store,
    );
    f.store.update({ native: state() });
    try {
      const attempt = deviceAbandonTarget(retained, hostId, closed)!;
      const request = {
        action: "releaseDirective",
        input: directive,
        beforeSend: expect.any(Function),
      };
      const finishes =
        scenario === "lost release reply" ||
        scenario === "permission revoked after release" ||
        scenario === "plan closed after release send" ||
        scenario === "plan closed before the final read reply" ||
        scenario === "plan closed before the native save";
      if (finishes) {
        if (scenario === "lost release reply") {
          await expect(abandonDevice(attempt)).rejects.toThrow("release response was lost");
          expect(retained.devices[0]?.abandoning).toBe(true);
        }
        await abandonDevice(attempt);
        expect(host).toHaveBeenCalledTimes(scenario === "lost release reply" ? 2 : 1);
        expect(host).toHaveBeenLastCalledWith(hostId, request);
        expect(retained.devices[0]?.membership).toBe("removed");
        if (scenario.startsWith("plan closed")) {
          expect(sent).toHaveBeenCalledExactlyOnceWith(hostId, directive);
          expect(f.installPlan).toHaveBeenLastCalledWith({
            type: "read-history",
            planId: retained.planId,
          });
        }
        if (closeWhileSaving)
          expect(
            f.installPlan.mock.calls.flatMap(([call]) =>
              call.type === "abandon-device" ? [call.expectedRevision] : [],
            ),
          ).toEqual([7, 8]);
      } else if (scenario === "plan closed before the native save with Read revoked") {
        await expect(abandonDevice(attempt)).rejects.toThrow("Read permission");
        expect(sent).toHaveBeenCalledExactlyOnceWith(hostId, directive);
        expect(retained.devices[0]?.abandoning).toBe(true);
        expect(
          f.installPlan.mock.calls.flatMap(([call]) =>
            call.type === "abandon-device" ? [call.expectedRevision] : [],
          ),
        ).toEqual([7]);
      } else if (scenario === "plan closed before release send") {
        await expect(abandonDevice(attempt)).rejects.toThrow("attempt changed");
        expect(sent).not.toHaveBeenCalled();
        expect(retained.devices[0]?.abandoning).toBe(true);
      } else if (
        scenario.includes("before retry") ||
        scenario === "read scope lost after release"
      ) {
        await expect(abandonDevice(attempt)).rejects.toThrow();
        expect(host).toHaveBeenCalledExactlyOnceWith(hostId, request);
        expect(sent).toHaveBeenCalledExactlyOnceWith(hostId, directive);
      } else {
        await expect(abandonDevice(attempt)).rejects.toThrow(
          scenario === "refused release on resume"
            ? "Release was refused."
            : "cannot restore the original follower directive",
        );
        expect(retained.devices[0]?.abandoning).toBe(resume ? true : undefined);
        expect(host).toHaveBeenCalledTimes(scenario === "refused release on resume" ? 1 : 0);
      }
      if (!finishes && !closeWhileSaving)
        expect(
          f.installPlan.mock.calls.every(([request]) => request.type !== "abandon-device"),
        ).toBe(true);
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

  it.each([
    "matching",
    "changed enrollment",
    "unknown enrollment",
    "different host",
    "mismatched grant",
    "expired in queue",
    "legacy unsupported",
    "fence replaces provisional",
    "provisional read-back missing",
    "verified unsupported",
  ] as const)("qualifies the exact archived Fence with %s", async (scenario) => {
    const f = bindFixture();
    const current = f.state.current!;
    const device =
      scenario === "unknown enrollment"
        ? {
            ...current.devices[0]!,
            route: "follower-operations" as const,
            enrollmentId: earlierOperationId,
          }
        : current.devices[0]!;
    const { receipt: _receipt, ...pending } = device.operations[0]!;
    const unsupported = {
      ...input,
      protocol: 1,
      phase: "refused",
      code: "unsupported",
      updatedAt: at,
    } as const;
    const tombstone = { ...input, protocol: 1, phase: "fenced", updatedAt: at } as const;
    const provisional =
      scenario === "fence replaces provisional" ||
      scenario === "provisional read-back missing" ||
      scenario === "verified unsupported";
    let closed: JonesInstallPlan = {
      ...current,
      revision: 7,
      dismissedAt: at,
      cancelled: true as const,
      devices: [
        {
          ...device,
          operations: [
            {
              ...pending,
              lease: { ...pending.lease, leaseUntil: at },
              ...(provisional ? { receipt: unsupported } : {}),
              ...(scenario === "verified unsupported"
                ? { unsupportedReceiptVerified: true as const }
                : {}),
            },
          ],
        },
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
    let now = Date.parse(at);
    f.installPlan.mockImplementation(async (request) => {
      if (request.type === "claim") {
        const member = closed.devices[0]!;
        closed = {
          ...closed,
          revision: closed.revision + 1,
          devices: [
            {
              ...member,
              operations: [
                {
                  ...member.operations[0]!,
                  fence: {
                    lease: {
                      claimant: request.claimant,
                      reservedAt: at,
                      leaseUntil: grant.leaseUntil,
                    },
                  },
                },
              ],
            },
          ],
        };
      }
      if (request.type === "report" && request.evidence.kind === "receipt") {
        const recorded = closed.devices[0]!;
        closed = {
          ...closed,
          revision: closed.revision + 1,
          devices: [
            {
              ...recorded,
              operations: [{ ...recorded.operations[0]!, receipt: request.evidence.receipt }],
            },
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
                  input:
                    scenario === "mismatched grant"
                      ? { ...input, expectedInstalledSource: "e".repeat(40) }
                      : input,
                },
              },
            }
          : {}),
      };
    });
    const sent = vi.fn();
    const operation = vi.fn<InstallPlanServices["operation"]>(async (id, request) => {
      if (request.action === "operationRead") {
        expect(request.input).toEqual({ environmentId: hostId, operationIds: [hostOperationId] });
        return {
          receipts: [
            {
              operationId: hostOperationId,
              receipt: scenario === "fence replaces provisional" ? tombstone : null,
            },
          ],
        };
      }
      if (request.action !== "operationFence") throw new Error("Unexpected host command.");
      if (scenario === "expired in queue") now = Date.parse(grant.leaseUntil);
      if (!(await request.beforeSend?.())) throw new JonesOperationNotSentError({});
      sent(id, request.input);
      if (scenario === "legacy unsupported") return unsupported;
      return provisional ? tombstone : accepted;
    });
    const observed = await f.services.observe(hostId);
    const host = {
      ...observed,
      fleetStatus:
        scenario === "changed enrollment"
          ? enrolledFleet
          : scenario === "different host"
            ? { ...enrolledFleet, environmentId }
            : scenario === "unknown enrollment"
              ? null
              : ordinaryFleet,
    };
    const unbind = bindInstallPlanServices(
      {
        ...f.services,
        operation,
        now: () => now,
        devices: () => [host],
        observe: async () => host,
      },
      f.store,
    );
    f.store.update({ native: state });
    try {
      const action = fencePanelOperation({
        planId: closed.planId,
        environmentId: hostId,
        operationId: hostOperationId,
        expectedRevision: closed.revision,
        archived: true,
      });
      if (
        scenario === "matching" ||
        scenario === "changed enrollment" ||
        scenario === "fence replaces provisional"
      ) {
        await action;
        expect(sent).toHaveBeenCalledExactlyOnceWith(hostId, input);
        expect(f.installPlan).toHaveBeenCalledWith({
          type: "report",
          planId: closed.planId,
          environmentId: hostId,
          operationId: hostOperationId,
          evidence:
            scenario === "fence replaces provisional"
              ? { kind: "receipt", receipt: tombstone, ledgerRead: true }
              : { kind: "receipt", receipt: accepted },
        });
      } else {
        await expect(action).rejects.toThrow();
        expect(sent).toHaveBeenCalledTimes(
          scenario === "legacy unsupported" || scenario === "provisional read-back missing" ? 1 : 0,
        );
        expect(f.installPlan.mock.calls.some(([request]) => request.type === "report")).toBe(false);
      }
      if (
        scenario === "different host" ||
        scenario === "unknown enrollment" ||
        scenario === "verified unsupported"
      ) {
        expect(f.installPlan.mock.calls.some(([request]) => request.type === "claim")).toBe(false);
      } else
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
  });

  it.each(["changed enrollment", "different host"] as const)(
    "checks an exact archived cleanup hold with %s",
    async (scope) => {
      const f = bindFixture();
      const current = f.state.current!;
      const device = current.devices[0]!;
      const jobId = earlierOperationId;
      const receipt = {
        ...running,
        phase: "held",
        code: "cleanup-unconfirmed",
        jobId,
      } as const;
      const closed = {
        ...current,
        dismissedAt: at,
        cancelled: true as const,
        devices: [
          {
            ...device,
            operations: [{ ...device.operations[0]!, receipt }],
          },
        ],
      };
      const state = {
        ...f.state,
        current: null,
        last: closed,
        view: { complete: false, recoveryHistory: 1 as const },
      };
      f.installPlan.mockResolvedValue({ state, historyPlan: closed });
      const observation = await f.services.observe(hostId);
      const host = {
        ...observation,
        fleetStatus:
          scope === "changed enrollment" ? enrolledFleet : { ...enrolledFleet, environmentId },
      };
      const inspection = {
        operationId: hostOperationId,
        status: "eligible" as const,
        recoveryToken: localStopId,
      };
      const operation = vi.fn<InstallPlanServices["operation"]>(async () => ({
        receipts: [{ operationId: hostOperationId, receipt }],
        cleanupInspection: inspection,
      }));
      const unbind = bindInstallPlanServices(
        { ...f.services, operation, devices: () => [host], observe: async () => host },
        f.store,
      );
      f.store.update({ native: state });
      try {
        const result = await inspectPanelCleanup({
          planId: closed.planId,
          environmentId: hostId,
          operationId: hostOperationId,
          archived: true,
          expectedRevision: closed.revision,
          jobId,
        });
        if (scope === "different host") {
          expect(result).toEqual({ status: "unproved" });
          expect(operation).not.toHaveBeenCalled();
        } else {
          expect(result).toEqual(inspection);
          expect(operation).toHaveBeenCalledExactlyOnceWith(hostId, {
            action: "operationRead",
            input: {
              environmentId: hostId,
              operationIds: [hostOperationId],
              inspectCleanup: hostOperationId,
            },
          });
        }
      } finally {
        unbind();
        f.unbind();
      }
    },
  );

  it.each([
    "supported",
    "historical enrollment",
    "disabled enrollment",
    "old host",
    "old host after enrollment change",
    "failed fleet read",
    "failed read after claim",
    "missing enrollment",
    "wrong fleet host",
    "wrong ordinary host",
    "mismatched grant revision",
    "advanced archive in queue",
    "different owner in queue",
    "expired lease in queue",
    "setup settled in queue",
    "capability changes during retry",
    "mismatched proof",
    "contradictory live directive",
    "legacy fence without exact proof",
    "enrollment changed in reply",
    "late authorized readback",
    "read permission lost after send",
  ] as const)("qualifies an explicit archived setup fence with a %s", async (scenario) => {
    const f = bindFixture();
    const current = f.state.current!;
    const enrollmentId = earlierOperationId;
    const directive = {
      directiveId: hostOperationId,
      environmentId: hostId,
      enrollmentId,
      repository: target.repository,
      targetSource: source,
      targetTree: target.sourceTree,
      expectedDirectiveId: null,
    } as const;
    const original = {
      operationId: hostOperationId,
      step: "directive" as const,
      request: { kind: "plan-directive" as const, input: directive },
      lease: { claimant: "forward-window", reservedAt: at, leaseUntil: grant.leaseUntil },
    };
    const applied = {
      directiveId: directive.directiveId,
      targetSource: directive.targetSource,
      targetTree: directive.targetTree,
      mode: "manual-review" as const,
      issuedAt: at,
    };
    const device = {
      ...current.devices[0]!,
      route: "follower-operations" as const,
      enrollmentId,
      operations: [original],
    };
    let closed: JonesInstallPlan = { ...current, revision: 7, dismissedAt: at, devices: [device] };
    const replace = (operation: JonesInstallPlan["devices"][number]["operations"][number]) => {
      closed = { ...closed, devices: [{ ...device, operations: [operation] }] };
    };
    const newer = { ...plan, planId: localStopId };
    const state = () => ({
      ...native(newer, closed),
      documentRevision: closed.revision,
      recoveryHistory: [
        {
          planId: closed.planId,
          revision: closed.revision,
          targetVersion: target.version,
          closedAt: at,
          pendingLaptop: false,
          pendingDevices: [{ environmentId: hostId, label: device.label }],
        },
      ],
      view: { complete: false, recoveryHistory: 1 as const, directiveFence: 1 as const },
    });
    const observed = await f.services.observe(hostId);
    const historical = scenario === "historical enrollment" || scenario === "disabled enrollment";
    const newEnrollment = historical || scenario === "old host after enrollment change";
    const fleetStatus: FleetHostStatus = {
      environmentId: scenario === "wrong fleet host" ? environmentId : hostId,
      operationProtocol: 1 as const,
      planDirectiveProtocol: 1 as const,
      releasePlanDirective: 1 as const,
      ...(scenario === "old host" ? {} : { fencePlanDirective: 1 as const }),
      ...(historical ? { fenceHistoricalPlanDirective: 1 as const } : {}),
      operation: null,
      update: observed.observation.state,
      enrollment:
        scenario === "missing enrollment"
          ? null
          : {
              environmentId: hostId,
              enrollmentId:
                newEnrollment && scenario !== "disabled enrollment" ? localStopId : enrollmentId,
              enabled: scenario !== "disabled enrollment",
              continueRunningThreads: false,
            },
    };
    const { fleetStatus: _previous, ...unknownFleet } = observed;
    let latest: InstallStatusDevice =
      scenario === "failed fleet read"
        ? unknownFleet
        : {
            ...unknownFleet,
            fleetStatus,
            ...(scenario === "wrong ordinary host"
              ? {
                  observation: {
                    freshness: "fresh",
                    state: { ...observed.observation.state!, environmentId },
                  },
                }
              : {}),
          };
    let now = Date.parse(at);
    let canRead = true;
    const sent = vi.fn();
    const proof: NonNullable<FleetHostStatus["directiveFence"]> =
      scenario === "disabled enrollment"
        ? { protocol: 1, outcome: "applied", input: directive, directive: applied }
        : {
            protocol: 1,
            outcome: "fenced",
            input:
              scenario === "mismatched proof"
                ? { ...directive, expectedDirectiveId: localStopId }
                : directive,
          };
    const host = vi.fn<InstallPlanServices["host"]>(async (id, request) => {
      if (request.action !== "fenceDirective") throw new Error("Unexpected host command.");
      expect(id).toBe(hostId);
      expect(request.input).toEqual(directive);
      if (scenario === "advanced archive in queue") closed = { ...closed, revision: 9 };
      if (scenario === "different owner in queue")
        replace({ ...original, fence: { lease: { ...original.lease, claimant: "other-window" } } });
      if (scenario === "setup settled in queue") {
        replace({
          ...closed.devices[0]!.operations[0]!,
          directive: { kind: "applied", observedAt: at, directive: applied },
        });
      }
      if (scenario === "expired lease in queue") now = Date.parse(grant.leaseUntil);
      if (!(await request.beforeSend?.())) throw new Error("The fence did not leave the queue.");
      sent(id, request.input);
      if (scenario === "capability changes during retry") {
        const { fencePlanDirective: _capability, ...oldFleet } = fleetStatus;
        latest = { ...latest, fleetStatus: oldFleet };
        if (await request.beforeSend?.()) sent(id, request.input);
        throw new Error("The authorized retry was not sent.");
      }
      if (scenario === "late authorized readback") {
        latest = { ...latest, canOperate: false };
        closed = { ...closed, revision: 9 };
        now = Date.parse(grant.leaseUntil);
      }
      if (scenario === "read permission lost after send") canRead = false;
      return {
        ...fleetStatus,
        ...(scenario === "enrollment changed in reply"
          ? {
              fenceHistoricalPlanDirective: 1 as const,
              enrollment: { ...fleetStatus.enrollment!, enrollmentId: localStopId },
            }
          : {}),
        ...(scenario === "legacy fence without exact proof" ? {} : { directiveFence: proof }),
        ...(scenario === "contradictory live directive"
          ? {
              follower: {
                protocol: 1,
                leaderSource: source,
                state: "current",
                planDirective: applied,
              },
            }
          : {}),
      };
    });
    f.installPlan.mockImplementation(async (request) => {
      if (request.type === "claim") {
        expect(request.expectedRevision).toBe(7);
        replace({
          ...original,
          fence: { lease: { ...original.lease, claimant: request.claimant } },
        });
        closed = { ...closed, revision: 8 };
        if (scenario === "failed read after claim") latest = unknownFleet;
        return {
          state: state(),
          grant: {
            ...grant,
            step: "fence",
            request: {
              kind: "plan-directive-fence",
              input: directive,
              expectedRevision: scenario === "mismatched grant revision" ? 9 : 8,
            },
          },
        };
      }
      if (request.type === "report" && request.evidence.kind === "directive-fence") {
        expect(request.evidence.expectedRevision).toBe(closed.revision);
        const { proof: verified } = request.evidence;
        replace({
          ...closed.devices[0]!.operations[0]!,
          ...(verified.outcome === "applied"
            ? { directive: { kind: "applied", observedAt: at, directive: verified.directive } }
            : { directiveFence: { outcome: verified.outcome, observedAt: at } }),
        });
        closed = { ...closed, revision: closed.revision + 1 };
      }
      return {
        state: state(),
        ...(request.type === "read-history" ? { historyPlan: closed } : {}),
      };
    });
    const unbind = bindInstallPlanServices(
      {
        ...f.services,
        observe: async () => latest,
        devices: () => [latest],
        canReadReceipts: () => canRead,
        host,
        now: () => now,
      },
      f.store,
    );
    f.store.update({ native: state() });
    try {
      const perform = () =>
        fencePanelOperation({
          archived: true,
          planId: closed.planId,
          environmentId: hostId,
          operationId: hostOperationId,
          expectedRevision: 7,
        });
      if (
        scenario === "supported" ||
        historical ||
        scenario === "late authorized readback" ||
        scenario === "enrollment changed in reply"
      ) {
        await perform();
        expect(host).toHaveBeenCalledExactlyOnceWith(hostId, {
          action: "fenceDirective",
          input: directive,
          beforeSend: expect.any(Function),
        });
        expect(sent).toHaveBeenCalledExactlyOnceWith(hostId, directive);
        expect(f.installPlan).toHaveBeenCalledWith({
          type: "report",
          planId: closed.planId,
          environmentId: hostId,
          operationId: hostOperationId,
          evidence: {
            kind: "directive-fence",
            expectedRevision: scenario === "late authorized readback" ? 9 : 8,
            proof,
          },
        });
      } else {
        await expect(perform()).rejects.toThrow();
        const refusalBeforeClaim = [
          "failed fleet read",
          "old host",
          "old host after enrollment change",
          "missing enrollment",
          "wrong fleet host",
          "wrong ordinary host",
        ].includes(scenario);
        expect(host).toHaveBeenCalledTimes(
          refusalBeforeClaim ||
            scenario === "failed read after claim" ||
            scenario === "mismatched grant revision"
            ? 0
            : 1,
        );
        expect(sent).toHaveBeenCalledTimes(
          [
            "capability changes during retry",
            "mismatched proof",
            "contradictory live directive",
            "legacy fence without exact proof",
            "read permission lost after send",
          ].includes(scenario)
            ? 1
            : 0,
        );
        const types = f.installPlan.mock.calls.map(([request]) => request.type);
        expect(types.includes("claim")).toBe(!refusalBeforeClaim);
        expect(types).not.toContain("report");
        expect(closed.devices[0]?.operations[0]?.directiveFence).toBeUndefined();
        if (scenario !== "setup settled in queue")
          expect(closed.devices[0]?.operations[0]?.directive).toBeUndefined();
      }
      expect(f.store.get().native?.current?.planId).toBe(newer.planId);
    } finally {
      unbind();
      f.unbind();
    }
  });

  it.each(["Check", "poll"] as const)(
    "recovers an archived ledger receipt after legacy unsupported through %s",
    async (method) => {
      const f = bindFixture();
      const current = f.state.current!;
      const device = current.devices[0]!;
      const receipt = { ...running, phase: "failed" as const };
      const unsupported: JonesOperationReceipt = {
        ...input,
        protocol: 1,
        phase: "refused",
        code: "unsupported",
        updatedAt: "2026-10-27T04:01:00.000Z",
      };
      const closed = {
        ...current,
        dismissedAt: at,
        devices: [
          {
            ...device,
            route: "follower-operations" as const,
            enrollmentId: earlierOperationId,
            operations: [{ ...device.operations[0]!, receipt: unsupported }],
          },
        ],
      };
      const observed = await f.services.observe(hostId);
      const changed = {
        ...observed,
        canOperate: false,
        fleetStatus: {
          environmentId: hostId,
          operationProtocol: 1 as const,
          operation: null,
          update: observed.observation.state,
          enrollment: {
            environmentId: hostId,
            enrollmentId: localStopId,
            enabled: true,
            continueRunningThreads: false,
          },
        },
      };
      const operation = vi.fn<InstallPlanServices["operation"]>(async () => ({
        receipts: [{ operationId: hostOperationId, receipt }],
      }));
      let archived = closed;
      const state = {
        ...native(null, closed),
        view: { complete: false, recoveryHistory: 1 as const },
      };
      f.installPlan.mockImplementation(async (request) => {
        if (request.type === "report")
          archived = {
            ...closed,
            revision: closed.revision + 1,
            devices: [
              { ...closed.devices[0]!, operations: [{ ...device.operations[0]!, receipt }] },
            ],
          };
        return { state: { ...state, last: archived }, historyPlan: archived };
      });
      let canRead = false;
      let now = Date.parse(at);
      const unbind = bindInstallPlanServices(
        {
          ...f.services,
          devices: () => [changed],
          observe: async () => changed,
          operation,
          canReadReceipts: () => canRead,
          now: () => now,
        },
        f.store,
      );
      f.store.update({ native: state, devices: [changed] });
      f.store.open(true, closed.planId);
      const binding = installPlanServices()!;
      const status =
        method === "poll"
          ? startInstallStatusPoller(binding.services, { store: f.store })
          : undefined;
      const refresh = async () => {
        if (status === undefined) {
          await performPanelAction(
            hostId,
            "check",
            { planId: closed.planId, archived: true, operationId: hostOperationId },
            binding,
          );
          return;
        }
        now += STATUS_POLL_CADENCE.regainedAfterMs + 1;
        status.poller.regained();
        await vi.waitFor(() => expect(status.poller.health(hostId)?.lastSuccessAt).toBe(now));
      };
      try {
        await refresh();
        expect(operation).not.toHaveBeenCalled();
        canRead = true;
        await refresh();
        expect(operation).toHaveBeenCalledExactlyOnceWith(hostId, {
          action: "operationRead",
          ...(method === "poll" ? { beforeSend: expect.any(Function) } : {}),
          input: { environmentId: hostId, operationIds: [hostOperationId] },
        });
        expect(f.installPlan).toHaveBeenCalledWith({
          type: "report",
          planId: closed.planId,
          environmentId: hostId,
          operationId: hostOperationId,
          evidence: { kind: "receipt", receipt, ledgerRead: true },
        });
        expect(archived.devices[0]?.operations[0]?.receipt).toEqual(receipt);
        expect(
          f.installPlan.mock.calls.every(([request]) =>
            ["read", "read-history", "report"].includes(request.type),
          ),
        ).toBe(true);
      } finally {
        status?.close();
        unbind();
        f.unbind();
      }
    },
  );

  it("keeps an orphan held when read permission is lost during its receipt request", async () => {
    const f = bindFixture();
    const command: JonesInstallPlanCommandReceipt = {
      command: {
        commandId: localStopId,
        environmentId: hostId,
        targetSource: source,
        action: "download-exact",
        planId: plan.planId,
        expectedRevision: 1,
      },
      planId: plan.planId,
      operationId: hostOperationId,
      bodyHash: "a".repeat(64),
      requestType: "claim",
      outcome: "reserved",
    };
    const state = {
      ...native(null),
      orphanedCommands: [command],
      view: { complete: false, orphanRecovery: 1 as const },
    };
    f.installPlan.mockResolvedValue({ state });
    const reading = deferred();
    const reply = deferred();
    let canRead = true;
    const unbind = bindInstallPlanServices(
      {
        ...f.services,
        canReadReceipts: () => canRead,
        operation: async () => {
          reading.resolve();
          await reply.promise;
          return {
            receipts: [{ operationId: hostOperationId, receipt: { ...running, phase: "failed" } }],
          };
        },
      },
      f.store,
    );
    try {
      const checking = checkOrphanedPlanCommand(command, installPlanServices());
      const result = expect(checking).rejects.toThrow("Read permission");
      await reading.promise;
      canRead = false;
      reply.resolve();
      await result;
      expect(f.installPlan.mock.calls.map(([request]) => request.type)).toEqual(["read"]);
    } finally {
      reply.resolve();
      unbind();
      f.unbind();
    }
  });

  it.each(["observe", "queued receipt read", "first report"] as const)(
    "expires an archive poll during %s without starting more work",
    async (delay) => {
      vi.useFakeTimers();
      const f = bindFixture();
      const current = f.state.current!;
      const device = current.devices[0]!;
      const first = device.operations[0]!;
      const second = {
        ...first,
        operationId: earlierOperationId,
        request: {
          kind: "operation-start" as const,
          input: { ...input, operationId: earlierOperationId },
        },
        receipt: { ...running, operationId: earlierOperationId },
      };
      const archived = {
        ...current,
        dismissedAt: at,
        devices: [{ ...device, operations: [first, second] }],
      };
      const state = {
        ...native(null, archived),
        view: { complete: false, recoveryHistory: 1 as const },
      };
      const entered = deferred();
      const release = deferred();
      f.installPlan.mockImplementation(async (request) => {
        if (request.type === "report" && request.operationId === first.operationId) {
          entered.resolve();
          await release.promise;
        }
        return { state, ...(request.type === "read-history" ? { historyPlan: archived } : {}) };
      });
      const observed = await f.services.observe(hostId);
      const sent = vi.fn();
      const operation = vi.fn<InstallPlanServices["operation"]>(async (_id, request) => {
        if (delay === "queued receipt read") {
          entered.resolve();
          await release.promise;
          if ("beforeSend" in request && request.beforeSend && !(await request.beforeSend()))
            throw new JonesOperationNotSentError({});
        }
        sent();
        return {
          receipts: [first, second].map(({ operationId }) => ({
            operationId,
            receipt: { ...running, operationId, phase: "failed" },
          })),
        };
      });
      let now = Date.parse(at);
      const unbind = bindInstallPlanServices(
        {
          ...f.services,
          now: () => now,
          operation,
          observe: async () => {
            if (delay === "observe") {
              entered.resolve();
              await release.promise;
            }
            return observed;
          },
        },
        f.store,
      );
      f.store.update({ native: state, devices: [observed] });
      f.store.open(true, archived.planId);
      const status = startInstallStatusPoller(installPlanServices()!.services, { store: f.store });
      try {
        now += STATUS_POLL_CADENCE.regainedAfterMs + 1;
        status.poller.regained();
        await entered.promise;
        await vi.advanceTimersByTimeAsync(STATUS_POLL_CADENCE.timeoutMs);
        expect(status.poller.health(hostId)?.failures).toBe(1);
        release.resolve();
        await vi.advanceTimersByTimeAsync(0);
        expect(operation).toHaveBeenCalledTimes(delay === "observe" ? 0 : 1);
        expect(sent).toHaveBeenCalledTimes(delay === "first report" ? 1 : 0);
        expect(f.installPlan.mock.calls.map(([request]) => request.type)).toEqual(
          delay === "observe"
            ? []
            : ["read", "read-history", ...(delay === "first report" ? ["report"] : [])],
        );
      } finally {
        release.resolve();
        status.close();
        unbind();
        f.unbind();
        vi.useRealTimers();
      }
    },
  );

  it("polls the open archive in a single bounded read of outstanding operation IDs", async () => {
    const f = bindFixture();
    const current = f.state.current!;
    const device = current.devices[0]!;
    const prototype = device.operations[0]!;
    const makeOperation = (operationId: string) => ({
      ...prototype,
      receipt: { ...running, operationId },
      operationId,
      request: { kind: "operation-start" as const, input: { ...input, operationId } },
    });
    const pending = Array.from({ length: 16 }, (_, offset) =>
      makeOperation(`00000000-0000-4000-8000-${String(offset).padStart(12, "0")}`),
    );
    const settled = {
      ...makeOperation(localStopId),
      receipt: { ...running, operationId: localStopId, phase: "stopped" as const },
    };
    let archived: JonesInstallPlan = {
      ...current,
      revision: 7,
      dismissedAt: at,
      devices: [{ ...device, operations: [settled, ...pending, makeOperation(hostStopId)] }],
    };
    const newer = { ...plan, planId: earlierOperationId };
    const state = () => ({
      ...native(newer, archived),
      view: { complete: false, recoveryHistory: 1 as const },
      documentRevision: archived.revision,
    });
    f.installPlan.mockImplementation(async (request) => {
      if (request.type === "report" && request.evidence.kind === "receipt") {
        const receipt = request.evidence.receipt;
        const member = archived.devices[0]!;
        expect(request.planId).toBe(archived.planId);
        archived = {
          ...archived,
          revision: archived.revision + 1,
          devices: [
            {
              ...member,
              operations: member.operations.map((item) =>
                item.operationId === request.operationId ? { ...item, receipt } : item,
              ),
            },
          ],
        };
      }
      return {
        state: state(),
        ...(request.type === "read-history" ? { historyPlan: archived } : {}),
      };
    });
    const first = pending[0]!;
    const last = pending.at(-1)!;
    const mismatch = pending[1]!;
    const terminal = { ...running, phase: "stopped" as const };
    const operation = vi.fn<InstallPlanServices["operation"]>(async () => ({
      receipts: [
        { operationId: last.operationId, receipt: { ...terminal, operationId: last.operationId } },
        {
          operationId: first.operationId,
          receipt: { ...terminal, operationId: first.operationId },
        },
        { operationId: mismatch.operationId, receipt: terminal },
        { operationId: hostStopId, receipt: { ...terminal, operationId: hostStopId } },
      ],
    }));
    const observed = await f.services.observe(hostId);
    let now = Date.parse(at);
    const unbind = bindInstallPlanServices({ ...f.services, operation, now: () => now }, f.store);
    f.store.update({ native: state(), devices: [observed] });
    const binding = installPlanServices()!;
    const poller = startInstallStatusPoller(binding.services, { store: f.store });
    const tick = async () => {
      now += STATUS_POLL_CADENCE.regainedAfterMs + 1;
      poller.poller.regained();
      await vi.waitFor(() => expect(poller.poller.health(hostId)?.lastSuccessAt).toBe(now));
    };
    try {
      await tick();
      expect(operation).not.toHaveBeenCalled();
      f.store.open(true, archived.planId);
      await tick();
      expect(operation).toHaveBeenCalledExactlyOnceWith(hostId, {
        action: "operationRead",
        input: { environmentId: hostId, operationIds: pending.map((item) => item.operationId) },
        beforeSend: expect.any(Function),
      });
      expect(
        f.installPlan.mock.calls.flatMap(([request]) =>
          request.type === "report" ? [request.operationId] : [],
        ),
      ).toEqual([first.operationId, last.operationId]);
      expect(
        f.installPlan.mock.calls.filter(([request]) => request.type === "read-history"),
      ).toEqual([
        [{ type: "read-history", planId: archived.planId }],
        [{ type: "read-history", planId: archived.planId }],
      ]);
      expect(f.store.get().historyPlan?.revision).toBe(9);
      expect(f.store.get().native?.current?.planId).toBe(newer.planId);
    } finally {
      poller.close();
      unbind();
      f.unbind();
    }
  });

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
          evidence: { kind: "receipt", receipt, ledgerRead: true },
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
