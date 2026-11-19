import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, type DesktopUpdateState } from "@t3tools/contracts";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import type {
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
  JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import { startPlan } from "@t3tools/client-runtime/jones/install-plan";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type { JonesExecutorHost } from "@t3tools/client-runtime/jones/updates/executor";
import { prepareReviewedInstall } from "@t3tools/client-runtime/jones/updates/executor";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import { createWebJonesJournal } from "./executionJournal";
import {
  createWebJonesExecutor,
  executeWebJonesReplacement,
  stopWebJonesDownload,
} from "./execution";
import { readLegacyConnectionsAuthority } from "./entrypoints";
import { createInstallStatusStore, type InstallStatusDevice } from "./panelUi";
import { bindInstallPlanServices, installPlanServices, type InstallPlanServices } from "./services";

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const environmentId = EnvironmentId.make("web-duty-host"),
  targetSource = "a".repeat(40);
const id = (n: number) => `${String(n).padStart(8, "0")}-1111-4111-8111-111111111111`;
const state: JonesUpdateState = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "available",
  environmentId,
  currentVersion: "old",
  revision: 0,
  capability: { check: true, download: true, install: true },
  provenance: {
    repository: "Jones-Systems/Jones-Code",
    sourceSha: targetSource,
    sourceTree: "b".repeat(40),
    workflow: "synthetic",
    runId: 1,
    runAttempt: 1,
    artifactId: 2,
    artifactDigest: "d".repeat(64),
    platform: "linux",
    architecture: "x64",
  },
};
function storage() {
  let disk: string | null = null;
  return {
    getItem: () => disk,
    setItem: (_key: string, value: string) => {
      disk = value;
    },
  };
}
function host(legacy = vi.fn()): JonesExecutorHost {
  return {
    readLatest: () => ({ observation: { state, freshness: "fresh" }, canOperate: true }),
    legacy,
    start: vi.fn(),
    confirm: async () => true,
  };
}
afterEach(() => vi.unstubAllGlobals());

function connectionsInstallFixture(
  route: "operations" | "follower-operations" = "follower-operations",
) {
  const at = "2026-10-27T04:00:00Z";
  const now = () => Date.parse(at);
  const laptop = EnvironmentId.make("laptop");
  const installedSource = "c".repeat(40);
  const enrollmentId = id(4);
  const plan = startPlan({
    planId: id(1),
    origin: "connections",
    target: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: targetSource,
      sourceTree: state.provenance!.sourceTree,
      version: "new",
    },
    from: { version: "old" },
    laptopEnvironmentId: laptop,
    select: [
      { environmentId, label: "Host", route, ...(route === "operations" ? {} : { enrollmentId }) },
    ],
    now: at,
  });
  const installState: JonesUpdateState = {
    ...state,
    phase: "staged",
    stagedHandle: "reviewed-stage",
    capability: { ...state.capability, operations: 1 },
    installedSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: installedSource,
      tree: "e".repeat(40),
    },
  };
  const view: InstallStatusDevice = {
    environmentId,
    label: "Host",
    canOperate: true,
    observation: { state: installState, freshness: "fresh" },
  };
  const matchingFleet: FleetHostStatus = {
    environmentId,
    operationProtocol: 1,
    planDirectiveProtocol: 1,
    operation: null,
    update: installState,
    enrollment: { environmentId, enrollmentId, enabled: true, continueRunningThreads: false },
  };
  let projected = view;
  let direct: FleetHostStatus | null | undefined = route === "operations" ? null : matchingFleet;
  let current: JonesInstallPlanState = {
    schema: 1,
    documentRevision: 1,
    current: plan,
    last: null,
    laptop: { environmentId: laptop, observedAt: at, activationBlocked: false },
    view: { complete: false, recoveryHistory: 1 },
    clientCommands: 1,
  };
  const waiting = deferred<JonesInstallPlanRequest>();
  const grantReply = deferred();
  const installPlan = vi.fn(
    async (request: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> => {
      if (request.type !== "claim") return { state: current };
      if (
        request.step !== "install" ||
        request.action?.kind !== "install" ||
        request.command === undefined
      )
        throw new Error("Expected a correlated reviewed install claim.");
      waiting.resolve(request);
      await grantReply.promise;
      current = {
        ...current,
        documentRevision: 2,
        current: { ...plan, revision: plan.revision + 1 },
      };
      return {
        state: current,
        grant: {
          planId: plan.planId,
          environmentId,
          operationId: id(3),
          step: "install",
          leaseUntil: "2026-10-27T04:02:00Z",
          request: {
            kind: "operation-start",
            input: {
              operationId: id(3),
              action: request.action,
              environmentId,
              expectedInstalledSource: installedSource,
              correlation: { planId: plan.planId, deviceKey: environmentId },
            },
          },
        },
      };
    },
  );
  const unused = async () => {
    throw new Error("Unexpected host call.");
  };
  const services: InstallPlanServices = {
    bridge: {
      installPlan,
      getUpdateState: async () => ({ jones: installState }) as DesktopUpdateState,
      onUpdateState: () => () => {},
    },
    // The Connections/status projection can omit fleet fields. Observe carries the direct result.
    devices: () => [projected],
    observe: async () => ({
      ...projected,
      ...(direct === undefined ? {} : { fleetStatus: direct }),
    }),
    operation: unused,
    legacy: unused,
    stop: unused,
    host: unused,
    confirm: unused,
    chooseRecovery: unused,
    now,
  };
  const store = createInstallStatusStore();
  const unbind = bindInstallPlanServices(services, store);
  vi.stubGlobal("window", { desktopBridge: { installPlan } });
  const start = vi.fn(
    async (request: JonesOperationStartInput): Promise<JonesOperationReceipt> => ({
      ...request,
      protocol: 1,
      phase: "running",
      acceptedAt: at,
      updatedAt: at,
    }),
  );
  const review = {
    reviewedAt: at,
    stagedHandle: "reviewed-stage",
    targetSource,
    currentVersion: "old",
    expectedInstalledSource: installedSource,
  };
  const input = {
    environmentId,
    targetSource,
    operationId: id(2),
    now,
    review,
    action: {
      kind: "install" as const,
      stagedHandle: review.stagedHandle,
      targetSource,
      currentVersion: review.currentVersion,
    },
    host: { ...host(), start, readLatest: () => view },
  };
  return {
    plan,
    matchingFleet,
    view,
    installPlan,
    start,
    input,
    waiting,
    grantReply,
    unbind,
    journal: createWebJonesJournal(storage()),
    project: (device: InstallStatusDevice) => {
      projected = device;
    },
    observeFleet: (fleetStatus: FleetHostStatus | null | undefined) => {
      direct = fleetStatus;
    },
  };
}

describe("Connections reviewed install enrollment", () => {
  it.each(["operations", "follower-operations"] as const)(
    "dispatches the exact granted %s install with positive direct evidence and an omitted cache field",
    async (route) => {
      const f = connectionsInstallFixture(route);
      try {
        f.grantReply.resolve();
        const result = await createWebJonesExecutor(f.journal)(f.input);
        expect(result.kind).toBe("accepted");
        expect(f.start).toHaveBeenCalledTimes(1);
        expect(f.start).toHaveBeenCalledWith(
          expect.objectContaining({
            operationId: id(3),
            correlation: { planId: f.plan.planId, deviceKey: environmentId },
          }),
        );
      } finally {
        f.unbind();
      }
    },
  );

  it.each(["unknown", "changed enrollment", "changed route"] as const)(
    "makes no native install claim with %s in the per-attempt fleet read",
    async (scenario) => {
      const f = connectionsInstallFixture();
      try {
        f.observeFleet(
          scenario === "unknown"
            ? undefined
            : scenario === "changed route"
              ? null
              : {
                  ...f.matchingFleet,
                  enrollment: { ...f.matchingFleet.enrollment!, enrollmentId: id(5) },
                },
        );
        f.grantReply.resolve();
        expect((await createWebJonesExecutor(f.journal)(f.input)).kind).toBe("refused");
        expect(f.installPlan.mock.calls.map(([request]) => request.type)).toEqual(["read", "read"]);
        expect(f.start).not.toHaveBeenCalled();
        expect(f.journal.getSnapshot().duties).toEqual([]);
      } finally {
        f.unbind();
      }
    },
  );

  it.each([
    "unknown",
    "changed enrollment",
    "changed route",
    "stale status",
    "receipt-only",
  ] as const)(
    "keeps the exact native duty without a host call when %s arrives during the grant",
    async (scenario) => {
      const f = connectionsInstallFixture();
      try {
        const execute = createWebJonesExecutor(f.journal);
        const result = execute(f.input);
        expect(await Promise.race([f.waiting.promise, result])).toMatchObject({
          type: "claim",
          step: "install",
        });
        if (scenario === "unknown") {
          f.observeFleet(undefined);
          await installPlanServices()!.services.observe(environmentId);
        } else if (scenario === "changed enrollment") {
          f.observeFleet({
            ...f.matchingFleet,
            enrollment: { ...f.matchingFleet.enrollment!, enrollmentId: id(5) },
          });
          await installPlanServices()!.services.observe(environmentId);
        } else if (scenario === "changed route") {
          f.project({ ...f.view, fleetStatus: null });
        } else {
          const observation = f.view.observation;
          f.project({
            ...f.view,
            observation:
              scenario === "stale status"
                ? { ...observation, freshness: "stale" }
                : {
                    ...observation,
                    state: {
                      ...observation.state!,
                      capability: {
                        check: false,
                        download: false,
                        install: false,
                        operationReceipts: 1,
                      },
                    },
                  },
          });
        }
        f.grantReply.resolve();
        expect(await result).toMatchObject({ kind: "unknown", code: "native-handoff-unknown" });
        expect(f.start).not.toHaveBeenCalled();
        expect(f.journal.getSnapshot().duties).toMatchObject([
          {
            id: id(2),
            kind: "native",
            environmentId,
            native: { requestType: "claim", planId: f.plan.planId, commandId: id(2) },
          },
        ]);
        expect((await execute({ ...f.input, operationId: id(6) })).kind).toBe("unknown");
        expect(
          f.installPlan.mock.calls.filter(([request]) => request.type === "claim"),
        ).toHaveLength(1);
        expect(f.start).not.toHaveBeenCalled();
      } finally {
        f.grantReply.resolve();
        f.unbind();
      }
    },
  );

  it("rechecks enrollment after the native handoff journal reserves the command", async () => {
    const f = connectionsInstallFixture();
    const reserved = deferred();
    const resume = deferred();
    const execute = createWebJonesExecutor({
      ...f.journal,
      reserve: async (duty) => {
        await f.journal.reserve(duty);
        reserved.resolve();
        await resume.promise;
      },
    });
    try {
      const result = execute(f.input);
      expect(await Promise.race([reserved.promise, result])).toBeUndefined();
      f.observeFleet(undefined);
      await installPlanServices()!.services.observe(environmentId);
      f.grantReply.resolve();
      resume.resolve();
      expect((await result).kind).toBe("refused");
      expect(f.installPlan.mock.calls.map(([request]) => request.type)).toEqual(["read", "read"]);
      expect(f.start).not.toHaveBeenCalled();
      expect(f.journal.getSnapshot().duties).toEqual([]);
    } finally {
      resume.resolve();
      f.grantReply.resolve();
      f.unbind();
    }
  });
});

describe("web durable execution", () => {
  it("releases an exact supported fence, retains an unrelated hold, and requires a fresh explicit install review", async () => {
    const journal = createWebJonesJournal(storage()),
      execute = createWebJonesExecutor(journal);
    let current: JonesUpdateState = {
      ...state,
      capability: { ...state.capability, operations: 1 },
      installedSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: "c".repeat(40),
        tree: "d".repeat(40),
      },
    };
    let captured!: JonesOperationStartInput;
    const start = vi.fn(
      async (request: JonesOperationStartInput): Promise<JonesOperationReceipt> => {
        captured = request;
        throw new Error("reply lost");
      },
    );
    const confirm = vi.fn(async () => true);
    const endpoint: JonesExecutorHost = {
      readLatest: () => ({ observation: { state: current, freshness: "fresh" }, canOperate: true }),
      legacy: vi.fn(),
      start,
      confirm,
    };
    const now = () => Date.parse("2026-10-27T04:00:00Z");
    const input = {
      environmentId,
      targetSource,
      operationId: id(1),
      action: { kind: "download" as const, artifactId: 2, sourceSha: targetSource },
      host: endpoint,
      now,
    };
    expect((await execute(input)).kind).toBe("unknown");
    await journal.reserve({
      id: id(4),
      environmentId: EnvironmentId.make("other-web-environment"),
      kind: "legacy",
      action: "download",
      targetSource,
    });
    expect((await execute({ ...input, operationId: id(2) })).kind).toBe("unknown");
    expect(start).toHaveBeenCalledTimes(1);
    await journal.reconcileReceipt({
      protocol: 1,
      ...captured,
      phase: "fenced",
      updatedAt: "2026-10-27T04:00:00Z",
    });
    current = { ...current, phase: "staged", stagedHandle: "fresh-stage" };
    const reviewed = await prepareReviewedInstall({
      environmentId,
      targetSource,
      label: "host",
      host: endpoint,
      now,
    });
    expect(reviewed.kind).toBe("ready");
    if (reviewed.kind !== "ready") throw new Error("No explicit install review");
    start.mockImplementation(async (request) => ({
      protocol: 1,
      ...request,
      phase: "running",
      acceptedAt: "2026-10-27T04:00:00Z",
      updatedAt: "2026-10-27T04:00:00Z",
    }));
    expect(
      (
        await execute({
          ...input,
          operationId: id(3),
          action: reviewed.action,
          review: reviewed.review,
        })
      ).kind,
    ).toBe("accepted");
    expect(confirm).toHaveBeenCalledTimes(1);
    expect(start).toHaveBeenCalledTimes(2);
    expect(
      journal.getSnapshot().duties.some((d) => d.environmentId === "other-web-environment"),
    ).toBe(true);
  });
  it("records a legacy dispatch before transport and blocks a fresh runtime after a lost response and revision reset", async () => {
    const disk = storage(),
      journal = createWebJonesJournal(disk);
    const legacy = vi.fn(async () => {
      expect(disk.getItem()).toContain(id(1));
      throw new Error("lost reply");
    });
    const input = {
      environmentId,
      targetSource,
      operationId: id(1),
      action: { kind: "download" as const, artifactId: 2, sourceSha: targetSource },
      host: host(legacy),
    };
    expect((await createWebJonesExecutor(journal)(input)).kind).toBe("unknown");
    const restarted = createWebJonesExecutor(createWebJonesJournal(disk));
    expect((await restarted({ ...input, operationId: id(2) })).kind).toBe("unknown");
    expect(legacy).toHaveBeenCalledTimes(1);
  });

  it("treats a lost preliminary native read as not sent, without an immortal duty", async () => {
    const journal = createWebJonesJournal(storage());
    const installPlan = vi.fn(async () => {
      throw new Error("preliminary read lost");
    });
    const unbind = bindInstallPlanServices({
      bridge: { installPlan },
    } as unknown as InstallPlanServices);
    vi.stubGlobal("window", { desktopBridge: { installPlan } });
    try {
      const result = await createWebJonesExecutor(journal)({
        environmentId,
        targetSource,
        operationId: id(1),
        action: { kind: "download", artifactId: 2, sourceSha: targetSource },
        host: host(),
      });
      expect(result.kind).toBe("refused");
      expect(journal.getSnapshot().duties).toEqual([]);
      expect(installPlan.mock.calls).toHaveLength(1);
    } finally {
      unbind();
    }
  });

  it("clears a replacement reservation when the verified plan changes before sending", async () => {
    const now = "2026-10-27T04:00:00Z";
    const laptop = EnvironmentId.make("laptop");
    const activeTree = "b".repeat(40);
    const plan = startPlan({
      planId: id(1),
      target: {
        repository: "Jones-Systems/Jones-Code",
        sourceSha: targetSource,
        sourceTree: activeTree,
        version: "new",
      },
      origin: "connections",
      from: { version: "old" },
      laptopEnvironmentId: laptop,
      select: [],
      now,
    });
    const installPlan = vi.fn(async (): Promise<JonesInstallPlanResult> => {
      const changed = journal.getSnapshot().duties.some((d) => d.id === id(2));
      return {
        state: {
          schema: 1,
          documentRevision: changed ? 2 : 1,
          current: {
            ...plan,
            revision: changed ? 2 : 1,
            laptop: {
              ...plan.laptop,
              gate: {
                openedAt: now,
                basis: "plan-install",
                transactionId: "transaction",
                activeGeneration: "generation",
                activeSource: targetSource,
                activeTree,
              },
            },
          },
          last: null,
          laptop: {
            environmentId: laptop,
            observedAt: now,
            activationBlocked: false,
            activeSource: targetSource,
            activeTree,
            activeGeneration: "generation",
            committedTransactionId: "transaction",
          },
          view: { complete: false },
        },
      };
    });
    const journal = createWebJonesJournal(storage());
    const unbind = bindInstallPlanServices(
      { bridge: { installPlan } } as unknown as InstallPlanServices,
      createInstallStatusStore(),
    );
    vi.stubGlobal("window", { desktopBridge: { installPlan } });
    try {
      const authority = await readLegacyConnectionsAuthority(targetSource, environmentId);
      if (typeof authority === "string") throw new Error(authority);
      const send = vi.fn(async () => true);
      await expect(
        executeWebJonesReplacement(
          {
            environmentId,
            operationId: id(2),
            targetSource,
            stagedHandle: "reviewed",
            authority,
            host: host(),
            canDispatch: () => true,
            send,
          },
          journal,
        ),
      ).rejects.toThrow("The verified Laptop plan changed.");
      expect(installPlan).toHaveBeenCalledTimes(3);
      expect(send).not.toHaveBeenCalled();
      expect(journal.getSnapshot().duties).toEqual([]);
    } finally {
      unbind();
    }
  });

  it("retains exactly one Stop through repeated calls, lost reply, fresh runtime and mismatched proof", async () => {
    const disk = storage(),
      journal = createWebJonesJournal(disk);
    const receipt: JonesOperationReceipt = {
      protocol: 1,
      environmentId,
      operationId: id(1),
      expectedInstalledSource: "c".repeat(40),
      action: { kind: "download-exact", targetSource },
      phase: "running",
      acceptedAt: "2026-10-27T04:00:00Z",
      updatedAt: "2026-10-27T04:00:00Z",
    };
    const uuid = vi.fn(() => id(2)),
      send = vi.fn(async () => {
        throw new Error("Stop reply lost");
      });
    const input = { environmentId, receipt, uuid, send, canOperate: () => true };
    await Promise.all([stopWebJonesDownload(input, journal), stopWebJonesDownload(input, journal)]);
    const restored = createWebJonesJournal(disk);
    await stopWebJonesDownload(input, restored);
    expect(uuid).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledTimes(1);
    expect(await restored.reconcileReceipt({ ...receipt, phase: "stopped" })).toBe(false);
    expect(
      await restored.reconcileReceipt({
        ...receipt,
        phase: "stopped",
        stop: { stopId: id(3), requestedAt: receipt.updatedAt, result: "stopped" },
      }),
    ).toBe(false);
    expect(
      await restored.reconcileReceipt({
        ...receipt,
        phase: "stopped",
        stop: { stopId: id(2), requestedAt: receipt.updatedAt, result: "stopped" },
      }),
    ).toBe(true);
    await stopWebJonesDownload(input, restored);
    expect(uuid).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledTimes(1);
  });
});
